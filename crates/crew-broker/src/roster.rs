//! The roster endpoints: who is in the unit, whether each role is live, and how many.
//!
//! `GET /roster` reports the live agent count and lists the registered roles with
//! their owned paths and current liveness (working / idle / stopped / dead), so a UI
//! shows the count and per-role status with no polling (issue #32). A role or the
//! supervisor registers on join with `POST /roster` and leaves with
//! `DELETE /roster/{role}`. Every change is a first-class event on the stream: the
//! transition publishes a [`Lifecycle`] event to `all-units`, so history, the
//! `/stream` feed, and each role's inbox all see who came and went, and a subscriber
//! keeps the count current from those events (see `docs/observability.md`). The roster
//! itself lives behind the storage trait, so a durable backend keeps it across a
//! restart.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get};
use axum::{Json, Router};
use crew_core::{
    ChannelId, Event, EventKind, Lifecycle, RoleId, Sender, TaskId, Timestamp, ALL_UNITS,
};
use serde::{Deserialize, Serialize};

use crate::error::ApiError;
use crate::events::JsonBody;
use crate::state::AppState;
use crate::store::{Liveness, RoleStatus, Roster};

/// The roster routes: list, register/update, and deregister a role.
pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route("/roster", get(list).post(register))
        .route("/roster/{role}", delete(deregister))
}

/// The `GET /roster` response: the live agent count and the registered roles.
#[derive(Debug, Serialize)]
struct RosterView {
    /// The live agent count and its per-liveness breakdown.
    count: Count,
    /// The roles, sorted by id.
    roles: Vec<RoleView>,
}

/// The live agent count and the per-liveness breakdown behind it (issue #32).
///
/// The count is the current liveness projection, so a UI shows it with no polling: it
/// reads this snapshot once and keeps it current from the `lifecycle` events every
/// roster change publishes to the stream (see `docs/observability.md`).
#[derive(Debug, Default, Serialize)]
struct Count {
    /// Agents present and up or resumable, `working` + `idle`: the headline live
    /// count. A `stopped` role has cleanly left the field and a `dead` one gave up,
    /// so neither is counted.
    live: usize,
    /// Agents up and working.
    working: usize,
    /// Agents registered but idle: parked to save context, resumable on demand.
    idle: usize,
    /// Agents cleanly stood down.
    stopped: usize,
    /// Agents that died and are not recovering.
    dead: usize,
}

/// One role's entry in the roster view.
#[derive(Debug, Serialize)]
struct RoleView {
    /// The role's id.
    role: RoleId,
    /// The directory boundaries the role owns.
    owned_paths: Vec<String>,
    /// The role's current liveness.
    liveness: Liveness,
}

impl RosterView {
    /// Renders a roster snapshot as the wire view, tallying the live count as it goes.
    fn of(roster: &Roster) -> Self {
        let mut count = Count::default();
        let roles = roster
            .iter()
            .map(|(role, status)| {
                match status.liveness {
                    Liveness::Working => count.working += 1,
                    Liveness::Idle => count.idle += 1,
                    Liveness::Stopped => count.stopped += 1,
                    Liveness::Dead => count.dead += 1,
                }
                RoleView {
                    role: role.clone(),
                    owned_paths: status.owned_paths.clone(),
                    liveness: status.liveness,
                }
            })
            .collect();
        count.live = count.working + count.idle;
        Self { count, roles }
    }
}

/// The `POST /roster` body: register a role on join, or update its status.
#[derive(Debug, Deserialize)]
struct Register {
    /// The role to register or update.
    role: String,
    /// The paths the role owns; if omitted, an existing role keeps its current paths.
    owned_paths: Option<Vec<String>>,
    /// The role's liveness; defaults to `working`.
    liveness: Option<Liveness>,
    /// The task this transition belongs to, when the supervisor threads one (issue
    /// #29), so the lifecycle event correlates to the task the role is working.
    #[serde(default)]
    task: Option<TaskId>,
}

/// `GET /roster`: list the registered roles, their owned paths, and their liveness.
async fn list(State(state): State<AppState>) -> Json<RosterView> {
    Json(RosterView::of(&state.storage.roster()))
}

/// `POST /roster`: register a role (on join) or update its liveness and owned paths.
///
/// Returns `201 Created` for a newly registered role, `200 OK` for an update, and
/// publishes the matching [`Lifecycle`] event to the stream.
///
/// # Errors
/// Returns a 400 [`ApiError`] if the body is malformed or the role is empty.
async fn register(
    State(state): State<AppState>,
    JsonBody(request): JsonBody<Register>,
) -> Result<(StatusCode, Json<RosterView>), ApiError> {
    let role = parse_role(&request.role)?;
    let liveness = request.liveness.unwrap_or(Liveness::Working);
    // Read the prior status once, before the update, for both the retained paths and
    // the lifecycle transition (a `dead` role coming back is a recovery, not a restart).
    let prior = state.storage.roster().get(&role).cloned();
    // A liveness-only update (no `owned_paths`) keeps the role's current paths.
    let owned_paths = match request.owned_paths {
        Some(paths) => paths,
        None => prior
            .as_ref()
            .map(|status| status.owned_paths.clone())
            .unwrap_or_default(),
    };

    state.storage.register_role(
        role.clone(),
        RoleStatus {
            owned_paths,
            liveness,
        },
    );
    let prior_liveness = prior.as_ref().map(|status| status.liveness);
    state.publish(roster_event(
        &role,
        lifecycle_for(liveness, prior_liveness),
        request.task,
    ));

    let code = if prior.is_some() {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    Ok((code, Json(RosterView::of(&state.storage.roster()))))
}

/// `DELETE /roster/{role}`: deregister a role (on leave), publishing `stopped`.
///
/// # Errors
/// Returns a 400 if the role is empty, or a 404 if it is not registered.
async fn deregister(
    Path(role): Path<String>,
    State(state): State<AppState>,
) -> Result<Json<RosterView>, ApiError> {
    let role = parse_role(&role)?;
    if state.storage.deregister_role(&role).is_none() {
        return Err(ApiError::not_found(format!(
            "role `{role}` is not registered"
        )));
    }
    // A role leaving the unit is not scoped to a task, so its `stopped` carries none.
    state.publish(roster_event(&role, Lifecycle::Stopped, None));
    Ok(Json(RosterView::of(&state.storage.roster())))
}

/// Validates a role name, rejecting an empty one.
fn parse_role(role: &str) -> Result<RoleId, ApiError> {
    let role = role.trim();
    if role.is_empty() {
        return Err(ApiError::bad_request("role must not be empty"));
    }
    Ok(RoleId::new(role))
}

/// The lifecycle transition a liveness change emits, given the role's prior liveness.
///
/// A role reaching `working` for the first time `started`; coming back from `dead`
/// (the defibrillator revived it) `recovered`; reaching it again from any other state
/// (idle, or after a stop) `restarted`. The other states map directly.
fn lifecycle_for(liveness: Liveness, prior: Option<Liveness>) -> Lifecycle {
    match liveness {
        Liveness::Working => match prior {
            None => Lifecycle::Started,
            Some(Liveness::Dead) => Lifecycle::Recovered,
            Some(_) => Lifecycle::Restarted,
        },
        Liveness::Idle => Lifecycle::Idle,
        Liveness::Stopped => Lifecycle::Stopped,
        Liveness::Dead => Lifecycle::Died,
    }
}

/// A roster change as a first-class stream event: the role's lifecycle transition,
/// addressed to `all-units` so the whole unit sees who is live, correlated to `task`
/// when the supervisor threads one (issue #29).
fn roster_event(role: &RoleId, lifecycle: Lifecycle, task: Option<TaskId>) -> Event {
    Event {
        ts: Timestamp::now(),
        from: Sender::Role(role.clone()),
        channel: ChannelId::new(ALL_UNITS),
        task,
        kind: EventKind::Lifecycle(lifecycle),
    }
}

#[cfg(test)]
mod tests {
    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use crew_core::{EventKind, Lifecycle, RoleId, Sender};
    use serde_json::{json, Value};
    use tower::ServiceExt;

    use crate::api;
    use crate::config::Config;
    use crate::state::AppState;

    async fn send(state: &AppState, request: Request<Body>) -> (StatusCode, Value) {
        let response = api::build(state.clone()).oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    async fn get_roster(state: &AppState) -> Value {
        let request = Request::builder()
            .uri("/roster")
            .body(Body::empty())
            .unwrap();
        send(state, request).await.1
    }

    async fn register(state: &AppState, body: Value) -> (StatusCode, Value) {
        let request = Request::builder()
            .method("POST")
            .uri("/roster")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap();
        send(state, request).await
    }

    async fn deregister(state: &AppState, role: &str) -> (StatusCode, Value) {
        let request = Request::builder()
            .method("DELETE")
            .uri(format!("/roster/{role}"))
            .body(Body::empty())
            .unwrap();
        send(state, request).await
    }

    /// The roster entry for `role` in a `GET /roster` body, if present.
    fn entry<'a>(roster: &'a Value, role: &str) -> Option<&'a Value> {
        roster["roles"]
            .as_array()
            .unwrap()
            .iter()
            .find(|entry| entry["role"] == role)
    }

    #[tokio::test]
    async fn the_roster_reflects_registered_roles_and_their_liveness() {
        let state = AppState::new(Config::default());
        assert_eq!(
            get_roster(&state).await["roles"].as_array().unwrap().len(),
            0
        );

        let (status, _) = register(
            &state,
            json!({ "role": "backend", "owned_paths": ["crates/crew-broker"] }),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "a fresh role is created");
        let (status, _) = register(&state, json!({ "role": "frontend", "liveness": "idle" })).await;
        assert_eq!(status, StatusCode::CREATED);

        let roster = get_roster(&state).await;
        let backend = entry(&roster, "backend").unwrap();
        assert_eq!(
            backend["liveness"], "working",
            "register defaults to working"
        );
        assert_eq!(backend["owned_paths"], json!(["crates/crew-broker"]));
        assert_eq!(entry(&roster, "frontend").unwrap()["liveness"], "idle");
    }

    #[tokio::test]
    async fn the_roster_reports_the_live_count_across_transitions() {
        let state = AppState::new(Config::default());

        // An empty roster has no live agents.
        assert_eq!(get_roster(&state).await["count"]["live"], 0);

        // Two start working, one registers idle: all three are live.
        register(&state, json!({ "role": "backend" })).await;
        register(&state, json!({ "role": "frontend" })).await;
        register(&state, json!({ "role": "qa", "liveness": "idle" })).await;
        let count = get_roster(&state).await["count"].clone();
        assert_eq!(count["working"], 2);
        assert_eq!(count["idle"], 1);
        assert_eq!(count["stopped"], 0);
        assert_eq!(count["dead"], 0);
        assert_eq!(count["live"], 3, "working and idle are both live");

        // backend idles: still live, but the breakdown shifts.
        register(&state, json!({ "role": "backend", "liveness": "idle" })).await;
        let count = get_roster(&state).await["count"].clone();
        assert_eq!(count["working"], 1);
        assert_eq!(count["idle"], 2);
        assert_eq!(count["live"], 3, "an idle agent stays live");

        // frontend dies and qa stops: both drop out of the live count.
        register(&state, json!({ "role": "frontend", "liveness": "dead" })).await;
        register(&state, json!({ "role": "qa", "liveness": "stopped" })).await;
        let count = get_roster(&state).await["count"].clone();
        assert_eq!(count["dead"], 1);
        assert_eq!(count["stopped"], 1);
        assert_eq!(count["live"], 1, "only the idle backend is still live");

        // Deregistering removes the role from the roster entirely.
        deregister(&state, "backend").await;
        let roster = get_roster(&state).await;
        assert_eq!(roster["count"]["live"], 0);
        assert_eq!(
            roster["roles"].as_array().unwrap().len(),
            2,
            "the dead and stopped roles stay listed",
        );
    }

    #[tokio::test]
    async fn updating_liveness_keeps_owned_paths_and_returns_200() {
        let state = AppState::new(Config::default());
        register(
            &state,
            json!({ "role": "backend", "owned_paths": ["crates/crew-broker"] }),
        )
        .await;

        // A liveness-only update omits owned_paths; they must be preserved.
        let (status, _) = register(&state, json!({ "role": "backend", "liveness": "idle" })).await;
        assert_eq!(status, StatusCode::OK, "updating an existing role is 200");
        let backend = get_roster(&state).await;
        let backend = entry(&backend, "backend").unwrap();
        assert_eq!(backend["liveness"], "idle");
        assert_eq!(
            backend["owned_paths"],
            json!(["crates/crew-broker"]),
            "paths preserved"
        );
    }

    #[tokio::test]
    async fn deregister_removes_the_role_and_404s_when_unknown() {
        let state = AppState::new(Config::default());
        register(&state, json!({ "role": "backend" })).await;

        let (status, _) = deregister(&state, "backend").await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            entry(&get_roster(&state).await, "backend").is_none(),
            "role removed"
        );

        let (status, body) = deregister(&state, "backend").await;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "deregistering an unknown role is 404"
        );
        assert!(body.get("error").is_some(), "typed error body: {body}");
    }

    #[tokio::test]
    async fn a_roster_change_emits_a_stream_event() {
        let state = AppState::new(Config::default());
        let mut stream = state.broadcast.subscribe();

        let (status, _) = register(&state, json!({ "role": "backend" })).await;
        assert_eq!(status, StatusCode::CREATED);

        // The register published a Lifecycle::Started for the role, on the stream...
        let started = stream
            .recv()
            .await
            .expect("a roster change reaches the stream")
            .event;
        assert!(matches!(
            started.kind,
            EventKind::Lifecycle(Lifecycle::Started)
        ));
        assert_eq!(started.from, Sender::Role(RoleId::new("backend")));
        assert_eq!(started.channel.as_str(), "all-units");
        // ...and it rode the log, so history and late joiners see it too.
        assert_eq!(state.storage.events().len(), 1);

        // Deregister publishes a Lifecycle::Stopped.
        deregister(&state, "backend").await;
        let stopped = stream
            .recv()
            .await
            .expect("deregister reaches the stream")
            .event;
        assert!(matches!(
            stopped.kind,
            EventKind::Lifecycle(Lifecycle::Stopped)
        ));
    }

    #[tokio::test]
    async fn reviving_a_dead_role_emits_recovered_not_restarted() {
        let state = AppState::new(Config::default());

        // A role that died (the defibrillator reaped it).
        register(&state, json!({ "role": "backend" })).await;
        register(&state, json!({ "role": "backend", "liveness": "dead" })).await;
        assert_eq!(
            entry(&get_roster(&state).await, "backend").unwrap()["liveness"],
            "dead"
        );

        // Bringing a dead role back to working is a recovery, not a plain restart.
        let mut stream = state.broadcast.subscribe();
        register(&state, json!({ "role": "backend", "liveness": "working" })).await;
        let event = stream
            .recv()
            .await
            .expect("the revive reaches the stream")
            .event;
        assert!(
            matches!(event.kind, EventKind::Lifecycle(Lifecycle::Recovered)),
            "a dead role coming back is `recovered`",
        );

        // Whereas restarting a role that was merely idle is a `restarted`.
        register(&state, json!({ "role": "frontend" })).await;
        register(&state, json!({ "role": "frontend", "liveness": "idle" })).await;
        let mut stream = state.broadcast.subscribe();
        register(&state, json!({ "role": "frontend", "liveness": "working" })).await;
        let event = stream
            .recv()
            .await
            .expect("the restart reaches the stream")
            .event;
        assert!(matches!(
            event.kind,
            EventKind::Lifecycle(Lifecycle::Restarted)
        ));
    }

    #[tokio::test]
    async fn a_threaded_task_correlates_the_lifecycle_event() {
        let state = AppState::new(Config::default());
        let mut stream = state.broadcast.subscribe();
        let task = crew_core::TaskId::new();

        // The supervisor threads the task on the registration; `started` carries it.
        let (status, _) = register(
            &state,
            json!({ "role": "backend", "owned_paths": ["api/"], "task": task.to_string() }),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let started = stream.recv().await.unwrap().event;
        assert!(matches!(
            started.kind,
            EventKind::Lifecycle(Lifecycle::Started)
        ));
        assert_eq!(
            started.task,
            Some(task),
            "the lifecycle event correlates to the threaded task",
        );

        // A role leaving the unit is not task-scoped, so `stopped` carries no task.
        deregister(&state, "backend").await;
        let stopped = stream.recv().await.unwrap().event;
        assert!(matches!(
            stopped.kind,
            EventKind::Lifecycle(Lifecycle::Stopped)
        ));
        assert_eq!(stopped.task, None, "a role leaving carries no task");
    }

    #[tokio::test]
    async fn malformed_registrations_are_typed_400s() {
        let state = AppState::new(Config::default());

        let (status, body) = register(&state, json!({ "role": "  " })).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "an empty role is rejected");
        assert!(body.get("error").is_some());

        // Structurally invalid JSON.
        let request = Request::builder()
            .method("POST")
            .uri("/roster")
            .header("content-type", "application/json")
            .body(Body::from("{ not json"))
            .unwrap();
        let (status, body) = send(&state, request).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(body.get("error").is_some());
    }
}
