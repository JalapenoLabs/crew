//! The roster endpoints: who is in the unit, and whether each role is live.
//!
//! `GET /roster` lists the registered roles with their owned paths and current
//! liveness (working / idle / stopped / dead), the substrate for the live agent
//! count. A role or the supervisor registers on join with `POST /roster` and leaves
//! with `DELETE /roster/{role}`. Every change is a first-class event on the stream:
//! the transition publishes a [`Lifecycle`] event to `all-units`, so history, the
//! `/stream` feed, and each role's inbox all see who came and went (see
//! `docs/observability.md`). The roster itself lives behind the storage trait, so a
//! durable backend keeps it across a restart.

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::routing::{delete, get};
use axum::{Json, Router};
use crew_core::{ChannelId, Event, EventKind, Lifecycle, RoleId, Sender, Timestamp, ALL_UNITS};
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

/// The `GET /roster` response: the registered roles and their status.
#[derive(Debug, Serialize)]
struct RosterView {
    /// The roles, sorted by id.
    roles: Vec<RoleView>,
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
    /// Renders a roster snapshot as the wire view.
    fn of(roster: &Roster) -> Self {
        let roles = roster
            .iter()
            .map(|(role, status)| RoleView {
                role: role.clone(),
                owned_paths: status.owned_paths.clone(),
                liveness: status.liveness,
            })
            .collect();
        Self { roles }
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
    // A liveness-only update (no `owned_paths`) keeps the role's current paths.
    let owned_paths = match request.owned_paths {
        Some(paths) => paths,
        None => state
            .storage
            .roster()
            .get(&role)
            .map(|status| status.owned_paths.clone())
            .unwrap_or_default(),
    };

    let existed = state.storage.register_role(
        role.clone(),
        RoleStatus {
            owned_paths,
            liveness,
        },
    );
    let mut event = roster_event(&role, lifecycle_for(liveness, existed));
    state.publish(&mut event);

    let code = if existed {
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
    let mut event = roster_event(&role, Lifecycle::Stopped);
    state.publish(&mut event);
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

/// The lifecycle transition a liveness change emits.
///
/// A role reaching `working` for the first time `started`; reaching it again (from
/// idle, or after a stop) `restarted`. The other states map directly.
fn lifecycle_for(liveness: Liveness, already_registered: bool) -> Lifecycle {
    match liveness {
        Liveness::Working if already_registered => Lifecycle::Restarted,
        Liveness::Working => Lifecycle::Started,
        Liveness::Idle => Lifecycle::Idle,
        Liveness::Stopped => Lifecycle::Stopped,
        Liveness::Dead => Lifecycle::Died,
    }
}

/// A roster change as a first-class stream event: the role's lifecycle transition,
/// addressed to `all-units` so the whole unit sees who is live.
fn roster_event(role: &RoleId, lifecycle: Lifecycle) -> Event {
    Event {
        ts: Timestamp::now(),
        from: Sender::Role(role.clone()),
        channel: ChannelId::new(ALL_UNITS),
        task: None,
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
            .expect("a roster change reaches the stream");
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
        let stopped = stream.recv().await.expect("deregister reaches the stream");
        assert!(matches!(
            stopped.kind,
            EventKind::Lifecycle(Lifecycle::Stopped)
        ));
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
