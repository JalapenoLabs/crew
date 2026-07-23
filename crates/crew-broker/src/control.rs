//! The crew control endpoints: pause, resume, and stand down (issue #41).
//!
//! The General's brake and kill switch. `POST /pause` and `POST /resume` gate
//! work per role (a `role` in the body) or crew-wide (no body, or an empty
//! one); `POST /standdown` halts the whole crew at once and preserves the
//! durable state, so the crew is recoverable. Each records the change as a
//! `lifecycle` event on the stream and returns the roster, so pause state is
//! visible on both the roster and the stream (see `docs/observability.md`).
//!
//! The control state lives in the broker ([`AppState`]); a role honors it by
//! pulling no new work while it, or the crew, is paused (its role card says
//! so).

use axum::{body::Bytes, extract::State, routing::post, Json, Router};
use crew_core::{ChannelId, Event, EventKind, Lifecycle, RoleId, Sender, Timestamp, ALL_UNITS};
use serde::Deserialize;

use crate::{error::ApiError, roster::RosterView, state::AppState};

/// The control routes: pause, resume, and stand down.
pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route("/pause", post(pause))
        .route("/resume", post(resume))
        .route("/standdown", post(standdown))
}

/// The body of `POST /pause` and `POST /resume`: an optional role. None (an
/// empty body or `{}`) means the whole crew.
#[derive(Debug, Default, Deserialize)]
struct Target {
    /// The role to pause or resume; omit to act on the whole crew.
    #[serde(default)]
    role: Option<String>,
}

/// `POST /pause`: pause one role (with a `role`) or the whole crew (without),
/// gating it from new work until resumed.
///
/// # Errors
/// Returns a 400 [`ApiError`] on a malformed body, or a 404 if a named role is
/// not registered.
async fn pause(State(state): State<AppState>, body: Bytes) -> Result<Json<RosterView>, ApiError> {
    if let Some(role) = target_role(&body, &state)? {
        state.pause_role(role.clone());
        state.publish(control_event(Sender::Role(role), Lifecycle::Paused));
    } else {
        state.pause_crew();
        state.publish(control_event(Sender::General, Lifecycle::Paused));
    }
    Ok(Json(RosterView::from_state(&state)))
}

/// `POST /resume`: resume one role (with a `role`) or the whole crew (without).
///
/// Resuming the crew clears a crew-wide pause or stand-down; a role paused on
/// its own stays paused until resumed by name.
///
/// # Errors
/// Returns a 400 [`ApiError`] on a malformed body, or a 404 if a named role is
/// not registered.
async fn resume(State(state): State<AppState>, body: Bytes) -> Result<Json<RosterView>, ApiError> {
    if let Some(role) = target_role(&body, &state)? {
        state.resume_role(&role);
        state.publish(control_event(Sender::Role(role), Lifecycle::Resumed));
    } else {
        // `crew resume` is the one escape hatch: it lifts a manual pause and any usage
        // auto-pause (issue #56). Surface the usage lift so an early resume is not
        // silent.
        let lifted_usage = state.resume_crew();
        state.publish(control_event(Sender::General, Lifecycle::Resumed));
        if lifted_usage {
            state.publish(crate::usage::usage_event(&state.usage_snapshot()));
        }
    }
    Ok(Json(RosterView::from_state(&state)))
}

/// `POST /standdown`: the emergency halt. Stands the whole crew down at once,
/// records it on the stream, and preserves the durable log and roster so the
/// crew is recoverable (resume, or a fresh `crew up`).
async fn standdown(State(state): State<AppState>) -> Json<RosterView> {
    state.stand_down();
    state.publish(control_event(Sender::General, Lifecycle::StoodDown));
    Json(RosterView::from_state(&state))
}

/// Parses the optional target role from a pause/resume body, validating that a
/// named role is registered.
///
/// An empty body or `{}` targets the whole crew (`None`).
fn target_role(body: &[u8], state: &AppState) -> Result<Option<RoleId>, ApiError> {
    let target: Target = if body.iter().all(u8::is_ascii_whitespace) {
        Target::default()
    } else {
        serde_json::from_slice(body)
            .map_err(|error| ApiError::bad_request(format!("invalid request body: {error}")))?
    };

    let Some(name) = target.role else {
        return Ok(None);
    };
    let name = name.trim();
    if name.is_empty() {
        return Err(ApiError::bad_request("role must not be empty"));
    }
    let role = RoleId::new(name);
    if state.storage.roster().get(&role).is_none() {
        return Err(ApiError::not_found(format!(
            "role `{role}` is not registered"
        )));
    }
    Ok(Some(role))
}

/// A crew control change as a first-class stream event, addressed to
/// `all-units`: a per-role change is `from` the role, a crew-wide one `from`
/// the General.
fn control_event(from: Sender, lifecycle: Lifecycle) -> Event {
    Event {
        ts: Timestamp::now(),
        from,
        channel: ChannelId::new(ALL_UNITS),
        task: None,
        kind: EventKind::Lifecycle(lifecycle),
    }
}

#[cfg(test)]
mod tests {
    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
    };
    use crew_core::{EventKind, Lifecycle, RoleId};
    use serde_json::{json, Value};
    use tower::ServiceExt;

    use crate::{
        api,
        config::Config,
        state::AppState,
        store::{Liveness, RoleStatus},
    };

    /// A broker state with `backend` and `frontend` registered and working.
    fn state_with_two_roles() -> AppState {
        let state = AppState::new(Config::default());
        for role in ["backend", "frontend"] {
            state.storage.register_role(
                RoleId::new(role),
                RoleStatus {
                    owned_paths: vec![],
                    liveness: Liveness::Working,
                },
            );
        }
        state
    }

    async fn post(state: &AppState, path: &str, body: Value) -> (StatusCode, Value) {
        let request = Request::builder()
            .method("POST")
            .uri(path)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let response = api::build(state.clone()).oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let value = if bytes.is_empty() {
            Value::Null
        } else {
            serde_json::from_slice(&bytes).unwrap()
        };
        (status, value)
    }

    #[tokio::test]
    async fn pausing_a_role_marks_it_on_the_roster_and_the_stream() {
        let state = state_with_two_roles();
        let mut stream = state.broadcast.subscribe();

        let (status, view) = post(&state, "/pause", json!({ "role": "backend" })).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            view["standing"], "running",
            "one role paused is not a crew pause"
        );

        // The roster flags backend paused, frontend not.
        let backend = view["roles"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["role"] == "backend")
            .unwrap();
        assert_eq!(backend["paused"], true);
        let frontend = view["roles"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["role"] == "frontend")
            .unwrap();
        assert_eq!(frontend["paused"], false);

        // The state gates backend but not frontend.
        assert!(state.is_role_paused(&RoleId::new("backend")));
        assert!(!state.is_role_paused(&RoleId::new("frontend")));

        // A `paused` lifecycle event reached the stream.
        let event = stream.try_recv().unwrap().event;
        assert!(matches!(
            event.kind,
            EventKind::Lifecycle(Lifecycle::Paused)
        ));
    }

    #[tokio::test]
    async fn a_global_pause_gates_every_role_then_resume_clears_it() {
        let state = state_with_two_roles();

        let (_, view) = post(&state, "/pause", json!({})).await;
        assert_eq!(view["standing"], "paused");
        assert!(state.is_role_paused(&RoleId::new("backend")));
        assert!(state.is_role_paused(&RoleId::new("frontend")));

        let (_, view) = post(&state, "/resume", json!({})).await;
        assert_eq!(view["standing"], "running");
        assert!(!state.is_role_paused(&RoleId::new("backend")));
    }

    #[tokio::test]
    async fn standdown_halts_the_crew_and_survives_a_plain_resume_of_a_role() {
        let state = state_with_two_roles();
        let mut stream = state.broadcast.subscribe();

        let (status, view) = post(&state, "/standdown", Value::Null).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(view["standing"], "stood_down");
        assert!(state.is_role_paused(&RoleId::new("backend")));

        let event = stream.try_recv().unwrap().event;
        assert!(matches!(
            event.kind,
            EventKind::Lifecycle(Lifecycle::StoodDown)
        ));

        // Resuming one role does not lift a crew stand-down.
        post(&state, "/resume", json!({ "role": "backend" })).await;
        assert!(state.is_role_paused(&RoleId::new("backend")));

        // Resuming the crew does.
        post(&state, "/resume", json!({})).await;
        assert!(!state.is_role_paused(&RoleId::new("backend")));
    }

    #[tokio::test]
    async fn pausing_an_unregistered_role_is_a_404() {
        let state = state_with_two_roles();
        let (status, _) = post(&state, "/pause", json!({ "role": "ghost" })).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }
}
