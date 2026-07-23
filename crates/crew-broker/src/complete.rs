//! The mission-complete endpoint: report a graceful mission finish (issue
//! #121).
//!
//! Stand-down (`POST /standdown`, issue #41) is the General's emergency halt; a
//! completed mission is the opposite, a graceful finish the crew reports when
//! the work is done. `POST /complete` records it as a `mission_complete`
//! lifecycle event on the stream (from the reporting role, typically the
//! commander, to `all-units`), so `crew notify` fires on a true completion
//! rather than approximating it with a stand-down. It is an announcement, not a
//! brake: unlike stand-down it does not gate the crew, since the mission is
//! finished, not paused.

use axum::{extract::State, routing::post, Json, Router};
use crew_core::{ChannelId, Event, EventKind, Lifecycle, RoleId, Sender, Timestamp, ALL_UNITS};
use serde::Deserialize;

use crate::{error::ApiError, events::JsonBody, state::AppState};

/// The mission-complete route: report a graceful mission finish.
pub(crate) fn routes() -> Router<AppState> {
    Router::new().route("/complete", post(report))
}

/// The `POST /complete` body: the role reporting the mission complete.
#[derive(Debug, Deserialize)]
struct CompleteReport {
    /// The role declaring the mission finished, typically the commander.
    role: String,
}

/// `POST /complete`: record a graceful mission completion on the stream.
///
/// # Errors
/// Returns a 400 [`ApiError`] if the role is empty.
async fn report(
    State(state): State<AppState>,
    JsonBody(request): JsonBody<CompleteReport>,
) -> Result<Json<Event>, ApiError> {
    let role = request.role.trim();
    if role.is_empty() {
        return Err(ApiError::bad_request("role must not be empty"));
    }

    let event = Event {
        ts: Timestamp::now(),
        from: Sender::Role(RoleId::new(role)),
        channel: ChannelId::new(ALL_UNITS),
        task: None,
        kind: EventKind::Lifecycle(Lifecycle::MissionComplete),
    };
    Ok(Json(state.publish(event).event))
}

#[cfg(test)]
mod tests {
    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
    };
    use crew_core::{EventKind, Lifecycle};
    use serde_json::{json, Value};
    use tower::ServiceExt;

    use crate::{api, config::Config, state::AppState};

    async fn post(state: &AppState, body: Value) -> (StatusCode, Value) {
        let request = Request::builder()
            .method("POST")
            .uri("/complete")
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        let response = api::build(state.clone()).oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    #[tokio::test]
    async fn a_completion_is_recorded_on_the_stream() {
        let state = AppState::new(Config::default());
        let mut stream = state.broadcast.subscribe();

        let (status, event) = post(&state, json!({ "role": "commander" })).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(event["kind"]["data"], "mission_complete");
        assert_eq!(event["from"]["id"], "commander");
        assert_eq!(event["channel"], "all-units");

        // The completion reached the live stream for a watcher (crew notify) to see.
        let streamed = stream.try_recv().unwrap().event;
        assert_eq!(
            streamed.kind,
            EventKind::Lifecycle(Lifecycle::MissionComplete)
        );

        // And it is in the durable log.
        let stored = state.storage.events();
        assert!(matches!(
            stored.last().unwrap().kind,
            EventKind::Lifecycle(Lifecycle::MissionComplete)
        ));
    }

    #[tokio::test]
    async fn a_completion_does_not_gate_the_crew() {
        // Completion is a graceful finish, not a brake: unlike stand-down it must
        // not pause new work.
        let state = AppState::new(Config::default());
        let (status, _) = post(&state, json!({ "role": "commander" })).await;
        assert_eq!(status, StatusCode::OK);
        assert!(
            !state.is_role_paused(&crew_core::RoleId::new("backend")),
            "a mission completion must not gate the crew"
        );
    }

    #[tokio::test]
    async fn an_empty_role_is_rejected() {
        let state = AppState::new(Config::default());
        let (status, _) = post(&state, json!({ "role": "  " })).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
}
