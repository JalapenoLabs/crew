//! The mission-complete endpoint: report a graceful mission finish (issue
//! #121).
//!
//! Stand-down (`POST /standdown`, issue #41) is the General's emergency halt; a
//! completed mission is the opposite, a graceful finish the crew reports when
//! the work is done. `POST /complete` records it as a `mission` event on the
//! stream (from the reporting role, typically the commander, to `all-units`),
//! so `crew notify` fires on a true completion rather than approximating it
//! with a stand-down. The event carries a short `summary` of what shipped
//! (issue #155), so the completion push has context instead of a bare marker;
//! an empty or missing summary is fine. It is an announcement, not a brake:
//! unlike stand-down it does not gate the crew, since the mission is finished,
//! not paused.

use axum::{extract::State, routing::post, Json, Router};
use crew_core::{ChannelId, Event, EventKind, MissionEvent, RoleId, Sender, Timestamp, ALL_UNITS};
use serde::Deserialize;

use crate::{error::ApiError, events::JsonBody, state::AppState};

/// The mission-complete route: report a graceful mission finish.
pub(crate) fn routes() -> Router<AppState> {
    Router::new().route("/complete", post(report))
}

/// The `POST /complete` body: the role reporting the mission complete, and a
/// short summary of what shipped.
#[derive(Debug, Deserialize)]
struct CompleteReport {
    /// The role declaring the mission finished, typically the commander.
    role: String,
    /// A short summary of what the mission shipped, for the completion
    /// notification (issue #155). Optional; empty when the reporter gave none.
    #[serde(default)]
    summary: String,
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
        kind: EventKind::Mission(MissionEvent {
            summary: request.summary.trim().to_owned(),
        }),
    };
    Ok(Json(state.publish(event).event))
}

#[cfg(test)]
mod tests {
    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
    };
    use crew_core::{EventKind, MissionEvent};
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

        let (status, event) = post(
            &state,
            json!({ "role": "commander", "summary": "shipped the auth gateway" }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(event["kind"]["kind"], "mission");
        assert_eq!(
            event["kind"]["data"]["summary"], "shipped the auth gateway",
            "the completion carries the mission summary for the notification"
        );
        assert_eq!(event["from"]["id"], "commander");
        assert_eq!(event["channel"], "all-units");

        // The completion reached the live stream for a watcher (crew notify) to see.
        let streamed = stream.try_recv().unwrap().event;
        assert_eq!(
            streamed.kind,
            EventKind::Mission(MissionEvent {
                summary: "shipped the auth gateway".to_owned()
            })
        );

        // And it is in the durable log.
        let stored = state.storage.events();
        assert!(matches!(stored.last().unwrap().kind, EventKind::Mission(_)));
    }

    #[tokio::test]
    async fn a_completion_without_a_summary_is_accepted() {
        // The summary is optional: a bare completion still records a `mission`
        // event, just with an empty summary.
        let state = AppState::new(Config::default());
        let (status, event) = post(&state, json!({ "role": "commander" })).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(event["kind"]["kind"], "mission");
        assert_eq!(event["kind"]["data"]["summary"], "");
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
