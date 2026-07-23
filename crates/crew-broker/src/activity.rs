//! The activity endpoint: record an agent's own stream-json activity (issue
//! #24).
//!
//! The supervisor parses each agent's headless `claude -p` stream-json into
//! [`Activity`] items (turn boundaries, tool calls, text output) and posts them
//! here. `POST /activity` records each as an `activity` event keyed by the
//! role, on the role's own `@role` channel, so it rides the aggregate stream
//! and the role's per-agent timeline (`GET /activity?agent=<role>`) without
//! reaching other roles' inboxes the way a broadcast would. The event carries
//! the supervisor's task when one is set, so activity correlates to the work
//! (issue #29). An unrecognized stream shape arrives as [`Activity::Other`], so
//! a schema drift is visible on the stream rather than dropped.

use axum::{extract::State, routing::post, Json, Router};
use crew_core::{Activity, Channel, Event, EventKind, RoleId, Sender, TaskId, Timestamp};
use serde::Deserialize;

use crate::{error::ApiError, events::JsonBody, state::AppState};

/// The activity route: record an agent's parsed stream-json activity.
pub(crate) fn routes() -> Router<AppState> {
    Router::new().route("/activity", post(record))
}

/// The `POST /activity` body: one parsed activity item for a role.
#[derive(Debug, Deserialize)]
struct ActivityReport {
    /// The role whose process produced the activity.
    role: String,
    /// The parsed activity item (turn boundary, tool call, output, or other).
    activity: Activity,
    /// The task the supervisor is working, correlated onto the event when set
    /// (issue #29); omitted outside any task context.
    #[serde(default)]
    task: Option<TaskId>,
}

/// `POST /activity`: record a role's parsed stream-json activity on the stream.
///
/// # Errors
/// Returns a 400 [`ApiError`] if the role is empty.
async fn record(
    State(state): State<AppState>,
    JsonBody(request): JsonBody<ActivityReport>,
) -> Result<Json<Event>, ApiError> {
    let role = request.role.trim();
    if role.is_empty() {
        return Err(ApiError::bad_request("role must not be empty"));
    }
    let role = RoleId::new(role);

    let event = Event {
        ts: Timestamp::now(),
        from: Sender::Role(role.clone()),
        // The role's own channel: on the aggregate stream and the role's timeline,
        // but off every other role's inbox, since activity is high-volume.
        channel: Channel::Direct(role.clone()).name(),
        task: request.task,
        kind: EventKind::Activity(request.activity),
    };
    Ok(Json(state.publish(event).event))
}

#[cfg(test)]
mod tests {
    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
    };
    use crew_core::{Activity, EventKind};
    use serde_json::{json, Value};
    use tower::ServiceExt;

    use crate::{api, config::Config, state::AppState};

    async fn post(state: &AppState, body: Value) -> (StatusCode, Value) {
        let request = Request::builder()
            .method("POST")
            .uri("/activity")
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
    async fn a_tool_call_is_recorded_on_the_stream() {
        let state = AppState::new(Config::default());
        let mut stream = state.broadcast.subscribe();

        let (status, event) = post(
            &state,
            json!({ "role": "backend", "activity": { "kind": "tool_call", "tool": "Read" } }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(event["kind"]["kind"], "activity");
        assert_eq!(event["from"]["id"], "backend");
        // The role's own channel, so it never reaches another role's inbox.
        assert_eq!(event["channel"], "@backend");

        // It reached the live stream for the per-agent activity view to render.
        let streamed = stream.try_recv().unwrap().event;
        assert_eq!(
            streamed.kind,
            EventKind::Activity(Activity::ToolCall {
                tool: "Read".to_owned(),
            })
        );

        // And it is in the durable log, filterable by kind.
        let stored = state.storage.events();
        assert!(matches!(
            stored.last().unwrap().kind,
            EventKind::Activity(_)
        ));
    }

    #[tokio::test]
    async fn a_turn_and_an_unknown_shape_both_record() {
        let state = AppState::new(Config::default());

        let (status, event) = post(
            &state,
            json!({ "role": "qa", "activity": { "kind": "turn_started" } }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(event["kind"]["data"]["kind"], "turn_started");

        // An `Other` (a drifted shape) round-trips and records, never rejected.
        let (status, event) = post(
            &state,
            json!({ "role": "qa", "activity": { "kind": "other", "raw": "telepathy_event" } }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(event["kind"]["data"]["raw"], "telepathy_event");
    }

    #[tokio::test]
    async fn a_task_correlates_onto_the_event() {
        let state = AppState::new(Config::default());
        let task = "11111111-1111-1111-1111-111111111111";
        let (status, event) = post(
            &state,
            json!({
                "role": "backend",
                "activity": { "kind": "turn_ended" },
                "task": task,
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(event["task"], task);
    }

    #[tokio::test]
    async fn an_empty_role_is_rejected() {
        let state = AppState::new(Config::default());
        let (status, _) = post(
            &state,
            json!({ "role": "  ", "activity": { "kind": "turn_started" } }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
}
