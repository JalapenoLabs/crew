//! The stall endpoint: surface a coordination stall on the stream (issue #120).
//!
//! The fleet-wide stall monitor (issue #48, `crew_supervisor::stall`) detects a
//! crew stuck waiting on itself and, until now, escalated it only as a
//! supervisor log. This endpoint is the surface: `POST /stall` records the
//! monitor's finding as a `stall` event on the stream (from the General, to
//! `all-units`), so `crew notify` can push the "a role is stalled" moment and
//! the `crew top` cockpit can render live stalls. The paired
//! [`StallStatus`](crew_core::StallStatus) says whether the reading raised a
//! stall or cleared it.
//!
//! The monitor is the sole authority on what a stall is, so the broker takes
//! the [`StallEvent`](crew_core::StallEvent) payload as given and only stamps
//! the envelope (timestamp, sender, channel) and publishes it.

use axum::{extract::State, routing::post, Json, Router};
use crew_core::{ChannelId, Event, EventKind, Sender, StallEvent, Timestamp, ALL_UNITS};

use crate::{error::ApiError, events::JsonBody, state::AppState};

/// The stall route: surface a detected or resolved coordination stall.
pub(crate) fn routes() -> Router<AppState> {
    Router::new().route("/stall", post(report))
}

/// `POST /stall`: record a detected or resolved coordination stall on the
/// stream.
///
/// The body is the [`StallEvent`] the monitor built (its kind, status, roles,
/// and specific detail); the broker stamps the envelope and publishes it.
///
/// # Errors
/// Returns a 400 [`ApiError`] if the detail is empty (a stall must name its
/// cause).
async fn report(
    State(state): State<AppState>,
    JsonBody(event): JsonBody<StallEvent>,
) -> Result<Json<Event>, ApiError> {
    if event.detail.trim().is_empty() {
        return Err(ApiError::bad_request("detail must not be empty"));
    }

    let event = Event {
        ts: Timestamp::now(),
        // A stall is a crew-level finding, not one role's action, so it comes
        // from the General to the whole unit, like a shared-usage reading.
        from: Sender::General,
        channel: ChannelId::new(ALL_UNITS),
        task: None,
        kind: EventKind::Stall(event),
    };
    Ok(Json(state.publish(event).event))
}

#[cfg(test)]
mod tests {
    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
    };
    use crew_core::{EventKind, RoleId, StallEvent, StallKind, StallStatus};
    use serde_json::{json, Value};
    use tower::ServiceExt;

    use crate::{api, config::Config, state::AppState};

    async fn post(state: &AppState, body: Value) -> (StatusCode, Value) {
        let request = Request::builder()
            .method("POST")
            .uri("/stall")
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
    async fn a_detected_stall_is_recorded_on_the_stream() {
        let state = AppState::new(Config::default());
        let mut stream = state.broadcast.subscribe();

        let (status, event) = post(
            &state,
            json!({
                "kind": "deadlock",
                "status": "detected",
                "roles": ["backend", "frontend"],
                "detail": "deadlock: backend waits on frontend, and frontend waits on backend",
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(event["kind"]["kind"], "stall");
        // A crew-level finding rides from the General to the whole unit.
        assert_eq!(event["from"]["kind"], "general");
        assert_eq!(event["channel"], "all-units");

        // The stall event reached the live stream for a watcher to see.
        let streamed = stream.try_recv().unwrap().event;
        assert_eq!(
            streamed.kind,
            EventKind::Stall(StallEvent {
                kind: StallKind::Deadlock,
                status: StallStatus::Detected,
                roles: vec![RoleId::new("backend"), RoleId::new("frontend")],
                detail: "deadlock: backend waits on frontend, and frontend waits on backend"
                    .to_owned(),
            })
        );

        // And it is in the durable log, filterable by kind.
        let stored = state.storage.events();
        assert!(matches!(stored.last().unwrap().kind, EventKind::Stall(_)));
    }

    #[tokio::test]
    async fn a_resolved_stall_is_recorded() {
        let state = AppState::new(Config::default());
        let (status, event) = post(
            &state,
            json!({
                "kind": "ledger_stall",
                "status": "resolved",
                "roles": ["backend"],
                "detail": "ledger task `login` moved forward",
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(event["kind"]["data"]["status"], "resolved");
    }

    #[tokio::test]
    async fn an_empty_detail_is_rejected() {
        let state = AppState::new(Config::default());
        let (status, _) = post(
            &state,
            json!({ "kind": "deadlock", "status": "detected", "roles": ["backend"], "detail": "  " }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
}
