//! The subscription-usage endpoints: report a reading, and read the gauge
//! (issue #56).
//!
//! The crew shares one subscription, so the broker keeps one usage gauge across
//! the crew. `POST /usage` records a reading of the shared window against its
//! limit; a reading at or above the configured threshold auto-pauses new work
//! until the window resets, publishing a `usage` event so the pause is visible,
//! never silent. The gate lifts lazily at the reset instant, and `crew resume`
//! lifts it early (see `control.rs`). `GET /usage` reads the gauge: the latest
//! reading, the threshold, and the pause. The usage signal is the supervisor's
//! to detect from the agents' rate-limit output (the stream-json parser, issue
//! #24); this is the surface it reports to.

use axum::{extract::State, routing::post, Json, Router};
use crew_core::{ChannelId, Event, EventKind, Sender, Timestamp, UsageEvent, ALL_UNITS};
use serde::Deserialize;

use crate::{
    events::JsonBody,
    state::{AppState, UsageView},
};

/// The usage routes: report a reading (`POST`), and read the gauge (`GET`).
pub(crate) fn routes() -> Router<AppState> {
    Router::new().route("/usage", post(report).get(read))
}

/// The `POST /usage` body: a reading of the shared window and when it resets.
#[derive(Debug, Deserialize)]
struct UsageReport {
    /// The window's fill against the shared limit, as a percent (`0..=100`).
    percent: u8,
    /// When the window resets, so an auto-pause knows when to lift.
    window_reset: Timestamp,
}

/// `POST /usage`: record a shared-subscription usage reading, auto-pausing if
/// it crosses the threshold. Returns the gauge.
async fn report(
    State(state): State<AppState>,
    JsonBody(request): JsonBody<UsageReport>,
) -> Json<UsageView> {
    // Auto-pause on crossing the threshold, and surface it so the pause is never
    // silent.
    if state.report_usage(request.percent, request.window_reset) {
        state.publish(usage_event(&state.usage_snapshot()));
    }
    Json(state.usage_snapshot())
}

/// `GET /usage`: the shared-subscription usage gauge (issue #56).
async fn read(State(state): State<AppState>) -> Json<UsageView> {
    Json(state.usage_snapshot())
}

/// Builds the `usage` stream event for a gauge snapshot: the reading and its
/// pause.
pub(crate) fn usage_event(view: &UsageView) -> Event {
    Event {
        ts: Timestamp::now(),
        from: Sender::General,
        channel: ChannelId::new(ALL_UNITS),
        task: None,
        kind: EventKind::Usage(UsageEvent {
            percent: view.percent,
            window_reset: view.resets_at,
            paused: view.paused,
        }),
    }
}

#[cfg(test)]
mod tests {
    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
    };
    use crew_core::EventKind;
    use serde_json::{json, Value};
    use tower::ServiceExt;

    use crate::{api, config::Config, state::AppState};

    async fn send(state: &AppState, method: &str, uri: &str, body: Value) -> (StatusCode, Value) {
        let request = Request::builder()
            .method(method)
            .uri(uri)
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

    /// A window reset far in the future, so an engaged pause holds through the
    /// test.
    const FUTURE_RESET: &str = "2099-01-01T00:00:00Z";

    #[tokio::test]
    async fn a_reading_below_the_threshold_does_not_pause() {
        // Default threshold is 90 percent.
        let state = AppState::new(Config::default());
        let (status, gauge) = send(
            &state,
            "POST",
            "/usage",
            json!({ "percent": 80, "window_reset": FUTURE_RESET }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(gauge["percent"], 80);
        assert_eq!(gauge["paused"], false, "under the threshold: {gauge}");
        assert!(!state.is_role_paused(&crew_core::RoleId::new("backend")));
    }

    #[tokio::test]
    async fn crossing_the_threshold_auto_pauses_and_is_surfaced() {
        let state = AppState::new(Config::default());
        let mut stream = state.broadcast.subscribe();

        let (status, gauge) = send(
            &state,
            "POST",
            "/usage",
            json!({ "percent": 95, "window_reset": FUTURE_RESET }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            gauge["paused"], true,
            "at the threshold work auto-pauses: {gauge}"
        );

        // Every role is gated, since one subscription is shared across the crew.
        assert!(state.is_role_paused(&crew_core::RoleId::new("backend")));
        assert!(state.is_role_paused(&crew_core::RoleId::new("frontend")));

        // The auto-pause reached the stream, never silent, carrying the reset time.
        let streamed = stream.try_recv().unwrap().event;
        let EventKind::Usage(reported) = streamed.kind else {
            panic!("expected a usage event");
        };
        assert!(reported.paused && reported.window_reset.is_some());
    }

    #[tokio::test]
    async fn resume_lifts_the_auto_pause_early() {
        let state = AppState::new(Config::default());
        send(
            &state,
            "POST",
            "/usage",
            json!({ "percent": 99, "window_reset": FUTURE_RESET }),
        )
        .await;
        assert!(state.is_usage_paused(), "armed");

        // `crew resume` (POST /resume with no role) is the manual escape hatch.
        let (status, _) = send(&state, "POST", "/resume", json!({})).await;
        assert_eq!(status, StatusCode::OK);
        assert!(!state.is_usage_paused(), "the operator resumed early");
        assert!(!state.is_role_paused(&crew_core::RoleId::new("backend")));

        let (_, gauge) = send(&state, "GET", "/usage", Value::Null).await;
        assert_eq!(gauge["paused"], false);
    }

    #[tokio::test]
    async fn the_pause_lifts_at_the_window_reset() {
        // A reset already in the past: the reading arms the pause, but it is already
        // lifted.
        let state = AppState::new(Config::default());
        send(
            &state,
            "POST",
            "/usage",
            json!({ "percent": 100, "window_reset": "2000-01-01T00:00:00Z" }),
        )
        .await;
        assert!(
            !state.is_usage_paused(),
            "a window already reset does not gate work"
        );
    }
}
