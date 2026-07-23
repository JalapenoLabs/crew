//! The usage endpoints: report a turn's spend, and read the rollup (issue #55).
//!
//! `POST /telemetry` records a role's per-turn token-and-cost usage as a `telemetry` event
//! on the stream (from the role, to `all-units`), so per-role and aggregate spend is legible
//! off the stream regardless of any budget. `GET /stats` reads the rollup the broker folds
//! from those `telemetry` events (tokens, cost) and the roles' `lifecycle` events (working
//! time): cost, tokens, and time per role and in aggregate, so the cockpit and the Seraphim
//! stats show spend live. The idle-stop that keeps that time bounded is the supervisor's
//! lifecycle machine (issue #22).

use axum::extract::State;
use axum::routing::{get, post};
use axum::{Json, Router};
use crew_core::{
    ChannelId, Event, EventKind, RoleId, Sender, TelemetryEvent, Timestamp, ALL_UNITS,
};
use serde::{Deserialize, Serialize};

use crate::error::ApiError;
use crate::events::JsonBody;
use crate::state::{AppState, RoleStatsView};

/// The usage routes: report a turn's spend, and read the rollup.
pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route("/telemetry", post(report))
        .route("/stats", get(read))
}

/// The `POST /telemetry` body: a role's per-turn token-and-cost usage.
///
/// The supervisor supplies each turn's counts (the token feed is the stream-json activity
/// parser, issue #24); the broker stamps the timestamp and publishes it.
#[derive(Debug, Deserialize)]
struct UsageReport {
    /// The role whose turn this usage belongs to.
    role: String,
    /// The tokens the turn spent.
    tokens: u64,
    /// The turn's cost in micro-USD (millionths of a dollar).
    #[serde(default)]
    cost_micro_usd: u64,
}

/// `POST /telemetry`: record a per-turn usage report on the stream.
///
/// # Errors
/// Returns a 400 [`ApiError`] if the role is empty.
async fn report(
    State(state): State<AppState>,
    JsonBody(request): JsonBody<UsageReport>,
) -> Result<Json<Event>, ApiError> {
    let role = request.role.trim();
    if role.is_empty() {
        return Err(ApiError::bad_request("role must not be empty"));
    }
    let role = RoleId::new(role);

    let event = Event {
        ts: Timestamp::now(),
        from: Sender::Role(role.clone()),
        channel: ChannelId::new(ALL_UNITS),
        task: None,
        kind: EventKind::Telemetry(TelemetryEvent {
            role,
            tokens: request.tokens,
            cost_micro_usd: request.cost_micro_usd,
        }),
    };
    Ok(Json(state.publish(event).event))
}

/// A tokens / cost / time total, per role and for the whole crew.
#[derive(Debug, Serialize)]
struct Totals {
    /// Total tokens spent.
    tokens: u64,
    /// Total cost in micro-USD (millionths of a dollar).
    cost_micro_usd: u64,
    /// Total working time in whole seconds.
    active_secs: u64,
}

/// The `GET /stats` response: the per-role rollup and the crew aggregate.
#[derive(Debug, Serialize)]
struct StatsView {
    /// One line per role that has spent tokens or worked, sorted by role.
    roles: Vec<RoleStatsView>,
    /// The crew total across every role.
    aggregate: Totals,
}

/// `GET /stats`: the cost / tokens / time rollup, per role and in aggregate (issue #55).
async fn read(State(state): State<AppState>) -> Json<StatsView> {
    let roles = state.stats_snapshot(Timestamp::now());
    let aggregate = roles.iter().fold(
        Totals {
            tokens: 0,
            cost_micro_usd: 0,
            active_secs: 0,
        },
        |mut total, role| {
            total.tokens = total.tokens.saturating_add(role.tokens);
            total.cost_micro_usd = total.cost_micro_usd.saturating_add(role.cost_micro_usd);
            total.active_secs = total.active_secs.saturating_add(role.active_secs);
            total
        },
    );
    Json(StatsView { roles, aggregate })
}

#[cfg(test)]
mod tests {
    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use serde_json::{json, Value};
    use tower::ServiceExt;

    use crate::api;
    use crate::config::Config;
    use crate::state::AppState;

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

    async fn post_usage(state: &AppState, role: &str, tokens: u64, cost: u64) -> StatusCode {
        send(
            state,
            "POST",
            "/telemetry",
            json!({ "role": role, "tokens": tokens, "cost_micro_usd": cost }),
        )
        .await
        .0
    }

    #[tokio::test]
    async fn usage_reports_roll_up_per_role_and_in_aggregate() {
        let state = AppState::new(Config::default());
        assert_eq!(
            post_usage(&state, "backend", 1_000, 30_000).await,
            StatusCode::OK
        );
        assert_eq!(
            post_usage(&state, "backend", 500, 15_000).await,
            StatusCode::OK
        );
        assert_eq!(
            post_usage(&state, "frontend", 200, 4_000).await,
            StatusCode::OK
        );

        let (status, rollup) = send(&state, "GET", "/stats", Value::Null).await;
        assert_eq!(status, StatusCode::OK);

        // Per-role: backend's two turns summed, frontend's one.
        let backend = rollup["roles"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["role"] == "backend")
            .unwrap();
        assert_eq!(backend["tokens"], 1_500);
        assert_eq!(backend["cost_micro_usd"], 45_000);

        // Aggregate across both roles.
        assert_eq!(rollup["aggregate"]["tokens"], 1_700);
        assert_eq!(rollup["aggregate"]["cost_micro_usd"], 49_000);
    }

    #[tokio::test]
    async fn a_telemetry_report_reaches_the_stream() {
        let state = AppState::new(Config::default());
        let (status, event) = send(
            &state,
            "POST",
            "/telemetry",
            json!({ "role": "docs", "tokens": 42, "cost_micro_usd": 7 }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(event["kind"]["kind"], "telemetry");
        assert_eq!(event["from"]["id"], "docs");
        assert!(matches!(
            state.storage.events().last().unwrap().kind,
            crew_core::EventKind::Telemetry(_)
        ));
    }

    #[tokio::test]
    async fn an_empty_role_is_rejected() {
        let state = AppState::new(Config::default());
        let (status, _) = send(
            &state,
            "POST",
            "/telemetry",
            json!({ "role": " ", "tokens": 1 }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
}
