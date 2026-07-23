//! The budget endpoint: report a role's token spend against the crew budget (issue #54).
//!
//! The supervisor holds the crew's token [`Budget`](crew_core::Budget) and records each
//! turn's spend against it. This endpoint is the surface: `POST /budget` records a spend
//! report as a `budget` event on the stream (from the role, to `all-units`), so a UI reads
//! spend against budget off the stream and a cap hit is never silent. When the report
//! carries a `breach`, it marks the moment the supervisor idle-stops the role or the crew
//! rather than overrun (see `docs/observability.md`).

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use crew_core::{
    BudgetEvent, BudgetScope, ChannelId, Event, EventKind, RoleId, Sender, Timestamp, ALL_UNITS,
};
use serde::Deserialize;

use crate::error::ApiError;
use crate::events::JsonBody;
use crate::state::AppState;

/// The budget route: report a role's spend against the crew budget.
pub(crate) fn routes() -> Router<AppState> {
    Router::new().route("/budget", post(report))
}

/// The `POST /budget` body: a role's token spend against the crew budget.
///
/// The supervisor supplies the running totals it computed from the crew's
/// [`Budget`](crew_core::Budget); the broker stamps the timestamp and publishes it.
#[derive(Debug, Deserialize)]
struct BudgetReport {
    /// The role whose spend this report is about.
    role: String,
    /// The role's cumulative token spend.
    role_spent: u64,
    /// The role's own cap, if it has one.
    #[serde(default)]
    role_cap: Option<u64>,
    /// The crew's cumulative token spend across every role.
    crew_spent: u64,
    /// The crew-wide budget, if the crew has one.
    #[serde(default)]
    crew_budget: Option<u64>,
    /// The ceiling this spend hit, if any: `role` or `crew`.
    #[serde(default)]
    breach: Option<BudgetScope>,
}

/// `POST /budget`: record a token-spend report on the stream.
///
/// # Errors
/// Returns a 400 [`ApiError`] if the role is empty.
async fn report(
    State(state): State<AppState>,
    JsonBody(request): JsonBody<BudgetReport>,
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
        kind: EventKind::Budget(BudgetEvent {
            role,
            role_spent: request.role_spent,
            role_cap: request.role_cap,
            crew_spent: request.crew_spent,
            crew_budget: request.crew_budget,
            breach: request.breach,
        }),
    };
    Ok(Json(state.publish(event).event))
}

#[cfg(test)]
mod tests {
    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use crew_core::{BudgetScope, EventKind};
    use serde_json::{json, Value};
    use tower::ServiceExt;

    use crate::api;
    use crate::config::Config;
    use crate::state::AppState;

    async fn post(state: &AppState, body: Value) -> (StatusCode, Value) {
        let request = Request::builder()
            .method("POST")
            .uri("/budget")
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
    async fn a_cap_hit_is_recorded_on_the_stream() {
        let state = AppState::new(Config::default());
        let mut stream = state.broadcast.subscribe();

        let (status, event) = post(
            &state,
            json!({
                "role": "backend",
                "role_spent": 1_000,
                "role_cap": 1_000,
                "crew_spent": 1_500,
                "crew_budget": 5_000,
                "breach": "role",
            }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(event["kind"]["kind"], "budget");
        assert_eq!(event["from"]["id"], "backend");

        // The breach reached the live stream for the operator to see, never silent.
        let streamed = stream.try_recv().unwrap().event;
        let EventKind::Budget(reported) = streamed.kind else {
            panic!("expected a budget event");
        };
        assert_eq!(reported.role, crew_core::RoleId::new("backend"));
        assert_eq!(reported.role_spent, 1_000);
        assert_eq!(reported.breach, Some(BudgetScope::Role));

        // And it is in the durable log, filterable by kind.
        let stored = state.storage.events();
        assert!(matches!(stored.last().unwrap().kind, EventKind::Budget(_)));
    }

    #[tokio::test]
    async fn a_within_budget_report_omits_the_breach() {
        let state = AppState::new(Config::default());
        let (status, event) = post(
            &state,
            json!({ "role": "docs", "role_spent": 200, "crew_spent": 200 }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(event["kind"]["kind"], "budget");
        assert!(
            event["kind"]["data"].get("breach").is_none(),
            "a within-budget report carries no breach: {event}"
        );
    }

    #[tokio::test]
    async fn an_empty_role_is_rejected() {
        let state = AppState::new(Config::default());
        let (status, _) = post(
            &state,
            json!({ "role": " ", "role_spent": 1, "crew_spent": 1 }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }
}
