//! The budget endpoints: report a role's token spend, and read the snapshot
//! (issues #54, #176).
//!
//! The supervisor holds the crew's token [`Budget`](crew_core::Budget) and
//! records each turn's spend against it. `POST /budget` records a spend report
//! as a `budget` event on the stream (from the role, to `all-units`), so a UI
//! reads spend against budget off the stream and a cap hit is never silent.
//! When the report carries a `breach`, it marks the moment the supervisor
//! idle-stops the role or the crew rather than overrun (see
//! `docs/observability.md`). `GET /budget` reads the snapshot the broker folds
//! from those `budget` events, rebuilt from the durable log on a restart like
//! the situation board, so the cockpit (issue #51) reads current spend against
//! budget per role and crew-wide rather than replaying events (issue #176).

use axum::{extract::State, routing::post, Json, Router};
use crew_core::{
    BudgetEvent, BudgetScope, ChannelId, Event, EventKind, RoleId, Sender, Timestamp, ALL_UNITS,
};
use serde::Deserialize;

use crate::{
    error::ApiError,
    events::JsonBody,
    state::{AppState, BudgetView},
};

/// The budget routes: report a role's spend, and read the snapshot.
pub(crate) fn routes() -> Router<AppState> {
    Router::new().route("/budget", post(report).get(read))
}

/// The `POST /budget` body: a role's token spend against the crew budget.
///
/// The supervisor supplies the running totals it computed from the crew's
/// [`Budget`](crew_core::Budget); the broker stamps the timestamp and publishes
/// it.
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

/// `GET /budget`: the spend-against-budget snapshot, per role and crew-wide
/// (issue #176).
///
/// Reads the projection the broker folds from the `budget` events, rebuilt from
/// the durable log on a restart, so the cockpit (issue #51) reads current spend
/// against budget rather than replaying the stream.
async fn read(State(state): State<AppState>) -> Json<BudgetView> {
    Json(state.budget_snapshot())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
    };
    use crew_core::{BudgetScope, EventKind};
    use serde_json::{json, Value};
    use tower::ServiceExt;

    use crate::{api, config::Config, state::AppState, store::LogStore};

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

    /// The `GET /budget` snapshot as JSON.
    async fn get(state: &AppState) -> Value {
        let request = Request::builder()
            .method("GET")
            .uri("/budget")
            .body(Body::empty())
            .unwrap();
        let response = api::build(state.clone()).oneshot(request).await.unwrap();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    }

    /// The role's line in a `/budget` snapshot, if present.
    fn role<'a>(view: &'a Value, name: &str) -> Option<&'a Value> {
        view["roles"]
            .as_array()
            .unwrap()
            .iter()
            .find(|row| row["role"] == name)
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

    #[tokio::test]
    async fn the_snapshot_reports_latest_spend_per_role_and_crew_wide() {
        let state = AppState::new(Config::default());
        // Each report carries running totals, so the projection keeps the latest, not a
        // sum: backend reports twice, and its second report supersedes the first.
        post(
            &state,
            json!({ "role": "backend", "role_spent": 600, "role_cap": 2_000,
                    "crew_spent": 600, "crew_budget": 5_000 }),
        )
        .await;
        post(
            &state,
            json!({ "role": "frontend", "role_spent": 400,
                    "crew_spent": 1_000, "crew_budget": 5_000 }),
        )
        .await;
        post(
            &state,
            json!({ "role": "backend", "role_spent": 1_500, "role_cap": 2_000,
                    "crew_spent": 1_900, "crew_budget": 5_000 }),
        )
        .await;

        let view = get(&state).await;

        let backend = role(&view, "backend").expect("backend reported spend");
        assert_eq!(
            backend["spent"], 1_500,
            "the latest running total, not a sum"
        );
        assert_eq!(backend["cap"], 2_000);

        let frontend = role(&view, "frontend").expect("frontend reported spend");
        assert_eq!(frontend["spent"], 400);
        assert!(
            frontend.get("cap").is_none(),
            "an uncapped role omits its cap: {frontend}"
        );

        // Crew-wide: the latest crew total and the configured budget.
        assert_eq!(view["crew"]["spent"], 1_900);
        assert_eq!(view["crew"]["budget"], 5_000);
    }

    #[tokio::test]
    async fn an_empty_budget_snapshot_reports_no_spend() {
        let state = AppState::new(Config::default());
        let view = get(&state).await;
        assert!(
            view["roles"].as_array().unwrap().is_empty(),
            "no roles before any report: {view}"
        );
        assert_eq!(view["crew"]["spent"], 0);
        assert!(
            view["crew"].get("budget").is_none(),
            "no crew budget is known before any report: {}",
            view["crew"]
        );
    }

    #[tokio::test]
    async fn the_snapshot_survives_a_restart() {
        let dir = TempDir::new();

        // First run: report spend against a durable store, then drop the broker.
        let store = Arc::new(LogStore::open(&dir.0).unwrap());
        let state = AppState::with_storage(Config::default(), store);
        post(
            &state,
            json!({ "role": "backend", "role_spent": 1_200, "role_cap": 2_000,
                    "crew_spent": 1_200, "crew_budget": 5_000 }),
        )
        .await;
        drop(state);

        // Second run: a fresh broker over the same dir rebuilds the projection from the
        // log, like the situation board (issue #49), so the cockpit reads a snapshot.
        let reopened = Arc::new(LogStore::open(&dir.0).unwrap());
        let restarted = AppState::with_storage(Config::default(), reopened);
        let view = get(&restarted).await;
        let backend = role(&view, "backend").expect("the role's spend survived the restart");
        assert_eq!(backend["spent"], 1_200);
        assert_eq!(view["crew"]["spent"], 1_200);
        assert_eq!(view["crew"]["budget"], 5_000);
    }

    /// A unique temp dir for the durability test, removed on drop.
    struct TempDir(std::path::PathBuf);
    impl TempDir {
        fn new() -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static N: AtomicU64 = AtomicU64::new(0);
            let n = N.fetch_add(1, Ordering::Relaxed);
            Self(std::env::temp_dir().join(format!("crew-budget-test-{}-{n}", std::process::id())))
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}
