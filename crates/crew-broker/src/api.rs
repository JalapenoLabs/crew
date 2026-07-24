//! The HTTP surface: the axum router and its handlers.

use axum::{extract::State, http::StatusCode, routing::get, Json, Router};
use serde::Serialize;

use crate::{
    activity, board, boundary, briefing, budget, complete, control, events, gate, history, inbox,
    ledger, roster, stall, state::AppState, stats, store::Durability, usage,
};

/// Builds the broker's axum [`Router`], wired to the shared [`AppState`].
///
/// Serves `GET /health`, `POST /channels/{channel}/messages` (post a message),
/// `GET /stream` (the whole live feed),
/// `GET /inbox?role=<role>` (a role's live, self-filtered SSE stream),
/// `GET /activity?agent=<role>` (a role's live activity timeline over SSE),
/// `GET /history` (read past events, filtered and paginated, or `summary=true`
/// for the rolling-summary compaction), the `/roster` endpoints (list,
/// register, deregister), the control endpoints (`POST /pause`, `POST /resume`,
/// `POST /standdown`), `POST /complete` (report a graceful mission finish,
/// issue #121), `POST /activity` (record an agent's parsed stream-json
/// activity, issue #24), `POST /boundary` (record a lane crossing), the
/// `/ledger` endpoints (`GET /ledger`, `POST /ledger`), the done-gate endpoints
/// (`GET /gate`, `POST /gate/submit`, `POST /gate/verdict`), the
/// situation-board endpoints (`GET /board`, `POST /board`), `POST /stall`
/// (surface a coordination stall, issue #120), and `GET /briefing?role=<role>`
/// (the bounded new-role briefing packet).
pub(crate) fn build(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .merge(events::routes())
        .merge(activity::routes())
        .merge(inbox::routes())
        .merge(history::routes())
        .merge(roster::routes())
        .merge(control::routes())
        .merge(boundary::routes())
        .merge(complete::routes())
        .merge(budget::routes())
        .merge(stats::routes())
        .merge(usage::routes())
        .merge(stall::routes())
        .merge(gate::routes())
        .merge(ledger::routes())
        .merge(board::routes())
        .merge(briefing::routes())
        .with_state(state)
}

/// The health probe payload.
#[derive(Debug, Serialize)]
struct Health {
    /// `ok` when durability is intact, `degraded` when a write has failed.
    status: &'static str,
    /// The service name, `crewd`.
    service: &'static str,
    /// The running build's version.
    version: &'static str,
    /// The active storage backend, e.g. `memory`.
    storage: &'static str,
    /// Persistence health: the count of failed writes and the last error.
    durability: Durability,
}

/// `GET /health`: reports that the broker is up, which storage backend it runs,
/// and whether persistence is healthy.
///
/// Returns `200 OK` while every write reaches disk, and `503 Service
/// Unavailable` once a write has failed, so an operator (or an automated probe)
/// learns durability is degraded rather than discovering it when a restart
/// replays a short log (issue #207). The broker keeps serving either way: a
/// failed write leaves the event in memory, so only durability is degraded.
async fn health(State(state): State<AppState>) -> (StatusCode, Json<Health>) {
    let durability = state.storage.durability();
    let (code, status) = if durability.is_healthy() {
        (StatusCode::OK, "ok")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "degraded")
    };
    let health = Health {
        status,
        service: "crewd",
        version: env!("CARGO_PKG_VERSION"),
        storage: state.storage.backend(),
        durability,
    };
    (code, Json(health))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::{extract::State, http::StatusCode};
    use crew_core::{Event, RoleId, Timestamp};

    use super::health;
    use crate::{
        config::Config,
        state::AppState,
        store::{Durability, RoleStatus, Roster, Storage, StoredEvent},
    };

    /// A storage double with a fixed durability snapshot, for the health probe.
    #[derive(Debug)]
    struct FixedDurability(Durability);

    impl Storage for FixedDurability {
        fn backend(&self) -> &'static str {
            "test"
        }
        fn next_seq(&self) -> u64 {
            0
        }
        fn append(&self, _event: Event) -> u64 {
            0
        }
        fn durability(&self) -> Durability {
            self.0.clone()
        }
        fn stored_events(&self) -> Vec<StoredEvent> {
            Vec::new()
        }
        fn retain(&self, _before: Timestamp) -> usize {
            0
        }
        fn roster(&self) -> Roster {
            Roster::new()
        }
        fn register_role(&self, _role: RoleId, _status: RoleStatus) -> bool {
            false
        }
        fn deregister_role(&self, _role: &RoleId) -> Option<RoleStatus> {
            None
        }
    }

    fn state_with(durability: Durability) -> AppState {
        AppState::with_storage(Config::default(), Arc::new(FixedDurability(durability)))
    }

    #[tokio::test]
    async fn health_is_ok_while_durability_is_intact() {
        let (code, body) = health(State(state_with(Durability::default()))).await;
        assert_eq!(code, StatusCode::OK);
        assert_eq!(body.status, "ok");
        assert!(body.durability.is_healthy(), "no write has failed");
    }

    #[tokio::test]
    async fn health_is_degraded_once_a_write_has_failed() {
        let (code, body) = health(State(state_with(Durability {
            write_failures: 2,
            last_error: Some("disk full".to_owned()),
        })))
        .await;
        assert_eq!(
            code,
            StatusCode::SERVICE_UNAVAILABLE,
            "a failed write flips the probe to 503 so a monitor notices",
        );
        assert_eq!(body.status, "degraded");
        assert_eq!(body.durability.write_failures, 2);
        assert_eq!(body.durability.last_error.as_deref(), Some("disk full"));
    }
}
