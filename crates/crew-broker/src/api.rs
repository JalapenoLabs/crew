//! The HTTP surface: the axum router and its handlers.

use axum::extract::State;
use axum::routing::get;
use axum::{Json, Router};
use serde::Serialize;

use crate::state::AppState;
use crate::{boundary, control, events, history, inbox, ledger, roster};

/// Builds the broker's axum [`Router`], wired to the shared [`AppState`].
///
/// Serves `GET /health`, `POST /channels/{channel}/messages` (post a message),
/// `GET /events` (read the log), `GET /stream` (the whole live feed),
/// `GET /inbox?role=<role>` (a role's live, self-filtered SSE stream),
/// `GET /activity?agent=<role>` (a role's live activity timeline over SSE),
/// `GET /history` (read past events, filtered and paginated, or `summary=true` for the
/// rolling-summary compaction), the `/roster` endpoints (list, register, deregister),
/// the control endpoints (`POST /pause`, `POST /resume`, `POST /standdown`), `POST
/// /boundary` (record a lane crossing), and the `/ledger` endpoints (`GET /ledger`,
/// `POST /ledger`).
pub(crate) fn build(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .merge(events::routes())
        .merge(inbox::routes())
        .merge(history::routes())
        .merge(roster::routes())
        .merge(control::routes())
        .merge(boundary::routes())
        .merge(ledger::routes())
        .with_state(state)
}

/// The health probe payload.
#[derive(Debug, Serialize)]
struct Health {
    /// Always `ok` when the broker is serving.
    status: &'static str,
    /// The service name, `crewd`.
    service: &'static str,
    /// The running build's version.
    version: &'static str,
    /// The active storage backend, e.g. `memory`.
    storage: &'static str,
}

/// `GET /health`: reports that the broker is up and which storage backend it runs.
async fn health(State(state): State<AppState>) -> Json<Health> {
    Json(Health {
        status: "ok",
        service: "crewd",
        version: env!("CARGO_PKG_VERSION"),
        storage: state.storage.backend(),
    })
}
