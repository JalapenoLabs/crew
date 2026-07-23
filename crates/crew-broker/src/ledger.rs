//! The work ledger: who holds which piece of work (issue #45).
//!
//! Two roles must never grab the same work. The ledger records each claimed
//! task, its owner, and its state; a role claims before it starts and moves the
//! claim to `done` when it finishes. `POST /ledger` sets a task's state,
//! refusing a claim on work another role already holds (409), so a conflict is
//! surfaced rather than raced. `GET /ledger` reads the live ownership.
//!
//! Every change also rides the event stream as a `ledger` event (from the
//! owner, to `all-units`), so the ledger is a projection of it: an observer
//! reconstructs the same ownership from the log. The live ledger lives in the
//! broker ([`AppState`]), the authority that serializes claims; rebuilding it
//! from the durable log on a broker restart is a later refinement.

use std::collections::BTreeMap;

use axum::{extract::State, routing::get, Json, Router};
use crew_core::{
    ChannelId, Event, EventKind, LedgerEvent, RoleId, Sender, TaskState, Timestamp, ALL_UNITS,
};
use serde::{Deserialize, Serialize};

use crate::{error::ApiError, events::JsonBody, state::AppState};

/// The shared work ledger: each claimed task, its owner, and its state.
///
/// The invariant, enforced by [`set`](Ledger::set): at most one role **holds**
/// a task (a task in any state but [`Done`](TaskState::Done)).
#[derive(Debug, Default)]
pub(crate) struct Ledger {
    tasks: BTreeMap<String, LedgerEntry>,
}

/// One task's record in the ledger.
#[derive(Debug, Clone)]
struct LedgerEntry {
    title: String,
    owner: RoleId,
    state: TaskState,
}

/// Who holds a task, when a claim is refused.
pub(crate) struct Conflict {
    /// The role that already holds the task.
    pub holder: RoleId,
    /// The state it holds it in.
    pub state: TaskState,
}

impl Ledger {
    /// Sets `task` to `state`, owned by `owner`, enforcing one owner per held
    /// task.
    ///
    /// A task another role currently holds is refused with the holder
    /// ([`Conflict`]); an unheld task (new, or `done`) is taken. A role
    /// setting the state of its own task always succeeds. An empty `title`
    /// keeps the task's current title.
    ///
    /// # Errors
    /// Returns a [`Conflict`] if another role holds the task.
    fn set(
        &mut self,
        task: &str,
        owner: &RoleId,
        state: TaskState,
        title: &str,
    ) -> Result<(), Conflict> {
        if let Some(entry) = self.tasks.get(task) {
            if entry.state.is_held() && &entry.owner != owner {
                return Err(Conflict {
                    holder: entry.owner.clone(),
                    state: entry.state,
                });
            }
        }
        let title = match self.tasks.get(task) {
            Some(entry) if title.is_empty() => entry.title.clone(),
            _ => title.to_owned(),
        };
        self.tasks.insert(
            task.to_owned(),
            LedgerEntry {
                title,
                owner: owner.clone(),
                state,
            },
        );
        Ok(())
    }

    /// The ledger as a wire view: every task, sorted by key.
    fn view(&self) -> LedgerView {
        LedgerView {
            tasks: self
                .tasks
                .iter()
                .map(|(task, entry)| LedgerItemView {
                    task: task.clone(),
                    title: entry.title.clone(),
                    owner: entry.owner.clone(),
                    state: entry.state,
                })
                .collect(),
        }
    }
}

/// The ledger routes: set a task's state, and read the ledger.
pub(crate) fn routes() -> Router<AppState> {
    Router::new().route("/ledger", get(list).post(claim))
}

/// The `GET /ledger` response: the current ledger.
#[derive(Debug, Serialize)]
struct LedgerView {
    /// The tasks in the ledger, sorted by key.
    tasks: Vec<LedgerItemView>,
}

/// One task in the ledger view.
#[derive(Debug, Serialize)]
struct LedgerItemView {
    /// The task's key.
    task: String,
    /// A short human title, or empty.
    title: String,
    /// The role that owns the claim.
    owner: RoleId,
    /// The task's state.
    state: TaskState,
}

/// The `POST /ledger` body: a role claiming or updating a task.
#[derive(Debug, Deserialize)]
struct ClaimRequest {
    /// The task key to claim or update.
    task: String,
    /// The role making the claim.
    owner: String,
    /// The state to move the task to (defaults to `claimed`).
    #[serde(default = "claimed")]
    state: TaskState,
    /// A short human title for the ledger view; optional.
    #[serde(default)]
    title: String,
}

/// The default state of a `POST /ledger` without one: a fresh claim.
fn claimed() -> TaskState {
    TaskState::Claimed
}

/// `GET /ledger`: read the current ledger, showing who holds which work.
async fn list(State(state): State<AppState>) -> Json<LedgerView> {
    Json(state.ledger().view())
}

/// `POST /ledger`: claim a task or move it to a new state, as `owner`.
///
/// Refuses a claim on a task another role holds, so a conflict surfaces rather
/// than silently racing; publishes a `ledger` event on success.
///
/// # Errors
/// Returns a 400 [`ApiError`] on an empty task or owner, or a 409 if another
/// role holds the task.
async fn claim(
    State(state): State<AppState>,
    JsonBody(request): JsonBody<ClaimRequest>,
) -> Result<Json<LedgerView>, ApiError> {
    let task = request.task.trim();
    if task.is_empty() {
        return Err(ApiError::bad_request("task must not be empty"));
    }
    let owner = request.owner.trim();
    if owner.is_empty() {
        return Err(ApiError::bad_request("owner must not be empty"));
    }
    let owner = RoleId::new(owner);
    let title = request.title.trim();

    // Hold the ledger lock across the set and the publish, so the stream order
    // matches the order claims are applied: a reader reconstructs the same
    // ownership.
    let mut ledger = state.ledger();
    ledger
        .set(task, &owner, request.state, title)
        .map_err(|conflict| {
            ApiError::conflict(format!(
                "task `{task}` is already held by `{}` ({})",
                conflict.holder,
                conflict.state.label()
            ))
        })?;
    state.publish(ledger_event(task, &owner, request.state, title));
    let view = ledger.view();
    drop(ledger);
    Ok(Json(view))
}

/// A ledger change as a first-class stream event, from the owner to
/// `all-units`.
fn ledger_event(task: &str, owner: &RoleId, state: TaskState, title: &str) -> Event {
    Event {
        ts: Timestamp::now(),
        from: Sender::Role(owner.clone()),
        channel: ChannelId::new(ALL_UNITS),
        task: None,
        kind: EventKind::Ledger(LedgerEvent {
            task: task.to_owned(),
            owner: owner.clone(),
            state,
            title: title.to_owned(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
    };
    use crew_core::{EventKind, TaskState};
    use serde_json::{json, Value};
    use tower::ServiceExt;

    use crate::{api, config::Config, state::AppState};

    async fn request(state: &AppState, method: &str, body: Value) -> (StatusCode, Value) {
        let builder = Request::builder().method(method).uri("/ledger");
        let request = if method == "GET" {
            builder.body(Body::empty()).unwrap()
        } else {
            builder
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap()
        };
        let response = api::build(state.clone()).oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (status, value)
    }

    fn owner_of(view: &Value, task: &str) -> Option<String> {
        view["tasks"]
            .as_array()?
            .iter()
            .find(|item| item["task"] == task)
            .and_then(|item| item["owner"].as_str().map(str::to_owned))
    }

    #[tokio::test]
    async fn a_second_role_cannot_claim_a_held_task() {
        let state = AppState::new(Config::default());
        let mut stream = state.broadcast.subscribe();

        // backend claims the login work.
        let (status, view) = request(
            &state,
            "POST",
            json!({ "task": "login", "owner": "backend", "title": "login flow" }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(owner_of(&view, "login").as_deref(), Some("backend"));

        // frontend claiming the same task is refused, and told who holds it.
        let (status, body) = request(
            &state,
            "POST",
            json!({ "task": "login", "owner": "frontend" }),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(
            body["error"].as_str().unwrap().contains("backend"),
            "the conflict names the holder: {body}"
        );

        // The ledger still shows backend as the owner.
        let (_, view) = request(&state, "GET", Value::Null).await;
        assert_eq!(owner_of(&view, "login").as_deref(), Some("backend"));

        // A single `ledger` event reached the stream (the accepted claim only).
        let event = stream.try_recv().unwrap().event;
        assert!(matches!(event.kind, EventKind::Ledger(_)));
        assert!(
            stream.try_recv().is_err(),
            "the refused claim published nothing"
        );
    }

    #[tokio::test]
    async fn the_owner_moves_its_task_through_states_and_done_frees_it() {
        let state = AppState::new(Config::default());

        request(&state, "POST", json!({ "task": "api", "owner": "backend" })).await;
        // The owner advances its own claim.
        for next in ["in_progress", "blocked", "done"] {
            let (status, _) = request(
                &state,
                "POST",
                json!({ "task": "api", "owner": "backend", "state": next }),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "the owner may set {next}");
        }

        // Now the task is done, so another role may claim it.
        let (status, view) = request(
            &state,
            "POST",
            json!({ "task": "api", "owner": "frontend" }),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "a done task is free to reclaim");
        assert_eq!(owner_of(&view, "api").as_deref(), Some("frontend"));
    }

    #[tokio::test]
    async fn a_non_owner_cannot_move_a_held_task() {
        let state = AppState::new(Config::default());
        request(&state, "POST", json!({ "task": "db", "owner": "backend" })).await;

        let (status, _) = request(
            &state,
            "POST",
            json!({ "task": "db", "owner": "frontend", "state": "done" }),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::CONFLICT,
            "only the owner may move its task, even to done"
        );
    }

    #[test]
    fn task_state_holds_until_done() {
        assert!(TaskState::Claimed.is_held());
        assert!(TaskState::InProgress.is_held());
        assert!(TaskState::Blocked.is_held());
        assert!(!TaskState::Done.is_held());
    }
}
