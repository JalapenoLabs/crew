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
//! broker ([`AppState`]), the authority that serializes claims, and it is
//! rebuilt from the durable log on a broker restart ([`Ledger::rebuild`]), so
//! live ownership survives a restart rather than being lost until roles
//! re-claim (issue #185).

use std::collections::BTreeMap;

use axum::{
    extract::State,
    routing::{get, post},
    Json, Router,
};
use crew_core::{
    Channel, ChannelId, Event, EventKind, LedgerEvent, MessageKind, RoleId, Sender, TaskId,
    TaskState, Timestamp, ALL_UNITS,
};
use serde::{Deserialize, Serialize};

use crate::{error::ApiError, events::JsonBody, state::AppState};

/// The shared work ledger: each claimed task, keyed by its [`TaskId`], with its
/// owner and state (issue #183).
///
/// The invariant, enforced by [`set`](Ledger::set): at most one role **holds**
/// a task id (a task in any state but [`Done`](TaskState::Done)). Keying by the
/// id, not a human string, means order work shares its order's id while two
/// roles typing the same ad-hoc path no longer collide by accident.
#[derive(Debug, Default)]
pub(crate) struct Ledger {
    tasks: BTreeMap<TaskId, LedgerEntry>,
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

/// The outcome of a [`reassign`](Ledger::reassign): who held the task and how
/// it stands after the move.
struct Reassignment {
    /// The role that held the task before the reassignment.
    previous_owner: RoleId,
    /// The state the task keeps across the move.
    state: TaskState,
    /// The task's title, carried onto the new owner.
    title: String,
}

/// Why a [`reassign`](Ledger::reassign) cannot proceed.
enum ReassignError {
    /// The task is absent or done, so there is nothing in flight to reassign.
    NotHeld,
    /// A `from` guard did not match the task's current holder (a stale view).
    Mismatch {
        /// The role that actually holds the task.
        holder: RoleId,
    },
    /// The task is already owned by the target role, so the move is a no-op.
    AlreadyOwned {
        /// The role that already owns the task.
        owner: RoleId,
    },
}

impl Ledger {
    /// Rebuilds the ledger by folding every `ledger` event in the log, oldest
    /// first (issue #185).
    ///
    /// Each `ledger` event is a full snapshot of a task's ownership at that
    /// moment (owner, state, title), so the latest event per task wins,
    /// reconstructing the same ownership the live [`set`](Ledger::set) and
    /// [`reassign`](Ledger::reassign) writes produced. This is how the ledger
    /// survives a restart, mirroring the situation board (issue #49) and the
    /// done-gate (issue #181): a role's claim is recovered rather than silently
    /// lost until it re-claims.
    pub(crate) fn rebuild(events: &[Event]) -> Self {
        let mut ledger = Self::default();
        for event in events {
            if let EventKind::Ledger(change) = &event.kind {
                ledger.apply(change);
            }
        }
        ledger
    }

    /// Folds one `ledger` event into the projection: insert or replace the
    /// task's entry with the event's owner, state, and title.
    fn apply(&mut self, change: &LedgerEvent) {
        self.tasks.insert(
            change.task,
            LedgerEntry {
                title: change.title.clone(),
                owner: change.owner.clone(),
                state: change.state,
            },
        );
    }

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
        task: TaskId,
        owner: &RoleId,
        state: TaskState,
        title: &str,
    ) -> Result<(), Conflict> {
        if let Some(entry) = self.tasks.get(&task) {
            if entry.state.is_held() && &entry.owner != owner {
                return Err(Conflict {
                    holder: entry.owner.clone(),
                    state: entry.state,
                });
            }
        }
        let title = match self.tasks.get(&task) {
            Some(entry) if title.is_empty() => entry.title.clone(),
            _ => title.to_owned(),
        };
        self.tasks.insert(
            task,
            LedgerEntry {
                title,
                owner: owner.clone(),
                state,
            },
        );
        Ok(())
    }

    /// Whether `task` is currently held by some role: present and in any state
    /// but [`Done`](TaskState::Done).
    fn is_held(&self, task: TaskId) -> bool {
        self.tasks
            .get(&task)
            .is_some_and(|entry| entry.state.is_held())
    }

    /// Moves a held `task`'s owner to `to`, the General's authoritative
    /// override (issue #42).
    ///
    /// Unlike [`set`](Ledger::set), this **overrides** the one-owner invariant:
    /// it takes an in-flight task from its current holder. The task keeps its
    /// state and title, so the work moves in place rather than restarting.
    /// Returns the [`Reassignment`] (the previous owner and the preserved state
    /// and title), so the caller can publish the change and notify the parties.
    ///
    /// `from`, when given, is a guard against a stale view: the move is refused
    /// unless that role is the task's current holder.
    ///
    /// # Errors
    /// Returns a [`ReassignError`] if `task` is not held (absent or done), if
    /// `from` is given but is not the current holder, or if `to` already owns
    /// it (nothing to move).
    fn reassign(
        &mut self,
        task: TaskId,
        to: &RoleId,
        from: Option<&RoleId>,
    ) -> Result<Reassignment, ReassignError> {
        // Read the current holder and validate the move before mutating, so a
        // refused reassignment leaves the ledger untouched.
        let (previous_owner, state, title) = {
            let entry = self.tasks.get(&task).ok_or(ReassignError::NotHeld)?;
            if !entry.state.is_held() {
                return Err(ReassignError::NotHeld);
            }
            if let Some(from) = from {
                if &entry.owner != from {
                    return Err(ReassignError::Mismatch {
                        holder: entry.owner.clone(),
                    });
                }
            }
            if &entry.owner == to {
                return Err(ReassignError::AlreadyOwned {
                    owner: entry.owner.clone(),
                });
            }
            (entry.owner.clone(), entry.state, entry.title.clone())
        };
        self.tasks.insert(
            task,
            LedgerEntry {
                title: title.clone(),
                owner: to.clone(),
                state,
            },
        );
        Ok(Reassignment {
            previous_owner,
            state,
            title,
        })
    }

    /// The ledger as a wire view: every task, sorted by key.
    fn view(&self) -> LedgerView {
        LedgerView {
            tasks: self
                .tasks
                .iter()
                .map(|(task, entry)| LedgerItemView {
                    task: *task,
                    title: entry.title.clone(),
                    owner: entry.owner.clone(),
                    state: entry.state,
                })
                .collect(),
        }
    }
}

/// The ledger routes: set a task's state, read the ledger, and reassign a held
/// task.
pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route("/ledger", get(list).post(claim))
        .route("/ledger/reassign", post(reassign))
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
    /// The task's key: its [`TaskId`], serialized as a UUID string (issue
    /// #183).
    task: TaskId,
    /// A short human title, or empty (display only).
    title: String,
    /// The role that owns the claim.
    owner: RoleId,
    /// The task's state.
    state: TaskState,
}

/// The `POST /ledger` body: a role claiming or updating a task.
#[derive(Debug, Deserialize)]
struct ClaimRequest {
    /// The task id to claim or update, deserialized from its UUID string (issue
    /// #183).
    task: TaskId,
    /// The role making the claim.
    owner: String,
    /// The state to move the task to (defaults to `claimed`).
    #[serde(default = "claimed")]
    state: TaskState,
    /// A short human title for the ledger view; display only, optional.
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
    let task = request.task;
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

/// The `POST /ledger/reassign` body: the General moving a task to a new owner.
#[derive(Debug, Deserialize)]
struct ReassignRequest {
    /// The task id to reassign, deserialized from its UUID string (issue #183).
    task: TaskId,
    /// The role to move the task to.
    to: String,
    /// The role the task is expected to be held by, a guard against a stale
    /// view; optional.
    #[serde(default)]
    from: Option<String>,
}

/// The `POST /ledger/reassign` response: the move that happened.
#[derive(Debug, Serialize)]
struct ReassignView {
    /// The task reassigned: its [`TaskId`], serialized as a UUID string (issue
    /// #183).
    task: TaskId,
    /// The role that held it before the move.
    from: RoleId,
    /// The role that owns it now.
    to: RoleId,
    /// The state the task kept across the move.
    state: TaskState,
    /// The task's title.
    title: String,
}

/// `POST /ledger/reassign`: move a held task to a new owner, the General's
/// authoritative override (issue #42).
///
/// Unlike a claim, this overrides the one-owner invariant to take work from its
/// current holder, and preserves the task's state and title so the work moves
/// in place. It publishes a `ledger` event with the new owner, so the change
/// rides the stream and rebuilds the same ownership as any claim. The response
/// names the previous owner, so the caller (`crew reassign`) can notify both
/// roles.
///
/// # Errors
/// Returns a 400 [`ApiError`] on an empty `task` or `to`, or a 409 if the task
/// is not held, is held by a role other than `from`, or is already owned by
/// `to`.
async fn reassign(
    State(state): State<AppState>,
    JsonBody(request): JsonBody<ReassignRequest>,
) -> Result<Json<ReassignView>, ApiError> {
    let task = request.task;
    let to = request.to.trim();
    if to.is_empty() {
        return Err(ApiError::bad_request(
            "the new owner (`to`) must not be empty",
        ));
    }
    let to = RoleId::new(to);
    let from = request
        .from
        .as_deref()
        .map(str::trim)
        .filter(|role| !role.is_empty())
        .map(RoleId::new);

    // Hold the ledger lock across the move and the publish, so the stream order
    // matches the order changes are applied (as the claim path does).
    let mut ledger = state.ledger();
    let reassignment = ledger
        .reassign(task, &to, from.as_ref())
        .map_err(|err| reassign_conflict(task, &err))?;
    state.publish(ledger_event(
        task,
        &to,
        reassignment.state,
        &reassignment.title,
    ));
    let view = ReassignView {
        task,
        from: reassignment.previous_owner,
        to,
        state: reassignment.state,
        title: reassignment.title,
    };
    drop(ledger);
    Ok(Json(view))
}

/// Renders a [`ReassignError`] as a 409 [`ApiError`] with a precise reason.
fn reassign_conflict(task: TaskId, error: &ReassignError) -> ApiError {
    let reason = match error {
        ReassignError::NotHeld => {
            format!("task `{task}` is not held by anyone; there is nothing in flight to reassign")
        }
        ReassignError::Mismatch { holder } => {
            format!("task `{task}` is held by `{holder}`, not the role you named")
        }
        ReassignError::AlreadyOwned { owner } => {
            format!("task `{task}` is already owned by `{owner}`")
        }
    };
    ApiError::conflict(reason)
}

/// Seeds a ledger claim for an order's recipient, so assigned-but-not-started
/// work appears on the ledger without a manual claim (issue #184).
///
/// An order (`crew_order`, and the General's `crew command`) assigns scoped
/// work to one role on its direct channel; this claims that work for the
/// recipient, keyed by the order's envelope [`TaskId`] and titled by the
/// order's title, so `crew_ledger` shows it `claimed` before the role starts
/// (issue #183). Keying by the order's id, not its title, is what threads a
/// claim -> `in_progress` -> done chain and the done-gate to one shared id. The
/// message-publish path calls it for every message; it acts only on a direct
/// order that carries a task.
///
/// It is non-destructive and best-effort: it seeds only when no one currently
/// holds the task id, so a re-order never regresses an in-progress claim and
/// never steals another role's held work (the General uses `crew reassign` for
/// that). A fresh claim publishes a `ledger` event, so the seed rides the
/// stream like any claim.
pub(crate) fn seed_order_claim(state: &AppState, event: &Event) {
    // Only an order carries a title to display, and only a direct channel names the
    // one recipient to own it. The order's envelope task is the key (issue #183).
    let EventKind::Message(message) = &event.kind else {
        return;
    };
    let MessageKind::Order { title, .. } = &message.kind else {
        return;
    };
    let Some(Channel::Direct(owner)) = Channel::parse(event.channel.as_str()) else {
        return;
    };
    let Some(task) = event.task else {
        return;
    };
    let title = title.trim();

    // Hold the ledger lock across the check, the claim, and the publish, so the
    // seed is serialized with any concurrent claim and cannot regress or
    // clobber one (mirroring the `claim` handler).
    let mut ledger = state.ledger();
    if ledger.is_held(task) {
        return;
    }
    // The task is unheld, so the set cannot conflict; publish the seeded claim.
    if ledger.set(task, &owner, TaskState::Claimed, title).is_ok() {
        state.publish(ledger_event(task, &owner, TaskState::Claimed, title));
    }
}

/// A ledger change as a first-class stream event, from the owner to
/// `all-units`, stamping the [`TaskId`] on the envelope so a `?task=<id>`
/// history read returns it (issue #183).
fn ledger_event(task: TaskId, owner: &RoleId, state: TaskState, title: &str) -> Event {
    Event {
        ts: Timestamp::now(),
        from: Sender::Role(owner.clone()),
        channel: ChannelId::new(ALL_UNITS),
        task: Some(task),
        kind: EventKind::Ledger(LedgerEvent {
            task,
            owner: owner.clone(),
            state,
            title: title.to_owned(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
    };
    use crew_core::{EventKind, TaskId, TaskState};
    use serde_json::{json, Value};
    use tower::ServiceExt;

    use crate::{api, config::Config, state::AppState, store::LogStore};

    /// A fresh task id as its UUID string, the key every claim now uses (issue
    /// #183).
    fn tid() -> String {
        TaskId::new().to_string()
    }

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

    /// Posts a reassignment to `/ledger/reassign`, returning the status and
    /// body.
    async fn reassign_request(state: &AppState, body: Value) -> (StatusCode, Value) {
        let request = Request::builder()
            .method("POST")
            .uri("/ledger/reassign")
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
    async fn a_second_role_cannot_claim_a_held_task() {
        let state = AppState::new(Config::default());
        let mut stream = state.broadcast.subscribe();
        let login = tid();

        // backend claims the login work.
        let (status, view) = request(
            &state,
            "POST",
            json!({ "task": login, "owner": "backend", "title": "login flow" }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(owner_of(&view, &login).as_deref(), Some("backend"));

        // frontend claiming the same task is refused, and told who holds it.
        let (status, body) = request(
            &state,
            "POST",
            json!({ "task": login, "owner": "frontend" }),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(
            body["error"].as_str().unwrap().contains("backend"),
            "the conflict names the holder: {body}"
        );

        // The ledger still shows backend as the owner.
        let (_, view) = request(&state, "GET", Value::Null).await;
        assert_eq!(owner_of(&view, &login).as_deref(), Some("backend"));

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
        // A claim -> `in_progress` -> done chain on ONE task id updates one entry
        // (issue #183): the owner advances its own claim without minting new keys.
        let state = AppState::new(Config::default());
        let api = tid();

        request(&state, "POST", json!({ "task": api, "owner": "backend" })).await;
        // The owner advances its own claim.
        for next in ["in_progress", "blocked", "done"] {
            let (status, view) = request(
                &state,
                "POST",
                json!({ "task": api, "owner": "backend", "state": next }),
            )
            .await;
            assert_eq!(status, StatusCode::OK, "the owner may set {next}");
            assert_eq!(
                view["tasks"].as_array().unwrap().len(),
                1,
                "the chain updates one entry, not many: {view}",
            );
        }

        // Now the task is done, so another role may claim it.
        let (status, view) =
            request(&state, "POST", json!({ "task": api, "owner": "frontend" })).await;
        assert_eq!(status, StatusCode::OK, "a done task is free to reclaim");
        assert_eq!(owner_of(&view, &api).as_deref(), Some("frontend"));
    }

    #[tokio::test]
    async fn a_non_owner_cannot_move_a_held_task() {
        let state = AppState::new(Config::default());
        let db = tid();
        request(&state, "POST", json!({ "task": db, "owner": "backend" })).await;

        let (status, _) = request(
            &state,
            "POST",
            json!({ "task": db, "owner": "frontend", "state": "done" }),
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

    #[tokio::test]
    async fn a_reassignment_moves_a_held_task_to_a_new_owner() {
        let state = AppState::new(Config::default());
        let login = tid();

        // backend claims and starts login.
        request(
            &state,
            "POST",
            json!({ "task": login, "owner": "backend", "state": "in_progress", "title": "login flow" }),
        )
        .await;

        // The General reassigns the in-flight task to frontend.
        let mut stream = state.broadcast.subscribe();
        let (status, view) =
            reassign_request(&state, json!({ "task": login, "to": "frontend" })).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            view["from"], "backend",
            "the response names the previous owner"
        );
        assert_eq!(view["to"], "frontend");
        assert_eq!(view["task"], login, "the response echoes the task id");
        assert_eq!(
            view["state"], "in_progress",
            "the task keeps its state across the move"
        );
        assert_eq!(view["title"], "login flow", "the title carries over");

        // The ledger now shows frontend as the owner, still in_progress.
        let (_, ledger) = request(&state, "GET", Value::Null).await;
        assert_eq!(owner_of(&ledger, &login).as_deref(), Some("frontend"));

        // A `ledger` event rode the stream with the new owner and preserved state, so a
        // consumer reconstructs the same ownership.
        let event = stream.try_recv().unwrap().event;
        let EventKind::Ledger(moved) = event.kind else {
            panic!(
                "the reassignment publishes a ledger event, got {:?}",
                event.kind
            );
        };
        assert_eq!(moved.owner.as_str(), "frontend");
        assert_eq!(moved.state, TaskState::InProgress);
    }

    #[tokio::test]
    async fn reassigning_an_unheld_task_is_refused() {
        let state = AppState::new(Config::default());

        // An absent task has nothing in flight to move.
        let (status, body) =
            reassign_request(&state, json!({ "task": tid(), "to": "frontend" })).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .contains("nothing in flight"),
            "refusal names the reason: {body}"
        );

        // A done task is likewise not in flight, so it is not reassignable.
        let api = tid();
        request(&state, "POST", json!({ "task": api, "owner": "backend" })).await;
        request(
            &state,
            "POST",
            json!({ "task": api, "owner": "backend", "state": "done" }),
        )
        .await;
        let (status, _) = reassign_request(&state, json!({ "task": api, "to": "frontend" })).await;
        assert_eq!(
            status,
            StatusCode::CONFLICT,
            "a done task has nothing in flight to reassign"
        );
    }

    #[tokio::test]
    async fn a_from_guard_that_does_not_match_the_holder_is_refused() {
        let state = AppState::new(Config::default());
        let db = tid();
        request(&state, "POST", json!({ "task": db, "owner": "backend" })).await;

        // The General's view is stale: it thinks qa holds `db`, but backend does.
        let (status, body) = reassign_request(
            &state,
            json!({ "task": db, "to": "frontend", "from": "qa" }),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(
            body["error"].as_str().unwrap().contains("backend"),
            "the refusal names the real holder: {body}"
        );

        // The move is refused, so the ledger is untouched.
        let (_, ledger) = request(&state, "GET", Value::Null).await;
        assert_eq!(owner_of(&ledger, &db).as_deref(), Some("backend"));
    }

    #[tokio::test]
    async fn reassigning_to_the_current_owner_is_refused_as_a_no_op() {
        let state = AppState::new(Config::default());
        let auth = tid();
        request(&state, "POST", json!({ "task": auth, "owner": "backend" })).await;

        let (status, body) =
            reassign_request(&state, json!({ "task": auth, "to": "backend" })).await;
        assert_eq!(status, StatusCode::CONFLICT);
        assert!(
            body["error"]
                .as_str()
                .unwrap()
                .contains("already owned by `backend`"),
            "the refusal explains it is already owned: {body}"
        );
    }

    /// A unique temp dir for the durability test, removed on drop.
    struct TempDir(std::path::PathBuf);
    impl TempDir {
        fn new() -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static N: AtomicU64 = AtomicU64::new(0);
            let n = N.fetch_add(1, Ordering::Relaxed);
            Self(std::env::temp_dir().join(format!("crew-ledger-test-{}-{n}", std::process::id())))
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[tokio::test]
    async fn a_claim_survives_a_restart() {
        let dir = TempDir::new();
        let login = tid();

        // First run: backend claims and starts `login` against a durable store, then
        // drop the broker with the work still in flight.
        let store = Arc::new(LogStore::open(&dir.0).unwrap());
        let state = AppState::with_storage(Config::default(), store);
        request(
            &state,
            "POST",
            json!({ "task": login, "owner": "backend", "state": "in_progress",
                    "title": "login flow" }),
        )
        .await;
        drop(state);

        // Second run: a fresh broker over the same dir rebuilds the ledger from the log
        // (issue #185), like the board (issue #49) and the done-gate (issue #181).
        let reopened = Arc::new(LogStore::open(&dir.0).unwrap());
        let restarted = AppState::with_storage(Config::default(), reopened);

        // The claim survived: backend still holds `login`, in progress and titled.
        let (_, view) = request(&restarted, "GET", Value::Null).await;
        assert_eq!(owner_of(&view, &login).as_deref(), Some("backend"));
        let entry = entry_of(&view, &login).unwrap();
        assert_eq!(entry["state"], "in_progress");
        assert_eq!(entry["title"], "login flow");

        // The ledger still enforces on the rebuilt claim: a second role cannot take it.
        let (status, body) = request(
            &restarted,
            "POST",
            json!({ "task": login, "owner": "frontend" }),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::CONFLICT,
            "the rebuilt claim still blocks a second role"
        );
        assert!(
            body["error"].as_str().unwrap().contains("backend"),
            "the conflict names the restored holder: {body}"
        );
    }

    #[tokio::test]
    async fn a_done_task_rebuilds_as_free_to_reclaim() {
        let dir = TempDir::new();
        let api = tid();

        // backend claims `api` and finishes it, freeing the claim.
        let store = Arc::new(LogStore::open(&dir.0).unwrap());
        let state = AppState::with_storage(Config::default(), store);
        request(&state, "POST", json!({ "task": api, "owner": "backend" })).await;
        request(
            &state,
            "POST",
            json!({ "task": api, "owner": "backend", "state": "done" }),
        )
        .await;
        drop(state);

        // After a restart the task rebuilds `done`, so it is free for another role to
        // claim rather than wrongly locked to backend.
        let reopened = Arc::new(LogStore::open(&dir.0).unwrap());
        let restarted = AppState::with_storage(Config::default(), reopened);
        let (status, view) = request(
            &restarted,
            "POST",
            json!({ "task": api, "owner": "frontend" }),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::OK,
            "a rebuilt done task is free to reclaim"
        );
        assert_eq!(owner_of(&view, &api).as_deref(), Some("frontend"));
    }

    /// The entry keyed by `task` in a ledger view, if present.
    fn entry_of<'a>(view: &'a Value, task: &str) -> Option<&'a Value> {
        view["tasks"]
            .as_array()?
            .iter()
            .find(|item| item["task"] == task)
    }

    /// Posts an order from `from` to `channel`, carrying `task` on the envelope
    /// and `title` as its display, driving the ledger auto-seed (issues #184,
    /// #183).
    async fn post_order_to(
        state: &AppState,
        from: &str,
        channel: &str,
        task: &str,
        title: &str,
    ) -> StatusCode {
        let body = json!({
            "from": { "kind": "role", "id": from },
            "kind": "order",
            "task": task,
            "title": title,
            "scope": "",
            "owned_paths": [],
            "acceptance": "",
            "body": "",
        });
        let request = Request::builder()
            .method("POST")
            .uri(format!("/channels/{channel}/messages"))
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .unwrap();
        api::build(state.clone())
            .oneshot(request)
            .await
            .unwrap()
            .status()
    }

    /// Posts an order from `from` to `role`'s direct channel, carrying `task`
    /// and `title`.
    async fn post_order(
        state: &AppState,
        from: &str,
        role: &str,
        task: &str,
        title: &str,
    ) -> StatusCode {
        post_order_to(state, from, &format!("@{role}"), task, title).await
    }

    #[tokio::test]
    async fn an_order_seeds_a_claim_keyed_by_the_orders_task_id() {
        let state = AppState::new(Config::default());
        let task = tid();

        // The commander orders backend, with no manual claim following (issue #184).
        // The seeded claim is keyed by the ORDER's envelope task id, titled by the
        // order title (issue #183).
        assert_eq!(
            post_order(&state, "commander", "backend", &task, "login flow").await,
            StatusCode::CREATED,
        );

        // The ledger already shows backend holding the work, keyed by the id and
        // titled by the order.
        let (_, view) = request(&state, "GET", Value::Null).await;
        let entry = entry_of(&view, &task).expect("the order seeded a ledger claim");
        assert_eq!(entry["owner"], "backend");
        assert_eq!(entry["state"], "claimed");
        assert_eq!(entry["title"], "login flow");
    }

    #[tokio::test]
    async fn a_seeded_claim_rides_the_stream_with_the_task_on_the_envelope() {
        let state = AppState::new(Config::default());
        let mut stream = state.broadcast.subscribe();
        let task = tid();
        let task_id: TaskId = task.parse().unwrap();

        post_order(&state, "commander", "backend", &task, "login flow").await;

        // The order message, then the seeded ledger claim.
        let order = stream.try_recv().unwrap().event;
        assert!(matches!(order.kind, EventKind::Message(_)));
        let seeded = stream.try_recv().unwrap().event;
        // The envelope carries the domain id, so a `?task=<id>` history read returns
        // it (issue #183).
        assert_eq!(
            seeded.task,
            Some(task_id),
            "the seeded claim stamps the task id on the envelope"
        );
        let EventKind::Ledger(claim) = seeded.kind else {
            panic!("expected a seeded ledger event, got {:?}", seeded.kind);
        };
        assert_eq!(claim.owner.as_str(), "backend");
        assert_eq!(claim.state, TaskState::Claimed);
        assert_eq!(claim.task, task_id, "the domain task is the order's id");
        assert_eq!(claim.title, "login flow", "titled by the order");
    }

    #[tokio::test]
    async fn a_re_order_does_not_regress_an_in_progress_claim() {
        let state = AppState::new(Config::default());
        let task = tid();

        // The recipient has already started the work.
        request(
            &state,
            "POST",
            json!({ "task": task, "owner": "backend", "state": "in_progress",
                    "title": "login flow" }),
        )
        .await;

        // Re-ordering the same task id must not reset the in-flight claim to `claimed`.
        post_order(&state, "commander", "backend", &task, "login flow").await;
        let (_, view) = request(&state, "GET", Value::Null).await;
        let entry = entry_of(&view, &task).unwrap();
        assert_eq!(
            entry["state"], "in_progress",
            "a re-order leaves an in-flight claim untouched"
        );
        assert_eq!(entry["owner"], "backend");
    }

    #[tokio::test]
    async fn an_order_does_not_steal_a_task_another_role_holds() {
        let state = AppState::new(Config::default());
        let task = tid();

        // qa already holds the work.
        request(
            &state,
            "POST",
            json!({ "task": task, "owner": "qa", "state": "in_progress" }),
        )
        .await;

        // Ordering the same task id to backend must not take it from qa; that is a
        // reassignment the General does explicitly.
        post_order(&state, "commander", "backend", &task, "login flow").await;
        let (_, view) = request(&state, "GET", Value::Null).await;
        assert_eq!(
            owner_of(&view, &task).as_deref(),
            Some("qa"),
            "the seed never steals a task another role holds"
        );
    }

    #[tokio::test]
    async fn an_order_broadcast_to_all_units_seeds_no_claim() {
        let state = AppState::new(Config::default());

        // A broadcast order names no single recipient to own the work.
        post_order_to(&state, "commander", "all-units", &tid(), "all hands").await;
        let (_, view) = request(&state, "GET", Value::Null).await;
        assert!(
            view["tasks"].as_array().unwrap().is_empty(),
            "a broadcast order seeds no claim: {view}"
        );
    }
}
