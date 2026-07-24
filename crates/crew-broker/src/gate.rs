//! The adversarial done-gate: submit work for verification, and record the
//! verdict (issue #47).
//!
//! "Done" is verified, not asserted. A role does not report its own task done;
//! when it believes the work meets the acceptance criteria it submits it here
//! (`POST /gate/submit`), and an independent role then tries to break it
//! against those criteria and records a verdict (`POST /gate/verdict`). The
//! gate refuses a verdict from the task's own owner and one on a task that is
//! not awaiting verification, so a task reaches [`Passed`](Verdict::Passed)
//! only when a role other than the owner could not break it. A
//! [`Failed`](Verdict::Failed) verdict returns the work to the owner with the
//! specific failure, as an actionable handback in its inbox.
//!
//! Every step is a first-class `verification` event on the stream (to
//! `all-units`), so the operator sees the gate in action and `GET /gate` reads
//! live ownership (see `docs/observability.md`). The gate state lives in the
//! broker ([`AppState`]).

use std::fmt::Write as _;

use axum::{
    extract::State,
    routing::{get, post},
    Json, Router,
};
use crew_core::{
    Channel, ChannelId, Event, EventKind, Message, MessageId, MessageKind, RoleId, Sender,
    Timestamp, Verdict, VerificationEvent, ALL_UNITS,
};
use serde::{Deserialize, Serialize};

use crate::{
    error::ApiError,
    events::JsonBody,
    state::{AppState, VerdictError, VerdictOutcome},
};

/// The done-gate routes: read the gate, submit work, and record a verdict.
pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route("/gate", get(read))
        .route("/gate/submit", post(submit))
        .route("/gate/verdict", post(verdict))
}

/// `GET /gate`: the live done-gate, every task under verification and its
/// standing.
async fn read(State(state): State<AppState>) -> Json<GateView> {
    Json(GateView::from_state(&state))
}

/// The `POST /gate/submit` body: work an owner submits for verification.
#[derive(Debug, Deserialize)]
struct Submission {
    /// The role submitting the work, which owns any rework.
    role: String,
    /// The task, named by its title (the order's title).
    task: String,
    /// The acceptance criteria the work claims to meet; the verifier tries to
    /// break it against these.
    #[serde(default)]
    acceptance: String,
    /// An optional reviewer to notify, so the request reaches its inbox; omit
    /// for an open call the crew picks up off the stream.
    #[serde(default)]
    to: Option<String>,
}

/// `POST /gate/submit`: submit work for adversarial verification.
///
/// Records the task as awaiting an independent verifier and announces it on the
/// stream. This does not mark the work done: only an independent
/// [`Passed`](Verdict::Passed) verdict does.
///
/// # Errors
/// Returns a 400 [`ApiError`] if the role or task is empty.
async fn submit(
    State(state): State<AppState>,
    JsonBody(request): JsonBody<Submission>,
) -> Result<Json<GateView>, ApiError> {
    let owner = RoleId::new(non_empty(&request.role, "role")?);
    let task = non_empty(&request.task, "task")?.to_owned();
    let acceptance = request.acceptance.trim().to_owned();

    state.submit_for_verification(task.clone(), owner.clone(), acceptance.clone());
    state.publish(verification_event(
        owner.clone(),
        VerificationEvent {
            task: task.clone(),
            owner: owner.clone(),
            verifier: None,
            verdict: Verdict::Submitted,
            detail: acceptance.clone(),
        },
    ));

    // A named reviewer is notified in its inbox; otherwise the submission is an
    // open call the commander or a verifier picks up off the stream.
    if let Some(reviewer) = request
        .to
        .as_deref()
        .map(str::trim)
        .filter(|to| !to.is_empty())
    {
        if let Some(Channel::Direct(reviewer)) = Channel::resolve(Some(reviewer), None, &owner) {
            state.publish(review_request(&owner, &reviewer, &task, &acceptance));
        }
    }

    Ok(Json(GateView::from_state(&state)))
}

/// The `POST /gate/verdict` body: an independent verifier's judgment on a task.
#[derive(Debug, Deserialize)]
struct VerdictReport {
    /// The role returning the verdict; it must not be the task's owner.
    role: String,
    /// The task under verification, by title.
    task: String,
    /// Whether the verifier could not break it (`true`: done) or broke it
    /// (`false`).
    pass: bool,
    /// The specific failure on a failed verdict; required when `pass` is false.
    #[serde(default)]
    failure: String,
}

/// `POST /gate/verdict`: record an independent verifier's verdict on a
/// submitted task.
///
/// A pass marks the task done; a failure returns it to the owner with the
/// specific failure as a handback in its inbox. Every verdict is announced on
/// the stream.
///
/// # Errors
/// Returns a 400 [`ApiError`] if a field is empty or a failing verdict carries
/// no failure, a 404 if the task was never submitted, or a 409 if the verifier
/// is the owner or the task is not awaiting a verdict.
async fn verdict(
    State(state): State<AppState>,
    JsonBody(request): JsonBody<VerdictReport>,
) -> Result<Json<GateView>, ApiError> {
    let verifier = RoleId::new(non_empty(&request.role, "role")?);
    let task = non_empty(&request.task, "task")?.to_owned();
    let failure = request.failure.trim().to_owned();
    if !request.pass && failure.is_empty() {
        return Err(ApiError::bad_request(
            "a failed verdict needs a `failure` describing what broke, so the handback is actionable",
        ));
    }

    let outcome = state
        .record_verdict(&task, verifier, request.pass, failure)
        .map_err(map_verdict_error)?;

    state.publish(verification_event(
        outcome.verifier.clone(),
        VerificationEvent {
            task: task.clone(),
            owner: outcome.owner.clone(),
            verifier: Some(outcome.verifier.clone()),
            verdict: outcome.verdict,
            detail: outcome.detail.clone(),
        },
    ));

    // A failure returns the work to the owner with the specific failure, in its
    // inbox.
    if outcome.verdict == Verdict::Failed {
        state.publish(handback_note(&outcome, &task));
    }

    Ok(Json(GateView::from_state(&state)))
}

/// A verification step as a first-class stream event, `from` a role, to
/// `all-units`.
fn verification_event(from: RoleId, event: VerificationEvent) -> Event {
    Event {
        ts: Timestamp::now(),
        from: Sender::Role(from),
        channel: ChannelId::new(ALL_UNITS),
        task: None,
        kind: EventKind::Verification(event),
    }
}

/// The review-request note a submission sends to a named reviewer's inbox.
fn review_request(owner: &RoleId, reviewer: &RoleId, task: &str, acceptance: &str) -> Event {
    let mut body = format!("Please verify `{task}` before it is done.");
    if !acceptance.is_empty() {
        let _ = write!(body, " Acceptance: {acceptance}.");
    }
    body.push_str(
        " Try to break it against the acceptance, then record a verdict with crew_verdict \
         (pass, or the specific failure).",
    );
    note(owner.clone(), reviewer.clone(), body)
}

/// The actionable handback a failed verdict returns to the owner's inbox.
fn handback_note(outcome: &VerdictOutcome, task: &str) -> Event {
    let body = format!(
        "Verification failed for `{task}`: {failure}. Returned for rework; fix it and \
         resubmit with crew_submit.",
        failure = outcome.detail,
    );
    note(outcome.verifier.clone(), outcome.owner.clone(), body)
}

/// A note `from` one role directly to `to`, so it lands in that role's inbox.
fn note(from: RoleId, to: RoleId, body: String) -> Event {
    Event {
        ts: Timestamp::now(),
        from: Sender::Role(from),
        channel: Channel::Direct(to).name(),
        task: None,
        kind: EventKind::Message(Message {
            id: MessageId::new(),
            kind: MessageKind::Note,
            body,
        }),
    }
}

/// Maps a refused verdict to the HTTP error a client reads.
fn map_verdict_error(error: VerdictError) -> ApiError {
    match error {
        VerdictError::UnknownTask => {
            ApiError::not_found("no such task is awaiting verification; submit it first")
        }
        VerdictError::SelfVerification => ApiError::conflict(
            "a role cannot verify its own work; an independent role must try to break it",
        ),
        VerdictError::NotAwaitingVerification(Verdict::Passed) => {
            ApiError::conflict("that task is already verified done")
        }
        VerdictError::NotAwaitingVerification(_) => ApiError::conflict(
            "that task is not awaiting a verdict; it was handed back and must be resubmitted",
        ),
    }
}

/// Trims a required field, erroring if it is blank.
fn non_empty<'a>(value: &'a str, field: &str) -> Result<&'a str, ApiError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ApiError::bad_request(format!("{field} must not be empty")));
    }
    Ok(trimmed)
}

/// The `GET /gate` response: every task under verification and its standing.
#[derive(Debug, Serialize)]
struct GateView {
    /// The tasks under the gate, ordered by title.
    tasks: Vec<TaskView>,
}

/// One task's standing in the done-gate.
#[derive(Debug, Serialize)]
struct TaskView {
    /// The task title.
    task: String,
    /// The role that submitted it and owns any rework.
    owner: RoleId,
    /// The independent role that returned the latest verdict, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    verifier: Option<RoleId>,
    /// Where the task stands: submitted, passed, or failed.
    verdict: Verdict,
    /// The acceptance being claimed, or the failure on a handback.
    #[serde(skip_serializing_if = "String::is_empty")]
    detail: String,
}

impl GateView {
    /// Builds the view from the broker's live gate snapshot.
    fn from_state(state: &AppState) -> Self {
        let tasks = state
            .gate_snapshot()
            .into_iter()
            .map(|(task, entry)| TaskView {
                task,
                owner: entry.owner,
                verifier: entry.verifier,
                verdict: entry.verdict,
                detail: entry.detail,
            })
            .collect();
        Self { tasks }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
    };
    use crew_core::{Channel, EventKind, RoleId};
    use serde_json::{json, Value};
    use tower::ServiceExt;

    use crate::{api, config::Config, state::AppState, store::LogStore};

    async fn post(state: &AppState, path: &str, body: Value) -> (StatusCode, Value) {
        send(state, "POST", path, Some(body)).await
    }

    async fn get(state: &AppState, path: &str) -> Value {
        send(state, "GET", path, None).await.1
    }

    async fn send(
        state: &AppState,
        method: &str,
        path: &str,
        body: Option<Value>,
    ) -> (StatusCode, Value) {
        let request = Request::builder()
            .method(method)
            .uri(path)
            .header("content-type", "application/json")
            .body(body.map_or_else(Body::empty, |value| Body::from(value.to_string())))
            .unwrap();
        let response = api::build(state.clone()).oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(Value::Null),
        )
    }

    /// The one task in a gate view, by title.
    fn task_in<'a>(view: &'a Value, title: &str) -> &'a Value {
        view["tasks"]
            .as_array()
            .unwrap()
            .iter()
            .find(|t| t["task"] == title)
            .unwrap_or_else(|| panic!("task `{title}` not in the gate view"))
    }

    #[tokio::test]
    async fn a_task_is_done_only_after_an_independent_pass() {
        let state = AppState::new(Config::default());

        // The owner submits; the task is awaiting a verdict, not done.
        let (status, view) = post(
            &state,
            "/gate/submit",
            json!({ "role": "backend", "task": "login", "acceptance": "tokens expire" }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(task_in(&view, "login")["verdict"], "submitted");
        assert_eq!(task_in(&view, "login")["owner"], "backend");

        // An independent verifier passes it: now it is done.
        let (status, view) = post(
            &state,
            "/gate/verdict",
            json!({ "role": "qa", "task": "login", "pass": true }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(task_in(&view, "login")["verdict"], "passed");
        assert_eq!(task_in(&view, "login")["verifier"], "qa");

        // GET /gate reads the same live ownership.
        let read = get(&state, "/gate").await;
        assert_eq!(task_in(&read, "login")["verdict"], "passed");
    }

    #[tokio::test]
    async fn a_role_cannot_verify_its_own_work() {
        let state = AppState::new(Config::default());
        post(
            &state,
            "/gate/submit",
            json!({ "role": "backend", "task": "login", "acceptance": "a" }),
        )
        .await;

        let (status, body) = post(
            &state,
            "/gate/verdict",
            json!({ "role": "backend", "task": "login", "pass": true }),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "self-verification is refused");
        assert!(body["error"].as_str().unwrap().contains("own work"));

        // The task did not slip through to done.
        let read = get(&state, "/gate").await;
        assert_eq!(task_in(&read, "login")["verdict"], "submitted");
    }

    #[tokio::test]
    async fn a_failed_verdict_hands_the_work_back_to_the_owner() {
        let state = AppState::new(Config::default());
        let mut stream = state.broadcast.subscribe();
        post(
            &state,
            "/gate/submit",
            json!({ "role": "backend", "task": "login", "acceptance": "tokens expire" }),
        )
        .await;
        // Drain the submission event.
        let _ = stream.try_recv();

        let (status, view) = post(
            &state,
            "/gate/verdict",
            json!({ "role": "qa", "task": "login", "pass": false, "failure": "tokens never expire" }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(task_in(&view, "login")["verdict"], "failed");
        assert_eq!(task_in(&view, "login")["detail"], "tokens never expire");

        // The verdict rides the stream, then a handback note reaches the owner's inbox.
        let verdict = stream.try_recv().unwrap().event;
        assert!(matches!(verdict.kind, EventKind::Verification(_)));
        let handback = stream.try_recv().unwrap().event;
        assert_eq!(
            handback.channel,
            Channel::Direct(RoleId::new("backend")).name(),
            "the handback is addressed to the owner",
        );
        match handback.kind {
            EventKind::Message(message) => {
                assert!(
                    message.body.contains("tokens never expire"),
                    "carries the failure"
                );
                assert!(
                    message.body.contains("resubmit"),
                    "tells the owner how to retry"
                );
            }
            other => panic!("expected a message handback, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_failing_verdict_without_a_failure_is_rejected() {
        let state = AppState::new(Config::default());
        post(
            &state,
            "/gate/submit",
            json!({ "role": "backend", "task": "login" }),
        )
        .await;
        let (status, _) = post(
            &state,
            "/gate/verdict",
            json!({ "role": "qa", "task": "login", "pass": false }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn a_verdict_on_an_unsubmitted_task_is_not_found() {
        let state = AppState::new(Config::default());
        let (status, _) = post(
            &state,
            "/gate/verdict",
            json!({ "role": "qa", "task": "ghost", "pass": true }),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn a_verdict_on_a_done_task_is_refused_until_resubmitted() {
        let state = AppState::new(Config::default());
        post(
            &state,
            "/gate/submit",
            json!({ "role": "backend", "task": "login", "acceptance": "a" }),
        )
        .await;
        post(
            &state,
            "/gate/verdict",
            json!({ "role": "qa", "task": "login", "pass": true }),
        )
        .await;

        // A second verdict on the passed task is a conflict.
        let (status, _) = post(
            &state,
            "/gate/verdict",
            json!({ "role": "security", "task": "login", "pass": false, "failure": "x" }),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT);

        // Resubmitting reopens the gate, and a fresh verdict lands.
        post(
            &state,
            "/gate/submit",
            json!({ "role": "backend", "task": "login", "acceptance": "a" }),
        )
        .await;
        let (status, view) = post(
            &state,
            "/gate/verdict",
            json!({ "role": "qa", "task": "login", "pass": true }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(task_in(&view, "login")["verdict"], "passed");
    }

    #[tokio::test]
    async fn a_submission_notifies_a_named_reviewer() {
        let state = AppState::new(Config::default());
        let mut stream = state.broadcast.subscribe();

        post(
            &state,
            "/gate/submit",
            json!({ "role": "backend", "task": "login", "acceptance": "tokens expire", "to": "qa" }),
        )
        .await;

        // The submission event, then a review-request note addressed to the reviewer.
        let submitted = stream.try_recv().unwrap().event;
        assert!(matches!(submitted.kind, EventKind::Verification(_)));
        let request = stream.try_recv().unwrap().event;
        assert_eq!(request.channel, Channel::Direct(RoleId::new("qa")).name());
        match request.kind {
            EventKind::Message(message) => {
                assert!(
                    message.body.contains("verify"),
                    "asks the reviewer to verify"
                );
                assert!(
                    message.body.contains("tokens expire"),
                    "states the acceptance"
                );
            }
            other => panic!("expected a review-request note, got {other:?}"),
        }
    }

    /// A unique temp dir for the durability test, removed on drop.
    struct TempDir(std::path::PathBuf);
    impl TempDir {
        fn new() -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static N: AtomicU64 = AtomicU64::new(0);
            let n = N.fetch_add(1, Ordering::Relaxed);
            Self(std::env::temp_dir().join(format!("crew-gate-test-{}-{n}", std::process::id())))
        }
    }
    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[tokio::test]
    async fn a_task_mid_verification_survives_a_restart() {
        let dir = TempDir::new();

        // First run: submit a task for verification against a durable store, then drop
        // the broker with the task still awaiting a verdict.
        let store = Arc::new(LogStore::open(&dir.0).unwrap());
        let state = AppState::with_storage(Config::default(), store);
        post(
            &state,
            "/gate/submit",
            json!({ "role": "backend", "task": "login", "acceptance": "tokens expire" }),
        )
        .await;
        drop(state);

        // Second run: a fresh broker over the same dir rebuilds the gate from the log
        // (issue #181), like the board (issue #49).
        let reopened = Arc::new(LogStore::open(&dir.0).unwrap());
        let restarted = AppState::with_storage(Config::default(), reopened);

        // The submission survived: still awaiting a verdict, owned by backend.
        let view = get(&restarted, "/gate").await;
        assert_eq!(task_in(&view, "login")["verdict"], "submitted");
        assert_eq!(task_in(&view, "login")["owner"], "backend");

        // The gate still enforces on the rebuilt task: the owner cannot self-verify it,
        // proving the owner (not just the task key) was restored.
        let (self_verdict, _) = post(
            &restarted,
            "/gate/verdict",
            json!({ "role": "backend", "task": "login", "pass": true }),
        )
        .await;
        assert_eq!(
            self_verdict,
            StatusCode::CONFLICT,
            "the rebuilt owner still blocks a self-verdict"
        );

        // An independent verifier passes the surviving submission: the gate works after
        // a restart just as before it.
        let (status, view) = post(
            &restarted,
            "/gate/verdict",
            json!({ "role": "qa", "task": "login", "pass": true }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(task_in(&view, "login")["verdict"], "passed");
        assert_eq!(task_in(&view, "login")["verifier"], "qa");
    }
}
