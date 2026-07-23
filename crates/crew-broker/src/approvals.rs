//! The approval gate: request a risky action, and record the General's decision (issue #39).
//!
//! A crew is safe to leave running because the moves that are expensive to undo, a push, a
//! merge, a delete, a spend, an external post, are gated behind human sign-off (see
//! `docs/roles.md`, rules of engagement). When a role's
//! [`RulesOfEngagement`](crew_core::RulesOfEngagement) gates an action it is about to take,
//! it requests approval here (`POST /approvals`) and blocks, polling `GET /approvals/{id}`
//! until the General decides it (`POST /approvals/{id}/decision`).
//!
//! Every step is a first-class `approval` event on the stream (to `all-units`): the request
//! so the General is notified, and the decision so the blocked role proceeds or abandons the
//! action with the reason recorded. `GET /approvals` reads the live gate. The gate state
//! lives in the broker ([`AppState`]).

use axum::extract::{Path, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use crew_core::{
    ApprovalDecision, ApprovalEvent, ApprovalId, ChannelId, Event, EventKind, RiskyAction, RoleId,
    Sender, Timestamp, ALL_UNITS,
};
use serde::{Deserialize, Serialize};

use crate::error::ApiError;
use crate::events::JsonBody;
use crate::state::{AppState, ApprovalEntry, ApprovalError};

/// The approval routes: read the gate, request approval, read one, and decide one.
pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route("/approvals", get(list).post(request))
        .route("/approvals/{id}", get(read))
        .route("/approvals/{id}/decision", post(decide))
}

/// `GET /approvals`: the live approval gate, every request and its standing, oldest first.
async fn list(State(state): State<AppState>) -> Json<ApprovalsView> {
    Json(ApprovalsView::from_state(&state))
}

/// The `POST /approvals` body: a role's request to take a gated action.
#[derive(Debug, Deserialize)]
struct Request {
    /// The role that must get sign-off before taking the action.
    role: String,
    /// The gated action, by its label (`push`, `merge`, `delete`, `spend`, `external_post`).
    action: String,
    /// What specifically the role wants to do, for the General to judge.
    #[serde(default)]
    detail: String,
}

/// `POST /approvals`: request approval for a gated action, returning the pending request.
///
/// Records the request as pending and announces it on the stream, so the General is
/// notified. The requesting role polls `GET /approvals/{id}` until the decision resolves.
///
/// # Errors
/// Returns a 400 [`ApiError`] if the role is empty or the action is not a known risky action.
async fn request(
    State(state): State<AppState>,
    JsonBody(request): JsonBody<Request>,
) -> Result<Json<ApprovalView>, ApiError> {
    let role = RoleId::new(non_empty(&request.role, "role")?);
    let action = RiskyAction::parse(request.action.trim()).ok_or_else(|| {
        ApiError::bad_request(format!(
            "`{}` is not a risky action; name one of push, merge, delete, spend, external_post",
            request.action.trim(),
        ))
    })?;
    let detail = request.detail.trim().to_owned();

    let id = state.request_approval(role.clone(), action, detail.clone());
    state.publish(approval_event(
        Sender::Role(role.clone()),
        ApprovalEvent {
            id,
            role: role.clone(),
            action,
            detail: detail.clone(),
            decision: ApprovalDecision::Pending,
            reason: String::new(),
        },
    ));

    let entry = state
        .approval(id)
        .expect("the request just recorded is present");
    Ok(Json(ApprovalView::new(id, entry)))
}

/// `GET /approvals/{id}`: read one request, for the blocked role to poll until it resolves.
///
/// # Errors
/// Returns a 400 if the id is malformed, or a 404 if no request has it.
async fn read(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<ApprovalView>, ApiError> {
    let id = parse_id(&id)?;
    let entry = state
        .approval(id)
        .ok_or_else(|| ApiError::not_found("no such approval request"))?;
    Ok(Json(ApprovalView::new(id, entry)))
}

/// The `POST /approvals/{id}/decision` body: the General's decision on a request.
#[derive(Debug, Deserialize)]
struct Decision {
    /// Whether the General approved (`true`) or denied (`false`) the action.
    approve: bool,
    /// The reason, required on a denial so the role learns why and abandons cleanly.
    #[serde(default)]
    reason: String,
}

/// `POST /approvals/{id}/decision`: approve or deny a pending request.
///
/// Records the decision and announces it on the stream, so the blocked role proceeds (on an
/// approval) or abandons the action with the reason (on a denial). A decision lands once: a
/// second decision on a resolved request is refused.
///
/// # Errors
/// Returns a 400 if the id is malformed or a denial carries no reason, a 404 if no request
/// has the id, or a 409 if it was already decided.
async fn decide(
    State(state): State<AppState>,
    Path(id): Path<String>,
    JsonBody(decision): JsonBody<Decision>,
) -> Result<Json<ApprovalView>, ApiError> {
    let id = parse_id(&id)?;
    let reason = decision.reason.trim().to_owned();
    if !decision.approve && reason.is_empty() {
        return Err(ApiError::bad_request(
            "a denial needs a `reason`, so the role learns why and abandons the action cleanly",
        ));
    }

    let outcome = state
        .decide_approval(id, decision.approve, reason)
        .map_err(map_approval_error)?;

    state.publish(approval_event(
        Sender::General,
        ApprovalEvent {
            id: outcome.id,
            role: outcome.role.clone(),
            action: outcome.action,
            detail: outcome.detail.clone(),
            decision: outcome.decision,
            reason: outcome.reason.clone(),
        },
    ));

    let entry = state
        .approval(id)
        .expect("the request just decided is present");
    Ok(Json(ApprovalView::new(id, entry)))
}

/// An approval step as a first-class stream event, to `all-units` so it is seen.
fn approval_event(from: Sender, event: ApprovalEvent) -> Event {
    Event {
        ts: Timestamp::now(),
        from,
        channel: ChannelId::new(ALL_UNITS),
        task: None,
        kind: EventKind::Approval(event),
    }
}

/// Maps a refused decision to the HTTP error a client reads.
fn map_approval_error(error: ApprovalError) -> ApiError {
    match error {
        ApprovalError::UnknownApproval => ApiError::not_found("no such approval request"),
        ApprovalError::AlreadyDecided(decision) => ApiError::conflict(format!(
            "that request was already {}",
            decision_label(decision),
        )),
    }
}

/// The wire label for a resolved decision, for the conflict message.
fn decision_label(decision: ApprovalDecision) -> &'static str {
    match decision {
        ApprovalDecision::Pending => "pending",
        ApprovalDecision::Approved => "approved",
        ApprovalDecision::Denied => "denied",
    }
}

/// Parses an [`ApprovalId`] from the path, erroring on a malformed one.
fn parse_id(raw: &str) -> Result<ApprovalId, ApiError> {
    ApprovalId::parse(raw.trim())
        .ok_or_else(|| ApiError::bad_request("that is not a valid approval id"))
}

/// Trims a required field, erroring if it is blank.
fn non_empty<'a>(value: &'a str, field: &str) -> Result<&'a str, ApiError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ApiError::bad_request(format!("{field} must not be empty")));
    }
    Ok(trimmed)
}

/// The `GET /approvals` response: every request under the gate and its standing.
#[derive(Debug, Serialize)]
struct ApprovalsView {
    /// The requests, oldest first.
    requests: Vec<ApprovalView>,
}

/// One request's standing in the approval gate.
#[derive(Debug, Serialize)]
struct ApprovalView {
    /// The request id, named when the General decides it.
    id: ApprovalId,
    /// The role that must get sign-off.
    role: RoleId,
    /// The gated action awaiting a decision.
    action: RiskyAction,
    /// What specifically the role wants to do.
    #[serde(skip_serializing_if = "String::is_empty")]
    detail: String,
    /// Where it stands: pending, approved, or denied.
    decision: ApprovalDecision,
    /// The General's reason, on a decision.
    #[serde(skip_serializing_if = "String::is_empty")]
    reason: String,
    /// When the request was made.
    requested_at: Timestamp,
}

impl ApprovalView {
    /// Builds the view of one request from its gate entry.
    fn new(id: ApprovalId, entry: ApprovalEntry) -> Self {
        Self {
            id,
            role: entry.role,
            action: entry.action,
            detail: entry.detail,
            decision: entry.decision,
            reason: entry.reason,
            requested_at: entry.requested_at,
        }
    }
}

impl ApprovalsView {
    /// Builds the view from the broker's live approval snapshot.
    fn from_state(state: &AppState) -> Self {
        let requests = state
            .approval_snapshot()
            .into_iter()
            .map(|(id, entry)| ApprovalView::new(id, entry))
            .collect();
        Self { requests }
    }
}

#[cfg(test)]
mod tests {
    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use crew_core::EventKind;
    use serde_json::{json, Value};
    use tower::ServiceExt;

    use crate::api;
    use crate::config::Config;
    use crate::state::AppState;

    async fn post(state: &AppState, path: &str, body: Value) -> (StatusCode, Value) {
        send(state, "POST", path, Some(body)).await
    }

    async fn get(state: &AppState, path: &str) -> (StatusCode, Value) {
        send(state, "GET", path, None).await
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

    #[tokio::test]
    async fn a_gated_action_waits_for_a_decision_then_is_approved() {
        let state = AppState::new(Config::default());
        let mut stream = state.broadcast.subscribe();

        // The role requests approval; the request is pending.
        let (status, request) = post(
            &state,
            "/approvals",
            json!({ "role": "backend", "action": "merge", "detail": "merge PR #42 into main" }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(request["decision"], "pending");
        assert_eq!(request["action"], "merge");
        let id = request["id"].as_str().unwrap().to_owned();

        // The request rides the stream as a pending approval event, so the General is notified.
        let requested = stream.try_recv().unwrap().event;
        assert!(matches!(requested.kind, EventKind::Approval(_)));

        // While pending, the role polling its request sees no decision.
        let (status, pending) = get(&state, &format!("/approvals/{id}")).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(pending["decision"], "pending");

        // The General approves it.
        let (status, decided) = post(
            &state,
            &format!("/approvals/{id}/decision"),
            json!({ "approve": true }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(decided["decision"], "approved");

        // The blocked role's next poll now reads the approval, so it proceeds.
        let (_, resolved) = get(&state, &format!("/approvals/{id}")).await;
        assert_eq!(resolved["decision"], "approved");
    }

    #[tokio::test]
    async fn a_denial_carries_the_reason_back_to_the_role() {
        let state = AppState::new(Config::default());
        let (_, request) = post(
            &state,
            "/approvals",
            json!({ "role": "backend", "action": "push" }),
        )
        .await;
        let id = request["id"].as_str().unwrap().to_owned();

        let (status, decided) = post(
            &state,
            &format!("/approvals/{id}/decision"),
            json!({ "approve": false, "reason": "not until CI is green" }),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(decided["decision"], "denied");
        assert_eq!(decided["reason"], "not until CI is green");
    }

    #[tokio::test]
    async fn a_denial_without_a_reason_is_rejected() {
        let state = AppState::new(Config::default());
        let (_, request) = post(
            &state,
            "/approvals",
            json!({ "role": "backend", "action": "delete" }),
        )
        .await;
        let id = request["id"].as_str().unwrap().to_owned();

        let (status, _) = post(
            &state,
            &format!("/approvals/{id}/decision"),
            json!({ "approve": false }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn a_second_decision_is_refused() {
        let state = AppState::new(Config::default());
        let (_, request) = post(
            &state,
            "/approvals",
            json!({ "role": "backend", "action": "merge" }),
        )
        .await;
        let id = request["id"].as_str().unwrap().to_owned();
        post(
            &state,
            &format!("/approvals/{id}/decision"),
            json!({ "approve": true }),
        )
        .await;

        let (status, _) = post(
            &state,
            &format!("/approvals/{id}/decision"),
            json!({ "approve": false, "reason": "changed my mind" }),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::CONFLICT,
            "a decided request does not flip"
        );
    }

    #[tokio::test]
    async fn an_unknown_action_is_rejected() {
        let state = AppState::new(Config::default());
        let (status, _) = post(
            &state,
            "/approvals",
            json!({ "role": "backend", "action": "stroll" }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn the_gate_lists_pending_requests_oldest_first() {
        let state = AppState::new(Config::default());
        post(
            &state,
            "/approvals",
            json!({ "role": "backend", "action": "push" }),
        )
        .await;
        post(
            &state,
            "/approvals",
            json!({ "role": "frontend", "action": "merge" }),
        )
        .await;

        let (status, view) = get(&state, "/approvals").await;
        assert_eq!(status, StatusCode::OK);
        let requests = view["requests"].as_array().unwrap();
        assert_eq!(requests.len(), 2);
        assert_eq!(
            requests[0]["role"], "backend",
            "the earlier request is first"
        );
        assert_eq!(requests[1]["role"], "frontend");
    }

    #[tokio::test]
    async fn a_decision_on_an_unknown_request_is_not_found() {
        let state = AppState::new(Config::default());
        let (status, _) = post(
            &state,
            "/approvals/11111111-1111-1111-1111-111111111111/decision",
            json!({ "approve": true }),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }
}
