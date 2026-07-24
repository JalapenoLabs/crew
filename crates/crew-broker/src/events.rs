//! The message endpoints: posting to a channel and subscribing.
//!
//! A `POST /channels/{channel}/messages` stamps the event server-side, masks
//! any configured secret out of it, persists it, and fans it to every
//! subscriber. `GET /stream` subscribes to the live aggregate feed over
//! Server-Sent Events, optionally narrowed by the shared
//! [`FilterQuery`](crate::filter::FilterQuery) so the live view matches the
//! filtered `GET /history` (issue #31). The per-role, self-filtered stream is
//! `GET /inbox`. Reading the stored log is `GET /history` alone: it filters,
//! orders, and paginates with a ceiling, so there is one bounded read path
//! rather than an unpaginated full-log dump (issue #209).

use std::convert::Infallible;

use axum::{
    body::Bytes,
    extract::{FromRequest, Path, Query, Request, State},
    http::{HeaderMap, StatusCode},
    response::sse::{Event as SseEvent, Sse},
    routing::{get, post},
    Json, Router,
};
use crew_core::{
    ChannelId, Event, EventKind, Message, MessageId, MessageKind, RoleId, Sender, TaskId,
};
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::Value;
use tokio_stream::Stream;
use tracing::{event, Level};

use crate::{error::ApiError, filter::FilterQuery, state::AppState, store::Storage};

/// The message routes: post to a channel and subscribe to the feed.
///
/// Reading the stored log is `GET /history` (see [`history`](crate::history)):
/// it filters, orders, and paginates with a ceiling, so the broker has one
/// bounded read path and no unpaginated full-log dump (issue #209).
pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route("/channels/{channel}/messages", post(post_message))
        .route("/stream", get(stream))
}

/// A JSON body extractor that fails with a typed [`ApiError`] on malformed
/// input.
///
/// Unlike axum's built-in `Json`, a bad body (invalid JSON, wrong types, an
/// unknown message kind, a missing per-kind field) yields a 400 with a
/// `{ "error": ... }` body instead of a plain-text rejection, and never a
/// panic.
pub(crate) struct JsonBody<T>(pub T);

impl<S, T> FromRequest<S> for JsonBody<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        let bytes = Bytes::from_request(req, state).await.map_err(|error| {
            ApiError::bad_request(format!("could not read request body: {error}"))
        })?;
        let value = serde_json::from_slice::<T>(&bytes)
            .map_err(|error| ApiError::bad_request(format!("invalid request body: {error}")))?;
        Ok(Self(value))
    }
}

/// The body of `POST /channels/{channel}/messages`: a message to post to a
/// channel.
///
/// The channel comes from the path, and the broker stamps the id and timestamp;
/// the client supplies who it is from, an optional task, the typed kind with
/// its per-kind fields (flattened), and a body. For example an order posts as
/// `{"from":{"kind":"role","id":"backend"},"kind":"order","title":"..","scope":
/// "..", "owned_paths":[],"acceptance":"..","body":".."}`.
#[derive(Debug, Deserialize)]
struct PostMessage {
    /// Who is sending: a role, or the General.
    from: Sender,
    /// The task this message belongs to, if any.
    #[serde(default)]
    task: Option<TaskId>,
    /// The typed intent and its per-kind fields.
    #[serde(flatten)]
    kind: MessageKind,
    /// The markdown body.
    #[serde(default)]
    body: String,
}

/// Fields the broker owns: it stamps `ts` and `id`, and routes by the path
/// `channel`, so a client that sends any of them is rejected rather than
/// trusted.
const BROKER_OWNED_FIELDS: &[&str] = &["ts", "id", "channel"];

impl PostMessage {
    /// Parses a request body, rejecting any broker-owned field the client tried
    /// to set.
    ///
    /// This is where a spoofed timestamp is refused: `ts` is the broker's to
    /// stamp, so its presence (like `id` or `channel`) is a client error,
    /// not an override.
    ///
    /// # Errors
    /// Returns a 400 [`ApiError`] if the body carries a broker-owned field or
    /// does not model a message.
    fn from_json(raw: Value) -> Result<Self, ApiError> {
        if let Value::Object(fields) = &raw {
            if let Some(owned) = BROKER_OWNED_FIELDS
                .iter()
                .find(|field| fields.contains_key(**field))
            {
                return Err(ApiError::bad_request(format!(
                    "the broker sets `{owned}`; it must not appear in the request body"
                )));
            }
        }
        serde_json::from_value(raw)
            .map_err(|error| ApiError::bad_request(format!("invalid request body: {error}")))
    }

    /// Validates the fields the broker will not fix up: the path `channel` and
    /// `from`.
    ///
    /// The `kind` is validated structurally by deserialization; this catches
    /// the semantic gaps serde cannot: an empty channel or an empty role
    /// sender.
    ///
    /// # Errors
    /// Returns a 400 [`ApiError`] on an empty channel or an empty role sender.
    fn validate(&self, channel: &str) -> Result<(), ApiError> {
        if channel.trim().is_empty() {
            return Err(ApiError::bad_request("channel must not be empty"));
        }
        if let Sender::Role(role) = &self.from {
            if role.as_str().trim().is_empty() {
                return Err(ApiError::bad_request("a role sender must not be empty"));
            }
        }
        Ok(())
    }
}

/// `POST /channels/{channel}/messages`: post a message to a channel.
///
/// The broker stamps the id and timestamp (rejecting a client-supplied one),
/// masks any configured secret out of the event, stores it, fans it to every
/// subscriber, and returns the scrubbed [`Event`] with `201 Created`.
///
/// # Errors
/// Returns a 400 [`ApiError`] if the body is malformed, carries a broker-owned
/// field, or fails validation.
async fn post_message(
    Path(channel): Path<String>,
    State(state): State<AppState>,
    JsonBody(raw): JsonBody<Value>,
) -> Result<(StatusCode, Json<Event>), ApiError> {
    let request = PostMessage::from_json(raw)?;
    request.validate(&channel)?;
    ensure_order_authorized(&request, &state.config.commander)?;
    ensure_answer_references_a_question(&request, state.storage.as_ref())?;

    let event = Event {
        ts: crew_core::Timestamp::now(),
        from: request.from,
        channel: ChannelId::new(channel),
        task: request.task,
        kind: EventKind::Message(Message {
            id: MessageId::new(),
            kind: request.kind,
            body: request.body,
        }),
    };
    // `publish` masks any secret, stores the event, and fans out the scrubbed
    // result to every subscriber, so the response, the log, and every stream
    // agree.
    let sequenced = state.publish(event);
    // An order assigns scoped work to one role; seed a ledger claim for the
    // recipient so assigned-but-not-started work shows on the ledger without a
    // manual claim (issue #184). A no-op for every other message.
    crate::ledger::seed_order_claim(&state, &sequenced.event);
    Ok((StatusCode::CREATED, Json(sequenced.event)))
}

/// Enforces that issuing an `order` is a commander (or General) act (issue
/// #194).
///
/// The hub-and-spoke design has the commander decompose the General's intent
/// and fan orders out; arbitration (work-claiming, interface disputes) resolves
/// at the hub (`docs/communication.md`). An order mints a task (issue #132) and
/// seeds a ledger claim (issue #184), so letting any specialist issue one would
/// route tracked assignments around the hub, the very free-for-all the topology
/// avoids. The General's direct override (`crew command`, issue #42) posts its
/// order from [`Sender::General`], so it is allowed; a specialist delegates
/// with a message ([`crew_send`](crew_mcp)) or asks the commander to assign the
/// work.
///
/// # Errors
/// Returns a 403 [`ApiError::Forbidden`] if the message is an `order` from a
/// role other than the crew's `commander`. Every other kind, and any order from
/// the General or the commander, passes.
fn ensure_order_authorized(request: &PostMessage, commander: &RoleId) -> Result<(), ApiError> {
    if let (MessageKind::Order { .. }, Sender::Role(role)) = (&request.kind, &request.from) {
        if role != commander {
            return Err(ApiError::forbidden(format!(
                "only the commander (`{commander}`) may issue an order; `{role}` should delegate \
                 with a message (crew_send) or ask the commander to assign the work"
            )));
        }
    }
    Ok(())
}

/// Ensures an `answer`'s `in_reply_to` names a stored question (issue #211).
///
/// An answer threads to the question it replies to by [`MessageId`], so a
/// front-end and the commander pair the two without parsing the prose body. A
/// reference to a message that is not a stored question would leave that thread
/// dangling, so it is rejected here rather than persisted; every other kind
/// passes untouched. The lookup is an O(1) [`Storage::message`] by-id read, not
/// a full-log clone-and-scan (issue #273).
///
/// # Errors
/// Returns a 400 [`ApiError`] if the answer's `in_reply_to` does not name an
/// existing `question` message.
fn ensure_answer_references_a_question(
    request: &PostMessage,
    storage: &dyn Storage,
) -> Result<(), ApiError> {
    let MessageKind::Answer { in_reply_to } = &request.kind else {
        return Ok(());
    };
    // O(1) by-id lookup rather than cloning and scanning the whole log (issue
    // #273): the reference must resolve to a stored `question` message.
    let names_a_question = storage
        .message(in_reply_to)
        .is_some_and(|message| matches!(message.kind, MessageKind::Question { .. }));
    if names_a_question {
        Ok(())
    } else {
        Err(ApiError::bad_request(format!(
            "an answer's `in_reply_to` (`{in_reply_to}`) must name an existing question message"
        )))
    }
}

/// `GET /stream`: subscribe to the live event feed as Server-Sent Events.
///
/// This is the aggregate activity log's live view (issue #31): the whole unit's
/// stream, optionally narrowed by the shared [`FilterQuery`] (`channel`,
/// `role`, `kind`, `task`, `since`). With no filter it is the firehose; with a
/// filter it delivers only matching events, using the very same filter test
/// ([`EventFilter::matches`](crate::store::EventFilter::matches)) the history
/// query applies, so the live and historical views agree. `GET /inbox` is the
/// per-role, self-filtered delivery view instead.
///
/// It resumes losslessly like `GET /inbox` (issue #134): on reconnect the
/// client's `Last-Event-ID` replays the matching events it missed from the log
/// before switching to the live tail, so a dropped or lagged consumer needs no
/// separate `/history` catch-up call. A fresh connection with no cursor starts
/// at the live tail rather than replaying the whole history. The replay reuses
/// the same [`EventFilter::matches`](crate::store::EventFilter::matches) as the
/// live tail and `GET /history`, so all three agree on the view. Each event
/// arrives already scrubbed and carries its log sequence as the SSE `id`; a
/// subscriber that lags past the broadcast buffer skips the gap and recovers it
/// from its `Last-Event-ID`.
///
/// # Errors
/// Returns a 400 [`ApiError`] if a filter parameter is malformed.
async fn stream(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(filter): Query<FilterQuery>,
) -> Result<Sse<impl Stream<Item = Result<SseEvent, Infallible>>>, ApiError> {
    let filter = filter.to_filter()?;
    Ok(crate::sse::resume_stream(
        &state,
        &headers,
        move |event| filter.matches(event),
        warn_lagged,
    ))
}

/// Logs a filtered `/stream` subscriber that lagged off the broadcast (issue
/// #134).
///
/// The gap is skipped, since the client replays it from `Last-Event-ID` on
/// reconnect; a recurring lag tells the operator the broadcast capacity is too
/// small under load.
fn warn_lagged(skipped: u64) {
    event!(
        name: "broker.stream.lagged",
        Level::WARN,
        skipped,
        "a /stream subscriber lagged off the broadcast and skipped {{skipped}} events; the \
         client replays them from Last-Event-ID. Raise the broadcast capacity if this recurs \
         under load.",
    );
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
    };
    use crew_core::{Event, EventKind, MessageKind};
    use serde_json::{json, Value};
    use tokio_stream::StreamExt;
    use tower::ServiceExt;

    use crate::{api, config::Config, state::AppState};

    /// Sends a raw body to `POST /channels/{channel}/messages`, returning the
    /// status and JSON response.
    async fn post_raw(
        state: &AppState,
        channel: &str,
        body: impl Into<Body>,
    ) -> (StatusCode, Value) {
        let request = Request::builder()
            .method("POST")
            .uri(format!("/channels/{channel}/messages"))
            .header("content-type", "application/json")
            .body(body.into())
            .unwrap();
        let response = api::build(state.clone()).oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (status, value)
    }

    async fn post(state: &AppState, channel: &str, message: Value) -> (StatusCode, Value) {
        post_raw(state, channel, serde_json::to_vec(&message).unwrap()).await
    }

    /// Reads the stored event log back through `GET /history` (one page at the
    /// max limit, which holds every event these small test logs produce).
    async fn stored_events(state: &AppState) -> Vec<Event> {
        let request = Request::builder()
            .uri("/history?limit=1000")
            .body(Body::empty())
            .unwrap();
        let response = api::build(state.clone()).oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        serde_json::from_value(value["events"].clone()).unwrap()
    }

    /// One valid `(channel, body)` per `MessageKind`, with its per-kind fields
    /// and no broker-owned field (the channel travels in the path).
    ///
    /// `answer` is posted separately by the round-trip test: its `in_reply_to`
    /// must name a real stored question (issue #211), an id the broker mints,
    /// so it cannot be a static fixture.
    fn one_of_each_kind() -> Vec<(&'static str, Value)> {
        let backend = json!({ "kind": "role", "id": "backend" });
        // An order is a commander act (issue #194), so the round-trip fixture issues
        // it as the commander; every other kind is a specialist's to send.
        let commander = json!({ "kind": "role", "id": "commander" });
        vec![
            (
                "all-units",
                json!({ "from": commander, "kind": "order",
                    "title": "Ship it", "scope": "here", "owned_paths": ["src"],
                    "acceptance": "green", "body": "detail" }),
            ),
            (
                "@backend",
                json!({ "from": backend, "kind": "question",
                    "options": ["a", "b"], "body": "which?" }),
            ),
            (
                "all-units",
                json!({ "from": backend, "kind": "status", "body": "working" }),
            ),
            (
                "all-units",
                json!({ "from": backend, "kind": "artifact",
                    "reference": "feature/x", "artifact_kind": "branch", "body": "opened" }),
            ),
            (
                "all-units",
                json!({ "from": { "kind": "general" }, "kind": "note", "body": "fyi" }),
            ),
            (
                "@backend",
                json!({ "from": { "kind": "general" }, "kind": "redirect",
                    "body": "prefer the async path" }),
            ),
            (
                "@backend",
                json!({ "from": { "kind": "general" }, "kind": "belay",
                    "body": "stop; switch to the login bug" }),
            ),
        ]
    }

    #[tokio::test]
    async fn every_message_kind_round_trips_through_the_api() {
        let state = AppState::new(Config::default());

        let mut posted = Vec::new();
        for (channel, message) in one_of_each_kind() {
            let (status, value) = post(&state, channel, message).await;
            assert_eq!(status, StatusCode::CREATED, "post should succeed: {value}");
            let event: Event = serde_json::from_value(value).unwrap();
            assert!(matches!(event.kind, EventKind::Message(_)));
            assert_eq!(
                event.channel.as_str(),
                channel,
                "channel comes from the path"
            );
            posted.push(event);
        }

        // An answer must name a real question (issue #211), so reply to the one
        // just posted; then confirm it round-trips like every other kind.
        let (status, value) = post(
            &state,
            "@backend",
            json!({ "from": { "kind": "role", "id": "backend" }, "kind": "answer",
                "in_reply_to": question_id(&posted), "body": "a" }),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::CREATED,
            "answer to a real question: {value}"
        );
        posted.push(serde_json::from_value(value).unwrap());

        // Read the log back and confirm every posted event survived intact.
        assert_eq!(
            stored_events(&state).await,
            posted,
            "the API must round-trip every message kind"
        );
    }

    /// The stored `MessageId` (as a string) of the single question in `events`.
    fn question_id(events: &[Event]) -> String {
        events
            .iter()
            .find_map(|event| match &event.kind {
                EventKind::Message(message)
                    if matches!(message.kind, MessageKind::Question { .. }) =>
                {
                    Some(message.id.to_string())
                }
                _ => None,
            })
            .expect("a question was posted")
    }

    /// The `MessageId` (as a string) carried by a posted message event.
    fn message_id(event: &Event) -> String {
        match &event.kind {
            EventKind::Message(message) => message.id.to_string(),
            other => panic!("expected a message event, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn an_answer_to_a_real_question_is_accepted() {
        let state = AppState::new(Config::default());
        let backend = json!({ "kind": "role", "id": "backend" });

        let (_, question) = post(
            &state,
            "@frontend",
            json!({ "from": backend, "kind": "question", "body": "which auth lib?" }),
        )
        .await;
        let question: Event = serde_json::from_value(question).unwrap();

        let (status, _) = post(
            &state,
            "@backend",
            json!({ "from": { "kind": "role", "id": "frontend" }, "kind": "answer",
                "in_reply_to": message_id(&question), "body": "use jwt" }),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::CREATED,
            "an answer naming a real question is accepted"
        );
    }

    #[tokio::test]
    async fn an_answer_to_a_nonexistent_question_is_rejected() {
        // A dangling reference would break threading (issue #211), so it is
        // refused rather than persisted.
        let state = AppState::new(Config::default());
        let (status, value) = post(
            &state,
            "@backend",
            json!({ "from": { "kind": "role", "id": "frontend" }, "kind": "answer",
                "in_reply_to": "11111111-1111-1111-1111-111111111111", "body": "a" }),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "no such question: {value}");
        assert!(value.get("error").is_some(), "typed error body: {value}");
        assert!(
            stored_events(&state).await.is_empty(),
            "the rejected answer is not persisted"
        );
    }

    #[tokio::test]
    async fn an_answer_referencing_a_non_question_message_is_rejected() {
        // `in_reply_to` must name a question, not just any message: replying to a
        // note is refused so the thread always resolves to a real question.
        let state = AppState::new(Config::default());
        let (_, note) = post(
            &state,
            "@backend",
            json!({ "from": { "kind": "general" }, "kind": "note", "body": "fyi" }),
        )
        .await;
        let note: Event = serde_json::from_value(note).unwrap();

        let (status, value) = post(
            &state,
            "@backend",
            json!({ "from": { "kind": "role", "id": "backend" }, "kind": "answer",
                "in_reply_to": message_id(&note), "body": "a" }),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "a note is not a question: {value}"
        );
    }

    /// An order carrying its per-kind fields, from `sender`, to one specialist.
    fn order_from(sender: &Value) -> Value {
        json!({ "from": sender, "kind": "order",
            "title": "build login", "scope": "the /login route",
            "owned_paths": ["api/"], "acceptance": "tests green", "body": "" })
    }

    #[tokio::test]
    async fn a_specialist_may_not_issue_an_order() {
        // Order-issuing is a commander act (issue #194): the hub fans work out and
        // arbitrates, so a specialist's order is refused with 403, pointing it at
        // the peer escape valve (crew_send) instead.
        let state = AppState::new(Config::default());
        let (status, value) = post(
            &state,
            "@frontend",
            order_from(&json!({ "kind": "role", "id": "backend" })),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::FORBIDDEN,
            "a specialist may not order: {value}"
        );
        assert!(
            value["error"]
                .as_str()
                .unwrap_or_default()
                .contains("only the commander"),
            "the refusal names the rule: {value}",
        );
        assert!(
            stored_events(&state).await.is_empty(),
            "a refused order never reaches the log",
        );
    }

    #[tokio::test]
    async fn the_commander_may_issue_an_order() {
        // The default commander is `commander`; its order is the fan-out handle.
        let state = AppState::new(Config::default());
        let (status, _) = post(
            &state,
            "@backend",
            order_from(&json!({ "kind": "role", "id": "commander" })),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::CREATED,
            "the commander may issue an order"
        );
    }

    #[tokio::test]
    async fn the_general_may_issue_a_direct_order() {
        // The General's direct override (`crew command`, issue #42) posts its order
        // from the General, so it is allowed and the commander is informed out of band.
        let state = AppState::new(Config::default());
        let (status, _) = post(
            &state,
            "@backend",
            order_from(&json!({ "kind": "general" })),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::CREATED,
            "the General may order a specialist directly"
        );
    }

    #[tokio::test]
    async fn the_broker_stamps_the_timestamp_and_rejects_a_spoofed_one() {
        let state = AppState::new(Config::default());
        let backend = json!({ "kind": "role", "id": "backend" });

        // A body that carries a broker-owned field is refused, not trusted.
        for spoof in [
            json!({ "from": backend, "kind": "note", "body": "hi", "ts": "2000-01-01T00:00:00Z" }),
            json!({ "from": backend, "kind": "note", "body": "hi", "id": "00000000-0000-0000-0000-000000000000" }),
            json!({ "from": backend, "kind": "note", "body": "hi", "channel": "elsewhere" }),
        ] {
            let (status, value) = post(&state, "all-units", spoof.clone()).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "should reject {spoof}");
            assert!(value.get("error").is_some(), "typed error body: {value}");
        }

        // A clean post is stamped by the broker with a fresh timestamp and id.
        let before = crew_core::Timestamp::now();
        let (status, value) = post(
            &state,
            "all-units",
            json!({ "from": backend, "kind": "note", "body": "hi" }),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
        let event: Event = serde_json::from_value(value).unwrap();
        assert!(event.ts >= before, "the broker stamps a current timestamp");
        assert!(stored_events(&state)
            .await
            .first()
            .is_some_and(|stored| stored == &event));
    }

    #[tokio::test]
    async fn a_message_posted_within_a_task_carries_its_id() {
        let state = AppState::new(Config::default());
        let task = crew_core::TaskId::new();

        // A client threads the task on the message; the broker preserves it.
        let (status, value) = post(
            &state,
            "@backend",
            json!({ "from": { "kind": "role", "id": "commander" }, "kind": "note",
                    "body": "work the parser", "task": task.to_string() }),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);

        let event: Event = serde_json::from_value(value).unwrap();
        assert_eq!(event.task, Some(task), "the response carries the task id");
        assert_eq!(
            stored_events(&state).await[0].task,
            Some(task),
            "the stored event correlates to the task",
        );
    }

    #[tokio::test]
    async fn a_posted_message_reaches_a_subscriber_with_secrets_masked() {
        let secret = "sk-ant-supersecrettoken";
        let config = Config {
            secrets: vec![secret.to_owned()],
            ..Config::default()
        };
        let state = AppState::new(config);
        let mut subscriber = state.broadcast.subscribe();

        let (status, value) = post(
            &state,
            "all-units",
            json!({ "from": { "kind": "role", "id": "backend" }, "kind": "note",
                    "body": format!("the key is {secret}, keep it safe") }),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);

        // The response, the stream, and storage all carry the same masked event.
        let returned: Event = serde_json::from_value(value).unwrap();
        let streamed = subscriber
            .recv()
            .await
            .expect("the message must reach the subscriber")
            .event;
        assert_eq!(
            streamed, returned,
            "the subscriber receives the posted event"
        );

        let body_of = |event: &Event| match &event.kind {
            EventKind::Message(message) => message.body.clone(),
            EventKind::Lifecycle(_)
            | EventKind::Activity(_)
            | EventKind::Ledger(_)
            | EventKind::Boundary(_)
            | EventKind::Verification(_)
            | EventKind::Board(_)
            | EventKind::Budget(_)
            | EventKind::Telemetry(_)
            | EventKind::Usage(_)
            | EventKind::Stall(_)
            | EventKind::Mission(_) => {
                panic!("expected a message")
            }
        };
        assert!(
            !body_of(&streamed).contains(secret),
            "the stream must not leak the secret"
        );
        let stored = stored_events(&state).await;
        assert_eq!(
            stored,
            vec![streamed],
            "storage holds the same masked event"
        );
        assert!(
            !body_of(&stored[0]).contains(secret),
            "storage must not leak the secret"
        );
    }

    #[tokio::test]
    async fn the_stream_endpoint_serves_server_sent_events() {
        let state = AppState::new(Config::default());
        let request = Request::builder()
            .uri("/stream")
            .body(Body::empty())
            .unwrap();
        // The SSE body is open-ended, so assert on the response head without reading
        // it.
        let response = api::build(state).oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok()),
            Some("text/event-stream"),
        );
    }

    #[tokio::test]
    async fn the_stream_accepts_a_filter_and_rejects_a_malformed_one() {
        let state = AppState::new(Config::default());
        // A well-formed filter opens the stream.
        let ok = Request::builder()
            .uri("/stream?role=backend&kind=lifecycle")
            .body(Body::empty())
            .unwrap();
        let response = api::build(state.clone()).oneshot(ok).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK, "a valid filter opens it");

        // A malformed filter is a typed 400 before the stream opens.
        let bad = Request::builder()
            .uri("/stream?kind=bogus")
            .body(Body::empty())
            .unwrap();
        let response = api::build(state).oneshot(bad).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        assert!(value.get("error").is_some(), "typed error body: {value}");
    }

    /// Opens `GET /stream` with `filter` query params, optionally resuming from
    /// a `Last-Event-ID`.
    async fn open_stream(
        state: &AppState,
        filter: &str,
        last_event_id: Option<&str>,
    ) -> axum::response::Response {
        let mut request = Request::builder().uri(format!("/stream?{filter}"));
        if let Some(id) = last_event_id {
            request = request.header("last-event-id", id);
        }
        api::build(state.clone())
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    /// Reads up to `want` Server-Sent Events `(id, data)` from a body, giving
    /// up after a short budget since the live tail never closes the stream.
    async fn read_sse(body: Body, want: usize) -> Vec<(u64, Value)> {
        let mut stream = body.into_data_stream();
        let mut buffer: Vec<u8> = Vec::new();
        let mut events = Vec::new();
        while events.len() < want {
            match tokio::time::timeout(Duration::from_millis(500), stream.next()).await {
                Ok(Some(Ok(chunk))) => {
                    buffer.extend_from_slice(&chunk);
                    while let Some(pos) = buffer.windows(2).position(|window| window == b"\n\n") {
                        let block: Vec<u8> = buffer.drain(..pos + 2).collect();
                        let block = String::from_utf8_lossy(&block);
                        let mut id = None;
                        let mut data = None;
                        for line in block.lines() {
                            if let Some(rest) = line.strip_prefix("id:") {
                                id = rest.trim().parse::<u64>().ok();
                            } else if let Some(rest) = line.strip_prefix("data:") {
                                data = serde_json::from_str::<Value>(rest.trim()).ok();
                            }
                        }
                        if let (Some(id), Some(data)) = (id, data) {
                            events.push((id, data));
                        }
                    }
                }
                _ => break, // timeout, end of stream, or a read error
            }
        }
        events
    }

    #[tokio::test]
    async fn the_stream_replays_the_filtered_gap_from_last_event_id() {
        // Issue #134: a dropped or lagged /stream consumer reconnects with its
        // Last-Event-ID and replays the events it missed, narrowed by the same
        // filter, so it resumes without a separate /history call.
        let state = AppState::new(Config::default());
        let backend = json!({ "kind": "role", "id": "backend" });
        let frontend = json!({ "kind": "role", "id": "frontend" });
        let note = |from: &Value, body: &str| json!({ "from": from, "kind": "note", "body": body });

        // A mix of senders lands on the log at seq 0..=3.
        post(&state, "all-units", note(&backend, "b0")).await; // seq 0
        post(&state, "all-units", note(&frontend, "f1")).await; // seq 1
        post(&state, "all-units", note(&backend, "b2")).await; // seq 2
        post(&state, "all-units", note(&frontend, "f3")).await; // seq 3

        // Reconnect filtering role=backend, last having seen seq 0: replay only
        // backend events after the cursor, so seq 2 alone, skipping the frontend
        // events (1, 3) and the already-seen seq 0.
        let response = open_stream(&state, "role=backend", Some("0")).await;
        assert_eq!(response.status(), StatusCode::OK);
        let events = read_sse(response.into_body(), 1).await;
        let ids: Vec<u64> = events.iter().map(|(id, _)| *id).collect();
        assert_eq!(
            ids,
            vec![2],
            "resumes after the cursor, replaying only the filtered events",
        );
        assert_eq!(events[0].1["kind"]["data"]["body"], "b2");
    }

    #[tokio::test]
    async fn a_fresh_stream_connection_starts_at_the_live_tail() {
        // Without a Last-Event-ID a fresh connection does not replay the backlog:
        // it starts at the live tail, exactly as /inbox does.
        let state = AppState::new(Config::default());
        let backend = json!({ "kind": "role", "id": "backend" });
        post(
            &state,
            "all-units",
            json!({ "from": backend, "kind": "note", "body": "before" }),
        )
        .await;

        let response = open_stream(&state, "", None).await;
        assert_eq!(response.status(), StatusCode::OK);
        let events = read_sse(response.into_body(), 1).await;
        assert!(
            events.is_empty(),
            "a fresh connection replays nothing, starting at the live tail: {events:?}",
        );
    }

    #[tokio::test]
    async fn malformed_messages_return_a_typed_4xx_never_a_panic() {
        let state = AppState::new(Config::default());
        let backend = json!({ "kind": "role", "id": "backend" });

        // Structurally invalid JSON.
        let (status, value) = post_raw(&state, "all-units", "{ not json").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(value.get("error").is_some(), "typed error body: {value}");

        // Well-formed JSON that fails to model a message.
        let bad = [
            json!({ "kind": "note" }),                   // missing `from`
            json!({ "from": backend, "kind": "bogus" }), // unknown kind
            json!({ "from": backend, "kind": "order" }), // order missing fields
        ];
        for message in bad {
            let (status, value) = post(&state, "all-units", message.clone()).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "should reject {message}");
            assert!(
                value.get("error").is_some(),
                "typed error body for {message}: {value}"
            );
        }

        // An all-whitespace channel and an empty role sender are semantic 400s.
        let (status, _) = post(&state, "%20", json!({ "from": backend, "kind": "note" })).await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "a blank channel is rejected"
        );
        let (status, _) = post(
            &state,
            "all-units",
            json!({ "from": { "kind": "role", "id": "" }, "kind": "note" }),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "an empty role sender is rejected"
        );

        // A rejected post must not have been stored.
        assert!(
            stored_events(&state).await.is_empty(),
            "malformed posts must not be stored"
        );
    }
}
