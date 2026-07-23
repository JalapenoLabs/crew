//! The message endpoints: posting to a channel, reading the log, and subscribing.
//!
//! A `POST /channels/{channel}/messages` stamps the event server-side, masks any
//! configured secret out of it, persists it, and fans it to every subscriber. A
//! `GET /events` reads the stored log, and `GET /stream` subscribes to the live
//! aggregate feed over Server-Sent Events, optionally narrowed by the shared
//! [`FilterQuery`](crate::filter::FilterQuery) so the live view matches the filtered
//! `GET /history` (issue #31). The per-role, self-filtered stream is `GET /inbox`.

use std::convert::Infallible;

use axum::body::Bytes;
use axum::extract::{FromRequest, Path, Query, Request, State};
use axum::http::StatusCode;
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::routing::{get, post};
use axum::{Json, Router};
use crew_core::{ChannelId, Event, EventKind, Message, MessageId, MessageKind, Sender, TaskId};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{Stream, StreamExt};

use crate::error::ApiError;
use crate::filter::FilterQuery;
use crate::state::{AppState, Sequenced};

/// The message routes: post to a channel, read the log, and subscribe to the feed.
pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route("/channels/{channel}/messages", post(post_message))
        .route("/events", get(list_events))
        .route("/stream", get(stream))
}

/// A JSON body extractor that fails with a typed [`ApiError`] on malformed input.
///
/// Unlike axum's built-in `Json`, a bad body (invalid JSON, wrong types, an
/// unknown message kind, a missing per-kind field) yields a 400 with a
/// `{ "error": ... }` body instead of a plain-text rejection, and never a panic.
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

/// The body of `POST /channels/{channel}/messages`: a message to post to a channel.
///
/// The channel comes from the path, and the broker stamps the id and timestamp; the
/// client supplies who it is from, an optional task, the typed kind with its
/// per-kind fields (flattened), and a body. For example an order posts as
/// `{"from":{"kind":"role","id":"backend"},"kind":"order","title":"..","scope":"..",
/// "owned_paths":[],"acceptance":"..","body":".."}`.
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
/// `channel`, so a client that sends any of them is rejected rather than trusted.
const BROKER_OWNED_FIELDS: &[&str] = &["ts", "id", "channel"];

impl PostMessage {
    /// Parses a request body, rejecting any broker-owned field the client tried to set.
    ///
    /// This is where a spoofed timestamp is refused: `ts` is the broker's to stamp,
    /// so its presence (like `id` or `channel`) is a client error, not an override.
    ///
    /// # Errors
    /// Returns a 400 [`ApiError`] if the body carries a broker-owned field or does
    /// not model a message.
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

    /// Validates the fields the broker will not fix up: the path `channel` and `from`.
    ///
    /// The `kind` is validated structurally by deserialization; this catches the
    /// semantic gaps serde cannot: an empty channel or an empty role sender.
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
/// The broker stamps the id and timestamp (rejecting a client-supplied one), masks
/// any configured secret out of the event, stores it, fans it to every subscriber,
/// and returns the scrubbed [`Event`] with `201 Created`.
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
    // `publish` masks any secret, stores the event, and fans out the scrubbed result
    // to every subscriber, so the response, the log, and every stream agree.
    let sequenced = state.publish(event);
    Ok((StatusCode::CREATED, Json(sequenced.event)))
}

/// The body of `GET /events`: the stored event log, oldest first.
#[derive(Debug, Serialize)]
struct EventLog {
    events: Vec<Event>,
}

/// `GET /events`: read the whole event log (oldest first), already scrubbed.
async fn list_events(State(state): State<AppState>) -> Json<EventLog> {
    Json(EventLog {
        events: state.storage.events(),
    })
}

/// `GET /stream`: subscribe to the live event feed as Server-Sent Events.
///
/// This is the aggregate activity log's live view (issue #31): the whole unit's
/// stream, optionally narrowed by the shared [`FilterQuery`] (`channel`, `role`,
/// `kind`, `task`, `since`). With no filter it is the firehose; with a filter it
/// delivers only matching events, using the very same filter test
/// ([`EventFilter::matches`](crate::store::EventFilter::matches)) the history query
/// applies, so the live and historical views agree. `GET /inbox` is the
/// per-role, self-filtered delivery view instead.
///
/// Each event arrives already scrubbed and carries its log sequence as the SSE `id`.
/// A subscriber that lags past the channel's buffer skips the dropped events rather
/// than closing the stream, so a slow reader still receives everything after the gap;
/// it catches up on the gap through `GET /history` with the same filter.
///
/// # Errors
/// Returns a 400 [`ApiError`] if a filter parameter is malformed.
async fn stream(
    State(state): State<AppState>,
    Query(filter): Query<FilterQuery>,
) -> Result<Sse<impl Stream<Item = Result<SseEvent, Infallible>>>, ApiError> {
    let filter = filter.to_filter()?;
    let receiver = state.broadcast.subscribe();
    let events = BroadcastStream::new(receiver).filter_map(move |result| {
        // A lagged receiver skips the gap; the firehose is live-only, and a consumer
        // catches up through `/history` with the same filter.
        let Ok(Sequenced { seq, event }) = result else {
            return None;
        };
        if !filter.matches(&event) {
            return None;
        }
        SseEvent::default()
            .id(seq.to_string())
            .json_data(&event)
            .ok()
            .map(Ok)
    });
    Ok(Sse::new(events).keep_alive(KeepAlive::default()))
}

#[cfg(test)]
mod tests {
    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use crew_core::{Event, EventKind};
    use serde_json::{json, Value};
    use tower::ServiceExt;

    use crate::api;
    use crate::config::Config;
    use crate::state::AppState;

    /// Sends a raw body to `POST /channels/{channel}/messages`, returning the status
    /// and JSON response.
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

    /// Reads the stored event log back through `GET /events`.
    async fn stored_events(state: &AppState) -> Vec<Event> {
        let request = Request::builder()
            .uri("/events")
            .body(Body::empty())
            .unwrap();
        let response = api::build(state.clone()).oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        serde_json::from_value(value["events"].clone()).unwrap()
    }

    /// One valid `(channel, body)` per `MessageKind`, with its per-kind fields and no
    /// broker-owned field (the channel travels in the path).
    fn one_of_each_kind() -> Vec<(&'static str, Value)> {
        let backend = json!({ "kind": "role", "id": "backend" });
        vec![
            (
                "all-units",
                json!({ "from": backend, "kind": "order",
                    "title": "Ship it", "scope": "here", "owned_paths": ["src"],
                    "acceptance": "green", "body": "detail" }),
            ),
            (
                "@backend",
                json!({ "from": backend, "kind": "question",
                    "options": ["a", "b"], "body": "which?" }),
            ),
            (
                "@backend",
                json!({ "from": backend, "kind": "answer",
                    "in_reply_to": "11111111-1111-1111-1111-111111111111", "body": "a" }),
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

        // Read the log back and confirm every posted event survived intact.
        assert_eq!(
            stored_events(&state).await,
            posted,
            "the API must round-trip every message kind"
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
            | EventKind::Approval(_) => {
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
        // The SSE body is open-ended, so assert on the response head without reading it.
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
