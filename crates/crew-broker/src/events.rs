//! The event endpoints: posting a message and reading the log.

use axum::body::Bytes;
use axum::extract::{FromRequest, Request, State};
use axum::http::StatusCode;
use axum::routing::post;
use axum::{Json, Router};
use crew_core::{ChannelId, Event, EventKind, Message, MessageId, MessageKind, Sender, TaskId};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::error::ApiError;
use crate::state::AppState;

/// The event routes: `POST /events` (post a message) and `GET /events` (read the log).
pub(crate) fn routes() -> Router<AppState> {
    Router::new().route("/events", post(post_event).get(list_events))
}

/// A JSON body extractor that fails with a typed [`ApiError`] on malformed input.
///
/// Unlike axum's built-in `Json`, a bad body (invalid JSON, wrong types, an
/// unknown message kind, a missing per-kind field) yields a 400 with a
/// `{ "error": ... }` body instead of a plain-text rejection, and never a panic.
struct JsonBody<T>(T);

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

/// The body of `POST /events`: a message to post to a channel.
///
/// The broker stamps the id and timestamp; the client supplies who it is from, the
/// channel, an optional task, the typed kind with its per-kind fields (flattened),
/// and a body. For example an order posts as
/// `{"from":{"kind":"role","id":"backend"},"channel":"all-units","kind":"order",
/// "title":"..","scope":"..","owned_paths":[],"acceptance":"..","body":".."}`.
#[derive(Debug, Deserialize)]
struct PostMessage {
    /// Who is sending: a role, or the General.
    from: Sender,
    /// The channel to post to.
    channel: ChannelId,
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

impl PostMessage {
    /// Validates the fields the broker will not fix up (`channel` and `from`).
    ///
    /// The `kind` is validated structurally by deserialization; this catches the
    /// semantic gaps serde cannot: an empty channel or an empty role sender.
    fn validate(&self) -> Result<(), ApiError> {
        if self.channel.as_str().trim().is_empty() {
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

/// `POST /events`: post a message. The broker stamps its id and timestamp, stores
/// the resulting [`Event`], and returns it with `201 Created`.
///
/// # Errors
/// Returns a 400 [`ApiError`] if the body is malformed or fails validation.
async fn post_event(
    State(state): State<AppState>,
    JsonBody(request): JsonBody<PostMessage>,
) -> Result<(StatusCode, Json<Event>), ApiError> {
    request.validate()?;
    let event = Event {
        ts: crew_core::Timestamp::now(),
        from: request.from,
        channel: request.channel,
        task: request.task,
        kind: EventKind::Message(Message {
            id: MessageId::new(),
            kind: request.kind,
            body: request.body,
        }),
    };
    state.storage.append(event.clone());
    Ok((StatusCode::CREATED, Json(event)))
}

/// The body of `GET /events`: the stored event log, oldest first.
#[derive(Debug, Serialize)]
struct EventLog {
    events: Vec<Event>,
}

/// `GET /events`: read the whole event log (oldest first).
async fn list_events(State(state): State<AppState>) -> Json<EventLog> {
    Json(EventLog {
        events: state.storage.events(),
    })
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

    /// Sends a raw body to `POST /events`, returning the status and JSON response.
    async fn post_raw(state: &AppState, body: impl Into<Body>) -> (StatusCode, Value) {
        let request = Request::builder()
            .method("POST")
            .uri("/events")
            .header("content-type", "application/json")
            .body(body.into())
            .unwrap();
        let response = api::build(state.clone()).oneshot(request).await.unwrap();
        let status = response.status();
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (status, value)
    }

    async fn post(state: &AppState, message: Value) -> (StatusCode, Value) {
        post_raw(state, serde_json::to_vec(&message).unwrap()).await
    }

    /// One valid post body per `MessageKind`, with its per-kind fields.
    fn one_of_each_kind() -> Vec<Value> {
        let backend = json!({ "kind": "role", "id": "backend" });
        vec![
            json!({ "from": backend, "channel": "all-units", "kind": "order",
                    "title": "Ship it", "scope": "here", "owned_paths": ["src"],
                    "acceptance": "green", "body": "detail" }),
            json!({ "from": backend, "channel": "@backend", "kind": "question",
                    "options": ["a", "b"], "body": "which?" }),
            json!({ "from": backend, "channel": "@backend", "kind": "answer", "body": "a" }),
            json!({ "from": backend, "channel": "all-units", "kind": "status", "body": "working" }),
            json!({ "from": backend, "channel": "all-units", "kind": "artifact",
                    "reference": "feature/x", "artifact_kind": "branch", "body": "opened" }),
            json!({ "from": { "kind": "general" }, "channel": "all-units", "kind": "note",
                    "body": "fyi" }),
        ]
    }

    #[tokio::test]
    async fn every_message_kind_round_trips_through_the_api() {
        let state = AppState::new(Config::default());

        let mut posted = Vec::new();
        for message in one_of_each_kind() {
            let (status, value) = post(&state, message).await;
            assert_eq!(status, StatusCode::CREATED, "post should succeed: {value}");
            let event: Event = serde_json::from_value(value).unwrap();
            assert!(matches!(event.kind, EventKind::Message(_)));
            posted.push(event);
        }

        // Read the log back and confirm every posted event survived intact.
        let request = Request::builder()
            .uri("/events")
            .body(Body::empty())
            .unwrap();
        let response = api::build(state.clone()).oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();
        let read: Vec<Event> = serde_json::from_value(value["events"].clone()).unwrap();
        assert_eq!(read, posted, "the API must round-trip every message kind");
    }

    #[tokio::test]
    async fn malformed_events_return_a_typed_4xx_never_a_panic() {
        let state = AppState::new(Config::default());
        let backend = json!({ "kind": "role", "id": "backend" });

        // Structurally invalid JSON.
        let (status, value) = post_raw(&state, "{ not json").await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert!(value.get("error").is_some(), "typed error body: {value}");

        // Well-formed JSON that fails to model an event.
        let bad = [
            json!({ "channel": "all-units", "kind": "note" }), // missing `from`
            json!({ "from": backend, "kind": "note" }),        // missing `channel`
            json!({ "from": backend, "channel": "all-units", "kind": "bogus" }), // unknown kind
            json!({ "from": backend, "channel": "all-units", "kind": "order" }), // order missing fields
            json!({ "from": backend, "channel": "", "kind": "note" }),           // empty channel
            json!({ "from": { "kind": "role", "id": "" }, "channel": "all-units", "kind": "note" }),
        ];
        for message in bad {
            let (status, value) = post(&state, message.clone()).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "should reject {message}");
            assert!(
                value.get("error").is_some(),
                "typed error body for {message}: {value}"
            );
        }

        // A rejected post must not have been stored.
        let stored = state.storage.events();
        assert!(stored.is_empty(), "malformed posts must not be stored");
    }
}
