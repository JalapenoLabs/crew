//! The inbox endpoint: a role's live, self-filtered event stream.
//!
//! `GET /inbox?role=<role>` delivers the events addressed to a role over
//! Server-Sent Events: its direct `@role` channel, any pair channel it belongs to,
//! and `all-units`. The role's own messages are filtered out at the source, so the
//! old tail self-echo hack is gone by construction, not by convention. Each event
//! carries its log sequence as the SSE `id`, so a reconnecting client resumes from
//! its `Last-Event-ID` without loss.
//!
//! The channel-naming model here is the minimal resolution the inbox needs; issue
//! #11 owns the canonical channel model and membership.

use std::convert::Infallible;

use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::routing::get;
use axum::Router;
use crew_core::{ChannelId, Event, RoleId, Sender};
use serde::Deserialize;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{Stream, StreamExt};

use crate::error::ApiError;
use crate::state::{AppState, Sequenced};

/// The channel that reaches every live role (see `docs/communication.md`).
const ALL_UNITS: &str = "all-units";

/// The inbox route: `GET /inbox?role=<role>` (subscribe to a role's event stream).
pub(crate) fn routes() -> Router<AppState> {
    Router::new().route("/inbox", get(inbox))
}

/// The query of `GET /inbox`: which role's inbox to stream.
#[derive(Debug, Deserialize)]
struct InboxQuery {
    /// The role whose inbox to subscribe to.
    role: Option<String>,
}

/// `GET /inbox?role=<role>`: stream the events addressed to a role over SSE.
///
/// Delivers direct (`@role`), pair, and `all-units` events, filtering out the
/// role's own messages. On reconnect the client's `Last-Event-ID` resumes the
/// stream right after the last event it received, replaying anything it missed from
/// the log before switching to the live tail, so no addressed event is lost. A
/// fresh connection with no cursor starts at the live tail rather than replaying the
/// whole history.
///
/// # Errors
/// Returns a 400 [`ApiError`] if the `role` query parameter is missing or empty.
async fn inbox(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<InboxQuery>,
) -> Result<Sse<impl Stream<Item = Result<SseEvent, Infallible>>>, ApiError> {
    let role = query.role.unwrap_or_default();
    let role = role.trim();
    if role.is_empty() {
        return Err(ApiError::bad_request(
            "the `role` query parameter is required",
        ));
    }
    let role = RoleId::new(role);

    // Subscribe before snapshotting the log, so an event appended while we read the
    // backlog is buffered on the receiver and delivered live rather than missed.
    let receiver = state.broadcast.subscribe();
    let backlog = state.storage.events();
    let live_from = backlog.len() as u64;

    // A reconnect resumes right after its last delivered event; a fresh connection
    // (no cursor) starts at the live tail.
    let resume_from = last_event_id(&headers).map_or(live_from, |id| id + 1);

    let replay: Vec<Result<SseEvent, Infallible>> = backlog
        .into_iter()
        .enumerate()
        .filter_map(|(index, event)| {
            let seq = index as u64;
            if seq >= resume_from && event_reaches_role(&event, &role) {
                to_sse(seq, &event).map(Ok)
            } else {
                None
            }
        })
        .collect();

    let live = BroadcastStream::new(receiver).filter_map(move |result| {
        // A lagged receiver skips the gap here; the client replays it on reconnect.
        let Ok(Sequenced { seq, event }) = result else {
            return None;
        };
        // Only events after the snapshot; earlier ones are already in `replay`.
        if seq >= live_from && event_reaches_role(&event, &role) {
            to_sse(seq, &event).map(Ok)
        } else {
            None
        }
    });

    let stream = tokio_stream::iter(replay).chain(live);
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

/// Parses the `Last-Event-ID` reconnect cursor from the request headers.
///
/// Returns `None` when the header is absent or not a sequence number, so a client
/// with no valid cursor starts from the live tail.
fn last_event_id(headers: &HeaderMap) -> Option<u64> {
    headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse().ok())
}

/// Whether `event` should be delivered to `role`'s inbox.
///
/// True when the event's channel addresses the role and the role is not the sender:
/// a role never receives its own messages.
fn event_reaches_role(event: &Event, role: &RoleId) -> bool {
    if let Sender::Role(from) = &event.from {
        if from == role {
            return false;
        }
    }
    channel_addresses_role(&event.channel, role)
}

/// Whether a channel addresses `role`, by the naming model in `docs/communication.md`.
///
/// `all-units` reaches every role, `@role` is a direct point-to-point channel, and a
/// `a+b` pair channel reaches its two named members. Any other name reaches no one
/// until issue #11 lands the canonical channel model.
fn channel_addresses_role(channel: &ChannelId, role: &RoleId) -> bool {
    let channel = channel.as_str();
    let role = role.as_str();
    if channel == ALL_UNITS {
        return true;
    }
    if let Some(direct) = channel.strip_prefix('@') {
        return direct == role;
    }
    if let Some((first, second)) = channel.split_once('+') {
        return first == role || second == role;
    }
    false
}

/// Renders an event as a Server-Sent Event carrying its sequence as the `id`.
///
/// Returns `None` only if the event fails to serialize, which cannot happen for a
/// well-formed [`Event`]; such an event is skipped rather than closing the stream.
fn to_sse(seq: u64, event: &Event) -> Option<SseEvent> {
    SseEvent::default()
        .id(seq.to_string())
        .json_data(event)
        .ok()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::body::{to_bytes, Body};
    use axum::http::{Request, StatusCode};
    use crew_core::{ChannelId, Event, EventKind, Message, MessageId, MessageKind, RoleId, Sender};
    use serde_json::{json, Value};
    use tokio_stream::StreamExt;
    use tower::ServiceExt;

    use super::{channel_addresses_role, event_reaches_role};
    use crate::api;
    use crate::config::Config;
    use crate::state::AppState;

    fn role(name: &str) -> RoleId {
        RoleId::new(name)
    }

    /// A message event on `channel` from role `from`, for the addressing unit tests.
    fn message_from(from: &str, channel: &str) -> Event {
        Event {
            ts: crew_core::Timestamp::now(),
            from: Sender::Role(RoleId::new(from)),
            channel: ChannelId::new(channel),
            task: None,
            kind: EventKind::Message(Message {
                id: MessageId::new(),
                kind: MessageKind::Note,
                body: String::new(),
            }),
        }
    }

    #[test]
    fn all_units_reaches_every_role() {
        let all = ChannelId::new("all-units");
        assert!(channel_addresses_role(&all, &role("backend")));
        assert!(channel_addresses_role(&all, &role("frontend")));
    }

    #[test]
    fn a_direct_channel_reaches_only_its_role() {
        let direct = ChannelId::new("@backend");
        assert!(channel_addresses_role(&direct, &role("backend")));
        assert!(!channel_addresses_role(&direct, &role("frontend")));
    }

    #[test]
    fn a_pair_channel_reaches_both_members_and_no_one_else() {
        let pair = ChannelId::new("frontend+backend");
        assert!(channel_addresses_role(&pair, &role("frontend")));
        assert!(channel_addresses_role(&pair, &role("backend")));
        assert!(!channel_addresses_role(&pair, &role("qa")));
    }

    #[test]
    fn an_unknown_channel_reaches_no_one() {
        let other = ChannelId::new("random");
        assert!(!channel_addresses_role(&other, &role("backend")));
    }

    #[test]
    fn a_role_never_receives_its_own_message() {
        let own = message_from("backend", "all-units");
        assert!(!event_reaches_role(&own, &role("backend")));
        assert!(event_reaches_role(&own, &role("frontend")));
    }

    #[test]
    fn the_general_is_never_filtered_as_self() {
        let from_general = Event {
            from: Sender::General,
            ..message_from("backend", "all-units")
        };
        assert!(event_reaches_role(&from_general, &role("backend")));
    }

    /// Posts a note from `from` to `channel`, asserting it is accepted.
    async fn post(state: &AppState, from: &str, channel: &str, body: &str) {
        let message = json!({
            "from": { "kind": "role", "id": from },
            "channel": channel,
            "kind": "note",
            "body": body,
        });
        let request = Request::builder()
            .method("POST")
            .uri("/events")
            .header("content-type", "application/json")
            .body(Body::from(serde_json::to_vec(&message).unwrap()))
            .unwrap();
        let response = api::build(state.clone()).oneshot(request).await.unwrap();
        assert_eq!(
            response.status(),
            StatusCode::CREATED,
            "post {body} should succeed"
        );
    }

    /// Opens `role`'s inbox, optionally resuming from a `Last-Event-ID`.
    async fn open_inbox(
        state: &AppState,
        role: &str,
        last_event_id: Option<&str>,
    ) -> axum::response::Response {
        let mut request = Request::builder().uri(format!("/inbox?role={role}"));
        if let Some(id) = last_event_id {
            request = request.header("last-event-id", id);
        }
        api::build(state.clone())
            .oneshot(request.body(Body::empty()).unwrap())
            .await
            .unwrap()
    }

    /// Reads up to `want` Server-Sent Events `(id, data)` from a body, giving up
    /// after a short budget since the live tail never closes the stream.
    async fn read_events(body: Body, want: usize) -> Vec<(u64, Value)> {
        let mut stream = body.into_data_stream();
        let mut buffer: Vec<u8> = Vec::new();
        let mut events = Vec::new();
        while events.len() < want {
            match tokio::time::timeout(Duration::from_millis(500), stream.next()).await {
                Ok(Some(Ok(chunk))) => {
                    buffer.extend_from_slice(&chunk);
                    drain_events(&mut buffer, &mut events);
                }
                _ => break, // timeout, end of stream, or a read error
            }
        }
        events
    }

    /// Drains whole SSE blocks (double-newline separated) from `buffer` into `events`.
    fn drain_events(buffer: &mut Vec<u8>, events: &mut Vec<(u64, Value)>) {
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

    /// The `body` field of a delivered message event.
    fn body_of(event: &Value) -> &str {
        event["kind"]["data"]["body"].as_str().unwrap_or_default()
    }

    #[tokio::test]
    async fn a_fresh_inbox_streams_live_messages_and_never_the_role_s_own() {
        let state = AppState::new(Config::default());
        // Posted before the subscription; a fresh connect must not replay it.
        post(&state, "frontend", "all-units", "before").await;

        let inbox = open_inbox(&state, "backend", None).await;
        assert_eq!(inbox.status(), StatusCode::OK);
        assert_eq!(
            inbox
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok()),
            Some("text/event-stream"),
        );
        let body = inbox.into_body();

        // Post after the connection is open: a peer broadcast, the role's own (must
        // be filtered), and a direct message.
        post(&state, "frontend", "all-units", "hello").await; // seq 1
        post(&state, "backend", "all-units", "mine").await; // seq 2, self-echo
        post(&state, "frontend", "@backend", "direct").await; // seq 3

        let events = read_events(body, 2).await;
        let ids: Vec<u64> = events.iter().map(|(id, _)| *id).collect();
        assert_eq!(
            ids,
            vec![1, 3],
            "the role's own message (seq 2) is filtered"
        );
        assert_eq!(body_of(&events[0].1), "hello");
        assert_eq!(body_of(&events[1].1), "direct");
    }

    #[tokio::test]
    async fn a_reconnect_resumes_from_last_event_id_without_loss() {
        let state = AppState::new(Config::default());
        post(&state, "frontend", "all-units", "a").await; // seq 0, delivered
        post(&state, "backend", "all-units", "self").await; // seq 1, self-echo
        post(&state, "frontend", "@backend", "b").await; // seq 2, delivered
        post(&state, "qa", "@frontend", "notmine").await; // seq 3, not addressed to backend
        post(&state, "frontend", "all-units", "c").await; // seq 4, delivered

        // Reconnect as backend having last seen event 0: replay only later addressed,
        // non-self events, skipping the self-echo (1) and the unaddressed one (3).
        let inbox = open_inbox(&state, "backend", Some("0")).await;
        let events = read_events(inbox.into_body(), 2).await;

        let ids: Vec<u64> = events.iter().map(|(id, _)| *id).collect();
        assert_eq!(
            ids,
            vec![2, 4],
            "resumes after the cursor, keeping the filter"
        );
        assert_eq!(body_of(&events[0].1), "b");
        assert_eq!(body_of(&events[1].1), "c");
    }

    #[tokio::test]
    async fn an_inbox_without_a_role_is_a_typed_400() {
        let state = AppState::new(Config::default());
        for uri in ["/inbox", "/inbox?role=", "/inbox?role=%20"] {
            let request = Request::builder().uri(uri).body(Body::empty()).unwrap();
            let response = api::build(state.clone()).oneshot(request).await.unwrap();
            assert_eq!(
                response.status(),
                StatusCode::BAD_REQUEST,
                "{uri} should be 400"
            );
            let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let value: Value = serde_json::from_slice(&bytes).unwrap();
            assert!(
                value.get("error").is_some(),
                "typed error body for {uri}: {value}"
            );
        }
    }
}
