//! A role's live event streams over Server-Sent Events: its inbox, and its
//! timeline.
//!
//! `GET /inbox?role=<role>` delivers the events **addressed to** a role: its
//! direct `@role` channel, any pair channel it belongs to, and `all-units`. The
//! role's own messages are filtered out at the source, so the old tail
//! self-echo hack is gone by construction, not by convention. This is the
//! delivery stream: what a role must act on.
//!
//! `GET /activity?agent=<role>` delivers the role's full **activity timeline**
//! (issue #30): what it sent (messages, its lifecycle, its activity) plus what
//! it received. Unlike the inbox it is not self-filtered, since a timeline is
//! what the role does as well as what reaches it. It is the live counterpart of
//! `GET /history?agent=<role>`.
//!
//! Both build on the shared replay-then-live SSE engine ([`crate::sse`]), which
//! the aggregate `GET /stream` uses too (issue #134), parameterized by a
//! per-event predicate: the inbox keeps the events addressed to a role, the
//! timeline keeps a role's whole timeline. Each event carries its log sequence
//! as the SSE `id`, so a reconnecting client resumes from `Last-Event-ID`
//! without loss.
//!
//! Channel membership is the canonical [`crew_core::Channel::addresses`], the
//! same test [`Event::in_timeline_of`] applies, so the inbox holds no second
//! source of truth for who a channel reaches.

use std::convert::Infallible;

use axum::{
    extract::{Query, State},
    http::HeaderMap,
    response::sse::{Event as SseEvent, Sse},
    routing::get,
    Router,
};
use crew_core::{Channel, Event, RoleId, Sender};
use serde::Deserialize;
use tokio_stream::Stream;
use tracing::{event, Level};

use crate::{error::ApiError, state::AppState};

/// The per-role stream routes: the self-filtered inbox and the full activity
/// timeline.
pub(crate) fn routes() -> Router<AppState> {
    Router::new()
        .route("/inbox", get(inbox))
        .route("/activity", get(activity))
}

/// The query of `GET /inbox`: which role's inbox to stream.
#[derive(Debug, Deserialize)]
struct InboxQuery {
    /// The role whose inbox to subscribe to.
    role: Option<String>,
}

/// The query of `GET /activity`: which role's activity timeline to stream.
#[derive(Debug, Deserialize)]
struct ActivityQuery {
    /// The role whose timeline to subscribe to.
    agent: Option<String>,
}

/// A per-event predicate deciding whether a role's stream keeps an event.
type Keep = fn(&Event, &RoleId) -> bool;

/// `GET /inbox?role=<role>`: stream the events addressed to a role over SSE.
///
/// Delivers direct (`@role`), pair, and `all-units` events, filtering out the
/// role's own messages. On reconnect the client's `Last-Event-ID` resumes the
/// stream right after the last event it received, replaying anything it missed
/// from the log before switching to the live tail, so no addressed event is
/// lost. A fresh connection with no cursor starts at the live tail rather than
/// replaying the whole history.
///
/// # Errors
/// Returns a 400 [`ApiError`] if the `role` query parameter is missing or
/// empty.
async fn inbox(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<InboxQuery>,
) -> Result<Sse<impl Stream<Item = Result<SseEvent, Infallible>>>, ApiError> {
    let role = require_role(query.role.as_deref(), "role")?;
    Ok(role_stream(&state, &headers, role, event_reaches_role))
}

/// `GET /activity?agent=<role>`: stream a role's full activity timeline over
/// SSE.
///
/// Delivers the role's own events (messages it sent, its lifecycle, its
/// activity) and the messages addressed to it. Unlike the inbox it is not
/// self-filtered, since a timeline is what the role does as well as what
/// reaches it. It resumes from `Last-Event-ID` and starts a fresh connection at
/// the live tail, exactly as the inbox does. This is the live counterpart of
/// `GET /history?agent=<role>`.
///
/// # Errors
/// Returns a 400 [`ApiError`] if the `agent` query parameter is missing or
/// empty.
async fn activity(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<ActivityQuery>,
) -> Result<Sse<impl Stream<Item = Result<SseEvent, Infallible>>>, ApiError> {
    let role = require_role(query.agent.as_deref(), "agent")?;
    Ok(role_stream(&state, &headers, role, Event::in_timeline_of))
}

/// Validates the required role-naming query parameter, rejecting a missing or
/// blank one.
fn require_role(value: Option<&str>, param: &str) -> Result<RoleId, ApiError> {
    let role = value.unwrap_or_default().trim();
    if role.is_empty() {
        return Err(ApiError::bad_request(format!(
            "the `{param}` query parameter is required"
        )));
    }
    Ok(RoleId::new(role))
}

/// Streams a role's slice of the log over the shared SSE engine: replay the
/// backlog it kept after the `Last-Event-ID` cursor, then the live tail.
///
/// `keep` decides which events reach this role's stream, so the inbox
/// (addressed to the role) and the activity timeline (the role's whole
/// timeline) share one replay-and-live implementation with the aggregate
/// `GET /stream` (see [`crate::sse`]).
fn role_stream(
    state: &AppState,
    headers: &HeaderMap,
    role: RoleId,
    keep: Keep,
) -> Sse<impl Stream<Item = Result<SseEvent, Infallible>>> {
    let lagged_role = role.clone();
    crate::sse::resume_stream(
        state,
        headers,
        move |event| keep(event, &role),
        move |skipped| warn_lagged(&lagged_role, skipped),
    )
}

/// Logs a per-role stream subscriber that lagged off the broadcast (issue
/// #116).
///
/// Observability is a first-class output, so the lag is named
/// (`broker.inbox.lagged`, with the role and skipped count) rather than dropped
/// silently. The gap itself is skipped: the client recovers it from its
/// `Last-Event-ID` on reconnect, so nothing is lost, but a recurring lag tells
/// the operator the broadcast capacity is too small under load.
fn warn_lagged(role: &RoleId, skipped: u64) {
    event!(
        name: "broker.inbox.lagged",
        Level::WARN,
        crew.role = %role,
        skipped,
        "inbox subscriber for `{{crew.role}}` lagged off the broadcast and skipped \
         {{skipped}} events; the client replays them from Last-Event-ID. Raise the \
         broadcast capacity if this recurs under load.",
    );
}

/// Whether `event` should be delivered to `role`'s inbox.
///
/// True when the event's channel addresses the role and the role is not the
/// sender: a role never receives its own messages. Membership is the canonical
/// [`Channel::addresses`], so the inbox and [`Event::in_timeline_of`] agree on
/// who a channel reaches; an unrecognized channel name parses to `None` and
/// reaches no one.
fn event_reaches_role(event: &Event, role: &RoleId) -> bool {
    if let Sender::Role(from) = &event.from {
        if from == role {
            return false;
        }
    }
    Channel::parse(event.channel.as_str()).is_some_and(|channel| channel.addresses(role))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use axum::{
        body::{to_bytes, Body},
        http::{Request, StatusCode},
    };
    use crew_core::{ChannelId, Event, EventKind, Message, MessageId, MessageKind, RoleId, Sender};
    use serde_json::{json, Value};
    use tokio_stream::StreamExt;
    use tower::ServiceExt;

    use super::event_reaches_role;
    use crate::{api, config::Config, state::AppState};

    fn role(name: &str) -> RoleId {
        RoleId::new(name)
    }

    /// A message event on `channel` from role `from`, for the addressing unit
    /// tests.
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

    // These pin the inbox's delivery predicate for each channel kind. Addressing
    // itself is the canonical `Channel::addresses` (tested in crew-core); here we
    // confirm `event_reaches_role` routes each kind through it. The sender differs
    // from every addressee, so the self-filter never masks the addressing.
    #[test]
    fn all_units_reaches_every_role() {
        let all = message_from("commander", "all-units");
        assert!(event_reaches_role(&all, &role("backend")));
        assert!(event_reaches_role(&all, &role("frontend")));
    }

    #[test]
    fn a_direct_channel_reaches_only_its_role() {
        let direct = message_from("commander", "@backend");
        assert!(event_reaches_role(&direct, &role("backend")));
        assert!(!event_reaches_role(&direct, &role("frontend")));
    }

    #[test]
    fn a_pair_channel_reaches_both_members_and_no_one_else() {
        let pair = message_from("commander", "frontend+backend");
        assert!(event_reaches_role(&pair, &role("frontend")));
        assert!(event_reaches_role(&pair, &role("backend")));
        assert!(!event_reaches_role(&pair, &role("qa")));
    }

    #[test]
    fn an_unknown_channel_reaches_no_one() {
        let other = message_from("commander", "random");
        assert!(!event_reaches_role(&other, &role("backend")));
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

    /// Posts a note from `from` to `channel`, asserting it is accepted. The
    /// channel travels in the path, so it is not a body field.
    async fn post(state: &AppState, from: &str, channel: &str, body: &str) {
        let message = json!({
            "from": { "kind": "role", "id": from },
            "kind": "note",
            "body": body,
        });
        let request = Request::builder()
            .method("POST")
            .uri(format!("/channels/{channel}/messages"))
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

    /// Opens `role`'s activity timeline stream (`GET /activity?agent=<role>`).
    async fn open_activity(state: &AppState, role: &str) -> axum::response::Response {
        let request = Request::builder()
            .uri(format!("/activity?agent={role}"))
            .body(Body::empty())
            .unwrap();
        api::build(state.clone()).oneshot(request).await.unwrap()
    }

    /// Reads up to `want` Server-Sent Events `(id, data)` from a body, giving
    /// up after a short budget since the live tail never closes the stream.
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

    /// Drains whole SSE blocks (double-newline separated) from `buffer` into
    /// `events`.
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
    async fn the_activity_stream_carries_the_role_s_own_and_received_events() {
        let state = AppState::new(Config::default());

        // A fresh connection starts at the live tail, so it sees only events posted
        // after it opens. Unlike the inbox, the timeline keeps the role's own message.
        let activity = open_activity(&state, "backend").await;
        assert_eq!(activity.status(), StatusCode::OK);
        assert_eq!(
            activity
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok()),
            Some("text/event-stream"),
        );
        let body = activity.into_body();

        post(&state, "backend", "all-units", "i am working").await; // seq 0, its own
        post(&state, "frontend", "@backend", "please build").await; // seq 1, received
        post(&state, "frontend", "@qa", "not yours").await; // seq 2, not addressed to it

        let events = read_events(body, 2).await;
        let ids: Vec<u64> = events.iter().map(|(id, _)| *id).collect();
        assert_eq!(
            ids,
            vec![0, 1],
            "its own message is kept (not self-filtered) and a peer's private one is not",
        );
        assert_eq!(body_of(&events[0].1), "i am working");
        assert_eq!(body_of(&events[1].1), "please build");
    }

    #[tokio::test]
    async fn an_activity_stream_without_an_agent_is_a_typed_400() {
        let state = AppState::new(Config::default());
        for uri in ["/activity", "/activity?agent=", "/activity?agent=%20"] {
            let request = Request::builder().uri(uri).body(Body::empty()).unwrap();
            let response = api::build(state.clone()).oneshot(request).await.unwrap();
            assert_eq!(
                response.status(),
                StatusCode::BAD_REQUEST,
                "{uri} should be 400"
            );
            let bytes = to_bytes(response.into_body(), usize::MAX).await.unwrap();
            let value: Value = serde_json::from_slice(&bytes).unwrap();
            assert!(value.get("error").is_some(), "typed error body for {uri}");
        }
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
