//! The shared replay-then-live SSE engine for the live event views (issues #10,
//! #31, #134).
//!
//! `GET /inbox`, `GET /activity`, and the aggregate `GET /stream` all deliver a
//! live slice of the one event log, and all resume losslessly: on reconnect the
//! client's `Last-Event-ID` replays the events it missed from the log before
//! the stream switches to the live tail, so a dropped or lagged connection
//! needs no separate `/history` catch-up call. The views differ only in which
//! events they keep, so the subscribe-snapshot-replay-then-live machinery lives
//! here once, parameterized by a `keep` predicate: the per-role inbox supplies
//! a channel membership test, the activity timeline supplies
//! [`Event::in_timeline_of`], and the aggregate stream supplies
//! [`EventFilter::matches`](crate::store::EventFilter::matches).
//!
//! Each event carries its stable absolute sequence as the SSE `id` (issues
//! #201, #274), so a reconnecting client resumes by that sequence rather than a
//! shifting log position: a prune or compaction of the events between never
//! gaps or duplicates the resume, since the sequence is never reused or
//! renumbered. A subscriber that lags off the broadcast skips the gap here
//! rather than closing the stream (`on_lag` reports it); the client recovers
//! the gap from its `Last-Event-ID` on reconnect, so nothing is lost.

use std::convert::Infallible;

use axum::{
    http::HeaderMap,
    response::sse::{Event as SseEvent, KeepAlive, Sse},
};
use crew_core::Event;
use tokio_stream::{
    wrappers::{errors::BroadcastStreamRecvError, BroadcastStream},
    Stream, StreamExt,
};

use crate::{
    state::{AppState, Sequenced},
    store::StoredEvent,
};

/// Subscribes, replays the matching backlog after the `Last-Event-ID` cursor,
/// then streams the matching live events.
///
/// Subscribes before snapshotting the log, so an event appended while the
/// backlog is read is buffered on the receiver and delivered live rather than
/// missed. `keep` decides which events belong to this view. `on_lag` reports a
/// subscriber that fell off the broadcast, so each caller names the lag with
/// its own event; the gap itself is skipped, since the client replays it from
/// `Last-Event-ID`.
pub(crate) fn resume_stream(
    state: &AppState,
    headers: &HeaderMap,
    keep: impl Fn(&Event) -> bool + Send + 'static,
    on_lag: impl Fn(u64) + Send + 'static,
) -> Sse<impl Stream<Item = Result<SseEvent, Infallible>>> {
    let receiver = state.broadcast.subscribe();
    // The live tail: the sequence the next appended event will take. Read before
    // the backlog so any event that lands between the two reads has a sequence at
    // or above it, and is delivered live rather than duplicated in the replay
    // (the `seq < live_from` bound below).
    let live_from = state.storage.next_seq();

    // A reconnect resumes right after its last delivered event; a fresh connection
    // (no cursor) starts at the live tail.
    let resume_from = last_event_id(headers).map_or(live_from, |id| id + 1);

    // Read only the gap after the cursor, not the whole log (issue #225): a
    // reconnecting client that missed the last few events replays only those.
    // Each event carries its stable stored sequence, so a resume stays correct
    // across a prune of the events between (issue #201).
    let backlog = state.storage.events_since(resume_from);

    // Replay is materialized eagerly, so `keep` is only borrowed here before it is
    // moved into the live filter below.
    let replay: Vec<Result<SseEvent, Infallible>> = backlog
        .into_iter()
        .filter_map(|StoredEvent { seq, event }| {
            if seq < live_from && keep(&event) {
                to_sse(seq, &event).map(Ok)
            } else {
                None
            }
        })
        .collect();

    let live = BroadcastStream::new(receiver)
        .filter_map(move |result| map_live_event(result, live_from, &keep, &on_lag));

    let stream = tokio_stream::iter(replay).chain(live);
    Sse::new(stream).keep_alive(KeepAlive::default())
}

/// Maps one broadcast result to a live SSE item, surfacing a lag.
///
/// A [`Lagged`](BroadcastStreamRecvError::Lagged) receiver has fallen behind
/// the broadcast capacity and skipped events. The gap is reported through
/// `on_lag` (each view names it with its own event) and skipped here: the
/// client recovers it from `Last-Event-ID` on reconnect, so nothing is lost,
/// but a recurring lag tells the operator the broadcast capacity is too small
/// under load. Otherwise the event is delivered when it passes `keep` and is
/// newer than the pre-subscription snapshot (`live_from`); earlier ones are
/// already in the replay.
pub(crate) fn map_live_event(
    result: Result<Sequenced, BroadcastStreamRecvError>,
    live_from: u64,
    keep: &impl Fn(&Event) -> bool,
    on_lag: &impl Fn(u64),
) -> Option<Result<SseEvent, Infallible>> {
    let Sequenced { seq, event } = match result {
        Ok(sequenced) => sequenced,
        Err(BroadcastStreamRecvError::Lagged(skipped)) => {
            on_lag(skipped);
            return None;
        }
    };
    if seq >= live_from && keep(&event) {
        to_sse(seq, &event).map(Ok)
    } else {
        None
    }
}

/// Parses the `Last-Event-ID` reconnect cursor from the request headers.
///
/// Returns `None` when the header is absent or not a sequence number, so a
/// client with no valid cursor starts from the live tail.
fn last_event_id(headers: &HeaderMap) -> Option<u64> {
    headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.trim().parse().ok())
}

/// Renders an event as a Server-Sent Event carrying its sequence as the `id`.
///
/// Returns `None` only if the event fails to serialize, which cannot happen for
/// a well-formed [`Event`]; such an event is skipped rather than closing the
/// stream.
fn to_sse(seq: u64, event: &Event) -> Option<SseEvent> {
    SseEvent::default()
        .id(seq.to_string())
        .json_data(event)
        .ok()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use crew_core::{
        ChannelId, Event, EventKind, Message, MessageId, MessageKind, RoleId, Sender, Timestamp,
    };
    use tokio_stream::wrappers::errors::BroadcastStreamRecvError;

    use super::map_live_event;
    use crate::state::Sequenced;

    /// A minimal note event at log position `seq`, for the engine's unit tests.
    fn sequenced(seq: u64) -> Sequenced {
        Sequenced {
            seq,
            event: Event {
                ts: Timestamp::now(),
                from: Sender::Role(RoleId::new("backend")),
                channel: ChannelId::new("all-units"),
                task: None,
                kind: EventKind::Message(Message {
                    id: MessageId::new(),
                    kind: MessageKind::Note,
                    body: String::new(),
                }),
            },
        }
    }

    /// A `keep` that accepts every event, so a test isolates the sequence
    /// logic.
    fn keep_all(_event: &Event) -> bool {
        true
    }

    #[test]
    fn a_lagged_receiver_reports_the_gap_and_delivers_nothing() {
        // A lagged receiver has fallen behind the broadcast capacity; the gap is
        // reported through `on_lag` (each view names its own event) and skipped
        // here, since the client replays it from Last-Event-ID.
        let skipped = AtomicU64::new(0);
        let mapped = map_live_event(
            Err(BroadcastStreamRecvError::Lagged(7)),
            0,
            &keep_all,
            &|count| skipped.store(count, Ordering::Relaxed),
        );
        assert!(
            mapped.is_none(),
            "a lag skips the gap rather than delivering it out of order",
        );
        assert_eq!(
            skipped.load(Ordering::Relaxed),
            7,
            "the lag is reported so the caller can log it",
        );
    }

    #[test]
    fn a_live_event_is_delivered_only_after_the_snapshot_and_when_it_passes_keep() {
        let noop = |_skipped: u64| {};

        // At or after the live snapshot and kept: delivered.
        assert!(
            map_live_event(Ok(sequenced(3)), 0, &keep_all, &noop).is_some(),
            "a live event past the snapshot that passes keep is delivered",
        );

        // Before the snapshot (already in the replay): skipped, not delivered twice.
        assert!(
            map_live_event(Ok(sequenced(2)), 5, &keep_all, &noop).is_none(),
            "an event before the live snapshot is already in the replay",
        );

        // Rejected by keep: dropped even though it is live.
        assert!(
            map_live_event(Ok(sequenced(9)), 0, &|_event| false, &noop).is_none(),
            "an event the view does not keep is not delivered",
        );
    }
}
