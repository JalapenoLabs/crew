//! The rolling-summary compaction behind `GET /history?summary=true`.
//!
//! A late joiner should not read the whole transcript to catch up, the failure mode
//! of the old file transport (see `docs/communication.md`). This module folds a slice
//! of older events into a [`HistorySummary`]: bounded aggregates (who spoke, of what
//! kind, on which lifecycle transitions) plus a capped digest of the most recent
//! orders and artifacts. The history handler pairs it with the raw recent tail, so the
//! cost of joining is bounded no matter how long the conversation has run.
//!
//! The summary is a deterministic projection of the typed event stream, not an LLM
//! rendering: the broker has no model, and typed events compact cleanly on their own.
//! A front-end or agent can render or expand the structured result.

use std::collections::BTreeMap;

use crew_core::{ArtifactKind, Event, EventKind, Lifecycle, MessageKind, Sender, Timestamp};
use serde::Serialize;

/// The most recent orders and artifacts named individually in a summary.
///
/// Older ones fold into the counts, so the summary stays bounded no matter how long
/// the log grows: raising this trades a larger response for more named recent detail.
const MAX_DIGEST_ITEMS: usize = 10;

/// A bounded compaction of older events: what a late joiner missed, in brief.
///
/// Every field is bounded independently of the event count: the tallies by the small
/// cardinality of senders, message kinds, and lifecycle states; the digests by
/// [`MAX_DIGEST_ITEMS`]. This is what keeps joining a long conversation cheap.
#[derive(Debug, Serialize)]
pub(crate) struct HistorySummary {
    /// How many older events this summary stands in for.
    pub event_count: usize,
    /// The earliest event timestamp covered, absent when nothing was summarized.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub since: Option<Timestamp>,
    /// The latest event timestamp covered (the boundary before the tail).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub through: Option<Timestamp>,
    /// A one-line, human-readable digest built from the aggregates below.
    pub headline: String,
    /// Message and event counts per sender, most active first.
    pub senders: Vec<Tally>,
    /// Message counts per kind (order, question, status, ...), most frequent first.
    pub message_kinds: Vec<Tally>,
    /// Lifecycle transition counts (started, idle, ...), omitted when there were none.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub lifecycle: Vec<Tally>,
    /// The most recent orders (handoffs), oldest first, capped at [`MAX_DIGEST_ITEMS`].
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub recent_orders: Vec<OrderDigest>,
    /// The most recent artifacts, oldest first, capped at [`MAX_DIGEST_ITEMS`].
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub recent_artifacts: Vec<ArtifactDigest>,
}

/// A name and how many events carried it (a sender, a message kind, a lifecycle state).
#[derive(Debug, Serialize)]
pub(crate) struct Tally {
    /// The name being counted.
    pub name: String,
    /// How many events carried it.
    pub count: usize,
}

/// A summarized order: the outstanding work a joiner should know about.
#[derive(Debug, Serialize)]
pub(crate) struct OrderDigest {
    /// The order's short title.
    pub title: String,
    /// The channel it was addressed to.
    pub channel: String,
    /// Who gave the order.
    pub from: String,
}

/// A summarized artifact: a produced thing the crew referenced.
#[derive(Debug, Serialize)]
pub(crate) struct ArtifactDigest {
    /// The reference (a branch name, a PR URL, a file path, or a route).
    pub reference: String,
    /// What the reference points to.
    pub artifact_kind: ArtifactKind,
}

/// Folds `events` (oldest first) into a bounded [`HistorySummary`].
///
/// Expects the events already ordered by time, as the history query returns them, so
/// `since` and `through` are the first and last, and the digests keep the most recent.
pub(crate) fn summarize(events: &[Event]) -> HistorySummary {
    let mut senders: BTreeMap<String, usize> = BTreeMap::new();
    let mut message_kinds: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut lifecycle: BTreeMap<&'static str, usize> = BTreeMap::new();
    let mut orders: Vec<OrderDigest> = Vec::new();
    let mut artifacts: Vec<ArtifactDigest> = Vec::new();

    for event in events {
        *senders.entry(sender_label(&event.from)).or_default() += 1;
        match &event.kind {
            EventKind::Message(message) => {
                *message_kinds
                    .entry(message_kind_label(&message.kind))
                    .or_default() += 1;
                match &message.kind {
                    MessageKind::Order { title, .. } => orders.push(OrderDigest {
                        title: title.clone(),
                        channel: event.channel.as_str().to_owned(),
                        from: sender_label(&event.from),
                    }),
                    MessageKind::Artifact {
                        reference,
                        artifact_kind,
                    } => artifacts.push(ArtifactDigest {
                        reference: reference.clone(),
                        artifact_kind: *artifact_kind,
                    }),
                    _ => {}
                }
            }
            EventKind::Lifecycle(state) => {
                *lifecycle.entry(lifecycle_label(*state)).or_default() += 1;
            }
            // Activity, boundary, verification, board, budget, and telemetry events are
            // their own projections (the activity timeline, `history?kind=boundary`, the
            // done-gate, `GET /board`, `history?kind=budget`, and `GET /stats`), so the
            // message/lifecycle summary skips them.
            EventKind::Activity(_)
            | EventKind::Boundary(_)
            | EventKind::Verification(_)
            | EventKind::Board(_)
            | EventKind::Budget(_)
            | EventKind::Telemetry(_) => {}
        }
    }

    // Snapshot the totals for the headline before the maps and vecs are consumed.
    let messages: usize = message_kinds.values().sum();
    let lifecycle_total: usize = lifecycle.values().sum();
    let order_count = orders.len();
    let artifact_count = artifacts.len();

    HistorySummary {
        event_count: events.len(),
        since: events.first().map(|event| event.ts),
        through: events.last().map(|event| event.ts),
        headline: headline(
            events.len(),
            messages,
            senders.len(),
            order_count,
            artifact_count,
            lifecycle_total,
        ),
        senders: ranked(senders),
        message_kinds: ranked(message_kinds),
        lifecycle: ranked(lifecycle),
        recent_orders: keep_recent(orders),
        recent_artifacts: keep_recent(artifacts),
    }
}

/// Ranks a count map into tallies, most frequent first, ties broken by name.
///
/// The name tiebreak makes the order deterministic, so the response is stable across
/// requests over the same events.
fn ranked<K: Into<String>>(counts: impl IntoIterator<Item = (K, usize)>) -> Vec<Tally> {
    let mut tallies: Vec<Tally> = counts
        .into_iter()
        .map(|(name, count)| Tally {
            name: name.into(),
            count,
        })
        .collect();
    tallies.sort_by(|a, b| b.count.cmp(&a.count).then_with(|| a.name.cmp(&b.name)));
    tallies
}

/// Keeps the most recent [`MAX_DIGEST_ITEMS`], dropping older ones from the front.
///
/// Events arrive oldest first, so the most recent items sit at the tail.
fn keep_recent<T>(mut items: Vec<T>) -> Vec<T> {
    if items.len() > MAX_DIGEST_ITEMS {
        items.drain(..items.len() - MAX_DIGEST_ITEMS);
    }
    items
}

/// Builds the one-line headline, naming only the non-empty categories.
fn headline(
    event_count: usize,
    messages: usize,
    senders: usize,
    orders: usize,
    artifacts: usize,
    lifecycle: usize,
) -> String {
    if event_count == 0 {
        return "No earlier events to summarize.".to_owned();
    }
    let mut clauses = vec![
        plural(messages, "message", "messages"),
        plural(senders, "sender", "senders"),
    ];
    if orders > 0 {
        clauses.push(plural(orders, "order", "orders"));
    }
    if artifacts > 0 {
        clauses.push(plural(artifacts, "artifact", "artifacts"));
    }
    if lifecycle > 0 {
        clauses.push(plural(lifecycle, "lifecycle change", "lifecycle changes"));
    }
    format!(
        "{} summarized: {}.",
        plural(event_count, "earlier event", "earlier events"),
        clauses.join(", "),
    )
}

/// `"1 order"` or `"3 orders"`: the count with its singular or plural noun.
fn plural(count: usize, one: &str, many: &str) -> String {
    format!("{count} {}", if count == 1 { one } else { many })
}

/// A sender's label: a role's id, or `general` for the human.
fn sender_label(from: &Sender) -> String {
    match from {
        Sender::Role(role) => role.as_str().to_owned(),
        Sender::General => "general".to_owned(),
    }
}

/// The wire label for a message's typed intent (matching its serde name).
fn message_kind_label(kind: &MessageKind) -> &'static str {
    match kind {
        MessageKind::Order { .. } => "order",
        MessageKind::Question { .. } => "question",
        MessageKind::Answer => "answer",
        MessageKind::Status => "status",
        MessageKind::Artifact { .. } => "artifact",
        MessageKind::Note => "note",
        MessageKind::Redirect => "redirect",
        MessageKind::Belay => "belay",
    }
}

/// The wire label for a lifecycle transition (matching its serde name).
fn lifecycle_label(state: Lifecycle) -> &'static str {
    match state {
        Lifecycle::Started => "started",
        Lifecycle::Idle => "idle",
        Lifecycle::Stopped => "stopped",
        Lifecycle::Restarted => "restarted",
        Lifecycle::Died => "died",
        Lifecycle::Recovered => "recovered",
        Lifecycle::Paused => "paused",
        Lifecycle::Resumed => "resumed",
        Lifecycle::StoodDown => "stood_down",
    }
}

#[cfg(test)]
mod tests {
    use super::{summarize, MAX_DIGEST_ITEMS};
    use crew_core::{
        ArtifactKind, ChannelId, Event, EventKind, Lifecycle, Message, MessageId, MessageKind,
        RoleId, Sender, Timestamp,
    };

    fn ts(seconds: u32) -> Timestamp {
        serde_json::from_str(&format!("\"2020-01-01T00:00:{seconds:02}Z\"")).unwrap()
    }

    fn event(from: Sender, channel: &str, at: Timestamp, kind: MessageKind) -> Event {
        Event {
            ts: at,
            from,
            channel: ChannelId::new(channel),
            task: None,
            kind: EventKind::Message(Message {
                id: MessageId::new(),
                kind,
                body: String::new(),
            }),
        }
    }

    fn note(role: &str, at: Timestamp) -> Event {
        event(
            Sender::Role(RoleId::new(role)),
            "all-units",
            at,
            MessageKind::Note,
        )
    }

    fn order(role: &str, channel: &str, at: Timestamp, title: &str) -> Event {
        event(
            Sender::Role(RoleId::new(role)),
            channel,
            at,
            MessageKind::Order {
                title: title.to_owned(),
                scope: String::new(),
                owned_paths: Vec::new(),
                acceptance: String::new(),
            },
        )
    }

    #[test]
    fn an_empty_slice_summarizes_nothing() {
        let summary = summarize(&[]);
        assert_eq!(summary.event_count, 0);
        assert!(summary.since.is_none() && summary.through.is_none());
        assert_eq!(summary.headline, "No earlier events to summarize.");
        assert!(summary.senders.is_empty());
    }

    #[test]
    fn it_counts_senders_and_kinds_and_spans_the_time_range() {
        let events = vec![
            note("backend", ts(1)),
            note("backend", ts(2)),
            note("frontend", ts(3)),
            order("commander", "@backend", ts(4), "Build the endpoint"),
        ];
        let summary = summarize(&events);

        assert_eq!(summary.event_count, 4);
        assert_eq!(summary.since, Some(ts(1)));
        assert_eq!(summary.through, Some(ts(4)));

        // Senders rank by count, so backend (2) leads frontend and commander (1 each).
        assert_eq!(summary.senders[0].name, "backend");
        assert_eq!(summary.senders[0].count, 2);

        let notes = summary
            .message_kinds
            .iter()
            .find(|tally| tally.name == "note")
            .unwrap();
        assert_eq!(notes.count, 3);
        assert_eq!(summary.recent_orders.len(), 1);
        assert_eq!(summary.recent_orders[0].title, "Build the endpoint");
        assert_eq!(summary.recent_orders[0].channel, "@backend");
        assert!(summary.headline.contains("4 earlier events"));
    }

    #[test]
    fn it_summarizes_lifecycle_and_artifacts() {
        let lifecycle = Event {
            kind: EventKind::Lifecycle(Lifecycle::Started),
            ..note("backend", ts(1))
        };
        let artifact = event(
            Sender::Role(RoleId::new("backend")),
            "all-units",
            ts(2),
            MessageKind::Artifact {
                reference: "https://example.test/pr/1".to_owned(),
                artifact_kind: ArtifactKind::PullRequest,
            },
        );
        let summary = summarize(&[lifecycle, artifact]);

        assert_eq!(
            summary
                .lifecycle
                .iter()
                .find(|t| t.name == "started")
                .unwrap()
                .count,
            1
        );
        assert_eq!(summary.recent_artifacts.len(), 1);
        assert_eq!(
            summary.recent_artifacts[0].reference,
            "https://example.test/pr/1"
        );
        assert_eq!(
            summary.recent_artifacts[0].artifact_kind,
            ArtifactKind::PullRequest
        );
        assert!(summary.headline.contains("1 artifact"));
        assert!(summary.headline.contains("1 lifecycle change"));
    }

    #[test]
    fn the_digests_keep_only_the_most_recent_items_bounded() {
        // Twice the cap, so half must be dropped and only the newest kept.
        let events: Vec<Event> = (0..MAX_DIGEST_ITEMS * 2)
            .map(|i| {
                let at = ts(u32::try_from(i).unwrap());
                order("commander", "@backend", at, &format!("order {i}"))
            })
            .collect();
        let summary = summarize(&events);

        assert_eq!(
            summary.recent_orders.len(),
            MAX_DIGEST_ITEMS,
            "digest is bounded"
        );
        assert_eq!(
            summary.recent_orders.last().unwrap().title,
            format!("order {}", MAX_DIGEST_ITEMS * 2 - 1),
            "the newest order is kept",
        );
        assert_eq!(
            summary.recent_orders.first().unwrap().title,
            format!("order {MAX_DIGEST_ITEMS}"),
            "older orders past the cap are dropped",
        );
    }
}
