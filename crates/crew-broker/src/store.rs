//! The broker's event storage: a swappable [`Storage`] trait and its backends.
//!
//! [`Storage`] is the one surface the broker depends on; it never names a
//! concrete backend, so a durable log today can become `SQLite` or Postgres
//! later without touching a handler. Two backends ship now:
//!
//! - [`MemoryStore`] keeps everything in memory, for tests and ephemeral use.
//! - [`LogStore`] persists to an on-disk append-only log (JSON per line) with
//!   an in-memory index, so a restart replays the log and no event is lost. A
//!   dedicated writer thread does the disk I/O, so an append never blocks the
//!   async runtime, even under a burst (issue #206).
//!
//! The trait covers the whole persisted surface: [`append`](Storage::append) an
//! event (returning the stable sequence it is assigned),
//! [`flush`](Storage::flush) pending writes, [`query`](Storage::query) the log
//! with filters and a stable page cursor, read every event or only those after
//! a cursor ([`events_since`](Storage::events_since), so an SSE replay is
//! O(gap), issue #225), [`retain`](Storage::retain) only the events worth
//! keeping (kind-aware retention, issue #201), and read or write the
//! [`Roster`]. `query` has a default that scans the in-memory index; a backend
//! with a real index (a database) overrides it to push the filter down, which
//! is why the query types here stay backend-neutral.
//!
//! Every event carries a stable absolute [sequence](StoredEvent) assigned on
//! append: monotonic, never reused, and never renumbered. Both the SSE
//! `Last-Event-ID` and the `/history` [`Cursor`] resolve against it, and the
//! log persists it, so pruning aged events from the middle of the log (issue
//! #201) leaves every surviving event's cursor pointing at the same event. A
//! position-derived sequence would shift under such a prune and silently gap or
//! duplicate a lossless SSE resume; the stored sequence does not.

use std::{
    collections::{BTreeMap, HashMap},
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc, Arc, Mutex, PoisonError,
    },
    thread::JoinHandle,
};

use crew_core::{
    Channel, ChannelId, Event, EventKind, Message, MessageId, RoleId, Sender, TaskId, Timestamp,
};
use eyre::{Result, WrapErr};
use serde::{Deserialize, Serialize};
use tracing::{event, Level};

/// The append-only event log's file name inside the state directory.
const EVENTS_FILE: &str = "events.jsonl";

/// The roster's file name inside the state directory.
const ROSTER_FILE: &str = "roster.json";

/// The pluggable backend the broker keeps its event log and roster in.
///
/// Kept behind a trait so the backend is swappable (see
/// `docs/architecture.md`): the broker holds a `dyn Storage` and never a
/// concrete type, so a database backend can drop in later. The query types
/// ([`EventQuery`], [`EventPage`]) are backend-neutral for the same reason. The
/// rolling-summary read (issue #19) is a projection over
/// [`query`](Storage::query); physically pruning aged-out events is a later
/// ticket.
pub trait Storage: std::fmt::Debug + Send + Sync {
    /// A short, stable name for the backend, such as `memory` or `log`.
    fn backend(&self) -> &'static str;

    /// The sequence number the next appended event will receive.
    ///
    /// A monotonic counter, advanced once per [`append`](Storage::append) and
    /// never rewound by a [`retain`](Storage::retain) prune, so a sequence is
    /// stable and never reused. It is the cursor the inbox stream hands out as
    /// a `Last-Event-ID`, letting a reconnecting subscriber resume exactly
    /// after the last event it saw even across a prune of the events
    /// between.
    fn next_seq(&self) -> u64;

    /// Records an event in the log, returning the stable sequence it is
    /// assigned.
    ///
    /// The sequence is monotonic and never reused (see
    /// [`next_seq`](Storage::next_seq)), so the returned value is the event's
    /// permanent identity on the stream.
    ///
    /// A durable backend may persist in the background, so the event is not
    /// guaranteed on disk when this returns; call [`flush`](Storage::flush) to
    /// wait for durability. Never blocks on disk I/O. A persist failure is not
    /// silent, though: the event stays in the in-memory index and the failure
    /// is counted in [`durability`](Storage::durability) so `GET /health`
    /// can report degraded durability (issues #206, #207).
    fn append(&self, event: Event) -> u64;

    /// Blocks until every appended event is durably persisted.
    ///
    /// The default is a no-op, for a backend that persists synchronously or
    /// holds nothing on disk. A background-writing backend overrides it to
    /// drain its queue, which the broker calls on graceful shutdown so a
    /// burst still in flight reaches disk before the process exits.
    fn flush(&self) {}

    /// A snapshot of the backend's persistence health.
    ///
    /// Reports how many writes failed to reach disk, so `GET /health` can flag
    /// degraded durability instead of a failure staying invisible until a
    /// restart replays a short log (issue #207). The default reports healthy,
    /// for a backend with no disk to fail (for example [`MemoryStore`]).
    fn durability(&self) -> Durability {
        Durability::default()
    }

    /// Returns every stored event with its stable sequence, oldest first.
    ///
    /// The primitive read the other read paths default onto: [`events`] strips
    /// the sequence, [`events_since`] slices the tail, and [`query`] filters
    /// and pages over it.
    ///
    /// [`events`]: Storage::events
    /// [`events_since`]: Storage::events_since
    /// [`query`]: Storage::query
    fn stored_events(&self) -> Vec<StoredEvent>;

    /// Returns every stored event, oldest first.
    ///
    /// The sequence-free view the projection rebuilds fold on boot; the default
    /// drops the sequence from [`stored_events`](Storage::stored_events).
    fn events(&self) -> Vec<Event> {
        self.stored_events()
            .into_iter()
            .map(|stored| stored.event)
            .collect()
    }

    /// Returns the events with sequence `>= after`, oldest first.
    ///
    /// This bounds a replay to the gap after a cursor rather than the whole log
    /// (issue #225): the SSE resume engine reads only what a reconnecting
    /// client missed, so a connect is O(gap) instead of O(log). The match is by
    /// stored sequence, not log position, so it stays correct after a prune
    /// drops earlier events (issue #201).
    ///
    /// The default filters [`stored_events`](Storage::stored_events), so it is
    /// correct for any backend; a backend with a sorted index (the in-memory
    /// stores below, a database later) overrides it to seek. `after` past the
    /// end yields an empty slice.
    fn events_since(&self, after: u64) -> Vec<StoredEvent> {
        self.stored_events()
            .into_iter()
            .filter(|stored| stored.seq >= after)
            .collect()
    }

    /// Prunes aged-out events the log need not keep, returning how many it
    /// dropped.
    ///
    /// Kind-aware retention (issue #201): a state-bearing event (one a boot
    /// projection folds) is kept forever, an ephemeral event older than
    /// `before` is dropped, and the single newest event is always kept as
    /// the sequence high-water anchor so a restart never reuses a pruned
    /// sequence. The change lands in both the in-memory index and, for a
    /// durable backend, on disk, so the broker's memory and its log stay
    /// bounded on a long-running unit.
    fn retain(&self, before: Timestamp) -> usize;

    /// Returns the current roster.
    fn roster(&self) -> Roster;

    /// Registers or updates a role in the roster, returning whether it was
    /// present.
    ///
    /// Atomic: the read, update, and (for a durable backend) persist happen
    /// under one lock, so concurrent registrations of different roles never
    /// lose each other.
    fn register_role(&self, role: RoleId, status: RoleStatus) -> bool;

    /// Removes a role from the roster, returning its prior status if it was
    /// present.
    ///
    /// Atomic, like [`register_role`](Storage::register_role).
    fn deregister_role(&self, role: &RoleId) -> Option<RoleStatus>;

    /// Returns one filtered, ordered page of events (see [`EventQuery`]).
    ///
    /// The default scans the in-memory index via [`events`](Storage::events). A
    /// backend with its own index should override this to push the filter and
    /// paging down to the store. It treats the log as untrimmed (`base_seq` 0);
    /// a backend that drops earlier events passes its own base.
    ///
    /// A malformed cursor cannot reach here: the opaque token is validated when
    /// it is decoded ([`Cursor::from_token`]), and a well-formed cursor past
    /// the end simply yields an empty page.
    fn query(&self, query: &EventQuery) -> EventPage {
        query_events(&self.stored_events(), query)
    }

    /// Returns the stored [`Message`] with `id`, or `None` if none is stored.
    ///
    /// A by-id lookup (issue #273), so a caller can confirm a reference without
    /// cloning and scanning the whole log: validating that an answer's
    /// `in_reply_to` names a question (issue #211) is the first use. The
    /// in-memory backends override this with an id-to-position index, so the
    /// lookup is O(1) and never clones more than the matched message; the
    /// default scans [`stored_events`](Storage::stored_events) so any backend
    /// works without an index (newest match wins, mirroring the override).
    fn message(&self, id: &MessageId) -> Option<Message> {
        self.stored_events()
            .into_iter()
            .rev()
            .find_map(|stored| match stored.event.kind {
                EventKind::Message(message) if message.id == *id => Some(message),
                _ => None,
            })
    }
}

/// Builds the id-to-position index over the message events in `events` (issue
/// #273), so a by-id lookup is O(1). Non-message events carry no message id to
/// index. Ids are minted unique, so an id maps to a single position.
fn index_messages(events: &[StoredEvent]) -> HashMap<MessageId, usize> {
    events
        .iter()
        .enumerate()
        .filter_map(|(index, stored)| match &stored.event.kind {
            EventKind::Message(message) => Some((message.id, index)),
            _ => None,
        })
        .collect()
}

/// Looks a message up through the id-to-position index, returning a clone of
/// the matching [`Message`] or `None`.
///
/// The index is verified against `events` on read (the position must still hold
/// a message with that id), so a stale entry yields `None` rather than the
/// wrong message; the backends keep it consistent, so this is defence in depth.
fn indexed_message(
    by_id: &HashMap<MessageId, usize>,
    events: &[StoredEvent],
    id: &MessageId,
) -> Option<Message> {
    let index = *by_id.get(id)?;
    match events.get(index).map(|stored| &stored.event.kind) {
        Some(EventKind::Message(message)) if message.id == *id => Some(message.clone()),
        _ => None,
    }
}

/// An event paired with its stable absolute sequence number.
///
/// The unit of the store's in-memory index and its on-disk log: the sequence is
/// assigned on [`append`](Storage::append), persisted alongside the event, and
/// never reused or renumbered, so it survives a prune of the events around it
/// (issue #201). Both the SSE `Last-Event-ID` and the `/history` [`Cursor`]
/// resolve against it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoredEvent {
    /// The stable absolute sequence assigned on append.
    pub seq: u64,
    /// The event itself.
    pub event: Event,
}

/// Whether an event must be kept forever because a boot projection folds it.
///
/// A state-bearing kind rebuilds a projection on restart: `lifecycle` feeds the
/// pause control (issue #185) and the stats rollup (issue #55), `verification`
/// the done-gate (issue #181), `board` the situation board (issue #49),
/// `ledger` the work ledger (issue #185), `telemetry` the stats rollup,
/// `budget` the budget snapshot (issue #176), and `mission` the stats rollup
/// (issue #155). Pruning one would silently corrupt the rebuilt state, so
/// retention keeps them regardless of age. Every other kind carries no state a
/// restart rebuilds, so it is prunable past the retention window.
///
/// The `match` is exhaustive with no wildcard arm on purpose: a new
/// [`EventKind`] variant fails to compile here until it is classified, so no
/// kind is ever pruned (or kept) without a deliberate retention decision. In
/// particular `mission` is state-bearing yet absent from [`EventKindTag`], so a
/// tag-driven keep-list would silently drop it; deriving the decision from
/// [`EventKind`] itself closes that trap (issue #201).
fn is_state_bearing(kind: &EventKind) -> bool {
    match kind {
        // Kept forever: a boot projection folds it.
        EventKind::Lifecycle(_)
        | EventKind::Verification(_)
        | EventKind::Board(_)
        | EventKind::Ledger(_)
        | EventKind::Telemetry(_)
        | EventKind::Budget(_)
        | EventKind::Mission(_) => true,
        // Prunable past the retention window: no projection rebuilds from it.
        EventKind::Message(_)
        | EventKind::Activity(_)
        | EventKind::Boundary(_)
        | EventKind::Usage(_)
        | EventKind::Stall(_) => false,
    }
}

/// Whether `stored` survives a retention pass with cutoff `before`.
///
/// Keeps a state-bearing event at any age, an ephemeral event at or after the
/// cutoff, and the single newest event (`newest_seq`) unconditionally as the
/// sequence high-water anchor, so a durable backend that reconstructs
/// `next_seq` from its log after a restart never reuses a pruned sequence.
fn survives_retention(stored: &StoredEvent, before: Timestamp, newest_seq: u64) -> bool {
    stored.seq == newest_seq || stored.event.ts >= before || is_state_bearing(&stored.event.kind)
}

/// A snapshot of a store's persistence health (issue #207).
///
/// A durable backend keeps the in-memory index consistent even when a write to
/// disk fails, so an append never errors. This carries the otherwise-invisible
/// signal that durability is degraded: `write_failures` counts the events and
/// roster writes that did not reach disk since the broker started, and
/// `last_error` is the most recent failure's message. `GET /health` reports it,
/// so an operator learns durability is degraded rather than discovering it when
/// a restart replays a short log.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Durability {
    /// Events and roster writes that failed to reach disk since start.
    pub write_failures: u64,
    /// The most recent persist failure's message, if any.
    pub last_error: Option<String>,
}

impl Durability {
    /// Whether every write has reached disk so far.
    #[must_use]
    pub fn is_healthy(&self) -> bool {
        self.write_failures == 0
    }
}

/// A role's liveness: the current-state projection of its lifecycle events.
///
/// Maps onto the [`Lifecycle`](crew_core::Lifecycle) transitions on the stream:
/// `working` is up and active, `idle` has no work in flight, `stopped` left
/// cleanly, and `dead` died unexpectedly (a defibrillator recovery point).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Liveness {
    /// Up and working.
    Working,
    /// Registered but idle, with no work in flight.
    Idle,
    /// Cleanly stopped.
    Stopped,
    /// Died unexpectedly.
    Dead,
}

impl Liveness {
    /// Whether the role is present and up or resumable: `working` or `idle`.
    ///
    /// The same "live" a `stopped` or `dead` role is not, so the live count
    /// (issue #32) and lane-ownership enforcement (issue #205) agree on which
    /// roles hold the field.
    #[must_use]
    pub fn is_live(self) -> bool {
        matches!(self, Self::Working | Self::Idle)
    }
}

/// A role's roster entry: the paths it owns and its current liveness.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleStatus {
    /// The directory boundaries the role owns while working.
    pub owned_paths: Vec<String>,
    /// The role's current liveness.
    pub liveness: Liveness,
}

/// The roles the crew knows about, each with its owned paths and liveness.
///
/// The substrate for the live agent count (issue #14): register a role on join,
/// deregister on leave, and read the current membership. A durable backend
/// persists it across a restart.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Roster {
    /// The known roles, keyed and sorted by id for a stable on-disk form.
    roles: BTreeMap<RoleId, RoleStatus>,
}

impl Roster {
    /// An empty roster.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers or updates `role`, returning whether it was already present.
    pub fn register(&mut self, role: RoleId, status: RoleStatus) -> bool {
        self.roles.insert(role, status).is_some()
    }

    /// Removes `role`, returning its prior status if it was present.
    pub fn deregister(&mut self, role: &RoleId) -> Option<RoleStatus> {
        self.roles.remove(role)
    }

    /// The status of `role`, if it is registered.
    #[must_use]
    pub fn get(&self, role: &RoleId) -> Option<&RoleStatus> {
        self.roles.get(role)
    }

    /// Whether `role` is registered.
    #[must_use]
    pub fn contains(&self, role: &RoleId) -> bool {
        self.roles.contains_key(role)
    }

    /// The roles and their status, sorted by role id.
    pub fn iter(&self) -> impl Iterator<Item = (&RoleId, &RoleStatus)> {
        self.roles.iter()
    }

    /// How many roles are registered.
    #[must_use]
    pub fn len(&self) -> usize {
        self.roles.len()
    }

    /// Whether the roster is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.roles.is_empty()
    }
}

/// The event kinds a query can filter on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventKindTag {
    /// Inter-agent communication.
    Message,
    /// A supervised lifecycle transition.
    Lifecycle,
    /// An agent's own parsed work.
    Activity,
    /// A work-ledger change (issue #45).
    Ledger,
    /// A lane boundary crossing (issue #46).
    Boundary,
    /// A done-gate step: a submission or a verdict (issue #47).
    Verification,
    /// A change to the shared situation board (issue #49).
    Board,
    /// A token-spend report against the crew budget (issue #54).
    Budget,
    /// A per-turn token-and-cost usage report (issue #55).
    Telemetry,
    /// A shared-subscription usage reading and its auto-pause (issue #56).
    Usage,
    /// A coordination stall the monitor detected or resolved (issue #48, #120).
    Stall,
}

impl EventKindTag {
    /// Parses a kind name, or `None` if it names no event kind.
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "message" => Some(Self::Message),
            "lifecycle" => Some(Self::Lifecycle),
            "activity" => Some(Self::Activity),
            "ledger" => Some(Self::Ledger),
            "boundary" => Some(Self::Boundary),
            "verification" => Some(Self::Verification),
            "board" => Some(Self::Board),
            "budget" => Some(Self::Budget),
            "telemetry" => Some(Self::Telemetry),
            "usage" => Some(Self::Usage),
            "stall" => Some(Self::Stall),
            _ => None,
        }
    }

    /// Whether `kind` is of this tag.
    fn matches(self, kind: &EventKind) -> bool {
        matches!(
            (self, kind),
            (Self::Message, EventKind::Message(_))
                | (Self::Lifecycle, EventKind::Lifecycle(_))
                | (Self::Activity, EventKind::Activity(_))
                | (Self::Ledger, EventKind::Ledger(_))
                | (Self::Boundary, EventKind::Boundary(_))
                | (Self::Verification, EventKind::Verification(_))
                | (Self::Board, EventKind::Board(_))
                | (Self::Budget, EventKind::Budget(_))
                | (Self::Telemetry, EventKind::Telemetry(_))
                | (Self::Usage, EventKind::Usage(_))
                | (Self::Stall, EventKind::Stall(_))
        )
    }
}

/// The filters a query narrows the log by; an unset field matches every event.
#[derive(Debug, Default, Clone)]
pub struct EventFilter {
    /// Keep only events on this channel (pair member order does not matter).
    pub channel: Option<ChannelId>,
    /// Keep only events sent by this role.
    pub role: Option<RoleId>,
    /// Keep only events on this role's activity timeline: the ones it sent
    /// (messages, its lifecycle, its activity) plus the messages addressed
    /// to it (issue #30).
    pub agent: Option<RoleId>,
    /// Keep only events of these kinds; an empty set matches every kind, so a
    /// consumer can narrow to a subset (e.g. `message,ledger,verification`) in
    /// one query rather than fetching everything and filtering client-side
    /// (issue #125).
    pub kind: Vec<EventKindTag>,
    /// Keep only events belonging to this task.
    pub task: Option<TaskId>,
    /// Keep only events at or after this instant.
    pub since: Option<Timestamp>,
}

impl EventFilter {
    /// Whether `event` satisfies every set filter.
    ///
    /// The store applies it to the log for `GET /history`; the live `GET
    /// /stream` applies the same test to each fanned-out event, so a
    /// filtered live subscription and a filtered history read agree on the
    /// view (issue #31).
    pub(crate) fn matches(&self, event: &Event) -> bool {
        if let Some(since) = self.since {
            if event.ts < since {
                return false;
            }
        }
        if let Some(task) = self.task {
            if event.task != Some(task) {
                return false;
            }
        }
        if let Some(role) = &self.role {
            if !matches!(&event.from, Sender::Role(from) if from == role) {
                return false;
            }
        }
        if let Some(agent) = &self.agent {
            if !event.in_timeline_of(agent) {
                return false;
            }
        }
        if let Some(channel) = &self.channel {
            if !channel_matches(&event.channel, channel) {
                return false;
            }
        }
        if !self.kind.is_empty() && !self.kind.iter().any(|tag| tag.matches(&event.kind)) {
            return false;
        }
        true
    }
}

/// An opaque, prune-stable pagination cursor: the `(ts, seq)` of the last event
/// a page returned.
///
/// `seq` is an event's monotonic sequence number, assigned in append order and
/// never reused. Unlike a raw log position it does not shift when compaction or
/// pruning drops earlier events (issue #208), so a cursor keeps resolving to
/// the right boundary after the log is trimmed. Ordering is by `ts` then `seq`,
/// the same total order [`query`](Storage::query) pages by.
///
/// Treat the [`to_token`](Cursor::to_token) string as opaque: it round-trips
/// through [`from_token`](Cursor::from_token) and a client must not construct
/// or parse it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Cursor {
    /// The last returned event's timestamp: the primary sort key.
    ts: Timestamp,
    /// The last returned event's monotonic sequence number: the tiebreaker.
    seq: u64,
}

impl Cursor {
    /// The `(ts, seq)` sort key a resume starts strictly after.
    fn key(self) -> (Timestamp, u64) {
        (self.ts, self.seq)
    }

    /// Encodes the cursor as an opaque, URL-safe token.
    ///
    /// The format (the seq and the epoch `(secs, nanos)` of `ts`, dot-joined)
    /// is an implementation detail; a client must treat the result as
    /// opaque.
    #[must_use]
    pub fn to_token(self) -> String {
        let (secs, nanos) = self.ts.to_unix();
        format!("{}.{secs}.{nanos}", self.seq)
    }

    /// Parses a token from [`to_token`](Cursor::to_token), or `None` if it is
    /// malformed.
    #[must_use]
    pub fn from_token(token: &str) -> Option<Self> {
        let mut parts = token.split('.');
        let seq = parts.next()?.parse().ok()?;
        let secs = parts.next()?.parse().ok()?;
        let nanos = parts.next()?.parse().ok()?;
        // Reject trailing junk so a token round-trips exactly or fails cleanly.
        if parts.next().is_some() {
            return None;
        }
        Some(Self {
            ts: Timestamp::from_unix(secs, nanos)?,
            seq,
        })
    }
}

/// A page request: the filter, an optional resume cursor, and a size limit.
#[derive(Debug, Clone)]
pub struct EventQuery {
    /// Which events to keep.
    pub filter: EventFilter,
    /// Resume strictly after this cursor (from a previous page's
    /// [`EventPage::next`]).
    pub after: Option<Cursor>,
    /// The maximum number of events to return.
    pub limit: usize,
}

/// One page of query results, ordered by `ts` then sequence number.
#[derive(Debug)]
pub struct EventPage {
    /// The matching events for this page, oldest first.
    pub events: Vec<Event>,
    /// The cursor to resume after for the next page, absent on the last page.
    pub next: Option<Cursor>,
}

/// Runs a query over an in-memory event slice, the scan both backends share.
///
/// Each event carries its stable stored sequence, so ordering by `(ts, seq)`
/// and resuming strictly after `query.after`'s key stays correct even after a
/// prune drops earlier events (issue #201): the cursor names a `(ts, seq)`, not
/// a log index, so paging never depends on an event sitting at a given
/// position.
fn query_events(events: &[StoredEvent], query: &EventQuery) -> EventPage {
    let boundary = query.after.map(Cursor::key);

    let mut matched: Vec<(u64, &Event)> = events
        .iter()
        .map(|stored| (stored.seq, &stored.event))
        .filter(|(_, event)| query.filter.matches(event))
        .collect();
    matched.sort_by(|a, b| a.1.ts.cmp(&b.1.ts).then_with(|| a.0.cmp(&b.0)));

    let start = match boundary {
        Some(key) => matched.partition_point(|(seq, event)| (event.ts, *seq) <= key),
        None => 0,
    };
    let rest = &matched[start..];
    let take = rest.len().min(query.limit);
    let events = rest[..take]
        .iter()
        .map(|(_, event)| (*event).clone())
        .collect();
    let next = (rest.len() > take).then(|| {
        let (seq, event) = rest[take - 1];
        Cursor { ts: event.ts, seq }
    });

    EventPage { events, next }
}

/// Whether `channel` matches the `filter`, treating a pair channel as
/// order-independent.
fn channel_matches(channel: &ChannelId, filter: &ChannelId) -> bool {
    if channel == filter {
        return true;
    }
    match (
        Channel::parse(channel.as_str()),
        Channel::parse(filter.as_str()),
    ) {
        (Some(channel), Some(filter)) => channel == filter,
        _ => false,
    }
}

/// The default in-memory event store, for tests and ephemeral use.
///
/// Holds the log and roster in memory behind mutexes; nothing survives a
/// restart (use [`LogStore`] for durability). A poisoned lock is recovered
/// rather than panicked, so a bad request can never take the store down.
#[derive(Debug, Default)]
pub struct MemoryStore {
    log: Mutex<MemLog>,
    roster: Mutex<Roster>,
}

/// The in-memory store's event index and its monotonic sequence counter.
///
/// Held under one lock so a sequence is assigned and the event indexed
/// together, keeping the index sorted by sequence. `next_seq` only advances (on
/// append) and never rewinds on a [`retain`](Storage::retain) prune, so a
/// sequence is never reused.
#[derive(Debug, Default)]
struct MemLog {
    /// Every stored event with its stable sequence, oldest first.
    events: Vec<StoredEvent>,
    /// The sequence the next appended event will take.
    next_seq: u64,
    /// Message id to its position in `events`, for an O(1) by-id lookup (issue
    /// #273). Maintained on append and rebuilt on a prune, so it always tracks
    /// the message events in `events`.
    by_id: HashMap<MessageId, usize>,
}

impl Storage for MemoryStore {
    fn backend(&self) -> &'static str {
        "memory"
    }

    fn next_seq(&self) -> u64 {
        self.log
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .next_seq
    }

    fn append(&self, event: Event) -> u64 {
        let mut log = self.log.lock().unwrap_or_else(PoisonError::into_inner);
        let seq = log.next_seq;
        log.next_seq += 1;
        // Index the message before the event moves into the log, so a later
        // `message` lookup is O(1) (issue #273).
        let index = log.events.len();
        if let EventKind::Message(message) = &event.kind {
            log.by_id.insert(message.id, index);
        }
        log.events.push(StoredEvent { seq, event });
        seq
    }

    fn stored_events(&self) -> Vec<StoredEvent> {
        self.log
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .events
            .clone()
    }

    fn events_since(&self, after: u64) -> Vec<StoredEvent> {
        let log = self.log.lock().unwrap_or_else(PoisonError::into_inner);
        let start = log.events.partition_point(|stored| stored.seq < after);
        log.events[start..].to_vec()
    }

    fn message(&self, id: &MessageId) -> Option<Message> {
        let log = self.log.lock().unwrap_or_else(PoisonError::into_inner);
        indexed_message(&log.by_id, &log.events, id)
    }

    fn retain(&self, before: Timestamp) -> usize {
        let mut log = self.log.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(newest_seq) = log.events.last().map(|stored| stored.seq) else {
            return 0;
        };
        let before_len = log.events.len();
        log.events
            .retain(|stored| survives_retention(stored, before, newest_seq));
        let pruned = before_len - log.events.len();
        if pruned > 0 {
            // Surviving positions shifted, so rebuild the by-id index (issue #273).
            log.by_id = index_messages(&log.events);
        }
        pruned
    }

    fn roster(&self) -> Roster {
        self.roster
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn register_role(&self, role: RoleId, status: RoleStatus) -> bool {
        self.roster
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .register(role, status)
    }

    fn deregister_role(&self, role: &RoleId) -> Option<RoleStatus> {
        self.roster
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .deregister(role)
    }
}

/// A request to the durable log's background writer thread.
///
/// The writer owns the file and drains this queue, so a caller's thread never
/// blocks on disk. A `Line` carries a serialized event; a `Rewrite` replaces
/// the whole file with the surviving lines after a prune (issue #201); a
/// `Barrier` lets a caller wait until everything queued before it is flushed
/// (see [`LogStore::flush`]).
enum LogWrite {
    /// Persist one serialized event line.
    Line(String),
    /// Replace the log file with these serialized survivor lines, oldest first,
    /// after a retention prune. Keeping the rewrite on the writer thread (which
    /// owns the file) means no other thread races its file handle.
    Rewrite(Vec<String>),
    /// Flush everything queued so far, then acknowledge on the sender.
    Barrier(mpsc::Sender<()>),
}

/// The in-memory event index paired with the handle that persists it.
///
/// Held under one lock so an event is enqueued for the writer in the same order
/// it enters the index, keeping the file's line order aligned with memory. The
/// `writer` is absent only while the store is being dropped.
#[derive(Debug)]
struct Log {
    /// The in-memory index: every event with its stable sequence, oldest first.
    events: Vec<StoredEvent>,
    /// The sequence the next appended event will take. Advanced on append and
    /// never rewound by a prune, so a sequence is never reused; reconstructed
    /// on [`open`](LogStore::open) from the highest sequence on disk.
    next_seq: u64,
    /// Message id to its position in `events`, for an O(1) by-id lookup (issue
    /// #273). Maintained on append, rebuilt on a prune, and reconstructed from
    /// the replayed events on [`open`](LogStore::open).
    by_id: HashMap<MessageId, usize>,
    /// Enqueues lines to the background writer thread.
    writer: Option<mpsc::Sender<LogWrite>>,
}

/// Tracks persistence failures so the store can report degraded durability.
///
/// A failed write is recorded here in addition to being logged, so it is
/// visible through [`Storage::durability`] rather than only in the log (issue
/// #207).
#[derive(Debug, Default)]
struct DurabilityState {
    /// Count of writes that failed to reach disk since start.
    write_failures: AtomicU64,
    /// The most recent failure's message.
    last_error: Mutex<Option<String>>,
}

impl DurabilityState {
    /// Records one persist failure: bumps the count and stores its message.
    fn record(&self, error: &dyn std::fmt::Display) {
        self.write_failures.fetch_add(1, Ordering::Relaxed);
        *self
            .last_error
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(error.to_string());
    }

    /// A snapshot of the current persistence health.
    fn snapshot(&self) -> Durability {
        Durability {
            write_failures: self.write_failures.load(Ordering::Relaxed),
            last_error: self
                .last_error
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone(),
        }
    }
}

/// A durable event store: an on-disk append-only log with an in-memory index.
///
/// The log is one JSON-encoded [`Event`] per line. [`open`](LogStore::open)
/// replays the file into memory, so a restart restores the full log. Each
/// append updates the in-memory index and hands the line to a dedicated writer
/// thread, so persistence never blocks the async runtime, even under a burst
/// (issue #206). Events still reach disk in append order because a single
/// writer drains one FIFO queue. [`flush`](LogStore::flush) waits for the queue
/// to drain, and dropping the store flushes it, so a clean shutdown loses
/// nothing.
///
/// An append stays infallible: a write that fails leaves the event in the index
/// and is counted in [`durability`](Storage::durability), so the running broker
/// is consistent and the operator can still see that durability is degraded
/// (issue #207). The writer thread records the failure, so the count reflects
/// disk faults even though the write happens off the request path.
///
/// The roster persists to its own file, rewritten atomically on each change. A
/// torn or unreadable line (for example a partial write from a crash) is
/// skipped on replay so one bad line never loses the rest of the log.
#[derive(Debug)]
pub struct LogStore {
    log: Mutex<Log>,
    writer_thread: Option<JoinHandle<()>>,
    roster: Mutex<Roster>,
    roster_path: PathBuf,
    // Shared with the writer thread so a background write failure is counted here
    // and surfaced through `durability` (issues #206, #207).
    durability: Arc<DurabilityState>,
}

impl LogStore {
    /// Opens (creating if needed) the store rooted at `dir`, replaying its log.
    ///
    /// # Errors
    /// Returns an error if the directory or files cannot be created, read, or
    /// opened, or if the background writer thread cannot start.
    pub fn open(dir: impl AsRef<Path>) -> Result<Self> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)
            .wrap_err_with(|| format!("could not create state dir {}", dir.display()))?;

        let log_path = dir.join(EVENTS_FILE);
        let (events, next_seq) = replay(&log_path)?;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .wrap_err_with(|| format!("could not open event log {}", log_path.display()))?;

        // Persist on a dedicated thread so an append never blocks the async
        // runtime on disk I/O (issue #206). The queue is unbounded, which suits a
        // broker at message rates with occasional bursts; a producer sustained
        // faster than the disk is out of scope. The writer shares `durability` so
        // a background write failure is counted for `GET /health` (issue #207),
        // and owns the log path so a retention rewrite (issue #201) never races
        // another thread's file handle.
        let durability = Arc::new(DurabilityState::default());
        let writer_durability = Arc::clone(&durability);
        let (writer, requests) = mpsc::channel();
        // Move the log path into the writer thread: it owns the file, so a
        // retention rewrite (issue #201) happens there and never races an append.
        let buffered = BufWriter::new(file);
        let writer_thread = std::thread::Builder::new()
            .name("crew-log-writer".to_owned())
            .spawn(move || run_writer(&log_path, buffered, &requests, &writer_durability))
            .wrap_err("could not start the log writer thread")?;

        let roster_path = dir.join(ROSTER_FILE);
        let roster = read_roster(&roster_path)?;

        event!(
            name: "broker.store.opened",
            Level::INFO,
            crew.events = events.len(),
            crew.roles = roster.len(),
            "replayed {{crew.events}} events from the log",
        );

        // Reconstruct the by-id index from the replayed events, so a by-id lookup
        // is O(1) after a restart too (issue #273).
        let by_id = index_messages(&events);
        Ok(Self {
            log: Mutex::new(Log {
                events,
                next_seq,
                by_id,
                writer: Some(writer),
            }),
            writer_thread: Some(writer_thread),
            roster: Mutex::new(roster),
            roster_path,
            durability,
        })
    }

    /// Blocks until every event appended so far is durably on disk.
    ///
    /// Sends a barrier through the writer's FIFO queue and waits for the writer
    /// thread to flush past it, so every prior append is persisted when this
    /// returns. The broker calls it on graceful shutdown so a burst still in
    /// the queue reaches disk before the process exits.
    pub fn flush(&self) {
        let (ack, acked) = mpsc::channel();
        {
            let log = self.log.lock().unwrap_or_else(PoisonError::into_inner);
            // An absent writer means the store is shutting down; nothing to drain.
            let Some(writer) = log.writer.as_ref() else {
                return;
            };
            if writer.send(LogWrite::Barrier(ack)).is_err() {
                return;
            }
        }
        // Wait outside the lock so appends keep flowing while the writer drains.
        let _ = acked.recv();
    }
}

impl Storage for LogStore {
    fn backend(&self) -> &'static str {
        "log"
    }

    fn append(&self, event: Event) -> u64 {
        let mut log = self.log.lock().unwrap_or_else(PoisonError::into_inner);
        // Assign the stable sequence and index under one lock, so the writer
        // receives events in the same order they enter the index and the file's
        // lines stay aligned with memory. The send is non-blocking; the writer
        // thread does the disk I/O and records any write failure. The event is
        // indexed regardless, so a persist failure degrades only durability, not
        // the running broker's consistency (issues #206, #207).
        let seq = log.next_seq;
        log.next_seq += 1;
        let stored = StoredEvent { seq, event };
        match serde_json::to_string(&stored) {
            Ok(line) => {
                if let Some(writer) = log.writer.as_ref() {
                    if writer.send(LogWrite::Line(line)).is_err() {
                        event!(
                            name: "broker.store.persist.failed",
                            Level::ERROR,
                            "the log writer thread is gone; keeping the event in memory only",
                        );
                        self.durability.record(&"the log writer thread is gone");
                    }
                }
            }
            Err(err) => {
                event!(
                    name: "broker.store.encode.failed",
                    Level::ERROR,
                    error = %err,
                    "could not encode event for the log; keeping it in memory only",
                );
                self.durability.record(&err);
            }
        }
        // Index the message before it moves into the log, so a later `message`
        // lookup is O(1) (issue #273).
        let index = log.events.len();
        if let EventKind::Message(message) = &stored.event.kind {
            log.by_id.insert(message.id, index);
        }
        log.events.push(stored);
        seq
    }

    fn flush(&self) {
        LogStore::flush(self);
    }

    fn durability(&self) -> Durability {
        self.durability.snapshot()
    }

    fn stored_events(&self) -> Vec<StoredEvent> {
        self.log
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .events
            .clone()
    }

    fn events_since(&self, after: u64) -> Vec<StoredEvent> {
        let log = self.log.lock().unwrap_or_else(PoisonError::into_inner);
        let start = log.events.partition_point(|stored| stored.seq < after);
        log.events[start..].to_vec()
    }

    fn message(&self, id: &MessageId) -> Option<Message> {
        let log = self.log.lock().unwrap_or_else(PoisonError::into_inner);
        indexed_message(&log.by_id, &log.events, id)
    }

    fn next_seq(&self) -> u64 {
        self.log
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .next_seq
    }

    fn retain(&self, before: Timestamp) -> usize {
        let mut log = self.log.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(newest_seq) = log.events.last().map(|stored| stored.seq) else {
            return 0;
        };
        let before_len = log.events.len();
        log.events
            .retain(|stored| survives_retention(stored, before, newest_seq));
        let pruned = before_len - log.events.len();
        if pruned == 0 {
            // Nothing aged out, so leave the file untouched: no needless rewrite
            // on a sweep that finds everything still worth keeping.
            return 0;
        }
        // Surviving positions shifted, so rebuild the by-id index (issue #273).
        log.by_id = index_messages(&log.events);
        // Rewrite the file to the survivors on the writer thread, which owns the
        // file handle. `next_seq` is untouched, and the newest event is always a
        // survivor, so the highest sequence stays on disk and a restart
        // reconstructs the same `next_seq`: no sequence is ever reused.
        let lines = log
            .events
            .iter()
            .filter_map(|stored| serde_json::to_string(stored).ok())
            .collect();
        if let Some(writer) = log.writer.as_ref() {
            if writer.send(LogWrite::Rewrite(lines)).is_err() {
                event!(
                    name: "broker.store.prune.failed",
                    Level::ERROR,
                    "the log writer thread is gone; pruned in memory only",
                );
                self.durability.record(&"the log writer thread is gone");
            }
        }
        pruned
    }

    fn query(&self, query: &EventQuery) -> EventPage {
        let log = self.log.lock().unwrap_or_else(PoisonError::into_inner);
        query_events(&log.events, query)
    }

    fn roster(&self) -> Roster {
        self.roster
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn register_role(&self, role: RoleId, status: RoleStatus) -> bool {
        let mut guard = self.roster.lock().unwrap_or_else(PoisonError::into_inner);
        let existed = guard.register(role, status);
        save_roster(&self.roster_path, &guard, &self.durability);
        existed
    }

    fn deregister_role(&self, role: &RoleId) -> Option<RoleStatus> {
        let mut guard = self.roster.lock().unwrap_or_else(PoisonError::into_inner);
        let prior = guard.deregister(role);
        if prior.is_some() {
            save_roster(&self.roster_path, &guard, &self.durability);
        }
        prior
    }
}

impl Drop for LogStore {
    fn drop(&mut self) {
        // Close the queue so the writer drains its backlog and returns, then wait
        // for it, so every queued event reaches disk before the store goes away.
        self.log
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .writer
            .take();
        if let Some(thread) = self.writer_thread.take() {
            let _ = thread.join();
        }
    }
}

/// Persists log lines on a dedicated thread, off the async runtime.
///
/// Blocks for the next request, then drains everything already queued so a
/// burst becomes a single flush, letting the writer keep pace with a fast
/// producer. Returns when the queue's sender is dropped (on store shutdown),
/// having flushed the backlog. A write or flush failure is recorded to
/// `durability` so `GET /health` reflects it even off the request path (issues
/// #206, #207).
fn run_writer(
    path: &Path,
    mut writer: BufWriter<File>,
    requests: &mpsc::Receiver<LogWrite>,
    durability: &DurabilityState,
) {
    while let Ok(first) = requests.recv() {
        let mut barriers = Vec::new();
        writer = drain_one(path, writer, first, &mut barriers, durability);
        while let Ok(next) = requests.try_recv() {
            writer = drain_one(path, writer, next, &mut barriers, durability);
        }
        // One flush per drained batch, so a restart can replay every line written
        // so far. A failure keeps the broker consistent in memory; only durability
        // degrades until the operator resolves the disk problem.
        if let Err(err) = writer.flush() {
            event!(
                name: "broker.store.persist.failed",
                Level::ERROR,
                error = %err,
                "could not flush the event log; keeping events in memory only",
            );
            durability.record(&err);
        }
        // Acknowledge each barrier now that its prior lines are flushed.
        for barrier in barriers {
            let _ = barrier.send(());
        }
    }
}

/// Applies one write request, returning the (possibly replaced) writer.
///
/// A [`Rewrite`](LogWrite::Rewrite) swaps the writer onto the freshly rewritten
/// file; every other request writes through the current one. Keeping the swap
/// here, on the one thread that owns the file, means no append ever races the
/// rewrite. A rewrite failure keeps the old writer so appends keep flowing; the
/// prune already landed in memory and only durability degrades (issue #201).
fn drain_one(
    path: &Path,
    mut writer: BufWriter<File>,
    request: LogWrite,
    barriers: &mut Vec<mpsc::Sender<()>>,
    durability: &DurabilityState,
) -> BufWriter<File> {
    if let LogWrite::Rewrite(lines) = request {
        match rewrite_log(path, &lines) {
            Ok(replaced) => return replaced,
            Err(err) => {
                event!(
                    name: "broker.store.prune.failed",
                    Level::ERROR,
                    error = %err,
                    "could not rewrite the pruned log; keeping the prune in memory only",
                );
                durability.record(&err);
                return writer;
            }
        }
    }
    write_request(&mut writer, request, barriers, durability);
    writer
}

/// Rewrites the log file to `lines`, returning a fresh append writer on it.
///
/// Writes a temp file, flushes it, and renames it over the target so the
/// replacement is atomic (a crash leaves either the old file or the new one,
/// never a torn mix), then opens a fresh append writer so subsequent appends
/// land after the survivors. The caller drops the old writer; its buffered
/// bytes are survivors already written to the new file, so nothing is lost.
fn rewrite_log(path: &Path, lines: &[String]) -> std::io::Result<BufWriter<File>> {
    let tmp = path.with_file_name(format!("{EVENTS_FILE}.tmp"));
    let mut scratch = BufWriter::new(File::create(&tmp)?);
    for line in lines {
        writeln!(scratch, "{line}")?;
    }
    scratch.flush()?;
    // Close the scratch file before the rename so the replacement is clean on
    // every platform.
    drop(scratch);
    std::fs::rename(&tmp, path)?;
    let file = OpenOptions::new().create(true).append(true).open(path)?;
    Ok(BufWriter::new(file))
}

/// Applies one write request: buffer a line (recording a failure to
/// `durability`), or hold a barrier's acknowledgement until the batch flushes.
///
/// Generic over the writer so the failure path is testable with a writer that
/// fails, without a real disk fault (see the tests).
fn write_request(
    writer: &mut impl Write,
    request: LogWrite,
    barriers: &mut Vec<mpsc::Sender<()>>,
    durability: &DurabilityState,
) {
    match request {
        LogWrite::Line(line) => {
            if let Err(err) = writeln!(writer, "{line}") {
                event!(
                    name: "broker.store.persist.failed",
                    Level::ERROR,
                    error = %err,
                    "could not write an event to the log; keeping it in memory only",
                );
                durability.record(&err);
            }
        }
        LogWrite::Barrier(ack) => barriers.push(ack),
        // A rewrite swaps the file handle, which this generic writer cannot do,
        // so `drain_one` intercepts it before it ever reaches here.
        LogWrite::Rewrite(_) => {
            unreachable!("a rewrite is handled by drain_one, not write_request")
        }
    }
}

/// Replays the on-disk log into memory, skipping any unreadable line.
///
/// Returns the stored events, each with its persisted sequence, and the next
/// sequence to assign, reconstructed as one past the highest sequence on disk.
/// Retention always keeps the newest event (issue #201), so the highest
/// sequence survives a prune and the reconstruction never reuses one.
fn replay(path: &Path) -> Result<(Vec<StoredEvent>, u64)> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok((Vec::new(), 0)),
        Err(err) => {
            return Err(err)
                .wrap_err_with(|| format!("could not read event log {}", path.display()))
        }
    };

    let mut events = Vec::new();
    let mut next_seq = 0;
    for (index, line) in BufReader::new(file).lines().enumerate() {
        let line = line.wrap_err_with(|| format!("could not read event log {}", path.display()))?;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<StoredEvent>(&line) {
            Ok(stored) => {
                next_seq = next_seq.max(stored.seq + 1);
                events.push(stored);
            }
            // A torn or corrupt line (e.g. a crash mid-append) must not lose the rest.
            Err(err) => event!(
                name: "broker.store.replay.skipped",
                Level::WARN,
                crew.line = index + 1,
                error = %err,
                "skipping an unreadable log line at {{crew.line}}",
            ),
        }
    }
    Ok((events, next_seq))
}

/// Reads the roster file, defaulting to empty if it is absent or unreadable.
fn read_roster(path: &Path) -> Result<Roster> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Roster::default()),
        Err(err) => {
            return Err(err).wrap_err_with(|| format!("could not read roster {}", path.display()))
        }
    };
    match serde_json::from_slice(&bytes) {
        Ok(roster) => Ok(roster),
        // The roster is rebuildable state, so a corrupt file starts empty, not fatal.
        Err(err) => {
            event!(
                name: "broker.store.roster.unreadable",
                Level::WARN,
                error = %err,
                "roster file unreadable; starting with an empty roster",
            );
            Ok(Roster::default())
        }
    }
}

/// Persists the roster, recording (not propagating) a write failure so a roster
/// change never fails the request; the change stays in memory until the disk
/// recovers, and the failure surfaces through [`Storage::durability`].
fn save_roster(path: &Path, roster: &Roster, durability: &DurabilityState) {
    if let Err(err) = persist_roster(path, roster) {
        event!(
            name: "broker.store.roster.persist.failed",
            Level::ERROR,
            error = %err,
            "could not persist roster; keeping it in memory only",
        );
        durability.record(&err);
    }
}

/// Writes the roster atomically: to a temp file, then rename over the target.
fn persist_roster(path: &Path, roster: &Roster) -> Result<()> {
    let tmp = path.with_file_name(format!("{ROSTER_FILE}.tmp"));
    let bytes = serde_json::to_vec_pretty(roster).wrap_err("could not encode roster")?;
    std::fs::write(&tmp, &bytes).wrap_err_with(|| format!("could not write {}", tmp.display()))?;
    std::fs::rename(&tmp, path)
        .wrap_err_with(|| format!("could not replace {}", path.display()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{
        io::{self, Write},
        path::{Path, PathBuf},
        sync::atomic::{AtomicU64, Ordering},
    };

    use crew_core::{
        Activity, BoardEvent, BoardSection, BoundaryEvent, BudgetEvent, ChannelId, Event,
        EventKind, LedgerEvent, Lifecycle, Message, MessageId, MessageKind, MissionEvent, RoleId,
        Sender, StallEvent, StallKind, StallStatus, TaskId, TaskState, TelemetryEvent, Timestamp,
        UsageEvent, Verdict, VerificationEvent,
    };

    use super::{
        is_state_bearing, query_events, survives_retention, write_request, Cursor, DurabilityState,
        EventFilter, EventKindTag, EventQuery, Liveness, LogStore, LogWrite, MemoryStore,
        RoleStatus, Storage, StoredEvent,
    };

    /// Wraps events into stored events, assigning sequences in order, so a scan
    /// helper reads the same stored shape the store's index holds.
    fn stored_log(events: Vec<Event>) -> Vec<StoredEvent> {
        events
            .into_iter()
            .enumerate()
            .map(|(index, event)| StoredEvent {
                seq: index as u64,
                event,
            })
            .collect()
    }

    /// A unique temp directory that removes itself on drop.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path = std::env::temp_dir()
                .join(format!("crew-store-test-{}-{unique}", std::process::id()));
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TempDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn ts(seconds: u32) -> Timestamp {
        let text = format!("\"2020-01-01T00:00:{seconds:02}Z\"");
        serde_json::from_str(&text).unwrap()
    }

    fn message(role: &str, channel: &str, at: Timestamp) -> Event {
        Event {
            ts: at,
            from: Sender::Role(RoleId::new(role)),
            channel: ChannelId::new(channel),
            task: None,
            kind: EventKind::Message(Message {
                id: MessageId::new(),
                kind: MessageKind::Note,
                body: String::new(),
            }),
        }
    }

    fn unfiltered(after: Option<Cursor>, limit: usize) -> EventQuery {
        EventQuery {
            filter: EventFilter::default(),
            after,
            limit,
        }
    }

    #[test]
    fn query_orders_by_timestamp_then_position() {
        let log = stored_log(vec![
            message("backend", "all-units", ts(3)),
            message("backend", "all-units", ts(1)),
            message("backend", "all-units", ts(2)),
        ]);
        let page = query_events(&log, &unfiltered(None, 10));
        let times: Vec<_> = page.events.iter().map(|event| event.ts).collect();
        assert_eq!(times, vec![ts(1), ts(2), ts(3)]);
        assert!(page.next.is_none());
    }

    #[test]
    fn query_paging_is_stable_when_events_are_appended_between_pages() {
        let mut log: Vec<Event> = (0..20)
            .map(|i| message("backend", "all-units", ts(i)))
            .collect();

        let page1 = query_events(&stored_log(log.clone()), &unfiltered(None, 8));
        assert_eq!(page1.events.len(), 8);

        // A concurrent writer appends newer events after page 1 was read.
        log.push(message("frontend", "all-units", ts(40)));
        log.push(message("frontend", "all-units", ts(41)));

        let page2 = query_events(&stored_log(log.clone()), &unfiltered(page1.next, 8));
        let page3 = query_events(&stored_log(log.clone()), &unfiltered(page2.next, 8));

        let seen: Vec<Timestamp> = page1
            .events
            .iter()
            .chain(&page2.events)
            .chain(&page3.events)
            .map(|event| event.ts)
            .collect();
        let originals: Vec<Timestamp> = (0..20).map(ts).collect();
        assert_eq!(
            seen[..20],
            originals[..],
            "the 20 originals page through intact"
        );
        assert_eq!(
            seen[20..],
            vec![ts(40), ts(41)],
            "new writes land after the cursor"
        );
        assert_eq!(seen.len(), 22, "no duplicates or skips");
    }

    #[test]
    fn query_filters_compose() {
        let log = stored_log(vec![
            message("backend", "all-units", ts(1)),
            message("frontend", "all-units", ts(2)),
            Event {
                kind: EventKind::Lifecycle(Lifecycle::Started),
                ..message("backend", "all-units", ts(3))
            },
            message("backend", "@backend", ts(4)),
        ]);

        let filtered = |filter: EventFilter| {
            query_events(
                &log,
                &EventQuery {
                    filter,
                    after: None,
                    limit: 100,
                },
            )
            .events
        };

        let by_role = filtered(EventFilter {
            role: Some(RoleId::new("frontend")),
            ..EventFilter::default()
        });
        assert_eq!(by_role.len(), 1);

        let by_kind = filtered(EventFilter {
            kind: vec![EventKindTag::Lifecycle],
            ..EventFilter::default()
        });
        assert_eq!(by_kind.len(), 1);
        assert!(matches!(by_kind[0].kind, EventKind::Lifecycle(_)));

        // A multi-kind filter keeps every event of any listed kind, in one query.
        let by_kinds = filtered(EventFilter {
            kind: vec![EventKindTag::Lifecycle, EventKindTag::Message],
            ..EventFilter::default()
        });
        assert!(
            by_kinds
                .iter()
                .all(|event| matches!(event.kind, EventKind::Lifecycle(_) | EventKind::Message(_))),
            "only the listed kinds pass",
        );
        assert!(
            by_kinds.len() >= by_kind.len(),
            "the union keeps at least the single-kind subset",
        );

        let by_since = filtered(EventFilter {
            since: Some(ts(3)),
            ..EventFilter::default()
        });
        assert_eq!(by_since.len(), 2, "ts 3 and 4 remain");
    }

    #[test]
    fn query_channel_filter_ignores_pair_member_order() {
        let log = stored_log(vec![message("backend", "frontend+backend", ts(1))]);
        let page = query_events(
            &log,
            &EventQuery {
                filter: EventFilter {
                    channel: Some(ChannelId::new("backend+frontend")),
                    ..EventFilter::default()
                },
                after: None,
                limit: 100,
            },
        );
        assert_eq!(page.events.len(), 1);
    }

    #[test]
    fn a_cursor_past_the_end_returns_an_empty_final_page() {
        // A well-formed cursor beyond every event is not an error: it just has
        // nothing after it. This is what lets a cursor survive a future trim that
        // drops the event it named (issue #208).
        let log = stored_log(vec![message("backend", "all-units", ts(1))]);
        let beyond = Cursor { ts: ts(9), seq: 99 };
        let page = query_events(&log, &unfiltered(Some(beyond), 10));
        assert!(page.events.is_empty(), "no event sorts after the cursor");
        assert!(page.next.is_none(), "and so no next page");
    }

    #[test]
    fn a_cursor_is_stable_when_earlier_events_are_pruned() {
        // The cursor carries `(ts, seq)` and the stored sequence never shifts, so
        // a page fetched before a prune and resumed after one neither repeats nor
        // skips an event even though earlier events were physically dropped
        // (issues #201, #208).
        let log = stored_log(
            (0..10)
                .map(|i| message("backend", "all-units", ts(i)))
                .collect(),
        );

        // Page 1 over the full log: the first four events, cursor at the fourth.
        let page1 = query_events(&log, &unfiltered(None, 4));
        let seen: Vec<Timestamp> = page1.events.iter().map(|event| event.ts).collect();
        assert_eq!(seen, (0..4).map(ts).collect::<Vec<_>>());
        let cursor = page1.next.expect("a fourth event remains");

        // Prune the first three events; the survivors keep their stored seqs.
        let pruned: Vec<StoredEvent> = log.into_iter().skip(3).collect();
        let page2 = query_events(&pruned, &unfiltered(Some(cursor), 4));
        let resumed: Vec<Timestamp> = page2.events.iter().map(|event| event.ts).collect();
        assert_eq!(
            resumed,
            (4..8).map(ts).collect::<Vec<_>>(),
            "resume yields event 4 onward: no repeat of 0-3, no skip",
        );
    }

    #[test]
    fn a_cursor_round_trips_through_its_opaque_token() {
        let cursor = Cursor { ts: ts(7), seq: 42 };
        assert_eq!(
            Cursor::from_token(&cursor.to_token()),
            Some(cursor),
            "a token decodes back to the cursor it came from",
        );
        // Malformed tokens are rejected, not coerced: the old cursor was a bare
        // integer, so that shape must no longer parse (issue #208).
        for malformed in ["", "999999", "1.2", "1.2.3.4", "a.b.c"] {
            assert_eq!(
                Cursor::from_token(malformed),
                None,
                "`{malformed}` is not a valid cursor token",
            );
        }
    }

    #[test]
    fn memory_store_round_trips_events_and_roster() {
        let store = MemoryStore::default();
        assert_eq!(store.backend(), "memory");
        store.append(message("backend", "all-units", ts(1)));
        assert_eq!(store.events().len(), 1);

        let backend = RoleId::new("backend");
        let status = RoleStatus {
            owned_paths: vec!["crates/crew-broker".to_owned()],
            liveness: Liveness::Working,
        };
        assert!(
            !store.register_role(backend.clone(), status.clone()),
            "a fresh role is newly registered"
        );
        assert_eq!(store.roster().get(&backend), Some(&status));
        assert!(store.deregister_role(&backend).is_some());
        assert!(
            !store.roster().contains(&backend),
            "deregister removes the role"
        );
    }

    #[test]
    fn log_store_replays_events_across_a_restart() {
        let dir = TempDir::new();

        // First run: append three events, then drop the store (closing the file).
        let store = LogStore::open(dir.path()).unwrap();
        assert_eq!(store.backend(), "log");
        store.append(message("backend", "all-units", ts(1)));
        store.append(message("frontend", "@backend", ts(2)));
        store.append(message("backend", "all-units", ts(3)));
        drop(store);

        // Second run: a fresh store over the same dir replays the log.
        let reopened = LogStore::open(dir.path()).unwrap();
        let events = reopened.events();
        assert_eq!(events.len(), 3, "every event survived the restart");
        assert_eq!(events[0].ts, ts(1));
        assert_eq!(events[2].ts, ts(3));

        // A new append extends the replayed log rather than truncating it.
        reopened.append(message("qa", "all-units", ts(4)));
        assert_eq!(reopened.events().len(), 4);
        drop(reopened);
        let again = LogStore::open(dir.path()).unwrap();
        assert_eq!(again.events().len(), 4);
    }

    #[test]
    fn log_store_flush_makes_appends_durable_without_dropping_the_store() {
        let dir = TempDir::new();
        let store = LogStore::open(dir.path()).unwrap();
        store.append(message("backend", "all-units", ts(1)));
        store.append(message("frontend", "@backend", ts(2)));

        // The writer persists in the background, so a bare append is not yet on
        // disk; flush blocks until it is. A second reader then sees both events
        // while the first store is still open (issue #206).
        store.flush();
        let reopened = LogStore::open(dir.path()).unwrap();
        assert_eq!(
            reopened.events().len(),
            2,
            "flush persists every prior append without dropping the store",
        );
    }

    #[test]
    fn events_since_returns_only_the_gap_after_the_cursor() {
        // Both the memory and the durable store slice to the events at or after a
        // sequence, so an SSE replay reads O(gap) rather than the whole log (issue
        // #225). The slice matches the full log's tail, so replay is unchanged.
        let dir = TempDir::new();
        let stores: [Box<dyn Storage>; 2] = [
            Box::new(MemoryStore::default()),
            Box::new(LogStore::open(dir.path()).unwrap()),
        ];
        for store in stores {
            store.append(message("backend", "all-units", ts(1)));
            store.append(message("frontend", "@backend", ts(2)));
            store.append(message("qa", "all-units", ts(3)));

            // From 0, the whole log; from a mid-point, only the tail after it.
            let since_0: Vec<Event> = store.events_since(0).into_iter().map(|s| s.event).collect();
            assert_eq!(since_0, store.events(), "since 0 is the whole log");
            let tail = store.events_since(1);
            assert_eq!(tail.len(), 2, "since 1 skips the first event");
            assert_eq!(tail[0].seq, 1, "the slice starts at the requested sequence");
            assert_eq!(tail[0].event.ts, ts(2), "and at the matching event");
            let tail_events: Vec<Event> = tail.into_iter().map(|s| s.event).collect();
            assert_eq!(
                tail_events,
                store.events()[1..].to_vec(),
                "the gap matches the full log's tail, so replay is behavior-preserving",
            );

            // At the tail, empty (a fresh connection replays nothing); a cursor past
            // the end (a stale, ahead one) yields nothing rather than a panic.
            assert!(
                store.events_since(3).is_empty(),
                "since the live tail is empty"
            );
            assert!(
                store.events_since(99).is_empty(),
                "a cursor past the end yields nothing, not an out-of-bounds slice",
            );
        }
    }

    #[test]
    fn events_since_resumes_by_stable_seq_after_a_prune() {
        // The SSE `Last-Event-ID` resume reads `events_since(last_id + 1)`, so a
        // reconnect after a prune must return exactly the surviving events after
        // the cursor, keyed by the stable sequence and never by log position
        // (issue #274, the invariant #201 gave the store). A prune shifts every
        // surviving event's position, yet the resume set stays correct: no
        // already-seen event repeats and no surviving event is skipped. Run over
        // both backends, since both back the SSE resume.
        let dir = TempDir::new();
        let stores: [Box<dyn Storage>; 2] = [
            Box::new(MemoryStore::default()),
            Box::new(LogStore::open(dir.path()).unwrap()),
        ];
        for store in stores {
            // seq 0,2,4,5 are aged ephemeral messages; 1,3 are state-bearing. All
            // are aged, so retention keeps 1,3 (a projection folds them) and 5 (the
            // newest anchor), pruning the ephemeral 0,2,4 from the MIDDLE.
            for kind in [
                message_kind(),
                EventKind::Lifecycle(Lifecycle::Started),
                message_kind(),
                EventKind::Lifecycle(Lifecycle::Started),
                message_kind(),
                message_kind(),
            ] {
                store.append(of_kind(kind, ts(1)));
            }

            // A client last saw seq 2; its reconnect replays `events_since(3)`.
            let before: Vec<u64> = store.events_since(3).iter().map(|s| s.seq).collect();
            assert_eq!(
                before,
                vec![3, 4, 5],
                "before a prune, the gap after id 2 is 3,4,5"
            );

            assert_eq!(
                store.retain(ts(5)),
                3,
                "the three aged ephemeral messages are pruned from the middle",
            );

            // Survivors are now at positions 0,1,2 (seq 1,3,5), so a position-keyed
            // resume would be wrong. The seq-keyed resume returns the events after
            // id 2 that survived, 3 and 5, in order: no duplicate of an already-seen
            // event, no gap among survivors (seq 4 is legitimately aged out).
            let after: Vec<u64> = store.events_since(3).iter().map(|s| s.seq).collect();
            assert_eq!(
                after,
                vec![3, 5],
                "resume returns the surviving events after id 2 by seq, not position",
            );

            // A stale cursor at a since-pruned id (Last-Event-ID 4, whose seq was
            // pruned) resumes at the next survivor, with no gap or duplicate.
            let from_pruned: Vec<u64> = store.events_since(5).iter().map(|s| s.seq).collect();
            assert_eq!(
                from_pruned,
                vec![5],
                "resume past the pruned id 4 yields the anchor"
            );
            assert!(
                store.events_since(6).is_empty(),
                "a cursor at the live tail replays nothing, even after a prune",
            );

            // The live-tail bound `next_seq` never rewinds, so the replay's
            // `seq < live_from` guard never mistakes a live event for a replayed one.
            assert_eq!(
                store.next_seq(),
                6,
                "the monotonic counter is untouched by the prune"
            );
        }
    }

    #[test]
    fn log_store_persists_the_roster_across_a_restart() {
        let dir = TempDir::new();
        let store = LogStore::open(dir.path()).unwrap();
        store.register_role(
            RoleId::new("backend"),
            RoleStatus {
                owned_paths: vec!["crates/crew-broker".to_owned()],
                liveness: Liveness::Working,
            },
        );
        store.register_role(
            RoleId::new("frontend"),
            RoleStatus {
                owned_paths: vec![],
                liveness: Liveness::Idle,
            },
        );
        drop(store);

        let reopened = LogStore::open(dir.path()).unwrap();
        let roster = reopened.roster();
        assert_eq!(roster.len(), 2);
        assert!(roster.contains(&RoleId::new("backend")));
        assert_eq!(
            roster
                .get(&RoleId::new("frontend"))
                .map(|status| status.liveness),
            Some(Liveness::Idle),
            "liveness persists across a restart",
        );
    }

    #[test]
    fn log_store_skips_a_torn_final_line_on_replay() {
        let dir = TempDir::new();
        let store = LogStore::open(dir.path()).unwrap();
        store.append(message("backend", "all-units", ts(1)));
        drop(store);

        // Simulate a crash mid-append: a partial JSON line appended to the log file.
        let log_path = dir.path().join(super::EVENTS_FILE);
        let mut existing = std::fs::read_to_string(&log_path).unwrap();
        existing.push_str("{\"ts\":\"2020-01-01T00:00:0");
        std::fs::write(&log_path, existing).unwrap();

        let reopened = LogStore::open(dir.path()).unwrap();
        assert_eq!(
            reopened.events().len(),
            1,
            "the good event survives; the torn line is skipped"
        );
    }

    /// A writer that always fails, standing in for a full or failing disk.
    struct FailingWriter;

    impl Write for FailingWriter {
        fn write(&mut self, _buf: &[u8]) -> io::Result<usize> {
            Err(io::Error::other("disk full"))
        }

        fn flush(&mut self) -> io::Result<()> {
            Err(io::Error::other("disk full"))
        }
    }

    #[test]
    fn a_failed_write_is_recorded_as_degraded_durability() {
        // A write failure must not be silent (issue #207): the writer thread
        // counts it so `GET /health` can report degraded durability. Drive the
        // writer's line path directly with a failing writer to prove the
        // recording without a real disk fault (issue #206).
        let durability = DurabilityState::default();
        assert!(
            durability.snapshot().is_healthy(),
            "a fresh store is durable"
        );

        let mut barriers = Vec::new();
        write_request(
            &mut FailingWriter,
            LogWrite::Line("{}".to_owned()),
            &mut barriers,
            &durability,
        );

        let snapshot = durability.snapshot();
        assert!(!snapshot.is_healthy(), "a failed write degrades durability");
        assert_eq!(snapshot.write_failures, 1, "the failure is counted");
        assert_eq!(
            snapshot.last_error.as_deref(),
            Some("disk full"),
            "the failure's message is captured for the operator",
        );

        // A second failure accumulates rather than resetting the count.
        write_request(
            &mut FailingWriter,
            LogWrite::Line("{}".to_owned()),
            &mut barriers,
            &durability,
        );
        assert_eq!(durability.snapshot().write_failures, 2);
        assert!(barriers.is_empty(), "a line request queues no barrier");
    }

    #[test]
    fn a_log_store_with_a_working_disk_reports_healthy_durability() {
        let dir = TempDir::new();
        let store = LogStore::open(dir.path()).unwrap();
        store.append(message("backend", "all-units", ts(1)));
        let durability = store.durability();
        assert!(durability.is_healthy(), "every write reached disk");
        assert_eq!(durability.write_failures, 0);
        assert!(durability.last_error.is_none());
    }

    #[test]
    fn the_memory_store_reports_healthy_durability() {
        // The ephemeral store has no disk to fail, so it never reports a write
        // failure; `storage: memory` already tells the operator it is not durable.
        let store = MemoryStore::default();
        store.append(message("backend", "all-units", ts(1)));
        assert!(store.durability().is_healthy());
    }

    /// A representative event of `kind`, stamped at `at`, for the retention
    /// tests.
    fn of_kind(kind: EventKind, at: Timestamp) -> Event {
        Event {
            ts: at,
            from: Sender::Role(RoleId::new("backend")),
            channel: ChannelId::new("all-units"),
            task: None,
            kind,
        }
    }

    fn message_kind() -> EventKind {
        EventKind::Message(Message {
            id: MessageId::new(),
            kind: MessageKind::Note,
            body: String::new(),
        })
    }

    fn role() -> RoleId {
        RoleId::new("backend")
    }

    #[test]
    fn is_state_bearing_classifies_every_kind_a_projection_rebuilds() {
        // The retention keep-set is derived from `EventKind` itself, not the
        // `EventKindTag` filter set, so a state-bearing kind absent from the tags
        // is not silently dropped (issue #201). The exhaustive `match` in
        // `is_state_bearing` fails to compile if a new kind is added without a
        // decision; this pins the decision for every current kind so a
        // misclassification (for example flipping `mission` to prunable) fails
        // loudly too.
        let keep = [
            EventKind::Lifecycle(Lifecycle::Started),
            EventKind::Verification(VerificationEvent {
                task: TaskId::new(),
                title: String::new(),
                owner: role(),
                verifier: None,
                verdict: Verdict::Submitted,
                detail: String::new(),
            }),
            EventKind::Board(BoardEvent {
                key: "k".to_owned(),
                section: BoardSection::Decision,
                author: role(),
                body: String::new(),
                retracted: false,
            }),
            EventKind::Ledger(LedgerEvent {
                task: TaskId::new(),
                owner: role(),
                state: TaskState::Claimed,
                title: String::new(),
            }),
            EventKind::Telemetry(TelemetryEvent {
                role: role(),
                tokens: 0,
                cost_micro_usd: 0,
            }),
            EventKind::Budget(BudgetEvent {
                role: role(),
                role_spent: 0,
                role_cap: None,
                crew_spent: 0,
                crew_budget: None,
                breach: None,
            }),
            // The landmine: `mission` is folded by the stats rollup yet absent
            // from `EventKindTag`, so it must be kept, not pruned.
            EventKind::Mission(MissionEvent {
                summary: String::new(),
            }),
        ];
        for kind in &keep {
            assert!(
                is_state_bearing(kind),
                "a projection rebuilds from this kind, so it must be kept: {kind:?}",
            );
        }

        let prune = [
            message_kind(),
            EventKind::Activity(Activity::TurnStarted),
            EventKind::Boundary(BoundaryEvent {
                role: role(),
                path: "x".to_owned(),
                blocked: false,
            }),
            EventKind::Usage(UsageEvent {
                percent: 0,
                window_reset: None,
                paused: false,
            }),
            EventKind::Stall(StallEvent {
                kind: StallKind::Deadlock,
                status: StallStatus::Detected,
                roles: Vec::new(),
                detail: String::new(),
            }),
        ];
        for kind in &prune {
            assert!(
                !is_state_bearing(kind),
                "no projection rebuilds from this kind, so it is prunable: {kind:?}",
            );
        }

        // Every kind is accounted for above, so the two sets partition the enum.
        assert_eq!(
            keep.len() + prune.len(),
            12,
            "every EventKind is classified"
        );
    }

    #[test]
    fn survives_retention_keeps_state_recent_and_the_newest_anchor() {
        let newest = 7;
        // Aged, ephemeral, and not the newest: the only combination that prunes.
        assert!(!survives_retention(
            &StoredEvent {
                seq: 3,
                event: of_kind(message_kind(), ts(1)),
            },
            ts(5),
            newest,
        ));
        // State-bearing at any age is kept.
        assert!(survives_retention(
            &StoredEvent {
                seq: 4,
                event: of_kind(EventKind::Lifecycle(Lifecycle::Started), ts(1)),
            },
            ts(5),
            newest,
        ));
        // An ephemeral event at or after the cutoff is kept.
        assert!(survives_retention(
            &StoredEvent {
                seq: 5,
                event: of_kind(message_kind(), ts(9)),
            },
            ts(5),
            newest,
        ));
        // The newest event is kept even when aged and ephemeral: the high-water
        // anchor a restart reconstructs `next_seq` from.
        assert!(survives_retention(
            &StoredEvent {
                seq: newest,
                event: of_kind(message_kind(), ts(1)),
            },
            ts(5),
            newest,
        ));
    }

    #[test]
    fn retain_prunes_aged_ephemeral_while_sequences_stay_monotonic() {
        let store = MemoryStore::default();
        let aged = store.append(of_kind(message_kind(), ts(1)));
        let kept_state = store.append(of_kind(EventKind::Lifecycle(Lifecycle::Started), ts(1)));
        let newest = store.append(of_kind(message_kind(), ts(1)));
        assert_eq!(
            (aged, kept_state, newest),
            (0, 1, 2),
            "sequences are assigned in order"
        );

        // Cutoff after every event, so age alone does not save the messages: the
        // kind and the newest-anchor rule decide.
        let pruned = store.retain(ts(5));
        assert_eq!(pruned, 1, "only the aged, non-newest message is pruned");

        let seqs: Vec<u64> = store.stored_events().iter().map(|s| s.seq).collect();
        assert_eq!(
            seqs,
            vec![kept_state, newest],
            "the state-bearing event and the newest anchor survive; the aged message does not",
        );

        // A prune never rewinds the counter, so the next append continues past
        // every sequence ever assigned rather than reusing the pruned one.
        assert_eq!(store.next_seq(), 3, "the counter is not rewound by a prune");
        assert_eq!(
            store.append(of_kind(message_kind(), ts(9))),
            3,
            "no sequence is reused"
        );
    }

    #[test]
    fn log_store_persists_sequences_and_a_pruned_restart_never_reuses_one() {
        let dir = TempDir::new();

        // Append three aged events, then prune: the middle (state-bearing) and the
        // newest (anchor) survive; the first (aged, ephemeral) is dropped.
        let store = LogStore::open(dir.path()).unwrap();
        store.append(of_kind(message_kind(), ts(1)));
        store.append(of_kind(EventKind::Lifecycle(Lifecycle::Started), ts(1)));
        store.append(of_kind(message_kind(), ts(1)));
        assert_eq!(store.retain(ts(5)), 1, "the aged message is pruned");
        // Barrier through the writer so the on-disk rewrite has landed.
        store.flush();
        let seqs: Vec<u64> = store.stored_events().iter().map(|s| s.seq).collect();
        assert_eq!(
            seqs,
            vec![1, 2],
            "the survivors keep their stored sequences"
        );
        drop(store);

        // A restart replays the pruned file: the surviving sequences are stable,
        // and `next_seq` is reconstructed past the highest on disk, so the next
        // append never collides with the pruned sequence 0.
        let reopened = LogStore::open(dir.path()).unwrap();
        let replayed: Vec<u64> = reopened.stored_events().iter().map(|s| s.seq).collect();
        assert_eq!(
            replayed,
            vec![1, 2],
            "the on-disk prune and its sequences survive the restart"
        );
        assert_eq!(
            reopened.next_seq(),
            3,
            "next_seq is reconstructed past the highest on disk"
        );
        assert_eq!(
            reopened.append(of_kind(message_kind(), ts(9))),
            3,
            "no sequence is reused"
        );
    }

    /// A message event of `kind` at `at`, returning its minted id so a test can
    /// look it up by id (issue #273).
    fn message_event(kind: MessageKind, at: Timestamp) -> (MessageId, Event) {
        let id = MessageId::new();
        let event = Event {
            ts: at,
            from: Sender::Role(RoleId::new("commander")),
            channel: ChannelId::new("all-units"),
            task: None,
            kind: EventKind::Message(Message {
                id,
                kind,
                body: String::new(),
            }),
        };
        (id, event)
    }

    #[test]
    fn message_looks_up_a_stored_message_by_id() {
        let store = MemoryStore::default();
        let (question_id, question) =
            message_event(MessageKind::Question { options: vec![] }, ts(1));
        let (note_id, note) = message_event(MessageKind::Note, ts(3));
        store.append(question);
        // A non-message event between them must not disturb the by-id positions.
        store.append(of_kind(EventKind::Lifecycle(Lifecycle::Started), ts(2)));
        store.append(note);

        let found = store.message(&question_id).expect("the question is stored");
        assert_eq!(
            found.id, question_id,
            "the lookup returns the right message"
        );
        assert!(
            matches!(found.kind, MessageKind::Question { .. }),
            "with its kind intact, so a caller can check it is a question",
        );
        assert!(matches!(
            store.message(&note_id).map(|message| message.kind),
            Some(MessageKind::Note),
        ));
        assert!(
            store.message(&MessageId::new()).is_none(),
            "an unknown id resolves to None, not a wrong message",
        );
    }

    #[test]
    fn message_index_is_rebuilt_after_a_prune() {
        // A pruned message's id stops resolving and a survivor still does, proving
        // the by-id index tracks the log across a retention prune (issue #273).
        let store = MemoryStore::default();
        let (aged_id, aged) = message_event(MessageKind::Note, ts(1));
        let (newest_id, newest) = message_event(MessageKind::Note, ts(1));
        store.append(aged);
        store.append(newest);

        // Cutoff after both: both are aged and prunable, so only the newest
        // survives (the sequence high-water anchor).
        assert_eq!(store.retain(ts(5)), 1, "the older message is pruned");
        assert!(
            store.message(&aged_id).is_none(),
            "the pruned message no longer resolves",
        );
        assert!(
            store.message(&newest_id).is_some(),
            "the surviving message still resolves after the index rebuild",
        );
    }

    #[test]
    fn log_store_message_lookup_survives_a_restart() {
        let dir = TempDir::new();
        let store = LogStore::open(dir.path()).unwrap();
        let (id, event) = message_event(MessageKind::Question { options: vec![] }, ts(1));
        store.append(event);
        store.flush();
        drop(store);

        // The by-id index is reconstructed from the replayed log, so the lookup is
        // O(1) after a restart too (issue #273).
        let reopened = LogStore::open(dir.path()).unwrap();
        assert!(
            matches!(
                reopened.message(&id).map(|message| message.kind),
                Some(MessageKind::Question { .. }),
            ),
            "the reopened store resolves the message from the reconstructed index",
        );
    }
}
