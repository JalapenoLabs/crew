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
//! event, [`flush`](Storage::flush) pending writes, [`query`](Storage::query)
//! the log with filters and a stable page cursor, read every event or only
//! those after a cursor ([`events_since`](Storage::events_since), so an SSE
//! replay is O(gap), issue #225), and read or write the [`Roster`]. `query` has
//! a default that scans the in-memory index;
//! a backend with a real index (a database) overrides it to push the filter
//! down, which is why the query types here stay backend-neutral.

use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        mpsc, Arc, Mutex, PoisonError,
    },
    thread::JoinHandle,
};

use crew_core::{Channel, ChannelId, Event, EventKind, RoleId, Sender, TaskId, Timestamp};
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
    /// Equal to the current event count, so an event's sequence is its position
    /// in the log. It is the cursor the inbox stream hands out as a
    /// `Last-Event-ID`, letting a reconnecting subscriber resume exactly
    /// after the last event it saw.
    fn next_seq(&self) -> u64;

    /// Records an event in the log.
    ///
    /// A durable backend may persist in the background, so the event is not
    /// guaranteed on disk when this returns; call [`flush`](Storage::flush) to
    /// wait for durability. Never blocks on disk I/O. A persist failure is not
    /// silent, though: the event stays in the in-memory index and the failure
    /// is counted in [`durability`](Storage::durability) so `GET /health`
    /// can report degraded durability (issues #206, #207).
    fn append(&self, event: Event);

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

    /// Returns every stored event, oldest first.
    fn events(&self) -> Vec<Event>;

    /// Returns the events at position `after` and later (sequence `>= after`),
    /// oldest first.
    ///
    /// This bounds a replay to the gap after a cursor rather than the whole log
    /// (issue #225): the SSE resume engine reads only what a reconnecting
    /// client missed, so a connect is O(gap) instead of O(log).
    /// The default clones the full log via [`events`](Storage::events) and
    /// slices, so it is correct for any backend; a backend with an index
    /// (the in-memory stores below, a database later) overrides it to seek.
    /// `after` past the end yields an empty slice.
    fn events_since(&self, after: u64) -> Vec<Event> {
        let events = self.events();
        let start = usize::try_from(after)
            .unwrap_or(usize::MAX)
            .min(events.len());
        events[start..].to_vec()
    }

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
        // base_seq 0: the in-memory log is never trimmed yet (issue #208).
        query_events(&self.events(), 0, query)
    }
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
/// `base_seq` is the sequence number of `events[0]`: `0` while the log is
/// untrimmed, and the count already dropped once compaction or pruning lands
/// (issue #208), so an event's `seq` is `base_seq + its index` and stays stable
/// across a trim. Orders matches by `(ts, seq)` and resumes strictly after
/// `query.after`'s key, which the cursor carries directly, so paging never
/// depends on an event still sitting at a given index.
fn query_events(events: &[Event], base_seq: u64, query: &EventQuery) -> EventPage {
    let boundary = query.after.map(Cursor::key);

    let mut matched: Vec<(u64, &Event)> = events
        .iter()
        .enumerate()
        .map(|(index, event)| (base_seq + index as u64, event))
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
    events: Mutex<Vec<Event>>,
    roster: Mutex<Roster>,
}

impl Storage for MemoryStore {
    fn backend(&self) -> &'static str {
        "memory"
    }

    fn next_seq(&self) -> u64 {
        self.events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .len() as u64
    }

    fn append(&self, event: Event) {
        self.events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(event);
    }

    fn events(&self) -> Vec<Event> {
        self.events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn events_since(&self, after: u64) -> Vec<Event> {
        let events = self.events.lock().unwrap_or_else(PoisonError::into_inner);
        let start = usize::try_from(after)
            .unwrap_or(usize::MAX)
            .min(events.len());
        events[start..].to_vec()
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
/// blocks on disk. A `Line` carries a serialized event; a `Barrier` lets a
/// caller wait until everything queued before it is flushed (see
/// [`LogStore::flush`]).
enum LogWrite {
    /// Persist one serialized event line.
    Line(String),
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
    /// The in-memory index: every event, oldest first.
    events: Vec<Event>,
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
        let events = replay(&log_path)?;
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log_path)
            .wrap_err_with(|| format!("could not open event log {}", log_path.display()))?;

        // Persist on a dedicated thread so an append never blocks the async
        // runtime on disk I/O (issue #206). The queue is unbounded, which suits a
        // broker at message rates with occasional bursts; a producer sustained
        // faster than the disk is out of scope. The writer shares `durability` so
        // a background write failure is counted for `GET /health` (issue #207).
        let durability = Arc::new(DurabilityState::default());
        let writer_durability = Arc::clone(&durability);
        let (writer, requests) = mpsc::channel();
        let writer_thread = std::thread::Builder::new()
            .name("crew-log-writer".to_owned())
            .spawn(move || run_writer(BufWriter::new(file), &requests, &writer_durability))
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

        Ok(Self {
            log: Mutex::new(Log {
                events,
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

    fn append(&self, event: Event) {
        let mut log = self.log.lock().unwrap_or_else(PoisonError::into_inner);
        // Encode and enqueue under the lock, so the writer receives events in the
        // same order they enter the index and the file's lines stay aligned with
        // memory. The send is non-blocking; the writer thread does the disk I/O
        // and records any write failure. The event is indexed regardless, so a
        // persist failure degrades only durability, not the running broker's
        // consistency (issues #206, #207).
        match serde_json::to_string(&event) {
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
        log.events.push(event);
    }

    fn flush(&self) {
        LogStore::flush(self);
    }

    fn durability(&self) -> Durability {
        self.durability.snapshot()
    }

    fn events(&self) -> Vec<Event> {
        self.log
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .events
            .clone()
    }

    fn events_since(&self, after: u64) -> Vec<Event> {
        let log = self.log.lock().unwrap_or_else(PoisonError::into_inner);
        let start = usize::try_from(after)
            .unwrap_or(usize::MAX)
            .min(log.events.len());
        log.events[start..].to_vec()
    }

    fn next_seq(&self) -> u64 {
        self.log
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .events
            .len() as u64
    }

    fn query(&self, query: &EventQuery) -> EventPage {
        let log = self.log.lock().unwrap_or_else(PoisonError::into_inner);
        // base_seq 0: the on-disk log is never trimmed yet (issue #208).
        query_events(&log.events, 0, query)
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
    mut writer: BufWriter<File>,
    requests: &mpsc::Receiver<LogWrite>,
    durability: &DurabilityState,
) {
    while let Ok(first) = requests.recv() {
        let mut barriers = Vec::new();
        write_request(&mut writer, first, &mut barriers, durability);
        while let Ok(next) = requests.try_recv() {
            write_request(&mut writer, next, &mut barriers, durability);
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
    }
}

/// Replays the on-disk log into memory, skipping any unreadable line.
fn replay(path: &Path) -> Result<Vec<Event>> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(err) => {
            return Err(err)
                .wrap_err_with(|| format!("could not read event log {}", path.display()))
        }
    };

    let mut events = Vec::new();
    for (index, line) in BufReader::new(file).lines().enumerate() {
        let line = line.wrap_err_with(|| format!("could not read event log {}", path.display()))?;
        if line.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<Event>(&line) {
            Ok(event) => events.push(event),
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
    Ok(events)
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
        ChannelId, Event, EventKind, Lifecycle, Message, MessageId, MessageKind, RoleId, Sender,
        Timestamp,
    };

    use super::{
        query_events, write_request, Cursor, DurabilityState, EventFilter, EventKindTag,
        EventQuery, Liveness, LogStore, LogWrite, MemoryStore, RoleStatus, Storage,
    };

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

    /// The untrimmed base sequence the store uses today (issue #208).
    const UNTRIMMED: u64 = 0;

    #[test]
    fn query_orders_by_timestamp_then_position() {
        let log = vec![
            message("backend", "all-units", ts(3)),
            message("backend", "all-units", ts(1)),
            message("backend", "all-units", ts(2)),
        ];
        let page = query_events(&log, UNTRIMMED, &unfiltered(None, 10));
        let times: Vec<_> = page.events.iter().map(|event| event.ts).collect();
        assert_eq!(times, vec![ts(1), ts(2), ts(3)]);
        assert!(page.next.is_none());
    }

    #[test]
    fn query_paging_is_stable_when_events_are_appended_between_pages() {
        let mut log: Vec<Event> = (0..20)
            .map(|i| message("backend", "all-units", ts(i)))
            .collect();

        let page1 = query_events(&log, UNTRIMMED, &unfiltered(None, 8));
        assert_eq!(page1.events.len(), 8);

        // A concurrent writer appends newer events after page 1 was read.
        log.push(message("frontend", "all-units", ts(40)));
        log.push(message("frontend", "all-units", ts(41)));

        let page2 = query_events(&log, UNTRIMMED, &unfiltered(page1.next, 8));
        let page3 = query_events(&log, UNTRIMMED, &unfiltered(page2.next, 8));

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
        let log = vec![
            message("backend", "all-units", ts(1)),
            message("frontend", "all-units", ts(2)),
            Event {
                kind: EventKind::Lifecycle(Lifecycle::Started),
                ..message("backend", "all-units", ts(3))
            },
            message("backend", "@backend", ts(4)),
        ];

        let filtered = |filter: EventFilter| {
            query_events(
                &log,
                UNTRIMMED,
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
        let log = vec![message("backend", "frontend+backend", ts(1))];
        let page = query_events(
            &log,
            UNTRIMMED,
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
        let log = vec![message("backend", "all-units", ts(1))];
        let beyond = Cursor { ts: ts(9), seq: 99 };
        let page = query_events(&log, UNTRIMMED, &unfiltered(Some(beyond), 10));
        assert!(page.events.is_empty(), "no event sorts after the cursor");
        assert!(page.next.is_none(), "and so no next page");
    }

    #[test]
    fn a_cursor_is_stable_when_earlier_events_are_trimmed() {
        // The cursor carries `(ts, seq)`, and `seq` is `base_seq + index`, so a
        // page fetched before a trim and resumed after one neither repeats nor
        // skips an event even though every surviving index shifted (issue #208).
        let log: Vec<Event> = (0..10)
            .map(|i| message("backend", "all-units", ts(i)))
            .collect();

        // Page 1 over the full log: the first four events, cursor at the fourth.
        let page1 = query_events(&log, UNTRIMMED, &unfiltered(None, 4));
        let seen: Vec<Timestamp> = page1.events.iter().map(|event| event.ts).collect();
        assert_eq!(seen, (0..4).map(ts).collect::<Vec<_>>());
        let cursor = page1.next.expect("a fourth event remains");

        // Now trim the first three events: the survivors keep their seqs because
        // `base_seq` advances by the trimmed count.
        let trimmed = &log[3..];
        let base = 3;
        let page2 = query_events(trimmed, base, &unfiltered(Some(cursor), 4));
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
            assert_eq!(
                store.events_since(0),
                store.events(),
                "since 0 is the whole log"
            );
            let tail = store.events_since(1);
            assert_eq!(tail.len(), 2, "since 1 skips the first event");
            assert_eq!(tail[0].ts, ts(2), "the slice starts at the cursor position");
            assert_eq!(
                store.events_since(1),
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
}
