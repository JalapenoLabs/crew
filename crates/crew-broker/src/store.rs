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
//! the log with filters and a stable page cursor, read every event, and read or
//! write the [`Roster`]. `query` has a default that scans the in-memory index;
//! a backend with a real index (a database) overrides it to push the filter
//! down, which is why the query types here stay backend-neutral.

use std::{
    collections::BTreeMap,
    fs::{File, OpenOptions},
    io::{BufRead, BufReader, BufWriter, Write},
    path::{Path, PathBuf},
    sync::{mpsc, Mutex, PoisonError},
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
    /// wait for durability. Never blocks on disk I/O.
    fn append(&self, event: Event);

    /// Blocks until every appended event is durably persisted.
    ///
    /// The default is a no-op, for a backend that persists synchronously or
    /// holds nothing on disk. A background-writing backend overrides it to
    /// drain its queue, which the broker calls on graceful shutdown so a
    /// burst still in flight reaches disk before the process exits.
    fn flush(&self) {}

    /// Returns every stored event, oldest first.
    fn events(&self) -> Vec<Event>;

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
    /// paging down to the store.
    ///
    /// # Errors
    /// Returns [`InvalidCursor`] if the query's `after` cursor is not a stored
    /// position.
    fn query(&self, query: &EventQuery) -> Result<EventPage, InvalidCursor> {
        query_events(&self.events(), query)
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

/// A page request: the filter, an optional resume cursor, and a size limit.
#[derive(Debug, Clone)]
pub struct EventQuery {
    /// Which events to keep.
    pub filter: EventFilter,
    /// Resume after this log position (from a previous page's
    /// [`EventPage::next`]).
    pub after: Option<u64>,
    /// The maximum number of events to return.
    pub limit: usize,
}

/// One page of query results, ordered by `ts` then log position.
#[derive(Debug)]
pub struct EventPage {
    /// The matching events for this page, oldest first.
    pub events: Vec<Event>,
    /// The cursor for the next page (a log position), absent on the last page.
    pub next: Option<u64>,
}

/// The query's `after` cursor did not point at a stored event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InvalidCursor;

/// Runs a query over an in-memory event slice, the scan both backends share.
///
/// Orders matches by `(ts, position)`, a total order the cursor resumes from:
/// since the log is append-only, a position never moves, so paging stays stable
/// under concurrent writes. Returns the page and the position to resume after,
/// if any.
fn query_events(events: &[Event], query: &EventQuery) -> Result<EventPage, InvalidCursor> {
    // Resolve the cursor to the `(ts, position)` boundary to resume strictly after.
    let boundary = match query.after {
        Some(position) => {
            let event = events
                .get(usize::try_from(position).unwrap_or(usize::MAX))
                .ok_or(InvalidCursor)?;
            Some((event.ts, position))
        }
        None => None,
    };

    let mut matched: Vec<(u64, &Event)> = events
        .iter()
        .enumerate()
        .map(|(position, event)| (position as u64, event))
        .filter(|(_, event)| query.filter.matches(event))
        .collect();
    matched.sort_by(|a, b| a.1.ts.cmp(&b.1.ts).then_with(|| a.0.cmp(&b.0)));

    let start = match boundary {
        Some(key) => matched.partition_point(|(position, event)| (event.ts, *position) <= key),
        None => 0,
    };
    let rest = &matched[start..];
    let take = rest.len().min(query.limit);
    let page = rest[..take]
        .iter()
        .map(|(_, event)| (*event).clone())
        .collect();
    let next = (rest.len() > take).then(|| rest[take - 1].0);

    Ok(EventPage { events: page, next })
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
/// The roster persists to its own file, rewritten atomically on each change. A
/// torn or unreadable line (for example a partial write from a crash) is
/// skipped on replay so one bad line never loses the rest of the log.
#[derive(Debug)]
pub struct LogStore {
    log: Mutex<Log>,
    writer_thread: Option<JoinHandle<()>>,
    roster: Mutex<Roster>,
    roster_path: PathBuf,
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
        // faster than the disk is out of scope.
        let (writer, requests) = mpsc::channel();
        let writer_thread = std::thread::Builder::new()
            .name("crew-log-writer".to_owned())
            .spawn(move || run_writer(BufWriter::new(file), &requests))
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
        // memory. The send is non-blocking; the writer thread does the disk I/O.
        // The event is indexed regardless, so a persist failure degrades only
        // durability, not the running broker's consistency.
        match serde_json::to_string(&event) {
            Ok(line) => {
                if let Some(writer) = log.writer.as_ref() {
                    if writer.send(LogWrite::Line(line)).is_err() {
                        event!(
                            name: "broker.store.persist.failed",
                            Level::ERROR,
                            "the log writer thread is gone; keeping the event in memory only",
                        );
                    }
                }
            }
            Err(err) => event!(
                name: "broker.store.encode.failed",
                Level::ERROR,
                error = %err,
                "could not encode event for the log; keeping it in memory only",
            ),
        }
        log.events.push(event);
    }

    fn flush(&self) {
        LogStore::flush(self);
    }

    fn events(&self) -> Vec<Event> {
        self.log
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .events
            .clone()
    }

    fn next_seq(&self) -> u64 {
        self.log
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .events
            .len() as u64
    }

    fn query(&self, query: &EventQuery) -> Result<EventPage, InvalidCursor> {
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
        save_roster(&self.roster_path, &guard);
        existed
    }

    fn deregister_role(&self, role: &RoleId) -> Option<RoleStatus> {
        let mut guard = self.roster.lock().unwrap_or_else(PoisonError::into_inner);
        let prior = guard.deregister(role);
        if prior.is_some() {
            save_roster(&self.roster_path, &guard);
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
/// having flushed the backlog.
fn run_writer(mut writer: BufWriter<File>, requests: &mpsc::Receiver<LogWrite>) {
    while let Ok(first) = requests.recv() {
        let mut barriers = Vec::new();
        write_request(&mut writer, first, &mut barriers);
        while let Ok(next) = requests.try_recv() {
            write_request(&mut writer, next, &mut barriers);
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
        }
        // Acknowledge each barrier now that its prior lines are flushed.
        for barrier in barriers {
            let _ = barrier.send(());
        }
    }
}

/// Applies one write request: buffer a line, or hold a barrier's
/// acknowledgement until the surrounding batch flushes.
fn write_request(
    writer: &mut BufWriter<File>,
    request: LogWrite,
    barriers: &mut Vec<mpsc::Sender<()>>,
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

/// Persists the roster, logging (not propagating) a write failure so a roster
/// change never fails the request; the change stays in memory until the disk
/// recovers.
fn save_roster(path: &Path, roster: &Roster) {
    if let Err(err) = persist_roster(path, roster) {
        event!(
            name: "broker.store.roster.persist.failed",
            Level::ERROR,
            error = %err,
            "could not persist roster; keeping it in memory only",
        );
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
        path::{Path, PathBuf},
        sync::atomic::{AtomicU64, Ordering},
    };

    use crew_core::{
        ChannelId, Event, EventKind, Lifecycle, Message, MessageId, MessageKind, RoleId, Sender,
        Timestamp,
    };

    use super::{
        query_events, EventFilter, EventKindTag, EventQuery, InvalidCursor, Liveness, LogStore,
        MemoryStore, RoleStatus, Storage,
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

    fn unfiltered(after: Option<u64>, limit: usize) -> EventQuery {
        EventQuery {
            filter: EventFilter::default(),
            after,
            limit,
        }
    }

    #[test]
    fn query_orders_by_timestamp_then_position() {
        let log = vec![
            message("backend", "all-units", ts(3)),
            message("backend", "all-units", ts(1)),
            message("backend", "all-units", ts(2)),
        ];
        let page = query_events(&log, &unfiltered(None, 10)).unwrap();
        let times: Vec<_> = page.events.iter().map(|event| event.ts).collect();
        assert_eq!(times, vec![ts(1), ts(2), ts(3)]);
        assert!(page.next.is_none());
    }

    #[test]
    fn query_paging_is_stable_when_events_are_appended_between_pages() {
        let mut log: Vec<Event> = (0..20)
            .map(|i| message("backend", "all-units", ts(i)))
            .collect();

        let page1 = query_events(&log, &unfiltered(None, 8)).unwrap();
        assert_eq!(page1.events.len(), 8);

        // A concurrent writer appends newer events after page 1 was read.
        log.push(message("frontend", "all-units", ts(40)));
        log.push(message("frontend", "all-units", ts(41)));

        let page2 = query_events(&log, &unfiltered(page1.next, 8)).unwrap();
        let page3 = query_events(&log, &unfiltered(page2.next, 8)).unwrap();

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
                &EventQuery {
                    filter,
                    after: None,
                    limit: 100,
                },
            )
            .unwrap()
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
            &EventQuery {
                filter: EventFilter {
                    channel: Some(ChannelId::new("backend+frontend")),
                    ..EventFilter::default()
                },
                after: None,
                limit: 100,
            },
        )
        .unwrap();
        assert_eq!(page.events.len(), 1);
    }

    #[test]
    fn query_rejects_a_cursor_past_the_end() {
        let log = vec![message("backend", "all-units", ts(1))];
        assert!(matches!(
            query_events(&log, &unfiltered(Some(99), 10)),
            Err(InvalidCursor)
        ));
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
}
