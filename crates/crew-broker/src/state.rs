//! The broker's shared application state.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, PoisonError},
    time::Duration,
};

use crew_core::{
    BoardEvent, BoardSection, Event, EventKind, Lifecycle, RoleId, Sender, TelemetryEvent,
    Timestamp, Verdict, VerificationEvent,
};
use serde::Serialize;
use tokio::sync::broadcast;

use crate::{
    config::Config,
    ledger::Ledger,
    router::ChannelRouter,
    secrets::Scrubber,
    store::{MemoryStore, Storage},
};

/// The crew's control standing: whether the General has gated the crew's work
/// (issue #41).
///
/// It rises from `Running` to `Paused` (the brake) to `StoodDown` (the kill
/// switch). Under `Paused` or `StoodDown` every role is gated regardless of its
/// own pause flag; `Running` leaves each role to its own
/// [`is_role_paused`](AppState::is_role_paused) state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Standing {
    /// Normal: roles pull work as usual.
    #[default]
    Running,
    /// Globally paused: no role pulls new work until the crew is resumed.
    Paused,
    /// Stood down: an emergency halt. Every role stops and the durable state is
    /// preserved, so the crew is recoverable.
    StoodDown,
}

/// The crew-wide control state: the standing, plus the individually paused
/// roles.
#[derive(Debug, Default)]
struct Control {
    standing: Standing,
    paused_roles: BTreeSet<RoleId>,
}

/// The shared-subscription usage gauge and auto-pause (issue #56).
///
/// The crew shares one subscription, so the broker keeps one gauge across the
/// crew. A usage report at or above the [`threshold`](Usage::threshold)
/// auto-pauses new work until the window resets; the gate lifts lazily at the
/// reset instant (the pause event advertises it), and the operator can resume
/// early. It is distinct from the manual pause control, so an auto-pause never
/// clobbers a manual pause and the reset never lifts one.
#[derive(Debug)]
struct Usage {
    /// The usage percent (`0..=100`) at which new work auto-pauses.
    threshold: u8,
    /// The latest reported usage percent.
    percent: u8,
    /// When the auto-pause lifts, if the crew is usage-paused now; `None` when
    /// it is not.
    paused_until: Option<Timestamp>,
}

impl Usage {
    /// A gauge that auto-pauses at `threshold` percent, starting un-paused.
    fn new(threshold: u8) -> Self {
        Self {
            threshold,
            percent: 0,
            paused_until: None,
        }
    }

    /// Records a usage reading as of `now`, returning `true` if it newly
    /// engaged the pause.
    ///
    /// At or above the threshold it (re)arms the auto-pause to lift at
    /// `window_reset`; a reading below the threshold leaves an active pause
    /// in place, since the window reset is the clear signal, not a dip.
    /// "Newly" is measured against whether the crew was usage-paused a
    /// moment ago, so a pause that already expired can engage afresh.
    fn report(&mut self, now: Timestamp, percent: u8, window_reset: Timestamp) -> bool {
        self.percent = percent;
        if percent < self.threshold {
            return false;
        }
        let was_paused = self.is_paused(now);
        self.paused_until = Some(window_reset);
        !was_paused
    }

    /// Lifts the auto-pause early, returning `true` if one was active as of
    /// `now`.
    fn resume(&mut self, now: Timestamp) -> bool {
        let was_paused = self.is_paused(now);
        self.paused_until = None;
        was_paused
    }

    /// Whether new work is auto-paused as of `now`: paused with the reset still
    /// ahead.
    fn is_paused(&self, now: Timestamp) -> bool {
        self.paused_until.is_some_and(|until| now < until)
    }

    /// Clears an auto-pause whose window has reset as of `now`, returning the
    /// reset instant that just passed, or `None` if none has expired.
    ///
    /// This is the lazy lift, made observable (issue #112). [`is_paused`]
    /// already reads `false` once `now` reaches the reset, but the armed
    /// `paused_until` lingers until this clears it; the broker's background
    /// sweep calls it and, on a `Some`, announces the lift on the stream.
    /// Clearing to `None` makes the clear idempotent, so a second sweep
    /// does not re-announce it.
    ///
    /// [`is_paused`]: Usage::is_paused
    fn expire(&mut self, now: Timestamp) -> Option<Timestamp> {
        match self.paused_until {
            Some(until) if now >= until => {
                self.paused_until = None;
                Some(until)
            }
            _ => None,
        }
    }
}

/// The shared-subscription usage gauge, as `GET /usage` reports it (issue #56).
#[derive(Debug, Clone, Serialize)]
pub struct UsageView {
    /// The latest reported usage percent (`0..=100`).
    pub percent: u8,
    /// The percent at which new work auto-pauses.
    pub threshold: u8,
    /// Whether new work is auto-paused right now.
    pub paused: bool,
    /// When the auto-pause lifts, while paused; `None` when it is not paused.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resets_at: Option<Timestamp>,
}

/// A task's live standing in the adversarial done-gate (issue #47).
///
/// It records who submitted the work and owns any rework, the independent role
/// that returned the latest verdict (if any), where the task stands, and the
/// acceptance being claimed or the failure on a handback. See
/// [`AppState::record_verdict`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GateEntry {
    /// The role that submitted the work and owns any rework.
    pub owner: RoleId,
    /// The independent role that returned the latest verdict, absent until one
    /// lands.
    pub verifier: Option<RoleId>,
    /// Where the task stands: submitted, passed, or failed.
    pub verdict: Verdict,
    /// The acceptance being claimed (while submitted) or the failure (on a
    /// handback).
    pub detail: String,
}

/// The adversarial done-gate: every task under verification, keyed by its
/// title.
///
/// A task title is a human label (the order's title), so the operator and the
/// crew name the same work the same way. Like the situation board (issue #49),
/// the gate is a projection of the `verification` events in the durable log, so
/// it is rebuilt on a restart rather than lost: a task mid-verification
/// survives a broker restart (issue #181). The broker is still the live
/// authority that enforces the gate, and records every transition as a
/// `verification` event.
#[derive(Debug, Default)]
struct Gate {
    tasks: BTreeMap<String, GateEntry>,
}

impl Gate {
    /// Rebuilds the gate by folding every `verification` event in the log,
    /// oldest first (issue #181).
    ///
    /// This is how the gate survives a restart, mirroring the board (issue
    /// #49): the durable log is the source of truth, and replaying it restores
    /// the live standing of every task under the gate.
    fn rebuild(events: &[Event]) -> Self {
        let mut gate = Self::default();
        for event in events {
            if let EventKind::Verification(change) = &event.kind {
                gate.apply(change);
            }
        }
        gate
    }

    /// Applies one `verification` event: each carries the task's full standing
    /// at that moment (owner, verifier, verdict, detail), so the latest event
    /// per task wins.
    ///
    /// A submission records the task [`Submitted`](Verdict::Submitted) awaiting
    /// a verifier; a verdict replaces it with [`Passed`](Verdict::Passed) or
    /// [`Failed`](Verdict::Failed); a resubmission after a failure records
    /// `Submitted` again. Folding in log order reconstructs exactly what the
    /// live [`submit_for_verification`](AppState::submit_for_verification) and
    /// [`record_verdict`](AppState::record_verdict) writes produced.
    fn apply(&mut self, change: &VerificationEvent) {
        self.tasks.insert(
            change.task.clone(),
            GateEntry {
                owner: change.owner.clone(),
                verifier: change.verifier.clone(),
                verdict: change.verdict,
                detail: change.detail.clone(),
            },
        );
    }
}

/// Why the broker refused a verdict on a task (issue #47).
///
/// The done-gate refuses a verdict that would let confident-but-wrong work
/// through: one from the task's own owner, or one on a task that is not
/// awaiting verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerdictError {
    /// No such task was submitted for verification.
    UnknownTask,
    /// A role cannot verify its own work; an independent role must try to break
    /// it.
    SelfVerification,
    /// The task is not awaiting a verdict: it already carries this one.
    NotAwaitingVerification(Verdict),
}

/// The result of recording a verdict, for the endpoint to publish and route
/// (issue #47).
#[derive(Debug, Clone)]
pub struct VerdictOutcome {
    /// The role whose work was judged, and to hand back to on a failure.
    pub owner: RoleId,
    /// The independent role that returned the verdict.
    pub verifier: RoleId,
    /// The resulting standing: [`Passed`](Verdict::Passed) or
    /// [`Failed`](Verdict::Failed).
    pub verdict: Verdict,
    /// The failure detail on a handback; empty on a pass.
    pub detail: String,
}

/// One entry on the shared situation board: its section, author, and content
/// (issue #49).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BoardEntry {
    /// Which section it belongs to: a decision, an interface, or a gotcha.
    pub section: BoardSection,
    /// The role that last recorded it.
    pub author: RoleId,
    /// The entry's content: a decision and its rationale, an interface, or a
    /// gotcha.
    pub body: String,
}

/// Why the broker refused a board retraction (issue #180).
///
/// The board lets the whole crew record, but curation is the commander's: only
/// the entry's author or the crew's commander may retract one, so a stray role
/// cannot erase a decision the commander curated.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetractError {
    /// No entry is recorded under the key, so there is nothing to retract.
    NoSuchEntry,
    /// The requester is neither the entry's author nor the commander.
    Unauthorized {
        /// The entry's author, who (with the commander) may retract it.
        author: RoleId,
    },
}

/// The shared situation board: the crew's durable memory, keyed by entry topic
/// (issue #49).
///
/// A projection of the `board` events in the durable log, so it is rebuilt on a
/// restart rather than kept in a separate persistence path: recording an entry
/// inserts or replaces it, retracting one removes it. Keyed and sorted by topic
/// for a stable read order.
#[derive(Debug, Default)]
struct Board {
    entries: BTreeMap<String, BoardEntry>,
}

impl Board {
    /// Rebuilds the board by folding every `board` event in the log, oldest
    /// first.
    ///
    /// This is how the board survives a restart (issue #49): the durable log is
    /// the source of truth, and replaying it restores the live projection.
    fn rebuild(events: &[Event]) -> Self {
        let mut board = Self::default();
        for event in events {
            if let EventKind::Board(change) = &event.kind {
                board.apply(change);
            }
        }
        board
    }

    /// Applies one board change: a record inserts or replaces the entry, a
    /// retraction removes it.
    fn apply(&mut self, change: &BoardEvent) {
        if change.retracted {
            self.entries.remove(&change.key);
        } else {
            self.entries.insert(
                change.key.clone(),
                BoardEntry {
                    section: change.section,
                    author: change.author.clone(),
                    body: change.body.clone(),
                },
            );
        }
    }
}

/// One role's usage rollup: its cumulative tokens, cost, and working time
/// (issue #55).
#[derive(Debug, Clone, Default)]
struct RoleStats {
    /// Cumulative tokens the role spent, summed from its `telemetry` events.
    tokens: u64,
    /// Cumulative cost in micro-USD (millionths of a dollar), summed the same
    /// way.
    cost_micro_usd: u64,
    /// Working time already closed: the sum of the role's finished working
    /// intervals.
    active: Duration,
    /// When the role entered its current working interval, if it is working
    /// now. The snapshot adds the in-progress time up to the read instant
    /// so a live role's time keeps climbing between transitions.
    working_since: Option<Timestamp>,
}

impl RoleStats {
    /// Folds one of the role's `lifecycle` transitions into its working time.
    ///
    /// Entering the working state (started, restarted, recovered, resumed)
    /// opens an interval; leaving it (idle, stopped, died, paused, stood
    /// down) closes the open one and banks its duration. Re-entering while
    /// already working keeps the earlier start.
    fn apply_lifecycle(&mut self, state: Lifecycle, at: Timestamp) {
        match state {
            Lifecycle::Started
            | Lifecycle::Restarted
            | Lifecycle::Recovered
            | Lifecycle::Resumed => {
                self.working_since.get_or_insert(at);
            }
            Lifecycle::Idle
            | Lifecycle::Stopped
            | Lifecycle::Died
            | Lifecycle::Paused
            | Lifecycle::StoodDown => self.close_working(at),
        }
    }

    /// Closes the role's open working interval at `at`, folding it into the
    /// total. A no-op when the role was not working. Both a stopping lifecycle
    /// transition and a `mission` completion (issue #155) end the interval.
    fn close_working(&mut self, at: Timestamp) {
        if let Some(since) = self.working_since.take() {
            self.active += span(since, at);
        }
    }

    /// The role's total working time as of `now`, including any open interval.
    fn active_as_of(&self, now: Timestamp) -> Duration {
        match self.working_since {
            Some(since) => self.active + span(since, now),
            None => self.active,
        }
    }
}

/// The non-negative time from `start` to `end`; a backwards pair (clock skew)
/// counts as zero.
fn span(start: Timestamp, end: Timestamp) -> Duration {
    (end.to_datetime() - start.to_datetime())
        .to_std()
        .unwrap_or(Duration::ZERO)
}

/// One role's line in the `GET /stats` rollup: tokens, cost, and working
/// seconds (issue #55).
#[derive(Debug, Clone, Serialize)]
pub struct RoleStatsView {
    /// The role.
    pub role: RoleId,
    /// Cumulative tokens spent.
    pub tokens: u64,
    /// Cumulative cost in micro-USD (millionths of a dollar).
    pub cost_micro_usd: u64,
    /// Working time in whole seconds, including any interval still open at read
    /// time.
    pub active_secs: u64,
}

/// The usage rollup: cost, tokens, and working time per role and in aggregate
/// (issue #55).
///
/// A projection of the `telemetry` events (tokens, cost) and `lifecycle` events
/// (working time) in the durable log, so it is rebuilt on a restart like the
/// situation board rather than kept in a separate persistence path. It makes
/// spend legible per role and overall, feeding the cockpit and the Seraphim
/// stats rollup.
#[derive(Debug, Default)]
struct Stats {
    roles: BTreeMap<RoleId, RoleStats>,
}

impl Stats {
    /// Rebuilds the rollup by folding every `telemetry` and `lifecycle` event,
    /// oldest first.
    fn rebuild(events: &[Event]) -> Self {
        let mut stats = Self::default();
        for event in events {
            stats.apply(event);
        }
        stats
    }

    /// Folds one event into the rollup: usage adds tokens and cost, a lifecycle
    /// transition advances working time. Every other kind is ignored.
    fn apply(&mut self, event: &Event) {
        match &event.kind {
            EventKind::Telemetry(TelemetryEvent {
                role,
                tokens,
                cost_micro_usd,
            }) => {
                let entry = self.roles.entry(role.clone()).or_default();
                entry.tokens = entry.tokens.saturating_add(*tokens);
                entry.cost_micro_usd = entry.cost_micro_usd.saturating_add(*cost_micro_usd);
            }
            EventKind::Lifecycle(state) => {
                if let Sender::Role(role) = &event.from {
                    self.roles
                        .entry(role.clone())
                        .or_default()
                        .apply_lifecycle(*state, event.ts);
                }
            }
            // A mission completion (issue #155) closes the reporting role's open
            // working interval, the same way a stopping lifecycle transition does.
            EventKind::Mission(_) => {
                if let Sender::Role(role) = &event.from {
                    self.roles
                        .entry(role.clone())
                        .or_default()
                        .close_working(event.ts);
                }
            }
            _ => {}
        }
    }

    /// The per-role rollup as of `now`, sorted by role, each with its working
    /// time closed through `now`.
    fn snapshot(&self, now: Timestamp) -> Vec<RoleStatsView> {
        self.roles
            .iter()
            .map(|(role, stats)| RoleStatsView {
                role: role.clone(),
                tokens: stats.tokens,
                cost_micro_usd: stats.cost_micro_usd,
                active_secs: stats.active_as_of(now).as_secs(),
            })
            .collect()
    }
}

/// One role's line in the `GET /budget` snapshot: its cumulative spend and cap
/// (issues #54, #176).
#[derive(Debug, Clone, Serialize)]
pub struct RoleBudgetView {
    /// The role.
    pub role: RoleId,
    /// The role's cumulative token spend, from its latest `budget` event.
    pub spent: u64,
    /// The role's own cap, if it has one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cap: Option<u64>,
}

/// The crew-wide line in the `GET /budget` snapshot: total spend against the
/// crew budget (issues #54, #176).
#[derive(Debug, Clone, Serialize)]
pub struct CrewBudgetView {
    /// The crew's cumulative token spend across every role.
    pub spent: u64,
    /// The crew-wide budget, if the crew has one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub budget: Option<u64>,
}

/// The `GET /budget` snapshot: spend against budget per role and crew-wide
/// (issue #176).
#[derive(Debug, Clone, Serialize)]
pub struct BudgetView {
    /// One line per role that has reported spend, sorted by role.
    pub roles: Vec<RoleBudgetView>,
    /// The crew total across every role.
    pub crew: CrewBudgetView,
}

/// One role's budget standing: its latest cumulative spend and cap.
#[derive(Debug, Clone, Default)]
struct RoleBudget {
    /// The role's cumulative token spend, from its latest `budget` event.
    spent: u64,
    /// The role's own cap, if it has one.
    cap: Option<u64>,
}

/// The budget projection: cumulative spend against budget per role and
/// crew-wide (issues #54, #176).
///
/// A projection of the `budget` events in the durable log, so it is rebuilt on
/// a restart like the situation board (issue #49) rather than kept in a
/// separate persistence path. Each `budget` event carries the running totals
/// the supervisor computed from the crew's [`Budget`](crew_core::Budget), so
/// the fold is latest-wins: the newest event for a role carries its current
/// spend and cap, and the newest event overall carries the crew total and
/// budget. It backs `GET /budget`, so the cockpit (issue #51) reads a snapshot
/// rather than replaying events (issue #176).
#[derive(Debug, Default)]
struct Budgets {
    roles: BTreeMap<RoleId, RoleBudget>,
    crew_spent: u64,
    crew_budget: Option<u64>,
}

impl Budgets {
    /// Rebuilds the projection by folding every `budget` event, oldest first.
    fn rebuild(events: &[Event]) -> Self {
        let mut budgets = Self::default();
        for event in events {
            budgets.apply(event);
        }
        budgets
    }

    /// Folds one event into the projection: a `budget` report advances the
    /// role's spend and the crew total to the running totals it carries. Every
    /// other kind is ignored.
    fn apply(&mut self, event: &Event) {
        let EventKind::Budget(report) = &event.kind else {
            return;
        };
        // Each report is a snapshot of running totals, so latest wins: the newest
        // report for the role carries its current spend, and the newest overall the
        // crew total.
        let entry = self.roles.entry(report.role.clone()).or_default();
        entry.spent = report.role_spent;
        entry.cap = report.role_cap;
        self.crew_spent = report.crew_spent;
        self.crew_budget = report.crew_budget;
    }

    /// The snapshot for `GET /budget`: the per-role spend sorted by role, and
    /// the crew total.
    fn snapshot(&self) -> BudgetView {
        let roles = self
            .roles
            .iter()
            .map(|(role, budget)| RoleBudgetView {
                role: role.clone(),
                spent: budget.spent,
                cap: budget.cap,
            })
            .collect();
        BudgetView {
            roles,
            crew: CrewBudgetView {
                spent: self.crew_spent,
                budget: self.crew_budget,
            },
        }
    }
}

/// How many events a subscriber may fall behind before the broker drops the
/// oldest for it. Large enough to absorb a burst while a slow reader catches
/// up; a lagged subscriber reconnects with its `Last-Event-ID` and replays the
/// gap from the log, so nothing is lost. Raising it trades memory for a longer
/// tolerated stall.
const BROADCAST_CAPACITY: usize = 1024;

/// An event paired with its sequence number: its position in the append-only
/// log.
///
/// The sequence is the cursor the inbox stream emits as a Server-Sent-Event
/// `id`, so a reconnecting subscriber resumes exactly after the last event it
/// received.
#[derive(Debug, Clone)]
pub struct Sequenced {
    /// The event's position in the log, assigned on append.
    pub seq: u64,
    /// The event itself.
    pub event: Event,
}

/// The shared state every request handler sees.
///
/// Cheap to clone (each field is an [`Arc`] or a broadcast [`Sender`], which
/// shares its channel on clone), which axum requires since it clones the state
/// per request. It wires the [`Config`], the [`Storage`] backend, the
/// [`ChannelRouter`], the secret [`Scrubber`], and the live fan-out channel
/// together so handlers read them without global state.
///
/// [`Sender`]: broadcast::Sender
#[derive(Debug, Clone)]
pub struct AppState {
    /// The runtime configuration.
    pub config: Arc<Config>,
    /// The message storage backend (swappable; see [`Storage`]).
    pub storage: Arc<dyn Storage>,
    /// The channel router.
    pub router: Arc<ChannelRouter>,
    /// Masks configured secret values out of every event before it is stored or
    /// streamed.
    pub scrubber: Arc<Scrubber>,
    /// The fan-out channel a publish sends to and every subscriber stream
    /// reads.
    pub broadcast: broadcast::Sender<Sequenced>,
    /// Serializes [`publish`](AppState::publish) so a sequence number is
    /// broadcast in the same order it is assigned, keeping every
    /// subscriber's `id` cursor monotonic.
    publish_order: Arc<Mutex<()>>,
    /// The crew's pause / stand-down state (issue #41). In memory: the broker
    /// is the live recoverable authority, and every pause change is also
    /// recorded as a `lifecycle` event in the durable log.
    control: Arc<Mutex<Control>>,
    /// The shared work ledger (issue #45): who holds which task. In memory,
    /// with every claim also recorded as a `ledger` event in the durable
    /// log.
    ledger: Arc<Mutex<Ledger>>,
    /// The adversarial done-gate: tasks under verification (issue #47). A
    /// projection of the `verification` events in the log, so it is rebuilt
    /// from the log on a restart like the board (issue #181); the broker stays
    /// the live authority enforcing it.
    gate: Arc<Mutex<Gate>>,
    /// The shared situation board: the crew's durable memory (issue #49). A
    /// projection of the `board` events in the log, so it is rebuilt from
    /// the log on a restart.
    board: Arc<Mutex<Board>>,
    /// The usage rollup: cost, tokens, and working time per role (issue #55). A
    /// projection of the `telemetry` and `lifecycle` events in the log,
    /// rebuilt from it on a restart.
    stats: Arc<Mutex<Stats>>,
    /// The budget projection: spend against budget per role and crew-wide
    /// (issues #54, #176). A projection of the `budget` events in the log,
    /// rebuilt from it on a restart, backing `GET /budget`.
    budgets: Arc<Mutex<Budgets>>,
    /// The shared-subscription usage gauge and auto-pause (issue #56). In
    /// memory like the pause control: the broker is the live authority,
    /// distinct from the manual pause.
    usage: Arc<Mutex<Usage>>,
}

impl AppState {
    /// Builds the application state with the default in-memory storage backend.
    ///
    /// For tests and ephemeral use; the `crewd` daemon injects a durable
    /// backend with [`with_storage`](AppState::with_storage).
    #[must_use]
    pub fn new(config: Config) -> Self {
        Self::with_storage(config, Arc::new(MemoryStore::default()))
    }

    /// Builds the application state over a chosen [`Storage`] backend.
    ///
    /// Builds the secret [`Scrubber`] once from [`Config::secrets`] and opens
    /// the fan-out channel; both are shared across every request. Takes the
    /// backend as a `dyn Storage`, so the broker never depends on a
    /// concrete store.
    #[must_use]
    pub fn with_storage(config: Config, storage: Arc<dyn Storage>) -> Self {
        let scrubber = Scrubber::new(config.secrets.iter().cloned());
        let (broadcast, _) = broadcast::channel(BROADCAST_CAPACITY);
        // Rebuild the durable projections from the log the storage just replayed, so a
        // decision recorded (issue #49), a task mid-verification (issue #181), or spend
        // accrued (issue #55) before a restart survives it.
        let events = storage.events();
        let board = Board::rebuild(&events);
        let gate = Gate::rebuild(&events);
        let stats = Stats::rebuild(&events);
        let budgets = Budgets::rebuild(&events);
        let usage = Usage::new(config.usage_threshold);
        Self {
            config: Arc::new(config),
            storage,
            router: Arc::new(ChannelRouter),
            scrubber: Arc::new(scrubber),
            broadcast,
            publish_order: Arc::new(Mutex::new(())),
            control: Arc::new(Mutex::new(Control::default())),
            ledger: Arc::new(Mutex::new(Ledger::default())),
            gate: Arc::new(Mutex::new(gate)),
            board: Arc::new(Mutex::new(board)),
            stats: Arc::new(Mutex::new(stats)),
            budgets: Arc::new(Mutex::new(budgets)),
            usage: Arc::new(Mutex::new(usage)),
        }
    }

    /// The work ledger behind its lock, recovering from a poisoned mutex.
    ///
    /// The claim handler holds this across its check, update, and publish, so
    /// the broker serializes claims: two roles can never both take the same
    /// task.
    pub(crate) fn ledger(&self) -> std::sync::MutexGuard<'_, Ledger> {
        self.ledger.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Pauses one role: it pulls no new work until resumed (issue #41).
    pub fn pause_role(&self, role: RoleId) {
        self.control().paused_roles.insert(role);
    }

    /// Resumes one role, clearing its own pause. The crew-wide standing is
    /// unchanged, so a role stays gated while the crew is paused or stood
    /// down.
    pub fn resume_role(&self, role: &RoleId) {
        self.control().paused_roles.remove(role);
    }

    /// Pauses the whole crew: no role pulls new work until resumed. A
    /// stand-down is not weakened to a pause.
    pub fn pause_crew(&self) {
        let mut control = self.control();
        if control.standing != Standing::StoodDown {
            control.standing = Standing::Paused;
        }
    }

    /// Resumes the crew, clearing a crew-wide pause or stand-down, and lifts
    /// any usage auto-pause (issue #56), so `crew resume` is the one escape
    /// hatch. Roles paused on their own stay paused until resumed
    /// individually.
    ///
    /// Returns `true` if a usage auto-pause was active, so the caller can
    /// surface that it lifted; a manual resume clears it early, before the
    /// window reset.
    #[must_use = "the return says whether a usage auto-pause lifted, to surface on the stream"]
    pub fn resume_crew(&self) -> bool {
        self.control().standing = Standing::Running;
        self.usage().resume(Timestamp::now())
    }

    /// Stands the crew down: the emergency halt. Every role is gated at once;
    /// the durable log and roster are preserved, so the crew is
    /// recoverable.
    pub fn stand_down(&self) {
        self.control().standing = Standing::StoodDown;
    }

    /// Whether `role` is gated from new work: the crew is paused, stood down,
    /// or usage auto-paused (issue #56), or the role is paused on its own.
    #[must_use]
    pub fn is_role_paused(&self, role: &RoleId) -> bool {
        let control = self.control();
        control.standing != Standing::Running
            || control.paused_roles.contains(role)
            || self.is_usage_paused()
    }

    /// A snapshot of the control state for the roster view: the crew standing
    /// and the set of individually paused roles.
    #[must_use]
    pub fn control_snapshot(&self) -> (Standing, BTreeSet<RoleId>) {
        let control = self.control();
        (control.standing, control.paused_roles.clone())
    }

    /// Records a shared-subscription usage reading, returning `true` if it
    /// newly engaged the auto-pause (issue #56). At or above the threshold
    /// it pauses new work until `window_reset`; the operator can resume
    /// early with `crew resume`.
    #[must_use = "the return says whether the reading newly auto-paused, to surface it"]
    pub fn report_usage(&self, percent: u8, window_reset: Timestamp) -> bool {
        self.usage().report(Timestamp::now(), percent, window_reset)
    }

    /// Whether new work is auto-paused on shared-subscription usage right now
    /// (issue #56).
    #[must_use]
    pub fn is_usage_paused(&self) -> bool {
        self.usage().is_paused(Timestamp::now())
    }

    /// Clears a usage auto-pause whose window has reset, returning the lifted
    /// gauge to announce, or `None` if none has expired (issue #112).
    ///
    /// The broker's background sweep calls this on a timer: on a `Some` it
    /// publishes a `usage` lift event, so the lazy auto-resume is observable on
    /// the stream, not just reflected in the gate. It is idempotent, a second
    /// call after the pause is cleared returns `None`, so the lift is announced
    /// once.
    #[must_use = "the returned gauge must be published so the auto-resume is announced"]
    pub fn expire_usage_pause(&self) -> Option<UsageView> {
        let mut usage = self.usage();
        usage.expire(Timestamp::now())?;
        // The pause just lifted: report the gauge, now un-paused, so the lift
        // event carries the same shape as a manual resume.
        Some(UsageView {
            percent: usage.percent,
            threshold: usage.threshold,
            paused: false,
            resets_at: None,
        })
    }

    /// The usage gauge for `GET /usage`: the latest reading, the threshold, and
    /// the pause.
    #[must_use]
    pub fn usage_snapshot(&self) -> UsageView {
        let usage = self.usage();
        let now = Timestamp::now();
        let paused = usage.is_paused(now);
        UsageView {
            percent: usage.percent,
            threshold: usage.threshold,
            paused,
            resets_at: paused.then_some(usage.paused_until).flatten(),
        }
    }

    /// The usage gauge behind its lock, recovering from a poisoned mutex.
    fn usage(&self) -> std::sync::MutexGuard<'_, Usage> {
        self.usage.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The control state behind its lock, recovering from a poisoned mutex (a
    /// panic in another handler must not wedge the brake).
    fn control(&self) -> std::sync::MutexGuard<'_, Control> {
        self.control.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Submits `task` for verification as `owner`, claiming the acceptance it
    /// holds to (issue #47).
    ///
    /// Records the task as [`Submitted`](Verdict::Submitted) and awaiting an
    /// independent verifier, replacing any prior standing so a fixed task
    /// can be resubmitted and re-verified. Submitting does not mark the
    /// work done: only an independent [`Passed`](Verdict::Passed) verdict
    /// does.
    pub fn submit_for_verification(&self, task: String, owner: RoleId, acceptance: String) {
        self.gate().tasks.insert(
            task,
            GateEntry {
                owner,
                verifier: None,
                verdict: Verdict::Submitted,
                detail: acceptance,
            },
        );
    }

    /// Records `verifier`'s verdict on `task`, enforcing the done-gate (issue
    /// #47).
    ///
    /// The gate refuses a verdict that would let confident-but-wrong work
    /// through: the task must have been submitted and still awaiting a
    /// verdict, and the verifier must not be its owner, so a task reaches
    /// [`Passed`](Verdict::Passed) only when a role other than the owner
    /// could not break it. A `pass` marks it done; otherwise it is
    /// handed back with `failure`. The lock is held across the check and the
    /// update, so two racing verdicts cannot both win.
    ///
    /// # Errors
    /// Returns a `VerdictError` if the task was never submitted, is not
    /// awaiting a verdict, or the verifier is the owner.
    pub fn record_verdict(
        &self,
        task: &str,
        verifier: RoleId,
        pass: bool,
        failure: String,
    ) -> Result<VerdictOutcome, VerdictError> {
        let mut gate = self.gate();
        let entry = gate.tasks.get_mut(task).ok_or(VerdictError::UnknownTask)?;
        if entry.verdict != Verdict::Submitted {
            return Err(VerdictError::NotAwaitingVerification(entry.verdict));
        }
        if entry.owner == verifier {
            return Err(VerdictError::SelfVerification);
        }
        entry.verifier = Some(verifier.clone());
        if pass {
            entry.verdict = Verdict::Passed;
            entry.detail = String::new();
        } else {
            entry.verdict = Verdict::Failed;
            entry.detail = failure;
        }
        Ok(VerdictOutcome {
            owner: entry.owner.clone(),
            verifier,
            verdict: entry.verdict,
            detail: entry.detail.clone(),
        })
    }

    /// A snapshot of the done-gate: every task under verification and its
    /// standing.
    #[must_use]
    pub fn gate_snapshot(&self) -> BTreeMap<String, GateEntry> {
        self.gate().tasks.clone()
    }

    /// The gate state behind its lock, recovering from a poisoned mutex (a
    /// panic in another handler must not wedge the done-gate).
    fn gate(&self) -> std::sync::MutexGuard<'_, Gate> {
        self.gate.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Records a board entry under `key`, or replaces the existing one (issue
    /// #49).
    ///
    /// Updates the live projection; the endpoint also publishes a `board`
    /// event, so the change is durable and the board rebuilds from the log
    /// on a restart.
    pub fn record_board(&self, key: String, section: BoardSection, author: RoleId, body: String) {
        self.board().entries.insert(
            key,
            BoardEntry {
                section,
                author,
                body,
            },
        );
    }

    /// Retracts the board entry under `key` for `requester`, enforcing that
    /// only the entry's author or the crew's `commander` may remove it
    /// (issues #49, #180).
    ///
    /// Holds the board lock across the authorization check and the removal, so
    /// a concurrent change cannot slip between them (mirroring the
    /// done-gate's check-then-update under one lock). Returns the removed
    /// entry, or a `RetractError` the endpoint renders as a 404 (no such
    /// entry) or a 403 (an unauthorized role), so curation is enforced
    /// rather than conventional.
    ///
    /// # Errors
    /// Returns `RetractError::NoSuchEntry` if no entry is recorded under
    /// `key`, or `RetractError::Unauthorized` if `requester` is neither
    /// the entry's author nor `commander`.
    pub fn retract_board_as(
        &self,
        key: &str,
        requester: &RoleId,
        commander: &RoleId,
    ) -> Result<BoardEntry, RetractError> {
        let mut board = self.board();
        let Some(author) = board.entries.get(key).map(|entry| entry.author.clone()) else {
            return Err(RetractError::NoSuchEntry);
        };
        if requester != &author && requester != commander {
            return Err(RetractError::Unauthorized { author });
        }
        // The lock is held across the read above and this removal, so the entry is
        // still present; `ok_or` keeps this total without a panicking `expect`.
        board.entries.remove(key).ok_or(RetractError::NoSuchEntry)
    }

    /// A snapshot of the whole board: every entry, keyed and ordered by topic
    /// (issue #49).
    #[must_use]
    pub fn board_snapshot(&self) -> BTreeMap<String, BoardEntry> {
        self.board().entries.clone()
    }

    /// The board projection behind its lock, recovering from a poisoned mutex
    /// (a panic in another handler must not wedge the board).
    fn board(&self) -> std::sync::MutexGuard<'_, Board> {
        self.board.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The usage rollup per role as of `now`: tokens, cost, and working time
    /// (issue #55).
    ///
    /// Each role's working time is closed through `now`, so a role working
    /// right now shows time that climbs between its lifecycle transitions.
    /// The caller (the `GET /stats` endpoint) sums these for the aggregate.
    #[must_use]
    pub fn stats_snapshot(&self, now: Timestamp) -> Vec<RoleStatsView> {
        self.stats().snapshot(now)
    }

    /// The stats projection behind its lock, recovering from a poisoned mutex
    /// (a panic in another handler must not wedge the rollup).
    fn stats(&self) -> std::sync::MutexGuard<'_, Stats> {
        self.stats.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The budget snapshot for `GET /budget`: spend against budget per role and
    /// crew-wide (issue #176).
    ///
    /// A projection of the `budget` events, rebuilt from the durable log on a
    /// restart like the situation board, so the cockpit (issue #51) reads a
    /// snapshot rather than replaying events.
    #[must_use]
    pub fn budget_snapshot(&self) -> BudgetView {
        self.budgets().snapshot()
    }

    /// The budget projection behind its lock, recovering from a poisoned mutex
    /// (a panic in another handler must not wedge the snapshot).
    fn budgets(&self) -> std::sync::MutexGuard<'_, Budgets> {
        self.budgets.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Scrubs an event of secrets, appends it to the log, and fans it to
    /// subscribers.
    ///
    /// The one path every emitter shares (a posted message, a roster change).
    /// Returns the stored event with the sequence number it was assigned.
    /// Masks any configured secret first, so a leaked value reaches neither
    /// the log nor a subscriber, then appends and broadcasts under one
    /// lock, so events reach subscribers in the same order they are stored
    /// and every `Last-Event-ID` cursor stays monotonic. A send
    /// with no live subscribers is not an error: the event is stored for a
    /// later reader.
    ///
    /// This is the single point every event enters the store and the stream, so
    /// it is where the stamping guarantee is enforced (issue #29): the
    /// event must carry the fields every projection needs
    /// ([`Event::is_well_formed`]). The public HTTP handlers validate
    /// untrusted input before they reach here; the assertion guards against
    /// any internal emitter regressing the invariant.
    ///
    /// [`Event::is_well_formed`]: crew_core::Event::is_well_formed
    pub fn publish(&self, mut event: Event) -> Sequenced {
        debug_assert!(
            event.is_well_formed(),
            "an event must be stamped before it reaches the store or stream: {event:?}",
        );
        // Mask before either sink, so the persisted log and every live stream carry
        // the same scrubbed event.
        self.scrubber.scrub_event(&mut event);
        // Held across the append and the send (both non-blocking, no `.await`), so a
        // concurrent publish cannot interleave and deliver sequences out of order.
        let _order = self
            .publish_order
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let seq = self.storage.next_seq();
        self.storage.append(event.clone());
        // Advance the usage rollup at the one choke point every event flows through, so
        // telemetry and lifecycle events from any handler are folded in (issue #55).
        self.stats().apply(&event);
        // Fold the same way for the budget projection, so a spend report from any
        // handler advances the snapshot `GET /budget` serves (issue #176).
        self.budgets().apply(&event);
        let sequenced = Sequenced { seq, event };
        // Err(_) only means no subscribers are listening right now, which is fine.
        let _ = self.broadcast.send(sequenced.clone());
        sequenced
    }
}

#[cfg(test)]
mod tests {
    use crew_core::{
        Activity, ChannelId, Event, EventKind, Lifecycle, Message, MessageId, MessageKind, RoleId,
        Sender, TaskId, Timestamp,
    };

    use super::AppState;
    use crate::config::Config;

    /// An event of `kind`, from a role on `all-units`, optionally within
    /// `task`.
    fn event(kind: EventKind, task: Option<TaskId>) -> Event {
        Event {
            ts: Timestamp::now(),
            from: Sender::Role(RoleId::new("backend")),
            channel: ChannelId::new("all-units"),
            task,
            kind,
        }
    }

    #[test]
    fn publish_stamps_and_correlates_every_event_kind() {
        // publish is the single choke point, so proving the invariant here proves it
        // for a message, a lifecycle transition, and an activity event alike (issue
        // #29).
        let state = AppState::new(Config::default());
        let mut stream = state.broadcast.subscribe();
        let task = TaskId::new();

        let message = event(
            EventKind::Message(Message {
                id: MessageId::new(),
                kind: MessageKind::Note,
                body: "work item".to_owned(),
            }),
            Some(task),
        );
        let lifecycle = event(EventKind::Lifecycle(Lifecycle::Started), Some(task));
        let activity = event(EventKind::Activity(Activity::TurnStarted), None);

        for event in [message.clone(), lifecycle.clone(), activity.clone()] {
            state.publish(event);
        }

        // No event reaches the store missing a required field.
        let stored = state.storage.events();
        assert_eq!(stored.len(), 3, "every event is stored");
        assert!(
            stored.iter().all(Event::is_well_formed),
            "no stored event misses a required field",
        );

        // The stream carries the same well-formed events, in order.
        for expected in [&message, &lifecycle, &activity] {
            let streamed = stream
                .try_recv()
                .expect("the event reaches the stream")
                .event;
            assert!(
                streamed.is_well_formed(),
                "no streamed event misses a field"
            );
            assert_eq!(&streamed, expected, "the stream carries the event intact");
        }

        // Events produced within a task carry its id; one produced outside carries
        // none.
        assert_eq!(
            stored[0].task,
            Some(task),
            "the message correlates to its task"
        );
        assert_eq!(
            stored[1].task,
            Some(task),
            "the lifecycle event correlates too"
        );
        assert_eq!(stored[2].task, None, "an event outside a task carries none");
    }

    /// A timestamp at an RFC 3339 instant, via the same serde form the wire
    /// uses.
    fn at(rfc3339: &str) -> Timestamp {
        serde_json::from_value(serde_json::Value::String(rfc3339.to_owned())).unwrap()
    }

    /// An event from `role` on `all-units` at `ts`, carrying `kind`.
    fn from_role(role: &str, ts: Timestamp, kind: EventKind) -> Event {
        Event {
            ts,
            from: Sender::Role(RoleId::new(role)),
            channel: ChannelId::new("all-units"),
            task: None,
            kind,
        }
    }

    #[test]
    fn expiring_a_usage_pause_reports_the_lift_once_when_the_window_has_reset() {
        let state = AppState::new(Config::default());

        // An active pause (its window still ahead) has not expired: nothing to lift.
        let _ = state.report_usage(99, at("2099-01-01T00:00:00Z"));
        assert!(state.is_usage_paused(), "armed with a future reset");
        assert!(
            state.expire_usage_pause().is_none(),
            "an active pause is not swept",
        );

        // A pause whose window has already reset is armed but lazily lifted; the sweep
        // clears it and reports the lifted gauge.
        let _ = state.report_usage(99, at("2000-01-01T00:00:00Z"));
        assert!(!state.is_usage_paused(), "the window already reset");
        let lifted = state
            .expire_usage_pause()
            .expect("an expired pause is swept");
        assert!(!lifted.paused, "the reported gauge is un-paused");
        assert_eq!(lifted.percent, 99, "it carries the last reading");
        assert_eq!(lifted.resets_at, None);

        // Idempotent: a second sweep finds nothing, so the lift is announced once.
        assert!(
            state.expire_usage_pause().is_none(),
            "the lift is reported once"
        );
    }

    #[test]
    fn stats_roll_up_tokens_cost_and_closed_working_time() {
        use crew_core::TelemetryEvent;

        let state = AppState::new(Config::default());
        // Backend works for a bounded interval, spending two turns in between.
        state.publish(from_role(
            "backend",
            at("2026-07-23T00:00:00Z"),
            EventKind::Lifecycle(Lifecycle::Started),
        ));
        for (tokens, cost) in [(1_000, 30_000), (500, 15_000)] {
            state.publish(from_role(
                "backend",
                at("2026-07-23T00:00:30Z"),
                EventKind::Telemetry(TelemetryEvent {
                    role: RoleId::new("backend"),
                    tokens,
                    cost_micro_usd: cost,
                }),
            ));
        }
        state.publish(from_role(
            "backend",
            at("2026-07-23T00:01:30Z"),
            EventKind::Lifecycle(Lifecycle::Idle),
        ));

        let rollup = state.stats_snapshot(at("2026-07-23T00:02:00Z"));
        let backend = rollup
            .iter()
            .find(|s| s.role == RoleId::new("backend"))
            .unwrap();
        assert_eq!(backend.tokens, 1_500, "the two turns' tokens sum");
        assert_eq!(backend.cost_micro_usd, 45_000, "the two turns' cost sums");
        assert_eq!(
            backend.active_secs, 90,
            "working time is the closed interval, not the wall clock since"
        );
    }

    #[test]
    fn working_time_climbs_while_a_role_is_still_working() {
        let state = AppState::new(Config::default());
        state.publish(from_role(
            "frontend",
            at("2026-07-23T00:00:00Z"),
            EventKind::Lifecycle(Lifecycle::Started),
        ));
        // No closing transition yet: the snapshot counts the open interval up to `now`.
        let rollup = state.stats_snapshot(at("2026-07-23T00:00:45Z"));
        let frontend = rollup
            .iter()
            .find(|s| s.role == RoleId::new("frontend"))
            .unwrap();
        assert_eq!(
            frontend.active_secs, 45,
            "an open working interval keeps climbing"
        );
    }

    #[test]
    fn stats_survive_a_restart_by_rebuilding_from_the_log() {
        use std::sync::Arc;

        use crew_core::TelemetryEvent;

        use crate::store::Storage;

        // Fold the projection over a log, then rebuild a fresh state over the same
        // store.
        let store: Arc<dyn Storage> = Arc::new(crate::store::MemoryStore::default());
        let first = AppState::with_storage(Config::default(), Arc::clone(&store));
        first.publish(from_role(
            "docs",
            at("2026-07-23T00:00:00Z"),
            EventKind::Telemetry(TelemetryEvent {
                role: RoleId::new("docs"),
                tokens: 700,
                cost_micro_usd: 1_200,
            }),
        ));

        let rebuilt = AppState::with_storage(Config::default(), store);
        let rollup = rebuilt.stats_snapshot(at("2026-07-23T00:01:00Z"));
        let docs = rollup
            .iter()
            .find(|s| s.role == RoleId::new("docs"))
            .unwrap();
        assert_eq!(docs.tokens, 700, "the rollup rebuilds from the durable log");
        assert_eq!(docs.cost_micro_usd, 1_200);
    }

    #[test]
    fn the_gate_survives_a_restart_by_rebuilding_from_the_log() {
        use std::sync::Arc;

        use crew_core::{Verdict, VerificationEvent};

        use crate::store::{MemoryStore, Storage};

        // A submission then an independent pass: two `verification` events for one
        // task.
        let store: Arc<dyn Storage> = Arc::new(MemoryStore::default());
        let first = AppState::with_storage(Config::default(), Arc::clone(&store));
        first.publish(from_role(
            "backend",
            at("2026-07-23T00:00:00Z"),
            EventKind::Verification(VerificationEvent {
                task: "login".to_owned(),
                owner: RoleId::new("backend"),
                verifier: None,
                verdict: Verdict::Submitted,
                detail: "tokens expire".to_owned(),
            }),
        ));
        first.publish(from_role(
            "qa",
            at("2026-07-23T00:01:00Z"),
            EventKind::Verification(VerificationEvent {
                task: "login".to_owned(),
                owner: RoleId::new("backend"),
                verifier: Some(RoleId::new("qa")),
                verdict: Verdict::Passed,
                detail: String::new(),
            }),
        ));

        // Rebuild over the same store: the latest verdict per task wins (issue #181).
        let rebuilt = AppState::with_storage(Config::default(), store);
        let gate = rebuilt.gate_snapshot();
        let login = gate.get("login").expect("the task survived the restart");
        assert_eq!(
            login.verdict,
            Verdict::Passed,
            "the pass verdict was folded"
        );
        assert_eq!(login.owner, RoleId::new("backend"));
        assert_eq!(login.verifier, Some(RoleId::new("qa")));
    }
}
