//! The agent lifecycle: lazy start, idle-stop, restart, and the defibrillator.
//!
//! An idle role should cost nothing, and an agent whose turn dies mid-flight
//! should recover. Each agent runs a small state machine on its own driver
//! thread, and a fleet-wide watchdog backs the drivers up.
//!
//! Lifecycle (issue #22):
//!
//! - **Lazy start.** A [`Fleet`] launches with every agent
//!   [`Stopped`](AgentState::Stopped) and no process; work triggers
//!   [`Fleet::start`], which spawns the process and registers the role (a
//!   `started` event).
//! - **Idle-stop.** After the quiet `idle_timeout` the driver stops the process
//!   but keeps the roster entry (an `idle` event), so a restart is fast and
//!   keeps context.
//! - **Restart on demand.** [`Fleet::start`] on a stopped or idle agent
//!   restarts it (a `restarted` event).
//!
//! Defibrillator (issue #23), mirroring Seraphim's: layered detection recovers
//! an agent whose turn died, whether it **crashed** (its process exited) or
//! **hung** (its process is alive but silent past the `heartbeat_timeout`).
//!
//! - **In-turn heartbeat.** Each driver polls its agent for a crash or a hang.
//!   On a death it reaps the orphaned process, records an [`Incident`] with the
//!   diagnostic detail, marks the role dead (a `died` event), and revives it (a
//!   `recovered` event) while it has recovery budget; once the budget is spent
//!   it stays dead and is handed to the operator.
//! - **Background watchdog.** A single fleet-wide thread catches a working
//!   agent that went silent past the longer `watchdog_timeout`, which the
//!   in-turn heartbeat should have caught first; only a driver that has itself
//!   wedged lets it through, so the watchdog reaps the orphan and hands the
//!   role to the operator.
//!
//! Every transition marks the broker roster, so the roster and the stream
//! reflect it (see `docs/observability.md`). The mechanics build on the
//! [`spawn`](crate::spawn) primitives, so a real `claude` process and a test
//! stub run identical code.

use std::{
    collections::BTreeMap,
    io::{BufRead, BufReader, Read},
    process::Child,
    sync::{
        mpsc::{self, Receiver, RecvTimeoutError, Sender},
        Arc, Mutex, PoisonError,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use crew_core::{Budget, BudgetEvent, BudgetScope, RoleId, TelemetryEvent};
use eyre::{eyre, Result};
use tracing::{event, Level};

use crate::{
    roster::{Liveness, RosterClient},
    spawn::{boot_command, spawn_process, AgentCommand, Captured, OutputStream, PreparedAgent},
    stall::{Stall, StallMonitor},
    worktree::Worktree,
};

/// How often a driver polls its agent, and the watchdog scans the fleet.
///
/// Small enough that a crash recovers and a hang is caught promptly, large
/// enough that an idle fleet costs no meaningful CPU. It bounds detection
/// latency, so every timeout below should be at least this long.
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// The lifecycle and defibrillator policy: the timeouts and the recovery
/// budget.
///
/// The three silence timeouts share one clock (the agent's last output). Which
/// fires first is set by their relative order: with the defaults `idle_timeout`
/// is shortest, so a quiet agent parks (idle-stop) rather than being
/// force-recovered, and the `heartbeat_timeout` catches a hang only where
/// idle-stop is lengthened or disabled. A crash (an exited process) is
/// recovered regardless of the timeouts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LifecyclePolicy {
    /// How long an agent may be quiet before it is idle-stopped.
    pub idle_timeout: Duration,
    /// How long a working agent may be silent before it is presumed hung and
    /// the defibrillator reaps it. Precise hang-versus-idle discrimination
    /// needs the activity parser's turn boundaries (issue #24); until then
    /// this is a coarse output-silence heartbeat.
    pub heartbeat_timeout: Duration,
    /// How long a working agent may be silent before the fleet watchdog reaps
    /// it as a backstop. Strictly greater than
    /// [`heartbeat_timeout`](Self::heartbeat_timeout) in production, so a
    /// live driver always defibrillates a hang first.
    pub watchdog_timeout: Duration,
    /// How many times a dead agent may be revived before it is left for the
    /// operator, so a crash or hang loop cannot recover forever.
    pub max_recoveries: u32,
    /// How long a coordination wait may persist before it is escalated as a
    /// stall (issue #48): an unanswered question, a mutual wait, or a held
    /// ledger task with no forward motion. This is about the crew waiting
    /// on itself, not a silent process, so it is shorter than the process
    /// `heartbeat_timeout`.
    pub stall_timeout: Duration,
    /// How often the coordination-stall monitor scans the stream. Coarse: a
    /// stall evolves over minutes, so a frequent scan would only re-read
    /// the same history.
    pub stall_scan_interval: Duration,
}

impl Default for LifecyclePolicy {
    /// Idle-stop after five quiet minutes, presume a hang at twenty, back that
    /// with a twenty-five-minute watchdog, and revive at most three times.
    ///
    /// The heartbeat and watchdog match Seraphim's defibrillator: twenty
    /// minutes is well above the longest realistic silent step (a slow
    /// build), and the watchdog sits above it so a live driver acts first.
    /// Three recoveries clears a transient death without masking a
    /// persistent one.
    fn default() -> Self {
        Self {
            idle_timeout: Duration::from_secs(5 * 60),
            heartbeat_timeout: Duration::from_secs(20 * 60),
            watchdog_timeout: Duration::from_secs(25 * 60),
            max_recoveries: 3,
            // Ten minutes is long enough that a slow but progressing exchange is not
            // flagged, short enough to catch a real deadlock before the crew wastes a
            // shift on it; scanning once a minute keeps the read light.
            stall_timeout: Duration::from_secs(10 * 60),
            stall_scan_interval: Duration::from_secs(60),
        }
    }
}

/// An agent's supervised lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentState {
    /// No process: never started (lazy), or cleanly stood down. Its roster
    /// entry is kept once known, so a restart is fast.
    Stopped,
    /// Its process is running and under supervision.
    Working,
    /// Idle-stopped: no process, but parked and ready to resume on demand.
    Idle,
    /// Died: it crashed or hung. Revived while it had recovery budget; once
    /// spent, it stays dead and is handed to the operator.
    Dead,
}

/// Why an agent's turn died, recorded on an [`Incident`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeathCause {
    /// The process exited unexpectedly.
    Crashed,
    /// The process was alive but produced no output past the heartbeat timeout.
    Hung,
    /// The driver itself stalled, so the fleet watchdog reaped the orphaned
    /// process.
    Wedged,
}

/// What the defibrillator did about a death.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Recovery {
    /// Revived within the recovery budget (a `recovered` event followed).
    Revived,
    /// The budget was spent (or the driver was untrusted): left dead for the
    /// operator.
    HandedOff,
}

/// A recorded defibrillator incident: one agent death and what was done about
/// it.
///
/// The fleet keeps these so the operator can see what died, why, and whether it
/// came back (read them with [`Fleet::incidents`]).
#[derive(Debug, Clone)]
pub struct Incident {
    /// The role whose turn died.
    pub role: RoleId,
    /// Why it died.
    pub cause: DeathCause,
    /// A one-line diagnostic of what happened.
    pub detail: String,
    /// What the defibrillator did about it.
    pub recovery: Recovery,
}

/// Charges a turn's usage against the crew budget and enforces the caps (issues
/// #54, #177).
///
/// The shared core behind [`Fleet::record_usage`]: it holds the crew
/// [`Budget`], a [`RosterClient`] for the `telemetry` and `budget` stream
/// events, and each role's driver command channel for idle-stopping on a
/// breach. The [`Fleet`] delegates its usage methods here, and the activity
/// forwarder (issue #24) holds a clone, so a per-turn usage parsed from an
/// agent's stream-json charges spend directly rather than only when a caller
/// pokes the seam (issue #177). Cloning shares the one budget and the one set
/// of channels, so every clone charges the same accountant and stops the same
/// drivers.
#[derive(Debug, Clone)]
pub(crate) struct UsageRecorder {
    /// The crew's token budget: the accountant spend is charged against.
    /// Unbounded by default; the crew opts in through the config.
    budget: Arc<Mutex<Budget>>,
    /// A client for surfacing the `telemetry` and `budget` events on the
    /// stream.
    roster: RosterClient,
    /// Each role's driver command channel, for idle-stopping it on a breach.
    stops: Arc<BTreeMap<RoleId, Sender<Command>>>,
}

impl UsageRecorder {
    /// Records a turn's `tokens` and `cost_micro_usd` for `role`: telemetry
    /// then budget. See [`Fleet::record_usage`].
    ///
    /// # Errors
    /// Returns an error if idle-stopping a role on a budget breach fails.
    pub(crate) fn record_usage(
        &self,
        role: &RoleId,
        tokens: u64,
        cost_micro_usd: u64,
    ) -> Result<()> {
        // Always surface the usage, so an unbounded crew is still legible (issue #55).
        let telemetry = TelemetryEvent {
            role: role.clone(),
            tokens,
            cost_micro_usd,
        };
        if let Err(err) = self.roster.report_telemetry(&telemetry) {
            event!(
                name: "supervisor.telemetry.report_failed",
                Level::WARN,
                crew.role = %role,
                "could not report telemetry for `{{crew.role}}`: {err}",
            );
        }
        // Then charge it against the crew budget, which enforces the caps (issue #54).
        self.record_spend(role, tokens)
    }

    /// Charges `tokens` of spend to `role` against the crew budget, enforcing
    /// the caps. See [`Fleet::record_spend`].
    ///
    /// # Errors
    /// Returns an error if idle-stopping a role on a breach fails.
    fn record_spend(&self, role: &RoleId, tokens: u64) -> Result<()> {
        let spend = {
            let mut budget = self.budget.lock().unwrap_or_else(PoisonError::into_inner);
            if !budget.is_bounded() {
                return Ok(());
            }
            budget.record(role, tokens)
        };
        let breach = spend.breach;

        // Surface the spend against budget on the stream, so a cap hit is never silent.
        if let Err(err) = self.roster.report_budget(&BudgetEvent::from(spend)) {
            event!(
                name: "supervisor.budget.report_failed",
                Level::WARN,
                crew.role = %role,
                "could not report budget for `{{crew.role}}`: {err}",
            );
        }

        // Enforce: idle-stop rather than overrun. A crew breach stands the whole crew
        // down; a role breach stops just that role.
        match breach {
            Some(BudgetScope::Crew) => {
                event!(
                    name: "supervisor.budget.crew_exhausted",
                    Level::WARN,
                    "the crew reached its token budget; idle-stopping every role",
                );
                self.stop_all()
            }
            Some(BudgetScope::Role) => {
                event!(
                    name: "supervisor.budget.role_capped",
                    Level::WARN,
                    crew.role = %role,
                    "`{{crew.role}}` reached its token cap; idle-stopping it",
                );
                self.stop(role)
            }
            None => Ok(()),
        }
    }

    /// Idle-stops every role in the crew, keeping each roster entry (a crew
    /// budget breach).
    fn stop_all(&self) -> Result<()> {
        for role in self.stops.keys() {
            self.stop(role)?;
        }
        Ok(())
    }

    /// Idle-stops `role`, keeping its roster entry (a role cap breach).
    ///
    /// # Errors
    /// Returns an error if `role` is not in the fleet, or its driver has
    /// stopped.
    fn stop(&self, role: &RoleId) -> Result<()> {
        let commands = self
            .stops
            .get(role)
            .ok_or_else(|| eyre!("no such role `{role}` in the fleet"))?;
        commands
            .send(Command::Stop)
            .map_err(|_error| eyre!("the `{role}` lifecycle driver has stopped"))
    }
}

/// A running fleet: one lifecycle-managed agent per role, backed by a watchdog.
///
/// Launch it with the roles resolved into [`PreparedAgent`]s; every agent
/// starts [`Stopped`](AgentState::Stopped), so an unused role costs nothing.
/// Drive an agent with [`start`](Fleet::start) and [`stop`](Fleet::stop), and
/// read recorded deaths with [`incidents`](Fleet::incidents). Each agent's
/// captured stream-json is parsed into activity events on the broker (issue
/// #24; see the `activity` module). Dropping the fleet, like
/// [`shutdown`](Fleet::shutdown), stops every agent and deregisters its role.
#[derive(Debug)]
pub struct Fleet {
    drivers: Vec<AgentDriver>,
    incidents: Arc<Mutex<Vec<Incident>>>,
    watchdog_stop: Sender<()>,
    watchdog: Option<JoinHandle<()>>,
    /// The coordination stalls the monitor has detected (issue #48), shared
    /// with its thread and read by [`stalls`](Fleet::stalls).
    stalls: Arc<Mutex<Vec<Stall>>>,
    stall_stop: Sender<()>,
    stall_monitor: Option<JoinHandle<()>>,
    /// The per-role git worktrees to clean up on stand-down (issue #43); empty
    /// unless the crew opted into worktree isolation.
    worktrees: Vec<Worktree>,
    /// Charges each turn's usage against the crew budget and enforces the caps
    /// (issues #54, #177). Shared with the activity forwarder, so a per-turn
    /// usage parsed from an agent's stream-json charges spend directly; the
    /// [`record_usage`](Fleet::record_usage) and
    /// [`record_spend`](Fleet::record_spend) methods delegate to it.
    recorder: UsageRecorder,
}

impl Fleet {
    /// Launches a lifecycle driver per agent (each stopped) and the fleet
    /// watchdog.
    ///
    /// No process is spawned and no role is registered until
    /// [`start`](Fleet::start), so launching an idle fleet is free.
    #[must_use]
    pub fn launch(
        roster: &RosterClient,
        agents: Vec<PreparedAgent>,
        policy: LifecyclePolicy,
    ) -> Self {
        let (sink, output) = mpsc::channel();
        let incidents = Arc::new(Mutex::new(Vec::new()));

        // Spawn the drivers first, collecting each role's command channel, so the usage
        // recorder can idle-stop a role on a budget breach (issue #54).
        let mut drivers = Vec::with_capacity(agents.len());
        let mut shared = Vec::with_capacity(agents.len());
        let mut stops = BTreeMap::new();
        for prepared in agents {
            let driver = AgentDriver::spawn(
                roster.clone(),
                prepared,
                policy,
                sink.clone(),
                Arc::clone(&incidents),
            );
            stops.insert(driver.shared.role.clone(), driver.commands.clone());
            shared.push(Arc::clone(&driver.shared));
            drivers.push(driver);
        }

        // The usage recorder charges each turn's spend and idle-stops on a cap. It
        // starts unbounded (the crew opts in through `with_budget`), and the
        // forwarder shares it.
        let recorder = UsageRecorder {
            budget: Arc::new(Mutex::new(Budget::default())),
            roster: roster.clone(),
            stops: Arc::new(stops),
        };

        // Parse each agent's captured stream-json into activity events on the broker
        // (issue #24) and charge each turn's usage against the crew budget (issue
        // #177). The forwarder owns the receiver and ends when every agent has
        // stopped and dropped its capture sink.
        crate::activity::forward_activity(output, roster.clone(), recorder.clone());

        let (watchdog_stop, stop) = mpsc::channel();
        let watchdog = {
            let roster = roster.clone();
            let incidents = Arc::clone(&incidents);
            let timeout = policy.watchdog_timeout;
            thread::spawn(move || run_watchdog(&shared, &roster, &incidents, timeout, &stop))
        };

        // The coordination-stall monitor: the fleet-wide half of the defibrillator that
        // watches the stream for a crew stuck waiting on itself (issue #48).
        let stalls = Arc::new(Mutex::new(Vec::new()));
        let (stall_stop, stall_stop_rx) = mpsc::channel();
        let stall_monitor = {
            let roles: Vec<RoleId> = drivers
                .iter()
                .map(|driver| driver.shared.role.clone())
                .collect();
            let monitor = StallMonitor::new(
                roster.clone(),
                roles,
                policy.stall_timeout,
                policy.stall_scan_interval,
                Arc::clone(&stalls),
            );
            thread::spawn(move || monitor.run(&stall_stop_rx))
        };

        Self {
            drivers,
            incidents,
            watchdog_stop,
            watchdog: Some(watchdog),
            stalls,
            stall_stop,
            stall_monitor: Some(stall_monitor),
            worktrees: Vec::new(),
            recorder,
        }
    }

    /// Hands the fleet the per-role worktrees to clean up on stand-down (issue
    /// #43).
    ///
    /// The supervisor creates them before launch; the fleet owns them so it can
    /// remove each unchanged one once its agent has stopped (see
    /// [`shutdown`](Fleet::shutdown)).
    #[must_use]
    pub fn with_worktrees(mut self, worktrees: Vec<Worktree>) -> Self {
        self.worktrees = worktrees;
        self
    }

    /// Sets the crew token budget the fleet enforces (issue #54).
    ///
    /// Build it from the crew config with
    /// [`CrewConfig::budget`](crew_core::CrewConfig::budget). An unbounded
    /// budget (no crew-wide budget and no per-role cap) leaves
    /// [`record_spend`](Fleet::record_spend) a no-op, so a crew that opts out
    /// pays nothing.
    #[must_use]
    pub fn with_budget(self, budget: Budget) -> Self {
        *self
            .recorder
            .budget
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = budget;
        self
    }

    /// Records a turn's `tokens` and `cost_micro_usd` for `role`: telemetry
    /// then budget.
    ///
    /// This is the full turn-usage seam the activity parser (issue #24) drives
    /// with each turn's usage: the forwarder calls it as it parses a `result`
    /// line (issue #177). It surfaces a `telemetry` event so per-role and
    /// aggregate spend is legible off the stream regardless of any budget
    /// (issue #55, feeding `GET /stats`), then charges the tokens against the
    /// crew budget, idle-stopping a role or the crew at a cap (issue #54).
    /// Reporting the telemetry is best-effort (a failure is logged, not
    /// fatal); the budget enforcement still runs.
    ///
    /// # Errors
    /// Returns an error if idle-stopping a role on a budget breach fails (its
    /// driver is gone).
    pub fn record_usage(&self, role: &RoleId, tokens: u64, cost_micro_usd: u64) -> Result<()> {
        self.recorder.record_usage(role, tokens, cost_micro_usd)
    }

    /// Charges `tokens` of spend to `role` against the crew budget, enforcing
    /// the caps.
    ///
    /// Surfaces a `budget` event so spend against budget is visible on the
    /// stream, and when the spend reaches a ceiling idle-stops the role
    /// (its own cap) or the whole crew (the crew-wide budget) rather than
    /// overrun (issue #54). An unbounded crew is a no-op.
    ///
    /// Prefer [`record_usage`](Fleet::record_usage), which also emits the
    /// per-turn telemetry the `GET /stats` rollup folds; this is the
    /// budget-only path.
    ///
    /// # Errors
    /// Returns an error if idle-stopping a role fails (its driver is gone).
    pub fn record_spend(&self, role: &RoleId, tokens: u64) -> Result<()> {
        self.recorder.record_spend(role, tokens)
    }

    /// Starts (or restarts) `role`'s agent: lazy start on first work, restart
    /// on demand.
    ///
    /// A no-op if the agent is already running. This is asynchronous: the
    /// driver applies it, so observe the change through the roster or
    /// [`state`](Fleet::state).
    ///
    /// # Errors
    /// Returns an error if `role` is not in the fleet, or its driver has
    /// stopped.
    pub fn start(&self, role: &RoleId) -> Result<()> {
        self.command(role, Command::Start)
    }

    /// Starts every agent in the fleet, bringing the whole unit online.
    ///
    /// This is what `crew up` calls after
    /// [`launch`](crate::Supervisor::launch): each role's process spawns
    /// and registers on the roster, so the unit is live and connected. Idle
    /// roles then park themselves on the idle-stop timeout, keeping
    /// their roster entry, so the unit stays visible while costing nothing when
    /// quiet.
    ///
    /// # Errors
    /// Returns the first error if any driver has stopped; the remaining agents
    /// are left untouched.
    pub fn start_all(&self) -> Result<()> {
        for driver in &self.drivers {
            self.command(&driver.shared.role, Command::Start)?;
        }
        Ok(())
    }

    /// Stands `role`'s agent down: stops its process, keeping its roster entry.
    ///
    /// # Errors
    /// Returns an error if `role` is not in the fleet, or its driver has
    /// stopped.
    pub fn stop(&self, role: &RoleId) -> Result<()> {
        self.command(role, Command::Stop)
    }

    /// The current lifecycle state of `role`'s agent, if it is in the fleet.
    #[must_use]
    pub fn state(&self, role: &RoleId) -> Option<AgentState> {
        self.driver(role).map(|driver| driver.shared.state())
    }

    /// The roles under management.
    pub fn roles(&self) -> impl Iterator<Item = &RoleId> {
        self.drivers.iter().map(|driver| &driver.shared.role)
    }

    /// A snapshot of the recorded defibrillator incidents, oldest first.
    #[must_use]
    pub fn incidents(&self) -> Vec<Incident> {
        self.incidents
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// A snapshot of the coordination stalls the monitor currently sees (issue
    /// #48).
    ///
    /// Each is a crew stuck waiting on itself: a deadlock, an unanswered
    /// question, or a ledger with no forward motion. The monitor refreshes
    /// this every scan, so a stall that clears leaves the list; the
    /// operator also sees each new one as a warning.
    #[must_use]
    pub fn stalls(&self) -> Vec<Stall> {
        self.stalls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Stops the fleet: stands every agent down, deregisters its role, ends the
    /// watchdog, and cleans up each role's worktree.
    ///
    /// Worktrees are removed only after every agent process has stopped (the
    /// driver joins below), so no cleanup races a running agent. An
    /// unchanged worktree is removed; one with uncommitted changes is kept
    /// for integration (issue #43).
    pub fn shutdown(mut self) {
        for driver in &self.drivers {
            let _ = driver.commands.send(Command::Shutdown);
        }
        let _ = self.watchdog_stop.send(());
        let _ = self.stall_stop.send(());
        for driver in &mut self.drivers {
            if let Some(handle) = driver.handle.take() {
                let _ = handle.join();
            }
        }
        if let Some(handle) = self.watchdog.take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.stall_monitor.take() {
            let _ = handle.join();
        }
        // Every agent has stopped, so its worktree is no longer in use: clean it up.
        crate::worktree::clean_all(&self.worktrees);
    }

    /// The driver for `role`, if present.
    fn driver(&self, role: &RoleId) -> Option<&AgentDriver> {
        self.drivers
            .iter()
            .find(|driver| &driver.shared.role == role)
    }

    /// Sends a command to `role`'s driver.
    fn command(&self, role: &RoleId, command: Command) -> Result<()> {
        let driver = self
            .driver(role)
            .ok_or_else(|| eyre!("no such role `{role}` in the fleet"))?;
        driver
            .commands
            .send(command)
            .map_err(|_error| eyre!("the `{role}` lifecycle driver has stopped"))
    }
}

impl Drop for Fleet {
    fn drop(&mut self) {
        // A dropped fleet must not leak agent processes or threads. Signalling every
        // driver and the watchdog stands the agents down; the detached threads finish
        // on their own, so drop never blocks.
        for driver in &self.drivers {
            let _ = driver.commands.send(Command::Shutdown);
        }
        let _ = self.watchdog_stop.send(());
        let _ = self.stall_stop.send(());
    }
}

/// A handle to one agent's lifecycle driver thread.
#[derive(Debug)]
struct AgentDriver {
    /// The state shared with the driver thread and the fleet watchdog.
    shared: Arc<AgentShared>,
    commands: Sender<Command>,
    handle: Option<JoinHandle<()>>,
}

impl AgentDriver {
    /// Spawns the driver thread for one agent, initially stopped.
    fn spawn(
        roster: RosterClient,
        prepared: PreparedAgent,
        policy: LifecyclePolicy,
        sink: Sender<Captured>,
        incidents: Arc<Mutex<Vec<Incident>>>,
    ) -> Self {
        let (commands, inbox) = mpsc::channel();
        let shared = Arc::new(AgentShared::new(prepared.role.clone()));
        let agent = AgentLifecycle::new(
            prepared,
            roster,
            sink,
            policy,
            Arc::clone(&shared),
            incidents,
        );
        let handle = thread::spawn(move || drive(agent, &inbox));
        Self {
            shared,
            commands,
            handle: Some(handle),
        }
    }
}

/// The state one agent's driver and the fleet watchdog both touch.
///
/// Each field is its own mutex, so the watchdog can read a state or reap a
/// process without contending on the driver's other work; every lock is held
/// only briefly.
#[derive(Debug)]
struct AgentShared {
    role: RoleId,
    state: Mutex<AgentState>,
    /// When the agent last produced output, the clock the heartbeat and
    /// watchdog read.
    last_activity: Mutex<Instant>,
    /// The current process, when running.
    child: Mutex<Option<Child>>,
}

impl AgentShared {
    fn new(role: RoleId) -> Self {
        Self {
            role,
            state: Mutex::new(AgentState::Stopped),
            last_activity: Mutex::new(Instant::now()),
            child: Mutex::new(None),
        }
    }

    fn state(&self) -> AgentState {
        *self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn set_state(&self, state: AgentState) {
        *self.state.lock().unwrap_or_else(PoisonError::into_inner) = state;
    }

    /// Stamps the activity clock to now.
    fn touch(&self) {
        *self
            .last_activity
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Instant::now();
    }

    fn last_activity(&self) -> Instant {
        *self
            .last_activity
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn set_child(&self, child: Child) {
        *self.child.lock().unwrap_or_else(PoisonError::into_inner) = Some(child);
    }

    /// Whether the running process has exited, reaping it if so.
    fn process_exited(&self) -> bool {
        match self
            .child
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_mut()
        {
            Some(child) => matches!(child.try_wait(), Ok(Some(_))),
            None => false,
        }
    }

    /// Kills the process if one is running, reaping it to avoid a zombie.
    /// Idempotent.
    fn reap(&self) {
        if let Some(mut child) = self
            .child
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

/// A command to an agent's driver.
enum Command {
    /// Start or restart the agent.
    Start,
    /// Stand the agent down, keeping its roster entry.
    Stop,
    /// Stand the agent down and deregister it, then exit the driver.
    Shutdown,
}

/// The driver loop: apply commands, and on each idle tick poll for crash, hang,
/// or idle.
fn drive(mut agent: AgentLifecycle, inbox: &Receiver<Command>) {
    loop {
        match inbox.recv_timeout(POLL_INTERVAL) {
            Ok(Command::Start) => {
                if let Err(err) = agent.ensure_running() {
                    event!(
                        name: "supervisor.agent.start.failed",
                        Level::WARN,
                        crew.role = %agent.shared.role,
                        error = %err,
                        "could not start `{{crew.role}}`",
                    );
                }
            }
            Ok(Command::Stop) => agent.stop(),
            Err(RecvTimeoutError::Timeout) => agent.poll(Instant::now()),
            // Shutdown, or the fleet was dropped (disconnected): stand down and exit.
            Ok(Command::Shutdown) | Err(RecvTimeoutError::Disconnected) => {
                agent.shutdown();
                break;
            }
        }
    }
}

/// The fleet watchdog: reap any working agent whose driver failed to handle its
/// death.
///
/// It scans every agent's output-silence clock. Because `watchdog_timeout` is
/// longer than the in-turn `heartbeat_timeout`, a live driver always
/// defibrillates a hang first (which resets the clock) or parks the agent
/// (which leaves `Working`); only a driver that has itself wedged lets an agent
/// stay working and silent this long. So the watchdog reaps the orphan and
/// hands the role to the operator rather than trying to revive it through a
/// driver it cannot trust. Its actions are idempotent, so the rare overlap with
/// a live driver is harmless.
fn run_watchdog(
    agents: &[Arc<AgentShared>],
    roster: &RosterClient,
    incidents: &Arc<Mutex<Vec<Incident>>>,
    watchdog_timeout: Duration,
    stop: &Receiver<()>,
) {
    // Scan on every tick; a stop signal (or the dropped fleet disconnecting) ends
    // it.
    while let Err(RecvTimeoutError::Timeout) = stop.recv_timeout(POLL_INTERVAL) {
        let now = Instant::now();
        for shared in agents {
            if shared.state() != AgentState::Working {
                continue;
            }
            if now.duration_since(shared.last_activity()) < watchdog_timeout {
                continue;
            }
            shared.reap();
            let detail = format!(
                "no output for over {}s and the driver did not recover it; \
                 reaped the orphaned process and handed it to the operator",
                watchdog_timeout.as_secs(),
            );
            event!(
                name: "supervisor.watchdog.reaped",
                Level::WARN,
                crew.role = %shared.role,
                "the watchdog reaped `{{crew.role}}`; its driver stalled",
            );
            // Record before the roster death event, so `died` on the stream never
            // arrives ahead of the incident behind it.
            incidents
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(Incident {
                    role: shared.role.clone(),
                    cause: DeathCause::Wedged,
                    detail,
                    recovery: Recovery::HandedOff,
                });
            shared.set_state(AgentState::Dead);
            let _ = roster.mark(&shared.role, Liveness::Dead);
        }
    }
}

/// One agent's lifecycle state machine, owned by its driver thread.
struct AgentLifecycle {
    shared: Arc<AgentShared>,
    owned_paths: Vec<String>,
    command: AgentCommand,
    roster: RosterClient,
    sink: Sender<Captured>,
    policy: LifecyclePolicy,
    /// How many times the agent has been revived since the last manual start.
    recoveries: u32,
    incidents: Arc<Mutex<Vec<Incident>>>,
}

impl AgentLifecycle {
    fn new(
        prepared: PreparedAgent,
        roster: RosterClient,
        sink: Sender<Captured>,
        policy: LifecyclePolicy,
        shared: Arc<AgentShared>,
        incidents: Arc<Mutex<Vec<Incident>>>,
    ) -> Self {
        Self {
            shared,
            owned_paths: prepared.owned_paths,
            command: prepared.command,
            roster,
            sink,
            policy,
            recoveries: 0,
            incidents,
        }
    }

    /// Starts or restarts the agent on demand, resetting the recovery budget.
    ///
    /// A no-op if it is already running.
    fn ensure_running(&mut self) -> Result<()> {
        if self.shared.state() == AgentState::Working {
            return Ok(());
        }
        // A work-driven or operator start earns a fresh recovery budget.
        self.recoveries = 0;
        self.spawn()
    }

    /// Spawns the process and registers the role working.
    ///
    /// The register publishes the lifecycle event: `started` for the first
    /// working, `recovered` coming back from dead, `restarted` otherwise.
    fn spawn(&mut self) -> Result<()> {
        // Fetch the briefing packet at this spawn (not at provision) so it is current
        // for a lazily started role, and fold it into the opening turn (issue #122).
        // Best-effort: an unreachable broker boots the agent on its card briefing.
        let command = boot_command(&self.command, &self.roster, &self.shared.role);
        let mut child = spawn_process(&command)?;
        if let Some(stdout) = child.stdout.take() {
            capture(
                OutputStream::Stdout,
                stdout,
                self.sink.clone(),
                Arc::clone(&self.shared),
            );
        }
        if let Some(stderr) = child.stderr.take() {
            capture(
                OutputStream::Stderr,
                stderr,
                self.sink.clone(),
                Arc::clone(&self.shared),
            );
        }
        self.shared.set_child(child);
        self.shared.touch();
        // Set state before notifying the roster, so observing the roster change implies
        // the state has caught up.
        self.shared.set_state(AgentState::Working);
        if let Err(err) = self.roster.register(&self.shared.role, &self.owned_paths) {
            self.warn_roster("register", &err);
        }
        Ok(())
    }

    /// Recovers the agent from a death: reap, record, mark dead, then revive or
    /// hand off.
    fn defibrillate(&mut self, cause: DeathCause) {
        // Shock: reap the orphan (a hang is still alive; a crash already exited).
        self.shared.reap();
        let within_budget = self.recoveries < self.policy.max_recoveries;
        let recovery = if within_budget {
            Recovery::Revived
        } else {
            Recovery::HandedOff
        };
        // Record the incident before the roster death event, so an observer that sees
        // the `died` on the stream also sees the incident behind it.
        self.record_incident(cause, recovery);
        // Flatline: mark the role dead so the stream carries the death.
        self.shared.set_state(AgentState::Dead);
        if let Err(err) = self.roster.mark(&self.shared.role, Liveness::Dead) {
            self.warn_roster("mark dead", &err);
        }

        if within_budget {
            self.recoveries += 1;
            // Revive: re-registering working from dead publishes a `recovered` event.
            if let Err(err) = self.spawn() {
                self.warn_roster("revive", &err);
            }
        } else {
            event!(
                name: "supervisor.agent.handoff",
                Level::WARN,
                crew.role = %self.shared.role,
                crew.recoveries = self.policy.max_recoveries,
                "`{{crew.role}}` died after {{crew.recoveries}} recoveries; leaving it for the operator",
            );
        }
    }

    /// Records a death, so the operator can see what happened even if the
    /// revive fails.
    fn record_incident(&self, cause: DeathCause, recovery: Recovery) {
        let detail = match cause {
            DeathCause::Crashed => "the agent process exited unexpectedly".to_owned(),
            DeathCause::Hung => format!(
                "no output for over {}s; presumed hung",
                self.policy.heartbeat_timeout.as_secs(),
            ),
            DeathCause::Wedged => "the driver stalled; the watchdog reaped it".to_owned(),
        };
        event!(
            name: "supervisor.agent.incident",
            Level::WARN,
            crew.role = %self.shared.role,
            crew.cause = ?cause,
            crew.recovery = ?recovery,
            "{{crew.role}}: {detail}",
        );
        self.incidents
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Incident {
                role: self.shared.role.clone(),
                cause,
                detail,
                recovery,
            });
    }

    /// Idle-stops the agent: stop its process, park it (an `idle`), keep its
    /// entry.
    fn idle_stop(&mut self) {
        self.shared.reap();
        self.shared.set_state(AgentState::Idle);
        if let Err(err) = self.roster.mark(&self.shared.role, Liveness::Idle) {
            self.warn_roster("mark idle", &err);
        }
        event!(
            name: "supervisor.agent.idle",
            Level::INFO,
            crew.role = %self.shared.role,
            "idle-stopped `{{crew.role}}` after a quiet period",
        );
    }

    /// Stands the agent down: stop its process and mark it stopped, keeping its
    /// entry.
    fn stop(&mut self) {
        if self.shared.state() == AgentState::Stopped {
            return;
        }
        self.shared.reap();
        self.shared.set_state(AgentState::Stopped);
        if let Err(err) = self.roster.mark(&self.shared.role, Liveness::Stopped) {
            self.warn_roster("mark stopped", &err);
        }
    }

    /// Stands the agent down and deregisters its role (on fleet shutdown).
    fn shutdown(&mut self) {
        self.shared.reap();
        self.shared.set_state(AgentState::Stopped);
        if let Err(err) = self.roster.deregister(&self.shared.role) {
            self.warn_roster("deregister", &err);
        }
    }

    /// One tick: recover a crash or a hang, else idle-stop when quiet.
    fn poll(&mut self, now: Instant) {
        if self.shared.state() != AgentState::Working {
            return;
        }
        if self.shared.process_exited() {
            self.defibrillate(DeathCause::Crashed);
            return;
        }
        let silent = now.duration_since(self.shared.last_activity());
        if silent >= self.policy.heartbeat_timeout {
            self.defibrillate(DeathCause::Hung);
            return;
        }
        if silent >= self.policy.idle_timeout {
            self.idle_stop();
        }
    }

    /// Logs a best-effort roster call that failed; a stale entry is not fatal.
    fn warn_roster(&self, action: &str, err: &eyre::Report) {
        event!(
            name: "supervisor.agent.roster.failed",
            Level::WARN,
            crew.role = %self.shared.role,
            crew.action = action,
            error = %err,
            "could not {{crew.action}} `{{crew.role}}` on the broker roster",
        );
    }
}

/// Reads `pipe` line by line on a detached thread, sending each line to `sink`
/// and stamping the shared activity clock so output defers the idle-stop and
/// the heartbeat.
///
/// Ends at EOF (the process closed the stream, i.e. exited) or once the
/// receiver is gone, so it never outlives its process.
fn capture(
    stream: OutputStream,
    pipe: impl Read + Send + 'static,
    sink: Sender<Captured>,
    shared: Arc<AgentShared>,
) {
    thread::spawn(move || {
        for line in BufReader::new(pipe).lines() {
            let Ok(line) = line else { break };
            shared.touch();
            let captured = Captured {
                role: shared.role.clone(),
                stream,
                line,
            };
            if sink.send(captured).is_err() {
                break;
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::LifecyclePolicy;

    #[test]
    fn the_default_policy_layers_idle_heartbeat_and_watchdog() {
        let policy = LifecyclePolicy::default();
        assert_eq!(policy.idle_timeout.as_secs(), 5 * 60);
        assert_eq!(policy.heartbeat_timeout.as_secs(), 20 * 60);
        assert_eq!(policy.watchdog_timeout.as_secs(), 25 * 60);
        assert_eq!(policy.max_recoveries, 3);
        // The watchdog must sit above the in-turn heartbeat, so a live driver acts
        // first.
        assert!(policy.watchdog_timeout > policy.heartbeat_timeout);
        // A coordination stall is escalated well before a hung process is reaped.
        assert_eq!(policy.stall_timeout.as_secs(), 10 * 60);
        assert_eq!(policy.stall_scan_interval.as_secs(), 60);
        assert!(policy.stall_timeout < policy.heartbeat_timeout);
    }
}
