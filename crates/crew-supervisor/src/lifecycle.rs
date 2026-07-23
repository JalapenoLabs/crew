//! The agent lifecycle state machine: lazy start, idle-stop, and restart (issue #22).
//!
//! An idle role should cost nothing and a crash should recover. Each agent runs a
//! small state machine on its own driver thread:
//!
//! - **Lazy start.** A [`Fleet`] launches with every agent [`Stopped`](AgentState::Stopped)
//!   and no process; work triggers [`Fleet::start`], which spawns the process and
//!   registers the role (a `started` event).
//! - **Idle-stop.** After a configurable quiet period the driver stops the process but
//!   keeps the roster entry (an `idle` event), so a restart is fast and keeps context.
//! - **Restart.** An unexpected exit restarts the agent, bounded by an attempt budget
//!   (a `restarted` event); exhausting the budget marks it dead (a `died` event). A
//!   [`Fleet::start`] on a stopped agent restarts it on demand.
//!
//! Every transition marks the broker roster, so the roster and the stream reflect it
//! (see `docs/observability.md`). The mechanics build on the [`spawn`](crate::spawn)
//! primitives, so a real `claude` process and a test stub run identical lifecycle code.

use std::io::{BufRead, BufReader, Read};
use std::process::Child;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use crew_core::RoleId;
use eyre::{eyre, Result};
use tracing::{event, Level};

use crate::roster::{Liveness, RosterClient};
use crate::spawn::{spawn_process, AgentCommand, Captured, OutputStream, PreparedAgent};

/// How often a driver checks its agent for an unexpected exit or an idle timeout.
///
/// Small enough that a crash recovers and an idle-stop fires promptly, large enough
/// that an idle fleet costs no meaningful CPU. It bounds the detection latency, so an
/// idle timeout should be at least this long.
const POLL_INTERVAL: Duration = Duration::from_millis(100);

/// The lifecycle policy: how long an agent may be quiet, and its restart budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LifecyclePolicy {
    /// How long an agent may be quiet before it is idle-stopped.
    pub idle_timeout: Duration,
    /// How many times an agent may be restarted after an unexpected exit before it is
    /// declared dead, so a crash loop cannot respawn forever.
    pub max_restarts: u32,
}

impl Default for LifecyclePolicy {
    /// Idle-stop after five quiet minutes, and restart at most three times.
    ///
    /// Five minutes is long enough not to stop an agent mid-thought yet short enough
    /// to reclaim an abandoned one; three restarts recovers a transient crash without
    /// masking a persistent one.
    fn default() -> Self {
        Self {
            idle_timeout: Duration::from_secs(5 * 60),
            max_restarts: 3,
        }
    }
}

/// An agent's supervised lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AgentState {
    /// No process: never started (lazy), or cleanly stood down. Its roster entry is
    /// kept once known, so a restart is fast.
    Stopped,
    /// Its process is running and under supervision.
    Working,
    /// Idle-stopped: no process, but parked and ready to resume on demand.
    Idle,
    /// Gave up after exhausting the restart budget on repeated crashes.
    Dead,
}

/// A running fleet: one lifecycle-managed agent per role.
///
/// Launch it with the roles resolved into [`PreparedAgent`]s; every agent starts
/// [`Stopped`](AgentState::Stopped), so an unused role costs nothing. Drive an agent
/// with [`start`](Fleet::start) and [`stop`](Fleet::stop); read captured output with
/// [`outputs`](Fleet::outputs). Dropping the fleet, like [`shutdown`](Fleet::shutdown),
/// stops every agent and deregisters its role.
#[derive(Debug)]
pub struct Fleet {
    drivers: Vec<AgentDriver>,
    output: Receiver<Captured>,
}

impl Fleet {
    /// Launches a lifecycle driver per agent, each starting stopped (lazy).
    ///
    /// No process is spawned and no role is registered until [`start`](Fleet::start),
    /// so launching an idle fleet is free.
    #[must_use]
    pub fn launch(
        roster: &RosterClient,
        agents: Vec<PreparedAgent>,
        policy: LifecyclePolicy,
    ) -> Self {
        let (sink, output) = mpsc::channel();
        let drivers = agents
            .into_iter()
            .map(|prepared| AgentDriver::spawn(roster.clone(), prepared, policy, sink.clone()))
            .collect();
        Self { drivers, output }
    }

    /// Starts (or restarts) `role`'s agent: lazy start on first work, restart on demand.
    ///
    /// A no-op if the agent is already running. This is asynchronous: the driver
    /// applies it, so observe the change through the roster or [`state`](Fleet::state).
    ///
    /// # Errors
    /// Returns an error if `role` is not in the fleet, or its driver has stopped.
    pub fn start(&self, role: &RoleId) -> Result<()> {
        self.command(role, Command::Start)
    }

    /// Stands `role`'s agent down: stops its process, keeping its roster entry.
    ///
    /// # Errors
    /// Returns an error if `role` is not in the fleet, or its driver has stopped.
    pub fn stop(&self, role: &RoleId) -> Result<()> {
        self.command(role, Command::Stop)
    }

    /// The current lifecycle state of `role`'s agent, if it is in the fleet.
    #[must_use]
    pub fn state(&self, role: &RoleId) -> Option<AgentState> {
        self.driver(role).map(AgentDriver::state)
    }

    /// The roles under management.
    pub fn roles(&self) -> impl Iterator<Item = &RoleId> {
        self.drivers.iter().map(|driver| &driver.role)
    }

    /// The stream of lines captured from every agent's stdout and stderr.
    #[must_use]
    pub fn outputs(&self) -> &Receiver<Captured> {
        &self.output
    }

    /// Stops the fleet: stands every agent down and deregisters its role.
    pub fn shutdown(mut self) {
        for driver in &self.drivers {
            let _ = driver.commands.send(Command::Shutdown);
        }
        for driver in &mut self.drivers {
            if let Some(handle) = driver.handle.take() {
                let _ = handle.join();
            }
        }
    }

    /// The driver for `role`, if present.
    fn driver(&self, role: &RoleId) -> Option<&AgentDriver> {
        self.drivers.iter().find(|driver| &driver.role == role)
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
        // A dropped fleet must not leak agent processes. Dropping the command senders
        // disconnects each driver, which stands its agent down; the detached driver
        // threads finish on their own, so drop never blocks.
        for driver in &self.drivers {
            let _ = driver.commands.send(Command::Shutdown);
        }
    }
}

/// A handle to one agent's lifecycle driver thread.
#[derive(Debug)]
struct AgentDriver {
    role: RoleId,
    commands: Sender<Command>,
    /// The agent's current state, published by the driver for [`Fleet::state`].
    state: Arc<Mutex<AgentState>>,
    handle: Option<JoinHandle<()>>,
}

impl AgentDriver {
    /// Spawns the driver thread for one agent, initially stopped.
    fn spawn(
        roster: RosterClient,
        prepared: PreparedAgent,
        policy: LifecyclePolicy,
        sink: Sender<Captured>,
    ) -> Self {
        let (commands, inbox) = mpsc::channel();
        let state = Arc::new(Mutex::new(AgentState::Stopped));
        let role = prepared.role.clone();
        let agent = AgentLifecycle::new(prepared, roster, sink, policy, Arc::clone(&state));
        let handle = thread::spawn(move || drive(agent, &inbox));
        Self {
            role,
            commands,
            state,
            handle: Some(handle),
        }
    }

    /// Reads the agent's current state.
    fn state(&self) -> AgentState {
        *self.state.lock().unwrap_or_else(PoisonError::into_inner)
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

/// The driver loop: apply commands, and on each idle tick poll for exit or idle-stop.
fn drive(mut agent: AgentLifecycle, inbox: &Receiver<Command>) {
    loop {
        match inbox.recv_timeout(POLL_INTERVAL) {
            Ok(Command::Start) => {
                if let Err(err) = agent.ensure_running() {
                    event!(
                        name: "supervisor.agent.start.failed",
                        Level::WARN,
                        crew.role = %agent.role,
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

/// One agent's lifecycle state machine, owned entirely by its driver thread.
struct AgentLifecycle {
    role: RoleId,
    owned_paths: Vec<String>,
    command: AgentCommand,
    roster: RosterClient,
    sink: Sender<Captured>,
    policy: LifecyclePolicy,
    /// Shared with the driver handle so [`Fleet::state`] can read it.
    state: Arc<Mutex<AgentState>>,
    /// How many times the running process has been restarted since the last manual start.
    restarts: u32,
    /// The current process, when running.
    child: Option<Child>,
    /// When the process last produced output, shared with the capture threads.
    last_activity: Arc<Mutex<Instant>>,
}

impl AgentLifecycle {
    fn new(
        prepared: PreparedAgent,
        roster: RosterClient,
        sink: Sender<Captured>,
        policy: LifecyclePolicy,
        state: Arc<Mutex<AgentState>>,
    ) -> Self {
        Self {
            role: prepared.role,
            owned_paths: prepared.owned_paths,
            command: prepared.command,
            roster,
            sink,
            policy,
            state,
            restarts: 0,
            child: None,
            last_activity: Arc::new(Mutex::new(Instant::now())),
        }
    }

    /// The current state.
    fn state(&self) -> AgentState {
        *self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Publishes a new state for [`Fleet::state`] to read.
    fn set_state(&self, state: AgentState) {
        *self.state.lock().unwrap_or_else(PoisonError::into_inner) = state;
    }

    /// Starts or restarts the agent on demand, resetting the restart budget.
    ///
    /// A no-op if it is already running.
    fn ensure_running(&mut self) -> Result<()> {
        if self.state() == AgentState::Working {
            return Ok(());
        }
        // A work-driven or operator start earns a fresh restart budget.
        self.restarts = 0;
        self.spawn()
    }

    /// Spawns the process and registers the role working (a `started` / `restarted`).
    fn spawn(&mut self) -> Result<()> {
        let mut child = spawn_process(&self.command)?;
        // Capture each stream, updating the activity clock so output defers the idle-stop.
        if let Some(stdout) = child.stdout.take() {
            capture(
                self.role.clone(),
                OutputStream::Stdout,
                stdout,
                self.sink.clone(),
                Arc::clone(&self.last_activity),
            );
        }
        if let Some(stderr) = child.stderr.take() {
            capture(
                self.role.clone(),
                OutputStream::Stderr,
                stderr,
                self.sink.clone(),
                Arc::clone(&self.last_activity),
            );
        }
        self.child = Some(child);
        *self
            .last_activity
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Instant::now();
        // Set state before notifying the roster, so observing the roster change implies
        // the state has caught up. The register is what publishes the lifecycle event:
        // the first `working` is `started`, a later one is `restarted`.
        self.set_state(AgentState::Working);
        if let Err(err) = self.roster.register(&self.role, &self.owned_paths) {
            self.warn_roster("register", &err);
        }
        Ok(())
    }

    /// Handles an unexpected process exit: restart within budget, else declare dead.
    fn on_unexpected_exit(&mut self) {
        // The process was reaped by the exit check, so drop the stale handle.
        self.child = None;
        if self.restarts < self.policy.max_restarts {
            self.restarts += 1;
            event!(
                name: "supervisor.agent.restart",
                Level::INFO,
                crew.role = %self.role,
                crew.restart = self.restarts,
                "restarting `{{crew.role}}` after an unexpected exit (attempt {{crew.restart}})",
            );
            if let Err(err) = self.spawn() {
                self.warn_roster("restart", &err);
            }
        } else {
            self.set_state(AgentState::Dead);
            if let Err(err) = self.roster.mark(&self.role, Liveness::Dead) {
                self.warn_roster("mark dead", &err);
            }
            event!(
                name: "supervisor.agent.dead",
                Level::WARN,
                crew.role = %self.role,
                crew.restarts = self.policy.max_restarts,
                "`{{crew.role}}` died after {{crew.restarts}} restarts; giving up",
            );
        }
    }

    /// Idle-stops the agent: stop its process, park it (an `idle`), keep its entry.
    fn idle_stop(&mut self) {
        self.kill();
        self.set_state(AgentState::Idle);
        if let Err(err) = self.roster.mark(&self.role, Liveness::Idle) {
            self.warn_roster("mark idle", &err);
        }
        event!(
            name: "supervisor.agent.idle",
            Level::INFO,
            crew.role = %self.role,
            "idle-stopped `{{crew.role}}` after a quiet period",
        );
    }

    /// Stands the agent down: stop its process and mark it stopped, keeping its entry.
    fn stop(&mut self) {
        if self.state() == AgentState::Stopped {
            return;
        }
        self.kill();
        self.set_state(AgentState::Stopped);
        if let Err(err) = self.roster.mark(&self.role, Liveness::Stopped) {
            self.warn_roster("mark stopped", &err);
        }
    }

    /// Stands the agent down and deregisters its role (on fleet shutdown).
    fn shutdown(&mut self) {
        self.kill();
        self.set_state(AgentState::Stopped);
        if let Err(err) = self.roster.deregister(&self.role) {
            self.warn_roster("deregister", &err);
        }
    }

    /// One tick: if running, restart on an unexpected exit, else idle-stop when quiet.
    fn poll(&mut self, now: Instant) {
        if self.state() != AgentState::Working {
            return;
        }
        if self.process_exited() {
            self.on_unexpected_exit();
            return;
        }
        let quiet = now.duration_since(
            *self
                .last_activity
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
        );
        if quiet >= self.policy.idle_timeout {
            self.idle_stop();
        }
    }

    /// Whether the running process has exited, reaping it if so.
    fn process_exited(&mut self) -> bool {
        match &mut self.child {
            Some(child) => matches!(child.try_wait(), Ok(Some(_))),
            None => false,
        }
    }

    /// Kills the process if one is running, reaping it to avoid a zombie.
    fn kill(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
    }

    /// Logs a best-effort roster call that failed; a stale entry is not fatal.
    fn warn_roster(&self, action: &str, err: &eyre::Report) {
        event!(
            name: "supervisor.agent.roster.failed",
            Level::WARN,
            crew.role = %self.role,
            crew.action = action,
            error = %err,
            "could not {{crew.action}} `{{crew.role}}` on the broker roster",
        );
    }
}

/// Reads `pipe` line by line on a detached thread, sending each line to `sink` and
/// stamping the activity clock so a burst of output defers the idle-stop.
///
/// Ends at EOF (the process closed the stream, i.e. exited) or once the receiver is
/// gone, so it never outlives its process.
fn capture(
    role: RoleId,
    stream: OutputStream,
    pipe: impl Read + Send + 'static,
    sink: Sender<Captured>,
    last_activity: Arc<Mutex<Instant>>,
) {
    thread::spawn(move || {
        for line in BufReader::new(pipe).lines() {
            let Ok(line) = line else { break };
            *last_activity.lock().unwrap_or_else(PoisonError::into_inner) = Instant::now();
            let captured = Captured {
                role: role.clone(),
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
    fn the_default_policy_is_a_five_minute_idle_stop_and_three_restarts() {
        let policy = LifecyclePolicy::default();
        assert_eq!(policy.idle_timeout.as_secs(), 300);
        assert_eq!(policy.max_restarts, 3);
    }
}
