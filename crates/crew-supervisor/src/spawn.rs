//! Spawning one agent process per role and wiring it to the broker.
//!
//! This is the core of the auto-spawn experience (issue #21): turn a set of resolved
//! [`RoleCard`]s into running, connected agents. For each role the supervisor
//! provisions its card, registers the role on the broker roster, spawns a headless
//! `claude -p` process that loads the crew MCP server, and captures the process's
//! stdout and stderr for the activity parser (issue #24). When a process exits, its
//! role is deregistered from the roster, so the roster always reflects who is live.
//!
//! [`Supervisor::up`] is the production entry that invokes `claude`. The spawn and
//! lifecycle mechanics live in [`Crew::spawn`], which takes fully-resolved
//! [`AgentCommand`]s, so the process management is exercised in tests with a stub
//! command instead of a real agent.

use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex, PoisonError};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crew_core::{BrokerEndpoint, RoleCard, RoleId};
use eyre::{Result, WrapErr};
use tracing::{event, Level};

use crate::mcp::{agent_turn_argv, locate_server, register_server, CLAUDE_BIN};
use crate::provision;
use crate::roster::RosterClient;

/// How often a monitor thread checks whether its agent process has exited.
///
/// Polling (rather than a blocking wait) keeps the child behind a mutex that a
/// shutdown can lock to kill it, so exit detection and shutdown never deadlock.
/// Agents are long-lived, so this interval adds negligible overhead.
const EXIT_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// Which of a process's two output streams a captured line came from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputStream {
    /// The process's standard output.
    Stdout,
    /// The process's standard error.
    Stderr,
}

/// One line captured from a spawned agent's output, tagged with its role and stream.
///
/// The supervisor streams these so the activity parser (issue #24) can turn a role's
/// stream-json into typed activity events; until then a consumer can read them raw.
#[derive(Debug, Clone)]
pub struct Captured {
    /// The role whose process produced the line.
    pub role: RoleId,
    /// Which stream it came from.
    pub stream: OutputStream,
    /// The line, without its trailing newline.
    pub line: String,
}

/// A fully-resolved description of the OS process that runs one agent.
///
/// [`agent_command`] builds the real `claude` command; a test builds a stub with the
/// same shape, so [`Crew::spawn`] drives identical lifecycle code either way.
#[derive(Debug, Clone)]
pub struct AgentCommand {
    /// The program to run (`claude` for a real agent).
    pub program: String,
    /// The arguments after the program.
    pub args: Vec<String>,
    /// Environment variables set for the process, on top of the inherited ones.
    pub env: Vec<(String, String)>,
    /// The working directory the process runs in.
    pub cwd: PathBuf,
}

/// A role prepared for spawning: its identity, the lane it owns, and its command.
#[derive(Debug, Clone)]
pub struct PreparedAgent {
    /// The role this agent plays.
    pub role: RoleId,
    /// The paths the role owns, registered with the roster.
    pub owned_paths: Vec<String>,
    /// The command that launches the agent process.
    pub command: AgentCommand,
}

/// Builds the `claude` command that runs one agent's headless turn from its boot card.
///
/// The command loads the user-scope crew MCP server with no prompt (see
/// [`agent_turn_argv`](crate::agent_turn_argv)) and inherits `launch`'s environment,
/// which points the MCP server at the role card (and thus the broker). `cwd` is the
/// directory the agent works in.
#[must_use]
pub fn agent_command(launch: &crate::Launch, cwd: &Path) -> AgentCommand {
    // The turn always starts with the program, then its args; guard the split so an
    // (impossible) empty argv falls back to the program name rather than panicking.
    let mut turn = agent_turn_argv(&launch.briefing);
    let program = if turn.is_empty() {
        CLAUDE_BIN.to_owned()
    } else {
        turn.remove(0)
    };
    AgentCommand {
        program,
        args: turn,
        env: launch.env.clone(),
        cwd: cwd.to_path_buf(),
    }
}

/// Spawns and manages the agent processes for a crew (issue #21).
///
/// [`up`](Supervisor::up) is the whole flow: register the crew MCP server, then
/// provision and spawn every role. It holds the broker address and the root directory
/// under which each role's card and working directory live.
#[derive(Debug, Clone)]
pub struct Supervisor {
    broker: BrokerEndpoint,
    root: PathBuf,
}

impl Supervisor {
    /// Builds a supervisor for the broker at `broker`, rooting agent dirs under `root`.
    #[must_use]
    pub fn new(broker: BrokerEndpoint, root: impl Into<PathBuf>) -> Self {
        Self {
            broker,
            root: root.into(),
        }
    }

    /// Brings a crew online: register the MCP server, then spawn every role.
    ///
    /// Registers the crew MCP server once at user scope so every agent loads the crew
    /// tools with no prompt (issue #20), provisions each role's card, and spawns one
    /// `claude` process per role, each registered on the roster. Returns the running
    /// [`Crew`].
    ///
    /// # Errors
    /// Returns an error if the MCP server cannot be located or registered, a card
    /// cannot be provisioned, or an agent cannot be registered or spawned; any agents
    /// already started are shut down before the error is returned.
    pub fn up(&self, cards: &[RoleCard]) -> Result<Crew> {
        // One-time, unit-wide: make the crew tools available with no approval gate.
        let server = locate_server()?;
        register_server(&server)?;

        let mut prepared = Vec::with_capacity(cards.len());
        for card in cards {
            let dir = self.root.join(card.role.as_str());
            std::fs::create_dir_all(&dir)
                .wrap_err_with(|| format!("could not create agent dir {}", dir.display()))?;
            let launch = provision(card, &dir)?;
            prepared.push(PreparedAgent {
                role: card.role.clone(),
                owned_paths: card.owned_paths.clone(),
                command: agent_command(&launch, &dir),
            });
        }

        let roster = RosterClient::new(self.broker.base_url());
        Crew::spawn(&roster, prepared)
    }
}

/// A running crew: the spawned agent processes and their broker registration.
///
/// Each agent is registered on the roster while its process runs and deregistered
/// when it exits. Read captured output with [`outputs`](Crew::outputs), and stop the
/// crew with [`shutdown`](Crew::shutdown). Dropping the crew without shutting down
/// still kills the processes, so it never leaks them.
#[derive(Debug)]
pub struct Crew {
    agents: Vec<AgentHandle>,
    output: Receiver<Captured>,
}

impl Crew {
    /// Spawns each prepared agent, registering it on `roster` and capturing its output.
    ///
    /// Registers a role, then spawns its process; when the process exits, a monitor
    /// deregisters the role. If any agent fails to register or spawn, the agents
    /// already started are shut down and the error is returned, so a partial bring-up
    /// never leaks a process or a registration.
    ///
    /// # Errors
    /// Returns the first agent's registration or spawn failure.
    pub fn spawn(roster: &RosterClient, agents: Vec<PreparedAgent>) -> Result<Self> {
        let (sink, output) = mpsc::channel();
        let mut handles = Vec::with_capacity(agents.len());
        for agent in agents {
            match spawn_agent(roster, agent, &sink) {
                Ok(handle) => handles.push(handle),
                Err(err) => {
                    stop_all(&mut handles);
                    return Err(err);
                }
            }
        }
        Ok(Self {
            agents: handles,
            output,
        })
    }

    /// The roles currently under supervision.
    pub fn roles(&self) -> impl Iterator<Item = &RoleId> {
        self.agents.iter().map(|agent| &agent.role)
    }

    /// The stream of lines captured from every agent's stdout and stderr.
    ///
    /// The channel closes once every agent's process has exited and its readers have
    /// drained, so a consumer can read until the channel disconnects.
    #[must_use]
    pub fn outputs(&self) -> &Receiver<Captured> {
        &self.output
    }

    /// Stops the crew: kills each process and waits for its role to be deregistered.
    ///
    /// # Errors
    /// Never returns an error today; the `Result` leaves room for surfacing a
    /// shutdown fault without breaking callers.
    pub fn shutdown(mut self) -> Result<()> {
        stop_all(&mut self.agents);
        Ok(())
    }
}

impl Drop for Crew {
    fn drop(&mut self) {
        // Safety net if `shutdown` was not called: kill every process so none leaks.
        // Monitor threads, if still running, deregister after the kill; they are not
        // joined here so drop never blocks.
        for agent in &self.agents {
            kill(&agent.child);
        }
    }
}

/// One supervised agent: its role, its process, and the monitor watching for exit.
#[derive(Debug)]
struct AgentHandle {
    role: RoleId,
    child: Arc<Mutex<Child>>,
    /// The monitor thread; taken by [`stop_all`] to join it after the kill.
    monitor: Option<JoinHandle<()>>,
}

/// Registers a role, spawns its process, and starts capture and monitor threads.
fn spawn_agent(
    roster: &RosterClient,
    agent: PreparedAgent,
    sink: &Sender<Captured>,
) -> Result<AgentHandle> {
    let PreparedAgent {
        role,
        owned_paths,
        command,
    } = agent;

    // The role is on the roster the moment its process starts.
    roster.register(&role, &owned_paths)?;

    let mut child = match spawn_process(&command) {
        Ok(child) => child,
        Err(err) => {
            // The process never started, so undo the registration we just made.
            let _ = roster.deregister(&role);
            return Err(err);
        }
    };

    // Capture each stream on its own thread; the readers end at EOF (on exit).
    if let Some(stdout) = child.stdout.take() {
        capture(role.clone(), OutputStream::Stdout, stdout, sink.clone());
    }
    if let Some(stderr) = child.stderr.take() {
        capture(role.clone(), OutputStream::Stderr, stderr, sink.clone());
    }

    let child = Arc::new(Mutex::new(child));
    let monitor = spawn_monitor(role.clone(), Arc::clone(&child), roster.clone());

    event!(
        name: "supervisor.agent.spawned",
        Level::INFO,
        crew.role = %role,
        "spawned agent `{{crew.role}}`",
    );

    Ok(AgentHandle {
        role,
        child,
        monitor: Some(monitor),
    })
}

/// Spawns the agent's OS process with its command, env, and cwd, output piped.
fn spawn_process(command: &AgentCommand) -> Result<Child> {
    Command::new(&command.program)
        .args(&command.args)
        .envs(command.env.iter().map(|(key, value)| (key, value)))
        .current_dir(&command.cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .wrap_err_with(|| format!("could not spawn agent process `{}`", command.program))
}

/// Reads `pipe` line by line on a detached thread, sending each line to `sink`.
///
/// Ends at EOF (the process closed the stream, i.e. exited) or once the receiver is
/// gone, so it never outlives the crew.
fn capture(
    role: RoleId,
    stream: OutputStream,
    pipe: impl Read + Send + 'static,
    sink: Sender<Captured>,
) {
    thread::spawn(move || {
        for line in BufReader::new(pipe).lines() {
            let Ok(line) = line else { break };
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

/// Starts the monitor thread that deregisters `role` once its process exits.
fn spawn_monitor(role: RoleId, child: Arc<Mutex<Child>>, roster: RosterClient) -> JoinHandle<()> {
    thread::spawn(move || {
        // Poll for exit, holding the lock only briefly so a shutdown can kill the
        // child between polls (see EXIT_POLL_INTERVAL).
        loop {
            let mut guard = child.lock().unwrap_or_else(PoisonError::into_inner);
            let exited = matches!(guard.try_wait(), Ok(Some(_)));
            drop(guard);
            if exited {
                break;
            }
            thread::sleep(EXIT_POLL_INTERVAL);
        }
        if let Err(err) = roster.deregister(&role) {
            event!(
                name: "supervisor.agent.deregister.failed",
                Level::WARN,
                crew.role = %role,
                error = %err,
                "could not deregister `{{crew.role}}` on exit",
            );
        }
    })
}

/// Kills a child process if it is still running; an already-exited child is fine.
fn kill(child: &Arc<Mutex<Child>>) {
    let mut guard = child.lock().unwrap_or_else(PoisonError::into_inner);
    let _ = guard.kill();
}

/// Kills every agent's process and joins its monitor, which deregisters the role.
fn stop_all(agents: &mut [AgentHandle]) {
    for agent in agents.iter() {
        kill(&agent.child);
    }
    for agent in agents.iter_mut() {
        if let Some(monitor) = agent.monitor.take() {
            let _ = monitor.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::agent_command;
    use crate::Launch;

    fn launch() -> Launch {
        Launch {
            card_path: Path::new("/tmp/agents/backend/role-card.toml").to_path_buf(),
            env: vec![(
                "CREW_ROLE_CARD".to_owned(),
                "/tmp/agents/backend/role-card.toml".to_owned(),
            )],
            briefing: "You are the backend role.".to_owned(),
        }
    }

    #[test]
    fn agent_command_runs_a_bypass_permissions_claude_turn() {
        let command = agent_command(&launch(), Path::new("/work/backend"));
        assert_eq!(command.program, "claude");
        // The briefing is the prompt, and the bypass flag loads the MCP with no prompt.
        assert_eq!(command.args[0], "-p");
        assert_eq!(command.args[1], "You are the backend role.");
        let mode = command
            .args
            .iter()
            .position(|a| a == "--permission-mode")
            .unwrap();
        assert_eq!(command.args[mode + 1], "bypassPermissions");
    }

    #[test]
    fn agent_command_carries_the_card_env_and_working_dir() {
        let command = agent_command(&launch(), Path::new("/work/backend"));
        assert_eq!(command.cwd, Path::new("/work/backend"));
        assert!(
            command.env.iter().any(|(key, _)| key == "CREW_ROLE_CARD"),
            "the process inherits the role card, so the MCP server reaches the broker",
        );
    }
}
