//! Spawning one agent process per role and wiring it to the broker.
//!
//! This is the core of the auto-spawn experience (issue #21): turn a set of
//! resolved [`RoleCard`]s into running, connected agents. For each role the
//! supervisor provisions its card, registers the role on the broker roster,
//! spawns a headless `claude -p` process that loads the crew MCP server, and
//! captures the process's stdout and stderr for the activity parser (issue
//! #24). When a process exits, its role is deregistered from the roster, so the
//! roster always reflects who is live.
//!
//! At the spawn moment [`boot_command`] fetches the role's briefing packet
//! (issue #50) and folds it into the opening turn (issue #122), best-effort, so
//! the bounded catch-up is in context even if the agent never calls
//! `crew_briefing`.
//!
//! [`Supervisor::up`] is the production entry that invokes `claude`. The spawn
//! and lifecycle mechanics live in [`Crew::spawn`], which takes fully-resolved
//! [`AgentCommand`]s, so the process management is exercised in tests with a
//! stub command instead of a real agent.

use std::{
    io::{BufRead, BufReader, Read},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::{
        mpsc::{self, Receiver, Sender},
        Arc, Mutex, PoisonError,
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use crew_core::{BrokerEndpoint, CrewConfig, RoleCard, RoleId, Runtime};
use eyre::{Result, WrapErr};
use tracing::{event, Level};

use crate::{
    lifecycle::{Fleet, LifecyclePolicy},
    mcp::{
        agent_turn_argv, codex_turn_argv, locate_server, register_server, CLAUDE_BIN, CODEX_BIN,
    },
    provision,
    roster::RosterClient,
    worktree::{self, Worktree},
};

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

/// One line captured from a spawned agent's output, tagged with its role and
/// stream.
///
/// The supervisor streams these so the activity parser (issue #24) can turn a
/// role's stream-json into typed activity events; until then a consumer can
/// read them raw.
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
/// [`agent_command`] builds the real `claude` command; a test builds a stub
/// with the same shape, so [`Crew::spawn`] drives identical lifecycle code
/// either way.
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

/// A role prepared for spawning: its identity, the lane it owns, and its
/// command.
#[derive(Debug, Clone)]
pub struct PreparedAgent {
    /// The role this agent plays.
    pub role: RoleId,
    /// The paths the role owns, registered with the roster.
    pub owned_paths: Vec<String>,
    /// The command that launches the agent process.
    pub command: AgentCommand,
}

/// Builds the command that runs one agent's headless turn from its boot card,
/// on the runtime the role is configured for (issue #128).
///
/// A `claude` role loads the user-scope crew MCP server (see
/// [`agent_turn_argv`](crate::agent_turn_argv)); a `codex` role runs a headless
/// `codex exec` wired to the CLI shim (see
/// [`codex_turn_argv`](crate::codex_turn_argv)). Either way it inherits
/// `launch`'s environment, which points the agent at the role card (and thus
/// the broker). `cwd` is the directory the agent works in.
#[must_use]
pub fn agent_command(launch: &crate::Launch, cwd: &Path) -> AgentCommand {
    // The turn always starts with the program, then its args; guard the split so an
    // (impossible) empty argv falls back to the program name rather than panicking.
    let (mut turn, fallback) = match launch.runtime {
        Runtime::Claude => (agent_turn_argv(&launch.briefing), CLAUDE_BIN),
        Runtime::Codex => (codex_turn_argv(&launch.briefing), CODEX_BIN),
    };
    let program = if turn.is_empty() {
        fallback.to_owned()
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

/// Registers the crew MCP server once, unit-wide, if any role runs on Claude
/// (issue #128).
///
/// A Claude role reaches the crew through the MCP tools, so the server must be
/// registered; a Codex role uses the CLI shim and needs no MCP. A crew with no
/// Claude role therefore skips registration entirely, so a Codex-only unit
/// never requires `claude` on `PATH`.
///
/// # Errors
/// Returns an error if the MCP server cannot be located or registered.
fn register_mcp_if_needed(mut runtimes: impl Iterator<Item = Runtime>) -> Result<()> {
    if runtimes.any(|runtime| runtime == Runtime::Claude) {
        let server = locate_server()?;
        register_server(&server)?;
    }
    Ok(())
}

/// The lead-in that introduces the injected briefing packet in the boot prompt,
/// so the agent knows this is its live situation and can re-read it any time.
const BRIEFING_PACKET_LEAD_IN: &str =
    "--- Your current briefing packet (fetched at spawn; re-read any time with the crew_briefing \
     tool) ---";

/// The command to spawn `base` with, with `role`'s freshly fetched briefing
/// packet folded into its opening `claude -p` turn (issue #122).
///
/// The new-role briefing packet (issue #50) is otherwise in context only if the
/// agent calls `crew_briefing` first thing. Pushing it into the opening turn
/// guarantees the bounded catch-up is in context even when the agent skips that
/// tool call. It is fetched here, at spawn rather than at provision, so the
/// board and rolling summary are current for a role the fleet starts lazily,
/// long after `launch`.
///
/// Best-effort by design: if the broker is briefly unreachable the agent boots
/// on its card briefing alone, and `crew_briefing` stays the re-read path. A
/// command with no `-p` boot prompt (a test stub) is returned unchanged.
pub(crate) fn boot_command(
    base: &AgentCommand,
    roster: &RosterClient,
    role: &RoleId,
) -> AgentCommand {
    match roster.briefing(role) {
        Ok(packet) if !packet.trim().is_empty() => with_briefing_packet(base, &packet),
        Ok(_) => base.clone(),
        Err(err) => {
            event!(
                name: "supervisor.briefing.skipped",
                Level::DEBUG,
                crew.role = %role,
                error = %err,
                "could not fetch the briefing packet at spawn; booting `{{crew.role}}` on its card briefing",
            );
            base.clone()
        }
    }
}

/// Returns `command` with `packet` appended to its `-p` boot prompt.
///
/// A real `claude` turn carries the boot prompt as the argument after `-p`; a
/// stub command has none, and is returned unchanged.
fn with_briefing_packet(command: &AgentCommand, packet: &str) -> AgentCommand {
    let mut augmented = command.clone();
    if let Some(prompt) = boot_prompt_mut(&mut augmented.args) {
        prompt.push_str("\n\n");
        prompt.push_str(BRIEFING_PACKET_LEAD_IN);
        prompt.push_str("\n\n");
        prompt.push_str(packet);
    }
    augmented
}

/// The mutable boot-prompt argument (the value after `-p`), if the command
/// carries one.
fn boot_prompt_mut(args: &mut [String]) -> Option<&mut String> {
    let flag = args.iter().position(|arg| arg == "-p")?;
    args.get_mut(flag + 1)
}

/// Spawns and manages the agent processes for a crew (issue #21).
///
/// [`up`](Supervisor::up) is the whole flow: register the crew MCP server, then
/// provision and spawn every role. It holds the broker address and the root
/// directory under which each role's card and working directory live.
#[derive(Debug, Clone)]
pub struct Supervisor {
    broker: BrokerEndpoint,
    root: PathBuf,
}

impl Supervisor {
    /// Builds a supervisor for the broker at `broker`, rooting agent dirs under
    /// `root`.
    #[must_use]
    pub fn new(broker: BrokerEndpoint, root: impl Into<PathBuf>) -> Self {
        Self {
            broker,
            root: root.into(),
        }
    }

    /// Brings a crew online: register the MCP server (for Claude roles), then
    /// spawn every role on its configured runtime.
    ///
    /// Registers the crew MCP server once at user scope so every Claude agent
    /// loads the crew tools with no prompt (issue #20), provisions each role's
    /// card, and spawns one process per role, `claude` or `codex` per the
    /// card's runtime (issue #128), each registered on the roster. Returns
    /// the running [`Crew`].
    ///
    /// When `config` opts into worktree isolation (`worktrees` on, with
    /// `repos`), each role works in its own git worktree and the returned crew
    /// owns them, cleaning them up on stand-down (issue #127); pass `None` for
    /// the plain, un-isolated bring-up. This mirrors
    /// [`launch`](Supervisor::launch) so both spawn paths isolate the same way.
    ///
    /// # Errors
    /// Returns an error if the MCP server cannot be located or registered, a
    /// card cannot be provisioned, or an agent cannot be registered or
    /// spawned; any agents already started, and any worktrees already created,
    /// are cleaned up before the error is returned.
    pub fn up(&self, cards: &[RoleCard], config: Option<&CrewConfig>) -> Result<Crew> {
        // Only a Claude role needs the MCP server; a Codex-only crew must not
        // require `claude` (issue #128).
        register_mcp_if_needed(cards.iter().map(|card| card.runtime))?;

        // Resolve worktrees when `config` opts into isolation (issue #127); the
        // card-based entry has no config file, so the workspace anchor for a bare
        // `repos` name is the current directory (issue #126).
        let (prepared, worktrees) = self.prepare(cards, config, Path::new("."))?;
        let roster = RosterClient::new(self.broker.base_url());
        match Crew::spawn(&roster, prepared) {
            // The crew owns the worktrees so they are cleaned up on stand-down.
            Ok(crew) => Ok(crew.with_worktrees(worktrees)),
            Err(err) => {
                // The agents failed to spawn; remove the worktrees `prepare` made,
                // so a failed bring-up leaves none behind.
                worktree::clean_all(&worktrees);
                Err(err)
            }
        }
    }

    /// Launches a lifecycle-managed [`Fleet`] for the crew described by
    /// `config`.
    ///
    /// The counterpart to [`up`](Supervisor::up) for the whole `crew up`
    /// experience (issue #26): it registers the MCP server (for Claude roles),
    /// provisions a card per role, and hands the resolved agents to a
    /// [`Fleet`], which manages lazy start, idle-stop, and the defibrillator
    /// (issues #22, #23). Each agent runs the config's model for its role (its
    /// tier, resolved through the crew's tier map, issue #53) on its configured
    /// runtime (issue #128), and the fleet idle-stops on the config's timeout.
    /// The fleet launches with every agent stopped; bring the unit online with
    /// [`Fleet::start_all`], which registers each role on the roster.
    ///
    /// `config_dir` is the crew config file's own directory: the crew's `repos`
    /// names resolve against it (issue #126), so a bare name means the same
    /// repo wherever `crew up` runs.
    ///
    /// # Errors
    /// Returns an error if the MCP server cannot be located or registered, or a
    /// card cannot be provisioned.
    pub fn launch(&self, config: &CrewConfig, config_dir: &Path) -> Result<Fleet> {
        // Claude roles load the crew tools over MCP; register the server once,
        // unit-wide, only when the crew has a Claude role. A Codex-only crew needs
        // no MCP (its roles use the CLI shim), so it must not require `claude` at
        // all (issue #128).
        register_mcp_if_needed(config.roles.iter().map(|role| role.runtime))?;

        let cards = config.to_cards(&self.broker);
        // With `worktrees` on, each role gets its own worktree of the crew's repos, and
        // the fleet cleans them up on stand-down.
        let (prepared, worktrees) = self.prepare(&cards, Some(config), config_dir)?;
        let roster = RosterClient::new(self.broker.base_url());
        let policy = LifecyclePolicy {
            idle_timeout: config.idle_stop,
            ..LifecyclePolicy::default()
        };
        Ok(Fleet::launch(&roster, prepared, policy)
            .with_worktrees(worktrees)
            .with_budget(config.budget()))
    }

    /// Provisions each card and builds its spawn command into a
    /// [`PreparedAgent`], isolating each role in its own worktree when the
    /// config opts in (issue #43).
    ///
    /// When `config` is given, each command runs the role's configured model
    /// ([`model_for`](CrewConfig::model_for), its tier resolved to an alias),
    /// so the same provisioning serves the eager [`up`](Supervisor::up) and
    /// the lifecycle-managed [`launch`](Supervisor::launch).
    /// With `worktrees` on and repos configured, each role works in its own git
    /// worktree of those repos; the returned worktrees are the ones to clean up
    /// on stand-down. A failure part-way cleans up the worktrees already
    /// created.
    fn prepare(
        &self,
        cards: &[RoleCard],
        config: Option<&CrewConfig>,
        config_dir: &Path,
    ) -> Result<(Vec<PreparedAgent>, Vec<Worktree>)> {
        // Isolation is opt-in and needs repos to isolate. Each `repos` entry is
        // resolved to a path against the workspace root, anchored to the config's
        // own directory, so a bare name works wherever `crew up` runs (issue #126).
        let repos: Vec<PathBuf> = config
            .filter(|config| config.worktrees)
            .map(|config| config.repo_paths(config_dir))
            .unwrap_or_default();

        let mut prepared = Vec::with_capacity(cards.len());
        let mut worktrees = Vec::new();
        for card in cards {
            let dir = self.root.join(card.role.as_str());
            std::fs::create_dir_all(&dir)
                .wrap_err_with(|| format!("could not create agent dir {}", dir.display()))?;
            let launch = provision(card, &dir)?;

            let cwd = match isolate_role(&card.role, &dir, &repos, &mut worktrees) {
                Ok(cwd) => cwd,
                Err(err) => {
                    // Undo the worktrees created so far, so a failed bring-up leaves none.
                    worktree::clean_all(&worktrees);
                    return Err(err);
                }
            };

            let mut command = agent_command(&launch, &cwd);
            if let Some(config) = config {
                command.args.push("--model".to_owned());
                command.args.push(config.model_for(&card.role).to_owned());
            }
            prepared.push(PreparedAgent {
                role: card.role.clone(),
                owned_paths: card.owned_paths.clone(),
                command,
            });
        }
        Ok((prepared, worktrees))
    }
}

/// The working directory for a role: its own worktree of each configured repo,
/// or the agent directory when isolation is off.
///
/// With one repo the role works directly in that worktree; with several, the
/// agent directory holds each repo's worktree as a subdirectory. Created
/// worktrees are pushed onto `worktrees`, so the caller can clean them up.
fn isolate_role(
    role: &RoleId,
    dir: &Path,
    repos: &[PathBuf],
    worktrees: &mut Vec<Worktree>,
) -> Result<PathBuf> {
    if repos.is_empty() {
        return Ok(dir.to_path_buf());
    }
    let start = worktrees.len();
    for repo in repos {
        let dest = dir.join(repo_name(repo));
        worktrees.push(Worktree::create(repo, role, &dest)?);
    }
    let cwd = if worktrees.len() - start == 1 {
        worktrees[start].path().to_path_buf()
    } else {
        dir.to_path_buf()
    };
    Ok(cwd)
}

/// A repo's short name, for its worktree subdirectory: the last path component.
fn repo_name(repo: &Path) -> String {
    repo.file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "repo".to_owned())
}

/// A running crew: the spawned agent processes and their broker registration.
///
/// Each agent is registered on the roster while its process runs and
/// deregistered when it exits. Read captured output with
/// [`outputs`](Crew::outputs), and stop the
/// crew with [`shutdown`](Crew::shutdown), which also cleans up any worktrees
/// the crew owns (see [`with_worktrees`](Crew::with_worktrees)). Dropping the
/// crew without shutting down still kills the processes, so it never leaks
/// them.
#[derive(Debug)]
pub struct Crew {
    agents: Vec<AgentHandle>,
    output: Receiver<Captured>,
    /// The per-role git worktrees to clean up on stand-down (issue #43); empty
    /// unless the crew opted into worktree isolation (issue #127).
    worktrees: Vec<Worktree>,
}

impl Crew {
    /// Spawns each prepared agent, registering it on `roster` and capturing its
    /// output.
    ///
    /// Registers a role, then spawns its process; when the process exits, a
    /// monitor deregisters the role. If any agent fails to register or
    /// spawn, the agents already started are shut down and the error is
    /// returned, so a partial bring-up never leaks a process or a
    /// registration.
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
            worktrees: Vec::new(),
        })
    }

    /// Hands the crew the per-role worktrees to clean up on stand-down (issue
    /// #43, #127).
    ///
    /// The supervisor creates them before spawning; the crew owns them so it
    /// can remove each unchanged one once its agent has stopped (see
    /// [`shutdown`](Crew::shutdown)). This mirrors the lazy
    /// [`Fleet`](crate::Fleet), so both spawn paths behave the same when
    /// isolation is on.
    #[must_use]
    pub fn with_worktrees(mut self, worktrees: Vec<Worktree>) -> Self {
        self.worktrees = worktrees;
        self
    }

    /// The roles currently under supervision.
    pub fn roles(&self) -> impl Iterator<Item = &RoleId> {
        self.agents.iter().map(|agent| &agent.role)
    }

    /// The stream of lines captured from every agent's stdout and stderr.
    ///
    /// The channel closes once every agent's process has exited and its readers
    /// have drained, so a consumer can read until the channel disconnects.
    #[must_use]
    pub fn outputs(&self) -> &Receiver<Captured> {
        &self.output
    }

    /// Stops the crew: kills each process, waits for its role to be
    /// deregistered, then cleans up the crew's worktrees.
    ///
    /// Every agent has stopped before the worktrees are touched, so cleanup
    /// never races a running agent. An unchanged worktree is removed; one with
    /// uncommitted changes is kept for integration (issue #43).
    ///
    /// # Errors
    /// Never returns an error today; the `Result` leaves room for surfacing a
    /// shutdown fault without breaking callers.
    pub fn shutdown(mut self) -> Result<()> {
        stop_all(&mut self.agents);
        crate::worktree::clean_all(&self.worktrees);
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

/// One supervised agent: its role, its process, and the monitor watching for
/// exit.
#[derive(Debug)]
struct AgentHandle {
    role: RoleId,
    child: Arc<Mutex<Child>>,
    /// The monitor thread; taken by [`stop_all`] to join it after the kill.
    monitor: Option<JoinHandle<()>>,
}

/// Registers a role, spawns its process, and starts capture and monitor
/// threads.
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

    // Fold the freshly fetched briefing packet into the opening turn (issue #122),
    // best-effort, so bounded context is in context even if the agent never calls
    // `crew_briefing`.
    let command = boot_command(&command, roster, &role);
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
pub(crate) fn spawn_process(command: &AgentCommand) -> Result<Child> {
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
/// Ends at EOF (the process closed the stream, i.e. exited) or once the
/// receiver is gone, so it never outlives the crew.
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

/// Kills a child process if it is still running; an already-exited child is
/// fine.
fn kill(child: &Arc<Mutex<Child>>) {
    let mut guard = child.lock().unwrap_or_else(PoisonError::into_inner);
    let _ = guard.kill();
}

/// Kills every agent's process and joins its monitor, which deregisters the
/// role.
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

    use crew_core::{RoleId, Runtime};

    use super::{
        agent_command, boot_command, with_briefing_packet, AgentCommand, BRIEFING_PACKET_LEAD_IN,
    };
    use crate::{roster::RosterClient, Launch};

    fn launch() -> Launch {
        launch_on(Runtime::Claude)
    }

    fn launch_on(runtime: Runtime) -> Launch {
        Launch {
            card_path: Path::new("/tmp/agents/backend/role-card.toml").to_path_buf(),
            env: vec![(
                "CREW_ROLE_CARD".to_owned(),
                "/tmp/agents/backend/role-card.toml".to_owned(),
            )],
            briefing: "You are the backend role.".to_owned(),
            runtime,
        }
    }

    /// A stub command with no `-p` boot prompt, like the ones the
    /// process-lifecycle tests drive.
    fn stub() -> AgentCommand {
        AgentCommand {
            program: "bash".to_owned(),
            args: vec!["-c".to_owned(), "echo hi".to_owned()],
            env: Vec::new(),
            cwd: Path::new("/work/backend").to_path_buf(),
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
    fn agent_command_runs_a_headless_codex_turn_for_a_codex_role() {
        // A codex role spawns `codex exec` instead of `claude -p`, with the briefing
        // as the prompt and the autonomy flag standing in for bypassPermissions (#128).
        let command = agent_command(&launch_on(Runtime::Codex), Path::new("/work/backend"));
        assert_eq!(command.program, "codex");
        assert_eq!(command.args[0], "exec");
        assert!(
            command
                .args
                .contains(&"--dangerously-bypass-approvals-and-sandbox".to_owned()),
            "codex runs unattended without approval gates: {:?}",
            command.args,
        );
        assert!(
            command.args.contains(&"--skip-git-repo-check".to_owned()),
            "a codex role outside a git repo must still boot: {:?}",
            command.args,
        );
        assert!(
            command
                .args
                .contains(&"You are the backend role.".to_owned()),
            "the briefing is the codex prompt",
        );
        // The same env reaches the codex agent, so the shim reads its role card.
        assert!(command.env.iter().any(|(key, _)| key == "CREW_ROLE_CARD"));
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

    #[test]
    fn with_briefing_packet_folds_the_packet_into_the_boot_prompt() {
        let base = agent_command(&launch(), Path::new("/work/backend"));
        let augmented =
            with_briefing_packet(&base, "Briefing for backend.\nOn the board: use JWT.");

        let prompt = &augmented.args[1];
        assert!(
            prompt.starts_with("You are the backend role."),
            "the card briefing still leads the prompt: {prompt}"
        );
        assert!(
            prompt.contains(BRIEFING_PACKET_LEAD_IN) && prompt.contains("On the board: use JWT."),
            "the packet is appended under a lead-in: {prompt}"
        );
        // Only the boot prompt changed; the flags and env are untouched.
        assert_eq!(augmented.args[0], "-p");
        assert_eq!(augmented.args[2..], base.args[2..]);
        assert_eq!(augmented.env, base.env);
    }

    #[test]
    fn with_briefing_packet_leaves_a_stub_without_a_prompt_unchanged() {
        let base = stub();
        let augmented = with_briefing_packet(&base, "some packet");
        assert_eq!(
            augmented.args, base.args,
            "a command with no `-p` boot prompt is untouched"
        );
    }

    #[test]
    fn boot_command_falls_back_to_the_base_when_the_broker_is_unreachable() {
        // Port 1 is not listening, so the fetch fails fast (connection refused): the
        // agent must still boot, on its card briefing alone.
        let roster = RosterClient::new("http://127.0.0.1:1");
        let base = agent_command(&launch(), Path::new("/work/backend"));
        let booted = boot_command(&base, &roster, &RoleId::new("backend"));
        assert_eq!(
            booted.args, base.args,
            "an unreachable broker is non-fatal: the boot prompt is the card briefing"
        );
    }
}
