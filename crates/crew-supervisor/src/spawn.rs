//! Building the spawn command for one agent and preparing a whole crew.
//!
//! The shared front half of the auto-spawn experience (issue #21): turn a set
//! of resolved [`RoleCard`](crew_core::RoleCard)s into [`PreparedAgent`]s, each
//! a [`RoleId`] plus the [`AgentCommand`] that launches its `claude -p` or
//! `codex exec` process. [`Supervisor::launch`] hands those agents to the
//! lifecycle-managed [`Fleet`], the single spawn engine, which owns the running
//! processes, the roster registration, idle-stop, and the defibrillator (issues
//! #22, #23). Provisioning here stays engine-agnostic, so a test drives the
//! same commands the production `crew up` does.
//!
//! At the spawn moment [`boot_command`] fetches the role's briefing packet
//! (issue #50) and folds it into the opening turn (issue #122), best-effort, so
//! the bounded catch-up is in context even if the agent never calls
//! `crew_briefing`.

use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
};

use crew_core::{BrokerEndpoint, CrewConfig, RoleId, Runtime};
use eyre::{Result, WrapErr};
use tracing::{event, Level};

use crate::{
    lifecycle::{Fleet, LifecyclePolicy},
    mcp::{
        agent_turn_argv, codex_turn_argv, locate_server, register_server, CLAUDE_BIN, CODEX_BIN,
    },
    provision,
    roster::RosterClient,
    worktree::Worktree,
};

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
/// with the same shape, so the [`Fleet`] drives identical lifecycle code either
/// way.
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

/// Prepares the agent commands for a crew and launches its [`Fleet`] (issue
/// #21).
///
/// [`launch`](Supervisor::launch) is the whole flow: register the crew MCP
/// server, then provision every role and hand the resolved agents to the
/// lifecycle-managed [`Fleet`]. It holds the broker address and the root
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

    /// Launches a lifecycle-managed [`Fleet`] for the crew described by
    /// `config`.
    ///
    /// Registers the MCP server (for Claude roles), provisions a card per role,
    /// and hands the resolved agents to a [`Fleet`], which manages lazy start,
    /// idle-stop, and the defibrillator (issues #22, #23). Each agent runs the
    /// config's model for its role (its tier, resolved through the crew's tier
    /// map, issue #53) on its configured runtime (issue #128), and the fleet
    /// idle-stops on the config's timeout. The fleet launches with every agent
    /// stopped; bring the unit online with [`Fleet::start_all`], which
    /// registers each role on the roster.
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

        // With `worktrees` on, each role gets its own worktree of the crew's repos, and
        // the fleet cleans them up on stand-down.
        let (prepared, worktrees) = self.prepare(config, config_dir)?;
        let roster = RosterClient::new(self.broker.base_url());
        let policy = LifecyclePolicy {
            idle_timeout: config.idle_stop,
            ..LifecyclePolicy::default()
        };
        Ok(Fleet::launch(&roster, prepared, policy)
            .with_worktrees(worktrees)
            .with_budget(config.budget()))
    }

    /// Provisions each role's card and builds its spawn command into a
    /// [`PreparedAgent`], isolating each role in its own worktree when the
    /// config opts in (issue #43).
    ///
    /// Each command runs the role's configured model
    /// ([`model_for`](CrewConfig::model_for), its tier resolved to an alias).
    /// With `worktrees` on and repos configured, each role works in its own git
    /// worktree of those repos; the returned worktrees are the ones to clean up
    /// on stand-down. A failure part-way cleans up the worktrees already
    /// created.
    fn prepare(
        &self,
        config: &CrewConfig,
        config_dir: &Path,
    ) -> Result<(Vec<PreparedAgent>, Vec<Worktree>)> {
        let cards = config.to_cards(&self.broker);

        // Isolation is opt-in and needs repos to isolate. Each `repos` entry is
        // resolved to a path against the workspace root, anchored to the config's
        // own directory, so a bare name works wherever `crew up` runs (issue #126).
        let repos: Vec<PathBuf> = if config.worktrees {
            config.repo_paths(config_dir)
        } else {
            Vec::new()
        };

        let mut prepared = Vec::with_capacity(cards.len());
        let mut worktrees = Vec::new();
        for card in &cards {
            let dir = self.root.join(card.role.as_str());
            std::fs::create_dir_all(&dir)
                .wrap_err_with(|| format!("could not create agent dir {}", dir.display()))?;
            let launch = provision(card, &dir)?;

            let cwd = match isolate_role(&card.role, &dir, &repos, &mut worktrees) {
                Ok(cwd) => cwd,
                Err(err) => {
                    // Undo the worktrees created so far, so a failed bring-up leaves none.
                    crate::worktree::clean_all(&worktrees);
                    return Err(err);
                }
            };

            let mut command = agent_command(&launch, &cwd);
            command.args.push("--model".to_owned());
            command.args.push(config.model_for(&card.role).to_owned());
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

/// Spawns the agent's OS process with its command, env, and cwd, output piped.
///
/// The single spawn primitive both a real bring-up (through the [`Fleet`]) and
/// a test drive through. The [`Fleet`] owns the resulting [`Child`], registers
/// its role, and captures its output.
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
