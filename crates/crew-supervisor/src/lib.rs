//! The crew process supervisor.
//!
//! Turns a set of resolved [`RoleCard`]s into running, connected agents: it
//! spawns one `claude -p` process per role with its role card and the crew MCP
//! server, wires each to the broker roster, and captures each process's output
//! for the activity parser. Built on the types in [`crew_core`].
//!
//! The pieces, in the order a bring-up uses them:
//!
//! - [`register_server`] auto-registers the crew MCP server at user scope so a
//!   spawned agent gets the crew tools with no per-task approval (issue #20),
//!   and [`agent_turn_argv`] builds the `bypassPermissions` turn that loads it
//!   silently.
//! - [`provision`] writes a role's card where the agent can read it and returns
//!   the [`Launch`] the child process starts from. The standalone flow (the
//!   `crew-mcp` binary) reads the very same card, so both paths share one
//!   loader in [`crew_core`].
//! - [`Supervisor::up`] runs the whole flow (issue #21): it spawns one process
//!   per role, registers each on the roster on start and deregisters on exit,
//!   and captures stdout and stderr, returning a running [`Crew`].
//! - [`Fleet`] manages each agent's lifecycle (issue #22): lazy start on first
//!   work, idle-stop after a quiet period, and restart on demand, emitting a
//!   lifecycle event on every transition. Its defibrillator (issue #23) detects
//!   an agent whose turn died, whether it crashed or hung, with a layered
//!   heartbeat and watchdog; it records an [`Incident`] and revives the agent
//!   within a recovery budget, handing it to the operator once the budget is
//!   spent. Its coordination-stall monitor (issue #48) extends that to the crew
//!   as a whole: it reads the event stream for a deadlock, an unanswered
//!   question, or a ledger with no forward motion, and escalates the specific
//!   [`Stall`] so silence never reads as progress.

mod activity;
mod integrate;
mod lifecycle;
mod mcp;
mod roster;
mod spawn;
mod stall;
mod worktree;

use std::{
    fs,
    path::{Path, PathBuf},
};

pub use crew_core::StallKind;
use crew_core::{RoleCard, Runtime, ROLE_CARD_ENV};
use eyre::{Result, WrapErr};
pub use integrate::{
    CheckOutcome, Conflict, IntegrationReport, Integrator, Standing, DEFAULT_INTEGRATION_BRANCH,
};
pub use lifecycle::{AgentState, DeathCause, Fleet, Incident, LifecyclePolicy, Recovery};
pub use mcp::{agent_turn_argv, codex_turn_argv, locate_server, register_server, MCP_SERVER_NAME};
pub use roster::{Liveness, RosterClient};
pub use spawn::{
    agent_command, AgentCommand, Captured, Crew, OutputStream, PreparedAgent, Supervisor,
};
pub use stall::{detect_stalls, Stall};
pub use worktree::Worktree;

/// The file name a provisioned role card is written under, in the agent's
/// directory.
const CARD_FILE_NAME: &str = "role-card.toml";

/// Everything needed to launch one role's agent process from its card.
///
/// The supervisor spawns the agent with [`env`](Launch::env) set and hands the
/// agent its [`briefing`](Launch::briefing) as the opening prompt.
#[derive(Debug, Clone)]
pub struct Launch {
    /// Where the role card was written, so the child can read it back.
    pub card_path: PathBuf,
    /// The environment the child MCP server reads to reach the unit.
    ///
    /// One entry today: [`ROLE_CARD_ENV`] pointing at
    /// [`card_path`](Launch::card_path).
    pub env: Vec<(String, String)>,
    /// The thin bootstrap prompt for the agent (see [`RoleCard::briefing`]).
    pub briefing: String,
    /// The runtime to spawn the agent on (issue #128): `claude` with the MCP
    /// server, or `codex` wired to the CLI shim.
    pub runtime: Runtime,
}

/// Writes `card` into `agent_dir` and returns the [`Launch`] its agent boots
/// from.
///
/// This is the boot step of spawning an agent: the card is serialized to
/// `agent_dir/role-card.toml`, and the returned environment points the child's
/// MCP server at it. `agent_dir` must already exist.
///
/// # Errors
/// Returns an error if the card cannot be serialized or written to `agent_dir`.
///
/// # Examples
/// ```no_run
/// use std::path::Path;
///
/// use crew_core::{BrokerEndpoint, RoleCard, RoleId};
/// use crew_supervisor::provision;
///
/// let card = RoleCard::new(
///     RoleId::new("backend"),
///     vec!["api/".to_owned()],
///     "Tests green.",
///     BrokerEndpoint::new("127.0.0.1", 2739),
/// );
/// let launch = provision(&card, Path::new("/tmp/agents/backend"))?;
/// assert_eq!(launch.env[0].0, "CREW_ROLE_CARD");
/// # Ok::<(), eyre::Report>(())
/// ```
pub fn provision(card: &RoleCard, agent_dir: &Path) -> Result<Launch> {
    let toml = card
        .to_toml()
        .wrap_err("could not serialize the role card")?;
    let card_path = agent_dir.join(CARD_FILE_NAME);
    fs::write(&card_path, toml)
        .wrap_err_with(|| format!("could not write the role card to {}", card_path.display()))?;

    Ok(Launch {
        env: vec![(ROLE_CARD_ENV.to_owned(), card_path.display().to_string())],
        briefing: card.briefing(),
        runtime: card.runtime,
        card_path,
    })
}

#[cfg(test)]
mod tests {
    use crew_core::{BrokerEndpoint, RoleCard, RoleId, ROLE_CARD_ENV};

    use super::{provision, CARD_FILE_NAME};

    /// A unique, empty directory under the system temp dir for one test.
    ///
    /// Named after the test so parallel tests never collide; cleaned on entry
    /// so a prior run never leaks in.
    fn scratch_dir(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("crew-supervisor-test-{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn sample_card() -> RoleCard {
        RoleCard::new(
            RoleId::new("backend"),
            vec!["api/".to_owned()],
            "Tests green, no clippy warnings.",
            BrokerEndpoint::new("127.0.0.1", 2739),
        )
    }

    #[test]
    fn provision_writes_a_card_the_loader_reads_back() {
        let dir = scratch_dir("round-trip");
        let card = sample_card();

        let launch = provision(&card, &dir).unwrap();

        // The child boots from the card path via the shared environment variable.
        assert_eq!(
            launch.env,
            [(
                ROLE_CARD_ENV.to_owned(),
                launch.card_path.display().to_string()
            )]
        );
        assert_eq!(launch.card_path, dir.join(CARD_FILE_NAME));

        // The very loader the standalone agent uses parses the provisioned card back.
        let text = std::fs::read_to_string(&launch.card_path).unwrap();
        let reloaded = RoleCard::from_toml(&text).unwrap();
        assert_eq!(
            reloaded, card,
            "the provisioned card round-trips through the loader"
        );
    }

    #[test]
    fn provision_hands_the_agent_its_briefing() {
        let dir = scratch_dir("briefing");
        let launch = provision(&sample_card(), &dir).unwrap();
        assert!(
            launch.briefing.contains("backend"),
            "the briefing names the role"
        );
        assert!(
            launch.briefing.contains("api/"),
            "the briefing states the lane"
        );
    }

    #[test]
    fn provision_fails_when_the_directory_is_missing() {
        let dir = scratch_dir("missing").join("does-not-exist");
        assert!(
            provision(&sample_card(), &dir).is_err(),
            "writing into a missing directory is an error",
        );
    }
}
