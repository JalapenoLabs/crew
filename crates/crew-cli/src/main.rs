//! The `crew` command-line front-end.
//!
//! Two audiences drive the crew through this one binary:
//!
//! - The operator brings a unit online and stands it down: `crew up` and `crew down`
//!   (issue #26), and gates its work with `crew pause` / `crew resume` / `crew standdown`
//!   (issue #41).
//! - An agent on a runtime without MCP coordinates through the CLI shim (issue #28):
//!   `crew register`, `crew send`, `crew inbox`, and `crew roster` act as the role the
//!   environment names, mapping its I/O onto the broker the same way the MCP tools do
//!   (see `docs/codex.md`). `crew watch` (issue #15) tails a role's self-filtered inbox
//!   stream live, so a peer sees a teammate's messages without polling and never its
//!   own; the upgraded `coworker` skill replaces its `tail -F` monitor with it (see
//!   `docs/communication.md`).
//! - The General steers a running agent with `crew redirect` and `crew belay` (issue
//!   #38): each posts a directive to a role's inbox that the role honors at once,
//!   without tearing the crew down (see `docs/communication.md`).
//!
//! `main` establishes the application conventions (issue #4): eyre errors, the
//! mimalloc allocator, and the shared structured-logging init, then dispatches the
//! parsed command.

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use eyre::Result;
use mimalloc::MiMalloc;

mod broker;
mod control;
mod down;
mod paths;
mod pause;
mod shim;
mod up;

/// mimalloc as the global allocator (M-MIMALLOC-APPS).
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// Command a unit of role-scoped agents as if you were a general directing a team.
#[derive(Debug, Parser)]
#[command(name = "crew", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Bring the whole unit online from the crew config, with roles assigned.
    Up {
        /// The crew config to read. Defaults to `./crew.toml`, then the default crew.
        #[arg(short, long, value_name = "PATH")]
        config: Option<PathBuf>,
    },
    /// Stand the running crew down gracefully: stop the agents and deregister them.
    Down,
    /// Register this agent's role on the roster (a runtime without MCP).
    Register,
    /// Send a message as this agent's role to a teammate, a channel, or the commander.
    Send {
        /// Direct-message one role (its `@role` channel).
        #[arg(long, value_name = "ROLE")]
        to: Option<String>,
        /// Post to a named channel: `all-units`, or a pair like `frontend+backend`.
        #[arg(long, value_name = "CHANNEL")]
        channel: Option<String>,
        /// The message text (markdown).
        body: String,
    },
    /// Read the messages currently addressed to this agent's role.
    Inbox,
    /// Tail a role's self-filtered inbox stream live, or the whole firehose.
    Watch {
        /// Watch one role's self-filtered inbox instead of the whole firehose.
        #[arg(long, value_name = "ROLE")]
        role: Option<String>,
        /// The broker base URL (default: the `CREW_BROKER_HOST` / `PORT` environment).
        #[arg(long, value_name = "URL")]
        broker: Option<String>,
    },
    /// Redirect a role mid-task: inject a steering message it honors immediately.
    Redirect {
        /// The role to steer (its `@role` channel).
        role: String,
        /// The steering message (markdown).
        message: String,
        /// The broker base URL (default: the `CREW_BROKER_HOST` / `PORT` environment).
        #[arg(long, value_name = "URL")]
        broker: Option<String>,
    },
    /// Belay a role: halt its current work and re-task it with a new order.
    Belay {
        /// The role to re-task (its `@role` channel).
        role: String,
        /// The new order (markdown).
        order: String,
        /// The broker base URL (default: the `CREW_BROKER_HOST` / `PORT` environment).
        #[arg(long, value_name = "URL")]
        broker: Option<String>,
    },
    /// Pause a role, or the whole crew: it pulls no new work until resumed.
    Pause {
        /// The role to pause; omit to pause the whole crew.
        role: Option<String>,
        /// The broker base URL (default: the `CREW_BROKER_HOST` / `PORT` environment).
        #[arg(long, value_name = "URL")]
        broker: Option<String>,
    },
    /// Resume a paused role, or the whole crew.
    Resume {
        /// The role to resume; omit to resume the whole crew.
        role: Option<String>,
        /// The broker base URL (default: the `CREW_BROKER_HOST` / `PORT` environment).
        #[arg(long, value_name = "URL")]
        broker: Option<String>,
    },
    /// Stand the crew down: halt every role now, preserving state for recovery.
    Standdown {
        /// The broker base URL (default: the `CREW_BROKER_HOST` / `PORT` environment).
        #[arg(long, value_name = "URL")]
        broker: Option<String>,
    },
    /// List the unit's roster: every role, its lane, and its liveness.
    Roster,
}

fn main() -> Result<()> {
    crew_telemetry::init();

    match Cli::parse().command {
        Command::Up { config } => up::run(config.as_deref()),
        Command::Down => down::run(),
        Command::Register => shim::register(),
        Command::Send { to, channel, body } => shim::send(to.as_deref(), channel.as_deref(), &body),
        Command::Inbox => shim::inbox(),
        Command::Watch { role, broker } => broker::watch(broker.as_deref(), role.as_deref()),
        Command::Redirect {
            role,
            message,
            broker,
        } => control::redirect(broker.as_deref(), &role, &message),
        Command::Belay {
            role,
            order,
            broker,
        } => control::belay(broker.as_deref(), &role, &order),
        Command::Pause { role, broker } => pause::pause(broker.as_deref(), role.as_deref()),
        Command::Resume { role, broker } => pause::resume(broker.as_deref(), role.as_deref()),
        Command::Standdown { broker } => pause::standdown(broker.as_deref()),
        Command::Roster => shim::roster(),
    }
}
