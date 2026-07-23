//! The `crew` command-line front-end.
//!
//! Two audiences drive the crew through this one binary:
//!
//! - The operator brings a unit online and stands it down: `crew up` and `crew down`
//!   (issue #26), and gates its work with `crew pause` / `crew resume` / `crew standdown`
//!   (issue #41).
//! - An agent on a runtime without MCP coordinates through the CLI shim (issue #28):
//!   `crew register`, `crew send`, `crew inbox`, `crew roster`, `crew lane`, `crew claim`,
//!   `crew ledger` (issue #45), the done-gate trio `crew submit` / `crew verdict` /
//!   `crew gate` (issue #47), the situation-board pair `crew board` / `crew record`
//!   (issue #49), and `crew briefing` (issue #50) act as the role the
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
mod integrate;
mod notify;
mod paths;
mod pause;
mod shim;
mod up;
mod usage;

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
    /// Push a native notification on each actionable moment: a question, a death, a stand-down.
    Notify {
        /// Mute one or more moments (comma-separated): `question`, `died`, `stood-down`.
        #[arg(long, value_delimiter = ',', value_name = "MOMENT")]
        mute: Vec<notify::Moment>,
        /// Skip the terminal bell; still show the desktop notification and the log line.
        #[arg(long)]
        no_sound: bool,
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
    /// Show the shared-subscription usage gauge: the reading, threshold, and any auto-pause.
    Usage {
        /// The broker base URL (default: the `CREW_BROKER_HOST` / `PORT` environment).
        #[arg(long, value_name = "URL")]
        broker: Option<String>,
    },
    /// Integrate the roles' `crew/<role>` branches into one coherent, green branch (issue #44).
    Integrate {
        /// The repo whose `crew/<role>` branches to merge. Defaults to the current directory.
        #[arg(long, value_name = "PATH", default_value = ".")]
        repo: String,
        /// The base ref the integration branch is cut from. Defaults to `HEAD`.
        #[arg(long, value_name = "REF", default_value = "HEAD")]
        base: String,
        /// The integration branch to merge onto. Defaults to `crew/integration`.
        #[arg(long, value_name = "NAME", default_value = "crew/integration")]
        branch: String,
        /// An acceptance check to run on the merged result, a shell command, e.g. "cargo test".
        #[arg(long, value_name = "CMD")]
        check: Option<String>,
    },
    /// List the unit's roster: every role, its lane, and its liveness.
    Roster,
    /// Check whether a file path is in this role's lane before editing it.
    Lane {
        /// The repo-relative file path to check against this role's owned lane.
        path: String,
    },
    /// Claim a task on the work ledger, or move this role's claim to a new state.
    Claim {
        /// The task key to claim (a path, a feature, or an order's title).
        task: String,
        /// The state to move it to: `claimed`, `in_progress`, `blocked`, or `done`.
        #[arg(long, default_value = "claimed")]
        state: String,
        /// An optional short label for the ledger.
        #[arg(long)]
        title: Option<String>,
    },
    /// Show the work ledger: every claimed task, its owner, and its state.
    Ledger,
    /// Submit finished work for adversarial verification (not done until it passes).
    Submit {
        /// The task title, matching the order it came from.
        task: String,
        /// The acceptance criteria the work claims to meet.
        #[arg(long, value_name = "TEXT")]
        acceptance: Option<String>,
        /// An optional reviewer role to notify (for example `qa`).
        #[arg(long, value_name = "ROLE")]
        to: Option<String>,
    },
    /// Return a verdict on a task another role submitted: try to break it, then judge.
    Verdict {
        /// The task title under verification.
        task: String,
        /// Pass the task: mark it done because you could not break it.
        #[arg(long)]
        pass: bool,
        /// The specific failure when the task does not pass (required to fail it).
        #[arg(long, value_name = "TEXT")]
        failure: Option<String>,
    },
    /// Read the done-gate: tasks under verification and their standing.
    Gate,
    /// Record or retract a shared situation board entry: a decision, interface, or gotcha.
    Record {
        /// The entry's stable key (its topic), for example `auth-strategy`.
        key: String,
        /// The section: `decision`, `interface`, or `gotcha` (required unless retracting).
        #[arg(long, value_name = "SECTION")]
        section: Option<String>,
        /// The entry's content (required unless retracting).
        #[arg(long, value_name = "TEXT")]
        body: Option<String>,
        /// Retract the entry named by `key` instead of recording one.
        #[arg(long)]
        retract: bool,
    },
    /// Read the shared situation board: the crew's durable memory.
    Board {
        /// Read just one section: `decision`, `interface`, or `gotcha`.
        #[arg(long, value_name = "SECTION")]
        section: Option<String>,
    },
    /// Get this role's bounded briefing packet: catch up without reading the whole log.
    Briefing {
        /// Narrow the summary to this task id, if you have one.
        #[arg(long, value_name = "TASK")]
        task: Option<String>,
        /// Cap the packet size in bytes (defaults to a few KB).
        #[arg(long, value_name = "BYTES")]
        budget: Option<usize>,
    },
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
        Command::Notify {
            mute,
            no_sound,
            broker,
        } => notify::notify(
            broker.as_deref(),
            &notify::NotifyPolicy::new(mute, !no_sound),
        ),
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
        Command::Usage { broker } => usage::usage(broker.as_deref()),
        Command::Integrate {
            repo,
            base,
            branch,
            check,
        } => integrate::integrate(&repo, &base, &branch, check.as_deref()),
        Command::Roster => shim::roster(),
        Command::Lane { path } => shim::lane(&path),
        Command::Claim { task, state, title } => {
            shim::claim(&task, &state, title.as_deref().unwrap_or_default())
        }
        Command::Ledger => shim::ledger(),
        Command::Submit {
            task,
            acceptance,
            to,
        } => shim::submit(&task, acceptance.as_deref(), to.as_deref()),
        Command::Verdict {
            task,
            pass,
            failure,
        } => shim::verdict(&task, pass, failure.as_deref()),
        Command::Gate => shim::gate(),
        Command::Record {
            key,
            section,
            body,
            retract,
        } => shim::record(&key, section.as_deref(), body.as_deref(), retract),
        Command::Board { section } => shim::board(section.as_deref()),
        Command::Briefing { task, budget } => shim::briefing(task.as_deref(), budget),
    }
}
