//! The `crew` command-line front-end.
//!
//! Two audiences drive the crew through this one binary:
//!
//! - The operator brings a unit online and stands it down: `crew up` and `crew
//!   down` (issue #26), and gates its work with `crew pause` / `crew resume` /
//!   `crew standdown` (issue #41).
//! - An agent on a runtime without MCP coordinates through the CLI shim (issue
//!   #28): `crew register`, `crew send`, `crew order` (issue #27), `crew
//!   inbox`, `crew roster`, `crew lane`, `crew claim`, `crew ledger` (issue
//!   #45), the done-gate trio `crew submit` / `crew verdict` / `crew gate`
//!   (issue #47), the situation-board pair `crew board` / `crew record` (issue
//!   #49), and `crew briefing` (issue #50) act as the role the environment
//!   names, mapping its I/O onto the broker the same way the MCP tools do (see
//!   `docs/codex.md`). `crew watch` (issue #15) tails a role's self-filtered
//!   inbox stream live, so a peer sees a teammate's messages without polling
//!   and never its own; the upgraded `coworker` skill replaces its `tail -F`
//!   monitor with it (see `docs/communication.md`).
//! - The General drives the unit with `crew brief` (issue #118): a free-form
//!   note posted as the General to the commander by default, a role, or a
//!   channel. `crew send` unifies with this (issue #192): with no role context
//!   it too posts as the General, so an operator injects a message without a
//!   role card; `crew brief` adds the explicit `--broker` and `--commander`
//!   controls. The General also steers a running agent with `crew redirect` and
//!   `crew belay` (issue #38): each posts a directive to a role's inbox that
//!   the role honors at once, without tearing the crew down (see
//!   `docs/communication.md`).
//!
//! `main` establishes the application conventions (issue #4): eyre errors, the
//! mimalloc allocator, and the shared structured-logging init, then dispatches
//! the parsed command.

use std::path::PathBuf;

use clap::{Parser, Subcommand};
use eyre::Result;
use mimalloc::MiMalloc;

mod broker;
mod broker_base;
mod control;
mod cursor;
mod down;
mod integrate;
mod notify;
mod paths;
mod pause;
mod shim;
mod top;
mod up;
mod usage;

/// mimalloc as the global allocator (M-MIMALLOC-APPS).
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

/// Command a unit of role-scoped agents as if you were a general directing a
/// team.
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
        /// The crew config to read. Defaults to `./crew.toml`, then the default
        /// crew.
        #[arg(short, long, value_name = "PATH")]
        config: Option<PathBuf>,
    },
    /// Stand the running crew down gracefully: stop the agents and deregister
    /// them.
    Down,
    /// Register this agent's role on the roster (a runtime without MCP).
    Register,
    /// Send a message to a teammate, a channel, or the commander: as your role
    /// when `CREW_ROLE` (or a role card) is set, else as the General.
    Send {
        /// Direct-message one role (its `@role` channel).
        #[arg(long, value_name = "ROLE")]
        to: Option<String>,
        /// Post to a named channel: `all-units`, or a pair like
        /// `frontend+backend`.
        #[arg(long, value_name = "CHANNEL")]
        channel: Option<String>,
        /// The message text (markdown).
        body: String,
    },
    /// Ask a typed question and wait on a decision (the kind coordination-stall
    /// detection keys on), as this agent's role.
    Ask {
        /// Ask one role directly (its `@role` channel).
        #[arg(long, value_name = "ROLE")]
        to: Option<String>,
        /// Ask on a named channel: `all-units`, or a pair like
        /// `frontend+backend`.
        #[arg(long, value_name = "CHANNEL")]
        channel: Option<String>,
        /// A suggested answer to offer; repeat for several.
        #[arg(long = "option", value_name = "TEXT")]
        options: Vec<String>,
        /// The question (markdown).
        body: String,
    },
    /// Answer a teammate's question, naming the question id from your inbox.
    Answer {
        /// Answer one role directly (usually the asker's `@role` channel).
        #[arg(long, value_name = "ROLE")]
        to: Option<String>,
        /// Answer on a named channel, if the question was asked on one.
        #[arg(long, value_name = "CHANNEL")]
        channel: Option<String>,
        /// The id of the question being answered (shown in `crew inbox`).
        #[arg(long, value_name = "ID")]
        in_reply_to: String,
        /// The answer (markdown).
        body: String,
    },
    /// Issue a structured order to a specialist (commander-only): a scoped
    /// task, not a plain message. The broker refuses an order from any
    /// other role.
    Order {
        /// The specialist role to order (its `@role` channel).
        to: String,
        /// A short title for the task.
        title: String,
        /// What is in and out of scope for the task.
        #[arg(long, value_name = "TEXT")]
        scope: Option<String>,
        /// A path the role owns while working the task; repeat for several.
        #[arg(long = "owns", value_name = "PATH")]
        owns: Vec<String>,
        /// How the task is judged done.
        #[arg(long, value_name = "TEXT")]
        acceptance: Option<String>,
        /// Optional freeform detail (markdown).
        #[arg(long, value_name = "TEXT")]
        body: Option<String>,
    },
    /// Report progress as this agent's role, without asking anything (a typed
    /// `status`, not a plain note).
    Status {
        /// Report to one role directly (its `@role` channel).
        #[arg(long, value_name = "ROLE")]
        to: Option<String>,
        /// Report on a named channel: `all-units`, or a pair like
        /// `frontend+backend`.
        #[arg(long, value_name = "CHANNEL")]
        channel: Option<String>,
        /// The progress note (markdown).
        body: String,
    },
    /// Reference a produced thing (branch, PR, file, or route) as this agent's
    /// role: a typed `artifact`, not a plain note.
    Artifact {
        /// Send to one role directly (its `@role` channel).
        #[arg(long, value_name = "ROLE")]
        to: Option<String>,
        /// Send on a named channel: `all-units`, or a pair like
        /// `frontend+backend`.
        #[arg(long, value_name = "CHANNEL")]
        channel: Option<String>,
        /// What the reference points to: `branch`, `pull_request`, `file`, or
        /// `route`.
        #[arg(long, value_name = "KIND")]
        kind: String,
        /// The produced thing: a branch name, a PR URL, a file path, or a
        /// route.
        reference: String,
        /// Optional freeform detail (markdown).
        #[arg(long, value_name = "TEXT")]
        body: Option<String>,
    },
    /// Read the messages currently addressed to this agent's role.
    Inbox,
    /// Tail a role's self-filtered inbox stream live, or the whole firehose.
    Watch {
        /// Watch one role's self-filtered inbox instead of the whole firehose.
        #[arg(long, value_name = "ROLE")]
        role: Option<String>,
        /// The broker base URL (default: the `CREW_BROKER_HOST` / `PORT`
        /// environment).
        #[arg(long, value_name = "URL")]
        broker: Option<String>,
    },
    /// Live terminal cockpit: htop for your crew, every role's status, action,
    /// and spend, updating live (issue #51).
    Top {
        /// The broker base URL (default: the `CREW_BROKER_HOST` / `PORT`
        /// environment).
        #[arg(long, value_name = "URL")]
        broker: Option<String>,
    },
    /// Push a native notification on each actionable moment: a question, a
    /// death, a stand-down, a stall, a completion, a budget breach.
    Notify {
        /// Mute one or more moments (comma-separated): `question`, `died`,
        /// `stood-down`, `stalled`, `complete`, `budget`.
        #[arg(long, value_delimiter = ',', value_name = "MOMENT")]
        mute: Vec<notify::Moment>,
        /// Skip the terminal bell; still show the desktop notification and the
        /// log line.
        #[arg(long)]
        no_sound: bool,
        /// The broker base URL (default: the `CREW_BROKER_HOST` / `PORT`
        /// environment).
        #[arg(long, value_name = "URL")]
        broker: Option<String>,
    },
    /// Brief the crew as the General: post a free-form note to the commander by
    /// default, a role, or a channel (distinct from the agent `send`).
    Brief {
        /// Brief one role directly (its `@role` channel) instead of the
        /// commander.
        #[arg(long, value_name = "ROLE")]
        to: Option<String>,
        /// Post to a named channel: `all-units`, or a pair like
        /// `frontend+backend`.
        #[arg(long, value_name = "CHANNEL")]
        channel: Option<String>,
        /// The crew's commander, the default addressee. Defaults to
        /// `commander`.
        #[arg(long, value_name = "ROLE")]
        commander: Option<String>,
        /// The message text (markdown).
        body: String,
        /// The broker base URL (default: the `CREW_BROKER_HOST` / `PORT`
        /// environment).
        #[arg(long, value_name = "URL")]
        broker: Option<String>,
    },
    /// Redirect a role mid-task: inject a steering message it honors
    /// immediately.
    Redirect {
        /// The role to steer (its `@role` channel).
        role: String,
        /// The steering message (markdown).
        message: String,
        /// The broker base URL (default: the `CREW_BROKER_HOST` / `PORT`
        /// environment).
        #[arg(long, value_name = "URL")]
        broker: Option<String>,
    },
    /// Belay a role: halt its current work and re-task it with a new order.
    Belay {
        /// The role to re-task (its `@role` channel).
        role: String,
        /// The new order (markdown).
        order: String,
        /// The broker base URL (default: the `CREW_BROKER_HOST` / `PORT`
        /// environment).
        #[arg(long, value_name = "URL")]
        broker: Option<String>,
    },
    /// Command a role directly, bypassing the commander: order a specialist
    /// yourself, and the commander is informed rather than bypassed
    /// silently (issue #42).
    Override {
        /// The role to command (its `@role` channel).
        role: String,
        /// The order: what you want the role to do (its title).
        order: String,
        /// What is in and out of scope for the order.
        #[arg(long)]
        scope: Option<String>,
        /// How the order is judged done.
        #[arg(long)]
        acceptance: Option<String>,
        /// The crew's commander to inform. Defaults to `commander`.
        #[arg(long, value_name = "ROLE")]
        commander: Option<String>,
        /// The broker base URL (default: the `CREW_BROKER_HOST` / `PORT`
        /// environment).
        #[arg(long, value_name = "URL")]
        broker: Option<String>,
    },
    /// Reassign an in-flight task to a new owner in the work ledger: the
    /// General moves a claimed task, and both roles and the commander are
    /// informed (issue #42).
    Reassign {
        /// The task key to reassign, as shown in `crew ledger`.
        task: String,
        /// The role to move the task to (its `@role` channel).
        #[arg(long, value_name = "ROLE")]
        to: String,
        /// The role the task is expected to be held by; a guard against a stale
        /// view. Optional.
        #[arg(long, value_name = "ROLE")]
        from: Option<String>,
        /// The crew's commander to inform. Defaults to `commander`.
        #[arg(long, value_name = "ROLE")]
        commander: Option<String>,
        /// The broker base URL (default: the `CREW_BROKER_HOST` / `PORT`
        /// environment).
        #[arg(long, value_name = "URL")]
        broker: Option<String>,
    },
    /// Pause a role, or the whole crew: it pulls no new work until resumed.
    Pause {
        /// The role to pause; omit to pause the whole crew.
        role: Option<String>,
        /// The broker base URL (default: the `CREW_BROKER_HOST` / `PORT`
        /// environment).
        #[arg(long, value_name = "URL")]
        broker: Option<String>,
    },
    /// Resume a paused role, or the whole crew.
    Resume {
        /// The role to resume; omit to resume the whole crew.
        role: Option<String>,
        /// The broker base URL (default: the `CREW_BROKER_HOST` / `PORT`
        /// environment).
        #[arg(long, value_name = "URL")]
        broker: Option<String>,
    },
    /// Stand the crew down: halt every role now, preserving state for recovery.
    Standdown {
        /// The broker base URL (default: the `CREW_BROKER_HOST` / `PORT`
        /// environment).
        #[arg(long, value_name = "URL")]
        broker: Option<String>,
    },
    /// Show the shared-subscription usage gauge: the reading, threshold, and
    /// any auto-pause.
    Usage {
        /// The broker base URL (default: the `CREW_BROKER_HOST` / `PORT`
        /// environment).
        #[arg(long, value_name = "URL")]
        broker: Option<String>,
    },
    /// Integrate the roles' `crew/<role>` branches into one coherent, green
    /// branch (issue #44).
    Integrate {
        /// The repo whose `crew/<role>` branches to merge. Defaults to the
        /// current directory.
        #[arg(long, value_name = "PATH", default_value = ".")]
        repo: String,
        /// The base ref the integration branch is cut from. Defaults to `HEAD`.
        #[arg(long, value_name = "REF", default_value = "HEAD")]
        base: String,
        /// The integration branch to merge onto. Defaults to
        /// `crew/integration`.
        #[arg(long, value_name = "NAME", default_value = "crew/integration")]
        branch: String,
        /// An acceptance check to run on the merged result, a shell command,
        /// e.g. "cargo test".
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
    /// Claim a task on the work ledger, or move this role's claim to a new
    /// state.
    Claim {
        /// The task key to claim (a path, a feature, or an order's title).
        task: String,
        /// The state to move it to: `claimed`, `in_progress`, `blocked`, or
        /// `done`.
        #[arg(long, default_value = "claimed")]
        state: String,
        /// An optional short label for the ledger.
        #[arg(long)]
        title: Option<String>,
    },
    /// Show the work ledger: every claimed task, its owner, and its state.
    Ledger,
    /// Submit finished work for adversarial verification (not done until it
    /// passes).
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
    /// Return a verdict on a task another role submitted: try to break it, then
    /// judge.
    Verdict {
        /// The task title under verification.
        task: String,
        /// Pass the task: mark it done because you could not break it.
        #[arg(long)]
        pass: bool,
        /// The specific failure when the task does not pass (required to fail
        /// it).
        #[arg(long, value_name = "TEXT")]
        failure: Option<String>,
    },
    /// Read the done-gate: tasks under verification and their standing.
    Gate,
    /// Report the mission gracefully complete, as the crew (typically the
    /// commander). Distinct from the emergency `standdown`; it announces a
    /// finish without halting the crew.
    Complete {
        /// A short summary of what the mission shipped, shown in the completion
        /// notification (issue #155).
        summary: Option<String>,
    },
    /// Record or retract a shared situation board entry: a decision, interface,
    /// or gotcha.
    Record {
        /// The entry's stable key (its topic), for example `auth-strategy`.
        key: String,
        /// The section: `decision`, `interface`, or `gotcha` (required unless
        /// retracting).
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
    /// Get this role's bounded briefing packet: catch up without reading the
    /// whole log.
    Briefing {
        /// Narrow the summary to this task id, if you have one.
        #[arg(long, value_name = "TASK")]
        task: Option<String>,
        /// Cap the packet size in bytes (defaults to a few KB).
        #[arg(long, value_name = "BYTES")]
        budget: Option<usize>,
    },
}

#[expect(
    clippy::too_many_lines,
    reason = "the command dispatch is a flat one-arm-per-subcommand match"
)]
fn main() -> Result<()> {
    crew_telemetry::init();

    match Cli::parse().command {
        Command::Up { config } => up::run(config.as_deref()),
        Command::Down => down::run(),
        Command::Register => shim::register(),
        Command::Send { to, channel, body } => shim::send(to.as_deref(), channel.as_deref(), &body),
        Command::Ask {
            to,
            channel,
            options,
            body,
        } => shim::ask(to.as_deref(), channel.as_deref(), &body, &options),
        Command::Answer {
            to,
            channel,
            in_reply_to,
            body,
        } => shim::answer(to.as_deref(), channel.as_deref(), &body, &in_reply_to),
        Command::Order {
            to,
            title,
            scope,
            owns,
            acceptance,
            body,
        } => shim::order(
            &to,
            &title,
            scope.as_deref().unwrap_or_default(),
            &owns,
            acceptance.as_deref().unwrap_or_default(),
            body.as_deref().unwrap_or_default(),
        ),
        Command::Status { to, channel, body } => {
            shim::status(to.as_deref(), channel.as_deref(), &body)
        }
        Command::Artifact {
            to,
            channel,
            kind,
            reference,
            body,
        } => shim::artifact(
            to.as_deref(),
            channel.as_deref(),
            body.as_deref().unwrap_or_default(),
            &reference,
            &kind,
        ),
        Command::Inbox => shim::inbox(),
        Command::Watch { role, broker } => broker::watch(broker.as_deref(), role.as_deref()),
        Command::Top { broker } => top::run(broker.as_deref()),
        Command::Notify {
            mute,
            no_sound,
            broker,
        } => notify::notify(
            broker.as_deref(),
            &notify::NotifyPolicy::new(mute, !no_sound),
        ),
        Command::Brief {
            to,
            channel,
            commander,
            body,
            broker,
        } => control::brief(
            broker.as_deref(),
            to.as_deref(),
            channel.as_deref(),
            commander.as_deref(),
            &body,
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
        Command::Override {
            role,
            order,
            scope,
            acceptance,
            commander,
            broker,
        } => control::command(
            broker.as_deref(),
            &role,
            &order,
            scope.as_deref(),
            acceptance.as_deref(),
            commander.as_deref(),
        ),
        Command::Reassign {
            task,
            to,
            from,
            commander,
            broker,
        } => control::reassign(
            broker.as_deref(),
            &task,
            &to,
            from.as_deref(),
            commander.as_deref(),
        ),
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
        Command::Complete { summary } => shim::complete(summary.as_deref().unwrap_or_default()),
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
