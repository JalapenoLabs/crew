//! The agent CLI shim: crew coordination for a runtime without MCP (issue #28).
//!
//! A Claude Code agent gets the crew tools over MCP (`crew_send`, `crew_inbox`,
//! ...). A runtime without MCP, such as Codex, reaches the same broker through
//! these `crew` subcommands instead. Each maps one-to-one onto an MCP tool and,
//! crucially, uses the very same [`crew_substrate::client::Broker`] the MCP
//! server dispatches to (issue #129), so a shim agent's I/O lands on the broker
//! identically to the MCP path (see `docs/codex.md`).
//!
//! Every command boots from the same role context the `crew-mcp` binary does:
//! the role card at `CREW_ROLE_CARD`, or `CREW_ROLE` plus the broker's own
//! `CREW_BROKER_*` environment. So the supervisor spawns a Codex agent with the
//! same environment it hands a Claude agent, and the agent shells out to `crew`
//! instead of calling a tool.
//!
//! Each invocation is its own short-lived process, so where the long-lived MCP
//! server holds per-session state in memory, the shim persists it on disk: a
//! per-role inbox cursor under the broker state dir lets `crew inbox` show only
//! messages that arrived since the last call ([`crate::cursor`], issue #130).
//! `docs/codex.md` records the remaining parity gaps.

use std::path::PathBuf;

use crew_substrate::{
    broker::Config as BrokerConfig,
    client::{
        BoardSnapshot, Broker, GateSnapshot, InboxItem, LedgerItem, RosterSnapshot, Standing,
    },
    core::{BrokerEndpoint, LaneEnforcement, RoleCard, RoleId, ROLE_CARD_ENV},
};
use eyre::{eyre, Result, WrapErr};

use crate::cursor::{InboxCursor, TaskContext};

/// The resolved agent context a shim command acts as: its broker, role, and
/// lane.
struct Agent {
    /// The broker base URL, such as `http://127.0.0.1:2739`.
    base: String,
    /// The role this agent plays on the crew.
    role: RoleId,
    /// The crew's commander, the default addressee for an unaddressed `send`.
    commander: RoleId,
    /// The paths the role owns, registered with the roster.
    owned_paths: Vec<String>,
    /// How the crew enforces this role's lane (issue #46).
    lane_enforcement: LaneEnforcement,
}

impl Agent {
    /// A broker client acting as this agent's role.
    ///
    /// Seeds the task the role adopted from an order (issue #132), restored
    /// from disk since each shim command is its own process, so `crew send`
    /// and `crew order` stamp it the way the long-lived MCP client would. A
    /// role with no task yet sends without one.
    fn broker(&self) -> Broker {
        let broker = Broker::new(self.base.clone(), self.role.clone(), self.commander.clone());
        if let Some(task) = self.task_context().and_then(|context| context.load()) {
            broker.with_task(task)
        } else {
            broker
        }
    }

    /// The persisted task-context store for this role, when the broker state
    /// dir is readable from the environment (issue #132).
    fn task_context(&self) -> Option<TaskContext> {
        BrokerConfig::from_env()
            .ok()
            .map(|config| TaskContext::new(&config.state_dir, &self.role))
    }
}

/// Registers this agent's role on the roster, so the unit sees it (mirrors the
/// MCP server's boot registration).
///
/// # Errors
/// Returns an error if no role context is set, or the broker rejects the
/// registration.
pub fn register() -> Result<()> {
    let agent = load_agent()?;
    agent
        .broker()
        .register(&agent.owned_paths)
        .map_err(|reason| eyre!("{reason}"))?;
    println!("registered {} on the roster", agent.role);
    Ok(())
}

/// Sends a message as this agent's role to a teammate, a channel, or the
/// commander.
///
/// Mirrors `crew_send`: `to` direct-messages a role, `channel` posts to a named
/// channel (`all-units` or a pair like `frontend+backend`), and neither reaches
/// the commander.
///
/// # Errors
/// Returns an error if no role context is set, or the broker rejects the
/// message.
pub fn send(to: Option<&str>, channel: Option<&str>, body: &str) -> Result<()> {
    let agent = load_agent()?;
    let confirmation = agent
        .broker()
        .send(to, channel, body)
        .map_err(|reason| eyre!("{reason}"))?;
    println!("{confirmation}");
    Ok(())
}

/// Asks a typed `question` as this agent's role (issue #123).
///
/// Mirrors `crew_ask`: a `question` is the kind the coordination-stall detector
/// keys on, so asking through this rather than a plain `send` note lets an
/// unanswered question surface a deadlock. `options` are optional suggested
/// answers.
///
/// # Errors
/// Returns an error if no role context is set, the target is not routable, or
/// the broker rejects the message.
pub fn ask(to: Option<&str>, channel: Option<&str>, body: &str, options: &[String]) -> Result<()> {
    let agent = load_agent()?;
    let confirmation = agent
        .broker()
        .ask(to, channel, body, options)
        .map_err(|reason| eyre!("{reason}"))?;
    println!("{confirmation}");
    Ok(())
}

/// Answers a `question` as this agent's role, naming the message it replies to
/// (issue #123).
///
/// Mirrors `crew_answer`: `in_reply_to` is the id of the question, shown in the
/// inbox as `[id ...]`; it threads the reply and clears the wait the stall
/// detector was tracking.
///
/// # Errors
/// Returns an error if no role context is set, the target is not routable,
/// `in_reply_to` is not a message id, or the broker rejects the message.
pub fn answer(
    to: Option<&str>,
    channel: Option<&str>,
    body: &str,
    in_reply_to: &str,
) -> Result<()> {
    let agent = load_agent()?;
    let confirmation = agent
        .broker()
        .answer(to, channel, body, in_reply_to)
        .map_err(|reason| eyre!("{reason}"))?;
    println!("{confirmation}");
    Ok(())
}

/// Issues an order as this agent's role to a specialist, giving it a scoped
/// task (issue #27).
///
/// Mirrors `crew_order`: the commander's fan-out handle for decomposing the
/// General's brief. It direct-messages `to` a structured `order` the specialist
/// can act on, rather than freeform prose: `title` names the task, `scope` says
/// what is in and out, `owned_paths` are the paths it owns while working, and
/// `acceptance` is how it is judged done. `body` adds any freeform detail.
///
/// # Errors
/// Returns an error if no role context is set, `to` is not a plain role name,
/// or the broker rejects the order.
pub fn order(
    to: &str,
    title: &str,
    scope: &str,
    owned_paths: &[String],
    acceptance: &str,
    body: &str,
) -> Result<()> {
    let agent = load_agent()?;
    let confirmation = agent
        .broker()
        .order(to, title, scope, owned_paths, acceptance, body)
        .map_err(|reason| eyre!("{reason}"))?;
    println!("{confirmation}");
    Ok(())
}

/// Prints the messages addressed to this agent's role that are new since the
/// last call.
///
/// Mirrors `crew_inbox`. The MCP server holds its inbox cursor and task context
/// in memory; the shim is a process per call, so it persists both per role
/// under the broker state dir (issues #130, #132): it seeds the client from the
/// saved position, reads only what arrived since, writes the advanced position
/// back, and saves the task any new order assigned so a later `crew send`
/// stamps it. A first call, or a role with no saved cursor, shows the whole
/// inbox.
///
/// # Errors
/// Returns an error if no role context is set, the broker configuration cannot
/// be read, the broker cannot be reached, or the cursor cannot be saved.
pub fn inbox() -> Result<()> {
    let agent = load_agent()?;
    let state_dir = BrokerConfig::from_env()
        .wrap_err("could not read the broker configuration")?
        .state_dir;
    let cursor = InboxCursor::new(&state_dir, &agent.role);
    let task_context = TaskContext::new(&state_dir, &agent.role);

    let mut broker = agent.broker().with_read_through(cursor.load());
    let items = broker.inbox().map_err(|reason| eyre!("{reason}"))?;
    // Print first, then advance the cursor, so a message is marked seen only once
    // it has been shown: a failed write reprints next time rather than dropping it.
    print_inbox(&items);
    cursor.save(broker.read_through())?;
    // Persist the task the read adopted from an order, so the next command stamps
    // it (issue #132). A role still working its prior task keeps it, since the
    // broker was seeded with the saved one.
    if let Some(task) = broker.task() {
        task_context.save(task)?;
    }
    Ok(())
}

/// Prints the unit's roster: every registered role, its lane, and its liveness.
///
/// # Errors
/// Returns an error if no role context is set, or the broker cannot be reached.
pub fn roster() -> Result<()> {
    let agent = load_agent()?;
    let snapshot = agent
        .broker()
        .roster()
        .map_err(|reason| eyre!("{reason}"))?;
    print_roster(&snapshot);
    Ok(())
}

/// Claims a task, or moves this role's claim to `state`, on the work ledger
/// (issue #45).
///
/// Mirrors `crew_claim`: the broker refuses a claim on work another role holds,
/// and the error names the holder.
///
/// # Errors
/// Returns an error if no role context is set, another role holds the task, or
/// the broker cannot be reached.
pub fn claim(task: &str, state: &str, title: &str) -> Result<()> {
    let agent = load_agent()?;
    let confirmation = agent
        .broker()
        .claim(task, state, title)
        .map_err(|reason| eyre!("{reason}"))?;
    println!("{confirmation}");
    Ok(())
}

/// Prints the work ledger: every claimed task, its owner, and its state.
///
/// # Errors
/// Returns an error if no role context is set, or the broker cannot be reached.
pub fn ledger() -> Result<()> {
    let agent = load_agent()?;
    let items = agent
        .broker()
        .ledger()
        .map_err(|reason| eyre!("{reason}"))?;
    print_ledger(&items);
    Ok(())
}

/// Checks whether `path` is in this role's lane, warning or blocking per policy
/// (issue #46).
///
/// Mirrors `crew_lane`: an in-lane path proceeds; an out-of-lane path is
/// reported on the stream, and under a blocking policy the check fails so the
/// role routes the change through the commander instead of editing silently.
///
/// # Errors
/// Returns an error if no role context is set, the path is out of lane and
/// enforcement is `block`, or the broker cannot be reached.
pub fn lane(path: &str) -> Result<()> {
    let agent = load_agent()?;
    let verdict = agent
        .broker()
        .check_lane(&agent.owned_paths, agent.lane_enforcement, path)
        .map_err(|reason| eyre!("{reason}"))?;
    println!("{verdict}");
    Ok(())
}

/// Submits this agent's finished work for adversarial verification (issue #47).
///
/// Mirrors `crew_submit`: the work is not done until an independent role tries
/// to break it and passes it. `to` optionally names a reviewer role to notify.
///
/// # Errors
/// Returns an error if no role context is set, or the broker rejects the
/// submission.
pub fn submit(task: &str, acceptance: Option<&str>, to: Option<&str>) -> Result<()> {
    let agent = load_agent()?;
    let confirmation = agent
        .broker()
        .submit(task, acceptance.unwrap_or_default(), to)
        .map_err(|reason| eyre!("{reason}"))?;
    println!("{confirmation}");
    Ok(())
}

/// Records this agent's verdict on a task another role submitted (issue #47).
///
/// Mirrors `crew_verdict`: a `pass` marks the task done; otherwise the work
/// returns to its owner with the `failure`. A role cannot verify its own work.
///
/// # Errors
/// Returns an error if no role context is set, the verdict is refused, or a
/// failing verdict carries no failure.
pub fn verdict(task: &str, pass: bool, failure: Option<&str>) -> Result<()> {
    let agent = load_agent()?;
    let confirmation = agent
        .broker()
        .verdict(task, pass, failure.unwrap_or_default())
        .map_err(|reason| eyre!("{reason}"))?;
    println!("{confirmation}");
    Ok(())
}

/// Prints the done-gate: every task under verification and its standing (issue
/// #47).
///
/// # Errors
/// Returns an error if no role context is set, or the broker cannot be reached.
pub fn gate() -> Result<()> {
    let agent = load_agent()?;
    let snapshot = agent.broker().gate().map_err(|reason| eyre!("{reason}"))?;
    print_gate(&snapshot);
    Ok(())
}

/// Reports the mission gracefully complete (issues #121, #155).
///
/// Mirrors `crew_complete`: the crew, typically through the commander, declares
/// the mission done so `crew notify` fires on a true finish, distinct from the
/// General's emergency stand-down. It announces completion; it does not halt
/// the crew. `summary` is a short account of what shipped, rendered in the
/// completion notification; empty when none is given.
///
/// # Errors
/// Returns an error if no role context is set, or the broker cannot be reached.
pub fn complete(summary: &str) -> Result<()> {
    let agent = load_agent()?;
    let confirmation = agent
        .broker()
        .complete(summary)
        .map_err(|reason| eyre!("{reason}"))?;
    println!("{confirmation}");
    Ok(())
}

/// Records or retracts a shared situation board entry (issue #49).
///
/// Mirrors `crew_record`: records a `decision`, `interface`, or `gotcha` under
/// `key`, or, with `retract`, removes the entry. The commander curates the
/// board.
///
/// # Errors
/// Returns an error if no role context is set, a required field is missing, a
/// retraction names a missing entry, or the broker cannot be reached.
pub fn record(key: &str, section: Option<&str>, body: Option<&str>, retract: bool) -> Result<()> {
    let agent = load_agent()?;
    let broker = agent.broker();
    let confirmation = if retract {
        broker.retract(key)
    } else {
        let section = section
            .ok_or_else(|| eyre!("recording needs --section (decision, interface, or gotcha)"))?;
        let body = body.ok_or_else(|| eyre!("recording needs --body (the content)"))?;
        broker.record(key, section, body)
    }
    .map_err(|reason| eyre!("{reason}"))?;
    println!("{confirmation}");
    Ok(())
}

/// Prints the shared situation board: the crew's durable memory (issue #49).
///
/// # Errors
/// Returns an error if no role context is set, or the broker cannot be reached.
pub fn board(section: Option<&str>) -> Result<()> {
    let agent = load_agent()?;
    let snapshot = agent
        .broker()
        .board(section)
        .map_err(|reason| eyre!("{reason}"))?;
    print_board(&snapshot);
    Ok(())
}

/// Prints this role's bounded new-role briefing packet (issue #50).
///
/// Mirrors `crew_briefing`: the decision board and a rolling summary scoped to
/// the role's lane, capped to a byte budget, so a fresh role catches up without
/// reading the whole log.
///
/// # Errors
/// Returns an error if no role context is set, or the broker cannot be reached.
pub fn briefing(task: Option<&str>, budget: Option<usize>) -> Result<()> {
    let agent = load_agent()?;
    let packet = agent
        .broker()
        .briefing(task, budget)
        .map_err(|reason| eyre!("{reason}"))?;
    println!("{}", packet.text);
    let fit = if packet.capped { "capped to" } else { "within" };
    println!(
        "[briefing {fit} {} of {} bytes]",
        packet.size, packet.budget
    );
    Ok(())
}

/// Resolves the agent context from the environment, the way the `crew-mcp`
/// binary does.
///
/// Prefers the role card at [`ROLE_CARD_ENV`], which carries the role, its
/// lane, and the broker address. Failing that, `CREW_ROLE` names the role and
/// the broker's own `CREW_BROKER_*` config gives the address, for a bare manual
/// boot with an empty lane.
fn load_agent() -> Result<Agent> {
    if let Some(path) = std::env::var_os(ROLE_CARD_ENV) {
        let path = PathBuf::from(path);
        let text = std::fs::read_to_string(&path)
            .wrap_err_with(|| format!("could not read the role card at {}", path.display()))?;
        let card = RoleCard::from_toml(&text).wrap_err("the role card is not valid")?;
        return Ok(Agent {
            base: card.broker.base_url(),
            role: card.role,
            commander: card.commander,
            owned_paths: card.owned_paths,
            lane_enforcement: card.lane_enforcement,
        });
    }

    let role = std::env::var("CREW_ROLE").ok();
    let role = role
        .as_deref()
        .map(str::trim)
        .filter(|role| !role.is_empty())
        .ok_or_else(|| {
            eyre!("set {ROLE_CARD_ENV} to a role card, or CREW_ROLE to name the role")
        })?;

    let config = BrokerConfig::from_env().wrap_err("could not read the broker configuration")?;
    let base = BrokerEndpoint::new(config.host.to_string(), config.port).base_url();
    Ok(Agent {
        base,
        role: RoleId::new(role),
        // No card to name the commander, so fall back to the conventional default,
        // matching `RoleCard`'s own default when a card omits it.
        commander: RoleId::new("commander"),
        owned_paths: Vec::new(),
        lane_enforcement: LaneEnforcement::default(),
    })
}

/// Prints the inbox items, one per line, the way the MCP server renders them.
fn print_inbox(items: &[InboxItem]) {
    if items.is_empty() {
        println!("No messages.");
        return;
    }
    println!("{} message(s):", items.len());
    for item in items {
        // Lead with the structured detail (an order's task); fall back to the body.
        let content = match (item.detail.as_str(), item.body.as_str()) {
            ("", body) => body.to_owned(),
            (detail, "") => detail.to_owned(),
            (detail, body) => format!("{detail}. {body}"),
        };
        // A redirect or belay is a General directive to honor at once, so flag it.
        let marker = if item.directive { "[honor now] " } else { "" };
        println!(
            "- {}{} on {} ({}): {}",
            marker, item.from, item.channel, item.kind, content
        );
    }
}

/// Prints the roster: the crew standing, then each role, its lane, and its
/// liveness.
fn print_roster(snapshot: &RosterSnapshot) {
    if snapshot.roles.is_empty() {
        println!("The roster is empty.");
        return;
    }
    let crew_gated = snapshot.standing != Standing::Running;
    match snapshot.standing {
        Standing::Running => println!("{} role(s):", snapshot.roles.len()),
        Standing::Paused => println!("{} role(s) (the crew is PAUSED):", snapshot.roles.len()),
        Standing::StoodDown => {
            println!("{} role(s) (the crew is STOOD DOWN):", snapshot.roles.len());
        }
    }
    for role in &snapshot.roles {
        let owns = if role.owned_paths.is_empty() {
            String::new()
        } else {
            format!(" owns {}", role.owned_paths.join(", "))
        };
        let gated = if role.paused || crew_gated {
            " [paused]"
        } else {
            ""
        };
        println!("- {} [{}]{}{}", role.role, role.liveness, owns, gated);
    }
}

/// Prints the work ledger, one task per line.
fn print_ledger(items: &[LedgerItem]) {
    if items.is_empty() {
        println!("The ledger is empty; no work is claimed.");
        return;
    }
    println!("{} task(s):", items.len());
    for item in items {
        let title = if item.title.is_empty() {
            String::new()
        } else {
            format!(" ({})", item.title)
        };
        println!("- {} [{}] {}{}", item.task, item.state, item.owner, title);
    }
}

/// Prints the done-gate: each task under verification, its owner, verifier, and
/// standing.
fn print_gate(snapshot: &GateSnapshot) {
    if snapshot.tasks.is_empty() {
        println!("The done-gate is empty; no task is under verification.");
        return;
    }
    println!("{} task(s) under the done-gate:", snapshot.tasks.len());
    for task in &snapshot.tasks {
        let verifier = task
            .verifier
            .as_deref()
            .map(|who| format!(" by {who}"))
            .unwrap_or_default();
        let detail = if task.detail.is_empty() {
            String::new()
        } else {
            format!(": {}", task.detail)
        };
        println!(
            "- {} owned by {} [{}{}]{}",
            task.task, task.owner, task.verdict, verifier, detail
        );
    }
}

/// Prints the situation board: each entry, its section, author, and content.
fn print_board(snapshot: &BoardSnapshot) {
    if snapshot.entries.is_empty() {
        println!("The situation board is empty.");
        return;
    }
    println!(
        "{} board entr{}:",
        snapshot.entries.len(),
        plural(snapshot.entries.len())
    );
    for entry in &snapshot.entries {
        println!(
            "- [{}] {} (by {}): {}",
            entry.section, entry.key, entry.author, entry.body
        );
    }
}

/// The suffix for `entr(y|ies)` given a count.
fn plural(count: usize) -> &'static str {
    if count == 1 {
        "y"
    } else {
        "ies"
    }
}
