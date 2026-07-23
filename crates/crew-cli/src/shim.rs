//! The agent CLI shim: crew coordination for a runtime without MCP (issue #28).
//!
//! A Claude Code agent gets the crew tools over MCP (`crew_send`, `crew_inbox`, ...).
//! A runtime without MCP, such as Codex, reaches the same broker through these `crew`
//! subcommands instead. Each maps one-to-one onto an MCP tool and, crucially, uses the
//! very same [`crew_substrate::mcp::Broker`] client the MCP server uses, so a shim agent's
//! I/O lands on the broker identically to the MCP path (see `docs/codex.md`).
//!
//! Every command boots from the same role context the `crew-mcp` binary does: the role
//! card at `CREW_ROLE_CARD`, or `CREW_ROLE` plus the broker's own `CREW_BROKER_*`
//! environment. So the supervisor spawns a Codex agent with the same environment it
//! hands a Claude agent, and the agent shells out to `crew` instead of calling a tool.
//!
//! Each invocation is its own short-lived process, so unlike the long-lived MCP server
//! the shim keeps no per-session state: `crew inbox` reports every message currently
//! addressed to the role, not only those since a previous call. `docs/codex.md` records
//! this and the other parity gaps.

use std::path::PathBuf;

use crew_substrate::broker::Config as BrokerConfig;
use crew_substrate::core::{BrokerEndpoint, RoleCard, RoleId, ROLE_CARD_ENV};
use crew_substrate::mcp::{Broker, InboxItem, RoleEntry};
use eyre::{eyre, Result, WrapErr};

/// The resolved agent context a shim command acts as: its broker, role, and lane.
struct Agent {
    /// The broker base URL, such as `http://127.0.0.1:2739`.
    base: String,
    /// The role this agent plays on the crew.
    role: RoleId,
    /// The crew's commander, the default addressee for an unaddressed `send`.
    commander: RoleId,
    /// The paths the role owns, registered with the roster.
    owned_paths: Vec<String>,
}

impl Agent {
    /// A broker client acting as this agent's role.
    fn broker(&self) -> Broker {
        Broker::new(self.base.clone(), self.role.clone(), self.commander.clone())
    }
}

/// Registers this agent's role on the roster, so the unit sees it (mirrors the MCP
/// server's boot registration).
///
/// # Errors
/// Returns an error if no role context is set, or the broker rejects the registration.
pub fn register() -> Result<()> {
    let agent = load_agent()?;
    agent
        .broker()
        .register(&agent.owned_paths)
        .map_err(|reason| eyre!("{reason}"))?;
    println!("registered {} on the roster", agent.role);
    Ok(())
}

/// Sends a message as this agent's role to a teammate, a channel, or the commander.
///
/// Mirrors `crew_send`: `to` direct-messages a role, `channel` posts to a named
/// channel (`all-units` or a pair like `frontend+backend`), and neither reaches the
/// commander.
///
/// # Errors
/// Returns an error if no role context is set, or the broker rejects the message.
pub fn send(to: Option<&str>, channel: Option<&str>, body: &str) -> Result<()> {
    let agent = load_agent()?;
    let confirmation = agent
        .broker()
        .send(to, channel, body)
        .map_err(|reason| eyre!("{reason}"))?;
    println!("{confirmation}");
    Ok(())
}

/// Prints the messages addressed to this agent's role.
///
/// Mirrors `crew_inbox`, but statelessly: a short-lived shim keeps no per-session
/// cursor, so it reports every message currently addressed to the role.
///
/// # Errors
/// Returns an error if no role context is set, or the broker cannot be reached.
pub fn inbox() -> Result<()> {
    let agent = load_agent()?;
    let items = agent.broker().inbox().map_err(|reason| eyre!("{reason}"))?;
    print_inbox(&items);
    Ok(())
}

/// Prints the unit's roster: every registered role, its lane, and its liveness.
///
/// # Errors
/// Returns an error if no role context is set, or the broker cannot be reached.
pub fn roster() -> Result<()> {
    let agent = load_agent()?;
    let roles = agent
        .broker()
        .roster()
        .map_err(|reason| eyre!("{reason}"))?;
    print_roster(&roles);
    Ok(())
}

/// Resolves the agent context from the environment, the way the `crew-mcp` binary does.
///
/// Prefers the role card at [`ROLE_CARD_ENV`], which carries the role, its lane, and
/// the broker address. Failing that, `CREW_ROLE` names the role and the broker's own
/// `CREW_BROKER_*` config gives the address, for a bare manual boot with an empty lane.
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
        println!(
            "- {} on {} ({}): {}",
            item.from, item.channel, item.kind, content
        );
    }
}

/// Prints the roster entries, one per line.
fn print_roster(roles: &[RoleEntry]) {
    if roles.is_empty() {
        println!("The roster is empty.");
        return;
    }
    println!("{} role(s):", roles.len());
    for role in roles {
        let owns = if role.owned_paths.is_empty() {
            String::new()
        } else {
            format!(" owns {}", role.owned_paths.join(", "))
        };
        println!("- {} [{}]{}", role.role, role.liveness, owns);
    }
}
