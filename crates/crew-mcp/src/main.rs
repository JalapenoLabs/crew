//! The `crew-mcp` binary: the agent-facing MCP server.
//!
//! The supervisor spawns one per agent and hands it a role card: `CREW_ROLE_CARD`
//! names a TOML card (see [`crew_core::RoleCard`]) with the role, its owned lane, its
//! acceptance bar, and the broker address. The server boots from the card, registers
//! the role on the roster so the unit sees it, and then speaks JSON-RPC over stdio
//! (see [`crew_mcp`]).
//!
//! Without a card it falls back to the discrete environment: `CREW_ROLE` names the
//! role and `CREW_BROKER_HOST` / `CREW_BROKER_PORT` (via the broker's own config)
//! give the address. This keeps a bare manual boot working.
//!
//! stdout carries the JSON-RPC protocol, so it stays clean: diagnostics go to stderr.

use std::io::{stdin, stdout, BufReader};

use crew_core::{BrokerEndpoint, RoleCard, RoleId, ROLE_CARD_ENV};
use crew_mcp::{Broker, Server};
use eyre::{eyre, Result, WrapErr};
use mimalloc::MiMalloc;

/// mimalloc as the global allocator (M-MIMALLOC-APPS).
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() -> Result<()> {
    let card = load_card()?;

    // The briefing goes to stderr so the operator sees the role boot knowing its
    // lane; stdout is reserved for the JSON-RPC protocol.
    eprintln!("{}", card.briefing());

    let broker = Broker::new(
        card.broker.base_url(),
        card.role.clone(),
        card.commander.clone(),
    );

    // Reach the broker at boot: announce the role and its lane on the roster. A
    // failure is reported but not fatal, so a briefly unavailable broker does not
    // strip the agent of its tools; the tools resurface the error when next called.
    if let Err(reason) = broker.register(&card.owned_paths) {
        eprintln!(
            "crew-mcp: could not register {} on the roster: {reason}",
            card.role
        );
    }

    let mut server = Server::new(broker, card.owned_paths.clone(), card.lane_enforcement);
    server
        .serve(BufReader::new(stdin().lock()), stdout().lock())
        .wrap_err("the MCP server exited with an I/O error")
}

/// Loads the role card from `CREW_ROLE_CARD`, or builds one from the environment.
///
/// The card file is the supervisor's path and the standalone default. The env
/// fallback (`CREW_ROLE` plus the broker's config) keeps a bare manual boot working
/// with an empty lane and acceptance bar.
fn load_card() -> Result<RoleCard> {
    if let Some(path) = std::env::var_os(ROLE_CARD_ENV) {
        let path = std::path::PathBuf::from(path);
        let text = std::fs::read_to_string(&path)
            .wrap_err_with(|| format!("could not read the role card at {}", path.display()))?;
        return RoleCard::from_toml(&text).wrap_err("the role card is not valid");
    }

    let role = std::env::var("CREW_ROLE").ok();
    let role = role
        .as_deref()
        .map(str::trim)
        .filter(|role| !role.is_empty())
        .ok_or_else(|| {
            eyre!("set {ROLE_CARD_ENV} to a role card, or CREW_ROLE to name the role")
        })?;

    // The broker address comes from the same environment the broker reads.
    let config =
        crew_broker::Config::from_env().wrap_err("could not read the broker configuration")?;
    let broker = BrokerEndpoint::new(config.host.to_string(), config.port);

    Ok(RoleCard::new(
        RoleId::new(role),
        Vec::new(),
        String::new(),
        broker,
    ))
}
