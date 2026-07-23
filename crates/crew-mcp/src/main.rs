//! The `crew-mcp` binary: the agent-facing MCP server.
//!
//! The supervisor spawns one per agent, wiring stdio to the agent's MCP client and
//! setting the environment: `CREW_ROLE` names the role this server acts as, and
//! `CREW_BROKER_HOST` / `CREW_BROKER_PORT` (via the broker's own config) give the
//! broker address. The server then speaks JSON-RPC over stdio (see [`crew_mcp`]).
//!
//! stdout carries the JSON-RPC protocol, so it stays clean: diagnostics go to stderr.

use std::io::{stdin, stdout, BufReader};

use crew_core::RoleId;
use crew_mcp::{Broker, Server};
use eyre::{eyre, Result, WrapErr};
use mimalloc::MiMalloc;

/// mimalloc as the global allocator (M-MIMALLOC-APPS).
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() -> Result<()> {
    let role = std::env::var("CREW_ROLE").ok();
    let role = role
        .as_deref()
        .map(str::trim)
        .filter(|role| !role.is_empty())
        .ok_or_else(|| eyre!("CREW_ROLE must be set to the role this MCP server acts as"))?;

    // The broker address comes from the same environment the broker reads.
    let config =
        crew_broker::Config::from_env().wrap_err("could not read the broker configuration")?;
    let base = format!("http://{}", config.bind_addr());

    let broker = Broker::new(base, RoleId::new(role));
    let mut server = Server::new(broker);
    server
        .serve(BufReader::new(stdin().lock()), stdout().lock())
        .wrap_err("the MCP server exited with an I/O error")
}
