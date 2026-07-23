//! The crew agent-facing MCP surface.
//!
//! An MCP (Model Context Protocol) server that gives agents real coordination tools
//! instead of shelling out to append to a file (see `docs/architecture.md`). A
//! Claude Code (or Codex) agent connects over stdio and calls:
//!
//! - `crew_send` (message a channel or role),
//! - `crew_inbox` (read the messages addressed to it, self-filtered),
//! - `crew_roster` (list teammates and the lanes they own),
//! - `crew_lane` (check a path against its owned lane before an out-of-lane edit),
//! - `crew_claim` / `crew_ledger` (the work ledger: claim a task before touching shared
//!   work, and read who holds what),
//! - `crew_submit` / `crew_verdict` / `crew_gate` (the adversarial done-gate: submit
//!   work for verification, judge a teammate's work, and read the gate).
//!
//! The server is a thin client over the broker's HTTP + SSE API ([`crew_broker`]);
//! it never touches the store. It acts as one role, configured when the supervisor
//! spawns it. Run it with the `crew-mcp` binary, or drive it as a library through
//! [`Server`].
//!
//! [`crew_broker`]: crew_broker

mod broker;
mod server;

pub use broker::{
    Broker, GateSnapshot, GateTask, InboxItem, LedgerItem, RoleEntry, RosterSnapshot, Standing,
};
pub use server::Server;
