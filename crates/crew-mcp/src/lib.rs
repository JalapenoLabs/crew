//! The crew agent-facing MCP surface.
//!
//! Exposes the coordination tools agents call instead of shelling out to append
//! to a file: `crew_send` (message a channel or role), `crew_inbox` (read new
//! messages), `crew_roster` (list teammates and their lanes), and friends. It is
//! a thin adapter over the broker ([`crew_broker`]) speaking [`crew_core`] types.
//!
//! This is the scaffold from issue #1; the tool surface lands in a later phase
//! (see `docs/architecture.md` and `docs/communication.md`).
