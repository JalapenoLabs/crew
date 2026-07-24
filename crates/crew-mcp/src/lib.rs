//! The crew agent-facing MCP surface.
//!
//! An MCP (Model Context Protocol) server that gives agents real coordination
//! tools instead of shelling out to append to a file (see
//! `docs/architecture.md`). A Claude Code (or Codex) agent connects over stdio
//! and calls:
//!
//! - `crew_send` (send a note to a channel or role),
//! - `crew_ask` / `crew_answer` (post a typed question or answer, so an
//!   unanswered question surfaces a coordination stall; issue #123),
//! - `crew_status` / `crew_artifact` (post a typed progress `status` or a
//!   reference to a produced branch, PR, file, or route, so the typed rendering
//!   and any projection that keys on the kind is not lost to a plain note;
//!   issue #167),
//! - `crew_inbox` (read the messages addressed to it, self-filtered, each with
//!   an id a `crew_answer` can reference),
//! - `crew_roster` (list teammates and the lanes they own),
//! - `crew_lane` (check a path against its owned lane before an out-of-lane
//!   edit),
//! - `crew_claim` / `crew_ledger` (the work ledger: claim a task before
//!   touching shared work, and read who holds what),
//! - `crew_submit` / `crew_verdict` / `crew_gate` (the adversarial done-gate:
//!   submit work for verification, judge a teammate's work, and read the gate),
//! - `crew_complete` (report the mission gracefully finished, typically as the
//!   commander, so `crew notify` fires on a true completion),
//! - `crew_board` / `crew_record` (the shared situation board: read the crew's
//!   durable memory, and record or retract a decision, interface, or gotcha),
//! - `crew_briefing` (the bounded new-role briefing packet: the board and a
//!   lane-scoped rolling summary, so a fresh role catches up without reading
//!   the whole log).
//!
//! The server dispatches each tool to a [`crew_client::Broker`], the shared
//! thin client over the broker's HTTP + SSE API (issue #129); it never touches
//! the store. It acts as one role, configured when the supervisor spawns it.
//! Run it with the `crew-mcp` binary, or drive it as a library through
//! [`Server`].

mod server;

pub use server::Server;
