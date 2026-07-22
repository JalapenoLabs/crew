//! The crew process supervisor.
//!
//! Spawns one agent process per role (each a `claude -p --output-format
//! stream-json` child with its role card), wires it to the broker, parses its
//! stream into per-agent activity events, and manages its lifecycle: lazy start,
//! idle-stop, and restart on death. Built on the types in [`crew_core`].
//!
//! This is the scaffold from issue #1; the supervisor lands in a later phase (see
//! `docs/architecture.md` and `docs/observability.md`).
