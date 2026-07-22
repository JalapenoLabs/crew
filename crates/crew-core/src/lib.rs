//! Shared crew types and the typed event model.
//!
//! `crew-core` is the root of the workspace's dependency graph: every other crate
//! depends on it, and it depends on none of them. It will hold the newtyped
//! identifiers (role, channel, message id), the message and event schema, and the
//! error types the broker, supervisor, MCP surface, and CLI all speak.
//!
//! This is the scaffold from issue #1; the types land in later phases (see
//! `docs/architecture.md` and `docs/communication.md`).
