//! The crew message broker.
//!
//! A localhost HTTP + SSE service that owns the message log, the roster, and
//! delivery: agents post a typed message and subscribe to a self-filtered stream,
//! and a history endpoint serves a compact rolling summary rather than the full
//! transcript. Built on the types in [`crew_core`].
//!
//! This is the scaffold from issue #1; the service lands in a later phase (see
//! `docs/architecture.md`).
