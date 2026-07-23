//! The crew message broker: the localhost `crewd` HTTP + SSE service.
//!
//! `crewd` owns the message log, the roster, and delivery (see
//! `docs/architecture.md`). It binds loopback-only, serves a health probe, and
//! shuts down gracefully (issue #7); it stores and reads the [`crew_core`] event
//! model over `POST`/`GET /events`, with typed per-kind message fields and typed
//! 4xx validation errors (issue #8). The self-filtered SSE streams, the roster,
//! and the rolling-summary history come in later tickets.
//!
//! Run it with the `crewd` binary, or drive it as a library through [`run`].
//!
//! [`crew_core`]: crew_core

mod api;
mod config;
mod error;
mod events;
mod router;
mod serve;
mod state;
mod store;

pub use config::{Config, DEFAULT_PORT, DEFAULT_STATE_DIR};
pub use error::ApiError;
pub use router::ChannelRouter;
pub use serve::run;
pub use state::AppState;
pub use store::{MemoryStore, Storage};
