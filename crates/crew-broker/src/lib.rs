//! The crew message broker: the localhost `crewd` HTTP + SSE service.
//!
//! `crewd` owns the message log, the roster, and delivery (see
//! `docs/architecture.md`). This is the service skeleton (issue #7): it binds
//! loopback-only, serves a health probe, wires the application state (the
//! [`Storage`] backend and the [`ChannelRouter`]), and shuts down gracefully. The
//! message endpoints, the self-filtered SSE streams, the roster, and history come
//! in later tickets.
//!
//! Run it with the `crewd` binary, or drive it as a library through [`run`].

mod api;
mod config;
mod router;
mod serve;
mod state;
mod store;

pub use config::{Config, DEFAULT_PORT, DEFAULT_STATE_DIR};
pub use router::ChannelRouter;
pub use serve::run;
pub use state::AppState;
pub use store::{MemoryStore, Storage};
