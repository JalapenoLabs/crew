//! The crew message broker: the localhost `crewd` HTTP + SSE service.
//!
//! `crewd` owns the message log, the roster, and delivery (see
//! `docs/architecture.md`). It binds loopback-only, serves a health probe, and
//! shuts down gracefully (issue #7); it stores and reads the [`crew_core`] event
//! model with typed per-kind message fields and typed 4xx validation errors
//! (issue #8). A `POST /channels/{channel}/messages` stamps the timestamp
//! server-side (rejecting a spoofed one), masks configured secret values out of
//! the event with the [`Scrubber`], persists it, and fans it to every subscriber
//! on the `GET /stream` SSE feed (issue #9). A `GET /history` reads past events,
//! filtered, time-ordered, and paginated with a cursor stable under concurrent
//! writes (issue #12). The self-filtered per-role streams, the roster, and the
//! rolling-summary history come in later tickets.
//!
//! Run it with the `crewd` binary, or drive it as a library through [`run`].
//!
//! [`crew_core`]: crew_core

mod api;
mod config;
mod error;
mod events;
mod history;
mod router;
mod secrets;
mod serve;
mod state;
mod store;

pub use config::{Config, DEFAULT_PORT, DEFAULT_STATE_DIR};
pub use error::ApiError;
pub use router::ChannelRouter;
pub use secrets::{mask, Scrubber};
pub use serve::run;
pub use state::AppState;
pub use store::{
    EventFilter, EventKindTag, EventPage, EventQuery, InvalidCursor, LogStore, MemoryStore, Roster,
    Storage,
};
