//! The crew message broker: the localhost `crewd` HTTP + SSE service.
//!
//! `crewd` owns the message log, the roster, and delivery (see
//! `docs/architecture.md`). It binds loopback-only, serves a health probe, and
//! shuts down gracefully (issue #7); it stores and reads the [`crew_core`]
//! event model with typed per-kind message fields and typed 4xx validation
//! errors (issue #8). A `POST /channels/{channel}/messages` stamps the
//! timestamp server-side (rejecting a spoofed one), masks configured secret
//! values out of the event with the [`Scrubber`], persists it, and fans it to
//! every subscriber (issue #9). Subscribers read either `GET /stream`, the
//! whole live feed, or `GET /inbox?role=<role>`, a role's live, self-filtered
//! events resumable from a `Last-Event-ID` cursor (issue #10). A `GET /history`
//! reads past events, filtered, time-ordered, and paginated with a cursor
//! stable under concurrent writes (issue #12), and `GET /history?summary=true`
//! folds older events into a bounded rolling summary plus the recent tail, so
//! joining a long conversation costs bounded context (issue #19). The `/roster`
//! endpoints expose the unit's roles and liveness, publishing each change as a
//! lifecycle event (issue #14).
//!
//! Run it with the `crewd` binary, or drive it as a library through [`run`].
//!
//! [`crew_core`]: crew_core

mod api;
mod board;
mod boundary;
mod briefing;
mod budget;
mod complete;
mod config;
mod control;
mod error;
mod events;
mod filter;
mod gate;
mod history;
mod inbox;
mod ledger;
mod roster;
mod router;
mod secrets;
mod serve;
mod stall;
mod state;
mod stats;
mod store;
mod summary;
mod usage;

pub use config::{Config, DEFAULT_PORT, DEFAULT_STATE_DIR};
pub use error::ApiError;
pub use router::ChannelRouter;
pub use secrets::{mask, Scrubber};
pub use serve::{run, run_until, serve};
pub use state::{AppState, Sequenced};
pub use store::{
    EventFilter, EventKindTag, EventPage, EventQuery, InvalidCursor, Liveness, LogStore,
    MemoryStore, RoleStatus, Roster, Storage,
};
