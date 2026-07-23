//! Shared crew types and the typed event model.
//!
//! `crew-core` is the root of the workspace's dependency graph: every other crate
//! depends on it, and it depends on none of them. It defines the strongly-typed
//! vocabulary the broker, supervisor, MCP surface, and CLI all speak, so an
//! identifier is never a bare string (C-NEWTYPE):
//!
//! - the identifier newtypes ([`RoleId`], [`ChannelId`], [`MessageId`],
//!   [`TaskId`]) and the [`Timestamp`] wrapper;
//! - the [`Sender`] of an event (a role, or the General);
//! - the [`Event`] stream item and its [`EventKind`] payloads ([`Message`] with a
//!   [`MessageKind`], [`Lifecycle`], and [`Activity`]);
//! - the [`Channel`] model that names a message's audience (`all-units`, a direct
//!   `@role`, or a `a+b` pair) and resolves which roles it reaches;
//! - the [`RoleCard`] an agent boots from: its lane, its acceptance bar, and how to
//!   reach the unit.
//!
//! Everything serializes to a stable wire format with serde, so the broker can
//! route it and any front-end can render it (see `docs/communication.md` and
//! `docs/observability.md`).

mod card;
mod channel;
mod event;
mod id;
mod time;

pub use card::{BrokerEndpoint, CardError, RoleCard, ROLE_CARD_ENV};
pub use channel::{Channel, ALL_UNITS};
pub use event::{Activity, ArtifactKind, Event, EventKind, Lifecycle, Message, MessageKind};
pub use id::{ChannelId, MessageId, RoleId, Sender, TaskId};
pub use time::Timestamp;
