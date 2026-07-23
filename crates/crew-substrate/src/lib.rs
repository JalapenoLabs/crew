//! The crew coordination substrate as one consumable crate.
//!
//! crew splits cleanly into a **substrate** (a message broker plus a process
//! supervisor) and the **front-ends** that drive it (the terminal CLI today, a
//! Seraphim panel later). This crate is the substrate: the single dependency a
//! front-end takes on, re-exporting the public API of the four library crates
//! it is built from, so a consumer says `crew-substrate` and gets the whole
//! coordination backbone without reaching into its internal crate split (see
//! `docs/architecture.md`, Distribution).
//!
//! The substrate is the reusable part; the CLI and any Seraphim glue are
//! consumers that depend only on this public API. There is no logic here: each
//! module is a sibling crate re-exported under a short name, and everything
//! below is documented in that crate.
//!
//! # Modules
//!
//! - [`core`] ([`crew_core`]) is the shared, strongly-typed vocabulary every
//!   part speaks: the identifier newtypes, the [`Event`](core::Event) stream
//!   item and its kinds, the [`Channel`](core::Channel) addressing model, the
//!   [`RoleCard`](core::RoleCard) an agent boots from, and the
//!   [`CrewConfig`](core::CrewConfig) that describes a crew. It is sans-io and
//!   depends on none of the others.
//! - [`broker`] ([`crew_broker`]) is the localhost `crewd` HTTP + SSE service:
//!   it owns the message log, the roster, and delivery. Run it in-process with
//!   [`broker::run`] or embed it with [`broker::serve`], reading and streaming
//!   the `core` event model behind a swappable [`broker::Storage`].
//! - [`supervisor`] ([`crew_supervisor`]) spawns and manages the role-scoped
//!   agent processes: [`supervisor::Supervisor`] brings a crew up,
//!   [`supervisor::Fleet`] drives each agent's lifecycle, and
//!   [`supervisor::RosterClient`] wires liveness to the broker roster.
//! - [`mcp`] ([`crew_mcp`]) is the agent-facing surface: [`mcp::Server`] speaks
//!   the Model Context Protocol over stdio, and [`mcp::Broker`] is the thin
//!   HTTP client an agent (or the CLI shim) uses to send, read its inbox, and
//!   list the roster.
//!
//! The one event stream these emit is documented as a stable public contract in
//! `docs/stream-contract.md`, so an external consumer (such as Runewood)
//! renders a crew from it directly.
//!
//! # Third-party types on the public API
//!
//! Per [`M-DONT-LEAK-TYPES`], sibling crates may leak each other's types
//! through this umbrella, but the substrate deliberately exposes a few
//! third-party types on its public surface, each for a substantial
//! interoperability benefit:
//!
//! - **tokio** on the broker's boundary. [`broker::serve`] takes a
//!   `tokio::net::TcpListener` and a shutdown future, and [`broker::AppState`]
//!   holds a `tokio::sync::broadcast` sender, because the broker is an async
//!   service a consumer runs on its own tokio runtime. Leaking tokio is what
//!   lets it.
//! - **serde** on the `core` wire types. [`Event`](core::Event) and its
//!   payloads derive `Serialize` / `Deserialize` so the broker can route them
//!   and any front-end can render them; the whole point is interchange.
//! - **chrono** through [`Timestamp`](core::Timestamp), whose `to_datetime`
//!   yields a `chrono::DateTime<Utc>` so a consumer can format or compare
//!   instants.
//! - **eyre** as the error type of the broker and supervisor entry points (for
//!   example [`broker::run`]), following the repository's application error
//!   convention ([`M-APP-ERROR`]); a future crates.io publish may narrow these
//!   to canonical error structs.
//!
//! [`M-DONT-LEAK-TYPES`]: https://microsoft.github.io/rust-guidelines/
//! [`M-APP-ERROR`]: https://microsoft.github.io/rust-guidelines/
//!
//! # Examples
//!
//! Take the substrate as one dependency and reach its parts through the
//! modules:
//!
//! ```
//! use crew_substrate::{broker, core, supervisor};
//!
//! // The broker binds loopback by default.
//! let config = broker::Config::default();
//! assert!(config.host.is_loopback());
//! assert_eq!(config.port, broker::DEFAULT_PORT);
//!
//! // The shared types describe a crew and the agents that run it.
//! let card = core::RoleCard::new(
//!     core::RoleId::new("backend"),
//!     vec!["api/".to_owned()],
//!     "Tests green.",
//!     core::BrokerEndpoint::new(config.host.to_string(), config.port),
//! );
//! assert_eq!(card.role.as_str(), "backend");
//!
//! // The supervisor drives agents against a broker at that address.
//! let _supervisor = supervisor::Supervisor::new(
//!     core::BrokerEndpoint::new("127.0.0.1", config.port),
//!     std::env::temp_dir(),
//! );
//! ```

#[doc(inline)]
pub use crew_broker as broker;
#[doc(inline)]
pub use crew_core as core;
#[doc(inline)]
pub use crew_mcp as mcp;
#[doc(inline)]
pub use crew_supervisor as supervisor;
