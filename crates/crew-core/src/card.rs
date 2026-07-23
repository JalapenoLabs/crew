//! The role card: the thin bootstrap that hands an agent its lane and the broker.
//!
//! A [`RoleCard`] is the shape a role boots from. It carries only what is specific
//! to this agent, its name, the paths it owns, the acceptance bar it holds work to,
//! and how to reach the unit, so the coordination rules live in crew itself and are
//! not restated per agent. The supervisor writes a card per role it spawns; a
//! standalone agent (the shape the `coworker` skill shrinks to) is handed one
//! directly. Both parse it with [`RoleCard::from_toml`] and both render the agent's
//! briefing with [`RoleCard::briefing`].
//!
//! The on-disk form is TOML, chosen so a human can read and author a card at a
//! glance:
//!
//! ```toml
//! role = "backend"
//! owned_paths = ["api/", "db/"]
//! acceptance = "Tests green, migrations reversible, no clippy warnings."
//!
//! [broker]
//! host = "127.0.0.1"
//! port = 2739
//! ```
//!
//! The card is sans-io: it parses from a string and serializes to a string, so the
//! caller owns the file or transport. This keeps `crew-core` free of I/O and makes
//! the format trivially testable (see [`M-IMPL-IO`] in the Rust guidelines).
//!
//! [`M-IMPL-IO`]: https://microsoft.github.io/rust-guidelines/

use std::backtrace::Backtrace;
use std::fmt::{self, Display, Formatter, Write as _};

use serde::{Deserialize, Serialize};

use crate::id::RoleId;

/// The environment variable naming the role card a spawned agent boots from.
///
/// The supervisor writes a card and sets this to its path; the `crew-mcp` server
/// reads it. Sharing the name here keeps the writer and the reader from drifting.
pub const ROLE_CARD_ENV: &str = "CREW_ROLE_CARD";

/// A role's boot card: its lane, its acceptance bar, and how to reach the unit.
///
/// This is the whole per-agent bootstrap. Everything else an agent needs, the
/// channels, the message schema, the chain of command, is common to the crew and
/// lives in crew, not in the card.
///
/// # Examples
/// ```
/// use crew_core::{BrokerEndpoint, RoleCard, RoleId};
///
/// let card = RoleCard::new(
///     RoleId::new("backend"),
///     vec!["api/".to_owned()],
///     "Tests green, no clippy warnings.",
///     BrokerEndpoint::new("127.0.0.1", 2739),
/// );
/// assert_eq!(card.broker.base_url(), "http://127.0.0.1:2739");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RoleCard {
    /// The role this agent plays: its stable id and its direct channel name.
    pub role: RoleId,
    /// The directory boundaries this role owns: its lane in the tree.
    ///
    /// Empty means the role owns no fixed lane and coordinates before touching
    /// shared code.
    #[serde(default)]
    pub owned_paths: Vec<String>,
    /// The bar the role holds its work to before reporting it done.
    ///
    /// Free-form prose. Empty means the role falls back to the crew's standard bar.
    #[serde(default)]
    pub acceptance: String,
    /// How to reach the unit: the broker's address.
    pub broker: BrokerEndpoint,
}

impl RoleCard {
    /// Builds a card for `role` with its owned paths, acceptance bar, and broker.
    #[must_use]
    pub fn new(
        role: RoleId,
        owned_paths: Vec<String>,
        acceptance: impl Into<String>,
        broker: BrokerEndpoint,
    ) -> Self {
        Self {
            role,
            owned_paths,
            acceptance: acceptance.into(),
            broker,
        }
    }

    /// Parses a card from its TOML form.
    ///
    /// # Errors
    /// Returns a [`CardError`] if `toml` is not a well-formed card.
    pub fn from_toml(toml: &str) -> Result<Self, CardError> {
        toml::from_str(toml).map_err(|source| CardError::new(ErrorKind::Parse(Box::new(source))))
    }

    /// Renders the card to its TOML form.
    ///
    /// # Errors
    /// Returns a [`CardError`] if the card cannot be serialized, which in practice
    /// only happens on an internal invariant violation.
    pub fn to_toml(&self) -> Result<String, CardError> {
        toml::to_string(self)
            .map_err(|source| CardError::new(ErrorKind::Serialize(Box::new(source))))
    }

    /// Renders the agent's briefing: the thin bootstrap prompt a role boots from.
    ///
    /// This is the shape the `coworker` skill shrinks to once crew exists: it states
    /// the role, its lane, its acceptance bar, and how to reach the unit, and stops
    /// there. It deliberately restates none of the coordination rules.
    ///
    /// # Examples
    /// ```
    /// use crew_core::{BrokerEndpoint, RoleCard, RoleId};
    ///
    /// let card = RoleCard::new(
    ///     RoleId::new("backend"),
    ///     vec!["api/".to_owned()],
    ///     "Tests green.",
    ///     BrokerEndpoint::new("127.0.0.1", 2739),
    /// );
    /// let briefing = card.briefing();
    /// assert!(briefing.contains("backend"));
    /// assert!(briefing.contains("api/"));
    /// assert!(briefing.contains("http://127.0.0.1:2739"));
    /// ```
    #[must_use]
    pub fn briefing(&self) -> String {
        // Built once from short pieces; `write!` to a String never fails.
        let mut out = String::new();
        let _ = writeln!(out, "You are the {} role on a crew.", self.role);
        out.push('\n');

        if self.owned_paths.is_empty() {
            out.push_str(
                "Your lane: you own no fixed paths yet. Coordinate before touching shared code.\n",
            );
        } else {
            let _ = writeln!(
                out,
                "Your lane: you own {}. Work within it and coordinate at its edges.",
                self.owned_paths.join(", "),
            );
        }

        if !self.acceptance.is_empty() {
            let _ = writeln!(out, "Acceptance bar: {}", self.acceptance);
        }

        out.push('\n');
        let _ = write!(
            out,
            "Reach the unit through the crew MCP tools: crew_send to message a teammate or \
             channel, crew_inbox to read what is addressed to you, crew_roster to see the team. \
             The broker is at {}. The coordination rules live in crew; do not restate them.",
            self.broker.base_url(),
        );
        out
    }
}

/// The broker's address: where an agent reaches the unit.
///
/// # Examples
/// ```
/// use crew_core::BrokerEndpoint;
///
/// let broker = BrokerEndpoint::new("127.0.0.1", 2739);
/// assert_eq!(broker.base_url(), "http://127.0.0.1:2739");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BrokerEndpoint {
    /// The host the broker listens on, an IP or a hostname.
    pub host: String,
    /// The TCP port the broker listens on.
    pub port: u16,
}

impl BrokerEndpoint {
    /// Builds an endpoint from a host and a port.
    #[must_use]
    pub fn new(host: impl Into<String>, port: u16) -> Self {
        Self {
            host: host.into(),
            port,
        }
    }

    /// The base URL a client uses to reach the broker, such as `http://127.0.0.1:2739`.
    ///
    /// The scheme is always `http`: the broker listens on loopback, so no TLS is
    /// involved. An IPv6 literal host is not bracketed, since a crew broker is
    /// addressed by `127.0.0.1` or a hostname in practice.
    #[must_use]
    pub fn base_url(&self) -> String {
        format!("http://{}:{}", self.host, self.port)
    }
}

/// The error returned when a [`RoleCard`] cannot be parsed or serialized.
///
/// Inspect it with [`CardError::is_parse`] and [`CardError::is_serialize`] to tell a
/// malformed card from an internal serialization fault.
#[derive(Debug)]
pub struct CardError {
    kind: ErrorKind,
    backtrace: Backtrace,
}

impl CardError {
    /// Wraps a kind, capturing a backtrace (empty unless `RUST_BACKTRACE` is set).
    fn new(kind: ErrorKind) -> Self {
        Self {
            kind,
            backtrace: Backtrace::capture(),
        }
    }

    /// Whether the card text was malformed and could not be parsed.
    #[must_use]
    pub fn is_parse(&self) -> bool {
        matches!(self.kind, ErrorKind::Parse(_))
    }

    /// Whether a well-formed card could not be serialized back to TOML.
    #[must_use]
    pub fn is_serialize(&self) -> bool {
        matches!(self.kind, ErrorKind::Serialize(_))
    }
}

impl Display for CardError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.kind {
            ErrorKind::Parse(source) => write!(f, "could not parse the role card: {source}")?,
            ErrorKind::Serialize(source) => {
                write!(f, "could not serialize the role card: {source}")?;
            }
        }
        if let std::backtrace::BacktraceStatus::Captured = self.backtrace.status() {
            write!(f, "\n{}", self.backtrace)?;
        }
        Ok(())
    }
}

impl std::error::Error for CardError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.kind {
            ErrorKind::Parse(source) => Some(&**source),
            ErrorKind::Serialize(source) => Some(&**source),
        }
    }
}

/// What went wrong loading a card. Kept private so new failure modes never break the
/// public API (callers match on the `is_*` methods instead).
///
/// The `toml` errors are boxed: they are large, and an error is the cold path, so a
/// pointer keeps the common `Ok` result small (clippy `result_large_err`).
#[derive(Debug)]
enum ErrorKind {
    Parse(Box<toml::de::Error>),
    Serialize(Box<toml::ser::Error>),
}

#[cfg(test)]
mod tests {
    use super::{BrokerEndpoint, RoleCard};
    use crate::id::RoleId;

    /// A fully specified card, as the supervisor would write it.
    fn sample() -> RoleCard {
        RoleCard::new(
            RoleId::new("backend"),
            vec!["api/".to_owned(), "db/".to_owned()],
            "Tests green, migrations reversible, no clippy warnings.",
            BrokerEndpoint::new("127.0.0.1", 2739),
        )
    }

    #[test]
    fn a_card_round_trips_through_toml() {
        let card = sample();
        let toml = card.to_toml().unwrap();
        let parsed = RoleCard::from_toml(&toml).unwrap();
        assert_eq!(parsed, card, "a serialized card parses back unchanged");
    }

    #[test]
    fn a_hand_authored_card_parses() {
        let toml = r#"
            role = "frontend"
            owned_paths = ["web/"]
            acceptance = "Renders at mobile and desktop widths."

            [broker]
            host = "127.0.0.1"
            port = 2739
        "#;
        let card = RoleCard::from_toml(toml).unwrap();
        assert_eq!(card.role, RoleId::new("frontend"));
        assert_eq!(card.owned_paths, ["web/"]);
        assert_eq!(card.broker.base_url(), "http://127.0.0.1:2739");
    }

    #[test]
    fn owned_paths_and_acceptance_default_to_empty() {
        // A minimal card names only its role and broker; the rest is optional.
        let toml = "role = \"docs\"\n\n[broker]\nhost = \"127.0.0.1\"\nport = 2739\n";
        let card = RoleCard::from_toml(toml).unwrap();
        assert!(card.owned_paths.is_empty(), "owned_paths defaults to empty");
        assert!(card.acceptance.is_empty(), "acceptance defaults to empty");
    }

    #[test]
    fn a_malformed_card_is_a_parse_error() {
        let error = RoleCard::from_toml("role = ").unwrap_err();
        assert!(error.is_parse(), "a broken card is a parse error");
        assert!(!error.is_serialize());
        assert!(error.to_string().contains("role card"));
    }

    #[test]
    fn a_missing_broker_is_a_parse_error() {
        // The broker is the one required section beyond the role: without it a role
        // cannot reach the unit, so the card is rejected.
        let error = RoleCard::from_toml("role = \"qa\"\n").unwrap_err();
        assert!(error.is_parse());
    }

    #[test]
    fn the_briefing_states_the_lane_bar_and_broker() {
        let briefing = sample().briefing();
        assert!(briefing.contains("backend"), "names the role");
        assert!(briefing.contains("api/, db/"), "lists the owned lane");
        assert!(
            briefing.contains("no clippy warnings"),
            "states the acceptance bar"
        );
        assert!(
            briefing.contains("http://127.0.0.1:2739"),
            "gives the broker address"
        );
        assert!(briefing.contains("crew_send"), "points at the MCP tools");
    }

    #[test]
    fn the_briefing_handles_a_role_without_a_lane() {
        let card = RoleCard::new(
            RoleId::new("commander"),
            Vec::new(),
            String::new(),
            BrokerEndpoint::new("127.0.0.1", 2739),
        );
        let briefing = card.briefing();
        assert!(
            briefing.contains("no fixed paths"),
            "explains the missing lane"
        );
        assert!(
            !briefing.contains("Acceptance bar:"),
            "omits the acceptance line when there is none",
        );
    }
}
