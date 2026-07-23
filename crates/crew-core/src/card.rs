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
    /// The role that leads and routes the unit: the hub of the hub-and-spoke topology.
    ///
    /// Every card names the commander so a role knows where an unaddressed message
    /// goes (to the commander) and whether it is itself the commander (see
    /// [`is_commander`](RoleCard::is_commander)). Defaults to `commander`, the
    /// conventional name, when a hand-authored card omits it.
    #[serde(default = "default_commander")]
    pub commander: RoleId,
    /// How to reach the unit: the broker's address.
    pub broker: BrokerEndpoint,
}

/// The conventional commander name, used when a card does not name one.
fn default_commander() -> RoleId {
    RoleId::new("commander")
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
            commander: default_commander(),
            broker,
        }
    }

    /// Sets the crew's commander, returning the card so calls can chain.
    ///
    /// The supervisor builds cards from the crew config, which names the commander
    /// (see [`CrewConfig`](crate::CrewConfig)); a bare [`new`](RoleCard::new) card
    /// falls back to the conventional `commander`.
    #[must_use]
    pub fn with_commander(mut self, commander: RoleId) -> Self {
        self.commander = commander;
        self
    }

    /// Whether this card's role is the commander, the unit's lead and router.
    #[must_use]
    pub fn is_commander(&self) -> bool {
        self.role == self.commander
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
        let _ = writeln!(
            out,
            "Reach the unit through the crew MCP tools: crew_send to message a teammate or \
             channel, crew_inbox to read what is addressed to you, crew_roster to see the team. \
             The broker is at {}. The coordination rules live in crew; do not restate them.",
            self.broker.base_url(),
        );

        out.push('\n');
        out.push_str(
            "A redirect or belay in your inbox is a command from the General: honor it at your \
             very next tool boundary, not when your current step finishes. A redirect steers you: \
             keep your task and adjust course. A belay overrides you: stop your current work and \
             take its message as your new order.\n",
        );

        out.push('\n');
        out.push_str(&self.topology_briefing());
        out
    }

    /// The role's place in the hub-and-spoke topology: commander duties or how a
    /// specialist reaches its commander (see `docs/communication.md`).
    fn topology_briefing(&self) -> String {
        if self.is_commander() {
            return "You are the commander: the unit's lead and router. The General briefs you. \
                Decompose the work and issue an order to each specialist with crew_order (name \
                the role, a title, the scope, the paths it owns, and the acceptance bar). \
                Arbitrate at lane boundaries and report progress back to the General. You route \
                and decide; you do not take the field, so you write no feature code yourself. A \
                direct peer message and the rare all-units broadcast are available but are not \
                the default."
                .to_owned();
        }

        format!(
            "Your commander is {commander}: brief it and report to it by default (a crew_send \
             with no `to` or `channel` reaches the commander). It may send you an order; work it \
             within your lane and report status back. For a tight loop you may direct-message a \
             peer (crew_send with `to`), and `all-units` reaches the whole unit for the rare \
             broadcast.",
            commander = self.commander,
        )
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
        assert!(
            briefing.contains("redirect") && briefing.contains("belay"),
            "tells the role to honor the General's redirect and belay"
        );
        assert!(
            briefing.contains("next tool boundary"),
            "says when to honor a directive"
        );
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

    #[test]
    fn the_commander_briefing_states_its_duties_and_fan_out_handle() {
        // A card whose role is the commander gets the commander's briefing.
        let card = RoleCard::new(
            RoleId::new("commander"),
            Vec::new(),
            String::new(),
            BrokerEndpoint::new("127.0.0.1", 2739),
        );
        assert!(card.is_commander(), "the role matches the commander");
        let briefing = card.briefing();
        assert!(briefing.contains("Decompose"), "states decomposition duty");
        assert!(briefing.contains("Arbitrate"), "states arbitration duty");
        assert!(
            briefing.contains("crew_order"),
            "gives the handle to issue orders",
        );
    }

    #[test]
    fn a_specialist_briefing_names_its_commander() {
        // The default crew names the commander `commander`; a specialist is told so.
        let card = sample();
        assert!(!card.is_commander(), "backend is not the commander");
        let briefing = card.briefing();
        assert!(
            briefing.contains("Your commander is commander"),
            "names the commander to brief and report to",
        );
        assert!(
            briefing.contains("reaches the commander"),
            "explains the default addressing",
        );
    }

    #[test]
    fn with_commander_sets_a_configured_commander_and_round_trips() {
        // A crew may name a different commander; the card carries it and survives TOML.
        let card = sample().with_commander(RoleId::new("lead"));
        assert_eq!(card.commander, RoleId::new("lead"));
        assert!(!card.is_commander(), "backend is not the `lead` commander");

        let parsed = RoleCard::from_toml(&card.to_toml().unwrap()).unwrap();
        assert_eq!(parsed, card, "the commander survives a TOML round trip");
        assert!(
            parsed.briefing().contains("Your commander is lead"),
            "the briefing names the configured commander",
        );
    }

    #[test]
    fn a_card_without_a_commander_defaults_to_the_conventional_one() {
        // A hand-authored card that omits the commander gets the conventional default.
        let toml = "role = \"qa\"\n\n[broker]\nhost = \"127.0.0.1\"\nport = 2739\n";
        let card = RoleCard::from_toml(toml).unwrap();
        assert_eq!(card.commander, RoleId::new("commander"));
    }
}
