//! The channel model: how a message names its audience, and who it reaches.
//!
//! A [`ChannelId`] is a name on the wire; a [`Channel`] is its parsed meaning.
//! The topology (see `docs/communication.md`) defines three kinds:
//!
//! - [`all-units`](Channel::AllUnits) reaches every role.
//! - a [direct](Channel::Direct) `@role` channel reaches one role,
//!   point-to-point.
//! - a [pair](Channel::Pair) `a+b` channel reaches its two named members only.
//!
//! Naming is canonical: a pair is order-independent, so `frontend+backend` and
//! `backend+frontend` are the same channel and [`name`](Channel::name) always
//! renders its members in a stable order. [`addresses`](Channel::addresses)
//! answers whether a channel reaches a role, the membership test that routing
//! to subscribers and self-filtered inbox delivery both build on.

use std::fmt;

use crate::id::{ChannelId, RoleId};

/// The name of the channel that reaches every role.
pub const ALL_UNITS: &str = "all-units";

/// A parsed channel: the audience a message names (see the module docs).
///
/// Build one with [`Channel::parse`] from a wire name, or from the variants
/// directly. A [`Pair`](Channel::Pair) must be built with [`Channel::pair`],
/// which orders its members canonically so that equality and
/// [`name`](Channel::name) do not depend on the order they were given.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Channel {
    /// Reaches every role. Named `all-units`.
    AllUnits,
    /// A direct, point-to-point channel to one role. Named `@role`.
    Direct(RoleId),
    /// A pair channel between two distinct roles, held in canonical order.
    /// Named `a+b`.
    Pair(RoleId, RoleId),
}

impl Channel {
    /// Parses a channel name into its meaning, or `None` if the name is
    /// unrecognized.
    ///
    /// Recognizes `all-units`, `@role`, and `a+b`. A malformed name (an empty
    /// or blank role, a role paired with itself, or a member carrying a
    /// reserved `@` or `+`) resolves to `None`, so an unroutable name is
    /// rejected, not guessed.
    ///
    /// # Examples
    /// ```
    /// use crew_core::{Channel, RoleId};
    /// assert_eq!(Channel::parse("all-units"), Some(Channel::AllUnits));
    /// assert_eq!(
    ///     Channel::parse("@qa"),
    ///     Some(Channel::Direct(RoleId::new("qa")))
    /// );
    /// assert_eq!(Channel::parse("a+b"), Channel::parse("b+a")); // order-independent
    /// assert_eq!(Channel::parse("nope"), None);
    /// ```
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        if name == ALL_UNITS {
            return Some(Self::AllUnits);
        }
        if let Some(role) = name.strip_prefix('@') {
            return is_plain_role(role).then(|| Self::Direct(RoleId::new(role)));
        }
        if let Some((first, second)) = name.split_once('+') {
            return Self::pair(RoleId::new(first), RoleId::new(second));
        }
        None
    }

    /// Builds a pair channel between two distinct roles, or `None` if invalid.
    ///
    /// Returns `None` when the roles are equal, blank, or either carries a
    /// reserved `@` or `+`. The members are stored in canonical order, so
    /// `pair(a, b)` and `pair(b, a)` are equal and share one
    /// [`name`](Channel::name).
    #[must_use]
    pub fn pair(first: RoleId, second: RoleId) -> Option<Self> {
        if first == second || !is_plain_role(first.as_str()) || !is_plain_role(second.as_str()) {
            return None;
        }
        let (low, high) = if first <= second {
            (first, second)
        } else {
            (second, first)
        };
        Some(Self::Pair(low, high))
    }

    /// The canonical name of this channel, as it appears on an
    /// [`Event`](crate::Event).
    #[must_use]
    pub fn name(&self) -> ChannelId {
        ChannelId::new(self.to_string())
    }

    /// Whether this channel reaches `role`.
    ///
    /// `all-units` reaches every role, a direct channel reaches only its
    /// addressee, and a pair reaches either of its two members.
    #[must_use]
    pub fn addresses(&self, role: &RoleId) -> bool {
        match self {
            Self::AllUnits => true,
            Self::Direct(addressee) => addressee == role,
            Self::Pair(first, second) => first == role || second == role,
        }
    }

    /// Resolves the channel a message addresses under the hub-and-spoke
    /// default.
    ///
    /// This is the one addressing rule the whole crew obeys (see
    /// `docs/communication.md`), so "brief the commander by default" means the
    /// same whether the General's front-end or an agent's `crew_send` sends
    /// it:
    ///
    /// - a non-blank `to` names a role for a direct message and wins if given;
    /// - otherwise a non-blank `channel` is parsed (`all-units`, a `@role`, or
    ///   a pair);
    /// - if neither is given, the message goes to the `commander`.
    ///
    /// Returns `None` when `to` is not a plain role name, or `channel` is given
    /// but is not a recognized channel, so an unroutable target is
    /// rejected, not guessed.
    ///
    /// # Examples
    /// ```
    /// use crew_core::{Channel, RoleId};
    ///
    /// let commander = RoleId::new("commander");
    ///
    /// // Neither given: the brief reaches the commander.
    /// assert_eq!(
    ///     Channel::resolve(None, None, &commander),
    ///     Some(Channel::Direct(commander.clone())),
    /// );
    /// // A named role wins: a direct message to that role.
    /// assert_eq!(
    ///     Channel::resolve(Some("backend"), None, &commander),
    ///     Some(Channel::Direct(RoleId::new("backend"))),
    /// );
    /// // A channel name is parsed.
    /// assert_eq!(
    ///     Channel::resolve(None, Some("all-units"), &commander),
    ///     Some(Channel::AllUnits)
    /// );
    /// // An unrecognized channel is rejected.
    /// assert_eq!(Channel::resolve(None, Some("nonsense"), &commander), None);
    /// ```
    #[must_use]
    pub fn resolve(to: Option<&str>, channel: Option<&str>, commander: &RoleId) -> Option<Self> {
        let to = to.map(str::trim).filter(|name| !name.is_empty());
        let channel = channel.map(str::trim).filter(|name| !name.is_empty());
        match (to, channel) {
            (Some(role), _) => is_plain_role(role).then(|| Self::Direct(RoleId::new(role))),
            (None, Some(name)) => Self::parse(name),
            (None, None) => Some(Self::Direct(commander.clone())),
        }
    }
}

impl fmt::Display for Channel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AllUnits => f.write_str(ALL_UNITS),
            Self::Direct(role) => write!(f, "@{role}"),
            Self::Pair(first, second) => write!(f, "{first}+{second}"),
        }
    }
}

/// Whether `name` is a plain role name: not blank, and free of the reserved
/// channel markers `@` and `+`, so it cannot be confused with a channel's own
/// syntax.
fn is_plain_role(name: &str) -> bool {
    !name.trim().is_empty() && !name.contains('@') && !name.contains('+')
}

#[cfg(test)]
mod tests {
    use super::{Channel, ALL_UNITS};
    use crate::id::RoleId;

    fn role(name: &str) -> RoleId {
        RoleId::new(name)
    }

    #[test]
    fn parses_the_three_channel_kinds() {
        assert_eq!(Channel::parse(ALL_UNITS), Some(Channel::AllUnits));
        assert_eq!(
            Channel::parse("@backend"),
            Some(Channel::Direct(role("backend")))
        );
        assert_eq!(
            Channel::parse("backend+frontend"),
            Channel::pair(role("backend"), role("frontend")),
        );
    }

    #[test]
    fn a_pair_is_order_independent_and_canonically_named() {
        let forward = Channel::parse("frontend+backend").unwrap();
        let reverse = Channel::parse("backend+frontend").unwrap();
        assert_eq!(forward, reverse, "member order must not matter");
        // The name renders members in a single canonical (sorted) order.
        assert_eq!(forward.name().as_str(), "backend+frontend");
        assert_eq!(reverse.name().as_str(), "backend+frontend");
    }

    #[test]
    fn names_round_trip_through_parse() {
        for channel in [
            Channel::AllUnits,
            Channel::Direct(role("qa")),
            Channel::pair(role("frontend"), role("backend")).unwrap(),
        ] {
            let name = channel.name();
            assert_eq!(
                Channel::parse(name.as_str()),
                Some(channel.clone()),
                "{name} must parse back to itself",
            );
        }
    }

    #[test]
    fn rejects_malformed_names() {
        for bad in [
            "",        // empty
            "  ",      // blank
            "@",       // direct with no role
            "@ ",      // direct with a blank role
            "backend", // a bare role name is not a channel
            "random",  // unknown word
            "a+a",     // a pair of one role with itself
            "a+b+c",   // more than two members
            "@a+b",    // a member carrying a reserved marker
            "+b",      // pair missing its first member
            "a+",      // pair missing its second member
        ] {
            assert_eq!(Channel::parse(bad), None, "{bad:?} must not parse");
        }
    }

    #[test]
    fn resolve_follows_the_hub_and_spoke_default() {
        let commander = role("commander");

        // Neither target given: the message reaches the commander.
        assert_eq!(
            Channel::resolve(None, None, &commander),
            Some(Channel::Direct(commander.clone())),
            "an unaddressed message defaults to the commander",
        );
        // A blank target is treated as absent, so it still defaults to the commander.
        assert_eq!(
            Channel::resolve(Some("  "), Some("  "), &commander),
            Some(Channel::Direct(commander.clone())),
        );

        // A named role wins over the default and over a channel.
        assert_eq!(
            Channel::resolve(Some("backend"), Some("all-units"), &commander),
            Some(Channel::Direct(role("backend"))),
            "a direct `to` takes precedence",
        );

        // A channel name resolves to its parsed meaning.
        assert_eq!(
            Channel::resolve(None, Some("all-units"), &commander),
            Some(Channel::AllUnits),
        );
        assert_eq!(
            Channel::resolve(None, Some("frontend+backend"), &commander),
            Channel::pair(role("frontend"), role("backend")),
        );

        // An unroutable target is rejected rather than guessed.
        assert_eq!(Channel::resolve(None, Some("nonsense"), &commander), None);
        assert_eq!(Channel::resolve(Some("a+b"), None, &commander), None);
    }

    #[test]
    fn addresses_resolves_membership_across_the_matrix() {
        let backend = role("backend");
        let frontend = role("frontend");
        let qa = role("qa");

        // all-units reaches every role.
        assert!(Channel::AllUnits.addresses(&backend));
        assert!(Channel::AllUnits.addresses(&qa));

        // a direct channel reaches only its addressee.
        let direct = Channel::Direct(backend.clone());
        assert!(direct.addresses(&backend));
        assert!(!direct.addresses(&frontend));

        // a pair reaches both members and no one else.
        let pair = Channel::pair(frontend.clone(), backend.clone()).unwrap();
        assert!(pair.addresses(&frontend));
        assert!(pair.addresses(&backend));
        assert!(!pair.addresses(&qa));
    }
}
