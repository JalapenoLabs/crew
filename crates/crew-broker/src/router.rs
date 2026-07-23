//! The broker's channel routing: which roles a message wakes.

use crew_core::{Channel, ChannelId, RoleId};

/// Routes a message to the roles its channel reaches (see
/// `docs/communication.md`).
///
/// Resolves the canonical [`Channel`] model against a roster of candidate
/// roles, so `all-units` wakes every role, a direct `@role` wakes only its
/// addressee, and a pair wakes only its two members. Stateless today: the
/// roster is supplied by the caller. It becomes the owner of the live roster
/// when the roster/liveness ticket lands, which is why it stays in
/// [`AppState`](crate::AppState).
#[derive(Debug, Default)]
pub struct ChannelRouter;

impl ChannelRouter {
    /// The roles from `roster` that a message on `channel` should reach.
    ///
    /// Returns the roster members the channel addresses, in the roster's order.
    /// An unrecognized channel name reaches no one, so a misaddressed
    /// message wakes nobody rather than everybody. Self-echo (a sender
    /// receiving its own message) is filtered at delivery, not here:
    /// routing answers only who a channel addresses.
    #[must_use]
    pub fn recipients<'a, I>(&self, channel: &ChannelId, roster: I) -> Vec<RoleId>
    where
        I: IntoIterator<Item = &'a RoleId>,
    {
        let Some(channel) = Channel::parse(channel.as_str()) else {
            return Vec::new();
        };
        roster
            .into_iter()
            .filter(|role| channel.addresses(role))
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use crew_core::{ChannelId, RoleId};

    use super::ChannelRouter;

    fn roles(names: &[&str]) -> Vec<RoleId> {
        names.iter().map(|name| RoleId::new(*name)).collect()
    }

    #[test]
    fn routes_each_channel_to_exactly_its_members() {
        let router = ChannelRouter;
        let live = roles(&["backend", "frontend", "qa"]);
        let reach = |name: &str| router.recipients(&ChannelId::new(name), &live);

        // all-units reaches every live role.
        assert_eq!(reach("all-units"), live);
        // a direct channel reaches only its addressee.
        assert_eq!(reach("@backend"), roles(&["backend"]));
        // a pair reaches only its two members, in the roster's order.
        assert_eq!(reach("frontend+backend"), roles(&["backend", "frontend"]));
        // an addressee that is not live reaches no one.
        assert_eq!(reach("@security"), roles(&[]));
        // an unrecognized channel reaches no one.
        assert_eq!(reach("bogus"), roles(&[]));
    }
}
