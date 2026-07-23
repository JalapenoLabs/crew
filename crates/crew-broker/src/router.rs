//! The broker's channel router.

/// Routes a message to the roles subscribed to a channel.
///
/// A placeholder for the broker skeleton (issue #7). The routing table and the
/// self-filtered delivery (a sender never receives its own message) land with the
/// delivery work; for now it holds no routes.
#[derive(Debug, Default)]
pub struct ChannelRouter;
