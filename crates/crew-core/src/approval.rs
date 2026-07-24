//! Rules of engagement: the per-role approval gates for risky actions (issue
//! #39).
//!
//! A crew is safe to leave running only if the dangerous, hard-to-undo actions
//! wait for human sign-off. Each role carries a [`RulesOfEngagement`]: the set
//! of [`ActionKind`]s it must get the General's approval for before it takes
//! them. A gated action pauses the role until the General grants or denies it
//! (the request rides the message stream, see `docs/communication.md`); an
//! ungated action proceeds with no wait.
//!
//! The defaults follow the chain of command ([`default_roe_for`]): the
//! commander integrates work, so it may push, merge, and spend without
//! sign-off, and is gated only on the irreversible `delete` and on posting
//! outside the crew; a specialist is gated on all five. A crew overrides them
//! per role in its config (see `docs/config.md`).
//!
//! This module is pure and sans-io: the policy is a value, decided by
//! [`RulesOfEngagement::requires_approval`], so the whole action matrix is
//! trivially unit-tested and the same decision drives the config, the role
//! card, and the MCP tool alike.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

/// A risky action a role might take, which its rules of engagement may gate
/// behind the General's approval (issue #39).
///
/// The five the crew models: pushing to a remote, merging, deleting something
/// not easily recovered, spending above a threshold, and posting outside the
/// crew.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionKind {
    /// Push commits to a remote.
    Push,
    /// Merge a branch or a pull request.
    Merge,
    /// Delete something not easily undone: a branch, a file, a resource.
    Delete,
    /// Spend above the role's threshold (tokens against the crew budget).
    Spend,
    /// Post outside the crew: a comment, an issue, a message to an external
    /// service.
    ExternalPost,
}

impl ActionKind {
    /// Every action kind, oldest listed first, for building the matrix or
    /// listing a role's gates.
    pub const ALL: [ActionKind; 5] = [
        ActionKind::Push,
        ActionKind::Merge,
        ActionKind::Delete,
        ActionKind::Spend,
        ActionKind::ExternalPost,
    ];

    /// The wire and config label (`push`, `merge`, `delete`, `spend`,
    /// `external_post`).
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            ActionKind::Push => "push",
            ActionKind::Merge => "merge",
            ActionKind::Delete => "delete",
            ActionKind::Spend => "spend",
            ActionKind::ExternalPost => "external_post",
        }
    }

    /// A short human phrase, so a briefing reads "you need approval to push to
    /// a remote".
    #[must_use]
    pub fn phrase(self) -> &'static str {
        match self {
            ActionKind::Push => "push to a remote",
            ActionKind::Merge => "merge a branch or pull request",
            ActionKind::Delete => "delete a branch, file, or resource",
            ActionKind::Spend => "spend above your threshold",
            ActionKind::ExternalPost => "post outside the crew",
        }
    }

    /// Parses a label back to an action, so an agent or the config can name
    /// one.
    ///
    /// Case-insensitive, and it accepts `external-post` and `post` as aliases
    /// for [`ExternalPost`](Self::ExternalPost), so an agent that names the
    /// action loosely still resolves it.
    #[must_use]
    pub fn parse(label: &str) -> Option<Self> {
        match label.trim().to_ascii_lowercase().as_str() {
            "push" => Some(ActionKind::Push),
            "merge" => Some(ActionKind::Merge),
            "delete" => Some(ActionKind::Delete),
            "spend" => Some(ActionKind::Spend),
            "external_post" | "external-post" | "post" => Some(ActionKind::ExternalPost),
            _ => None,
        }
    }
}

/// The default spend threshold: a spend at or above this many tokens needs
/// approval when a role is gated on spend.
///
/// A round, deliberately conservative ceiling: roughly a large single task's
/// worth of tokens, so a routine turn proceeds but an outsized spend pauses for
/// sign-off. A crew retunes it per role in the config, and the token budget
/// (issue #54) still caps total spend independently.
pub const DEFAULT_SPEND_THRESHOLD: u64 = 1_000_000;

/// A role's rules of engagement: which actions require the General's approval
/// before the role may take them (issue #39).
///
/// The gate is a plain membership test ([`requires_approval`]): a non-spend
/// action is gated when it is in the set; a spend is gated when spend is in the
/// set and the amount reaches the [`spend_threshold`]. Build the sensible
/// per-role default with [`default_roe_for`] and override it from the config.
///
/// [`requires_approval`]: RulesOfEngagement::requires_approval
/// [`spend_threshold`]: RulesOfEngagement::spend_threshold
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RulesOfEngagement {
    /// The actions gated behind approval. A spend in this set is gated only
    /// above [`spend_threshold`](Self::spend_threshold).
    #[serde(default)]
    gated: BTreeSet<ActionKind>,
    /// The spend that needs approval when spend is gated: an amount at or above
    /// this many tokens. Below it, a gated spend still proceeds.
    #[serde(default = "default_spend_threshold")]
    spend_threshold: u64,
}

/// The serde default for [`RulesOfEngagement::spend_threshold`].
fn default_spend_threshold() -> u64 {
    DEFAULT_SPEND_THRESHOLD
}

impl Default for RulesOfEngagement {
    /// The safe default: a specialist's rules of engagement (gated on every
    /// action), so a card or config that omits its rules errs toward asking.
    fn default() -> Self {
        default_roe_for(false)
    }
}

impl RulesOfEngagement {
    /// Builds rules of engagement from the gated actions and the spend
    /// threshold.
    #[must_use]
    pub fn new(gated: impl IntoIterator<Item = ActionKind>, spend_threshold: u64) -> Self {
        Self {
            gated: gated.into_iter().collect(),
            spend_threshold,
        }
    }

    /// Whether `action` requires the General's approval before the role takes
    /// it.
    ///
    /// A non-spend action is gated when it is in the set. A
    /// [`Spend`](ActionKind::Spend) is gated when spend is in the set and
    /// `amount` reaches the [`spend_threshold`](Self::spend_threshold); a spend
    /// below the threshold, or one with no amount given, proceeds. `amount` is
    /// ignored for the non-spend actions.
    #[must_use]
    pub fn requires_approval(&self, action: ActionKind, amount: Option<u64>) -> bool {
        if action == ActionKind::Spend {
            self.gated.contains(&ActionKind::Spend)
                && amount.is_some_and(|amount| amount >= self.spend_threshold)
        } else {
            self.gated.contains(&action)
        }
    }

    /// Whether the action is gated at all, ignoring any spend amount.
    ///
    /// Used to render the briefing: spend is "gated" here when it is in the
    /// set, since the threshold is a detail of the phrase, not of whether
    /// the role must think about it.
    #[must_use]
    pub fn gates(&self, action: ActionKind) -> bool {
        self.gated.contains(&action)
    }

    /// The gated actions, in a stable order, so the briefing lists them the
    /// same way every time.
    pub fn gated_actions(&self) -> impl Iterator<Item = ActionKind> + '_ {
        ActionKind::ALL
            .into_iter()
            .filter(|action| self.gates(*action))
    }

    /// Whether the role gates nothing: every action proceeds without sign-off.
    #[must_use]
    pub fn is_unrestricted(&self) -> bool {
        self.gated.is_empty()
    }

    /// The spend that needs approval when spend is gated (in tokens).
    #[must_use]
    pub fn spend_threshold(&self) -> u64 {
        self.spend_threshold
    }

    /// Replaces the gated set, keeping the spend threshold, for a config
    /// override.
    #[must_use]
    pub fn with_gated(mut self, gated: impl IntoIterator<Item = ActionKind>) -> Self {
        self.gated = gated.into_iter().collect();
        self
    }

    /// Replaces the spend threshold, keeping the gated set, for a config
    /// override.
    #[must_use]
    pub fn with_spend_threshold(mut self, threshold: u64) -> Self {
        self.spend_threshold = threshold;
        self
    }
}

/// The sensible default rules of engagement for a role (issue #39).
///
/// The commander integrates the crew's work, so it may push, merge, and spend
/// without sign-off and is gated only on the irreversible `delete` and on
/// posting outside the crew. A specialist is gated on all five, so it gets
/// human sign-off before it pushes, merges, deletes, posts externally, or
/// spends above the threshold. A crew overrides these per role in its config.
#[must_use]
pub fn default_roe_for(is_commander: bool) -> RulesOfEngagement {
    let gated = if is_commander {
        vec![ActionKind::Delete, ActionKind::ExternalPost]
    } else {
        ActionKind::ALL.to_vec()
    };
    RulesOfEngagement::new(gated, DEFAULT_SPEND_THRESHOLD)
}

#[cfg(test)]
mod tests {
    use super::{default_roe_for, ActionKind, RulesOfEngagement, DEFAULT_SPEND_THRESHOLD};

    #[test]
    fn action_labels_round_trip_and_cover_every_kind() {
        for action in ActionKind::ALL {
            assert_eq!(
                ActionKind::parse(action.label()),
                Some(action),
                "`{}` round-trips",
                action.label(),
            );
            assert!(!action.phrase().is_empty(), "every action has a phrase");
        }
        // Case and the loose aliases resolve, so an agent naming an action is
        // forgiving.
        assert_eq!(ActionKind::parse("MERGE"), Some(ActionKind::Merge));
        assert_eq!(
            ActionKind::parse("external-post"),
            Some(ActionKind::ExternalPost)
        );
        assert_eq!(ActionKind::parse("post"), Some(ActionKind::ExternalPost));
        assert_eq!(ActionKind::parse("nonsense"), None);
    }

    #[test]
    fn the_specialist_default_gates_every_action() {
        let roe = default_roe_for(false);
        // Push, merge, delete, and external post are gated outright.
        for action in [
            ActionKind::Push,
            ActionKind::Merge,
            ActionKind::Delete,
            ActionKind::ExternalPost,
        ] {
            assert!(
                roe.requires_approval(action, None),
                "a specialist needs approval to {}",
                action.label(),
            );
        }
        // Spend is gated only above the threshold.
        assert!(
            !roe.requires_approval(ActionKind::Spend, Some(DEFAULT_SPEND_THRESHOLD - 1)),
            "a spend below the threshold proceeds",
        );
        assert!(
            roe.requires_approval(ActionKind::Spend, Some(DEFAULT_SPEND_THRESHOLD)),
            "a spend at the threshold needs approval",
        );
        assert!(
            !roe.requires_approval(ActionKind::Spend, None),
            "a spend with no amount is not gated (the amount decides)",
        );
    }

    #[test]
    fn the_commander_default_lets_it_integrate_but_gates_the_irreversible() {
        let roe = default_roe_for(true);
        // The lead may push, merge, and spend without sign-off.
        assert!(
            !roe.requires_approval(ActionKind::Push, None),
            "commander may push"
        );
        assert!(
            !roe.requires_approval(ActionKind::Merge, None),
            "commander may merge"
        );
        assert!(
            !roe.requires_approval(ActionKind::Spend, Some(u64::MAX)),
            "commander may spend any amount",
        );
        // But not delete or post outside the crew.
        assert!(
            roe.requires_approval(ActionKind::Delete, None),
            "even the commander gets sign-off to delete",
        );
        assert!(
            roe.requires_approval(ActionKind::ExternalPost, None),
            "even the commander gets sign-off to post externally",
        );
    }

    #[test]
    fn a_full_action_matrix_of_gated_and_ungated() {
        // The acceptance: check every action against a role's rules, both ways.
        let roe = RulesOfEngagement::new([ActionKind::Merge, ActionKind::Spend], 500);
        let matrix = [
            (ActionKind::Push, None, false),
            (ActionKind::Merge, None, true),
            (ActionKind::Delete, None, false),
            (ActionKind::ExternalPost, None, false),
            (ActionKind::Spend, Some(499), false),
            (ActionKind::Spend, Some(500), true),
            (ActionKind::Spend, Some(1000), true),
        ];
        for (action, amount, expected) in matrix {
            assert_eq!(
                roe.requires_approval(action, amount),
                expected,
                "{}{:?} should be {}",
                action.label(),
                amount,
                if expected { "gated" } else { "ungated" },
            );
        }
    }

    #[test]
    fn an_unrestricted_role_gates_nothing() {
        let roe = RulesOfEngagement::new([], DEFAULT_SPEND_THRESHOLD);
        assert!(roe.is_unrestricted());
        for action in ActionKind::ALL {
            assert!(
                !roe.requires_approval(action, Some(u64::MAX)),
                "nothing is gated for an unrestricted role",
            );
        }
        assert_eq!(roe.gated_actions().count(), 0);
    }

    #[test]
    fn gated_actions_lists_in_a_stable_order() {
        let roe = RulesOfEngagement::new(
            [
                ActionKind::ExternalPost,
                ActionKind::Push,
                ActionKind::Delete,
            ],
            DEFAULT_SPEND_THRESHOLD,
        );
        assert_eq!(
            roe.gated_actions().collect::<Vec<_>>(),
            [
                ActionKind::Push,
                ActionKind::Delete,
                ActionKind::ExternalPost
            ],
            "listed in the canonical ActionKind::ALL order, not insertion order",
        );
    }

    #[test]
    fn overrides_replace_the_gated_set_and_the_threshold() {
        let roe = default_roe_for(false)
            .with_gated([ActionKind::Push])
            .with_spend_threshold(42);
        assert!(roe.requires_approval(ActionKind::Push, None));
        assert!(
            !roe.requires_approval(ActionKind::Merge, None),
            "merge is no longer gated"
        );
        assert_eq!(roe.spend_threshold(), 42);
    }
}
