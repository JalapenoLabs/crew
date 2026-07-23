//! Rules of engagement: the risky actions a role must get signed off (issue #39).
//!
//! A crew is safe to leave running only when the moves that are expensive to undo, a
//! push, a merge, a delete, a spend, an external post, do not happen without a human in
//! the loop. The [`RulesOfEngagement`] a role carries name which [`RiskyAction`]s it must
//! get approved before taking; everything else it does freely.
//!
//! The policy is pure and broker-agnostic: it answers "does this action need sign-off?"
//! and nothing else, so the approval request and decision plumbing (the broker gate and
//! the `crew_request_approval` tool) build on it without the policy knowing they exist.
//!
//! Defaults follow trust: a specialist gates every risky action, while the commander, the
//! unit's integrator, may merge without sign-off (see [`RulesOfEngagement::default_for`]).
//! A crew overrides them per role in its config (see `docs/config.md`).

use std::collections::BTreeSet;
use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};

/// An action risky enough to gate behind human approval (issue #39).
///
/// These are the moves that are expensive or impossible to undo: pushing or merging code,
/// deleting, spending, or posting somewhere external. A role's [`RulesOfEngagement`] names
/// which of them it must get approved first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RiskyAction {
    /// Pushing commits to a remote.
    Push,
    /// Merging a branch or a pull request.
    Merge,
    /// Deleting a branch, a file tree, or data.
    Delete,
    /// Spending money or tokens (gated above a threshold, see [`RulesOfEngagement`]).
    Spend,
    /// Posting outside the crew: a comment, an issue, a chat message, a webhook.
    ExternalPost,
}

impl RiskyAction {
    /// Every risky action, the full set a policy is drawn from.
    pub const ALL: [RiskyAction; 5] = [
        RiskyAction::Push,
        RiskyAction::Merge,
        RiskyAction::Delete,
        RiskyAction::Spend,
        RiskyAction::ExternalPost,
    ];

    /// The action's wire and display label (`push`, `merge`, `external_post`, ...).
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            RiskyAction::Push => "push",
            RiskyAction::Merge => "merge",
            RiskyAction::Delete => "delete",
            RiskyAction::Spend => "spend",
            RiskyAction::ExternalPost => "external_post",
        }
    }

    /// Parses an action from its label, the inverse of [`label`](RiskyAction::label).
    #[must_use]
    pub fn parse(label: &str) -> Option<Self> {
        RiskyAction::ALL
            .into_iter()
            .find(|action| action.label() == label)
    }
}

impl Display for RiskyAction {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// A role's rules of engagement: the risky actions it must get approved before taking.
///
/// The policy is a set of gated [`RiskyAction`]s plus an optional spend threshold. It is
/// the pure decision behind the approval gate: [`requires_approval`] answers whether a
/// given action needs sign-off, and the broker turns a "yes" into a blocking approval
/// request (issue #39).
///
/// # Examples
/// ```
/// use crew_core::{RiskyAction, RulesOfEngagement};
///
/// // A specialist gates every risky action.
/// let specialist = RulesOfEngagement::default_for(false);
/// assert!(specialist.requires_approval(RiskyAction::Merge, None));
///
/// // The commander, the integrator, may merge without sign-off.
/// let commander = RulesOfEngagement::default_for(true);
/// assert!(!commander.requires_approval(RiskyAction::Merge, None));
/// assert!(commander.requires_approval(RiskyAction::Push, None));
///
/// // A spend threshold lets small spends through and gates the large ones.
/// let thrifty = RulesOfEngagement::new([RiskyAction::Spend]).with_spend_threshold(1_000);
/// assert!(!thrifty.requires_approval(RiskyAction::Spend, Some(500)));
/// assert!(thrifty.requires_approval(RiskyAction::Spend, Some(5_000)));
/// ```
///
/// [`requires_approval`]: RulesOfEngagement::requires_approval
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RulesOfEngagement {
    /// The actions this role must get signed off before taking.
    #[serde(default)]
    gated: BTreeSet<RiskyAction>,
    /// When set, a [`Spend`](RiskyAction::Spend) needs approval only above this magnitude,
    /// so routine small spends proceed while a large one is gated. With no threshold a
    /// gated `Spend` gates every spend.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    spend_threshold: Option<u64>,
}

impl RulesOfEngagement {
    /// Builds a policy gating exactly `gated`, with no spend threshold.
    pub fn new(gated: impl IntoIterator<Item = RiskyAction>) -> Self {
        Self {
            gated: gated.into_iter().collect(),
            spend_threshold: None,
        }
    }

    /// A policy gating every risky action: the safe fallback when none is stated.
    #[must_use]
    pub fn gate_all() -> Self {
        Self::new(RiskyAction::ALL)
    }

    /// Sets the spend threshold, so a spend needs approval only above `threshold`.
    #[must_use]
    pub fn with_spend_threshold(mut self, threshold: u64) -> Self {
        self.spend_threshold = Some(threshold);
        self
    }

    /// The sensible default policy for a role, by whether it is the commander (issue #39).
    ///
    /// A specialist gates every risky action; the commander, the unit's integrator, may
    /// merge without sign-off but still gates the rest. A crew overrides this per role in
    /// its config.
    #[must_use]
    pub fn default_for(is_commander: bool) -> Self {
        let mut gated: BTreeSet<RiskyAction> = RiskyAction::ALL.into_iter().collect();
        if is_commander {
            gated.remove(&RiskyAction::Merge);
        }
        Self {
            gated,
            spend_threshold: None,
        }
    }

    /// Whether `action` at `magnitude` needs human approval before the role may take it.
    ///
    /// `magnitude` matters only for a [`Spend`](RiskyAction::Spend) once a threshold is
    /// set: a spend at or below the threshold proceeds, above it is gated. For every other
    /// action `magnitude` is ignored and the answer is whether the action is gated.
    #[must_use]
    pub fn requires_approval(&self, action: RiskyAction, magnitude: Option<u64>) -> bool {
        if action == RiskyAction::Spend {
            if let Some(threshold) = self.spend_threshold {
                return magnitude.unwrap_or(0) > threshold;
            }
        }
        self.gated.contains(&action)
    }

    /// Whether `action` is gated at all, ignoring any spend magnitude.
    #[must_use]
    pub fn gates(&self, action: RiskyAction) -> bool {
        self.gated.contains(&action)
    }

    /// The gated actions, in a stable order.
    pub fn gated_actions(&self) -> impl Iterator<Item = RiskyAction> + '_ {
        self.gated.iter().copied()
    }

    /// The spend threshold, if one narrows the [`Spend`](RiskyAction::Spend) gate.
    #[must_use]
    pub fn spend_threshold(&self) -> Option<u64> {
        self.spend_threshold
    }
}

#[cfg(test)]
mod tests {
    use super::{RiskyAction, RulesOfEngagement};

    #[test]
    fn a_risky_action_round_trips_through_its_label() {
        for action in RiskyAction::ALL {
            assert_eq!(RiskyAction::parse(action.label()), Some(action));
        }
        assert_eq!(RiskyAction::parse("stroll"), None);
    }

    /// The acceptance matrix: role default policy times every action.
    ///
    /// A specialist gates all five; the commander gates all but `merge`.
    #[test]
    fn the_default_policy_matrix_gates_by_role_and_action() {
        let specialist = RulesOfEngagement::default_for(false);
        let commander = RulesOfEngagement::default_for(true);

        for action in RiskyAction::ALL {
            assert!(
                specialist.requires_approval(action, None),
                "a specialist gates {action}",
            );
            let commander_gates = action != RiskyAction::Merge;
            assert_eq!(
                commander.requires_approval(action, None),
                commander_gates,
                "the commander gates {action} unless it is a merge",
            );
        }
    }

    #[test]
    fn an_ungated_action_needs_no_approval() {
        let policy = RulesOfEngagement::new([RiskyAction::Push, RiskyAction::Delete]);
        assert!(policy.requires_approval(RiskyAction::Push, None));
        assert!(policy.requires_approval(RiskyAction::Delete, None));
        assert!(
            !policy.requires_approval(RiskyAction::Merge, None),
            "merge is not gated"
        );
        assert!(!policy.requires_approval(RiskyAction::ExternalPost, None));
    }

    #[test]
    fn a_spend_threshold_gates_only_above_the_line() {
        let policy = RulesOfEngagement::new([RiskyAction::Spend]).with_spend_threshold(1_000);
        assert!(
            !policy.requires_approval(RiskyAction::Spend, Some(1_000)),
            "at the line proceeds"
        );
        assert!(!policy.requires_approval(RiskyAction::Spend, Some(999)));
        assert!(
            policy.requires_approval(RiskyAction::Spend, Some(1_001)),
            "above the line gates"
        );
    }

    #[test]
    fn a_gated_spend_without_a_threshold_gates_every_spend() {
        let policy = RulesOfEngagement::new([RiskyAction::Spend]);
        assert!(policy.requires_approval(RiskyAction::Spend, None));
        assert!(policy.requires_approval(RiskyAction::Spend, Some(1)));
    }

    #[test]
    fn gate_all_gates_everything_and_the_default_is_permissive() {
        let all = RulesOfEngagement::gate_all();
        for action in RiskyAction::ALL {
            assert!(
                all.requires_approval(action, None),
                "gate_all gates {action}"
            );
        }
        let empty = RulesOfEngagement::default();
        for action in RiskyAction::ALL {
            assert!(
                !empty.requires_approval(action, None),
                "the empty default gates nothing"
            );
        }
    }
}
