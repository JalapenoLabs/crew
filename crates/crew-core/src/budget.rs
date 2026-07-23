//! The crew token budget: a crew-wide ceiling and per-role caps, so a crew cannot quietly
//! burn a fortune (issue #54).
//!
//! A [`Budget`] is the crew's spend accountant. Built from the crew config
//! ([`CrewConfig::budget`](crate::CrewConfig::budget)), it tracks cumulative token spend
//! per role and crew-wide, and each [`record`](Budget::record) reports the running totals
//! and whether the spend just crossed a cap. The supervisor holds one, feeds it each
//! turn's token usage, and on a breach idle-stops the role (a per-role cap) or the whole
//! crew (the crew-wide budget) rather than overrun, surfacing the moment on the event
//! stream so a cap is never hit silently (see `docs/observability.md`).
//!
//! The shape follows the Workflow budget pattern: a total, the spend so far, and the
//! remaining headroom, with the cap a hard bound. The accountant is pure and free of I/O,
//! so enforcement is decided here and applied by the supervisor.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::id::RoleId;

/// Which ceiling a spend crossed: one role's cap, or the crew-wide budget.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BudgetScope {
    /// A single role's own token cap.
    Role,
    /// The crew-wide token budget, across every role.
    Crew,
}

/// The result of recording a role's token spend: the running totals and any cap it hit.
///
/// The supervisor turns this into a `budget` event on the stream and, when
/// [`breach`](Spend::breach) is set, idle-stops the role or the crew. A `None` breach is a
/// spend report still within budget.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Spend {
    /// The role whose turn this spend belongs to.
    pub role: RoleId,
    /// The role's cumulative spend after this record.
    pub role_spent: u64,
    /// The role's cap, if it has one.
    pub role_cap: Option<u64>,
    /// The crew's cumulative spend after this record.
    pub crew_spent: u64,
    /// The crew-wide budget, if the crew has one.
    pub crew_budget: Option<u64>,
    /// The ceiling this spend newly crossed, if any: the trigger to idle-stop and surface.
    /// `None` when still within budget or already over (so a cap acts once, not on every
    /// later record). The crew budget takes precedence over a role cap crossed at once.
    pub breach: Option<BudgetScope>,
}

/// A crew's token budget: a crew-wide ceiling and optional per-role caps (issue #54).
///
/// Build one from the crew config with [`CrewConfig::budget`](crate::CrewConfig::budget),
/// or directly with [`new`](Budget::new). Feed it each turn's spend with
/// [`record`](Budget::record); read the standing with the query methods. A crew with no
/// crew-wide budget and no per-role cap is unbounded and never breaches.
///
/// # Examples
/// ```
/// use std::collections::BTreeMap;
/// use crew_core::{Budget, BudgetScope, RoleId};
///
/// let backend = RoleId::new("backend");
/// let mut budget = Budget::new(None, BTreeMap::from([(backend.clone(), 1_000)]));
///
/// assert!(budget.record(&backend, 600).breach.is_none());
/// // The next spend reaches the cap: the role is over and the breach is surfaced once.
/// assert_eq!(budget.record(&backend, 500).breach, Some(BudgetScope::Role));
/// assert!(budget.record(&backend, 100).breach.is_none(), "a cap acts once");
/// ```
#[derive(Debug, Clone, Default)]
pub struct Budget {
    /// The crew-wide ceiling, if any.
    crew_ceiling: Option<u64>,
    /// Each role's own cap.
    caps: BTreeMap<RoleId, u64>,
    /// Each role's cumulative spend.
    spent: BTreeMap<RoleId, u64>,
    /// The crew's cumulative spend across every role.
    crew_spent: u64,
    /// Roles whose cap-hit has already been surfaced, so it fires once.
    flagged: BTreeSet<RoleId>,
    /// Whether the crew-wide breach has already been surfaced.
    crew_flagged: bool,
}

impl Budget {
    /// A budget with a crew-wide ceiling (`crew_budget`) and per-role `caps`.
    #[must_use]
    pub fn new(crew_budget: Option<u64>, caps: BTreeMap<RoleId, u64>) -> Self {
        Self {
            crew_ceiling: crew_budget,
            caps,
            ..Self::default()
        }
    }

    /// Records `tokens` of spend for `role`, returning the running totals and any breach.
    ///
    /// The returned [`Spend`] carries a [`breach`](Spend::breach) only the first time a
    /// ceiling is crossed, so the caller idle-stops and surfaces the moment once. The crew
    /// budget takes precedence over a role cap crossed by the same spend.
    pub fn record(&mut self, role: &RoleId, tokens: u64) -> Spend {
        let role_spent = {
            let entry = self.spent.entry(role.clone()).or_default();
            *entry = entry.saturating_add(tokens);
            *entry
        };
        self.crew_spent = self.crew_spent.saturating_add(tokens);
        let role_cap = self.caps.get(role).copied();

        let crew_over = self
            .crew_ceiling
            .is_some_and(|ceiling| self.crew_spent >= ceiling);
        let role_over = role_cap.is_some_and(|cap| role_spent >= cap);
        let breach = self.newly_breached(role, crew_over, role_over);

        Spend {
            role: role.clone(),
            role_spent,
            role_cap,
            crew_spent: self.crew_spent,
            crew_budget: self.crew_ceiling,
            breach,
        }
    }

    /// The scope a spend newly drove over its ceiling, flagging it so it fires once.
    ///
    /// The crew budget wins when both are crossed at once, since idle-stopping the crew
    /// subsumes idle-stopping the role.
    fn newly_breached(
        &mut self,
        role: &RoleId,
        crew_over: bool,
        role_over: bool,
    ) -> Option<BudgetScope> {
        // Flag the role either way once it is over, so a later spend does not re-fire it.
        let role_newly = role_over && self.flagged.insert(role.clone());
        if crew_over && !self.crew_flagged {
            self.crew_flagged = true;
            return Some(BudgetScope::Crew);
        }
        role_newly.then_some(BudgetScope::Role)
    }

    /// Whether the crew has any ceiling at all: a crew-wide budget or a per-role cap.
    ///
    /// An unbounded crew never breaches, so the supervisor skips recording and reporting
    /// its spend rather than emit a report against no budget.
    #[must_use]
    pub fn is_bounded(&self) -> bool {
        self.crew_ceiling.is_some() || !self.caps.is_empty()
    }

    /// The crew's cumulative spend across every role.
    #[must_use]
    pub fn crew_spent(&self) -> u64 {
        self.crew_spent
    }

    /// The crew-wide budget, if the crew has one.
    #[must_use]
    pub fn crew_budget(&self) -> Option<u64> {
        self.crew_ceiling
    }

    /// The crew's remaining headroom, or `None` when the crew is unbounded.
    #[must_use]
    pub fn crew_remaining(&self) -> Option<u64> {
        self.crew_ceiling
            .map(|ceiling| ceiling.saturating_sub(self.crew_spent))
    }

    /// A role's cumulative spend.
    #[must_use]
    pub fn role_spent(&self, role: &RoleId) -> u64 {
        self.spent.get(role).copied().unwrap_or(0)
    }

    /// A role's remaining headroom under its own cap, or `None` when it is uncapped.
    #[must_use]
    pub fn role_remaining(&self, role: &RoleId) -> Option<u64> {
        self.caps
            .get(role)
            .map(|cap| cap.saturating_sub(self.role_spent(role)))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{Budget, BudgetScope};
    use crate::id::RoleId;

    fn role(name: &str) -> RoleId {
        RoleId::new(name)
    }

    #[test]
    fn an_unbounded_crew_never_breaches() {
        let mut budget = Budget::new(None, BTreeMap::new());
        let backend = role("backend");
        for _ in 0..5 {
            assert!(budget.record(&backend, 1_000_000).breach.is_none());
        }
        assert_eq!(budget.crew_spent(), 5_000_000);
        assert_eq!(budget.crew_remaining(), None);
        assert_eq!(budget.role_remaining(&backend), None);
    }

    #[test]
    fn a_role_cap_breaches_once_when_reached() {
        let backend = role("backend");
        let mut budget = Budget::new(None, BTreeMap::from([(backend.clone(), 1_000)]));

        let under = budget.record(&backend, 900);
        assert_eq!(under.breach, None);
        assert_eq!(under.role_spent, 900);
        assert_eq!(budget.role_remaining(&backend), Some(100));

        // Reaching the cap breaches; a later spend does not re-fire it.
        assert_eq!(budget.record(&backend, 100).breach, Some(BudgetScope::Role));
        assert_eq!(budget.record(&backend, 500).breach, None);
        assert_eq!(budget.role_remaining(&backend), Some(0));
    }

    #[test]
    fn a_role_cap_leaves_other_roles_alone() {
        let backend = role("backend");
        let frontend = role("frontend");
        let mut budget = Budget::new(None, BTreeMap::from([(backend.clone(), 1_000)]));

        assert_eq!(
            budget.record(&backend, 1_000).breach,
            Some(BudgetScope::Role)
        );
        // Frontend is uncapped, so its spend never breaches.
        assert_eq!(budget.record(&frontend, 5_000).breach, None);
    }

    #[test]
    fn the_crew_budget_breaches_across_roles() {
        let backend = role("backend");
        let frontend = role("frontend");
        let mut budget = Budget::new(Some(1_000), BTreeMap::new());

        assert_eq!(budget.record(&backend, 600).breach, None);
        // The crew total (600 + 400) reaches the crew budget, though no single role did.
        assert_eq!(
            budget.record(&frontend, 400).breach,
            Some(BudgetScope::Crew)
        );
        assert_eq!(budget.crew_remaining(), Some(0));
        assert_eq!(
            budget.record(&backend, 100).breach,
            None,
            "the crew cap acts once"
        );
    }

    #[test]
    fn the_crew_budget_takes_precedence_over_a_role_cap_crossed_at_once() {
        let backend = role("backend");
        let mut budget = Budget::new(Some(1_000), BTreeMap::from([(backend.clone(), 1_000)]));

        // A single spend crosses both ceilings; the crew scope wins (it stops the crew).
        assert_eq!(
            budget.record(&backend, 1_000).breach,
            Some(BudgetScope::Crew)
        );
    }
}
