//! Model tiers: how much model muscle each role gets, and the sensible default mapping.
//!
//! A crew should spend its strong-model budget where reasoning pays off and run a cheap
//! model for mechanical work (issue #53). Rather than pin every role to an exact build,
//! a crew assigns a [`ModelTier`] per role, an intent (`strong`, `standard`, `cheap`),
//! and maps each tier to a concrete model alias with [`ModelTiers`]. Retuning spend, say
//! moving every cheap role from `haiku` to `sonnet`, is then a config change to the tier
//! map, not a code change and not a per-role edit.
//!
//! Two things make the mapping sensible out of the box:
//!
//! - [`default_tier_for`] gives every role a default tier by name: the lead roles run
//!   strong, the mechanical roles run cheap, and the builders run standard.
//! - [`ModelTiers::default`] maps the tiers to the Claude Code aliases `opus` / `sonnet`
//!   / `haiku`, which resolve to the current build of each.
//!
//! A crew config overrides both: a role's tier (or an exact model), and what each tier
//! resolves to. See `docs/config.md` and [`CrewConfig`](crate::CrewConfig).

use std::fmt::{self, Display, Formatter};

use serde::{Deserialize, Serialize};

use crate::id::RoleId;

/// The strong model's default alias: the lead and the architect run it.
const DEFAULT_STRONG: &str = "opus";
/// The standard model's default alias: the builders run it.
const DEFAULT_STANDARD: &str = "sonnet";
/// The cheap model's default alias: the mechanical roles run it.
const DEFAULT_CHEAP: &str = "haiku";

/// Roles that default to the [`Strong`](ModelTier::Strong) tier: the lead and architect.
const STRONG_ROLES: &[&str] = &["commander", "architect"];

/// Roles that default to the [`Cheap`](ModelTier::Cheap) tier: mechanical, low-reasoning
/// work (docs, the pipeline, lint, and the test-writing roles).
const CHEAP_ROLES: &[&str] = &[
    "docs",
    "ci",
    "release",
    "lint",
    "test",
    "sdet-unit",
    "sdet-e2e",
];

/// A model tier: how much model muscle a role gets, independent of the exact build.
///
/// A tier names the intent; the crew's [`ModelTiers`] map resolves it to a concrete model
/// alias. Spending strong-model budget where it matters and a cheap model everywhere else
/// is then a tier assignment per role, retunable without touching code (issue #53).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ModelTier {
    /// The strong model: the lead and the architect, where reasoning pays off.
    Strong,
    /// The standard model: the builders (backend, frontend, qa) doing the real work.
    #[default]
    Standard,
    /// The cheap model: mechanical roles (docs, ci, lint, test) where a small model does.
    Cheap,
}

impl Display for ModelTier {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Strong => "strong",
            Self::Standard => "standard",
            Self::Cheap => "cheap",
        })
    }
}

/// The concrete model alias each [`ModelTier`] resolves to for a crew.
///
/// Defaults to the Claude Code aliases (`opus` / `sonnet` / `haiku`), which resolve to the
/// current build of each. A crew overrides any of them in its config `[models]` table, so
/// changing what `cheap` means retunes spend across every cheap role at once.
///
/// # Examples
/// ```
/// use crew_core::{ModelTier, ModelTiers};
///
/// let tiers = ModelTiers::default();
/// assert_eq!(tiers.resolve(ModelTier::Strong), "opus");
/// assert_eq!(tiers.resolve(ModelTier::Cheap), "haiku");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelTiers {
    /// The alias the [`Strong`](ModelTier::Strong) tier resolves to.
    strong: String,
    /// The alias the [`Standard`](ModelTier::Standard) tier resolves to.
    standard: String,
    /// The alias the [`Cheap`](ModelTier::Cheap) tier resolves to.
    cheap: String,
}

impl Default for ModelTiers {
    fn default() -> Self {
        Self {
            strong: DEFAULT_STRONG.to_owned(),
            standard: DEFAULT_STANDARD.to_owned(),
            cheap: DEFAULT_CHEAP.to_owned(),
        }
    }
}

impl ModelTiers {
    /// The concrete model alias `tier` resolves to under this map.
    #[must_use]
    pub fn resolve(&self, tier: ModelTier) -> &str {
        match tier {
            ModelTier::Strong => &self.strong,
            ModelTier::Standard => &self.standard,
            ModelTier::Cheap => &self.cheap,
        }
    }

    /// Builds the tier map from optional overrides, each falling back to its default alias.
    ///
    /// A blank or whitespace-only override is treated as absent, so an empty `[models]`
    /// entry keeps the default rather than pinning the empty string.
    pub(crate) fn from_overrides(
        strong: Option<String>,
        standard: Option<String>,
        cheap: Option<String>,
    ) -> Self {
        let default = Self::default();
        Self {
            strong: alias_or(strong, default.strong),
            standard: alias_or(standard, default.standard),
            cheap: alias_or(cheap, default.cheap),
        }
    }
}

/// The trimmed `override` alias, or `fallback` when it is absent or blank.
fn alias_or(r#override: Option<String>, fallback: String) -> String {
    match r#override {
        Some(alias) if !alias.trim().is_empty() => alias.trim().to_owned(),
        _ => fallback,
    }
}

/// The default [`ModelTier`] for a role by its name: the sensible mapping (issue #53).
///
/// The lead roles (`commander`, `architect`) default to [`Strong`](ModelTier::Strong), the
/// mechanical roles (`docs`, `ci`, `release`, `lint`, `test`, and the `sdet` test split) to
/// [`Cheap`](ModelTier::Cheap), and every other role, including any custom one, to
/// [`Standard`](ModelTier::Standard). A crew overrides any of these per role in its config.
#[must_use]
pub fn default_tier_for(role: &RoleId) -> ModelTier {
    // Normalize so a capitalized or padded role name still maps predictably.
    let name = role.as_str().trim().to_ascii_lowercase();
    if STRONG_ROLES.contains(&name.as_str()) {
        ModelTier::Strong
    } else if CHEAP_ROLES.contains(&name.as_str()) {
        ModelTier::Cheap
    } else {
        ModelTier::Standard
    }
}

#[cfg(test)]
mod tests {
    use super::{default_tier_for, ModelTier, ModelTiers};
    use crate::id::RoleId;

    #[test]
    fn the_default_tier_map_uses_the_claude_code_aliases() {
        let tiers = ModelTiers::default();
        assert_eq!(tiers.resolve(ModelTier::Strong), "opus");
        assert_eq!(tiers.resolve(ModelTier::Standard), "sonnet");
        assert_eq!(tiers.resolve(ModelTier::Cheap), "haiku");
    }

    #[test]
    fn overrides_replace_only_the_named_tiers() {
        let tiers = ModelTiers::from_overrides(None, Some("opus".to_owned()), Some(" ".to_owned()));
        assert_eq!(
            tiers.resolve(ModelTier::Strong),
            "opus",
            "unset keeps its default"
        );
        assert_eq!(
            tiers.resolve(ModelTier::Standard),
            "opus",
            "a set override wins"
        );
        assert_eq!(
            tiers.resolve(ModelTier::Cheap),
            "haiku",
            "a blank override is treated as absent"
        );
    }

    #[test]
    fn the_default_mapping_spends_strong_where_it_matters_and_cheap_elsewhere() {
        // The lead roles run strong.
        assert_eq!(
            default_tier_for(&RoleId::new("commander")),
            ModelTier::Strong
        );
        assert_eq!(
            default_tier_for(&RoleId::new("architect")),
            ModelTier::Strong
        );

        // The mechanical roles run cheap.
        for cheap in [
            "docs",
            "ci",
            "release",
            "lint",
            "test",
            "sdet-unit",
            "sdet-e2e",
        ] {
            assert_eq!(
                default_tier_for(&RoleId::new(cheap)),
                ModelTier::Cheap,
                "{cheap} defaults to cheap"
            );
        }

        // The builders and any custom role run standard.
        for standard in ["backend", "frontend", "qa", "security", "some-custom-role"] {
            assert_eq!(
                default_tier_for(&RoleId::new(standard)),
                ModelTier::Standard,
                "{standard} defaults to standard"
            );
        }
    }

    #[test]
    fn the_default_mapping_is_case_and_whitespace_insensitive() {
        assert_eq!(
            default_tier_for(&RoleId::new(" Commander ")),
            ModelTier::Strong
        );
        assert_eq!(default_tier_for(&RoleId::new("DOCS")), ModelTier::Cheap);
    }
}
