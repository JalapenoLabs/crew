//! The crew config: the declarative description `crew up` reads to bring a crew
//! online.
//!
//! A [`CrewConfig`] names the roles and the lane each owns, the model they run,
//! the repos in scope, the idle-stop timeout, and which role is the commander.
//! It resolves sensible defaults (the default crew: commander, backend,
//! frontend, qa) and validates itself, so a documented config produces a valid
//! crew and an invalid one fails with a precise message (see `docs/config.md`).
//! The config is broker-agnostic; it produces the per-role [`RoleCard`]s with
//! [`to_cards`](CrewConfig::to_cards), taking the broker address at that point.
//!
//! Like [`RoleCard`], the config is sans-io: it parses from a string and never
//! touches the filesystem, so `crew-core` stays free of I/O and the format is
//! trivially testable (the caller owns the file).

use std::{
    backtrace::Backtrace,
    collections::BTreeSet,
    fmt::{self, Display, Formatter},
    path::{Path, PathBuf},
    time::Duration,
};

use serde::{Deserialize, Serialize};

use crate::{
    budget::Budget,
    card::{BrokerEndpoint, RoleCard},
    id::RoleId,
    model::{default_tier_for, ModelTier, ModelTiers},
};

/// How the crew enforces lane ownership when a role edits outside its owned
/// paths (issue #46).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LaneEnforcement {
    /// Off: out-of-lane edits are not checked.
    Off,
    /// Warn: an out-of-lane edit is reported on the stream, but the role may
    /// proceed.
    #[default]
    Warn,
    /// Block: an out-of-lane edit is refused; the role routes through the
    /// commander.
    Block,
}

/// The agent runtime a role runs on: which CLI the supervisor spawns, and how
/// the role reaches the crew (issue #28, #128).
///
/// The broker and roster do not care which runtime produced a role, so a crew
/// can mix them: `crew up` spawns each role on its configured runtime, and they
/// coordinate through the same broker (see `docs/codex.md`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Runtime {
    /// Claude Code (the default): the supervisor spawns `claude` with the crew
    /// MCP server, and the agent reaches the crew through the MCP tools
    /// (`crew_send`, `crew_inbox`, ...).
    #[default]
    Claude,
    /// Codex: the supervisor spawns `codex` and the agent reaches the crew
    /// through the CLI shim (`crew send`, `crew inbox`, ...), which mirrors the
    /// MCP tools over the same broker client (see `docs/codex.md`).
    Codex,
}

/// The default commander: the lead and router the General briefs.
const DEFAULT_COMMANDER: &str = "commander";

/// The default idle-stop timeout: how long a role may be quiet before it is
/// stopped.
const DEFAULT_IDLE_STOP: Duration = Duration::from_secs(5 * 60);

/// A resolved, validated crew: its roles, its defaults, and its commander.
///
/// Build one from a config file with [`from_toml`](CrewConfig::from_toml), or
/// take the [`default`](CrewConfig::default) crew. Every field is resolved
/// (defaults applied) and the whole is validated, so holding a `CrewConfig`
/// means it is well-formed.
///
/// # Examples
/// ```
/// use crew_core::CrewConfig;
///
/// let config = CrewConfig::default();
/// assert_eq!(config.roles.len(), 4);
/// assert_eq!(config.commander.as_str(), "commander");
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CrewConfig {
    /// The roles that make up the crew, each with the lane it owns.
    pub roles: Vec<RoleSpec>,
    /// The role that leads and routes: the one the General briefs.
    pub commander: RoleId,
    /// A crew-wide model alias that runs every un-tiered role, overriding the
    /// default per-role tier mapping (issue #53). `None` (the default) lets
    /// each role run its tier: its own, or the sensible default for its
    /// name (see [`model_for`] and [`default_tier_for`]).
    ///
    /// [`model_for`]: CrewConfig::model_for
    pub model: Option<String>,
    /// The concrete model alias each tier resolves to, so retuning spend is a
    /// config change, not a code change (issue #53). Defaults to `opus` /
    /// `sonnet` / `haiku`.
    pub models: ModelTiers,
    /// The crew-wide token budget: the ceiling on total spend across every role
    /// (issue #54). `None` (the default) leaves the crew unbounded. When the
    /// crew reaches it, the supervisor idle-stops the whole crew rather
    /// than overrun (see [`Budget`](crate::Budget) and
    /// `docs/observability.md`).
    pub token_budget: Option<u64>,
    /// How long a role may be quiet before the supervisor idle-stops it.
    pub idle_stop: Duration,
    /// The repos in scope for the crew: each a path or a bare name, resolved to
    /// a filesystem path by [`repo_paths`](CrewConfig::repo_paths) against the
    /// [`workspace`](CrewConfig::workspace) root (issue #126).
    pub repos: Vec<String>,
    /// The directory the crew's named repos live under, so a bare `repos` name
    /// resolves to a concrete clone (issue #126). Defaults to the crew config
    /// file's own directory; set it (absolute, or relative to the config) to
    /// point at a workspace of sibling clones elsewhere, e.g. `..` when the
    /// config lives inside one of the repos. Absolute `repos` entries ignore
    /// it.
    pub workspace: Option<PathBuf>,
    /// Whether each role works in its own git worktree of the crew's repos, so
    /// parallel roles never clobber each other's edits (issue #43). Off by
    /// default; opt a crew in with `worktrees = true`.
    pub worktrees: bool,
    /// How the crew enforces lane ownership when a role edits outside its lane
    /// (issue #46). Defaults to `warn`.
    pub lane_enforcement: LaneEnforcement,
}

/// One role in a crew: its name, its lane, its acceptance bar, and its model.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleSpec {
    /// The role's stable id (`commander`, `backend`, ...).
    pub role: RoleId,
    /// The directory boundaries the role owns, its lane in the tree.
    pub owned_paths: Vec<String>,
    /// The bar the role holds its work to; empty falls back to the crew's
    /// standard bar.
    pub acceptance: String,
    /// The tier this role runs, overriding the default tier for its name (issue
    /// #53). `None` uses the sensible default (see [`default_tier_for`]).
    pub tier: Option<ModelTier>,
    /// An exact model alias for this role, the escape hatch that pins the build
    /// precisely and wins over any tier. `None` (the usual case) lets the
    /// role's [`tier`] decide.
    ///
    /// [`tier`]: RoleSpec::tier
    pub model: Option<String>,
    /// The role's token cap: the ceiling on its own spend (issue #54). `None`
    /// leaves the role bounded only by the crew-wide
    /// [`token_budget`](CrewConfig::token_budget). When the role reaches
    /// its cap, the supervisor idle-stops it rather than overrun.
    pub token_cap: Option<u64>,
    /// The runtime the supervisor spawns this role on (issue #128). Defaults to
    /// [`Claude`](Runtime::Claude); set `runtime = "codex"` to run it as a
    /// Codex agent wired to the CLI shim instead.
    pub runtime: Runtime,
}

impl Default for CrewConfig {
    /// The default crew: commander, backend, frontend, and qa (see
    /// `docs/roles.md`).
    ///
    /// A starting point the operator customizes: the commander routes and owns
    /// no lane, and the specialists own the clean boundaries most repos
    /// hand you.
    fn default() -> Self {
        Self {
            roles: default_roles(),
            commander: RoleId::new(DEFAULT_COMMANDER),
            model: None,
            models: ModelTiers::default(),
            token_budget: None,
            idle_stop: DEFAULT_IDLE_STOP,
            repos: Vec::new(),
            workspace: None,
            worktrees: false,
            lane_enforcement: LaneEnforcement::default(),
        }
    }
}

impl CrewConfig {
    /// Parses a config from its TOML form, applying defaults and validating it.
    ///
    /// Omitted fields take their defaults: no `roles` yields the default crew,
    /// and no `commander` / `model` / `idle_stop` take the crew defaults.
    ///
    /// # Errors
    /// Returns a [`ConfigError`] if the TOML is malformed, an unknown field is
    /// present, the idle-stop duration is unparseable, or validation fails
    /// (an empty or duplicate role, a commander that is not a declared
    /// role, or two roles owning overlapping paths). The message names the
    /// offending value.
    pub fn from_toml(toml: &str) -> Result<Self, ConfigError> {
        let raw: RawConfig =
            toml::from_str(toml).map_err(|source| ConfigError::parse(Box::new(source)))?;
        let config = raw.resolve()?;
        config.validate()?;
        Ok(config)
    }

    /// Produces one [`RoleCard`] per role, reaching the broker at `broker`.
    ///
    /// Each card names the crew's [`commander`](CrewConfig::commander), so
    /// every agent boots knowing where an unaddressed message goes and
    /// whether it is the commander itself. The config is broker-agnostic;
    /// the broker address (where `crewd` listens) is supplied here, so the
    /// same config drives any broker.
    #[must_use]
    pub fn to_cards(&self, broker: &BrokerEndpoint) -> Vec<RoleCard> {
        self.roles
            .iter()
            .map(|spec| {
                RoleCard::new(
                    spec.role.clone(),
                    spec.owned_paths.clone(),
                    spec.acceptance.clone(),
                    broker.clone(),
                )
                .with_commander(self.commander.clone())
                .with_lane_enforcement(self.lane_enforcement)
                .with_runtime(spec.runtime)
            })
            .collect()
    }

    /// The model alias `role` runs, resolved by the tier precedence (issue
    /// #53).
    ///
    /// Most specific wins:
    ///
    /// 1. the role's exact `model` override, if it pins one;
    /// 2. else the role's explicit `tier`, resolved through the crew's tier
    ///    map;
    /// 3. else a crew-wide `model`, if the operator set one for the whole crew;
    /// 4. else the sensible default tier for the role's name (see
    ///    [`default_tier_for`]), resolved through the crew's tier map.
    ///
    /// A `role` not declared in the crew still resolves, through steps 3 and 4,
    /// so the caller always gets a concrete alias.
    #[must_use]
    pub fn model_for(&self, role: &RoleId) -> &str {
        let spec = self.roles.iter().find(|spec| &spec.role == role);
        if let Some(model) = spec.and_then(|spec| spec.model.as_deref()) {
            return model;
        }
        if let Some(tier) = spec.and_then(|spec| spec.tier) {
            return self.models.resolve(tier);
        }
        if let Some(model) = self.model.as_deref() {
            return model;
        }
        self.models.resolve(default_tier_for(role))
    }

    /// The crew's token [`Budget`]: the crew-wide ceiling and each role's cap
    /// (issue #54).
    ///
    /// The supervisor holds one and records spend against it, idle-stopping a
    /// role or the crew when it reaches a cap. A crew with no
    /// `token_budget` and no per-role `token_cap` is unbounded, so the
    /// budget never breaches.
    #[must_use]
    pub fn budget(&self) -> Budget {
        let caps = self
            .roles
            .iter()
            .filter_map(|spec| spec.token_cap.map(|cap| (spec.role.clone(), cap)))
            .collect();
        Budget::new(self.token_budget, caps)
    }

    /// The directory a bare `repos` name resolves under (issue #126).
    ///
    /// The [`workspace`](Self::workspace) field if set (used as-is when
    /// absolute, else joined onto `config_dir`), otherwise `config_dir` itself,
    /// the crew config file's own directory. Anchoring to the config, not the
    /// current directory, is what makes a name mean the same repo wherever
    /// `crew up` runs.
    #[must_use]
    pub fn workspace_root(&self, config_dir: &Path) -> PathBuf {
        match &self.workspace {
            Some(workspace) if workspace.is_absolute() => workspace.clone(),
            Some(workspace) => config_dir.join(workspace),
            None => config_dir.to_path_buf(),
        }
    }

    /// Resolves the crew's `repos` to filesystem paths for worktree isolation
    /// (issue #126).
    ///
    /// Each entry is a path or a name (the `repos` field, see
    /// `docs/config.md`): an absolute path is taken as-is, and anything else, a
    /// bare name like `api` or a relative path like `../api`, resolves under
    /// the [`workspace_root`](Self::workspace_root). Purely a path join, so
    /// a repo that does not exist is caught later, when the worktree is
    /// created, with a clear error rather than here.
    #[must_use]
    pub fn repo_paths(&self, config_dir: &Path) -> Vec<PathBuf> {
        let root = self.workspace_root(config_dir);
        self.repos
            .iter()
            .map(|repo| {
                let path = Path::new(repo);
                if path.is_absolute() {
                    path.to_path_buf()
                } else {
                    root.join(path)
                }
            })
            .collect()
    }

    /// Validates the resolved config, returning the first problem it finds.
    fn validate(&self) -> Result<(), ConfigError> {
        if self.roles.is_empty() {
            return Err(ConfigError::invalid("a crew must have at least one role"));
        }

        // Role ids must be present and unique.
        let mut seen = BTreeSet::new();
        for spec in &self.roles {
            let id = spec.role.as_str();
            if id.trim().is_empty() {
                return Err(ConfigError::invalid("a role's name must not be empty"));
            }
            if !seen.insert(id) {
                return Err(ConfigError::invalid(format!(
                    "the role `{id}` is declared more than once"
                )));
            }
        }

        // The commander must be one of the declared roles.
        if !self.roles.iter().any(|spec| spec.role == self.commander) {
            return Err(ConfigError::invalid(format!(
                "the commander `{}` is not one of the declared roles ({})",
                self.commander,
                self.role_names(),
            )));
        }

        self.check_ownership_overlaps()
    }

    /// Rejects two roles owning overlapping directory boundaries.
    ///
    /// Two lanes overlap when one path is a prefix of the other (or they are
    /// equal), so `api/` and `api/routes/` collide but `api/` and `apiv2/`
    /// do not.
    fn check_ownership_overlaps(&self) -> Result<(), ConfigError> {
        // Each owned lane, with the role and the path as written for the message.
        let mut lanes: Vec<(&RoleId, &str, String)> = Vec::new();
        for spec in &self.roles {
            for path in &spec.owned_paths {
                let boundary = directory_boundary(path);
                if boundary.is_empty() {
                    continue;
                }
                for (other_role, other_path, other_boundary) in &lanes {
                    if *other_role != &spec.role && boundaries_overlap(&boundary, other_boundary) {
                        return Err(ConfigError::invalid(format!(
                            "roles `{}` and `{other_role}` own overlapping paths (`{}` and `{other_path}`)",
                            spec.role,
                            path.trim(),
                        )));
                    }
                }
                lanes.push((&spec.role, path.trim(), boundary));
            }
        }
        Ok(())
    }

    /// The declared role names, joined for an error message.
    fn role_names(&self) -> String {
        self.roles
            .iter()
            .map(|spec| spec.role.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    }
}

/// The default crew's roles: the commander routes, the specialists own their
/// lanes.
fn default_roles() -> Vec<RoleSpec> {
    let role = |name: &str, paths: &[&str]| RoleSpec {
        role: RoleId::new(name),
        owned_paths: paths.iter().map(|path| (*path).to_owned()).collect(),
        acceptance: String::new(),
        tier: None,
        model: None,
        token_cap: None,
        runtime: Runtime::default(),
    };
    vec![
        role("commander", &[]),
        role("backend", &["api/"]),
        role("frontend", &["frontend/"]),
        role("qa", &["tests/"]),
    ]
}

/// Normalizes a path to a directory boundary: trimmed, with exactly one
/// trailing slash.
///
/// So `api`, `api/`, and `api//` all become `api/`, and a blank path becomes
/// empty.
fn directory_boundary(path: &str) -> String {
    let trimmed = path.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return String::new();
    }
    format!("{trimmed}/")
}

/// Whether two directory boundaries overlap: equal, or one nested under the
/// other.
///
/// Both end with `/`, so a prefix test is exactly the nesting test: `api/` is a
/// prefix of `api/routes/`, but not of `apiv2/`.
fn boundaries_overlap(a: &str, b: &str) -> bool {
    a.starts_with(b) || b.starts_with(a)
}

/// Parses a human duration: a plain number of seconds, or a number with an
/// `s`/`m`/`h` suffix (`30s`, `5m`, `2h`).
fn parse_duration(text: &str) -> Result<Duration, String> {
    let text = text.trim();
    let expected = "expected a duration like `5m`, `30s`, `2h`, or a number of seconds";
    if text.is_empty() {
        return Err(expected.to_owned());
    }
    if text.chars().all(|c| c.is_ascii_digit()) {
        let seconds = text.parse().map_err(|_error| expected.to_owned())?;
        return Ok(Duration::from_secs(seconds));
    }

    let (number, unit) = text.split_at(text.len() - 1);
    let count: u64 = number
        .trim()
        .parse()
        .map_err(|_error| format!("expected a number before the unit; {expected}"))?;
    let seconds = match unit {
        "s" => count,
        "m" => count * 60,
        "h" => count * 3600,
        other => return Err(format!("unknown time unit `{other}`; use s, m, or h")),
    };
    Ok(Duration::from_secs(seconds))
}

/// The TOML wire form of a crew config: every field optional, so a default
/// applies.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    model: Option<String>,
    models: Option<RawModels>,
    token_budget: Option<u64>,
    commander: Option<String>,
    idle_stop: Option<String>,
    repos: Option<Vec<String>>,
    workspace: Option<PathBuf>,
    worktrees: Option<bool>,
    lane_enforcement: Option<LaneEnforcement>,
    roles: Option<Vec<RawRole>>,
}

/// The TOML wire form of the tier map: the alias each tier resolves to (issue
/// #53).
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawModels {
    strong: Option<String>,
    standard: Option<String>,
    cheap: Option<String>,
}

/// The TOML wire form of one role.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawRole {
    role: String,
    owned_paths: Option<Vec<String>>,
    acceptance: Option<String>,
    tier: Option<ModelTier>,
    model: Option<String>,
    token_cap: Option<u64>,
    runtime: Option<Runtime>,
}

impl RawConfig {
    /// Applies defaults, parsing the idle-stop duration.
    fn resolve(self) -> Result<CrewConfig, ConfigError> {
        let idle_stop = match self.idle_stop {
            Some(text) => parse_duration(&text).map_err(|reason| {
                ConfigError::invalid(format!("could not parse idle_stop `{text}`: {reason}"))
            })?,
            None => DEFAULT_IDLE_STOP,
        };
        let roles = match self.roles {
            Some(roles) => roles.into_iter().map(RawRole::resolve).collect(),
            None => default_roles(),
        };
        Ok(CrewConfig {
            roles,
            commander: RoleId::new(self.commander.map_or_else(
                || DEFAULT_COMMANDER.to_owned(),
                |name| name.trim().to_owned(),
            )),
            model: normalize_alias(self.model),
            models: self.models.map(RawModels::resolve).unwrap_or_default(),
            token_budget: self.token_budget,
            idle_stop,
            repos: self.repos.unwrap_or_default(),
            workspace: self.workspace,
            worktrees: self.worktrees.unwrap_or(false),
            lane_enforcement: self.lane_enforcement.unwrap_or_default(),
        })
    }
}

impl RawModels {
    /// Resolves the tier map, each tier falling back to its default alias.
    fn resolve(self) -> ModelTiers {
        ModelTiers::from_overrides(self.strong, self.standard, self.cheap)
    }
}

impl RawRole {
    /// Applies per-role defaults.
    fn resolve(self) -> RoleSpec {
        RoleSpec {
            role: RoleId::new(self.role.trim()),
            owned_paths: self.owned_paths.unwrap_or_default(),
            acceptance: self.acceptance.unwrap_or_default(),
            tier: self.tier,
            model: normalize_alias(self.model),
            token_cap: self.token_cap,
            runtime: self.runtime.unwrap_or_default(),
        }
    }
}

/// Trims a model alias, treating a blank or whitespace-only value as absent.
fn normalize_alias(alias: Option<String>) -> Option<String> {
    alias
        .map(|alias| alias.trim().to_owned())
        .filter(|alias| !alias.is_empty())
}

/// The error returned when a crew config cannot be parsed or is invalid.
///
/// Inspect it with [`is_parse`](ConfigError::is_parse) and
/// [`is_invalid`](ConfigError::is_invalid); its [`Display`] carries the precise
/// reason.
#[derive(Debug)]
pub struct ConfigError {
    kind: ErrorKind,
    backtrace: Backtrace,
}

impl ConfigError {
    /// Wraps a kind, capturing a backtrace (empty unless `RUST_BACKTRACE` is
    /// set).
    fn new(kind: ErrorKind) -> Self {
        Self {
            kind,
            backtrace: Backtrace::capture(),
        }
    }

    /// A malformed-TOML (or unknown-field) parse error.
    fn parse(source: Box<toml::de::Error>) -> Self {
        Self::new(ErrorKind::Parse(source))
    }

    /// A validation error carrying a precise, human-readable reason.
    fn invalid(reason: impl Into<String>) -> Self {
        Self::new(ErrorKind::Invalid(reason.into()))
    }

    /// Whether the config text was malformed and could not be parsed.
    #[must_use]
    pub fn is_parse(&self) -> bool {
        matches!(self.kind, ErrorKind::Parse(_))
    }

    /// Whether the config parsed but failed validation.
    #[must_use]
    pub fn is_invalid(&self) -> bool {
        matches!(self.kind, ErrorKind::Invalid(_))
    }
}

impl Display for ConfigError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.kind {
            ErrorKind::Parse(source) => write!(f, "could not parse the crew config: {source}")?,
            ErrorKind::Invalid(reason) => write!(f, "invalid crew config: {reason}")?,
        }
        if let std::backtrace::BacktraceStatus::Captured = self.backtrace.status() {
            write!(f, "\n{}", self.backtrace)?;
        }
        Ok(())
    }
}

impl std::error::Error for ConfigError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match &self.kind {
            ErrorKind::Parse(source) => Some(&**source),
            ErrorKind::Invalid(_) => None,
        }
    }
}

/// What went wrong with a config. Kept private so new failure modes never break
/// the public API (callers match on the `is_*` methods and read the `Display`).
#[derive(Debug)]
enum ErrorKind {
    Parse(Box<toml::de::Error>),
    Invalid(String),
}

#[cfg(test)]
mod tests {
    use std::path::{Path, PathBuf};

    use super::{parse_duration, CrewConfig, Runtime};
    use crate::{BrokerEndpoint, RoleId};

    #[test]
    fn a_role_runtime_defaults_to_claude_and_parses_codex() {
        let config = CrewConfig::from_toml(
            "commander = \"backend\"\n\n\
             [[roles]]\nrole = \"backend\"\n\n\
             [[roles]]\nrole = \"scout\"\nruntime = \"codex\"",
        )
        .expect("a per-role runtime is valid config");
        let runtime = |name: &str| {
            config
                .roles
                .iter()
                .find(|role| role.role == RoleId::new(name))
                .unwrap()
                .runtime
        };
        assert_eq!(
            runtime("backend"),
            Runtime::Claude,
            "runtime defaults to claude"
        );
        assert_eq!(
            runtime("scout"),
            Runtime::Codex,
            "runtime = \"codex\" parses"
        );

        // to_cards carries the runtime onto each card, so the spawn path can branch.
        let cards = config.to_cards(&BrokerEndpoint::new("127.0.0.1", 2739));
        let scout = cards
            .iter()
            .find(|card| card.role == RoleId::new("scout"))
            .unwrap();
        assert_eq!(
            scout.runtime,
            Runtime::Codex,
            "the card carries the codex runtime"
        );
    }

    /// A fully specified config exercising every field, mirrored in
    /// `docs/config.md`.
    const DOCUMENTED: &str = r#"
        commander = "commander"
        idle_stop = "10m"
        repos = ["api", "web"]
        worktrees = true
        lane_enforcement = "block"

        [models]
        strong = "opus"
        standard = "sonnet"
        cheap = "haiku"

        [[roles]]
        role = "commander"
        owned_paths = []

        [[roles]]
        role = "backend"
        owned_paths = ["api/", "db/"]
        acceptance = "Tests green, migrations reversible."
        model = "haiku"

        [[roles]]
        role = "frontend"
        owned_paths = ["web/"]

        [[roles]]
        role = "qa"
        owned_paths = ["tests/"]
        tier = "cheap"
    "#;

    #[test]
    fn the_documented_config_produces_a_valid_crew() {
        let config = CrewConfig::from_toml(DOCUMENTED).expect("the documented config is valid");
        assert_eq!(config.roles.len(), 4);
        assert_eq!(config.commander, RoleId::new("commander"));
        assert_eq!(config.model, None, "no crew-wide model override");
        assert_eq!(config.idle_stop.as_secs(), 10 * 60);
        assert_eq!(config.repos, ["api", "web"]);
        assert!(config.worktrees, "worktree isolation is opted in");
        assert_eq!(config.lane_enforcement, super::LaneEnforcement::Block);

        // Each precedence level resolves as expected: an exact model pins the build, an
        // explicit tier and the default-by-name tier both resolve through the tier map.
        assert_eq!(
            config.model_for(&RoleId::new("backend")),
            "haiku",
            "exact model"
        );
        assert_eq!(
            config.model_for(&RoleId::new("qa")),
            "haiku",
            "explicit cheap tier"
        );
        assert_eq!(
            config.model_for(&RoleId::new("commander")),
            "opus",
            "default strong tier"
        );
        assert_eq!(
            config.model_for(&RoleId::new("frontend")),
            "sonnet",
            "default standard tier"
        );

        // It produces one role card per role, reaching the given broker.
        let cards = config.to_cards(&BrokerEndpoint::new("127.0.0.1", 2739));
        assert_eq!(cards.len(), 4);
        let backend = cards
            .iter()
            .find(|c| c.role == RoleId::new("backend"))
            .unwrap();
        assert_eq!(backend.owned_paths, ["api/", "db/"]);
        assert_eq!(backend.broker.base_url(), "http://127.0.0.1:2739");

        // Every card names the crew's commander, so each agent boots knowing the hub.
        assert!(
            cards
                .iter()
                .all(|c| c.commander == RoleId::new("commander")),
            "each card carries the crew's commander",
        );
        let commander = cards
            .iter()
            .find(|c| c.role == RoleId::new("commander"))
            .unwrap();
        assert!(
            commander.is_commander(),
            "the commander card knows it leads"
        );
        assert!(!backend.is_commander(), "a specialist card does not");
    }

    #[test]
    fn repo_names_resolve_under_the_config_directory_by_default() {
        // A bare name anchors to the config file's own directory, so it means the
        // same repo wherever `crew up` runs, not just from that directory (#126).
        let config = CrewConfig::from_toml("repos = [\"api\", \"web\"]").unwrap();
        assert_eq!(
            config.repo_paths(Path::new("/workspace")),
            [
                PathBuf::from("/workspace/api"),
                PathBuf::from("/workspace/web")
            ],
        );
    }

    #[test]
    fn absolute_and_relative_repo_entries_are_paths_not_names() {
        let config =
            CrewConfig::from_toml("repos = [\"/opt/api\", \"../sibling\", \"nested/web\"]")
                .unwrap();
        assert_eq!(
            config.repo_paths(Path::new("/workspace/crew")),
            [
                // Absolute: taken as-is, ignoring the workspace root.
                PathBuf::from("/opt/api"),
                // Relative: joined onto the workspace root (the config dir here).
                PathBuf::from("/workspace/crew/../sibling"),
                PathBuf::from("/workspace/crew/nested/web"),
            ],
        );
    }

    #[test]
    fn the_workspace_field_overrides_the_root_for_named_repos() {
        // A relative `workspace` is anchored to the config dir: `..` points a
        // config that lives inside a repo at the surrounding clones.
        let relative = CrewConfig::from_toml("repos = [\"api\"]\nworkspace = \"..\"").unwrap();
        assert_eq!(
            relative.repo_paths(Path::new("/workspace/crew")),
            [PathBuf::from("/workspace/crew/../api")],
        );

        // An absolute `workspace` is the root outright; an absolute repo entry
        // still ignores it.
        let absolute =
            CrewConfig::from_toml("repos = [\"api\", \"/opt/x\"]\nworkspace = \"/clones\"")
                .unwrap();
        assert_eq!(
            absolute.repo_paths(Path::new("/anywhere")),
            [PathBuf::from("/clones/api"), PathBuf::from("/opt/x")],
        );
    }

    #[test]
    fn the_default_crew_is_valid_and_complete() {
        let config = CrewConfig::default();
        assert_eq!(
            config
                .roles
                .iter()
                .map(|spec| spec.role.as_str())
                .collect::<Vec<_>>(),
            ["commander", "backend", "frontend", "qa"],
        );
        assert_eq!(config.commander, RoleId::new("commander"));
        assert_eq!(config.model, None, "no crew-wide model override by default");
        // The sensible default mapping: the lead runs strong, the builders run standard
        // (issue #53), with no per-role config.
        assert_eq!(config.model_for(&RoleId::new("commander")), "opus");
        assert_eq!(config.model_for(&RoleId::new("backend")), "sonnet");
        assert_eq!(config.model_for(&RoleId::new("frontend")), "sonnet");
        assert_eq!(config.model_for(&RoleId::new("qa")), "sonnet");
        assert_eq!(config.idle_stop.as_secs(), 5 * 60);
        assert!(config.repos.is_empty());
        assert!(!config.worktrees, "worktree isolation is off by default");
        assert_eq!(
            config.lane_enforcement,
            super::LaneEnforcement::Warn,
            "lane enforcement defaults to warn"
        );
        // The default crew round-trips through validation via an empty config document.
        assert_eq!(CrewConfig::from_toml("").unwrap(), config);
    }

    #[test]
    fn omitted_roles_yield_the_default_crew_with_overrides_applied() {
        let config = CrewConfig::from_toml("model = \"haiku\"\nidle_stop = \"90s\"").unwrap();
        assert_eq!(config.roles.len(), 4, "roles default to the standard crew");
        assert_eq!(config.model.as_deref(), Some("haiku"));
        assert_eq!(config.idle_stop.as_secs(), 90);
        // A crew-wide model runs every un-tiered role, overriding the default mapping.
        assert_eq!(config.model_for(&RoleId::new("commander")), "haiku");
        assert_eq!(config.model_for(&RoleId::new("backend")), "haiku");
    }

    #[test]
    fn the_tier_map_retunes_spend_without_touching_roles() {
        // Remapping a tier alias changes what every role on that tier runs (issue #53):
        // bumping the cheap tier to sonnet moves the docs role up with no per-role
        // edit.
        let config = CrewConfig::from_toml(
            "[models]\ncheap = \"sonnet\"\n\
             [[roles]]\nrole = \"commander\"\n[[roles]]\nrole = \"docs\"",
        )
        .unwrap();
        assert_eq!(
            config.model_for(&RoleId::new("docs")),
            "sonnet",
            "the remapped cheap tier"
        );
        assert_eq!(
            config.model_for(&RoleId::new("commander")),
            "opus",
            "the strong tier keeps its default alias"
        );
    }

    #[test]
    fn a_per_role_tier_overrides_the_default_mapping() {
        // The commander defaults to strong; an explicit cheap tier overrides that.
        let config = CrewConfig::from_toml(
            "[[roles]]\nrole = \"commander\"\ntier = \"cheap\"\n[[roles]]\nrole = \"backend\"",
        )
        .unwrap();
        assert_eq!(config.model_for(&RoleId::new("commander")), "haiku");
        assert_eq!(config.model_for(&RoleId::new("backend")), "sonnet");
    }

    #[test]
    fn idle_stop_parses_seconds_minutes_and_hours() {
        assert_eq!(parse_duration("30s").unwrap().as_secs(), 30);
        assert_eq!(parse_duration("5m").unwrap().as_secs(), 300);
        assert_eq!(parse_duration("2h").unwrap().as_secs(), 7200);
        assert_eq!(parse_duration("300").unwrap().as_secs(), 300);
        // An unknown unit, non-numeric text, and a blank string are all rejected.
        parse_duration("5x").unwrap_err();
        parse_duration("abc").unwrap_err();
        parse_duration("").unwrap_err();
    }

    /// Parses a config expected to be invalid, returning the precise message.
    fn invalid(toml: &str) -> String {
        let error = CrewConfig::from_toml(toml).expect_err("should be invalid");
        assert!(error.is_invalid(), "expected a validation error: {error}");
        error.to_string()
    }

    #[test]
    fn an_unknown_commander_fails_with_a_precise_message() {
        let message = invalid(
            "commander = \"lead\"\n\
             [[roles]]\nrole = \"backend\"\nowned_paths = [\"api/\"]",
        );
        assert!(message.contains("commander `lead`"), "{message}");
        assert!(
            message.contains("backend"),
            "it lists the declared roles: {message}"
        );
    }

    #[test]
    fn overlapping_ownership_fails_naming_both_roles_and_paths() {
        // Nested lanes overlap: `api/` contains `api/routes/`.
        let nested = invalid(
            "commander = \"backend\"\n\
             [[roles]]\nrole = \"backend\"\nowned_paths = [\"api/\"]\n\
             [[roles]]\nrole = \"frontend\"\nowned_paths = [\"api/routes/\"]",
        );
        assert!(
            nested.contains("backend") && nested.contains("frontend"),
            "{nested}"
        );
        assert!(
            nested.contains("api/") && nested.contains("api/routes/"),
            "{nested}"
        );

        // Identical lanes overlap too, even when written differently (`api` vs `api/`).
        let identical = invalid(
            "commander = \"backend\"\n\
             [[roles]]\nrole = \"backend\"\nowned_paths = [\"api\"]\n\
             [[roles]]\nrole = \"qa\"\nowned_paths = [\"api/\"]",
        );
        assert!(identical.contains("overlapping"), "{identical}");
    }

    #[test]
    fn sibling_lanes_that_only_share_a_prefix_do_not_overlap() {
        // `api/` and `apiv2/` share a string prefix but are distinct directories.
        let config = CrewConfig::from_toml(
            "commander = \"backend\"\n\
             [[roles]]\nrole = \"backend\"\nowned_paths = [\"api/\"]\n\
             [[roles]]\nrole = \"frontend\"\nowned_paths = [\"apiv2/\"]",
        );
        assert!(
            config.is_ok(),
            "sibling directories are not an overlap: {config:?}"
        );
    }

    #[test]
    fn a_duplicate_or_empty_role_is_rejected() {
        let duplicate = invalid(
            "commander = \"backend\"\n\
             [[roles]]\nrole = \"backend\"\n[[roles]]\nrole = \"backend\"",
        );
        assert!(duplicate.contains("more than once"), "{duplicate}");

        let empty = invalid("commander = \"backend\"\n[[roles]]\nrole = \"  \"");
        assert!(empty.contains("must not be empty"), "{empty}");
    }

    #[test]
    fn a_bad_idle_stop_or_unknown_field_fails_precisely() {
        let bad_duration = CrewConfig::from_toml("idle_stop = \"soon\"").expect_err("invalid");
        assert!(bad_duration.is_invalid());
        assert!(
            bad_duration.to_string().contains("idle_stop `soon`"),
            "{bad_duration}",
        );

        // A typo'd field is caught at parse time, naming the unknown field.
        let typo = CrewConfig::from_toml("modle = \"opus\"").expect_err("unknown field");
        assert!(typo.is_parse(), "unknown field is a parse error: {typo}");
        assert!(typo.to_string().contains("modle"), "{typo}");
    }
}
