//! The broker's runtime configuration.

use std::{
    env,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
};

use crew_core::RoleId;
use eyre::{Result, WrapErr};

/// The default port `crewd` listens on (2739 spells `crew` on a phone keypad).
pub const DEFAULT_PORT: u16 = 2739;

/// The default on-disk state directory, relative to the working directory.
pub const DEFAULT_STATE_DIR: &str = ".crew";

/// The default shared-subscription usage percent at which new work auto-pauses
/// (issue #56).
///
/// Ninety percent leaves headroom to finish an in-flight turn before the window
/// is spent, mirroring Seraphim's usage auto-pause. A crew retunes it with
/// `CREW_BROKER_USAGE_THRESHOLD`.
pub const DEFAULT_USAGE_THRESHOLD: u8 = 90;

/// The default crew commander, when none is configured (issue #180).
///
/// Matches [`CrewConfig`](crew_core::CrewConfig)'s own commander default, so a
/// bare `crewd` and the default crew agree on who curates the board. `crew up`
/// overrides it with the crew config's commander.
pub const DEFAULT_COMMANDER: &str = "commander";

/// The broker's runtime configuration.
///
/// Loopback-only by default: [`host`](Config::host) is `127.0.0.1` and
/// [`allow_non_local`](Config::allow_non_local) is `false`, so the broker never
/// exposes itself to the network unless the operator opts in.
///
/// # Examples
/// ```
/// use crew_broker::Config;
/// let config = Config::default();
/// assert!(config.host.is_loopback());
/// assert!(!config.allow_non_local);
/// ```
#[derive(Debug, Clone)]
pub struct Config {
    /// The address to bind. Defaults to `127.0.0.1` (loopback).
    pub host: IpAddr,
    /// The TCP port to listen on. Defaults to [`DEFAULT_PORT`].
    pub port: u16,
    /// The directory the broker keeps its on-disk state in. Defaults to
    /// [`DEFAULT_STATE_DIR`].
    pub state_dir: PathBuf,
    /// Whether to allow binding a non-loopback (network-reachable) address.
    /// Defaults to `false`, so a non-local bind is refused unless opted in.
    pub allow_non_local: bool,
    /// Secret values masked out of every message before it is stored or
    /// streamed. Empty by default; a leaked token never reaches the log or
    /// a subscriber.
    pub secrets: Vec<String>,
    /// The shared-subscription usage percent at which new work auto-pauses
    /// (issue #56). Defaults to
    /// [`DEFAULT_USAGE_THRESHOLD`](crate::DEFAULT_USAGE_THRESHOLD); a value
    /// at or above 100 disables the auto-pause, since a reading never
    /// reaches it.
    pub usage_threshold: u8,
    /// The crew's commander, who may curate the situation board alongside each
    /// entry's author (issue #180). Defaults to
    /// [`DEFAULT_COMMANDER`](crate::DEFAULT_COMMANDER); `crew up` sets it from
    /// the crew config so the real commander is enforced.
    pub commander: RoleId,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: IpAddr::V4(Ipv4Addr::LOCALHOST),
            port: DEFAULT_PORT,
            state_dir: PathBuf::from(DEFAULT_STATE_DIR),
            allow_non_local: false,
            secrets: Vec::new(),
            usage_threshold: DEFAULT_USAGE_THRESHOLD,
            commander: RoleId::new(DEFAULT_COMMANDER),
        }
    }
}

impl Config {
    /// Builds a [`Config`] from the environment, falling back to the defaults.
    ///
    /// Reads `CREW_BROKER_HOST`, `CREW_BROKER_PORT`, `CREW_BROKER_STATE_DIR`,
    /// `CREW_BROKER_ALLOW_NON_LOCAL` (`1`, `true`, or `yes` enable it),
    /// `CREW_BROKER_SECRETS` (a whitespace-separated list of secret values to
    /// mask), `CREW_BROKER_USAGE_THRESHOLD` (the usage percent at which new
    /// work auto-pauses), and `CREW_BROKER_COMMANDER` (the role that may
    /// curate the board, issue #180).
    ///
    /// # Errors
    /// Returns an error if `CREW_BROKER_HOST` is not a valid IP address,
    /// `CREW_BROKER_PORT` is not a valid port number, or
    /// `CREW_BROKER_USAGE_THRESHOLD` is not a percent.
    pub fn from_env() -> Result<Self> {
        let mut config = Self::default();
        if let Ok(host) = env::var("CREW_BROKER_HOST") {
            config.host = host
                .parse()
                .wrap_err("CREW_BROKER_HOST is not a valid IP address")?;
        }
        if let Ok(port) = env::var("CREW_BROKER_PORT") {
            config.port = port
                .parse()
                .wrap_err("CREW_BROKER_PORT is not a valid port number")?;
        }
        if let Ok(dir) = env::var("CREW_BROKER_STATE_DIR") {
            config.state_dir = PathBuf::from(dir);
        }
        if let Ok(flag) = env::var("CREW_BROKER_ALLOW_NON_LOCAL") {
            config.allow_non_local = matches!(flag.as_str(), "1" | "true" | "yes");
        }
        // Tokens carry no internal whitespace, so splitting on it lets one env var
        // hold several secrets without a delimiter that a secret might contain.
        if let Ok(secrets) = env::var("CREW_BROKER_SECRETS") {
            config.secrets = secrets.split_whitespace().map(str::to_owned).collect();
        }
        if let Ok(threshold) = env::var("CREW_BROKER_USAGE_THRESHOLD") {
            config.usage_threshold = threshold
                .parse()
                .wrap_err("CREW_BROKER_USAGE_THRESHOLD is not a percent (0..=100)")?;
        }
        // Only override the default when the value names a role; a blank env var leaves
        // the default commander in place rather than an unnamed one.
        if let Ok(commander) = env::var("CREW_BROKER_COMMANDER") {
            let commander = commander.trim();
            if !commander.is_empty() {
                config.commander = RoleId::new(commander);
            }
        }
        Ok(config)
    }

    /// The socket address the broker binds, from its host and port.
    #[must_use]
    pub fn bind_addr(&self) -> SocketAddr {
        SocketAddr::new(self.host, self.port)
    }
}

/// Whether binding to `ip` is permitted given the non-local opt-in.
///
/// A loopback address is always allowed; any network-reachable address is
/// refused unless `allow_non_local` opts in, so the broker never exposes itself
/// by accident.
#[must_use]
pub(crate) fn is_bind_allowed(ip: IpAddr, allow_non_local: bool) -> bool {
    ip.is_loopback() || allow_non_local
}

#[cfg(test)]
mod tests {
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

    use super::{is_bind_allowed, Config, DEFAULT_PORT};

    #[test]
    fn defaults_are_loopback_only() {
        let config = Config::default();
        assert!(config.host.is_loopback());
        assert_eq!(config.port, DEFAULT_PORT);
        assert!(!config.allow_non_local);
    }

    #[test]
    fn the_default_commander_matches_the_crew_default() {
        // A bare broker and the default crew must agree on who curates the board
        // (issue #180), so board retraction resolves the same commander either way.
        assert_eq!(
            Config::default().commander,
            crew_core::RoleId::new("commander")
        );
    }

    #[test]
    fn loopback_binds_without_opt_in() {
        assert!(is_bind_allowed(IpAddr::V4(Ipv4Addr::LOCALHOST), false));
        assert!(is_bind_allowed(IpAddr::V6(Ipv6Addr::LOCALHOST), false));
    }

    #[test]
    fn non_local_bind_is_refused_unless_opted_in() {
        let network = IpAddr::V4(Ipv4Addr::UNSPECIFIED); // 0.0.0.0, all interfaces
        assert!(
            !is_bind_allowed(network, false),
            "a network-reachable bind must be refused by default",
        );
        assert!(
            is_bind_allowed(network, true),
            "an explicit opt-in allows a non-local bind",
        );
    }
}
