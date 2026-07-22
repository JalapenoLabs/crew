//! The broker's runtime configuration.

use std::env;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use eyre::{Result, WrapErr};

/// The default port `crewd` listens on (2739 spells `crew` on a phone keypad).
pub const DEFAULT_PORT: u16 = 2739;

/// The default on-disk state directory, relative to the working directory.
pub const DEFAULT_STATE_DIR: &str = ".crew";

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
}

impl Default for Config {
    fn default() -> Self {
        Self {
            host: IpAddr::V4(Ipv4Addr::LOCALHOST),
            port: DEFAULT_PORT,
            state_dir: PathBuf::from(DEFAULT_STATE_DIR),
            allow_non_local: false,
        }
    }
}

impl Config {
    /// Builds a [`Config`] from the environment, falling back to the defaults.
    ///
    /// Reads `CREW_BROKER_HOST`, `CREW_BROKER_PORT`, `CREW_BROKER_STATE_DIR`, and
    /// `CREW_BROKER_ALLOW_NON_LOCAL` (`1`, `true`, or `yes` enable it).
    ///
    /// # Errors
    /// Returns an error if `CREW_BROKER_HOST` is not a valid IP address or
    /// `CREW_BROKER_PORT` is not a valid port number.
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
/// A loopback address is always allowed; any network-reachable address is refused
/// unless `allow_non_local` opts in, so the broker never exposes itself by accident.
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
