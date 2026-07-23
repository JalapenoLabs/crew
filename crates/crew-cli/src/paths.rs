//! Where `crew up` and `crew down` rendezvous.
//!
//! `crew up` writes its PID to a file under the broker's state directory; `crew down`
//! reads it to find and signal the running unit. Both derive the path from the same
//! broker config, so they always agree.

use std::path::PathBuf;

use crew_substrate::broker::Config as BrokerConfig;

/// The pidfile name, kept under the broker's state directory (`.crew/` by default).
const PIDFILE: &str = "crew.pid";

/// The pidfile path for the broker described by `config`.
pub fn pidfile(config: &BrokerConfig) -> PathBuf {
    config.state_dir.join(PIDFILE)
}
