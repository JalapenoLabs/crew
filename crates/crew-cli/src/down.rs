//! `crew down`: stand the running crew down gracefully (issue #26).
//!
//! `crew up` runs the crew in the foreground, so standing it down is a matter
//! of signaling that process: `crew down` reads the pidfile, verifies the PID
//! still names that `crew up` (via the pidfile's identity marker, so a stale
//! pidfile whose PID was reused is refused rather than SIGTERM-ing an unrelated
//! process, issue #195), and sends `SIGTERM`, which `crew up` handles by
//! stopping every agent, deregistering it, and draining the broker it started.
//! The graceful shutdown itself lives in `crew up`, so `crew down` is a thin,
//! reliable trigger with a single source of truth for how a unit stands down.

use crew_substrate::broker::Config as BrokerConfig;
use eyre::{eyre, Result, WrapErr};
use tracing::{event, Level};

use crate::pidfile;

/// Signals the running crew to stand down.
///
/// # Errors
/// Returns an error if no crew is running (no pidfile, or a stale one whose
/// process is gone or was replaced), the pidfile is unreadable or malformed, or
/// the process could not be signaled.
pub fn run() -> Result<()> {
    let broker_config = BrokerConfig::from_env()?;
    let path = pidfile::path(&broker_config);

    // Resolve the PID only after confirming it still names this crew, so a stale
    // pidfile whose PID was reused never gets an errant SIGTERM (issue #195).
    let pid = pidfile::verified_target(&path)?;

    signal_term(pid)?;
    event!(
        name: "cli.down.signaled",
        Level::INFO,
        crew.pid = pid,
        "asked crew process {{crew.pid}} to stand down",
    );
    println!("Signaled the crew (pid {pid}) to stand down.");
    Ok(())
}

/// Sends `SIGTERM` to `pid`, so the `crew up` process runs its graceful
/// shutdown.
///
/// Shelling out to `kill` keeps this dependency-free; the crew targets Unix.
#[cfg(unix)]
fn signal_term(pid: u32) -> Result<()> {
    let status = std::process::Command::new("kill")
        .arg("-TERM")
        .arg(pid.to_string())
        .status()
        .wrap_err("could not run `kill` to signal the crew")?;
    if !status.success() {
        return Err(eyre!(
            "could not signal crew process {pid}; it may have already stopped"
        ));
    }
    Ok(())
}

#[cfg(not(unix))]
fn signal_term(_pid: u32) -> Result<()> {
    Err(eyre!(
        "`crew down` is supported on Unix only; stop the `crew up` process directly"
    ))
}
