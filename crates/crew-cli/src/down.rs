//! `crew down`: stand the running crew down gracefully (issue #26).
//!
//! `crew up` runs the crew in the foreground, so standing it down is a matter of
//! signaling that process: `crew down` reads the pidfile and sends `SIGTERM`, which
//! `crew up` handles by stopping every agent, deregistering it, and draining the broker
//! it started. The graceful shutdown itself lives in `crew up`, so `crew down` is a
//! thin, reliable trigger with a single source of truth for how a unit stands down.

use crew_substrate::broker::Config as BrokerConfig;
use eyre::{eyre, Result, WrapErr};
use tracing::{event, Level};

use crate::paths::pidfile;

/// Signals the running crew to stand down.
///
/// # Errors
/// Returns an error if no crew is running (no pidfile), the pidfile is unreadable or
/// malformed, or the process could not be signaled.
pub fn run() -> Result<()> {
    let broker_config = BrokerConfig::from_env()?;
    let pidfile = pidfile(&broker_config);

    if !pidfile.exists() {
        return Err(eyre!(
            "no crew is running (no pidfile at {})",
            pidfile.display()
        ));
    }

    let text = std::fs::read_to_string(&pidfile)
        .wrap_err_with(|| format!("could not read pidfile {}", pidfile.display()))?;
    let pid: u32 = text
        .trim()
        .parse()
        .wrap_err_with(|| format!("the pidfile {} does not hold a PID", pidfile.display()))?;

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

/// Sends `SIGTERM` to `pid`, so the `crew up` process runs its graceful shutdown.
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
