//! The `crew up` / `crew down` rendezvous pidfile, with a process-identity
//! marker so `crew down` never signals a reused, unrelated PID (issue #195).
//!
//! `crew up` writes its PID plus a marker that identifies the specific process
//! instance; `crew down` re-derives the running process's marker and refuses to
//! signal when it does not match, the case where `crew up` crashed without
//! cleanup and its PID was later reused by an unrelated process. Both derive
//! the path from the same broker config, so they always agree.
//!
//! The marker pins the exact process: on Linux it is the boot id plus the
//! process start time (from `/proc`), which together survive neither a reboot
//! nor PID reuse within a boot. On a platform with no portable identity source
//! the marker is empty and `crew down` falls back to signaling by PID alone,
//! the pre-#195 behavior, so this never regresses a non-Linux Unix.

use std::path::{Path, PathBuf};

use crew_substrate::broker::Config as BrokerConfig;
use eyre::{eyre, Result, WrapErr};

/// The pidfile name, kept under the broker's state directory (`.crew/` by
/// default).
const PIDFILE: &str = "crew.pid";

/// The pidfile path for the broker described by `config`.
pub fn path(config: &BrokerConfig) -> PathBuf {
    config.state_dir.join(PIDFILE)
}

/// Writes this process's PID and identity marker to `path`, creating the state
/// directory if needed.
///
/// # Errors
/// Returns an error if the state directory cannot be created or the pidfile
/// cannot be written.
pub fn write(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .wrap_err_with(|| format!("could not create state dir {}", parent.display()))?;
    }
    let pid = std::process::id();
    let record = Record {
        pid,
        marker: process_marker(pid),
    };
    std::fs::write(path, record.render())
        .wrap_err_with(|| format!("could not write pidfile {}", path.display()))
}

/// The PID of the live `crew up` process this pidfile names, verified against
/// its identity marker.
///
/// A verified marker that no longer matches the live process (a crashed `crew
/// up` whose PID was reused) or names a process that is gone marks the pidfile
/// stale: it is removed and an error is returned rather than signaling an
/// unrelated process. A pidfile with no marker (an older one, or one written on
/// a platform with no identity source) falls back to signaling by PID alone.
///
/// # Errors
/// Returns an error if no pidfile exists, it is unreadable or malformed, or it
/// is stale (its process is gone or has been replaced).
pub fn verified_target(path: &Path) -> Result<u32> {
    if !path.exists() {
        return Err(eyre!(
            "no crew is running (no pidfile at {})",
            path.display()
        ));
    }
    let text = std::fs::read_to_string(path)
        .wrap_err_with(|| format!("could not read pidfile {}", path.display()))?;
    let record = Record::parse(&text)
        .ok_or_else(|| eyre!("the pidfile {} does not hold a PID", path.display()))?;

    let live = process_marker(record.pid);
    match classify(&record, live.as_deref()) {
        Decision::Signal(pid) => Ok(pid),
        Decision::Gone => {
            let _ = std::fs::remove_file(path);
            Err(eyre!(
                "no crew is running (process {} is gone; removed the stale pidfile {})",
                record.pid,
                path.display()
            ))
        }
        Decision::Replaced => {
            let _ = std::fs::remove_file(path);
            Err(eyre!(
                "the pidfile named crew process {}, but that PID is now a different process; the \
                 crew is not running (removed the stale pidfile {})",
                record.pid,
                path.display()
            ))
        }
    }
}

/// A parsed pidfile: the `crew up` PID and, when the platform supplied one, the
/// marker pinning that process instance.
#[derive(Debug, PartialEq, Eq)]
struct Record {
    pid: u32,
    /// The identity marker, or `None` when it could not be captured (skipping
    /// verification).
    marker: Option<String>,
}

impl Record {
    /// Renders the pidfile: the PID on the first line, the marker (if any) on
    /// the second.
    fn render(&self) -> String {
        match &self.marker {
            Some(marker) => format!("{}\n{marker}\n", self.pid),
            None => format!("{}\n", self.pid),
        }
    }

    /// Parses a pidfile, tolerating the old single-line PID form (no marker).
    ///
    /// Returns `None` when the first line does not hold a PID.
    fn parse(text: &str) -> Option<Self> {
        let mut lines = text.lines();
        let pid = lines.next()?.trim().parse().ok()?;
        let marker = lines
            .next()
            .map(str::trim)
            .filter(|marker| !marker.is_empty())
            .map(str::to_owned);
        Some(Self { pid, marker })
    }
}

/// What `crew down` should do with a pidfile, given the marker of the process
/// its PID currently names.
#[derive(Debug, PartialEq, Eq)]
enum Decision {
    /// Signal this PID: the marker matched, or there is none to check.
    Signal(u32),
    /// The named process is gone; the pidfile is stale.
    Gone,
    /// The PID now names a different process; the pidfile is stale.
    Replaced,
}

/// Decides what to do with `record`, given `live` (the marker of the process
/// its PID currently names, or `None` if that process is gone).
///
/// With no recorded marker there is nothing to verify, so it signals by PID
/// alone. With a marker, it signals only when the live process matches.
fn classify(record: &Record, live: Option<&str>) -> Decision {
    let Some(marker) = record.marker.as_deref() else {
        return Decision::Signal(record.pid);
    };
    match live {
        Some(live) if live == marker => Decision::Signal(record.pid),
        Some(_) => Decision::Replaced,
        None => Decision::Gone,
    }
}

/// A token identifying the specific process instance behind `pid`, so a reused
/// PID cannot masquerade as the original.
///
/// On Linux it is the boot id plus the process start time (`/proc`): the start
/// time distinguishes a reused PID within one boot, and the boot id catches a
/// pidfile that outlived a reboot. `None` when the source is unreadable or the
/// process is gone; also `None` on a platform with no portable source, where
/// verification is skipped.
#[cfg(target_os = "linux")]
fn process_marker(pid: u32) -> Option<String> {
    let boot_id = std::fs::read_to_string("/proc/sys/kernel/random/boot_id").ok()?;
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let start_time = parse_start_time(&stat)?;
    Some(format!("{}:{start_time}", boot_id.trim()))
}

#[cfg(not(target_os = "linux"))]
fn process_marker(_pid: u32) -> Option<String> {
    None
}

/// The process start time (clock ticks since boot) from a `/proc/<pid>/stat`
/// line: field 22, read after the parenthesized `comm`.
///
/// `comm` is wrapped in parentheses and may itself contain spaces and
/// parentheses (a process can rename itself), so the scan goes to the *last*
/// `)`; the whitespace-separated fields after it are at fixed offsets.
#[cfg(target_os = "linux")]
fn parse_start_time(stat: &str) -> Option<u64> {
    let after_comm = stat.rsplit_once(')')?.1;
    // After the closing paren the fields are state (3), ppid (4), ...; start time
    // is field 22, i.e. index 19 in this zero-based, post-paren list.
    after_comm.split_whitespace().nth(19)?.parse().ok()
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use super::{classify, verified_target, write, Decision, Record};

    /// A unique pidfile path under the system temp dir for one test.
    fn temp_pidfile(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("crew-pidfile-test-{}-{name}", std::process::id()))
    }

    #[test]
    fn write_then_verify_resolves_the_running_process() {
        // `write` stamps this process's marker, so `verified_target` re-derives a
        // matching marker and returns our own PID: the happy path end to end.
        let path = temp_pidfile("roundtrip");
        write(&path).unwrap();
        assert_eq!(verified_target(&path).unwrap(), std::process::id());
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn a_stale_pidfile_naming_a_dead_pid_is_refused_and_removed() {
        // PID 0 is never a live user process, so a pidfile carrying a marker for it
        // resolves to `Gone`: `crew down` refuses to signal and clears the stale
        // file rather than SIGTERM-ing whatever reused the PID (issue #195).
        let path = temp_pidfile("stale");
        std::fs::write(&path, "0\nboot-1234:987654\n").unwrap();
        let error = verified_target(&path).expect_err("a stale pidfile is refused");
        assert!(
            error.to_string().contains("gone"),
            "the error explains the process is gone: {error}",
        );
        assert!(!path.exists(), "the stale pidfile is removed");
    }

    #[test]
    fn an_old_format_pidfile_falls_back_to_signaling_by_pid() {
        // A pidfile written before issue #195 has no marker line; `verified_target`
        // returns its PID unverified, preserving the pre-#195 behavior.
        let path = temp_pidfile("oldformat");
        std::fs::write(&path, "424242\n").unwrap();
        assert_eq!(verified_target(&path).unwrap(), 424_242);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn a_missing_pidfile_reports_no_crew() {
        let path = temp_pidfile("missing");
        let _ = std::fs::remove_file(&path);
        let error = verified_target(&path).expect_err("no pidfile means no crew");
        assert!(
            error.to_string().contains("no crew is running"),
            "the error says no crew is running: {error}",
        );
    }

    #[test]
    fn a_record_round_trips_with_and_without_a_marker() {
        let with = Record {
            pid: 4321,
            marker: Some("boot:99".to_owned()),
        };
        assert_eq!(Record::parse(&with.render()), Some(with));

        let without = Record {
            pid: 4321,
            marker: None,
        };
        assert_eq!(Record::parse(&without.render()), Some(without));
    }

    #[test]
    fn parse_reads_the_old_single_line_pid_form() {
        // A pidfile written before issue #195 held only the PID; it still parses,
        // with no marker, so `crew down` falls back to signaling by PID.
        assert_eq!(
            Record::parse("2468\n"),
            Some(Record {
                pid: 2468,
                marker: None
            })
        );
        assert_eq!(Record::parse("not-a-pid"), None);
    }

    #[test]
    fn a_matching_marker_signals_and_a_mismatch_is_stale() {
        let record = Record {
            pid: 100,
            marker: Some("boot:5".to_owned()),
        };
        assert_eq!(classify(&record, Some("boot:5")), Decision::Signal(100));
        assert_eq!(classify(&record, Some("boot:9")), Decision::Replaced);
        assert_eq!(classify(&record, None), Decision::Gone);
    }

    #[test]
    fn no_marker_always_signals_by_pid() {
        // Without a marker there is nothing to verify (an old pidfile, or a
        // platform with no identity source), so it signals by PID regardless.
        let record = Record {
            pid: 100,
            marker: None,
        };
        assert_eq!(classify(&record, None), Decision::Signal(100));
        assert_eq!(classify(&record, Some("boot:5")), Decision::Signal(100));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn parse_start_time_reads_field_22_past_a_tricky_comm() {
        use super::parse_start_time;

        // A well-behaved comm.
        let stat = "1234 (crew) S 1 1234 1234 0 -1 4194304 100 0 0 0 5 6 0 0 20 0 1 0 987654 0";
        assert_eq!(parse_start_time(stat), Some(987_654));

        // A comm containing spaces and a close-paren must not shift the fields:
        // the scan goes to the last `)`.
        let tricky =
            "1234 (weird )name) S 1 1234 1234 0 -1 4194304 100 0 0 0 5 6 0 0 20 0 1 0 42 0";
        assert_eq!(parse_start_time(tricky), Some(42));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn this_process_has_a_stable_marker_a_dead_pid_does_not() {
        use super::process_marker;

        // The current process has a marker, and it is stable across reads.
        let mine = process_marker(std::process::id()).expect("a live process has a marker");
        assert_eq!(
            process_marker(std::process::id()).as_deref(),
            Some(mine.as_str())
        );

        // PID 0 is never a real user process, so it has no marker: the classic
        // stale-pidfile case resolves to `Gone` rather than a spurious signal.
        assert_eq!(process_marker(0), None);
    }
}
