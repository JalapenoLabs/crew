//! Shared operational logging for the crew binaries.
//!
//! One `tracing` init the broker (`crewd`), the supervisor, and the CLI
//! (`crew`) all call once at startup, so every binary emits the same
//! structured, named events. Events follow the
//! `<component>.<operation>.<state>` naming and the message-template rules from
//! `~/.claude/docs/rust.md` (M-LOG-STRUCTURED):
//!
//! ```ignore
//! use tracing::{event, Level};
//!
//! event!(
//!     name: "broker.message.routed",
//!     Level::INFO,
//!     crew.channel = channel,
//!     crew.message.id = id,
//!     "routed {{crew.message.id}} on {{crew.channel}}",
//! );
//! ```
//!
//! The output is human-readable by default, for development; setting
//! `CREW_LOG_FORMAT=json` switches to one JSON object per event, so a
//! daemonized crewd feeds machine-parseable logs into aggregation (issue #213).
//!
//! This is operational telemetry (how the process is behaving). It is distinct
//! from the crew message event model, the typed inter-agent stream that lives
//! in `crew-core` (see `docs/observability.md`).

use std::io::IsTerminal;

use tracing::{event, Level};
use tracing_subscriber::EnvFilter;

/// The level filter used when `RUST_LOG` is unset.
const DEFAULT_FILTER: &str = "info";

/// The env var that selects the log output format (issue #213).
///
/// Set `CREW_LOG_FORMAT=json` (case-insensitive) for machine-parseable output:
/// one JSON object per event carrying its level, target, timestamp, and named
/// fields, so structured events ingest cleanly into log aggregation once crewd
/// runs as a daemon. Any other value, or unset, keeps the human-readable
/// default that suits development.
const LOG_FORMAT_ENV: &str = "CREW_LOG_FORMAT";

/// Initializes global structured logging for a crew binary.
///
/// Reads the level filter from `RUST_LOG` (falling back to `info`) and the
/// output format from `CREW_LOG_FORMAT`, writing events with
/// their named fields to stderr. The default is human-readable, colored only
/// when stderr is a terminal; `CREW_LOG_FORMAT=json` selects one JSON object
/// per event instead. Call it once, early in `main`, before any other work.
///
/// Calling it more than once is a no-op: the first call wins (the global
/// subscriber can only be set once) and any later call logs a warning rather
/// than panicking, so a stray second call never takes down the process.
pub fn init() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_FILTER));

    let builder = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr);

    // The formatters have different types, so each branch builds and installs its
    // own; only one runs, so `builder` is moved once.
    let already_initialized = if wants_json() {
        // One JSON object per event for log aggregation; ANSI coloring does not
        // apply to JSON, so it is left off.
        builder.json().try_init().is_err()
    } else {
        // Human-readable, colored only when stderr is an interactive terminal.
        builder
            .with_ansi(std::io::stderr().is_terminal())
            .try_init()
            .is_err()
    };

    if already_initialized {
        event!(
            name: "telemetry.init.duplicate",
            Level::WARN,
            "logging was already initialized; ignoring this call",
        );
    }
}

/// Whether the process environment selects JSON output.
fn wants_json() -> bool {
    log_format_is_json(std::env::var(LOG_FORMAT_ENV).ok().as_deref())
}

/// Whether a [`CREW_LOG_FORMAT`](LOG_FORMAT_ENV) value selects JSON.
///
/// Matches `json` case-insensitively, ignoring surrounding whitespace. Anything
/// else, including `None`, keeps the human-readable default, so an unrecognized
/// value never silently disables logging.
fn log_format_is_json(value: Option<&str>) -> bool {
    value.is_some_and(|value| value.trim().eq_ignore_ascii_case("json"))
}

/// Redaction helpers, so secrets never reach the logs (M-LOG-STRUCTURED).
pub mod redact {
    /// Masks a secret so it can appear in a log without leaking its value.
    ///
    /// Returns a fixed marker and never any bytes of `value`, so tokens, auth
    /// keys, and passwords stay out of the logs. Apply it to any field carrying
    /// a secret, and name the field `*.redacted` so the redaction is
    /// obvious:
    ///
    /// ```
    /// use crew_telemetry::redact;
    ///
    /// assert_eq!(redact::secret("sk-ant-super-secret"), "[redacted]");
    /// assert_eq!(redact::secret(""), "[unset]");
    /// ```
    #[must_use]
    pub fn secret(value: &str) -> &'static str {
        if value.is_empty() {
            "[unset]"
        } else {
            "[redacted]"
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        io::Write,
        sync::{Arc, Mutex, PoisonError},
    };

    use tracing::{event, Level};
    use tracing_subscriber::fmt::MakeWriter;

    use super::{log_format_is_json, redact};

    #[test]
    fn log_format_selects_json_only_for_a_case_insensitive_json_value() {
        assert!(log_format_is_json(Some("json")));
        assert!(log_format_is_json(Some("JSON")));
        assert!(
            log_format_is_json(Some("  Json  ")),
            "trimmed and case-insensitive"
        );
        assert!(!log_format_is_json(Some("text")));
        assert!(
            !log_format_is_json(Some("jsonl")),
            "an unrecognized value keeps the human default"
        );
        assert!(!log_format_is_json(Some("")));
        assert!(!log_format_is_json(None), "unset keeps the human default");
    }

    /// A `MakeWriter` that captures everything written into a shared buffer, so
    /// a test can read back what a scoped subscriber emitted.
    #[derive(Clone)]
    struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for CaptureWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for CaptureWriter {
        type Writer = CaptureWriter;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    #[test]
    fn json_format_emits_one_parseable_object_per_event_with_its_named_fields() {
        // The point of the JSON format (issue #213): a machine can parse each
        // event and read its named fields straight out of `fields`. This uses a
        // scoped subscriber (not the global `init`) so it never fights the global
        // logger other tests or the process may have set.
        let buffer = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::fmt()
            .json()
            .with_writer(CaptureWriter(Arc::clone(&buffer)))
            .finish();

        tracing::subscriber::with_default(subscriber, || {
            event!(
                name: "telemetry.test.emitted",
                Level::INFO,
                crew.channel = "all-units",
                "a test event",
            );
        });

        let output = String::from_utf8(
            buffer
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone(),
        )
        .expect("valid UTF-8");
        let line = output.lines().next().expect("one JSON line per event");
        let json: serde_json::Value = serde_json::from_str(line).expect("each event is valid JSON");

        assert_eq!(json["level"], "INFO", "the level is a top-level field");
        assert_eq!(
            json["fields"]["crew.channel"], "all-units",
            "a named field ingests cleanly under `fields`",
        );
        assert_eq!(
            json["fields"]["message"], "a test event",
            "the rendered message is a field too",
        );
        assert!(
            json.get("target").is_some(),
            "the event's target is carried"
        );
    }

    #[test]
    fn redacted_secret_is_a_fixed_marker_independent_of_the_value() {
        // Different non-empty secrets mask to the same fixed marker, so the
        // output cannot encode any byte of the value: nothing to leak.
        assert_eq!(redact::secret("sk-ant-abc123"), "[redacted]");
        assert_eq!(redact::secret("ghp_deadbeef"), "[redacted]");
        assert_eq!(
            redact::secret("sk-ant-abc123"),
            redact::secret("ghp_deadbeef")
        );
    }

    #[test]
    fn redact_distinguishes_set_from_unset() {
        assert_eq!(redact::secret(""), "[unset]");
        assert_ne!(redact::secret("x"), "[unset]");
    }
}
