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
//! This is operational telemetry (how the process is behaving). It is distinct
//! from the crew message event model, the typed inter-agent stream that lives
//! in `crew-core` (see `docs/observability.md`).

use std::io::IsTerminal;

use tracing::{event, Level};
use tracing_subscriber::EnvFilter;

/// The level filter used when `RUST_LOG` is unset.
const DEFAULT_FILTER: &str = "info";

/// Initializes global structured logging for a crew binary.
///
/// Reads the level filter from `RUST_LOG` (falling back to [`DEFAULT_FILTER`]),
/// writes human-readable events with their named fields to stderr, and colors
/// the output only when stderr is a terminal. Call it once, early in `main`,
/// before any other work.
///
/// Calling it more than once is a no-op: the first call wins (the global
/// subscriber can only be set once) and any later call logs a warning rather
/// than panicking, so a stray second call never takes down the process.
pub fn init() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_FILTER));

    let already_initialized = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .with_ansi(std::io::stderr().is_terminal())
        .try_init()
        .is_err();

    if already_initialized {
        event!(
            name: "telemetry.init.duplicate",
            Level::WARN,
            "logging was already initialized; ignoring this call",
        );
    }
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
    use super::redact;

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
