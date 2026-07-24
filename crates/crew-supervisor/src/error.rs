//! The canonical error the supervisor's public entry points return.
//!
//! [`provision`](crate::provision),
//! [`Supervisor::launch`](crate::Supervisor::launch),
//! and the [`RosterClient`](crate::RosterClient) methods return an [`Error`]
//! rather than an application error type, so the supervisor reads as a library
//! (`M-ERRORS-CANONICAL-STRUCTS`): a caller branches on the cause through the
//! `is_*` accessors instead of matching on message text. Internally the crate
//! still composes failures with `eyre`; the boundary wraps them here.

use std::{
    backtrace::Backtrace,
    error::Error as StdError,
    fmt::{self, Display, Formatter},
};

/// An error from provisioning, launching, or a roster interaction.
///
/// Carries a [`Backtrace`] captured at the failure and, for a boundary-wrapped
/// `eyre` failure, the underlying error as its [`source`](StdError::source).
/// The human message is preserved in [`Display`], so a front-end still shows
/// the specific reason.
#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    message: String,
    source: Option<Box<dyn StdError + Send + Sync>>,
    backtrace: Backtrace,
}

/// The area a [`Error`] came from, kept private so a finer split later is not a
/// breaking change (callers use the `is_*` accessors).
#[derive(Debug, Clone, Copy)]
enum ErrorKind {
    /// Writing a role's card to its agent directory failed.
    Provision,
    /// Bringing the crew online failed.
    Launch,
    /// A roster or broker interaction failed (register, mark, report, read).
    Roster,
}

impl Error {
    fn new(
        kind: ErrorKind,
        message: String,
        source: Option<Box<dyn StdError + Send + Sync>>,
    ) -> Self {
        Self {
            kind,
            message,
            source,
            backtrace: Backtrace::capture(),
        }
    }

    /// Provisioning a role's card failed, wrapping the underlying `eyre` error.
    pub(crate) fn provision(source: eyre::Report) -> Self {
        Self::new(
            ErrorKind::Provision,
            source.to_string(),
            Some(source.into()),
        )
    }

    /// Bringing the crew online failed, wrapping the underlying `eyre` error.
    pub(crate) fn launch(source: eyre::Report) -> Self {
        Self::new(ErrorKind::Launch, source.to_string(), Some(source.into()))
    }

    /// A roster or broker interaction failed; `message` names it and the cause.
    pub(crate) fn roster(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Roster, message.into(), None)
    }

    /// The backtrace captured when the error was created.
    ///
    /// Empty unless backtrace capture is enabled (for example
    /// `RUST_BACKTRACE=1`).
    pub fn backtrace(&self) -> &Backtrace {
        &self.backtrace
    }

    /// Whether writing a role's card to its agent directory failed.
    #[must_use]
    pub fn is_provision(&self) -> bool {
        matches!(self.kind, ErrorKind::Provision)
    }

    /// Whether bringing the crew online failed.
    #[must_use]
    pub fn is_launch(&self) -> bool {
        matches!(self.kind, ErrorKind::Launch)
    }

    /// Whether a roster or broker interaction failed (the broker rejected the
    /// call or could not be reached).
    #[must_use]
    pub fn is_roster(&self) -> bool {
        matches!(self.kind, ErrorKind::Roster)
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl StdError for Error {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        self.source
            .as_deref()
            .map(|source| source as &(dyn StdError + 'static))
    }
}

/// Builds a [`Error::roster`] from a format string, the roster methods' one
/// error path (the broker rejected a call or could not be reached).
macro_rules! roster_error {
    ($($arg:tt)*) => {
        $crate::error::Error::roster(format!($($arg)*))
    };
}

pub(crate) use roster_error;
