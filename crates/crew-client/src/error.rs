//! The canonical error the broker client returns.
//!
//! [`Broker`](crate::Broker)'s methods return an [`Error`] rather than a bare
//! `String`, so the client reads as a library (`M-ERRORS-CANONICAL-STRUCTS`): a
//! caller branches on the cause through the `is_*` accessors (an unreachable
//! broker, a rejected request, a malformed response, an invalid argument)
//! instead of matching on message text. The human message is preserved in
//! [`Display`], so a front-end still surfaces the broker's own reason verbatim.

use std::{
    backtrace::Backtrace,
    error::Error as StdError,
    fmt::{self, Display, Formatter},
};

/// An error from a broker call.
///
/// Carries a [`Backtrace`] captured at the failure. The upstream cause (a
/// `ureq` transport failure, the broker's 4xx/5xx reason, a JSON parse error)
/// is rendered into the message rather than kept as a typed source, since the
/// client is a thin HTTP boundary and the message is what a front-end shows.
#[derive(Debug)]
pub struct Error {
    kind: ErrorKind,
    backtrace: Backtrace,
}

/// The specific failure behind an [`Error`], kept private so a new cause is not
/// a breaking change (callers use the `is_*` accessors).
#[derive(Debug)]
enum ErrorKind {
    /// The broker could not be reached (a transport failure).
    Unreachable(String),
    /// The broker answered with an error status; the message is its reason.
    Rejected {
        status: Option<u16>,
        message: String,
    },
    /// The broker's response could not be read or parsed.
    Malformed(String),
    /// The call's arguments were invalid before any request went out (an
    /// unroutable target, an unknown artifact kind, a bad id).
    Invalid(String),
}

impl Error {
    fn new(kind: ErrorKind) -> Self {
        Self {
            kind,
            backtrace: Backtrace::capture(),
        }
    }

    /// The broker could not be reached; `message` names the base and cause.
    pub(crate) fn unreachable(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Unreachable(message.into()))
    }

    /// The broker rejected the request with `status`, carrying its `message`.
    pub(crate) fn rejected(status: Option<u16>, message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Rejected {
            status,
            message: message.into(),
        })
    }

    /// The broker's response could not be read or parsed.
    pub(crate) fn malformed(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Malformed(message.into()))
    }

    /// The call's arguments were invalid before any request went out.
    pub(crate) fn invalid(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Invalid(message.into()))
    }

    /// The backtrace captured when the error was created.
    ///
    /// Empty unless backtrace capture is enabled (for example
    /// `RUST_BACKTRACE=1`).
    pub fn backtrace(&self) -> &Backtrace {
        &self.backtrace
    }

    /// The broker's HTTP status, when this is a rejection; `None` otherwise.
    #[must_use]
    pub fn status(&self) -> Option<u16> {
        match self.kind {
            ErrorKind::Rejected { status, .. } => status,
            _ => None,
        }
    }

    /// Whether the broker could not be reached (is it running?).
    #[must_use]
    pub fn is_unreachable(&self) -> bool {
        matches!(self.kind, ErrorKind::Unreachable(_))
    }

    /// Whether the broker answered with an error status, for example refusing a
    /// claim another role holds.
    #[must_use]
    pub fn is_rejected(&self) -> bool {
        matches!(self.kind, ErrorKind::Rejected { .. })
    }

    /// Whether the broker's response could not be read or parsed.
    #[must_use]
    pub fn is_malformed(&self) -> bool {
        matches!(self.kind, ErrorKind::Malformed(_))
    }

    /// Whether the call's arguments were invalid before any request went out.
    #[must_use]
    pub fn is_invalid(&self) -> bool {
        matches!(self.kind, ErrorKind::Invalid(_))
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.kind {
            ErrorKind::Unreachable(message)
            | ErrorKind::Malformed(message)
            | ErrorKind::Invalid(message)
            | ErrorKind::Rejected { message, .. } => f.write_str(message),
        }
    }
}

impl StdError for Error {}
