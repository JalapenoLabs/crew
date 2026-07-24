//! The canonical error the broker's run/serve entry points return.
//!
//! [`run`](crate::run), [`run_until`](crate::run_until), and
//! [`serve`](crate::serve) return a [`ServeError`] rather than an application
//! error type, so the broker reads as a library (`M-ERRORS-CANONICAL-STRUCTS`):
//! an embedder acts on the cause (a refused bind, an unwritable state dir, a
//! busy port) through the `is_*` accessors instead of parsing a string. It is
//! distinct from [`ApiError`](crate::ApiError), which is the per-request
//! 4xx/5xx the HTTP handlers return.

use std::{
    backtrace::Backtrace,
    error::Error as StdError,
    fmt::{self, Display, Formatter},
    io,
    net::SocketAddr,
    path::PathBuf,
};

/// An error from starting or running the broker.
///
/// Names why the broker could not start, or stopped with an error, so an
/// embedder can branch on the cause rather than a message. Carries a
/// [`Backtrace`] captured at the failure and, where one exists, the underlying
/// I/O or storage error as its [`source`](StdError::source).
#[derive(Debug)]
pub struct ServeError {
    kind: ServeErrorKind,
    backtrace: Backtrace,
}

/// The specific failure behind a [`ServeError`], kept private so new causes are
/// not a breaking change (callers use the `is_*` accessors).
#[derive(Debug)]
enum ServeErrorKind {
    /// The configured address is non-loopback and the non-local opt-in is off.
    NonLocalBind(SocketAddr),
    /// The state directory could not be created.
    StateDir { path: PathBuf, source: io::Error },
    /// The durable log could not be opened or replayed.
    Log(Box<dyn StdError + Send + Sync>),
    /// The configured address could not be bound.
    Bind { addr: SocketAddr, source: io::Error },
    /// The bound listener has no local address.
    LocalAddr(io::Error),
    /// The server exited with an error while running.
    Serve(io::Error),
}

impl ServeError {
    fn new(kind: ServeErrorKind) -> Self {
        Self {
            kind,
            backtrace: Backtrace::capture(),
        }
    }

    /// The address was non-loopback without the non-local opt-in.
    pub(crate) fn non_local_bind(addr: SocketAddr) -> Self {
        Self::new(ServeErrorKind::NonLocalBind(addr))
    }

    /// The state directory `path` could not be created.
    pub(crate) fn state_dir(path: PathBuf, source: io::Error) -> Self {
        Self::new(ServeErrorKind::StateDir { path, source })
    }

    /// The durable log could not be opened, wrapping the storage error.
    pub(crate) fn log(source: impl Into<Box<dyn StdError + Send + Sync>>) -> Self {
        Self::new(ServeErrorKind::Log(source.into()))
    }

    /// The address `addr` could not be bound.
    pub(crate) fn bind(addr: SocketAddr, source: io::Error) -> Self {
        Self::new(ServeErrorKind::Bind { addr, source })
    }

    /// The listener had no local address.
    pub(crate) fn local_addr(source: io::Error) -> Self {
        Self::new(ServeErrorKind::LocalAddr(source))
    }

    /// The server exited with an error while running.
    pub(crate) fn serve(source: io::Error) -> Self {
        Self::new(ServeErrorKind::Serve(source))
    }

    /// The backtrace captured when the error was created.
    ///
    /// Empty unless backtrace capture is enabled (for example
    /// `RUST_BACKTRACE=1`), so it costs nothing in the common case.
    pub fn backtrace(&self) -> &Backtrace {
        &self.backtrace
    }

    /// Whether the bind was refused because the address is non-loopback and the
    /// non-local opt-in is off (set `CREW_BROKER_ALLOW_NON_LOCAL`).
    #[must_use]
    pub fn is_bind_refused(&self) -> bool {
        matches!(self.kind, ServeErrorKind::NonLocalBind(_))
    }

    /// Whether the configured address could not be bound (for example the port
    /// is already in use).
    #[must_use]
    pub fn is_bind(&self) -> bool {
        matches!(self.kind, ServeErrorKind::Bind { .. })
    }

    /// Whether the state directory could not be created.
    #[must_use]
    pub fn is_state_dir(&self) -> bool {
        matches!(self.kind, ServeErrorKind::StateDir { .. })
    }

    /// Whether the durable log could not be opened or replayed.
    #[must_use]
    pub fn is_log(&self) -> bool {
        matches!(self.kind, ServeErrorKind::Log(_))
    }

    /// Whether the server exited with an error while running, including having
    /// no local address to serve on.
    #[must_use]
    pub fn is_serve(&self) -> bool {
        matches!(
            self.kind,
            ServeErrorKind::Serve(_) | ServeErrorKind::LocalAddr(_)
        )
    }
}

impl Display for ServeError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match &self.kind {
            ServeErrorKind::NonLocalBind(addr) => write!(
                f,
                "refusing to bind non-loopback address {addr}; set \
                 CREW_BROKER_ALLOW_NON_LOCAL=1 to allow it"
            ),
            ServeErrorKind::StateDir { path, .. } => {
                write!(f, "could not create state dir {}", path.display())
            }
            ServeErrorKind::Log(_) => write!(f, "could not open the durable log"),
            ServeErrorKind::Bind { addr, .. } => write!(f, "could not bind {addr}"),
            ServeErrorKind::LocalAddr(_) => write!(f, "the listener has no local address"),
            ServeErrorKind::Serve(_) => write!(f, "the crewd server exited with an error"),
        }
    }
}

impl StdError for ServeError {
    fn source(&self) -> Option<&(dyn StdError + 'static)> {
        match &self.kind {
            ServeErrorKind::NonLocalBind(_) => None,
            ServeErrorKind::StateDir { source, .. } | ServeErrorKind::Bind { source, .. } => {
                Some(source)
            }
            ServeErrorKind::LocalAddr(source) | ServeErrorKind::Serve(source) => Some(source),
            ServeErrorKind::Log(source) => Some(source.as_ref()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::future::pending;

    use crate::Config;

    #[tokio::test]
    async fn a_non_loopback_bind_is_refused_with_a_typed_error() {
        // A public address without the opt-in is refused before any I/O, so the
        // caller reads the cause off the error rather than a string (issue #193).
        let config = Config {
            host: "8.8.8.8".parse().unwrap(),
            allow_non_local: false,
            ..Config::default()
        };
        let error = crate::run_until(config, pending())
            .await
            .expect_err("a non-loopback bind is refused");
        assert!(error.is_bind_refused(), "it is a refused-bind error");
        assert!(!error.is_bind(), "a refusal is not a bind failure");
        assert!(
            error.to_string().contains("CREW_BROKER_ALLOW_NON_LOCAL"),
            "the message names the opt-in: {error}",
        );
    }
}
