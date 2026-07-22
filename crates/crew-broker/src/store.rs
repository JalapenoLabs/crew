//! The broker's message storage backend.

/// The pluggable backend the broker keeps its message log in.
///
/// Kept behind a trait so the backend is swappable (see `docs/architecture.md`):
/// the default [`MemoryStore`] holds everything in memory, and a durable backend
/// (an on-disk log or `SQLite`) can drop in later. The append and read surface (over
/// [`crew_core`] events) lands with the message-log work; for now the trait only
/// names the backend, which the health probe reports.
///
/// [`crew_core`]: crew_core
pub trait Storage: std::fmt::Debug + Send + Sync {
    /// A short, stable name for the backend, such as `memory`.
    fn backend(&self) -> &'static str;
}

/// The default in-memory message store.
///
/// A placeholder for the broker skeleton (issue #7): it holds nothing yet. The
/// message log and the on-disk state land in later tickets.
#[derive(Debug, Default)]
pub struct MemoryStore;

impl Storage for MemoryStore {
    fn backend(&self) -> &'static str {
        "memory"
    }
}
