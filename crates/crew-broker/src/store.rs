//! The broker's event storage backend.

use std::sync::{Mutex, PoisonError};

use crew_core::Event;

/// The pluggable backend the broker keeps its event log in.
///
/// Kept behind a trait so the backend is swappable (see `docs/architecture.md`):
/// the default [`MemoryStore`] holds everything in memory, and a durable backend
/// (an on-disk log or `SQLite`) can drop in later. Pruning and the rolling-summary
/// read (`history?summary=true`) land in later tickets.
pub trait Storage: std::fmt::Debug + Send + Sync {
    /// A short, stable name for the backend, such as `memory`.
    fn backend(&self) -> &'static str;

    /// Appends an event to the log.
    fn append(&self, event: Event);

    /// Returns every stored event, oldest first.
    fn events(&self) -> Vec<Event>;
}

/// The default in-memory event store.
///
/// Holds the log in a `Vec` behind a mutex; the on-disk log and pruning land in
/// later tickets. A poisoned lock is recovered rather than panicked, so a bad
/// request can never take the store down.
#[derive(Debug, Default)]
pub struct MemoryStore {
    events: Mutex<Vec<Event>>,
}

impl Storage for MemoryStore {
    fn backend(&self) -> &'static str {
        "memory"
    }

    fn append(&self, event: Event) {
        self.events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(event);
    }

    fn events(&self) -> Vec<Event> {
        self.events
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}
