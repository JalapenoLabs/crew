//! The broker's shared application state.

use std::sync::Arc;

use crate::config::Config;
use crate::router::ChannelRouter;
use crate::store::{MemoryStore, Storage};

/// The shared state every request handler sees.
///
/// Cheap to clone (each field is behind an [`Arc`]), which axum requires since it
/// clones the state per request. It wires the [`Config`], the [`Storage`] backend,
/// and the [`ChannelRouter`] together so handlers read them without global state.
#[derive(Debug, Clone)]
pub struct AppState {
    /// The runtime configuration.
    pub config: Arc<Config>,
    /// The message storage backend (swappable; see [`Storage`]).
    pub storage: Arc<dyn Storage>,
    /// The channel router.
    pub router: Arc<ChannelRouter>,
}

impl AppState {
    /// Builds the application state with the default in-memory storage backend.
    #[must_use]
    pub fn new(config: Config) -> Self {
        Self {
            config: Arc::new(config),
            storage: Arc::new(MemoryStore),
            router: Arc::new(ChannelRouter),
        }
    }
}
