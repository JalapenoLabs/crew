//! The broker's shared application state.

use std::sync::Arc;

use crew_core::Event;
use tokio::sync::broadcast;

use crate::config::Config;
use crate::router::ChannelRouter;
use crate::secrets::Scrubber;
use crate::store::{MemoryStore, Storage};

/// How many events a subscriber may fall behind before the broker drops the oldest
/// for it. Large enough to absorb a brief burst while a reader reconnects; a lagged
/// subscriber skips the gap rather than stalling the broker (see the `/stream`
/// handler). Raising it trades memory for a longer tolerated stall.
const BROADCAST_CAPACITY: usize = 256;

/// The shared state every request handler sees.
///
/// Cheap to clone (each field is an [`Arc`] or a broadcast [`Sender`], which shares
/// its channel on clone), which axum requires since it clones the state per request.
/// It wires the [`Config`], the [`Storage`] backend, the [`ChannelRouter`], the
/// secret [`Scrubber`], and the fan-out channel together so handlers read them
/// without global state.
///
/// [`Sender`]: broadcast::Sender
#[derive(Debug, Clone)]
pub struct AppState {
    /// The runtime configuration.
    pub config: Arc<Config>,
    /// The message storage backend (swappable; see [`Storage`]).
    pub storage: Arc<dyn Storage>,
    /// The channel router.
    pub router: Arc<ChannelRouter>,
    /// Masks configured secret values out of every event before it is stored or streamed.
    pub scrubber: Arc<Scrubber>,
    /// The fan-out channel a `POST` publishes to and every subscriber stream reads.
    pub broadcast: broadcast::Sender<Event>,
}

impl AppState {
    /// Builds the application state with the default in-memory storage backend.
    ///
    /// For tests and ephemeral use; the `crewd` daemon injects a durable backend with
    /// [`with_storage`](AppState::with_storage).
    #[must_use]
    pub fn new(config: Config) -> Self {
        Self::with_storage(config, Arc::new(MemoryStore::default()))
    }

    /// Builds the application state over a chosen [`Storage`] backend.
    ///
    /// Builds the secret [`Scrubber`] once from [`Config::secrets`] and opens the
    /// fan-out channel; both are shared across every request. Takes the backend as a
    /// `dyn Storage`, so the broker never depends on a concrete store.
    #[must_use]
    pub fn with_storage(config: Config, storage: Arc<dyn Storage>) -> Self {
        let scrubber = Scrubber::new(config.secrets.iter().cloned());
        let (broadcast, _) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            config: Arc::new(config),
            storage,
            router: Arc::new(ChannelRouter),
            scrubber: Arc::new(scrubber),
            broadcast,
        }
    }

    /// Scrubs an event of secrets in place, stores it, and fans it to subscribers.
    ///
    /// The one path every emitter shares (a posted message, a roster change), so the
    /// persisted log and every live stream carry the same scrubbed event; the caller
    /// keeps `event`, now masked. A send with no live subscribers is not an error:
    /// the event is stored for a later reader.
    pub fn publish(&self, event: &mut Event) {
        self.scrubber.scrub_event(event);
        self.storage.append(event.clone());
        let _ = self.broadcast.send(event.clone());
    }
}
