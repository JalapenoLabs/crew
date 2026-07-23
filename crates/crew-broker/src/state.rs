//! The broker's shared application state.

use std::sync::{Arc, Mutex, PoisonError};

use crew_core::Event;
use tokio::sync::broadcast;

use crate::config::Config;
use crate::router::ChannelRouter;
use crate::secrets::Scrubber;
use crate::store::{MemoryStore, Storage};

/// How many events a subscriber may fall behind before the broker drops the oldest
/// for it. Large enough to absorb a burst while a slow reader catches up; a lagged
/// subscriber reconnects with its `Last-Event-ID` and replays the gap from the log,
/// so nothing is lost. Raising it trades memory for a longer tolerated stall.
const BROADCAST_CAPACITY: usize = 1024;

/// An event paired with its sequence number: its position in the append-only log.
///
/// The sequence is the cursor the inbox stream emits as a Server-Sent-Event `id`,
/// so a reconnecting subscriber resumes exactly after the last event it received.
#[derive(Debug, Clone)]
pub struct Sequenced {
    /// The event's position in the log, assigned on append.
    pub seq: u64,
    /// The event itself.
    pub event: Event,
}

/// The shared state every request handler sees.
///
/// Cheap to clone (each field is an [`Arc`] or a broadcast [`Sender`], which shares
/// its channel on clone), which axum requires since it clones the state per request.
/// It wires the [`Config`], the [`Storage`] backend, the [`ChannelRouter`], the
/// secret [`Scrubber`], and the live fan-out channel together so handlers read them
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
    /// The fan-out channel a publish sends to and every subscriber stream reads.
    pub broadcast: broadcast::Sender<Sequenced>,
    /// Serializes [`publish`](AppState::publish) so a sequence number is broadcast in
    /// the same order it is assigned, keeping every subscriber's `id` cursor monotonic.
    publish_order: Arc<Mutex<()>>,
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
            publish_order: Arc::new(Mutex::new(())),
        }
    }

    /// Scrubs an event of secrets, appends it to the log, and fans it to subscribers.
    ///
    /// Returns the stored event with the sequence number it was assigned. Masks any
    /// configured secret first, so a leaked value reaches neither the log nor a
    /// subscriber, then appends and broadcasts under one lock, so events reach
    /// subscribers in the same order they are stored and every `Last-Event-ID` cursor
    /// stays monotonic. A send with no live subscribers is not an error: the event is
    /// stored, so a later subscriber replays it from the log.
    pub fn publish(&self, mut event: Event) -> Sequenced {
        // Mask before either sink, so the persisted log and every live stream carry
        // the same scrubbed event.
        self.scrubber.scrub_event(&mut event);
        // Held across the append and the send (both non-blocking, no `.await`), so a
        // concurrent publish cannot interleave and deliver sequences out of order.
        let _order = self
            .publish_order
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let seq = self.storage.next_seq();
        self.storage.append(event.clone());
        let sequenced = Sequenced { seq, event };
        // Err(_) only means no subscribers are listening right now, which is fine.
        let _ = self.broadcast.send(sequenced.clone());
        sequenced
    }
}
