//! The broker's shared application state.

use std::sync::{Arc, Mutex, PoisonError};

use crew_core::Event;
use tokio::sync::broadcast;

use crate::config::Config;
use crate::router::ChannelRouter;
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
/// It wires the [`Config`], the [`Storage`] backend, the [`ChannelRouter`], and the
/// live fan-out channel together so handlers read them without global state.
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
    /// The fan-out channel a publish sends to and every inbox subscriber reads.
    pub broadcast: broadcast::Sender<Sequenced>,
    /// Serializes [`publish`](AppState::publish) so a sequence number is broadcast in
    /// the same order it is assigned, keeping every subscriber's `id` cursor monotonic.
    publish_order: Arc<Mutex<()>>,
}

impl AppState {
    /// Builds the application state with the default in-memory storage backend.
    #[must_use]
    pub fn new(config: Config) -> Self {
        let (broadcast, _) = broadcast::channel(BROADCAST_CAPACITY);
        Self {
            config: Arc::new(config),
            storage: Arc::new(MemoryStore::default()),
            router: Arc::new(ChannelRouter),
            broadcast,
            publish_order: Arc::new(Mutex::new(())),
        }
    }

    /// Appends an event to the log and fans it out to live subscribers.
    ///
    /// Returns the stored event with the sequence number it was assigned. Appends
    /// and broadcasts under one lock, so events reach subscribers in the same order
    /// they are stored and every `Last-Event-ID` cursor stays monotonic. A send with
    /// no live subscribers is not an error: the event is stored, so a later
    /// subscriber replays it from the log.
    pub fn publish(&self, event: Event) -> Sequenced {
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
