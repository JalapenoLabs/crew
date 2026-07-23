//! The broker's shared application state.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex, PoisonError};

use crew_core::{Event, RoleId};
use serde::Serialize;
use tokio::sync::broadcast;

use crate::config::Config;
use crate::router::ChannelRouter;
use crate::secrets::Scrubber;
use crate::store::{MemoryStore, Storage};

/// The crew's control standing: whether the General has gated the crew's work (issue
/// #41).
///
/// It rises from `Running` to `Paused` (the brake) to `StoodDown` (the kill switch).
/// Under `Paused` or `StoodDown` every role is gated regardless of its own pause flag;
/// `Running` leaves each role to its own [`is_role_paused`](AppState::is_role_paused)
/// state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Standing {
    /// Normal: roles pull work as usual.
    #[default]
    Running,
    /// Globally paused: no role pulls new work until the crew is resumed.
    Paused,
    /// Stood down: an emergency halt. Every role stops and the durable state is
    /// preserved, so the crew is recoverable.
    StoodDown,
}

/// The crew-wide control state: the standing, plus the individually paused roles.
#[derive(Debug, Default)]
struct Control {
    standing: Standing,
    paused_roles: BTreeSet<RoleId>,
}

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
    /// The crew's pause / stand-down state (issue #41). In memory: the broker is the
    /// live recoverable authority, and every pause change is also recorded as a
    /// `lifecycle` event in the durable log.
    control: Arc<Mutex<Control>>,
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
            control: Arc::new(Mutex::new(Control::default())),
        }
    }

    /// Pauses one role: it pulls no new work until resumed (issue #41).
    pub fn pause_role(&self, role: RoleId) {
        self.control().paused_roles.insert(role);
    }

    /// Resumes one role, clearing its own pause. The crew-wide standing is unchanged,
    /// so a role stays gated while the crew is paused or stood down.
    pub fn resume_role(&self, role: &RoleId) {
        self.control().paused_roles.remove(role);
    }

    /// Pauses the whole crew: no role pulls new work until resumed. A stand-down is not
    /// weakened to a pause.
    pub fn pause_crew(&self) {
        let mut control = self.control();
        if control.standing != Standing::StoodDown {
            control.standing = Standing::Paused;
        }
    }

    /// Resumes the crew, clearing a crew-wide pause or stand-down. Roles paused on their
    /// own stay paused until resumed individually.
    pub fn resume_crew(&self) {
        self.control().standing = Standing::Running;
    }

    /// Stands the crew down: the emergency halt. Every role is gated at once; the
    /// durable log and roster are preserved, so the crew is recoverable.
    pub fn stand_down(&self) {
        self.control().standing = Standing::StoodDown;
    }

    /// Whether `role` is gated from new work: the crew is paused or stood down, or the
    /// role is paused on its own.
    #[must_use]
    pub fn is_role_paused(&self, role: &RoleId) -> bool {
        let control = self.control();
        control.standing != Standing::Running || control.paused_roles.contains(role)
    }

    /// A snapshot of the control state for the roster view: the crew standing and the
    /// set of individually paused roles.
    #[must_use]
    pub fn control_snapshot(&self) -> (Standing, BTreeSet<RoleId>) {
        let control = self.control();
        (control.standing, control.paused_roles.clone())
    }

    /// The control state behind its lock, recovering from a poisoned mutex (a panic in
    /// another handler must not wedge the brake).
    fn control(&self) -> std::sync::MutexGuard<'_, Control> {
        self.control.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Scrubs an event of secrets, appends it to the log, and fans it to subscribers.
    ///
    /// The one path every emitter shares (a posted message, a roster change). Returns
    /// the stored event with the sequence number it was assigned. Masks any configured
    /// secret first, so a leaked value reaches neither the log nor a subscriber, then
    /// appends and broadcasts under one lock, so events reach subscribers in the same
    /// order they are stored and every `Last-Event-ID` cursor stays monotonic. A send
    /// with no live subscribers is not an error: the event is stored for a later reader.
    ///
    /// This is the single point every event enters the store and the stream, so it is
    /// where the stamping guarantee is enforced (issue #29): the event must carry the
    /// fields every projection needs ([`Event::is_well_formed`]). The public HTTP
    /// handlers validate untrusted input before they reach here; the assertion guards
    /// against any internal emitter regressing the invariant.
    ///
    /// [`Event::is_well_formed`]: crew_core::Event::is_well_formed
    pub fn publish(&self, mut event: Event) -> Sequenced {
        debug_assert!(
            event.is_well_formed(),
            "an event must be stamped before it reaches the store or stream: {event:?}",
        );
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

#[cfg(test)]
mod tests {
    use crew_core::{
        Activity, ChannelId, Event, EventKind, Lifecycle, Message, MessageId, MessageKind, RoleId,
        Sender, TaskId, Timestamp,
    };

    use super::AppState;
    use crate::config::Config;

    /// An event of `kind`, from a role on `all-units`, optionally within `task`.
    fn event(kind: EventKind, task: Option<TaskId>) -> Event {
        Event {
            ts: Timestamp::now(),
            from: Sender::Role(RoleId::new("backend")),
            channel: ChannelId::new("all-units"),
            task,
            kind,
        }
    }

    #[test]
    fn publish_stamps_and_correlates_every_event_kind() {
        // publish is the single choke point, so proving the invariant here proves it
        // for a message, a lifecycle transition, and an activity event alike (issue #29).
        let state = AppState::new(Config::default());
        let mut stream = state.broadcast.subscribe();
        let task = TaskId::new();

        let message = event(
            EventKind::Message(Message {
                id: MessageId::new(),
                kind: MessageKind::Note,
                body: "work item".to_owned(),
            }),
            Some(task),
        );
        let lifecycle = event(EventKind::Lifecycle(Lifecycle::Started), Some(task));
        let activity = event(EventKind::Activity(Activity::TurnStarted), None);

        for event in [message.clone(), lifecycle.clone(), activity.clone()] {
            state.publish(event);
        }

        // No event reaches the store missing a required field.
        let stored = state.storage.events();
        assert_eq!(stored.len(), 3, "every event is stored");
        assert!(
            stored.iter().all(Event::is_well_formed),
            "no stored event misses a required field",
        );

        // The stream carries the same well-formed events, in order.
        for expected in [&message, &lifecycle, &activity] {
            let streamed = stream
                .try_recv()
                .expect("the event reaches the stream")
                .event;
            assert!(
                streamed.is_well_formed(),
                "no streamed event misses a field"
            );
            assert_eq!(&streamed, expected, "the stream carries the event intact");
        }

        // Events produced within a task carry its id; one produced outside carries none.
        assert_eq!(
            stored[0].task,
            Some(task),
            "the message correlates to its task"
        );
        assert_eq!(
            stored[1].task,
            Some(task),
            "the lifecycle event correlates too"
        );
        assert_eq!(stored[2].task, None, "an event outside a task carries none");
    }
}
