//! The event model: one typed stream item, and the kinds it can carry.
//!
//! The broker and supervisor emit a single stream of [`Event`]s; every
//! observability view (task history, per-agent log, aggregate log, live count) is
//! a projection of it (see `docs/observability.md`). Nothing here carries a
//! secret, so every type derives `Debug`; a secret-bearing field would instead
//! need a redacting `Debug` and a leak test (M-PUBLIC-DEBUG).

use serde::{Deserialize, Serialize};

use crate::channel::Channel;
use crate::id::{ChannelId, MessageId, RoleId, Sender, TaskId};
use crate::time::Timestamp;

/// A single, typed, addressed item on the crew event stream.
///
/// The envelope stamps every event with when it happened, who sent it, the
/// channel it was addressed to, and the task it belongs to (when one applies);
/// the [`kind`](Event::kind) carries the payload.
///
/// # Examples
/// ```
/// use crew_core::{ChannelId, Event, EventKind, Lifecycle, RoleId, Sender, Timestamp};
/// let event = Event {
///     ts: Timestamp::now(),
///     from: Sender::Role(RoleId::new("backend")),
///     channel: ChannelId::new("all-units"),
///     task: None,
///     kind: EventKind::Lifecycle(Lifecycle::Started),
/// };
/// assert_eq!(event.channel.as_str(), "all-units");
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Event {
    /// When the event occurred.
    pub ts: Timestamp,
    /// Who emitted it: a role, or the General (the human).
    pub from: Sender,
    /// The channel it was addressed to.
    pub channel: ChannelId,
    /// The task it belongs to, when a task context applies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub task: Option<TaskId>,
    /// The payload: what kind of event this is.
    pub kind: EventKind,
}

impl Event {
    /// Whether the event carries the fields every projection needs, non-degenerate.
    ///
    /// `ts`, `from`, `channel`, and `kind` are mandatory in the type, so they are
    /// always present; this additionally rejects the two ways a present field can
    /// still be useless to a projection: a blank channel or a blank role sender. The
    /// broker asserts it at the one point every event enters the store and stream (its
    /// `publish` path), so a malformed event is never persisted or streamed (issue #29).
    ///
    /// This is the invariant behind "no event reaches the store or stream missing a
    /// required field" (see `docs/observability.md`).
    ///
    /// # Examples
    /// ```
    /// use crew_core::{Activity, ChannelId, Event, EventKind, RoleId, Sender, Timestamp};
    ///
    /// let event = Event {
    ///     ts: Timestamp::now(),
    ///     from: Sender::Role(RoleId::new("backend")),
    ///     channel: ChannelId::new("all-units"),
    ///     task: None,
    ///     kind: EventKind::Activity(Activity::TurnStarted),
    /// };
    /// assert!(event.is_well_formed());
    /// assert!(!Event { channel: ChannelId::new(" "), ..event.clone() }.is_well_formed());
    /// ```
    #[must_use]
    pub fn is_well_formed(&self) -> bool {
        if self.channel.as_str().trim().is_empty() {
            return false;
        }
        if let Sender::Role(role) = &self.from {
            if role.as_str().trim().is_empty() {
                return false;
            }
        }
        true
    }

    /// Whether this event belongs to `role`'s activity timeline (issue #30).
    ///
    /// A role's full timeline, the "watch what the backend engineer is doing" view, is
    /// every event the role took part in (see `docs/observability.md`):
    ///
    /// - the role's **own** events: the messages it sent, its lifecycle transitions,
    ///   and its stream-json activity (all stamped `from` the role);
    /// - the messages it **received**: message events whose channel addresses the role
    ///   (its direct `@role` channel, a pair it belongs to, or `all-units`).
    ///
    /// It is not self-filtered like the inbox, since the timeline is what the role does
    /// as well as what reaches it. Another role's lifecycle or activity is excluded
    /// even when broadcast to `all-units`: only messages count as "received".
    ///
    /// # Examples
    /// ```
    /// use crew_core::{ChannelId, Event, EventKind, Lifecycle, MessageId, Message,
    ///     MessageKind, RoleId, Sender, Timestamp};
    ///
    /// let backend = RoleId::new("backend");
    /// let note = |from: &str, channel: &str| Event {
    ///     ts: Timestamp::now(),
    ///     from: Sender::Role(RoleId::new(from)),
    ///     channel: ChannelId::new(channel),
    ///     task: None,
    ///     kind: EventKind::Message(Message {
    ///         id: MessageId::new(),
    ///         kind: MessageKind::Note,
    ///         body: String::new(),
    ///     }),
    /// };
    ///
    /// assert!(note("backend", "@frontend").in_timeline_of(&backend), "a message it sent");
    /// assert!(note("frontend", "@backend").in_timeline_of(&backend), "a message it received");
    /// assert!(note("frontend", "all-units").in_timeline_of(&backend), "a broadcast reaches it");
    /// assert!(!note("frontend", "@qa").in_timeline_of(&backend), "not its concern");
    /// ```
    #[must_use]
    pub fn in_timeline_of(&self, role: &RoleId) -> bool {
        // The role's own events: sent messages, its lifecycle, and its activity.
        if matches!(&self.from, Sender::Role(from) if from == role) {
            return true;
        }
        // Plus messages addressed to it: its direct channel, a pair, or `all-units`.
        matches!(self.kind, EventKind::Message(_))
            && Channel::parse(self.channel.as_str()).is_some_and(|channel| channel.addresses(role))
    }
}

/// The three kinds of item on the event stream (see `docs/observability.md`).
///
/// `message` is inter-agent communication, `lifecycle` is a supervised state
/// change, and `activity` is an agent's own work parsed from its stream-json.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum EventKind {
    /// Inter-agent communication.
    Message(Message),
    /// An agent's supervised lifecycle transition.
    Lifecycle(Lifecycle),
    /// An agent's own work, parsed from its `claude -p` stream-json.
    Activity(Activity),
}

/// An inter-agent message: a typed intent, its per-kind fields, and a markdown body.
///
/// The [`kind`](Message::kind) lets a front-end render an order differently from a
/// status ping and lets the commander arbitrate (see `docs/communication.md`). The
/// kind and its structured fields are flattened onto the message, so an order
/// serializes as `{"id":..,"kind":"order","title":..,"body":..}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    /// The message's unique id, referenced when an `answer` replies to a `question`.
    pub id: MessageId,
    /// The typed intent and its per-kind structured fields.
    #[serde(flatten)]
    pub kind: MessageKind,
    /// The markdown body: freeform detail alongside the structured fields.
    #[serde(default)]
    pub body: String,
}

/// The typed intent of a [`Message`] and its per-kind structured fields
/// (see `docs/communication.md`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum MessageKind {
    /// Gives a task to a role.
    Order {
        /// A short title for the task.
        title: String,
        /// What is in and out of scope.
        scope: String,
        /// The paths the role owns while working the task.
        owned_paths: Vec<String>,
        /// How the task is judged done.
        acceptance: String,
    },
    /// Asks for a decision, with optional suggested options.
    Question {
        /// Suggested options for the answer, if any.
        #[serde(default)]
        options: Vec<String>,
    },
    /// Responds to a question.
    Answer,
    /// Reports progress without asking anything.
    Status,
    /// References a produced thing: a branch, a PR, a file, or a route.
    Artifact {
        /// The reference to the produced thing (a branch name, a PR URL, a file
        /// path, or a route).
        reference: String,
        /// What kind of artifact the reference points to.
        artifact_kind: ArtifactKind,
    },
    /// Freeform prose for anything the other kinds do not cover.
    Note,
}

/// What a [`MessageKind::Artifact`] reference points to (see `docs/communication.md`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactKind {
    /// A git branch.
    Branch,
    /// A pull request.
    PullRequest,
    /// A file.
    File,
    /// A route: a URL path the crew produced or touched.
    Route,
}

/// An agent's supervised lifecycle state (see `docs/observability.md`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Lifecycle {
    /// The agent process started.
    Started,
    /// The agent went idle, with no work in flight.
    Idle,
    /// The agent was stopped.
    Stopped,
    /// The agent was restarted.
    Restarted,
    /// The agent died mid-flight: it crashed or hung, and the defibrillator reaped
    /// its orphaned process (see `docs/observability.md`).
    Died,
    /// The defibrillator revived the agent after a death, within its recovery budget.
    Recovered,
    /// The role was paused: it pulls no new work until resumed (issue #41). The
    /// General's brake, per role or crew-wide.
    Paused,
    /// The role was resumed: it may pull work again (issue #41).
    Resumed,
    /// The crew was stood down: every role halts at once and the state is preserved so
    /// the crew is recoverable (issue #41). The General's emergency kill switch.
    StoodDown,
}

/// An agent's own work item, parsed from its `claude -p` stream-json.
///
/// The turn and tool payloads grow when the supervisor's stream-json parsing
/// lands; this is the vocabulary the parse targets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Activity {
    /// A turn began.
    TurnStarted,
    /// A turn ended.
    TurnEnded,
    /// The agent called a tool.
    ToolCall {
        /// The tool's name.
        tool: String,
    },
    /// The agent produced text output.
    Output {
        /// The output text.
        text: String,
    },
}

#[cfg(test)]
mod tests {
    use super::{Activity, ArtifactKind, Event, EventKind, Lifecycle, Message, MessageKind};
    use crate::id::{ChannelId, MessageId, RoleId, Sender, TaskId};
    use crate::time::Timestamp;

    /// One representative event per kind, to exercise every serde path.
    fn sample_events() -> Vec<Event> {
        let envelope = |kind| Event {
            ts: Timestamp::now(),
            from: Sender::General,
            channel: ChannelId::new("all-units"),
            task: Some(TaskId::new()),
            kind,
        };
        let message = |kind| {
            EventKind::Message(Message {
                id: MessageId::new(),
                kind,
                body: "detail as markdown".to_owned(),
            })
        };
        vec![
            Event {
                from: Sender::Role(RoleId::new("commander")),
                channel: ChannelId::new("@backend"),
                task: None,
                ..envelope(message(MessageKind::Order {
                    title: "Scaffold the broker".to_owned(),
                    scope: "crew-broker only".to_owned(),
                    owned_paths: vec!["crates/crew-broker".to_owned()],
                    acceptance: "crewd serves /health".to_owned(),
                }))
            },
            envelope(message(MessageKind::Question {
                options: vec!["SQLite".to_owned(), "in-memory".to_owned()],
            })),
            envelope(message(MessageKind::Answer)),
            envelope(message(MessageKind::Status)),
            envelope(message(MessageKind::Artifact {
                reference: "https://github.com/JalapenoLabs/crew/pull/8".to_owned(),
                artifact_kind: ArtifactKind::PullRequest,
            })),
            envelope(message(MessageKind::Note)),
            envelope(EventKind::Lifecycle(Lifecycle::Started)),
            envelope(EventKind::Lifecycle(Lifecycle::Died)),
            envelope(EventKind::Lifecycle(Lifecycle::Paused)),
            envelope(EventKind::Lifecycle(Lifecycle::Resumed)),
            envelope(EventKind::Lifecycle(Lifecycle::StoodDown)),
            envelope(EventKind::Activity(Activity::TurnStarted)),
            envelope(EventKind::Activity(Activity::ToolCall {
                tool: "cargo".to_owned(),
            })),
            envelope(EventKind::Activity(Activity::Output {
                text: "build succeeded".to_owned(),
            })),
        ]
    }

    #[test]
    fn every_event_kind_round_trips_through_json() {
        for event in sample_events() {
            let json = serde_json::to_string(&event).unwrap();
            let back: Event = serde_json::from_str(&json).unwrap();
            assert_eq!(event, back, "round trip changed the event: {json}");
        }
    }

    #[test]
    fn event_kinds_are_adjacently_tagged() {
        let json = serde_json::to_value(EventKind::Lifecycle(Lifecycle::Idle)).unwrap();
        assert_eq!(
            json,
            serde_json::json!({ "kind": "lifecycle", "data": "idle" })
        );
    }

    #[test]
    fn message_kind_and_fields_flatten_onto_the_message() {
        let message = Message {
            id: MessageId::new(),
            kind: MessageKind::Order {
                title: "Ship it".to_owned(),
                scope: "here".to_owned(),
                owned_paths: vec!["src".to_owned()],
                acceptance: "green".to_owned(),
            },
            body: "the detail".to_owned(),
        };
        let json = serde_json::to_value(&message).unwrap();
        // The kind discriminant and its fields sit alongside id and body, not nested.
        assert_eq!(json["kind"], "order");
        assert_eq!(json["title"], "Ship it");
        assert_eq!(json["owned_paths"], serde_json::json!(["src"]));
        assert_eq!(json["body"], "the detail");
        assert!(json.get("data").is_none());
    }

    #[test]
    fn well_formed_requires_a_channel_and_a_named_sender() {
        let base = Event {
            ts: Timestamp::now(),
            from: Sender::Role(RoleId::new("backend")),
            channel: ChannelId::new("all-units"),
            task: None,
            kind: EventKind::Activity(Activity::TurnStarted),
        };
        assert!(base.is_well_formed(), "a stamped event is well formed");
        assert!(
            Event {
                from: Sender::General,
                ..base.clone()
            }
            .is_well_formed(),
            "the General is a valid sender",
        );
        assert!(
            !Event {
                channel: ChannelId::new("  "),
                ..base.clone()
            }
            .is_well_formed(),
            "a blank channel is not well formed",
        );
        assert!(
            !Event {
                from: Sender::Role(RoleId::new("")),
                ..base
            }
            .is_well_formed(),
            "a blank role sender is not well formed",
        );
    }

    #[test]
    fn in_timeline_of_covers_sent_received_and_own_lifecycle_and_activity() {
        let backend = RoleId::new("backend");
        let note = |from: Sender, channel: &str| Event {
            ts: Timestamp::now(),
            from,
            channel: ChannelId::new(channel),
            task: None,
            kind: EventKind::Message(Message {
                id: MessageId::new(),
                kind: MessageKind::Note,
                body: String::new(),
            }),
        };
        let role = |name: &str| Sender::Role(RoleId::new(name));

        // Messages: sent by the role, and received (direct, pair, or all-units).
        assert!(
            note(role("backend"), "@frontend").in_timeline_of(&backend),
            "sent"
        );
        assert!(
            note(role("frontend"), "@backend").in_timeline_of(&backend),
            "direct"
        );
        assert!(
            note(role("qa"), "backend+qa").in_timeline_of(&backend),
            "a pair it belongs to",
        );
        assert!(
            note(role("frontend"), "all-units").in_timeline_of(&backend),
            "a broadcast it receives",
        );
        assert!(
            !note(role("frontend"), "@qa").in_timeline_of(&backend),
            "a message between others is not its concern",
        );

        // Its own lifecycle and activity (stamped `from` the role) belong to it...
        let own = |kind| Event {
            ts: Timestamp::now(),
            from: role("backend"),
            channel: ChannelId::new("all-units"),
            task: None,
            kind,
        };
        assert!(own(EventKind::Lifecycle(Lifecycle::Started)).in_timeline_of(&backend));
        assert!(own(EventKind::Activity(Activity::TurnStarted)).in_timeline_of(&backend));

        // ...but another role's lifecycle broadcast to all-units is not "received".
        let others_lifecycle = Event {
            from: role("frontend"),
            ..own(EventKind::Lifecycle(Lifecycle::Started))
        };
        assert!(
            !others_lifecycle.in_timeline_of(&backend),
            "only messages count as received; a peer's lifecycle is not",
        );
    }

    #[test]
    fn absent_task_is_omitted_from_json() {
        let event = Event {
            ts: Timestamp::now(),
            from: Sender::General,
            channel: ChannelId::new("all-units"),
            task: None,
            kind: EventKind::Activity(Activity::TurnEnded),
        };
        let json = serde_json::to_string(&event).unwrap();
        assert!(
            !json.contains("task"),
            "None task should be omitted: {json}"
        );
    }
}
