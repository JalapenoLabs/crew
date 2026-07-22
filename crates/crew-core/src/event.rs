//! The event model: one typed stream item, and the kinds it can carry.
//!
//! The broker and supervisor emit a single stream of [`Event`]s; every
//! observability view (task history, per-agent log, aggregate log, live count) is
//! a projection of it (see `docs/observability.md`). Nothing here carries a
//! secret, so every type derives `Debug`; a secret-bearing field would instead
//! need a redacting `Debug` and a leak test (M-PUBLIC-DEBUG).

use serde::{Deserialize, Serialize};

use crate::id::{ChannelId, MessageId, Sender, TaskId};
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

/// An inter-agent message: a typed intent plus a markdown body.
///
/// The [`kind`](Message::kind) lets a front-end render an order differently from a
/// status ping and lets the commander arbitrate (see `docs/communication.md`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    /// The message's unique id, referenced when an `answer` replies to a `question`.
    pub id: MessageId,
    /// The typed intent.
    pub kind: MessageKind,
    /// The markdown body.
    pub body: String,
}

/// The typed intent of a [`Message`] (see `docs/communication.md`).
///
/// The per-kind structured fields (an order's scope and acceptance, a question's
/// options) are deliberately not modeled yet: the schema calls them illustrative,
/// so for now the [`body`](Message::body) carries the detail as markdown.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageKind {
    /// Gives a task to a role.
    Order,
    /// Asks for a decision.
    Question,
    /// Responds to a question.
    Answer,
    /// Reports progress without asking anything.
    Status,
    /// References a produced thing: a branch, a PR, a file, or a route.
    Artifact,
    /// Freeform prose for anything the other kinds do not cover.
    Note,
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
    /// The agent died mid-flight (a defibrillator recovery point).
    Died,
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
    use super::{Activity, Event, EventKind, Lifecycle, Message, MessageKind};
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
        vec![
            Event {
                from: Sender::Role(RoleId::new("commander")),
                channel: ChannelId::new("@backend"),
                task: None,
                ..envelope(EventKind::Message(Message {
                    id: MessageId::new(),
                    kind: MessageKind::Order,
                    body: "scaffold the broker".to_owned(),
                }))
            },
            envelope(EventKind::Message(Message {
                id: MessageId::new(),
                kind: MessageKind::Question,
                body: "SQLite or in-memory?".to_owned(),
            })),
            envelope(EventKind::Lifecycle(Lifecycle::Started)),
            envelope(EventKind::Lifecycle(Lifecycle::Died)),
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
