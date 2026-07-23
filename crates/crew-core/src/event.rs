//! The event model: one typed stream item, and the kinds it can carry.
//!
//! The broker and supervisor emit a single stream of [`Event`]s; every
//! observability view (task history, per-agent log, aggregate log, live count)
//! is a projection of it (see `docs/observability.md`). Nothing here carries a
//! secret, so every type derives `Debug`; a secret-bearing field would instead
//! need a redacting `Debug` and a leak test (M-PUBLIC-DEBUG).

use serde::{Deserialize, Serialize};

use crate::{
    budget::{BudgetScope, Spend},
    channel::Channel,
    id::{ChannelId, MessageId, RoleId, Sender, TaskId},
    time::Timestamp,
};

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
    /// Whether the event carries the fields every projection needs,
    /// non-degenerate.
    ///
    /// `ts`, `from`, `channel`, and `kind` are mandatory in the type, so they
    /// are always present; this additionally rejects the two ways a present
    /// field can still be useless to a projection: a blank channel or a
    /// blank role sender. The broker asserts it at the one point every
    /// event enters the store and stream (its `publish` path), so a
    /// malformed event is never persisted or streamed (issue #29).
    ///
    /// This is the invariant behind "no event reaches the store or stream
    /// missing a required field" (see `docs/observability.md`).
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
    /// assert!(!Event {
    ///     channel: ChannelId::new(" "),
    ///     ..event.clone()
    /// }
    /// .is_well_formed());
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
    /// A role's full timeline, the "watch what the backend engineer is doing"
    /// view, is every event the role took part in (see
    /// `docs/observability.md`):
    ///
    /// - the role's **own** events: the messages it sent, its lifecycle
    ///   transitions, and its stream-json activity (all stamped `from` the
    ///   role);
    /// - the messages it **received**: message events whose channel addresses
    ///   the role (its direct `@role` channel, a pair it belongs to, or
    ///   `all-units`).
    ///
    /// It is not self-filtered like the inbox, since the timeline is what the
    /// role does as well as what reaches it. Another role's lifecycle or
    /// activity is excluded even when broadcast to `all-units`: only
    /// messages count as "received".
    ///
    /// # Examples
    /// ```
    /// use crew_core::{
    ///     ChannelId, Event, EventKind, Lifecycle, Message, MessageId, MessageKind, RoleId, Sender,
    ///     Timestamp,
    /// };
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
    /// assert!(
    ///     note("backend", "@frontend").in_timeline_of(&backend),
    ///     "a message it sent"
    /// );
    /// assert!(
    ///     note("frontend", "@backend").in_timeline_of(&backend),
    ///     "a message it received"
    /// );
    /// assert!(
    ///     note("frontend", "all-units").in_timeline_of(&backend),
    ///     "a broadcast reaches it"
    /// );
    /// assert!(
    ///     !note("frontend", "@qa").in_timeline_of(&backend),
    ///     "not its concern"
    /// );
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

/// A typed item on the crew event stream (see `docs/observability.md`).
///
/// `message` is inter-agent communication, `lifecycle` is a supervised state
/// change, `activity` is an agent's own work parsed from its stream-json,
/// `ledger` is a change to the shared work ledger (issue #45), `boundary` is a
/// lane crossing (issue #46), `verification` is a step through the adversarial
/// done-gate (issue #47), `board` is a change to the shared situation board
/// (issue #49), `budget` is a token-spend report against the crew budget (issue
/// #54), `telemetry` is a per-turn token-and-cost usage report (issue #55), and
/// `usage` is a shared-subscription usage reading and its auto-pause (issue
/// #56), and `stall` is a coordination stall the monitor detected or resolved
/// (issue #48, #120).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "data", rename_all = "snake_case")]
pub enum EventKind {
    /// Inter-agent communication.
    Message(Message),
    /// An agent's supervised lifecycle transition.
    Lifecycle(Lifecycle),
    /// An agent's own work, parsed from its `claude -p` stream-json.
    Activity(Activity),
    /// A change to the shared work ledger: a role claiming or updating work
    /// (issue #45).
    Ledger(LedgerEvent),
    /// A role reaching outside its owned lane (issue #46).
    Boundary(BoundaryEvent),
    /// A step through the adversarial done-gate: a submission or a verdict
    /// (issue #47).
    Verification(VerificationEvent),
    /// A change to the shared situation board: an entry recorded or retracted
    /// (issue #49).
    Board(BoardEvent),
    /// A token-spend report against the crew budget, and any cap it hit (issue
    /// #54).
    Budget(BudgetEvent),
    /// A per-turn usage report: the tokens and cost a role spent on a turn
    /// (issue #55).
    Telemetry(TelemetryEvent),
    /// A shared-subscription usage reading, and whether it auto-paused the crew
    /// (issue #56).
    Usage(UsageEvent),
    /// A coordination stall the fleet-wide monitor detected or resolved (issue
    /// #48, surfaced on the stream by issue #120).
    Stall(StallEvent),
}

/// An inter-agent message: a typed intent, its per-kind fields, and a markdown
/// body.
///
/// The [`kind`](Message::kind) lets a front-end render an order differently
/// from a status ping and lets the commander arbitrate (see
/// `docs/communication.md`). The kind and its structured fields are flattened
/// onto the message, so an order serializes as
/// `{"id":..,"kind":"order","title":..,"body":..}`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    /// The message's unique id, referenced when an `answer` replies to a
    /// `question`.
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
    /// Responds to a question, naming the question it replies to.
    Answer {
        /// The id of the [`Message`] this answers, so a front-end can thread
        /// the reply to its question and the commander can correlate
        /// the two. An answer always replies to a specific question, so
        /// the reference is required.
        in_reply_to: MessageId,
    },
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
    /// Steers a role mid-task without stopping it: the General's `crew
    /// redirect` (issue #38). The role honors it at its next tool boundary,
    /// keeping its current task and adjusting course; the steering text is
    /// the [`body`](Message::body).
    Redirect,
    /// Halts a role's current work and re-tasks it: the General's `crew belay`
    /// (issue #38). The role stops what it is doing and takes the
    /// [`body`](Message::body) as its new order.
    Belay,
}

impl MessageKind {
    /// Whether this is a General directive the receiving role must honor at
    /// once, at its next tool boundary rather than at its leisure.
    ///
    /// A [`Redirect`](Self::Redirect) steers a role without stopping it; a
    /// [`Belay`](Self::Belay) halts its current work and re-tasks it. Both are
    /// the General interjecting to steer a running agent, so a front-end
    /// flags them and an agent acts on them immediately (see
    /// `docs/communication.md`, command and control).
    #[must_use]
    pub fn is_directive(&self) -> bool {
        matches!(self, Self::Redirect | Self::Belay)
    }
}

/// What a [`MessageKind::Artifact`] reference points to (see
/// `docs/communication.md`).
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
    /// The agent died mid-flight: it crashed or hung, and the defibrillator
    /// reaped its orphaned process (see `docs/observability.md`).
    Died,
    /// The defibrillator revived the agent after a death, within its recovery
    /// budget.
    Recovered,
    /// The role was paused: it pulls no new work until resumed (issue #41). The
    /// General's brake, per role or crew-wide.
    Paused,
    /// The role was resumed: it may pull work again (issue #41).
    Resumed,
    /// The crew was stood down: every role halts at once and the state is
    /// preserved so the crew is recoverable (issue #41). The General's
    /// emergency kill switch.
    StoodDown,
    /// The crew gracefully finished its mission: the work is done, not halted
    /// (issue #121). A role, typically the commander, reports it as the mission
    /// completes, so `crew notify` can pull the General back on a true finish
    /// rather than approximating it with a [`StoodDown`](Self::StoodDown)
    /// emergency halt.
    MissionComplete,
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

/// A change to the shared work ledger: a role claiming a piece of work or
/// moving it to a new state (issue #45).
///
/// The ledger keeps two roles from grabbing the same work: a role claims before
/// it starts and moves the claim to `done` when it finishes. The broker
/// enforces one owner per task, and every change rides the event stream, so the
/// ledger is a projection of it (see `docs/observability.md`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LedgerEvent {
    /// The work item's key: a stable identifier the crew agrees on, such as a
    /// path, a feature name, or an order's title. Two roles must not hold
    /// the same key at once.
    pub task: String,
    /// The role that owns the claim.
    pub owner: RoleId,
    /// The state the work moved to.
    pub state: TaskState,
    /// A short human title for the ledger view; may be empty.
    #[serde(default)]
    pub title: String,
}

/// The state of a claimed piece of work in the ledger (issue #45).
///
/// A task is **held** while [`Claimed`](Self::Claimed),
/// [`InProgress`](Self::InProgress), or [`Blocked`](Self::Blocked): another
/// role's claim is refused. [`Done`](Self::Done) frees it, so a finished task
/// no longer blocks a new claim.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskState {
    /// A role has claimed the work but not started it.
    Claimed,
    /// The owner is working on it.
    InProgress,
    /// The owner is blocked and cannot proceed.
    Blocked,
    /// The work is finished; the claim is released.
    Done,
}

impl TaskState {
    /// Whether a task in this state is still held, so another role's claim is
    /// refused.
    ///
    /// Every state but [`Done`](Self::Done) holds the claim.
    #[must_use]
    pub fn is_held(self) -> bool {
        !matches!(self, Self::Done)
    }

    /// The state's stable label, matching its serialized name.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Claimed => "claimed",
            Self::InProgress => "in_progress",
            Self::Blocked => "blocked",
            Self::Done => "done",
        }
    }
}

/// A role reaching outside its owned lane: a boundary crossing (issue #46).
///
/// Lane enforcement (`docs/roles.md`, ownership model) warns or blocks a role
/// editing a path outside its owned boundaries, and records the crossing here
/// so the operator sees it on the stream. A genuine cross-lane need should go
/// through the commander, not a silent edit.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoundaryEvent {
    /// The role that reached outside its lane.
    pub role: RoleId,
    /// The out-of-lane path it reached for.
    pub path: String,
    /// Whether the crew's policy blocked the edit (`true`) or only warned
    /// (`false`).
    pub blocked: bool,
}

/// A step through the adversarial done-gate: a submission, and the verdict on
/// it (issue #47).
///
/// "Done" is verified, not asserted. When a role believes its task meets the
/// acceptance criteria it submits the work rather than declaring it done; an
/// independent role then tries to break it against those criteria and returns a
/// [`Verdict`]. Only a [`Passed`](Verdict::Passed) verdict from a role other
/// than the owner marks the task done, and a [`Failed`](Verdict::Failed)
/// verdict carries the specific failure back to the owner as an actionable
/// handback (see `docs/roles.md`, the done-gate).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VerificationEvent {
    /// The task under the gate, named by its title (the order's title).
    pub task: String,
    /// The role whose work is under verification: the one that submitted it.
    pub owner: RoleId,
    /// The independent role that returned the verdict; absent on the submission
    /// itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verifier: Option<RoleId>,
    /// Where the task stands in the gate.
    pub verdict: Verdict,
    /// The acceptance criteria being claimed (on a submission) or the specific
    /// failure (on a failed verdict); empty when there is none.
    #[serde(default)]
    pub detail: String,
}

/// A task's standing in the adversarial done-gate (issue #47).
///
/// It moves from [`Submitted`](Self::Submitted) to either
/// [`Passed`](Self::Passed), when an independent verifier could not break it
/// (the task is done), or [`Failed`](Self::Failed), when a verifier broke it
/// (the work returns to the owner).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Verdict {
    /// The owner submitted the work; it awaits an independent verifier.
    Submitted,
    /// An independent verifier could not break it against the acceptance
    /// criteria: done.
    Passed,
    /// A verifier broke it; the work returns to the owner with the specific
    /// failure.
    Failed,
}

/// A change to the shared situation board: an entry recorded or retracted
/// (issue #49).
///
/// The board is the crew's durable memory, distinct from the transient message
/// stream: agreed interfaces, decisions and their rationale, and known gotchas,
/// so the crew stops re-deriving and re-litigating what is settled. Every
/// change is a first-class `board` event, so the board is auditable and
/// rebuildable from the log across a restart (see `docs/communication.md`,
/// context management).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BoardEvent {
    /// The entry's stable key: a topic the crew agrees on, such as
    /// `auth-strategy` or `api-error-format`. Recording the same key again
    /// updates the entry.
    pub key: String,
    /// Which section of the board the entry belongs to.
    pub section: BoardSection,
    /// The role that recorded or retracted the entry.
    pub author: RoleId,
    /// The entry's content: a decision and its rationale, an agreed interface,
    /// or a gotcha. Empty on a retraction.
    #[serde(default)]
    pub body: String,
    /// Whether this change retracts the entry, removing it from the board.
    #[serde(default)]
    pub retracted: bool,
}

/// A section of the shared situation board (issue #49).
///
/// The three kinds of durable memory the crew keeps: what it decided, what
/// interfaces it agreed on, and what gotchas it hit, so a new or returning role
/// reads them rather than re-deriving them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BoardSection {
    /// A decision the crew agreed on, with its rationale.
    Decision,
    /// An agreed interface or contract between roles.
    Interface,
    /// A known gotcha or pitfall, so the crew does not rediscover it.
    Gotcha,
}

/// A token-spend report against the crew budget, and any cap it hit (issue
/// #54).
///
/// The supervisor publishes one as it records a role's spend, so a UI reads
/// spend against budget off the stream and a cap hit is never silent. When
/// [`breach`](BudgetEvent::breach) is set, the report marks the moment the
/// supervisor idle-stops the role (a [`Role`] cap) or the crew (the [`Crew`]
/// budget) rather than overrun.
///
/// [`Role`]: BudgetScope::Role
/// [`Crew`]: BudgetScope::Crew
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BudgetEvent {
    /// The role whose spend this report is about.
    pub role: RoleId,
    /// The role's cumulative token spend.
    pub role_spent: u64,
    /// The role's own cap, if it has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role_cap: Option<u64>,
    /// The crew's cumulative token spend across every role.
    pub crew_spent: u64,
    /// The crew-wide budget, if the crew has one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub crew_budget: Option<u64>,
    /// The ceiling this spend hit, if any: the role idle-stops (a [`Role`]
    /// breach) or the whole crew does (a [`Crew`] breach). `None` is a
    /// report still within budget.
    ///
    /// [`Role`]: BudgetScope::Role
    /// [`Crew`]: BudgetScope::Crew
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub breach: Option<BudgetScope>,
}

impl From<Spend> for BudgetEvent {
    /// Renders a recorded [`Spend`] as the `budget` event that surfaces it on
    /// the stream.
    fn from(spend: Spend) -> Self {
        Self {
            role: spend.role,
            role_spent: spend.role_spent,
            role_cap: spend.role_cap,
            crew_spent: spend.crew_spent,
            crew_budget: spend.crew_budget,
            breach: spend.breach,
        }
    }
}

/// A per-turn usage report: the tokens and cost a role spent on one turn (issue
/// #55).
///
/// The supervisor publishes one as it records each turn's usage, so per-role
/// and aggregate spend is legible off the stream regardless of any budget. The
/// broker folds these (with the role's working time, read from its `lifecycle`
/// events) into the `GET /stats` rollup that feeds the cockpit and the Seraphim
/// stats. The counts are per turn (incremental), so the rollup is their running
/// sum.
///
/// Cost is carried as micro-USD (millionths of a dollar) so it accumulates
/// exactly, without floating-point drift; a consumer divides by 1,000,000 to
/// render dollars.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TelemetryEvent {
    /// The role whose turn this usage belongs to.
    pub role: RoleId,
    /// The tokens the turn spent.
    pub tokens: u64,
    /// The turn's cost in micro-USD (millionths of a dollar).
    pub cost_micro_usd: u64,
}

/// A shared-subscription usage reading, and whether it auto-paused the crew
/// (issue #56).
///
/// The crew shares one subscription, so the broker keeps one usage gauge. The
/// supervisor reports the window's fill against the shared limit; the broker
/// publishes a `usage` event when a reading auto-pauses the crew (crossing the
/// threshold) or lifts the pause, so the moment is visible on the stream, never
/// silent. A paused reading carries the
/// [`window_reset`](UsageEvent::window_reset) the pause lifts at, so an
/// observer knows when work resumes without polling.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageEvent {
    /// The window's fill against the shared subscription limit, as a percent
    /// (`0..=100`).
    pub percent: u8,
    /// When the usage window resets and the auto-pause lifts. `Some` on a
    /// pause, `None` when the pause lifts (the operator resumed early).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_reset: Option<Timestamp>,
    /// Whether the crew is auto-paused on usage: `true` when this reading
    /// engaged the pause, `false` when it lifted (the window reset, or the
    /// operator resumed early).
    pub paused: bool,
}

/// A coordination stall the fleet-wide monitor detected, or its resolution
/// (issue #48, surfaced on the stream by issue #120).
///
/// The defibrillator's stall monitor reads the event stream for a crew stuck
/// waiting on itself and escalates the specific cause. Publishing it as a
/// first-class `stall` event lets `crew notify` push the "a role is stalled"
/// moment (issue #52) and the `crew top` cockpit (issue #51) render live
/// stalls, rather than the stall living only in a supervisor log. The paired
/// [`status`](StallEvent::status) says whether this reading raised the stall or
/// cleared it, so a watcher can light it up and later clear it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StallEvent {
    /// Which shape of stall this is.
    pub kind: StallKind,
    /// Whether the stall was just detected or has since resolved.
    pub status: StallStatus,
    /// The roles caught in the stall, sorted for a stable identity across
    /// scans.
    pub roles: Vec<RoleId>,
    /// A one-line, specific description of the stall: who is waiting on what.
    pub detail: String,
}

/// The shape of a coordination stall (issue #48).
///
/// A [`Deadlock`](Self::Deadlock) is a cycle of unanswered questions (the crew
/// waiting on itself), an [`UnansweredQuestion`](Self::UnansweredQuestion) is a
/// one-sided wait past the threshold with no cycle, and a
/// [`LedgerStall`](Self::LedgerStall) is a held task with no forward motion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StallKind {
    /// A cycle of unanswered questions: the crew is waiting on itself.
    Deadlock,
    /// One agent has waited past the threshold for another to answer.
    UnansweredQuestion,
    /// A task sits in a held state past the threshold, with no forward motion.
    LedgerStall,
}

impl StallKind {
    /// The stall's stable label, matching its serialized name.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Deadlock => "deadlock",
            Self::UnansweredQuestion => "unanswered_question",
            Self::LedgerStall => "ledger_stall",
        }
    }
}

/// Whether a [`StallEvent`] raises a stall or clears it (issue #120).
///
/// The monitor publishes [`Detected`](Self::Detected) when a stall first
/// crosses the threshold and [`Resolved`](Self::Resolved) once it clears, so a
/// front-end lights a stall up and later takes it down off the same stream.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StallStatus {
    /// The stall was just detected: the crew is stuck and needs the General.
    Detected,
    /// The stall has resolved: the crew moved on, so a watcher can clear it.
    Resolved,
}

impl BoardSection {
    /// The section's stable label, matching its serialized name.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Decision => "decision",
            Self::Interface => "interface",
            Self::Gotcha => "gotcha",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        Activity, ArtifactKind, BoardEvent, BoardSection, Event, EventKind, Lifecycle, Message,
        MessageKind, StallEvent, StallKind, StallStatus, Verdict, VerificationEvent,
    };
    use crate::{
        id::{ChannelId, MessageId, RoleId, Sender, TaskId},
        time::Timestamp,
    };

    /// One representative event per kind, to exercise every serde path.
    #[expect(
        clippy::too_many_lines,
        reason = "a flat one-event-per-kind fixture is inherently long but clearer as one list"
    )]
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
            envelope(message(MessageKind::Answer {
                in_reply_to: MessageId::new(),
            })),
            envelope(message(MessageKind::Status)),
            envelope(message(MessageKind::Artifact {
                reference: "https://github.com/JalapenoLabs/crew/pull/8".to_owned(),
                artifact_kind: ArtifactKind::PullRequest,
            })),
            envelope(message(MessageKind::Note)),
            envelope(message(MessageKind::Redirect)),
            envelope(message(MessageKind::Belay)),
            envelope(EventKind::Lifecycle(Lifecycle::Started)),
            envelope(EventKind::Lifecycle(Lifecycle::Died)),
            envelope(EventKind::Lifecycle(Lifecycle::Paused)),
            envelope(EventKind::Lifecycle(Lifecycle::Resumed)),
            envelope(EventKind::Lifecycle(Lifecycle::StoodDown)),
            envelope(EventKind::Lifecycle(Lifecycle::MissionComplete)),
            envelope(EventKind::Activity(Activity::TurnStarted)),
            envelope(EventKind::Activity(Activity::ToolCall {
                tool: "cargo".to_owned(),
            })),
            envelope(EventKind::Activity(Activity::Output {
                text: "build succeeded".to_owned(),
            })),
            envelope(EventKind::Verification(VerificationEvent {
                task: "Scaffold the broker".to_owned(),
                owner: RoleId::new("backend"),
                verifier: None,
                verdict: Verdict::Submitted,
                detail: "crewd serves /health".to_owned(),
            })),
            envelope(EventKind::Verification(VerificationEvent {
                task: "Scaffold the broker".to_owned(),
                owner: RoleId::new("backend"),
                verifier: Some(RoleId::new("qa")),
                verdict: Verdict::Passed,
                detail: String::new(),
            })),
            envelope(EventKind::Verification(VerificationEvent {
                task: "Scaffold the broker".to_owned(),
                owner: RoleId::new("backend"),
                verifier: Some(RoleId::new("qa")),
                verdict: Verdict::Failed,
                detail: "/health returns 500 under load".to_owned(),
            })),
            envelope(EventKind::Board(BoardEvent {
                key: "auth-strategy".to_owned(),
                section: BoardSection::Decision,
                author: RoleId::new("commander"),
                body: "JWT with 15m access tokens; rationale: stateless, matches the gateway."
                    .to_owned(),
                retracted: false,
            })),
            envelope(EventKind::Board(BoardEvent {
                key: "auth-strategy".to_owned(),
                section: BoardSection::Decision,
                author: RoleId::new("commander"),
                body: String::new(),
                retracted: true,
            })),
            envelope(EventKind::Stall(StallEvent {
                kind: StallKind::Deadlock,
                status: StallStatus::Detected,
                roles: vec![RoleId::new("backend"), RoleId::new("frontend")],
                detail: "deadlock: backend waits on frontend, and frontend waits on backend"
                    .to_owned(),
            })),
            envelope(EventKind::Stall(StallEvent {
                kind: StallKind::LedgerStall,
                status: StallStatus::Resolved,
                roles: vec![RoleId::new("backend")],
                detail: "ledger task `login` moved forward".to_owned(),
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
    fn directives_are_the_redirect_and_belay_kinds() {
        assert!(
            MessageKind::Redirect.is_directive(),
            "a redirect is a directive"
        );
        assert!(MessageKind::Belay.is_directive(), "a belay is a directive");
        for kind in [
            MessageKind::Note,
            MessageKind::Status,
            MessageKind::Answer {
                in_reply_to: MessageId::new(),
            },
            MessageKind::Question { options: vec![] },
        ] {
            assert!(!kind.is_directive(), "{kind:?} is not a directive");
        }
    }

    #[test]
    fn directives_serialize_with_their_snake_case_tag() {
        let redirect = serde_json::to_value(MessageKind::Redirect).unwrap();
        assert_eq!(redirect, serde_json::json!({ "kind": "redirect" }));
        let belay = serde_json::to_value(MessageKind::Belay).unwrap();
        assert_eq!(belay, serde_json::json!({ "kind": "belay" }));
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
