//! The cockpit state: a pure model of the crew, folded from the roster and the
//! event stream (issue #51).
//!
//! [`Cockpit`] holds everything `crew top` renders: each role with its status,
//! current action, tokens, and cost; the recent message flow; and the crew
//! header (the live count and the aggregate spend). It is seeded once from the
//! `/roster` and `/stats` snapshots ([`seed_roster`](Cockpit::seed_roster),
//! [`seed_stats`](Cockpit::seed_stats)) and then advanced by folding each live
//! `/stream` event ([`apply`](Cockpit::apply)): a `lifecycle` event moves a
//! role's status, an `activity` event sets its current action, a `telemetry`
//! event adds to its tokens and cost, and a `message` event lands on the feed.
//! It is a rendering of the stream and the roster, so it captures nothing new.
//!
//! The model is pure and sans-io: it takes typed snapshots and events, so the
//! whole "does the cockpit reflect the crew" question is a unit test, and the
//! ratatui shell ([`super::render`]) is a thin projection of it. The
//! interaction state (the selected role, the drill-in, the role and channel
//! filters) lives here too, so the shell only translates key presses into
//! calls.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crew_substrate::core::{
    Activity, Event, EventKind, Lifecycle, Message, MessageKind, RoleId, Sender,
};
use serde::Deserialize;

/// The most message-flow lines the cockpit keeps, so a long-running crew never
/// grows the feed without bound.
const FEED_CAP: usize = 500;

/// The most recent activity lines the cockpit keeps per role, for the drill-in.
const ACTIVITY_CAP: usize = 200;

/// The most characters a rendered summary or action carries before it is
/// elided.
const MAX_SUMMARY: usize = 200;

/// A role's live status in the cockpit: the roster's liveness, folded from
/// `lifecycle` events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Status {
    /// Up and working.
    Working,
    /// Registered but idle, its process stopped to save context and money.
    Idle,
    /// Cleanly stood down, its roster entry kept for a fast restart.
    Stopped,
    /// Gave up after exhausting its restart budget on repeated crashes.
    Dead,
}

impl Status {
    /// Parses the roster's wire liveness label, defaulting an unknown one to
    /// [`Stopped`](Status::Stopped) so a role is never shown as live on a
    /// guess.
    fn from_liveness(label: &str) -> Self {
        match label {
            "working" => Status::Working,
            "idle" => Status::Idle,
            "dead" => Status::Dead,
            _ => Status::Stopped,
        }
    }

    /// The status a `lifecycle` transition moves a role to, if it changes the
    /// liveness.
    ///
    /// `paused` / `resumed` toggle a flag rather than the status, and
    /// `stood_down` / `mission_complete` are crew-wide signals, so they return
    /// `None` (the caller handles them).
    fn from_lifecycle(lifecycle: Lifecycle) -> Option<Self> {
        match lifecycle {
            Lifecycle::Started | Lifecycle::Restarted | Lifecycle::Recovered => {
                Some(Status::Working)
            }
            Lifecycle::Idle => Some(Status::Idle),
            Lifecycle::Stopped => Some(Status::Stopped),
            Lifecycle::Died => Some(Status::Dead),
            Lifecycle::Paused
            | Lifecycle::Resumed
            | Lifecycle::StoodDown
            | Lifecycle::MissionComplete => None,
        }
    }

    /// Whether a role in this status counts toward the live agent count (issue
    /// #32): working or idle, present and up.
    fn is_live(self) -> bool {
        matches!(self, Status::Working | Status::Idle)
    }

    /// The short label shown in the roles table.
    pub(crate) fn label(self) -> &'static str {
        match self {
            Status::Working => "working",
            Status::Idle => "idle",
            Status::Stopped => "stopped",
            Status::Dead => "dead",
        }
    }
}

/// One role's row in the cockpit: its status, current action, and spend.
#[derive(Debug, Clone)]
pub(crate) struct RoleRow {
    /// The role this row is for.
    pub role: RoleId,
    /// Its live status.
    pub status: Status,
    /// Whether the role is individually paused (issue #41), an overlay on the
    /// status.
    pub paused: bool,
    /// What the role is doing now, from its latest `activity` event; empty
    /// until one lands.
    pub action: String,
    /// Cumulative tokens spent.
    pub tokens: u64,
    /// Cumulative cost in micro-USD (millionths of a dollar).
    pub cost_micro_usd: u64,
    /// Working time in whole seconds, as of the last snapshot.
    pub active_secs: u64,
    /// The paths the role owns, its lane.
    pub owned_paths: Vec<String>,
}

impl RoleRow {
    /// A fresh row for a role first seen on the stream, before any snapshot.
    fn new(role: RoleId, status: Status) -> Self {
        Self {
            role,
            status,
            paused: false,
            action: String::new(),
            tokens: 0,
            cost_micro_usd: 0,
            active_secs: 0,
            owned_paths: Vec::new(),
        }
    }
}

/// One line in the message feed: who sent it, where, its kind, and a summary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FeedLine {
    /// Who sent it: a role's id, or `general`.
    pub from: String,
    /// The channel it was sent on.
    pub channel: String,
    /// The message kind (order, question, note, ...).
    pub kind: &'static str,
    /// A one-line summary of the message.
    pub summary: String,
}

/// The filter narrowing the message feed: nothing, one role's traffic, or one
/// channel.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Filter {
    /// The whole feed.
    All,
    /// Only lines involving `role`: sent by it, or on its own channel.
    Role(RoleId),
    /// Only lines on `channel`.
    Channel(String),
}

/// The typed `/roster` snapshot the cockpit seeds from (issue #32).
///
/// The roster's `count` is not read here: the live count is a projection of the
/// roles' statuses ([`live_count`](Cockpit::live_count)), so it stays
/// consistent as events fold in, rather than being seeded and drifting.
#[derive(Debug, Deserialize)]
pub(crate) struct RosterSeed {
    /// The crew's control standing: `running`, `paused`, or `stood_down`.
    #[serde(default)]
    pub standing: String,
    /// The registered roles.
    #[serde(default)]
    pub roles: Vec<RosterRoleSeed>,
}

/// One role in the `/roster` snapshot.
#[derive(Debug, Deserialize)]
pub(crate) struct RosterRoleSeed {
    /// The role's id.
    pub role: RoleId,
    /// The role's current liveness (`working` / `idle` / `stopped` / `dead`).
    pub liveness: String,
    /// Whether the role is individually paused.
    #[serde(default)]
    pub paused: bool,
    /// The paths the role owns.
    #[serde(default)]
    pub owned_paths: Vec<String>,
}

/// The typed `/stats` snapshot the cockpit seeds tokens and cost from (issue
/// #55).
#[derive(Debug, Default, Deserialize)]
pub(crate) struct StatsSeed {
    /// The per-role rollup.
    #[serde(default)]
    pub roles: Vec<StatsRoleSeed>,
}

/// One role's tokens / cost / time in the `/stats` snapshot.
#[derive(Debug, Deserialize)]
pub(crate) struct StatsRoleSeed {
    /// The role.
    pub role: RoleId,
    /// Cumulative tokens spent.
    #[serde(default)]
    pub tokens: u64,
    /// Cumulative cost in micro-USD.
    #[serde(default)]
    pub cost_micro_usd: u64,
    /// Working time in whole seconds.
    #[serde(default)]
    pub active_secs: u64,
}

/// The whole cockpit state: the folded crew plus the interaction state.
#[derive(Debug)]
pub(crate) struct Cockpit {
    /// Every role, keyed and sorted by id.
    roles: BTreeMap<RoleId, RoleRow>,
    /// The recent message flow, oldest first, capped at [`FEED_CAP`].
    feed: VecDeque<FeedLine>,
    /// Each role's recent activity, for the drill-in view.
    activity: BTreeMap<RoleId, VecDeque<String>>,
    /// The crew's control standing, for the header.
    standing: String,
    /// The selected row in the roles table (an index into
    /// [`roles`](Self::roles) order), for the highlight and the drill-in
    /// target.
    selected: usize,
    /// Whether the drill-in detail view is showing.
    detail: bool,
    /// The active feed filter.
    filter: Filter,
}

impl Default for Cockpit {
    fn default() -> Self {
        Self {
            roles: BTreeMap::new(),
            feed: VecDeque::new(),
            activity: BTreeMap::new(),
            standing: "running".to_owned(),
            selected: 0,
            detail: false,
            filter: Filter::All,
        }
    }
}

impl Cockpit {
    /// Seeds the roles and the standing from a `/roster` snapshot (issue #32).
    pub fn seed_roster(&mut self, seed: RosterSeed) {
        if !seed.standing.is_empty() {
            self.standing = seed.standing;
        }
        for role in seed.roles {
            let entry = self
                .roles
                .entry(role.role.clone())
                .or_insert_with(|| RoleRow::new(role.role.clone(), Status::Stopped));
            entry.status = Status::from_liveness(&role.liveness);
            entry.paused = role.paused;
            entry.owned_paths = role.owned_paths;
        }
    }

    /// Seeds each role's tokens, cost, and working time from a `/stats`
    /// snapshot (issue #55).
    pub fn seed_stats(&mut self, seed: StatsSeed) {
        for role in seed.roles {
            let entry = self
                .roles
                .entry(role.role.clone())
                .or_insert_with(|| RoleRow::new(role.role.clone(), Status::Stopped));
            entry.tokens = role.tokens;
            entry.cost_micro_usd = role.cost_micro_usd;
            entry.active_secs = role.active_secs;
        }
    }

    /// Folds one live `/stream` event into the state (issue #51).
    ///
    /// A `lifecycle` event moves the sender's status (and the crew standing on
    /// a stand-down or mission-complete); an `activity` event sets the
    /// sender's current action and appends to its drill-in log; a
    /// `telemetry` event adds to the role's tokens and cost; a `message`
    /// event lands on the feed. Other kinds do not change the cockpit.
    pub fn apply(&mut self, event: &Event) {
        match &event.kind {
            EventKind::Lifecycle(lifecycle) => self.apply_lifecycle(&event.from, *lifecycle),
            EventKind::Activity(activity) => self.apply_activity(&event.from, activity),
            EventKind::Telemetry(telemetry) => {
                let row = self.role_row(&telemetry.role);
                row.tokens = row.tokens.saturating_add(telemetry.tokens);
                row.cost_micro_usd = row.cost_micro_usd.saturating_add(telemetry.cost_micro_usd);
            }
            EventKind::Message(message) => self.push_feed(event, message),
            _ => {}
        }
    }

    /// Folds a `lifecycle` event: move the role's status, toggle its pause, or
    /// update the crew standing.
    fn apply_lifecycle(&mut self, from: &Sender, lifecycle: Lifecycle) {
        if let Sender::Role(role) = from {
            let row = self.role_row(role);
            if let Some(status) = Status::from_lifecycle(lifecycle) {
                row.status = status;
            }
            match lifecycle {
                Lifecycle::Paused => row.paused = true,
                Lifecycle::Resumed => row.paused = false,
                _ => {}
            }
        }
        match lifecycle {
            Lifecycle::StoodDown => "stood down".clone_into(&mut self.standing),
            Lifecycle::MissionComplete => "mission complete".clone_into(&mut self.standing),
            _ => {}
        }
    }

    /// Folds an `activity` event: set the role's current action and append to
    /// its drill-in log.
    fn apply_activity(&mut self, from: &Sender, activity: &Activity) {
        let Sender::Role(role) = from else { return };
        let action = describe_activity(activity);
        self.role_row(role).action.clone_from(&action);
        let log = self.activity.entry(role.clone()).or_default();
        log.push_back(action);
        while log.len() > ACTIVITY_CAP {
            log.pop_front();
        }
    }

    /// Appends a `message` event to the feed, capped at [`FEED_CAP`].
    fn push_feed(&mut self, event: &Event, message: &Message) {
        self.feed.push_back(FeedLine {
            from: sender_label(&event.from),
            channel: event.channel.as_str().to_owned(),
            kind: message_kind_label(&message.kind),
            summary: message_summary(message),
        });
        while self.feed.len() > FEED_CAP {
            self.feed.pop_front();
        }
    }

    /// The row for `role`, inserting a fresh one (seen first on the stream) if
    /// absent.
    fn role_row(&mut self, role: &RoleId) -> &mut RoleRow {
        self.roles
            .entry(role.clone())
            .or_insert_with(|| RoleRow::new(role.clone(), Status::Working))
    }

    // --- Queries the renderer reads -------------------------------------------

    /// Every role, sorted by id.
    pub fn roles(&self) -> Vec<&RoleRow> {
        self.roles.values().collect()
    }

    /// The number of roles the cockpit tracks.
    pub fn role_count(&self) -> usize {
        self.roles.len()
    }

    /// The live agent count: the roles that are working or idle (issue #32).
    pub fn live_count(&self) -> usize {
        self.roles
            .values()
            .filter(|row| row.status.is_live())
            .count()
    }

    /// The crew's aggregate tokens and cost, summed across every role.
    pub fn aggregate(&self) -> (u64, u64) {
        self.roles.values().fold((0, 0), |(tokens, cost), row| {
            (
                tokens.saturating_add(row.tokens),
                cost.saturating_add(row.cost_micro_usd),
            )
        })
    }

    /// The crew's control standing, for the header.
    pub fn standing(&self) -> &str {
        &self.standing
    }

    /// The message feed under the active filter, oldest first.
    pub fn feed(&self) -> Vec<&FeedLine> {
        self.feed
            .iter()
            .filter(|line| self.feed_shows(line))
            .collect()
    }

    /// A label for the active filter, for the header (`all`, `role:backend`,
    /// `channel:@backend`).
    pub fn filter_label(&self) -> String {
        match &self.filter {
            Filter::All => "all".to_owned(),
            Filter::Role(role) => format!("role:{role}"),
            Filter::Channel(channel) => format!("channel:{channel}"),
        }
    }

    /// The selected row index, clamped to the current roles.
    pub fn selected(&self) -> usize {
        self.selected.min(self.roles.len().saturating_sub(1))
    }

    /// The selected role, if any role is present.
    pub fn selected_role(&self) -> Option<&RoleRow> {
        self.roles().into_iter().nth(self.selected())
    }

    /// Whether the drill-in detail view is showing.
    pub fn in_detail(&self) -> bool {
        self.detail && self.selected_role().is_some()
    }

    /// The selected role's recent activity, oldest first, for the drill-in.
    pub fn detail_activity(&self) -> Vec<&str> {
        self.selected_role()
            .and_then(|row| self.activity.get(&row.role))
            .map(|log| log.iter().map(String::as_str).collect())
            .unwrap_or_default()
    }

    /// Whether `line` is shown under the active filter.
    fn feed_shows(&self, line: &FeedLine) -> bool {
        match &self.filter {
            Filter::All => true,
            Filter::Role(role) => {
                line.from == role.as_str() || channel_involves(&line.channel, role.as_str())
            }
            Filter::Channel(channel) => &line.channel == channel,
        }
    }

    // --- Interaction the shell drives -----------------------------------------

    /// Moves the selection down one role (wrapping at the end).
    pub fn select_next(&mut self) {
        let count = self.roles.len();
        if count > 0 {
            self.selected = (self.selected() + 1) % count;
        }
    }

    /// Moves the selection up one role (wrapping at the start).
    pub fn select_prev(&mut self) {
        let count = self.roles.len();
        if count > 0 {
            self.selected = (self.selected() + count - 1) % count;
        }
    }

    /// Toggles the drill-in detail view for the selected role.
    pub fn toggle_detail(&mut self) {
        if self.selected_role().is_some() {
            self.detail = !self.detail;
        }
    }

    /// Leaves the detail view; returns whether it was showing (so a shell can
    /// treat Esc as "back out, then quit").
    pub fn leave_detail(&mut self) -> bool {
        let was = self.detail;
        self.detail = false;
        was
    }

    /// Toggles the feed filter to the selected role's traffic, or back to all.
    pub fn toggle_role_filter(&mut self) {
        let selected = self.selected_role().map(|row| row.role.clone());
        self.filter = match (&self.filter, selected) {
            (Filter::Role(role), Some(selected)) if role == &selected => Filter::All,
            (_, Some(selected)) => Filter::Role(selected),
            (_, None) => Filter::All,
        };
    }

    /// Cycles the feed filter through the channels seen in the feed: off, then
    /// each channel in turn, then off again.
    pub fn cycle_channel_filter(&mut self) {
        let channels = self.feed_channels();
        if channels.is_empty() {
            self.filter = Filter::All;
            return;
        }
        let current = match &self.filter {
            Filter::Channel(channel) => channels.iter().position(|c| c == channel),
            _ => None,
        };
        self.filter = match current {
            None => Filter::Channel(channels[0].clone()),
            Some(index) if index + 1 < channels.len() => {
                Filter::Channel(channels[index + 1].clone())
            }
            Some(_) => Filter::All,
        };
    }

    /// Clears any filter, showing the whole feed.
    pub fn clear_filter(&mut self) {
        self.filter = Filter::All;
    }

    /// The distinct channels present in the feed, sorted, for cycling.
    fn feed_channels(&self) -> Vec<String> {
        self.feed
            .iter()
            .map(|line| line.channel.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }
}

/// A sender's label: a role's id, or `general`.
fn sender_label(from: &Sender) -> String {
    match from {
        Sender::Role(role) => role.as_str().to_owned(),
        Sender::General => "general".to_owned(),
    }
}

/// A one-line description of an activity, for the action column and the
/// drill-in.
fn describe_activity(activity: &Activity) -> String {
    match activity {
        Activity::TurnStarted => "turn started".to_owned(),
        Activity::TurnEnded => "turn ended".to_owned(),
        Activity::ToolCall { tool } => format!("tool: {tool}"),
        Activity::Output { text } => elide(text),
        Activity::Other { raw } => format!("({raw})"),
    }
}

/// The wire label for a message's kind (matching its serde name).
fn message_kind_label(kind: &MessageKind) -> &'static str {
    match kind {
        MessageKind::Order { .. } => "order",
        MessageKind::Question { .. } => "question",
        MessageKind::Answer { .. } => "answer",
        MessageKind::Status => "status",
        MessageKind::Artifact { .. } => "artifact",
        MessageKind::Note => "note",
        MessageKind::Redirect => "redirect",
        MessageKind::Belay => "belay",
        MessageKind::ApprovalRequest { .. } => "approval_request",
        MessageKind::ApprovalDecision { .. } => "approval_decision",
    }
}

/// A one-line summary of a message: its title for an order, else its body.
fn message_summary(message: &Message) -> String {
    let body = message.body.trim();
    match &message.kind {
        MessageKind::Order { title, .. } if !title.is_empty() => {
            if body.is_empty() {
                title.clone()
            } else {
                elide(&format!("{title}: {body}"))
            }
        }
        _ => elide(body),
    }
}

/// Whether a channel involves `role`: its direct `@role` channel, or a pair it
/// belongs to.
fn channel_involves(channel: &str, role: &str) -> bool {
    if channel == format!("@{role}") {
        return true;
    }
    // A pair channel `a+b` involves the role when it is one of the two members.
    channel
        .split_once('+')
        .is_some_and(|(first, second)| first == role || second == role)
}

/// Truncates `text` to one tidy line: whitespace collapsed and capped at
/// [`MAX_SUMMARY`] with an ellipsis.
fn elide(text: &str) -> String {
    let flat = text.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.chars().count() <= MAX_SUMMARY {
        return flat;
    }
    let mut out: String = flat.chars().take(MAX_SUMMARY).collect();
    out.push('\u{2026}');
    out
}

#[cfg(test)]
mod tests {
    use crew_substrate::core::{
        ArtifactKind, ChannelId, Event, EventKind, Lifecycle, Message, MessageId, MessageKind,
        RoleId, Sender, TelemetryEvent, Timestamp,
    };

    use super::{Cockpit, Filter, StatsSeed, Status};

    /// An event on `channel` from `from` carrying `kind`.
    fn event(from: Sender, channel: &str, kind: EventKind) -> Event {
        Event {
            ts: Timestamp::now(),
            from,
            channel: ChannelId::new(channel),
            task: None,
            kind,
        }
    }

    fn role(name: &str) -> Sender {
        Sender::Role(RoleId::new(name))
    }

    fn message(kind: MessageKind, body: &str) -> EventKind {
        EventKind::Message(Message {
            id: MessageId::new(),
            kind,
            body: body.to_owned(),
        })
    }

    fn seeded() -> Cockpit {
        let mut cockpit = Cockpit::default();
        cockpit.seed_roster(
            serde_json::from_value(serde_json::json!({
                "standing": "running",
                "count": { "live": 2 },
                "roles": [
                    { "role": "commander", "liveness": "working", "owned_paths": [] },
                    { "role": "backend", "liveness": "idle", "owned_paths": ["api/"] }
                ]
            }))
            .unwrap(),
        );
        cockpit.seed_stats(
            serde_json::from_value::<StatsSeed>(serde_json::json!({
                "roles": [ { "role": "backend", "tokens": 1000, "cost_micro_usd": 30000, "active_secs": 42 } ]
            }))
            .unwrap(),
        );
        cockpit
    }

    #[test]
    fn seeding_reflects_the_roster_and_stats() {
        let cockpit = seeded();
        assert_eq!(cockpit.role_count(), 2, "both roles are tracked");
        assert_eq!(cockpit.live_count(), 2, "working + idle are live");
        assert_eq!(cockpit.standing(), "running");

        let backend = cockpit
            .roles()
            .into_iter()
            .find(|row| row.role == RoleId::new("backend"))
            .unwrap();
        assert_eq!(backend.status, Status::Idle);
        assert_eq!(backend.tokens, 1000, "stats seeded the tokens");
        assert_eq!(backend.cost_micro_usd, 30000);
        assert_eq!(backend.owned_paths, ["api/"]);
        assert_eq!(cockpit.aggregate(), (1000, 30000));
    }

    #[test]
    fn a_lifecycle_event_moves_a_role_status_and_the_live_count() {
        let mut cockpit = seeded();
        // backend goes to working, then dies.
        cockpit.apply(&event(
            role("backend"),
            "all-units",
            EventKind::Lifecycle(Lifecycle::Started),
        ));
        assert_eq!(cockpit.roles()[0].role, RoleId::new("backend"));
        let status_of = |cockpit: &Cockpit, name: &str| {
            cockpit
                .roles()
                .into_iter()
                .find(|row| row.role == RoleId::new(name))
                .unwrap()
                .status
        };
        assert_eq!(status_of(&cockpit, "backend"), Status::Working);
        assert_eq!(cockpit.live_count(), 2);

        cockpit.apply(&event(
            role("backend"),
            "all-units",
            EventKind::Lifecycle(Lifecycle::Died),
        ));
        assert_eq!(status_of(&cockpit, "backend"), Status::Dead);
        assert_eq!(cockpit.live_count(), 1, "a dead role is not live");
    }

    #[test]
    fn a_role_first_seen_on_the_stream_is_added() {
        let mut cockpit = seeded();
        cockpit.apply(&event(
            role("scout"),
            "all-units",
            EventKind::Lifecycle(Lifecycle::Started),
        ));
        assert_eq!(
            cockpit.role_count(),
            3,
            "a role that registers after boot appears"
        );
        assert!(cockpit
            .roles()
            .iter()
            .any(|r| r.role == RoleId::new("scout")));
    }

    #[test]
    fn pause_and_resume_toggle_the_flag_without_dropping_the_status() {
        let mut cockpit = seeded();
        cockpit.apply(&event(
            role("backend"),
            "all-units",
            EventKind::Lifecycle(Lifecycle::Paused),
        ));
        let backend = cockpit
            .roles()
            .into_iter()
            .find(|r| r.role == RoleId::new("backend"))
            .unwrap()
            .clone();
        assert!(backend.paused, "paused sets the flag");
        assert_eq!(
            backend.status,
            Status::Idle,
            "the status is unchanged by a pause"
        );

        cockpit.apply(&event(
            role("backend"),
            "all-units",
            EventKind::Lifecycle(Lifecycle::Resumed),
        ));
        let backend = cockpit
            .roles()
            .into_iter()
            .find(|r| r.role == RoleId::new("backend"))
            .unwrap()
            .clone();
        assert!(!backend.paused, "resumed clears the flag");
    }

    #[test]
    fn an_activity_event_sets_the_current_action_and_the_drill_in_log() {
        use crew_substrate::core::Activity;
        let mut cockpit = seeded();
        cockpit.apply(&event(
            role("backend"),
            "@backend",
            EventKind::Activity(Activity::ToolCall {
                tool: "Read".to_owned(),
            }),
        ));
        cockpit.apply(&event(
            role("backend"),
            "@backend",
            EventKind::Activity(Activity::TurnEnded),
        ));

        let backend = cockpit
            .roles()
            .into_iter()
            .find(|r| r.role == RoleId::new("backend"))
            .unwrap()
            .clone();
        assert_eq!(
            backend.action, "turn ended",
            "the latest activity is the current action"
        );

        // Select backend and drill in; its log carries both activities, in order.
        while cockpit.selected_role().map(|r| r.role.clone()) != Some(RoleId::new("backend")) {
            cockpit.select_next();
        }
        cockpit.toggle_detail();
        assert!(cockpit.in_detail());
        assert_eq!(cockpit.detail_activity(), ["tool: Read", "turn ended"]);
    }

    #[test]
    fn a_telemetry_event_accumulates_tokens_and_cost() {
        let mut cockpit = seeded();
        cockpit.apply(&event(
            role("backend"),
            "all-units",
            EventKind::Telemetry(TelemetryEvent {
                role: RoleId::new("backend"),
                tokens: 500,
                cost_micro_usd: 15000,
            }),
        ));
        let backend = cockpit
            .roles()
            .into_iter()
            .find(|r| r.role == RoleId::new("backend"))
            .unwrap()
            .clone();
        assert_eq!(
            backend.tokens, 1500,
            "live telemetry adds to the seeded 1000"
        );
        assert_eq!(backend.cost_micro_usd, 45000);
        assert_eq!(cockpit.aggregate(), (1500, 45000));
    }

    #[test]
    fn a_message_event_lands_on_the_feed_and_is_filterable() {
        let mut cockpit = seeded();
        cockpit.apply(&event(
            role("commander"),
            "@backend",
            message(
                MessageKind::Order {
                    title: "build login".to_owned(),
                    scope: String::new(),
                    owned_paths: vec![],
                    acceptance: String::new(),
                },
                "",
            ),
        ));
        cockpit.apply(&event(
            role("backend"),
            "@commander",
            message(MessageKind::Status, "on it"),
        ));
        cockpit.apply(&event(
            Sender::General,
            "all-units",
            message(MessageKind::Note, "all hands"),
        ));

        assert_eq!(cockpit.feed().len(), 3, "every message is on the feed");
        let first = cockpit.feed()[0];
        assert_eq!(first.from, "commander");
        assert_eq!(first.channel, "@backend");
        assert_eq!(first.kind, "order");
        assert_eq!(first.summary, "build login");

        // Filter to backend: its own status message and the order addressed to it.
        cockpit.filter = Filter::Role(RoleId::new("backend"));
        let backend_feed = cockpit.feed();
        assert_eq!(
            backend_feed.len(),
            2,
            "the order to @backend and backend's own message"
        );
        assert!(backend_feed
            .iter()
            .all(|line| line.from == "backend" || line.channel == "@backend"));

        // Filter to a channel: only lines on it.
        cockpit.filter = Filter::Channel("all-units".to_owned());
        assert_eq!(cockpit.feed().len(), 1);
        assert_eq!(cockpit.feed()[0].summary, "all hands");
    }

    #[test]
    fn the_feed_is_bounded() {
        let mut cockpit = seeded();
        for n in 0..(super::FEED_CAP + 50) {
            cockpit.apply(&event(
                role("backend"),
                "all-units",
                message(MessageKind::Note, &format!("note {n}")),
            ));
        }
        assert_eq!(
            cockpit.feed().len(),
            super::FEED_CAP,
            "the feed never grows past its cap"
        );
        assert_eq!(
            cockpit.feed().last().unwrap().summary,
            format!("note {}", super::FEED_CAP + 49),
            "the newest message is kept",
        );
    }

    #[test]
    fn selection_wraps_and_the_role_filter_toggles() {
        let mut cockpit = seeded(); // commander, backend (sorted)
        assert_eq!(
            cockpit.selected_role().unwrap().role,
            RoleId::new("backend"),
            "backend sorts first"
        );
        cockpit.select_next();
        assert_eq!(
            cockpit.selected_role().unwrap().role,
            RoleId::new("commander")
        );
        cockpit.select_next();
        assert_eq!(
            cockpit.selected_role().unwrap().role,
            RoleId::new("backend"),
            "selection wraps"
        );

        // Toggling the role filter targets the selected role, then clears.
        cockpit.toggle_role_filter();
        assert_eq!(cockpit.filter_label(), "role:backend");
        cockpit.toggle_role_filter();
        assert_eq!(cockpit.filter_label(), "all");
    }

    #[test]
    fn the_channel_filter_cycles_through_the_feed_channels() {
        let mut cockpit = seeded();
        cockpit.apply(&event(
            role("backend"),
            "@commander",
            message(MessageKind::Status, "a"),
        ));
        cockpit.apply(&event(
            role("commander"),
            "all-units",
            message(MessageKind::Note, "b"),
        ));
        // Channels seen, sorted: "@commander", "all-units".
        cockpit.cycle_channel_filter();
        assert_eq!(cockpit.filter_label(), "channel:@commander");
        cockpit.cycle_channel_filter();
        assert_eq!(cockpit.filter_label(), "channel:all-units");
        cockpit.cycle_channel_filter();
        assert_eq!(
            cockpit.filter_label(),
            "all",
            "cycling past the last channel clears the filter"
        );
    }

    #[test]
    fn a_non_cockpit_event_kind_is_ignored() {
        let mut cockpit = seeded();
        let before = cockpit.feed().len();
        // An artifact message still lands (it is a message), but a board event does
        // not.
        cockpit.apply(&event(
            role("backend"),
            "@commander",
            message(
                MessageKind::Artifact {
                    reference: "pr/8".to_owned(),
                    artifact_kind: ArtifactKind::PullRequest,
                },
                "",
            ),
        ));
        assert_eq!(cockpit.feed().len(), before + 1);
    }
}
