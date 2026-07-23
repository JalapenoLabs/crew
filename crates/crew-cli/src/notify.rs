//! Push notifications on the actionable moments (`crew notify`).
//!
//! `crew notify` lets the General walk away and be pulled back only when it
//! matters. It subscribes to the broker firehose (`GET /stream`, the same event
//! stream every reader consumes, so there is no separate signal path) and, for
//! each event, decides whether it is an *actionable moment*: something that
//! changes what the General would do next. On a match it fires a push; routine
//! chatter passes silently.
//!
//! ## Actionable moments
//!
//! The moments below are the ones the event stream carries today. Each maps to
//! one event kind, classified by [`moment_of`]:
//!
//! - **A General-facing question is asked** ([`MessageKind::Question`]): a role
//!   wants a decision the General would field, rather than peer coordination.
//! - **A role dies** ([`Lifecycle::Died`]): a role crashed or hung past
//!   recovery.
//! - **The crew stands down** ([`Lifecycle::StoodDown`]): every role halts and
//!   the mission is on hold.
//!
//! ## Which questions reach the General
//!
//! Not every question needs the General: a peer loop (`@backend` asking a live
//! `@frontend`) is coordination the crew resolves on its own, so pushing on it
//! would drown the General in chatter that is not theirs to answer. A question
//! is **General-facing** only when it is broadcast to `all-units`, or addressed
//! to a role that is not a live agent (stopped, dead, or never in the unit).
//! This mirrors the stall monitor's rule (issue #48): a directed question to a
//! live teammate is a wait on the crew, not a wait on the General.
//!
//! To tell the two apart the notifier tracks roster liveness from the
//! `lifecycle` events on the same stream ([`Roster`]): a role is live while it
//! is working or idle, and drops out when it stops or dies. The firehose is
//! live-only, so the roster reflects the events seen since `crew notify`
//! connected; an addressee not yet known to be live is treated as
//! General-facing, so a real question is never silently dropped.
//!
//! Everything else (status, notes, orders, answers, artifacts, ordinary
//! lifecycle such as `started` or `idle`, activity, board, boundary, and
//! verification events) is routine and stays quiet by default. Two further
//! moments in the issue's scope, an approval pending (issue #40) and a role
//! stalled (surfaced on the stream as a later refinement of issue #48), light
//! up here for free once their events reach the stream: extend [`moment_of`]
//! and [`Moment`] and the rest of the pipeline carries them.
//!
//! ## Configurable, quiet by default
//!
//! The default policy notifies on every actionable moment with the terminal
//! bell on; routine events never notify. `--mute <moment>` suppresses a
//! specific moment (for a General who does not want peer questions, say), and
//! `--no-sound` drops the bell while keeping the desktop notification and the
//! log line.
//!
//! ## How a push is delivered
//!
//! Each push does three things, so it lands whatever the environment:
//!
//! - a printed log line, always, the durable record even on a headless server;
//! - the terminal bell, the audible pull (mirroring Seraphim's notification
//!   sound), unless `--no-sound`;
//! - a native desktop notification via the platform notifier (`notify-send` on
//!   Linux, `osascript` on macOS).
//!
//! A missing or failing notifier is not an error: the printed line and the bell
//! already carry the alert, so delivery degrades quietly rather than taking the
//! watcher down.

use std::{collections::HashSet, io::Write, process::Command};

use crew_substrate::core::{Channel, Event, EventKind, Lifecycle, MessageKind, RoleId, Sender};
use eyre::Result;

use crate::broker;

/// The most detail text a notification body carries before it is elided.
///
/// Long enough to convey the gist of a question, short enough that a desktop
/// notification and a log line stay one glance. Desktop notifiers truncate long
/// bodies anyway; this keeps the printed line tidy too.
const MAX_DETAIL: usize = 160;

/// An actionable moment: a stream event that changes what the General would do
/// next.
///
/// The `clap` value names (`question`, `died`, `stood-down`) are the tokens
/// `--mute` accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub(crate) enum Moment {
    /// A role asked a question and is waiting on a decision.
    #[value(name = "question")]
    QuestionAsked,
    /// A role died: it crashed or hung past recovery and needs attention.
    #[value(name = "died")]
    RoleDied,
    /// The crew stood down: every role halted and the mission is on hold.
    #[value(name = "stood-down")]
    CrewStoodDown,
}

/// Which moments push a notification, and whether a push sounds the bell.
///
/// The default notifies on every actionable moment with the bell on; routine
/// events stay quiet. Muting narrows the set.
#[derive(Debug, Clone)]
pub(crate) struct NotifyPolicy {
    /// Moments the General has muted; every other actionable moment still
    /// notifies.
    muted: Vec<Moment>,
    /// Whether a push sounds the terminal bell.
    sound: bool,
}

impl NotifyPolicy {
    /// A policy that mutes `muted` and sounds the bell when `sound` is set.
    pub(crate) fn new(muted: Vec<Moment>, sound: bool) -> Self {
        Self { muted, sound }
    }

    /// Whether `moment` should push a notification under this policy.
    fn wants(&self, moment: Moment) -> bool {
        !self.muted.contains(&moment)
    }
}

/// A rendered push: the title and body shown in the notification and the log
/// line.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Notification {
    /// The headline: what happened, at a glance.
    title: String,
    /// The detail: who, where, and the gist.
    body: String,
}

/// Watches the event stream and pushes a notification on each actionable
/// moment.
///
/// Reuses the broker firehose (`GET /stream`), so it needs no separate signal
/// path and updates live without polling. Runs until the stream closes or the
/// user interrupts.
///
/// # Errors
/// Returns an error if the broker configuration is invalid, or the broker
/// cannot be reached or refuses the stream.
pub(crate) fn notify(broker: Option<&str>, policy: &NotifyPolicy) -> Result<()> {
    let base = broker::resolve_base(broker)?;
    let mut roster = Roster::default();
    broker::tail_events(&base, "/stream", |event| {
        // Fold each lifecycle event first, so a question is classified against
        // the crew's liveness as of this event.
        roster.observe(event);
        if let Some(notification) = notification_for(event, policy, &roster) {
            push(&notification, policy.sound);
        }
    })
}

/// The crew's live roster, folded from the `lifecycle` events on the stream.
///
/// The notifier tracks who is currently a live agent so it can tell a peer
/// coordination question (a role asking a live teammate, which the General
/// would not field) from a General-facing one (a broadcast, or a question to a
/// role that is not up to answer). Liveness is the roster's own model (issue
/// #32): a role counts as live while it is working or idle, present and up or
/// resumable.
#[derive(Debug, Default)]
struct Roster {
    /// The roles currently known to be live agents.
    live: HashSet<RoleId>,
}

impl Roster {
    /// Folds a `lifecycle` event into the roster, updating the sender's
    /// liveness.
    ///
    /// Events that are not a role's lifecycle transition, and control-plane
    /// transitions that do not change liveness (pause, resume, stand-down),
    /// leave the roster untouched.
    fn observe(&mut self, event: &Event) {
        let (EventKind::Lifecycle(lifecycle), Sender::Role(role)) = (&event.kind, &event.from)
        else {
            return;
        };
        match liveness_after(*lifecycle) {
            Some(true) => {
                self.live.insert(role.clone());
            }
            Some(false) => {
                self.live.remove(role);
            }
            None => {}
        }
    }

    /// Whether `role` is currently a live agent (working or idle).
    fn is_live(&self, role: &RoleId) -> bool {
        self.live.contains(role)
    }

    /// Whether a question `event` is one the General would field.
    ///
    /// A broadcast, or a question addressed to a role that is not a live agent,
    /// is a wait for the General; a directed question to a live teammate is
    /// peer coordination the crew resolves itself. Mirrors the stall
    /// monitor's rule (issue #48). A question the General itself posed is
    /// not General-facing: pushing it back to the General would be
    /// pointless.
    fn is_general_facing(&self, event: &Event) -> bool {
        let Sender::Role(asker) = &event.from else {
            return false;
        };
        match addressee(event.channel.as_str(), asker) {
            Some(peer) => !self.is_live(&peer),
            None => true,
        }
    }
}

/// How a lifecycle event changes a role's liveness.
///
/// `Some(true)` means the role is now a live agent (up or resumable),
/// `Some(false)` means it is down (stopped or dead), and `None` is a
/// control-plane transition (pause, resume, stand-down) that the roster's
/// liveness model does not track, so it leaves a role's liveness unchanged.
fn liveness_after(lifecycle: Lifecycle) -> Option<bool> {
    match lifecycle {
        Lifecycle::Started | Lifecycle::Restarted | Lifecycle::Recovered | Lifecycle::Idle => {
            Some(true)
        }
        Lifecycle::Stopped | Lifecycle::Died => Some(false),
        Lifecycle::Paused | Lifecycle::Resumed | Lifecycle::StoodDown => None,
    }
}

/// The single agent a question is addressed to, if any.
///
/// A direct `@role` channel resolves to that role, a `a+b` pair to the asker's
/// peer, and a broadcast (`all-units`) or unparseable channel to `None` so it
/// reads as a wait for the General. Mirrors the stall monitor's `addressee`
/// (issue #48).
fn addressee(channel: &str, asker: &RoleId) -> Option<RoleId> {
    match Channel::parse(channel)? {
        Channel::Direct(role) => Some(role),
        Channel::Pair(first, second) => Some(if &first == asker { second } else { first }),
        Channel::AllUnits => None,
    }
}

/// The notification to push for `event`, or `None` if it is routine or a muted
/// moment.
fn notification_for(event: &Event, policy: &NotifyPolicy, roster: &Roster) -> Option<Notification> {
    let moment = moment_of(event, roster)?;
    policy.wants(moment).then(|| render(event, moment))
}

/// Classifies an event as an actionable moment, or `None` for routine chatter.
///
/// A question is a moment only when it is General-facing (see
/// [`Roster::is_general_facing`]); peer coordination between live agents stays
/// quiet.
fn moment_of(event: &Event, roster: &Roster) -> Option<Moment> {
    match &event.kind {
        EventKind::Message(message) => match message.kind {
            MessageKind::Question { .. } if roster.is_general_facing(event) => {
                Some(Moment::QuestionAsked)
            }
            _ => None,
        },
        EventKind::Lifecycle(Lifecycle::Died) => Some(Moment::RoleDied),
        EventKind::Lifecycle(Lifecycle::StoodDown) => Some(Moment::CrewStoodDown),
        _ => None,
    }
}

/// Renders the title and body for `event` at its actionable `moment`.
fn render(event: &Event, moment: Moment) -> Notification {
    let who = sender_name(&event.from);
    match moment {
        Moment::QuestionAsked => {
            let channel = event.channel.as_str();
            let detail = message_body(event).map_or_else(|| "no detail given".to_owned(), elide);
            Notification {
                title: "crew: a question needs you".to_owned(),
                body: format!("{who} asked on {channel}: {detail}"),
            }
        }
        Moment::RoleDied => Notification {
            title: format!("crew: {who} died"),
            body: format!("the {who} role crashed or hung past recovery; check the roster"),
        },
        Moment::CrewStoodDown => Notification {
            title: "crew: the crew stood down".to_owned(),
            body: "every role halted and the mission is on hold".to_owned(),
        },
    }
}

/// The display name of a sender: a role's name, or `the general` for the human.
fn sender_name(sender: &Sender) -> &str {
    match sender {
        Sender::Role(role) => role.as_str(),
        Sender::General => "the general",
    }
}

/// The trimmed message body of `event`, if it is a message that carries one.
fn message_body(event: &Event) -> Option<&str> {
    match &event.kind {
        EventKind::Message(message) => {
            let body = message.body.trim();
            (!body.is_empty()).then_some(body)
        }
        _ => None,
    }
}

/// Shortens `text` to [`MAX_DETAIL`] characters, appending an ellipsis when it
/// is cut.
fn elide(text: &str) -> String {
    if text.chars().count() <= MAX_DETAIL {
        return text.to_owned();
    }
    let head: String = text.chars().take(MAX_DETAIL).collect();
    format!("{head}...")
}

/// Fires a notification: the printed log line, the optional bell, and the
/// desktop notifier.
fn push(notification: &Notification, sound: bool) {
    // The printed line is the durable record, shown even where no desktop notifier
    // exists.
    println!("[notify] {}: {}", notification.title, notification.body);
    if sound {
        // The terminal bell is the audible pull; flush so it rings without waiting on a
        // full line buffer.
        print!("\x07");
        let _ = std::io::stdout().flush();
    }
    deliver_native(notification);
}

/// Shows `notification` through the platform desktop notifier, ignoring absence
/// or failure.
///
/// A notifier that is absent or fails must not take the watcher down: the
/// printed line and bell already recorded the moment, so delivery degrades
/// quietly.
#[cfg(target_os = "linux")]
fn deliver_native(notification: &Notification) {
    // `notify-send` takes the title and body as separate arguments, so no shell
    // quoting and no injection risk.
    let _ = Command::new("notify-send")
        .arg(&notification.title)
        .arg(&notification.body)
        .status();
}

/// Shows `notification` through the platform desktop notifier, ignoring absence
/// or failure.
///
/// A notifier that is absent or fails must not take the watcher down: the
/// printed line and bell already recorded the moment, so delivery degrades
/// quietly.
#[cfg(target_os = "macos")]
fn deliver_native(notification: &Notification) {
    // Pass the text through `argv` rather than interpolating it into the
    // AppleScript, so quotes and backslashes never need escaping and cannot
    // alter the script. Every title and body starts with a letter, so no
    // argument is mistaken for an option.
    let _ = Command::new("osascript")
        .arg("-e")
        .arg("on run argv")
        .arg("-e")
        .arg("display notification (item 1 of argv) with title (item 2 of argv)")
        .arg("-e")
        .arg("end run")
        .arg(&notification.body)
        .arg(&notification.title)
        .status();
}

/// Shows `notification` through the platform desktop notifier, ignoring absence
/// or failure.
///
/// No native notifier is wired for this platform, so the printed line and bell
/// carry it.
#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn deliver_native(_notification: &Notification) {}

#[cfg(test)]
mod tests {
    use crew_substrate::core::{
        Activity, ChannelId, Event, EventKind, Lifecycle, Message, MessageId, MessageKind, RoleId,
        Sender, Timestamp,
    };

    use super::{moment_of, notification_for, Moment, NotifyPolicy, Roster};

    /// A roster with `roles` folded in as live agents, via their `started`
    /// lifecycle events.
    fn roster_with(roles: &[&str]) -> Roster {
        let mut roster = Roster::default();
        for role in roles {
            roster.observe(&lifecycle(role, Lifecycle::Started));
        }
        roster
    }

    /// An event from `from` on `channel` carrying `kind`.
    fn event(from: Sender, channel: &str, kind: EventKind) -> Event {
        Event {
            ts: Timestamp::now(),
            from,
            channel: ChannelId::new(channel),
            task: None,
            kind,
        }
    }

    /// A question from `role` on `channel` with `body`.
    fn question(role: &str, channel: &str, body: &str) -> Event {
        event(
            Sender::Role(RoleId::new(role)),
            channel,
            EventKind::Message(Message {
                id: MessageId::new(),
                kind: MessageKind::Question { options: vec![] },
                body: body.to_owned(),
            }),
        )
    }

    /// A lifecycle event from `role` (or the crew) carrying `lifecycle`.
    fn lifecycle(role: &str, lifecycle: Lifecycle) -> Event {
        event(
            Sender::Role(RoleId::new(role)),
            "all-units",
            EventKind::Lifecycle(lifecycle),
        )
    }

    #[test]
    fn classifies_the_actionable_moments() {
        let roster = Roster::default();
        assert_eq!(
            moment_of(&question("backend", "all-units", "which db?"), &roster),
            Some(Moment::QuestionAsked)
        );
        assert_eq!(
            moment_of(&lifecycle("backend", Lifecycle::Died), &roster),
            Some(Moment::RoleDied)
        );
        assert_eq!(
            moment_of(&lifecycle("commander", Lifecycle::StoodDown), &roster),
            Some(Moment::CrewStoodDown)
        );
    }

    #[test]
    fn stays_quiet_on_routine_chatter() {
        let note = event(
            Sender::Role(RoleId::new("backend")),
            "all-units",
            EventKind::Message(Message {
                id: MessageId::new(),
                kind: MessageKind::Note,
                body: "on it".to_owned(),
            }),
        );
        let status = event(
            Sender::Role(RoleId::new("frontend")),
            "@commander",
            EventKind::Message(Message {
                id: MessageId::new(),
                kind: MessageKind::Status,
                body: "halfway".to_owned(),
            }),
        );
        let started = lifecycle("backend", Lifecycle::Started);
        let idle = lifecycle("backend", Lifecycle::Idle);
        let activity = event(
            Sender::Role(RoleId::new("backend")),
            "all-units",
            EventKind::Activity(Activity::TurnStarted),
        );

        let roster = Roster::default();
        for routine in [note, status, started, idle, activity] {
            assert_eq!(
                moment_of(&routine, &roster),
                None,
                "routine event: {routine:?}"
            );
        }
    }

    #[test]
    fn a_question_notification_names_the_asker_the_channel_and_the_gist() {
        let policy = NotifyPolicy::new(vec![], true);
        let notification = notification_for(
            &question("frontend", "all-units", "REST or gRPC?"),
            &policy,
            &Roster::default(),
        )
        .expect("a question is an actionable moment");
        assert!(
            notification.title.contains("question"),
            "title: {}",
            notification.title
        );
        assert!(
            notification.body.contains("frontend")
                && notification.body.contains("all-units")
                && notification.body.contains("REST or gRPC?"),
            "body: {}",
            notification.body
        );
    }

    #[test]
    fn a_death_notification_names_the_role() {
        let policy = NotifyPolicy::new(vec![], true);
        let notification = notification_for(
            &lifecycle("backend", Lifecycle::Died),
            &policy,
            &Roster::default(),
        )
        .expect("a death is an actionable moment");
        assert!(
            notification.title.contains("backend") && notification.body.contains("backend"),
            "title {} / body {}",
            notification.title,
            notification.body
        );
    }

    #[test]
    fn a_muted_moment_does_not_notify() {
        let policy = NotifyPolicy::new(vec![Moment::QuestionAsked], true);
        let roster = Roster::default();
        assert!(
            notification_for(
                &question("backend", "all-units", "which db?"),
                &policy,
                &roster
            )
            .is_none(),
            "a muted question does not notify"
        );
        assert!(
            notification_for(&lifecycle("backend", Lifecycle::Died), &policy, &roster).is_some(),
            "an unmuted death still notifies"
        );
    }

    #[test]
    fn a_long_question_body_is_elided() {
        let long = "x".repeat(500);
        let policy = NotifyPolicy::new(vec![], true);
        let notification = notification_for(
            &question("backend", "all-units", &long),
            &policy,
            &Roster::default(),
        )
        .expect("a question is an actionable moment");
        assert!(
            notification.body.ends_with("..."),
            "an overlong body is elided: {}",
            notification.body
        );
        assert!(
            notification.body.len() < long.len(),
            "the body is shorter than the raw question"
        );
    }

    #[test]
    fn a_peer_question_to_a_live_agent_stays_quiet() {
        let roster = roster_with(&["frontend"]);
        assert_eq!(
            moment_of(&question("backend", "@frontend", "which lib?"), &roster),
            None,
            "a directed question to a live teammate is peer coordination, not the General's"
        );
    }

    #[test]
    fn a_pair_question_to_a_live_peer_stays_quiet() {
        let roster = roster_with(&["frontend"]);
        assert_eq!(
            moment_of(
                &question("backend", "backend+frontend", "who owns auth?"),
                &roster
            ),
            None,
            "a pair question to a live peer is peer coordination"
        );
    }

    #[test]
    fn a_directed_question_to_a_non_live_role_reaches_the_general() {
        // `frontend` is not on the roster (never seen up), so the General must
        // field the question no one live can answer.
        let roster = roster_with(&["backend"]);
        assert_eq!(
            moment_of(&question("backend", "@frontend", "which lib?"), &roster),
            Some(Moment::QuestionAsked),
            "a question to a role that is not a live agent is General-facing"
        );
    }

    #[test]
    fn a_broadcast_question_always_reaches_the_general() {
        // Even with every role live, a broadcast is a wait on the General.
        let roster = roster_with(&["backend", "frontend"]);
        assert_eq!(
            moment_of(&question("backend", "all-units", "ship it?"), &roster),
            Some(Moment::QuestionAsked),
            "a broadcast question is General-facing"
        );
    }

    #[test]
    fn a_stopped_agent_makes_its_question_general_facing() {
        // A peer question to frontend is quiet while it is live, then reaches the
        // General once frontend stops and can no longer answer.
        let mut roster = roster_with(&["frontend"]);
        let ask = question("backend", "@frontend", "which lib?");
        assert_eq!(
            moment_of(&ask, &roster),
            None,
            "quiet while frontend is live"
        );

        roster.observe(&lifecycle("frontend", Lifecycle::Stopped));
        assert_eq!(
            moment_of(&ask, &roster),
            Some(Moment::QuestionAsked),
            "General-facing once frontend is stopped"
        );
    }

    #[test]
    fn a_question_from_the_general_does_not_ping_the_general() {
        let roster = Roster::default();
        let ask = event(
            Sender::General,
            "all-units",
            EventKind::Message(Message {
                id: MessageId::new(),
                kind: MessageKind::Question { options: vec![] },
                body: "status?".to_owned(),
            }),
        );
        assert_eq!(
            moment_of(&ask, &roster),
            None,
            "the General asked it, so it is not pushed back to the General"
        );
    }
}
