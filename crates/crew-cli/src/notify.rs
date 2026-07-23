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
//! - **A question is asked** ([`MessageKind::Question`]): a role wants a
//!   decision.
//! - **A role dies** ([`Lifecycle::Died`]): a role crashed or hung past
//!   recovery.
//! - **The crew stands down** ([`Lifecycle::StoodDown`]): every role halts and
//!   the mission is on hold.
//! - **The crew is stalled** (a `stall` event, [`StallStatus::Detected`]): the
//!   crew is stuck waiting on itself and needs the General (issue #48, #120). A
//!   resolved stall is good news and stays quiet.
//!
//! Everything else (status, notes, orders, answers, artifacts, ordinary
//! lifecycle such as `started` or `idle`, activity, board, boundary, and
//! verification events) is routine and stays quiet by default. One further
//! moment in the issue's scope, an approval pending (issue #40), lights up here
//! for free once its event reaches the stream: extend [`moment_of`] and
//! [`Moment`] and the rest of the pipeline carries it.
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

use std::{io::Write, process::Command};

use crew_substrate::core::{Event, EventKind, Lifecycle, MessageKind, Sender, StallStatus};
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
/// The `clap` value names (`question`, `died`, `stood-down`, `stalled`) are the
/// tokens `--mute` accepts.
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
    /// The crew is stalled: it is stuck waiting on itself and needs the General
    /// to unstick it (issue #48, #120).
    #[value(name = "stalled")]
    RoleStalled,
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
    broker::tail_events(&base, "/stream", |event| {
        if let Some(notification) = notification_for(event, policy) {
            push(&notification, policy.sound);
        }
    })
}

/// The notification to push for `event`, or `None` if it is routine or a muted
/// moment.
fn notification_for(event: &Event, policy: &NotifyPolicy) -> Option<Notification> {
    let moment = moment_of(event)?;
    policy.wants(moment).then(|| render(event, moment))
}

/// Classifies an event as an actionable moment, or `None` for routine chatter.
fn moment_of(event: &Event) -> Option<Moment> {
    match &event.kind {
        EventKind::Message(message) => match message.kind {
            MessageKind::Question { .. } => Some(Moment::QuestionAsked),
            _ => None,
        },
        EventKind::Lifecycle(Lifecycle::Died) => Some(Moment::RoleDied),
        EventKind::Lifecycle(Lifecycle::StoodDown) => Some(Moment::CrewStoodDown),
        // Only a newly detected stall pulls the General in; a resolved one is
        // good news that needs no push.
        EventKind::Stall(stall) if stall.status == StallStatus::Detected => {
            Some(Moment::RoleStalled)
        }
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
        Moment::RoleStalled => {
            let detail = match &event.kind {
                EventKind::Stall(stall) => elide(stall.detail.trim()),
                _ => "the crew is stuck waiting on itself".to_owned(),
            };
            Notification {
                title: "crew: the crew is stalled".to_owned(),
                body: format!("coordination stall: {detail}"),
            }
        }
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
        Sender, StallEvent, StallKind, StallStatus, Timestamp,
    };

    use super::{moment_of, notification_for, Moment, NotifyPolicy};

    /// A stall event from the crew carrying `kind`, `status`, and `detail`.
    fn stall(kind: StallKind, status: StallStatus, detail: &str) -> Event {
        event(
            Sender::General,
            "all-units",
            EventKind::Stall(StallEvent {
                kind,
                status,
                roles: vec![RoleId::new("backend"), RoleId::new("frontend")],
                detail: detail.to_owned(),
            }),
        )
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
        assert_eq!(
            moment_of(&question("backend", "all-units", "which db?")),
            Some(Moment::QuestionAsked)
        );
        assert_eq!(
            moment_of(&lifecycle("backend", Lifecycle::Died)),
            Some(Moment::RoleDied)
        );
        assert_eq!(
            moment_of(&lifecycle("commander", Lifecycle::StoodDown)),
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

        for routine in [note, status, started, idle, activity] {
            assert_eq!(moment_of(&routine), None, "routine event: {routine:?}");
        }
    }

    #[test]
    fn a_question_notification_names_the_asker_the_channel_and_the_gist() {
        let policy = NotifyPolicy::new(vec![], true);
        let notification =
            notification_for(&question("frontend", "all-units", "REST or gRPC?"), &policy)
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
        let notification = notification_for(&lifecycle("backend", Lifecycle::Died), &policy)
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
        assert!(
            notification_for(&question("backend", "all-units", "which db?"), &policy).is_none(),
            "a muted question does not notify"
        );
        assert!(
            notification_for(&lifecycle("backend", Lifecycle::Died), &policy).is_some(),
            "an unmuted death still notifies"
        );
    }

    #[test]
    fn a_long_question_body_is_elided() {
        let long = "x".repeat(500);
        let policy = NotifyPolicy::new(vec![], true);
        let notification = notification_for(&question("backend", "all-units", &long), &policy)
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
    fn a_detected_stall_is_an_actionable_moment() {
        assert_eq!(
            moment_of(&stall(
                StallKind::Deadlock,
                StallStatus::Detected,
                "deadlock: backend waits on frontend, and frontend waits on backend"
            )),
            Some(Moment::RoleStalled)
        );
    }

    #[test]
    fn a_resolved_stall_stays_quiet() {
        assert_eq!(
            moment_of(&stall(
                StallKind::Deadlock,
                StallStatus::Resolved,
                "the deadlock cleared"
            )),
            None,
            "a resolved stall is good news and needs no push"
        );
    }

    #[test]
    fn a_stall_notification_names_the_cause() {
        let policy = NotifyPolicy::new(vec![], true);
        let notification = notification_for(
            &stall(
                StallKind::UnansweredQuestion,
                StallStatus::Detected,
                "backend has waited 12m for frontend to answer",
            ),
            &policy,
        )
        .expect("a detected stall is an actionable moment");
        assert!(
            notification.title.contains("stalled"),
            "title: {}",
            notification.title
        );
        assert!(
            notification
                .body
                .contains("backend has waited 12m for frontend to answer"),
            "body names the specific cause: {}",
            notification.body
        );
    }

    #[test]
    fn a_muted_stall_does_not_notify() {
        let policy = NotifyPolicy::new(vec![Moment::RoleStalled], true);
        assert!(
            notification_for(
                &stall(
                    StallKind::LedgerStall,
                    StallStatus::Detected,
                    "task `login` is stuck"
                ),
                &policy
            )
            .is_none(),
            "a muted stall does not notify"
        );
    }
}
