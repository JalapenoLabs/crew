//! The broker client behind `crew watch`.
//!
//! A thin synchronous SSE client over the broker's localhost API: `watch` tails
//! an event stream (`/stream` for the whole firehose, or `/inbox?role=<role>`
//! for one role's self-filtered inbox) and renders each event so a reader sees
//! who said what, on which channel. The broker drops a subscriber's own
//! messages at the source (issue #10), so a peer watching its own inbox never
//! sees a self-echo.
//!
//! Sending a message is the shim's job (`crew send`, `src/shim.rs`), which
//! posts as the agent's role. This module only reads.

use std::{
    io::{BufRead, BufReader},
    thread,
    time::Duration,
};

use crew_substrate::core::{
    Activity, BoardEvent, BudgetEvent, BudgetScope, Event, EventKind, MessageKind, Sender,
    StallEvent, StallStatus, TaskId, TelemetryEvent, UsageEvent, Verdict, VerificationEvent,
};
use eyre::{eyre, Result};
use tracing::{event, Level};

/// Tails a role's self-filtered inbox, or the whole firehose when `role` is
/// `None`, rendering each event until the user interrupts (Ctrl-C).
///
/// Like `tail -F`, it reconnects on a dropped connection (a broker restart or a
/// network blip), resuming a role inbox from `Last-Event-ID` without loss
/// (issue #117). The broker base comes from `broker` when set, else the
/// broker's own `CREW_BROKER_HOST` / `CREW_BROKER_PORT` environment.
///
/// # Errors
/// Returns an error if the broker configuration is invalid, or the broker
/// cannot be reached on the first connection.
pub fn watch(broker: Option<&str>, role: Option<&str>) -> Result<()> {
    let base = crate::broker_base::resolve_base(broker)?;
    tail(&base, &watch_path(role))
}

/// The SSE path a `watch` reads: one role's self-filtered inbox, or the
/// firehose.
fn watch_path(role: Option<&str>) -> String {
    match role {
        Some(role) => format!("/inbox?role={}", role.trim()),
        None => "/stream".to_owned(),
    }
}

/// Tails `path` (an SSE endpoint) on `base`, rendering each event as it
/// arrives.
///
/// # Errors
/// Returns an error if the broker cannot be reached or refuses the stream.
fn tail(base: &str, path: &str) -> Result<()> {
    tail_events(base, path, |event| println!("{}", render_event(event)))
}

/// How long a dropped watch waits before reconnecting, so a restarting broker
/// is not hammered but the tail resumes promptly (issue #117).
const RECONNECT_BACKOFF: Duration = Duration::from_secs(2);

/// Tails `path` (an SSE endpoint) on `base`, invoking `on_event` for each
/// event, reconnecting on a dropped connection like `tail -F` (issue #117).
///
/// This is the shared read half behind `crew watch` and `crew notify`. It opens
/// the stream, parses each SSE `data:` line into an [`Event`], and hands it to
/// `on_event`, tracking the id of the last event delivered. When the connection
/// ends (a broker restart or a network blip), it waits a brief backoff and
/// reconnects, passing the last id as `Last-Event-ID`. For `/inbox` the broker
/// resumes right after that cursor, so no addressed event is lost or repeated;
/// `/stream` is live-only, so a firehose reconnect resumes at the live tail and
/// a consumer catches the gap up through `/history`. It runs until the caller
/// interrupts (Ctrl-C).
///
/// # Errors
/// Returns an error only if the **first** connection cannot be opened (so a
/// wrong address or a broker that is not running fails fast). Once connected, a
/// drop is recovered rather than surfaced, since the point is to survive it.
pub(crate) fn tail_events(base: &str, path: &str, mut on_event: impl FnMut(&Event)) -> Result<()> {
    let url = format!("{base}{path}");
    let mut resume_from: Option<u64> = None;
    let mut connected = false;

    loop {
        match stream_once(&url, resume_from, &mut on_event) {
            Ok(last_id) => {
                connected = true;
                resume_from = last_id.or(resume_from);
                event!(
                    name: "cli.stream.reconnecting",
                    Level::DEBUG,
                    url = %url,
                    "watch stream ended; reconnecting from {{url}}",
                );
            }
            // Fail fast if we never connected: a clear "is crewd running?" error.
            Err(err) if !connected => return Err(err),
            // A blip while the broker restarts: back off and retry, never give up.
            Err(err) => event!(
                name: "cli.stream.reconnecting",
                Level::DEBUG,
                url = %url,
                error = %err,
                "watch reconnect failed; retrying {{url}}",
            ),
        }
        thread::sleep(RECONNECT_BACKOFF);
    }
}

/// Opens one SSE connection and reads it until it ends, returning the id of the
/// last event delivered (for the next reconnect's cursor).
///
/// Resumes from `resume_from` by sending it as `Last-Event-ID`. Returns an
/// error only if the connection cannot be opened; a mid-stream read error ends
/// the read cleanly, so the caller reconnects rather than aborts.
fn stream_once(
    url: &str,
    resume_from: Option<u64>,
    on_event: &mut impl FnMut(&Event),
) -> Result<Option<u64>> {
    let mut request = ureq::get(url);
    if let Some(id) = resume_from {
        request = request.set("Last-Event-ID", &id.to_string());
    }
    let response = request.call().map_err(|err| match err {
        ureq::Error::Status(code, _) => eyre!("the broker refused the stream: HTTP {code}"),
        ureq::Error::Transport(transport) => {
            eyre!("could not reach the broker at {url}; is `crewd` running? ({transport})")
        }
    })?;
    event!(name: "cli.stream.connected", Level::DEBUG, url = %url, "streaming {{url}}");

    let mut last_id = resume_from;
    let mut pending: Option<u64> = None;
    let reader = BufReader::new(response.into_reader());
    for line in reader.lines() {
        // A read error means the connection dropped; end the read so the caller
        // reconnects.
        let Ok(line) = line else { break };
        apply_line(&line, &mut pending, &mut last_id, on_event);
    }
    Ok(last_id)
}

/// Applies one SSE line to the tail state.
///
/// An `id:` line arms the pending cursor; a `data:` line delivers its [`Event`]
/// and only then commits the pending id as `last_id`. Committing after
/// delivery, not on the `id:` line, means a connection that drops between the
/// two does not advance the cursor past an event the reader never received, so
/// the reconnect replays it rather than skipping it.
fn apply_line(
    line: &str,
    pending: &mut Option<u64>,
    last_id: &mut Option<u64>,
    on_event: &mut impl FnMut(&Event),
) {
    if let Some(id) = parse_id_line(line) {
        *pending = Some(id);
    } else if let Some(event) = parse_data_line(line) {
        on_event(&event);
        if let Some(id) = pending.take() {
            *last_id = Some(id);
        }
    }
}

/// Parses one Server-Sent-Event `id:` line into its sequence cursor, if it is
/// one.
fn parse_id_line(line: &str) -> Option<u64> {
    line.strip_prefix("id:")?.trim().parse().ok()
}

/// Parses one Server-Sent-Event `data:` line into an [`Event`], if it is one.
///
/// Non-data lines (the `id:` cursor, keep-alive comments, blank separators)
/// yield `None` and are skipped.
fn parse_data_line(line: &str) -> Option<Event> {
    let data = line.strip_prefix("data:")?.trim_start();
    serde_json::from_str::<Event>(data).ok()
}

/// Renders an event for `crew watch`: who said what, on which channel.
fn render_event(event: &Event) -> String {
    let time = event.ts.to_datetime().format("%H:%M:%S");
    let from = match &event.from {
        Sender::General => "general",
        Sender::Role(role) => role.as_str(),
    };
    let channel = event.channel.as_str();
    let (kind, body) = describe(&event.kind);
    if body.is_empty() {
        format!("[{time}] {from} -> {channel} ({kind})")
    } else {
        format!("[{time}] {from} -> {channel} ({kind}) {body}")
    }
}

/// The kind label and body text to show for an event's payload.
fn describe(kind: &EventKind) -> (&'static str, String) {
    match kind {
        EventKind::Message(message) => (message_kind(&message.kind), message.body.clone()),
        EventKind::Lifecycle(lifecycle) => ("lifecycle", format!("{lifecycle:?}").to_lowercase()),
        EventKind::Activity(activity) => ("activity", activity_body(activity)),
        EventKind::Ledger(ledger) => (
            "ledger",
            format!(
                "{} {} {}",
                ledger.owner,
                ledger.state.label(),
                task_label(&ledger.title, ledger.task),
            ),
        ),
        EventKind::Boundary(boundary) => (
            "boundary",
            format!(
                "{} reached {} ({})",
                boundary.role,
                boundary.path,
                if boundary.blocked {
                    "blocked"
                } else {
                    "warned"
                }
            ),
        ),
        EventKind::Verification(verification) => ("verification", verification_body(verification)),
        EventKind::Board(board) => ("board", board_body(board)),
        EventKind::Budget(budget) => ("budget", budget_body(budget)),
        EventKind::Telemetry(telemetry) => ("telemetry", telemetry_body(telemetry)),
        EventKind::Usage(usage) => ("usage", usage_body(usage)),
        EventKind::Stall(stall) => ("stall", stall_body(stall)),
        EventKind::Mission(mission) => (
            "mission",
            if mission.summary.trim().is_empty() {
                "complete".to_owned()
            } else {
                format!("complete: {}", mission.summary.trim())
            },
        ),
    }
}

/// A short description of a coordination stall the monitor surfaced (issue
/// #120).
fn stall_body(stall: &StallEvent) -> String {
    let verb = match stall.status {
        StallStatus::Detected => "detected",
        StallStatus::Resolved => "resolved",
    };
    format!("{verb} {}: {}", stall.kind.label(), stall.detail)
}

/// A short description of a shared-subscription usage reading (issue #56).
fn usage_body(usage: &UsageEvent) -> String {
    if usage.paused {
        match usage.window_reset {
            Some(reset) => format!(
                "subscription at {}%; new work paused until {}",
                usage.percent,
                reset.to_datetime().format("%H:%M:%S")
            ),
            None => format!("subscription at {}%; new work paused", usage.percent),
        }
    } else {
        format!("subscription at {}%; work resumed", usage.percent)
    }
}

/// A short description of a per-turn token-and-cost usage report (issue #55).
fn telemetry_body(telemetry: &TelemetryEvent) -> String {
    // Cost rides the wire in micro-USD; render whole dollars and cents.
    let dollars = telemetry.cost_micro_usd / 1_000_000;
    let cents = (telemetry.cost_micro_usd % 1_000_000) / 10_000;
    format!(
        "{} spent {} tokens (${dollars}.{cents:02})",
        telemetry.role, telemetry.tokens
    )
}

/// A short description of a token-spend report against the crew budget (issue
/// #54).
fn budget_body(budget: &BudgetEvent) -> String {
    let spend = |spent: u64, cap: Option<u64>| match cap {
        Some(cap) => format!("{spent}/{cap}"),
        None => format!("{spent}"),
    };
    let role = format!(
        "{} spent {} tokens",
        budget.role,
        spend(budget.role_spent, budget.role_cap)
    );
    let crew = format!("crew {}", spend(budget.crew_spent, budget.crew_budget));
    match budget.breach {
        Some(BudgetScope::Role) => format!("{role} (cap reached, idle-stopped); {crew}"),
        Some(BudgetScope::Crew) => format!("{role}; {crew} (budget reached, crew idle-stopped)"),
        None => format!("{role}; {crew}"),
    }
}

/// A short description of a situation-board change (issue #49).
fn board_body(board: &BoardEvent) -> String {
    if board.retracted {
        format!(
            "{} retracted `{}` ({})",
            board.author,
            board.key,
            board.section.label()
        )
    } else {
        format!(
            "{} recorded `{}` ({}): {}",
            board.author,
            board.key,
            board.section.label(),
            board.body
        )
    }
}

/// A short description of a done-gate verification step (issues #47, #183).
///
/// Names the task by its human title (display), falling back to its id when the
/// title is empty, rather than showing the raw uuid.
fn verification_body(verification: &VerificationEvent) -> String {
    let task = task_label(&verification.title, verification.task);
    let owner = &verification.owner;
    let verifier = verification
        .verifier
        .as_ref()
        .map_or("the verifier", |role| role.as_str());
    let detail = &verification.detail;
    match verification.verdict {
        Verdict::Submitted if detail.is_empty() => {
            format!("{owner} submitted `{task}` for verification")
        }
        Verdict::Submitted => format!("{owner} submitted `{task}` for verification: {detail}"),
        Verdict::Passed => format!("{verifier} passed `{task}` (owner {owner}); it is done"),
        Verdict::Failed if detail.is_empty() => {
            format!("{verifier} failed `{task}` (owner {owner})")
        }
        Verdict::Failed => format!("{verifier} failed `{task}` (owner {owner}): {detail}"),
    }
}

/// The display label for a task on the watch stream: its human title, or its id
/// when the title is empty (issue #183).
fn task_label(title: &str, task: TaskId) -> String {
    if title.is_empty() {
        task.to_string()
    } else {
        title.to_owned()
    }
}

/// The wire label for a message's typed intent.
fn message_kind(kind: &MessageKind) -> &'static str {
    match kind {
        MessageKind::Order { .. } => "order",
        MessageKind::Question { .. } => "question",
        MessageKind::Answer { .. } => "answer",
        MessageKind::Status => "status",
        MessageKind::Artifact { .. } => "artifact",
        MessageKind::Note => "note",
        MessageKind::Redirect => "redirect",
        MessageKind::Belay => "belay",
    }
}

/// A short description of an activity event.
fn activity_body(activity: &Activity) -> String {
    match activity {
        Activity::TurnStarted => "turn started".to_owned(),
        Activity::TurnEnded => "turn ended".to_owned(),
        Activity::ToolCall { tool } => format!("tool {tool}"),
        Activity::Output { text } => text.clone(),
        Activity::Other { raw } => format!("({raw})"),
    }
}

#[cfg(test)]
mod tests {
    use crew_substrate::core::{
        ChannelId, Event, EventKind, Lifecycle, Message, MessageId, MessageKind, RoleId, Sender,
        Timestamp,
    };

    use super::{apply_line, parse_data_line, parse_id_line, render_event, watch_path};

    fn note(from: Sender, channel: &str, body: &str) -> Event {
        Event {
            ts: Timestamp::now(),
            from,
            channel: ChannelId::new(channel),
            task: None,
            kind: EventKind::Message(Message {
                id: MessageId::new(),
                kind: MessageKind::Note,
                body: body.to_owned(),
            }),
        }
    }

    #[test]
    fn renders_a_message_with_its_routing_visible() {
        let event = note(
            Sender::Role(RoleId::new("frontend")),
            "@backend",
            "api is ready",
        );
        let line = render_event(&event);
        assert!(
            line.contains("frontend -> @backend"),
            "sender and channel: {line}"
        );
        assert!(
            line.contains("(note) api is ready"),
            "kind and body: {line}"
        );
    }

    #[test]
    fn renders_the_general_and_a_lifecycle_event() {
        let from_general = note(Sender::General, "all-units", "stand by");
        assert!(render_event(&from_general).contains("general -> all-units (note) stand by"));

        let lifecycle = Event {
            kind: EventKind::Lifecycle(Lifecycle::Started),
            ..note(Sender::Role(RoleId::new("backend")), "all-units", "")
        };
        assert!(render_event(&lifecycle).contains("(lifecycle) started"));
    }

    #[test]
    fn renders_a_verification_step() {
        use crew_substrate::core::{TaskId, Verdict, VerificationEvent};

        // The event keys by an id but renders the human title, so the watch line
        // reads by the task's name, not a raw uuid (issue #183).
        let event = |verdict, verifier: Option<&str>, detail: &str| Event {
            kind: EventKind::Verification(VerificationEvent {
                task: TaskId::new(),
                title: "login".to_owned(),
                owner: RoleId::new("backend"),
                verifier: verifier.map(RoleId::new),
                verdict,
                detail: detail.to_owned(),
            }),
            ..note(Sender::Role(RoleId::new("backend")), "all-units", "")
        };

        let submitted = render_event(&event(Verdict::Submitted, None, "tokens expire"));
        assert!(
            submitted.contains("(verification) backend submitted `login`"),
            "submission names the owner and title, not the uuid: {submitted}"
        );

        let failed = render_event(&event(Verdict::Failed, Some("qa"), "tokens never expire"));
        assert!(
            failed.contains("qa failed `login`") && failed.contains("tokens never expire"),
            "a failure names the verifier and the reason: {failed}"
        );

        let passed = render_event(&event(Verdict::Passed, Some("qa"), ""));
        assert!(
            passed.contains("qa passed `login`") && passed.contains("done"),
            "a pass reads as done: {passed}"
        );
    }

    #[test]
    fn parses_a_data_line_and_skips_the_rest() {
        let event = note(Sender::General, "all-units", "hi");
        let data = format!("data: {}", serde_json::to_string(&event).unwrap());
        assert!(parse_data_line(&data).is_some(), "a data line parses");
        assert!(parse_data_line("id: 7").is_none(), "an id line is skipped");
        assert!(
            parse_data_line(": keep-alive").is_none(),
            "a comment is skipped"
        );
        assert!(parse_data_line("").is_none(), "a blank line is skipped");
    }

    #[test]
    fn watch_path_is_the_firehose_or_a_role_inbox() {
        assert_eq!(watch_path(None), "/stream");
        assert_eq!(watch_path(Some("backend")), "/inbox?role=backend");
    }

    #[test]
    fn parse_id_line_reads_the_sse_cursor() {
        assert_eq!(parse_id_line("id: 7"), Some(7));
        assert_eq!(parse_id_line("id:42"), Some(42));
        assert_eq!(
            parse_id_line("data: {}"),
            None,
            "a data line is not a cursor"
        );
        assert_eq!(
            parse_id_line("id: nope"),
            None,
            "a non-numeric id is skipped"
        );
        assert_eq!(parse_id_line(": keep-alive"), None);
    }

    #[test]
    fn apply_line_commits_the_cursor_only_after_the_event_is_delivered() {
        // Committing the cursor after delivery, not on the `id:` line, is what lets a
        // reconnect resume without a gap: an event the reader never received does not
        // advance the cursor past it (issue #117).
        let event = note(Sender::General, "all-units", "hi");
        let data = format!("data: {}", serde_json::to_string(&event).unwrap());

        let delivered = std::cell::Cell::new(0);
        let mut on_event = |_: &Event| delivered.set(delivered.get() + 1);
        let mut pending = None;
        let mut last_id = None;

        // An `id:` line arms the pending cursor but does not commit it yet.
        apply_line("id: 5", &mut pending, &mut last_id, &mut on_event);
        assert_eq!(last_id, None, "the cursor is not advanced before delivery");
        assert_eq!(pending, Some(5));

        // The `data:` line delivers the event and commits the cursor.
        apply_line(&data, &mut pending, &mut last_id, &mut on_event);
        assert_eq!(delivered.get(), 1);
        assert_eq!(
            last_id,
            Some(5),
            "the cursor advances to the delivered event"
        );
        assert_eq!(pending, None);

        // A later `id:` with no `data:` (the connection dropped mid-event) must not
        // advance the cursor past an event the reader never received.
        apply_line("id: 6", &mut pending, &mut last_id, &mut on_event);
        assert_eq!(
            last_id,
            Some(5),
            "an undelivered event does not advance the cursor"
        );
    }
}
