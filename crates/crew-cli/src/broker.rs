//! The broker client behind `crew watch`.
//!
//! A thin synchronous SSE client over the broker's localhost API: `watch` tails an
//! event stream (`/stream` for the whole firehose, or `/inbox?role=<role>` for one
//! role's self-filtered inbox) and renders each event so a reader sees who said what,
//! on which channel. The broker drops a subscriber's own messages at the source (issue
//! #10), so a peer watching its own inbox never sees a self-echo.
//!
//! Sending a message is the shim's job (`crew send`, `src/shim.rs`), which posts as the
//! agent's role. This module only reads.

use std::io::{BufRead, BufReader};

use crew_substrate::broker::Config as BrokerConfig;
use crew_substrate::core::{
    Activity, BoardEvent, BrokerEndpoint, BudgetEvent, BudgetScope, Event, EventKind, MessageKind,
    Sender, TelemetryEvent, UsageEvent, Verdict, VerificationEvent,
};
use eyre::{eyre, Result, WrapErr};
use tracing::{event, Level};

/// Tails a role's self-filtered inbox, or the whole firehose when `role` is `None`,
/// rendering each event until the stream closes or the user interrupts.
///
/// The broker base comes from `broker` when set, else the broker's own
/// `CREW_BROKER_HOST` / `CREW_BROKER_PORT` environment.
///
/// # Errors
/// Returns an error if the broker configuration is invalid, or the broker cannot be
/// reached or refuses the stream.
pub fn watch(broker: Option<&str>, role: Option<&str>) -> Result<()> {
    let base = resolve_base(broker)?;
    tail(&base, &watch_path(role))
}

/// The broker base URL: the `--broker` value if given, else the broker's environment.
///
/// Shared by every stream reader (`crew watch`, `crew notify`).
///
/// # Errors
/// Returns an error if `CREW_BROKER_HOST` or `CREW_BROKER_PORT` is set but invalid.
pub(crate) fn resolve_base(flag: Option<&str>) -> Result<String> {
    if let Some(url) = flag {
        return Ok(normalize_base(url));
    }
    let config = BrokerConfig::from_env().wrap_err("could not read the broker configuration")?;
    Ok(BrokerEndpoint::new(config.host.to_string(), config.port).base_url())
}

/// Normalizes a `--broker` value: default the scheme to `http`, drop a trailing slash.
fn normalize_base(url: &str) -> String {
    let url = url.trim().trim_end_matches('/');
    if url.starts_with("http://") || url.starts_with("https://") {
        url.to_owned()
    } else {
        format!("http://{url}")
    }
}

/// The SSE path a `watch` reads: one role's self-filtered inbox, or the firehose.
fn watch_path(role: Option<&str>) -> String {
    match role {
        Some(role) => format!("/inbox?role={}", role.trim()),
        None => "/stream".to_owned(),
    }
}

/// Tails `path` (an SSE endpoint) on `base`, rendering each event as it arrives.
///
/// # Errors
/// Returns an error if the broker cannot be reached or refuses the stream.
fn tail(base: &str, path: &str) -> Result<()> {
    tail_events(base, path, |event| println!("{}", render_event(event)))
}

/// Tails `path` (an SSE endpoint) on `base`, invoking `on_event` for each event.
///
/// This is the shared read half behind `crew watch` and `crew notify`: it opens the
/// stream, parses each SSE `data:` line into an [`Event`], and hands it to `on_event`.
/// The tail runs until the connection ends or the caller interrupts.
///
/// # Errors
/// Returns an error if the broker cannot be reached or refuses the stream.
pub(crate) fn tail_events(base: &str, path: &str, mut on_event: impl FnMut(&Event)) -> Result<()> {
    let url = format!("{base}{path}");
    let response = ureq::get(&url).call().map_err(|err| match err {
        ureq::Error::Status(code, _) => eyre!("the broker refused the stream: HTTP {code}"),
        ureq::Error::Transport(transport) => {
            eyre!("could not reach the broker at {base}; is `crewd` running? ({transport})")
        }
    })?;
    event!(name: "cli.stream.connected", Level::DEBUG, url = %url, "streaming {{url}}");

    // Read the SSE body line by line as events arrive; the tail runs until the
    // connection ends or the caller interrupts.
    let reader = BufReader::new(response.into_reader());
    for line in reader.lines() {
        let line = line.wrap_err("the event stream ended unexpectedly")?;
        if let Some(event) = parse_data_line(&line) {
            on_event(&event);
        }
    }
    Ok(())
}

/// Parses one Server-Sent-Event `data:` line into an [`Event`], if it is one.
///
/// Non-data lines (the `id:` cursor, keep-alive comments, blank separators) yield
/// `None` and are skipped.
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
    }
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

/// A short description of a token-spend report against the crew budget (issue #54).
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

/// A short description of a done-gate verification step (issue #47).
fn verification_body(verification: &VerificationEvent) -> String {
    let task = &verification.task;
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

/// The wire label for a message's typed intent.
fn message_kind(kind: &MessageKind) -> &'static str {
    match kind {
        MessageKind::Order { .. } => "order",
        MessageKind::Question { .. } => "question",
        MessageKind::Answer => "answer",
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
    }
}

#[cfg(test)]
mod tests {
    use crew_substrate::core::{
        ChannelId, Event, EventKind, Lifecycle, Message, MessageId, MessageKind, RoleId, Sender,
        Timestamp,
    };

    use super::{normalize_base, parse_data_line, render_event, watch_path};

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
        use crew_substrate::core::{Verdict, VerificationEvent};

        let event = |verdict, verifier: Option<&str>, detail: &str| Event {
            kind: EventKind::Verification(VerificationEvent {
                task: "login".to_owned(),
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
            "submission names the owner and task: {submitted}"
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
    fn normalize_base_defaults_the_scheme_and_trims() {
        assert_eq!(normalize_base("localhost:2739/"), "http://localhost:2739");
        assert_eq!(
            normalize_base("http://127.0.0.1:2739"),
            "http://127.0.0.1:2739"
        );
        assert_eq!(
            normalize_base("https://broker.internal"),
            "https://broker.internal"
        );
    }
}
