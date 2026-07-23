//! The broker client behind `crew send` and `crew watch`.
//!
//! A thin synchronous HTTP client over the broker's localhost API: `send` posts a
//! message as the General to `POST /channels/{channel}/messages`, and `watch` tails
//! an SSE endpoint (`/stream` or `/inbox`), rendering each event so the General sees
//! who said what, on which channel.

use std::io::{BufRead, BufReader};

use crew_core::{Activity, Event, EventKind, MessageKind, Sender};
use eyre::{bail, eyre, Result, WrapErr};
use serde_json::json;
use tracing::{event, Level};

/// Posts `message` from the General to `channel`, printing a confirmation.
///
/// # Errors
/// Returns an error if the broker cannot be reached or rejects the message.
pub fn send(base: &str, channel: &str, message: &str) -> Result<()> {
    let url = format!("{base}/channels/{channel}/messages");
    let body = json!({ "from": { "kind": "general" }, "kind": "note", "body": message });

    match ureq::post(&url)
        .set("content-type", "application/json")
        .send_string(&body.to_string())
    {
        Ok(_) => {
            event!(name: "cli.send.posted", Level::DEBUG, crew.channel = channel, "posted to {{crew.channel}}");
            println!("sent to {channel}");
            Ok(())
        }
        // The broker answered with a typed 4xx/5xx; surface its reason.
        Err(ureq::Error::Status(code, response)) => {
            let reason = broker_error(response).unwrap_or_else(|| format!("HTTP {code}"));
            bail!("the broker rejected the message: {reason}")
        }
        // A transport error means the broker is unreachable.
        Err(err) => Err(err)
            .wrap_err_with(|| format!("could not reach the broker at {base}; is `crewd` running?")),
    }
}

/// Tails `path` (an SSE endpoint) on the broker, rendering each event until the
/// stream closes or the user interrupts.
///
/// # Errors
/// Returns an error if the broker cannot be reached or refuses the stream.
pub fn watch(base: &str, path: &str) -> Result<()> {
    let url = format!("{base}{path}");
    let response = ureq::get(&url).call().map_err(|err| match err {
        ureq::Error::Status(code, _) => eyre!("the broker refused the watch: HTTP {code}"),
        ureq::Error::Transport(transport) => {
            eyre!("could not reach the broker at {base}; is `crewd` running? ({transport})")
        }
    })?;
    event!(name: "cli.watch.connected", Level::DEBUG, url = %url, "watching {{url}}");

    // Read the SSE body line by line as events arrive; the tail runs until the
    // connection ends or the user interrupts.
    let reader = BufReader::new(response.into_reader());
    for line in reader.lines() {
        let line = line.wrap_err("the watch stream ended unexpectedly")?;
        if let Some(event) = parse_data_line(&line) {
            println!("{}", render_event(&event));
        }
    }
    Ok(())
}

/// Extracts the `{ "error": ... }` message from a broker error response, if any.
fn broker_error(response: ureq::Response) -> Option<String> {
    let text = response.into_string().ok()?;
    let value: serde_json::Value = serde_json::from_str(&text).ok()?;
    value.get("error")?.as_str().map(str::to_owned)
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
    use crew_core::{
        ChannelId, Event, EventKind, Lifecycle, Message, MessageId, MessageKind, RoleId, Sender,
        Timestamp,
    };

    use super::{parse_data_line, render_event};

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
}
