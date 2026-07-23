//! The broker client behind the MCP tools.
//!
//! A thin synchronous client over the broker's localhost HTTP + SSE API; the MCP
//! server never touches the store directly. It acts as one role (the agent it
//! serves): `send` posts as that role, `inbox` reads the messages addressed to it
//! (self-filtered), and `roster` lists the unit.

use std::time::Duration;

use crew_core::{Channel, Event, EventKind, MessageKind, RoleId, Sender};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};

/// How long to wait to connect to the broker before giving up.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// How long to wait for a broker response before giving up, so a stuck broker
/// surfaces as a tool error rather than hanging the agent.
const READ_TIMEOUT: Duration = Duration::from_secs(30);

/// The channel `crew_send` posts to when neither a role nor a channel is given.
const DEFAULT_CHANNEL: &str = "@commander";

/// The broker client for one agent role.
#[derive(Debug)]
pub struct Broker {
    base: String,
    role: RoleId,
    agent: ureq::Agent,
    /// How many message events the inbox has already delivered, so a later read
    /// returns only what is new. The event log is append-only, so the count is a
    /// stable cursor.
    read_through: usize,
}

impl Broker {
    /// Builds a client for `role` against the broker at `base` (e.g. `http://127.0.0.1:2739`).
    #[must_use]
    pub fn new(base: impl Into<String>, role: RoleId) -> Self {
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(CONNECT_TIMEOUT)
            .timeout_read(READ_TIMEOUT)
            .build();
        Self {
            base: base.into(),
            role,
            agent,
            read_through: 0,
        }
    }

    /// The role this client acts as.
    #[must_use]
    pub fn role(&self) -> &RoleId {
        &self.role
    }

    /// Posts a note as this role to a role's direct channel, a named channel, or the
    /// commander by default, returning a short confirmation.
    ///
    /// # Errors
    /// Returns a message if the broker rejects the post or cannot be reached.
    pub fn send(
        &self,
        to: Option<&str>,
        channel: Option<&str>,
        body: &str,
    ) -> Result<String, String> {
        let channel = resolve_channel(to, channel);
        let url = format!("{}/channels/{channel}/messages", self.base);
        let payload = json!({
            "from": { "kind": "role", "id": self.role.as_str() },
            "kind": "note",
            "body": body,
        });
        match self
            .agent
            .post(&url)
            .set("content-type", "application/json")
            .send_string(&payload.to_string())
        {
            Ok(_) => Ok(format!("sent to {channel}")),
            Err(err) => Err(self.explain(err)),
        }
    }

    /// Returns the messages addressed to this role that arrived since the last read.
    ///
    /// Reads through the broker's history and keeps the message events on the role's
    /// channels (direct, a pair it belongs to, or `all-units`), dropping its own so a
    /// role never sees its own messages.
    ///
    /// # Errors
    /// Returns a message if the broker cannot be reached or its response is malformed.
    pub fn inbox(&mut self) -> Result<Vec<InboxItem>, String> {
        let messages = self.message_log()?;
        let start = self.read_through.min(messages.len());
        let new = messages[start..]
            .iter()
            .filter_map(|event| self.addressed(event))
            .collect();
        self.read_through = messages.len();
        Ok(new)
    }

    /// Registers this role on the roster with the lane it owns, so the unit sees it.
    ///
    /// This is how a role reaches the broker at boot: it announces itself and its
    /// owned paths, publishing a `lifecycle` event to the unit. Registering again
    /// updates the owned paths and marks the role working, so a restart is safe.
    ///
    /// # Errors
    /// Returns a message if the broker rejects the registration or cannot be reached.
    pub fn register(&self, owned_paths: &[String]) -> Result<(), String> {
        let url = format!("{}/roster", self.base);
        let payload = json!({
            "role": self.role.as_str(),
            "owned_paths": owned_paths,
        });
        match self
            .agent
            .post(&url)
            .set("content-type", "application/json")
            .send_string(&payload.to_string())
        {
            Ok(_) => Ok(()),
            Err(err) => Err(self.explain(err)),
        }
    }

    /// Lists the roster: every registered role with its owned paths and liveness.
    ///
    /// # Errors
    /// Returns a message if the broker cannot be reached or its response is malformed.
    pub fn roster(&self) -> Result<Vec<RoleEntry>, String> {
        let view: RosterView = self.get("/roster")?;
        Ok(view.roles)
    }

    /// Fetches the whole message log, oldest first, following the history cursor.
    fn message_log(&self) -> Result<Vec<Event>, String> {
        let mut events = Vec::new();
        let mut after: Option<String> = None;
        loop {
            let mut path = "/history?kind=message&limit=500".to_owned();
            if let Some(cursor) = &after {
                path.push_str("&after=");
                path.push_str(cursor);
            }
            let page: HistoryPage = self.get(&path)?;
            events.extend(page.events);
            match page.next_cursor {
                Some(cursor) => after = Some(cursor),
                None => break,
            }
        }
        Ok(events)
    }

    /// The event as an inbox item, if it is a message addressed to this role and not
    /// sent by it.
    fn addressed(&self, event: &Event) -> Option<InboxItem> {
        let EventKind::Message(message) = &event.kind else {
            return None;
        };
        if event.from == Sender::Role(self.role.clone()) {
            return None; // self-echo: a role never receives its own message
        }
        if !Channel::parse(event.channel.as_str())
            .is_some_and(|channel| channel.addresses(&self.role))
        {
            return None;
        }
        Some(InboxItem {
            from: sender_label(&event.from),
            channel: event.channel.as_str().to_owned(),
            kind: message_kind_label(&message.kind).to_owned(),
            body: message.body.clone(),
        })
    }

    /// Reads a JSON endpoint into `T`.
    fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T, String> {
        let url = format!("{}{path}", self.base);
        let response = self
            .agent
            .get(&url)
            .call()
            .map_err(|err| self.explain(err))?;
        let text = response
            .into_string()
            .map_err(|err| format!("could not read the broker response: {err}"))?;
        serde_json::from_str(&text)
            .map_err(|err| format!("could not parse the broker response: {err}"))
    }

    /// Turns a `ureq` error into an agent-readable message.
    fn explain(&self, err: ureq::Error) -> String {
        match err {
            ureq::Error::Status(code, response) => broker_message(response)
                .unwrap_or_else(|| format!("the broker returned HTTP {code}")),
            ureq::Error::Transport(transport) => {
                format!("could not reach the broker at {}: {transport}", self.base)
            }
        }
    }
}

/// One message addressed to the role, for display.
#[derive(Debug)]
pub struct InboxItem {
    /// Who sent it: a role's id, or `general`.
    pub from: String,
    /// The channel it was sent on.
    pub channel: String,
    /// The message kind (order, question, status, note, ...).
    pub kind: String,
    /// The message body.
    pub body: String,
}

/// One roster entry from `GET /roster`.
#[derive(Debug, Deserialize)]
pub struct RoleEntry {
    /// The role's id.
    pub role: String,
    /// The paths the role owns.
    pub owned_paths: Vec<String>,
    /// The role's liveness (working / idle / stopped / dead).
    pub liveness: String,
}

/// The shape of `GET /roster`.
#[derive(Debug, Deserialize)]
struct RosterView {
    roles: Vec<RoleEntry>,
}

/// The shape of `GET /history`.
#[derive(Debug, Deserialize)]
struct HistoryPage {
    events: Vec<Event>,
    #[serde(default)]
    next_cursor: Option<String>,
}

/// The channel a `send` targets: `to` becomes `@role`, a `channel` is used as is,
/// and neither defaults to the commander.
fn resolve_channel(to: Option<&str>, channel: Option<&str>) -> String {
    match (nonempty(to), nonempty(channel)) {
        (Some(role), _) => format!("@{role}"),
        (None, Some(name)) => name.to_owned(),
        (None, None) => DEFAULT_CHANNEL.to_owned(),
    }
}

/// The trimmed value if present and not blank.
fn nonempty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

/// The `{ "error": ... }` message from a broker error response, if any.
fn broker_message(response: ureq::Response) -> Option<String> {
    let text = response.into_string().ok()?;
    let value: Value = serde_json::from_str(&text).ok()?;
    value.get("error")?.as_str().map(str::to_owned)
}

/// A sender's label: a role's id, or `general`.
fn sender_label(from: &Sender) -> String {
    match from {
        Sender::Role(role) => role.as_str().to_owned(),
        Sender::General => "general".to_owned(),
    }
}

/// The wire label for a message's typed intent.
fn message_kind_label(kind: &MessageKind) -> &'static str {
    match kind {
        MessageKind::Order { .. } => "order",
        MessageKind::Question { .. } => "question",
        MessageKind::Answer => "answer",
        MessageKind::Status => "status",
        MessageKind::Artifact { .. } => "artifact",
        MessageKind::Note => "note",
    }
}

#[cfg(test)]
mod tests {
    use super::resolve_channel;

    #[test]
    fn resolve_channel_defaults_to_the_commander() {
        assert_eq!(resolve_channel(None, None), "@commander");
    }

    #[test]
    fn resolve_channel_maps_a_role_and_uses_a_named_channel_verbatim() {
        assert_eq!(resolve_channel(Some("backend"), None), "@backend");
        assert_eq!(resolve_channel(None, Some("all-units")), "all-units");
        // A role takes precedence and blank values fall through to the default.
        assert_eq!(resolve_channel(Some("qa"), Some("all-units")), "@qa");
        assert_eq!(resolve_channel(Some("  "), None), "@commander");
    }
}
