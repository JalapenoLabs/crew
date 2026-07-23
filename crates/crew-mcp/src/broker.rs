//! The broker client behind the MCP tools.
//!
//! A thin synchronous client over the broker's localhost HTTP + SSE API; the MCP
//! server never touches the store directly. It acts as one role (the agent it
//! serves): `send` posts as that role, `inbox` reads the messages addressed to it
//! (self-filtered), and `roster` lists the unit.

use std::fmt::Write as _;
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

/// The broker client for one agent role.
#[derive(Debug)]
pub struct Broker {
    base: String,
    role: RoleId,
    commander: RoleId,
    agent: ureq::Agent,
    /// How many message events the inbox has already delivered, so a later read
    /// returns only what is new. The event log is append-only, so the count is a
    /// stable cursor.
    read_through: usize,
}

impl Broker {
    /// Builds a client for `role` against the broker at `base`, with `commander` as the
    /// default addressee.
    ///
    /// The `commander` is the hub of the topology: a [`send`](Broker::send) with neither
    /// `to` nor `channel` reaches it (see `docs/communication.md`). It comes from the
    /// role card, which names the crew's commander.
    #[must_use]
    pub fn new(base: impl Into<String>, role: RoleId, commander: RoleId) -> Self {
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(CONNECT_TIMEOUT)
            .timeout_read(READ_TIMEOUT)
            .build();
        Self {
            base: base.into(),
            role,
            commander,
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
    /// The target follows the crew's one addressing rule ([`Channel::resolve`]): a `to`
    /// role wins, else a `channel` name, else the commander.
    ///
    /// # Errors
    /// Returns a message if the target is not routable, or the broker rejects the post
    /// or cannot be reached.
    pub fn send(
        &self,
        to: Option<&str>,
        channel: Option<&str>,
        body: &str,
    ) -> Result<String, String> {
        let target = Channel::resolve(to, channel, &self.commander).ok_or_else(|| {
            "that is not a routable target; name a role, `all-units`, or a pair like `a+b`"
                .to_owned()
        })?;
        let payload = json!({
            "from": { "kind": "role", "id": self.role.as_str() },
            "kind": "note",
            "body": body,
        });
        self.post_message(target.name().as_str(), &payload)
    }

    /// Issues an order as this role to `to`, giving the specialist a scoped task.
    ///
    /// This is the commander's fan-out handle: it posts an `order` message (title,
    /// scope, owned paths, acceptance) on the specialist's direct channel, so the
    /// specialist reads a task it can act on rather than freeform prose.
    ///
    /// # Errors
    /// Returns a message if `to` is not a plain role name, or the broker rejects the
    /// post or cannot be reached.
    pub fn order(
        &self,
        to: &str,
        title: &str,
        scope: &str,
        owned_paths: &[String],
        acceptance: &str,
        body: &str,
    ) -> Result<String, String> {
        let target = Channel::resolve(Some(to), None, &self.commander)
            .filter(|channel| matches!(channel, Channel::Direct(_)))
            .ok_or_else(|| format!("`{to}` is not a role to order; name a single specialist"))?;
        let payload = json!({
            "from": { "kind": "role", "id": self.role.as_str() },
            "kind": "order",
            "title": title,
            "scope": scope,
            "owned_paths": owned_paths,
            "acceptance": acceptance,
            "body": body,
        });
        self.post_message(target.name().as_str(), &payload)?;
        Ok(format!("ordered {to}: {title}"))
    }

    /// Posts `payload` to `channel`, returning `sent to {channel}` or a broker error.
    fn post_message(&self, channel: &str, payload: &Value) -> Result<String, String> {
        let url = format!("{}/channels/{channel}/messages", self.base);
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

    /// Reads the roster: the crew's control standing and every registered role.
    ///
    /// # Errors
    /// Returns a message if the broker cannot be reached or its response is malformed.
    pub fn roster(&self) -> Result<RosterSnapshot, String> {
        let view: RosterView = self.get("/roster")?;
        Ok(RosterSnapshot {
            standing: view.standing,
            roles: view.roles,
        })
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
            detail: kind_detail(&message.kind),
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
    /// A human summary of the kind's structured fields, empty when it has none.
    ///
    /// An order carries its title, scope, owned paths, and acceptance here, so a
    /// specialist reads the task from its inbox rather than losing it to the body.
    pub detail: String,
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
    /// Whether the role is paused on its own (issue #41); it is also gated whenever the
    /// crew is not `running`.
    #[serde(default)]
    pub paused: bool,
}

/// A roster read: the crew's control standing and its registered roles (issue #41).
#[derive(Debug)]
pub struct RosterSnapshot {
    /// The crew's control standing.
    pub standing: Standing,
    /// The registered roles, sorted by id.
    pub roles: Vec<RoleEntry>,
}

/// The shape of `GET /roster`.
#[derive(Debug, Deserialize)]
struct RosterView {
    /// The crew's control standing: `running`, `paused`, or `stood_down` (issue #41).
    #[serde(default)]
    standing: Standing,
    roles: Vec<RoleEntry>,
}

/// The crew's control standing from `GET /roster` (issue #41).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Standing {
    /// Normal: roles pull work as usual.
    #[default]
    Running,
    /// Globally paused: no role pulls new work.
    Paused,
    /// Stood down: an emergency halt.
    StoodDown,
}

/// The shape of `GET /history`.
#[derive(Debug, Deserialize)]
struct HistoryPage {
    events: Vec<Event>,
    #[serde(default)]
    next_cursor: Option<String>,
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

/// A human summary of a kind's structured fields, so the inbox surfaces them.
///
/// Returns an empty string for kinds whose content is only their body (note, status,
/// answer). An order renders as the task a specialist can act on.
fn kind_detail(kind: &MessageKind) -> String {
    match kind {
        MessageKind::Order {
            title,
            scope,
            owned_paths,
            acceptance,
        } => {
            // Built from short pieces; `write!` to a String never fails.
            let mut detail = title.clone();
            if !scope.is_empty() {
                let _ = write!(detail, "; scope: {scope}");
            }
            if !owned_paths.is_empty() {
                let _ = write!(detail, "; owns: {}", owned_paths.join(", "));
            }
            if !acceptance.is_empty() {
                let _ = write!(detail, "; acceptance: {acceptance}");
            }
            detail
        }
        MessageKind::Question { options } if !options.is_empty() => {
            format!("options: {}", options.join(", "))
        }
        MessageKind::Artifact { reference, .. } => format!("artifact: {reference}"),
        _ => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::{message_kind_label, sender_label};
    use crew_core::{MessageKind, RoleId, Sender};

    #[test]
    fn a_sender_labels_a_role_or_the_general() {
        assert_eq!(
            sender_label(&Sender::Role(RoleId::new("backend"))),
            "backend"
        );
        assert_eq!(sender_label(&Sender::General), "general");
    }

    #[test]
    fn a_message_kind_labels_an_order() {
        let order = MessageKind::Order {
            title: "build login".to_owned(),
            scope: "the /login route".to_owned(),
            owned_paths: vec!["api/".to_owned()],
            acceptance: "tests green".to_owned(),
        };
        assert_eq!(message_kind_label(&order), "order");
        assert_eq!(message_kind_label(&MessageKind::Note), "note");
    }
}
