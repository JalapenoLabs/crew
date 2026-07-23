//! The synchronous broker client an agent talks to the crew through.
//!
//! A thin synchronous client over the broker's localhost HTTP + SSE API; it
//! never touches the store directly. Both agent-facing front-ends share it: the
//! MCP server ([`crew_mcp`]) dispatches its tools to a [`Broker`], and the CLI
//! shim ([the `crew` binary]) reuses the same client so a runtime without MCP,
//! such as Codex, maps its I/O onto the broker identically (issue #129).
//!
//! A client acts as one role (the agent it serves): [`send`](Broker::send)
//! posts as that role, [`inbox`](Broker::inbox) reads the messages addressed to
//! it (self-filtered), and [`roster`](Broker::roster) lists the unit. The
//! [`Broker`] is the entry point; the view structs it returns
//! ([`InboxItem`], [`RoleEntry`], [`LedgerItem`], and friends) shape one broker
//! response each for a front-end to render.
//!
//! [`crew_mcp`]: https://docs.rs/crew-mcp
//! [the `crew` binary]: https://docs.rs/crew-cli
//!
//! `inbox` has two paths (issue #76). With [`subscribe`](Broker::subscribe) it
//! holds a background subscription to the broker's per-role SSE inbox (`GET
//! /inbox?role=<role>`, issue #10), buffering events as they arrive; a read
//! then drains the buffered batch rather than refetching the whole message
//! history. Without a subscription (a runtime without streaming) it falls back
//! to the pull-based history read.

use std::{
    collections::{HashSet, VecDeque},
    fmt::Write as _,
    io::{BufRead, BufReader, Read},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex, PoisonError,
    },
    thread,
    time::Duration,
};

use crew_core::{
    path_in_lane, Channel, Event, EventKind, LaneEnforcement, MessageId, MessageKind, RoleId,
    Sender,
};
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::{json, Value};

/// How long to wait to connect to the broker before giving up.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// How long to wait for a broker response before giving up, so a stuck broker
/// surfaces as a tool error rather than hanging the agent.
const READ_TIMEOUT: Duration = Duration::from_secs(30);

/// How long the inbox stream waits before retrying a dropped connection.
///
/// Short, since the broker is on localhost and usually returns at once; long
/// enough that a genuinely down broker is not hammered. A resume carries the
/// `Last-Event-ID` cursor, so nothing is missed across the gap.
const RECONNECT_BACKOFF: Duration = Duration::from_secs(1);

/// The broker client for one agent role.
#[derive(Debug)]
pub struct Broker {
    base: String,
    role: RoleId,
    commander: RoleId,
    agent: ureq::Agent,
    /// How many message events the pull-based inbox has already delivered, so a
    /// later read returns only what is new. The event log is append-only,
    /// so the count is a stable cursor. Used only in the pull fallback; the
    /// push path uses [`InboxStream`].
    read_through: usize,
    /// The live inbox subscription, when [`subscribe`](Broker::subscribe) has
    /// established one (issue #76). When set, [`inbox`](Broker::inbox)
    /// drains its buffer; when `None`, it falls back to the pull-based
    /// read.
    inbox_stream: Option<InboxStream>,
}

impl Broker {
    /// Builds a client for `role` against the broker at `base`, with
    /// `commander` as the default addressee.
    ///
    /// The `commander` is the hub of the topology: a [`send`](Broker::send)
    /// with neither `to` nor `channel` reaches it (see
    /// `docs/communication.md`). It comes from the role card, which names
    /// the crew's commander.
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
            inbox_stream: None,
        }
    }

    /// Seeds the pull-based inbox cursor, so the next [`inbox`](Broker::inbox)
    /// returns only messages past `read_through`.
    ///
    /// The MCP server holds this cursor in memory across its session; a
    /// short-lived caller such as the CLI shim persists it instead and seeds a
    /// fresh client with it, so `crew inbox` resumes where the last call left
    /// off rather than re-reading the whole history (issue #130). Pass the
    /// value a prior [`read_through`](Broker::read_through) returned. It
    /// seeds only the pull fallback; the push path
    /// ([`subscribe`](Broker::subscribe)) tracks its own position.
    #[must_use]
    pub fn with_read_through(mut self, read_through: usize) -> Self {
        self.read_through = read_through;
        self
    }

    /// The role this client acts as.
    #[must_use]
    pub fn role(&self) -> &RoleId {
        &self.role
    }

    /// The number of inbox messages the pull-based read has delivered, a stable
    /// cursor into the append-only message log.
    ///
    /// Persist it after an [`inbox`](Broker::inbox) read and hand it to
    /// [`with_read_through`](Broker::with_read_through) on the next client, so
    /// a stateless caller shows only messages that arrived since (issue
    /// #130).
    #[must_use]
    pub fn read_through(&self) -> usize {
        self.read_through
    }

    /// Posts a note as this role to a role's direct channel, a named channel,
    /// or the commander by default, returning a short confirmation.
    ///
    /// The target follows the crew's one addressing rule
    /// ([`Channel::resolve`]): a `to` role wins, else a `channel` name,
    /// else the commander.
    ///
    /// # Errors
    /// Returns a message if the target is not routable, or the broker rejects
    /// the post or cannot be reached.
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

    /// Asks a typed `question` as this role, to a role, a channel, or the
    /// commander by default (issue #123).
    ///
    /// A `question` is the message kind the coordination-stall detector keys on
    /// (issue #48), so asking through this, rather than a plain `crew_send`
    /// note, is what lets a real deadlock surface on the stream. `options` are
    /// suggested answers, if any. The target follows the same one addressing
    /// rule as [`send`](Broker::send).
    ///
    /// # Errors
    /// Returns a message if the target is not routable, or the broker rejects
    /// the post or cannot be reached.
    pub fn ask(
        &self,
        to: Option<&str>,
        channel: Option<&str>,
        body: &str,
        options: &[String],
    ) -> Result<String, String> {
        let target = Channel::resolve(to, channel, &self.commander).ok_or_else(|| {
            "that is not a routable target; name a role, `all-units`, or a pair like `a+b`"
                .to_owned()
        })?;
        let payload = json!({
            "from": { "kind": "role", "id": self.role.as_str() },
            "kind": "question",
            "options": options,
            "body": body,
        });
        self.post_message(target.name().as_str(), &payload)
    }

    /// Answers a `question` as this role, naming the message it replies to
    /// (issue #123).
    ///
    /// `in_reply_to` is the id of the question being answered, as shown in the
    /// asker's inbox (`crew_inbox`); it threads the reply to its question and,
    /// as a substantive reply from the blocker, clears the wait the stall
    /// detector was tracking. The target follows the same addressing rule as
    /// [`send`](Broker::send).
    ///
    /// # Errors
    /// Returns a message if the target is not routable, `in_reply_to` is not a
    /// message id, or the broker rejects the post or cannot be reached.
    pub fn answer(
        &self,
        to: Option<&str>,
        channel: Option<&str>,
        body: &str,
        in_reply_to: &str,
    ) -> Result<String, String> {
        let target = Channel::resolve(to, channel, &self.commander).ok_or_else(|| {
            "that is not a routable target; name a role, `all-units`, or a pair like `a+b`"
                .to_owned()
        })?;
        let payload = json!({
            "from": { "kind": "role", "id": self.role.as_str() },
            "kind": "answer",
            "in_reply_to": in_reply_to,
            "body": body,
        });
        self.post_message(target.name().as_str(), &payload)
    }

    /// Issues an order as this role to `to`, giving the specialist a scoped
    /// task.
    ///
    /// This is the commander's fan-out handle: it posts an `order` message
    /// (title, scope, owned paths, acceptance) on the specialist's direct
    /// channel, so the specialist reads a task it can act on rather than
    /// freeform prose.
    ///
    /// # Errors
    /// Returns a message if `to` is not a plain role name, or the broker
    /// rejects the post or cannot be reached.
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

    /// Claims `task` for this role, or moves the role's claim to `state` (issue
    /// #45).
    ///
    /// A role claims before it starts and moves its claim to `done` when it
    /// finishes. The broker refuses a claim on work another role already
    /// holds; the returned error names the holder, so a conflict is
    /// surfaced rather than raced. An empty `title` keeps the task's
    /// current title.
    ///
    /// # Errors
    /// Returns a message if another role holds the task, or the broker cannot
    /// be reached.
    pub fn claim(&self, task: &str, state: &str, title: &str) -> Result<String, String> {
        let url = format!("{}/ledger", self.base);
        let payload = json!({
            "task": task,
            "owner": self.role.as_str(),
            "state": state,
            "title": title,
        });
        match self
            .agent
            .post(&url)
            .set("content-type", "application/json")
            .send_string(&payload.to_string())
        {
            Ok(_) => Ok(format!("{state}: {task}")),
            Err(err) => Err(self.explain(err)),
        }
    }

    /// Reads the work ledger: every claimed task, its owner, and its state.
    ///
    /// # Errors
    /// Returns a message if the broker cannot be reached or its response is
    /// malformed.
    pub fn ledger(&self) -> Result<Vec<LedgerItem>, String> {
        let view: LedgerView = self.get("/ledger")?;
        Ok(view.tasks)
    }

    /// Posts `payload` to `channel`, returning `sent to {channel}` or a broker
    /// error.
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

    /// Subscribes to this role's live inbox, so [`inbox`](Broker::inbox) drains
    /// buffered events instead of refetching the whole history each call
    /// (issue #76).
    ///
    /// Opens the broker's per-role SSE inbox (`GET /inbox?role=<role>`, issue
    /// #10) and seeds the backlog from history once, since a fresh stream
    /// starts at the live tail; a background thread then buffers new events
    /// as they arrive, resuming from a `Last-Event-ID` cursor across
    /// reconnects so nothing is missed. The push path takes over from the
    /// pull fallback from here on.
    ///
    /// If the stream cannot be opened (a runtime without streaming), this
    /// returns an error and leaves the client on the pull-based read.
    ///
    /// # Errors
    /// Returns a message if the broker's inbox stream cannot be opened, or the
    /// backlog read fails.
    pub fn subscribe(&mut self) -> Result<(), String> {
        // Open the stream first, so its live-tail cursor is fixed before the backlog
        // read. Any event that lands in the overlap is deduplicated by message
        // id below.
        let reader = open_inbox(&self.agent, &self.base, &self.role, None)
            .map_err(|err| self.explain(*err))?;

        // Seed the backlog: the messages already addressed to the role, which a fresh
        // stream (starting at the live tail) does not replay. This is the one-time
        // catch-up; later reads never refetch history.
        let mut seen: HashSet<MessageId> = HashSet::new();
        let mut backlog: VecDeque<Event> = VecDeque::new();
        for event in self.message_log()? {
            if let Some(id) = addressed_message_id(&event, &self.role) {
                seen.insert(id);
                backlog.push_back(event);
            }
        }

        let buffer = Arc::new(Mutex::new(backlog));
        let stop = Arc::new(AtomicBool::new(false));
        // The thread is a daemon for the client's life, stopped by the flag when the
        // subscription is dropped; it owns clones of the shared buffer and stop flag.
        let thread_buffer = Arc::clone(&buffer);
        let thread_stop = Arc::clone(&stop);
        let agent = self.agent.clone();
        let base = self.base.clone();
        let role = self.role.clone();
        thread::spawn(move || {
            run_inbox_stream(
                reader,
                &agent,
                &base,
                &role,
                &thread_buffer,
                seen,
                &thread_stop,
            );
        });
        self.inbox_stream = Some(InboxStream { buffer, stop });
        Ok(())
    }

    /// Returns the messages addressed to this role that arrived since the last
    /// read.
    ///
    /// With a live subscription (issue #76) this drains the events the
    /// background stream has buffered since the last read, advancing the
    /// cursor with no full-history refetch. Without one it falls back to
    /// reading the history and slicing from the pull cursor. Either way it
    /// keeps the message events on the role's channels (direct, a pair it
    /// belongs to, or `all-units`), dropping its own so a role never
    /// sees its own messages.
    ///
    /// # Errors
    /// Returns a message if the pull fallback cannot reach the broker or its
    /// response is malformed. The push path never fails: it drains an
    /// in-memory buffer.
    pub fn inbox(&mut self) -> Result<Vec<InboxItem>, String> {
        // Push path: drain the buffered batch the background stream has delivered.
        if let Some(events) = self.inbox_stream.as_ref().map(InboxStream::drain) {
            return Ok(events
                .iter()
                .filter_map(|event| self.addressed(event))
                .collect());
        }

        // Pull fallback (a runtime without streaming): read history, slice from the
        // cursor.
        let messages = self.message_log()?;
        let start = self.read_through.min(messages.len());
        let new = messages[start..]
            .iter()
            .filter_map(|event| self.addressed(event))
            .collect();
        self.read_through = messages.len();
        Ok(new)
    }

    /// Registers this role on the roster with the lane it owns, so the unit
    /// sees it.
    ///
    /// This is how a role reaches the broker at boot: it announces itself and
    /// its owned paths, publishing a `lifecycle` event to the unit.
    /// Registering again updates the owned paths and marks the role
    /// working, so a restart is safe.
    ///
    /// # Errors
    /// Returns a message if the broker rejects the registration or cannot be
    /// reached.
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

    /// Checks whether `path` is in the role's lane, warning or blocking per
    /// `enforcement` when it is not (issue #46).
    ///
    /// An in-lane path proceeds untouched. An out-of-lane path is reported on
    /// the stream (a `boundary` event) so the operator sees it, and the
    /// role is told to route the change through the commander rather than
    /// editing it silently. Under [`Block`](LaneEnforcement::Block) the
    /// edit is refused (an error); under [`Warn`](LaneEnforcement::Warn)
    /// the role may proceed after being warned.
    ///
    /// # Errors
    /// Returns a message if the path is out of lane and enforcement is `block`,
    /// or the broker cannot be reached to record the crossing.
    pub fn check_lane(
        &self,
        owned_paths: &[String],
        enforcement: LaneEnforcement,
        path: &str,
    ) -> Result<String, String> {
        if path_in_lane(owned_paths, path) {
            return Ok(format!("`{path}` is in your lane; proceed."));
        }
        match enforcement {
            LaneEnforcement::Off => Ok(format!(
                "`{path}` is outside your lane, but lane enforcement is off; proceed with care."
            )),
            LaneEnforcement::Warn => {
                self.report_boundary(path, false)?;
                Ok(format!(
                    "`{path}` is OUTSIDE your lane. This is reported to the unit. Do not edit it \
                     silently: route the change through the commander (crew_send) unless it is \
                     genuinely yours."
                ))
            }
            LaneEnforcement::Block => {
                self.report_boundary(path, true)?;
                Err(format!(
                    "`{path}` is OUTSIDE your lane and edits there are blocked. Route the change \
                     through the commander (crew_send) instead of editing it."
                ))
            }
        }
    }

    /// Records a lane crossing on the stream, so the operator sees it (issue
    /// #46).
    fn report_boundary(&self, path: &str, blocked: bool) -> Result<(), String> {
        let payload = json!({
            "role": self.role.as_str(),
            "path": path,
            "blocked": blocked,
        });
        self.post_json("/boundary", &payload)
    }

    /// Submits this role's finished work for adversarial verification (issue
    /// #47).
    ///
    /// Announces the task on the stream and, when `to` names a reviewer, asks
    /// it to verify. Submitting does not mark the work done: an independent
    /// role must pass it first, so confident-but-wrong work never ships.
    ///
    /// # Errors
    /// Returns a message if the broker rejects the submission or cannot be
    /// reached.
    pub fn submit(&self, task: &str, acceptance: &str, to: Option<&str>) -> Result<String, String> {
        let mut payload = json!({
            "role": self.role.as_str(),
            "task": task,
            "acceptance": acceptance,
        });
        if let Some(reviewer) = to {
            payload["to"] = json!(reviewer);
        }
        self.post_json("/gate/submit", &payload)?;
        Ok(format!(
            "submitted `{task}` for verification. It is not done until an independent role \
             tries to break it and passes it."
        ))
    }

    /// Reports the mission gracefully complete (`POST /complete`, issue #121).
    ///
    /// The graceful counterpart to the General's stand-down: the crew,
    /// typically through the commander, declares the work done so `crew
    /// notify` fires on a true finish. It announces, it does not halt: the
    /// broker records a `mission_complete` lifecycle event without gating
    /// the crew.
    ///
    /// # Errors
    /// Returns a message if the broker rejects the report or cannot be reached.
    pub fn complete(&self) -> Result<String, String> {
        self.post_json("/complete", &json!({ "role": self.role.as_str() }))?;
        Ok("reported the mission complete to the unit.".to_owned())
    }

    /// Records this role's verdict on a task another role submitted (issue
    /// #47).
    ///
    /// A `pass` marks the task done; otherwise the work returns to its owner
    /// with the `failure`. The broker refuses a verdict on one's own work,
    /// so a task passes only when an independent role could not break it.
    ///
    /// # Errors
    /// Returns a message if the verdict is refused (the verifier is the owner,
    /// or the task is not awaiting a verdict), or the broker cannot be
    /// reached.
    pub fn verdict(&self, task: &str, pass: bool, failure: &str) -> Result<String, String> {
        let payload = json!({
            "role": self.role.as_str(),
            "task": task,
            "pass": pass,
            "failure": failure,
        });
        self.post_json("/gate/verdict", &payload)?;
        Ok(if pass {
            format!("verified `{task}`: it holds against its acceptance. Marked done.")
        } else {
            format!("failed `{task}` and returned it to its owner with the failure.")
        })
    }

    /// Reads the done-gate: every task under verification and its standing
    /// (issue #47).
    ///
    /// # Errors
    /// Returns a message if the broker cannot be reached or its response is
    /// malformed.
    pub fn gate(&self) -> Result<GateSnapshot, String> {
        self.get("/gate")
    }

    /// Records a decision, interface, or gotcha on the shared situation board
    /// (issue #49).
    ///
    /// The board is the crew's durable memory: recording here so the crew stops
    /// re-deriving a settled `key`. Recording the same key again updates the
    /// entry.
    ///
    /// # Errors
    /// Returns a message if the broker rejects the change or cannot be reached.
    pub fn record(&self, key: &str, section: &str, body: &str) -> Result<String, String> {
        let payload = json!({
            "role": self.role.as_str(),
            "key": key,
            "section": section,
            "body": body,
        });
        self.post_json("/board", &payload)?;
        Ok(format!("recorded `{key}` on the board ({section})."))
    }

    /// Retracts a board entry the crew no longer holds (issue #49).
    ///
    /// # Errors
    /// Returns a message if the entry is not on the board, or the broker cannot
    /// be reached.
    pub fn retract(&self, key: &str) -> Result<String, String> {
        let payload = json!({
            "role": self.role.as_str(),
            "key": key,
            "retract": true,
        });
        self.post_json("/board", &payload)?;
        Ok(format!("retracted `{key}` from the board."))
    }

    /// Reads the shared situation board, optionally filtered to one section
    /// (issue #49).
    ///
    /// # Errors
    /// Returns a message if the broker cannot be reached or its response is
    /// malformed.
    pub fn board(&self, section: Option<&str>) -> Result<BoardSnapshot, String> {
        match section {
            Some(section) => self.get(&format!("/board?section={section}")),
            None => self.get("/board"),
        }
    }

    /// Fetches this role's bounded new-role briefing packet (issue #50).
    ///
    /// The packet is the current decision board plus a rolling summary scoped
    /// to the role's own timeline, capped to a byte budget, so a freshly
    /// spawned role catches up in seconds without reading the whole log.
    /// `task` optionally narrows the summary, and `budget` overrides the
    /// byte cap.
    ///
    /// # Errors
    /// Returns a message if the broker cannot be reached or its response is
    /// malformed.
    pub fn briefing(
        &self,
        task: Option<&str>,
        budget: Option<usize>,
    ) -> Result<BriefingPacket, String> {
        let url = format!("{}/briefing", self.base);
        let mut request = self.agent.get(&url).query("role", self.role.as_str());
        if let Some(task) = task {
            request = request.query("task", task);
        }
        let budget = budget.map(|budget| budget.to_string());
        if let Some(budget) = &budget {
            request = request.query("budget", budget);
        }
        let text = request
            .call()
            .map_err(|err| self.explain(err))?
            .into_string()
            .map_err(|err| format!("could not read the broker response: {err}"))?;
        serde_json::from_str(&text)
            .map_err(|err| format!("could not parse the broker response: {err}"))
    }

    /// Posts a JSON `payload` to `path`, discarding the body on success.
    fn post_json(&self, path: &str, payload: &Value) -> Result<(), String> {
        let url = format!("{}{path}", self.base);
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
    /// Returns a message if the broker cannot be reached or its response is
    /// malformed.
    pub fn roster(&self) -> Result<RosterSnapshot, String> {
        let view: RosterView = self.get("/roster")?;
        Ok(RosterSnapshot {
            standing: view.standing,
            roles: view.roles,
        })
    }

    /// Fetches the whole message log, oldest first, following the history
    /// cursor.
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

    /// The event as an inbox item, if it is a message addressed to this role
    /// and not sent by it.
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
            id: message.id.to_string(),
            from: sender_label(&event.from),
            channel: event.channel.as_str().to_owned(),
            kind: message_kind_label(&message.kind).to_owned(),
            detail: kind_detail(&message.kind),
            body: message.body.clone(),
            directive: message.kind.is_directive(),
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

/// A live subscription to the role's inbox, buffering events for a drain (issue
/// #76).
///
/// A background thread reads the per-role SSE stream into `buffer`;
/// [`inbox`](Broker::inbox) drains it. Dropping the subscription signals the
/// thread to stop.
#[derive(Debug)]
struct InboxStream {
    /// The events the stream has delivered since the last drain, oldest first.
    buffer: Arc<Mutex<VecDeque<Event>>>,
    /// Set to stop the background thread; it exits at its next read boundary.
    stop: Arc<AtomicBool>,
}

impl InboxStream {
    /// Takes every event buffered since the last drain, oldest first.
    fn drain(&self) -> Vec<Event> {
        let mut buffer = self.buffer.lock().unwrap_or_else(PoisonError::into_inner);
        buffer.drain(..).collect()
    }
}

impl Drop for InboxStream {
    fn drop(&mut self) {
        // Signal the thread to stop. It notices at its next read boundary: the SSE
        // keep-alive comment unblocks the read within the keep-alive interval, so the
        // thread exits shortly without the drop blocking on a join.
        self.stop.store(true, Ordering::Relaxed);
    }
}

/// Runs the background inbox stream: buffer events, reconnecting with a
/// `Last-Event-ID` cursor when the connection drops, until `stop` is set (issue
/// #76).
fn run_inbox_stream(
    reader: Box<dyn Read + Send + Sync>,
    agent: &ureq::Agent,
    base: &str,
    role: &RoleId,
    buffer: &Mutex<VecDeque<Event>>,
    mut seen: HashSet<MessageId>,
    stop: &AtomicBool,
) {
    let mut reader = reader;
    let mut last_seq = None;
    loop {
        last_seq = drain_connection(reader, role, buffer, &mut seen, last_seq, stop);
        if stop.load(Ordering::Relaxed) {
            return;
        }
        // The connection dropped; wait, then resume right after the last event
        // delivered. A failed reopen (the broker is briefly down) just loops to
        // back off and retry.
        reader = loop {
            thread::sleep(RECONNECT_BACKOFF);
            if stop.load(Ordering::Relaxed) {
                return;
            }
            if let Ok(next) = open_inbox(agent, base, role, last_seq) {
                break next;
            }
        };
    }
}

/// Reads one SSE connection to its end, buffering each addressed message, and
/// returns the highest event sequence seen, so a reconnect resumes right after
/// it.
fn drain_connection(
    reader: Box<dyn Read + Send + Sync>,
    role: &RoleId,
    buffer: &Mutex<VecDeque<Event>>,
    seen: &mut HashSet<MessageId>,
    mut last_seq: Option<u64>,
    stop: &AtomicBool,
) -> Option<u64> {
    let reader = BufReader::new(reader);
    for line in reader.lines() {
        if stop.load(Ordering::Relaxed) {
            return last_seq;
        }
        // A read error or EOF ends this connection; the caller reconnects from
        // `last_seq`.
        let Ok(line) = line else {
            return last_seq;
        };
        if let Some(id) = line.strip_prefix("id:") {
            if let Ok(seq) = id.trim().parse::<u64>() {
                last_seq = Some(seq);
            }
        } else if let Some(data) = line.strip_prefix("data:") {
            let Ok(event) = serde_json::from_str::<Event>(data.trim_start()) else {
                continue;
            };
            let Some(id) = addressed_message_id(&event, role) else {
                continue; // the inbox also carries non-message events; keep
                          // only messages
            };
            // Skip a message the backlog seed already holds (the connect / read overlap).
            if seen.contains(&id) {
                continue;
            }
            buffer
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push_back(event);
        }
    }
    last_seq
}

/// Opens the role's SSE inbox stream, resuming right after `last_seq` when one
/// is given.
///
/// The error is boxed because [`ureq::Error`] carries the whole response, so
/// returning it unboxed would bloat every `Ok` in the hot read loop.
fn open_inbox(
    agent: &ureq::Agent,
    base: &str,
    role: &RoleId,
    last_seq: Option<u64>,
) -> Result<Box<dyn Read + Send + Sync>, Box<ureq::Error>> {
    let url = format!("{base}/inbox?role={}", role.as_str());
    let mut request = agent.get(&url);
    if let Some(seq) = last_seq {
        request = request.set("Last-Event-ID", &seq.to_string());
    }
    Ok(request.call().map_err(Box::new)?.into_reader())
}

/// The message id of `event`, if it is a message addressed to `role` and not
/// sent by it.
///
/// The per-role inbox stream already filters to the role's channels and drops
/// its own messages, but re-checking keeps the push and pull paths agreeing,
/// and yields the id the backlog seed deduplicates against.
fn addressed_message_id(event: &Event, role: &RoleId) -> Option<MessageId> {
    let EventKind::Message(message) = &event.kind else {
        return None;
    };
    if event.from == Sender::Role(role.clone()) {
        return None;
    }
    Channel::parse(event.channel.as_str())
        .is_some_and(|channel| channel.addresses(role))
        .then_some(message.id)
}

/// One message addressed to the role, for display.
#[derive(Debug)]
pub struct InboxItem {
    /// The message's id, so a reply can reference it: `crew_answer` names the
    /// question it answers by this id.
    pub id: String,
    /// Who sent it: a role's id, or `general`.
    pub from: String,
    /// The channel it was sent on.
    pub channel: String,
    /// The message kind (order, question, status, note, ...).
    pub kind: String,
    /// A human summary of the kind's structured fields, empty when it has none.
    ///
    /// An order carries its title, scope, owned paths, and acceptance here, so
    /// a specialist reads the task from its inbox rather than losing it to
    /// the body.
    pub detail: String,
    /// The message body.
    pub body: String,
    /// Whether this is a General directive (a `redirect` or `belay`) the role
    /// must honor at once, so the inbox render can flag it (see
    /// `docs/communication.md`).
    pub directive: bool,
}

/// One task in the work ledger from `GET /ledger` (issue #45).
#[derive(Debug, Deserialize)]
pub struct LedgerItem {
    /// The task's key.
    pub task: String,
    /// A short human title, or empty.
    pub title: String,
    /// The role that owns the claim.
    pub owner: String,
    /// The task's state: `claimed`, `in_progress`, `blocked`, or `done`.
    pub state: String,
}

/// The shape of `GET /ledger`.
#[derive(Debug, Deserialize)]
struct LedgerView {
    tasks: Vec<LedgerItem>,
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
    /// Whether the role is paused on its own (issue #41); it is also gated
    /// whenever the crew is not `running`.
    #[serde(default)]
    pub paused: bool,
}

/// A roster read: the crew's control standing and its registered roles (issue
/// #41).
#[derive(Debug)]
pub struct RosterSnapshot {
    /// The crew's control standing.
    pub standing: Standing,
    /// The registered roles, sorted by id.
    pub roles: Vec<RoleEntry>,
}

/// The done-gate read from `GET /gate`: every task under verification (issue
/// #47).
#[derive(Debug, Deserialize)]
pub struct GateSnapshot {
    /// The tasks under the gate, ordered by title.
    pub tasks: Vec<GateTask>,
}

/// One task's standing in the done-gate.
#[derive(Debug, Deserialize)]
pub struct GateTask {
    /// The task title.
    pub task: String,
    /// The role that submitted the work and owns any rework.
    pub owner: String,
    /// The independent role that returned the latest verdict, if any.
    #[serde(default)]
    pub verifier: Option<String>,
    /// Where the task stands: `submitted`, `passed`, or `failed`.
    pub verdict: String,
    /// The acceptance being claimed, or the failure on a handback.
    #[serde(default)]
    pub detail: String,
}

/// The situation board read from `GET /board`: the crew's durable memory (issue
/// #49).
#[derive(Debug, Deserialize)]
pub struct BoardSnapshot {
    /// The board's entries, ordered by section then topic.
    pub entries: Vec<BoardEntryView>,
}

/// One entry on the situation board.
#[derive(Debug, Deserialize)]
pub struct BoardEntryView {
    /// The entry's stable key (its topic).
    pub key: String,
    /// Which section it belongs to: `decision`, `interface`, or `gotcha`.
    pub section: String,
    /// The role that recorded it.
    pub author: String,
    /// The entry's content.
    pub body: String,
}

/// The bounded new-role briefing packet from `GET /briefing` (issue #50).
#[derive(Debug, Deserialize)]
pub struct BriefingPacket {
    /// The rendered packet text a role reads on boot.
    pub text: String,
    /// The packet's size in bytes.
    pub size: usize,
    /// The byte budget it was held to.
    pub budget: usize,
    /// Whether content was dropped to fit the budget.
    pub capped: bool,
}

/// The shape of `GET /roster`.
#[derive(Debug, Deserialize)]
struct RosterView {
    /// The crew's control standing: `running`, `paused`, or `stood_down` (issue
    /// #41).
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
        MessageKind::Answer { .. } => "answer",
        MessageKind::Status => "status",
        MessageKind::Artifact { .. } => "artifact",
        MessageKind::Note => "note",
        MessageKind::Redirect => "redirect",
        MessageKind::Belay => "belay",
    }
}

/// A human summary of a kind's structured fields, so the inbox surfaces them.
///
/// Returns an empty string for kinds whose content is only their body (note,
/// status, answer). An order renders as the task a specialist can act on.
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
    use crew_core::{MessageKind, RoleId, Sender};

    use super::{message_kind_label, sender_label};

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
