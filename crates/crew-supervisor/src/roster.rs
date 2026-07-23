//! The supervisor's roster client: register a role on spawn, deregister on exit.
//!
//! The supervisor owns each agent's process lifecycle, so it is the authority on
//! liveness: it registers a role with the broker the moment the process starts and
//! deregisters it when the process exits (issue #21). This is a thin synchronous
//! client over the broker's `/roster` HTTP API, distinct from the agent-facing
//! [`crew_mcp`](crew_mcp) client, which registers only its own role.

use std::time::Duration;

use crew_core::{BudgetEvent, RoleId, TaskId, TelemetryEvent, Timestamp};
use eyre::{eyre, Result};
use serde::Deserialize;
use serde_json::{json, Value};

/// A role's liveness, as the broker roster labels it.
///
/// The supervisor marks each transition of its lifecycle state machine with the
/// matching liveness (issue #22); the broker turns the change into a `lifecycle`
/// stream event (`working` first is `started`, again is `restarted`, and `idle` /
/// `stopped` / `dead` map directly).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Liveness {
    /// Up and working.
    Working,
    /// Registered but idle, its process stopped to save context and money.
    Idle,
    /// Cleanly stood down, its roster entry kept for a fast restart.
    Stopped,
    /// Gave up after exhausting its restart budget on repeated crashes.
    Dead,
}

impl Liveness {
    /// The wire label the broker roster expects.
    fn wire(self) -> &'static str {
        match self {
            Self::Working => "working",
            Self::Idle => "idle",
            Self::Stopped => "stopped",
            Self::Dead => "dead",
        }
    }
}

/// How long to wait to connect to the broker before giving up.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// How long to wait for a broker response before giving up, so a stuck broker
/// surfaces as an error rather than hanging the supervisor.
const READ_TIMEOUT: Duration = Duration::from_secs(30);

/// A client for the broker roster, used to register and deregister spawned roles.
///
/// Cheap to clone: [`ureq::Agent`] shares its connection pool on clone, so each
/// agent's monitor thread holds a copy to deregister its role on exit.
#[derive(Debug, Clone)]
pub struct RosterClient {
    base: String,
    agent: ureq::Agent,
    /// The task this supervisor is working, threaded onto every lifecycle transition
    /// so its events correlate to the task (issue #29). `None` outside a task context.
    task: Option<TaskId>,
}

impl RosterClient {
    /// Builds a client against the broker at `base` (e.g. `http://127.0.0.1:2739`).
    #[must_use]
    pub fn new(base: impl Into<String>) -> Self {
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(CONNECT_TIMEOUT)
            .timeout_read(READ_TIMEOUT)
            .build();
        Self {
            base: base.into(),
            agent,
            task: None,
        }
    }

    /// Sets the task context, so every lifecycle transition this client publishes
    /// carries the task id (issue #29).
    ///
    /// The supervisor threads the task it is working, so the roster's `started` /
    /// `idle` / `restarted` events correlate to it; the clone shares the connection
    /// pool, so a per-agent monitor keeps the same task context.
    #[must_use]
    pub fn with_task(mut self, task: TaskId) -> Self {
        self.task = Some(task);
        self
    }

    /// Registers `role` with the lane it owns, marking it working (`POST /roster`).
    ///
    /// # Errors
    /// Returns an error if the broker rejects the registration or cannot be reached.
    pub fn register(&self, role: &RoleId, owned_paths: &[String]) -> Result<()> {
        let url = format!("{}/roster", self.base);
        let mut body = json!({ "role": role.as_str(), "owned_paths": owned_paths });
        self.attach_task(&mut body);
        self.agent
            .post(&url)
            .set("content-type", "application/json")
            .send_string(&body.to_string())
            .map(|_response| ())
            .map_err(|err| eyre!("could not register role `{role}` with the broker: {err}"))
    }

    /// Marks `role` with a new liveness, keeping its owned paths (`POST /roster`).
    ///
    /// The role must already be registered; this changes only its liveness, which the
    /// broker publishes as the matching `lifecycle` event. Used for the idle, stopped,
    /// and dead transitions (a restart re-registers via [`register`](Self::register)).
    ///
    /// # Errors
    /// Returns an error if the broker rejects the update or cannot be reached.
    pub fn mark(&self, role: &RoleId, liveness: Liveness) -> Result<()> {
        let url = format!("{}/roster", self.base);
        let mut body = json!({ "role": role.as_str(), "liveness": liveness.wire() });
        self.attach_task(&mut body);
        self.agent
            .post(&url)
            .set("content-type", "application/json")
            .send_string(&body.to_string())
            .map(|_response| ())
            .map_err(|err| eyre!("could not mark role `{role}` as {}: {err}", liveness.wire()))
    }

    /// Reports a role's token spend against the crew budget (`POST /budget`, issue #54).
    ///
    /// The broker records it as a `budget` event on the stream, so spend against budget is
    /// visible and a cap hit is never silent. The supervisor computes the totals from the
    /// crew [`Budget`](crew_core::Budget); this only surfaces them.
    ///
    /// # Errors
    /// Returns an error if the broker rejects the report or cannot be reached.
    pub fn report_budget(&self, event: &BudgetEvent) -> Result<()> {
        let url = format!("{}/budget", self.base);
        let body = serde_json::to_string(event)
            .map_err(|err| eyre!("could not encode the budget report: {err}"))?;
        self.agent
            .post(&url)
            .set("content-type", "application/json")
            .send_string(&body)
            .map(|_response| ())
            .map_err(|err| eyre!("could not report budget for role `{}`: {err}", event.role))
    }

    /// Reports a role's per-turn token-and-cost usage (`POST /telemetry`, issue #55).
    ///
    /// The broker records it as a `telemetry` event and folds it into the `GET /stats`
    /// rollup, so per-role and aggregate spend is legible regardless of any budget.
    ///
    /// # Errors
    /// Returns an error if the broker rejects the report or cannot be reached.
    pub fn report_telemetry(&self, event: &TelemetryEvent) -> Result<()> {
        let url = format!("{}/telemetry", self.base);
        let body = serde_json::to_string(event)
            .map_err(|err| eyre!("could not encode the telemetry report: {err}"))?;
        self.agent
            .post(&url)
            .set("content-type", "application/json")
            .send_string(&body)
            .map(|_response| ())
            .map_err(|err| {
                eyre!(
                    "could not report telemetry for role `{}`: {err}",
                    event.role
                )
            })
    }

    /// Reports a shared-subscription usage reading (`POST /usage`, issue #56).
    ///
    /// The crew shares one subscription, so a single reading of the window against its limit
    /// drives the broker's one gauge: at or above the threshold it auto-pauses new work until
    /// `window_reset`. This is the seam the rate-limit detection (the stream-json parser,
    /// issue #24) drives; `percent` is the window fill (`0..=100`).
    ///
    /// # Errors
    /// Returns an error if the broker rejects the report or cannot be reached.
    pub fn report_usage(&self, percent: u8, window_reset: Timestamp) -> Result<()> {
        let url = format!("{}/usage", self.base);
        let body = json!({ "percent": percent, "window_reset": window_reset });
        self.agent
            .post(&url)
            .set("content-type", "application/json")
            .send_string(&body.to_string())
            .map(|_response| ())
            .map_err(|err| eyre!("could not report subscription usage: {err}"))
    }

    /// Adds the task id to a roster request body when a task context is set.
    fn attach_task(&self, body: &mut Value) {
        if let (Some(task), Value::Object(fields)) = (self.task, body) {
            fields.insert("task".to_owned(), json!(task));
        }
    }

    /// Deregisters `role` on exit (`DELETE /roster/{role}`).
    ///
    /// Idempotent: a `404` (the role is already gone) is treated as success, so a
    /// double deregister or a role the broker never saw is not an error.
    ///
    /// # Errors
    /// Returns an error if the broker rejects the request (other than `404`) or cannot
    /// be reached.
    pub fn deregister(&self, role: &RoleId) -> Result<()> {
        let url = format!("{}/roster/{}", self.base, role.as_str());
        match self.agent.delete(&url).call() {
            Ok(_response) => Ok(()),
            // Already gone: deregistering is idempotent, so this is not a failure.
            Err(ureq::Error::Status(404, _)) => Ok(()),
            Err(err) => Err(eyre!(
                "could not deregister role `{role}` from the broker: {err}"
            )),
        }
    }

    /// The ids of the roles currently on the roster (`GET /roster`).
    ///
    /// # Errors
    /// Returns an error if the broker cannot be reached or its response is malformed.
    pub fn roles(&self) -> Result<Vec<RoleId>> {
        let url = format!("{}/roster", self.base);
        let text = self
            .agent
            .get(&url)
            .call()
            .map_err(|err| eyre!("could not read the broker roster: {err}"))?
            .into_string()
            .map_err(|err| eyre!("could not read the roster response: {err}"))?;
        let view: RosterView = serde_json::from_str(&text)
            .map_err(|err| eyre!("could not parse the roster response: {err}"))?;
        Ok(view.roles.into_iter().map(|entry| entry.role).collect())
    }

    /// Reads the events at or after `since`, oldest first, following the history pages.
    ///
    /// The coordination-stall monitor (issue #48) reads a recent window of the stream to
    /// look for a crew stuck waiting on itself. Events are returned as raw JSON so the
    /// supervisor reads the broker's stable stream contract rather than coupling to
    /// `crew_core::EventKind`, which lets an event kind it does not model pass through.
    ///
    /// # Errors
    /// Returns an error if the broker cannot be reached or a page is malformed.
    pub fn history_since(&self, since: Timestamp) -> Result<Vec<Value>> {
        let url = format!("{}/history", self.base);
        let since = since.to_string();
        let mut events = Vec::new();
        let mut after: Option<String> = None;
        loop {
            let mut request = self.agent.get(&url).query("since", &since);
            if let Some(cursor) = &after {
                request = request.query("after", cursor);
            }
            let text = request
                .call()
                .map_err(|err| eyre!("could not read the broker history: {err}"))?
                .into_string()
                .map_err(|err| eyre!("could not read the history response: {err}"))?;
            let page: HistoryPage = serde_json::from_str(&text)
                .map_err(|err| eyre!("could not parse the history response: {err}"))?;
            events.extend(page.events);
            match page.next_cursor {
                Some(cursor) => after = Some(cursor),
                None => break,
            }
        }
        Ok(events)
    }
}

/// One page of `GET /history`: the events, and the cursor to the next page if any.
#[derive(Debug, Deserialize)]
struct HistoryPage {
    events: Vec<Value>,
    #[serde(default)]
    next_cursor: Option<String>,
}

/// The shape of `GET /roster` (only the role ids are read here).
#[derive(Debug, Deserialize)]
struct RosterView {
    roles: Vec<RosterEntry>,
}

/// One roster entry; extra fields (owned paths, liveness) are ignored.
#[derive(Debug, Deserialize)]
struct RosterEntry {
    role: RoleId,
}
