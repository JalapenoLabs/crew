//! The supervisor's roster client: register a role on spawn, deregister on
//! exit.
//!
//! The supervisor owns each agent's process lifecycle, so it is the authority
//! on liveness: it registers a role with the broker the moment the process
//! starts and deregisters it when the process exits (issue #21). This is a thin
//! synchronous client over the broker's `/roster` HTTP API, distinct from the
//! agent-facing [`crew_mcp`](crew_mcp) client, which registers only its own
//! role.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, PoisonError},
};

use crew_core::{Activity, BudgetEvent, RoleId, StallEvent, TaskId, TelemetryEvent, Timestamp};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::error::{roster_error, Error};

/// The roster client's result: a canonical [`Error`] on failure (issue #193).
type Result<T> = std::result::Result<T, Error>;

/// A role's liveness, as the broker roster labels it.
///
/// The supervisor marks each transition of its lifecycle state machine with the
/// matching liveness (issue #22); the broker turns the change into a
/// `lifecycle` stream event (`working` first is `started`, again is
/// `restarted`, and `idle` / `stopped` / `dead` map directly).
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

/// A client for the broker roster, used to register and deregister spawned
/// roles.
///
/// Cheap to clone: [`ureq::Agent`] shares its connection pool on clone, so each
/// agent's monitor thread holds a copy to deregister its role on exit.
#[derive(Debug, Clone)]
pub struct RosterClient {
    base: String,
    agent: ureq::Agent,
    /// The task this supervisor is working, threaded onto every lifecycle
    /// transition so its events correlate to the task (issue #29). `None`
    /// outside a task context. A per-role assignment (`tasks`) supersedes it.
    task: Option<TaskId>,
    /// The task an order assigned each role, so a multi-agent fleet correlates
    /// each role's own lifecycle and activity events to the task that role
    /// adopted, not one task for the whole client (issue #223).
    ///
    /// Shared behind an [`Arc`] so every clone (the per-agent drivers, the
    /// activity forwarder, and the monitors) reads what the fleet's order
    /// watcher writes; the map key is the role.
    tasks: Arc<Mutex<HashMap<RoleId, TaskId>>>,
}

impl RosterClient {
    /// Builds a client against the broker at `base` (e.g. `http://127.0.0.1:2739`).
    #[must_use]
    pub fn new(base: impl Into<String>) -> Self {
        Self {
            base: base.into(),
            agent: crew_client::broker_agent(),
            task: None,
            tasks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Stamps the `task` an order assigned onto `role`, so this client's
    /// subsequent lifecycle and activity events for that role correlate to it
    /// (issue #223).
    ///
    /// The fleet's order watcher calls this when an order addressed to a role
    /// it manages appears on the stream. Because the assignment is shared
    /// across every clone of this client (the per-agent drivers, the
    /// activity forwarder, and the monitors all hold clones), the
    /// correlation reaches every event any of them publishes for the role.
    /// A newer order for the same role supersedes the old task, matching
    /// how the assigned agent adopts the newest order (issue #132).
    pub fn set_task(&self, role: RoleId, task: TaskId) {
        self.tasks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .insert(role, task);
    }

    /// The task to correlate `role`'s events to: the per-role task an order
    /// assigned (issue #223), falling back to this client's own task context
    /// (issue #29) when the role has no assignment.
    fn task_for(&self, role: &RoleId) -> Option<TaskId> {
        self.tasks
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(role)
            .copied()
            .or(self.task)
    }

    /// Sets the task context, so every lifecycle transition this client
    /// publishes carries the task id (issue #29).
    ///
    /// The supervisor threads the task it is working, so the roster's `started`
    /// / `idle` / `restarted` events correlate to it; the clone shares the
    /// connection pool, so a per-agent monitor keeps the same task context.
    #[must_use]
    pub fn with_task(mut self, task: TaskId) -> Self {
        self.task = Some(task);
        self
    }

    /// Registers `role` with the lane it owns, marking it working (`POST
    /// /roster`).
    ///
    /// # Errors
    /// Returns an error if the broker rejects the registration or cannot be
    /// reached.
    pub fn register(&self, role: &RoleId, owned_paths: &[String]) -> Result<()> {
        let url = format!("{}/roster", self.base);
        let mut body = json!({ "role": role.as_str(), "owned_paths": owned_paths });
        self.attach_task(role, &mut body);
        self.agent
            .post(&url)
            .set("content-type", "application/json")
            .send_string(&body.to_string())
            .map(|_response| ())
            .map_err(|err| roster_error!("could not register role `{role}` with the broker: {err}"))
    }

    /// Marks `role` with a new liveness, keeping its owned paths (`POST
    /// /roster`).
    ///
    /// The role must already be registered; this changes only its liveness,
    /// which the broker publishes as the matching `lifecycle` event. Used
    /// for the idle, stopped, and dead transitions (a restart re-registers
    /// via [`register`](Self::register)).
    ///
    /// # Errors
    /// Returns an error if the broker rejects the update or cannot be reached.
    pub fn mark(&self, role: &RoleId, liveness: Liveness) -> Result<()> {
        let url = format!("{}/roster", self.base);
        let mut body = json!({ "role": role.as_str(), "liveness": liveness.wire() });
        self.attach_task(role, &mut body);
        self.agent
            .post(&url)
            .set("content-type", "application/json")
            .send_string(&body.to_string())
            .map(|_response| ())
            .map_err(|err| {
                roster_error!("could not mark role `{role}` as {}: {err}", liveness.wire())
            })
    }

    /// Reports a role's token spend against the crew budget (`POST /budget`,
    /// issue #54).
    ///
    /// The broker records it as a `budget` event on the stream, so spend
    /// against budget is visible and a cap hit is never silent. The
    /// supervisor computes the totals from the
    /// crew [`Budget`](crew_core::Budget); this only surfaces them.
    ///
    /// # Errors
    /// Returns an error if the broker rejects the report or cannot be reached.
    pub fn report_budget(&self, event: &BudgetEvent) -> Result<()> {
        let url = format!("{}/budget", self.base);
        let body = serde_json::to_string(event)
            .map_err(|err| roster_error!("could not encode the budget report: {err}"))?;
        self.agent
            .post(&url)
            .set("content-type", "application/json")
            .send_string(&body)
            .map(|_response| ())
            .map_err(|err| {
                roster_error!("could not report budget for role `{}`: {err}", event.role)
            })
    }

    /// Reports a role's per-turn token-and-cost usage (`POST /telemetry`, issue
    /// #55).
    ///
    /// The broker records it as a `telemetry` event and folds it into the `GET
    /// /stats` rollup, so per-role and aggregate spend is legible
    /// regardless of any budget.
    ///
    /// # Errors
    /// Returns an error if the broker rejects the report or cannot be reached.
    pub fn report_telemetry(&self, event: &TelemetryEvent) -> Result<()> {
        let url = format!("{}/telemetry", self.base);
        let body = serde_json::to_string(event)
            .map_err(|err| roster_error!("could not encode the telemetry report: {err}"))?;
        self.agent
            .post(&url)
            .set("content-type", "application/json")
            .send_string(&body)
            .map(|_response| ())
            .map_err(|err| {
                roster_error!(
                    "could not report telemetry for role `{}`: {err}",
                    event.role
                )
            })
    }

    /// Records a role's parsed stream-json activity on the stream (`POST
    /// /activity`, issue #24).
    ///
    /// The broker records it as an `activity` event keyed by the role and
    /// correlated to this client's task (issue #29), so a role's turns and tool
    /// calls appear on its per-agent timeline (`GET /activity?agent=<role>`)
    /// and the aggregate stream. The supervisor's parser produces the
    /// [`Activity`](crew_core::Activity); this only surfaces it.
    ///
    /// # Errors
    /// Returns an error if the broker rejects the report or cannot be reached.
    pub fn emit_activity(&self, role: &RoleId, activity: &Activity) -> Result<()> {
        let url = format!("{}/activity", self.base);
        let mut body = json!({ "role": role.as_str(), "activity": activity });
        if let Some(task) = self.task_for(role) {
            body["task"] = json!(task);
        }
        self.agent
            .post(&url)
            .set("content-type", "application/json")
            .send_string(&body.to_string())
            .map(|_response| ())
            .map_err(|err| roster_error!("could not report activity for role `{role}`: {err}"))
    }

    /// Reports a shared-subscription usage reading (`POST /usage`, issue #56).
    ///
    /// The crew shares one subscription, so a single reading of the window
    /// against its limit drives the broker's one gauge: at or above the
    /// threshold it auto-pauses new work until `window_reset`. This is the
    /// seam the rate-limit detection (the stream-json parser, issue #24)
    /// drives; `percent` is the window fill (`0..=100`).
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
            .map_err(|err| roster_error!("could not report subscription usage: {err}"))
    }

    /// Surfaces a coordination stall on the stream (`POST /stall`, issue #120).
    ///
    /// The stall monitor (issue #48) supplies the finding it built (its kind,
    /// detected-or-resolved status, roles, and specific detail); the broker
    /// records it as a `stall` event, so `crew notify` fires the "a role is
    /// stalled" moment and the `crew top` cockpit renders live stalls.
    ///
    /// # Errors
    /// Returns an error if the broker rejects the report or cannot be reached.
    pub fn report_stall(&self, event: &StallEvent) -> Result<()> {
        let url = format!("{}/stall", self.base);
        let body = serde_json::to_string(event)
            .map_err(|err| roster_error!("could not encode the stall report: {err}"))?;
        self.agent
            .post(&url)
            .set("content-type", "application/json")
            .send_string(&body)
            .map(|_response| ())
            .map_err(|err| {
                roster_error!("could not report the {} stall: {err}", event.kind.label())
            })
    }

    /// Adds `role`'s correlated task id to a roster request body when one is
    /// set.
    fn attach_task(&self, role: &RoleId, body: &mut Value) {
        if let (Some(task), Value::Object(fields)) = (self.task_for(role), body) {
            fields.insert("task".to_owned(), json!(task));
        }
    }

    /// Deregisters `role` on exit (`DELETE /roster/{role}`).
    ///
    /// Idempotent: a `404` (the role is already gone) is treated as success, so
    /// a double deregister or a role the broker never saw is not an error.
    ///
    /// # Errors
    /// Returns an error if the broker rejects the request (other than `404`) or
    /// cannot be reached.
    pub fn deregister(&self, role: &RoleId) -> Result<()> {
        let url = format!("{}/roster/{}", self.base, role.as_str());
        match self.agent.delete(&url).call() {
            Ok(_response) => Ok(()),
            // Already gone: deregistering is idempotent, so this is not a failure.
            Err(ureq::Error::Status(404, _)) => Ok(()),
            Err(err) => Err(roster_error!(
                "could not deregister role `{role}` from the broker: {err}"
            )),
        }
    }

    /// The ids of the roles currently on the roster (`GET /roster`).
    ///
    /// # Errors
    /// Returns an error if the broker cannot be reached or its response is
    /// malformed.
    pub fn roles(&self) -> Result<Vec<RoleId>> {
        let url = format!("{}/roster", self.base);
        let text = self
            .agent
            .get(&url)
            .call()
            .map_err(|err| roster_error!("could not read the broker roster: {err}"))?
            .into_string()
            .map_err(|err| roster_error!("could not read the roster response: {err}"))?;
        let view: RosterView = serde_json::from_str(&text)
            .map_err(|err| roster_error!("could not parse the roster response: {err}"))?;
        Ok(view.roles.into_iter().map(|entry| entry.role).collect())
    }

    /// Reads the crew's current pause state from `GET /roster` (issue #187).
    ///
    /// Returns which roles are gated from new work, so the Fleet can enforce
    /// the brake and kill switch (issue #41) at the process level,
    /// idle-holding a paused role, rather than trusting each agent to honor
    /// the role-card contract. A role is gated when the whole crew is (a
    /// manual pause or stand-down, or the usage auto-pause) or when it is
    /// paused on its own.
    ///
    /// # Errors
    /// Returns an error if the broker cannot be reached or its response is
    /// malformed, so the caller can hold the last-known gates rather than act
    /// on a bad read.
    pub(crate) fn pause_snapshot(&self) -> Result<PauseSnapshot> {
        let url = format!("{}/roster", self.base);
        let text = self
            .agent
            .get(&url)
            .call()
            .map_err(|err| roster_error!("could not read the broker roster: {err}"))?
            .into_string()
            .map_err(|err| roster_error!("could not read the roster response: {err}"))?;
        let view: RosterView = serde_json::from_str(&text)
            .map_err(|err| roster_error!("could not parse the roster response: {err}"))?;
        Ok(PauseSnapshot::from_view(view))
    }

    /// Fetches `role`'s bounded briefing packet text (`GET /briefing?role=`,
    /// issue #50), for injecting into the agent's opening turn at spawn (issue
    /// #122).
    ///
    /// Returns just the rendered packet (the board plus a lane-scoped rolling
    /// summary, size-capped), which the caller folds into the boot prompt. The
    /// supervisor fetches this at spawn, not provision, so the packet reflects
    /// the current situation; a spawn treats an error as "no packet" and boots
    /// on the card briefing alone, keeping `crew_briefing` the re-read path.
    ///
    /// # Errors
    /// Returns an error if the broker cannot be reached or its response is
    /// malformed.
    pub fn briefing(&self, role: &RoleId) -> Result<String> {
        let url = format!("{}/briefing", self.base);
        let text = self
            .agent
            .get(&url)
            .query("role", role.as_str())
            .call()
            .map_err(|err| roster_error!("could not fetch the briefing for role `{role}`: {err}"))?
            .into_string()
            .map_err(|err| roster_error!("could not read the briefing response: {err}"))?;
        let packet: BriefingResponse = serde_json::from_str(&text)
            .map_err(|err| roster_error!("could not parse the briefing response: {err}"))?;
        Ok(packet.text)
    }

    /// Reads the events at or after `since`, oldest first, following the
    /// history pages, keeping only the given `kinds`.
    ///
    /// The coordination-stall monitor (issue #48) reads a recent window of the
    /// stream to look for a crew stuck waiting on itself. It passes the kinds
    /// it actually inspects (`message`, `ledger`, `verification`) so the
    /// broker filters server-side and a busy crew's high-volume `activity`
    /// events never ride the wire each scan (issue #125); an empty `kinds`
    /// fetches every kind. Events are returned as raw JSON so the
    /// supervisor reads the broker's stable stream contract rather than
    /// coupling to `crew_core::EventKind`, which lets an event kind it does
    /// not model pass through.
    ///
    /// # Errors
    /// Returns an error if the broker cannot be reached or a page is malformed.
    pub fn history_since(&self, since: Timestamp, kinds: &[&str]) -> Result<Vec<Value>> {
        let url = format!("{}/history", self.base);
        let since = since.to_string();
        // The broker accepts a comma-separated set of kinds (issue #125).
        let kind = kinds.join(",");
        let mut events = Vec::new();
        let mut after: Option<String> = None;
        loop {
            let mut request = self.agent.get(&url).query("since", &since);
            if !kind.is_empty() {
                request = request.query("kind", &kind);
            }
            if let Some(cursor) = &after {
                request = request.query("after", cursor);
            }
            let text = request
                .call()
                .map_err(|err| roster_error!("could not read the broker history: {err}"))?
                .into_string()
                .map_err(|err| roster_error!("could not read the history response: {err}"))?;
            let page: HistoryPage = serde_json::from_str(&text)
                .map_err(|err| roster_error!("could not parse the history response: {err}"))?;
            events.extend(page.events);
            match page.next_cursor {
                Some(cursor) => after = Some(cursor),
                None => break,
            }
        }
        Ok(events)
    }
}

/// One page of `GET /history`: the events, and the cursor to the next page if
/// any.
#[derive(Debug, Deserialize)]
struct HistoryPage {
    events: Vec<Value>,
    #[serde(default)]
    next_cursor: Option<String>,
}

/// The shape of `GET /briefing` (only the rendered packet text is read here).
#[derive(Debug, Deserialize)]
struct BriefingResponse {
    text: String,
}

/// The shape of `GET /roster`: the roles, the crew standing, and the
/// shared-subscription usage pause (the fields the supervisor reads; the count
/// and per-entry owned paths are ignored).
#[derive(Debug, Deserialize)]
struct RosterView {
    roles: Vec<RosterEntry>,
    /// The crew's control standing: `running`, `paused`, or `stood_down` (issue
    /// #41). Defaults to `running` if an older broker omits it, so a missing
    /// standing never spuriously gates the crew.
    #[serde(default = "running_standing")]
    standing: String,
    /// Whether new work is auto-paused on shared-subscription usage (issue
    /// #56).
    #[serde(default)]
    usage_paused: bool,
}

/// The default crew standing when the broker omits one: `running`, so a missing
/// field reads as not gated.
fn running_standing() -> String {
    "running".to_owned()
}

/// One roster entry: its role and whether it is paused on its own (issue #41);
/// extra fields (owned paths, liveness) are ignored.
#[derive(Debug, Deserialize)]
struct RosterEntry {
    role: RoleId,
    #[serde(default)]
    paused: bool,
}

/// A snapshot of the crew's pause state, for the Fleet to enforce at the
/// process level (issue #187).
///
/// A role is gated when the whole crew is (a manual pause or stand-down, or the
/// shared-subscription usage auto-pause), or when it is paused on its own.
#[derive(Debug, Clone, Default)]
pub(crate) struct PauseSnapshot {
    /// Whether the whole crew is gated: the standing is not `running`, or new
    /// work is usage auto-paused, so every role is held.
    crew_gated: bool,
    /// The roles paused on their own.
    paused_roles: HashSet<RoleId>,
}

impl PauseSnapshot {
    /// Derives the pause state from a parsed roster view: the crew is gated
    /// when its standing is not `running` or work is usage auto-paused, and
    /// each role paused on its own joins the set.
    fn from_view(view: RosterView) -> Self {
        let crew_gated = view.standing != "running" || view.usage_paused;
        let paused_roles = view
            .roles
            .into_iter()
            .filter(|entry| entry.paused)
            .map(|entry| entry.role)
            .collect();
        Self {
            crew_gated,
            paused_roles,
        }
    }

    /// Whether `role` is gated from new work: the crew is gated, or the role is
    /// paused on its own.
    pub(crate) fn is_gated(&self, role: &RoleId) -> bool {
        self.crew_gated || self.paused_roles.contains(role)
    }
}

#[cfg(test)]
mod tests {
    use crew_core::RoleId;

    use super::{PauseSnapshot, RosterClient, RosterView};

    /// Derives a pause snapshot from a `/roster`-shaped JSON body.
    fn snapshot(json: &str) -> PauseSnapshot {
        PauseSnapshot::from_view(
            serde_json::from_str::<RosterView>(json).expect("the roster parses"),
        )
    }

    #[test]
    fn a_dead_broker_registration_is_a_typed_roster_error() {
        // A register against a port with no broker fails as a canonical roster
        // error rather than a bare eyre report (issue #193).
        let client = RosterClient::new("http://127.0.0.1:1");
        let error = client
            .register(&RoleId::new("backend"), &["api/".to_owned()])
            .expect_err("no broker is listening on port 1");
        assert!(error.is_roster(), "a failed roster call is a roster error");
        assert!(!error.is_launch(), "it is not a launch error");
        assert!(
            error.to_string().contains("could not register"),
            "the message names the failed action: {error}",
        );
    }

    #[test]
    fn a_running_crew_gates_only_roles_paused_on_their_own() {
        let snap = snapshot(
            r#"{ "standing": "running", "usage_paused": false, "roles": [
                { "role": "backend", "paused": true },
                { "role": "frontend", "paused": false }
            ] }"#,
        );
        assert!(
            snap.is_gated(&RoleId::new("backend")),
            "a self-paused role is gated"
        );
        assert!(
            !snap.is_gated(&RoleId::new("frontend")),
            "an unpaused role is not gated by a running crew"
        );
        assert!(
            !snap.is_gated(&RoleId::new("qa")),
            "an absent role is not gated by a running crew"
        );
    }

    #[test]
    fn a_stood_down_crew_gates_every_role() {
        let snap = snapshot(r#"{ "standing": "stood_down", "roles": [ { "role": "backend" } ] }"#);
        assert!(
            snap.is_gated(&RoleId::new("backend")),
            "a stood-down crew gates a role"
        );
        assert!(
            snap.is_gated(&RoleId::new("anyone")),
            "and every role, even one absent from the roster"
        );
    }

    #[test]
    fn a_usage_auto_pause_gates_every_role() {
        let snap = snapshot(r#"{ "standing": "running", "usage_paused": true, "roles": [] }"#);
        assert!(
            snap.is_gated(&RoleId::new("backend")),
            "a shared-subscription usage auto-pause gates work crew-wide"
        );
    }

    #[test]
    fn a_missing_standing_reads_as_running() {
        // An older broker that omits `standing` must not spuriously gate the crew.
        let snap = snapshot(r#"{ "roles": [ { "role": "backend" } ] }"#);
        assert!(
            !snap.is_gated(&RoleId::new("backend")),
            "a missing standing is not a gate"
        );
    }
}
