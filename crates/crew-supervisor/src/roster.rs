//! The supervisor's roster client: register a role on spawn, deregister on exit.
//!
//! The supervisor owns each agent's process lifecycle, so it is the authority on
//! liveness: it registers a role with the broker the moment the process starts and
//! deregisters it when the process exits (issue #21). This is a thin synchronous
//! client over the broker's `/roster` HTTP API, distinct from the agent-facing
//! [`crew_mcp`](crew_mcp) client, which registers only its own role.

use std::time::Duration;

use crew_core::RoleId;
use eyre::{eyre, Result};
use serde::Deserialize;
use serde_json::json;

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
        }
    }

    /// Registers `role` with the lane it owns, marking it working (`POST /roster`).
    ///
    /// # Errors
    /// Returns an error if the broker rejects the registration or cannot be reached.
    pub fn register(&self, role: &RoleId, owned_paths: &[String]) -> Result<()> {
        let url = format!("{}/roster", self.base);
        let body = json!({ "role": role.as_str(), "owned_paths": owned_paths });
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
        let body = json!({ "role": role.as_str(), "liveness": liveness.wire() });
        self.agent
            .post(&url)
            .set("content-type", "application/json")
            .send_string(&body.to_string())
            .map(|_response| ())
            .map_err(|err| eyre!("could not mark role `{role}` as {}: {err}", liveness.wire()))
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
