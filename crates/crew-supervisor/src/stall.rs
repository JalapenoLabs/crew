//! Coordination-stall and deadlock detection: catch a crew stuck waiting on
//! itself.
//!
//! The defibrillator (issue #23, [`crate::lifecycle`]) recovers a single agent
//! whose turn died. A crew can also be fully alive yet stuck: every agent idle,
//! each waiting on another, so silence reads as progress when it is really a
//! deadlock. This module is the coordination-defibrillator (issue #48): it
//! reads the broker's event stream and finds the three shapes of a stall, then
//! escalates the specific cause to the operator rather than a generic timeout.
//!
//! - **Deadlock.** A cycle of unanswered questions: `backend` waits on
//!   `frontend` and `frontend` waits on `backend`, so neither can proceed.
//! - **An unanswered question.** One agent has waited past the threshold for
//!   another to answer, with no cycle: the blocker is simply not responding.
//! - **A stalled ledger.** A task sits in a held state (a work-ledger claim
//!   that is not `done`, or a done-gate submission with no verdict) past the
//!   threshold: no forward motion.
//!
//! A legitimate wait for the human (a question broadcast to `all-units`, or
//! addressed to anyone who is not a live agent) is **not** a deadlock: the crew
//! is waiting on input, not on itself, so it is never escalated as a stall (see
//! the scope of issue #48).
//!
//! Detection ([`detect_stalls`]) is a pure function over the parsed stream, so
//! every shape is unit-testable without a running broker; the `StallMonitor`
//! loop feeds it the recent history on a timer and escalates each new stall.

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        mpsc::{Receiver, RecvTimeoutError},
        Arc, Mutex, PoisonError,
    },
    time::Duration,
};

use chrono::{DateTime, Utc};
use crew_core::{Channel, RoleId, StallEvent, StallKind, StallStatus, Timestamp};
use serde_json::Value;
use tracing::{event, Level};

use crate::roster::RosterClient;

/// The event kinds [`detect_stalls`] inspects, so a scan fetches only these
/// from the broker instead of the whole window (issue #125).
///
/// A stall is read from the wait graph of `message` questions and answers, and
/// from held `ledger` claims and unverified `verification` submissions; every
/// other kind, notably a busy crew's high-volume `activity` events, is
/// irrelevant, so filtering server-side keeps each scan's fetch small.
const STALL_EVENT_KINDS: &[&str] = &["message", "ledger", "verification"];

/// A detected coordination stall: the crew stuck waiting on itself, not one
/// dead agent.
///
/// The detector escalates this to the operator with a precise
/// [`detail`](Self::detail) (who is waiting on what), rather than a generic
/// timeout, so the General can unstick the specific cause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Stall {
    /// Which shape of stall this is.
    pub kind: StallKind,
    /// The roles caught in the stall, sorted, for a stable identity across
    /// scans.
    pub roles: Vec<RoleId>,
    /// A one-line, specific description of the stall: who is waiting on what.
    pub detail: String,
}

impl Stall {
    /// A stable key identifying this stall, so a persistent one is escalated
    /// once.
    ///
    /// Keyed on the kind and the roles involved (not the free-text detail), so
    /// a stall that persists across scans keeps the same key and a
    /// resolved-then-recurring one is escalated afresh.
    #[must_use]
    pub fn key(&self) -> String {
        let roles: Vec<&str> = self.roles.iter().map(RoleId::as_str).collect();
        format!("{}:{}", self.kind.label(), roles.join(","))
    }
}

/// One agent waiting on another: an unanswered question, the edge of the wait
/// graph.
#[derive(Debug, Clone)]
struct WaitEdge {
    /// The agent that asked and is waiting.
    waiter: RoleId,
    /// The agent it is waiting on to answer.
    blocker: RoleId,
    /// The question's text, so the escalation names what is awaited.
    subject: String,
    /// When the (latest unanswered) question was asked, for the wait's age.
    since: Timestamp,
}

/// A parsed stream event, lenient enough to survive kinds this crate does not
/// model.
///
/// The supervisor reads the broker's public stream contract
/// (`docs/stream-contract.md`), not `crew_core::EventKind`, so an event kind
/// added later (a work-ledger `ledger` event before that crate ships it here)
/// parses rather than breaking the scan.
#[derive(Debug, Clone)]
struct StreamEvent {
    /// When the event happened.
    ts: Timestamp,
    /// Who emitted it: a role, or `None` for the General (the human).
    from: Option<RoleId>,
    /// The channel it was addressed to.
    channel: String,
    /// The tagged payload kind: `message`, `lifecycle`, `ledger`,
    /// `verification`, ...
    kind: String,
    /// For a `message`, its typed intent (`question`, `answer`, `note`, ...).
    message_kind: Option<String>,
    /// A message's body, or a ledger/verification task's detail; empty when
    /// absent.
    text: String,
    /// For a `ledger` or `verification` event, the task key it concerns.
    task: Option<String>,
    /// For a `ledger` or `verification` event, the role that owns the task.
    owner: Option<RoleId>,
    /// For a `ledger` event, its state (`claimed` / `in_progress` / `blocked` /
    /// `done`); for a `verification` event, its verdict (`submitted` /
    /// `passed` / `failed`).
    state: Option<String>,
}

impl StreamEvent {
    /// Parses one `/history` event object, or `None` if it is missing required
    /// fields.
    fn parse(value: &Value) -> Option<Self> {
        let ts: Timestamp = serde_json::from_value(value.get("ts")?.clone()).ok()?;
        let from = match value
            .get("from")
            .and_then(|from| from.get("kind"))
            .and_then(Value::as_str)
        {
            Some("role") => Some(RoleId::new(from_id(value)?)),
            _ => None, // "general", or malformed: treated as the human.
        };
        let channel = value.get("channel")?.as_str()?.to_owned();
        let kind_obj = value.get("kind")?;
        let kind = kind_obj.get("kind")?.as_str()?.to_owned();
        let data = kind_obj.get("data");

        let (message_kind, task, owner, state, text) = match kind.as_str() {
            "message" => (
                data.and_then(|d| d.get("kind"))
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                None,
                None,
                None,
                string_field(data, "body"),
            ),
            "ledger" | "verification" => (
                None,
                data.and_then(|d| d.get("task"))
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                data.and_then(|d| d.get("owner"))
                    .and_then(Value::as_str)
                    .map(RoleId::new),
                data.and_then(|d| d.get("state").or_else(|| d.get("verdict")))
                    .and_then(Value::as_str)
                    .map(str::to_owned),
                string_field(data, "detail"),
            ),
            _ => (None, None, None, None, String::new()),
        };

        Some(Self {
            ts,
            from,
            channel,
            kind,
            message_kind,
            text,
            task,
            owner,
            state,
        })
    }

    /// Whether this is a message of the given typed intent.
    fn is_message(&self, intent: &str) -> bool {
        self.kind == "message" && self.message_kind.as_deref() == Some(intent)
    }
}

/// The `from.id` of an event, if present.
fn from_id(value: &Value) -> Option<&str> {
    value
        .get("from")
        .and_then(|from| from.get("id"))
        .and_then(Value::as_str)
}

/// A string field of an event's `data`, or empty when absent.
fn string_field(data: Option<&Value>, field: &str) -> String {
    data.and_then(|d| d.get(field))
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_owned()
}

/// Detects every coordination stall in `events`, given the live agent `roster`.
///
/// `events` is the recent history, oldest first; `roster` is the set of agent
/// roles (so a wait on anyone else is a wait for the human, not a deadlock);
/// `now` and `timeout` set how long a wait must persist to count. Pure: the
/// `StallMonitor` supplies the inputs.
///
/// # Examples
/// ```
/// use std::time::Duration;
/// use crew_core::{RoleId, Timestamp};
/// use crew_supervisor::detect_stalls;
///
/// // Two agents each waiting on the other's answer: a deadlock.
/// let now = Timestamp::now();
/// let old = "2000-01-01T00:00:00Z";
/// let events = serde_json::json!([
///   { "ts": old, "from": { "kind": "role", "id": "backend" }, "channel": "@frontend",
///     "kind": { "kind": "message", "data": { "kind": "question", "body": "which lib?" } } },
///   { "ts": old, "from": { "kind": "role", "id": "frontend" }, "channel": "@backend",
///     "kind": { "kind": "message", "data": { "kind": "question", "body": "what TTL?" } } }
/// ]);
/// let roster = [RoleId::new("backend"), RoleId::new("frontend")];
/// let stalls = detect_stalls(events.as_array().unwrap(), &roster, now, Duration::from_secs(60));
/// assert_eq!(stalls.len(), 1);
/// ```
#[must_use]
pub fn detect_stalls(
    events: &[Value],
    roster: &[RoleId],
    now: Timestamp,
    timeout: Duration,
) -> Vec<Stall> {
    let parsed: Vec<StreamEvent> = events.iter().filter_map(StreamEvent::parse).collect();
    let cutoff = now.to_datetime() - chrono_timeout(timeout);

    let mut stalls = waiting_stalls(&parsed, roster, cutoff, now);
    stalls.extend(ledger_stalls(&parsed, cutoff, now));
    stalls
}

/// The wait-graph stalls: deadlock cycles and one-sided unanswered questions.
fn waiting_stalls(
    events: &[StreamEvent],
    roster: &[RoleId],
    cutoff: DateTime<Utc>,
    now: Timestamp,
) -> Vec<Stall> {
    let edges = wait_edges(events, roster, cutoff);
    let cyclic: Vec<bool> = edges
        .iter()
        .map(|edge| reaches(&edge.blocker, &edge.waiter, &edges))
        .collect();

    let mut stalls = Vec::new();

    // Deadlocks: the cyclic edges, grouped into connected components (one stall
    // each).
    let deadlock_edges: Vec<&WaitEdge> = edges
        .iter()
        .zip(&cyclic)
        .filter_map(|(edge, in_cycle)| in_cycle.then_some(edge))
        .collect();
    for component in components(&deadlock_edges) {
        let mut roles: BTreeSet<RoleId> = BTreeSet::new();
        let mut waits: Vec<String> = Vec::new();
        for edge in &component {
            roles.insert(edge.waiter.clone());
            roles.insert(edge.blocker.clone());
            waits.push(format!(
                "{} waits on {} ({})",
                edge.waiter,
                edge.blocker,
                describe_subject(&edge.subject),
            ));
        }
        stalls.push(Stall {
            kind: StallKind::Deadlock,
            roles: roles.into_iter().collect(),
            detail: format!(
                "deadlock: {}; neither can proceed until the General intervenes",
                waits.join(", and "),
            ),
        });
    }

    // Unanswered questions: the non-cyclic edges, each its own stall.
    for (edge, in_cycle) in edges.iter().zip(&cyclic) {
        if *in_cycle {
            continue;
        }
        stalls.push(Stall {
            kind: StallKind::UnansweredQuestion,
            roles: sorted_pair(&edge.waiter, &edge.blocker),
            detail: format!(
                "{} has waited {} for {} to answer ({})",
                edge.waiter,
                humanize(now, edge.since),
                edge.blocker,
                describe_subject(&edge.subject),
            ),
        });
    }

    stalls
}

/// Builds one wait edge per agent-to-agent question left unanswered past
/// `cutoff`.
///
/// A question is a wait only when its asker and its single addressee are both
/// live agents; a broadcast, or a question to the human, is a legitimate wait
/// for input and is skipped. The edge uses the latest unanswered question for
/// the pair, so a re-ask does not age a wait the blocker has since engaged
/// with.
fn wait_edges(events: &[StreamEvent], roster: &[RoleId], cutoff: DateTime<Utc>) -> Vec<WaitEdge> {
    let mut edges: Vec<WaitEdge> = Vec::new();
    for event in events {
        if !event.is_message("question") {
            continue;
        }
        let Some(waiter) = event.from.clone() else {
            continue; // from the General: the human is not waiting on the crew.
        };
        let Some(blocker) = addressee(&event.channel, &waiter) else {
            continue; // a broadcast or an unaddressed question: a wait for
                      // input.
        };
        if waiter == blocker || !is_agent(roster, &waiter) || !is_agent(roster, &blocker) {
            continue; // a wait on the human or on a role no longer live: not a
                      // deadlock.
        }
        if answered_after(events, &blocker, &waiter, event.ts) {
            continue; // the blocker engaged since the question: not waiting.
        }
        if event.ts.to_datetime() > cutoff {
            continue; // too recent: give the blocker time to answer.
        }
        upsert_edge(&mut edges, waiter, blocker, event);
    }
    edges
}

/// Records the wait edge, keeping only the latest unanswered question for a
/// pair.
fn upsert_edge(edges: &mut Vec<WaitEdge>, waiter: RoleId, blocker: RoleId, question: &StreamEvent) {
    if let Some(existing) = edges
        .iter_mut()
        .find(|edge| edge.waiter == waiter && edge.blocker == blocker)
    {
        if question.ts > existing.since {
            existing.since = question.ts;
            existing.subject.clone_from(&question.text);
        }
        return;
    }
    edges.push(WaitEdge {
        waiter,
        blocker,
        subject: question.text.clone(),
        since: question.ts,
    });
}

/// Whether `blocker` sent `waiter` a non-question message after `since` (so it
/// engaged).
///
/// A counter-question does not count: two agents asking each other and never
/// answering is the deadlock, so only a substantive reply (an answer, status,
/// note, ...) clears the wait.
fn answered_after(
    events: &[StreamEvent],
    blocker: &RoleId,
    waiter: &RoleId,
    since: Timestamp,
) -> bool {
    events.iter().any(|event| {
        event.kind == "message"
            && event.message_kind.as_deref() != Some("question")
            && event.from.as_ref() == Some(blocker)
            && event.ts > since
            && Channel::parse(&event.channel).is_some_and(|channel| channel.addresses(waiter))
    })
}

/// The single agent a question is addressed to, if any: a direct target or a
/// pair peer.
///
/// A broadcast (`all-units`) has no single addressee and returns `None`, so it
/// is treated as a wait for input rather than a directed wait on one agent.
fn addressee(channel: &str, asker: &RoleId) -> Option<RoleId> {
    match Channel::parse(channel)? {
        Channel::Direct(role) => Some(role),
        // The peer is the other member; if the asker is in neither slot, the canonical
        // first member stands in as the addressee.
        Channel::Pair(first, second) => Some(if &first == asker { second } else { first }),
        Channel::AllUnits => None,
    }
}

/// The stalled-ledger stalls: a held task with no forward motion past `cutoff`.
///
/// Covers a work-ledger claim (`kind: "ledger"`) left in `claimed` /
/// `in_progress` / `blocked`, and a done-gate submission (`kind:
/// "verification"`) left `submitted` with no verdict. A task's latest event
/// decides its state; `done` / `passed` / `failed` free it, so a finished task
/// never reads as stalled.
fn ledger_stalls(events: &[StreamEvent], cutoff: DateTime<Utc>, now: Timestamp) -> Vec<Stall> {
    let mut tasks: Vec<&StreamEvent> = Vec::new();
    for event in events {
        if (event.kind == "ledger" || event.kind == "verification") && event.task.is_some() {
            // Keep the latest event per task key; the stream is oldest first.
            match tasks.iter_mut().find(|held| held.task == event.task) {
                Some(held) => *held = event,
                None => tasks.push(event),
            }
        }
    }

    tasks
        .into_iter()
        .filter(|event| is_held(event) && event.ts.to_datetime() <= cutoff)
        .map(|event| Stall {
            kind: StallKind::LedgerStall,
            roles: event.owner.clone().into_iter().collect(),
            detail: ledger_detail(event, now),
        })
        .collect()
}

/// Whether a ledger or verification task is still held (not finished).
fn is_held(event: &StreamEvent) -> bool {
    let state = event.state.as_deref().unwrap_or_default();
    match event.kind.as_str() {
        "ledger" => matches!(state, "claimed" | "in_progress" | "blocked"),
        "verification" => state == "submitted",
        _ => false,
    }
}

/// The specific description of a stalled task, naming the owner, state, and
/// age.
fn ledger_detail(event: &StreamEvent, now: Timestamp) -> String {
    let task = event.task.as_deref().unwrap_or("(unknown)");
    let age = humanize(now, event.ts);
    let owner = event
        .owner
        .as_ref()
        .map_or_else(|| "no owner".to_owned(), |role| format!("owner {role}"));
    match event.kind.as_str() {
        "verification" => format!(
            "task `{task}` has awaited verification for {age} ({owner}); it was submitted but no \
             role has verified it"
        ),
        _ => format!(
            "ledger task `{task}` has sat in `{}` for {age} ({owner}) with no update",
            event.state.as_deref().unwrap_or("held"),
        ),
    }
}

/// Whether `target` is reachable from `start` by following wait edges (a cycle
/// test).
fn reaches(start: &RoleId, target: &RoleId, edges: &[WaitEdge]) -> bool {
    let mut frontier = vec![start.clone()];
    let mut seen: BTreeSet<RoleId> = BTreeSet::new();
    while let Some(node) = frontier.pop() {
        if &node == target {
            return true;
        }
        if !seen.insert(node.clone()) {
            continue;
        }
        for edge in edges.iter().filter(|edge| edge.waiter == node) {
            frontier.push(edge.blocker.clone());
        }
    }
    false
}

/// Groups edges that share any role into connected components (one deadlock
/// each).
fn components<'a>(edges: &[&'a WaitEdge]) -> Vec<Vec<&'a WaitEdge>> {
    let mut groups: Vec<Vec<&WaitEdge>> = Vec::new();
    for edge in edges {
        let touching: Vec<usize> = groups
            .iter()
            .enumerate()
            .filter(|(_, group)| group.iter().any(|member| shares_role(member, edge)))
            .map(|(index, _)| index)
            .collect();
        match touching.split_first() {
            None => groups.push(vec![edge]),
            Some((&first, rest)) => {
                // Merge every group this edge touches into the first, then add the edge.
                for &index in rest.iter().rev() {
                    let merged = groups.remove(index);
                    groups[first].extend(merged);
                }
                groups[first].push(edge);
            }
        }
    }
    groups
}

/// Whether two wait edges share at least one role.
fn shares_role(left: &WaitEdge, right: &WaitEdge) -> bool {
    let members = [&right.waiter, &right.blocker];
    members.contains(&&left.waiter) || members.contains(&&left.blocker)
}

/// Whether `role` is one of the live agents.
fn is_agent(roster: &[RoleId], role: &RoleId) -> bool {
    roster.contains(role)
}

/// A sorted two-role vector, so a stall's identity does not depend on edge
/// direction.
fn sorted_pair(a: &RoleId, b: &RoleId) -> Vec<RoleId> {
    let mut pair = vec![a.clone(), b.clone()];
    pair.sort();
    pair
}

/// A short, quoted rendering of a question's subject, defaulting when it is
/// empty.
fn describe_subject(subject: &str) -> String {
    let trimmed = subject.trim();
    if trimmed.is_empty() {
        "a question".to_owned()
    } else {
        let short: String = trimmed.chars().take(80).collect();
        format!("question: \"{short}\"")
    }
}

/// A rough human duration since `ts`, in minutes (or hours), for the escalation
/// text.
fn humanize(now: Timestamp, ts: Timestamp) -> String {
    let minutes = now
        .to_datetime()
        .signed_duration_since(ts.to_datetime())
        .num_minutes();
    let minutes = minutes.max(0);
    if minutes >= 120 {
        format!("{}h", minutes / 60)
    } else {
        format!("{minutes}m")
    }
}

/// Converts a stall timeout into a `chrono` duration, saturating an absurd
/// value.
fn chrono_timeout(timeout: Duration) -> chrono::Duration {
    chrono::Duration::from_std(timeout).unwrap_or_else(|_| chrono::Duration::days(365))
}

/// The coordination-stall monitor: the fleet-wide half of the defibrillator
/// (issue #48).
///
/// A background thread scans the broker's recent history on a timer and
/// escalates each new stall it finds, so a crew stuck waiting on itself is
/// caught the way a dead agent is. It records the stalls (read with
/// [`Fleet::stalls`](crate::Fleet::stalls)) and logs a specific warning the
/// operator sees, and re-escalates a stall only after it resolves.
pub(crate) struct StallMonitor {
    /// A broker client, for reading the recent event history.
    roster: RosterClient,
    /// The agent roles, so a wait on anyone else reads as a wait for the human.
    roles: Vec<RoleId>,
    /// How long a wait must persist to count as a stall.
    timeout: Duration,
    /// How often to scan the stream.
    scan_interval: Duration,
    /// The shared record of detected stalls, surfaced to the operator.
    stalls: Arc<Mutex<Vec<Stall>>>,
}

impl StallMonitor {
    /// Builds a monitor over `roster`, watching `roles` against the policy's
    /// thresholds.
    pub(crate) fn new(
        roster: RosterClient,
        roles: Vec<RoleId>,
        timeout: Duration,
        scan_interval: Duration,
        stalls: Arc<Mutex<Vec<Stall>>>,
    ) -> Self {
        Self {
            roster,
            roles,
            timeout,
            scan_interval,
            stalls,
        }
    }

    /// Runs the scan loop until `stop` fires (or the fleet drops, disconnecting
    /// it).
    pub(crate) fn run(self, stop: &Receiver<()>) {
        // The stalls reported on the previous scan, keyed for a stable identity:
        // a persistent one is escalated once, and one that clears is surfaced as
        // resolved and forgotten, so a recurrence is escalated afresh.
        let mut reported: BTreeMap<String, Stall> = BTreeMap::new();
        while let Err(RecvTimeoutError::Timeout) = stop.recv_timeout(self.scan_interval) {
            self.scan(&mut reported);
        }
    }

    /// One scan: read the recent history, detect stalls, and surface the ones
    /// that newly appeared or have since resolved.
    fn scan(&self, reported: &mut BTreeMap<String, Stall>) {
        let events = match self
            .roster
            .history_since(self.lookback_start(), STALL_EVENT_KINDS)
        {
            Ok(events) => events,
            // A transient broker read failure is not fatal: skip this scan and retry.
            Err(err) => {
                event!(
                    name: "supervisor.stall.scan.skipped",
                    Level::DEBUG,
                    error = %err,
                    "could not read the broker history to scan for stalls; retrying next tick",
                );
                return;
            }
        };
        let now = Timestamp::now();
        let stalls = detect_stalls(&events, &self.roles, now, self.timeout);
        let current: BTreeSet<String> = stalls.iter().map(Stall::key).collect();

        // Newly detected stalls: escalate to the operator and surface on the stream.
        for stall in &stalls {
            if !reported.contains_key(&stall.key()) {
                escalate(stall);
                self.publish(stall, StallStatus::Detected);
            }
        }
        // Stalls that were reported before but are gone now: surface as resolved,
        // so a watcher can clear them.
        for (key, stall) in reported.iter() {
            if !current.contains(key) {
                self.publish(stall, StallStatus::Resolved);
            }
        }

        // Track exactly the current stalls, so next scan escalates only what is new.
        *reported = stalls
            .iter()
            .map(|stall| (stall.key(), stall.clone()))
            .collect();
        *self.stalls.lock().unwrap_or_else(PoisonError::into_inner) = stalls;
    }

    /// Surfaces a stall on the stream as a `stall` event (`POST /stall`, issue
    /// #120), so `crew notify` and the `crew top` cockpit see it live.
    ///
    /// A broker write failure is not fatal: the stall is already escalated to
    /// the operator through the log and `Fleet::stalls`, so a missed stream
    /// event degrades quietly rather than taking the monitor down.
    fn publish(&self, stall: &Stall, status: StallStatus) {
        let event = StallEvent {
            kind: stall.kind,
            status,
            roles: stall.roles.clone(),
            detail: stall.detail.clone(),
        };
        if let Err(err) = self.roster.report_stall(&event) {
            event!(
                name: "supervisor.stall.publish.failed",
                Level::DEBUG,
                error = %err,
                "could not surface the stall on the stream; it is still logged and recorded",
            );
        }
    }

    /// The start of the history window to scan: far enough back to see a wait
    /// that first crossed the threshold, bounded so a scan reads a bounded
    /// slice of the log.
    fn lookback_start(&self) -> Timestamp {
        let lookback = self.timeout.checked_mul(3).unwrap_or(self.timeout);
        (Timestamp::now().to_datetime() - chrono_timeout(lookback)).into()
    }
}

/// Escalates one newly detected stall to the operator: a specific warning the
/// General sees in the `crew up` foreground, naming the exact cause rather than
/// a generic timeout.
fn escalate(stall: &Stall) {
    event!(
        name: "supervisor.stall.detected",
        Level::WARN,
        crew.stall = stall.kind.label(),
        crew.roles = %stall.roles.iter().map(RoleId::as_str).collect::<Vec<_>>().join(","),
        "coordination stall: {}",
        stall.detail,
    );
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crew_core::{RoleId, Timestamp};
    use serde_json::{json, Value};

    use super::{detect_stalls, StallKind};

    /// A fixed "now", with helpers to build events at a chosen age.
    fn now() -> Timestamp {
        Timestamp::now()
    }

    /// An RFC 3339 instant `minutes` before now, for building aged events.
    fn ago(minutes: i64) -> String {
        (Timestamp::now().to_datetime() - chrono::Duration::minutes(minutes)).to_rfc3339()
    }

    fn question(ts: &str, from: &str, channel: &str, body: &str) -> Value {
        json!({
            "ts": ts,
            "from": { "kind": "role", "id": from },
            "channel": channel,
            "kind": { "kind": "message", "data": { "kind": "question", "body": body } },
        })
    }

    fn answer(ts: &str, from: &str, channel: &str) -> Value {
        json!({
            "ts": ts,
            "from": { "kind": "role", "id": from },
            "channel": channel,
            "kind": { "kind": "message", "data": { "kind": "answer",
                "in_reply_to": "11111111-1111-1111-1111-111111111111", "body": "here you go" } },
        })
    }

    fn roster() -> Vec<RoleId> {
        vec![RoleId::new("backend"), RoleId::new("frontend")]
    }

    const TIMEOUT: Duration = Duration::from_secs(10 * 60);

    #[test]
    fn a_mutual_wait_is_a_deadlock_with_a_precise_cause() {
        let events = vec![
            question(&ago(30), "backend", "@frontend", "which auth lib?"),
            question(&ago(25), "frontend", "@backend", "what token TTL?"),
        ];
        let stalls = detect_stalls(&events, &roster(), now(), TIMEOUT);
        assert_eq!(stalls.len(), 1, "one deadlock, not two questions");
        let stall = &stalls[0];
        assert_eq!(stall.kind, StallKind::Deadlock);
        assert_eq!(stall.roles, roster(), "names both roles, sorted");
        assert!(
            stall.detail.contains("backend waits on frontend"),
            "{}",
            stall.detail
        );
        assert!(
            stall.detail.contains("frontend waits on backend"),
            "{}",
            stall.detail
        );
        assert!(
            stall.detail.contains("which auth lib?"),
            "names the subject"
        );
    }

    #[test]
    fn an_answered_question_is_not_a_stall() {
        let events = vec![
            question(&ago(30), "backend", "@frontend", "which auth lib?"),
            answer(&ago(20), "frontend", "@backend"),
            question(&ago(25), "frontend", "@backend", "what token TTL?"),
            answer(&ago(15), "backend", "@frontend"),
        ];
        let stalls = detect_stalls(&events, &roster(), now(), TIMEOUT);
        assert!(stalls.is_empty(), "both answered: no stall, got {stalls:?}");
    }

    #[test]
    fn a_one_sided_unanswered_question_is_its_own_stall() {
        let events = vec![question(
            &ago(30),
            "backend",
            "@frontend",
            "which auth lib?",
        )];
        let stalls = detect_stalls(&events, &roster(), now(), TIMEOUT);
        assert_eq!(stalls.len(), 1);
        assert_eq!(stalls[0].kind, StallKind::UnansweredQuestion);
        assert!(
            stalls[0].detail.contains("backend has waited"),
            "{}",
            stalls[0].detail
        );
        assert!(stalls[0].detail.contains("frontend to answer"));
    }

    #[test]
    fn a_recent_question_is_given_time_before_it_stalls() {
        let events = vec![question(&ago(2), "backend", "@frontend", "which auth lib?")];
        let stalls = detect_stalls(&events, &roster(), now(), TIMEOUT);
        assert!(stalls.is_empty(), "under the threshold: not yet a stall");
    }

    #[test]
    fn a_question_to_the_whole_unit_is_a_wait_for_input_not_a_deadlock() {
        // A broadcast question is awaiting the human, so it is never a deadlock, even
        // if another broadcast question exists (which would otherwise look mutual).
        let events = vec![
            question(&ago(30), "backend", "all-units", "should we ship?"),
            question(&ago(30), "frontend", "all-units", "which design?"),
        ];
        let stalls = detect_stalls(&events, &roster(), now(), TIMEOUT);
        assert!(
            stalls.is_empty(),
            "waiting for input, not a stall: {stalls:?}"
        );
    }

    #[test]
    fn a_counter_question_does_not_clear_the_wait() {
        // frontend answers backend's question with a question of its own: still stuck.
        let events = vec![
            question(&ago(30), "backend", "@frontend", "which auth lib?"),
            question(&ago(20), "frontend", "@backend", "what token TTL?"),
        ];
        let stalls = detect_stalls(&events, &roster(), now(), TIMEOUT);
        assert_eq!(stalls.len(), 1);
        assert_eq!(stalls[0].kind, StallKind::Deadlock);
    }

    #[test]
    fn a_stalled_work_ledger_claim_is_detected() {
        let events = vec![json!({
            "ts": ago(40),
            "from": { "kind": "role", "id": "backend" },
            "channel": "all-units",
            "kind": { "kind": "ledger", "data": {
                "task": "login", "owner": "backend", "state": "in_progress", "title": "login flow"
            } },
        })];
        let stalls = detect_stalls(&events, &roster(), now(), TIMEOUT);
        assert_eq!(stalls.len(), 1);
        assert_eq!(stalls[0].kind, StallKind::LedgerStall);
        assert_eq!(stalls[0].roles, vec![RoleId::new("backend")]);
        assert!(stalls[0].detail.contains("login"), "{}", stalls[0].detail);
        assert!(
            stalls[0].detail.contains("in_progress"),
            "{}",
            stalls[0].detail
        );
    }

    #[test]
    fn a_done_ledger_task_is_not_stalled() {
        let events = vec![
            json!({
                "ts": ago(40), "from": { "kind": "role", "id": "backend" }, "channel": "all-units",
                "kind": { "kind": "ledger", "data": { "task": "login", "owner": "backend", "state": "claimed" } },
            }),
            json!({
                "ts": ago(35), "from": { "kind": "role", "id": "backend" }, "channel": "all-units",
                "kind": { "kind": "ledger", "data": { "task": "login", "owner": "backend", "state": "done" } },
            }),
        ];
        let stalls = detect_stalls(&events, &roster(), now(), TIMEOUT);
        assert!(
            stalls.is_empty(),
            "the latest state is done: not stalled, got {stalls:?}"
        );
    }

    #[test]
    fn a_submission_with_no_verdict_reads_as_a_stalled_task() {
        let events = vec![json!({
            "ts": ago(40),
            "from": { "kind": "role", "id": "backend" },
            "channel": "all-units",
            "kind": { "kind": "verification", "data": {
                "task": "login", "owner": "backend", "verdict": "submitted", "detail": "tokens expire"
            } },
        })];
        let stalls = detect_stalls(&events, &roster(), now(), TIMEOUT);
        assert_eq!(stalls.len(), 1);
        assert_eq!(stalls[0].kind, StallKind::LedgerStall);
        assert!(
            stalls[0].detail.contains("awaited verification"),
            "{}",
            stalls[0].detail
        );
    }

    #[test]
    fn a_role_that_left_the_crew_is_not_treated_as_a_deadlock() {
        // backend asks a role no longer on the roster: a wait for someone absent, which
        // is escalated to the operator, not a self-deadlock among live agents.
        let events = vec![
            question(&ago(30), "backend", "@ghost", "are you there?"),
            question(&ago(30), "frontend", "@ghost", "hello?"),
        ];
        let stalls = detect_stalls(&events, &roster(), now(), TIMEOUT);
        assert!(
            stalls.is_empty(),
            "waits on a non-agent are not deadlocks: {stalls:?}"
        );
    }
}
