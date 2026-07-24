//! End-to-end test of coordination-stall detection (issue #48) and its
//! surfacing on the stream (issue #120).
//!
//! It proves the acceptance against a real broker: a mutual-wait deadlock and a
//! stalled task (a done-gate submission with no verdict, the on-`main` shape of
//! "a ledger with no forward motion") are read off the live stream and detected
//! with a precise cause, while an answered exchange is not. Detection runs over
//! the same `history_since` fetch the fleet's stall monitor uses, so this
//! exercises the wire path, not just the pure logic. It also proves the
//! monitor's `report_stall` surfaces a detected or resolved stall as a
//! first-class `stall` event, filterable by `kind=stall`, and that the scan's
//! fetch filters to the kinds it inspects server-side, dropping noise (issue
//! #125).

mod common;

use std::time::Duration;

use common::start_broker;
use crew_core::{RoleId, StallEvent, StallKind, StallStatus, Timestamp};
use crew_supervisor::{detect_stalls, RosterClient};
use serde_json::{json, Value};

/// The `stall` events on the broker log, oldest first, read over the
/// `kind=stall` history filter.
fn stall_events(base: &str) -> Vec<Value> {
    let text = ureq::get(&format!("{base}/history?kind=stall"))
        .call()
        .unwrap()
        .into_string()
        .unwrap();
    let value: Value = serde_json::from_str(&text).unwrap();
    value["events"].as_array().cloned().unwrap_or_default()
}

/// The crew whose members can wait on each other.
fn roster() -> Vec<RoleId> {
    vec![RoleId::new("backend"), RoleId::new("frontend")]
}

/// Posts a message of `kind` from `role` to `channel` through the broker,
/// returning the id the broker minted for it.
///
/// An `answer` must reference an existing question (issue #211): pass that
/// question's id as `in_reply_to`. Other kinds pass `None`.
fn post_message(
    base: &str,
    role: &str,
    channel: &str,
    kind: &str,
    body: &str,
    in_reply_to: Option<&str>,
) -> String {
    let mut payload = json!({ "from": { "kind": "role", "id": role }, "kind": kind, "body": body });
    if let Some(id) = in_reply_to {
        payload["in_reply_to"] = json!(id);
    }
    let text = ureq::post(&format!("{base}/channels/{channel}/messages"))
        .set("content-type", "application/json")
        .send_string(&payload.to_string())
        .unwrap()
        .into_string()
        .unwrap();
    let event: Value = serde_json::from_str(&text).unwrap();
    event["kind"]["data"]["id"]
        .as_str()
        .expect("the broker mints a message id")
        .to_owned()
}

/// A wait of zero, so an event posted a moment ago already counts as stalled:
/// the test controls timing by what it posts, not by sleeping past a real
/// threshold.
const IMMEDIATE: Duration = Duration::from_secs(0);

#[test]
fn a_mutual_wait_deadlock_is_read_off_the_stream_and_named() {
    let base = start_broker();
    let since = Timestamp::now();
    let roster_client = RosterClient::new(base.clone());

    // Each agent asks the other a question and neither answers: a deadlock.
    post_message(
        &base,
        "backend",
        "@frontend",
        "question",
        "which auth lib?",
        None,
    );
    post_message(
        &base,
        "frontend",
        "@backend",
        "question",
        "what token TTL?",
        None,
    );

    let events = roster_client
        .history_since(since, &["message", "ledger", "verification"])
        .unwrap();
    let stalls = detect_stalls(&events, &roster(), Timestamp::now(), IMMEDIATE);

    assert_eq!(stalls.len(), 1, "one deadlock: {stalls:?}");
    assert_eq!(stalls[0].kind, StallKind::Deadlock);
    assert_eq!(stalls[0].roles, roster(), "both roles, sorted");
    assert!(
        stalls[0].detail.contains("backend waits on frontend")
            && stalls[0].detail.contains("frontend waits on backend"),
        "names who waits on whom: {}",
        stalls[0].detail,
    );
}

#[test]
fn an_answered_exchange_is_not_a_stall() {
    let base = start_broker();
    let since = Timestamp::now();
    let roster_client = RosterClient::new(base.clone());

    let question = post_message(
        &base,
        "backend",
        "@frontend",
        "question",
        "which auth lib?",
        None,
    );
    post_message(
        &base,
        "frontend",
        "@backend",
        "answer",
        "use the crew one",
        Some(&question),
    );

    let events = roster_client
        .history_since(since, &["message", "ledger", "verification"])
        .unwrap();
    let stalls = detect_stalls(&events, &roster(), Timestamp::now(), IMMEDIATE);

    assert!(stalls.is_empty(), "the question was answered: {stalls:?}");
}

#[test]
fn a_submitted_task_with_no_verdict_is_a_stalled_ledger() {
    let base = start_broker();
    let since = Timestamp::now();
    let roster_client = RosterClient::new(base.clone());

    // A role submits work to the done-gate; no one verifies it. The task sits with
    // no forward motion: the on-`main` shape of a stalled ledger (issue #47's
    // `verification` event; a work-ledger `ledger` event once issue #45 lands
    // is handled the same way). The gate is keyed by the task id now (issue #183),
    // titled for display.
    let task = "33333333-3333-3333-3333-333333333333";
    ureq::post(&format!("{base}/gate/submit"))
        .set("content-type", "application/json")
        .send_string(
            &json!({ "role": "backend", "task": task, "title": "login", "acceptance": "tokens expire" })
                .to_string(),
        )
        .unwrap();

    let events = roster_client
        .history_since(since, &["message", "ledger", "verification"])
        .unwrap();
    let stalls = detect_stalls(&events, &roster(), Timestamp::now(), IMMEDIATE);

    assert_eq!(stalls.len(), 1, "one stalled task: {stalls:?}");
    assert_eq!(stalls[0].kind, StallKind::LedgerStall);
    assert_eq!(stalls[0].roles, vec![RoleId::new("backend")]);
    assert!(
        stalls[0].detail.contains(task) && stalls[0].detail.contains("awaited verification"),
        "names the stalled task id and why: {}",
        stalls[0].detail,
    );
}

#[test]
fn a_detected_stall_is_surfaced_on_the_stream() {
    let base = start_broker();
    let roster_client = RosterClient::new(base.clone());

    // The monitor found a deadlock; publishing it makes it a first-class `stall`
    // event a watcher (crew notify, crew top) reads off the stream (issue #120).
    roster_client
        .report_stall(&StallEvent {
            kind: StallKind::Deadlock,
            status: StallStatus::Detected,
            roles: roster(),
            detail: "deadlock: backend waits on frontend, and frontend waits on backend".to_owned(),
        })
        .unwrap();

    let events = stall_events(&base);
    assert_eq!(events.len(), 1, "one stall event: {events:?}");
    let data = &events[0]["kind"]["data"];
    assert_eq!(data["kind"], "deadlock");
    assert_eq!(data["status"], "detected");
    assert_eq!(data["roles"], json!(["backend", "frontend"]));
    // A crew-level finding rides from the General to the whole unit.
    assert_eq!(events[0]["from"]["kind"], "general");
    assert_eq!(events[0]["channel"], "all-units");
}

#[test]
fn a_resolved_stall_is_surfaced_and_filterable() {
    let base = start_broker();
    let roster_client = RosterClient::new(base.clone());

    roster_client
        .report_stall(&StallEvent {
            kind: StallKind::LedgerStall,
            status: StallStatus::Resolved,
            roles: vec![RoleId::new("backend")],
            detail: "ledger task `login` moved forward".to_owned(),
        })
        .unwrap();

    let events = stall_events(&base);
    assert_eq!(events.len(), 1, "the resolved stall is on the stream");
    assert_eq!(events[0]["kind"]["data"]["status"], "resolved");
}

#[test]
fn the_stall_fetch_filters_noise_kinds_server_side() {
    // Issue #125: a scan fetches only the kinds the detector inspects, so a busy
    // crew's high-volume events it ignores never ride the wire each interval.
    let base = start_broker();
    let roster_client = RosterClient::new(base.clone());
    let since = Timestamp::now();

    // A role registering emits a `lifecycle` event, which the detector ignores.
    ureq::post(&format!("{base}/roster"))
        .set("content-type", "application/json")
        .send_string(&json!({ "role": "backend", "owned_paths": ["api/"] }).to_string())
        .unwrap();
    // A `question` is a kind the detector does inspect.
    post_message(
        &base,
        "frontend",
        "@backend",
        "question",
        "which auth lib?",
        None,
    );

    let filtered = roster_client
        .history_since(since, &["message", "ledger", "verification"])
        .unwrap();
    assert!(
        filtered
            .iter()
            .all(|event| event["kind"]["kind"] == "message"),
        "only the requested kinds ride the wire, not the lifecycle noise: {filtered:?}",
    );
    assert_eq!(filtered.len(), 1, "the question is still fetched");

    // Without a filter the same window includes the lifecycle event, so the
    // filter is what drops it, not an empty window.
    let unfiltered = roster_client.history_since(since, &[]).unwrap();
    assert!(
        unfiltered
            .iter()
            .any(|event| event["kind"]["kind"] == "lifecycle"),
        "the noise is in the window; the server-side filter is what drops it",
    );
}
