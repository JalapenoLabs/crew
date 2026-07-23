//! End-to-end test of coordination-stall detection (issue #48).
//!
//! It proves the acceptance against a real broker: a mutual-wait deadlock and a
//! stalled task (a done-gate submission with no verdict, the on-`main` shape of
//! "a ledger with no forward motion") are read off the live stream and detected
//! with a precise cause, while an answered exchange is not. Detection runs over
//! the same `history_since` fetch the fleet's stall monitor uses, so this
//! exercises the wire path, not just the pure logic.

mod common;

use std::time::Duration;

use common::start_broker;
use crew_core::{MessageId, RoleId, Timestamp};
use crew_supervisor::{detect_stalls, RosterClient, StallKind};
use serde_json::json;

/// The crew whose members can wait on each other.
fn roster() -> Vec<RoleId> {
    vec![RoleId::new("backend"), RoleId::new("frontend")]
}

/// Posts a message of `kind` from `role` to `channel` through the broker.
///
/// An `answer` carries a required `in_reply_to` reference (the question it
/// answers); the stall detector keys on the sender and the kind, not the
/// referenced id, so a fresh id stands in here.
fn post_message(base: &str, role: &str, channel: &str, kind: &str, body: &str) {
    let mut payload = json!({ "from": { "kind": "role", "id": role }, "kind": kind, "body": body });
    if kind == "answer" {
        payload["in_reply_to"] = json!(MessageId::new());
    }
    ureq::post(&format!("{base}/channels/{channel}/messages"))
        .set("content-type", "application/json")
        .send_string(&payload.to_string())
        .unwrap();
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
    post_message(&base, "backend", "@frontend", "question", "which auth lib?");
    post_message(&base, "frontend", "@backend", "question", "what token TTL?");

    let events = roster_client.history_since(since).unwrap();
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

    post_message(&base, "backend", "@frontend", "question", "which auth lib?");
    post_message(&base, "frontend", "@backend", "answer", "use the crew one");

    let events = roster_client.history_since(since).unwrap();
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
    // is handled the same way).
    ureq::post(&format!("{base}/gate/submit"))
        .set("content-type", "application/json")
        .send_string(
            &json!({ "role": "backend", "task": "login", "acceptance": "tokens expire" })
                .to_string(),
        )
        .unwrap();

    let events = roster_client.history_since(since).unwrap();
    let stalls = detect_stalls(&events, &roster(), Timestamp::now(), IMMEDIATE);

    assert_eq!(stalls.len(), 1, "one stalled task: {stalls:?}");
    assert_eq!(stalls[0].kind, StallKind::LedgerStall);
    assert_eq!(stalls[0].roles, vec![RoleId::new("backend")]);
    assert!(
        stalls[0].detail.contains("login") && stalls[0].detail.contains("awaited verification"),
        "names the stalled task and why: {}",
        stalls[0].detail,
    );
}
