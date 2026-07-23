//! Per-agent activity, end to end against a real broker (issue #24).
//!
//! Proves the acceptance: a stub agent emits `--output-format stream-json` on
//! stdout (a session init, an assistant turn with text and a tool call, a
//! result line, plus an unknown shape), and the fleet's forwarder parses each
//! line into `activity` events on the broker, keyed by role. So a role's tool
//! calls and turns appear on the stream, and an unknown stream shape is kept as
//! `other` rather than crashing the parser.

mod common;

use common::{liveness, start_broker, stub, wait_until};
use crew_core::RoleId;
use crew_supervisor::{Fleet, LifecyclePolicy, RosterClient};
use serde_json::Value;

/// The parsed activity items on the broker log for `role`, oldest first: each
/// is the inner `Activity` JSON (`{"kind":"tool_call","tool":"Read"}`, ...).
fn activities(base: &str, role: &str) -> Vec<Value> {
    let text = ureq::get(&format!("{base}/history?kind=activity"))
        .call()
        .unwrap()
        .into_string()
        .unwrap();
    let value: Value = serde_json::from_str(&text).unwrap();
    value["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|event| event["from"]["id"] == role)
        .map(|event| event["kind"]["data"].clone())
        .collect()
}

/// Whether an activity of `kind` with `field == value` is present.
fn has(activities: &[Value], kind: &str, field: &str, value: &str) -> bool {
    activities
        .iter()
        .any(|item| item["kind"] == kind && item[field] == value)
}

#[test]
fn an_agents_stream_json_becomes_activity_events_on_the_stream() {
    let base = start_broker();
    let roster = RosterClient::new(base.clone());
    let backend = RoleId::new("backend");

    // A stub agent that emits one turn of stream-json (init, an assistant turn with
    // text and a tool call, a result), plus an unknown shape, then idles. It stands
    // in for a real `claude -p` turn, which needs no external services in CI.
    let script = concat!(
        r#"echo '{"type":"system","subtype":"init","session_id":"s1","model":"opus"}'; "#,
        r#"echo '{"type":"assistant","message":{"content":[{"type":"text","text":"on it"},{"type":"tool_use","name":"Read","input":{"path":"api.rs"}}]}}'; "#,
        r#"echo '{"type":"result","subtype":"success","result":"done","session_id":"s1"}'; "#,
        r#"echo '{"type":"telepathy_event","payload":42}'; "#,
        "sleep 30",
    );
    let fleet = Fleet::launch(
        &roster,
        vec![stub("backend", script)],
        LifecyclePolicy::default(),
    );
    fleet.start(&backend).unwrap();
    assert!(
        wait_until(|| liveness(&base, "backend").as_deref() == Some("working")),
        "the role comes online",
    );

    // The forwarder parses the stdout stream-json into activity events: the role's
    // tool call and its turn boundaries appear on the stream.
    assert!(
        wait_until(|| {
            let items = activities(&base, "backend");
            has(&items, "tool_call", "tool", "Read")
                && items.iter().any(|item| item["kind"] == "turn_started")
                && items.iter().any(|item| item["kind"] == "turn_ended")
        }),
        "the role's tool call and turns are on the stream: {:?}",
        activities(&base, "backend"),
    );

    // Assistant text is surfaced as output, and the unknown shape is kept as
    // `other` rather than dropped or crashing the parser.
    let items = activities(&base, "backend");
    assert!(
        has(&items, "output", "text", "on it"),
        "assistant text is recorded as output: {items:?}",
    );
    assert!(
        has(&items, "other", "raw", "telepathy_event"),
        "an unknown stream shape is kept as `other`: {items:?}",
    );

    fleet.shutdown();
}
