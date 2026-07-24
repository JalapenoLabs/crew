//! End-to-end test of spawning a crew and wiring it to the broker (issue #21).
//!
//! It starts a real `crewd` in-process on an ephemeral loopback port, then
//! launches a [`Fleet`] of stub agent processes (a shell that prints a line and
//! idles, standing in for a real `claude` turn, which needs no external
//! services or an API key in CI). The assertions prove the acceptance on the
//! single lifecycle engine (issue #163): given N roles, N agents start,
//! register with the broker, can message each other, and deregister on exit.
//! Output capture feeding the activity parser (issue #24) is covered end to end
//! by `tests/activity.rs`.

mod common;

use std::time::Duration;

use common::{liveness, start_broker, stub, wait_until};
use crew_supervisor::{Fleet, LifecyclePolicy, RosterClient};

/// Posts a note as `from` to `channel` through the broker.
fn post_note(base: &str, channel: &str, from: &str, body: &str) {
    let payload = serde_json::json!({
        "from": { "kind": "role", "id": from },
        "kind": "note",
        "body": body,
    });
    ureq::post(&format!("{base}/channels/{channel}/messages"))
        .set("content-type", "application/json")
        .send_string(&payload.to_string())
        .expect("the broker accepts the note");
}

/// The message bodies the broker has stored.
fn history_bodies(base: &str) -> Vec<String> {
    let text = ureq::get(&format!("{base}/history?kind=message"))
        .call()
        .unwrap()
        .into_string()
        .unwrap();
    let value: serde_json::Value = serde_json::from_str(&text).unwrap();
    value["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|event| event["kind"]["data"]["body"].as_str().map(str::to_owned))
        .collect()
}

#[test]
fn a_crew_starts_registers_messages_and_deregisters() {
    let base = start_broker();
    let roster = RosterClient::new(base.clone());

    // A config of N roles becomes N prepared agents, each a stub that idles.
    let roles = ["commander", "backend", "frontend"];
    let agents = roles
        .iter()
        .map(|role| stub(role, "echo ready; sleep 30"))
        .collect();

    // Keep idle-stop far out so the roles stay working through the assertions.
    let policy = LifecyclePolicy {
        idle_timeout: Duration::from_secs(3600),
        ..LifecyclePolicy::default()
    };
    let fleet = Fleet::launch(&roster, agents, policy);
    fleet.start_all().expect("every role starts");

    // N agents started and registered with the broker on start.
    for role in roles {
        assert!(
            wait_until(|| liveness(&base, role).as_deref() == Some("working")),
            "role `{role}` registers on start, working",
        );
    }

    // The agents can message each other: a note to @backend is routed and stored,
    // so any teammate's inbox would receive it.
    post_note(&base, "@backend", "frontend", "the API is ready");
    assert!(
        history_bodies(&base)
            .iter()
            .any(|body| body == "the API is ready"),
        "the message reaches the broker log",
    );

    fleet.shutdown();

    // Every role deregistered on exit, so the roster reflects the live unit.
    for role in roles {
        assert!(
            wait_until(|| liveness(&base, role).is_none()),
            "role `{role}` is gone from the roster after shutdown",
        );
    }
}
