//! Usage telemetry, end to end against a real broker (issue #55).
//!
//! Proves the second half of the acceptance: per-role and aggregate
//! cost/tokens/time are queryable and shown live. (The first half, an idle role
//! stops on schedule, is the lifecycle machine of issue #22, covered by
//! `tests/lifecycle.rs`.) The token feed is driven directly here through
//! [`Fleet::record_usage`]; in production the activity parser (issue #24) feeds
//! it each turn's usage.

mod common;

use common::{liveness, start_broker, stub, wait_until};
use crew_core::RoleId;
use crew_supervisor::{Fleet, LifecyclePolicy, RosterClient};
use serde_json::Value;

/// The `GET /stats` rollup as JSON.
fn stats(base: &str) -> Value {
    let text = ureq::get(&format!("{base}/stats"))
        .call()
        .unwrap()
        .into_string()
        .unwrap();
    serde_json::from_str(&text).unwrap()
}

/// One role's row in the rollup.
fn role_row<'a>(stats: &'a Value, role: &str) -> &'a Value {
    stats["roles"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["role"] == role)
        .unwrap_or_else(|| panic!("role `{role}` is in the rollup: {stats}"))
}

/// The number of `telemetry` events on the broker log.
fn telemetry_count(base: &str) -> usize {
    let text = ureq::get(&format!("{base}/history?kind=telemetry"))
        .call()
        .unwrap()
        .into_string()
        .unwrap();
    let value: Value = serde_json::from_str(&text).unwrap();
    value["events"].as_array().unwrap().len()
}

#[test]
fn usage_rolls_up_per_role_and_in_aggregate_and_is_queryable() {
    let base = start_broker();
    let roster = RosterClient::new(base.clone());
    let backend = RoleId::new("backend");
    let frontend = RoleId::new("frontend");

    let fleet = Fleet::launch(
        &roster,
        vec![
            stub("backend", "echo ready; sleep 30"),
            stub("frontend", "echo ready; sleep 30"),
        ],
        LifecyclePolicy::default(),
    );

    fleet.start(&backend).unwrap();
    fleet.start(&frontend).unwrap();
    assert!(
        wait_until(|| liveness(&base, "backend").as_deref() == Some("working")
            && liveness(&base, "frontend").as_deref() == Some("working")),
        "both roles come online"
    );

    // No budget is set, yet usage is still recorded: telemetry is always-on (issue
    // #55).
    fleet.record_usage(&backend, 1_000, 30_000).unwrap();
    fleet.record_usage(&backend, 500, 15_000).unwrap();
    fleet.record_usage(&frontend, 200, 4_000).unwrap();

    // The rollup is queryable: per-role sums and the crew aggregate.
    assert!(
        wait_until(|| telemetry_count(&base) == 3),
        "every usage report reached the stream"
    );
    let stats = stats(&base);

    let backend_row = role_row(&stats, "backend");
    assert_eq!(backend_row["tokens"], 1_500, "backend's two turns sum");
    assert_eq!(backend_row["cost_micro_usd"], 45_000);
    // A working role's time is present and folds its live interval (issue #22 +
    // #55).
    assert!(
        backend_row["active_secs"].as_u64().is_some(),
        "working time is reported: {backend_row}"
    );

    assert_eq!(
        stats["aggregate"]["tokens"], 1_700,
        "the crew aggregate sums both roles"
    );
    assert_eq!(stats["aggregate"]["cost_micro_usd"], 49_000);
}
