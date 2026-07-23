//! Budget enforcement, end to end against a real broker (issue #54).
//!
//! Proves the acceptance: a crew respects its budget, and hitting a cap is visible on the
//! stream and bounded (the role or the crew idle-stops), never a silent overrun. The token
//! feed is driven directly here through [`Fleet::record_spend`]; in production the activity
//! parser (issue #24) feeds it each turn's usage.

mod common;

use std::collections::BTreeMap;

use common::{liveness, start_broker, stub, wait_until};
use crew_core::{Budget, RoleId};
use crew_supervisor::{Fleet, LifecyclePolicy, RosterClient};

/// The budget events on the broker log, each as `(role, breach)` where `breach` is the
/// scope a spend hit (`role` / `crew`), or `None` for a within-budget report.
fn budget_events(base: &str) -> Vec<(String, Option<String>)> {
    let text = ureq::get(&format!("{base}/history?kind=budget"))
        .call()
        .unwrap()
        .into_string()
        .unwrap();
    let value: serde_json::Value = serde_json::from_str(&text).unwrap();
    value["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|event| {
            let data = &event["kind"]["data"];
            let role = data["role"].as_str().unwrap_or_default().to_owned();
            let breach = data["breach"].as_str().map(str::to_owned);
            (role, breach)
        })
        .collect()
}

#[test]
fn a_role_cap_idle_stops_only_that_role_and_is_surfaced() {
    let base = start_broker();
    let roster = RosterClient::new(base.clone());
    let backend = RoleId::new("backend");
    let frontend = RoleId::new("frontend");

    // Backend is capped at 1000 tokens; frontend is uncapped.
    let budget = Budget::new(None, BTreeMap::from([(backend.clone(), 1_000)]));
    let fleet = Fleet::launch(
        &roster,
        vec![
            stub("backend", "echo ready; sleep 30"),
            stub("frontend", "echo ready; sleep 30"),
        ],
        LifecyclePolicy::default(),
    )
    .with_budget(budget);

    fleet.start(&backend).unwrap();
    fleet.start(&frontend).unwrap();
    assert!(
        wait_until(|| liveness(&base, "backend").as_deref() == Some("working")
            && liveness(&base, "frontend").as_deref() == Some("working")),
        "both roles come online"
    );

    // A spend under the cap does not stop the role, but is still reported.
    fleet.record_spend(&backend, 900).unwrap();
    assert_eq!(liveness(&base, "backend").as_deref(), Some("working"));

    // Reaching the cap idle-stops backend, and only backend.
    fleet.record_spend(&backend, 100).unwrap();
    assert!(
        wait_until(|| liveness(&base, "backend").as_deref() == Some("stopped")),
        "backend is bounded, not overrun"
    );
    assert_eq!(
        liveness(&base, "frontend").as_deref(),
        Some("working"),
        "an uncapped role keeps working"
    );

    // The cap hit is visible on the stream, never silent.
    let events = budget_events(&base);
    assert!(
        events
            .iter()
            .any(|(role, breach)| role == "backend" && breach.as_deref() == Some("role")),
        "a role breach is surfaced: {events:?}"
    );
}

#[test]
fn the_crew_budget_idle_stops_the_whole_crew_and_is_surfaced() {
    let base = start_broker();
    let roster = RosterClient::new(base.clone());
    let backend = RoleId::new("backend");
    let frontend = RoleId::new("frontend");

    // A crew-wide budget of 1000 tokens, no per-role caps.
    let budget = Budget::new(Some(1_000), BTreeMap::new());
    let fleet = Fleet::launch(
        &roster,
        vec![
            stub("backend", "echo ready; sleep 30"),
            stub("frontend", "echo ready; sleep 30"),
        ],
        LifecyclePolicy::default(),
    )
    .with_budget(budget);

    fleet.start(&backend).unwrap();
    fleet.start(&frontend).unwrap();
    assert!(
        wait_until(|| liveness(&base, "backend").as_deref() == Some("working")
            && liveness(&base, "frontend").as_deref() == Some("working")),
        "both roles come online"
    );

    // Two roles' spend together reaches the crew budget, though neither alone would.
    fleet.record_spend(&backend, 600).unwrap();
    fleet.record_spend(&frontend, 400).unwrap();

    // The crew is bounded: every role idle-stops.
    assert!(
        wait_until(|| liveness(&base, "backend").as_deref() == Some("stopped")
            && liveness(&base, "frontend").as_deref() == Some("stopped")),
        "the whole crew stands down at the crew budget"
    );

    // The crew breach is surfaced on the stream.
    let events = budget_events(&base);
    assert!(
        events
            .iter()
            .any(|(_, breach)| breach.as_deref() == Some("crew")),
        "a crew breach is surfaced: {events:?}"
    );
}

#[test]
fn an_unbounded_crew_records_nothing() {
    let base = start_broker();
    let roster = RosterClient::new(base.clone());
    let backend = RoleId::new("backend");

    // No crew budget and no caps: budget enforcement is off.
    let fleet = Fleet::launch(
        &roster,
        vec![stub("backend", "echo ready; sleep 30")],
        LifecyclePolicy::default(),
    );

    fleet.start(&backend).unwrap();
    assert!(wait_until(
        || liveness(&base, "backend").as_deref() == Some("working")
    ));

    // A large spend neither stops the role nor emits a report.
    fleet.record_spend(&backend, 10_000_000).unwrap();
    assert_eq!(liveness(&base, "backend").as_deref(), Some("working"));
    assert!(
        budget_events(&base).is_empty(),
        "an unbounded crew reports no spend"
    );
}
