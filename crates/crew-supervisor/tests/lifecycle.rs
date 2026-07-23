//! End-to-end test of the agent lifecycle state machine (issue #22).
//!
//! It proves the acceptance: an idle role stops and restarts on demand, with the
//! roster and the stream reflecting every transition. The defibrillator's death
//! detection and recovery are covered in `defibrillator.rs`.

mod common;

use std::time::Duration;

use common::{lifecycle_events, liveness, start_broker, stub, wait_until};
use crew_core::RoleId;
use crew_supervisor::{AgentState, Fleet, LifecyclePolicy, RosterClient};

#[test]
fn an_idle_role_stops_and_restarts_on_demand() {
    let base = start_broker();
    let roster = RosterClient::new(base.clone());
    let commander = RoleId::new("commander");

    // Idle-stop quickly; keep the heartbeat and watchdog well above it so only the
    // idle path fires, and keep a generous recovery budget.
    let policy = LifecyclePolicy {
        idle_timeout: Duration::from_millis(300),
        heartbeat_timeout: Duration::from_secs(30),
        watchdog_timeout: Duration::from_secs(60),
        max_recoveries: 3,
    };
    // The stub prints once (activity), then idles, standing in for a finished turn.
    let fleet = Fleet::launch(
        &roster,
        vec![stub("commander", "echo ready; sleep 30")],
        policy,
    );

    // Lazy: nothing is running or registered until there is work.
    assert_eq!(fleet.state(&commander), Some(AgentState::Stopped));
    assert_eq!(
        liveness(&base, "commander"),
        None,
        "no roster entry before start"
    );

    // Start on first work: the role registers working (a `started`).
    fleet.start(&commander).unwrap();
    assert!(
        wait_until(|| liveness(&base, "commander").as_deref() == Some("working")),
        "the role starts and registers working",
    );

    // After the quiet period it idle-stops: parked idle, its entry kept.
    assert!(
        wait_until(|| liveness(&base, "commander").as_deref() == Some("idle")),
        "the idle role stops, keeping its roster entry",
    );
    assert_eq!(fleet.state(&commander), Some(AgentState::Idle));

    // Restart on demand: the role registers working again (a `restarted`).
    fleet.start(&commander).unwrap();
    assert!(
        wait_until(|| liveness(&base, "commander").as_deref() == Some("working")),
        "the role restarts on demand",
    );

    // The stream reflects each transition, in order.
    let events = lifecycle_events(&base);
    assert_eq!(
        &events[..3],
        ["started", "idle", "restarted"],
        "the stream reflects start, idle-stop, and restart",
    );

    fleet.shutdown();
    assert!(
        wait_until(|| liveness(&base, "commander").is_none()),
        "shutdown deregisters the role",
    );
}
