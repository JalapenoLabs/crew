//! End-to-end test of the defibrillator (issue #23).
//!
//! It proves the acceptance: a stalled or crashed agent is detected, recorded,
//! and revived, or handed to the operator once the recovery budget is spent,
//! with the roster and the stream reflecting each transition. Detection is
//! layered: the in-turn heartbeat catches a crash or a hang, and the background
//! watchdog catches an agent whose driver failed to.

mod common;

use std::time::Duration;

use common::{lifecycle_events, liveness, start_broker, stub, wait_until, TURN_CLOSE, TURN_OPEN};
use crew_core::RoleId;
use crew_supervisor::{AgentState, DeathCause, Fleet, LifecyclePolicy, Recovery, RosterClient};

/// A long timeout, so a given detection path never fires in a sub-second test.
const NEVER: Duration = Duration::from_secs(60);

#[test]
fn a_crashed_agent_is_recorded_revived_then_handed_off() {
    let base = start_broker();
    let roster = RosterClient::new(base.clone());
    let qa = RoleId::new("qa");

    // Only the crash path matters here (an exited process is caught regardless of
    // the silence timeouts); revive once, then hand off.
    let policy = LifecyclePolicy {
        idle_timeout: NEVER,
        heartbeat_timeout: NEVER,
        watchdog_timeout: NEVER,
        max_recoveries: 1,
        ..LifecyclePolicy::default()
    };
    // The stub exits immediately, so every start is an unexpected exit.
    let fleet = Fleet::launch(&roster, vec![stub("qa", "exit 1")], policy);

    fleet.start(&qa).unwrap();

    // The stream converges on: each death emits `died`, each revival `recovered`,
    // and the final handoff leaves it dead. Waiting on the whole sequence
    // avoids racing a transient `dead` during the revival.
    assert!(
        wait_until(|| lifecycle_events(&base) == ["started", "died", "recovered", "died"]),
        "the crash is recorded, revived, then handed off; got {:?}",
        lifecycle_events(&base),
    );
    // Terminal: handed to the operator, left dead.
    assert_eq!(fleet.state(&qa), Some(AgentState::Dead));
    assert_eq!(liveness(&base, "qa").as_deref(), Some("dead"));

    // Both incidents are recorded with the crash diagnostic.
    let incidents = fleet.incidents();
    assert_eq!(incidents.len(), 2, "one revived, one handed off");
    assert!(incidents
        .iter()
        .all(|incident| incident.cause == DeathCause::Crashed));
    assert!(
        incidents[0].detail.contains("exited"),
        "diagnostic detail is recorded"
    );
    assert_eq!(incidents[0].recovery, Recovery::Revived);
    assert_eq!(incidents[1].recovery, Recovery::HandedOff);

    fleet.shutdown();
}

#[test]
fn a_hung_agent_is_detected_by_the_heartbeat_and_recovered() {
    let base = start_broker();
    let roster = RosterClient::new(base.clone());
    let backend = RoleId::new("backend");

    // The agent is mid-turn, so its silence is a hang the heartbeat recovers, not
    // an idle to park. One recovery, then hand off.
    let policy = LifecyclePolicy {
        idle_timeout: NEVER,
        heartbeat_timeout: Duration::from_millis(300),
        watchdog_timeout: NEVER,
        max_recoveries: 1,
        ..LifecyclePolicy::default()
    };
    // The stub opens a turn (an `init`), then goes silent: a turn hung mid-flight.
    let fleet = Fleet::launch(
        &roster,
        vec![stub("backend", &format!("echo '{TURN_OPEN}'; sleep 300"))],
        policy,
    );

    fleet.start(&backend).unwrap();

    // The heartbeat presumes it dead, reaps it, revives once, then hands it off.
    assert!(
        wait_until(|| lifecycle_events(&base) == ["started", "died", "recovered", "died"]),
        "the hang is recorded, revived, then handed off; got {:?}",
        lifecycle_events(&base),
    );
    assert_eq!(fleet.state(&backend), Some(AgentState::Dead));
    assert_eq!(liveness(&base, "backend").as_deref(), Some("dead"));

    let incidents = fleet.incidents();
    assert_eq!(incidents.len(), 2);
    assert!(
        incidents
            .iter()
            .all(|incident| incident.cause == DeathCause::Hung),
        "the hang is diagnosed, not a crash",
    );
    assert!(incidents[0].detail.contains("hung"));

    fleet.shutdown();
}

#[test]
fn the_watchdog_reaps_an_agent_the_in_turn_path_missed() {
    let base = start_broker();
    let roster = RosterClient::new(base.clone());
    let docs = RoleId::new("docs");

    // Disable the in-turn heartbeat and idle-stop (huge timeouts), so the driver
    // never acts on the silent agent; only the watchdog can catch it, standing
    // in for a driver that has wedged.
    let policy = LifecyclePolicy {
        idle_timeout: NEVER,
        heartbeat_timeout: NEVER,
        watchdog_timeout: Duration::from_millis(300),
        max_recoveries: 3,
        ..LifecyclePolicy::default()
    };
    // The stub opens a turn (an `init`), then goes silent: a hang mid-turn that a
    // wedged driver leaves for the watchdog.
    let fleet = Fleet::launch(
        &roster,
        vec![stub("docs", &format!("echo '{TURN_OPEN}'; sleep 300"))],
        policy,
    );

    fleet.start(&docs).unwrap();

    // The watchdog reaps the orphan and hands it straight to the operator: no
    // revival.
    assert!(
        wait_until(|| lifecycle_events(&base) == ["started", "died"]),
        "the watchdog reaps the agent its driver did not handle; got {:?}",
        lifecycle_events(&base),
    );
    assert_eq!(fleet.state(&docs), Some(AgentState::Dead));
    assert_eq!(liveness(&base, "docs").as_deref(), Some("dead"));

    let incidents = fleet.incidents();
    assert_eq!(incidents.len(), 1);
    assert_eq!(incidents[0].cause, DeathCause::Wedged);
    assert_eq!(incidents[0].recovery, Recovery::HandedOff);

    fleet.shutdown();
}

#[test]
fn a_quiet_mid_turn_agent_is_recovered_not_idle_stopped() {
    // Issue #217: a mid-turn agent that goes silent is hung, not idle, even when
    // the idle-stop clock is the shorter one. The driver recovers it rather than
    // parking it, so its in-flight work resumes instead of stalling.
    let base = start_broker();
    let roster = RosterClient::new(base.clone());
    let backend = RoleId::new("backend");

    // Idle-stop is shorter than the heartbeat, so the raw output-silence clock
    // would have parked this agent first; its turn state must override that.
    let policy = LifecyclePolicy {
        idle_timeout: Duration::from_millis(300),
        heartbeat_timeout: Duration::from_millis(700),
        watchdog_timeout: NEVER,
        max_recoveries: 1,
        ..LifecyclePolicy::default()
    };
    // Open a turn, then go silent mid-turn.
    let fleet = Fleet::launch(
        &roster,
        vec![stub("backend", &format!("echo '{TURN_OPEN}'; sleep 300"))],
        policy,
    );

    fleet.start(&backend).unwrap();

    // It is recovered as a hang and, its budget spent, handed off dead; it never
    // parks idle, the way the shorter idle clock alone would have made it.
    assert!(
        wait_until(|| lifecycle_events(&base) == ["started", "died", "recovered", "died"]),
        "a mid-turn hang is recovered, not idle-stopped; got {:?}",
        lifecycle_events(&base),
    );
    assert!(
        !lifecycle_events(&base).iter().any(|event| event == "idle"),
        "a mid-turn agent is never parked idle; got {:?}",
        lifecycle_events(&base),
    );
    assert_eq!(liveness(&base, "backend").as_deref(), Some("dead"));
    let incidents = fleet.incidents();
    assert!(
        incidents
            .iter()
            .all(|incident| incident.cause == DeathCause::Hung),
        "the mid-turn silence is diagnosed as a hang, not idleness",
    );

    fleet.shutdown();
}

#[test]
fn the_watchdog_parks_a_quiet_between_turns_agent() {
    // Issue #217: the watchdog tells a hang from an idle. An agent that finished
    // its turn (an `init` then a `result`) and went quiet is idle, so a watchdog
    // backing up a wedged driver parks it rather than reaping it as a death.
    let base = start_broker();
    let roster = RosterClient::new(base.clone());
    let docs = RoleId::new("docs");

    // Disable the driver's own idle-stop and heartbeat (huge timeouts), so only the
    // watchdog acts, standing in for a driver that has wedged.
    let policy = LifecyclePolicy {
        idle_timeout: NEVER,
        heartbeat_timeout: NEVER,
        watchdog_timeout: Duration::from_millis(300),
        max_recoveries: 3,
        ..LifecyclePolicy::default()
    };
    // Open then close a turn, then go quiet: idle between turns, not hung.
    let fleet = Fleet::launch(
        &roster,
        vec![stub(
            "docs",
            &format!("echo '{TURN_OPEN}'; echo '{TURN_CLOSE}'; sleep 300"),
        )],
        policy,
    );

    fleet.start(&docs).unwrap();

    // The watchdog parks it idle, keeping its roster entry: no death, no incident.
    assert!(
        wait_until(|| liveness(&base, "docs").as_deref() == Some("idle")),
        "the watchdog parks a between-turns idle agent; got {:?}",
        liveness(&base, "docs"),
    );
    assert_eq!(fleet.state(&docs), Some(AgentState::Idle));
    assert!(
        !lifecycle_events(&base).iter().any(|event| event == "died"),
        "a between-turns idle agent is not reaped as a death; got {:?}",
        lifecycle_events(&base),
    );
    assert!(fleet.incidents().is_empty(), "parking records no incident");

    fleet.shutdown();
}
