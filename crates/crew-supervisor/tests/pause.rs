//! Pause enforced at the supervisor Fleet, not only via the agent contract
//! (issue #187).
//!
//! The broker's pause control (issue #41) gates work in state and asks each
//! role to honor a pause, but a non-compliant or wedged agent would keep
//! running. These tests prove the Fleet's pause monitor turns the brake and
//! kill switch into a process-level guarantee: a paused or stood-down role's
//! process is actually stopped, and the Fleet refuses to start a gated role
//! until it is resumed.
//!
//! Each test starts a real `crewd` in-process and drives its control endpoints,
//! observing the roster liveness the supervisor marks.

mod common;

use std::{thread, time::Duration};

use common::{liveness, start_broker, stub, wait_until};
use crew_core::RoleId;
use crew_supervisor::{Fleet, LifecyclePolicy, RosterClient};

/// A policy whose pause monitor polls the roster fast, so a test observes the
/// process-level enforcement promptly rather than on the one-second default.
fn fast_pause_policy() -> LifecyclePolicy {
    LifecyclePolicy {
        pause_poll_interval: Duration::from_millis(50),
        ..LifecyclePolicy::default()
    }
}

/// Posts to a broker control endpoint that takes no body (`/standdown`,
/// `/resume`, `/pause` crew-wide).
fn control(base: &str, path: &str) {
    ureq::post(&format!("{base}{path}"))
        .call()
        .unwrap_or_else(|err| panic!("POST {path} should succeed: {err}"));
}

/// Posts to a broker control endpoint with a JSON body (a per-role
/// `/pause` / `/resume`).
fn control_role(base: &str, path: &str, role: &str) {
    ureq::post(&format!("{base}{path}"))
        .set("content-type", "application/json")
        .send_string(&serde_json::json!({ "role": role }).to_string())
        .unwrap_or_else(|err| panic!("POST {path} for {role} should succeed: {err}"));
}

#[test]
fn a_stand_down_stops_a_running_role_at_the_process_level() {
    let base = start_broker();
    let roster = RosterClient::new(base.clone());
    let backend = RoleId::new("backend");

    let fleet = Fleet::launch(
        &roster,
        vec![stub("backend", "echo ready; sleep 30")],
        fast_pause_policy(),
    );
    fleet.start(&backend).unwrap();
    assert!(
        wait_until(|| liveness(&base, "backend").as_deref() == Some("working")),
        "backend comes online"
    );

    // The General stands the crew down: the Fleet must actually stop the process,
    // not leave it running and trust the agent to idle (issue #187).
    control(&base, "/standdown");
    assert!(
        wait_until(|| liveness(&base, "backend").as_deref() == Some("stopped")),
        "the pause monitor reaps a stood-down role at the process level"
    );
}

#[test]
fn a_per_role_pause_stops_only_that_role() {
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
        fast_pause_policy(),
    );
    fleet.start(&backend).unwrap();
    fleet.start(&frontend).unwrap();
    assert!(
        wait_until(|| liveness(&base, "backend").as_deref() == Some("working")
            && liveness(&base, "frontend").as_deref() == Some("working")),
        "both roles come online"
    );

    // Pausing one role holds only it; an unpaused peer keeps working.
    control_role(&base, "/pause", "backend");
    assert!(
        wait_until(|| liveness(&base, "backend").as_deref() == Some("stopped")),
        "the paused role is held at the process level"
    );
    assert_eq!(
        liveness(&base, "frontend").as_deref(),
        Some("working"),
        "an unpaused role keeps working"
    );
}

#[test]
fn the_fleet_refuses_to_start_a_gated_role_until_it_is_resumed() {
    let base = start_broker();
    let roster = RosterClient::new(base.clone());
    let backend = RoleId::new("backend");

    let fleet = Fleet::launch(
        &roster,
        vec![stub("backend", "echo ready; sleep 30")],
        fast_pause_policy(),
    );

    // Stand the crew down before the role ever starts, then give the pause monitor
    // a few ticks to read the gate.
    control(&base, "/standdown");
    thread::sleep(Duration::from_millis(300));

    // Starting a gated role must not bring it online: the Fleet refuses to feed it.
    fleet.start(&backend).unwrap();
    thread::sleep(Duration::from_millis(300));
    assert_ne!(
        liveness(&base, "backend").as_deref(),
        Some("working"),
        "the Fleet refuses to start a stood-down role"
    );

    // A resume clears the gate, so a start now brings the role online.
    control(&base, "/resume");
    thread::sleep(Duration::from_millis(300));
    fleet.start(&backend).unwrap();
    assert!(
        wait_until(|| liveness(&base, "backend").as_deref() == Some("working")),
        "a resumed role starts and comes online"
    );
}
