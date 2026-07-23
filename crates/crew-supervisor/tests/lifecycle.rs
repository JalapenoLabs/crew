//! End-to-end test of the agent lifecycle state machine (issue #22).
//!
//! It starts a real `crewd` in-process and manages a fleet of stub agent processes
//! (shells that stand in for a real `claude` turn, which needs no external services in
//! CI). The assertions prove the acceptance: an idle role stops and restarts on
//! demand, and a crash-looping role dies after its restart budget, with the roster and
//! the stream reflecting every transition.

use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::thread;
use std::time::{Duration, Instant};

use crew_broker::{AppState, Config};
use crew_core::RoleId;
use crew_supervisor::{
    AgentCommand, AgentState, Fleet, LifecyclePolicy, PreparedAgent, RosterClient,
};

/// Starts a broker over a fresh in-memory store, returning the address it serves on.
fn start_broker() -> SocketAddr {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            let state = AppState::new(Config::default());
            let _ = crew_broker::serve(listener, state, std::future::pending::<()>()).await;
        });
    });
    addr
}

/// A stub agent process: run `script` under a shell.
fn stub(role: &str, script: &str) -> PreparedAgent {
    PreparedAgent {
        role: RoleId::new(role),
        owned_paths: vec![format!("{role}/")],
        command: AgentCommand {
            program: "bash".to_owned(),
            args: vec!["-c".to_owned(), script.to_owned()],
            env: Vec::new(),
            cwd: std::env::temp_dir(),
        },
    }
}

/// The lifecycle events on the broker log, oldest first (`started`, `idle`, ...).
fn lifecycle_events(base: &str) -> Vec<String> {
    let text = ureq::get(&format!("{base}/history?kind=lifecycle"))
        .call()
        .unwrap()
        .into_string()
        .unwrap();
    let value: serde_json::Value = serde_json::from_str(&text).unwrap();
    value["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|event| event["kind"]["data"].as_str().map(str::to_owned))
        .collect()
}

/// The liveness the roster records for `role`, if it is registered.
fn liveness(base: &str, role: &str) -> Option<String> {
    let text = ureq::get(&format!("{base}/roster"))
        .call()
        .unwrap()
        .into_string()
        .unwrap();
    let value: serde_json::Value = serde_json::from_str(&text).unwrap();
    value["roles"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["role"] == role)
        .and_then(|entry| entry["liveness"].as_str().map(str::to_owned))
}

/// Polls `condition` until it holds or the deadline passes, returning whether it held.
fn wait_until(mut condition: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if condition() {
            return true;
        }
        thread::sleep(Duration::from_millis(20));
    }
    condition()
}

#[test]
fn an_idle_role_stops_and_restarts_on_demand() {
    let base = format!("http://{}", start_broker());
    let roster = RosterClient::new(base.clone());
    let commander = RoleId::new("commander");

    // Idle-stop quickly so the test does not wait; keep a generous restart budget.
    let policy = LifecyclePolicy {
        idle_timeout: Duration::from_millis(300),
        max_restarts: 3,
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

#[test]
fn a_crash_looping_role_dies_after_its_restart_budget() {
    let base = format!("http://{}", start_broker());
    let roster = RosterClient::new(base.clone());
    let qa = RoleId::new("qa");

    // A long idle timeout so only the crash path fires; one restart, then dead.
    let policy = LifecyclePolicy {
        idle_timeout: Duration::from_secs(30),
        max_restarts: 1,
    };
    // The stub exits immediately, so every start is an unexpected exit.
    let fleet = Fleet::launch(&roster, vec![stub("qa", "exit 1")], policy);

    fleet.start(&qa).unwrap();

    // It restarts once, then gives up: marked dead on the roster.
    assert!(
        wait_until(|| fleet.state(&qa) == Some(AgentState::Dead)),
        "the role dies after exhausting its restart budget",
    );
    assert!(
        wait_until(|| liveness(&base, "qa").as_deref() == Some("dead")),
        "the roster records the death",
    );

    // The stream shows the start, the one bounded restart, and the death.
    assert_eq!(
        lifecycle_events(&base),
        ["started", "restarted", "died"],
        "the stream reflects the bounded restart then death",
    );

    fleet.shutdown();
}
