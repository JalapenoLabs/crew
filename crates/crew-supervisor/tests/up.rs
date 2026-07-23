//! End-to-end test of the `crew up` / `crew down` orchestration mechanics
//! (issue #26).
//!
//! It proves the acceptance without a real `claude`: given the crew config,
//! `Fleet::start_all` brings every role online and connected to a real
//! in-process broker, and `Fleet::shutdown` stands the unit down leaving no
//! orphaned processes and no stale roster entry. The stub agents (shells that
//! idle until the fleet kills them) stand in for the `claude` processes
//! `Supervisor::launch` would spawn.

use std::{
    net::{Ipv4Addr, SocketAddr, TcpListener},
    thread,
    time::{Duration, Instant},
};

use crew_broker::{AppState, Config};
use crew_core::{CrewConfig, RoleId};
use crew_supervisor::{AgentCommand, Fleet, LifecyclePolicy, PreparedAgent, RosterClient};

/// Starts a broker over a fresh in-memory store, returning the base URL it
/// serves on.
fn start_broker() -> String {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
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
    format!("http://{addr}")
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

/// Polls `condition` until it holds or a five-second deadline passes.
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

/// A stub agent for each role the config declares: a shell that idles until the
/// fleet kills it, standing in for the `claude` process `Supervisor::launch`
/// would spawn.
fn stub_agents(config: &CrewConfig) -> Vec<PreparedAgent> {
    config
        .roles
        .iter()
        .map(|spec| PreparedAgent {
            role: spec.role.clone(),
            owned_paths: spec.owned_paths.clone(),
            command: AgentCommand {
                program: "bash".to_owned(),
                args: vec!["-c".to_owned(), "echo ready; sleep 30".to_owned()],
                env: Vec::new(),
                cwd: std::env::temp_dir(),
            },
        })
        .collect()
}

#[test]
fn crew_up_brings_the_unit_online_and_down_leaves_no_orphans() {
    let base = start_broker();
    let roster = RosterClient::new(base.clone());

    // The default crew: commander, backend, frontend, qa (see docs/config.md).
    let config = CrewConfig::default();
    let expected: Vec<RoleId> = config.roles.iter().map(|spec| spec.role.clone()).collect();
    assert_eq!(expected.len(), 4, "the default crew is four roles");

    // Keep idle-stop far out so the roles stay working through the assertions
    // rather than parking mid-test; `crew up` bringing the unit online is what
    // is under test.
    let policy = LifecyclePolicy {
        idle_timeout: Duration::from_secs(3600),
        ..LifecyclePolicy::default()
    };
    let fleet = Fleet::launch(&roster, stub_agents(&config), policy);

    // crew up: bring the whole unit online.
    fleet.start_all().expect("every role starts");

    // A live, connected unit: every configured role registers on the roster,
    // working.
    for role in &expected {
        assert!(
            wait_until(|| liveness(&base, role.as_str()).as_deref() == Some("working")),
            "role `{role}` should be working after crew up",
        );
    }

    // crew down: stand the unit down gracefully.
    fleet.shutdown();

    // No orphaned processes and no stale roster: every role deregistered on the way
    // down.
    for role in &expected {
        assert!(
            wait_until(|| liveness(&base, role.as_str()).is_none()),
            "role `{role}` should be gone from the roster after crew down",
        );
    }
}
