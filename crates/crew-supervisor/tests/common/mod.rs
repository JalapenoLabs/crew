//! Shared helpers for the lifecycle, defibrillator, and worktree integration
//! tests.
//!
//! Each test starts a real `crewd` in-process and manages a fleet of stub agent
//! processes (shells that stand in for a real `claude` turn, which needs no
//! external services in CI), observing the roster and the stream over HTTP.
#![allow(
    dead_code,
    reason = "each test binary that includes this module uses only a subset of the helpers"
)]

use std::{
    net::{Ipv4Addr, SocketAddr, TcpListener},
    thread,
    time::{Duration, Instant},
};

use crew_broker::{AppState, Config};
use crew_core::RoleId;
use crew_supervisor::{AgentCommand, PreparedAgent};

/// Starts a broker over a fresh in-memory store, returning the base URL it
/// serves on.
pub fn start_broker() -> String {
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

/// A stub agent process for `role`: run `script` under a shell.
pub fn stub(role: &str, script: &str) -> PreparedAgent {
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

/// The lifecycle events on the broker log, oldest first (`started`, `died`,
/// ...).
pub fn lifecycle_events(base: &str) -> Vec<String> {
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

/// Posts a `note` as the General to `channel` (for example `@commander` or
/// `all-units`), standing in for a human brief that should wake a parked role.
pub fn post_message(base: &str, channel: &str, body: &str) {
    let payload = serde_json::json!({
        "from": { "kind": "general" },
        "kind": "note",
        "body": body,
    });
    ureq::post(&format!("{base}/channels/{channel}/messages"))
        .set("content-type", "application/json")
        .send_string(&payload.to_string())
        .unwrap();
}

/// The liveness the roster records for `role`, if it is registered.
pub fn liveness(base: &str, role: &str) -> Option<String> {
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
pub fn wait_until(mut condition: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if condition() {
            return true;
        }
        thread::sleep(Duration::from_millis(20));
    }
    condition()
}
