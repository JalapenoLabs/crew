//! End-to-end test of spawning a crew and wiring it to the broker (issue #21).
//!
//! It starts a real `crewd` in-process on an ephemeral loopback port, then
//! spawns a crew of stub agent processes (a shell that prints a line and idles,
//! standing in for a real `claude` turn, which needs no external services or an
//! API key in CI). The assertions prove the acceptance: given N roles, N agents
//! start, register with the broker, can message each other, and deregister on
//! exit. Output capture is proven too, since the activity parser (issue #24)
//! reads that stream.

use std::{
    net::{Ipv4Addr, SocketAddr, TcpListener},
    sync::mpsc::Receiver,
    thread,
    time::{Duration, Instant},
};

use crew_broker::{AppState, Config};
use crew_core::RoleId;
use crew_supervisor::{AgentCommand, Captured, Crew, OutputStream, PreparedAgent, RosterClient};

/// Starts a broker over a fresh in-memory store, returning the address it
/// serves on.
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

/// A stub agent process: print a line to stdout, then idle until the supervisor
/// kills it. It stands in for a real `claude -p` turn so the lifecycle runs
/// without claude.
fn stub_command() -> AgentCommand {
    AgentCommand {
        program: "bash".to_owned(),
        args: vec!["-c".to_owned(), "echo ready; sleep 30".to_owned()],
        env: Vec::new(),
        cwd: std::env::temp_dir(),
    }
}

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

/// Waits for a stdout line with the given text, or panics after `within`.
fn wait_for_stdout(outputs: &Receiver<Captured>, text: &str, within: Duration) -> Captured {
    let deadline = Instant::now() + within;
    loop {
        let remaining = deadline
            .checked_duration_since(Instant::now())
            .expect("timed out waiting for the agent's output");
        let captured = outputs
            .recv_timeout(remaining)
            .expect("an output line arrives");
        if captured.stream == OutputStream::Stdout && captured.line == text {
            return captured;
        }
    }
}

#[test]
fn a_crew_starts_registers_messages_and_deregisters() {
    let base = format!("http://{}", start_broker());
    let roster = RosterClient::new(base.clone());

    // A config of N roles becomes N prepared agents.
    let roles = ["commander", "backend", "frontend"];
    let prepared: Vec<PreparedAgent> = roles
        .iter()
        .map(|role| PreparedAgent {
            role: RoleId::new(*role),
            owned_paths: vec![format!("{role}/")],
            command: stub_command(),
        })
        .collect();

    let crew = Crew::spawn(&roster, prepared).expect("the crew spawns");

    // N agents started and registered with the broker on start.
    let mut registered = roster.roles().expect("the roster is readable");
    registered.sort();
    assert_eq!(
        registered,
        vec![
            RoleId::new("backend"),
            RoleId::new("commander"),
            RoleId::new("frontend"),
        ],
        "every role registers on start",
    );

    // The agents can message each other: a note to @backend is routed and stored,
    // so any teammate's inbox would receive it.
    post_note(&base, "@backend", "frontend", "the API is ready");
    assert!(
        history_bodies(&base)
            .iter()
            .any(|body| body == "the API is ready"),
        "the message reaches the broker log",
    );

    // Each process's stdout is captured for the activity parser (issue #24).
    let line = wait_for_stdout(crew.outputs(), "ready", Duration::from_secs(5));
    assert!(
        roles.contains(&line.role.as_str()),
        "the line is tagged with its role"
    );

    crew.shutdown().expect("the crew shuts down");

    // Every role deregistered on exit, so the roster reflects the live unit.
    assert!(
        roster.roles().expect("the roster is readable").is_empty(),
        "the roster is empty after shutdown",
    );
}
