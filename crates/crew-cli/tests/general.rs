//! End-to-end test of the General's plain `crew brief` (issue #118).
//!
//! It starts a real `crewd` in-process on an ephemeral loopback port, then
//! drives the actual `crew` binary the way the operator would: `crew brief`
//! posts a free-form note as the General, to the commander by default, a named
//! role, or a channel. Unlike the agent shim it needs no role card, so the runs
//! set only the broker environment. The assertions prove the acceptance: the
//! General can post a plain note and an all-units broadcast as `general`, on
//! the stream.

use std::{
    net::{Ipv4Addr, TcpListener},
    process::{Command, Output},
    thread,
    time::{Duration, Instant},
};

use crew_substrate::broker::{AppState, Config};
use serde_json::Value;

/// Starts a broker over a fresh in-memory store, returning the loopback port it
/// serves.
fn start_broker() -> u16 {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    listener.set_nonblocking(true).unwrap();
    let port = listener.local_addr().unwrap().port();
    thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        runtime.block_on(async move {
            let listener = tokio::net::TcpListener::from_std(listener).unwrap();
            let state = AppState::new(Config::default());
            let _ =
                crew_substrate::broker::serve(listener, state, std::future::pending::<()>()).await;
        });
    });

    // Wait for the broker to accept connections before the binary reaches it.
    let base = base_url(port);
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if ureq::get(&format!("{base}/health")).call().is_ok() {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    port
}

fn base_url(port: u16) -> String {
    format!("http://127.0.0.1:{port}")
}

/// Runs `crew` pointed at the broker on `port`, with no role context: a General
/// command needs only the broker address, not a role card.
fn crew_general(port: u16, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_crew"))
        .args(args)
        .env("CREW_BROKER_HOST", "127.0.0.1")
        .env("CREW_BROKER_PORT", port.to_string())
        .env_remove("CREW_ROLE")
        .env_remove("CREW_ROLE_CARD")
        .output()
        .expect("the crew binary runs")
}

/// Asserts the run succeeded, returning its stdout, or panics naming the
/// failure.
fn stdout_of(output: Output, what: &str) -> String {
    assert!(
        output.status.success(),
        "`crew {what}` failed: {}",
        String::from_utf8_lossy(&output.stderr),
    );
    String::from_utf8(output.stdout).unwrap()
}

/// The message events on the broker's stream.
fn messages(port: u16) -> Vec<Value> {
    let text = ureq::get(&format!("{}/history?kind=message", base_url(port)))
        .call()
        .unwrap()
        .into_string()
        .unwrap();
    let history: Value = serde_json::from_str(&text).unwrap();
    history["events"].as_array().unwrap().clone()
}

/// Whether a `note` from the General with `body` reached `channel`.
fn general_note_on(events: &[Value], channel: &str, body: &str) -> bool {
    events.iter().any(|event| {
        event["from"]["kind"] == "general"
            && event["channel"] == channel
            && event["kind"]["data"]["kind"] == "note"
            && event["kind"]["data"]["body"] == body
    })
}

#[test]
fn the_general_briefs_the_commander_a_role_and_all_units() {
    let port = start_broker();

    // The default brief, no target, reaches the commander as a General note.
    stdout_of(
        crew_general(port, &["brief", "ship the login flow"]),
        "brief",
    );
    // A named role wins over the default.
    stdout_of(
        crew_general(port, &["brief", "--to", "backend", "own the api lane"]),
        "brief --to",
    );
    // A channel name broadcasts to the whole unit.
    stdout_of(
        crew_general(port, &["brief", "--channel", "all-units", "all hands"]),
        "brief --channel",
    );

    let events = messages(port);
    assert!(
        general_note_on(&events, "@commander", "ship the login flow"),
        "the default brief reaches the commander as a General note: {events:?}",
    );
    assert!(
        general_note_on(&events, "@backend", "own the api lane"),
        "a role brief reaches that role: {events:?}",
    );
    assert!(
        general_note_on(&events, "all-units", "all hands"),
        "a channel brief broadcasts to all-units: {events:?}",
    );
}

#[test]
fn a_brief_to_an_unroutable_target_fails_cleanly() {
    let port = start_broker();
    let output = crew_general(port, &["brief", "--channel", "nonsense", "hello"]);
    assert!(
        !output.status.success(),
        "an unroutable channel is refused, not posted",
    );
    // Nothing was posted for the bad target.
    assert!(
        messages(port).is_empty(),
        "no message is sent when the target does not resolve",
    );
}
