//! Rules of engagement through the `crew` binary, end to end (issue #39).
//!
//! Drives the real CLI both ways: a shim agent runs `crew request-approval` and
//! blocks on a gated action, the General runs `crew approvals` then `crew
//! approve` to grant it, and the blocked command unblocks and exits zero. It
//! also proves an ungated action proceeds at once with no wait.

use std::{
    io::Write,
    net::{Ipv4Addr, TcpListener},
    process::{Command, Output},
    thread,
    time::{Duration, Instant},
};

use crew_substrate::broker::{AppState, Config};
use serde_json::Value;

/// Starts a broker over a fresh in-memory store, returning the loopback port.
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

/// Runs `crew` as the shim role `backend`, pointed at the broker on `port`.
fn crew_role(port: u16, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_crew"))
        .args(args)
        .env("CREW_ROLE", "backend")
        .env("CREW_BROKER_HOST", "127.0.0.1")
        .env("CREW_BROKER_PORT", port.to_string())
        .env_remove("CREW_ROLE_CARD")
        .output()
        .expect("the crew binary runs")
}

/// Runs `crew` as the General (no role), pointed at the broker on `port`.
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

/// Waits for a pending `approval_request` and returns its message id.
fn await_request_id(port: u16) -> String {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if let Some(id) = messages(port).iter().find_map(|event| {
            let data = &event["kind"]["data"];
            (data["kind"] == "approval_request").then(|| data["id"].as_str().unwrap().to_owned())
        }) {
            return id;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("no approval request appeared on the stream");
}

#[test]
fn the_general_grants_a_blocked_request_end_to_end() {
    let port = start_broker();

    // A specialist requests approval for a gated merge; the command blocks.
    let requester = thread::spawn(move || {
        crew_role(
            port,
            &[
                "request-approval",
                "merge",
                "--detail",
                "merge the login PR",
            ],
        )
    });

    // The General lists the pending request and grants it.
    let request_id = await_request_id(port);
    let listed = crew_general(port, &["approvals"]);
    assert!(listed.status.success(), "crew approvals runs");
    let listed = String::from_utf8_lossy(&listed.stdout);
    assert!(
        listed.contains(&request_id) && listed.contains("backend") && listed.contains("merge"),
        "the pending request is listed with its id: {listed}",
    );

    let approve = crew_general(port, &["approve", &request_id, "--reason", "ship it"]);
    assert!(
        approve.status.success(),
        "crew approve succeeds: {}",
        String::from_utf8_lossy(&approve.stderr),
    );

    // The blocked request unblocks and exits zero on the grant.
    let outcome = requester.join().unwrap();
    assert!(
        outcome.status.success(),
        "the granted request proceeds: {}",
        String::from_utf8_lossy(&outcome.stderr),
    );
    assert!(
        String::from_utf8_lossy(&outcome.stdout).contains("approved"),
        "the requester is told it was approved",
    );

    // The decision is on the stream, threaded to the request.
    let decided = messages(port).iter().any(|event| {
        let data = &event["kind"]["data"];
        data["kind"] == "approval_decision"
            && data["in_reply_to"] == request_id.as_str()
            && data["granted"] == true
    });
    assert!(decided, "the grant is recorded as an approval_decision");
}

#[test]
fn an_ungated_action_proceeds_through_the_shim() {
    let port = start_broker();

    // A role card whose rules gate nothing: the action proceeds with no wait.
    let card = format!(
        "role = \"backend\"\n\n[roe]\ngated = []\n\n[broker]\nhost = \"127.0.0.1\"\nport = {port}\n",
    );
    let card_path = std::env::temp_dir().join(format!("crew-roe-{}.toml", std::process::id()));
    std::fs::File::create(&card_path)
        .unwrap()
        .write_all(card.as_bytes())
        .unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_crew"))
        .args(["request-approval", "push"])
        .env("CREW_ROLE_CARD", &card_path)
        .env_remove("CREW_ROLE")
        .output()
        .expect("the crew binary runs");
    assert!(
        output.status.success(),
        "an ungated action proceeds: {}",
        String::from_utf8_lossy(&output.stderr),
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("needs no approval"),
        "the shim reports the action is ungated",
    );
    assert!(
        messages(port).is_empty(),
        "no request was posted for an ungated action"
    );

    let _ = std::fs::remove_file(&card_path);
}
