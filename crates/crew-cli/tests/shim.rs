//! End-to-end test of the agent CLI shim (issue #28).
//!
//! It starts a real `crewd` in-process on an ephemeral loopback port, then drives the
//! actual `crew` binary the way a Codex agent (a runtime without MCP) would: it boots
//! from the role environment, registers on the roster, sends a message, and reads its
//! inbox. The assertions prove the acceptance: a shim agent participates in a crew and
//! appears on the roster and the stream, exactly as the MCP path does.

use std::io::Write;
use std::net::{Ipv4Addr, TcpListener};
use std::process::{Command, Output};
use std::thread;
use std::time::{Duration, Instant};

use crew_substrate::broker::{AppState, Config};
use serde_json::Value;

/// Starts a broker over a fresh in-memory store, returning the loopback port it serves.
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

    // Wait for the broker to accept connections before the shim tries to reach it.
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

/// Runs the `crew` binary as `role`, pointed at the broker on `port` via the env boot.
fn crew(port: u16, role: &str, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_crew"))
        .args(args)
        .env("CREW_ROLE", role)
        .env("CREW_BROKER_HOST", "127.0.0.1")
        .env("CREW_BROKER_PORT", port.to_string())
        .env_remove("CREW_ROLE_CARD")
        .output()
        .expect("the crew binary runs")
}

/// The stdout of a successful `crew` run, or a panic naming the failure.
fn stdout_of(output: Output, what: &str) -> String {
    assert!(
        output.status.success(),
        "`crew {what}` failed: {}",
        String::from_utf8_lossy(&output.stderr),
    );
    String::from_utf8(output.stdout).unwrap()
}

/// Fetches and parses a JSON endpoint on the broker.
fn get_json(port: u16, path: &str) -> Value {
    let text = ureq::get(&format!("{}{path}", base_url(port)))
        .call()
        .unwrap()
        .into_string()
        .unwrap();
    serde_json::from_str(&text).unwrap()
}

#[test]
fn a_shim_agent_registers_sends_and_reads_its_inbox() {
    let port = start_broker();

    // Boot: the Codex agent registers, so the unit sees it on the roster.
    stdout_of(crew(port, "codexbot", &["register"]), "register");
    let roster = get_json(port, "/roster");
    let entry = roster["roles"]
        .as_array()
        .unwrap()
        .iter()
        .find(|role| role["role"] == "codexbot")
        .expect("the shim agent appears on the roster");
    assert_eq!(
        entry["liveness"], "working",
        "a freshly registered role is working"
    );

    // It sends a message, which lands on the broker's stream.
    stdout_of(
        crew(
            port,
            "codexbot",
            &["send", "--channel", "all-units", "codex reporting in"],
        ),
        "send",
    );
    let history = get_json(port, "/history?kind=message");
    let sent = history["events"].as_array().unwrap().iter().find(|event| {
        event["from"]["id"] == "codexbot" && event["kind"]["data"]["body"] == "codex reporting in"
    });
    assert!(sent.is_some(), "the shim agent's message is on the stream");

    // A teammate direct-messages it; its inbox reads the message, self-filtered.
    crew(port, "backend", &["register"]);
    stdout_of(
        crew(
            port,
            "backend",
            &["send", "--to", "codexbot", "build the parser"],
        ),
        "send --to",
    );
    let inbox = stdout_of(crew(port, "codexbot", &["inbox"]), "inbox");
    assert!(
        inbox.contains("build the parser"),
        "the inbox shows the message addressed to the role: {inbox}",
    );
    assert!(
        !inbox.contains("codex reporting in"),
        "the inbox never shows the role's own message: {inbox}",
    );

    // The roster command lists the unit the agent joined.
    let listed = stdout_of(crew(port, "codexbot", &["roster"]), "roster");
    assert!(listed.contains("codexbot"), "roster lists the shim agent");
    assert!(listed.contains("backend"), "roster lists its teammate");
}

#[test]
fn the_shim_boots_from_a_role_card_with_its_lane() {
    let port = start_broker();

    // A role card carries the lane, so registration announces the paths it owns.
    let card = format!(
        "role = \"codexbot\"\nowned_paths = [\"adapters/\"]\n\n[broker]\nhost = \"127.0.0.1\"\nport = {port}\n",
    );
    let card_path = std::env::temp_dir().join(format!("crew-shim-{}.toml", std::process::id()));
    std::fs::File::create(&card_path)
        .unwrap()
        .write_all(card.as_bytes())
        .unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_crew"))
        .arg("register")
        .env("CREW_ROLE_CARD", &card_path)
        .env_remove("CREW_ROLE")
        .output()
        .expect("the crew binary runs");
    stdout_of(output, "register (card)");

    let roster = get_json(port, "/roster");
    let entry = roster["roles"]
        .as_array()
        .unwrap()
        .iter()
        .find(|role| role["role"] == "codexbot")
        .expect("the card-booted role appears on the roster");
    assert_eq!(
        entry["owned_paths"],
        serde_json::json!(["adapters/"]),
        "it registers the lane its card declares",
    );

    let _ = std::fs::remove_file(&card_path);
}

#[test]
fn the_shim_errors_without_a_role_context() {
    // No role card and no CREW_ROLE: the shim cannot know who it acts as.
    let output = Command::new(env!("CARGO_BIN_EXE_crew"))
        .args(["send", "hello"])
        .env_remove("CREW_ROLE")
        .env_remove("CREW_ROLE_CARD")
        .output()
        .expect("the crew binary runs");
    assert!(
        !output.status.success(),
        "a shim command needs a role context"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("CREW_ROLE"),
        "the error names the missing environment: {stderr}",
    );
}
