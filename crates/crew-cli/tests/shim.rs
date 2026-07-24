//! End-to-end test of the agent CLI shim (issue #28).
//!
//! It starts a real `crewd` in-process on an ephemeral loopback port, then
//! drives the actual `crew` binary the way a Codex agent (a runtime without
//! MCP) would: it boots from the role environment, registers on the roster,
//! sends a message, and reads its inbox. The assertions prove the acceptance: a
//! shim agent participates in a crew and appears on the roster and the stream,
//! exactly as the MCP path does.

use std::{
    io::Write,
    net::{Ipv4Addr, TcpListener},
    path::{Path, PathBuf},
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

/// Runs the `crew` binary as `role`, pointed at the broker on `port` via the
/// env boot, with `state_dir` for its per-role shim cursor (issue #130).
fn crew(port: u16, state_dir: &Path, role: &str, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_crew"))
        .args(args)
        .env("CREW_ROLE", role)
        .env("CREW_BROKER_HOST", "127.0.0.1")
        .env("CREW_BROKER_PORT", port.to_string())
        .env("CREW_BROKER_STATE_DIR", state_dir)
        .env_remove("CREW_ROLE_CARD")
        .output()
        .expect("the crew binary runs")
}

/// A fresh, unique state dir for one test's shim cursors, isolated from the
/// other tests sharing this binary's process id.
fn state_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("crew-shim-{}-{tag}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
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
    let state = state_dir("basic");

    // Boot: the Codex agent registers, so the unit sees it on the roster.
    stdout_of(crew(port, &state, "codexbot", &["register"]), "register");
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
            &state,
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
    crew(port, &state, "backend", &["register"]);
    stdout_of(
        crew(
            port,
            &state,
            "backend",
            &["send", "--to", "codexbot", "build the parser"],
        ),
        "send --to",
    );
    let inbox = stdout_of(crew(port, &state, "codexbot", &["inbox"]), "inbox");
    assert!(
        inbox.contains("build the parser"),
        "the inbox shows the message addressed to the role: {inbox}",
    );
    assert!(
        !inbox.contains("codex reporting in"),
        "the inbox never shows the role's own message: {inbox}",
    );

    // The roster command lists the unit the agent joined.
    let listed = stdout_of(crew(port, &state, "codexbot", &["roster"]), "roster");
    assert!(listed.contains("codexbot"), "roster lists the shim agent");
    assert!(listed.contains("backend"), "roster lists its teammate");
}

#[test]
fn a_shim_commander_orders_a_specialist_a_structured_task() {
    let port = start_broker();
    let state = state_dir("order");

    crew(port, &state, "commander", &["register"]);
    crew(port, &state, "backend", &["register"]);

    // The commander fans a scoped task out to a specialist, not a plain note.
    stdout_of(
        crew(
            port,
            &state,
            "commander",
            &[
                "order",
                "backend",
                "Ship the parser",
                "--scope",
                "the tokenizer only",
                "--owns",
                "src/parser/",
                "--acceptance",
                "cargo test passes",
                "--body",
                "start from the grammar",
            ],
        ),
        "order",
    );

    // It lands on the broker's stream as a structured `order`, its fields intact.
    let history = get_json(port, "/history?kind=message");
    let order = history["events"]
        .as_array()
        .unwrap()
        .iter()
        .find(|event| event["kind"]["data"]["kind"] == "order")
        .expect("the order is on the stream");
    assert_eq!(order["from"]["id"], "commander", "the commander issued it");
    let detail = &order["kind"]["data"];
    assert_eq!(detail["title"], "Ship the parser");
    assert_eq!(detail["scope"], "the tokenizer only");
    assert_eq!(detail["owned_paths"], serde_json::json!(["src/parser/"]));
    assert_eq!(detail["acceptance"], "cargo test passes");
    assert_eq!(detail["body"], "start from the grammar");

    // The specialist reads it in its inbox, the way the MCP path renders one.
    let inbox = stdout_of(crew(port, &state, "backend", &["inbox"]), "inbox");
    assert!(
        inbox.contains("Ship the parser"),
        "the specialist's inbox shows the order: {inbox}",
    );
}

#[test]
fn a_shim_order_needs_a_single_specialist_not_a_channel() {
    let port = start_broker();
    let state = state_dir("order-target");

    crew(port, &state, "commander", &["register"]);

    // An order is a direct task to one specialist; a channel name (a pair here)
    // is not a role to order.
    let output = crew(
        port,
        &state,
        "commander",
        &["order", "frontend+backend", "do everything"],
    );
    assert!(!output.status.success(), "an order to a channel is refused");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("frontend+backend"),
        "the error names the bad target: {stderr}",
    );
}

/// The task id on the newest message of `kind`, if it carries one.
fn task_of_kind(port: u16, kind: &str) -> Option<String> {
    let history = get_json(port, "/history?kind=message");
    let events = history["events"].as_array().unwrap().clone();
    events
        .iter()
        .rev()
        .find(|event| event["kind"]["data"]["kind"] == kind)
        .and_then(|event| event["task"].as_str().map(str::to_owned))
}

/// The task id on the message whose body is `body`, if it carries one.
fn task_of_body(port: u16, body: &str) -> Option<String> {
    let history = get_json(port, "/history?kind=message");
    let events = history["events"].as_array().unwrap().clone();
    events
        .iter()
        .find(|event| event["kind"]["data"]["body"] == body)
        .and_then(|event| event["task"].as_str().map(str::to_owned))
}

#[test]
fn a_shim_role_stamps_its_adopted_task_across_separate_commands() {
    // Issue #132: each shim command is its own process, so the task a role adopts
    // from an order is persisted on disk. A later `crew send` restores it and
    // stamps it, so the role's work correlates to the task the order minted, the
    // way the long-lived MCP client does in memory.
    let port = start_broker();
    let state = state_dir("task");

    crew(port, &state, "commander", &["register"]);
    crew(port, &state, "backend", &["register"]);

    // The commander orders backend: the order mints a task and stamps it.
    stdout_of(
        crew(
            port,
            &state,
            "commander",
            &[
                "order",
                "backend",
                "build login",
                "--acceptance",
                "tests green",
            ],
        ),
        "order",
    );
    let order_task = task_of_kind(port, "order").expect("the order carries a minted task id");

    // Backend reads the order in one process (adopting and persisting the task) ...
    stdout_of(crew(port, &state, "backend", &["inbox"]), "inbox");
    // ... then reports back in a separate process, which restores the task from
    // disk.
    stdout_of(
        crew(
            port,
            &state,
            "backend",
            &["send", "--to", "commander", "on it"],
        ),
        "send",
    );

    let reply_task = task_of_body(port, "on it").expect("backend's reply is on the stream");
    assert_eq!(
        reply_task, order_task,
        "the shim send stamps the task from the earlier order, across processes",
    );
}

#[test]
fn the_shim_inbox_shows_only_new_messages_across_calls() {
    let port = start_broker();
    let state = state_dir("cursor");

    crew(port, &state, "reader", &["register"]);
    crew(port, &state, "sender", &["register"]);

    // A message arrives, then the reader drains its inbox: it sees the message
    // and the persisted cursor advances past it.
    stdout_of(
        crew(
            port,
            &state,
            "sender",
            &["send", "--to", "reader", "first message"],
        ),
        "send first",
    );
    let first = stdout_of(crew(port, &state, "reader", &["inbox"]), "inbox 1");
    assert!(
        first.contains("first message"),
        "the first read shows the new message: {first}",
    );

    // A second read with nothing new is empty, not a reprint of the seen message.
    let repeat = stdout_of(crew(port, &state, "reader", &["inbox"]), "inbox 2");
    assert!(
        !repeat.contains("first message"),
        "a repeat read does not reprint a seen message: {repeat}",
    );
    assert!(
        repeat.contains("No messages"),
        "with nothing new the inbox is empty: {repeat}",
    );

    // A later message shows on the next read, and only that one.
    stdout_of(
        crew(
            port,
            &state,
            "sender",
            &["send", "--to", "reader", "second message"],
        ),
        "send second",
    );
    let third = stdout_of(crew(port, &state, "reader", &["inbox"]), "inbox 3");
    assert!(
        third.contains("second message"),
        "a later message shows on the next read: {third}",
    );
    assert!(
        !third.contains("first message"),
        "and the earlier message is not repeated: {third}",
    );
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
