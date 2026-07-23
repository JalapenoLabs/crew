//! End-to-end test of the MCP tools against a real broker (issue #17).
//!
//! It starts a `crewd` instance in-process on an ephemeral loopback port, then drives
//! the synchronous [`Broker`] client the MCP server uses. Together the assertions
//! prove the acceptance: an agent can send, receive its addressed messages (with its
//! own filtered out), and list the roster.
//!
//! The broker runs on a background thread with its own tokio runtime, so the test body
//! stays synchronous and can call the blocking `ureq`-based client directly.

use std::net::{Ipv4Addr, TcpListener};
use std::thread;

use crew_broker::{AppState, Config};
use crew_core::{Channel, RoleId};
use crew_mcp::Broker;

/// A broker serving on an ephemeral loopback port, driven over HTTP.
///
/// The serve thread is detached: it lives until the test process exits, which is all
/// the request/response tools need (none hold a long-lived stream open).
struct TestBroker {
    base: String,
}

impl TestBroker {
    /// Starts a broker over a fresh in-memory store on an ephemeral port.
    fn start() -> Self {
        // Bind synchronously so the address is known before the runtime thread starts;
        // hand the socket to tokio inside the thread.
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
        listener.set_nonblocking(true).unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
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
        Self { base }
    }

    /// A client acting as `role`, with `commander` as the topology hub.
    fn client(&self, role: &str) -> Broker {
        Broker::new(
            self.base.clone(),
            RoleId::new(role),
            RoleId::new("commander"),
        )
    }

    /// Registers `role` with the paths it owns, so it appears in the roster.
    fn register(&self, role: &str, owned_paths: &[&str]) {
        let payload = serde_json::json!({ "role": role, "owned_paths": owned_paths });
        ureq::post(&format!("{}/roster", self.base))
            .set("content-type", "application/json")
            .send_string(&payload.to_string())
            .unwrap();
    }

    /// Posts a plain note as the General to `channel`, standing in for a human brief.
    fn general_note(&self, channel: &str, body: &str) {
        let payload = serde_json::json!({
            "from": { "kind": "general" },
            "kind": "note",
            "body": body,
        });
        ureq::post(&format!("{}/channels/{channel}/messages", self.base))
            .set("content-type", "application/json")
            .send_string(&payload.to_string())
            .unwrap();
    }
}

#[test]
fn an_agent_sends_receives_self_filtered_and_lists_the_roster() {
    let broker = TestBroker::start();
    broker.register("backend", &["api/"]);
    broker.register("frontend", &["web/"]);

    let mut backend = broker.client("backend");
    let mut frontend = broker.client("frontend");

    // A teammate direct-messages backend; backend reads it on its inbox.
    frontend
        .send(Some("backend"), None, "please build the login endpoint")
        .unwrap();
    let inbox = backend.inbox().unwrap();
    assert_eq!(
        inbox.len(),
        1,
        "backend receives the message addressed to it"
    );
    let received = &inbox[0];
    assert_eq!(received.from, "frontend");
    assert_eq!(received.channel, "@backend");
    assert_eq!(received.body, "please build the login endpoint");

    // Backend broadcasts to the unit; it must not receive its own message.
    backend
        .send(None, Some("all-units"), "starting on the endpoint")
        .unwrap();
    assert!(
        backend.inbox().unwrap().is_empty(),
        "a role never sees its own message",
    );

    // The broadcast does reach a different teammate. Frontend's own earlier direct
    // message went to `@backend`, which does not address frontend, so its inbox holds
    // only the broadcast.
    let broadcast = frontend.inbox().unwrap();
    assert_eq!(
        broadcast.len(),
        1,
        "frontend receives the all-units broadcast"
    );
    assert_eq!(broadcast[0].channel, "all-units");
    assert_eq!(broadcast[0].from, "backend");

    // The roster lists every registered teammate and the lanes it owns.
    let roster = backend.roster().unwrap();
    let backend_entry = roster.iter().find(|entry| entry.role == "backend").unwrap();
    assert_eq!(backend_entry.owned_paths, ["api/"]);
    assert_eq!(backend_entry.liveness, "working");
    assert!(
        roster.iter().any(|entry| entry.role == "frontend"),
        "the roster lists the other teammate too",
    );
}

#[test]
fn a_brief_defaults_to_the_commander_who_fans_orders_out() {
    // The hub-and-spoke acceptance (issue #27): a brief reaches the commander by
    // default, and the commander issues orders to specialists.
    let broker = TestBroker::start();
    broker.register("commander", &[]);
    broker.register("backend", &["api/"]);

    let mut commander = broker.client("commander");
    let mut backend = broker.client("backend");

    // The General briefs the crew without naming a target: the shared rule resolves it
    // to the commander's channel, and only the commander receives it.
    let default_channel = Channel::resolve(None, None, &RoleId::new("commander"))
        .expect("an unaddressed brief resolves to the commander")
        .name();
    assert_eq!(default_channel.as_str(), "@commander");
    broker.general_note(default_channel.as_str(), "ship the login flow");

    let briefed = commander.inbox().unwrap();
    assert_eq!(briefed.len(), 1, "the commander receives the brief");
    assert_eq!(briefed[0].from, "general");
    assert_eq!(briefed[0].channel, "@commander");
    assert_eq!(briefed[0].body, "ship the login flow");
    assert!(
        backend.inbox().unwrap().is_empty(),
        "a specialist does not receive the General's brief to the commander",
    );

    // The commander fans the work out: it orders the backend a scoped task.
    commander
        .order(
            "backend",
            "build the login endpoint",
            "POST /login only",
            &["api/".to_owned()],
            "tests green, no clippy warnings",
            "coordinate the token shape with frontend",
        )
        .unwrap();

    let ordered = backend.inbox().unwrap();
    assert_eq!(ordered.len(), 1, "the specialist receives the order");
    let order = &ordered[0];
    assert_eq!(order.from, "commander");
    assert_eq!(order.channel, "@backend");
    assert_eq!(order.kind, "order", "it arrives as an order, not a note");
    assert!(
        order.detail.contains("build the login endpoint"),
        "the order carries its title so the specialist reads the task",
    );
    assert!(
        order.detail.contains("acceptance: tests green"),
        "the order carries its acceptance bar",
    );
    assert_eq!(order.body, "coordinate the token shape with frontend");

    // The commander does not receive its own order back.
    assert!(
        commander.inbox().unwrap().is_empty(),
        "the commander never sees its own order echoed",
    );
}
