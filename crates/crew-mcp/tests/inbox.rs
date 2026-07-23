//! End-to-end test of the `crew_inbox` push path (issue #76).
//!
//! It starts a `crewd` instance in-process, then drives the broker client the
//! way the MCP server does: a role subscribes to its live inbox, and reads
//! drain the buffered batch the background SSE stream delivers rather than
//! refetching the whole history. The pull-based read stays covered as the
//! fallback for a runtime without streaming.
//!
//! The broker runs on a background thread with its own tokio runtime, so the
//! test body stays synchronous and calls the blocking `ureq`-based client
//! directly.

use std::{
    net::{Ipv4Addr, SocketAddr, TcpListener},
    thread,
    time::{Duration, Instant},
};

use crew_broker::{AppState, Config};
use crew_core::RoleId;
use crew_mcp::Broker;

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

/// A client for `role`, using `commander` as its default addressee.
fn client(addr: SocketAddr, role: &str) -> Broker {
    Broker::new(
        format!("http://{addr}"),
        RoleId::new(role),
        RoleId::new("commander"),
    )
}

/// Drains `broker`'s inbox until a message body equals `body`, or a deadline
/// passes.
///
/// Draining is destructive, so the caller must ask for one message at a time;
/// each poll returns only what arrived since the last one.
fn wait_for_message(broker: &mut Broker, body: &str) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        let batch = broker.inbox().expect("the inbox reads");
        if batch.iter().any(|item| item.body == body) {
            return true;
        }
        thread::sleep(Duration::from_millis(20));
    }
    false
}

#[test]
fn a_subscribed_inbox_seeds_the_backlog_then_pushes_live_messages() {
    let addr = start_broker();
    let commander = client(addr, "commander");
    let mut backend = client(addr, "backend");
    backend.register(&["api/".to_owned()]).unwrap();

    // A message addressed to backend before it subscribes: the backlog a fresh
    // stream does not replay, so the subscription must seed it from history.
    commander
        .send(Some("backend"), None, "before you subscribed")
        .unwrap();

    backend.subscribe().expect("the inbox stream opens");

    // The first read returns the seeded backlog, exactly once (no duplicate from
    // the stream's overlap window).
    let first = backend.inbox().unwrap();
    let backlog: Vec<&str> = first
        .iter()
        .filter(|item| item.body == "before you subscribed")
        .map(|item| item.body.as_str())
        .collect();
    assert_eq!(
        backlog,
        ["before you subscribed"],
        "the backlog is seeded once"
    );

    // A read with nothing new returns an empty batch, not a refetch of the backlog.
    assert!(
        backend.inbox().unwrap().is_empty(),
        "a drained inbox is empty, and the backlog is not refetched"
    );

    // A live message arrives over the stream and drains on the next read.
    commander
        .send(Some("backend"), None, "sent after you subscribed")
        .unwrap();
    assert!(
        wait_for_message(&mut backend, "sent after you subscribed"),
        "a live message is pushed to the subscribed inbox"
    );
}

#[test]
fn a_subscribed_inbox_drops_the_roles_own_messages() {
    let addr = start_broker();
    let mut backend = client(addr, "backend");
    backend.register(&["api/".to_owned()]).unwrap();
    backend.subscribe().expect("the inbox stream opens");

    // Backend broadcasts to all-units: its own message must never come back to it.
    backend
        .send(None, Some("all-units"), "note to the unit")
        .unwrap();

    // Give the stream a moment; the self-message must not appear.
    thread::sleep(Duration::from_millis(300));
    let batch = backend.inbox().unwrap();
    assert!(
        batch.iter().all(|item| item.body != "note to the unit"),
        "a role never receives its own message: {batch:?}"
    );
}

#[test]
fn the_pull_fallback_reads_the_inbox_without_a_subscription() {
    let addr = start_broker();
    let commander = client(addr, "commander");
    let mut backend = client(addr, "backend");
    backend.register(&["api/".to_owned()]).unwrap();

    // No subscribe(): the client stays on the pull-based history read.
    commander
        .send(Some("backend"), None, "pulled from history")
        .unwrap();

    let batch = backend.inbox().unwrap();
    assert!(
        batch.iter().any(|item| item.body == "pulled from history"),
        "the pull fallback returns the addressed message: {batch:?}"
    );
    // The cursor advanced: a second read returns nothing new.
    assert!(
        backend.inbox().unwrap().is_empty(),
        "the pull cursor advances"
    );
}
