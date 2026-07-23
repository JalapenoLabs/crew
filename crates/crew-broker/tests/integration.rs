//! End-to-end integration suite for the broker (issue #16).
//!
//! Each test starts a real `crewd` instance in-process, serving on an ephemeral
//! loopback port, and drives it over HTTP and Server-Sent Events with a real client.
//! Together they prove the Phase 1 transport end to end so later phases build on
//! solid ground: post then receive, self-echo suppression, channel routing (direct /
//! pair / all-units), history filters and pagination, and restart replay.
//!
//! The per-module unit tests exercise each handler in isolation (via `oneshot`);
//! this suite exercises the assembled service over a real socket.

use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crew_broker::{AppState, Config, LogStore};
use reqwest::StatusCode;
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

/// How long to wait for an event that should arrive (generous; it arrives in ms).
const EXPECTED: Duration = Duration::from_secs(2);

/// How long to wait before concluding an event will not arrive (a suppression check).
const ABSENT: Duration = Duration::from_millis(300);

/// A broker serving on an ephemeral loopback port, with a client and shutdown handle.
struct TestBroker {
    base: String,
    client: reqwest::Client,
    server: JoinHandle<()>,
}

impl TestBroker {
    /// Starts a broker over the given state on a fresh ephemeral port.
    async fn start(state: AppState) -> Self {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let addr = listener.local_addr().unwrap();
        // Serve with a never-resolving shutdown; `stop` aborts the task, so an open
        // SSE stream never stalls a graceful drain.
        let server = tokio::spawn(async move {
            let _ = crew_broker::serve(listener, state, std::future::pending::<()>()).await;
        });
        Self {
            base: format!("http://{addr}"),
            client: reqwest::Client::new(),
            server,
        }
    }

    /// Starts a broker over the default in-memory store.
    async fn in_memory() -> Self {
        Self::start(AppState::new(Config::default())).await
    }

    /// Stops the broker, aborting its serve task.
    async fn stop(self) {
        self.server.abort();
        let _ = self.server.await;
    }

    fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base)
    }

    /// Posts a JSON message body to `channel`, returning the response.
    async fn post(&self, channel: &str, body: Value) -> reqwest::Response {
        self.client
            .post(self.url(&format!("/channels/{channel}/messages")))
            .json(&body)
            .send()
            .await
            .unwrap()
    }

    /// Posts a note from `from` to `channel`, asserting it is accepted.
    async fn post_note(&self, channel: &str, from: Value, text: &str) {
        let response = self
            .post(
                channel,
                json!({ "from": from, "kind": "note", "body": text }),
            )
            .await;
        assert_eq!(
            response.status(),
            StatusCode::CREATED,
            "post `{text}` should succeed"
        );
    }

    /// Reads a JSON endpoint.
    async fn get_json(&self, path: &str) -> Value {
        self.client
            .get(self.url(path))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap()
    }

    /// Subscribes to a role's inbox SSE stream, live from now.
    async fn inbox(&self, role: &str) -> Inbox {
        let response = self
            .client
            .get(self.url(&format!("/inbox?role={role}")))
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "inbox for {role} should open"
        );
        Inbox {
            response,
            buffer: String::new(),
        }
    }

    /// Subscribes to the aggregate live stream with a query (e.g. `?role=backend`).
    async fn stream(&self, query: &str) -> Inbox {
        let response = self
            .client
            .get(self.url(&format!("/stream{query}")))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "the stream should open");
        Inbox {
            response,
            buffer: String::new(),
        }
    }
}

/// A live Server-Sent-Events subscription, reading one event at a time.
struct Inbox {
    response: reqwest::Response,
    buffer: String,
}

impl Inbox {
    /// The next event, or `None` if none arrives within `within`.
    async fn recv(&mut self, within: Duration) -> Option<Value> {
        loop {
            if let Some(event) = self.take_event() {
                return Some(event);
            }
            match tokio::time::timeout(within, self.response.chunk()).await {
                Ok(Ok(Some(chunk))) => self.buffer.push_str(&String::from_utf8_lossy(&chunk)),
                // Timeout, stream end, or a read error: no event.
                _ => return None,
            }
        }
    }

    /// Drains complete lines from the buffer, returning the first `data:` event.
    fn take_event(&mut self) -> Option<Value> {
        while let Some(newline) = self.buffer.find('\n') {
            let line: String = self.buffer.drain(..=newline).collect();
            if let Some(data) = line.strip_prefix("data:") {
                if let Ok(event) = serde_json::from_str::<Value>(data.trim()) {
                    return Some(event);
                }
            }
        }
        None
    }
}

fn role(id: &str) -> Value {
    json!({ "kind": "role", "id": id })
}

fn general() -> Value {
    json!({ "kind": "general" })
}

/// The channel an event was posted to.
fn channel_of(event: &Value) -> &str {
    event["channel"].as_str().unwrap_or_default()
}

/// The message body of an event.
fn body_of(event: &Value) -> &str {
    event["kind"]["data"]["body"].as_str().unwrap_or_default()
}

/// The sender of an event: a role's id, or `general`.
fn from_of(event: &Value) -> &str {
    event["from"]["id"].as_str().unwrap_or("general")
}

/// The message bodies of a `GET /history` (or page) response.
fn bodies(page: &Value) -> Vec<String> {
    page["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|event| body_of(event).to_owned())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_posted_message_is_received_live_and_appears_in_history() {
    let broker = TestBroker::in_memory().await;
    // A subscriber on backend's inbox, live before the post.
    let mut backend = broker.inbox("backend").await;

    broker
        .post_note("@backend", role("frontend"), "the API is ready")
        .await;

    // Delivered live over SSE...
    let event = backend
        .recv(EXPECTED)
        .await
        .expect("backend receives the message");
    assert_eq!(body_of(&event), "the API is ready");
    assert_eq!(from_of(&event), "frontend");
    assert_eq!(channel_of(&event), "@backend");

    // ...and persisted, so history returns it.
    let history = broker.get_json("/history").await;
    assert_eq!(bodies(&history), vec!["the API is ready"]);

    broker.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_aggregate_view_is_filterable_live_and_historically() {
    // The aggregate activity log (issue #31): the whole unit's stream, the same filter
    // applied live and historically so the two views agree.
    let broker = TestBroker::in_memory().await;

    // A filtered live subscription: only backend's events, opened before any post.
    let mut backend_only = broker.stream("?role=backend").await;

    broker.post_note("all-units", role("backend"), "b1").await; // seq 0, backend
    broker.post_note("all-units", role("frontend"), "f1").await; // seq 1, frontend
    broker.post_note("@qa", role("backend"), "b2").await; // seq 2, backend

    // Live: only backend's events arrive, in order; frontend's is filtered out.
    assert_eq!(body_of(&backend_only.recv(EXPECTED).await.unwrap()), "b1");
    assert_eq!(body_of(&backend_only.recv(EXPECTED).await.unwrap()), "b2");
    assert!(
        backend_only.recv(ABSENT).await.is_none(),
        "an event from another role is filtered out of the live stream",
    );

    // Historically: the same filter over `/history` returns the same set, time-ordered.
    let history = broker.get_json("/history?role=backend").await;
    assert_eq!(
        bodies(&history),
        vec!["b1", "b2"],
        "history and the live stream agree under one filter",
    );

    // Unfiltered, the aggregate view is the whole firehose, time-ordered.
    assert_eq!(
        bodies(&broker.get_json("/history").await),
        vec!["b1", "f1", "b2"],
    );

    broker.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_role_never_receives_its_own_message() {
    let broker = TestBroker::in_memory().await;
    let mut backend = broker.inbox("backend").await;
    let mut qa = broker.inbox("qa").await;

    // backend broadcasts to all-units.
    broker
        .post_note("all-units", role("backend"), "team update")
        .await;

    // A peer receives it; the sender does not (self-echo is filtered at the source).
    assert_eq!(body_of(&qa.recv(EXPECTED).await.unwrap()), "team update");
    assert!(
        backend.recv(ABSENT).await.is_none(),
        "backend must not receive its own broadcast"
    );

    broker.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn channels_route_to_direct_pair_and_all_units_members_only() {
    let broker = TestBroker::in_memory().await;
    let mut backend = broker.inbox("backend").await;
    let mut frontend = broker.inbox("frontend").await;
    let mut qa = broker.inbox("qa").await;

    // Direct: `@backend` reaches only backend.
    broker
        .post_note("@backend", general(), "for backend only")
        .await;
    assert_eq!(
        body_of(&backend.recv(EXPECTED).await.unwrap()),
        "for backend only"
    );
    assert!(
        frontend.recv(ABSENT).await.is_none(),
        "direct must not reach frontend"
    );
    assert!(qa.recv(ABSENT).await.is_none(), "direct must not reach qa");

    // Pair: `frontend+backend` reaches those two members, not qa.
    broker
        .post_note("frontend+backend", general(), "pair thread")
        .await;
    assert_eq!(
        body_of(&backend.recv(EXPECTED).await.unwrap()),
        "pair thread"
    );
    assert_eq!(
        body_of(&frontend.recv(EXPECTED).await.unwrap()),
        "pair thread"
    );
    assert!(qa.recv(ABSENT).await.is_none(), "pair must not reach qa");

    // all-units reaches every role (the General is the sender, so no self-echo).
    broker.post_note("all-units", general(), "all hands").await;
    assert_eq!(body_of(&backend.recv(EXPECTED).await.unwrap()), "all hands");
    assert_eq!(
        body_of(&frontend.recv(EXPECTED).await.unwrap()),
        "all hands"
    );
    assert_eq!(body_of(&qa.recv(EXPECTED).await.unwrap()), "all hands");

    broker.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn history_filters_by_role_channel_and_kind_and_paginates() {
    let broker = TestBroker::in_memory().await;
    // A spread of messages across roles and channels, in a known order.
    broker.post_note("all-units", role("backend"), "b1").await;
    broker.post_note("@frontend", role("backend"), "b2").await;
    broker.post_note("all-units", role("frontend"), "f1").await;
    broker.post_note("@backend", role("qa"), "q1").await;

    // Filter by sender.
    let by_role = broker.get_json("/history?role=backend").await;
    assert_eq!(
        bodies(&by_role),
        vec!["b1", "b2"],
        "only backend's messages"
    );

    // Filter by channel (canonical, so member order would not matter).
    let by_channel = broker.get_json("/history?channel=all-units").await;
    assert_eq!(bodies(&by_channel), vec!["b1", "f1"]);

    // Filter by kind: all four are messages.
    let by_kind = broker.get_json("/history?kind=message").await;
    assert_eq!(by_kind["events"].as_array().unwrap().len(), 4);
    // ...and there are no lifecycle events, since nothing registered.
    let lifecycle = broker.get_json("/history?kind=lifecycle").await;
    assert_eq!(lifecycle["events"].as_array().unwrap().len(), 0);

    // Paginate two at a time, following the cursor to the end.
    let mut collected = Vec::new();
    let mut path = "/history?limit=2".to_owned();
    loop {
        let page = broker.get_json(&path).await;
        collected.extend(bodies(&page));
        match page.get("next_cursor").and_then(Value::as_str) {
            Some(cursor) => path = format!("/history?limit=2&after={cursor}"),
            None => break,
        }
    }
    assert_eq!(
        collected,
        vec!["b1", "b2", "f1", "q1"],
        "every event once, time-ordered, across pages"
    );

    broker.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn joining_a_long_conversation_costs_bounded_context() {
    let broker = TestBroker::in_memory().await;

    // An early order handoff, then a long-running conversation on top of it, so the
    // order is old enough to fall behind the summary rather than into the tail.
    broker
        .post(
            "@backend",
            json!({
                "from": role("commander"),
                "kind": "order",
                "title": "Ship the endpoint",
                "scope": "api only",
                "owned_paths": ["api/"],
                "acceptance": "tests green",
                "body": "please build it",
            }),
        )
        .await;
    let backlog = 200;
    for i in 0..backlog {
        broker
            .post_note("all-units", role("backend"), &format!("m{i}"))
            .await;
    }

    // A late joiner asks for the rolling summary with a small tail.
    let view = broker.get_json("/history?summary=true&limit=10").await;

    // The tail is bounded to the requested size, not the whole 201-event log.
    let tail = view["tail"].as_array().unwrap();
    assert_eq!(
        tail.len(),
        10,
        "the joiner reads a bounded tail, not the full log"
    );

    // The rest is compacted into aggregates that stand in for the older events.
    assert_eq!(view["summary"]["event_count"], backlog + 1 - 10);
    assert_eq!(
        view["summary"]["senders"][0]["name"], "backend",
        "the busiest sender leads the tally",
    );
    // The order handoff surfaces in the digest, so a joiner still sees the work.
    let orders = view["summary"]["recent_orders"].as_array().unwrap();
    assert_eq!(orders.len(), 1);
    assert_eq!(orders[0]["title"], "Ship the endpoint");
    assert!(view["summary"]["headline"]
        .as_str()
        .unwrap()
        .contains("summarized"));

    broker.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn events_survive_a_broker_restart_via_log_replay() {
    let dir = TempDir::new();

    // First run: a durable broker over the temp state dir.
    let first = TestBroker::start(durable(dir.path())).await;
    first
        .post_note("all-units", role("backend"), "durable one")
        .await;
    first
        .post_note("@frontend", role("backend"), "durable two")
        .await;
    first.stop().await;

    // Second run: a fresh broker over the same dir replays the log.
    let second = TestBroker::start(durable(dir.path())).await;
    let history = second.get_json("/history").await;
    assert_eq!(
        bodies(&history),
        vec!["durable one", "durable two"],
        "both events survived the restart"
    );
    second.stop().await;
}

/// Application state over a durable [`LogStore`] rooted at `dir`.
fn durable(dir: &Path) -> AppState {
    let store = LogStore::open(dir).expect("open the log store");
    AppState::with_storage(Config::default(), Arc::new(store))
}

/// A unique temp directory that removes itself on drop.
struct TempDir(PathBuf);

impl TempDir {
    fn new() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("crew-it-{}-{unique}", std::process::id()));
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
