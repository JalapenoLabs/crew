//! End-to-end integration suite for the broker (issue #16).
//!
//! Each test starts a real `crewd` instance in-process, serving on an ephemeral
//! loopback port, and drives it over HTTP and Server-Sent Events with a real
//! client. Together they prove the Phase 1 transport end to end so later phases
//! build on solid ground: post then receive, self-echo suppression, channel
//! routing (direct / pair / all-units), history filters and pagination, and
//! restart replay.
//!
//! The per-module unit tests exercise each handler in isolation (via
//! `oneshot`); this suite exercises the assembled service over a real socket.

use std::{
    net::Ipv4Addr,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use crew_broker::{AppState, Config, LogStore};
use reqwest::StatusCode;
use serde_json::{json, Value};
use tokio::{net::TcpListener, task::JoinHandle};

/// How long to wait for an event that should arrive (generous; it arrives in
/// ms).
const EXPECTED: Duration = Duration::from_secs(2);

/// How long to wait before concluding an event will not arrive (a suppression
/// check).
const ABSENT: Duration = Duration::from_millis(300);

/// A broker serving on an ephemeral loopback port, with a client and shutdown
/// handle.
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
        Inbox::new(response)
    }

    /// Subscribes to `role`'s inbox resuming after `last_event_id`, the way a
    /// dropped client reconnects to replay the events it missed (issue #10).
    async fn inbox_resume(&self, role: &str, last_event_id: &str) -> Inbox {
        let response = self
            .client
            .get(self.url(&format!("/inbox?role={role}")))
            .header("Last-Event-ID", last_event_id)
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "the resumed inbox for {role} should open"
        );
        Inbox::new(response)
    }

    /// Subscribes to a role's activity timeline SSE stream, live from now.
    async fn activity(&self, agent: &str) -> Inbox {
        let response = self
            .client
            .get(self.url(&format!("/activity?agent={agent}")))
            .send()
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::OK,
            "activity for {agent} should open"
        );
        Inbox::new(response)
    }

    /// Subscribes to the aggregate live stream with a query (e.g.
    /// `?role=backend`), or the whole firehose when the query is empty.
    async fn stream(&self, query: &str) -> Inbox {
        let response = self
            .client
            .get(self.url(&format!("/stream{query}")))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "the stream should open");
        Inbox::new(response)
    }

    /// Registers `role` on the roster (`POST /roster`) with an optional
    /// liveness, so the transition publishes its lifecycle event.
    async fn register(&self, role: &str, liveness: Option<&str>) {
        let mut body = json!({ "role": role });
        if let Some(liveness) = liveness {
            body["liveness"] = json!(liveness);
        }
        let response = self
            .client
            .post(self.url("/roster"))
            .json(&body)
            .send()
            .await
            .unwrap();
        assert!(
            response.status().is_success(),
            "register {role} should succeed"
        );
    }
}

/// A live Server-Sent-Events subscription, reading one event at a time.
struct Inbox {
    response: reqwest::Response,
    buffer: String,
    /// The SSE `id` of the most recent event [`recv`](Inbox::recv) returned:
    /// the `Last-Event-ID` a reconnect resumes from. `None` before the
    /// first event.
    last_id: Option<String>,
}

impl Inbox {
    /// Wraps an open SSE response, ready to read events from.
    fn new(response: reqwest::Response) -> Self {
        Self {
            response,
            buffer: String::new(),
            last_id: None,
        }
    }

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

    /// The SSE `id` of the last event [`recv`](Inbox::recv) delivered, for a
    /// reconnect to resume from as its `Last-Event-ID`.
    fn last_id(&self) -> Option<&str> {
        self.last_id.as_deref()
    }

    /// Drains complete lines from the buffer, tracking each event's `id:` and
    /// returning the first `data:` event.
    fn take_event(&mut self) -> Option<Value> {
        while let Some(newline) = self.buffer.find('\n') {
            let line: String = self.buffer.drain(..=newline).collect();
            if let Some(id) = line.strip_prefix("id:") {
                self.last_id = Some(id.trim().to_owned());
            } else if let Some(data) = line.strip_prefix("data:") {
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

/// The lifecycle transition a lifecycle event carries (started / idle / died /
/// ...).
fn lifecycle_of(event: &Value) -> &str {
    event["kind"]["data"].as_str().unwrap_or_default()
}

/// The live agent count reported by a `GET /roster` body.
fn live_count(roster: &Value) -> u64 {
    roster["count"]["live"].as_u64().unwrap()
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
async fn a_subscriber_sees_the_live_count_update_as_agents_transition() {
    // The live agent count (issue #32): a subscriber on the stream sees a
    // roster-change event on every transition, and `GET /roster` reports the
    // current live count.
    let broker = TestBroker::in_memory().await;
    let mut stream = broker.stream("").await;

    // Two agents start: each emits `started`, and the live count climbs to 2.
    broker.register("backend", None).await;
    assert_eq!(
        lifecycle_of(&stream.recv(EXPECTED).await.unwrap()),
        "started"
    );
    assert_eq!(live_count(&broker.get_json("/roster").await), 1);

    broker.register("qa", None).await;
    assert_eq!(
        lifecycle_of(&stream.recv(EXPECTED).await.unwrap()),
        "started"
    );
    assert_eq!(live_count(&broker.get_json("/roster").await), 2);

    // backend idles: an `idle` event, still live, so the count holds at 2.
    broker.register("backend", Some("idle")).await;
    assert_eq!(lifecycle_of(&stream.recv(EXPECTED).await.unwrap()), "idle");
    let roster = broker.get_json("/roster").await;
    assert_eq!(live_count(&roster), 2, "an idle agent is still live");
    assert_eq!(roster["count"]["idle"], 1);

    // backend dies: a `died` event, and the count drops to 1.
    broker.register("backend", Some("dead")).await;
    assert_eq!(lifecycle_of(&stream.recv(EXPECTED).await.unwrap()), "died");
    assert_eq!(live_count(&broker.get_json("/roster").await), 1);

    // qa stops: a `stopped` event, and the count drops to 0.
    broker.register("qa", Some("stopped")).await;
    assert_eq!(
        lifecycle_of(&stream.recv(EXPECTED).await.unwrap()),
        "stopped"
    );
    let roster = broker.get_json("/roster").await;
    assert_eq!(live_count(&roster), 0, "no agents remain live");
    assert_eq!(
        roster["roles"].as_array().unwrap().len(),
        2,
        "the dead and stopped roles are still listed, just not counted live",
    );

    broker.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_consumer_renders_the_unit_from_the_stream_alone() {
    // The external-consumer contract (issue #33, see docs/stream-contract.md): from
    // the one live stream a viz like Runewood renders agents, messages, and the
    // live count, with no crew-specific capture. Each event carries the whole
    // envelope it needs.
    let broker = TestBroker::in_memory().await;
    let mut stream = broker.stream("").await; // the whole firehose, before anything happens

    // An agent appearing is a `started` lifecycle event: a consumer spawns an
    // entity for the role, and every event carries its typed, timestamped,
    // addressed envelope.
    broker.register("backend", None).await;
    let started = stream.recv(EXPECTED).await.expect("a live event arrives");
    assert!(
        started["ts"].is_string(),
        "timestamped: every event has `ts`"
    );
    assert_eq!(
        started["from"]["id"], "backend",
        "addressed: who it is about"
    );
    assert_eq!(started["channel"], "all-units");
    assert_eq!(started["kind"]["kind"], "lifecycle", "typed: the kind tag");
    assert_eq!(started["kind"]["data"], "started", "the transition");

    broker.register("frontend", None).await;
    assert_eq!(
        lifecycle_of(&stream.recv(EXPECTED).await.unwrap()),
        "started"
    );

    // A message renders as a particle between agents: source `from`, destination
    // `channel`, and a typed intent, all on the one event.
    broker
        .post_note("@backend", role("frontend"), "the API is ready")
        .await;
    let message = stream.recv(EXPECTED).await.expect("the message arrives");
    assert_eq!(from_of(&message), "frontend", "the source agent");
    assert_eq!(channel_of(&message), "@backend", "the destination");
    assert_eq!(message["kind"]["kind"], "message");
    assert_eq!(message["kind"]["data"]["kind"], "note", "the typed intent");
    assert_eq!(body_of(&message), "the API is ready");

    // A transition parks an agent; the consumer keeps its live count from these.
    broker.register("backend", Some("idle")).await;
    assert_eq!(lifecycle_of(&stream.recv(EXPECTED).await.unwrap()), "idle");

    // The live count is also a snapshot on /roster, and it agrees with the stream:
    // both agents present, one idle.
    let roster = broker.get_json("/roster").await;
    assert_eq!(roster["count"]["live"], 2, "both agents are live");
    assert_eq!(roster["count"]["idle"], 1, "one is parked");

    broker.stop().await;
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
async fn a_role_s_timeline_is_retrievable_as_history_and_live() {
    // The per-agent activity log (issue #30): one role's full timeline, retrievable
    // both as history and as a live SSE stream.
    let broker = TestBroker::in_memory().await;

    // A live subscription to backend's timeline, open before anything is posted.
    let mut timeline = broker.activity("backend").await;

    // What backend does and receives, plus a message between others it should not
    // see.
    broker
        .post_note("@frontend", role("backend"), "sent") // backend sent
        .await;
    broker
        .post_note("@backend", role("frontend"), "received") // backend received
        .await;
    broker
        .post_note("@qa", role("frontend"), "not backend's") // between others
        .await;

    // Live: the timeline carries backend's own message and the one it received, in
    // order, but never a message between other roles.
    let first = timeline.recv(EXPECTED).await.expect("its own message");
    assert_eq!(body_of(&first), "sent");
    assert_eq!(from_of(&first), "backend");
    let second = timeline.recv(EXPECTED).await.expect("the received message");
    assert_eq!(body_of(&second), "received");
    assert!(
        timeline.recv(ABSENT).await.is_none(),
        "a message between other roles is not on backend's timeline",
    );

    // History: the same timeline is retrievable after the fact, and it differs from
    // the sender-only `role` filter (which omits what backend received).
    let history = broker.get_json("/history?agent=backend").await;
    assert_eq!(bodies(&history), vec!["sent", "received"]);
    let sent_only = broker.get_json("/history?role=backend").await;
    assert_eq!(bodies(&sent_only), vec!["sent"]);

    broker.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_aggregate_view_is_filterable_live_and_historically() {
    // The aggregate activity log (issue #31): the whole unit's stream, the same
    // filter applied live and historically so the two views agree.
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

    // Historically: the same filter over `/history` returns the same set,
    // time-ordered.
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

    // A peer receives it; the sender does not (self-echo is filtered at the
    // source).
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

    // The tail is bounded to the requested size, not the whole 202-event log (the
    // 200 notes, the order, and the ledger claim the order auto-seeds, issue
    // #184).
    let tail = view["tail"].as_array().unwrap();
    assert_eq!(
        tail.len(),
        10,
        "the joiner reads a bounded tail, not the full log"
    );

    // The rest is compacted into aggregates that stand in for the older events: the
    // 200 notes plus the order and its auto-seeded ledger claim (issue #184),
    // less the tail.
    assert_eq!(view["summary"]["event_count"], backlog + 2 - 10);
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_dropped_inbox_resumes_from_last_event_id_without_loss() {
    // The inbox reconnect-resume path (issue #10): a dropped `/inbox` reconnects
    // with `Last-Event-ID` set to the last event it saw and the broker replays
    // exactly the events it missed, in order, with no gap and no duplicate.
    let broker = TestBroker::in_memory().await;

    // Subscribe live, then receive the first two messages, tracking the last id.
    let mut inbox = broker.inbox("backend").await;
    broker.post_note("@backend", general(), "one").await;
    broker.post_note("@backend", general(), "two").await;

    let first = inbox.recv(EXPECTED).await.expect("the first arrives");
    assert_eq!(body_of(&first), "one");
    let second = inbox.recv(EXPECTED).await.expect("the second arrives");
    assert_eq!(body_of(&second), "two");
    let last_seen = inbox
        .last_id()
        .expect("the delivered event carried an id")
        .to_owned();

    // Drop the connection, standing in for a network blip.
    drop(inbox);

    // Two more arrive while the client is disconnected; they must not be lost.
    broker.post_note("@backend", general(), "three").await;
    broker.post_note("@backend", general(), "four").await;

    // Reconnect from the last id: the two missed events replay first, in order.
    // The first delivered is `three`, not `one`/`two`, so nothing already seen is
    // re-sent (no duplicate) and nothing between is skipped (no gap).
    let mut resumed = broker.inbox_resume("backend", &last_seen).await;
    let replayed_three = resumed
        .recv(EXPECTED)
        .await
        .expect("the first missed replays");
    assert_eq!(
        body_of(&replayed_three),
        "three",
        "resume replays the first missed event, not one already delivered",
    );
    let replayed_four = resumed
        .recv(EXPECTED)
        .await
        .expect("the second missed replays");
    assert_eq!(
        body_of(&replayed_four),
        "four",
        "resume replays the second missed event, in order",
    );

    // After the backlog, live delivery continues seamlessly on the same stream.
    broker.post_note("@backend", general(), "five").await;
    let live = resumed
        .recv(EXPECTED)
        .await
        .expect("live delivery resumes after the replayed backlog");
    assert_eq!(
        body_of(&live),
        "five",
        "the resumed stream carries new events with no further gap",
    );

    broker.stop().await;
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
