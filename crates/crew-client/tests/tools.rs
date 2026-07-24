//! End-to-end test of the MCP tools against a real broker (issue #17).
//!
//! It starts a `crewd` instance in-process on an ephemeral loopback port, then
//! drives the synchronous [`Broker`] client the MCP server uses. Together the
//! assertions prove the acceptance: an agent can send, receive its addressed
//! messages (with its own filtered out), and list the roster.
//!
//! The broker runs on a background thread with its own tokio runtime, so the
//! test body stays synchronous and can call the blocking `ureq`-based client
//! directly.

mod common;

use crew_client::Broker;
use crew_core::{Channel, MessageId, RoleId};

/// A broker serving on an ephemeral loopback port, driven over HTTP.
///
/// The serve thread is detached: it lives until the test process exits, which
/// is all the request/response tools need (none hold a long-lived stream open).
struct TestBroker {
    base: String,
}

impl TestBroker {
    /// Starts a broker over a fresh in-memory store on an ephemeral port.
    fn start() -> Self {
        Self {
            base: format!("http://{}", common::start_broker()),
        }
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

    /// Posts a plain note as the General to `channel`, standing in for a human
    /// brief.
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
    // message went to `@backend`, which does not address frontend, so its inbox
    // holds only the broadcast.
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
    let backend_entry = roster
        .roles
        .iter()
        .find(|entry| entry.role == "backend")
        .unwrap();
    assert_eq!(backend_entry.owned_paths, ["api/"]);
    assert_eq!(backend_entry.liveness, "working");
    assert!(
        roster.roles.iter().any(|entry| entry.role == "frontend"),
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

    // The General briefs the crew without naming a target: the shared rule resolves
    // it to the commander's channel, and only the commander receives it.
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

/// Every message event on the broker, oldest first, as raw JSON.
fn message_events(base: &str) -> Vec<serde_json::Value> {
    let text = ureq::get(&format!("{base}/history?kind=message"))
        .call()
        .unwrap()
        .into_string()
        .unwrap();
    let history: serde_json::Value = serde_json::from_str(&text).unwrap();
    history["events"].as_array().unwrap().clone()
}

#[test]
fn an_order_mints_a_task_the_assignee_adopts_and_stamps_on_its_work() {
    // Issue #132: the commander's order mints a TaskId and stamps it on the order;
    // the assignee adopts it from its inbox, so its next message carries the same
    // id. The order and the work done under it correlate to one task.
    let broker = TestBroker::start();
    broker.register("commander", &[]);
    broker.register("backend", &["api/"]);

    let commander = broker.client("commander");
    let mut backend = broker.client("backend");

    // A commander broadcast before any order carries no task: work outside a task
    // correlates to nothing.
    commander.send(None, Some("all-units"), "stand by").unwrap();

    // The commander orders backend: this mints the task and stamps it on the order.
    commander
        .order(
            "backend",
            "build login",
            "POST /login",
            &["api/".to_owned()],
            "tests green",
            "",
        )
        .unwrap();

    let events = message_events(&broker.base);
    let order = events
        .iter()
        .find(|event| event["kind"]["data"]["kind"] == "order")
        .expect("the order is on the stream");
    let task = order["task"]
        .as_str()
        .expect("the order carries the minted task id")
        .to_owned();
    let stand_by = events
        .iter()
        .find(|event| event["kind"]["data"]["body"] == "stand by")
        .unwrap();
    assert!(
        stand_by["task"].is_null(),
        "a message sent outside any task carries no id",
    );

    // Backend reads the order (adopting its task), then reports back.
    let inbox = backend.inbox().unwrap();
    assert!(
        inbox.iter().any(|item| item.kind == "order"),
        "backend receives the order",
    );
    backend.send(Some("commander"), None, "on it").unwrap();

    // Backend's reply carries the very task the order minted, so its work
    // correlates to the assignment.
    let reply = message_events(&broker.base)
        .into_iter()
        .find(|event| event["kind"]["data"]["body"] == "on it")
        .expect("backend's reply is on the stream");
    assert_eq!(
        reply["task"].as_str(),
        Some(task.as_str()),
        "the assignee stamps the adopted task on the messages it sends next",
    );
}

#[test]
fn own_work_shares_one_task_from_the_order_through_claim_submit_and_an_independent_verdict() {
    // Issue #183: the done-gate and work ledger key by the adopted TaskId, not a
    // human title. So a role's order -> claim -> in_progress -> submit chain rides
    // one id with no bookkeeping, and an independent verifier names that same id.
    use crew_core::TaskId;

    let broker = TestBroker::start();
    broker.register("commander", &[]);
    broker.register("backend", &["api/"]);
    broker.register("qa", &["tests/"]);

    let commander = broker.client("commander");
    let mut backend = broker.client("backend");
    let qa = broker.client("qa");

    // The commander orders backend: this mints the task and stamps it on the order.
    commander
        .order(
            "backend",
            "build login",
            "POST /login",
            &[],
            "tests green",
            "",
        )
        .unwrap();

    // Backend reads the order and adopts its task, with no manual id bookkeeping.
    backend.inbox().unwrap();
    let task = backend.task().expect("backend adopted the order's task");

    // Own-work claim and submit reuse the adopted id, so the whole chain shares it.
    backend.claim("in_progress", "build login").unwrap();
    backend
        .submit("build login", "tests green", Some("qa"))
        .unwrap();
    assert_eq!(
        backend.task(),
        Some(task),
        "claim and submit reuse the adopted task, minting nothing new",
    );

    // The ledger holds the work under the adopted id, titled for display.
    let ledger = qa.ledger().unwrap();
    let claim = ledger
        .iter()
        .find(|item| item.task == task.to_string())
        .expect("the ledger holds the claim keyed by the adopted id");
    assert_eq!(claim.owner, "backend");
    assert_eq!(claim.state, "in_progress");
    assert_eq!(claim.title, "build login", "titled for display");

    // The verifier reads the gate, names the task by its id, and passes it.
    let gate = qa.gate().unwrap();
    let submitted = gate
        .tasks
        .iter()
        .find(|t| t.task == task.to_string())
        .expect("the gate holds the submission keyed by the adopted id");
    assert_eq!(submitted.verdict, "submitted");
    assert_eq!(submitted.title, "build login", "titled for display");
    let task_id: TaskId = submitted
        .task
        .parse()
        .expect("the gate exposes a parseable id");
    qa.verdict(task_id, true, "").unwrap();

    // The task is done end to end: keyed by one id from the order to the pass.
    let gate = qa.gate().unwrap();
    let passed = gate
        .tasks
        .iter()
        .find(|t| t.task == task.to_string())
        .expect("the task is still in the gate");
    assert_eq!(
        passed.verdict, "passed",
        "an independent pass marks it done"
    );
    assert_eq!(passed.verifier.as_deref(), Some("qa"));
}

#[test]
fn a_commander_steers_a_specialist_in_band_with_redirect_and_belay() {
    // Issue #190: the commander steers a working specialist through its own tools,
    // not only the General over the CLI. A `redirect` nudges without stopping; a
    // `belay` halts and re-tasks. Both arrive typed and flagged to honor at once.
    let broker = TestBroker::start();
    broker.register("commander", &[]);
    broker.register("backend", &["api/"]);

    let commander = broker.client("commander");
    let mut backend = broker.client("backend");

    // The commander redirects backend mid-task: it arrives as a typed `redirect`,
    // flagged as a directive the specialist honors at its next tool boundary.
    commander
        .redirect(
            "backend",
            "prioritize the login flow before the profile page",
        )
        .unwrap();
    let inbox = backend.inbox().unwrap();
    assert_eq!(inbox.len(), 1, "backend receives the redirect");
    let redirect = &inbox[0];
    assert_eq!(redirect.from, "commander");
    assert_eq!(redirect.channel, "@backend");
    assert_eq!(
        redirect.kind, "redirect",
        "it arrives as a typed redirect, not a note"
    );
    assert!(
        redirect.directive,
        "a redirect is flagged as a directive to honor at once"
    );
    assert_eq!(
        redirect.body,
        "prioritize the login flow before the profile page"
    );

    // The commander then belays backend: halt the current work and take a new
    // order.
    commander
        .belay("backend", "stop the refactor, patch the auth regression")
        .unwrap();
    let inbox = backend.inbox().unwrap();
    assert_eq!(inbox.len(), 1, "backend receives the belay");
    let belay = &inbox[0];
    assert_eq!(belay.kind, "belay", "it arrives as a typed belay");
    assert!(belay.directive, "a belay is flagged as a directive");
    assert_eq!(belay.body, "stop the refactor, patch the auth regression");

    // The commander never sees its own directives echoed back.
    assert!(
        broker.client("commander").inbox().unwrap().is_empty(),
        "the commander does not receive its own steering directives",
    );
}

#[test]
fn an_agent_asks_a_typed_question_and_receives_a_typed_answer() {
    let broker = TestBroker::start();
    broker.register("backend", &["api/"]);
    broker.register("frontend", &["web/"]);

    let mut backend = broker.client("backend");
    let mut frontend = broker.client("frontend");

    // backend asks frontend a typed question, the kind stall detection keys on.
    backend
        .ask(
            Some("frontend"),
            None,
            "which auth library?",
            &["jwt".to_owned(), "sessions".to_owned()],
        )
        .unwrap();

    // frontend reads it as a `question` (not a note), carrying an id to answer.
    let inbox = frontend.inbox().unwrap();
    assert_eq!(inbox.len(), 1);
    let question = &inbox[0];
    assert_eq!(
        question.kind, "question",
        "it arrives as a typed question, not a note"
    );
    assert_eq!(question.from, "backend");
    assert_eq!(question.body, "which auth library?");
    assert!(
        !question.id.is_empty(),
        "the question carries an id to reply to"
    );

    // frontend answers, naming the question id; the answer threads back to backend.
    frontend
        .answer(Some("backend"), None, "use jwt", &question.id)
        .unwrap();
    let reply = backend.inbox().unwrap();
    assert_eq!(reply.len(), 1);
    assert_eq!(reply[0].kind, "answer", "it arrives as a typed answer");
    assert_eq!(reply[0].from, "frontend");
    assert_eq!(reply[0].body, "use jwt");
}

#[test]
fn a_typed_question_lands_on_the_stream_for_stall_detection() {
    // The point of typed questions: the coordination-stall detector (issue #48)
    // keys on `question` events. Prove one lands on the broker as a question kind.
    let broker = TestBroker::start();
    broker.register("backend", &["api/"]);
    let backend = broker.client("backend");

    backend
        .ask(Some("frontend"), None, "what token TTL?", &[])
        .unwrap();

    let text = ureq::get(&format!("{}/history?kind=message", broker.base))
        .call()
        .unwrap()
        .into_string()
        .unwrap();
    let history: serde_json::Value = serde_json::from_str(&text).unwrap();
    let events = history["events"].as_array().unwrap();
    assert!(
        events
            .iter()
            .any(|event| event["kind"]["data"]["kind"] == "question"),
        "the question is on the stream as a `question` event: {events:?}"
    );
}

#[test]
fn a_seeded_read_cursor_shows_only_messages_past_it() {
    // The shim persists this cursor so a per-call process resumes where the last
    // one left off, instead of reprinting the whole inbox (issue #130).
    let broker = TestBroker::start();
    broker.register("reader", &[]);
    broker.register("sender", &[]);

    let sender = broker.client("sender");
    sender.send(Some("reader"), None, "first").unwrap();

    // A fresh client (a new shim process) reads from the start: it sees the
    // message and reports how far it read.
    let mut first = broker.client("reader");
    assert_eq!(
        first.inbox().unwrap().len(),
        1,
        "the first read sees the message"
    );
    let cursor = first.cursor();
    assert!(
        cursor.is_some(),
        "reading advanced the cursor past the message"
    );

    // A second message arrives after that read.
    sender.send(Some("reader"), None, "second").unwrap();

    // The next process seeds from the saved cursor: it sees only the new message,
    // not the one already read.
    let mut resumed = broker.client("reader").with_cursor(cursor);
    let items = resumed.inbox().unwrap();
    assert_eq!(items.len(), 1, "only the message past the cursor");
    assert_eq!(items[0].body, "second");

    // Seeding at the current end shows nothing new.
    let mut caught_up = broker.client("reader").with_cursor(resumed.cursor());
    assert!(
        caught_up.inbox().unwrap().is_empty(),
        "nothing is past the end of the log",
    );
}

#[test]
fn an_unrecognized_cursor_replays_rather_than_skipping() {
    // Keying the cursor on the message id, not a count, makes it survive a broker
    // log reset (issue #160). A cursor naming a message the log no longer holds (a
    // fresh, shorter log after the state dir is reset) falls back to delivering the
    // whole log, rather than silently skipping genuinely new messages until the log
    // grows past a stale count.
    let broker = TestBroker::start();
    broker.register("reader", &[]);
    broker.register("sender", &[]);
    broker
        .client("sender")
        .send(Some("reader"), None, "after reset")
        .unwrap();

    // A cursor from a prior, since-reset log: its id is absent from this one.
    let stale = MessageId::new();
    let mut reader = broker.client("reader").with_cursor(Some(stale));
    let items = reader.inbox().unwrap();
    assert_eq!(
        items.len(),
        1,
        "an unknown cursor replays the log instead of skipping new messages"
    );
    assert_eq!(items[0].body, "after reset");
}
