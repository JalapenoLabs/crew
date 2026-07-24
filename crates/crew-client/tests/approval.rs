//! Rules of engagement, end to end against a real broker (issue #39).
//!
//! Proves the acceptance: a gated action pauses the role until the General
//! decides, an ungated action proceeds at once, and silence fails closed. The
//! client posts an `approval_request` and blocks polling the stream; a helper
//! plays the General, finding the request and posting the `approval_decision`.

use std::{
    net::{Ipv4Addr, TcpListener},
    thread,
    time::{Duration, Instant},
};

use crew_broker::{AppState, Config};
use crew_client::{ApprovalOutcome, Broker};
use crew_core::{ActionKind, RoleId, RulesOfEngagement};
use serde_json::Value;

/// Starts a broker over a fresh in-memory store, returning its base URL.
fn start_broker() -> String {
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
    base
}

/// A client acting as `role`.
fn client(base: &str, role: &str) -> Broker {
    Broker::new(base.to_owned(), RoleId::new(role), RoleId::new("commander"))
}

/// The message events on the broker's stream.
fn messages(base: &str) -> Vec<Value> {
    let text = ureq::get(&format!("{base}/history?kind=message"))
        .call()
        .unwrap()
        .into_string()
        .unwrap();
    let history: Value = serde_json::from_str(&text).unwrap();
    history["events"].as_array().unwrap().clone()
}

/// Waits for a pending `approval_request` and returns its message id.
fn await_request_id(base: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if let Some(id) = messages(base).iter().find_map(|event| {
            let data = &event["kind"]["data"];
            (data["kind"] == "approval_request").then(|| data["id"].as_str().unwrap().to_owned())
        }) {
            return id;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("no approval request appeared on the stream");
}

/// Plays the General: posts an `approval_decision` for `request_id` to `role`.
fn decide(base: &str, role: &str, request_id: &str, granted: bool, reason: &str) {
    let payload = serde_json::json!({
        "from": { "kind": "general" },
        "kind": "approval_decision",
        "in_reply_to": request_id,
        "granted": granted,
        "body": reason,
    });
    ureq::post(&format!("{base}/channels/@{role}/messages"))
        .set("content-type", "application/json")
        .send_string(&payload.to_string())
        .expect("the broker accepts the decision");
}

/// A specialist's rules: gated on every action.
fn gated() -> RulesOfEngagement {
    crew_core::default_roe_for(false)
}

#[test]
fn an_ungated_action_proceeds_with_no_wait() {
    let base = start_broker();
    let backend = client(&base, "backend");

    // A role that gates nothing: the action proceeds at once, touching no broker.
    let free = RulesOfEngagement::new([], 1_000);
    let outcome = backend
        .request_approval(
            &free,
            ActionKind::Push,
            None,
            "push api/",
            Duration::from_secs(1),
        )
        .unwrap();
    assert_eq!(
        outcome,
        ApprovalOutcome::NotGated,
        "an ungated action proceeds"
    );
    assert!(
        messages(&base).is_empty(),
        "no request was posted for an ungated action"
    );
}

#[test]
fn a_gated_action_blocks_until_the_general_grants_it() {
    let base = start_broker();
    let requester = client(&base, "backend");
    let base_for_general = base.clone();

    // The role requests approval for a gated push and blocks.
    let handle = thread::spawn(move || {
        requester
            .request_approval(
                &gated(),
                ActionKind::Push,
                None,
                "push api/ to origin/main",
                Duration::from_secs(5),
            )
            .unwrap()
    });

    // The General finds the pending request and grants it.
    let request_id = await_request_id(&base_for_general);
    decide(
        &base_for_general,
        "backend",
        &request_id,
        true,
        "looks good",
    );

    let outcome = handle.join().unwrap();
    assert_eq!(
        outcome,
        ApprovalOutcome::Granted,
        "the role proceeds on a grant"
    );
}

#[test]
fn a_gated_action_is_denied_with_the_generals_reason() {
    let base = start_broker();
    let requester = client(&base, "backend");
    let base_for_general = base.clone();

    let handle = thread::spawn(move || {
        requester
            .request_approval(
                &gated(),
                ActionKind::Delete,
                None,
                "delete the release branch",
                Duration::from_secs(5),
            )
            .unwrap()
    });

    let request_id = await_request_id(&base_for_general);
    decide(
        &base_for_general,
        "backend",
        &request_id,
        false,
        "keep the branch for now",
    );

    assert_eq!(
        handle.join().unwrap(),
        ApprovalOutcome::Denied {
            reason: "keep the branch for now".to_owned(),
        },
        "the role gets the denial and its reason",
    );
}

#[test]
fn no_decision_within_the_timeout_fails_closed() {
    let base = start_broker();
    let backend = client(&base, "backend");

    // No General answers: the request times out, and the role must not proceed.
    let outcome = backend
        .request_approval(
            &gated(),
            ActionKind::Merge,
            None,
            "merge the PR",
            Duration::from_millis(300),
        )
        .unwrap();
    assert_eq!(
        outcome,
        ApprovalOutcome::TimedOut,
        "silence fails closed, never proceeds"
    );
}
