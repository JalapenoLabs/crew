//! The supervisor threads the task id onto lifecycle events (issue #29).
//!
//! It starts a real `crewd` in-process on an ephemeral loopback port, drives a
//! `RosterClient` carrying a task context, and reads the broker's history back to
//! prove every lifecycle event the supervisor produced correlates to that task.

use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::thread;
use std::time::{Duration, Instant};

use crew_broker::{AppState, Config};
use crew_core::{RoleId, TaskId};
use crew_supervisor::{Liveness, RosterClient};

/// Starts a broker over a fresh in-memory store, returning the base URL it serves on.
fn start_broker() -> String {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).unwrap();
    listener.set_nonblocking(true).unwrap();
    let addr: SocketAddr = listener.local_addr().unwrap();
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

    let base = format!("http://{addr}");
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if ureq::get(&format!("{base}/health")).call().is_ok() {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    base
}

/// The task id stamped on each lifecycle event for `role`, oldest first.
fn lifecycle_tasks(base: &str, role: &str) -> Vec<Option<String>> {
    let text = ureq::get(&format!("{base}/history?kind=lifecycle"))
        .call()
        .unwrap()
        .into_string()
        .unwrap();
    let value: serde_json::Value = serde_json::from_str(&text).unwrap();
    value["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|event| event["from"]["id"] == role)
        .map(|event| event["task"].as_str().map(str::to_owned))
        .collect()
}

#[test]
fn the_supervisor_threads_the_task_onto_lifecycle_events() {
    let base = start_broker();
    let task = TaskId::new();
    let backend = RoleId::new("backend");

    // A supervisor working a task threads it onto every transition it publishes.
    let roster = RosterClient::new(base.clone()).with_task(task);
    roster
        .register(&backend, &["api/".to_owned()])
        .expect("the role registers");
    roster
        .mark(&backend, Liveness::Idle)
        .expect("the role is marked idle");

    // Both the `started` and the `idle` lifecycle events correlate to the task.
    let tasks = lifecycle_tasks(&base, "backend");
    assert_eq!(
        tasks,
        vec![Some(task.to_string()), Some(task.to_string())],
        "each lifecycle event carries the threaded task id",
    );

    // A client with no task context threads none, so its events correlate to nothing.
    let untasked = RosterClient::new(base.clone());
    untasked
        .register(&RoleId::new("frontend"), &["web/".to_owned()])
        .expect("the untasked role registers");
    assert_eq!(
        lifecycle_tasks(&base, "frontend"),
        vec![None],
        "an event outside a task carries no task id",
    );
}
