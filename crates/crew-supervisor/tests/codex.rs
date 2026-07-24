//! Smoke test for the Codex spawn path: a Codex role registers on the roster
//! (issue #162).
//!
//! Two things are proven without a real `codex` process. First, the command the
//! supervisor builds for a `codex` role is the invocation verified against
//! codex-cli 0.145.0: `codex exec --dangerously-bypass-approvals-and-sandbox
//! --skip-git-repo-check <briefing>`. Second, a spawned Codex role registers on
//! a real in-process broker's roster and deregisters on shutdown, the same
//! lifecycle a Claude role rides.
//!
//! A real `codex exec` needs `OpenAI` credentials and the `codex` binary,
//! absent in CI, so a shell stub stands in for the spawned process to drive the
//! roster lifecycle, exactly as `tests/up.rs` does for `claude -p`. The Fleet,
//! not the agent process, is what registers the role (via [`RosterClient`]), so
//! the stub exercises the real registration path.

use std::{
    net::{Ipv4Addr, SocketAddr, TcpListener},
    thread,
    time::{Duration, Instant},
};

use crew_broker::{AppState, Config};
use crew_core::{BrokerEndpoint, RoleCard, RoleId, Runtime};
use crew_supervisor::{
    agent_command, provision, AgentCommand, Fleet, LifecyclePolicy, PreparedAgent, RosterClient,
};

/// Starts a broker over a fresh in-memory store, returning the base URL.
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
    format!("http://{addr}")
}

/// The liveness the roster records for `role`, if it is registered.
fn liveness(base: &str, role: &str) -> Option<String> {
    let text = ureq::get(&format!("{base}/roster"))
        .call()
        .unwrap()
        .into_string()
        .unwrap();
    let value: serde_json::Value = serde_json::from_str(&text).unwrap();
    value["roles"]
        .as_array()
        .unwrap()
        .iter()
        .find(|entry| entry["role"] == role)
        .and_then(|entry| entry["liveness"].as_str().map(str::to_owned))
}

/// Polls `condition` until it holds or a five-second deadline passes.
fn wait_until(mut condition: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline {
        if condition() {
            return true;
        }
        thread::sleep(Duration::from_millis(20));
    }
    condition()
}

#[test]
fn a_spawned_codex_role_registers_on_the_roster() {
    let base = start_broker();
    let roster = RosterClient::new(base.clone());
    let role = RoleId::new("backend");

    // The card the supervisor provisions for a Codex role, pointing at the broker.
    let addr = base.trim_start_matches("http://");
    let (host, port) = addr.split_once(':').unwrap();
    let card = RoleCard::new(
        role.clone(),
        vec!["api/".to_owned()],
        "Tests green.",
        BrokerEndpoint::new(host.to_owned(), port.parse().unwrap()),
    )
    .with_runtime(Runtime::Codex);

    let agent_dir = std::env::temp_dir().join(format!("crew-codex-test-{}", std::process::id()));
    std::fs::create_dir_all(&agent_dir).unwrap();
    let launch = provision(&card, &agent_dir).expect("provision the codex card");

    // The command crew would spawn for this role: the invocation verified against
    // codex-cli 0.145.0. `exec` is the non-interactive mode, the bypass flag drops
    // approvals and the sandbox, and `--skip-git-repo-check` lets a scratch-dir
    // role boot outside a repo.
    let codex = agent_command(&launch, &agent_dir);
    assert_eq!(codex.program, "codex", "a codex role spawns the codex CLI");
    assert_eq!(
        codex.args,
        vec![
            "exec".to_owned(),
            "--dangerously-bypass-approvals-and-sandbox".to_owned(),
            "--skip-git-repo-check".to_owned(),
            launch.briefing.clone(),
        ],
        "the verified codex exec invocation, briefing last",
    );

    // Real `codex exec` needs auth and the binary (absent in CI), so a shell stub
    // stands in for the spawned process while keeping the codex role's env, exactly
    // as tests/up.rs stubs `claude -p`. The Fleet is what registers the role.
    let stub = PreparedAgent {
        role: role.clone(),
        owned_paths: card.owned_paths.clone(),
        command: AgentCommand {
            program: "bash".to_owned(),
            args: vec!["-c".to_owned(), "echo ready; sleep 30".to_owned()],
            env: codex.env.clone(),
            cwd: agent_dir.clone(),
        },
    };

    // Keep idle-stop far out so the role stays working through the assertions.
    let policy = LifecyclePolicy {
        idle_timeout: Duration::from_secs(3600),
        ..LifecyclePolicy::default()
    };
    let fleet = Fleet::launch(&roster, vec![stub], policy);
    fleet.start_all().expect("the codex role starts");

    assert!(
        wait_until(|| liveness(&base, role.as_str()).as_deref() == Some("working")),
        "a spawned codex role registers on the roster, working",
    );

    fleet.shutdown();
    assert!(
        wait_until(|| liveness(&base, role.as_str()).is_none()),
        "the codex role deregisters on shutdown, leaving no stale roster entry",
    );

    let _ = std::fs::remove_dir_all(&agent_dir);
}
