//! Fleet-level worktree isolation and cleanup on stand-down (issue #43).
//!
//! Proves the acceptance against a real git repo and a real in-process broker, with
//! stub agents standing in for `claude`: two roles get isolated worktrees, and standing
//! the fleet down cleans up an unchanged worktree while preserving a changed one for
//! integration.

mod common;

use std::path::Path;
use std::process::Command;
use std::time::Duration;

use crew_core::RoleId;
use crew_supervisor::{
    AgentCommand, Fleet, LifecyclePolicy, PreparedAgent, RosterClient, Worktree,
};

use common::{liveness, start_broker, wait_until};

/// Runs a git command in `dir`, asserting it succeeds.
fn git(dir: &Path, args: &[&str]) {
    let ok = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .status()
        .unwrap()
        .success();
    assert!(ok, "git {args:?} failed in {}", dir.display());
}

/// Initializes a git repo with one commit under `dir`.
fn init_repo(dir: &Path) {
    std::fs::create_dir_all(dir).unwrap();
    git(dir, &["init", "-q", "-b", "main"]);
    git(dir, &["config", "user.email", "crew@test"]);
    git(dir, &["config", "user.name", "crew"]);
    std::fs::write(dir.join("file.txt"), "base\n").unwrap();
    git(dir, &["add", "."]);
    git(dir, &["commit", "-q", "-m", "base"]);
}

/// A fresh temp directory unique to a test, cleaned on entry.
fn scratch(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("crew-wt-fleet-{name}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A stub agent for `role` that idles in `cwd` until the fleet kills it.
fn stub_in(role: &str, cwd: &Path) -> PreparedAgent {
    PreparedAgent {
        role: RoleId::new(role),
        owned_paths: Vec::new(),
        command: AgentCommand {
            program: "bash".to_owned(),
            args: vec!["-c".to_owned(), "echo ready; sleep 30".to_owned()],
            env: Vec::new(),
            cwd: cwd.to_path_buf(),
        },
    }
}

/// The fleet policy the tests use: never idle-stop, so roles stay working throughout.
fn policy() -> LifecyclePolicy {
    LifecyclePolicy {
        idle_timeout: Duration::from_secs(3600),
        ..LifecyclePolicy::default()
    }
}

#[test]
fn parallel_worktrees_are_isolated_and_cleaned_up_on_stand_down() {
    let base = start_broker();
    let roster = RosterClient::new(base.clone());

    let root = scratch("isolation");
    let repo = root.join("repo");
    init_repo(&repo);

    // Two roles, each isolated in its own worktree of the shared repo.
    let backend = Worktree::create(&repo, &RoleId::new("backend"), &root.join("backend")).unwrap();
    let frontend =
        Worktree::create(&repo, &RoleId::new("frontend"), &root.join("frontend")).unwrap();
    let backend_path = backend.path().to_path_buf();
    let frontend_path = frontend.path().to_path_buf();

    let agents = vec![
        stub_in("backend", &backend_path),
        stub_in("frontend", &frontend_path),
    ];
    let fleet = Fleet::launch(&roster, agents, policy()).with_worktrees(vec![backend, frontend]);
    fleet.start_all().expect("both roles start");
    assert!(wait_until(
        || liveness(&base, "backend").as_deref() == Some("working")
    ));
    assert!(wait_until(
        || liveness(&base, "frontend").as_deref() == Some("working")
    ));

    // Each role edits the same file in its own worktree; neither leaks into the other.
    std::fs::write(backend_path.join("file.txt"), "backend\n").unwrap();
    std::fs::write(frontend_path.join("file.txt"), "frontend\n").unwrap();
    assert_eq!(
        std::fs::read_to_string(backend_path.join("file.txt")).unwrap(),
        "backend\n"
    );
    assert_eq!(
        std::fs::read_to_string(frontend_path.join("file.txt")).unwrap(),
        "frontend\n"
    );

    // Commit each role's work, so its worktree is clean and removing it keeps the branch.
    for tree in [&backend_path, &frontend_path] {
        git(tree, &["commit", "-q", "-am", "role work"]);
    }

    // Stand down: agents stop, and their now-unchanged worktrees are cleaned up.
    fleet.shutdown();
    assert!(
        !backend_path.exists(),
        "backend's worktree is cleaned up on stand-down"
    );
    assert!(
        !frontend_path.exists(),
        "frontend's worktree is cleaned up on stand-down"
    );
    // The work survives on each role's branch for integration (#48).
    let branches = Command::new("git")
        .arg("-C")
        .arg(&repo)
        .args(["branch", "--list", "crew/*"])
        .output()
        .unwrap();
    let branches = String::from_utf8_lossy(&branches.stdout);
    assert!(branches.contains("crew/backend") && branches.contains("crew/frontend"));
}

#[test]
fn a_changed_worktree_survives_stand_down_for_integration() {
    let base = start_broker();
    let roster = RosterClient::new(base.clone());

    let root = scratch("preserve");
    let repo = root.join("repo");
    init_repo(&repo);

    let backend = Worktree::create(&repo, &RoleId::new("backend"), &root.join("backend")).unwrap();
    let backend_path = backend.path().to_path_buf();

    let fleet = Fleet::launch(&roster, vec![stub_in("backend", &backend_path)], policy())
        .with_worktrees(vec![backend]);
    fleet.start_all().expect("the role starts");
    assert!(wait_until(
        || liveness(&base, "backend").as_deref() == Some("working")
    ));

    // Uncommitted work in flight: the role edited a file but has not committed.
    std::fs::write(backend_path.join("file.txt"), "work in progress\n").unwrap();

    // Stand down keeps the changed worktree, so the unintegrated work is not lost.
    fleet.shutdown();
    assert!(
        backend_path.exists(),
        "a changed worktree survives stand-down for integration",
    );
    assert_eq!(
        std::fs::read_to_string(backend_path.join("file.txt")).unwrap(),
        "work in progress\n",
        "the in-flight work is intact",
    );
}
