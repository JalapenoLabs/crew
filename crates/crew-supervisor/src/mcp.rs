//! Auto-registering the crew MCP server so a spawned agent gets the crew tools.
//!
//! An agent only has the `crew_send` / `crew_inbox` / `crew_roster` tools if
//! its Claude Code process loads the [`crew-mcp`](crew_mcp) server, and it must
//! load with no per-task approval gate. This mirrors how Seraphim registers the
//! Playwright MCP (issue #20): register the server once at **user** scope in
//! the agent config (`claude mcp add -s user crew -- <path>`), so a `claude -p
//! --permission-mode bypassPermissions` turn loads it silently. A
//! project-scoped `.mcp.json` would sit unapproved and never connect under
//! `bypassPermissions`.
//!
//! Registration is a one-time, unit-wide step, not per-agent:
//! [`register_server`] records only the command. Each spawned agent's own
//! broker address, role, and lane ride the environment its `claude` process is
//! launched with (the [`Launch::env`] from [`provision`](crate::provision)),
//! which the `crew-mcp` child inherits. So the flow is: [`locate_server`] and
//! [`register_server`] once at startup, then `provision` and spawn per role
//! with [`agent_turn_argv`].
//!
//! [`Launch::env`]: crate::Launch::env

use std::{
    ffi::{OsStr, OsString},
    path::{Path, PathBuf},
    process::Command,
};

use eyre::{eyre, Result, WrapErr};

/// The name the crew MCP server is registered under in the agent config.
pub const MCP_SERVER_NAME: &str = "crew";

/// The Claude Code CLI, resolved from `PATH`, that owns the MCP registry.
pub(crate) const CLAUDE_BIN: &str = "claude";

/// The Codex CLI, resolved from `PATH`, for roles that run on Codex (issue
/// #128). Codex has no MCP surface, so a Codex role reaches the crew through the
/// CLI shim (`crew send`, ...) instead (see `docs/codex.md`).
pub(crate) const CODEX_BIN: &str = "codex";

/// The crew MCP server binary's base name (the platform executable suffix is
/// added).
const SERVER_BINARY: &str = "crew-mcp";

/// Finds the `crew-mcp` server binary, the build/boot check.
///
/// Looks next to the running executable first (a co-installed release layout),
/// then on `PATH`. Returns the resolved path, so [`register_server`] records an
/// absolute command that does not depend on the agent's `PATH`.
///
/// # Errors
/// Fails loudly, naming where it looked, if the binary is nowhere to be found,
/// so a missing build never becomes a silently tool-less agent.
pub fn locate_server() -> Result<PathBuf> {
    let name = server_file_name();

    // Next to the supervisor's own executable: the release layout ships them
    // together.
    if let Some(dir) = std::env::current_exe()
        .ok()
        .and_then(|exe| sibling_dir(&exe))
    {
        let candidate = dir.join(&name);
        if candidate.is_file() {
            return Ok(candidate);
        }
    }

    if let Some(path) = std::env::var_os("PATH") {
        if let Some(found) = find_on_path(&name, &path) {
            return Ok(found);
        }
    }

    Err(eyre!(
        "could not find the `{}` server binary next to the supervisor or on PATH; \
         build it with `cargo build -p crew-mcp` and install it alongside the supervisor",
        name.to_string_lossy(),
    ))
}

/// Registers the crew MCP server at user scope so every spawned agent loads it.
///
/// Idempotent by construction: it removes any prior `crew` registration, then
/// adds `server`, so re-running re-asserts the current path. User scope (`-s
/// user`) writes it to the agent config, so a `claude -p --permission-mode
/// bypassPermissions` turn loads it with no approval prompt.
///
/// # Errors
/// Fails if `server` is not an existing file (the build/boot check), or if the
/// `claude mcp add` call cannot run or reports failure.
pub fn register_server(server: &Path) -> Result<()> {
    ensure_binary(server)?;

    // Best-effort teardown: a missing prior registration is the normal first-run
    // state, so ignore any failure. The following add is what must succeed.
    let _ = Command::new(CLAUDE_BIN).args(mcp_remove_argv()).status();

    let status = Command::new(CLAUDE_BIN)
        .args(mcp_add_argv(server))
        .status()
        .wrap_err_with(|| {
            format!("could not run `{CLAUDE_BIN}`; is Claude Code installed and on PATH?")
        })?;
    if !status.success() {
        return Err(eyre!(
            "`{CLAUDE_BIN} mcp add -s user {MCP_SERVER_NAME}` failed ({status}); \
             the crew MCP server was not registered",
        ));
    }
    Ok(())
}

/// Builds the `claude` argv for a headless agent turn that loads the crew MCP
/// silently.
///
/// `-p` runs the turn headless with `briefing` as the opening prompt (the role
/// card's thin bootstrap), `--output-format stream-json --verbose` makes the
/// agent emit its per-turn activity as stream-json for the supervisor to parse
/// (issue #24; `--verbose` is required alongside `stream-json` under `-p`), and
/// `--permission-mode bypassPermissions` is what makes the user-scope crew
/// server load with no approval gate. `argv[0]` is the program.
#[must_use]
pub fn agent_turn_argv(briefing: &str) -> Vec<String> {
    vec![
        CLAUDE_BIN.to_owned(),
        "-p".to_owned(),
        briefing.to_owned(),
        "--output-format".to_owned(),
        "stream-json".to_owned(),
        "--verbose".to_owned(),
        "--permission-mode".to_owned(),
        "bypassPermissions".to_owned(),
    ]
}

/// Builds the `codex` argv for a headless agent turn, the Codex analog of
/// [`agent_turn_argv`] (issue #128).
///
/// `codex exec` runs a non-interactive turn with `briefing` as the opening
/// prompt (the shim-adapted role card bootstrap), and
/// `--dangerously-bypass-approvals-and-sandbox` is the analog of Claude's
/// `bypassPermissions`: it drops the approval gate and the sandbox so an
/// unattended crew agent can work without prompting, which is the whole point
/// of a spawned agent. `argv[0]` is the program. A Codex role needs no MCP
/// registration; it reaches the crew through the CLI shim, which reads the same
/// `CREW_ROLE_CARD` environment (see `docs/codex.md`).
#[must_use]
pub fn codex_turn_argv(briefing: &str) -> Vec<String> {
    vec![
        CODEX_BIN.to_owned(),
        "exec".to_owned(),
        "--dangerously-bypass-approvals-and-sandbox".to_owned(),
        briefing.to_owned(),
    ]
}

/// The `claude mcp add` argv registering the stdio server at user scope by
/// path.
fn mcp_add_argv(server: &Path) -> Vec<OsString> {
    let mut argv = os_args(&["mcp", "add", "-s", "user", MCP_SERVER_NAME, "--"]);
    argv.push(server.as_os_str().to_os_string());
    argv
}

/// The `claude mcp remove` argv that clears a prior registration before
/// re-adding.
fn mcp_remove_argv() -> Vec<OsString> {
    os_args(&["mcp", "remove", "-s", "user", MCP_SERVER_NAME])
}

/// The build/boot check: the server binary must be an existing file.
fn ensure_binary(server: &Path) -> Result<()> {
    if server.is_file() {
        return Ok(());
    }
    Err(eyre!(
        "the crew MCP server binary is missing at {}; build it with `cargo build -p crew-mcp`",
        server.display(),
    ))
}

/// The server binary's file name, with the platform executable suffix (`.exe`
/// on Windows, empty elsewhere).
fn server_file_name() -> OsString {
    OsString::from(format!("{SERVER_BINARY}{}", std::env::consts::EXE_SUFFIX))
}

/// The directory holding `exe`, if it has one.
fn sibling_dir(exe: &Path) -> Option<PathBuf> {
    exe.parent().map(Path::to_path_buf)
}

/// The first directory on `path` that holds a file named `name`, if any.
fn find_on_path(name: &OsStr, path: &OsStr) -> Option<PathBuf> {
    std::env::split_paths(path)
        .map(|dir| dir.join(name))
        .find(|candidate| candidate.is_file())
}

/// Collects string literals into owned OS strings for a [`Command`]'s args.
fn os_args(args: &[&str]) -> Vec<OsString> {
    args.iter().map(OsString::from).collect()
}

#[cfg(test)]
mod tests {
    use std::{
        ffi::OsString,
        path::{Path, PathBuf},
        sync::atomic::{AtomicU64, Ordering},
    };

    use super::{
        agent_turn_argv, ensure_binary, find_on_path, mcp_add_argv, mcp_remove_argv,
        register_server, server_file_name,
    };

    /// Renders an OS-string argv as UTF-8 for comparison.
    fn rendered(argv: &[OsString]) -> Vec<String> {
        argv.iter()
            .map(|arg| arg.to_string_lossy().into_owned())
            .collect()
    }

    /// A unique, empty directory under the system temp dir, removed on drop.
    struct TempDir(PathBuf);

    impl TempDir {
        fn new() -> Self {
            static COUNTER: AtomicU64 = AtomicU64::new(0);
            let unique = COUNTER.fetch_add(1, Ordering::Relaxed);
            let path =
                std::env::temp_dir().join(format!("crew-mcp-test-{}-{unique}", std::process::id()));
            std::fs::create_dir_all(&path).unwrap();
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

    #[test]
    fn add_registers_the_stdio_server_at_user_scope_by_path() {
        let argv = mcp_add_argv(Path::new("/opt/crew/crew-mcp"));
        assert_eq!(
            rendered(&argv),
            [
                "mcp",
                "add",
                "-s",
                "user",
                "crew",
                "--",
                "/opt/crew/crew-mcp",
            ],
            "the `--` separates the server command from the mcp-add flags",
        );
    }

    #[test]
    fn remove_clears_the_user_scope_registration() {
        assert_eq!(
            rendered(&mcp_remove_argv()),
            ["mcp", "remove", "-s", "user", "crew"],
        );
    }

    #[test]
    fn a_turn_bypasses_permissions_so_the_server_loads_with_no_prompt() {
        let argv = agent_turn_argv("You are the backend role.");
        assert_eq!(argv[0], "claude");
        assert_eq!(argv[1], "-p");
        assert_eq!(
            argv[2], "You are the backend role.",
            "the briefing is the prompt"
        );
        // The bypass flag is the no-approval guarantee, as a flag/value pair.
        let mode = argv
            .iter()
            .position(|arg| arg == "--permission-mode")
            .unwrap();
        assert_eq!(argv[mode + 1], "bypassPermissions");
        // The turn emits stream-json so the supervisor can parse its activity
        // (issue #24); `--verbose` is required alongside it under `-p`.
        let format = argv
            .iter()
            .position(|arg| arg == "--output-format")
            .expect("the turn sets an output format");
        assert_eq!(argv[format + 1], "stream-json");
        assert!(
            argv.iter().any(|arg| arg == "--verbose"),
            "stream-json under -p requires --verbose: {argv:?}",
        );
    }

    #[test]
    fn the_binary_name_carries_the_platform_exe_suffix() {
        let name = server_file_name();
        let expected = format!("crew-mcp{}", std::env::consts::EXE_SUFFIX);
        assert_eq!(name, OsString::from(expected));
    }

    #[test]
    fn the_build_check_passes_for_a_present_binary_and_fails_loudly_when_missing() {
        let dir = TempDir::new();
        let present = dir.path().join("crew-mcp");
        std::fs::write(&present, b"#!/bin/sh\n").unwrap();
        assert!(ensure_binary(&present).is_ok(), "an existing file passes");

        let missing = dir.path().join("absent");
        let error = ensure_binary(&missing).unwrap_err().to_string();
        assert!(error.contains("missing"), "the error is loud: {error}");
        assert!(
            error.contains(&missing.display().to_string()),
            "it names the path"
        );
    }

    #[test]
    fn register_fails_the_build_check_before_touching_claude() {
        // The binary is missing, so registration must fail on the check, never
        // reaching (a possibly absent) `claude`.
        let error = register_server(Path::new("/no/such/crew-mcp"))
            .unwrap_err()
            .to_string();
        assert!(error.contains("missing"), "loud, actionable error: {error}");
    }

    #[test]
    fn find_on_path_locates_a_binary_in_a_listed_directory() {
        let dir = TempDir::new();
        let name = OsString::from("crew-mcp");
        std::fs::write(dir.path().join(&name), b"").unwrap();

        // A PATH with a bogus entry first, then the real directory.
        let path = std::env::join_paths([Path::new("/nonexistent-crew-dir"), dir.path()]).unwrap();
        assert_eq!(find_on_path(&name, &path), Some(dir.path().join(&name)));

        let absent = OsString::from("not-there");
        assert_eq!(find_on_path(&absent, &path), None);
    }
}
