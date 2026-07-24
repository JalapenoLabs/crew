//! Per-role shim state persisted under the broker state dir (issues #130,
//! #132).
//!
//! The MCP server is one long-lived process, so its inbox cursor and the task
//! it is working live in memory across a session. The shim is a short-lived
//! process per call, so it keeps that state on disk instead, one small file per
//! role under the broker state dir, so a later invocation behaves like the
//! long-lived path and the parity gaps in `docs/codex.md` close:
//!
//! - [`InboxCursor`] holds the count of inbox messages the role has seen, so
//!   `crew inbox` shows only what arrived since the last call (issue #130).
//! - [`TaskContext`] holds the task the role adopted from an order, so a later
//!   `crew send` / `crew order` stamps it and its messages correlate to the
//!   task, the way the MCP server's in-memory client does (issue #132).

use std::{
    fs,
    path::{Path, PathBuf},
};

use crew_substrate::core::{RoleId, TaskId};
use eyre::{Result, WrapErr};

/// The subdirectory under the broker state dir that holds the shim cursor
/// files, one per role, so they never clutter the state dir root next to the
/// log.
const CURSOR_DIR: &str = "shim-cursors";

/// A per-role inbox cursor file: the count of inbox messages the role has read.
#[derive(Debug)]
pub struct InboxCursor {
    /// The file holding this role's cursor, under the state dir's cursor
    /// subdir.
    path: PathBuf,
}

impl InboxCursor {
    /// The cursor file for `role` under the broker's `state_dir`.
    #[must_use]
    pub fn new(state_dir: &Path, role: &RoleId) -> Self {
        let file = format!("{}.cursor", sanitize(role.as_str()));
        Self {
            path: state_dir.join(CURSOR_DIR).join(file),
        }
    }

    /// Loads the saved cursor, or `0` when there is none yet.
    ///
    /// A missing, unreadable, or malformed file reads as `0`, so a first call,
    /// or a corrupt cursor, safely shows the whole inbox rather than
    /// failing the command.
    #[must_use]
    pub fn load(&self) -> usize {
        fs::read_to_string(&self.path)
            .ok()
            .and_then(|text| text.trim().parse().ok())
            .unwrap_or(0)
    }

    /// Saves `read_through` as the new cursor, creating the cursor dir if
    /// needed.
    ///
    /// # Errors
    /// Returns an error if the cursor directory cannot be created or the file
    /// cannot be written.
    pub fn save(&self, read_through: usize) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)
                .wrap_err_with(|| format!("could not create cursor dir {}", parent.display()))?;
        }
        fs::write(&self.path, read_through.to_string())
            .wrap_err_with(|| format!("could not write inbox cursor {}", self.path.display()))
    }
}

/// A per-role task-context file: the task the role adopted from an order, so a
/// later invocation stamps it on the messages it sends (issue #132).
#[derive(Debug)]
pub struct TaskContext {
    /// The file holding this role's task, under the state dir's cursor subdir.
    path: PathBuf,
}

impl TaskContext {
    /// The task-context file for `role` under the broker's `state_dir`.
    #[must_use]
    pub fn new(state_dir: &Path, role: &RoleId) -> Self {
        let file = format!("{}.task", sanitize(role.as_str()));
        Self {
            path: state_dir.join(CURSOR_DIR).join(file),
        }
    }

    /// Loads the saved task, or `None` when there is none yet.
    ///
    /// A missing, unreadable, or malformed file reads as `None`, so a role that
    /// has not been ordered yet, or a corrupt file, simply sends without a task
    /// rather than failing the command.
    #[must_use]
    pub fn load(&self) -> Option<TaskId> {
        let text = fs::read_to_string(&self.path).ok()?;
        serde_json::from_str(text.trim()).ok()
    }

    /// Saves `task` as the role's current task, creating the cursor dir if
    /// needed.
    ///
    /// # Errors
    /// Returns an error if the cursor directory cannot be created or the file
    /// cannot be written.
    pub fn save(&self, task: TaskId) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent)
                .wrap_err_with(|| format!("could not create cursor dir {}", parent.display()))?;
        }
        let text = serde_json::to_string(&task).wrap_err("could not encode the task context")?;
        fs::write(&self.path, text)
            .wrap_err_with(|| format!("could not write task context {}", self.path.display()))
    }
}

/// Makes `role` safe as a single path component: keeps `[A-Za-z0-9._-]` and
/// maps every other character to `_`, so a role name can never escape the
/// cursor directory (path traversal) or nest into a subpath.
///
/// Real role names are plain lane identifiers, so the mapping is an identity in
/// practice; it is a guard against a hand-authored `CREW_ROLE`, not a
/// namespace.
fn sanitize(role: &str) -> String {
    role.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use crew_substrate::core::{RoleId, TaskId};

    use super::{sanitize, InboxCursor, TaskContext};

    #[test]
    fn an_unset_cursor_loads_as_zero() {
        let dir = tempdir();
        let cursor = InboxCursor::new(&dir, &RoleId::new("backend"));
        assert_eq!(
            cursor.load(),
            0,
            "no file yet means start from the beginning"
        );
    }

    #[test]
    fn a_saved_cursor_round_trips() {
        let dir = tempdir();
        let cursor = InboxCursor::new(&dir, &RoleId::new("backend"));
        cursor.save(7).expect("the cursor saves");
        assert_eq!(
            cursor.load(),
            7,
            "a later read resumes from the saved count"
        );

        // Advancing overwrites, so the newest read position wins.
        cursor.save(12).expect("the cursor advances");
        assert_eq!(cursor.load(), 12);
    }

    #[test]
    fn each_role_keeps_its_own_cursor() {
        let dir = tempdir();
        InboxCursor::new(&dir, &RoleId::new("backend"))
            .save(3)
            .unwrap();
        InboxCursor::new(&dir, &RoleId::new("frontend"))
            .save(9)
            .unwrap();
        assert_eq!(InboxCursor::new(&dir, &RoleId::new("backend")).load(), 3);
        assert_eq!(InboxCursor::new(&dir, &RoleId::new("frontend")).load(), 9);
    }

    #[test]
    fn a_malformed_cursor_file_loads_as_zero() {
        let dir = tempdir();
        let cursor = InboxCursor::new(&dir, &RoleId::new("backend"));
        cursor.save(4).unwrap();
        std::fs::write(&cursor.path, "not a number").unwrap();
        assert_eq!(cursor.load(), 0, "a corrupt cursor falls back to the start");
    }

    #[test]
    fn an_unset_task_context_loads_as_none() {
        let dir = tempdir();
        let context = TaskContext::new(&dir, &RoleId::new("backend"));
        assert!(
            context.load().is_none(),
            "no file yet means the role has no task"
        );
    }

    #[test]
    fn a_saved_task_round_trips_and_the_newest_wins() {
        let dir = tempdir();
        let context = TaskContext::new(&dir, &RoleId::new("backend"));
        let task = TaskId::new();
        context.save(task).expect("the task saves");
        assert_eq!(context.load(), Some(task), "a later read restores the task");

        // Re-tasking overwrites, so the newest assignment wins.
        let next = TaskId::new();
        context.save(next).expect("the task advances");
        assert_eq!(context.load(), Some(next));
    }

    #[test]
    fn a_malformed_task_file_loads_as_none() {
        let dir = tempdir();
        let context = TaskContext::new(&dir, &RoleId::new("backend"));
        context.save(TaskId::new()).unwrap();
        std::fs::write(&context.path, "not a task").unwrap();
        assert!(
            context.load().is_none(),
            "a corrupt task file falls back to no task"
        );
    }

    #[test]
    fn sanitize_neutralizes_path_separators() {
        assert_eq!(sanitize("backend"), "backend");
        assert_eq!(sanitize("sdet-unit"), "sdet-unit");
        // Separators become `_`, so a traversal attempt stays one component; the
        // dots are harmless once the `.cursor` suffix is appended.
        assert_eq!(sanitize("../escape"), ".._escape");
        assert_eq!(sanitize("a/b"), "a_b");
    }

    /// A fresh, unique temp directory for one test's cursor files.
    fn tempdir() -> std::path::PathBuf {
        // `Instant`/`SystemTime` give a per-call unique suffix without a crate.
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir =
            std::env::temp_dir().join(format!("crew-cursor-test-{}-{unique}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }
}
