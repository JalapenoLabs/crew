//! A persisted per-role inbox cursor for the CLI shim (issue #130).
//!
//! The MCP server is one long-lived process, so its inbox cursor lives in
//! memory across a session and `crew_inbox` returns only what arrived since the
//! last call. The shim is a short-lived process per call, so it keeps the
//! cursor on disk instead: one small file per role under the broker state dir,
//! holding the count of inbox messages the role has already seen. `crew inbox`
//! seeds the client from it and writes the advanced value back, so it shows
//! only messages that arrived since the last call, closing the parity gap in
//! `docs/codex.md`.

use std::{
    fs,
    path::{Path, PathBuf},
};

use crew_substrate::core::RoleId;
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
    use crew_substrate::core::RoleId;

    use super::{sanitize, InboxCursor};

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
