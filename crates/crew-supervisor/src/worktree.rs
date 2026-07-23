//! Per-role git worktree isolation (issue #43).
//!
//! Parallel roles editing the same repo clobber each other's working tree. A git
//! **worktree** gives each role its own checked-out copy of a repo on its own branch,
//! so two roles editing at once never corrupt each other: git keeps each worktree's
//! index and files separate, sharing only the object store. The supervisor creates one
//! per role of each repo the crew is configured to touch, points the agent's working
//! directory at it, and cleans it up on stand-down.
//!
//! Cleanup preserves work: [`remove`](Worktree::remove) runs `git worktree remove`
//! without `--force`, so an **unchanged** worktree is removed and one with uncommitted
//! changes is kept, since integrating a role's work is a deliberate later step (#48).
//! A role that commits its work to its branch leaves a clean worktree, so removing it
//! drops only the checkout while the branch (and its commits) survive for integration.

use std::path::{Path, PathBuf};
use std::process::Command;

use crew_core::RoleId;
use eyre::{bail, Result, WrapErr};
use tracing::{event, Level};

/// One role's isolated git worktree of a repo.
///
/// Created with [`create`](Worktree::create) and removed with
/// [`remove`](Worktree::remove). The branch is named `crew/<role>`, so a later
/// integration step can find each role's work by a stable name.
#[derive(Debug, Clone)]
pub struct Worktree {
    /// The repository this is a worktree of.
    repo: PathBuf,
    /// The worktree's own checked-out directory.
    path: PathBuf,
    /// The branch checked out in it (`crew/<role>`).
    branch: String,
}

impl Worktree {
    /// Creates an isolated worktree of `repo` for `role` at `path`, on branch
    /// `crew/<role>`.
    ///
    /// The branch is created from the repo's current `HEAD`. A stale worktree left by a
    /// prior run is pruned first, and an existing `crew/<role>` branch is reused rather
    /// than duplicated, so bringing the same crew up twice is idempotent.
    ///
    /// # Errors
    /// Returns an error if `repo` is not a git repository, or git cannot add the
    /// worktree (for example the branch is already checked out elsewhere).
    pub fn create(repo: &Path, role: &RoleId, path: &Path) -> Result<Self> {
        let repo = repo
            .canonicalize()
            .wrap_err_with(|| format!("repo `{}` does not exist", repo.display()))?;
        let branch = format!("crew/{role}");

        // Drop any admin entry for a worktree directory that no longer exists, so a
        // re-run does not trip over its own leftovers.
        let _ = git(&repo, &["worktree", "prune"]);

        let dest = path.to_string_lossy();
        // A fresh branch off HEAD is the common path; if the branch already exists
        // (a prior run), check it out into the new worktree instead of failing.
        let created = git(&repo, &["worktree", "add", "-b", &branch, &dest]);
        if created.is_err() {
            git(&repo, &["worktree", "add", &dest, &branch]).wrap_err_with(|| {
                format!(
                    "could not create a worktree for `{role}` at {}",
                    path.display()
                )
            })?;
        }

        event!(
            name: "supervisor.worktree.created",
            Level::INFO,
            crew.role = %role,
            crew.branch = %branch,
            path = %path.display(),
            "isolated `{{crew.role}}` in a worktree on `{{crew.branch}}`",
        );

        Ok(Self {
            repo,
            path: path.to_path_buf(),
            branch,
        })
    }

    /// The worktree's directory, where the role's agent works.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The branch checked out in the worktree (`crew/<role>`).
    #[must_use]
    pub fn branch(&self) -> &str {
        &self.branch
    }

    /// Whether the worktree has uncommitted changes (tracked edits or untracked files).
    ///
    /// # Errors
    /// Returns an error if git cannot read the worktree's status.
    pub fn is_dirty(&self) -> Result<bool> {
        let out = git(&self.path, &["status", "--porcelain"])?;
        Ok(!out.trim().is_empty())
    }

    /// Removes the worktree if it has no uncommitted changes, returning whether it was
    /// removed.
    ///
    /// A clean worktree is removed (its branch, and any commits on it, survive); one
    /// with uncommitted changes is kept, so a role's unintegrated work is never
    /// discarded. This is the automatic cleanup of an unchanged worktree.
    ///
    /// # Errors
    /// Returns an error only if git fails for a reason other than the worktree being
    /// dirty; a dirty worktree is reported as kept (`Ok(false)`), not an error.
    pub fn remove(&self) -> Result<bool> {
        // `git worktree remove` (without --force) refuses a dirty worktree, which is
        // exactly the policy: unchanged is cleaned, changed is preserved.
        match git(
            &self.repo,
            &["worktree", "remove", &self.path.to_string_lossy()],
        ) {
            Ok(_) => {
                event!(
                    name: "supervisor.worktree.removed",
                    Level::INFO,
                    crew.branch = %self.branch,
                    path = %self.path.display(),
                    "cleaned up the unchanged worktree on `{{crew.branch}}`",
                );
                Ok(true)
            }
            Err(_) if self.is_dirty().unwrap_or(true) => {
                event!(
                    name: "supervisor.worktree.kept",
                    Level::INFO,
                    crew.branch = %self.branch,
                    path = %self.path.display(),
                    "kept the changed worktree on `{{crew.branch}}` for integration",
                );
                Ok(false)
            }
            Err(err) => Err(err).wrap_err_with(|| {
                format!("could not remove the worktree at {}", self.path.display())
            }),
        }
    }
}

/// Removes every worktree, best-effort: a changed one is kept and a hard failure is
/// logged, so cleaning up the crew never fails a stand-down.
pub(crate) fn clean_all(worktrees: &[Worktree]) {
    for tree in worktrees {
        if let Err(err) = tree.remove() {
            event!(
                name: "supervisor.worktree.clean.failed",
                Level::WARN,
                crew.branch = %tree.branch,
                path = %tree.path.display(),
                error = %err,
                "could not clean up the worktree on `{{crew.branch}}`",
            );
        }
    }
}

/// Runs a git command in `dir`, returning its stdout or an error carrying stderr.
fn git(dir: &Path, args: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .wrap_err("could not run git")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("git {} failed: {}", args.join(" "), stderr.trim());
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

#[cfg(test)]
mod tests {
    use super::{git, Worktree};
    use crew_core::RoleId;

    /// A fresh temp directory unique to a test, cleaned on entry.
    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("crew-worktree-test-{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Initializes a git repo with one commit, so a worktree can branch off HEAD.
    fn init_repo(dir: &std::path::Path) {
        git(dir, &["init", "-q", "-b", "main"]).unwrap();
        git(dir, &["config", "user.email", "crew@test"]).unwrap();
        git(dir, &["config", "user.name", "crew"]).unwrap();
        std::fs::write(dir.join("file.txt"), "base\n").unwrap();
        git(dir, &["add", "."]).unwrap();
        git(dir, &["commit", "-q", "-m", "base"]).unwrap();
    }

    #[test]
    fn two_roles_edit_in_parallel_without_corrupting_each_other() {
        let root = scratch("parallel");
        let repo = root.join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        init_repo(&repo);

        let backend =
            Worktree::create(&repo, &RoleId::new("backend"), &root.join("backend")).unwrap();
        let frontend =
            Worktree::create(&repo, &RoleId::new("frontend"), &root.join("frontend")).unwrap();

        // Each role edits the same file in its own worktree, differently.
        std::fs::write(backend.path().join("file.txt"), "backend edit\n").unwrap();
        std::fs::write(frontend.path().join("file.txt"), "frontend edit\n").unwrap();

        // Neither edit leaks into the other's working tree.
        assert_eq!(
            std::fs::read_to_string(backend.path().join("file.txt")).unwrap(),
            "backend edit\n"
        );
        assert_eq!(
            std::fs::read_to_string(frontend.path().join("file.txt")).unwrap(),
            "frontend edit\n"
        );
        // The shared repo's own working tree is untouched.
        assert_eq!(
            std::fs::read_to_string(repo.join("file.txt")).unwrap(),
            "base\n"
        );
        // Each is on its own branch.
        assert_eq!(backend.branch(), "crew/backend");
        assert_eq!(frontend.branch(), "crew/frontend");
    }

    #[test]
    fn an_unchanged_worktree_is_removed() {
        let root = scratch("clean-remove");
        let repo = root.join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        init_repo(&repo);

        let tree = Worktree::create(&repo, &RoleId::new("qa"), &root.join("qa")).unwrap();
        assert!(tree.path().exists());
        assert!(!tree.is_dirty().unwrap(), "a fresh worktree is clean");

        let removed = tree.remove().unwrap();
        assert!(removed, "an unchanged worktree is removed");
        assert!(!tree.path().exists(), "its directory is gone");
    }

    #[test]
    fn a_changed_worktree_is_kept_for_integration() {
        let root = scratch("dirty-keep");
        let repo = root.join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        init_repo(&repo);

        let tree = Worktree::create(&repo, &RoleId::new("backend"), &root.join("backend")).unwrap();
        std::fs::write(tree.path().join("file.txt"), "work in progress\n").unwrap();
        assert!(tree.is_dirty().unwrap(), "an edited worktree is dirty");

        let removed = tree.remove().unwrap();
        assert!(!removed, "a changed worktree is kept, not discarded");
        assert!(tree.path().exists(), "its work survives for integration");
    }

    #[test]
    fn creating_a_worktree_twice_is_idempotent() {
        let root = scratch("idempotent");
        let repo = root.join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        init_repo(&repo);

        let role = RoleId::new("docs");
        let first = Worktree::create(&repo, &role, &root.join("docs")).unwrap();
        first.remove().unwrap();
        // The branch `crew/docs` now exists; creating again reuses it rather than failing.
        let second = Worktree::create(&repo, &role, &root.join("docs")).unwrap();
        assert_eq!(second.branch(), "crew/docs");
        assert!(second.path().exists());
    }
}
