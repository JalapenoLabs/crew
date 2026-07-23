//! The integration step: merge the roles' isolated work into one coherent result (issue #44).
//!
//! Parallel roles work in isolated worktrees on `crew/<role>` branches (issue #43). Their
//! work is only done when it comes together: this module merges those branches into a single
//! integration branch, runs the crew's acceptance checks (build, tests) on the merged result,
//! and reports where it stands, so "done" means the integrated whole is green, not just each
//! part in isolation (issue #47's done-gate judges a part; this judges the whole).
//!
//! Conflicts are resolved, not dropped. A conflicting merge is aborted and reported with the
//! branch and the files it collides on, so a human (or the commander) resolves it, never a
//! force-merge that discards a role's work behind its back. Migrations and other ordering
//! concerns stay linear because the acceptance checks run on the integrated branch: a
//! collision that breaks the build or the tests fails the integration rather than shipping.
//!
//! The merge runs in a dedicated integration worktree, so it never disturbs the main checkout
//! or the role worktrees; the integration branch keeps the merged commits after the worktree
//! is cleaned up, ready for the operator to push and open as a pull request.

use std::path::{Path, PathBuf};
use std::process::Command;

use eyre::{bail, eyre, Result, WrapErr};
use tracing::{event, Level};

/// The default branch the role branches merge into.
pub const DEFAULT_INTEGRATION_BRANCH: &str = "crew/integration";

/// The prefix every per-role branch carries (`crew/<role>`, issue #43).
const ROLE_BRANCH_PREFIX: &str = "crew/";

/// How many trailing lines of a failed check's output the report keeps, enough to see the
/// failure without carrying a whole build log.
const MAX_CHECK_OUTPUT_LINES: usize = 40;

/// A merge that could not be applied cleanly: the branch, and the files it collides on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Conflict {
    /// The role branch that conflicted.
    pub branch: String,
    /// The files it collides on, named so the conflict is resolved by hand, not dropped.
    pub files: Vec<String>,
}

/// The acceptance-check outcome on the integrated branch.
#[derive(Debug, Clone)]
pub struct CheckOutcome {
    /// The command that ran.
    pub command: String,
    /// Whether it passed (exit status zero).
    pub passed: bool,
    /// The tail of its combined output, so a failure is diagnosable without the whole log.
    pub output: String,
}

/// Where an integration stands overall (issue #44).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Standing {
    /// Every branch merged cleanly and the acceptance checks passed: a green, coherent branch.
    Green,
    /// Every branch merged cleanly; no acceptance check was requested.
    Merged,
    /// One or more branches conflicted; nothing was dropped, the conflicts are reported to resolve.
    Conflicts,
    /// Every branch merged cleanly, but the acceptance checks failed on the result.
    ChecksFailed,
}

impl Standing {
    /// Whether the integration produced a coherent branch that is safe to ship: everything
    /// merged, and any checks passed.
    #[must_use]
    pub fn is_green(self) -> bool {
        matches!(self, Standing::Green | Standing::Merged)
    }
}

/// The result of an integration run: what merged, what conflicted, and how it checks out.
#[derive(Debug, Clone)]
pub struct IntegrationReport {
    /// The integration branch the work was merged into.
    pub branch: String,
    /// The base ref the integration branch was cut from.
    pub base: String,
    /// The role branches merged cleanly, in the order they were applied.
    pub merged: Vec<String>,
    /// The branches that conflicted, with the files they collide on.
    pub conflicts: Vec<Conflict>,
    /// The acceptance-check outcome, if a check was run (skipped when a merge conflicted).
    pub check: Option<CheckOutcome>,
    /// The overall standing.
    pub standing: Standing,
}

/// The integration step over one repo: merge role branches into an integration branch.
///
/// # Examples
/// ```no_run
/// use std::path::Path;
/// use crew_supervisor::{Integrator, DEFAULT_INTEGRATION_BRANCH};
///
/// let integrator = Integrator::new(Path::new("."), DEFAULT_INTEGRATION_BRANCH, "main")?;
/// let branches = integrator.role_branches()?;
/// let report = integrator.integrate(&branches, Some("cargo test"))?;
/// assert!(report.standing.is_green());
/// # Ok::<(), eyre::Report>(())
/// ```
#[derive(Debug, Clone)]
pub struct Integrator {
    /// The repository whose role branches are merged.
    repo: PathBuf,
    /// The integration branch the merges land on.
    branch: String,
    /// The base ref the integration branch is cut from each run.
    base: String,
}

impl Integrator {
    /// Builds an integrator over `repo`, merging into `branch` cut from `base`.
    ///
    /// `base` is any ref the merge starts from (a branch like `main`, or `HEAD`).
    ///
    /// # Errors
    /// Returns an error if `repo` does not exist.
    pub fn new(repo: &Path, branch: impl Into<String>, base: impl Into<String>) -> Result<Self> {
        let repo = repo
            .canonicalize()
            .wrap_err_with(|| format!("repo `{}` does not exist", repo.display()))?;
        Ok(Self {
            repo,
            branch: branch.into(),
            base: base.into(),
        })
    }

    /// The `crew/<role>` branches in the repo, the ones an integration merges (issue #43).
    ///
    /// Excludes the integration branch itself, so re-integrating does not merge the branch
    /// into itself. Returned in git's stable sorted order.
    ///
    /// # Errors
    /// Returns an error if git cannot list the branches.
    pub fn role_branches(&self) -> Result<Vec<String>> {
        let pattern = format!("{ROLE_BRANCH_PREFIX}*");
        let out = git(
            &self.repo,
            &[
                "branch",
                "--list",
                pattern.as_str(),
                "--format=%(refname:short)",
            ],
        )?;
        Ok(out
            .lines()
            .map(str::trim)
            .filter(|branch| !branch.is_empty() && *branch != self.branch)
            .map(str::to_owned)
            .collect())
    }

    /// Integrates `branches` into the integration branch, optionally running `check` on the
    /// merged result.
    ///
    /// Resets the integration branch to `base` in a dedicated worktree, then merges each
    /// branch in order. A conflicting merge is aborted and recorded (its branch and the files
    /// it collides on) rather than force-applied, so no role's work is silently dropped. When
    /// every branch merges cleanly and `check` is given, it runs on the integrated result (via
    /// `sh -c`), and the standing reflects whether it passed. A merge that conflicts skips the
    /// checks, since the integration is not yet whole.
    ///
    /// The integration worktree is cleaned up afterward; the integration branch keeps the
    /// merged commits for the operator to push and open as a pull request.
    ///
    /// # Errors
    /// Returns an error if git cannot set up the integration worktree, or a merge fails for a
    /// reason other than a resolvable conflict.
    pub fn integrate(&self, branches: &[String], check: Option<&str>) -> Result<IntegrationReport> {
        let workdir = self.setup_worktree()?;
        let result = self.run(&workdir, branches, check);
        // Always clean up the integration worktree; the branch keeps the merged commits.
        let workdir_str = workdir.to_string_lossy();
        let _ = git(
            &self.repo,
            &["worktree", "remove", "--force", workdir_str.as_ref()],
        );
        result
    }

    /// Creates the integration worktree, reset to `base`, idempotently.
    fn setup_worktree(&self) -> Result<PathBuf> {
        // A dedicated worktree keyed by the repo, so the integration never disturbs the main
        // checkout or the role worktrees, and a stale one from a prior run is replaced.
        let key = self.repo.to_string_lossy().replace(['/', '\\', ':'], "_");
        let workdir = std::env::temp_dir().join(format!("crew-integration-{key}"));
        let workdir_str = workdir.to_string_lossy();

        let _ = git(&self.repo, &["worktree", "prune"]);
        let _ = git(
            &self.repo,
            &["worktree", "remove", "--force", workdir_str.as_ref()],
        );
        git(
            &self.repo,
            &[
                "worktree",
                "add",
                "-f",
                "-B",
                self.branch.as_str(),
                workdir_str.as_ref(),
                self.base.as_str(),
            ],
        )
        .wrap_err("could not set up the integration worktree")?;
        Ok(workdir)
    }

    /// Merges each branch and, if clean, runs the check, folding it all into a report.
    fn run(
        &self,
        workdir: &Path,
        branches: &[String],
        check: Option<&str>,
    ) -> Result<IntegrationReport> {
        let mut merged = Vec::new();
        let mut conflicts = Vec::new();
        for branch in branches {
            match merge_one(workdir, branch)? {
                MergeResult::Clean => merged.push(branch.clone()),
                MergeResult::Conflicted(files) => conflicts.push(Conflict {
                    branch: branch.clone(),
                    files,
                }),
            }
        }

        // Only judge the whole once it is whole: a conflict means the integration is
        // incomplete, so the acceptance checks wait until the conflicts are resolved.
        let check = if conflicts.is_empty() {
            check
                .map(|command| run_check(workdir, command))
                .transpose()?
        } else {
            None
        };

        let standing = if conflicts.is_empty() {
            match &check {
                None => Standing::Merged,
                Some(outcome) if outcome.passed => Standing::Green,
                Some(_) => Standing::ChecksFailed,
            }
        } else {
            Standing::Conflicts
        };

        event!(
            name: "supervisor.integrate.done",
            Level::INFO,
            crew.branch = %self.branch,
            merged = merged.len(),
            conflicts = conflicts.len(),
            green = standing.is_green(),
            "integrated {{merged}} branches onto `{{crew.branch}}` ({{conflicts}} conflicts)",
        );

        Ok(IntegrationReport {
            branch: self.branch.clone(),
            base: self.base.clone(),
            merged,
            conflicts,
            check,
            standing,
        })
    }
}

/// One branch's merge outcome into the integration worktree.
enum MergeResult {
    /// The branch merged cleanly.
    Clean,
    /// The branch conflicts on these files; the merge was aborted, nothing dropped.
    Conflicted(Vec<String>),
}

/// Merges `branch` into the integration worktree, aborting and reporting a conflict.
fn merge_one(workdir: &Path, branch: &str) -> Result<MergeResult> {
    let attempt = run_git(workdir, &["merge", "--no-ff", "--no-edit", branch]);
    if attempt.ok {
        return Ok(MergeResult::Clean);
    }

    // A merge that stopped on a conflict leaves the conflicting files marked `U`nmerged.
    let files: Vec<String> = git(workdir, &["diff", "--name-only", "--diff-filter=U"])
        .map(|out| out.lines().map(str::to_owned).collect())
        .unwrap_or_default();
    // Abort so the integration branch stays clean up to the last good merge; the conflict is
    // reported, not left half-applied.
    let _ = run_git(workdir, &["merge", "--abort"]);

    if files.is_empty() {
        // The merge failed for a reason other than a conflict (e.g. the branch is missing).
        bail!("merging `{branch}` failed: {}", attempt.stderr.trim());
    }
    Ok(MergeResult::Conflicted(files))
}

/// Runs the acceptance `command` in the integrated worktree via the shell, capturing its
/// standing and the tail of its output.
fn run_check(workdir: &Path, command: &str) -> Result<CheckOutcome> {
    let output = Command::new("sh")
        .arg("-c")
        .arg(command)
        .current_dir(workdir)
        .output()
        .wrap_err_with(|| format!("could not run the acceptance check `{command}`"))?;
    let mut combined = String::from_utf8_lossy(&output.stdout).into_owned();
    combined.push_str(&String::from_utf8_lossy(&output.stderr));
    Ok(CheckOutcome {
        command: command.to_owned(),
        passed: output.status.success(),
        output: tail(&combined, MAX_CHECK_OUTPUT_LINES),
    })
}

/// The last `lines` lines of `text`, so a report carries the diagnostic tail, not a whole log.
fn tail(text: &str, lines: usize) -> String {
    let all: Vec<&str> = text.lines().collect();
    let start = all.len().saturating_sub(lines);
    all[start..].join("\n")
}

/// The outcome of a git command run where a non-zero exit is expected (a merge, an abort).
struct GitRun {
    ok: bool,
    stderr: String,
}

/// Runs a git command in `dir` without failing on a non-zero exit, for the merge path where a
/// conflict is a non-zero exit to inspect, not an error to propagate.
fn run_git(dir: &Path, args: &[&str]) -> GitRun {
    match Command::new("git").arg("-C").arg(dir).args(args).output() {
        Ok(out) => GitRun {
            ok: out.status.success(),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
        },
        Err(err) => GitRun {
            ok: false,
            stderr: format!("could not run git: {err}"),
        },
    }
}

/// Runs a git command in `dir`, returning its stdout or an error carrying stderr.
fn git(dir: &Path, args: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .arg("-C")
        .arg(dir)
        .args(args)
        .output()
        .map_err(|err| eyre!("could not run git: {err}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("git {} failed: {}", args.join(" "), stderr.trim());
    }
    Ok(String::from_utf8_lossy(&output.stdout).into_owned())
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::{git, Integrator, Standing, DEFAULT_INTEGRATION_BRANCH};

    /// A fresh temp directory unique to a test, cleaned on entry.
    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("crew-integrate-test-{name}"));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// Initializes a git repo with one commit on `main`.
    fn init_repo(dir: &Path) {
        git(dir, &["init", "-q", "-b", "main"]).unwrap();
        git(dir, &["config", "user.email", "crew@test"]).unwrap();
        git(dir, &["config", "user.name", "crew"]).unwrap();
        write(dir, "base.txt", "base\n");
        git(dir, &["add", "."]).unwrap();
        git(dir, &["commit", "-q", "-m", "base"]).unwrap();
    }

    /// Commits `contents` to `file` on a fresh `crew/<role>` branch off `main`, then returns
    /// to `main`, mirroring what a role worktree leaves behind (issue #43).
    fn role_branch(repo: &Path, role: &str, file: &str, contents: &str) {
        git(
            repo,
            &["checkout", "-q", "-b", &format!("crew/{role}"), "main"],
        )
        .unwrap();
        write(repo, file, contents);
        git(repo, &["add", "."]).unwrap();
        git(repo, &["commit", "-q", "-m", &format!("{role} work")]).unwrap();
        git(repo, &["checkout", "-q", "main"]).unwrap();
    }

    fn write(dir: &Path, file: &str, contents: &str) {
        std::fs::write(dir.join(file), contents).unwrap();
    }

    #[test]
    fn parallel_work_on_separate_files_integrates_into_a_coherent_branch() {
        let repo = scratch("clean").join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        init_repo(&repo);
        role_branch(&repo, "backend", "api.rs", "backend\n");
        role_branch(&repo, "frontend", "ui.rs", "frontend\n");

        let integrator = Integrator::new(&repo, DEFAULT_INTEGRATION_BRANCH, "main").unwrap();
        let branches = integrator.role_branches().unwrap();
        assert_eq!(branches, ["crew/backend", "crew/frontend"]);

        let report = integrator.integrate(&branches, None).unwrap();
        assert_eq!(report.standing, Standing::Merged);
        assert_eq!(report.merged, ["crew/backend", "crew/frontend"]);
        assert!(report.conflicts.is_empty());

        // The integration branch carries both roles' work.
        let files = git(
            &repo,
            &["ls-tree", "-r", "--name-only", DEFAULT_INTEGRATION_BRANCH],
        )
        .unwrap();
        assert!(files.contains("api.rs") && files.contains("ui.rs"));
    }

    #[test]
    fn a_conflict_is_surfaced_with_its_files_and_not_dropped() {
        let repo = scratch("conflict").join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        init_repo(&repo);
        // Both roles edit the same file differently: a real conflict.
        role_branch(&repo, "backend", "shared.rs", "backend version\n");
        role_branch(&repo, "frontend", "shared.rs", "frontend version\n");

        let integrator = Integrator::new(&repo, DEFAULT_INTEGRATION_BRANCH, "main").unwrap();
        let branches = integrator.role_branches().unwrap();
        let report = integrator.integrate(&branches, Some("true")).unwrap();

        assert_eq!(report.standing, Standing::Conflicts);
        // The first branch merges; the second conflicts and is reported, not dropped.
        assert_eq!(report.merged, ["crew/backend"]);
        assert_eq!(report.conflicts.len(), 1);
        assert_eq!(report.conflicts[0].branch, "crew/frontend");
        assert_eq!(report.conflicts[0].files, ["shared.rs"]);
        // The checks did not run on an incomplete integration.
        assert!(report.check.is_none());
    }

    #[test]
    fn a_green_check_makes_the_integration_green() {
        let repo = scratch("green").join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        init_repo(&repo);
        role_branch(&repo, "backend", "api.rs", "backend\n");

        let integrator = Integrator::new(&repo, DEFAULT_INTEGRATION_BRANCH, "main").unwrap();
        let branches = integrator.role_branches().unwrap();
        let report = integrator
            .integrate(&branches, Some("test -f api.rs"))
            .unwrap();

        assert_eq!(report.standing, Standing::Green);
        assert!(report.standing.is_green());
        let check = report.check.unwrap();
        assert!(check.passed);
    }

    #[test]
    fn a_failing_check_fails_the_integration_even_when_the_merge_is_clean() {
        let repo = scratch("checkfail").join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        init_repo(&repo);
        role_branch(&repo, "backend", "api.rs", "backend\n");

        let integrator = Integrator::new(&repo, DEFAULT_INTEGRATION_BRANCH, "main").unwrap();
        let branches = integrator.role_branches().unwrap();
        // A check that always fails stands in for a broken build or a red test.
        let report = integrator.integrate(&branches, Some("exit 1")).unwrap();

        assert_eq!(report.standing, Standing::ChecksFailed);
        assert!(!report.standing.is_green(), "a red check is not green");
        assert!(!report.check.unwrap().passed);
    }

    #[test]
    fn integrating_twice_is_idempotent() {
        let repo = scratch("idempotent").join("repo");
        std::fs::create_dir_all(&repo).unwrap();
        init_repo(&repo);
        role_branch(&repo, "backend", "api.rs", "backend\n");

        let integrator = Integrator::new(&repo, DEFAULT_INTEGRATION_BRANCH, "main").unwrap();
        let branches = integrator.role_branches().unwrap();
        let first = integrator.integrate(&branches, None).unwrap();
        let second = integrator.integrate(&branches, None).unwrap();
        assert_eq!(first.standing, Standing::Merged);
        assert_eq!(second.standing, Standing::Merged);
        assert_eq!(second.merged, ["crew/backend"]);
    }
}
