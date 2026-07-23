//! `crew integrate`: merge the roles' branches into one coherent, green branch (issue #44).
//!
//! Parallel roles work in isolated worktrees on `crew/<role>` branches (issue #43). This
//! command runs the integration step over them: it merges each role branch into an
//! integration branch, surfaces any conflicts precisely (rather than force-merging and
//! dropping a role's work), and runs the acceptance checks (build, tests) on the merged
//! result when `--check` is given. It prints where the integration stands, so the operator
//! (or the commander) knows the whole is green before declaring the work done.
//!
//! The integration branch keeps the merged commits for the operator to push and open as a
//! pull request; the stacked-PR strategy for roles that build on each other is to order the
//! branches so a dependency merges before its dependents (see `docs/roles.md`).

use std::fmt::Write as _;
use std::path::Path;

use crew_substrate::supervisor::{IntegrationReport, Integrator, Standing};
use eyre::{Result, WrapErr};

/// Merges the crew's role branches into an integration branch and reports the standing.
///
/// Discovers the `crew/<role>` branches in `repo`, merges them into `branch` cut from `base`,
/// and, when `check` is given, runs it on the integrated result. Prints the report.
///
/// # Errors
/// Returns an error if `repo` is not a git repository, or git cannot list or merge the
/// branches.
pub fn integrate(repo: &str, base: &str, branch: &str, check: Option<&str>) -> Result<()> {
    let integrator = Integrator::new(Path::new(repo), branch, base)
        .wrap_err("could not start the integration")?;
    let branches = integrator
        .role_branches()
        .wrap_err("could not list the crew/<role> branches to integrate")?;

    if branches.is_empty() {
        println!("No crew/<role> branches to integrate in {repo}.");
        return Ok(());
    }

    let report = integrator
        .integrate(&branches, check)
        .wrap_err("the integration could not run")?;
    println!("{}", render(&report));
    Ok(())
}

/// Renders the integration report for the operator: what merged, what conflicted, and the
/// overall standing.
fn render(report: &IntegrationReport) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "Integrated onto {} (from {}):",
        report.branch, report.base
    );

    if report.merged.is_empty() {
        out.push_str("  merged: nothing\n");
    } else {
        let _ = writeln!(out, "  merged: {}", report.merged.join(", "));
    }

    for conflict in &report.conflicts {
        let _ = writeln!(
            out,
            "  CONFLICT: {} collides on {}",
            conflict.branch,
            conflict.files.join(", ")
        );
    }

    if let Some(check) = &report.check {
        let verdict = if check.passed { "passed" } else { "FAILED" };
        let _ = writeln!(out, "  check `{}`: {verdict}", check.command);
        if !check.passed && !check.output.is_empty() {
            out.push_str("  ---\n");
            for line in check.output.lines() {
                let _ = writeln!(out, "  {line}");
            }
            out.push_str("  ---\n");
        }
    }

    out.push_str(&standing_line(report.standing, &report.branch));
    out
}

/// The closing line: what the standing means and what to do next.
fn standing_line(standing: Standing, branch: &str) -> String {
    match standing {
        Standing::Green => format!(
            "GREEN: every branch merged and the checks passed. Push `{branch}` and open a PR."
        ),
        Standing::Merged => format!(
            "MERGED: every branch merged cleanly (no checks run). Run the acceptance checks, \
             then push `{branch}`."
        ),
        Standing::Conflicts => {
            "CONFLICTS: resolve the conflicts above (merge each branch by hand and fix them, \
             or redraw the lanes), then integrate again. Nothing was dropped."
                .to_owned()
        }
        Standing::ChecksFailed => format!(
            "CHECKS FAILED: the branches merged but the acceptance checks are red on `{branch}`. \
             Fix the integrated result before declaring done."
        ),
    }
}

#[cfg(test)]
mod tests {
    use crew_substrate::supervisor::{CheckOutcome, Conflict, IntegrationReport, Standing};

    use super::render;

    fn report(standing: Standing) -> IntegrationReport {
        IntegrationReport {
            branch: "crew/integration".to_owned(),
            base: "main".to_owned(),
            merged: vec!["crew/backend".to_owned()],
            conflicts: Vec::new(),
            check: None,
            standing,
        }
    }

    #[test]
    fn a_conflict_is_shown_with_its_files_and_says_nothing_was_dropped() {
        let mut report = report(Standing::Conflicts);
        report.conflicts.push(Conflict {
            branch: "crew/frontend".to_owned(),
            files: vec!["shared.rs".to_owned()],
        });
        let text = render(&report);
        assert!(text.contains("CONFLICT: crew/frontend collides on shared.rs"));
        assert!(text.contains("Nothing was dropped"));
    }

    #[test]
    fn a_green_integration_says_to_push_and_open_a_pr() {
        let text = render(&report(Standing::Green));
        assert!(text.contains("GREEN") && text.contains("open a PR"));
    }

    #[test]
    fn a_failed_check_shows_the_output_tail() {
        let mut report = report(Standing::ChecksFailed);
        report.check = Some(CheckOutcome {
            command: "cargo test".to_owned(),
            passed: false,
            output: "error[E0308]: mismatched types".to_owned(),
        });
        let text = render(&report);
        assert!(text.contains("CHECKS FAILED"));
        assert!(
            text.contains("mismatched types"),
            "shows the failure: {text}"
        );
    }
}
