//! Owned-path lane boundaries: whether a path falls in a role's lane (issue #46).
//!
//! A role owns directory boundaries (its lane in the tree, see `docs/roles.md`). Lane
//! enforcement keeps a role from wandering into another's lane: a path is **in lane**
//! when it sits under one of the role's owned boundaries, and **out of lane** otherwise.
//!
//! A role that owns no lane (the commander routes and owns nothing) is unrestricted:
//! it has no boundary to cross, so every path is in lane for it.

/// Whether `path` sits within one of the `owned_paths` boundaries.
///
/// Each owned entry is a directory boundary (`api/`) or a specific file; `path` is in
/// lane when it equals an owned file or sits under an owned directory. Trailing slashes
/// and a leading `./` do not matter, and a boundary matches on a whole path segment, so
/// `api/` owns `api/routes.rs` but not `apiv2/routes.rs`. A role with no owned paths is
/// unrestricted (every path is in lane).
///
/// # Examples
/// ```
/// use crew_core::path_in_lane;
///
/// let lane = ["api/".to_owned(), "db/".to_owned()];
/// assert!(path_in_lane(&lane, "api/routes.rs"));
/// assert!(path_in_lane(&lane, "db/migrations/001.sql"));
/// assert!(!path_in_lane(&lane, "frontend/app.tsx"));
/// assert!(!path_in_lane(&lane, "apiv2/routes.rs"), "a boundary matches a whole segment");
///
/// // A role with no lane is unrestricted.
/// assert!(path_in_lane(&[], "anywhere/at/all"));
/// ```
#[must_use]
pub fn path_in_lane(owned_paths: &[String], path: &str) -> bool {
    let path = normalize(path);
    // A role with no declared lane owns nothing to cross, so it is unrestricted.
    let mut has_lane = false;
    for owned in owned_paths {
        let owned = normalize(owned);
        if owned.is_empty() {
            continue;
        }
        has_lane = true;
        // In lane when the path is the owned file, or sits under the owned directory.
        if path == owned || path.starts_with(&format!("{owned}/")) {
            return true;
        }
    }
    !has_lane
}

/// Normalizes a path for boundary comparison: trims whitespace, a leading `./`, and any
/// trailing slash, so `./api/` and `api` compare equal.
fn normalize(path: &str) -> String {
    path.trim()
        .trim_start_matches("./")
        .trim_end_matches('/')
        .to_owned()
}

#[cfg(test)]
mod tests {
    use super::path_in_lane;

    fn lane(paths: &[&str]) -> Vec<String> {
        paths.iter().map(|p| (*p).to_owned()).collect()
    }

    #[test]
    fn a_path_under_an_owned_directory_is_in_lane() {
        let lane = lane(&["api/", "db/"]);
        assert!(path_in_lane(&lane, "api/routes.rs"));
        assert!(path_in_lane(&lane, "api/handlers/login.rs"));
        assert!(path_in_lane(&lane, "db/schema.sql"));
    }

    #[test]
    fn a_path_outside_every_boundary_is_out_of_lane() {
        let lane = lane(&["api/"]);
        assert!(!path_in_lane(&lane, "frontend/app.tsx"));
        assert!(!path_in_lane(&lane, "README.md"));
    }

    #[test]
    fn a_boundary_matches_a_whole_segment_not_a_prefix() {
        // `api/` must not own `apiv2/`, which merely shares a text prefix.
        let lane = lane(&["api/"]);
        assert!(!path_in_lane(&lane, "apiv2/routes.rs"));
        assert!(!path_in_lane(&lane, "apix"));
    }

    #[test]
    fn trailing_slashes_and_dot_prefixes_do_not_matter() {
        let lane = lane(&["api"]);
        assert!(path_in_lane(&lane, "./api/routes.rs"));
        assert!(path_in_lane(&lane, "api/"));
        assert!(path_in_lane(&lane, "api"));
    }

    #[test]
    fn a_specific_owned_file_is_in_lane_only_for_itself() {
        let lane = lane(&["Cargo.toml"]);
        assert!(path_in_lane(&lane, "Cargo.toml"));
        assert!(!path_in_lane(&lane, "Cargo.lock"));
    }

    #[test]
    fn a_role_with_no_lane_is_unrestricted() {
        assert!(path_in_lane(&[], "anything/goes"));
        // A lane of only-blank entries is the same as no lane.
        assert!(path_in_lane(&lane(&["", "  "]), "anything/goes"));
    }
}
