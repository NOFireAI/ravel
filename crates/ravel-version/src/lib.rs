//! `--version` for every shipped Ravel binary (issue #1177), resolved once
//! here so `ravel-server`, `ravel-cli`, `ravel-operator`, and
//! `ravel-ingest-router` cannot drift from each other.
//!
//! Resolution order:
//!
//! 1. [`OVERRIDE_ENV`] (`RAVEL_VERSION_OVERRIDE`), if it was set when this
//!    crate was BUILT: used verbatim. This is the only path a released image
//!    can take -- `.dockerignore` excludes `.git`, so the git-derived branch
//!    below always misses inside an image build -- and
//!    `.github/workflows/publish-images.yml` sets it from the release tag
//!    through the matching Dockerfile build argument.
//! 2. Otherwise, the short commit sha `build.rs` captured via `git
//!    rev-parse` at compile time, combined with the workspace version's next
//!    minor: `v0.14.0-f00b4r` for a workspace at `0.13.x`. This says "newer
//!    than the last release, not yet the next one", which is what a binary
//!    built from a commit is.
//! 3. Otherwise (no override, no git metadata -- a source tree with neither),
//!    a fallback that is visibly not a real version rather than a wrong one.

/// Name of the build-time override environment variable. The Dockerfile
/// build argument and `publish-images.yml` both use this exact name.
pub const OVERRIDE_ENV: &str = "RAVEL_VERSION_OVERRIDE";

/// The resolved version string, e.g. `v0.13.0` or `v0.14.0-f00b4r`.
pub fn version() -> String {
    resolve(
        option_env!("RAVEL_VERSION_OVERRIDE"),
        option_env!("RAVEL_GIT_SHA"),
        env!("CARGO_PKG_VERSION"),
    )
}

/// The resolution logic, parameterized so it can be exercised with known
/// inputs: [`version`] is the only real caller, feeding it the three
/// compile-time-baked sources in order.
pub fn resolve(override_version: Option<&str>, git_sha: Option<&str>, pkg_version: &str) -> String {
    if let Some(v) = override_version.filter(|s| !s.is_empty()) {
        return v.to_string();
    }
    match git_sha.filter(|s| !s.is_empty()) {
        Some(sha) => format!("v{}-{sha}", next_minor(pkg_version)),
        None => "v0.0.0-unknown".to_string(),
    }
}

/// `major.minor.0` one minor past `pkg_version` (`0.13.7` -> `0.14.0`).
pub fn next_minor(pkg_version: &str) -> String {
    let mut parts = pkg_version.split('.');
    let major: u64 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    let minor: u64 = parts.next().and_then(|s| s.parse().ok()).unwrap_or(0);
    format!("{major}.{}.0", minor + 1)
}

/// Parse `C` from the real process arguments with [`version`] wired in as
/// `--version`'s output. The one call site every shipped binary's `main`
/// uses in place of `C::parse()`, so this resolution order applies
/// identically to all four.
pub fn parse<C: clap::CommandFactory + clap::FromArgMatches>() -> C {
    let command = C::command().version(version());
    let matches = command.get_matches();
    C::from_arg_matches(&matches).unwrap_or_else(|e| e.exit())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn override_wins_verbatim_over_git_metadata() {
        assert_eq!(
            resolve(Some("v0.13.0"), Some("f00b4r"), "0.13.7"),
            "v0.13.0"
        );
    }

    #[test]
    fn empty_override_is_treated_as_absent() {
        assert_eq!(
            resolve(Some(""), Some("f00b4r"), "0.13.0"),
            "v0.14.0-f00b4r"
        );
    }

    #[test]
    fn git_sha_without_override_uses_next_minor_and_short_sha() {
        assert_eq!(resolve(None, Some("f00b4r"), "0.13.0"), "v0.14.0-f00b4r");
    }

    #[test]
    fn minor_rolls_from_0_13_x_to_0_14_0() {
        assert_eq!(next_minor("0.13.0"), "0.14.0");
        assert_eq!(next_minor("0.13.9"), "0.14.0");
    }

    #[test]
    fn neither_source_falls_back_to_an_obviously_fake_version() {
        assert_eq!(resolve(None, None, "0.13.0"), "v0.0.0-unknown");
    }
}
