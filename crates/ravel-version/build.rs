//! Captures the short commit sha `ravel_version::version()`'s git-derived
//! branch needs, at compile time. The released-image build has no `.git`
//! (`.dockerignore` excludes it; see the crate's `lib.rs`), so `git
//! rev-parse` failing here is an expected, silent no-op in that build, not an
//! error: the override branch is what carries the version there instead.

use std::process::Command;

fn main() {
    println!("cargo:rerun-if-env-changed=RAVEL_VERSION_OVERRIDE");

    // Best-effort: triggers a rebuild when HEAD moves. Harmless if `.git` is
    // absent (the path just never changes, so this line never re-fires).
    println!("cargo:rerun-if-changed=../../.git/HEAD");

    let sha = Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty());

    if let Some(sha) = sha {
        println!("cargo:rustc-env=RAVEL_GIT_SHA={sha}");
    }
}
