//! Process allocator figures for the `/metrics` endpoint (#1170): a whole-
//! process RSS number cannot say which subsystem grew (the ClickBench OOM
//! this module exists to make attributable retracted two published memory
//! claims for exactly that reason), so this reports the allocator's own
//! breakdown instead. `main.rs` compiles jemalloc in as the global allocator
//! on every target this repo builds for (`#[cfg(not(target_env = "msvc"))]`,
//! and this repo does not target msvc); this module names that fact plainly
//! rather than silently formatting zeros for an allocator that is not
//! actually configured.

/// The three jemalloc-native figures (`stats.allocated`/`active`/`resident`)
/// on a build where jemalloc is the global allocator, or an explicit marker
/// naming whichever allocator this process actually uses instead. There is
/// no all-zeros case: a non-jemalloc build says so by name rather than
/// reporting jemalloc figures it does not have.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllocatorStats {
    Jemalloc {
        allocated: u64,
        active: u64,
        resident: u64,
    },
    Other {
        name: &'static str,
    },
}

/// Reads the live figures from whichever allocator this binary actually
/// links. On every target this repo builds, `main.rs` sets
/// `#[global_allocator]` to jemalloc, so this is a real mallctl read of the
/// process's own allocator at call time, not a cached or startup-time
/// snapshot: `epoch::advance` refreshes jemalloc's cached stats immediately
/// before each read.
#[cfg(not(target_env = "msvc"))]
pub fn read() -> AllocatorStats {
    use tikv_jemalloc_ctl::{epoch, stats};

    // A refresh or read failure here is a diagnostic-path degradation, not a
    // correctness path: fall back to 0 for that one figure rather than
    // panicking a scrape over a mallctl hiccup. This is distinct from "not
    // running under jemalloc," which this function never claims via a zero --
    // that state is reported by the `Other` variant below, on msvc only.
    let _ = epoch::advance();
    AllocatorStats::Jemalloc {
        allocated: stats::allocated::read().unwrap_or(0) as u64,
        active: stats::active::read().unwrap_or(0) as u64,
        resident: stats::resident::read().unwrap_or(0) as u64,
    }
}

/// msvc has no jemalloc global allocator (`tikv-jemallocator`'s own
/// documented unsupported target, `main.rs`'s `#[global_allocator]` is
/// `#[cfg]`-absent here), so this process runs under Rust's default `System`
/// allocator instead.
#[cfg(target_env = "msvc")]
pub fn read() -> AllocatorStats {
    AllocatorStats::Other { name: "system" }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    /// Non-vacuity: on every target this repo actually builds for, `read`
    /// must report the `Jemalloc` variant, and by the time any test runs this
    /// process has allocated well past zero, so each figure must be nonzero.
    /// This is a smoke check, not a magnitude assertion -- jemalloc's own
    /// allocated-bytes accounting under a real allocation delta is exercised
    /// by `main.rs`'s `binary_runs_under_jemalloc`, the regression gate for
    /// "is jemalloc actually linked as the global allocator."
    #[cfg(not(target_env = "msvc"))]
    #[test]
    fn read_reports_jemalloc_with_nonzero_figures() {
        match read() {
            AllocatorStats::Jemalloc {
                allocated,
                active,
                resident,
            } => {
                assert!(allocated > 0, "a running process has allocated bytes");
                assert!(active > 0, "a running process has active bytes");
                assert!(resident > 0, "a running process has resident bytes");
            }
            AllocatorStats::Other { name } => {
                panic!("expected Jemalloc on a non-msvc target, got Other({name})");
            }
        }
    }
}
