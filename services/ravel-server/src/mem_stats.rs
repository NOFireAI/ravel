//! Process allocator figures for the `/metrics` endpoint (#1170): a whole-
//! process RSS number cannot say which subsystem grew (the ClickBench OOM
//! this module exists to make attributable retracted two published memory
//! claims for exactly that reason), so this reports the allocator's own
//! breakdown instead. `main.rs` compiles jemalloc in as the global allocator
//! on every target this repo builds for (`#[cfg(not(target_env = "msvc"))]`,
//! and this repo does not target msvc); this module names that fact plainly
//! rather than silently formatting zeros for an allocator that is not
//! actually configured. [`configure_background_thread`] is the startup step
//! that turns on jemalloc's background purge thread (#2633).

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
        /// Whether jemalloc's background purge thread is running, read back
        /// from the allocator at the same time as the byte figures.
        background_thread: bool,
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
    use tikv_jemalloc_ctl::{background_thread, epoch, stats};

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
        background_thread: background_thread::read().unwrap_or(false),
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

/// The environment variable the vendored jemalloc reads its options from. It
/// is built with an `_rjem_` symbol prefix, so it does not read `MALLOC_CONF`.
pub const MALLOC_CONF_ENV: &str = "_RJEM_MALLOC_CONF";

/// The state of jemalloc's background purge thread after the startup step.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BackgroundThread {
    /// Read back from the allocator after the step, not the value written.
    pub enabled: bool,
    /// Who decided the state: `"server"` when the startup step enabled it,
    /// `"malloc_conf"` when the operator's [`MALLOC_CONF_ENV`] names
    /// `background_thread` and the step left it alone, `"not-jemalloc"` when
    /// this process does not run under jemalloc.
    pub source: &'static str,
}

impl BackgroundThread {
    /// The startup stamp: one INFO line naming the read-back state.
    pub fn emit(&self) {
        tracing::info!(
            allocator_background_thread = self.enabled,
            source = self.source,
            "allocator background purge thread resolved"
        );
    }
}

/// Whether a jemalloc option string sets `background_thread` itself.
/// jemalloc's option syntax is comma-separated `key:value` pairs.
fn malloc_conf_names_background_thread(malloc_conf: &str) -> bool {
    malloc_conf.split(',').any(|pair| {
        pair.split_once(':')
            .is_some_and(|(key, _)| key == "background_thread")
    })
}

/// Enables jemalloc's background thread, which purges freed dirty pages on a
/// timer. Without it jemalloc purges only on allocator activity, so an idle
/// process keeps pages it freed resident. `malloc_conf` is the operator's
/// [`MALLOC_CONF_ENV`] value: when it names `background_thread`, either way,
/// this writes nothing, because the safe API cannot tell an explicit
/// `background_thread:false` from jemalloc's default of false. A failed write
/// is a warning, never a startup failure.
#[cfg(not(target_env = "msvc"))]
pub fn configure_background_thread(malloc_conf: Option<&str>) -> BackgroundThread {
    use tikv_jemalloc_ctl::background_thread;

    let source = if malloc_conf.is_some_and(malloc_conf_names_background_thread) {
        "malloc_conf"
    } else {
        if let Err(err) = background_thread::write(true) {
            tracing::warn!(
                error = %err,
                "could not enable the jemalloc background purge thread; freed pages are \
                 returned to the operating system only on allocator activity"
            );
        }
        "server"
    };
    let enabled = match background_thread::read() {
        Ok(enabled) => enabled,
        Err(err) => {
            tracing::warn!(
                error = %err,
                "could not read back the jemalloc background purge thread state"
            );
            false
        }
    };
    BackgroundThread { enabled, source }
}

/// No jemalloc on msvc, so there is no background thread to enable.
#[cfg(target_env = "msvc")]
pub fn configure_background_thread(_malloc_conf: Option<&str>) -> BackgroundThread {
    BackgroundThread {
        enabled: false,
        source: "not-jemalloc",
    }
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
                ..
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

    #[test]
    fn malloc_conf_names_background_thread_only_as_a_key() {
        assert!(malloc_conf_names_background_thread(
            "background_thread:false"
        ));
        assert!(malloc_conf_names_background_thread(
            "prof:true,background_thread:true,lg_prof_sample:17"
        ));
        assert!(!malloc_conf_names_background_thread(""));
        assert!(!malloc_conf_names_background_thread(
            "prof:true,lg_prof_sample:17"
        ));
        assert!(!malloc_conf_names_background_thread(
            "max_background_threads:2"
        ));
        assert!(!malloc_conf_names_background_thread(
            "prof_prefix:background_thread"
        ));
    }

    /// One test, not two: the background thread is process-wide state, and
    /// two tests toggling it would race. This binary does not install jemalloc
    /// as its global allocator, but jemalloc is linked and its mallctl
    /// interface works all the same.
    #[cfg(not(target_env = "msvc"))]
    #[test]
    fn configure_respects_an_operator_setting_and_enables_otherwise() {
        use tikv_jemalloc_ctl::background_thread;

        background_thread::write(false).expect("background_thread write must succeed");
        let respected = configure_background_thread(Some("prof:false,background_thread:false"));
        assert_eq!(
            respected,
            BackgroundThread {
                enabled: false,
                source: "malloc_conf",
            },
            "an operator's background_thread:false must not be overridden"
        );
        assert!(!background_thread::read().expect("background_thread read must succeed"));

        let enabled = configure_background_thread(Some("prof:false"));
        assert_eq!(
            enabled,
            BackgroundThread {
                enabled: true,
                source: "server",
            }
        );
        assert!(background_thread::read().expect("background_thread read must succeed"));
    }
}
