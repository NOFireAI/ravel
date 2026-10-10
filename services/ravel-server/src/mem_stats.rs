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

/// Whether this build runs under jemalloc, the allocator the memory gate's
/// reading and purge need (ADR-2633 section 2).
pub const JEMALLOC: bool = cfg!(not(target_env = "msvc"));

/// Refreshes jemalloc's stats epoch and reads `stats.resident`, the memory
/// gate's one reading. `None` when either call fails.
#[cfg(not(target_env = "msvc"))]
pub fn read_resident() -> Option<u64> {
    use tikv_jemalloc_ctl::{epoch, stats};

    epoch::advance().ok()?;
    stats::resident::read().ok().map(|resident| resident as u64)
}

/// No jemalloc on msvc, so nothing to read.
#[cfg(target_env = "msvc")]
pub fn read_resident() -> Option<u64> {
    None
}

/// What one forced purge did, arena by arena.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct PurgeReport {
    /// Arenas whose decay settings were set to 0 and back.
    pub arenas_purged: u32,
    /// Arena indices below `arenas.narenas` whose decay settings could not be
    /// read or set, which jemalloc answers with EFAULT for an uninitialised
    /// arena. Nothing was changed on them.
    pub arenas_skipped: u32,
    /// Decay writes that set a value back and failed, leaving that arena
    /// purging eagerly until something else writes it.
    pub restore_failures: u32,
}

/// Forces jemalloc to return every initialised arena's unused dirty and muzzy
/// pages to the operating system (ADR-2633 section 2). For each arena below
/// `arenas.narenas` it sets `muzzy_decay_ms` to 0 when it is nonzero, sets
/// `dirty_decay_ms` to 0 (jemalloc purges synchronously on a write of 0), then
/// restores both to the values the writes returned. The `tikv-jemalloc-ctl`
/// error type exposes no errno, so every failed read or set is treated as
/// the EFAULT of an uninitialised arena and the arena is skipped.
#[cfg(not(target_env = "msvc"))]
pub fn purge_arenas() -> PurgeReport {
    use tikv_jemalloc_ctl::{Access, AsName, arenas};

    let mut report = PurgeReport::default();
    let narenas = match arenas::narenas::read() {
        Ok(narenas) => narenas,
        Err(err) => {
            tracing::warn!(error = %err, "could not read arenas.narenas; no purge ran");
            return report;
        }
    };
    for index in 0..narenas {
        let muzzy_key = format!("arena.{index}.muzzy_decay_ms\0");
        let dirty_key = format!("arena.{index}.dirty_decay_ms\0");
        let muzzy = muzzy_key.as_str().name();
        let dirty = dirty_key.as_str().name();

        let muzzy_current: isize = match muzzy.read() {
            Ok(value) => value,
            Err(_) => {
                report.arenas_skipped += 1;
                continue;
            }
        };
        let muzzy_restore = if muzzy_current != 0 {
            match muzzy.update(0_isize) {
                Ok(previous) => Some(previous),
                Err(_) => {
                    report.arenas_skipped += 1;
                    continue;
                }
            }
        } else {
            None
        };
        match dirty.update(0_isize) {
            Ok(previous) => {
                if dirty.write(previous).is_err() {
                    report.restore_failures += 1;
                }
                report.arenas_purged += 1;
            }
            Err(_) => report.arenas_skipped += 1,
        }
        if let Some(previous) = muzzy_restore
            && muzzy.write(previous).is_err()
        {
            report.restore_failures += 1;
        }
    }
    if report.restore_failures > 0 {
        tracing::warn!(
            restore_failures = report.restore_failures,
            "memory gate purge could not restore some arena decay settings; those arenas \
             purge eagerly until restarted"
        );
    }
    report
}

/// No jemalloc on msvc, so nothing to purge.
#[cfg(target_env = "msvc")]
pub fn purge_arenas() -> PurgeReport {
    PurgeReport::default()
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
