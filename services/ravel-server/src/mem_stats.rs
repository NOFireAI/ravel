//! Process allocator figures for the `/metrics` endpoint (#1170): a whole-
//! process RSS number cannot say which subsystem grew (the ClickBench OOM
//! this module exists to make attributable retracted two published memory
//! claims for exactly that reason), so this reports the allocator's own
//! breakdown instead. `main.rs` compiles jemalloc in as the global allocator
//! on every target this repo builds for (`#[cfg(not(target_env = "msvc"))]`,
//! and this repo does not target msvc); this module names that fact plainly
//! rather than silently formatting zeros for an allocator that is not
//! actually configured. [`configure_background_thread`] is the startup step
//! that turns on jemalloc's background purge thread (#2633). On macOS
//! jemalloc is built without background-thread support and that mallctl
//! answers ENOENT, so on that platform the step stamps the thread
//! `"unavailable"` with an info line rather than stamping `"server"` and
//! warning as if a write had failed (#2966).

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
    /// `background_thread` and the step left it alone, `"unavailable"` when
    /// this jemalloc build has no background-thread support (macOS),
    /// `"not-jemalloc"` when this process does not run under jemalloc.
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

/// A `background_thread` mallctl result, classified so the startup step can
/// tell a jemalloc build without background-thread support apart from a call
/// that failed.
#[cfg(not(target_env = "msvc"))]
#[derive(Debug)]
enum Mallctl<T> {
    Done(T),
    /// ENOENT: this jemalloc build has no background-thread support (macOS),
    /// so there is no state to set or read.
    Unavailable(tikv_jemalloc_ctl::Error),
    /// Any other error: the mallctl exists and the call itself failed.
    Failed(tikv_jemalloc_ctl::Error),
}

#[cfg(not(target_env = "msvc"))]
fn classify_mallctl<T>(result: tikv_jemalloc_ctl::Result<T>) -> Mallctl<T> {
    match result {
        Ok(value) => Mallctl::Done(value),
        Err(err) if mallctl_name_is_unknown(err) => Mallctl::Unavailable(err),
        Err(err) => Mallctl::Failed(err),
    }
}

/// Whether `err` is jemalloc's ENOENT, its answer for a mallctl this build
/// does not have. `tikv-jemalloc-ctl` exposes no errno, so this compares
/// against the error a read of a name no jemalloc defines returns.
#[cfg(not(target_env = "msvc"))]
fn mallctl_name_is_unknown(err: tikv_jemalloc_ctl::Error) -> bool {
    use tikv_jemalloc_ctl::{Access, AsName};

    let reference: tikv_jemalloc_ctl::Result<bool> = "ravel.no_such_mallctl\0".name().read();
    reference.err() == Some(err)
}

/// Enables jemalloc's background thread, which purges freed dirty pages on a
/// timer. Without it jemalloc purges only on allocator activity, so an idle
/// process keeps pages it freed resident. `malloc_conf` is the operator's
/// [`MALLOC_CONF_ENV`] value: when it names `background_thread`, either way,
/// this writes nothing, because the safe API cannot tell an explicit
/// `background_thread:false` from jemalloc's default of false. A failed write
/// is a warning, never a startup failure. A jemalloc build without
/// background-thread support (macOS) is not a failure: it stamps
/// `"unavailable"` with an info line.
#[cfg(not(target_env = "msvc"))]
pub fn configure_background_thread(malloc_conf: Option<&str>) -> BackgroundThread {
    use tikv_jemalloc_ctl::background_thread;

    resolve_background_thread(
        malloc_conf.is_some_and(malloc_conf_names_background_thread),
        || background_thread::write(true),
        background_thread::read,
    )
}

/// The startup step's decision over injected `write` and `read` mallctl
/// calls. `operator_set` skips the write. An unavailable write returns
/// without calling `read`, which would answer the same way.
#[cfg(not(target_env = "msvc"))]
fn resolve_background_thread(
    operator_set: bool,
    write: impl FnOnce() -> tikv_jemalloc_ctl::Result<()>,
    read: impl FnOnce() -> tikv_jemalloc_ctl::Result<bool>,
) -> BackgroundThread {
    let source = if operator_set {
        "malloc_conf"
    } else {
        match classify_mallctl(write()) {
            Mallctl::Done(()) => {}
            Mallctl::Unavailable(err) => return unavailable(err),
            Mallctl::Failed(err) => {
                tracing::warn!(
                    error = %err,
                    "could not enable the jemalloc background purge thread; freed pages are \
                     returned to the operating system only on allocator activity"
                );
            }
        }
        "server"
    };
    match classify_mallctl(read()) {
        Mallctl::Done(enabled) => BackgroundThread { enabled, source },
        Mallctl::Unavailable(err) => unavailable(err),
        Mallctl::Failed(err) => {
            tracing::warn!(
                error = %err,
                "could not read back the jemalloc background purge thread state"
            );
            BackgroundThread {
                enabled: false,
                source,
            }
        }
    }
}

#[cfg(not(target_env = "msvc"))]
fn unavailable(err: tikv_jemalloc_ctl::Error) -> BackgroundThread {
    tracing::info!(
        error = %err,
        "this jemalloc build has no background purge thread (as on macOS); freed pages are \
         returned to the operating system only on allocator activity"
    );
    BackgroundThread {
        enabled: false,
        source: "unavailable",
    }
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
    /// arena. Their decay settings are left as found.
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
    /// interface works all the same. Linux-only: macOS jemalloc has no
    /// background-thread support (#2966), so the assertions below do not
    /// hold there; the `resolve_` tests below drive that case on any
    /// platform instead.
    #[cfg(all(not(target_env = "msvc"), target_os = "linux"))]
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

    /// The ENOENT jemalloc returns for a mallctl this build does not have,
    /// as `background_thread` answers on macOS (#2966). A different name
    /// from the one `mallctl_name_is_unknown` reads, so the match is on the
    /// error, not the name.
    #[cfg(not(target_env = "msvc"))]
    fn enoent() -> tikv_jemalloc_ctl::Error {
        use tikv_jemalloc_ctl::{Access, AsName};

        let unknown: tikv_jemalloc_ctl::Result<bool> = "ravel.no_such_mallctl_key\0".name().read();
        unknown.expect_err("an unknown mallctl name must fail")
    }

    /// A mallctl that exists refusing the call: `config.*` is read-only, so
    /// jemalloc answers a write with EPERM.
    #[cfg(not(target_env = "msvc"))]
    fn eperm() -> tikv_jemalloc_ctl::Error {
        use tikv_jemalloc_ctl::{Access, AsName};

        "config.debug\0"
            .name()
            .write(true)
            .expect_err("config.debug is read-only")
    }

    /// Runs `f` under a subscriber that captures every INFO-and-above line.
    #[cfg(not(target_env = "msvc"))]
    fn with_captured_log(f: impl FnOnce() -> BackgroundThread) -> (BackgroundThread, String) {
        #[derive(Clone)]
        struct CapturedLog(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

        impl std::io::Write for CapturedLog {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        let buffer = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let writer = CapturedLog(buffer.clone());
        let subscriber = tracing_subscriber::fmt()
            .with_writer(move || writer.clone())
            .with_max_level(tracing::Level::INFO)
            .with_ansi(false)
            .finish();
        let state = tracing::subscriber::with_default(subscriber, f);
        let logged = String::from_utf8(buffer.lock().unwrap().clone()).unwrap();
        (state, logged)
    }

    #[cfg(not(target_env = "msvc"))]
    #[test]
    fn classify_mallctl_separates_an_unknown_mallctl_from_a_refused_call() {
        assert!(matches!(
            classify_mallctl(Err::<(), _>(enoent())),
            Mallctl::Unavailable(_)
        ));
        let refused = eperm();
        assert!(
            matches!(classify_mallctl(Err::<(), _>(refused)), Mallctl::Failed(_)),
            "a mallctl that exists and refuses the call is a failure, got: {refused}"
        );
        assert!(matches!(classify_mallctl(Ok(true)), Mallctl::Done(true)));
    }

    #[cfg(not(target_env = "msvc"))]
    #[test]
    fn resolve_stamps_an_unavailable_write_without_a_read_back_or_warning() {
        let (state, logged) = with_captured_log(|| {
            resolve_background_thread(
                false,
                || Err(enoent()),
                || panic!("no read-back after an unavailable write"),
            )
        });
        assert_eq!(
            state,
            BackgroundThread {
                enabled: false,
                source: "unavailable",
            }
        );
        assert_eq!(logged.lines().count(), 1, "got: {logged:?}");
        assert_eq!(logged.matches(" INFO ").count(), 1, "got: {logged:?}");
        assert!(!logged.contains("WARN"), "got: {logged:?}");
    }

    #[cfg(not(target_env = "msvc"))]
    #[test]
    fn resolve_warns_on_a_refused_write_and_stamps_the_read_back() {
        let (state, logged) =
            with_captured_log(|| resolve_background_thread(false, || Err(eperm()), || Ok(false)));
        assert_eq!(
            state,
            BackgroundThread {
                enabled: false,
                source: "server",
            }
        );
        assert_eq!(logged.lines().count(), 1, "got: {logged:?}");
        assert_eq!(logged.matches(" WARN ").count(), 1, "got: {logged:?}");
    }

    /// An operator's `background_thread` setting on a build without
    /// background-thread support: the read-back is unavailable, not a
    /// failure.
    #[cfg(not(target_env = "msvc"))]
    #[test]
    fn resolve_stamps_an_unavailable_read_back_of_an_operator_setting() {
        let (state, logged) = with_captured_log(|| {
            resolve_background_thread(
                true,
                || panic!("no write when the operator set the thread"),
                || Err(enoent()),
            )
        });
        assert_eq!(
            state,
            BackgroundThread {
                enabled: false,
                source: "unavailable",
            }
        );
        assert_eq!(logged.lines().count(), 1, "got: {logged:?}");
        assert_eq!(logged.matches(" INFO ").count(), 1, "got: {logged:?}");
        assert!(!logged.contains("WARN"), "got: {logged:?}");
    }
}
