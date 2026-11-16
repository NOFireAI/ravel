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

#[cfg(not(target_env = "msvc"))]
use std::sync::atomic::{AtomicBool, Ordering};

/// The five jemalloc-native figures (`stats.allocated`/`active`/`resident`/
/// `mapped`/`retained`) on a build where jemalloc is the global allocator, or an explicit marker
/// naming whichever allocator this process actually uses instead. There is
/// no all-zeros case: a non-jemalloc build says so by name rather than
/// reporting jemalloc figures it does not have.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AllocatorStats {
    Jemalloc {
        allocated: u64,
        active: u64,
        resident: u64,
        mapped: u64,
        retained: u64,
        /// Whether jemalloc's background purge thread is enabled, read from
        /// the allocator at the same time as the byte figures, or `false`
        /// when the latest [`configure_background_thread`] failed to start it.
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
/// before each read. `background_thread` is `false` whatever jemalloc reports
/// when the latest [`configure_background_thread`] failed to start the thread.
#[cfg(not(target_env = "msvc"))]
pub fn read() -> AllocatorStats {
    read_with(
        &BACKGROUND_THREAD_START_FAILED,
        tikv_jemalloc_ctl::background_thread::read,
    )
}

/// [`read`] over an injected start-failed flag and `background_thread` read.
#[cfg(not(target_env = "msvc"))]
fn read_with(
    start_failed: &AtomicBool,
    background_thread: impl FnOnce() -> tikv_jemalloc_ctl::Result<bool>,
) -> AllocatorStats {
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
        mapped: stats::mapped::read().unwrap_or(0) as u64,
        retained: stats::retained::read().unwrap_or(0) as u64,
        background_thread: !start_failed.load(Ordering::Relaxed)
            && background_thread().unwrap_or(false),
    }
}

/// Whether the startup step's latest write to start the background thread
/// failed. After an EFAULT jemalloc's own enabled flag stays set with no
/// thread running unless the step's write turning it back off succeeds, so
/// [`read`] reports this instead.
#[cfg(not(target_env = "msvc"))]
static BACKGROUND_THREAD_START_FAILED: AtomicBool = AtomicBool::new(false);

/// The [`BackgroundThread::source`] of a failed start.
#[cfg(not(target_env = "msvc"))]
const START_FAILED: &str = "start-failed";

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
    /// `false` without a read-back when the source is `"start-failed"` or
    /// `"unavailable"`.
    pub enabled: bool,
    /// Who decided the state: `"server"` when the startup step enabled it,
    /// `"malloc_conf"` when the operator's [`MALLOC_CONF_ENV`] names
    /// `background_thread` and the step left it alone, `"unavailable"` when
    /// this jemalloc build has no background-thread support (macOS),
    /// `"start-failed"` when the step's write to start the thread failed
    /// (usually because the host refused to create it), `"not-jemalloc"` when
    /// this process does not run under jemalloc.
    pub source: &'static str,
}

impl BackgroundThread {
    /// The startup stamp: one INFO line naming [`Self::enabled`] and
    /// [`Self::source`].
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
/// is a warning, never a startup failure: it stamps `"start-failed"`, writes
/// the thread back off when jemalloc may have set its enabled flag (see
/// [`resolve_background_thread`]), and makes [`read`] report the thread as
/// not running until a later call starts it. A jemalloc build without
/// background-thread support (macOS) is not a failure: it stamps
/// `"unavailable"` with an info line.
#[cfg(not(target_env = "msvc"))]
pub fn configure_background_thread(malloc_conf: Option<&str>) -> BackgroundThread {
    use tikv_jemalloc_ctl::background_thread;

    configure_with(
        &BACKGROUND_THREAD_START_FAILED,
        malloc_conf.is_some_and(malloc_conf_names_background_thread),
        start_failure_of,
        background_thread::write,
        background_thread::read,
    )
}

/// [`configure_background_thread`] over an injected start-failed flag, which
/// it sets to whether this start failed, so the flag mirrors the latest call.
/// No production path calls it more than once.
#[cfg(not(target_env = "msvc"))]
fn configure_with(
    start_failed: &AtomicBool,
    operator_set: bool,
    failure_of: impl FnOnce(tikv_jemalloc_ctl::Error) -> StartFailure,
    write: impl FnMut(bool) -> tikv_jemalloc_ctl::Result<()>,
    read: impl FnOnce() -> tikv_jemalloc_ctl::Result<bool>,
) -> BackgroundThread {
    let state = resolve_background_thread(operator_set, failure_of, write, read);
    start_failed.store(state.source == START_FAILED, Ordering::Relaxed);
    state
}

/// How far a failed `background_thread` write of `true` got in the vendored
/// jemalloc 5.3.1, read from its errno.
#[cfg(not(target_env = "msvc"))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StartFailure {
    /// EFAULT: `background_thread_ctl` set the enabled flag, then failed to
    /// create thread 0.
    FlagSet,
    /// EAGAIN: `malloc_init` or `ctl_init` failed before the handler ran, so
    /// the enabled flag is as it was.
    NotReached,
    /// Any other error, or one whose message is not recognised, which the
    /// vendored jemalloc does not return for this typed write: whether the
    /// flag was set is unknown.
    Unknown,
}

/// Recovers the errno of a failed start from the error's message, because
/// `tikv-jemalloc-ctl` exposes no errno. A message it does not recognise is
/// [`StartFailure::Unknown`], which takes the same turn-off path as EFAULT.
#[cfg(not(target_env = "msvc"))]
fn start_failure_of(err: tikv_jemalloc_ctl::Error) -> StartFailure {
    let message = err.to_string();
    if message.starts_with("An interface with side effects failed") {
        StartFailure::FlagSet
    } else if message.starts_with("A memory allocation failure occurred") {
        StartFailure::NotReached
    } else {
        StartFailure::Unknown
    }
}

/// The startup step's decision over injected `background_thread` mallctl
/// `write` and `read` calls. `operator_set` skips the write. An unavailable
/// write returns without calling `read`, which would answer the same way.
///
/// Any failed write of `true`, not only EFAULT, returns `"start-failed"`
/// without calling `read`, because none of them leaves a thread running. In
/// the vendored jemalloc 5.3.1, `background_thread_ctl` sets the enabled flag
/// before it creates thread 0 and returns EFAULT with the flag still set when
/// creation fails. EAGAIN comes from mallctl's own setup (`malloc_init`,
/// `ctl_init`) before the handler runs, and the handler's EINVAL is
/// unreachable through a typed `bool` write.
///
/// With the flag set and no thread, application threads never decay-purge,
/// so a failed start is followed by a write of `false`. That clears the flag
/// and succeeds although thread 0 never started, because the disable path
/// joins only a started thread, and purging on allocator activity resumes as
/// under `background_thread:false`. The slots jemalloc marked for other arenas
/// stay marked started with no thread, which trips an assert only in a debug
/// build of jemalloc. If that write fails as well after an EFAULT, nothing
/// decay-purges until restart and freed pages return only through forced
/// purges ([`purge_arenas`]). After an EAGAIN the handler never touched the
/// flag, so there is nothing to turn back off and no write of `false`; the
/// stamp still says `"start-failed"`, because the step's write failed, and
/// the warning says the setting was left as it was.
#[cfg(not(target_env = "msvc"))]
fn resolve_background_thread(
    operator_set: bool,
    failure_of: impl FnOnce(tikv_jemalloc_ctl::Error) -> StartFailure,
    mut write: impl FnMut(bool) -> tikv_jemalloc_ctl::Result<()>,
    read: impl FnOnce() -> tikv_jemalloc_ctl::Result<bool>,
) -> BackgroundThread {
    let source = if operator_set {
        "malloc_conf"
    } else {
        match classify_mallctl(write(true)) {
            Mallctl::Done(()) => {}
            Mallctl::Unavailable(err) => return unavailable(err),
            Mallctl::Failed(err) => {
                let failure = failure_of(err);
                let turned_off = match failure {
                    StartFailure::NotReached => None,
                    StartFailure::FlagSet | StartFailure::Unknown => Some(write(false)),
                };
                return start_failed(err, failure, turned_off);
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

/// The stamp and warnings for a failed start, given how far it got and the
/// result of the write that turned the thread back off, `None` when there was
/// no such write.
#[cfg(not(target_env = "msvc"))]
fn start_failed(
    err: tikv_jemalloc_ctl::Error,
    failure: StartFailure,
    turned_off: Option<tikv_jemalloc_ctl::Result<()>>,
) -> BackgroundThread {
    match turned_off {
        None => tracing::warn!(
            error = %err,
            "could not start the jemalloc background purge thread; jemalloc failed before it \
             reached the setting, so the setting and decay purging are as they were; reporting \
             the thread as not running"
        ),
        Some(Ok(())) => tracing::warn!(
            error = %err,
            "could not start the jemalloc background purge thread; turned it back off and \
             reporting it as not running, so freed pages are returned to the operating system \
             only on allocator activity"
        ),
        Some(Err(off_err)) => {
            tracing::warn!(
                error = %err,
                "could not start the jemalloc background purge thread; reporting it as not running"
            );
            if failure == StartFailure::FlagSet {
                tracing::warn!(
                    error = %off_err,
                    "could not turn the jemalloc background purge thread back off after its \
                     failed start; decay purging is off until restart, and freed pages are \
                     returned to the operating system only through forced purges"
                );
            } else {
                tracing::warn!(
                    error = %off_err,
                    "could not turn the jemalloc background purge thread back off after its \
                     failed start; if that start left jemalloc's enabled flag set, decay purging \
                     is off until restart, and freed pages are returned to the operating system \
                     only through forced purges"
                );
            }
        }
    }
    BackgroundThread {
        enabled: false,
        source: START_FAILED,
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

    /// `mapped` and `retained` are live reads in the same epoch as the other
    /// figures. jemalloc documents `stats.mapped` as larger than
    /// `stats.active` and as having no strict ordering against
    /// `stats.resident`, so the ordering pinned here is the documented one.
    #[cfg(not(target_env = "msvc"))]
    #[test]
    fn read_reports_mapped_and_retained_live() {
        match read() {
            AllocatorStats::Jemalloc { active, mapped, .. } => {
                assert!(mapped > 0, "a running process has mapped bytes");
                assert!(
                    mapped >= active,
                    "mapped ({mapped}) must cover active ({active})"
                );
            }
            AllocatorStats::Other { name } => {
                panic!("expected Jemalloc on a non-msvc target, got Other({name})");
            }
        }
        tikv_jemalloc_ctl::epoch::advance().expect("epoch advances");
        tikv_jemalloc_ctl::stats::retained::read().expect("stats.retained reads");
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

    /// The EFAULT `background_thread::write(true)` returns when jemalloc
    /// cannot create the thread. `arena.<narenas>.extent_hooks` names the
    /// index one past the last arena, which the name lookup accepts and the
    /// mallctl itself answers with EFAULT.
    #[cfg(not(target_env = "msvc"))]
    fn efault() -> tikv_jemalloc_ctl::Error {
        use tikv_jemalloc_ctl::{Access, AsName, arenas};

        let narenas = arenas::narenas::read().expect("arenas.narenas read must succeed");
        let key = format!("arena.{narenas}.extent_hooks\0");
        let past_the_end: tikv_jemalloc_ctl::Result<usize> = key.as_str().name().read();
        let err = past_the_end.expect_err("an arena index past the last arena must fail");
        assert!(
            err.to_string()
                .starts_with("An interface with side effects failed"),
            "expected EFAULT, got: {err}"
        );
        err
    }

    /// A `background_thread` write that answers `true` with `result` and
    /// panics on `false`, for the paths that must not turn the thread off.
    #[cfg(not(target_env = "msvc"))]
    fn write_true_only(
        result: impl FnOnce() -> tikv_jemalloc_ctl::Result<()>,
    ) -> impl FnMut(bool) -> tikv_jemalloc_ctl::Result<()> {
        let mut result = Some(result);
        move |value| {
            assert!(value, "no write of false on this path");
            (result.take().expect("one write of true"))()
        }
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
                start_failure_of,
                write_true_only(|| Err(enoent())),
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
    fn resolve_warns_on_a_refused_write_and_stamps_a_failed_start() {
        let (state, logged) = with_captured_log(|| {
            resolve_background_thread(
                false,
                start_failure_of,
                |value| if value { Err(eperm()) } else { Ok(()) },
                || panic!("no read-back after a failed write"),
            )
        });
        assert_eq!(
            state,
            BackgroundThread {
                enabled: false,
                source: "start-failed",
            }
        );
        assert_eq!(logged.lines().count(), 1, "got: {logged:?}");
        assert_eq!(logged.matches(" WARN ").count(), 1, "got: {logged:?}");
    }

    /// Issue #3096: jemalloc sets its enabled flag before it creates the
    /// thread and leaves it set when creation fails, so the read-back says
    /// `true` with no thread running. The stamp must not report it.
    #[cfg(not(target_env = "msvc"))]
    #[test]
    fn resolve_stamps_a_failed_start_as_not_running_despite_the_read_back() {
        let (state, logged) = with_captured_log(|| {
            let state = resolve_background_thread(
                false,
                start_failure_of,
                |value| if value { Err(efault()) } else { Ok(()) },
                || Ok(true),
            );
            state.emit();
            state
        });
        assert_eq!(
            state,
            BackgroundThread {
                enabled: false,
                source: "start-failed",
            }
        );
        assert_eq!(logged.lines().count(), 2, "got: {logged:?}");
        assert_eq!(logged.matches(" WARN ").count(), 1, "got: {logged:?}");
        assert_eq!(
            logged
                .matches("allocator_background_thread=false source=\"start-failed\"")
                .count(),
            1,
            "the startup stamp must name the failed start, got: {logged:?}"
        );
    }

    /// Issue #3096: after a failed start the scrape's read reports the
    /// thread as not running although jemalloc's flag reads `true`, and a
    /// successful start leaves the read on jemalloc's answer.
    #[cfg(not(target_env = "msvc"))]
    #[test]
    fn a_failed_start_makes_the_read_report_the_thread_as_not_running() {
        let background_thread_of = |stats| match stats {
            AllocatorStats::Jemalloc {
                background_thread, ..
            } => background_thread,
            AllocatorStats::Other { name } => panic!("expected Jemalloc, got Other({name})"),
        };

        let started = AtomicBool::new(false);
        let (state, _) = with_captured_log(|| {
            configure_with(
                &started,
                false,
                start_failure_of,
                write_true_only(|| Ok(())),
                || Ok(true),
            )
        });
        assert_eq!(
            state,
            BackgroundThread {
                enabled: true,
                source: "server",
            }
        );
        assert!(!started.load(Ordering::Relaxed));
        assert!(background_thread_of(read_with(&started, || Ok(true))));

        let failed = AtomicBool::new(false);
        let (state, _) = with_captured_log(|| {
            configure_with(
                &failed,
                false,
                start_failure_of,
                |value| if value { Err(efault()) } else { Err(eperm()) },
                || Ok(true),
            )
        });
        assert_eq!(state.source, "start-failed");
        assert!(
            failed.load(Ordering::Relaxed),
            "the startup step must record a failed start"
        );
        assert!(
            !background_thread_of(read_with(&failed, || Ok(true))),
            "a failed start must read as not running whatever jemalloc's flag says"
        );
    }

    /// Issue #3096: with jemalloc's enabled flag left set and no thread, no
    /// application thread decay-purges, so a failed start writes the thread
    /// back off, once, and the stamp still names the failed start.
    #[cfg(not(target_env = "msvc"))]
    #[test]
    fn a_failed_start_turns_the_thread_back_off() {
        let failed = AtomicBool::new(false);
        let mut written = Vec::new();
        let (state, logged) = with_captured_log(|| {
            configure_with(
                &failed,
                false,
                start_failure_of,
                |value| {
                    written.push(value);
                    if value { Err(efault()) } else { Ok(()) }
                },
                || panic!("no read-back after a failed write"),
            )
        });
        assert_eq!(written, [true, false]);
        assert_eq!(
            state,
            BackgroundThread {
                enabled: false,
                source: "start-failed",
            }
        );
        assert!(failed.load(Ordering::Relaxed));
        assert_eq!(logged.lines().count(), 1, "got: {logged:?}");
        assert_eq!(logged.matches(" WARN ").count(), 1, "got: {logged:?}");
        assert_eq!(
            logged
                .matches("turned it back off and reporting it as not running")
                .count(),
            1,
            "got: {logged:?}"
        );
        assert!(!logged.contains("decay purging is off"), "got: {logged:?}");
    }

    /// Issue #3096: when the write turning the thread back off fails too,
    /// the flag stays set and nothing decay-purges until restart, which the
    /// step warns about on top of the failed start.
    #[cfg(not(target_env = "msvc"))]
    #[test]
    fn a_failed_turn_off_after_a_failed_start_warns_that_decay_purging_is_off() {
        let failed = AtomicBool::new(false);
        let mut written = Vec::new();
        let (state, logged) = with_captured_log(|| {
            configure_with(
                &failed,
                false,
                start_failure_of,
                |value| {
                    written.push(value);
                    if value { Err(efault()) } else { Err(eperm()) }
                },
                || panic!("no read-back after a failed write"),
            )
        });
        assert_eq!(written, [true, false]);
        assert_eq!(
            state,
            BackgroundThread {
                enabled: false,
                source: "start-failed",
            }
        );
        assert!(failed.load(Ordering::Relaxed));
        assert_eq!(logged.lines().count(), 2, "got: {logged:?}");
        assert_eq!(logged.matches(" WARN ").count(), 2, "got: {logged:?}");
        assert_eq!(
            logged
                .matches(
                    "could not turn the jemalloc background purge thread back off after its \
                     failed start; decay purging is off until restart, and freed pages are \
                     returned to the operating system only through forced purges"
                )
                .count(),
            1,
            "got: {logged:?}"
        );
        assert!(!logged.contains("turned it back off"), "got: {logged:?}");
    }

    /// Issue #3140: the start-failed flag mirrors the latest start, so a
    /// successful start after a failed one clears it and the read reports
    /// jemalloc's answer again.
    #[cfg(not(target_env = "msvc"))]
    #[test]
    fn a_successful_start_after_a_failed_one_clears_the_flag() {
        let background_thread_of = |stats| match stats {
            AllocatorStats::Jemalloc {
                background_thread, ..
            } => background_thread,
            AllocatorStats::Other { name } => panic!("expected Jemalloc, got Other({name})"),
        };

        let flag = AtomicBool::new(false);
        let (state, _) = with_captured_log(|| {
            configure_with(
                &flag,
                false,
                start_failure_of,
                |value| if value { Err(efault()) } else { Ok(()) },
                || panic!("no read-back after a failed write"),
            )
        });
        assert_eq!(state.source, "start-failed");
        assert!(flag.load(Ordering::Relaxed));
        assert!(!background_thread_of(read_with(&flag, || Ok(true))));

        let (state, _) = with_captured_log(|| {
            configure_with(
                &flag,
                false,
                start_failure_of,
                write_true_only(|| Ok(())),
                || Ok(true),
            )
        });
        assert_eq!(
            state,
            BackgroundThread {
                enabled: true,
                source: "server",
            }
        );
        assert!(
            !flag.load(Ordering::Relaxed),
            "a successful start must clear the start-failed flag"
        );
        assert!(
            background_thread_of(read_with(&flag, || Ok(true))),
            "after a successful start the read must report jemalloc's answer"
        );
    }

    /// Issue #3140: an EAGAIN start failed before jemalloc's handler set the
    /// enabled flag, so there is nothing to turn back off and the warning
    /// must not say decay purging is off. `tikv-jemalloc-ctl` cannot build
    /// an EAGAIN and no mallctl returns one on demand, so the classifier is
    /// injected and the carried error is an EPERM.
    #[cfg(not(target_env = "msvc"))]
    #[test]
    fn an_eagain_start_writes_nothing_back_and_leaves_decay_purging_alone() {
        let failed = AtomicBool::new(false);
        let mut written = Vec::new();
        let (state, logged) = with_captured_log(|| {
            configure_with(
                &failed,
                false,
                |_| StartFailure::NotReached,
                |value| {
                    written.push(value);
                    Err(eperm())
                },
                || panic!("no read-back after a failed write"),
            )
        });
        assert!(!logged.contains("decay purging is off"), "got: {logged:?}");
        assert_eq!(
            logged
                .matches(
                    "jemalloc failed before it reached the setting, so the setting and decay \
                     purging are as they were; reporting the thread as not running"
                )
                .count(),
            1,
            "got: {logged:?}"
        );
        assert_eq!(logged.lines().count(), 1, "got: {logged:?}");
        assert_eq!(logged.matches(" WARN ").count(), 1, "got: {logged:?}");
        assert_eq!(written, [true], "no write of false after an EAGAIN");
        assert_eq!(
            state,
            BackgroundThread {
                enabled: false,
                source: "start-failed",
            }
        );
        assert!(failed.load(Ordering::Relaxed));
    }

    /// A start failure whose errno is neither EFAULT nor EAGAIN is still
    /// turned back off, and when that fails too the second warning says
    /// decay purging is off only if the flag was left set.
    #[cfg(not(target_env = "msvc"))]
    #[test]
    fn an_unknown_start_failure_qualifies_the_decay_purging_warning() {
        let failed = AtomicBool::new(false);
        let mut written = Vec::new();
        let (state, logged) = with_captured_log(|| {
            configure_with(
                &failed,
                false,
                start_failure_of,
                |value| {
                    written.push(value);
                    Err(eperm())
                },
                || panic!("no read-back after a failed write"),
            )
        });
        assert_eq!(written, [true, false]);
        assert_eq!(state.source, "start-failed");
        assert_eq!(logged.matches(" WARN ").count(), 2, "got: {logged:?}");
        assert_eq!(
            logged
                .matches(
                    "if that start left jemalloc's enabled flag set, decay purging is off until \
                     restart"
                )
                .count(),
            1,
            "got: {logged:?}"
        );
        assert!(
            !logged.contains("failed start; decay purging is off until restart"),
            "got: {logged:?}"
        );
    }

    /// The real errors the classifier can be shown: EFAULT is the flag-set
    /// case, and an errno this write does not return is unknown.
    #[cfg(not(target_env = "msvc"))]
    #[test]
    fn start_failure_of_reads_efault_as_the_flag_set() {
        assert_eq!(start_failure_of(efault()), StartFailure::FlagSet);
        assert_eq!(start_failure_of(eperm()), StartFailure::Unknown);
        assert_eq!(start_failure_of(enoent()), StartFailure::Unknown);
    }

    /// A successful start is left running and read back.
    #[cfg(not(target_env = "msvc"))]
    #[test]
    fn resolve_leaves_a_started_thread_on() {
        let (state, logged) = with_captured_log(|| {
            resolve_background_thread(false, start_failure_of, write_true_only(|| Ok(())), || {
                Ok(true)
            })
        });
        assert_eq!(
            state,
            BackgroundThread {
                enabled: true,
                source: "server",
            }
        );
        assert!(logged.is_empty(), "got: {logged:?}");
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
                start_failure_of,
                |_| panic!("no write when the operator set the thread"),
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
