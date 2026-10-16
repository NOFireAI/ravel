//! Support for tests that open and count `debug_span!`-level spans while the
//! crate's unit tests share one process (`cargo test --lib`, the coverage lane).
//!
//! Two process-global properties of `tracing` make such a test flaky under the
//! parallel harness, and this module neutralizes both:
//!
//! 1. Whether a `debug_span!` is enabled at all is gated by a single global
//!    max-level atomic (`LevelFilter::current()`). That atomic is recomputed
//!    every time any thread registers a new callsite or installs/drops a
//!    default subscriber, and each recompute sets it from the *recomputing
//!    thread's* current subscriber. A thread with no thread-local default falls
//!    back to the process default, which is `NoSubscriber` (max-level hint
//!    `OFF`). So while hundreds of tests on parallel threads churn callsites and
//!    thread-local defaults, the atomic briefly drops to `OFF`, and a
//!    `debug_span!` created in that window on another thread is silently
//!    disabled -- never opened, never counted. [`install_floor`] pins a process
//!    default whose hint is `TRACE`, so every recompute floors the atomic at
//!    `TRACE` and the span is always enabled. It is a bare `Registry` with no
//!    collecting layer, and a test's own `set_default` subscriber takes
//!    precedence for span dispatch, so this default never observes any test's
//!    spans -- it exists only to hold the level floor.
//!
//! 2. A subscriber whose max-level hint is below `DEBUG` (an `INFO`-only
//!    subscriber) lowers that same atomic to `INFO` on its own thread while it
//!    is the default, disabling `debug_span!` on every other thread for that
//!    window. A test that installs such a subscriber and a test that counts
//!    debug spans must therefore not run at the same time. [`guard`] returns a
//!    process-wide lock that both kinds of test hold for their whole body, so
//!    they never overlap.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::{LazyLock, Mutex, MutexGuard, Once};

use tracing_subscriber::util::SubscriberInitExt;

static TRACING_LEVEL_LOCK: LazyLock<Mutex<()>> = LazyLock::new(|| Mutex::new(()));
static FLOOR: Once = Once::new();

/// Install, once for the process, a bare `Registry` as the global default so
/// the global max-level atomic is floored at `TRACE`. Idempotent: only the
/// first call installs anything, and it is a no-op if a global default is
/// already set (`set_default` on the registry returns an error we ignore).
fn install_floor() {
    FLOOR.call_once(|| {
        // A no-op registry: it allocates span ids and drops them, records
        // nothing, and is overridden on any thread that sets its own default.
        // Its only job is to keep `LevelFilter::current()` at TRACE.
        let _ = tracing_subscriber::registry().try_init();
    });
}

/// Acquire the process-wide guard for a tracing-level-sensitive test and ensure
/// the global level floor is installed. Hold the returned guard for the whole
/// test body: it serializes this test against every other test that either
/// counts debug spans or installs a sub-`DEBUG` subscriber.
pub(crate) fn guard() -> MutexGuard<'static, ()> {
    install_floor();
    TRACING_LEVEL_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}
