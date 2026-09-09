//! Injected clock so flush identity (ADR-0010 §1) is deterministic in tests.
//!
//! Actor logic must never read `SystemTime`/`Instant::now()` directly; every
//! clock read goes through this trait so tests can pin, replay, and advance
//! time across retries and hour boundaries.
//!
//! One bounded exception (ADR-0104 decision 1): the `stage-timing` cargo
//! feature, off by default, compiles in a profiling seam (`crate::stage_timing`)
//! that reads `Instant::now()` at stage boundaries to measure per-stage
//! durations. It is sound only because it never ships (off by default, so every
//! production and default-gate build is exactly as today), uses monotonic
//! `Instant` rather than this wall-clock, and its accumulated timings feed no
//! decision -- no control flow, flush trigger, flush identity, or backpressure
//! choice reads a stage timing, only the bench reporter does. Extending the
//! seam must preserve that no-decision-reads-a-timing property, or the
//! exception stops being defensible.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub trait Clock: Send + Sync + 'static {
    /// Current time as unix nanoseconds.
    fn now_ns(&self) -> i64;

    /// A future that completes once `dur` of this clock's time has elapsed.
    ///
    /// The default implementation sleeps on the real tokio timer, which is
    /// correct for a wall-clock [`SystemClock`]. A deterministic test clock
    /// overrides this to complete the future once the test advances the
    /// injected clock past the deadline. Routing the shard actor's flush tick
    /// through this method (crate `shard.rs`) puts the age-based flush cadence
    /// on the same injected clock as the age check itself, so a test can drive
    /// a flush tick deterministically by advancing the clock, with no
    /// wall-clock sleep.
    fn sleep(&self, dur: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        Box::pin(tokio::time::sleep(dur))
    }
}

impl<C: Clock + ?Sized> Clock for Arc<C> {
    fn now_ns(&self) -> i64 {
        (**self).now_ns()
    }

    // Forward to the inner clock so an `Arc<dyn Clock>` (how the shard actor
    // and router hold it) uses that clock's own `sleep`, not the default.
    fn sleep(&self, dur: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
        (**self).sleep(dur)
    }
}

/// Production clock backed by the OS wall clock. Uses the default real-timer
/// [`Clock::sleep`].
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ns(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_clock_reports_positive_recent_time() {
        // Sanity check only: any time after 2020-01-01 in unix ns.
        let ns = SystemClock.now_ns();
        assert!(ns > 1_577_836_800_000_000_000);
    }
}
