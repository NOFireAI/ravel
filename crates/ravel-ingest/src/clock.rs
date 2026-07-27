//! Injected clock so flush identity (ADR-0010 §1) is deterministic in tests.
//!
//! Actor logic must never read `SystemTime`/`Instant::now()` directly; every
//! clock read goes through this trait so tests can pin, replay, and advance
//! time across retries and hour boundaries.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

pub trait Clock: Send + Sync + 'static {
    /// Current time as unix nanoseconds.
    fn now_ns(&self) -> i64;
}

impl<C: Clock + ?Sized> Clock for Arc<C> {
    fn now_ns(&self) -> i64 {
        (**self).now_ns()
    }
}

/// Production clock backed by the OS wall clock.
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
