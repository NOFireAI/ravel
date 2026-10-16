//! Injected clock, so the round-robin idle-eviction logic reads no wall clock
//! itself and every eviction rule is deterministic under test (the repo's
//! "time is injected" testing convention).

use std::time::{SystemTime, UNIX_EPOCH};

/// A source of the current time in nanoseconds since the Unix epoch.
pub(crate) trait Clock: Send + Sync {
    fn now_ns(&self) -> i64;
}

/// The one blessed wall-clock reader. Library logic never calls
/// `SystemTime::now` directly; only this wrapper does, so tests inject a fake
/// [`Clock`] instead.
pub(crate) struct SystemClock;

impl Clock for SystemClock {
    fn now_ns(&self) -> i64 {
        // Saturate rather than panic on a pre-epoch or absurd clock: the value
        // only orders idle eviction, so a clamp is harmless and unwrap-free.
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
            .unwrap_or(0)
    }
}
