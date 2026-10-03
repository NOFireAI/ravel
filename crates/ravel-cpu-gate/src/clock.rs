//! The monotonic time source the gate measures waits and runs with.

use std::time::Instant;

/// Monotonic time seam. Only differences between two readings are used, and
/// they are taken with `saturating_sub`, so a reading that goes backwards
/// costs one short observation and nothing more.
pub trait MonotonicClock: Send + Sync + 'static {
    /// Nanoseconds since an arbitrary, implementation-chosen origin.
    fn now_nanos(&self) -> u64;
}

/// `std::time::Instant` elapsed since construction.
#[derive(Debug)]
pub struct InstantClock {
    origin: Instant,
}

impl InstantClock {
    pub fn new() -> Self {
        InstantClock {
            origin: Instant::now(),
        }
    }
}

impl Default for InstantClock {
    fn default() -> Self {
        InstantClock::new()
    }
}

impl MonotonicClock for InstantClock {
    fn now_nanos(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }
}
