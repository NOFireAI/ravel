//! The delay between one store call that failed retryably (or a conditional
//! HEAD write another writer beat) and the next attempt. The object store
//! contract asks for jittered exponential backoff on `Transient`, `Timeout`
//! and `Throttled`, and a `Throttled` answer names the wait it wants; two
//! writers that retry back to back re-collide on every attempt, so a bound
//! on attempts alone bounds requests, not contention.
//!
//! The policy is injected so tests and the simulation run with no wall-clock
//! wait and can assert which delays the store asked for
//! (`tests::a_retry_waits_through_the_backoff_seam`).

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// The delay before a retry. `attempt` counts the attempts already made (1
/// before the first retry). `retry_after_ms` is the backend's own ask when
/// the failure carried one; a policy honours it as a floor.
pub trait Backoff: Send + Sync {
    fn delay(&self, attempt: u32, retry_after_ms: Option<u64>) -> Duration;
}

/// Full-jitter exponential backoff: a uniform draw from zero to
/// `min(base * 2^(attempt - 1), max)`, floored at the backend's
/// `retry_after_ms`. The draw comes from a xorshift generator seeded from
/// the wall clock at construction; it decorrelates writers and nothing
/// depends on its distribution.
#[derive(Debug)]
pub struct JitteredBackoff {
    base: Duration,
    max: Duration,
    state: AtomicU64,
}

impl JitteredBackoff {
    pub fn new(base: Duration, max: Duration) -> JitteredBackoff {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX));
        JitteredBackoff {
            base,
            max,
            state: AtomicU64::new(nanos | 1),
        }
    }

    fn next_u64(&self) -> u64 {
        let mut x = self.state.load(Ordering::Relaxed);
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.state.store(x, Ordering::Relaxed);
        x
    }
}

impl Default for JitteredBackoff {
    /// 20 ms base, 2 s cap: eight CAS attempts span about 2.5 s at most.
    fn default() -> Self {
        JitteredBackoff::new(Duration::from_millis(20), Duration::from_secs(2))
    }
}

impl Backoff for JitteredBackoff {
    fn delay(&self, attempt: u32, retry_after_ms: Option<u64>) -> Duration {
        let shift = attempt.saturating_sub(1).min(20);
        let ceiling = self.base.saturating_mul(1u32 << shift).min(self.max);
        let ceiling_ms = u64::try_from(ceiling.as_millis()).unwrap_or(u64::MAX);
        let drawn_ms = if ceiling_ms == 0 {
            0
        } else {
            self.next_u64() % (ceiling_ms + 1)
        };
        Duration::from_millis(drawn_ms.max(retry_after_ms.unwrap_or(0)))
    }
}

/// No delay at all. For tests and the simulation, which advance time
/// themselves.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoBackoff;

impl Backoff for NoBackoff {
    fn delay(&self, _attempt: u32, _retry_after_ms: Option<u64>) -> Duration {
        Duration::ZERO
    }
}
