//! Bounded per-key backoff for provisioning-record reads that failed (issue
//! #3653).
//!
//! A tenant whose provisioning record cannot be read would otherwise be
//! re-read on every request: the router path fails each write closed and
//! remembers nothing, and the server's first-write `ensure` fails open and
//! remembers nothing either. [`ReadBackoff`] remembers the failure: after a
//! failed read the key is not read again until its retry time, which starts
//! [`PROVISIONING_READ_BACKOFF_INITIAL_NS`] after the failure and doubles on
//! each consecutive failure up to [`PROVISIONING_READ_BACKOFF_MAX_NS`]. Only a
//! failure at or after the current retry time counts as consecutive: failures
//! of reads issued before or inside the window leave it unchanged. A
//! successful read clears the entry, so the map holds only keys whose last
//! read failed.
//!
//! Time is the caller's injected `now_ns`; this type reads no clock.

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};

/// The first backoff window after a failed read: one second.
pub const PROVISIONING_READ_BACKOFF_INITIAL_NS: i64 = 1_000_000_000;

/// The largest backoff window: thirty seconds.
pub const PROVISIONING_READ_BACKOFF_MAX_NS: i64 = 30_000_000_000;

/// The backoff state of one key whose last read failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadBackoffEntry {
    /// When the last read failed.
    pub failed_at_ns: i64,
    /// The current window: the next read is allowed at
    /// `failed_at_ns + delay_ns`.
    pub delay_ns: i64,
}

impl ReadBackoffEntry {
    /// The earliest time a read of this key is allowed again.
    pub fn retry_at_ns(&self) -> i64 {
        self.failed_at_ns.saturating_add(self.delay_ns)
    }
}

/// Per-key read backoff. Not synchronised: the owner keeps it behind its own
/// lock.
#[derive(Debug)]
pub struct ReadBackoff<K> {
    entries: HashMap<K, ReadBackoffEntry>,
}

impl<K> Default for ReadBackoff<K> {
    fn default() -> Self {
        ReadBackoff {
            entries: HashMap::new(),
        }
    }
}

impl<K: Hash + Eq> ReadBackoff<K> {
    pub fn new() -> Self {
        Self::default()
    }

    /// The retry time of `key` when a read at `now_ns` falls inside its
    /// backoff window, `None` when a read is allowed.
    pub fn blocked_until(&self, key: &K, now_ns: i64) -> Option<i64> {
        let retry_at = self.entries.get(key)?.retry_at_ns();
        (now_ns < retry_at).then_some(retry_at)
    }

    /// Record a failed read of `key` at `now_ns` and return the delay until
    /// the next read is allowed. A failure at or after the current retry time
    /// (or of a key with no entry) starts a new window:
    /// [`PROVISIONING_READ_BACKOFF_INITIAL_NS`] for a first failure, double
    /// the previous window otherwise, capped at
    /// [`PROVISIONING_READ_BACKOFF_MAX_NS`]. A failure before the current
    /// retry time came from a read issued before or inside that window, so the
    /// entry is kept and the remaining delay returned: concurrent reads that
    /// all fail at once arm one window, not one doubling each.
    pub fn record_failure(&mut self, key: K, now_ns: i64) -> i64 {
        let delay_ns = match self.entries.get(&key) {
            Some(entry) if now_ns < entry.retry_at_ns() => {
                return entry.retry_at_ns().saturating_sub(now_ns);
            }
            Some(entry) => entry
                .delay_ns
                .saturating_mul(2)
                .min(PROVISIONING_READ_BACKOFF_MAX_NS),
            None => PROVISIONING_READ_BACKOFF_INITIAL_NS,
        };
        self.entries.insert(
            key,
            ReadBackoffEntry {
                failed_at_ns: now_ns,
                delay_ns,
            },
        );
        delay_ns
    }

    /// Forget `key` after a successful read.
    pub fn clear(&mut self, key: &K) {
        self.entries.remove(key);
    }

    /// The entry for `key`, if its last read failed.
    pub fn entry(&self, key: &K) -> Option<ReadBackoffEntry> {
        self.entries.get(key).copied()
    }

    /// Drop every entry whose last failure is more than `ttl_ns` before
    /// `now_ns`, for idle-tenant eviction. Returns how many were dropped.
    pub fn evict_idle(&mut self, now_ns: i64, ttl_ns: i64) -> usize {
        let before = self.entries.len();
        self.entries
            .retain(|_, entry| now_ns.saturating_sub(entry.failed_at_ns) <= ttl_ns);
        before - self.entries.len()
    }

    /// How many keys are backing off.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Why a provisioning read failed, as the `cause` label of
/// `ravel_ingest_provisioning_read_failures_total`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProvisioningReadCause {
    /// The GET itself failed.
    Store,
    /// The GET returned bytes that did not decode or failed a checked read.
    Decode,
}

impl ProvisioningReadCause {
    /// The metric label value.
    pub fn label(self) -> &'static str {
        match self {
            ProvisioningReadCause::Store => "store",
            ProvisioningReadCause::Decode => "decode",
        }
    }
}

/// Failed provisioning reads issued by one router, by cause. Shared by `Arc`
/// between the router's [`crate::GenerationSwitch`], which counts, and the
/// router's metrics, which report.
#[derive(Debug, Default)]
pub struct ProvisioningReadFailures {
    store: AtomicU64,
    decode: AtomicU64,
}

impl ProvisioningReadFailures {
    pub(crate) fn record(&self, cause: ProvisioningReadCause) {
        let counter = match cause {
            ProvisioningReadCause::Store => &self.store,
            ProvisioningReadCause::Decode => &self.decode,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Failed reads whose GET failed.
    pub fn store(&self) -> u64 {
        self.store.load(Ordering::Relaxed)
    }

    /// Failed reads whose record did not decode or failed its checked read.
    pub fn decode(&self) -> u64 {
        self.decode.load(Ordering::Relaxed)
    }
}

/// A store whose GETs of `key` fail with a transient error `faults` times and
/// then pass through. `sequence_progress(0)` counts every GET of `key`, up to
/// `faults + 16`.
#[cfg(test)]
pub(crate) fn failing_then_passing_get_store(
    key: &str,
    faults: usize,
) -> std::sync::Arc<ravel_object_store::fault::FaultStore<ravel_object_store::memory::MemoryStore>>
{
    use ravel_object_store::fault::{FaultPlan, FaultStore, Op, ScriptedFault, Sequence};
    let mut sequence = Sequence::new(Op::Get).with_key_contains(key);
    for _ in 0..faults {
        sequence = sequence.then_fault(ScriptedFault::Transient("provisioning GET".into()));
    }
    for _ in 0..16 {
        sequence = sequence.then_passthrough();
    }
    std::sync::Arc::new(FaultStore::new(
        ravel_object_store::memory::MemoryStore::new(),
        FaultPlan::empty().with_sequence(sequence),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Consecutive failures give windows of 1, 2, 4, 8, 16 seconds, then 30
    /// and never more; a success resets to 1.
    #[test]
    fn windows_double_from_one_second_and_cap_at_thirty() {
        let mut backoff = ReadBackoff::new();
        let mut now = 1_000_i64;
        let mut windows = Vec::new();
        for _ in 0..9 {
            let delay = backoff.record_failure("k", now);
            assert_eq!(backoff.blocked_until(&"k", now), Some(now + delay));
            assert_eq!(
                backoff.blocked_until(&"k", now + delay - 1),
                Some(now + delay)
            );
            assert_eq!(backoff.blocked_until(&"k", now + delay), None);
            windows.push(delay / 1_000_000_000);
            now += delay;
        }
        assert_eq!(windows, vec![1, 2, 4, 8, 16, 30, 30, 30, 30]);
        backoff.clear(&"k");
        assert!(backoff.is_empty());
        assert_eq!(backoff.blocked_until(&"k", now), None);
        assert_eq!(backoff.record_failure("k", now), 1_000_000_000);
    }

    /// Failures of reads issued before the first failure was recorded land at
    /// the same instant; they belong to the window that failure armed and
    /// leave it at one second.
    #[test]
    fn same_instant_failures_keep_a_one_second_window() {
        let mut backoff = ReadBackoff::new();
        let now = 5_000_i64;
        for _ in 0..6 {
            assert_eq!(
                backoff.record_failure("k", now),
                PROVISIONING_READ_BACKOFF_INITIAL_NS
            );
        }
        assert_eq!(
            backoff.entry(&"k"),
            Some(ReadBackoffEntry {
                failed_at_ns: now,
                delay_ns: PROVISIONING_READ_BACKOFF_INITIAL_NS,
            })
        );
        assert_eq!(
            backoff.blocked_until(&"k", now),
            Some(now + PROVISIONING_READ_BACKOFF_INITIAL_NS)
        );
    }

    /// A failure recorded inside the window keeps the entry and its retry
    /// time, and returns the remaining delay; one at or after the retry time
    /// doubles.
    #[test]
    fn failure_inside_window_keeps_retry_time_and_at_retry_doubles() {
        const SEC: i64 = 1_000_000_000;
        let mut backoff = ReadBackoff::new();
        let t0 = 10 * SEC;
        backoff.record_failure("k", t0);
        let inside = t0 + SEC / 4;
        assert_eq!(backoff.record_failure("k", inside), SEC - SEC / 4);
        assert_eq!(
            backoff.entry(&"k"),
            Some(ReadBackoffEntry {
                failed_at_ns: t0,
                delay_ns: SEC,
            })
        );
        assert_eq!(backoff.record_failure("k", t0 + SEC - 1), 1);
        assert_eq!(backoff.entry(&"k").map(|e| e.retry_at_ns()), Some(t0 + SEC));

        let t1 = t0 + SEC;
        assert_eq!(backoff.record_failure("k", t1), 2 * SEC);
        assert_eq!(
            backoff.entry(&"k"),
            Some(ReadBackoffEntry {
                failed_at_ns: t1,
                delay_ns: 2 * SEC,
            })
        );
        let late = t1 + 5 * SEC;
        assert_eq!(backoff.record_failure("k", late), 4 * SEC);
        assert_eq!(backoff.blocked_until(&"k", late), Some(late + 4 * SEC));
    }

    /// Many failures inside and at the edge of each window never take it past
    /// the cap.
    #[test]
    fn cap_holds_with_failures_inside_windows() {
        let mut backoff = ReadBackoff::new();
        let mut now = 0_i64;
        for _ in 0..20 {
            let delay = backoff.record_failure("k", now);
            for _ in 0..3 {
                backoff.record_failure("k", now);
            }
            assert!(delay <= PROVISIONING_READ_BACKOFF_MAX_NS);
            now += delay;
        }
        assert_eq!(
            backoff.entry(&"k").map(|e| e.delay_ns),
            Some(PROVISIONING_READ_BACKOFF_MAX_NS)
        );
    }

    #[test]
    fn idle_eviction_drops_old_failures_only() {
        let mut backoff = ReadBackoff::new();
        backoff.record_failure(1_u8, 0);
        backoff.record_failure(2_u8, 500);
        assert_eq!(backoff.evict_idle(1_000, 600), 1);
        assert_eq!(backoff.entry(&1), None);
        assert!(backoff.entry(&2).is_some());
    }
}
