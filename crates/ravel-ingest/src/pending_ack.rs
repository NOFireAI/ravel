//! Bound on pending strict-mode acks (ADR-2793 decision 4), process-wide when
//! one instance is shared across the three routers.
//!
//! A strict write's own ack wait is bounded by its `ack_deadline`, but nothing
//! bounds how many such waits may be outstanding across the process at once:
//! [`PendingAckBudget`] is that bound, a semaphore-backed gauge shared by
//! `Arc` across the metrics, log, and span routers so one ceiling (sized by
//! ravel-server from `--max-pending-strict-acks`) bounds the sum across every
//! signal, the same sharing shape [`crate::IngestByteBudget`] uses for the
//! buffered-byte ceiling.
//!
//! A strict `submit` takes a permit before charging the byte budget and before
//! touching any shard, so a refused submit holds no byte charge. When the
//! permit is released is stated once, on [`PendingAckPermit`].
//!
//! The gauge and counter are process-wide only when every router is built
//! from one shared `Arc<PendingAckBudget>`; each router reports the budget it
//! was given through its `metrics_snapshot()`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// `0` disables the ceiling (unlimited), matching every other admission
/// ceiling in this crate that spells "no limit" as `0` rather than a sentinel
/// `u64::MAX` ([`crate::IngestByteBudgetLimit`]).
#[derive(Debug)]
pub struct PendingAckBudget {
    /// `None` when unlimited: [`Self::held`] still tracks, but
    /// [`Self::try_acquire`] never refuses.
    semaphore: Option<Arc<Semaphore>>,
    /// Currently outstanding pending-ack permits, for the
    /// `pending_strict_acks` gauge. Tracked independently of the semaphore so
    /// the gauge reads correctly even when unlimited, when no semaphore exists
    /// to query.
    held: Arc<AtomicU64>,
    /// Cumulative refusals since construction, for the `pending_ack_shed`
    /// counter.
    shed_total: AtomicU64,
}

impl PendingAckBudget {
    /// A budget of `limit` permits, `0` for unlimited. A limit above
    /// [`Semaphore::MAX_PERMITS`] is clamped to it, since tokio's semaphore
    /// cannot hold more permits than that.
    pub fn new(limit: u64) -> Self {
        let permits = usize::try_from(limit)
            .unwrap_or(usize::MAX)
            .min(Semaphore::MAX_PERMITS);
        PendingAckBudget {
            semaphore: (permits > 0).then(|| Arc::new(Semaphore::new(permits))),
            held: Arc::new(AtomicU64::new(0)),
            shed_total: AtomicU64::new(0),
        }
    }

    /// Takes one pending-ack permit, or refuses immediately -- never queuing a
    /// waiter -- once the ceiling is already held. Always succeeds under an
    /// unlimited budget.
    pub fn try_acquire(&self) -> Option<PendingAckPermit> {
        let permit = match &self.semaphore {
            Some(semaphore) => match Arc::clone(semaphore).try_acquire_owned() {
                Ok(permit) => Some(permit),
                Err(_) => {
                    self.shed_total.fetch_add(1, Ordering::Relaxed);
                    return None;
                }
            },
            None => None,
        };
        self.held.fetch_add(1, Ordering::AcqRel);
        Some(PendingAckPermit {
            held: Arc::clone(&self.held),
            _permit: permit,
        })
    }

    /// Outstanding pending-ack permits, for the `pending_strict_acks` gauge.
    pub fn held(&self) -> u64 {
        self.held.load(Ordering::Acquire)
    }

    /// Cumulative refusals since construction, for the `pending_ack_shed`
    /// counter.
    pub fn shed_total(&self) -> u64 {
        self.shed_total.load(Ordering::Relaxed)
    }
}

/// A held pending-ack permit. Releases on `Drop`, exactly once: when `acks()`
/// resolves (ack success or a shard-level error), when its `ack_deadline`
/// elapses (the duration is captured at submit and the clock starts at the
/// first poll of `acks()`), or when the `Submitted` value is dropped. A
/// `Submitted` that is never polled holds its permit until it is dropped. A
/// plain RAII guard gives all three, with no explicit release call on any
/// path.
#[derive(Debug)]
pub struct PendingAckPermit {
    held: Arc<AtomicU64>,
    _permit: Option<OwnedSemaphorePermit>,
}

impl Drop for PendingAckPermit {
    fn drop(&mut self) {
        self.held.fetch_sub(1, Ordering::AcqRel);
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn unlimited_never_refuses_but_tracks_the_gauge() {
        let budget = PendingAckBudget::new(0);
        let permits: Vec<_> = (0..1_000)
            .map(|_| budget.try_acquire().expect("unlimited never refuses"))
            .collect();
        assert_eq!(budget.held(), 1_000);
        assert_eq!(budget.shed_total(), 0);
        drop(permits);
        assert_eq!(budget.held(), 0);
    }

    #[test]
    fn bounded_refuses_past_the_limit_and_releases_on_drop() {
        let budget = PendingAckBudget::new(2);
        let a = budget.try_acquire().expect("under limit");
        let b = budget.try_acquire().expect("at limit");
        assert!(budget.try_acquire().is_none(), "over limit refuses");
        assert_eq!(budget.shed_total(), 1);
        assert_eq!(budget.held(), 2);
        drop(a);
        assert_eq!(budget.held(), 1, "dropping one permit frees one slot");
        let _c = budget.try_acquire().expect("room freed after drop");
        assert_eq!(budget.held(), 2);
        drop(b);
    }

    #[test]
    fn limit_above_semaphore_max_is_clamped() {
        let budget = PendingAckBudget::new(u64::MAX);
        let permit = budget.try_acquire().expect("a clamped budget still admits");
        assert_eq!(budget.held(), 1);
        assert_eq!(budget.shed_total(), 0);
        drop(permit);
        assert_eq!(budget.held(), 0);
    }

    #[test]
    fn shed_counter_is_cumulative() {
        let budget = PendingAckBudget::new(1);
        let _held = budget.try_acquire().expect("at limit");
        for _ in 0..5 {
            assert!(budget.try_acquire().is_none());
        }
        assert_eq!(budget.shed_total(), 5);
    }
}
