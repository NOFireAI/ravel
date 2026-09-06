//! Process-wide memory accountant (ADR-1170 decision 1).
//!
//! This crate exposes two shapes over one counter. `MemoryBudget`'s
//! `try_reserve`/`reserve_unchecked`/`release` methods are direct counter
//! operations for a caller that manages its own lifetime (ravel-sql).
//! `MemoryBudget::reserve` returns a `Reservation`, an RAII guard that
//! releases its size on drop, for a caller that wants the accounting tied
//! to a value's lifetime (ravel-query). Ownership rule: whoever holds the
//! buffer holds the guard, so a `Reservation` is constructed from an
//! `Arc<MemoryBudget>` and travels with the buffer it accounts for across
//! threads and tasks, rather than borrowing the budget for a scope.
//!
//! `MemoryBudget` bounds the sum of allocations that were explicitly
//! reserved through it. It is not an RSS ceiling: memory never routed
//! through a `try_reserve`/`reserve`/`reserve_unchecked` call is invisible
//! to it (ADR-1170 constraint 4).

use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// A process-wide (or scope-wide) accounting of reserved bytes against a
/// fixed limit. All operations are lock-free and safe to call from any
/// number of threads concurrently.
#[derive(Debug)]
pub struct MemoryBudget {
    limit: u64,
    reserved: AtomicU64,
    handoff_overlap: AtomicU64,
}

impl MemoryBudget {
    /// Builds a budget that admits at most `limit` reserved bytes at a time.
    pub fn new(limit: u64) -> Self {
        Self {
            limit,
            reserved: AtomicU64::new(0),
            handoff_overlap: AtomicU64::new(0),
        }
    }

    /// Builds a budget that never refuses a reservation for any total that
    /// fits in a `u64`. At the `u64` ceiling it refuses rather than
    /// miscounts: see [`try_reserve`]'s overflow rule.
    ///
    /// [`try_reserve`]: MemoryBudget::try_reserve
    pub fn unlimited() -> Self {
        Self::new(u64::MAX)
    }

    /// The configured limit.
    pub fn limit(&self) -> u64 {
        self.limit
    }

    /// Bytes currently reserved.
    pub fn reserved(&self) -> u64 {
        self.reserved.load(Ordering::Acquire)
    }

    /// Bytes currently counted as handed off (see [`note_handoff`]).
    ///
    /// [`note_handoff`]: MemoryBudget::note_handoff
    pub fn handoff_overlap(&self) -> u64 {
        self.handoff_overlap.load(Ordering::Acquire)
    }

    /// Reserves `n` bytes if doing so would not exceed `limit`. On
    /// success, `reserved()` grows by exactly `n`. On failure, nothing
    /// changes.
    ///
    /// Overflow rule: a total that would not fit in a `u64` is refused
    /// with `Err`, regardless of `limit` (so `unlimited()`, whose limit is
    /// `u64::MAX`, still refuses at the ceiling instead of recording fewer
    /// bytes than it admitted).
    pub fn try_reserve(&self, n: u64) -> Result<(), MemoryExhausted> {
        let mut current = self.reserved.load(Ordering::Acquire);
        loop {
            let next = match current.checked_add(n) {
                Some(next) if next <= self.limit => next,
                _ => {
                    return Err(MemoryExhausted {
                        requested: n,
                        reserved: current,
                        limit: self.limit,
                    });
                }
            };
            match self.reserved.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(observed) => current = observed,
            }
        }
    }

    /// Unconditionally reserves `n` bytes, ignoring `limit`, and returns
    /// the new total so a caller can detect a breach itself. This is the
    /// infallible-grow path: it never fails, it saturates instead of
    /// wrapping. Saturating at the `u64` ceiling here is a caller bug the
    /// counter cannot repair: a `reserve_unchecked` call that pins the
    /// counter at `u64::MAX` makes every later `try_reserve` on this
    /// budget refuse (per its overflow rule above), which is the closest
    /// this type can come to surfacing the caller's error.
    pub fn reserve_unchecked(&self, n: u64) -> u64 {
        let mut current = self.reserved.load(Ordering::Acquire);
        loop {
            let next = current.saturating_add(n);
            match self.reserved.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return next,
                Err(observed) => current = observed,
            }
        }
    }

    /// Releases `n` previously reserved bytes. Saturates at zero rather
    /// than underflowing: a release larger than what is outstanding is a
    /// caller bug, and this method does not report it as an error.
    pub fn release(&self, n: u64) {
        let mut current = self.reserved.load(Ordering::Acquire);
        loop {
            let next = current.saturating_sub(n);
            match self.reserved.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }

    /// Notes that `n` bytes are currently double-counted across two
    /// ledgers (the fetch-layer handoff overlap from ADR-1170). Saturating
    /// add; the semantics of when to call this belong to the fetch layer.
    pub fn note_handoff(&self, n: u64) {
        let mut current = self.handoff_overlap.load(Ordering::Acquire);
        loop {
            let next = current.saturating_add(n);
            match self.handoff_overlap.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }

    /// Clears `n` bytes previously noted with [`note_handoff`]. Saturating
    /// subtract.
    ///
    /// [`note_handoff`]: MemoryBudget::note_handoff
    pub fn clear_handoff(&self, n: u64) {
        let mut current = self.handoff_overlap.load(Ordering::Acquire);
        loop {
            let next = current.saturating_sub(n);
            match self.handoff_overlap.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }

    /// Reserves `n` bytes and returns a guard that releases them on drop.
    /// Takes `self` as `&Arc<Self>` so the returned [`Reservation`] owns
    /// its own `Arc` clone and can travel with the buffer it accounts for
    /// across threads and tasks, rather than being tied to a borrow scope.
    pub fn reserve(self: &Arc<Self>, n: u64) -> Result<Reservation, MemoryExhausted> {
        self.try_reserve(n)?;
        Ok(Reservation {
            budget: Arc::clone(self),
            size: n,
            handed_off: false,
        })
    }
}

/// An RAII guard over a reservation made against a [`MemoryBudget`].
/// Releases exactly its size when dropped. Not `Clone`: a reservation
/// represents one exclusive claim on the budget.
#[derive(Debug)]
pub struct Reservation {
    budget: Arc<MemoryBudget>,
    size: u64,
    handed_off: bool,
}

impl Reservation {
    /// The number of bytes this guard holds reserved.
    pub fn size(&self) -> u64 {
        self.size
    }

    /// Marks this reservation's bytes as hidden off to another ledger,
    /// calling [`MemoryBudget::note_handoff`] exactly once no matter how
    /// many times this is called. On drop, the corresponding
    /// `clear_handoff` runs only if this was called.
    pub fn mark_handed_off(&mut self) {
        if !self.handed_off {
            self.budget.note_handoff(self.size);
            self.handed_off = true;
        }
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.budget.release(self.size);
        if self.handed_off {
            self.budget.clear_handoff(self.size);
        }
    }
}

/// A `try_reserve` or `reserve` call was refused because it would have
/// exceeded the budget's limit. Carries no strings, keys, or tenant
/// values by construction: only the three figures needed to explain the
/// refusal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryExhausted {
    pub requested: u64,
    pub reserved: u64,
    pub limit: u64,
}

impl fmt::Display for MemoryExhausted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "memory exhausted: requested {} bytes, {} of {} byte limit already reserved",
            self.requested, self.reserved, self.limit
        )
    }
}

impl Error for MemoryExhausted {}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn try_reserve_succeeds_up_to_limit_then_fails() {
        let budget = MemoryBudget::new(100);
        budget.try_reserve(60).expect("60 of 100 fits");
        budget.try_reserve(40).expect("100 of 100 fits exactly");
        let before = budget.reserved();
        let err = budget.try_reserve(1).expect_err("101 of 100 must not fit");
        assert_eq!(err.requested, 1);
        assert_eq!(err.reserved, 100);
        assert_eq!(err.limit, 100);
        assert_eq!(budget.reserved(), before);
    }

    #[test]
    fn try_reserve_over_limit_from_zero_fails_and_counts_nothing() {
        let budget = MemoryBudget::new(100);
        let err = budget
            .try_reserve(101)
            .expect_err("101 of 100 must not fit");
        assert_eq!(err.requested, 101);
        assert_eq!(err.reserved, 0);
        assert_eq!(err.limit, 100);
        assert_eq!(budget.reserved(), 0);
    }

    /// Discriminates the CAS loop from a fetch_add-then-rollback
    /// implementation: a losing thread must observe no change at all, not
    /// a transient over-admit that gets corrected after the fact.
    /// Replacing the CAS loop in `try_reserve` with a bare `fetch_add`
    /// followed by a check-and-subtract makes this test flaky/fail, since
    /// the losing thread's rollback runs after other threads may have
    /// already observed the bogus intermediate total.
    #[test]
    fn concurrent_try_reserve_rolls_back_losing_attempt_cleanly() {
        let budget = Arc::new(MemoryBudget::new(100));
        budget.try_reserve(60).expect("60 of 100 fits");

        let budget_b = Arc::clone(&budget);
        let b = thread::spawn(move || budget_b.try_reserve(110));
        let budget_c = Arc::clone(&budget);
        let c = thread::spawn(move || budget_c.try_reserve(40));

        let b_result = b.join().expect("thread B panicked");
        let c_result = c.join().expect("thread C panicked");

        assert!(b_result.is_err(), "60 + 110 must not fit in 100");
        assert!(c_result.is_ok(), "60 + 40 must fit in 100 exactly");
        assert_eq!(budget.reserved(), 100);
    }

    #[test]
    fn reserve_unchecked_breaches_limit_and_release_recovers() {
        let budget = MemoryBudget::new(100);
        let total = budget.reserve_unchecked(150);
        assert_eq!(total, 150);
        assert_eq!(budget.reserved(), 150);

        budget.release(100);
        assert_eq!(budget.reserved(), 50);
        budget
            .try_reserve(50)
            .expect("50 of 100 fits after release");
        assert_eq!(budget.reserved(), 100);
    }

    #[test]
    fn reservation_drop_releases_exact_size_either_order() {
        let budget = Arc::new(MemoryBudget::new(100));
        let a = budget.reserve(30).expect("30 fits");
        let b = budget.reserve(70).expect("70 fits");
        assert_eq!(budget.reserved(), 100);
        drop(a);
        assert_eq!(budget.reserved(), 70);
        drop(b);
        assert_eq!(budget.reserved(), 0);

        let a = budget.reserve(30).expect("30 fits");
        let b = budget.reserve(70).expect("70 fits");
        assert_eq!(budget.reserved(), 100);
        drop(b);
        assert_eq!(budget.reserved(), 30);
        drop(a);
        assert_eq!(budget.reserved(), 0);
    }

    #[test]
    fn concurrent_try_reserve_admits_exactly_the_limit() {
        let budget = Arc::new(MemoryBudget::new(32));
        let handles: Vec<_> = (0..64)
            .map(|_| {
                let budget = Arc::clone(&budget);
                thread::spawn(move || budget.try_reserve(1).is_ok())
            })
            .collect();
        let ok_count = handles
            .into_iter()
            .map(|h| h.join().expect("thread panicked"))
            .filter(|ok| *ok)
            .count();
        assert_eq!(ok_count, 32);
        assert_eq!(budget.reserved(), 32);
    }

    #[test]
    fn unlimited_never_fails_and_counts_exactly() {
        let budget = MemoryBudget::unlimited();
        let half = u64::MAX / 2;
        budget.try_reserve(half).expect("unlimited never fails");
        budget.try_reserve(half).expect("unlimited never fails");
        assert_eq!(budget.reserved(), u64::MAX - 1);

        // At the u64 ceiling, unlimited() refuses rather than miscounts.
        let err = budget
            .try_reserve(2)
            .expect_err("u64::MAX - 1 + 2 overflows u64 and must be refused");
        assert_eq!(err.requested, 2);
        assert_eq!(err.reserved, u64::MAX - 1);
        assert_eq!(err.limit, u64::MAX);
        assert_eq!(budget.reserved(), u64::MAX - 1);

        budget
            .try_reserve(1)
            .expect("u64::MAX - 1 + 1 fits exactly at the ceiling");
        assert_eq!(budget.reserved(), u64::MAX);
    }

    #[test]
    fn reserve_unchecked_saturates_at_ceiling() {
        let budget = MemoryBudget::new(100);
        assert_eq!(budget.reserve_unchecked(u64::MAX), u64::MAX);
        assert_eq!(budget.reserve_unchecked(1), u64::MAX);
        assert_eq!(budget.reserved(), u64::MAX);
    }

    #[test]
    fn clear_handoff_past_zero_saturates() {
        let budget = MemoryBudget::new(100);
        budget.note_handoff(5);
        budget.clear_handoff(10);
        assert_eq!(budget.handoff_overlap(), 0);
    }

    #[test]
    fn release_past_zero_saturates() {
        let budget = MemoryBudget::new(100);
        budget.try_reserve(10).expect("10 of 100 fits");
        budget.release(50);
        assert_eq!(budget.reserved(), 0);
    }

    #[test]
    fn reservation_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Reservation>();
    }

    #[test]
    fn handoff_notes_once_and_clears_on_drop() {
        let budget = Arc::new(MemoryBudget::new(100));
        let mut guard = budget.reserve(40).expect("40 fits");
        guard.mark_handed_off();
        guard.mark_handed_off();
        assert_eq!(budget.handoff_overlap(), 40);
        drop(guard);
        assert_eq!(budget.handoff_overlap(), 0);

        let guard = budget.reserve(40).expect("40 fits");
        assert_eq!(budget.handoff_overlap(), 0);
        drop(guard);
        assert_eq!(budget.handoff_overlap(), 0);
    }
}
