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
//!
//! Each budget also carries a resident gate (ADR-2633): a flag that, while
//! closed, makes every `try_reserve` of more than zero bytes refuse with
//! [`ExhaustionCause::Resident`], whatever the ledger holds. See
//! [`MemoryBudget::set_resident_gate`].

use std::error::Error;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering, fence};

/// A process-wide (or scope-wide) accounting of reserved bytes against a
/// fixed limit. All operations are lock-free and safe to call from any
/// number of threads concurrently.
#[derive(Debug)]
pub struct MemoryBudget {
    limit: u64,
    reserved: AtomicU64,
    fetch_reserved: AtomicU64,
    handoff_overlap: AtomicU64,
    gate: ResidentGate,
    refusals: RefusalCounts,
}

/// The resident gate's state (ADR-2633 section 1): whether it is open, and
/// the process resident reading and high-water mark it was last set from.
///
/// Written only by [`MemoryBudget::set_resident_gate`], whose caller is the
/// server's memory-gate sampler thread (ADR-2633 task 2, issue #2730). Until
/// that sampler exists nothing in production calls the setter, so every
/// budget stays open and `try_reserve` behaves as it did before the gate.
///
/// The 128-byte alignment gives the group a block of its own, apart from
/// `reserved`: a check site's load of `open` never shares a cache line with
/// a reservation's CAS, and the sampler's write at most once per interval is
/// the only thing that invalidates the line. 128 rather than 64 covers
/// adjacent-line prefetch on x86_64 and the 128-byte lines of some aarch64
/// cores.
#[derive(Debug)]
#[repr(align(128))]
struct ResidentGate {
    open: AtomicBool,
    resident: AtomicU64,
    high_water: AtomicU64,
}

/// Refusals of `try_reserve`, by cause, on a block of their own so a burst
/// of refusals does not contend with the reservation CAS or the gate flag.
#[derive(Debug)]
#[repr(align(128))]
struct RefusalCounts {
    accounted: AtomicU64,
    gate: AtomicU64,
}

impl MemoryBudget {
    /// Builds a budget that admits at most `limit` reserved bytes at a time.
    /// The resident gate starts open, with a reading and mark of 0.
    pub fn new(limit: u64) -> Self {
        Self {
            limit,
            reserved: AtomicU64::new(0),
            fetch_reserved: AtomicU64::new(0),
            handoff_overlap: AtomicU64::new(0),
            gate: ResidentGate {
                open: AtomicBool::new(true),
                resident: AtomicU64::new(0),
                high_water: AtomicU64::new(0),
            },
            refusals: RefusalCounts {
                accounted: AtomicU64::new(0),
                gate: AtomicU64::new(0),
            },
        }
    }

    /// Records a process resident reading against the high-water mark and
    /// sets the gate from them: closed when `resident >= high_water`, open
    /// otherwise. This is the gate's only writer; the server's sampler thread
    /// calls it once per sample with the post-purge reading (ADR-2633
    /// section 2), and a test can call it to hold the gate closed.
    pub fn set_resident_gate(&self, resident: u64, high_water: u64) {
        self.gate.resident.store(resident, Ordering::Relaxed);
        self.gate.high_water.store(high_water, Ordering::Relaxed);
        // Release pairs with the acquire fence on the refusal path, so a
        // refusal reports this call's figures or a later call's, never an
        // earlier one's.
        self.gate
            .open
            .store(resident < high_water, Ordering::Release);
    }

    /// Whether the resident gate is open. A relaxed load: the check sites
    /// need the flag, not an ordering with the reading or the mark.
    pub fn gate_open(&self) -> bool {
        self.gate.open.load(Ordering::Relaxed)
    }

    /// The resident reading of the last [`set_resident_gate`] call, 0 before
    /// the first.
    ///
    /// [`set_resident_gate`]: MemoryBudget::set_resident_gate
    pub fn gate_resident(&self) -> u64 {
        self.gate.resident.load(Ordering::Relaxed)
    }

    /// The high-water mark of the last [`set_resident_gate`] call, 0 before
    /// the first.
    ///
    /// [`set_resident_gate`]: MemoryBudget::set_resident_gate
    pub fn gate_high_water(&self) -> u64 {
        self.gate.high_water.load(Ordering::Relaxed)
    }

    /// `try_reserve` calls (including those made by [`reserve`]) refused
    /// because the ledger would exceed `limit`: the
    /// [`ExhaustionCause::Accounted`] refusals.
    ///
    /// [`reserve`]: MemoryBudget::reserve
    pub fn accounted_refusals(&self) -> u64 {
        self.refusals.accounted.load(Ordering::Relaxed)
    }

    /// `try_reserve` calls (including those made by [`reserve`]) refused
    /// because the resident gate was closed: the
    /// [`ExhaustionCause::Resident`] refusals.
    ///
    /// [`reserve`]: MemoryBudget::reserve
    pub fn gate_refusals(&self) -> u64 {
        self.refusals.gate.load(Ordering::Relaxed)
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

    /// Bytes currently counted as handed off (see [`note_handoff`]): the
    /// summed sizes of live [`Reservation`]s on which
    /// [`Reservation::mark_handed_off`] was called, each at its full size from
    /// the mark until the reservation drops. The fetch layer marks a
    /// reservation when the bytes it covers go through a consumer with its own
    /// byte ledger (the read cache), so this is how much of `fetch_reserved()`
    /// that other ledger also counts. It is a subset of `fetch_reserved()` when
    /// no reservation is changing, never an addition to `reserved()`.
    ///
    /// [`note_handoff`]: MemoryBudget::note_handoff
    pub fn handoff_overlap(&self) -> u64 {
        self.handoff_overlap.load(Ordering::Acquire)
    }

    /// Bytes currently reserved through [`reserve`] (the RAII `Reservation`
    /// API, whose callers include `SegmentFetcher`, `LogSegmentFetcher`,
    /// `SpanSegmentFetcher`, and the DDL footer reads `ravel-parquet`'s
    /// `snapshot_location` makes for `CREATE EXTERNAL TABLE`), a subset of
    /// `reserved()`. `try_reserve` and `reserve_unchecked` (the raw counter
    /// API SQL's `TenantMemoryAccountant` uses exclusively) never touch this
    /// counter, so it never counts SQL execution's share.
    ///
    /// [`reserve`]: MemoryBudget::reserve
    pub fn fetch_reserved(&self) -> u64 {
        self.fetch_reserved.load(Ordering::Acquire)
    }

    /// Bytes reserved by everything other than the fetch layer:
    /// `reserved()` minus `fetch_reserved()`, saturating. The raw counter API
    /// is this budget's only other reserver, so this is SQL's share, without
    /// SQL needing its own counter or any change to `ravel-sql`.
    ///
    /// The two counters are separate atomics, not updated together. [`reserve`]
    /// adds to `reserved` before `fetch_reserved`, and dropping a
    /// [`Reservation`] clears `fetch_reserved` before `reserved`, so the
    /// counters never hold a state that puts this below SQL's share. They sum
    /// to the reserved total when no reservation is changing. While fetch
    /// reservations are being made or dropped, this method's two separate
    /// loads can skew a reading by the summed sizes of the reservations that
    /// changed between them, in either direction.
    ///
    /// [`reserve`]: MemoryBudget::reserve
    pub fn sql_reserved(&self) -> u64 {
        self.reserved().saturating_sub(self.fetch_reserved())
    }

    /// Reserves `n` bytes if the resident gate is open and doing so would
    /// not exceed `limit`. On success, `reserved()` grows by exactly `n`. On
    /// failure, `reserved()` does not change and exactly one of
    /// [`accounted_refusals`] and [`gate_refusals`] grows by one.
    ///
    /// Gate rule: while the gate is closed, any `n > 0` is refused with
    /// [`ExhaustionCause::Resident`] before the reservation CAS runs. A
    /// zero-byte reservation is always admitted by the gate (the ledger
    /// still refuses it when it is already over `limit`).
    ///
    /// Overflow rule: a total that would not fit in a `u64` is refused
    /// with `Err`, regardless of `limit` (so `unlimited()`, whose limit is
    /// `u64::MAX`, still refuses at the ceiling instead of recording fewer
    /// bytes than it admitted).
    ///
    /// [`accounted_refusals`]: MemoryBudget::accounted_refusals
    /// [`gate_refusals`]: MemoryBudget::gate_refusals
    pub fn try_reserve(&self, n: u64) -> Result<(), MemoryExhausted> {
        if n > 0
            && !self.gate_open()
            && let Some(refusal) = self.refuse_resident(n)
        {
            return Err(refusal);
        }
        let mut current = self.reserved.load(Ordering::Acquire);
        loop {
            let next = match current.checked_add(n) {
                Some(next) if next <= self.limit => next,
                _ => {
                    self.refusals.accounted.fetch_add(1, Ordering::Relaxed);
                    return Err(MemoryExhausted {
                        requested: n,
                        reserved: current,
                        limit: self.limit,
                        cause: ExhaustionCause::Accounted,
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

    /// The gate refusal of an `n`-byte `try_reserve`, out of line so the
    /// open-gate path carries none of it. `None` when the figures read here
    /// show the gate reopened after the caller saw it closed: the caller
    /// then goes on to the ledger, and no refusal is counted.
    #[cold]
    #[inline(never)]
    fn refuse_resident(&self, n: u64) -> Option<MemoryExhausted> {
        // Pairs with the release store in `set_resident_gate`: the figures
        // below are those of the write that closed the gate or a later one.
        fence(Ordering::Acquire);
        let resident = self.gate_resident();
        let high_water = self.gate_high_water();
        if resident < high_water {
            return None;
        }
        self.refusals.gate.fetch_add(1, Ordering::Relaxed);
        Some(MemoryExhausted {
            requested: n,
            reserved: self.reserved.load(Ordering::Relaxed),
            limit: self.limit,
            cause: ExhaustionCause::Resident {
                resident,
                high_water,
            },
        })
    }

    /// Unconditionally reserves `n` bytes, ignoring `limit`, and returns
    /// the new total so a caller can detect a breach itself. This is the
    /// infallible-grow path: it never fails, it saturates instead of
    /// wrapping. Saturating at the `u64` ceiling here is a caller bug the
    /// counter cannot repair: a `reserve_unchecked` call that pins the
    /// counter at `u64::MAX` makes every later `try_reserve` on this
    /// budget refuse (per its overflow rule above), which is the closest
    /// this type can come to surfacing the caller's error.
    ///
    /// The resident gate does not apply here: a closed gate neither refuses
    /// nor counts this call.
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

    /// Grows `fetch_reserved` by `n`. Saturating add, mirroring
    /// `reserve_unchecked`: called only from [`reserve`] right after
    /// `try_reserve` already admitted `n` against `limit`, so saturation
    /// here is unreachable in practice, not a silent-miscount path.
    ///
    /// [`reserve`]: MemoryBudget::reserve
    fn note_fetch_reserved(&self, n: u64) {
        let mut current = self.fetch_reserved.load(Ordering::Acquire);
        loop {
            let next = current.saturating_add(n);
            match self.fetch_reserved.compare_exchange_weak(
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

    /// Shrinks `fetch_reserved` by `n`. Saturating subtract, called only
    /// from [`Reservation`]'s `Drop`.
    fn clear_fetch_reserved(&self, n: u64) {
        let mut current = self.fetch_reserved.load(Ordering::Acquire);
        loop {
            let next = current.saturating_sub(n);
            match self.fetch_reserved.compare_exchange_weak(
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
    /// Also grows `fetch_reserved()` by `n`, since this RAII path is the
    /// fetch layer's alone (see [`fetch_reserved`]'s doc comment).
    ///
    /// [`fetch_reserved`]: MemoryBudget::fetch_reserved
    pub fn reserve(self: &Arc<Self>, n: u64) -> Result<Reservation, MemoryExhausted> {
        self.try_reserve(n)?;
        self.note_fetch_reserved(n);
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

    /// Marks this reservation's bytes as handed off to another ledger,
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
        // Innermost share first: the hand-off overlap is part of the fetch
        // share, which is part of the total, so each counter is cleared before
        // the one that contains it.
        if self.handed_off {
            self.budget.clear_handoff(self.size);
        }
        self.budget.clear_fetch_reserved(self.size);
        self.budget.release(self.size);
    }
}

/// A `try_reserve` or `reserve` call was refused, for the reason `cause`
/// names. Carries no strings, keys, or tenant values by construction: only
/// figures.
///
/// `requested`, `reserved` and `limit` are filled for both causes:
/// `reserved` is the ledger's total as the refusal read it. For a
/// [`ExhaustionCause::Resident`] refusal they do not explain the refusal
/// (the ledger is usually far from `limit` when the gate closes); the cause
/// carries the figures that do.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MemoryExhausted {
    pub requested: u64,
    pub reserved: u64,
    pub limit: u64,
    pub cause: ExhaustionCause,
}

/// Why a reservation was refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExhaustionCause {
    /// The ledger would have exceeded its limit (or overflowed a `u64`).
    Accounted,
    /// The resident gate was closed: the process resident reading was at or
    /// above the high-water mark when the gate was last set.
    Resident { resident: u64, high_water: u64 },
}

impl fmt::Display for MemoryExhausted {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.cause {
            ExhaustionCause::Accounted => write!(
                f,
                "memory exhausted: requested {} bytes, {} of {} byte limit already reserved",
                self.requested, self.reserved, self.limit
            ),
            ExhaustionCause::Resident {
                resident,
                high_water,
            } => write!(
                f,
                "memory exhausted: process resident {resident} bytes at or above the \
                 {high_water} byte high-water mark (requested {})",
                self.requested
            ),
        }
    }
}

impl Error for MemoryExhausted {}

/// The bytes a decode of a `declared`-byte body (a section, frame, part,
/// postings or column-statistics object) may allocate under a decoder whose
/// ceiling is `ceiling`: the declared length itself, or 0 when it is over the
/// ceiling, since the decoder refuses an oversized one before it allocates
/// anything (ADR-1702 decision 6).
///
/// Charging the ceiling instead turns that refusal into a budget refusal
/// whenever the budget has less than the ceiling free, which reports memory
/// pressure where an oversized object was, replaces the decoder's own typed
/// error with a retryable one, and takes the caller down a different path (a
/// catalog resolve fails instead of falling back to listing) than the
/// decoder's refusal would. Charging 0 leaves the outcome to the decoder only
/// while the budget is within its limit: a budget already over it (a
/// `reserve_unchecked` caller can put it there) refuses even a 0-byte
/// reservation.
#[must_use]
pub fn decoded_charge(declared: u64, ceiling: u64) -> u64 {
    if declared > ceiling { 0 } else { declared }
}

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

    /// `fetch_reserved()` counts only the RAII path; `try_reserve` (SQL's
    /// raw counter API) grows `reserved()` without moving it, so
    /// `sql_reserved()` isolates SQL's share even though both reservers
    /// share one `MemoryBudget`.
    #[test]
    fn fetch_reserved_counts_only_the_raii_path() {
        let budget = Arc::new(MemoryBudget::new(200));
        budget.try_reserve(50).expect("50 of 200 fits");
        assert_eq!(budget.reserved(), 50);
        assert_eq!(budget.fetch_reserved(), 0, "raw try_reserve is not fetch");
        assert_eq!(budget.sql_reserved(), 50);

        let guard = budget.reserve(30).expect("30 of 150 remaining fits");
        assert_eq!(budget.reserved(), 80);
        assert_eq!(
            budget.fetch_reserved(),
            30,
            "reserve()'s RAII path must grow fetch_reserved by exactly its size"
        );
        assert_eq!(
            budget.sql_reserved(),
            50,
            "the earlier try_reserve share must be unaffected by the new fetch reservation"
        );

        drop(guard);
        assert_eq!(budget.reserved(), 50);
        assert_eq!(
            budget.fetch_reserved(),
            0,
            "dropping the Reservation must release fetch_reserved back to 0"
        );
        assert_eq!(budget.sql_reserved(), 50);
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

    /// A closed gate refuses with the resident cause before the reservation
    /// CAS runs, on a ledger that has room.
    ///
    /// FLIP: move the gate check in `try_reserve` after the CAS (refusing
    /// once `compare_exchange_weak` succeeds) and `reserved()` reads 11, not
    /// 10. Make it part of the ledger's limit arm instead and
    /// `try_reserve(1)` is admitted, since 11 of 100 fits.
    #[test]
    fn a_closed_gate_refuses_with_the_resident_cause_and_reserves_nothing() {
        let budget = MemoryBudget::new(100);
        budget.try_reserve(10).expect("the gate starts open");
        assert!(budget.gate_open());

        budget.set_resident_gate(700, 500);
        assert!(!budget.gate_open());
        assert_eq!(budget.gate_resident(), 700);
        assert_eq!(budget.gate_high_water(), 500);

        let err = budget
            .try_reserve(1)
            .expect_err("a closed gate refuses one byte");
        assert_eq!(
            err,
            MemoryExhausted {
                requested: 1,
                reserved: 10,
                limit: 100,
                cause: ExhaustionCause::Resident {
                    resident: 700,
                    high_water: 500,
                },
            }
        );
        assert_eq!(budget.reserved(), 10, "a gate refusal reserves nothing");

        budget
            .try_reserve(0)
            .expect("a zero-byte reservation passes a closed gate");
        assert_eq!(budget.reserved(), 10);
    }

    #[test]
    fn a_reading_equal_to_the_mark_closes_and_one_below_reopens() {
        let budget = Arc::new(MemoryBudget::new(100));
        budget.set_resident_gate(500, 500);
        assert!(!budget.gate_open(), "at the mark closes");
        budget
            .reserve(1)
            .expect_err("reserve goes through the closed gate");

        budget.set_resident_gate(499, 500);
        assert!(budget.gate_open(), "below the mark opens");
        let guard = budget.reserve(1).expect("a reopened gate admits");
        assert_eq!(budget.reserved(), 1);
        drop(guard);
        budget.try_reserve(100).expect("the whole limit fits again");
    }

    /// Each refusal kind moves its own counter by exactly one.
    ///
    /// FLIP: count both causes in one counter (increment
    /// `refusals.accounted` in `refuse_resident`) and the first
    /// `gate_refusals()` assertion reads 0, the `accounted_refusals()` one 1.
    #[test]
    fn each_refusal_kind_moves_only_its_own_counter() {
        let budget = Arc::new(MemoryBudget::new(100));
        assert_eq!(
            (budget.accounted_refusals(), budget.gate_refusals()),
            (0, 0)
        );

        budget.set_resident_gate(10, 5);
        budget.try_reserve(1).expect_err("closed");
        assert_eq!(budget.gate_refusals(), 1);
        assert_eq!(budget.accounted_refusals(), 0);
        budget.try_reserve(0).expect("zero bytes pass the gate");
        assert_eq!(
            (budget.accounted_refusals(), budget.gate_refusals()),
            (0, 1),
            "an admitted call counts nothing"
        );

        budget.set_resident_gate(4, 5);
        let err = budget.try_reserve(101).expect_err("101 of 100");
        assert_eq!(
            err,
            MemoryExhausted {
                requested: 101,
                reserved: 0,
                limit: 100,
                cause: ExhaustionCause::Accounted,
            },
            "an open gate leaves the accounted refusal as it was"
        );
        assert_eq!(budget.accounted_refusals(), 1);
        assert_eq!(budget.gate_refusals(), 1);

        budget.reserve(101).expect_err("reserve refuses the same way");
        assert_eq!(
            (budget.accounted_refusals(), budget.gate_refusals()),
            (2, 1)
        );
    }

    /// A caller that loaded the flag closed while the setter was reopening
    /// it goes on to the ledger rather than reporting a reading below the
    /// mark as "at or above" it. The flag is stored directly to hold that
    /// window open.
    #[test]
    fn a_closed_flag_with_a_reading_below_the_mark_admits_and_counts_nothing() {
        let budget = MemoryBudget::new(100);
        budget.set_resident_gate(400, 500);
        budget.gate.open.store(false, Ordering::Relaxed);
        budget.try_reserve(1).expect("the figures show the gate open");
        assert_eq!(budget.reserved(), 1);
        assert_eq!(
            (budget.accounted_refusals(), budget.gate_refusals()),
            (0, 0)
        );
    }

    #[test]
    fn reserve_unchecked_ignores_a_closed_gate() {
        let budget = MemoryBudget::new(100);
        budget.set_resident_gate(10, 5);
        assert_eq!(budget.reserve_unchecked(30), 30);
        assert_eq!(budget.reserved(), 30);
        assert_eq!(
            (budget.accounted_refusals(), budget.gate_refusals()),
            (0, 0)
        );
    }

    #[test]
    fn display_names_each_cause() {
        let accounted = MemoryExhausted {
            requested: 3,
            reserved: 98,
            limit: 100,
            cause: ExhaustionCause::Accounted,
        };
        assert_eq!(
            accounted.to_string(),
            "memory exhausted: requested 3 bytes, 98 of 100 byte limit already reserved"
        );
        let resident = MemoryExhausted {
            requested: 3,
            reserved: 98,
            limit: 100,
            cause: ExhaustionCause::Resident {
                resident: 700,
                high_water: 500,
            },
        };
        assert_eq!(
            resident.to_string(),
            "memory exhausted: process resident 700 bytes at or above the 500 byte \
             high-water mark (requested 3)"
        );
    }

    /// The gate block is 128-aligned and 128 bytes, so no other field of the
    /// budget can fall on its cache line, `reserved`'s included.
    #[test]
    fn the_gate_state_sits_on_a_block_of_its_own() {
        assert_eq!(std::mem::align_of::<ResidentGate>(), 128);
        assert_eq!(std::mem::size_of::<ResidentGate>(), 128);
        assert_eq!(std::mem::align_of::<RefusalCounts>(), 128);
        let budget = MemoryBudget::new(1);
        let base = std::ptr::from_ref(&budget).addr();
        let gate = std::ptr::from_ref(&budget.gate).addr() - base;
        let reserved = std::ptr::from_ref(&budget.reserved).addr() - base;
        assert!(
            reserved / 128 != gate / 128,
            "reserved at offset {reserved} shares the gate's block at {gate}"
        );
    }

    #[test]
    fn decoded_charge_is_the_declared_length_up_to_the_ceiling_and_zero_over_it() {
        assert_eq!(decoded_charge(4_095, 4_096), 4_095, "under the ceiling");
        assert_eq!(decoded_charge(4_096, 4_096), 4_096, "equal to the ceiling");
        assert_eq!(decoded_charge(4_097, 4_096), 0, "over the ceiling");
        assert_eq!(decoded_charge(u64::MAX, 4_096), 0, "far over the ceiling");
        assert_eq!(decoded_charge(0, 0), 0, "ceiling 0, nothing declared");
        assert_eq!(decoded_charge(1, 0), 0, "ceiling 0, one byte declared");
    }
}
