//! Process-wide ingest buffer byte budget (ADR-0069 decision 1).
//!
//! Ravel's memory model is configuration-bounded per tenant and per query, but
//! nothing bounded the *sum* of buffered ingest state across tenants: each
//! per-(tenant, shard, signal) buffer may hold up to the memory backstop
//! ([`crate::config::buffer_memory_backstop_bytes`]: an eighth of this ceiling,
//! capped at 64 MiB and never below `target_bytes`), so the worst case grows
//! with active-tenant count and can exceed an 8 GB host's RAM before any
//! per-tenant limit trips (ADR-0069 Context). The backstop is derived from the
//! ceiling configured here, so raising or lowering `--max-ingest-buffer-bytes`
//! moves the per-buffer bound with it and no single buffer can take a large
//! share of the budget. [`IngestByteBudget`] is the one
//! process-wide gauge that bounds it: a request's estimated buffered bytes are
//! charged at admission (after decode, before buffering), and refunded when the
//! flush that held them completes or fails.
//!
//! # Charge / refund ownership
//!
//! [`IngestByteBudget::try_charge`] performs both the ceiling check and the
//! charge in one atomic step and hands back an [`IngestByteCharge`] guard. The
//! guard refunds the exact charged amount on `Drop`, once, no matter how the
//! bytes stop being held: a normal flush completion, an abandoned or failed
//! flush, a series-id-collision rejection, a stale-provisioning early return,
//! or a panic on the flush task. The router clones the guard (`Arc`) into every
//! shard message the request fans out to, and each shard buffer holds its
//! clones until the buffer flushes; the refund fires when the last clone drops,
//! i.e. when the last buffer holding any of this request's bytes has flushed.
//! Because the refund is exactly the charge, the gauge returns to its baseline
//! precisely once every in-flight flush has drained -- no drift, no leak on any
//! early return.
//!
//! Deliberately process-local, like [`crate::AdmissionController`]'s per-process
//! caps and unlike the fleet-reconciled query admission (ADR-0061): it holds no
//! durable state and coordinates nothing across processes. With N ingest
//! replicas the fleet-wide effective ceiling is N times the configured one.
//!
//! # Waiting charges (ADR-2614 decision 5)
//!
//! The server sheds at the ceiling; the bulk loader cannot, because its input
//! is a file it must finish. [`IngestByteBudget::charge_waiting`] and
//! [`IngestByteCharge::resize_waiting`] block the calling thread until the
//! bytes fit instead of shedding. One charge larger than the whole ceiling is
//! admitted once nothing else is held, so a waiter can always make progress;
//! [`IngestByteBudget::peak_bytes`] records the highest gauge reading, which
//! only exceeds the ceiling in that case.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

/// Upper bound on one condvar wait in [`IngestByteBudget::wait_for_room`]. The
/// wake-up protocol does not lose notifications; the bound only keeps a waiter
/// re-checking if it ever did.
const WAIT_RECHECK: Duration = Duration::from_millis(100);

/// `--max-ingest-buffer-bytes`: `Bounded(n)` caps the process-wide sum of
/// estimated buffered ingest bytes at `n`; `Unlimited` (the flag's `0` value)
/// disables the ceiling, matching every other admission ceiling in this crate
/// that spells "no limit" as `0` rather than a sentinel `u64::MAX`. A charge
/// still updates the gauge under `Unlimited`, so the `ravel_ingest_buffer_bytes`
/// metric stays meaningful for capacity planning even with the ceiling off.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestByteBudgetLimit {
    Bounded(u64),
    Unlimited,
}

/// The process-wide ceiling was reached: charging this request's estimated
/// bytes would push the gauge past the configured ceiling. Carries no reason
/// beyond this; there is exactly one thing this budget sheds for. The gateway
/// maps it to HTTP 429 with a fixed `Retry-After` (a slot frees as soon as any
/// in-flight flush completes, so there is no per-caller refill estimate to
/// surface, the same shape the in-flight shed uses) or gRPC
/// `RESOURCE_EXHAUSTED`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("process ingest buffer byte budget reached")]
pub struct IngestByteShed;

/// Process-wide gauge over estimated buffered ingest bytes (ADR-0069).
///
/// One instance is built in `services/ravel-server` at startup and shared (via
/// `Arc`) into every ingest router -- metrics, logs, and spans -- so a single
/// `--max-ingest-buffer-bytes` ceiling bounds the sum across all signals and
/// tenants regardless of which pipeline a request lands in.
#[derive(Debug)]
pub struct IngestByteBudget {
    /// `None` for `Unlimited`: the gauge still tracks, but `try_charge` never
    /// sheds.
    limit: Option<u64>,
    /// Currently-held estimated buffered bytes: incremented by a successful
    /// charge, decremented by the matching refund on `IngestByteCharge::drop`.
    in_flight: AtomicU64,
    /// Cumulative sheds since process start, for the `/metrics` counter.
    shed_total: AtomicU64,
    /// Highest `in_flight` reading any charge or resize produced.
    peak: AtomicU64,
    /// Threads blocked in [`Self::wait_for_room`].
    waiters: AtomicUsize,
    /// Cumulative count of charges and resizes that had to block.
    waits_total: AtomicU64,
    /// Held by a waiter between its check and its wait, and by a refund
    /// before it notifies, so a refund cannot slip between the two.
    wake_lock: Mutex<()>,
    wake: Condvar,
}

impl IngestByteBudget {
    pub fn new(limit: IngestByteBudgetLimit) -> Self {
        IngestByteBudget {
            limit: match limit {
                IngestByteBudgetLimit::Bounded(n) => Some(n),
                IngestByteBudgetLimit::Unlimited => None,
            },
            in_flight: AtomicU64::new(0),
            shed_total: AtomicU64::new(0),
            peak: AtomicU64::new(0),
            waiters: AtomicUsize::new(0),
            waits_total: AtomicU64::new(0),
            wake_lock: Mutex::new(()),
            wake: Condvar::new(),
        }
    }

    pub fn shared(limit: IngestByteBudgetLimit) -> Arc<Self> {
        Arc::new(Self::new(limit))
    }

    /// Atomically checks the ceiling and, if `bytes` fit, charges them,
    /// returning an [`IngestByteCharge`] guard that refunds exactly `bytes` on
    /// `Drop`. On a ceiling breach nothing is charged, the shed counter is
    /// incremented, and [`IngestByteShed`] is returned so the caller sheds the
    /// request before any buffering.
    ///
    /// Under `Unlimited` the charge always succeeds and the gauge is still
    /// updated. A `bytes == 0` charge always succeeds (an empty request), and
    /// its guard's refund is a no-op.
    pub fn try_charge(self: &Arc<Self>, bytes: u64) -> Result<IngestByteCharge, IngestByteShed> {
        match self.limit {
            None => {
                self.in_flight.fetch_add(bytes, Ordering::AcqRel);
            }
            Some(limit) => {
                let mut current = self.in_flight.load(Ordering::Acquire);
                loop {
                    // saturating_add: an adversarial estimate near u64::MAX
                    // must shed, never wrap past the ceiling.
                    let next = current.saturating_add(bytes);
                    if next > limit {
                        self.shed_total.fetch_add(1, Ordering::Relaxed);
                        return Err(IngestByteShed);
                    }
                    match self.in_flight.compare_exchange_weak(
                        current,
                        next,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => break,
                        Err(observed) => current = observed,
                    }
                }
            }
        }
        self.note_peak();
        Ok(IngestByteCharge {
            budget: Arc::clone(self),
            bytes,
        })
    }

    /// Charges `bytes`, blocking the calling thread until they fit under the
    /// ceiling instead of shedding. A charge larger than the ceiling is
    /// admitted once nothing else is held. Never call this from an async task:
    /// it parks the thread, and the refunds it waits for may need that thread.
    pub fn charge_waiting(self: &Arc<Self>, bytes: u64) -> IngestByteCharge {
        self.wait_for_room(bytes, 0);
        IngestByteCharge {
            budget: Arc::clone(self),
            bytes,
        }
    }

    /// Adds `bytes` to the gauge once `in_flight + bytes` fits under the
    /// ceiling, or once the gauge holds no more than `own` (the caller's
    /// already-held bytes, so a resize of the only held charge never waits on
    /// itself).
    fn wait_for_room(&self, bytes: u64, own: u64) {
        let Some(limit) = self.limit else {
            self.in_flight.fetch_add(bytes, Ordering::AcqRel);
            self.note_peak();
            return;
        };
        let admit = |current: u64| current.saturating_add(bytes) <= limit || current <= own;
        let mut guard = None;
        let mut current = self.in_flight.load(Ordering::Acquire);
        loop {
            if admit(current) {
                match self.in_flight.compare_exchange_weak(
                    current,
                    current.saturating_add(bytes),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break,
                    Err(observed) => {
                        current = observed;
                        continue;
                    }
                }
            }
            match guard.take() {
                None => {
                    // Register, take the lock, and re-check before waiting: a
                    // refund that lands after this re-check must take the
                    // lock to notify, which it cannot do until the wait below
                    // has released it.
                    self.waiters.fetch_add(1, Ordering::SeqCst);
                    self.waits_total.fetch_add(1, Ordering::Relaxed);
                    guard = Some(self.wake_lock.lock().unwrap_or_else(|e| e.into_inner()));
                }
                Some(held) => {
                    let (held, _) = self
                        .wake
                        .wait_timeout(held, WAIT_RECHECK)
                        .unwrap_or_else(|e| e.into_inner());
                    guard = Some(held);
                }
            }
            current = self.in_flight.load(Ordering::SeqCst);
        }
        if guard.is_some() {
            self.waiters.fetch_sub(1, Ordering::SeqCst);
        }
        drop(guard);
        self.note_peak();
    }

    fn note_peak(&self) {
        self.peak
            .fetch_max(self.in_flight.load(Ordering::Acquire), Ordering::AcqRel);
    }

    /// The highest gauge reading since construction. Under a bounded ceiling
    /// it exceeds the ceiling only when a single charge larger than the
    /// ceiling was admitted alone.
    pub fn peak_bytes(&self) -> u64 {
        self.peak.load(Ordering::Acquire)
    }

    /// Threads currently blocked in [`Self::charge_waiting`] or
    /// [`IngestByteCharge::resize_waiting`].
    pub fn waiting(&self) -> usize {
        self.waiters.load(Ordering::SeqCst)
    }

    /// How many [`Self::charge_waiting`] or [`IngestByteCharge::resize_waiting`]
    /// calls found no room and registered to wait, since construction. One
    /// that registers counts even if its re-check under the lock admits it
    /// before it parks.
    pub fn waits_total(&self) -> u64 {
        self.waits_total.load(Ordering::Relaxed)
    }

    /// Estimated buffered bytes currently held, for the
    /// `ravel_ingest_buffer_bytes` gauge on `/metrics`.
    pub fn in_flight_bytes(&self) -> u64 {
        self.in_flight.load(Ordering::Acquire)
    }

    /// The configured ceiling, or `None` when unlimited. For `/metrics` and
    /// operational visibility.
    pub fn ceiling(&self) -> Option<u64> {
        self.limit
    }

    /// The configured ceiling in the form the callers configured it, for a
    /// router to publish to its shard actors via [`BufferBudgetCeiling`].
    pub(crate) fn limit(&self) -> IngestByteBudgetLimit {
        match self.limit {
            Some(n) => IngestByteBudgetLimit::Bounded(n),
            None => IngestByteBudgetLimit::Unlimited,
        }
    }

    /// Cumulative requests shed at the ceiling since process start, for the
    /// `ravel_ingest_buffer_shed_total` counter on `/metrics`.
    pub fn shed_total(&self) -> u64 {
        self.shed_total.load(Ordering::Relaxed)
    }

    fn refund(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        // A refund can never take the gauge below zero: every refund matches a
        // prior successful charge of the same amount. `fetch_sub` on an
        // unsigned atomic would wrap on an accounting bug, so guard against it
        // rather than corrupt the gauge into a huge value that sheds forever.
        let mut current = self.in_flight.load(Ordering::Acquire);
        loop {
            let next = current.saturating_sub(bytes);
            debug_assert!(
                current >= bytes,
                "ingest byte budget refund {bytes} exceeds in-flight {current}"
            );
            match self.in_flight.compare_exchange_weak(
                current,
                next,
                Ordering::SeqCst,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
        if self.waiters.load(Ordering::SeqCst) > 0 {
            let _held = self.wake_lock.lock().unwrap_or_else(|e| e.into_inner());
            self.wake.notify_all();
        }
    }
}

/// The configured ceiling, shared live with every shard actor of one router so
/// the per-buffer memory backstop
/// ([`crate::config::buffer_memory_backstop_bytes`]) can be a fraction of the
/// limit an operator actually set rather than of the default.
///
/// It is a shared cell rather than a plain value because the budget arrives
/// after the actors exist: [`crate::IngestRouter::new`] spawns the shard-actor
/// factory, and `services/ravel-server` installs the configured budget
/// afterwards with `with_budget`. Every actor clones this cell and reads it at
/// trigger time, so the install order does not matter and a later generation's
/// actors see the same value.
///
/// `u64::MAX` encodes [`IngestByteBudgetLimit::Unlimited`]. That makes
/// `Bounded(u64::MAX)` indistinguishable from `Unlimited`, which costs nothing:
/// the backstop is capped well below an eighth of `u64::MAX`, so both spellings
/// derive the identical backstop.
#[derive(Debug, Clone)]
pub(crate) struct BufferBudgetCeiling(Arc<AtomicU64>);

/// The `Unlimited` encoding for [`BufferBudgetCeiling`].
const CEILING_UNLIMITED: u64 = u64::MAX;

impl BufferBudgetCeiling {
    pub(crate) fn unlimited() -> Self {
        BufferBudgetCeiling(Arc::new(AtomicU64::new(CEILING_UNLIMITED)))
    }

    pub(crate) fn set(&self, limit: IngestByteBudgetLimit) {
        let encoded = match limit {
            IngestByteBudgetLimit::Bounded(n) => n,
            IngestByteBudgetLimit::Unlimited => CEILING_UNLIMITED,
        };
        self.0.store(encoded, Ordering::Relaxed);
    }

    pub(crate) fn get(&self) -> IngestByteBudgetLimit {
        match self.0.load(Ordering::Relaxed) {
            CEILING_UNLIMITED => IngestByteBudgetLimit::Unlimited,
            n => IngestByteBudgetLimit::Bounded(n),
        }
    }
}

/// A held charge against an [`IngestByteBudget`]. Refunds its exact bytes on
/// `Drop`, exactly once. Cloneable via `Arc` at the router so one request's
/// charge can be held jointly by every shard buffer its points fanned out to;
/// the refund fires when the last clone drops (the last holding buffer flushes).
///
/// This is why no ingest path needs an explicit refund call: the guard's `Drop`
/// covers every exit -- flush success, flush abandonment, a fail-loud
/// collision rejection, a stale-provisioning early return, or a panic that
/// drops a flush task's future.
#[derive(Debug)]
pub struct IngestByteCharge {
    budget: Arc<IngestByteBudget>,
    bytes: u64,
}

impl IngestByteCharge {
    /// The charged amount, for tests and diagnostics.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    /// The budget this charge is held against.
    pub fn budget(&self) -> &Arc<IngestByteBudget> {
        &self.budget
    }

    /// Changes the charged amount to `bytes`, for a charge taken on an
    /// estimate and corrected once the real size is known. Shrinking refunds
    /// the difference at once. Growing blocks the calling thread, as
    /// [`IngestByteBudget::charge_waiting`] does, until the difference fits or
    /// this charge is the only one held.
    pub fn resize_waiting(&mut self, bytes: u64) {
        if bytes < self.bytes {
            self.budget.refund(self.bytes - bytes);
        } else if bytes > self.bytes {
            self.budget.wait_for_room(bytes - self.bytes, self.bytes);
        }
        self.bytes = bytes;
    }
}

impl Drop for IngestByteCharge {
    fn drop(&mut self) {
        self.budget.refund(self.bytes);
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn charge_updates_gauge_and_refund_on_drop_returns_to_baseline() {
        let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(1_000));
        assert_eq!(budget.in_flight_bytes(), 0);
        let charge = budget.try_charge(400).expect("under ceiling");
        assert_eq!(budget.in_flight_bytes(), 400);
        drop(charge);
        assert_eq!(budget.in_flight_bytes(), 0, "refund returns to baseline");
    }

    #[test]
    fn charge_at_the_ceiling_is_admitted_over_it_is_shed() {
        let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(1_000));
        let _held = budget.try_charge(1_000).expect("exactly at ceiling admits");
        assert_eq!(budget.in_flight_bytes(), 1_000);
        let shed = budget.try_charge(1).expect_err("one byte over sheds");
        assert_eq!(shed, IngestByteShed);
        assert_eq!(budget.shed_total(), 1);
        // A shed request charged nothing: the gauge is untouched.
        assert_eq!(budget.in_flight_bytes(), 1_000);
    }

    #[test]
    fn released_charge_frees_room_for_the_next() {
        let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(1_000));
        let first = budget.try_charge(1_000).expect("at ceiling");
        assert!(budget.try_charge(1).is_err());
        drop(first);
        let _second = budget.try_charge(1_000).expect("room freed after refund");
        assert_eq!(budget.in_flight_bytes(), 1_000);
    }

    #[test]
    fn unlimited_never_sheds_but_still_tracks_the_gauge() {
        let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Unlimited);
        let charges: Vec<_> = (0..1_000)
            .map(|_| budget.try_charge(1_000_000).expect("unlimited never sheds"))
            .collect();
        assert_eq!(budget.in_flight_bytes(), 1_000 * 1_000_000);
        assert_eq!(budget.shed_total(), 0);
        drop(charges);
        assert_eq!(budget.in_flight_bytes(), 0);
    }

    #[test]
    fn shed_counter_is_cumulative() {
        let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(10));
        let _held = budget.try_charge(10).expect("at ceiling");
        for _ in 0..5 {
            assert!(budget.try_charge(1).is_err());
        }
        assert_eq!(budget.shed_total(), 5);
    }

    #[test]
    fn zero_byte_charge_is_a_noop_guard() {
        let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(10));
        let charge = budget.try_charge(0).expect("zero always admits");
        assert_eq!(charge.bytes(), 0);
        assert_eq!(budget.in_flight_bytes(), 0);
        drop(charge);
        assert_eq!(budget.in_flight_bytes(), 0);
    }

    #[test]
    fn charges_are_shared_and_refund_only_when_the_last_clone_drops() {
        let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(1_000));
        let charge = Arc::new(budget.try_charge(300).expect("under ceiling"));
        let clone_a = Arc::clone(&charge);
        let clone_b = Arc::clone(&charge);
        assert_eq!(budget.in_flight_bytes(), 300);
        drop(charge);
        drop(clone_a);
        assert_eq!(budget.in_flight_bytes(), 300, "held while a clone survives");
        drop(clone_b);
        assert_eq!(budget.in_flight_bytes(), 0, "refunded on last clone drop");
    }

    /// Spins until `n` threads are parked in the budget's wait, which is the
    /// proof a charge blocked rather than a timing guess.
    fn await_waiters(budget: &IngestByteBudget, n: usize) {
        while budget.waiting() != n {
            std::thread::yield_now();
        }
    }

    #[test]
    fn charge_waiting_blocks_until_a_refund_makes_room() {
        let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(1_000));
        let held = budget.try_charge(700).expect("under ceiling");
        let waiter = {
            let budget = Arc::clone(&budget);
            std::thread::spawn(move || budget.charge_waiting(400))
        };
        await_waiters(&budget, 1);
        assert_eq!(
            budget.in_flight_bytes(),
            700,
            "the waiter charged nothing yet"
        );
        drop(held);
        let charge = waiter.join().expect("waiter thread");
        assert_eq!(charge.bytes(), 400);
        assert_eq!(budget.in_flight_bytes(), 400);
        assert_eq!(budget.waiting(), 0);
        assert_eq!(budget.waits_total(), 1, "the blocked charge is counted");
        assert_eq!(budget.shed_total(), 0, "waiting never sheds");
    }

    #[test]
    fn charge_larger_than_the_ceiling_is_admitted_alone() {
        let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(1_000));
        let held = budget.try_charge(1).expect("under ceiling");
        let waiter = {
            let budget = Arc::clone(&budget);
            std::thread::spawn(move || budget.charge_waiting(5_000))
        };
        await_waiters(&budget, 1);
        drop(held);
        let big = waiter.join().expect("waiter thread");
        assert_eq!(budget.in_flight_bytes(), 5_000);
        assert_eq!(budget.peak_bytes(), 5_000);
        drop(big);
        assert_eq!(budget.in_flight_bytes(), 0);
    }

    #[test]
    fn resize_shrink_refunds_and_wakes_a_waiter() {
        let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(1_000));
        let mut estimate = budget.charge_waiting(1_000);
        let waiter = {
            let budget = Arc::clone(&budget);
            std::thread::spawn(move || budget.charge_waiting(600))
        };
        await_waiters(&budget, 1);
        estimate.resize_waiting(300);
        let second = waiter.join().expect("waiter thread");
        assert_eq!(budget.in_flight_bytes(), 900);
        drop(estimate);
        drop(second);
        assert_eq!(
            budget.in_flight_bytes(),
            0,
            "resized charge refunds its new size"
        );
    }

    #[test]
    fn resize_grow_waits_for_room_but_not_on_itself() {
        let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(1_000));
        // Alone, a grow past the ceiling never waits on its own bytes.
        let mut alone = budget.charge_waiting(400);
        alone.resize_waiting(1_500);
        assert_eq!(budget.in_flight_bytes(), 1_500);
        drop(alone);

        let other = budget.try_charge(500).expect("under ceiling");
        let grower = {
            let budget = Arc::clone(&budget);
            std::thread::spawn(move || {
                let mut charge = budget.charge_waiting(400);
                charge.resize_waiting(800);
                charge
            })
        };
        await_waiters(&budget, 1);
        assert_eq!(
            budget.in_flight_bytes(),
            900,
            "grow waits; base charge held"
        );
        drop(other);
        let grown = grower.join().expect("grower thread");
        assert_eq!(grown.bytes(), 800);
        assert_eq!(budget.in_flight_bytes(), 800);
    }

    #[test]
    fn peak_tracks_the_highest_gauge_reading() {
        let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(1_000));
        let a = budget.charge_waiting(300);
        let b = budget.try_charge(600).expect("under ceiling");
        drop(a);
        let _c = budget.charge_waiting(100);
        assert_eq!(budget.peak_bytes(), 900);
        assert_eq!(budget.waits_total(), 0, "no charge here had to wait");
        drop(b);
        assert_eq!(budget.peak_bytes(), 900, "a refund never lowers the peak");
    }
}
