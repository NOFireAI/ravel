//! Process-wide ingest buffer byte budget (ADR-0069 decision 1).
//!
//! Ravel's memory model is configuration-bounded per tenant and per query, but
//! nothing bounded the *sum* of buffered ingest state across tenants: the
//! per-(tenant, shard, signal) buffer cap is ~8 MiB, so the worst case grows
//! with active-tenant count and can exceed an 8 GB host's RAM before any
//! per-tenant limit trips (ADR-0069 Context). [`IngestByteBudget`] is the one
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

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

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
        Ok(IngestByteCharge {
            budget: Arc::clone(self),
            bytes,
        })
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
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
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
}
