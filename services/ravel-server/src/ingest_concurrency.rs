//! Process-wide in-flight ingest-request limit.
//!
//! One [`IngestConcurrencyController`] is built in [`crate::start`] and shared
//! (via `Arc`) into every `GatewayState`/`RemoteWriteState` built for the
//! public listener, the mTLS listener, and both the axum and tonic surfaces,
//! so a single `--max-inflight-ingest-requests` ceiling bounds gateway memory
//! regardless of which transport or listener a request arrives on. It covers
//! only the OTLP metrics/logs/traces and Remote Write ingest routes; query,
//! health, and `/metrics` are unaffected.
//!
//! Deliberately process-local: unlike `ravel_query::QueryAdmissionController`
//! (ADR-0061), which reconciles a fleet-wide ceiling through object storage,
//! this holds no durable state and coordinates nothing across processes. A
//! request that does not fit under this process's ceiling is shed
//! immediately, never queued (docs/consistency-model.md: "admission failures
//! ... reject before buffering"), so the caller's own retry/backoff decides
//! where the request lands next.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tokio::sync::{OwnedSemaphorePermit, Semaphore, TryAcquireError};

/// `--max-inflight-ingest-requests`: `Bounded(n)` caps process-wide in-flight
/// ingest requests at `n`; `Unlimited` (the flag's `0` value) disables the
/// gate, matching every other admission ceiling in this crate that spells
/// "no limit" as `0` rather than a sentinel `u64::MAX`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IngestConcurrencyLimit {
    Bounded(u64),
    Unlimited,
}

/// Process-wide admission gate over in-flight ingest requests. Backed by a
/// [`tokio::sync::Semaphore`] rather than the hand-rolled `Mutex<state>` shape
/// `ravel_query::QueryAdmissionController` uses, since a permit here needs no
/// custom release bookkeeping: `OwnedSemaphorePermit::drop` returns the slot
/// on every exit path (normal return, early error return, or panic) with no
/// help from the caller.
pub struct IngestConcurrencyController {
    /// `None` for `Unlimited`: no semaphore to acquire, so `try_admit` never
    /// sheds. `Some` holds the bound as the semaphore's starting permit count.
    semaphore: Option<Arc<Semaphore>>,
    shed_total: AtomicU64,
}

/// A held admission slot. Carries no reference back to the controller: the
/// wrapped [`OwnedSemaphorePermit`] already owns the `Arc<Semaphore>` it must
/// release into, so dropping this is sufficient and no manual `release()`
/// call is needed anywhere a permit goes out of scope.
#[derive(Debug)]
pub struct IngestPermit {
    _permit: Option<OwnedSemaphorePermit>,
}

/// The process-wide in-flight ceiling is reached. Never carries a reason
/// beyond this: there is exactly one thing this controller rejects for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("process in-flight ingest-request limit reached")]
pub struct IngestShed;

impl IngestConcurrencyController {
    pub fn new(limit: IngestConcurrencyLimit) -> Self {
        let semaphore = match limit {
            IngestConcurrencyLimit::Bounded(n) => Some(Arc::new(Semaphore::new(n as usize))),
            IngestConcurrencyLimit::Unlimited => None,
        };
        IngestConcurrencyController {
            semaphore,
            shed_total: AtomicU64::new(0),
        }
    }

    pub fn shared(limit: IngestConcurrencyLimit) -> Arc<Self> {
        Arc::new(Self::new(limit))
    }

    /// Admits the caller immediately or sheds it: this never queues. Call
    /// this as the first thing an ingest handler does, before tenant
    /// resolution or any per-signal admission check, so a shed request does
    /// none of that work and reaches neither a shard channel nor a commit
    /// token.
    pub fn try_admit(&self) -> Result<IngestPermit, IngestShed> {
        let Some(semaphore) = &self.semaphore else {
            return Ok(IngestPermit { _permit: None });
        };
        match semaphore.clone().try_acquire_owned() {
            Ok(permit) => Ok(IngestPermit {
                _permit: Some(permit),
            }),
            Err(TryAcquireError::NoPermits) => {
                self.shed_total.fetch_add(1, Ordering::Relaxed);
                Err(IngestShed)
            }
            Err(TryAcquireError::Closed) => {
                unreachable!("this semaphore is never closed: no code path calls close()")
            }
        }
    }

    /// Cumulative sheds since process start, for the `/metrics` counter
    /// (`services/ravel-server/src/metrics.rs`).
    pub fn shed_total(&self) -> u64 {
        self.shed_total.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn admits_up_to_the_bound() {
        let controller = IngestConcurrencyController::new(IngestConcurrencyLimit::Bounded(2));
        let first = controller.try_admit().expect("first admit under bound");
        let second = controller.try_admit().expect("second admit under bound");
        assert_eq!(controller.shed_total(), 0);
        drop(first);
        drop(second);
    }

    #[test]
    fn sheds_the_request_over_the_bound() {
        let controller = IngestConcurrencyController::new(IngestConcurrencyLimit::Bounded(1));
        let _first = controller.try_admit().expect("first admit under bound");
        let second = controller.try_admit();
        assert_eq!(second.unwrap_err(), IngestShed);
        assert_eq!(controller.shed_total(), 1);
    }

    #[test]
    fn dropping_a_permit_frees_the_slot_for_the_next_admit() {
        let controller = IngestConcurrencyController::new(IngestConcurrencyLimit::Bounded(1));
        let first = controller.try_admit().expect("first admit under bound");
        assert!(controller.try_admit().is_err());
        drop(first);
        let second = controller.try_admit();
        assert!(second.is_ok());
        assert_eq!(controller.shed_total(), 1);
    }

    #[test]
    fn unlimited_never_sheds() {
        let controller = IngestConcurrencyController::new(IngestConcurrencyLimit::Unlimited);
        let permits: Vec<_> = (0..10_000)
            .map(|_| controller.try_admit().expect("unlimited never sheds"))
            .collect();
        assert_eq!(controller.shed_total(), 0);
        drop(permits);
    }

    #[test]
    fn shed_counter_is_cumulative_across_repeated_sheds() {
        let controller = IngestConcurrencyController::new(IngestConcurrencyLimit::Bounded(1));
        let _held = controller.try_admit().expect("first admit under bound");
        for _ in 0..3 {
            assert!(controller.try_admit().is_err());
        }
        assert_eq!(controller.shed_total(), 3);
    }
}
