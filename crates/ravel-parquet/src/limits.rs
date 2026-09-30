//! What one query's Parquet reads are admitted against: the process memory
//! budget, and the request and byte budgets every other SQL query obeys
//! (ADR-1170 decision 2, ADR-2040).
//!
//! One [`ReadLimits`] is built per query and cloned into every reader that
//! query opens, so the clones share one in-flight ledger.
//!
//! - **Memory.** [`ReadLimits::reserve`] takes a range's byte length from the
//!   process budget before anything is fetched or looked up. The returned guard
//!   rides inside the `Bytes` the reader hands parquet ([`attach`]), so it is
//!   released when parquet drops the data, not when the GET returns. The
//!   footer cache ([`crate::MetadataCache`]) is a separate, separately bounded
//!   cache and is not charged here.
//! - **Requests and bytes.** [`ReadLimits::admit`] counts one store request and
//!   the wire bytes of its body against `max_s3_requests` and
//!   `max_bytes_scanned` and refuses the request that would cross either,
//!   before it is issued. The totals compared are the query's pooled
//!   accounting, so the manifest and grant reads of the resolve phase count
//!   toward the same budgets. Bytes are wire bytes of GET response bodies as
//!   the accounting records them: a cache hit issues no request and adds none,
//!   and a retry the store performs inside one `get_pinned` call is neither
//!   requested nor recorded a second time.

use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use ravel_memory::{MemoryBudget, Reservation};
use ravel_query::{ByteLimit, PhaseAccounting, QueryPhase, RequestLimit};
use ravel_types::accounting::AccountedOp;

use crate::error::ParquetReadError;

/// Requests admitted but not yet fully recorded: the request itself is
/// recorded at admission, so only the bytes of bodies still in flight are
/// pending.
#[derive(Debug, Default)]
struct InFlight {
    bytes: u64,
}

/// The budgets one query's Parquet reads are admitted against.
#[derive(Clone)]
pub struct ReadLimits {
    memory: Arc<MemoryBudget>,
    max_bytes: ByteLimit,
    max_requests: RequestLimit,
    in_flight: Arc<Mutex<InFlight>>,
}

impl ReadLimits {
    pub fn new(
        memory: Arc<MemoryBudget>,
        max_bytes: ByteLimit,
        max_requests: RequestLimit,
    ) -> Self {
        ReadLimits {
            memory,
            max_bytes,
            max_requests,
            in_flight: Arc::new(Mutex::new(InFlight::default())),
        }
    }

    /// No memory, byte or request limit: reads are counted and never refused.
    pub fn unlimited() -> Self {
        Self::new(
            Arc::new(MemoryBudget::unlimited()),
            ByteLimit::Unlimited,
            RequestLimit::Unlimited,
        )
    }

    /// Reserve `len` bytes of the process memory budget for a range about to
    /// be read. A refusal is returned before any lookup or GET for the range.
    pub(crate) fn reserve(&self, len: u64) -> Result<Reservation, ParquetReadError> {
        self.memory
            .reserve(len)
            .map_err(|e| ParquetReadError::MemoryExhausted {
                requested: e.requested,
                reserved: e.reserved,
                limit: e.limit,
            })
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, InFlight> {
        self.in_flight
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// The refusal a request of `len` bytes would meet against `accounting`'s
    /// totals plus what is in flight, if any.
    fn refusal(
        &self,
        in_flight: &InFlight,
        accounting: &PhaseAccounting,
        len: u64,
    ) -> Option<ParquetReadError> {
        let pooled = accounting.pooled_snapshot();
        let requests = pooled.total_s3_requests().saturating_add(1);
        if self.max_requests.is_exceeded_by(requests) {
            let max = match self.max_requests {
                RequestLimit::Bounded(max) => max,
                RequestLimit::Unlimited => requests,
            };
            return Some(ParquetReadError::RequestBudgetExceeded { requests, max });
        }
        let scanned = pooled
            .total_s3_bytes()
            .saturating_add(in_flight.bytes)
            .saturating_add(len);
        if self.max_bytes.is_exceeded_by(scanned) {
            let max = match self.max_bytes {
                ByteLimit::Bounded(max) => max,
                ByteLimit::Unlimited => scanned,
            };
            return Some(ParquetReadError::BytesBudgetExceeded { scanned, max });
        }
        None
    }

    /// Whether a request of `len` bytes would be admitted now, changing
    /// nothing. Run before a read joins the shared cache's single flight, so
    /// the common over-budget refusal never reaches a flight other queries may
    /// be waiting on; [`Self::admit`] is the authoritative check.
    pub(crate) fn precheck(
        &self,
        accounting: &PhaseAccounting,
        len: u64,
    ) -> Result<(), ParquetReadError> {
        match self.refusal(&self.lock(), accounting, len) {
            Some(refused) => Err(refused),
            None => Ok(()),
        }
    }

    /// Admit one GET of `len` bytes: refuse it if it would cross a budget,
    /// otherwise record the request on `phase` and hold `len` bytes as in
    /// flight until [`Admission::complete`] records what arrived. The check and
    /// the recording happen under one lock, so concurrent readers of one query
    /// cannot both be admitted past the budget.
    pub(crate) fn admit(
        &self,
        accounting: &PhaseAccounting,
        phase: QueryPhase,
        len: u64,
    ) -> Result<Admission, ParquetReadError> {
        let mut in_flight = self.lock();
        if let Some(refused) = self.refusal(&in_flight, accounting, len) {
            return Err(refused);
        }
        accounting.phase(phase).record_s3_request(AccountedOp::Get);
        in_flight.bytes = in_flight.bytes.saturating_add(len);
        Ok(Admission {
            limits: self.clone(),
            len,
            settled: false,
        })
    }
}

impl fmt::Debug for ReadLimits {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadLimits")
            .field("memory_limit", &self.memory.limit())
            .field("max_bytes", &self.max_bytes)
            .field("max_requests", &self.max_requests)
            .finish_non_exhaustive()
    }
}

/// One admitted GET. Dropping it without [`Admission::complete`] (the GET
/// failed) releases its in-flight bytes and records none.
pub(crate) struct Admission {
    limits: ReadLimits,
    len: u64,
    settled: bool,
}

impl Admission {
    /// Record the `received` wire bytes of the GET's body on `phase` and stop
    /// holding the admitted length in flight, in one step under the limits'
    /// lock.
    pub(crate) fn complete(
        mut self,
        accounting: &PhaseAccounting,
        phase: QueryPhase,
        received: u64,
    ) {
        let mut in_flight = self.limits.lock();
        accounting
            .phase(phase)
            .add_s3_bytes(AccountedOp::Get, received);
        in_flight.bytes = in_flight.bytes.saturating_sub(self.len);
        self.settled = true;
    }
}

impl Drop for Admission {
    fn drop(&mut self) {
        if !self.settled {
            let mut in_flight = self.limits.lock();
            in_flight.bytes = in_flight.bytes.saturating_sub(self.len);
        }
    }
}

/// Owns a fetched `Bytes` together with its reservation, so the reservation is
/// released when the last clone of the returned `Bytes` drops.
struct ReservedBytes {
    bytes: Bytes,
    _reservation: Reservation,
}

impl AsRef<[u8]> for ReservedBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

/// Wrap `bytes` so `reservation` lives exactly as long as the returned
/// `Bytes` and every clone of it. Zero-copy: the allocation is shared.
pub(crate) fn attach(bytes: Bytes, reservation: Reservation) -> Bytes {
    Bytes::from_owner(ReservedBytes {
        bytes,
        _reservation: reservation,
    })
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn a_reservation_lives_as_long_as_the_last_clone_of_the_bytes() {
        let budget = Arc::new(MemoryBudget::new(64));
        let limits = ReadLimits::new(
            Arc::clone(&budget),
            ByteLimit::Unlimited,
            RequestLimit::Unlimited,
        );
        let reservation = limits.reserve(16).expect("fits");
        assert_eq!(budget.reserved(), 16);

        let wrapped = attach(Bytes::from_static(b"sixteen bytes!!!"), reservation);
        let clone = wrapped.clone();
        drop(wrapped);
        assert_eq!(budget.reserved(), 16, "a clone keeps the reservation");
        drop(clone);
        assert_eq!(budget.reserved(), 0, "the last drop releases it");
    }

    #[test]
    fn a_reservation_past_the_budget_is_refused_with_the_figures() {
        let limits = ReadLimits::new(
            Arc::new(MemoryBudget::new(10)),
            ByteLimit::Unlimited,
            RequestLimit::Unlimited,
        );
        let _held = limits.reserve(8).expect("fits");
        let err = limits.reserve(3).expect_err("refused");
        assert!(
            matches!(
                err,
                ParquetReadError::MemoryExhausted {
                    requested: 3,
                    reserved: 8,
                    limit: 10
                }
            ),
            "{err:?}"
        );
    }

    #[test]
    fn admission_counts_the_request_and_holds_its_bytes_until_complete() {
        let accounting = PhaseAccounting::new();
        let limits = ReadLimits::new(
            Arc::new(MemoryBudget::unlimited()),
            ByteLimit::Bounded(100),
            RequestLimit::Bounded(2),
        );
        let first = limits
            .admit(&accounting, QueryPhase::Scan, 60)
            .expect("first fits");
        assert_eq!(accounting.pooled_snapshot().total_s3_requests(), 1);

        let err = limits
            .admit(&accounting, QueryPhase::Scan, 41)
            .err()
            .expect("the first's 60 in-flight bytes count");
        assert!(
            matches!(
                err,
                ParquetReadError::BytesBudgetExceeded {
                    scanned: 101,
                    max: 100
                }
            ),
            "{err:?}"
        );
        assert_eq!(
            accounting.pooled_snapshot().total_s3_requests(),
            1,
            "a refused request is not recorded"
        );

        first.complete(&accounting, QueryPhase::Scan, 60);
        assert_eq!(accounting.pooled_snapshot().total_s3_bytes(), 60);
        let second = limits
            .admit(&accounting, QueryPhase::Scan, 40)
            .expect("exactly at the byte budget");
        drop(second);

        let err = limits
            .admit(&accounting, QueryPhase::Scan, 1)
            .err()
            .expect("the third request");
        assert!(
            matches!(
                err,
                ParquetReadError::RequestBudgetExceeded {
                    requests: 3,
                    max: 2
                }
            ),
            "{err:?}"
        );
    }

    #[test]
    fn a_failed_get_releases_its_in_flight_bytes() {
        let accounting = PhaseAccounting::new();
        let limits = ReadLimits::new(
            Arc::new(MemoryBudget::unlimited()),
            ByteLimit::Bounded(100),
            RequestLimit::Unlimited,
        );
        drop(
            limits
                .admit(&accounting, QueryPhase::Scan, 100)
                .expect("fits"),
        );
        limits
            .admit(&accounting, QueryPhase::Scan, 100)
            .expect("the failed GET's bytes are no longer held");
    }
}
