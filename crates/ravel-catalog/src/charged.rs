//! Decoded output carried together with its memory reservation (ADR-1702
//! decision 6, task 5).
//!
//! The resolve path reserves a part's or postings object's declared
//! uncompressed length against the process [`ravel_memory::MemoryBudget`]
//! before decoding it. The guard has to live exactly as long as the decoded
//! value, including while a decoded-object cache holds it, so the two travel
//! as one value and every clone of the `Arc` around it shares one guard.

use std::ops::Deref;
use std::sync::Arc;

use ravel_memory::{MemoryBudget, MemoryExhausted, Reservation};
use ravel_segment::CatalogCharge;

use crate::snapshot_format::{DecodedPart, DecodedPostings};

/// A [`ravel_segment::CatalogCharge`] backed by a [`MemoryBudget`]: each figure
/// a catalog decode admits is reserved against the budget before the decode
/// allocates it, and the reservation of that figure is dropped when the decode
/// reports it freed. A decode the budget refuses stops with
/// [`ravel_segment::SegmentError::CatalogChargeRefused`] before allocating, and
/// [`refused`](Self::refused) carries the exhaustion it hit.
///
/// This is the same hook `ravel-query`'s fetcher uses to charge the per-sample
/// provenance decode on the PromQL read path (issue #4118); it is shared here
/// so the catalog fold's postings build and the `query_exemplars` surface charge
/// that same decode against the process budget rather than decoding it uncharged
/// (issue #4194). Both of those callers discard the provenance, so each drops
/// this charge (and frees what it decoded) as soon as the decode returns.
#[derive(Debug)]
pub struct BudgetCatalogCharge {
    budget: Arc<MemoryBudget>,
    held: Vec<Reservation>,
    refused: Option<MemoryExhausted>,
}

impl BudgetCatalogCharge {
    /// A charge against `budget`.
    pub fn new(budget: &Arc<MemoryBudget>) -> Self {
        Self {
            budget: Arc::clone(budget),
            held: Vec::new(),
            refused: None,
        }
    }

    /// The exhaustion a refused admission hit, set once a decode this charge
    /// backed returns [`ravel_segment::SegmentError::CatalogChargeRefused`].
    pub fn refused(&self) -> Option<&MemoryExhausted> {
        self.refused.as_ref()
    }
}

impl CatalogCharge for BudgetCatalogCharge {
    fn admit(&mut self, bytes: u64) -> bool {
        if bytes == 0 {
            return true;
        }
        match self.budget.reserve(bytes) {
            Ok(reservation) => {
                self.held.push(reservation);
                true
            }
            Err(e) => {
                self.refused = Some(e);
                false
            }
        }
    }

    fn release(&mut self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        if let Some(i) = self.held.iter().rposition(|r| r.size() == bytes) {
            self.held.swap_remove(i);
        }
    }
}

/// A decoded snapshot part and the reservation for its entry body.
pub(crate) type ChargedPart = Charged<DecodedPart>;

/// A decoded postings object and the reservation for its body.
pub(crate) type ChargedPostings = Charged<DecodedPostings>;

/// Reserves [`ravel_memory::decoded_charge`] for a decode of a
/// `declared`-byte body.
///
/// This is the bare reservation. Every resolve-path decode reaches it through
/// `Catalog::reserve_decoded` instead (parts and postings directly, column
/// statistics through [`DecodeReserver`]), so that a refusal gives the
/// decoded-output caches a chance to hand memory back first.
pub(crate) fn reserve_decoded(
    budget: &Arc<MemoryBudget>,
    declared: u64,
    ceiling: u64,
) -> Result<Reservation, MemoryExhausted> {
    budget.reserve(ravel_memory::decoded_charge(declared, ceiling))
}

/// What a decode site reserves its declared output bytes through.
///
/// Every implementation is [`Catalog::reserve_decoded`](crate::Catalog)
/// (on the catalog itself or the handle [`Self::owned`] returns), which
/// answers a refusal by evicting decoded-output cache entries and retrying
/// once. A resolve helper takes this rather than the bare
/// `MemoryBudget` because the two are not interchangeable under memory
/// pressure: a site holding the budget directly fails while cached decoded
/// parts and postings still hold memory that would have let it through, which
/// is what the column-statistics path did (issue #2107).
///
/// `Sync` so a `&dyn`/`&impl` of it can be held across an await in a `Send`
/// future, which every resolve helper's is.
pub(crate) trait DecodeReserver: Sync {
    /// [`reserve_decoded`]'s contract, with whatever the implementation does
    /// about a refusal.
    fn reserve_decoded(&self, declared: u64, ceiling: u64) -> Result<Reservation, MemoryExhausted>;

    /// An owned reserver with the same refusal handling, for a decode job
    /// that runs off the caller's task and reserves as it reads.
    fn owned(&self) -> Box<dyn DecodeReserver + Send>;
}

/// A decoded value and the reservation that charges its bytes. Derefs to the
/// value, so readers use it exactly as they used the bare value.
#[derive(Debug)]
pub(crate) struct Charged<T> {
    value: T,
    _reservation: Reservation,
}

impl<T> Charged<T> {
    pub(crate) fn new(value: T, reservation: Reservation) -> Self {
        Self {
            value,
            _reservation: reservation,
        }
    }

    /// The bytes this value's reservation holds against its budget.
    pub(crate) fn reserved_bytes(&self) -> u64 {
        self._reservation.size()
    }

    /// A value charged 0 bytes against a private unlimited budget, for tests
    /// that build decoded values by hand.
    #[cfg(test)]
    #[allow(clippy::expect_used)]
    pub(crate) fn for_test(value: T) -> Self {
        let budget = Arc::new(MemoryBudget::unlimited());
        Self::new(value, budget.reserve(0).expect("a 0-byte reservation fits"))
    }
}

impl<T> Deref for Charged<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.value
    }
}
