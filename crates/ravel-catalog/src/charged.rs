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

use crate::snapshot_format::{DecodedPart, DecodedPostings};

/// A decoded snapshot part and the reservation for its entry body.
pub(crate) type ChargedPart = Charged<DecodedPart>;

/// A decoded postings object and the reservation for its body.
pub(crate) type ChargedPostings = Charged<DecodedPostings>;

/// Reserves the bytes a decode may allocate: the object's declared
/// uncompressed length, clamped to the decoder's own ceiling, since the
/// decoder refuses a declared length over its ceiling before allocating.
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
    budget.reserve(declared.min(ceiling))
}

/// What a decode site reserves its declared output bytes through.
///
/// The one implementation is [`Catalog::reserve_decoded`](crate::Catalog),
/// which answers a refusal by evicting decoded-output cache entries and
/// retrying once. A resolve helper takes this rather than the bare
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
