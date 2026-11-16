//! Making a fetch-layer reservation ride with a bare-`Bytes` return value.
//!
//! ADR-1170 decision 2 requires a reservation guard's lifetime to be its
//! buffer's lifetime, not the GET's. Several fetch paths return a bare `Bytes`,
//! so the guard has to travel inside it; that is what [`attach_reservation`]
//! does.
//!
//! This lives in one module rather than once per fetcher on purpose. The RSEG,
//! RLOG and RSPAN paths each need it, and a reservation whose lifetime rule
//! drifts between two copies is precisely the defect class decision 2 exists to
//! prevent: a guard that outlives its buffer overcounts, and a buffer that
//! outlives its guard leaves bytes under no ledger at all.

use bytes::Bytes;

/// Owns a fetched `Bytes` together with its fetch-layer reservation, so the
/// guard releases when the last clone of the returned `Bytes` is dropped, never
/// because the GET completed. The `AsRef<[u8]>` forwards to the inner `Bytes`,
/// so the wrapped view is byte-identical to the original.
struct ReservedBytes {
    bytes: Bytes,
    _reservation: ravel_memory::Reservation,
}

impl AsRef<[u8]> for ReservedBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

/// Wraps `bytes` so `reservation` lives exactly as long as the returned
/// `Bytes`. Zero-copy: the backing allocation is shared, only the owner
/// changes.
pub(crate) fn attach_reservation(bytes: Bytes, reservation: ravel_memory::Reservation) -> Bytes {
    Bytes::from_owner(ReservedBytes {
        bytes,
        _reservation: reservation,
    })
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// The wrapped view is byte-identical, and the guard is held for exactly as
    /// long as the returned `Bytes` rather than being released at wrap time.
    #[test]
    fn the_guard_lives_as_long_as_the_bytes_and_the_view_is_identical() {
        let budget = Arc::new(ravel_memory::MemoryBudget::new(1024));
        let payload = Bytes::from_static(b"reserved payload");
        let reservation = budget
            .reserve(payload.len() as u64)
            .expect("budget admits the payload");

        let wrapped = attach_reservation(payload.clone(), reservation);
        assert_eq!(wrapped.as_ref(), payload.as_ref());
        assert_eq!(
            budget.reserved(),
            payload.len() as u64,
            "the guard is still held while the wrapped Bytes lives"
        );

        let clone = wrapped.clone();
        drop(wrapped);
        assert_eq!(
            budget.reserved(),
            payload.len() as u64,
            "a surviving clone keeps the guard alive"
        );

        drop(clone);
        assert_eq!(
            budget.reserved(),
            0,
            "the guard releases with the last clone, not at wrap time"
        );
    }
}
