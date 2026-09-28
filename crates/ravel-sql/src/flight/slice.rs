//! Worker-side verification of a SQL slice capability (ADR-1689 decision 2).
//!
//! A slice `DoGet` carries no client credential. The slice ticket is the
//! capability: a keyed MAC under the slice key over the tenant, the deadline,
//! the slice position, and the exact segment set. Verification is stateless:
//! the MAC under any configured slice key, `is_expired` against the injected
//! clock, `slice_count > 1`, and the listener role the server mounted the
//! service in. The slice executes under the ticket's tenant; `FlightAuth` is
//! never consulted on this path.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use tonic::Status;

use crate::flight_ticket::{FlightTicket, FlightTicketError, SqlTicketKeys, TicketSurface};

/// Which Flight surfaces a listener serves (ADR-1689 decision 1). The server
/// supplies it when it mounts the service.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FlightListenerRole {
    /// Both the client Flight SQL surface and slice `DoGet`, on one listener.
    /// The layout of a process without a dedicated fragment listener.
    #[default]
    Combined,
    /// The public listener once a dedicated fragment listener is configured:
    /// the client surface only. A slice ticket is refused outright.
    ClientOnly,
    /// The dedicated fragment listener: slice `DoGet` only. Every client
    /// method is refused with `permission_denied`.
    SliceOnly,
}

impl FlightListenerRole {
    /// Whether this listener serves slice `DoGet`.
    pub fn serves_slices(self) -> bool {
        matches!(self, Self::Combined | Self::SliceOnly)
    }

    /// Whether this listener serves the client Flight SQL surface.
    pub fn serves_clients(self) -> bool {
        matches!(self, Self::Combined | Self::ClientOnly)
    }
}

/// The closed reason label for a refused slice capability, matching the
/// fragment capability counter's shape. Every refusal increments exactly one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SliceReject {
    /// The `DoGet` carried no slice ticket at all, or a handle too short or
    /// too malformed to be one, so no MAC was ever checked.
    Missing,
    /// The ticket did not verify under any configured slice key, and not
    /// under a client key either.
    BadMac,
    /// The ticket's `deadline_ns` is at or before the injected clock.
    Expired,
    /// The ticket is for another surface: a client whole-set ticket presented
    /// as a slice, a slice-key ticket that is not a slice (`slice_count <= 1`),
    /// or a slice ticket presented on a listener that serves no slices.
    WrongSurface,
}

impl SliceReject {
    /// Every reason, in the fixed order the counters are indexed by.
    pub const ALL: [SliceReject; 4] = [
        SliceReject::Missing,
        SliceReject::BadMac,
        SliceReject::Expired,
        SliceReject::WrongSurface,
    ];

    /// The stable `reason` metric label value.
    pub fn reason(self) -> &'static str {
        match self {
            SliceReject::Missing => "missing",
            SliceReject::BadMac => "bad_mac",
            SliceReject::Expired => "expired",
            SliceReject::WrongSurface => "wrong_surface",
        }
    }

    fn index(self) -> usize {
        match self {
            SliceReject::Missing => 0,
            SliceReject::BadMac => 1,
            SliceReject::Expired => 2,
            SliceReject::WrongSurface => 3,
        }
    }

    /// The status a refused `DoGet` answers with. A missing, forged, or
    /// expired capability is `unauthenticated`, as on the fragment lane; a
    /// valid ticket on the wrong surface is `permission_denied`. The message
    /// names the reason label.
    pub fn status(self) -> Status {
        let message = format!("slice fetch rejected: {}", self.reason());
        match self {
            SliceReject::WrongSurface => Status::permission_denied(message),
            SliceReject::Missing | SliceReject::BadMac | SliceReject::Expired => {
                Status::unauthenticated(message)
            }
        }
    }
}

/// Per-reason counts of refused slice capabilities. Cloning shares the counts,
/// so the server can export them as a labeled counter.
#[derive(Debug, Clone, Default)]
pub struct SliceRejectCounters {
    inner: Arc<[AtomicU64; 4]>,
}

impl SliceRejectCounters {
    /// Refusals counted under `reason`.
    pub fn get(&self, reason: SliceReject) -> u64 {
        self.inner[reason.index()].load(Ordering::Relaxed)
    }

    /// Every reason's count paired with its `reason` label.
    pub fn by_reason(&self) -> [(&'static str, u64); 4] {
        SliceReject::ALL.map(|reason| (reason.reason(), self.get(reason)))
    }

    pub(super) fn record(&self, reason: SliceReject) {
        self.inner[reason.index()].fetch_add(1, Ordering::Relaxed);
    }
}

/// Verify `handle` as a slice capability at `now_ns`. The checks run in a
/// fixed order (present, MAC, expiry, slice count) so a ticket that fails
/// several ways is attributed to the first. A handle the decoder reports as
/// truncated is counted as missing, not as a bad MAC: before the MAC is
/// checked that means shorter than the smallest ticket, and after it only a
/// payload that verified yet ends early. The role is checked by the caller,
/// which decides whether a `DoGet` is a slice fetch at all.
pub(super) fn verify_slice(
    keys: &SqlTicketKeys,
    handle: &[u8],
    now_ns: i64,
) -> Result<FlightTicket, SliceReject> {
    if handle.is_empty() {
        return Err(SliceReject::Missing);
    }
    let ticket = match keys.decode(handle, TicketSurface::Slice) {
        Ok(ticket) => ticket,
        Err(FlightTicketError::Truncated) => return Err(SliceReject::Missing),
        Err(_) if keys.decode(handle, TicketSurface::Client).is_ok() => {
            return Err(SliceReject::WrongSurface);
        }
        Err(_) => return Err(SliceReject::BadMac),
    };
    check_slice_claims(ticket, now_ns)
}

/// The checks after the MAC, for a ticket that already verified under a slice
/// key: expiry, then `slice_count > 1`.
pub(super) fn check_slice_claims(
    ticket: FlightTicket,
    now_ns: i64,
) -> Result<FlightTicket, SliceReject> {
    if ticket.is_expired(now_ns) {
        return Err(SliceReject::Expired);
    }
    if ticket.slice_count <= 1 {
        return Err(SliceReject::WrongSurface);
    }
    Ok(ticket)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    const NOW_NS: i64 = 1_000;

    fn slice_ticket() -> FlightTicket {
        FlightTicket {
            tenant: ravel_types::TenantId::new("acme").hash(),
            statement: String::new(),
            segments: Vec::new(),
            min_commit_tokens: Vec::new(),
            now_ns: NOW_NS,
            deadline_ns: NOW_NS + 1_000,
            slice_index: 0,
            slice_count: 2,
            pending_erasure: Vec::new(),
            declared_columns: Vec::new(),
        }
    }

    /// A handle cut short of the smallest possible ticket never reaches a MAC
    /// check, so it is counted as missing; the same ticket at full length with
    /// its MAC flipped is the bad MAC case.
    #[test]
    fn a_truncated_slice_handle_is_missing_not_bad_mac() {
        let keys = SqlTicketKeys::from_file_key(&[0x42; 32]);
        let encoded = keys
            .encode(&slice_ticket(), TicketSurface::Slice)
            .expect("encode");
        assert_eq!(
            verify_slice(&keys, &encoded, NOW_NS).expect("the full handle verifies"),
            slice_ticket()
        );

        // A ticket with no statement, pins or columns is the smallest one the
        // decoder accepts, so 97 bytes is the full handle verified above and
        // 96 is the longest length the guard still refuses.
        assert_eq!(encoded.len(), 97, "the fixture sits on the length guard");
        for len in [1, 4, 16, 32, 96] {
            assert_eq!(
                verify_slice(&keys, &encoded[..len], NOW_NS),
                Err(SliceReject::Missing),
                "a {len}-byte prefix of a slice ticket"
            );
        }

        let mut forged = encoded.clone();
        let last = forged.len() - 1;
        forged[last] ^= 0x01;
        assert_eq!(
            verify_slice(&keys, &forged, NOW_NS),
            Err(SliceReject::BadMac),
            "a 97-byte handle clears the length guard, so its MAC is checked"
        );
    }
}
