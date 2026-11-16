//! Registry-record authentication shared by the query-worker registry
//! (ADR-1918) and the maintain-worker registry (ADR-3130).
//!
//! Both registries sign each heartbeat record with a keyed BLAKE3 MAC under a
//! record key derived from an operator key file, read each sibling record with
//! a bounded range read, and count every rejected record by the same closed
//! set of reasons. Each registry keeps its own `blake3::derive_key` context,
//! so a record MAC minted for one registry never verifies in the other, even
//! when both are keyed from the same file.

use std::sync::atomic::{AtomicU64, Ordering};

use ravel_object_store::{GetOutcome, GetRange, ObjectMeta, ObjectStoreBackend, StoreError};

/// The largest registry record a live-set read accepts (ADR-1918 decision 5,
/// ADR-3130 decision 4). The reader asks for one byte more, so a longer
/// object is recognised as oversize without being decoded.
pub const MAX_RECORD_BYTES: u64 = 4096;

/// Why a live-set read skipped a sibling's registry record (ADR-1918 decision
/// 4, ADR-3130 decision 4). Rendered as the closed `reason` label of
/// `ravel_query_worker_record_rejects_total` and
/// `ravel_maintain_worker_record_rejects_total`. A key that is not a UUID and
/// a record skipped before the GET because its LIST modification time is
/// outside the liveness window are not rejects.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordReject {
    /// The record decoded but carried no MAC (for the maintain registry, also
    /// any record whose `format_version` is not the current one).
    MissingMac,
    /// The MAC is not well formed or verifies under no record key.
    BadMac,
    /// The object is longer than [`MAX_RECORD_BYTES`]; it was not decoded.
    Oversize,
    /// The object is empty or does not decode, or its `process_id` disagrees
    /// with its key.
    Malformed,
    /// The record verified, but its stamp is outside the liveness window
    /// although its modification time is inside it: a replayed older heartbeat
    /// or a clock skewed past the window.
    StaleStamp,
}

impl RecordReject {
    /// Every reason, in the fixed order the counters are indexed by.
    pub const ALL: [RecordReject; 5] = [
        RecordReject::MissingMac,
        RecordReject::BadMac,
        RecordReject::Oversize,
        RecordReject::Malformed,
        RecordReject::StaleStamp,
    ];

    /// The stable `reason` metric label value.
    pub fn reason(self) -> &'static str {
        match self {
            RecordReject::MissingMac => "missing_mac",
            RecordReject::BadMac => "bad_mac",
            RecordReject::Oversize => "oversize",
            RecordReject::Malformed => "malformed",
            RecordReject::StaleStamp => "stale_stamp",
        }
    }

    pub(crate) fn index(self) -> usize {
        match self {
            RecordReject::MissingMac => 0,
            RecordReject::BadMac => 1,
            RecordReject::Oversize => 2,
            RecordReject::Malformed => 3,
            RecordReject::StaleStamp => 4,
        }
    }
}

/// Cumulative [`RecordReject`] counts of one process's live-set reads.
#[derive(Debug, Default)]
pub struct RecordRejectMetrics {
    rejects: [AtomicU64; RecordReject::ALL.len()],
}

impl RecordRejectMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Records rejected for `reason` so far.
    pub fn get(&self, reason: RecordReject) -> u64 {
        self.rejects[reason.index()].load(Ordering::Relaxed)
    }

    /// Every reason with its count, in [`RecordReject::ALL`] order.
    pub fn by_reason(&self) -> [(RecordReject, u64); RecordReject::ALL.len()] {
        RecordReject::ALL.map(|reason| (reason, self.get(reason)))
    }

    pub(crate) fn record(&self, reason: RecordReject) {
        self.rejects[reason.index()].fetch_add(1, Ordering::Relaxed);
    }
}

/// One registry's record keys: one per file key, each
/// `blake3::derive_key(context, file_key)`. The first mints; a MAC verifies
/// under any of them. Never empty.
#[derive(Clone)]
pub(crate) struct DerivedKeys {
    keys: Vec<[u8; 32]>,
}

impl DerivedKeys {
    /// Derive one key per file key, in order, or `None` for an empty list.
    pub(crate) fn derive(context: &str, file_keys: &[[u8; 32]]) -> Option<Self> {
        if file_keys.is_empty() {
            return None;
        }
        let keys = file_keys
            .iter()
            .map(|file_key| blake3::derive_key(context, file_key))
            .collect();
        Some(DerivedKeys { keys })
    }

    pub(crate) fn len(&self) -> usize {
        self.keys.len()
    }

    /// The MAC of `claim` under the minting (first) key.
    pub(crate) fn mac(&self, claim: &[u8]) -> blake3::Hash {
        // `derive` refuses an empty list, so `keys[0]` always exists; the
        // fallback keeps the type total without a panic path.
        let mint = self.keys.first().copied().unwrap_or([0; 32]);
        blake3::keyed_hash(&mint, claim)
    }

    /// Whether `presented` is the MAC of `claim` under any key. Each
    /// comparison is `blake3::Hash`'s constant-time equality.
    pub(crate) fn verifies(&self, claim: &[u8], presented: &blake3::Hash) -> bool {
        self.keys
            .iter()
            .any(|key| blake3::keyed_hash(key, claim) == *presented)
    }
}

impl std::fmt::Debug for DerivedKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DerivedKeys")
            .field("keys", &self.keys.len())
            .finish()
    }
}

/// Read the record at `meta` with a range read of [`MAX_RECORD_BYTES`] plus
/// one byte. The outer `Err` is a store failure; the inner one is
/// [`RecordReject::Malformed`] for an empty object or
/// [`RecordReject::Oversize`] for a longer one, which is never decoded.
pub(crate) async fn read_bounded(
    store: &dyn ObjectStoreBackend,
    meta: &ObjectMeta,
) -> Result<Result<GetOutcome, RecordReject>, StoreError> {
    if meta.size == 0 {
        return Ok(Err(RecordReject::Malformed));
    }
    let got = match store
        .get(&meta.key, GetRange::Range(0, MAX_RECORD_BYTES + 1))
        .await
    {
        Ok(got) => got,
        // The object shrank to nothing between the LIST and the GET.
        Err(StoreError::InvalidRange(_)) => return Ok(Err(RecordReject::Malformed)),
        Err(err) => return Err(err),
    };
    if u64::try_from(got.data.len()).unwrap_or(u64::MAX) > MAX_RECORD_BYTES {
        return Ok(Err(RecordReject::Oversize));
    }
    Ok(Ok(got))
}
