//! Error taxonomy for the compactor. Every stored-byte or protocol
//! inconsistency is a typed variant; nothing panics on adversarial input
//! (CLAUDE.md invariants, docs/compaction-retention-plan.md §3.6).

use ravel_commit::erasure::ErasureError;
use ravel_commit::keys::{KeyError, ReconstructionError};
use ravel_commit::record::RecordError;
use ravel_logseg::LogSegError;
use ravel_object_store::StoreError;
use ravel_rspan::SpanSegError;
use ravel_segment::SegmentError;

/// A compaction run's failure. Retryable store faults surface as
/// [`MaintainError::Store`] carrying the underlying [`StoreError`]; the caller
/// decides whether to re-run the whole bucket (the idempotent recovery path,
/// plan §3.4/§3.6). The `*Divergence` and `*Mismatch` variants are invariant
/// breaches that must never be silently worked around (plan §3.4 point 3).
#[derive(Debug, thiserror::Error)]
pub enum MaintainError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error(transparent)]
    Key(#[from] KeyError),
    #[error(transparent)]
    Reconstruction(#[from] ReconstructionError),
    #[error(transparent)]
    Record(#[from] RecordError),
    #[error(transparent)]
    Segment(#[from] SegmentError),
    #[error(transparent)]
    LogSeg(#[from] LogSegError),
    #[error(transparent)]
    SpanSeg(#[from] SpanSegError),
    #[error(transparent)]
    Erasure(#[from] ErasureError),
    #[error("segment write error: {0}")]
    Write(String),
    #[error(
        "two inputs claim log stream {stream_id} with different resource+scope attribute blobs ({a_len} and {b_len} bytes): stream identity is violated upstream or a stream-id hash collided (fatal invariant breach)"
    )]
    StreamAttrsConflict {
        stream_id: String,
        a_len: usize,
        b_len: usize,
    },
    #[error("unknown object shape in bucket listing: {0:?}")]
    UnknownBucketEntry(String),
    #[error("decoded record signal {actual} does not match the queried signal {expected}")]
    SignalMismatch { expected: String, actual: String },
    #[error(
        "sealed bucket holds two compaction records with different input_set_hash (fatal invariant breach at {observed_key:?}): ours {ours}, theirs {theirs}"
    )]
    InputSetHashDivergence {
        observed_key: String,
        ours: String,
        theirs: String,
    },
    #[error(
        "compaction record-count conservation violated for tenant {tenant_hash} signal {signal} shard {shard} hour {ingest_hour_bucket}: inputs carry {input_sample_count} records, built parts carry {part_sample_count}; the merge dropped or invented records, publish aborted (fatal invariant breach, ADR-0048)"
    )]
    ConservationViolation {
        /// Hex tenant hash of the bucket (the key-prefix form operators see).
        tenant_hash: String,
        /// Signal key prefix (`m`, `l`, `s`).
        signal: String,
        shard: u32,
        ingest_hour_bucket: u32,
        /// Sum of `sample_count` over the compaction input set.
        input_sample_count: u64,
        /// Sum of `sample_count` over the built output parts.
        part_sample_count: u64,
    },
    #[error(
        "rspan compaction input record-count cross-check failed for object {object_key:?}: the streaming k-way merge decoded {decoded_record_count} records from this input but its footer declares {footer_record_count} (a logically inconsistent but CRC-valid input, silent decode-side record loss or gain, fatal invariant breach, issue #926); publish aborted, nothing written"
    )]
    SpanInputRecordCountMismatch {
        /// Data-object key of the input whose decode diverged from its footer.
        object_key: String,
        /// Records the streaming merge actually decoded out of this input.
        decoded_record_count: u64,
        /// `record_count` this input's own RSPAN footer declares, the
        /// independent authority the decode tally is checked against.
        footer_record_count: u64,
    },
    #[error("compaction invariant breach: {0}")]
    Invariant(String),
    #[error(
        "erasure rewrite record-count conservation violated for tenant {tenant_hash} signal {signal} shard {shard} hour {ingest_hour_bucket}: live set carries {input_sample_count} records, {output_sample_count} survived plus {dropped_sample_count} were dropped, which do not sum to the input (fatal invariant breach, ADR-0064 decision 3); publish aborted, nothing written"
    )]
    ErasureConservationViolation {
        /// Hex tenant hash of the bucket (the key-prefix form operators see).
        tenant_hash: String,
        /// Signal key prefix (`m`, `l`, `s`).
        signal: String,
        shard: u32,
        ingest_hour_bucket: u32,
        /// Sum of `sample_count` over the live input record set.
        input_sample_count: u64,
        /// Sum of `sample_count` over the built surviving-record output parts.
        output_sample_count: u64,
        /// Sum of samples dropped by the applied erasure predicates.
        dropped_sample_count: u64,
    },
    #[error(
        "erasure rewrite input record-count cross-check failed for tenant {tenant_hash} signal {signal} shard {shard} hour {ingest_hour_bucket}: the decode scanned {scanned_record_count} input records but the input objects' footers declare {footer_record_count} (a silent decode-side record loss, fatal invariant breach, ADR-0064 decision 3, issue #981); publish aborted, nothing written, originals preserved"
    )]
    ErasureInputConservationViolation {
        /// Hex tenant hash of the bucket (the key-prefix form operators see).
        tenant_hash: String,
        /// Signal key prefix (`m`, `l`, `s`).
        signal: String,
        shard: u32,
        ingest_hour_bucket: u32,
        /// Records the decode actually scanned out of the live input objects
        /// (the tally the conservation gate treats as the input total).
        scanned_record_count: u64,
        /// Sum of `record_count` declared by every live input object's footer
        /// (RLOG/RSPAN), the independent authority the scan is checked against.
        footer_record_count: u64,
    },
    #[error(
        "erasure rewrite found no live record in a bucket that is not empty (fatal invariant breach at {bucket_prefix:?}): {live_count} compaction/rewrite records present but every one is named by another's superseded_record_key"
    )]
    NoLiveRecord {
        bucket_prefix: String,
        live_count: usize,
    },
    #[error(
        "erasure rewrite found more than one live record in a bucket (fatal invariant breach at {bucket_prefix:?}): {live_keys:?} are all un-superseded, violating the resolver's at-most-one-live-record-per-generation invariant"
    )]
    MultipleLiveRecords {
        bucket_prefix: String,
        live_keys: Vec<String>,
    },
    #[error(
        "provisioning-record access failed during a format migration (shard-generation range or \
         format-floor history): {0}"
    )]
    Provisioning(String),
    #[error("audit batch flush failed: {0}")]
    AuditFlush(String),
    #[error(
        "tenant discovery found a prefix under t/ that is not a valid 32-hex-character tenant hash: {0:?} (ADR-0048 decision 3; storage's key-shape discipline never permits a silent skip here)"
    )]
    InvalidTenantPrefix(String),
    #[error(
        "orphan GC mass-orphan breaker tripped for tenant {tenant_hash} signal {signal} shard {shard}: {candidates} orphan candidates out of {l0_objects_listed} listed L0 objects, at or above the breaker threshold (>= {min_count} candidates and > {max_ratio} of listed objects); halted with zero deletions, sticky until commit records are restored or force_orphan_gc overrides (ADR-0048 decision 4)"
    )]
    OrphanBreakerTripped {
        /// Hex tenant hash of the shard (the key-prefix form operators see).
        tenant_hash: String,
        /// Signal key prefix (`m`, `l`, `s`).
        signal: String,
        shard: u32,
        /// Orphan candidates left after the batched re-verify: what this
        /// pass would have deleted.
        candidates: usize,
        /// L0 data objects listed this pass, the breaker ratio's denominator.
        l0_objects_listed: usize,
        /// The configured [`crate::config::CompactorConfig::orphan_breaker_min_count`].
        min_count: usize,
        /// The configured [`crate::config::CompactorConfig::orphan_breaker_max_ratio`].
        max_ratio: f64,
    },
}

impl From<ravel_segment::WriteError> for MaintainError {
    fn from(e: ravel_segment::WriteError) -> Self {
        MaintainError::Write(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, MaintainError>;
