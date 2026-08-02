//! Error taxonomy for the compactor. Every stored-byte or protocol
//! inconsistency is a typed variant; nothing panics on adversarial input
//! (CLAUDE.md invariants, docs/compaction-retention-plan.md §3.6).

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
    #[error("compaction invariant breach: {0}")]
    Invariant(String),
}

impl From<ravel_segment::WriteError> for MaintainError {
    fn from(e: ravel_segment::WriteError) -> Self {
        MaintainError::Write(e.to_string())
    }
}

pub type Result<T> = std::result::Result<T, MaintainError>;
