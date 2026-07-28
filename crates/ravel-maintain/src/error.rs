//! Error taxonomy for the P4 compactor.

use ravel_commit::keys::{KeyError, ReconstructionError};
use ravel_commit::record::RecordError;
use ravel_object_store::StoreError;
use ravel_segment::{SegmentError, WriteError};

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
    Write(#[from] WriteError),
    #[error("protobuf decode error: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error(
        "compaction record for this bucket already exists with a different input_set_hash (invariant breach): ours {ours}, stored {stored}"
    )]
    InputSetHashMismatch { ours: String, stored: String },
    #[error(
        "compaction run abandoned: exceeded max_compaction_lifetime before publishing (started {started_ns}ns, now {now_ns}ns)"
    )]
    Abandoned { started_ns: i64, now_ns: i64 },
    #[error("footer version {0} is not a valid L0 compaction input (expected 1, 2, or 3)")]
    UnsupportedInputVersion(u16),
    #[error("series {series_id} carries mixed run value kinds across inputs")]
    MixedValueKindAcrossInputs { series_id: String },
    #[error("hour bucket {0} overflows scan range (u32 exhausted)")]
    HourBucketOverflow(u32),
}
