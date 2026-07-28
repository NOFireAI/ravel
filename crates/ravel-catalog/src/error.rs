//! Catalog errors (docs/catalog-and-mvcc.md).

use ravel_commit::keys::{KeyError, ReconstructionError};
use ravel_commit::record::RecordError;
use ravel_object_store::StoreError;

use crate::snapshot_format::SnapshotFormatError;

#[derive(Debug, thiserror::Error)]
pub enum CatalogError {
    #[error("catalog config invalid: shard_count must be > 0")]
    InvalidConfig,
    #[error("store error: {0}")]
    Store(#[from] StoreError),
    #[error("key error: {0}")]
    Key(#[from] KeyError),
    #[error("commit record decode/validation error: {0}")]
    Record(#[from] RecordError),
    /// Fatal: a commit record's own identity fields do not reconstruct to
    /// its stored `object_key` (ADR-0010 §7). Never silently prefer either
    /// value; surfaces the whole `resolve` call as an error.
    #[error("commit record at {key:?} failed object_key reconstruction: {source}")]
    Reconstruction {
        key: String,
        #[source]
        source: ReconstructionError,
    },
    /// A decoded record's tenant_hash/signal/shard does not match the
    /// (tenant, signal, shard) it was listed or addressed under (ADR-0010
    /// §10: validated on every cache hit and every fresh decode).
    #[error("commit record at {key:?} has unexpected {field}: expected {expected}, got {actual}")]
    FieldMismatch {
        key: String,
        field: &'static str,
        expected: String,
        actual: String,
    },
    /// A `min_token` did not resolve to a commit record after one retry
    /// (docs/catalog-and-mvcc.md step 4).
    #[error(
        "min_token unsatisfiable after retry: shard={shard} writer_id={writer_id} epoch={epoch} seq={seq} ingest_hour_bucket={ingest_hour_bucket}"
    )]
    UnsatisfiableToken {
        shard: u32,
        writer_id: String,
        epoch: u64,
        seq: u64,
        ingest_hour_bucket: u32,
    },
    /// A snapshot part or HEAD record `fold` produced or read failed the
    /// envelope codec's own validation (docs/metric-index-plan.md 3.1, 3.2).
    #[error("snapshot format error: {0}")]
    SnapshotFormat(#[from] SnapshotFormatError),
    /// `fold` found two commit records sharing the same identity
    /// (ingest_hour_bucket, shard, writer_id, writer_epoch, writer_seq)
    /// while building a snapshot part. `encode_part` would also reject this,
    /// but fold checks first so the error names the exact colliding
    /// identity (docs/metric-index-plan.md section 4, step 5).
    #[error(
        "duplicate commit identity while folding: shard={shard} ingest_hour_bucket={ingest_hour_bucket} writer_id={writer_id} writer_epoch={writer_epoch} writer_seq={writer_seq}"
    )]
    DuplicateEntryIdentity {
        shard: u32,
        ingest_hour_bucket: u32,
        writer_id: String,
        writer_epoch: u64,
        writer_seq: u64,
    },
    /// `fold`'s HEAD CAS lost to a concurrent folder more times than the
    /// bounded retry budget allows (docs/metric-index-plan.md section 4,
    /// step 7). Not a correctness failure: the winning folder's HEAD is
    /// intact and a later fold attempt can retry from scratch.
    #[error(
        "fold gave up after {attempts} HEAD CAS attempts at watermark_hour={watermark_hour}: a concurrent folder keeps winning"
    )]
    FoldCasRetriesExhausted { attempts: u32, watermark_hour: u32 },
}
