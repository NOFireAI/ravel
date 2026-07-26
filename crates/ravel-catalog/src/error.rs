//! Catalog errors (docs/catalog-and-mvcc.md).

use ravel_commit::keys::{KeyError, ReconstructionError};
use ravel_commit::record::RecordError;
use ravel_object_store::StoreError;

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
}
