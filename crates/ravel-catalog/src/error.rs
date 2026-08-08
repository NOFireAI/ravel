//! Catalog errors (docs/catalog-and-mvcc.md).

use ravel_commit::keys::{KeyError, ReconstructionError};
use ravel_commit::record::RecordError;
use ravel_object_store::StoreError;

use crate::provisioning::ProvisioningError;
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
    /// A compaction record failed protobuf decode (docs/catalog-and-mvcc.md
    /// step 2). Fatal: layout drift or corruption, never silently skipped.
    #[error("compaction record at {key:?} failed to decode: {source}")]
    CompactionRecordDecode {
        key: String,
        #[source]
        source: prost::DecodeError,
    },
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
    /// §10: validated on every cache hit and every fresh decode). Also used,
    /// per ADR-0050 §2, for: a catalog HEAD or postings object whose
    /// tenant_hash does not match the requesting tenant (hard failure, never
    /// a fallback to listing), and for a listing helper result whose key
    /// does not begin with the requesting tenant's prefix (`field ==
    /// "list_prefix"`, a hard isolation-breach error, never a silently
    /// dropped or served foreign key).
    #[error("object at {key:?} has unexpected {field}: expected {expected}, got {actual}")]
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
    /// `fold`'s HEAD CAS lost to a concurrent folder more times than the
    /// bounded retry budget allows (docs/metric-index-plan.md section 4,
    /// step 7). Not a correctness failure: the winning folder's HEAD is
    /// intact and a later fold attempt can retry from scratch.
    #[error(
        "fold gave up after {attempts} HEAD CAS attempts at watermark_hour={watermark_hour}: a concurrent folder keeps winning"
    )]
    FoldCasRetriesExhausted { attempts: u32, watermark_hour: u32 },
    /// The window's pre-execution catalog-request estimate (ADR-0044
    /// decision 3, amended 2026-08-05 for issue #635) exceeds the configured
    /// [`CatalogConfig::max_catalog_list_requests`](crate::CatalogConfig::max_catalog_list_requests)
    /// ceiling. Fail-closed admission control: the resolve is refused before
    /// any LIST is issued, never silently narrowed, so an unbounded client
    /// window cannot make one request fan out to hundreds of thousands of
    /// LISTs. `estimate` and `limit` are counts only (no object key, no tenant
    /// identity), so the message is safe to surface to a client and lets a
    /// caller narrow its window and retry knowing by how much it was over.
    #[error(
        "query window too wide: it would issue an estimated {estimate} catalog list requests, over the limit of {limit}; narrow the query time range and retry"
    )]
    WindowTooWide { estimate: u64, limit: u64 },
    /// The configured `shard_count` disagrees with the (tenant, signal)'s
    /// durable provisioning record, or the record could not be read/decoded
    /// (ADR-0050 section 5, EC5). Fails the resolve rather than iterating
    /// `0..shard_count` and silently dropping the shards a lower value omits.
    /// A `shard_count` disagreement carries the same loud, named semantics as
    /// the fold-HEAD [`CatalogError::FieldMismatch`] this extends to Phase 1
    /// listing and fresh tenants.
    #[error("provisioning check failed: {0}")]
    Provisioning(#[from] ProvisioningError),
    /// The HEAD read at the top of a fold carries a `format_version` newer
    /// than this process understands (ADR-0066 decision 2,
    /// "fail-closed-on-newer"). Fatal by design: a newer-format HEAD may be a
    /// multi-part HEAD written by an already-upgraded peer during a rolling
    /// deployment, and rebuilding a single-part HEAD to CAS-overwrite it would
    /// silently discard whatever the newer format carries. The fold refuses
    /// rather than clobber; the operator must upgrade this process. Distinct
    /// from [`CatalogError::SnapshotFormat`]/`Corrupt`, which is genuine
    /// corruption and is self-healed by rebuild.
    #[error(
        "HEAD format_version {format_version} is newer than this process understands; upgrade this process (it must not rebuild and overwrite a newer HEAD)"
    )]
    UnsupportedHeadVersion { format_version: u32 },
}
