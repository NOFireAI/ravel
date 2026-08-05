//! Catalog: snapshot resolution over commit records (MVCC).
//!
//! Implementer contract: docs/catalog-and-mvcc.md and ADR-0003. Phase 1 is
//! listing-based discovery behind the Catalog API; snapshots come with
//! compaction.

mod cache;
mod catalog;
mod config;
mod error;
mod fold;
mod provisioning;
mod snapshot;
mod snapshot_format;
mod snapshot_resolve;

pub use catalog::Catalog;
pub use config::{
    CatalogConfig, DEFAULT_BYTE_CACHE_MAX_BYTES, DEFAULT_BYTE_CACHE_MAX_ENTRIES,
    DEFAULT_BYTE_CACHE_MAX_ENTRY_BYTES, DEFAULT_CACHE_CAPACITY_PER_TENANT,
    DEFAULT_CLOCK_SKEW_ALLOWANCE_NS, DEFAULT_FOLD_SAFETY_MARGIN_NS, DEFAULT_HEAD_CACHE_CAPACITY,
    DEFAULT_HEAD_CACHE_TTL_NS, DEFAULT_MAX_FLUSH_LIFETIME_NS, DEFAULT_MAX_INGEST_LAG_NS,
    DEFAULT_POSTINGS_CACHE_ENTRIES, DEFAULT_SNAPSHOT_CACHE_PARTS,
};
pub use error::CatalogError;
pub use fold::{FoldReport, Transaction};
pub use provisioning::{
    AbsentPolicy, DEFAULT_SCAN_SLACK_HOURS, GenerationDefect, MAX_SHARD_COUNT,
    PROVISIONING_FORMAT_VERSION, ProvisioningCheck, ProvisioningError, ReshardOutcome,
    ShardGeneration, active_shard_count, append_generation, provisioning_key, read_generations,
    read_generations_checked, read_generations_from_store, scan_count, shard_ceiling,
    validate_or_adopt,
};
pub use snapshot::{SegmentLevel, SegmentRef, Snapshot};
pub use snapshot_format::{
    DEFAULT_MAX_POSTINGS_BYTES, DEFAULT_MAX_SNAPSHOT_PART_BYTES, DecodedPart, DecodedPostings,
    HEAD_FORMAT_VERSION, MAGIC, NamePostings, PartLimits, PostingsLimits, SnapshotFormatError,
    VERSION, decode_head, decode_part, decode_postings, encode_head, encode_part, encode_postings,
};
