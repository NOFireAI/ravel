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
mod snapshot;
mod snapshot_format;

pub use catalog::Catalog;
pub use config::{
    CatalogConfig, DEFAULT_CACHE_CAPACITY_PER_TENANT, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
    DEFAULT_FOLD_SAFETY_MARGIN_NS, DEFAULT_HEAD_CACHE_TTL_NS, DEFAULT_MAX_FLUSH_LIFETIME_NS,
    DEFAULT_MAX_INGEST_LAG_NS, DEFAULT_SNAPSHOT_CACHE_PARTS,
};
pub use error::CatalogError;
pub use fold::{FoldReport, Transaction};
pub use snapshot::{SegmentRef, Snapshot};
pub use snapshot_format::{
    DEFAULT_MAX_SNAPSHOT_PART_BYTES, DecodedPart, HEAD_FORMAT_VERSION, MAGIC, PartLimits,
    SnapshotFormatError, VERSION, decode_head, decode_part, encode_head, encode_part,
};
