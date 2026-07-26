//! Catalog: snapshot resolution over commit records (MVCC).
//!
//! Implementer contract: docs/catalog-and-mvcc.md and ADR-0003. Phase 1 is
//! listing-based discovery behind the Catalog API; snapshots come with
//! compaction.

mod cache;
mod catalog;
mod config;
mod error;
mod snapshot;

pub use catalog::Catalog;
pub use config::{
    CatalogConfig, DEFAULT_CACHE_CAPACITY_PER_TENANT, DEFAULT_CLOCK_SKEW_ALLOWANCE_NS,
    DEFAULT_MAX_INGEST_LAG_NS,
};
pub use error::CatalogError;
pub use snapshot::{SegmentRef, Snapshot};
