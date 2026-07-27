//! ravel-sql: DataFusion-backed SQL execution over Ravel metric samples.
//!
//! This crate is the structural isolation boundary that keeps `datafusion`
//! (and, transitively, its own `arrow`) out of the PromQL and ingest paths
//! (ADR-0013 / docs/arrow-datafusion-plan.md section 3). Nothing in the
//! ingest-critical or PromQL crates links this crate.
//!
//! Ticket B1 (issue #20) implements the read pipeline skeleton described in
//! docs/arrow-datafusion-plan.md section 2, redesigned per the F4/F5/F6/F8/
//! F11/F12 findings in docs/reviews/2026-07-27-arrow-datafusion-plan-review.md:
//!
//! ```text
//! RsegScanExec (N partitions, each sorted by (series_id, ts, provenance))
//!   -> SortPreservingMergeExec on (series_id, ts)
//!   -> RsegDedupExec (single partition, streaming, full dedup total order)
//!   -> DataFusion operators (later tickets)
//! ```
//!
//! Arrow types are used exclusively through the `datafusion::arrow`
//! re-export so this crate is internally version-consistent regardless of
//! the workspace `arrow` pin.

mod config;
mod dedup;
mod error;
mod labels;
mod memory;
mod provider;
mod pushdown;
mod scan;
mod schema;
mod udf;

pub use config::{DEFAULT_MAX_QUERY_BYTES, SqlConfig};
pub use error::SqlError;
pub use memory::{TenantDelegatingPool, TenantMemoryAccountant};
pub use provider::RavelTableProvider;
pub use pushdown::Pushdown;
pub use schema::{internal_schema, public_schema};
pub use udf::{label_match_udf, label_udf};

/// The internal provenance column names, in scan-output order after the
/// four public columns. Consumed by [`dedup::RsegDedupExec`] and dropped
/// before any public operator sees them.
pub use schema::{
    COL_CREATED_UNIX_NS, COL_IN_PAGE_INDEX, COL_LABELS, COL_SERIES_ID, COL_TS, COL_VALUE,
    COL_WRITER_EPOCH, COL_WRITER_SEQ,
};
