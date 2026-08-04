//! Process-global prune-selectivity counters for the logs query path
//! (ADR-0049, issue #511 deliverable 2).
//!
//! The `LogsScanExec` publishes `blocks_total`, `blocks_scanned`, and
//! `blocks_pruned_by_postings` as DataFusion metrics (#544). `ravel-sql` reads
//! those counters off the plan after each query drains and returns them on
//! `SqlOutcome::stats`; the SQL endpoint folds each query's numbers in here,
//! and `/metrics` renders the cumulative totals. Nothing counts a block twice:
//! these are the scan's own counters, summed across queries.
//!
//! Prune selectivity is `blocks_scanned / blocks_total` (blocks surviving over
//! blocks total): 1.0 means nothing was pruned, lower is better. It is exposed
//! as the two cumulative counters plus the pruned-by-postings counter, so a
//! scraper computes the ratio itself over any window rather than reading a
//! pre-averaged gauge.
//!
//! Process-global with a single source and no per-query state, mirroring
//! [`crate::provisioning::shard_count_mismatch_count`] and
//! [`crate::tenancy::v1_unkeyed_adoption_count`]. The counters only ever
//! increase, so a scrape reads a monotone series.

use std::sync::atomic::{AtomicU64, Ordering};

static BLOCKS_TOTAL: AtomicU64 = AtomicU64::new(0);
static BLOCKS_SCANNED: AtomicU64 = AtomicU64::new(0);
static BLOCKS_PRUNED_BY_POSTINGS: AtomicU64 = AtomicU64::new(0);

/// Fold one query's `LogsScanExec` block counters into the process totals. A
/// metrics query, or a logs query whose plan had no scan node, passes all
/// zeros and moves nothing.
pub fn record(blocks_total: u64, blocks_scanned: u64, blocks_pruned_by_postings: u64) {
    BLOCKS_TOTAL.fetch_add(blocks_total, Ordering::Relaxed);
    BLOCKS_SCANNED.fetch_add(blocks_scanned, Ordering::Relaxed);
    BLOCKS_PRUNED_BY_POSTINGS.fetch_add(blocks_pruned_by_postings, Ordering::Relaxed);
}

/// The cumulative `(blocks_total, blocks_scanned, blocks_pruned_by_postings)`
/// for the `/metrics` renderer.
pub fn snapshot() -> (u64, u64, u64) {
    (
        BLOCKS_TOTAL.load(Ordering::Relaxed),
        BLOCKS_SCANNED.load(Ordering::Relaxed),
        BLOCKS_PRUNED_BY_POSTINGS.load(Ordering::Relaxed),
    )
}
