//! Per-query cost accounting and pre-execution cost estimation
//! (ADR-0044, "1. A per-request accounting handle in `ravel-types`" and
//! "3. A pre-execution cost estimate").
//!
//! [`QueryAccounting`] is a cheap-to-clone handle over atomic counters,
//! created once per query and passed explicitly (by reference or by clone)
//! into every component that touches the object store on that query's
//! behalf: the catalog's `guarded_get`/`guarded_list_all`, the segment
//! fetcher, and the log fetcher.
//!
//! # Why a parameter, not a task-local
//!
//! The query path fans out with `join_all` and `buffered`: a catalog resolve
//! or a segment fetch spawns futures that run on tasks the query itself did
//! not create, and those tasks may be reused by the executor for a sibling
//! query's work before or after. A thread-local or task-local accounting
//! context would attribute whatever counter increments happen on a given
//! task to whichever query last installed itself there, silently mixing one
//! query's cost into another's. Passing [`QueryAccounting`] explicitly as a
//! parameter means the counters an increment lands on are exactly the ones
//! named at the call site, independent of which task happens to run it.
//!
//! This mirrors the counting shape in
//! `ravel_object_store::instrument::StoreMetrics`: plain `AtomicU64` fields
//! behind an `Arc`, a `snapshot()` that copies out plain `u64`s, documented
//! as a scrape rather than a consistent cut. The difference is scope:
//! `StoreMetrics` is one process-global instance shared by every caller;
//! `QueryAccounting` is one instance per query, so its counters mean "this
//! query's cost" without needing to subtract anything.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// The store operation kinds a query can be charged for. Distinct from
/// `ravel_object_store::instrument::StoreOp`: this crate does not depend on
/// `ravel-object-store`, and a query only ever issues gets, lists, and
/// heads through its funnels (puts and deletes are never on the query
/// path).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AccountedOp {
    Get,
    List,
    Head,
}

/// Number of [`AccountedOp`] variants; the width of the per-op arrays below.
pub const ACCOUNTED_OP_COUNT: usize = 3;

impl AccountedOp {
    /// Every variant, in `index()` order.
    pub const ALL: [AccountedOp; ACCOUNTED_OP_COUNT] =
        [AccountedOp::Get, AccountedOp::List, AccountedOp::Head];

    /// Dense array index, stable and `< ACCOUNTED_OP_COUNT` by construction.
    pub fn index(self) -> usize {
        match self {
            AccountedOp::Get => 0,
            AccountedOp::List => 1,
            AccountedOp::Head => 2,
        }
    }

    /// Variant name, for labelling an export built on a snapshot.
    pub fn name(self) -> &'static str {
        match self {
            AccountedOp::Get => "get",
            AccountedOp::List => "list",
            AccountedOp::Head => "head",
        }
    }
}

/// One slot per [`AccountedOp`], each a request count and a byte count.
#[derive(Debug, Default)]
struct OpCounters {
    requests: AtomicU64,
    bytes: AtomicU64,
}

/// Shared counter block behind [`QueryAccounting`]'s `Arc`. All fields are
/// atomics so every clone of the handle observes every other clone's
/// increments; see the [module docs](self) for why this is a parameter
/// rather than a task-local.
#[derive(Debug, Default)]
struct Inner {
    ops: [OpCounters; ACCOUNTED_OP_COUNT],
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
    cache_bytes: AtomicU64,
    decompressed_bytes: AtomicU64,
    segments_opened: AtomicU64,
    segments_pruned: AtomicU64,
    series_matched: AtomicU64,
    bytes_reused: AtomicU64,
    /// Recorded as a running maximum (compare-and-swap loop), never a sum;
    /// see [`QueryAccounting::observe_intermediate_bytes`].
    peak_intermediate_bytes: AtomicU64,
}

/// Cheap-to-clone, `Send + Sync` handle over one query's cost counters.
/// Create one per query and clone it into every component that touches the
/// object store on that query's behalf. All clones share the same counters
/// through an inner `Arc`.
///
/// See the [module docs](self) for why this is passed explicitly rather
/// than held in a task-local.
#[derive(Debug, Clone, Default)]
pub struct QueryAccounting(Arc<Inner>);

impl QueryAccounting {
    /// New handle with every counter at zero, for one query.
    pub fn new() -> Self {
        QueryAccounting::default()
    }

    /// Record one completed store request of kind `op`.
    pub fn record_s3_request(&self, op: AccountedOp) {
        self.0.ops[op.index()]
            .requests
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Add bytes transferred by a completed store request of kind `op`.
    pub fn add_s3_bytes(&self, op: AccountedOp, bytes: u64) {
        self.0.ops[op.index()]
            .bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }

    /// Record one in-process cache hit.
    pub fn record_cache_hit(&self) {
        self.0.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record one in-process cache miss.
    pub fn record_cache_miss(&self) {
        self.0.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Add bytes served from an in-process cache.
    pub fn add_cache_bytes(&self, bytes: u64) {
        self.0.cache_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Add bytes produced by decompressing a fetched object.
    pub fn add_decompressed_bytes(&self, bytes: u64) {
        self.0
            .decompressed_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }

    /// Add to the count of segments opened for this query.
    pub fn add_segments_opened(&self, count: u64) {
        self.0.segments_opened.fetch_add(count, Ordering::Relaxed);
    }

    /// Add to the count of segments pruned (skipped without opening) for
    /// this query.
    pub fn add_segments_pruned(&self, count: u64) {
        self.0.segments_pruned.fetch_add(count, Ordering::Relaxed);
    }

    /// Add to the count of series matched for this query.
    pub fn add_series_matched(&self, count: u64) {
        self.0.series_matched.fetch_add(count, Ordering::Relaxed);
    }

    /// Add bytes served from an in-request buffer (e.g. a fetcher's own
    /// already-fetched region) without a second store call.
    pub fn add_bytes_reused(&self, bytes: u64) {
        self.0.bytes_reused.fetch_add(bytes, Ordering::Relaxed);
    }

    /// Update the peak intermediate byte high-water mark. This is a
    /// maximum, never a sum: call it with the current intermediate size at
    /// each point it might grow, and the counter keeps the largest value
    /// seen across the whole query. Named `observe_*` rather than `add_*`
    /// or `record_*` so a caller cannot mistake it for an accumulator.
    pub fn observe_intermediate_bytes(&self, bytes: u64) {
        let mut current = self.0.peak_intermediate_bytes.load(Ordering::Relaxed);
        loop {
            if bytes <= current {
                return;
            }
            match self.0.peak_intermediate_bytes.compare_exchange_weak(
                current,
                bytes,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(observed) => current = observed,
            }
        }
    }

    /// Point-in-time copy of every counter. Not atomic across fields:
    /// concurrent increments may land between two reads, so this is a
    /// scrape, not a consistent cut, mirroring
    /// `StoreMetrics::snapshot`.
    pub fn snapshot(&self) -> QueryAccountingSnapshot {
        let mut s3_requests = [0u64; ACCOUNTED_OP_COUNT];
        let mut s3_bytes = [0u64; ACCOUNTED_OP_COUNT];
        for (i, op) in self.0.ops.iter().enumerate() {
            s3_requests[i] = op.requests.load(Ordering::Relaxed);
            s3_bytes[i] = op.bytes.load(Ordering::Relaxed);
        }
        QueryAccountingSnapshot {
            s3_requests,
            s3_bytes,
            cache_hits: self.0.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.0.cache_misses.load(Ordering::Relaxed),
            cache_bytes: self.0.cache_bytes.load(Ordering::Relaxed),
            decompressed_bytes: self.0.decompressed_bytes.load(Ordering::Relaxed),
            segments_opened: self.0.segments_opened.load(Ordering::Relaxed),
            segments_pruned: self.0.segments_pruned.load(Ordering::Relaxed),
            series_matched: self.0.series_matched.load(Ordering::Relaxed),
            bytes_reused: self.0.bytes_reused.load(Ordering::Relaxed),
            peak_intermediate_bytes: self.0.peak_intermediate_bytes.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time copy of a [`QueryAccounting`]'s counters. Plain `u64`
/// fields and fixed-size arrays, so this is `Copy` and needs no allocation;
/// taking a snapshot and then incrementing the live handle never changes an
/// already-taken snapshot.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct QueryAccountingSnapshot {
    /// Requests issued, by [`AccountedOp::index`].
    pub s3_requests: [u64; ACCOUNTED_OP_COUNT],
    /// Bytes transferred, by [`AccountedOp::index`].
    pub s3_bytes: [u64; ACCOUNTED_OP_COUNT],
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_bytes: u64,
    pub decompressed_bytes: u64,
    pub segments_opened: u64,
    pub segments_pruned: u64,
    pub series_matched: u64,
    pub bytes_reused: u64,
    pub peak_intermediate_bytes: u64,
}

impl QueryAccountingSnapshot {
    /// Requests issued for one operation kind.
    pub fn s3_requests(&self, op: AccountedOp) -> u64 {
        self.s3_requests[op.index()]
    }

    /// Bytes transferred for one operation kind.
    pub fn s3_bytes(&self, op: AccountedOp) -> u64 {
        self.s3_bytes[op.index()]
    }

    /// Requests issued across every operation kind.
    pub fn total_s3_requests(&self) -> u64 {
        self.s3_requests.iter().sum()
    }

    /// Bytes transferred across every operation kind.
    pub fn total_s3_bytes(&self) -> u64 {
        self.s3_bytes.iter().sum()
    }
}

/// Pre-execution cost estimate for a query, computed after `Catalog::resolve`
/// pins a snapshot and before any page fetch (ADR-0044 "3. A pre-execution
/// cost estimate").
///
/// This is an upper envelope, **never a prediction**: where the planner
/// cannot bound a quantity exactly, it must take the worst case rather than
/// a typical or expected case. Under-estimating is the failure mode that
/// matters, not over-estimating, because a later ADR will reject queries
/// whose actual cost diverges too far above this number; a query admitted
/// on a low estimate is exactly the runaway the future limiter exists to
/// stop. `segments` and `series` are the planner inputs the estimate was
/// derived from, kept alongside it so the estimate is auditable against
/// what the planner actually saw.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CostEstimate {
    pub estimated_requests: u64,
    pub estimated_store_bytes: u64,
    pub estimated_decompressed_bytes: u64,
    /// Segments the estimate was derived from.
    pub segments: u64,
    /// Series the estimate was derived from.
    pub series: u64,
}

impl CostEstimate {
    pub fn new(
        estimated_requests: u64,
        estimated_store_bytes: u64,
        estimated_decompressed_bytes: u64,
        segments: u64,
        series: u64,
    ) -> Self {
        CostEstimate {
            estimated_requests,
            estimated_store_bytes,
            estimated_decompressed_bytes,
            segments,
            series,
        }
    }

    /// Ratio of actual to estimated cost, per dimension: `actual /
    /// estimated`. A value above 1.0 means the estimate under-shot, the
    /// failure mode that matters. Dividing by a zero estimate returns
    /// [`f64::INFINITY`] when the actual is non-zero, and `0.0` when both
    /// are zero.
    pub fn divergence(&self, actual: &QueryAccountingSnapshot) -> CostDivergence {
        CostDivergence {
            requests: divergence_ratio(actual.total_s3_requests(), self.estimated_requests),
            store_bytes: divergence_ratio(actual.total_s3_bytes(), self.estimated_store_bytes),
            decompressed_bytes: divergence_ratio(
                actual.decompressed_bytes,
                self.estimated_decompressed_bytes,
            ),
        }
    }
}

fn divergence_ratio(actual: u64, estimated: u64) -> f64 {
    if estimated == 0 {
        if actual == 0 { 0.0 } else { f64::INFINITY }
    } else {
        actual as f64 / estimated as f64
    }
}

/// Actual-over-estimated ratio per dimension, from [`CostEstimate::divergence`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CostDivergence {
    pub requests: f64,
    pub store_bytes: f64,
    pub decompressed_bytes: f64,
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn accounting_clone_shares_counters_and_peak_is_a_max() {
        let original = QueryAccounting::new();
        let clone = original.clone();

        clone.record_s3_request(AccountedOp::Get);
        clone.add_s3_bytes(AccountedOp::Get, 100);
        let snap = original.snapshot();
        assert_eq!(snap.s3_requests(AccountedOp::Get), 1);
        assert_eq!(snap.s3_bytes(AccountedOp::Get), 100);

        original.observe_intermediate_bytes(500);
        original.observe_intermediate_bytes(300);
        original.observe_intermediate_bytes(400);
        let snap = clone.snapshot();
        assert_eq!(
            snap.peak_intermediate_bytes, 500,
            "descending observations after the max must not lower it"
        );
    }

    #[test]
    fn per_operation_counters_do_not_contaminate_each_other() {
        let acc = QueryAccounting::new();
        acc.record_s3_request(AccountedOp::Get);
        acc.add_s3_bytes(AccountedOp::Get, 10);
        acc.record_s3_request(AccountedOp::List);
        acc.record_s3_request(AccountedOp::List);
        acc.add_s3_bytes(AccountedOp::List, 7);

        let snap = acc.snapshot();
        assert_eq!(snap.s3_requests(AccountedOp::Get), 1);
        assert_eq!(snap.s3_bytes(AccountedOp::Get), 10);
        assert_eq!(snap.s3_requests(AccountedOp::List), 2);
        assert_eq!(snap.s3_bytes(AccountedOp::List), 7);
        assert_eq!(snap.s3_requests(AccountedOp::Head), 0);
        assert_eq!(snap.s3_bytes(AccountedOp::Head), 0);

        assert_eq!(snap.total_s3_requests(), 3);
        assert_eq!(snap.total_s3_bytes(), 17);
    }

    #[test]
    fn concurrent_increments_land_exactly_once_each() {
        let acc = QueryAccounting::new();
        let threads: Vec<_> = (0..8)
            .map(|_| {
                let acc = acc.clone();
                thread::spawn(move || {
                    for _ in 0..1_000 {
                        acc.record_s3_request(AccountedOp::Get);
                        acc.add_bytes_reused(3);
                    }
                })
            })
            .collect();
        for t in threads {
            t.join().expect("thread panicked");
        }

        let snap = acc.snapshot();
        assert_eq!(snap.s3_requests(AccountedOp::Get), 8_000);
        assert_eq!(snap.bytes_reused, 24_000);
    }

    #[test]
    fn divergence_handles_zero_denominator_cases() {
        let estimate = CostEstimate::new(0, 0, 0, 1, 1);
        let zero_actual = QueryAccountingSnapshot::default();
        let d = estimate.divergence(&zero_actual);
        assert_eq!(d.requests, 0.0, "zero over zero is zero, not NaN");
        assert_eq!(d.store_bytes, 0.0);
        assert_eq!(d.decompressed_bytes, 0.0);

        let acc = QueryAccounting::new();
        acc.record_s3_request(AccountedOp::Get);
        acc.add_s3_bytes(AccountedOp::Get, 50);
        acc.add_decompressed_bytes(50);
        let d = estimate.divergence(&acc.snapshot());
        assert_eq!(d.requests, f64::INFINITY, "non-zero over zero is infinite");
        assert_eq!(d.store_bytes, f64::INFINITY);
        assert_eq!(d.decompressed_bytes, f64::INFINITY);
    }

    #[test]
    fn divergence_normal_case_is_actual_over_estimated() {
        let estimate = CostEstimate::new(10, 1_000, 500, 4, 4);
        let acc = QueryAccounting::new();
        for _ in 0..5 {
            acc.record_s3_request(AccountedOp::Get);
        }
        acc.add_s3_bytes(AccountedOp::Get, 500);
        acc.add_decompressed_bytes(250);

        let d = estimate.divergence(&acc.snapshot());
        assert_eq!(d.requests, 0.5);
        assert_eq!(d.store_bytes, 0.5);
        assert_eq!(d.decompressed_bytes, 0.5);
    }

    #[test]
    fn snapshot_is_a_value_type_unaffected_by_later_increments() {
        let acc = QueryAccounting::new();
        acc.record_s3_request(AccountedOp::Get);
        acc.add_cache_bytes(42);
        let snap = acc.snapshot();

        acc.record_s3_request(AccountedOp::Get);
        acc.add_cache_bytes(1_000);
        acc.observe_intermediate_bytes(999);

        assert_eq!(snap.s3_requests(AccountedOp::Get), 1);
        assert_eq!(snap.cache_bytes, 42);
        assert_eq!(snap.peak_intermediate_bytes, 0);
    }
}
