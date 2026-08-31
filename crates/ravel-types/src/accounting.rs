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

use crate::TenantHash;

/// Adds `add` to an atomic counter, saturating at `u64::MAX` instead of
/// wrapping. A plain `fetch_add` wraps silently even under `overflow-checks`
/// (atomics are not the checked arithmetic operators), which would let a
/// wrapped-low aggregate slip under a bytes-scanned budget the coordinator
/// re-enforces; this clamps instead. The compare-exchange loop retries only
/// under contention, and never past the clamp.
fn saturating_fetch_add(counter: &AtomicU64, add: u64) {
    let mut current = counter.load(Ordering::Relaxed);
    loop {
        let next = current.saturating_add(add);
        if next == current {
            return;
        }
        match counter.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed) {
            Ok(_) => return,
            Err(observed) => current = observed,
        }
    }
}

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
    /// Stored bytes of pages present in the RLOG blocks a logs scan decoded,
    /// regardless of column filtering. Decode-time column-filtering
    /// measurement, NOT wire bytes; see `ops`'s `s3_bytes` for wire bytes
    /// (ADR-0107 decision 4).
    ///
    /// Its name predates ADR-0699 and it is no longer a fetch figure on a
    /// version-4 object: the column-chunk fetcher brings only the projected
    /// columns' pages, so a page counted here may never have crossed the wire.
    /// The gap to `page_bytes_decoded` is then what a whole-block read WOULD
    /// have cost, which is the saving the chunk path made. The bytes actually
    /// moved are `s3_bytes`, on every version.
    page_bytes_fetched: AtomicU64,
    /// Stored bytes of the pages a logs scan actually decoded after column
    /// filtering. Decode-time column-filtering measurement, NOT wire bytes; see
    /// `ops`'s `s3_bytes` for wire bytes (ADR-0107 decision 4).
    page_bytes_decoded: AtomicU64,
    /// Logs-scan segment opens that read the whole object in one GET, tallied
    /// per segment per query (ADR-0904 decision 5). See the snapshot field of
    /// the same name for why cache-hit-served opens count identically.
    logs_whole_object_opens: AtomicU64,
    /// Logs-scan segment opens that read the object by column-chunk ranges
    /// instead of one whole-object GET (ADR-0904 decision 5).
    logs_ranged_opens: AtomicU64,
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
    ///
    /// This is a **logical-call** count, not a billed-request count: it fires
    /// once per store operation the query funnels through, so the `object_store`
    /// retry loop *below* the object-store trait boundary (default
    /// `max_retries = 10`) is invisible here --- one throttled `get` that retried
    /// nine times is one request here while the provider bills ten (issue #928).
    /// The billed count is measured process-globally as `attempts` on
    /// `ravel_object_store::instrument::StoreMetrics` (surfaced as
    /// `ravel_store_attempts_total`), filled in by the S3 adapter's counting HTTP
    /// connector. It is deliberately *not* mirrored per query here: the connector
    /// runs below this crate and holds no [`QueryAccounting`] handle (this type
    /// is passed explicitly, never via a task-local; see the [module docs](self)),
    /// so a per-query billed count is not reachable from the layer that sees the
    /// retries. A budget that must bound billed rather than logical requests
    /// therefore reads the process-global figure, or needs the attempt count
    /// threaded up through the fetch return path first.
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

    /// Add stored bytes of pages present in the RLOG blocks a logs scan
    /// decoded, regardless of column filtering (the "fetched" side of ADR-0107
    /// decision 4's decode-time measurement). This is NOT wire-byte accounting:
    /// actual bytes moved over the wire stay measured by [`Self::add_s3_bytes`].
    /// Paired with [`Self::add_page_bytes_decoded`], the two expose how much of
    /// each fetched block a narrow projection throws away at decode.
    ///
    /// On a version-4 object (ADR-0699 decision 5) the unselected pages counted
    /// here were never fetched at all; see the field's own documentation.
    pub fn add_page_bytes_fetched(&self, bytes: u64) {
        self.0
            .page_bytes_fetched
            .fetch_add(bytes, Ordering::Relaxed);
    }

    /// Add stored bytes of the pages a logs scan actually decoded after column
    /// filtering (the "decoded" side of ADR-0107 decision 4). NOT wire bytes;
    /// see [`Self::add_s3_bytes`] for wire bytes.
    pub fn add_page_bytes_decoded(&self, bytes: u64) {
        self.0
            .page_bytes_decoded
            .fetch_add(bytes, Ordering::Relaxed);
    }

    /// Add to the count of logs-scan segment opens that took the whole-object
    /// read shape (one GET), tallied per segment per query. Recorded by the log
    /// fetcher (ADR-0904 task 904-4); this handle only accumulates. Paired with
    /// [`Self::add_logs_ranged_opens`], the two are the exact evidence of which
    /// read route the request-cost knob selected.
    pub fn add_logs_whole_object_opens(&self, count: u64) {
        self.0
            .logs_whole_object_opens
            .fetch_add(count, Ordering::Relaxed);
    }

    /// Add to the count of logs-scan segment opens that took the ranged
    /// (column-chunk) read shape instead of one whole-object GET. See
    /// [`Self::add_logs_whole_object_opens`].
    pub fn add_logs_ranged_opens(&self, count: u64) {
        self.0.logs_ranged_opens.fetch_add(count, Ordering::Relaxed);
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

    /// Folds a cost snapshot reported by another process into this handle
    /// (ADR-0071 distributed read fan-out): the coordinator calls this with
    /// each slice worker's returned [`QueryAccountingSnapshot`] so the query's
    /// aggregate cost reflects the remote fetches it dispatched. Every counter
    /// accumulates **saturating** at `u64::MAX`; `peak_intermediate_bytes`
    /// merges through [`Self::observe_intermediate_bytes`] (a max), never a
    /// sum, because it is a per-worker-process high-water mark, not a total any
    /// single process ever held. This is the live-handle counterpart of
    /// [`QueryAccountingSnapshot::saturating_merge`], and clamps identically: a
    /// worker reporting a counter near `u64::MAX` pins the aggregate at the
    /// maximum rather than wrapping it (a wrapped low value would silently slip
    /// under a bytes-scanned budget the coordinator re-enforces).
    pub fn merge_snapshot(&self, other: &QueryAccountingSnapshot) {
        for op in AccountedOp::ALL {
            saturating_fetch_add(&self.0.ops[op.index()].requests, other.s3_requests(op));
            saturating_fetch_add(&self.0.ops[op.index()].bytes, other.s3_bytes(op));
        }
        saturating_fetch_add(&self.0.cache_hits, other.cache_hits);
        saturating_fetch_add(&self.0.cache_misses, other.cache_misses);
        saturating_fetch_add(&self.0.cache_bytes, other.cache_bytes);
        saturating_fetch_add(&self.0.decompressed_bytes, other.decompressed_bytes);
        saturating_fetch_add(&self.0.segments_opened, other.segments_opened);
        saturating_fetch_add(&self.0.segments_pruned, other.segments_pruned);
        saturating_fetch_add(&self.0.series_matched, other.series_matched);
        saturating_fetch_add(&self.0.bytes_reused, other.bytes_reused);
        saturating_fetch_add(&self.0.page_bytes_fetched, other.page_bytes_fetched);
        saturating_fetch_add(&self.0.page_bytes_decoded, other.page_bytes_decoded);
        saturating_fetch_add(
            &self.0.logs_whole_object_opens,
            other.logs_whole_object_opens,
        );
        saturating_fetch_add(&self.0.logs_ranged_opens, other.logs_ranged_opens);
        self.observe_intermediate_bytes(other.peak_intermediate_bytes);
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
            page_bytes_fetched: self.0.page_bytes_fetched.load(Ordering::Relaxed),
            page_bytes_decoded: self.0.page_bytes_decoded.load(Ordering::Relaxed),
            logs_whole_object_opens: self.0.logs_whole_object_opens.load(Ordering::Relaxed),
            logs_ranged_opens: self.0.logs_ranged_opens.load(Ordering::Relaxed),
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
    /// Stored bytes of pages present in the RLOG blocks a logs scan decoded,
    /// regardless of column filtering. A decode-time column-filtering
    /// measurement, NOT wire bytes; see [`Self::s3_bytes`] for wire bytes
    /// (ADR-0107 decision 4).
    pub page_bytes_fetched: u64,
    /// Stored bytes of the pages a logs scan actually decoded after column
    /// filtering. Equal to `page_bytes_fetched` for an all-columns scan; the
    /// gap to it is the column-filtering waste. NOT wire bytes; see
    /// [`Self::s3_bytes`] for wire bytes (ADR-0107 decision 4).
    pub page_bytes_decoded: u64,
    /// Logs-scan segment opens that read the whole object in one GET, tallied
    /// per segment per query (ADR-0904 decision 5). The request-cost knob
    /// selects one of two read shapes per open; this and
    /// [`Self::logs_ranged_opens`] are the exact, per-query evidence of which
    /// route ran, so a benchmark proves routing rather than inferring it from
    /// the configured value. A cache-hit-served open counts the same as a live
    /// one: the shape is a property of the entry point the object takes, not of
    /// whether its bytes were already resident.
    pub logs_whole_object_opens: u64,
    /// Logs-scan segment opens that read the object by column-chunk ranges
    /// instead of one whole-object GET (ADR-0904 decision 5). See
    /// [`Self::logs_whole_object_opens`].
    pub logs_ranged_opens: u64,
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

    /// Requests issued across every operation kind. Saturating: a coordinator
    /// folds several workers' saturated snapshots (ADR-0071), so a plain sum
    /// could overflow and panic under `overflow-checks`; the total pins at
    /// `u64::MAX` instead.
    pub fn total_s3_requests(&self) -> u64 {
        self.s3_requests
            .iter()
            .copied()
            .fold(0u64, u64::saturating_add)
    }

    /// Bytes transferred across every operation kind. Saturating, for the same
    /// reason as [`Self::total_s3_requests`]: this is the value the
    /// bytes-scanned budget is checked against, so it must never wrap.
    pub fn total_s3_bytes(&self) -> u64 {
        self.s3_bytes
            .iter()
            .copied()
            .fold(0u64, u64::saturating_add)
    }

    /// Field-wise saturating sum of two snapshots, for one request that runs
    /// several independently-accounted sub-queries and must report one
    /// combined cost. The `/api/v1/series` and `/api/v1/labels` endpoints are
    /// the case that needs it: each `match[]` selector resolves under its own
    /// [`QueryAccounting`] handle, so the request's total cost is the sum of
    /// the per-selector snapshots. Every accumulating counter adds, but
    /// `peak_intermediate_bytes` takes the maximum, not the sum: it is a
    /// high-water mark, so the peak across sub-queries is the larger of their
    /// peaks and never their total (the same rule
    /// [`QueryAccounting::observe_intermediate_bytes`] keeps within one query).
    pub fn saturating_add(&self, other: &QueryAccountingSnapshot) -> QueryAccountingSnapshot {
        let mut s3_requests = [0u64; ACCOUNTED_OP_COUNT];
        let mut s3_bytes = [0u64; ACCOUNTED_OP_COUNT];
        for i in 0..ACCOUNTED_OP_COUNT {
            s3_requests[i] = self.s3_requests[i].saturating_add(other.s3_requests[i]);
            s3_bytes[i] = self.s3_bytes[i].saturating_add(other.s3_bytes[i]);
        }
        QueryAccountingSnapshot {
            s3_requests,
            s3_bytes,
            cache_hits: self.cache_hits.saturating_add(other.cache_hits),
            cache_misses: self.cache_misses.saturating_add(other.cache_misses),
            cache_bytes: self.cache_bytes.saturating_add(other.cache_bytes),
            decompressed_bytes: self
                .decompressed_bytes
                .saturating_add(other.decompressed_bytes),
            segments_opened: self.segments_opened.saturating_add(other.segments_opened),
            segments_pruned: self.segments_pruned.saturating_add(other.segments_pruned),
            series_matched: self.series_matched.saturating_add(other.series_matched),
            bytes_reused: self.bytes_reused.saturating_add(other.bytes_reused),
            page_bytes_fetched: self
                .page_bytes_fetched
                .saturating_add(other.page_bytes_fetched),
            page_bytes_decoded: self
                .page_bytes_decoded
                .saturating_add(other.page_bytes_decoded),
            logs_whole_object_opens: self
                .logs_whole_object_opens
                .saturating_add(other.logs_whole_object_opens),
            logs_ranged_opens: self
                .logs_ranged_opens
                .saturating_add(other.logs_ranged_opens),
            peak_intermediate_bytes: self
                .peak_intermediate_bytes
                .max(other.peak_intermediate_bytes),
        }
    }

    /// Field-wise combine of a coordinator's per-slice accounting snapshots
    /// into one query total (ADR-0071 distributed read fan-out). Each slice
    /// runs the existing fetch path over a disjoint segment set under its own
    /// [`QueryAccounting`] handle and returns a snapshot in its terminal
    /// summary frame; the coordinator merges them to report the whole query's
    /// cost, exactly as it merges per-selector snapshots locally.
    ///
    /// The per-field semantics are identical to [`saturating_add`], and this
    /// delegates to it so the two can never drift: every accumulating counter
    /// adds saturating (the slices touched disjoint segments, so their request,
    /// byte, and matched counts genuinely sum), while `peak_intermediate_bytes`
    /// takes the maximum, never the sum. The peak is a per-process high-water
    /// mark of memory held at one instant; slices run in separate worker
    /// processes, so the query's peak is the largest single slice's peak, not
    /// the total that never coexisted. Summing it would fabricate a peak no
    /// process ever reached.
    ///
    /// [`saturating_add`]: QueryAccountingSnapshot::saturating_add
    pub fn saturating_merge(&self, other: &QueryAccountingSnapshot) -> QueryAccountingSnapshot {
        self.saturating_add(other)
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

    /// Field-wise saturating sum of two estimates, for one request whose cost
    /// is the total of several independently-estimated sub-queries (the
    /// multi-`match[]` metadata endpoints; see
    /// [`QueryAccountingSnapshot::saturating_add`]). The upper envelope of a
    /// request that runs N selectors is the sum of the N per-selector
    /// envelopes, so every field adds.
    pub fn saturating_add(&self, other: &CostEstimate) -> CostEstimate {
        CostEstimate {
            estimated_requests: self
                .estimated_requests
                .saturating_add(other.estimated_requests),
            estimated_store_bytes: self
                .estimated_store_bytes
                .saturating_add(other.estimated_store_bytes),
            estimated_decompressed_bytes: self
                .estimated_decompressed_bytes
                .saturating_add(other.estimated_decompressed_bytes),
            segments: self.segments.saturating_add(other.segments),
            series: self.series.saturating_add(other.series),
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

/// How a completed query reached the engine, carried to a
/// [`QueryCostRecorder`] so an aggregate can dimension cost by workload
/// (ADR-0044 section 4, the `workload_class` label). A closed set:
/// `Interactive` is a client-driven query (an HTTP or Flight SQL request);
/// `Background` is an internally scheduled one (alert-rule evaluation). It
/// lives here, not in the service crate that renders the label, for the same
/// dependency reason the recorder trait does: `ravel-query` and `ravel-sql`
/// stamp it at the call site and neither can name a `services/ravel-server`
/// type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueryWorkloadClass {
    Interactive,
    Background,
}

/// Sink for one completed query's cost, to be folded into a process-global
/// aggregate exported at `/metrics` (ADR-0044 section 4). The query path owns
/// a [`QueryAccounting`] per query and, once the response is built, hands its
/// finished [`snapshot`](QueryAccounting::snapshot) here, so the same numbers
/// the response reports also reach the scrape.
///
/// The seam lives in `ravel-types` for the same dependency reason
/// [`QueryAccounting`] itself does: `ravel-query` and `ravel-sql` produce the
/// accounting but cannot depend on the `services/ravel-server` crate that owns
/// the aggregator, so the two sides meet at this trait. A caller holds an
/// `Arc<dyn QueryCostRecorder>`; a deployment sets the real aggregator and a
/// test or a library-only embedding uses [`NoopQueryCostRecorder`], so no call
/// site needs an `Option` branch.
///
/// # An implementation must be cheap and must not block
///
/// [`record`](Self::record) runs on the request path, synchronously, after the
/// response has been built but before the handler returns it. It is called
/// once per query, never per store call, so it is off the hot per-request-I/O
/// path; but it is still on the query's own tail latency. An implementation
/// must do bounded, non-blocking work only: fold the counters (for example
/// under a short mutex) and return. It must not perform I/O, `await`, sleep, or
/// hold a lock across an await, because any stall here stalls the response the
/// client is waiting on.
pub trait QueryCostRecorder: Send + Sync {
    /// Fold one finished query's actual `accounting` and its pre-execution
    /// `estimate` into the aggregate, attributed to `tenant_hash` and
    /// `workload_class`. Returns nothing: recording never changes the query's
    /// result and a caller never inspects an outcome.
    fn record(
        &self,
        accounting: &QueryAccountingSnapshot,
        estimate: &CostEstimate,
        tenant_hash: TenantHash,
        workload_class: QueryWorkloadClass,
    );
}

/// A [`QueryCostRecorder`] that discards every recording. It lets a query path
/// with no aggregator configured (every test, and any library-only embedding
/// of the routers) hold a recorder unconditionally instead of an `Option`, so
/// each call site records without a branch. Every method is a no-op the
/// optimizer can see through.
#[derive(Debug, Clone, Copy, Default)]
pub struct NoopQueryCostRecorder;

impl QueryCostRecorder for NoopQueryCostRecorder {
    fn record(
        &self,
        _accounting: &QueryAccountingSnapshot,
        _estimate: &CostEstimate,
        _tenant_hash: TenantHash,
        _workload_class: QueryWorkloadClass,
    ) {
    }
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
    fn snapshot_saturating_add_sums_counters_but_maxes_the_peak() {
        let a = QueryAccounting::new();
        a.record_s3_request(AccountedOp::Get);
        a.add_s3_bytes(AccountedOp::Get, 100);
        a.record_cache_hit();
        a.observe_intermediate_bytes(500);
        let b = QueryAccounting::new();
        b.record_s3_request(AccountedOp::Get);
        b.record_s3_request(AccountedOp::List);
        b.add_s3_bytes(AccountedOp::Get, 40);
        b.observe_intermediate_bytes(300);

        let sum = a.snapshot().saturating_add(&b.snapshot());
        assert_eq!(sum.s3_requests(AccountedOp::Get), 2, "get requests add");
        assert_eq!(sum.s3_requests(AccountedOp::List), 1, "list requests add");
        assert_eq!(sum.s3_bytes(AccountedOp::Get), 140, "get bytes add");
        assert_eq!(sum.cache_hits, 1);
        assert_eq!(
            sum.peak_intermediate_bytes, 500,
            "peak is the max of the two peaks, never their sum"
        );
    }

    #[test]
    fn snapshot_saturating_merge_sums_counters_but_maxes_the_peak() {
        // Two per-slice snapshots as a coordinator would receive them: disjoint
        // segment work, so counters sum, but the peak is a per-process
        // high-water mark that must never be summed (ADR-0071 merge rule).
        let a = QueryAccounting::new();
        a.record_s3_request(AccountedOp::Get);
        a.record_s3_request(AccountedOp::Head);
        a.add_s3_bytes(AccountedOp::Get, 100);
        a.add_decompressed_bytes(70);
        a.observe_intermediate_bytes(300);
        let b = QueryAccounting::new();
        b.record_s3_request(AccountedOp::Get);
        b.add_s3_bytes(AccountedOp::Get, 40);
        b.add_decompressed_bytes(30);
        b.observe_intermediate_bytes(900);

        let merged = a.snapshot().saturating_merge(&b.snapshot());
        assert_eq!(merged.s3_requests(AccountedOp::Get), 2, "get requests sum");
        assert_eq!(
            merged.s3_requests(AccountedOp::Head),
            1,
            "head requests sum"
        );
        assert_eq!(merged.s3_bytes(AccountedOp::Get), 140, "get bytes sum");
        assert_eq!(merged.decompressed_bytes, 100, "decompressed bytes sum");
        assert_eq!(
            merged.peak_intermediate_bytes, 900,
            "peak is the max of the two slices' peaks, never their sum"
        );
    }

    #[test]
    fn snapshot_saturating_merge_matches_saturating_add_field_for_field() {
        // The coordinator merge and the per-selector sum share one field math
        // by contract; delegating keeps them from drifting. Prove they agree
        // on a snapshot exercising every counter and a large peak.
        let a = QueryAccounting::new();
        a.record_s3_request(AccountedOp::List);
        a.add_s3_bytes(AccountedOp::List, 11);
        a.record_cache_hit();
        a.record_cache_miss();
        a.add_cache_bytes(5);
        a.add_bytes_reused(9);
        a.observe_intermediate_bytes(1_234);
        let b = QueryAccounting::new();
        b.record_s3_request(AccountedOp::Head);
        b.add_s3_bytes(AccountedOp::Head, 3);
        b.record_cache_hit();
        b.add_cache_bytes(7);
        b.observe_intermediate_bytes(4_321);

        let sa = a.snapshot();
        let sb = b.snapshot();
        assert_eq!(sa.saturating_merge(&sb), sa.saturating_add(&sb));
    }

    #[test]
    fn merge_snapshot_folds_slice_snapshots_into_a_live_handle() {
        // The coordinator's live handle after folding several slice snapshots:
        // counters accumulate across every slice, and the peak is the max of
        // the folded peaks (and any the handle already held), never a sum.
        let agg = QueryAccounting::new();
        agg.observe_intermediate_bytes(120); // a peak the handle already holds

        let s1 = QueryAccounting::new();
        s1.record_s3_request(AccountedOp::Get);
        s1.add_s3_bytes(AccountedOp::Get, 100);
        s1.add_series_matched(3);
        s1.observe_intermediate_bytes(200);

        let s2 = QueryAccounting::new();
        s2.record_s3_request(AccountedOp::Get);
        s2.record_s3_request(AccountedOp::List);
        s2.add_s3_bytes(AccountedOp::Get, 40);
        s2.add_series_matched(5);
        s2.observe_intermediate_bytes(90);

        agg.merge_snapshot(&s1.snapshot());
        agg.merge_snapshot(&s2.snapshot());

        let merged = agg.snapshot();
        assert_eq!(merged.s3_requests(AccountedOp::Get), 2, "get requests sum");
        assert_eq!(
            merged.s3_requests(AccountedOp::List),
            1,
            "list requests sum"
        );
        assert_eq!(merged.s3_bytes(AccountedOp::Get), 140, "get bytes sum");
        assert_eq!(merged.series_matched, 8, "series matched sum across slices");
        assert_eq!(
            merged.peak_intermediate_bytes, 200,
            "peak is the max across folded slices and the handle's own, never a sum"
        );
    }

    #[test]
    fn merge_snapshot_saturates_and_total_does_not_wrap_or_panic() {
        // A worker reporting a byte counter near u64::MAX must clamp the live
        // handle at the maximum, never wrap it down to a small value that
        // slips under a bytes-scanned budget (ADR-0071 coordinator
        // re-enforcement). And total_s3_bytes must sum the clamped per-op
        // fields without panicking under overflow-checks.
        let agg = QueryAccounting::new();
        let near_max = QueryAccountingSnapshot {
            s3_bytes: [u64::MAX - 5, 0, 0],
            ..QueryAccountingSnapshot::default()
        };
        let more = QueryAccountingSnapshot {
            s3_bytes: [100, u64::MAX, 0],
            ..QueryAccountingSnapshot::default()
        };
        agg.merge_snapshot(&near_max);
        agg.merge_snapshot(&more);
        let merged = agg.snapshot();
        assert_eq!(
            merged.s3_bytes(AccountedOp::Get),
            u64::MAX,
            "get bytes clamp at u64::MAX rather than wrapping to a small value"
        );
        assert_eq!(
            merged.total_s3_bytes(),
            u64::MAX,
            "the budget-checked total saturates instead of panicking on overflow"
        );
    }

    #[test]
    fn snapshot_saturating_merge_counters_saturate_at_u64_max() {
        let a = QueryAccountingSnapshot {
            bytes_reused: u64::MAX,
            ..QueryAccountingSnapshot::default()
        };
        let b = QueryAccountingSnapshot {
            bytes_reused: 10,
            ..QueryAccountingSnapshot::default()
        };
        let merged = a.saturating_merge(&b);
        assert_eq!(
            merged.bytes_reused,
            u64::MAX,
            "a lying/overflowing slice cannot wrap the coordinator total"
        );
    }

    #[test]
    fn estimate_saturating_add_sums_every_field() {
        let a = CostEstimate::new(3, 100, 50, 2, 4);
        let b = CostEstimate::new(1, 20, 5, 1, 3);
        let sum = a.saturating_add(&b);
        assert_eq!(sum.estimated_requests, 4);
        assert_eq!(sum.estimated_store_bytes, 120);
        assert_eq!(sum.estimated_decompressed_bytes, 55);
        assert_eq!(sum.segments, 3);
        assert_eq!(sum.series, 7);
    }

    #[test]
    fn noop_recorder_is_object_safe_and_records_nothing_observable() {
        // Holding the recorder as a trait object is the object-safety check:
        // the whole seam exists to be an `Arc<dyn QueryCostRecorder>`.
        let recorder: Arc<dyn QueryCostRecorder> = Arc::new(NoopQueryCostRecorder);
        let acc = QueryAccounting::new();
        acc.record_s3_request(AccountedOp::Get);
        // The no-op consumes the snapshot and returns; there is nothing to
        // assert but that this compiles and does not panic. The value is the
        // object-safe call itself.
        recorder.record(
            &acc.snapshot(),
            &CostEstimate::new(1, 2, 3, 4, 5),
            TenantHash([7u8; 16]),
            QueryWorkloadClass::Interactive,
        );
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

    #[test]
    fn logs_opens_by_shape_snapshot_round_trip_is_exact() {
        // Both counters are the routing evidence a differential test asserts on
        // (ADR-0904 task 904-4), so they must be exact, never indicative. A
        // fresh handle reads exactly zero for both; after recording, each reads
        // exactly what was added and the two do not contaminate each other.
        let fresh = QueryAccounting::new().snapshot();
        assert_eq!(fresh.logs_whole_object_opens, 0);
        assert_eq!(fresh.logs_ranged_opens, 0);

        let acc = QueryAccounting::new();
        acc.add_logs_whole_object_opens(3);
        acc.add_logs_ranged_opens(5);

        let snap = acc.snapshot();
        assert_eq!(snap.logs_whole_object_opens, 3, "whole-object opens exact");
        assert_eq!(snap.logs_ranged_opens, 5, "ranged opens exact");
    }

    #[test]
    fn merge_snapshot_sums_logs_opens_by_shape_exactly() {
        // Two slice snapshots folded into a live handle: each opens-by-shape
        // counter is the exact sum of the two slices' values.
        let agg = QueryAccounting::new();
        let s1 = QueryAccounting::new();
        s1.add_logs_whole_object_opens(2);
        s1.add_logs_ranged_opens(7);
        let s2 = QueryAccounting::new();
        s2.add_logs_whole_object_opens(4);
        s2.add_logs_ranged_opens(1);

        agg.merge_snapshot(&s1.snapshot());
        agg.merge_snapshot(&s2.snapshot());

        let merged = agg.snapshot();
        assert_eq!(merged.logs_whole_object_opens, 6, "whole-object opens sum");
        assert_eq!(merged.logs_ranged_opens, 8, "ranged opens sum");
    }

    #[test]
    fn snapshot_saturating_add_sums_logs_opens_by_shape_exactly() {
        // The per-selector sum path (saturating_add, which saturating_merge
        // delegates to) sums each opens-by-shape counter field-for-field.
        let a = QueryAccounting::new();
        a.add_logs_whole_object_opens(10);
        a.add_logs_ranged_opens(20);
        let b = QueryAccounting::new();
        b.add_logs_whole_object_opens(1);
        b.add_logs_ranged_opens(2);

        let sum = a.snapshot().saturating_add(&b.snapshot());
        assert_eq!(sum.logs_whole_object_opens, 11, "whole-object opens add");
        assert_eq!(sum.logs_ranged_opens, 22, "ranged opens add");
        // The same math backs the coordinator merge (delegates to saturating_add).
        let merged = a.snapshot().saturating_merge(&b.snapshot());
        assert_eq!(merged.logs_whole_object_opens, 11);
        assert_eq!(merged.logs_ranged_opens, 22);
    }
}
