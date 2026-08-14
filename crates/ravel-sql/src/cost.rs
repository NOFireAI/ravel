//! Two-part pre-execution cost estimate for SQL queries (ADR-0044 "3. A
//! pre-execution cost estimate", amended 2026-08-02).
//!
//! The estimate is an upper envelope, never a prediction: under-estimating
//! is the failure mode a later ADR's admission control will act on, so
//! every term here takes the worst case rather than a typical case.
//!
//! The catalog term (`Catalog::estimated_catalog_requests`, computed by the
//! caller before `resolve` runs) and the segment term (computed here, after
//! `resolve` returns a pinned snapshot) are combined by the caller into one
//! [`CostEstimate`]. The per-segment constants mirror
//! `ravel-query/src/engine.rs`'s private `estimate_cost`, since that
//! function is not public and this crate must not add a new entry point to
//! `ravel-query`.

use ravel_catalog::{SegmentRef, Snapshot};
use ravel_segment::ReaderLimits;
use ravel_types::accounting::CostEstimate;

/// Store GETs a metrics segment open can issue: one for the footer/index
/// section, one for a page that lands outside it. Mirrors
/// `ravel-query/src/engine.rs`'s `OPEN_REQUESTS_PER_SEGMENT`.
const OPEN_REQUESTS_PER_SEGMENT: u64 = 2;

/// Catalog-side requests a metrics segment's own resolution can add beyond
/// the window-level catalog term (per-segment postings/name lookups).
/// Mirrors `ravel-query/src/engine.rs`'s `CATALOG_REQUESTS_PER_SEGMENT`.
const CATALOG_REQUESTS_PER_SEGMENT: u64 = 4;

/// Page requests per matched series run. Mirrors `ravel-query/src/engine.rs`'s
/// `PAGE_REQUESTS_PER_RUN`.
const PAGE_REQUESTS_PER_RUN: u64 = 2;

/// Safety factor applied to a metrics segment's `object_size` to bound
/// store bytes: a segment can be opened, miss its first range guess, and be
/// re-read. Mirrors `ravel-query/src/engine.rs`'s `STORE_BYTES_SAFETY_FACTOR`.
const STORE_BYTES_SAFETY_FACTOR: u64 = 2;

/// GETs `LogSegmentFetcher::fetch_accounted` issues per relevant log
/// segment: always exactly one (`GetRange::Full`, no chase/retry path), so
/// this is exact rather than a bound.
const LOG_REQUESTS_PER_SEGMENT: u64 = 1;

/// GETs `SpanSegmentFetcher::fetch_accounted` issues per relevant span
/// segment: always exactly one (`GetRange::Full`, no chase/retry path), so
/// this is exact rather than a bound. Mirrors [`LOG_REQUESTS_PER_SEGMENT`].
const SPAN_REQUESTS_PER_SEGMENT: u64 = 1;

/// Upper bound on the decompressed bytes one metrics segment can produce:
/// every sample scaled to the largest page a reader will decompress, capped
/// by the largest section a reader will decompress twice over. Mirrors
/// `ravel-query/src/engine.rs`'s `segment_decompressed_bytes_upper_bound`.
fn segment_decompressed_bytes_upper_bound(seg: &SegmentRef, limits: &ReaderLimits) -> u64 {
    let per_sample_scaled = seg
        .sample_count
        .saturating_mul(limits.max_page_uncompressed_bytes);
    let section_capped = limits.max_section_uncompressed_bytes.saturating_mul(2);
    per_sample_scaled.min(section_capped)
}

/// Cost estimate for a metrics-target SQL query, given the pinned snapshot
/// and the catalog term computed before `resolve` ran.
///
/// SQL never fans out per-selector the way PromQL's `prefetch` can, so
/// unlike `engine.rs`'s `estimate_cost` there is no `fetch_multiplier`: a
/// segment is fetched once regardless of how many columns a `SELECT`
/// projects out of it.
pub fn estimate_metrics_cost(snapshot: &Snapshot, catalog_requests: u64) -> CostEstimate {
    let segments = snapshot.segments.len() as u64;
    let series: u64 = snapshot.segments.iter().map(|s| s.series_count).sum();
    let limits = ReaderLimits::default();

    let mut estimated_requests =
        catalog_requests + segments * (OPEN_REQUESTS_PER_SEGMENT + CATALOG_REQUESTS_PER_SEGMENT);
    let mut estimated_store_bytes: u64 = 0;
    let mut estimated_decompressed_bytes: u64 = 0;
    for seg in &snapshot.segments {
        estimated_requests += seg.series_count.saturating_mul(PAGE_REQUESTS_PER_RUN);
        estimated_store_bytes = estimated_store_bytes
            .saturating_add(seg.object_size.saturating_mul(STORE_BYTES_SAFETY_FACTOR));
        estimated_decompressed_bytes = estimated_decompressed_bytes
            .saturating_add(segment_decompressed_bytes_upper_bound(seg, &limits));
    }

    CostEstimate::new(
        estimated_requests,
        estimated_store_bytes,
        estimated_decompressed_bytes,
        segments,
        series,
    )
}

/// Cost estimate for a logs-target SQL query, given the pinned snapshot and
/// the catalog term computed before `resolve` ran.
///
/// The log fetch funnel (`LogSegmentFetcher::fetch_accounted`) issues
/// exactly one GET per relevant segment and never decompresses on a
/// separately-accounted path (it never calls `add_decompressed_bytes`), so
/// both the request count and the byte sums here are exact bounds rather
/// than padded guesses: no chase/retry safety factor applies because there
/// is no chase/retry path to guard against.
pub fn estimate_logs_cost(snapshot: &Snapshot, catalog_requests: u64) -> CostEstimate {
    let segments = snapshot.segments.len() as u64;
    let series: u64 = snapshot.segments.iter().map(|s| s.series_count).sum();

    let estimated_requests = catalog_requests + segments * LOG_REQUESTS_PER_SEGMENT;
    let estimated_store_bytes: u64 = snapshot
        .segments
        .iter()
        .map(|seg| seg.object_size)
        .fold(0u64, |acc, size| acc.saturating_add(size));

    CostEstimate::new(
        estimated_requests,
        estimated_store_bytes,
        0,
        segments,
        series,
    )
}

/// Cost estimate for a spans-target SQL query, given the pinned snapshot and
/// the catalog term computed before `resolve` ran.
///
/// The span fetch funnel (`SpanSegmentFetcher::fetch_accounted`) issues
/// exactly one GET per relevant segment and never decompresses on a
/// separately-accounted path (it never calls `add_decompressed_bytes`), so
/// both the request count and the byte sums here are exact bounds rather than
/// padded guesses, exactly like [`estimate_logs_cost`]: no chase/retry safety
/// factor applies because there is no chase/retry path to guard against.
///
/// Not yet reached from a production caller: SQL query routing
/// (`executor::TargetSignal`) has no `Spans` arm until the spans query surface
/// is wired in (ADR-0045 decision 5, phase 2). It lands here beside its sibling
/// estimators so that wiring is a one-line `match` arm rather than a new
/// function; `#[allow(dead_code)]` marks it as intentionally caller-less until
/// then.
#[allow(dead_code)]
pub fn estimate_spans_cost(snapshot: &Snapshot, catalog_requests: u64) -> CostEstimate {
    let segments = snapshot.segments.len() as u64;
    let series: u64 = snapshot.segments.iter().map(|s| s.series_count).sum();

    let estimated_requests = catalog_requests + segments * SPAN_REQUESTS_PER_SEGMENT;
    let estimated_store_bytes: u64 = snapshot
        .segments
        .iter()
        .map(|seg| seg.object_size)
        .fold(0u64, |acc, size| acc.saturating_add(size));

    CostEstimate::new(
        estimated_requests,
        estimated_store_bytes,
        0,
        segments,
        series,
    )
}
