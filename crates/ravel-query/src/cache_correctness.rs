//! Acceptance gate for ADR-0046's read cache.
//!
//! Wires `Cache::with_corruption` into the two ADR-0046 funnels this crate
//! owns -- `SegmentFetcher::guarded_get` (RSEG) and
//! `LogSegmentFetcher::fetch_accounted_with_tenant` (RLOG) -- and proves
//! query correctness never depends on the cache being intact: every query
//! against a cache whose every hit returns deliberately corrupted bytes
//! either fails with a typed error or returns a result bit-identical
//! (including NaN bit patterns) to the same query run with no cache at all.
//!
//! `Catalog::guarded_get` is ADR-0046's third funnel
//! (crates/ravel-catalog/src/catalog.rs), covered elsewhere, so this suite
//! covers RSEG and RLOG only.
//!
//! Known limitation, stated in ADR-0046 decision 4's amendment:
//! `maybe_corrupt` only runs inside `Cache::get`'s hit path, never on the
//! bytes a single-flight leader or follower receives directly. A first-ever
//! fetch (a miss) always sees clean bytes regardless of corruption mode.
//! Every test below therefore issues the same fetch twice against one
//! cache-attached fetcher: the first call populates the cache (a miss, no
//! corruption possible), and the second call is a genuine hit against the
//! now-corrupted entry -- the actual exercise of the gate.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use bytes::Bytes;
use ravel_cache::{Cache, CacheKey, CacheLimits, DiskCache, TieredCache};
use ravel_catalog::{SegmentLevel, SegmentRef};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_promql::LabelMatcher;
use ravel_segment::{
    FooterOutcome, HistogramCounts, HistogramSample, HistogramSpan, HistogramValue, IngestBounds,
    ReaderLimits, ResetHint, SegmentIdentity, SegmentWriter, SeriesInputV3, SeriesValues,
    ValueKind, decode_catalog_v4, open_from_suffix, plan_ranges_v4,
};
use ravel_types::accounting::{AccountedOp, QueryAccounting};
use ravel_types::logstream::log_stream_id;
use ravel_types::{Label, LabelSet, TenantHash};
use uuid::Uuid;

use crate::fetcher::FetchedHistogramSeries;
use crate::{
    BlockRangeFetcher, FetchError, FetchStats, FetchedSeriesSoa, LogFetchError, LogFetchOutput,
    LogQuery, LogSegmentFetcher, SegmentFetcher,
};

const RSEG_TENANT: TenantHash = TenantHash([13u8; 16]);
const RLOG_TENANT: TenantHash = TenantHash([21u8; 16]);

/// Two tenants for the cross-tenant isolation tests below. Chosen to differ
/// in every byte, with no all-zero/all-ones byte and no single-byte-apart or
/// byte-swapped relationship, so a plausible truncation, zero-extension, or
/// byte-order bug in key construction could not make one alias the other.
/// Neither is `TenantHash([7u8; 16])`, the literal every existing `ravel-sql`
/// call site passes, precisely so a hardcoded-tenant provider bug would be
/// caught here rather than pass silently.
const TENANT_A: TenantHash = TenantHash([
    0x5A, 0x3C, 0x91, 0xE7, 0x02, 0xBD, 0x44, 0x68, 0xF1, 0x09, 0xAC, 0x37, 0xD0, 0x6B, 0x8E, 0x25,
]);
const TENANT_B: TenantHash = TenantHash([
    0xC4, 0x71, 0x2F, 0x9A, 0xEB, 0x50, 0x86, 0x1D, 0x3B, 0xF6, 0x0C, 0xA9, 0x47, 0xD2, 0x68, 0xBE,
]);

/// Mirrors `fetcher::coalesce_ranges` (module-private there): merges ranges
/// whose gap is at most `max_gap`, so a test can predict exactly which
/// merged range `ensure_ranges` fetched (and therefore cached) a target byte
/// range under.
fn coalesce_ranges(mut ranges: Vec<(u64, u64)>, max_gap: u64) -> Vec<(u64, u64)> {
    ranges.sort_by_key(|r| r.0);
    let mut out: Vec<(u64, u64)> = Vec::new();
    for (start, end) in ranges {
        if let Some(last) = out.last_mut()
            && start <= last.1.saturating_add(max_gap)
        {
            last.1 = last.1.max(end);
            continue;
        }
        out.push((start, end));
    }
    out
}

// ---- RSEG test segment ------------------------------------------------

fn labels(metric: &str) -> LabelSet {
    LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: metric.to_string(),
    }])
    .expect("valid labels")
}

fn hist_value(count: u64, sum: f64) -> HistogramValue {
    HistogramValue {
        scale: 2,
        zero_threshold: 1e-9,
        sum: Some(sum),
        custom_values: None,
        positive_spans: vec![HistogramSpan {
            offset: 0,
            length: 1,
        }],
        negative_spans: vec![],
        counts: HistogramCounts::Int {
            zero_count: 0,
            count,
            positive: vec![count],
            negative: vec![],
        },
        reset_hint: ResetHint::Unknown,
    }
}

fn hist_series_v3(metric: &str, samples: Vec<HistogramSample>) -> SeriesInputV3 {
    let label_set = labels(metric);
    let tenant_id = ravel_types::TenantId::new("t".to_string());
    let series_id =
        ravel_types::SeriesId::compute(&tenant_id, metric, &label_set).expect("series id");
    SeriesInputV3 {
        series_id,
        labels: label_set,
        values: SeriesValues::Histogram(samples),
    }
}

fn scalar_series_v3(metric: &str, samples: &[(i64, f64)]) -> SeriesInputV3 {
    let label_set = labels(metric);
    let tenant_id = ravel_types::TenantId::new("t".to_string());
    let series_id =
        ravel_types::SeriesId::compute(&tenant_id, metric, &label_set).expect("series id");
    SeriesInputV3 {
        series_id,
        labels: label_set,
        values: SeriesValues::Scalar(
            samples
                .iter()
                .map(|(ts_ns, value)| ravel_types::Sample {
                    ts_ns: *ts_ns,
                    value: *value,
                })
                .collect(),
        ),
    }
}

/// Writes an RSEG segment with one scalar series carrying two
/// maximally-differing samples (0.0 and the all-ones NaN bit pattern, so a
/// bit-identity check on `values` is not vacuous) and one histogram series,
/// puts it on a fresh `MemoryStore`, and returns a matching L0 `SegmentRef`
/// under `RSEG_TENANT`.
async fn write_rseg_segment() -> (Arc<MemoryStore>, SegmentRef) {
    write_rseg_segment_under(RSEG_TENANT).await
}

/// Like [`write_rseg_segment`] but writes the segment identity (and therefore
/// the footer's `tenant_hash`) under `tenant`, so a fetch as any other tenant
/// fails `check_identity`. The cross-tenant tests write under `TENANT_A` and
/// fetch first as `TENANT_A` (which populates the cache and passes identity)
/// and then as `TENANT_B`.
async fn write_rseg_segment_under(tenant: TenantHash) -> (Arc<MemoryStore>, SegmentRef) {
    let writer_id = Uuid::from_u128(5);
    let identity = SegmentIdentity {
        tenant_hash: tenant.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: 0,
        max_ingest_ts_ns: 0,
    };
    const NS: i64 = 1_000_000_000;
    let chaotic = scalar_series_v3(
        "chaotic_metric",
        &[(1_000 * NS, 0.0), (1_001 * NS, f64::from_bits(u64::MAX))],
    );
    let hist = hist_series_v3(
        "hist_metric",
        vec![
            HistogramSample {
                ts_ns: 1_000 * NS,
                value: hist_value(3, 6.0),
            },
            HistogramSample {
                ts_ns: 1_001 * NS,
                value: hist_value(5, 11.0),
            },
        ],
    );
    let written =
        SegmentWriter::write_histograms(vec![hist, chaotic], identity, bounds).expect("write");

    let store = Arc::new(MemoryStore::new());
    let key = "test/cache-correctness-segment.rseg";
    store
        .put(key, written.bytes.clone(), PutOptions::default())
        .await
        .expect("put segment object");

    let seg_ref = SegmentRef {
        data_object_key: key.to_string(),
        object_size: written.bytes.len() as u64,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        ingest_hour_bucket: 0,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        shard: 0,
        content_hash: written.summary.blake3,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        created_unix_ns: 123,
        level: SegmentLevel::L0,
    };
    (store, seg_ref)
}

// ---- RLOG test segment -------------------------------------------------

fn rlog_identity_under(tenant: TenantHash) -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: tenant.0,
        shard: 0,
        writer_id: [22u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

/// Writes a small RLOG object (20 records, one stream), puts it on a fresh
/// `MemoryStore`, and returns a matching L0 `SegmentRef` under `RLOG_TENANT`.
async fn write_rlog_segment() -> (Arc<MemoryStore>, SegmentRef) {
    write_rlog_segment_under(RLOG_TENANT).await
}

/// Like [`write_rlog_segment`] but writes the object identity under `tenant`.
/// Unlike RSEG, the RLOG fetch/scan path carries no tenant identity check, so
/// a cross-tenant cache hit here would silently return the first tenant's
/// records; the isolation these tests prove therefore rests entirely on the
/// cache key including `tenant_hash`.
async fn write_rlog_segment_under(tenant: TenantHash) -> (Arc<MemoryStore>, SegmentRef) {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("cache-test".to_string()),
    )];
    let stream_id = log_stream_id(&resource, "scope", "1.0", &[]);
    let stream_attrs = stream_attrs_bytes(&resource, "scope", "1.0", &[]);
    let records: Vec<LogRecord> = (0..20)
        .map(|i| LogRecord {
            stream_id,
            stream_attrs: stream_attrs.clone(),
            ts_ns: i,
            observed_ts_ns: i,
            severity_num: 9,
            severity_text: "INFO".to_string(),
            body: format!("line {i}"),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: Vec::new(),
        })
        .collect();

    let mut writer = RlogWriter::new(RlogConfig::default(), rlog_identity_under(tenant));
    for record in &records {
        writer.push(record.clone()).expect("push record");
    }
    // Version 3: this fixture exists to exercise the block-range path's cache
    // discipline, and `BlockRangeFetcher` reads a version-4 object whole until
    // ADR-0699 decision 5's PAGE_DIR-driven fetcher replaces that path (see
    // `tests/log_block_range.rs`). A version-4 fixture would take the
    // whole-object fallback and test nothing here.
    let bytes = writer.finish_v3_for_tests().expect("finish rlog object");
    let size = bytes.len() as u64;

    let store = Arc::new(MemoryStore::new());
    let key = "test/cache-correctness-segment.rlog";
    store
        .put(key, Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put log segment object");

    let seg_ref = SegmentRef {
        data_object_key: key.to_string(),
        object_size: size,
        min_event_ts_ns: 0,
        max_event_ts_ns: 19,
        ingest_hour_bucket: 0,
        sample_count: records.len() as u64,
        series_count: 0,
        shard: 0,
        content_hash: [19u8; 32],
        writer_id: Uuid::from_u128(6),
        writer_epoch: 1,
        writer_seq: 1,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
    };
    (store, seg_ref)
}

// ---- Bit-pattern equality (NaN and -0.0 are significant; `==` is not
// enough for either) ------------------------------------------------------

fn f64_bits_eq(a: f64, b: f64) -> bool {
    a.to_bits() == b.to_bits()
}

fn opt_f64_bits_eq(a: Option<f64>, b: Option<f64>) -> bool {
    match (a, b) {
        (Some(x), Some(y)) => f64_bits_eq(x, y),
        (None, None) => true,
        _ => false,
    }
}

fn opt_vec_f64_bits_eq(a: &Option<Vec<f64>>, b: &Option<Vec<f64>>) -> bool {
    match (a, b) {
        (Some(x), Some(y)) => {
            x.len() == y.len() && x.iter().zip(y.iter()).all(|(p, q)| f64_bits_eq(*p, *q))
        }
        (None, None) => true,
        _ => false,
    }
}

fn counts_bits_eq(a: &HistogramCounts, b: &HistogramCounts) -> bool {
    match (a, b) {
        (
            HistogramCounts::Int {
                zero_count: az,
                count: ac,
                positive: ap,
                negative: an,
            },
            HistogramCounts::Int {
                zero_count: bz,
                count: bc,
                positive: bp,
                negative: bn,
            },
        ) => az == bz && ac == bc && ap == bp && an == bn,
        (
            HistogramCounts::Float {
                zero_count: az,
                count: ac,
                positive: ap,
                negative: an,
            },
            HistogramCounts::Float {
                zero_count: bz,
                count: bc,
                positive: bp,
                negative: bn,
            },
        ) => {
            f64_bits_eq(*az, *bz)
                && f64_bits_eq(*ac, *bc)
                && ap.len() == bp.len()
                && ap.iter().zip(bp.iter()).all(|(x, y)| f64_bits_eq(*x, *y))
                && an.len() == bn.len()
                && an.iter().zip(bn.iter()).all(|(x, y)| f64_bits_eq(*x, *y))
        }
        _ => false,
    }
}

fn hist_value_bits_eq(a: &HistogramValue, b: &HistogramValue) -> bool {
    a.scale == b.scale
        && f64_bits_eq(a.zero_threshold, b.zero_threshold)
        && opt_f64_bits_eq(a.sum, b.sum)
        && opt_vec_f64_bits_eq(&a.custom_values, &b.custom_values)
        && a.positive_spans == b.positive_spans
        && a.negative_spans == b.negative_spans
        && counts_bits_eq(&a.counts, &b.counts)
        && a.reset_hint == b.reset_hint
}

fn soa_bits_eq(a: &FetchedSeriesSoa, b: &FetchedSeriesSoa) -> bool {
    a.series_id == b.series_id
        && a.labels == b.labels
        && a.timestamps == b.timestamps
        && a.created_unix_ns == b.created_unix_ns
        && a.writer_epoch == b.writer_epoch
        && a.writer_seq == b.writer_seq
        && a.values.len() == b.values.len()
        && a.values
            .iter()
            .zip(b.values.iter())
            .all(|(x, y)| f64_bits_eq(*x, *y))
        && a.per_sample_priorities == b.per_sample_priorities
}

fn hist_series_bits_eq(a: &FetchedHistogramSeries, b: &FetchedHistogramSeries) -> bool {
    a.series_id == b.series_id
        && a.labels == b.labels
        && a.timestamps == b.timestamps
        && a.created_unix_ns == b.created_unix_ns
        && a.writer_epoch == b.writer_epoch
        && a.writer_seq == b.writer_seq
        && a.values.len() == b.values.len()
        && a.values
            .iter()
            .zip(b.values.iter())
            .all(|(x, y)| hist_value_bits_eq(x, y))
        && a.per_sample_priorities == b.per_sample_priorities
}

#[allow(clippy::type_complexity)]
fn assert_soa_hist_matches_or_errors(
    result: Result<
        (
            Vec<FetchedSeriesSoa>,
            FetchStats,
            Vec<FetchedHistogramSeries>,
        ),
        FetchError,
    >,
    truth_soa: &[FetchedSeriesSoa],
    truth_hist: &[FetchedHistogramSeries],
) {
    let (mut soa, _stats, mut hist) = match result {
        Err(_typed_error) => return, // a typed error is an acceptable gate outcome
        Ok(ok) => ok,
    };
    soa.sort_by_key(|s| s.series_id.0);
    hist.sort_by_key(|s| s.series_id.0);
    let mut truth_soa: Vec<_> = truth_soa.to_vec();
    let mut truth_hist: Vec<_> = truth_hist.to_vec();
    truth_soa.sort_by_key(|s| s.series_id.0);
    truth_hist.sort_by_key(|s| s.series_id.0);

    assert_eq!(soa.len(), truth_soa.len(), "scalar series count diverged");
    for (a, b) in soa.iter().zip(truth_soa.iter()) {
        assert!(
            soa_bits_eq(a, b),
            "cache-routed scalar result diverged from the uncached truth without a typed error"
        );
    }
    assert_eq!(
        hist.len(),
        truth_hist.len(),
        "histogram series count diverged"
    );
    for (a, b) in hist.iter().zip(truth_hist.iter()) {
        assert!(
            hist_series_bits_eq(a, b),
            "cache-routed histogram result diverged from the uncached truth without a typed error"
        );
    }
}

fn assert_log_matches_or_errors(
    result: Result<Option<LogFetchOutput>, LogFetchError>,
    truth: &LogFetchOutput,
) {
    match result {
        Err(_typed_error) => {} // a typed error is an acceptable gate outcome
        Ok(None) => panic!("segment must overlap the query range like the uncached baseline did"),
        Ok(Some(out)) => assert_eq!(
            out.records, truth.records,
            "cache-routed log result diverged from the uncached truth without a typed error"
        ),
    }
}

// ---- The acceptance gate -------------------------------------------------

/// ADR-0046 decision 4's acceptance gate: a cache whose every hit returns
/// deliberately corrupted bytes must never let a query return a wrong
/// result. Covers both funnels this crate owns; `Catalog::guarded_get`
/// is out of scope.
#[tokio::test]
async fn corrupted_cache_hits_never_produce_wrong_results() {
    let (rseg_store, rseg_ref) = write_rseg_segment().await;
    let rseg_backend: Arc<dyn ObjectStoreBackend> = rseg_store;
    let uncached_seg_fetcher = SegmentFetcher::new(rseg_backend.clone());
    let (truth_soa, _truth_stats, truth_hist) = uncached_seg_fetcher
        .fetch_soa_and_histograms(RSEG_TENANT, &rseg_ref, &[])
        .await
        .expect("uncached baseline fetch must succeed");

    let (rlog_store, rlog_ref) = write_rlog_segment().await;
    let rlog_backend: Arc<dyn ObjectStoreBackend> = rlog_store;
    let uncached_log_fetcher = LogSegmentFetcher::new(rlog_backend.clone());
    let query = LogQuery::new(0, 19);
    let truth_log = uncached_log_fetcher
        .fetch_accounted_with_tenant(&rlog_ref, RLOG_TENANT, &query, &QueryAccounting::new())
        .await
        .expect("uncached baseline log fetch must succeed")
        .expect("segment overlaps the query range");

    let limits = CacheLimits::new(16 * 1024 * 1024, 100, 16 * 1024 * 1024);
    let cache: Arc<Cache<crate::fetcher::CacheFetchError>> =
        Arc::new(Cache::with_corruption(limits));

    let seg_fetcher = SegmentFetcher::new(rseg_backend).with_cache(cache.clone());
    let log_fetcher = LogSegmentFetcher::new(rlog_backend).with_cache(cache);

    // First call per funnel is a genuine miss: it populates the cache with
    // clean bytes (corruption only ever applies inside `Cache::get`'s hit
    // path). It must therefore still match truth.
    let accounting = QueryAccounting::new();
    let miss_result = seg_fetcher
        .fetch_soa_and_histograms_accounted(RSEG_TENANT, &rseg_ref, &[], &accounting)
        .await;
    assert_soa_hist_matches_or_errors(miss_result, &truth_soa, &truth_hist);

    // Second call against the same (seg_ref, tenant_hash, range) is a
    // genuine hit against the now-corrupted entry -- the actual gate.
    let hit_result = seg_fetcher
        .fetch_soa_and_histograms_accounted(RSEG_TENANT, &rseg_ref, &[], &accounting)
        .await;
    assert_soa_hist_matches_or_errors(hit_result, &truth_soa, &truth_hist);

    let log_accounting = QueryAccounting::new();
    let log_miss = log_fetcher
        .fetch_accounted_with_tenant(&rlog_ref, RLOG_TENANT, &query, &log_accounting)
        .await;
    assert_log_matches_or_errors(log_miss, &truth_log);

    let log_hit = log_fetcher
        .fetch_accounted_with_tenant(&rlog_ref, RLOG_TENANT, &query, &log_accounting)
        .await;
    assert_log_matches_or_errors(log_hit, &truth_log);
}

// ---- Supporting correctness tests ---------------------------------------

/// A clean (non-corrupting) cache's miss result and hit result must both be
/// bit-identical to the uncached fetch -- including the NaN sample.
#[tokio::test]
async fn cache_hit_returns_bit_identical_result_to_uncached_fetch() {
    let (store, seg_ref) = write_rseg_segment().await;
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let uncached = SegmentFetcher::new(backend.clone());
    let (mut truth, _stats) = uncached
        .fetch_soa(RSEG_TENANT, &seg_ref, &[])
        .await
        .expect("uncached fetch");
    truth.sort_by_key(|s| s.series_id.0);

    let limits = CacheLimits::new(16 * 1024 * 1024, 100, 16 * 1024 * 1024);
    let cache = Arc::new(Cache::new(limits));
    let cached = SegmentFetcher::new(backend).with_cache(cache);

    let (mut miss, _stats) = cached
        .fetch_soa(RSEG_TENANT, &seg_ref, &[])
        .await
        .expect("miss fetch");
    miss.sort_by_key(|s| s.series_id.0);
    let (mut hit, _stats) = cached
        .fetch_soa(RSEG_TENANT, &seg_ref, &[])
        .await
        .expect("hit fetch");
    hit.sort_by_key(|s| s.series_id.0);

    assert_eq!(miss.len(), truth.len());
    for (a, b) in miss.iter().zip(truth.iter()) {
        assert!(soa_bits_eq(a, b), "cache-miss result must match uncached");
    }
    assert_eq!(hit.len(), truth.len());
    for (a, b) in hit.iter().zip(truth.iter()) {
        assert!(soa_bits_eq(a, b), "cache-hit result must match uncached");
    }
}

/// A `SegmentFetcher` with no cache attached at all (the production default
/// with no read cache, and any deployment that never calls `with_cache`) must
/// still decode the exact chaotic NaN-bit sample.
#[tokio::test]
async fn disabled_cache_produces_correct_result() {
    let (store, seg_ref) = write_rseg_segment().await;
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let fetcher = SegmentFetcher::new(backend);

    let (mut soa, _stats) = fetcher
        .fetch_soa(RSEG_TENANT, &seg_ref, &[])
        .await
        .expect("fetch with no cache attached");
    soa.sort_by_key(|s| s.series_id.0);

    assert_eq!(
        soa.len(),
        1,
        "fetch_soa returns only the scalar-kind series"
    );
    let chaotic = soa
        .iter()
        .find(|s| s.labels == labels("chaotic_metric"))
        .expect("chaotic series present");
    assert_eq!(chaotic.values.len(), 2);
    assert_eq!(chaotic.values[0].to_bits(), 0.0f64.to_bits());
    assert_eq!(
        chaotic.values[1].to_bits(),
        f64::from_bits(u64::MAX).to_bits()
    );
}

/// A cache too small to admit this segment's entries forces every call back
/// to the store. Eviction (or, as here, a standing refusal to admit) must
/// degrade to a correct, slower read, never a wrong one.
#[tokio::test]
async fn evicted_entry_falls_back_to_store_and_produces_correct_result() {
    let (store, seg_ref) = write_rseg_segment().await;
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let uncached = SegmentFetcher::new(backend.clone());
    let (mut truth, _stats) = uncached
        .fetch_soa(RSEG_TENANT, &seg_ref, &[])
        .await
        .expect("uncached fetch");
    truth.sort_by_key(|s| s.series_id.0);

    let limits = CacheLimits::new(1, 1, 1);
    let cache = Arc::new(Cache::new(limits));
    let fetcher = SegmentFetcher::new(backend).with_cache(cache);

    let (mut first, _stats) = fetcher
        .fetch_soa(RSEG_TENANT, &seg_ref, &[])
        .await
        .expect("first fetch, forced miss");
    first.sort_by_key(|s| s.series_id.0);
    let (mut second, _stats) = fetcher
        .fetch_soa(RSEG_TENANT, &seg_ref, &[])
        .await
        .expect("second fetch, forced miss again");
    second.sort_by_key(|s| s.series_id.0);

    for (a, b) in first.iter().zip(truth.iter()) {
        assert!(soa_bits_eq(a, b));
    }
    for (a, b) in second.iter().zip(truth.iter()) {
        assert!(soa_bits_eq(a, b));
    }
}

/// ADR-0044's accounting must observe the cache truthfully: a miss records a
/// real S3 request and a cache miss; a subsequent hit on the same key adds
/// cache hits and cache bytes but no additional S3 request.
#[tokio::test]
async fn cache_accounting_counts_hits_misses_and_bytes_without_double_counting_s3_requests() {
    let (store, seg_ref) = write_rseg_segment().await;
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let limits = CacheLimits::new(16 * 1024 * 1024, 100, 16 * 1024 * 1024);
    let cache = Arc::new(Cache::new(limits));
    let fetcher = SegmentFetcher::new(backend).with_cache(cache);

    let accounting = QueryAccounting::new();
    fetcher
        .fetch_soa_accounted(RSEG_TENANT, &seg_ref, &[], &accounting)
        .await
        .expect("miss fetch");
    let after_miss = accounting.snapshot();
    assert!(after_miss.cache_misses >= 1, "a first fetch must miss");
    assert_eq!(after_miss.cache_hits, 0, "a first fetch must not hit");
    assert!(
        after_miss.s3_requests(AccountedOp::Get) >= 1,
        "a genuine miss must issue at least one real GET"
    );
    let s3_requests_after_miss = after_miss.s3_requests(AccountedOp::Get);

    fetcher
        .fetch_soa_accounted(RSEG_TENANT, &seg_ref, &[], &accounting)
        .await
        .expect("hit fetch");
    let after_hit = accounting.snapshot();
    assert!(
        after_hit.cache_hits >= 1,
        "the second identical fetch must produce at least one cache hit"
    );
    assert_eq!(
        after_hit.cache_misses, after_miss.cache_misses,
        "the second call must not add new misses"
    );
    assert!(after_hit.cache_bytes > 0, "a hit must record cache bytes");
    assert_eq!(
        after_hit.s3_requests(AccountedOp::Get),
        s3_requests_after_miss,
        "a cache hit must not also record an S3 request"
    );
}

/// Same gate as [`corrupted_cache_hits_never_produce_wrong_results`], but
/// forced onto the ranged (footer-suffix + `NeedRange` chase + coalesced
/// page GETs) path instead of the whole-object path this test segment's
/// size (499 bytes) would otherwise take. The reviewer that flagged this gap
/// ran this exact shape and it failed closed correctly, but only at the
/// footer fetch -- nothing before this test exercised a *ranged* page GET
/// landing on corrupted bytes.
#[tokio::test]
async fn corrupted_cache_hits_never_produce_wrong_results_ranged_path() {
    let (rseg_store, rseg_ref) = write_rseg_segment().await;
    let rseg_backend: Arc<dyn ObjectStoreBackend> = rseg_store;
    let uncached_seg_fetcher = SegmentFetcher::new(rseg_backend.clone())
        .with_whole_object_threshold(0)
        .with_suffix_len(64);
    let (truth_soa, _truth_stats, truth_hist) = uncached_seg_fetcher
        .fetch_soa_and_histograms(RSEG_TENANT, &rseg_ref, &[])
        .await
        .expect("uncached baseline fetch must succeed");

    let limits = CacheLimits::new(16 * 1024 * 1024, 100, 16 * 1024 * 1024);
    let cache: Arc<Cache<crate::fetcher::CacheFetchError>> =
        Arc::new(Cache::with_corruption(limits));
    let seg_fetcher = SegmentFetcher::new(rseg_backend)
        .with_whole_object_threshold(0)
        .with_suffix_len(64)
        .with_cache(cache);

    let accounting = QueryAccounting::new();
    let miss_result = seg_fetcher
        .fetch_soa_and_histograms_accounted(RSEG_TENANT, &rseg_ref, &[], &accounting)
        .await;
    assert_soa_hist_matches_or_errors(miss_result, &truth_soa, &truth_hist);

    let hit_result = seg_fetcher
        .fetch_soa_and_histograms_accounted(RSEG_TENANT, &rseg_ref, &[], &accounting)
        .await;
    assert_soa_hist_matches_or_errors(hit_result, &truth_soa, &truth_hist);
}

/// Isolates the one shape [`corrupted_cache_hits_never_produce_wrong_results`]
/// cannot reach: a corrupted VAL_PAGES page behind an otherwise-clean footer
/// and catalog. `Cache::with_corruption` corrupts every hit alike, so with a
/// global corrupting cache the footer/catalog reads (fetched first) always
/// fail closed before a page read is ever attempted -- the reviewer's report
/// noted this exact gap. `Cache::insert` is deliberately a no-op on a key
/// that already holds an entry (content-addressed: same key means same
/// bytes, so nothing later is ever allowed to overwrite it), which rules out
/// "warm the cache clean, then overwrite one entry" as a way to simulate a
/// corrupted resident entry. Instead this pre-seeds the VAL page's `CacheKey`
/// with corrupted bytes *before* any fetch ever touches it, so the real fetch
/// gets a genuine cache hit on already-wrong bytes -- the same shape a
/// real bit-rotted disk-tier entry would produce -- while the footer and
/// catalog are fetched fresh from the store and cached clean.
#[tokio::test]
async fn corrupted_page_hit_behind_clean_footer_fails_closed() {
    let (store, seg_ref) = write_rseg_segment().await;
    let backend: Arc<dyn ObjectStoreBackend> = store.clone();

    let uncached = SegmentFetcher::new(backend.clone());
    let (mut truth, _stats) = uncached
        .fetch_soa(RSEG_TENANT, &seg_ref, &[])
        .await
        .expect("uncached fetch");
    truth.sort_by_key(|s| s.series_id.0);

    // Recompute the exact byte range `fetch_scalar_pages` will plan for the
    // scalar run's VAL page, by decoding the catalog and re-running
    // `plan_ranges_v4` and `coalesce_ranges` the same way the fetcher itself
    // does (reading the object directly from the store, bypassing the
    // fetcher/cache entirely for this calculation). The VAL_PAGES *section*
    // the footer describes can span more than one run's page, and
    // `ensure_ranges` coalesces the TS and VAL ranges together on an object
    // this small, so only this exact recomputed range is the one `cached_get`
    // will use as a `CacheKey` -- corrupting anything else would test
    // nothing.
    let limits = ReaderLimits::default();
    let object = store
        .get(&seg_ref.data_object_key, GetRange::Full)
        .await
        .expect("read whole object directly from the store");
    let footer =
        match open_from_suffix(&object.data, object.total_size, limits).expect("parse footer") {
            FooterOutcome::Ready(loc) => loc.footer,
            FooterOutcome::NeedRange { .. } => {
                panic!("a whole-object suffix must resolve the footer directly")
            }
        };
    const SECTION_LABEL_DICT: u32 = 1;
    const SECTION_SERIES_IDS: u32 = 5;
    const SECTION_SERIES_META: u32 = 6;
    let section = |kind: u32| {
        footer
            .sections
            .iter()
            .find(|s| s.kind == kind)
            .unwrap_or_else(|| panic!("segment has a section of kind {kind}"))
    };
    let dict = &object.data[section(SECTION_LABEL_DICT).offset as usize
        ..(section(SECTION_LABEL_DICT).offset + section(SECTION_LABEL_DICT).len) as usize];
    let ids = &object.data[section(SECTION_SERIES_IDS).offset as usize
        ..(section(SECTION_SERIES_IDS).offset + section(SECTION_SERIES_IDS).len) as usize];
    let meta = &object.data[section(SECTION_SERIES_META).offset as usize
        ..(section(SECTION_SERIES_META).offset + section(SECTION_SERIES_META).len) as usize];
    let entries = decode_catalog_v4(&footer, dict, ids, meta, limits).expect("decode catalog");
    let scalar: Vec<_> = entries
        .iter()
        .filter(|e| e.entry.value_kind == ValueKind::Scalar)
        .collect();
    let planned = plan_ranges_v4(&footer, &scalar).expect("plan ranges");
    let plan = planned
        .first()
        .expect("the scalar series has exactly one planned run");
    let page_ranges = [
        (plan.ts_range.0, plan.ts_range.0 + plan.ts_range.1),
        (plan.val_range.0, plan.val_range.0 + plan.val_range.1),
    ];
    let coalesced = coalesce_ranges(page_ranges.to_vec(), crate::fetcher::DEFAULT_COALESCE_GAP);
    let (start, end) = *coalesced
        .iter()
        .find(|(s, e)| *s <= plan.val_range.0 && plan.val_range.0 + plan.val_range.1 <= *e)
        .expect("one coalesced group must cover the VAL range");
    let cache_key = CacheKey::new(RSEG_TENANT.0, seg_ref.content_hash, start, end - start);
    let clean = object.data.slice(start as usize..end as usize);
    let corrupted: Bytes = clean.iter().map(|b| b ^ 0xA5).collect::<Vec<u8>>().into();

    // Force every section onto the ranged path, so the VAL page is a
    // cache-routed range GET under its own `CacheKey`, distinct from the
    // footer/catalog keys -- then seed that key with corrupted bytes before
    // the fetcher ever runs.
    let limits = CacheLimits::new(16 * 1024 * 1024, 100, 16 * 1024 * 1024);
    let cache = Arc::new(Cache::new(limits));
    cache.insert(cache_key, corrupted);
    let fetcher = SegmentFetcher::new(backend)
        .with_whole_object_threshold(0)
        .with_suffix_len(64)
        .with_cache(cache);

    match fetcher.fetch_soa(RSEG_TENANT, &seg_ref, &[]).await {
        Err(err) => assert!(
            matches!(err, FetchError::Corrupt { .. }),
            "expected a page-level Corrupt error with the footer/catalog clean, got: {err:?}"
        ),
        Ok((mut result, _stats)) => {
            result.sort_by_key(|s| s.series_id.0);
            for (a, b) in result.iter().zip(truth.iter()) {
                assert!(
                    !soa_bits_eq(a, b),
                    "corrupted VAL bytes must not silently decode to the correct result"
                );
            }
            panic!(
                "corrupted VAL_PAGES bytes decoded without a typed error instead of failing closed: {result:?}"
            );
        }
    }
}

// ---- Cross-tenant cache isolation --------------------------
//
// ADR-0046 decision 2: `tenant_hash` is in the `CacheKey` as a
// defence-in-depth boundary so "a hash collision or a programming error
// cannot serve one tenant's bytes to another". Cache-key construction was
// sound by inspection but untested. These four tests pin it from both
// sides -- a different tenant misses, the same tenant hits -- for each of
// the two funnels this crate owns (RSEG `SegmentFetcher::guarded_get` and
// RLOG `LogSegmentFetcher::fetch_accounted_with_tenant`).
//
// The shape common to all four: tenant A fetches once against a real
// `MemoryStore`, warming a shared cache. A second fetcher over the same
// cache is given a `FaultStore` whose every GET fails permanently. Whether
// the second fetch reaches that store is the whole signal: a cache hit
// never touches it (fault counter stays 0), a cache miss does (counter
// rises). The fault counter, not merely the presence of an error, is the
// discriminator -- an RSEG cross-tenant hit still errors, on the footer
// identity check, so "is_err()" alone would pass even with a broken key.

/// A store whose every GET fails with a permanent error. The wrapped
/// `MemoryStore` is empty and never reached: the `Permanent` fault returns
/// before delegating, so a fetch that reaches this store fails, and one that
/// does not never moves the counter. `fault_count(Op::Get,
/// FaultKind::Permanent)` is therefore an exact "was the store consulted?"
/// probe.
fn get_failing_store() -> Arc<FaultStore<MemoryStore>> {
    Arc::new(FaultStore::new(
        MemoryStore::new(),
        FaultPlan::empty().with_rule(Rule::new(
            Op::Get,
            ScriptedFault::Permanent("cross-tenant isolation test: every GET must fail".into()),
        )),
    ))
}

/// RSEG: a second tenant must not read the first tenant's cached bytes.
///
/// Tenant A fetches the segment (written under `TENANT_A`, so identity
/// passes) and warms the cache. Tenant B then fetches the SAME `SegmentRef`
/// -- same `content_hash`, same whole-object range -- through a GET-failing
/// store sharing that cache. A correct key includes `tenant_hash`, so B's
/// key differs from A's, B misses, and B must consult the store, which
/// fails. Dropping `tenant_hash` from the key would let B hit A's entry and
/// never touch the store: it would still error (on `check_identity`, since
/// the footer carries `TENANT_A`), so the fault counter is the assertion
/// that actually distinguishes the two.
#[tokio::test]
async fn rseg_a_second_tenant_does_not_read_the_first_tenants_cached_bytes() {
    let (store, seg_ref) = write_rseg_segment_under(TENANT_A).await;

    let limits = CacheLimits::new(16 * 1024 * 1024, 100, 16 * 1024 * 1024);
    let cache = Arc::new(Cache::new(limits));

    let backend_a: Arc<dyn ObjectStoreBackend> = store;
    let fetcher_a = SegmentFetcher::new(backend_a).with_cache(cache.clone());
    fetcher_a
        .fetch_soa(TENANT_A, &seg_ref, &[])
        .await
        .expect("tenant A's own fetch populates the cache");

    let fault_store = get_failing_store();
    let backend_b: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let fetcher_b = SegmentFetcher::new(backend_b).with_cache(cache);
    let result = fetcher_b.fetch_soa(TENANT_B, &seg_ref, &[]).await;

    assert!(
        result.is_err(),
        "tenant B must not receive a result for tenant A's object; got {result:?}"
    );
    assert!(
        fault_store.fault_count(Op::Get, FaultKind::Permanent) >= 1,
        "tenant B's fetch must consult the store (proving a cache miss keyed on tenant_hash), \
         but no GET fault fired -- the cache served one tenant's bytes to another"
    );
}

/// RLOG: a second tenant must not read the first tenant's cached bytes.
///
/// Same shape as the RSEG test, on the log funnel. RLOG has no tenant
/// identity check on its scan path, so a cross-tenant cache hit here would
/// return tenant A's records to tenant B as `Ok(Some(..))` -- the isolation
/// rests entirely on the cache key. Both `is_err()` and the fault counter
/// therefore fail under a key that drops `tenant_hash`.
#[tokio::test]
async fn rlog_a_second_tenant_does_not_read_the_first_tenants_cached_bytes() {
    let (store, seg_ref) = write_rlog_segment_under(TENANT_A).await;
    let query = LogQuery::new(0, 19);

    let limits = CacheLimits::new(16 * 1024 * 1024, 100, 16 * 1024 * 1024);
    let cache: Arc<Cache<crate::fetcher::CacheFetchError>> = Arc::new(Cache::new(limits));

    let backend_a: Arc<dyn ObjectStoreBackend> = store;
    let fetcher_a = LogSegmentFetcher::new(backend_a).with_cache(cache.clone());
    fetcher_a
        .fetch_accounted_with_tenant(&seg_ref, TENANT_A, &query, &QueryAccounting::new())
        .await
        .expect("tenant A's own fetch must succeed")
        .expect("segment overlaps the query range");

    let fault_store = get_failing_store();
    let backend_b: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let fetcher_b = LogSegmentFetcher::new(backend_b).with_cache(cache);
    let result = fetcher_b
        .fetch_accounted_with_tenant(&seg_ref, TENANT_B, &query, &QueryAccounting::new())
        .await;

    assert!(
        result.is_err(),
        "tenant B must not receive tenant A's log records from the cache; got {result:?}"
    );
    assert!(
        fault_store.fault_count(Op::Get, FaultKind::Permanent) >= 1,
        "tenant B's log fetch must consult the store (proving a cache miss keyed on tenant_hash), \
         but no GET fault fired -- the cache served one tenant's bytes to another"
    );
}

/// RSEG: the same tenant reads its own cached bytes without consulting the
/// store. The complement to the cross-tenant test: without it, a cache that
/// never hits (so every tenant always misses to the store) would also pass
/// the cross-tenant test. Here tenant A fetches twice; the second fetcher's
/// every GET fails, so the second fetch can only succeed from the cache, and
/// the fault counter must stay 0 to prove the store was never consulted.
#[tokio::test]
async fn rseg_the_same_tenant_reads_its_own_cached_bytes_without_consulting_the_store() {
    let (store, seg_ref) = write_rseg_segment_under(TENANT_A).await;

    let limits = CacheLimits::new(16 * 1024 * 1024, 100, 16 * 1024 * 1024);
    let cache = Arc::new(Cache::new(limits));

    let backend: Arc<dyn ObjectStoreBackend> = store;
    let fetcher = SegmentFetcher::new(backend).with_cache(cache.clone());
    let (mut truth, _stats) = fetcher
        .fetch_soa(TENANT_A, &seg_ref, &[])
        .await
        .expect("first fetch populates the cache");
    truth.sort_by_key(|s| s.series_id.0);

    let fault_store = get_failing_store();
    let backend_b: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let fetcher_b = SegmentFetcher::new(backend_b).with_cache(cache);
    let (mut hit, _stats) = fetcher_b
        .fetch_soa(TENANT_A, &seg_ref, &[])
        .await
        .expect("the same tenant must be served from cache without touching the failing store");
    hit.sort_by_key(|s| s.series_id.0);

    assert_eq!(hit.len(), truth.len());
    for (a, b) in hit.iter().zip(truth.iter()) {
        assert!(
            soa_bits_eq(a, b),
            "the same-tenant cache hit must be bit-identical to tenant A's first fetch"
        );
    }
    assert_eq!(
        fault_store.fault_count(Op::Get, FaultKind::Permanent),
        0,
        "a same-tenant cache hit must not consult the store at all"
    );
}

/// RLOG: the same tenant reads its own cached bytes without consulting the
/// store. Log-funnel complement to the RSEG same-tenant test, for the same
/// reason: it stops the cross-tenant test from being satisfiable by a cache
/// that simply never hits.
#[tokio::test]
async fn rlog_the_same_tenant_reads_its_own_cached_bytes_without_consulting_the_store() {
    let (store, seg_ref) = write_rlog_segment_under(TENANT_A).await;
    let query = LogQuery::new(0, 19);

    let limits = CacheLimits::new(16 * 1024 * 1024, 100, 16 * 1024 * 1024);
    let cache: Arc<Cache<crate::fetcher::CacheFetchError>> = Arc::new(Cache::new(limits));

    let backend: Arc<dyn ObjectStoreBackend> = store;
    let fetcher = LogSegmentFetcher::new(backend).with_cache(cache.clone());
    let truth = fetcher
        .fetch_accounted_with_tenant(&seg_ref, TENANT_A, &query, &QueryAccounting::new())
        .await
        .expect("first fetch must succeed")
        .expect("segment overlaps the query range");

    let fault_store = get_failing_store();
    let backend_b: Arc<dyn ObjectStoreBackend> = fault_store.clone();
    let fetcher_b = LogSegmentFetcher::new(backend_b).with_cache(cache);
    let hit = fetcher_b
        .fetch_accounted_with_tenant(&seg_ref, TENANT_A, &query, &QueryAccounting::new())
        .await
        .expect("the same tenant must be served from cache without touching the failing store")
        .expect("segment overlaps the query range");

    assert_eq!(
        hit.records, truth.records,
        "the same-tenant cache hit must return the same records as tenant A's first fetch"
    );
    assert_eq!(
        fault_store.fault_count(Op::Get, FaultKind::Permanent),
        0,
        "a same-tenant cache hit must not consult the store at all"
    );
}

// ---- ADR-0046 disk tier (issue #95) --------------------------------------
//
// The tests above wire a RAM-only `Cache` into each funnel through
// `with_cache`, which is what a process with no `--cache-dir` builds (#97
// wired the flag to attach a disk tier; absent, behavior is exactly this).
// The tests below prove the funnels are equally correct when `with_cache` is
// handed a `TieredCache` (RAM over disk), the second configuration the
// `ReadCache` enum this task introduced can hold. `SegmentFetcher`/
// `LogSegmentFetcher`/`BlockRangeFetcher` accept either via
// `impl Into<ReadCache>`, so these construct the disk-backed variant
// directly, exactly as #97's server wiring does.

/// Generous limits: nothing this suite's fixtures produce is evicted or refused.
fn generous_cache_limits() -> CacheLimits {
    CacheLimits::new(16 * 1024 * 1024, 100, 16 * 1024 * 1024)
}

/// RAM limits that refuse every real-sized entry (max entry bytes 1), so the
/// RAM tier of a `TieredCache` stays empty and every cache hit falls through to
/// the disk tier -- the configuration ADR-0046 decision 4's disk-tier
/// acceptance gate needs.
/// [`evicted_entry_falls_back_to_store_and_produces_correct_result`] above
/// proves a `CacheLimits::new(1, 1, 1)` cache admits nothing.
fn ram_rejecting_limits() -> CacheLimits {
    CacheLimits::new(1, 1, 1)
}

/// ADR-0046 decision 4's acceptance gate on the DISK tier specifically
/// (issue #95): a disk-served hit returning deliberately corrupted bytes must
/// never let a query return a wrong result. Mirrors
/// [`corrupted_cache_hits_never_produce_wrong_results`] but wires a
/// `TieredCache` whose RAM tier refuses admission (so every hit is served from
/// disk, proven by the disk hit counter) and is in corruption mode (so the
/// disk-served bytes arrive corrupted, through `TieredCache`'s serve-time
/// transform). Both fixtures cache the whole object under one key, so the
/// corruption lands on the footer/header and the read fails closed with a typed
/// error rather than decoding wrong data.
///
/// Prove-the-test: change either RAM tier below from `Cache::with_corruption` to
/// `Cache::new` and the disk-served hit returns clean bytes, so the second fetch
/// succeeds and the `is_err()` assertions fire. That corruption mode is exactly
/// what this gate depends on. (Demonstrated failing during development by that
/// substitution.)
#[tokio::test]
async fn corrupted_disk_tier_hits_never_produce_wrong_results() {
    let tmp = tempfile::TempDir::new().expect("temp dir for the disk cache tier");

    // Uncached baselines.
    let (rseg_store, rseg_ref) = write_rseg_segment().await;
    let rseg_backend: Arc<dyn ObjectStoreBackend> = rseg_store;
    let (truth_soa, _s, truth_hist) = SegmentFetcher::new(rseg_backend.clone())
        .fetch_soa_and_histograms(RSEG_TENANT, &rseg_ref, &[])
        .await
        .expect("uncached RSEG baseline");

    let (rlog_store, rlog_ref) = write_rlog_segment().await;
    let rlog_backend: Arc<dyn ObjectStoreBackend> = rlog_store;
    let query = LogQuery::new(0, 19);
    let truth_log = LogSegmentFetcher::new(rlog_backend.clone())
        .fetch_accounted_with_tenant(&rlog_ref, RLOG_TENANT, &query, &QueryAccounting::new())
        .await
        .expect("uncached RLOG baseline")
        .expect("segment overlaps");

    // Disk-backed, corruption-mode caches: RAM refuses admission (hits come from
    // disk), disk is generous. One per funnel so each disk hit counter is clean.
    let rseg_disk = DiskCache::new(tmp.path().join("rseg"), generous_cache_limits());
    let rseg_disk_metrics = rseg_disk.metrics();
    let rseg_cache = Arc::new(TieredCache::new(
        Cache::<crate::fetcher::CacheFetchError>::with_corruption(ram_rejecting_limits()),
        rseg_disk,
    ));
    let rlog_disk = DiskCache::new(tmp.path().join("rlog"), generous_cache_limits());
    let rlog_disk_metrics = rlog_disk.metrics();
    let rlog_cache = Arc::new(TieredCache::new(
        Cache::<crate::fetcher::CacheFetchError>::with_corruption(ram_rejecting_limits()),
        rlog_disk,
    ));

    let seg_fetcher = SegmentFetcher::new(rseg_backend).with_cache(rseg_cache.clone());
    let log_fetcher = LogSegmentFetcher::new(rlog_backend).with_cache(rlog_cache.clone());

    // First call per funnel: a genuine both-tier miss populating the disk tier
    // with CLEAN bytes (corruption applies only on a hit). Must match truth.
    let acc = QueryAccounting::new();
    let rseg_miss = seg_fetcher
        .fetch_soa_and_histograms_accounted(RSEG_TENANT, &rseg_ref, &[], &acc)
        .await;
    assert_soa_hist_matches_or_errors(rseg_miss, &truth_soa, &truth_hist);
    let log_acc = QueryAccounting::new();
    let log_miss = log_fetcher
        .fetch_accounted_with_tenant(&rlog_ref, RLOG_TENANT, &query, &log_acc)
        .await;
    assert_log_matches_or_errors(log_miss, &truth_log);

    let rseg_hits_before = rseg_disk_metrics.snapshot().hits;
    let rlog_hits_before = rlog_disk_metrics.snapshot().hits;

    // Second call per funnel: RAM is empty, so the whole object is served from
    // the disk tier -- corrupted. The read must fail closed with a typed error.
    let rseg_hit = seg_fetcher
        .fetch_soa_and_histograms_accounted(RSEG_TENANT, &rseg_ref, &[], &acc)
        .await;
    assert!(
        rseg_hit.is_err(),
        "a corrupted whole-object disk hit must fail closed with a typed error, not decode wrong \
         data; got Ok"
    );
    let log_hit = log_fetcher
        .fetch_accounted_with_tenant(&rlog_ref, RLOG_TENANT, &query, &log_acc)
        .await;
    assert!(
        log_hit.is_err(),
        "a corrupted whole-object disk hit on the log funnel must fail closed with a typed error; \
         got {log_hit:?}"
    );

    // The hits were served from the DISK tier, not RAM (RAM refused admission):
    // the gate this test names would not otherwise reach the disk tier at all.
    assert!(
        rseg_disk_metrics.snapshot().hits > rseg_hits_before,
        "the RSEG hit must have been served from the disk tier"
    );
    assert!(
        rlog_disk_metrics.snapshot().hits > rlog_hits_before,
        "the RLOG hit must have been served from the disk tier"
    );
    assert!(
        rseg_cache.is_empty() && rlog_cache.is_empty(),
        "the RAM tier must be empty, so every hit was disk-served"
    );
}

/// Deliverable 5: the same correctness path the RAM-only
/// [`cache_hit_returns_bit_identical_result_to_uncached_fetch`] exercises, but
/// against a disk-backed tier configuration with the hit served from disk. A
/// CLEAN disk-tier hit must be bit-identical (including the NaN sample) to the
/// uncached fetch, proving the disk tier introduces no regression on the
/// non-corrupt path.
#[tokio::test]
async fn clean_disk_tier_hit_is_bit_identical_to_uncached_fetch() {
    let tmp = tempfile::TempDir::new().expect("temp dir for the disk cache tier");
    let (store, seg_ref) = write_rseg_segment().await;
    let backend: Arc<dyn ObjectStoreBackend> = store;

    let (mut truth, _s) = SegmentFetcher::new(backend.clone())
        .fetch_soa(RSEG_TENANT, &seg_ref, &[])
        .await
        .expect("uncached fetch");
    truth.sort_by_key(|s| s.series_id.0);

    // RAM refuses admission so the hit is disk-served; no corruption.
    let disk = DiskCache::new(tmp.path().join("clean"), generous_cache_limits());
    let disk_metrics = disk.metrics();
    let cache = Arc::new(TieredCache::new(
        Cache::<crate::fetcher::CacheFetchError>::new(ram_rejecting_limits()),
        disk,
    ));
    let fetcher = SegmentFetcher::new(backend).with_cache(cache.clone());

    let (mut miss, _s) = fetcher
        .fetch_soa(RSEG_TENANT, &seg_ref, &[])
        .await
        .expect("first fetch (both-tier miss)");
    miss.sort_by_key(|s| s.series_id.0);
    let hits_before = disk_metrics.snapshot().hits;
    let (mut hit, _s) = fetcher
        .fetch_soa(RSEG_TENANT, &seg_ref, &[])
        .await
        .expect("second fetch (disk-served hit)");
    hit.sort_by_key(|s| s.series_id.0);

    assert!(
        disk_metrics.snapshot().hits > hits_before,
        "the second fetch must be served from the disk tier"
    );
    assert!(
        cache.is_empty(),
        "RAM refused admission, so the hit was disk-served"
    );
    assert_eq!(miss.len(), truth.len());
    assert_eq!(hit.len(), truth.len());
    for (a, b) in miss.iter().zip(truth.iter()) {
        assert!(
            soa_bits_eq(a, b),
            "disk-tier miss result must match uncached"
        );
    }
    for (a, b) in hit.iter().zip(truth.iter()) {
        assert!(
            soa_bits_eq(a, b),
            "clean disk-tier hit result must be bit-identical to uncached"
        );
    }
}

/// Deliverable 6: with NO disk tier configured -- the RAM-only `ReadCache::Ram`
/// variant every production caller builds -- both funnels return results
/// byte-for-byte identical to a fetch with no cache at all. This pins the
/// "behavior is exactly today's" guarantee the enum change must preserve; the
/// entire RAM suite above passing unchanged is the broader proof.
#[tokio::test]
async fn no_disk_tier_ram_only_matches_uncached_on_both_funnels() {
    // RSEG.
    let (rseg_store, rseg_ref) = write_rseg_segment().await;
    let rseg_backend: Arc<dyn ObjectStoreBackend> = rseg_store;
    let (mut truth_soa, _s) = SegmentFetcher::new(rseg_backend.clone())
        .fetch_soa(RSEG_TENANT, &rseg_ref, &[])
        .await
        .expect("uncached RSEG fetch");
    truth_soa.sort_by_key(|s| s.series_id.0);

    let ram: Arc<Cache<crate::fetcher::CacheFetchError>> =
        Arc::new(Cache::new(generous_cache_limits()));
    let seg_fetcher = SegmentFetcher::new(rseg_backend).with_cache(ram);
    // Miss then hit: both must equal the uncached truth.
    for pass in ["ram miss", "ram hit"] {
        let (mut got, _s) = seg_fetcher
            .fetch_soa(RSEG_TENANT, &rseg_ref, &[])
            .await
            .unwrap_or_else(|e| panic!("RSEG {pass} fetch: {e:?}"));
        got.sort_by_key(|s| s.series_id.0);
        assert_eq!(got.len(), truth_soa.len(), "RSEG {pass} series count");
        for (a, b) in got.iter().zip(truth_soa.iter()) {
            assert!(
                soa_bits_eq(a, b),
                "RSEG {pass} must match uncached byte-for-byte"
            );
        }
    }

    // RLOG.
    let (rlog_store, rlog_ref) = write_rlog_segment().await;
    let rlog_backend: Arc<dyn ObjectStoreBackend> = rlog_store;
    let query = LogQuery::new(0, 19);
    let truth_log = LogSegmentFetcher::new(rlog_backend.clone())
        .fetch_accounted_with_tenant(&rlog_ref, RLOG_TENANT, &query, &QueryAccounting::new())
        .await
        .expect("uncached RLOG fetch")
        .expect("segment overlaps");

    let ram: Arc<Cache<crate::fetcher::CacheFetchError>> =
        Arc::new(Cache::new(generous_cache_limits()));
    let log_fetcher = LogSegmentFetcher::new(rlog_backend).with_cache(ram);
    for pass in ["ram miss", "ram hit"] {
        let got = log_fetcher
            .fetch_accounted_with_tenant(&rlog_ref, RLOG_TENANT, &query, &QueryAccounting::new())
            .await
            .unwrap_or_else(|e| panic!("RLOG {pass} fetch: {e:?}"))
            .expect("segment overlaps");
        assert_eq!(
            got.records, truth_log.records,
            "RLOG {pass} must match uncached byte-for-byte"
        );
    }
}

/// The double-counting discipline from ADR-0046's `TieredCache::get` docstring
/// (issue #95, the "Read first" section): `BlockRangeFetcher`'s peek-then-defer
/// pattern -- peek each candidate block with `ReadCache::get` (the one accounted
/// miss) and resolve a miss through a later coalesced fetch -- must record
/// exactly ONE miss per logical cache miss on the tiered tier's `CacheMetrics`,
/// not two.
///
/// The RAM-only `Cache` is the oracle: its `get_or_fetch` is miss-only and never
/// re-peeks, so a cold block-range fetch records exactly one miss per logical
/// lookup -- the correct count. A `TieredCache` cold fetch of the same object
/// with the same protocol must record the SAME number of misses on its RAM tier.
/// The fetcher achieves this by resolving a peeked-then-deferred block through
/// `ReadCache::fetch_peeked` (which `insert`s on the tiered tier rather than
/// re-entering `TieredCache::get_or_fetch`, whose internal peek would count a
/// second miss).
///
/// Prove-the-test: replace the two `cache.fetch_peeked(...)` calls in
/// `BlockRangeFetcher::fetch_run` (log_fetcher.rs) with `cache.get_or_fetch(...)`
/// and the tiered tier's miss count exceeds the oracle, so the final assertion
/// fires. (Verified failing by that substitution during development.)
#[tokio::test]
async fn block_range_peek_then_defer_records_one_miss_per_logical_miss() {
    let (store, seg_ref) = write_rlog_segment().await;
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let (ts_min, ts_max) = (0, 19);

    // Force the true block-range path: whole-object crossover off (threshold 0),
    // a 64-byte suffix probe that does not cover the front blocks (so they are
    // genuinely fetched, not probe-resident), and the coverage crossover
    // disabled (>1.0) so it does not fall back to a whole-object GET that would
    // skip fetch_blocks' per-block peek entirely.
    let configure = |br: BlockRangeFetcher| {
        br.with_whole_object_threshold(0)
            .with_suffix_len(64)
            .with_coverage_threshold(2.0)
    };

    // Oracle: RAM-only cache. `Cache::get_or_fetch` is miss-only, so each logical
    // miss is exactly one `CacheMetrics` miss.
    let ram_only: Arc<Cache<crate::fetcher::CacheFetchError>> =
        Arc::new(Cache::new(generous_cache_limits()));
    let ram_metrics_oracle = ram_only.metrics();
    let br_ram = configure(BlockRangeFetcher::new(backend.clone()).with_cache(ram_only));
    let (_bytes, stats) = br_ram
        .fetch_object(
            &seg_ref,
            RLOG_TENANT,
            ts_min,
            ts_max,
            &QueryAccounting::new(),
        )
        .await
        .expect("oracle block-range fetch");
    let oracle_misses = ram_metrics_oracle.snapshot().misses;

    // The path must be genuinely exercised, or the discipline is untested.
    assert!(
        !stats.whole_object,
        "the fixture must take the ranged path, not a whole-object crossover"
    );
    assert!(
        stats.candidate_blocks >= 1,
        "at least one candidate block must be fetched through the peek-then-defer path"
    );
    assert!(
        oracle_misses >= 1,
        "the cold fetch must record at least one miss"
    );

    // Tiered cache: same object, same protocol, cold. The RAM tier's miss count
    // must equal the oracle -- a double-count would inflate it.
    let tmp = tempfile::TempDir::new().expect("temp dir for the disk cache tier");
    let ram = Cache::<crate::fetcher::CacheFetchError>::new(generous_cache_limits());
    let ram_metrics_tiered = ram.metrics();
    let disk = DiskCache::new(tmp.path().to_path_buf(), generous_cache_limits());
    let tiered = Arc::new(TieredCache::new(ram, disk));
    let br_tiered = configure(BlockRangeFetcher::new(backend).with_cache(tiered));
    br_tiered
        .fetch_object(
            &seg_ref,
            RLOG_TENANT,
            ts_min,
            ts_max,
            &QueryAccounting::new(),
        )
        .await
        .expect("tiered block-range fetch");

    assert_eq!(
        ram_metrics_tiered.snapshot().misses,
        oracle_misses,
        "the tiered tier must record exactly one miss per logical cache miss (peek-then-defer), \
         not two: a second miss layered on the deferred fetch would inflate this above the \
         RAM-only oracle of {oracle_misses}"
    );
}

// ---- #811: footer-suffix reads are cache-eligible ----------------------

/// Wraps a `MemoryStore` and records the exact `GetRange` of every `get()`
/// call, so a test can distinguish *which* bytes a fetcher re-requested
/// instead of only counting GETs in aggregate. The RSEG footer trailer sits
/// at the very end of the object (docs/segment-format.md), so a range whose
/// `end` equals the object's total size is a footer-opening read; every
/// other range this fetcher issues (catalog sections, value pages) lands
/// strictly before that tail.
#[derive(Default)]
struct RangeLog {
    calls: std::sync::Mutex<Vec<GetRange>>,
}

impl RangeLog {
    fn snapshot(&self) -> Vec<GetRange> {
        self.calls.lock().expect("range log lock").clone()
    }

    /// Count of logged calls whose range's end lands exactly on
    /// `total_size`: the footer trailer's fixed position, regardless of
    /// whether the range was requested as `Suffix` (no explicit end) or as
    /// an absolute `Range`/`Full`.
    fn footer_range_calls(&self, total_size: u64) -> usize {
        self.calls
            .lock()
            .expect("range log lock")
            .iter()
            .filter(|r| match r {
                GetRange::Full => true,
                GetRange::Range(_, end) => *end == total_size,
                GetRange::Suffix(_) => true,
            })
            .count()
    }
}

struct RangeLoggingStore {
    inner: MemoryStore,
    log: Arc<RangeLog>,
}

impl RangeLoggingStore {
    fn new(inner: MemoryStore) -> (Self, Arc<RangeLog>) {
        let log = Arc::new(RangeLog::default());
        (
            RangeLoggingStore {
                inner,
                log: Arc::clone(&log),
            },
            log,
        )
    }
}

#[async_trait::async_trait]
impl ObjectStoreBackend for RangeLoggingStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        self.log.calls.lock().expect("range log lock").push(range);
        self.inner.get(key, range).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(prefix, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}

/// Forces the ranged (footer-suffix + `NeedRange` chase) path regardless of
/// this fixture's actual size, the same way
/// `corrupted_cache_hits_never_produce_wrong_results_ranged_path` does:
/// `whole_object_threshold(0)` disables the whole-object short-circuit, and
/// `suffix_len(64)` is deliberately smaller than the footer proto, so the
/// first probe always needs a `NeedRange` chase.
fn ranged_footer_fetcher(backend: Arc<dyn ObjectStoreBackend>) -> SegmentFetcher {
    SegmentFetcher::new(backend)
        .with_whole_object_threshold(0)
        .with_suffix_len(64)
}

/// #811: a footer probe that used to be `GetRange::Suffix` (uncacheable, no
/// absolute start to key a cache entry on) is now an absolute `GetRange`,
/// routed through the same cache every other ranged GET uses. A second,
/// *different* query over the same segment (scalar matcher, then a
/// histogram matcher selecting disjoint pages) must not reissue either of
/// the footer-opening GETs the first query already paid for, while it still
/// issues fresh GETs for the pages only the second query needs -- proving
/// the footer specifically is cached, not that the whole query happened to
/// repeat.
#[tokio::test]
async fn footer_probe_and_needrange_chase_are_cache_hits_on_a_second_distinct_query() {
    let (store, seg_ref) = write_rseg_segment().await;
    let bytes = store
        .get(&seg_ref.data_object_key, GetRange::Full)
        .await
        .expect("read back segment bytes")
        .data;
    let total_size = seg_ref.object_size;

    let inner = MemoryStore::new();
    inner
        .put(&seg_ref.data_object_key, bytes, PutOptions::default())
        .await
        .expect("seed logging store");
    let (logging_store, log) = RangeLoggingStore::new(inner);
    let backend: Arc<dyn ObjectStoreBackend> = Arc::new(logging_store);

    let limits = CacheLimits::new(16 * 1024 * 1024, 100, 16 * 1024 * 1024);
    let cache = Arc::new(Cache::new(limits));
    let fetcher = ranged_footer_fetcher(backend).with_cache(cache);

    // First query: scalar matcher on `chaotic_metric`. A genuine cache miss,
    // so it must pay at least the footer probe plus its `NeedRange` chase.
    let scalar_matchers = [LabelMatcher::equal("__name__", "chaotic_metric")];
    fetcher
        .fetch_soa(RSEG_TENANT, &seg_ref, &scalar_matchers)
        .await
        .expect("first query, scalar path");
    let footer_calls_after_first = log.footer_range_calls(total_size);
    let total_calls_after_first = log.snapshot().len();
    assert_eq!(
        footer_calls_after_first,
        2,
        "the 64-byte suffix probe is too small for this segment's footer, so opening it must \
         take exactly two footer-range GETs (the probe, then the NeedRange chase), got {:?}",
        log.snapshot()
    );

    // Second query: a disjoint matcher on the histogram series, so it needs
    // pages the first query never touched, but the same footer.
    let hist_matchers = [LabelMatcher::equal("__name__", "hist_metric")];
    fetcher
        .fetch_histograms(RSEG_TENANT, &seg_ref, &hist_matchers)
        .await
        .expect("second query, histogram path");
    let footer_calls_after_second = log.footer_range_calls(total_size);
    let total_calls_after_second = log.snapshot().len();

    assert_eq!(
        footer_calls_after_second,
        footer_calls_after_first,
        "a second, different query over the same segment must add zero footer-range GETs \
         (both the probe and the chase are cache hits); log: {:?}",
        log.snapshot()
    );
    assert!(
        total_calls_after_second > total_calls_after_first,
        "the second query selects pages the first one never fetched, so it must still issue \
         new (non-footer) store GETs -- a total of zero here would mean this test failed to \
         exercise a genuinely different query, not that caching worked"
    );
}

/// Same shape as the accounting proof above
/// (`cache_accounting_counts_hits_misses_and_bytes_without_double_counting_s3_requests`),
/// but forced onto the ranged footer-suffix path instead of the
/// whole-object path that test's small fixture takes by default: before
/// #811 a `GetRange::Suffix` footer probe always bypassed the cache
/// (`guarded_get`'s `cacheable_range` match returns `None` for `Suffix`),
/// so this exact scenario never even reached `cached_get` on the first
/// GET. `matches_series` is `matchers.iter().all(...)`, vacuously true for
/// an empty matcher slice, so an empty matcher set selects every series,
/// not none; a matcher naming a `__name__` value absent from the fixture
/// selects zero series instead, so no page GET follows the catalog decode
/// and the only work either call does is opening the segment (footer
/// probe, chase, and the catalog sections `decode_selected` always reads
/// to know nothing matched). Repeating the identical query isolates that
/// cost cleanly.
#[tokio::test]
async fn footer_only_query_hits_add_zero_s3_requests_and_zero_bytes() {
    let (store, seg_ref) = write_rseg_segment().await;
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let limits = CacheLimits::new(16 * 1024 * 1024, 100, 16 * 1024 * 1024);
    let cache = Arc::new(Cache::new(limits));
    let fetcher = ranged_footer_fetcher(backend).with_cache(cache);
    let no_match = [LabelMatcher::equal("__name__", "no_such_metric")];

    let accounting = QueryAccounting::new();
    fetcher
        .fetch_soa_accounted(RSEG_TENANT, &seg_ref, &no_match, &accounting)
        .await
        .expect("miss: open the segment, match nothing");
    let after_miss = accounting.snapshot();
    assert_eq!(
        after_miss.s3_requests(AccountedOp::Get),
        3,
        "opening this segment's footer through the 64-byte ranged probe takes two GETs \
         (probe, then NeedRange chase), plus one coalesced GET for the LABEL_DICT/SERIES_IDS/ \
         SERIES_META catalog sections decode_selected always reads; a matcher that selects \
         zero series still needs the catalog decoded to know that, so that is the whole \
         query's S3 cost with no page GET behind it"
    );
    let requests_after_miss = after_miss.s3_requests(AccountedOp::Get);
    let bytes_after_miss = after_miss.s3_bytes(AccountedOp::Get);

    fetcher
        .fetch_soa_accounted(RSEG_TENANT, &seg_ref, &no_match, &accounting)
        .await
        .expect("hit: identical query, footer served from cache");
    let after_hit = accounting.snapshot();
    assert_eq!(
        after_hit.s3_requests(AccountedOp::Get),
        requests_after_miss,
        "a fully cached footer-open must add zero S3 requests"
    );
    assert_eq!(
        after_hit.s3_bytes(AccountedOp::Get),
        bytes_after_miss,
        "a fully cached footer-open must add zero S3 bytes"
    );
    assert_eq!(
        after_hit.cache_hits, 3,
        "the probe, the chase, and the catalog-sections GET must all register as cache hits"
    );
    assert!(
        after_hit.cache_bytes > 0,
        "a cache hit must still report the bytes it served, just not as S3 traffic"
    );
}

/// #811 deliverable 3: the one call site in scope (`SegmentFetcher::open_segment`)
/// still takes the old `GetRange::Suffix` path, unconverted, when
/// `object_size == 0` -- the only representation of "unavailable" this
/// `u64` field has (every real `SegmentRef` carries the commit/compaction
/// record's actual size, docs/catalog-and-mvcc.md). That path must still
/// produce correct results, including a `NeedRange` chase over it: this
/// pins the same segment decoding identically whether opened through the
/// absolute-range path (`object_size` known) or the suffix path
/// (`object_size` absent), and that only `Suffix` ranges are used on the
/// unavailable-size path -- never a silent, unlogged fallback to something
/// else.
#[tokio::test]
async fn needrange_chase_still_correct_when_object_size_is_unavailable() {
    let (store, mut seg_ref) = write_rseg_segment().await;
    let backend: Arc<dyn ObjectStoreBackend> = store.clone();

    let known_size_fetcher = ranged_footer_fetcher(Arc::clone(&backend));
    let (mut truth, _stats) = known_size_fetcher
        .fetch_soa(RSEG_TENANT, &seg_ref, &[])
        .await
        .expect("baseline fetch with object_size known");
    truth.sort_by_key(|s| s.series_id.0);

    let inner = MemoryStore::new();
    let bytes = store
        .get(&seg_ref.data_object_key, GetRange::Full)
        .await
        .expect("read back segment bytes")
        .data;
    inner
        .put(&seg_ref.data_object_key, bytes, PutOptions::default())
        .await
        .expect("seed logging store");
    let (logging_store, log) = RangeLoggingStore::new(inner);
    let logging_backend: Arc<dyn ObjectStoreBackend> = Arc::new(logging_store);

    seg_ref.object_size = 0;
    let unknown_size_fetcher = ranged_footer_fetcher(logging_backend);
    let (mut unknown, _stats) = unknown_size_fetcher
        .fetch_soa(RSEG_TENANT, &seg_ref, &[])
        .await
        .expect("fetch with object_size unavailable must still succeed");
    unknown.sort_by_key(|s| s.series_id.0);

    assert_eq!(truth.len(), unknown.len());
    for (a, b) in truth.iter().zip(unknown.iter()) {
        assert!(soa_bits_eq(a, b));
    }

    let calls = log.snapshot();
    assert!(
        calls
            .iter()
            .all(|r| matches!(r, GetRange::Suffix(_) | GetRange::Range(..))),
        "unexpected range kind on the object_size-unavailable path: {calls:?}"
    );
    assert!(
        matches!(calls.first(), Some(GetRange::Suffix(64))),
        "with object_size unavailable the first GET must stay the old suffix probe, never a \
         silently fabricated absolute range: {calls:?}"
    );
}
