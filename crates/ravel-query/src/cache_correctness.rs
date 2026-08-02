//! Acceptance gate for ADR-0046's read-cache epic (issue #444, epic #437).
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
//! (crates/ravel-catalog/src/catalog.rs); issue #443 owns it, so this suite
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
use ravel_cache::{Cache, CacheKey, CacheLimits};
use ravel_catalog::{SegmentLevel, SegmentRef};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions};
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
    FetchError, FetchStats, FetchedSeriesSoa, LogFetchError, LogFetchOutput, LogQuery,
    LogSegmentFetcher, SegmentFetcher,
};

const RSEG_TENANT: TenantHash = TenantHash([13u8; 16]);
const RLOG_TENANT: TenantHash = TenantHash([21u8; 16]);

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
    let writer_id = Uuid::from_u128(5);
    let identity = SegmentIdentity {
        tenant_hash: RSEG_TENANT.0,
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

fn rlog_identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: RLOG_TENANT.0,
        shard: 0,
        writer_id: [22u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

/// Writes a small RLOG object (20 records, one stream), puts it on a fresh
/// `MemoryStore`, and returns a matching L0 `SegmentRef` under `RLOG_TENANT`.
async fn write_rlog_segment() -> (Arc<MemoryStore>, SegmentRef) {
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

    let mut writer = RlogWriter::new(RlogConfig::default(), rlog_identity());
    for record in &records {
        writer.push(record.clone()).expect("push record");
    }
    let bytes = writer.finish().expect("finish rlog object");
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
/// (issue #443) is out of scope.
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
/// before this epic, and any deployment that never calls `with_cache`) must
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
