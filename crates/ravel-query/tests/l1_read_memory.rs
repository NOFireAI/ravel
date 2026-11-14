//! What a PromQL read over large L1 metrics objects allocates, against what
//! it charges to the memory budget (issue #4118), native-histogram objects
//! included (issue #4198).
//!
//! The shape is the one that took ravel-server past its container limit: a
//! few L1 parts, each holding many series with every hour of each series in
//! the one object. The objects are built with the real compactor in a
//! `MemoryStore`, and the statements run through
//! `QueryEngine::range_hist_with_budgets`, the method the
//! `/api/v1/query_range` handler calls.
//!
//! This binary installs jemalloc as its global allocator and samples its
//! `stats.allocated` figure (the one ravel-server reports) on a separate
//! thread while a statement runs. A sampled maximum is a lower bound on the
//! true peak, so an assertion that the peak exceeds a bound is sound as
//! sampled; the bound the tests assert after the fix is checked against the
//! same samples.

#![cfg(not(target_env = "msvc"))]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use ravel_catalog::{Catalog, CatalogConfig, SegmentLevel};
use ravel_commit::keys::commit_key_for_record;
use ravel_commit::record::{self, NewCommitRecord};
use ravel_maintain::{
    Bucket, CompactionOutcome, CompactorConfig, FixedClock, PublishOutcome, compact_bucket,
};
use ravel_memory::MemoryBudget;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions};
use ravel_query::{EngineConfig, FetchError, GetLimiter, QueryEngine, QueryError};
use ravel_segment::{
    HistogramCounts, HistogramSample, HistogramSpan, HistogramValue, IngestBounds, ReaderLimits,
    ResetHint, SegmentIdentity, SegmentWriter, SeriesEntryV4, SeriesInput, SeriesInputV3,
    SeriesValues, VERSION_V7, ValueKind, WrittenSegment, decode_catalog_v5, open_from_full,
};
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantHash, TenantId, TimeRange};
use tikv_jemalloc_ctl::{epoch, stats};
use uuid::Uuid;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_HOUR: i64 = 3_600 * NS_PER_SEC;
const HOUR: u32 = 500_000;
const TENANT: TenantHash = TenantHash([0x41; 16]);
/// Hours of event time each series carries, all of them in one L1 part.
const SPAN_HOURS: i64 = 12;
/// L0 flushes compacted into the part. Each covers a contiguous quarter of
/// the span for every series, as a replay writes them.
const FLUSHES: i64 = 4;
const PERMITS: usize = 4;
const FANOUT: usize = 4;

/// The memory budget of the over-budget runs. The full-span fan-out
/// statement's merged samples alone need several times this.
const SMALL_BUDGET: u64 = 8 * 1024 * 1024;

/// The memory budget of the in-budget runs: room for the whole decoded
/// catalog, provenance included, and every merged sample of either shape.
const IN_BUDGET: u64 = 256 * 1024 * 1024;

/// What the process may allocate beyond the budget while one statement runs:
/// the engine's fixed per-query structures, the snapshot and plan, the
/// tokio runtime's and this sampler's own allocations, and the memory
/// jemalloc's thread caches hold as allocated. None of these scales with
/// object size; the reproduction varies object size to tell the two apart.
const FIXED_OVERHEAD: u64 = 4 * 1024 * 1024;

/// Steps of the range statements: the whole span at a 60 s step.
const STEPS: u64 = (SPAN_HOURS * 60 + 1) as u64;

/// What the evaluator itself holds for `query`, which the memory budget does
/// not charge: the inner range function's per-series output at every step,
/// and the per-step vectors an aggregation reads it as, each a 16-byte
/// sample. It scales with the series matched and the steps, not with the
/// objects read.
fn evaluation_len(query: &str, shape: Shape) -> u64 {
    let series = if query == SINGLE_RATE {
        1
    } else {
        shape.series as u64
    };
    2 * series * STEPS * std::mem::size_of::<Sample>() as u64
}

fn hour_start() -> i64 {
    i64::from(HOUR) * NS_PER_HOUR
}

fn span_end() -> i64 {
    hour_start() + SPAN_HOURS * NS_PER_HOUR
}

/// The read's wall clock: past the span, so the whole span is queryable.
fn now_ns() -> i64 {
    span_end() + NS_PER_HOUR
}

fn series_labels(i: usize) -> (SeriesId, LabelSet) {
    let labels = LabelSet::new(vec![
        Label {
            name: "__name__".to_string(),
            value: "requests_total".to_string(),
        },
        Label {
            name: "instance".to_string(),
            value: format!("instance-{i}"),
        },
        Label {
            name: "job".to_string(),
            value: "api".to_string(),
        },
    ])
    .expect("labels");
    let id = SeriesId::compute(&TenantId::new("t"), "requests_total", &labels).expect("series id");
    (id, labels)
}

/// The value of series `i` at sample `k`: a counter that grows by a small
/// per-series step, the shape the corpus' `rate` statements read.
fn counter_value(i: usize, k: i64) -> f64 {
    let step = (i % 7 + 1) as f64;
    step * k as f64 + (k % 3) as f64 * 0.25
}

fn l0_writer_id(seq: u64) -> Uuid {
    Uuid::from_u128(u128::from(seq) + 1)
}

fn l0_identity(seq: u64) -> SegmentIdentity {
    SegmentIdentity {
        tenant_hash: TENANT.0,
        shard: 0,
        writer_id: l0_writer_id(seq).to_string(),
        writer_epoch: 1,
        writer_seq: seq,
    }
}

fn l0_bounds() -> IngestBounds {
    IngestBounds {
        min_ingest_ts_ns: hour_start(),
        max_ingest_ts_ns: hour_start() + 10,
    }
}

async fn put_l0(store: &MemoryStore, seq: u64, series: Vec<SeriesInput>) {
    let written = SegmentWriter::write(series, l0_identity(seq), l0_bounds()).expect("write L0");
    put_written_l0(store, seq, written).await;
}

async fn put_written_l0(store: &MemoryStore, seq: u64, written: WrittenSegment) {
    let writer_id = l0_writer_id(seq);
    let rec = record::build(NewCommitRecord {
        tenant_hash: TENANT,
        signal: Signal::Metrics,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: seq,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: hour_start(),
        max_ingest_ts_ns: hour_start() + 10,
        segment_format_version: u32::from(VERSION_V7),
        created_unix_ns: hour_start() + i64::try_from(seq).expect("seq"),
        ingest_hour_bucket: HOUR,
    })
    .expect("commit record");
    store
        .put(
            &rec.object_key,
            written.bytes,
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put data object");
    let commit_key = commit_key_for_record(&rec).expect("commit key");
    store
        .put(
            &commit_key,
            record::encode(&rec),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("put commit record");
}

/// One reproduction tenant's shape: how many series, sampled how often, and
/// how its L0 flushes split the samples. Below 4096 series the L1 part
/// carries a whole SERIES_META catalog section; at or above it, a chunked
/// SERIES_META_CHUNKS one.
#[derive(Debug, Clone, Copy)]
struct Shape {
    series: usize,
    interval_ns: i64,
    /// Each flush holds a quarter of the span for every series, as a replay
    /// writes them, so the compactor merges each series' runs and writes the
    /// per-sample provenance extension. Otherwise each of two flushes holds
    /// the whole span for half the series, and the part carries none.
    merged_runs: bool,
}

/// The whole-SERIES_META shape: one sample every 15 s, the corpus' scrape
/// interval.
const DENSE: Shape = Shape {
    series: 500,
    interval_ns: 15 * NS_PER_SEC,
    merged_runs: true,
};

/// The chunked-catalog shape, sampled every 2 min to keep its sample count
/// near the dense one's.
const CHUNKED: Shape = Shape {
    series: 5_000,
    interval_ns: 120 * NS_PER_SEC,
    merged_runs: true,
};

/// The dense shape without the provenance extension: its read holds no
/// provenance, so what it allocates is the page batches' decoded samples and
/// the sample fold.
const UNMERGED: Shape = Shape {
    merged_runs: false,
    ..DENSE
};

impl Shape {
    fn doubled(self) -> Self {
        Self {
            series: self.series * 2,
            ..self
        }
    }
}

/// `shape.series` series, each sampled every `shape.interval_ns` for
/// `SPAN_HOURS`, written as `FLUSHES` L0 flushes into one ingest hour and
/// compacted into L1. Returns the store and the summed size of its L1 parts.
async fn l1_tenant(shape: Shape) -> (Arc<MemoryStore>, u64) {
    let Shape {
        series,
        interval_ns,
        merged_runs,
    } = shape;
    let store = Arc::new(MemoryStore::new());
    let total = SPAN_HOURS * NS_PER_HOUR / interval_ns;
    let flushes = if merged_runs { FLUSHES } else { 2 };
    for flush in 0..flushes {
        let (series_range, samples_range) = if merged_runs {
            let per_flush = total / FLUSHES;
            (0..series, flush * per_flush..(flush + 1) * per_flush)
        } else {
            let half = series / 2;
            let start = usize::try_from(flush).expect("flush") * half;
            (start..start + half, 0..total)
        };
        let inputs: Vec<SeriesInput> = series_range
            .map(|i| {
                let (series_id, labels) = series_labels(i);
                let samples = samples_range
                    .clone()
                    .map(|k| Sample {
                        ts_ns: hour_start() + k * interval_ns,
                        value: counter_value(i, k),
                    })
                    .collect();
                SeriesInput {
                    series_id,
                    labels,
                    samples,
                }
            })
            .collect();
        put_l0(&store, u64::try_from(flush).expect("flush") + 1, inputs).await;
    }
    let (bytes, entries) = compact_l1(&store).await;
    let with_provenance = entries
        .iter()
        .any(|e| e.per_sample_provenance.iter().any(Option::is_some));
    assert_eq!(
        with_provenance, merged_runs,
        "{shape:?}: the part carries the provenance extension exactly when runs merged"
    );
    (store, bytes)
}

/// Compacts the hour's L0 flushes into L1 and returns the summed size of
/// the L1 parts that resolve and every catalog entry they hold.
async fn compact_l1(store: &Arc<MemoryStore>) -> (u64, Vec<SeriesEntryV4>) {
    let outcome = compact_bucket(
        store.as_ref(),
        &FixedClock::new(hour_start() + 3 * NS_PER_HOUR),
        &CompactorConfig::default(),
        &Bucket::new(TENANT, Signal::Metrics, 0, HOUR),
    )
    .await
    .expect("compact_bucket");
    match outcome {
        CompactionOutcome::Compacted { publish, .. } => {
            assert_eq!(publish, PublishOutcome::Published);
        }
        other => panic!("expected the hour to compact, got {other:?}"),
    }
    let backend: Arc<dyn ObjectStoreBackend> = store.clone();
    let catalog = Catalog::new(backend, CatalogConfig::default()).expect("catalog");
    let snapshot = catalog
        .resolve(
            &TENANT,
            Signal::Metrics,
            TimeRange {
                start_ns: hour_start(),
                end_ns: span_end(),
            },
            &[],
            now_ns(),
        )
        .await
        .expect("resolve");
    assert!(
        snapshot
            .segments
            .iter()
            .all(|s| matches!(s.level, SegmentLevel::L1 { .. })),
        "only L1 parts resolve"
    );
    let mut bytes = 0;
    let mut entries = Vec::new();
    for segment in &snapshot.segments {
        bytes += segment.object_size;
        let object = store
            .get(&segment.data_object_key, GetRange::Full)
            .await
            .expect("get L1 part")
            .data;
        let footer = open_from_full(&object, ReaderLimits::default())
            .expect("footer")
            .footer;
        entries
            .extend(decode_catalog_v5(&footer, &object, ReaderLimits::default()).expect("catalog"));
    }
    (bytes, entries)
}

fn engine(store: &Arc<MemoryStore>, budget: &Arc<MemoryBudget>) -> QueryEngine {
    let backend: Arc<dyn ObjectStoreBackend> = store.clone();
    let catalog = Catalog::new(Arc::clone(&backend), CatalogConfig::default()).expect("catalog");
    let config = EngineConfig {
        promql_fetch_fanout: Some(FANOUT),
        max_series: 1_000_000,
        max_samples: 500_000_000,
        ..EngineConfig::default()
    };
    QueryEngine::new(Arc::new(catalog), backend, config)
        .with_get_limiter(Arc::new(GetLimiter::new(PERMITS).expect("limiter")))
        .with_memory_budget(Arc::clone(budget))
}

/// One statement's figures: the result, the most the process allocated
/// above what it held before the statement, the most the budget held
/// reserved, and the refusals the budget counted.
struct Measured {
    result: Result<String, QueryError>,
    peak_allocated: u64,
    peak_reserved: u64,
    refusals: u64,
}

fn allocated() -> u64 {
    epoch::advance().expect("jemalloc epoch");
    stats::allocated::read().expect("jemalloc stats.allocated") as u64
}

/// jemalloc's allocated figure is process-wide, so a test measuring it must
/// not run beside another test of this binary.
static MEASURING: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Runs `test` on a current-thread runtime of its own while no other test of
/// this binary runs.
fn serialized(test: impl std::future::Future<Output = ()>) {
    let _alone = MEASURING
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime")
        .block_on(test);
}

/// Runs `query` as a range query over the whole span at a 60 s step,
/// sampling jemalloc's allocated bytes and the budget's reserved bytes on a
/// separate thread until it returns.
async fn measure(store: &Arc<MemoryStore>, budget_bytes: u64, query: &str) -> Measured {
    let budget = Arc::new(MemoryBudget::new(budget_bytes));
    let eng = engine(store, &budget);
    let baseline = allocated();
    let done = Arc::new(AtomicBool::new(false));
    let sampler = {
        let done = Arc::clone(&done);
        let budget = Arc::clone(&budget);
        std::thread::spawn(move || {
            let mut peak_allocated = 0u64;
            let mut peak_reserved = 0u64;
            loop {
                let finished = done.load(Ordering::Acquire);
                peak_allocated = peak_allocated.max(allocated());
                peak_reserved = peak_reserved.max(budget.reserved());
                if finished {
                    return (peak_allocated, peak_reserved);
                }
                std::thread::sleep(Duration::from_micros(50));
            }
        })
    };
    let start_ms = hour_start() / 1_000_000;
    let end_ms = span_end() / 1_000_000;
    let result = eng
        .range_hist_with_budgets(
            TENANT,
            query,
            start_ms,
            end_ms,
            60_000,
            &[],
            now_ns(),
            Duration::from_secs(600),
            None,
        )
        .await
        .map(|(value, _annotations, _stats)| format!("{value:?}"));
    done.store(true, Ordering::Release);
    let (peak, peak_reserved) = sampler.join().expect("sampler");
    let refusals = budget.accounted_refusals();
    drop(eng);
    Measured {
        result,
        peak_allocated: peak.saturating_sub(baseline),
        peak_reserved,
        refusals,
    }
}

const FANOUT_RATE: &str = "sum(rate(requests_total[5m]))";
const FULL_RANGE_INCREASE: &str = "sum(increase(requests_total[30m]))";
const SINGLE_RATE: &str = r#"rate(requests_total{instance="instance-3"}[5m])"#;

fn assert_budget_error(m: &Measured, what: &str) {
    match &m.result {
        Err(QueryError::Fetch(FetchError::FetchMemoryExhausted { limit, .. })) => {
            assert_eq!(*limit, SMALL_BUDGET, "{what}: refused against the budget");
        }
        Err(other) => panic!("{what}: expected FetchMemoryExhausted, got {other:?}"),
        Ok(_) => panic!("{what}: expected the budget to refuse, the statement answered"),
    }
}

/// A statement whose merged samples need several times the budget is
/// refused with the budget's typed error, and the process never allocates
/// more than the budget plus a fixed overhead on the way to that refusal,
/// at either object size: the figure must not grow with the object.
async fn assert_over_budget_refused(shape: Shape) {
    let mut failures = Vec::new();
    for shape in [shape, shape.doubled()] {
        let series = shape.series;
        let (store, object_bytes) = l1_tenant(shape).await;
        for query in [FANOUT_RATE, FULL_RANGE_INCREASE, SINGLE_RATE] {
            let m = measure(&store, SMALL_BUDGET, query).await;
            let figures = format!(
                "{query} over {series} series ({object_bytes} B of L1): peak allocated {} B, \
                 peak reserved {} B, budget {SMALL_BUDGET} B, {} refusals, answered {}",
                m.peak_allocated,
                m.peak_reserved,
                m.refusals,
                m.result.is_ok()
            );
            eprintln!("{figures}");
            if query != SINGLE_RATE {
                assert_budget_error(&m, query);
                assert!(m.refusals >= 1, "{query}: the budget counted the refusal");
            }
            if m.peak_allocated > SMALL_BUDGET + FIXED_OVERHEAD {
                failures.push(figures);
            }
        }
    }
    assert!(
        failures.is_empty(),
        "the process allocated past the budget plus {FIXED_OVERHEAD} B:\n{}",
        failures.join("\n")
    );
}

#[test]
fn an_over_budget_read_of_a_whole_series_meta_l1_part_is_refused_within_the_budget() {
    serialized(assert_over_budget_refused(DENSE));
}

#[test]
fn an_over_budget_read_of_a_chunked_catalog_l1_part_is_refused_within_the_budget() {
    serialized(assert_over_budget_refused(CHUNKED));
}

#[test]
fn an_over_budget_read_of_an_l1_part_without_provenance_is_refused_within_the_budget() {
    serialized(assert_over_budget_refused(UNMERGED));
}

/// A statement that fits the budget answers exactly what an unlimited
/// engine answers, and allocates no more than the budget held reserved plus
/// the fixed overhead. The two answers' debug renderings are compared whole:
/// they print every sample's value in the shortest form that parses back to
/// the same `f64`, so equal renderings are bitwise-equal values (the
/// fixture holds no NaN, the one value whose payload the rendering drops).
async fn assert_in_budget_answers(shape: Shape) {
    let (store, object_bytes) = l1_tenant(shape).await;
    for (query, budget) in [(SINGLE_RATE, IN_BUDGET), (FANOUT_RATE, IN_BUDGET)] {
        let unlimited = measure(&store, u64::MAX, query).await;
        let reference = unlimited.result.expect("an unlimited engine answers");
        let m = measure(&store, budget, query).await;
        eprintln!(
            "objects={object_bytes}B query={query}: peak allocated {} B, peak reserved {} B, \
             budget {budget} B, refusals {}; unlimited peak allocated {} B",
            m.peak_allocated, m.peak_reserved, m.refusals, unlimited.peak_allocated
        );
        let answer = m
            .result
            .unwrap_or_else(|e| panic!("{query} at {budget} B answers, got: {e}"));
        assert_eq!(answer, reference, "{query}: the same answer as unlimited");
        assert!(
            m.peak_allocated <= budget + FIXED_OVERHEAD,
            "{query}: the process allocated {} B at its peak against a {budget} B budget \
             (peak reserved {} B)",
            m.peak_allocated,
            m.peak_reserved
        );
        // Tighter than the budget: what the read allocated was charged,
        // beyond the fixed overhead and the evaluator's own output.
        let evaluation = evaluation_len(query, shape);
        assert!(
            m.peak_allocated <= m.peak_reserved + FIXED_OVERHEAD + evaluation,
            "{query}: the process allocated {} B at its peak but the budget held at most {} B \
             (plus {FIXED_OVERHEAD} B fixed and {evaluation} B of evaluation output)",
            m.peak_allocated,
            m.peak_reserved
        );
    }
}

#[test]
fn an_in_budget_read_of_a_whole_series_meta_l1_part_answers_as_unlimited() {
    serialized(assert_in_budget_answers(DENSE));
}

#[test]
fn an_in_budget_read_of_a_chunked_catalog_l1_part_answers_as_unlimited() {
    serialized(assert_in_budget_answers(CHUNKED));
}

#[test]
fn an_in_budget_read_of_an_l1_part_without_provenance_answers_as_unlimited() {
    serialized(assert_in_budget_answers(UNMERGED));
}

// Native histograms (issue #4198).
//
// `/api/v1/query_range` calls `QueryEngine::range_hist_with_budgets`, which
// runs `range_inner`; its `prefetch` reaches `prefetch_metric_plans`, then
// `fetch_samples_and_histograms_maybe_distributed` and
// `fetch_all_samples_and_histograms`, which hands each L1 part to
// `SegmentFetcher::fetch_soa_and_histograms_into`. Its `fetch_decoded`
// fetches a page batch, charges each decode group with `reserve_fetch`
// (`decode_groups`), and only then decodes it: `build_histogram_decodes`,
// `decode_histogram_run`, `ravel_segment::decode_run_histogram_pages`. That
// decode allocates one span vector and one bucket vector per sample, 8 B per
// entry, which the per-sample charge does not cover; the group charge adds
// a bound on them taken from the fetched pages' sizes.

const HIST_METRIC: &str = "request_duration_seconds";

/// A native-histogram tenant's shape. Each sample carries one span of
/// `buckets` integer buckets.
#[derive(Debug, Clone, Copy)]
struct HistShape {
    series: usize,
    buckets: usize,
}

/// Two series of 500-bucket histograms every 15 s. Each series' span and
/// bucket vectors hold about 11 MiB once decoded, more than the small
/// budget, while its fetched pages (about 3 MB), the scratch they decode
/// through, and its samples' per-sample charge (under 1 MiB) fit in it: a
/// read that charged the vectors a fixed few bytes per sample would admit
/// the decode.
const WIDE_HIST: HistShape = HistShape {
    series: 2,
    buckets: 500,
};

const HIST_INTERVAL_NS: i64 = 15 * NS_PER_SEC;

fn hist_series_labels(i: usize) -> (SeriesId, LabelSet) {
    let labels = LabelSet::new(vec![
        Label {
            name: "__name__".to_string(),
            value: HIST_METRIC.to_string(),
        },
        Label {
            name: "instance".to_string(),
            value: format!("instance-{i}"),
        },
    ])
    .expect("labels");
    let id = SeriesId::compute(&TenantId::new("t"), HIST_METRIC, &labels).expect("series id");
    (id, labels)
}

/// Series `i` at sample `k`: a counter histogram whose every bucket grows
/// by a small per-bucket step.
fn hist_value(i: usize, k: i64, buckets: usize) -> HistogramValue {
    let k = u64::try_from(k).expect("sample index");
    let positive: Vec<u64> = (0..buckets)
        .map(|j| k * (j as u64 % 3 + 1) + i as u64)
        .collect();
    HistogramValue {
        scale: 0,
        zero_threshold: 0.0,
        sum: Some(k as f64 * 0.25 * (i + 1) as f64),
        custom_values: None,
        positive_spans: vec![HistogramSpan {
            offset: 0,
            length: u32::try_from(buckets).expect("buckets"),
        }],
        negative_spans: vec![],
        counts: HistogramCounts::Int {
            zero_count: 0,
            count: positive.iter().sum(),
            positive,
            negative: vec![],
        },
        reset_hint: ResetHint::Unknown,
    }
}

/// `shape.series` histogram series over `SPAN_HOURS`, written as `FLUSHES`
/// L0 flushes that each hold a quarter of the span for every series, and
/// compacted into L1.
async fn l1_hist_tenant(shape: HistShape) -> (Arc<MemoryStore>, u64) {
    let store = Arc::new(MemoryStore::new());
    let per_flush = SPAN_HOURS * NS_PER_HOUR / HIST_INTERVAL_NS / FLUSHES;
    for flush in 0..FLUSHES {
        let inputs: Vec<SeriesInputV3> = (0..shape.series)
            .map(|i| {
                let (series_id, labels) = hist_series_labels(i);
                let samples = (flush * per_flush..(flush + 1) * per_flush)
                    .map(|k| HistogramSample {
                        ts_ns: hour_start() + k * HIST_INTERVAL_NS,
                        value: hist_value(i, k, shape.buckets),
                    })
                    .collect();
                SeriesInputV3 {
                    series_id,
                    labels,
                    values: SeriesValues::Histogram(samples),
                }
            })
            .collect();
        let seq = u64::try_from(flush).expect("flush") + 1;
        let written = SegmentWriter::write_histograms(inputs, l0_identity(seq), l0_bounds())
            .expect("write histogram L0");
        put_written_l0(&store, seq, written).await;
    }
    let (bytes, entries) = compact_l1(&store).await;
    assert_eq!(
        entries.len(),
        shape.series,
        "{shape:?}: every series compacted"
    );
    assert!(
        entries
            .iter()
            .all(|e| e.entry.value_kind == ValueKind::Histogram),
        "{shape:?}: every series is a histogram series"
    );
    (store, bytes)
}

const HIST_FANOUT: &str = "sum(histogram_count(rate(request_duration_seconds[5m])))";
const HIST_SINGLE: &str =
    r#"histogram_sum(rate(request_duration_seconds{instance="instance-1"}[5m]))"#;

/// A histogram read whose decoded spans and buckets alone need more than
/// the budget is refused with the budget's typed error, and the process
/// never allocates more than the budget plus the fixed overhead on the way:
/// the decode's span and bucket vectors are charged before they exist.
#[test]
fn an_over_budget_read_of_wide_native_histograms_is_refused_within_the_budget() {
    serialized(async {
        let shape = WIDE_HIST;
        let (store, object_bytes) = l1_hist_tenant(shape).await;
        let mut failures = Vec::new();
        for query in [HIST_FANOUT, HIST_SINGLE] {
            let m = measure(&store, SMALL_BUDGET, query).await;
            let figures = format!(
                "{query} over {shape:?} ({object_bytes} B of L1): peak allocated {} B, \
                 peak reserved {} B, budget {SMALL_BUDGET} B, {} refusals",
                m.peak_allocated, m.peak_reserved, m.refusals
            );
            eprintln!("{figures}");
            assert_budget_error(&m, query);
            assert!(m.refusals >= 1, "{query}: the budget counted the refusal");
            if m.peak_allocated > SMALL_BUDGET + FIXED_OVERHEAD {
                failures.push(figures);
            }
        }
        assert!(
            failures.is_empty(),
            "the process allocated past the budget plus {FIXED_OVERHEAD} B:\n{}",
            failures.join("\n")
        );
    });
}

/// What the evaluator holds for a histogram statement, uncharged: the inner
/// `rate`'s per-series histogram at every step, and as much again for the
/// vectors the outer function reads it as.
fn hist_evaluation_len(query: &str, shape: HistShape) -> u64 {
    let series = if query == HIST_SINGLE {
        1
    } else {
        shape.series as u64
    };
    let histogram = std::mem::size_of::<ravel_promql::FloatHistogram>()
        + std::mem::size_of::<ravel_promql::HistogramSample>()
        + shape.buckets * std::mem::size_of::<f64>()
        + std::mem::size_of::<HistogramSpan>();
    2 * series * STEPS * histogram as u64
}

/// A histogram read that fits the budget answers exactly what an unlimited
/// engine answers, and allocates no more than the budget held reserved plus
/// the fixed overhead and the evaluator's own output. The renderings are
/// compared whole, as in `assert_in_budget_answers`.
#[test]
fn an_in_budget_read_of_wide_native_histograms_answers_as_unlimited() {
    serialized(async {
        let shape = WIDE_HIST;
        let (store, object_bytes) = l1_hist_tenant(shape).await;
        for query in [HIST_SINGLE, HIST_FANOUT] {
            let unlimited = measure(&store, u64::MAX, query).await;
            let reference = unlimited.result.expect("an unlimited engine answers");
            let m = measure(&store, IN_BUDGET, query).await;
            eprintln!(
                "objects={object_bytes}B query={query}: peak allocated {} B, peak reserved {} B, \
                 budget {IN_BUDGET} B, refusals {}; unlimited peak allocated {} B",
                m.peak_allocated, m.peak_reserved, m.refusals, unlimited.peak_allocated
            );
            let answer = m
                .result
                .unwrap_or_else(|e| panic!("{query} at {IN_BUDGET} B answers, got: {e}"));
            assert_eq!(answer, reference, "{query}: the same answer as unlimited");
            assert!(
                m.peak_allocated <= IN_BUDGET + FIXED_OVERHEAD,
                "{query}: the process allocated {} B at its peak against a {IN_BUDGET} B budget",
                m.peak_allocated
            );
            let evaluation = hist_evaluation_len(query, shape);
            assert!(
                m.peak_allocated <= m.peak_reserved + FIXED_OVERHEAD + evaluation,
                "{query}: the process allocated {} B at its peak but the budget held at most \
                 {} B (plus {FIXED_OVERHEAD} B fixed and {evaluation} B of evaluation output)",
                m.peak_allocated,
                m.peak_reserved
            );
        }
    });
}
