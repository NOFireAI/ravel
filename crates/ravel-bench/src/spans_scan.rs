//! Spans SQL scan bench lane: measures the columnar fast path against the row
//! path over one shared corpus (issue #641, epic #630, ADR-0110 decision 7).
//!
//! Epic #630 gave the SQL spans scan a columnar fast path. It is eligible when
//! the projection excludes the `attrs` map column, no pending erasure predicate
//! applies, and the block carries no `attrs_raw` overflow page; otherwise the
//! row path runs unchanged. This lane drives a `SpansScanExec` twice over the
//! same block: once with a projection that EXCLUDES `attrs` (columnar path) and
//! once with a projection that INCLUDES it (row path), reporting rows/second and
//! decoded page bytes for each shape plus the ratio between them through the
//! standard bench provenance and report machinery rather than printing ad-hoc
//! numbers.
//!
//! Note on which "decoded" counter is comparable. The `pages_decoded`/
//! `pages_skipped` PARTITION METRICS (ADR-0110 decision 5) are written on both
//! paths since issue #669: the row shape reports every page of every block it
//! scanned as decoded and none skipped, because it requests every column. They
//! are page COUNTS, though, and a page's stored size varies by column, so the
//! quantity decision 7's regression test asserts on stays `page_bytes_decoded`
//! in `QueryAccounting` (decision 2): equal `page_bytes_fetched` on both shapes,
//! strictly lower `page_bytes_decoded` on the columnar shape. This lane reports
//! the partition metrics for transparency and computes the ratio on
//! `page_bytes_decoded`.
//!
//! The corpus MUST carry attributes and events, or the two shapes decode the
//! identical set of pages: with no dynamic attribute, `attrs_raw`, or event
//! pages there is nothing for the columnar path to skip, `page_bytes_decoded`
//! is equal on both shapes, and the ratio is a vacuous 1.0 that still reads as a
//! comparison. [`SpansScanConfig::attrs_per_span`] and
//! [`SpansScanConfig::events_per_span`] must therefore stay positive; the
//! acceptance test asserts the generator produces both.
//!
//! The lane asserts it exercised what it claims: the attrs-free shape must show
//! `columnar_batches > 0` and `rowpath_batches == 0`, the attrs-including shape
//! the reverse. A lane that silently measured the same path twice is worse than
//! no lane, because its number still looks like a comparison.
//!
//! Report-only: it never changes library behavior. Gated on the `sql-latency`
//! feature, like the other SQL bench lanes.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::time::Instant;

use datafusion::execution::TaskContext;
use datafusion::physical_plan::ExecutionPlan;
use futures::StreamExt;
use ravel_catalog::{SegmentLevel, SegmentRef};
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_rspan::record::{EVENTS_RAW_KEY, reconstruct_events_raw};
use ravel_rspan::{
    ObjectIdentity, RspanConfig, RspanWriter, SpanEvent, SpanQuery, SpanRecord, StatusCode,
};
use ravel_sql::{
    SPAN_COL_ATTRS, SPAN_COL_DURATION_NS, SPAN_COL_NAME, SPAN_COL_SERVICE_NAME, SPAN_COL_START_TS,
    SPAN_COL_TRACE_ID, SpanSegmentFetcher, SpansScanExec,
};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use serde::Serialize;
use uuid::Uuid;

const TENANT: TenantHash = TenantHash([7u8; 16]);

/// Inputs for one spans-scan measurement.
pub struct SpansScanConfig {
    pub store: Arc<dyn ObjectStoreBackend>,
    pub store_label: String,
    /// Total spans generated, split across [`records_per_object`](Self::records_per_object)
    /// RSPAN objects.
    pub spans: usize,
    /// Real (non-`_events_raw`) attributes carried by every span, beyond the
    /// lifted `service.name`. These become the dynamic attribute pages the
    /// columnar path skips. MUST be positive: an attribute-free corpus makes the
    /// two shapes decode identical pages and the ratio a vacuous 1.0.
    pub attrs_per_span: usize,
    /// Span events carried by every span, encoded through `_events_raw` into the
    /// nested event pages the columnar path skips. MUST be positive, for the same
    /// reason as [`attrs_per_span`](Self::attrs_per_span).
    pub events_per_span: usize,
    /// Spans per RSPAN object; the lever that sets the object count.
    pub records_per_object: usize,
    /// Writer block target. Small so each object carries several blocks.
    pub block_target_records: usize,
    /// Timed repetitions per shape; the first is the run whose deterministic
    /// path metrics are reported.
    pub runs: usize,
}

impl SpansScanConfig {
    /// A cheap fixture for the acceptance test and CI: a small corpus that still
    /// carries attributes and events on every span, so both shapes decode a real
    /// page difference.
    pub fn smoke(store: Arc<dyn ObjectStoreBackend>, store_label: &str) -> Self {
        SpansScanConfig {
            store,
            store_label: store_label.to_string(),
            spans: 200,
            attrs_per_span: 4,
            events_per_span: 2,
            records_per_object: 50,
            block_target_records: 8,
            runs: 2,
        }
    }
}

/// The projection for the columnar fast path: one column of every kind the fast
/// path builds, and NOT [`SPAN_COL_ATTRS`], so the query is eligible. This is
/// the attrs-free shape.
pub fn attrs_free_projection() -> Vec<usize> {
    vec![
        SPAN_COL_TRACE_ID,
        SPAN_COL_NAME,
        SPAN_COL_START_TS,
        SPAN_COL_DURATION_NS,
        SPAN_COL_SERVICE_NAME,
    ]
}

/// The projection for the row path: the attrs-free columns plus
/// [`SPAN_COL_ATTRS`]. Including the `attrs` map column makes the query
/// ineligible for the fast path, so the unchanged row path runs.
pub fn attrs_including_projection() -> Vec<usize> {
    let mut p = attrs_free_projection();
    p.push(SPAN_COL_ATTRS);
    p
}

/// One shape's measurement: which path ran (via the partition metrics), how many
/// pages it decoded, and its throughput.
#[derive(Serialize)]
pub struct ShapeResult {
    /// Human label: `"attrs-free (columnar)"` or `"attrs-including (row)"`.
    pub shape: String,
    /// The projected column ids driven into the scan.
    pub projection: Vec<usize>,
    /// Whether this shape includes the `attrs` map column (the eligibility axis).
    pub includes_attrs: bool,
    /// Batches the scan emitted through the columnar fast path (ADR-0110
    /// decision 5). Read from the scan's partition metrics, not inferred.
    pub columnar_batches: usize,
    /// Batches the scan emitted through the unchanged row path.
    pub rowpath_batches: usize,
    /// The `pages_decoded` PARTITION METRIC (ADR-0110 decision 5), written on
    /// both paths (#669): on the row shape it is every page of every block the
    /// scan decoded, on the columnar shape only the projected ones. Comparable
    /// across shapes as a count; the byte-weighted comparison the ratio uses is
    /// [`page_bytes_decoded`](Self::page_bytes_decoded).
    pub pages_decoded: usize,
    /// The `pages_skipped` partition metric. Nonzero on the columnar shape,
    /// which walks past the attribute and event pages; 0 on the row shape, which
    /// requests every column and so skips nothing.
    pub pages_skipped: usize,
    /// Stored bytes of the pages the scan actually fetched, from
    /// `QueryAccounting` (ADR-0107 decision 4). Recorded on BOTH paths and, per
    /// ADR-0110 decision 2, IDENTICAL across the two shapes: the object is
    /// fetched whole either way.
    pub page_bytes_fetched: u64,
    /// Stored bytes of the pages the scan actually DECODED, from
    /// `QueryAccounting`. Recorded on both paths and the quantity ADR-0110
    /// decision 7 asserts on: strictly lower on the columnar (attrs-free) shape,
    /// because it skips the attribute/event page decode the row shape performs.
    pub page_bytes_decoded: u64,
    /// Rows the scan returned (identical across shapes by construction).
    pub rows: usize,
    pub runs_taken: usize,
    pub min_ms: f64,
    pub median_ms: f64,
    pub max_ms: f64,
    pub stddev_ms: f64,
    pub samples_ms: Vec<f64>,
    /// Rows per second, from [`rows`](Self::rows) and the median latency.
    pub rows_per_sec: f64,
}

/// The corpus shape, stated in the report so a null result cannot hide behind an
/// attribute-free corpus.
#[derive(Serialize)]
pub struct CorpusShape {
    pub spans: usize,
    pub attrs_per_span: usize,
    pub events_per_span: usize,
    pub objects: usize,
    pub records_per_object: usize,
    pub block_target_records: usize,
}

#[derive(Serialize)]
pub struct ReportConfig {
    pub store: String,
    pub runs: usize,
    pub cores: usize,
    pub profile: String,
}

#[derive(Serialize)]
pub struct Report {
    pub config: ReportConfig,
    pub corpus: CorpusShape,
    pub attrs_free: ShapeResult,
    pub attrs_including: ShapeResult,
    /// `attrs_including.page_bytes_decoded / attrs_free.page_bytes_decoded`, the
    /// cross-path decoded-bytes ratio (ADR-0110 decision 7). Above 1.0 when the
    /// columnar shape decoded fewer page bytes than the row shape; 1.0 means a
    /// vacuous corpus with no pages to skip. This uses `page_bytes_decoded`, not
    /// the `pages_decoded` partition metric: that one counts pages, whose stored
    /// sizes differ by column, so a count ratio would not weight the skipped
    /// attribute and event pages by what they actually cost to decode.
    pub page_bytes_decoded_ratio: f64,
    /// `attrs_free.rows_per_sec / attrs_including.rows_per_sec`. Reported as a
    /// measured ratio, never described as "faster": interpret it against the
    /// observed noise band, not as a verdict.
    pub rows_per_sec_ratio: f64,
}

/// Generate the corpus spans: `config.spans` spans, each carrying
/// `config.attrs_per_span` real attributes (plus the lifted `service.name`) and
/// `config.events_per_span` events. Trace ids ascend so the writer's
/// `(trace_id, start_ts)` sort has real ordering to preserve.
///
/// The single line whose flip empties this of attributes and events is the
/// `attrs_per_span`/`events_per_span` config the caller passes: set either to 0
/// and the corresponding pages vanish. The acceptance test flips exactly that to
/// prove the assertion is load-bearing.
pub fn generate_spans(config: &SpansScanConfig) -> Vec<SpanRecord> {
    let services = ["checkout", "payments", "inventory", "search"];
    let mut spans = Vec::with_capacity(config.spans);
    for i in 0..config.spans {
        let trace = (i as u128).to_be_bytes();
        let start = 1_000i64 + i as i64 * 1_000;
        let service = services[i % services.len()];

        let mut attrs: Vec<(String, String)> = Vec::new();
        attrs.push(("service.name".to_string(), service.to_string()));
        for a in 0..config.attrs_per_span {
            attrs.push((format!("attr.key.{a}"), format!("value-{i}-{a}")));
        }
        if config.events_per_span > 0 {
            let events: Vec<SpanEvent> = (0..config.events_per_span)
                .map(|e| SpanEvent {
                    ts_ns: start + e as i64,
                    name: format!("event-{e}"),
                    // Arbitrary non-empty payload; the writer keeps it verbatim
                    // and `reconstruct_events_raw` frames it so it round-trips.
                    attrs_blob: format!("event-{i}-{e}-payload").into_bytes(),
                })
                .collect();
            attrs.push((EVENTS_RAW_KEY.to_string(), reconstruct_events_raw(&events)));
        }

        spans.push(SpanRecord {
            trace_id: trace,
            span_id: [0u8; 8],
            parent_span_id: None,
            name: format!("span-{i}"),
            start_ts_ns: start,
            end_ts_ns: start + 100,
            status_code: StatusCode::Ok,
            status_message: Some(format!("msg {i}")),
            attrs,
        });
    }
    spans
}

/// Write the generated spans into `config.records_per_object`-span RSPAN
/// objects in the store and return the matching L0 segment refs. Returns the
/// segment set and the object count.
async fn build_corpus(config: &SpansScanConfig) -> (Vec<SegmentRef>, usize) {
    let spans = generate_spans(config);
    let per_object = config.records_per_object.max(1);
    let cfg = RspanConfig {
        block_target_records: config.block_target_records.max(1),
        ..RspanConfig::default()
    };

    let mut segments = Vec::new();
    for (obj_idx, chunk) in spans.chunks(per_object).enumerate() {
        let writer_seq = (obj_idx + 1) as u64;
        let identity = ObjectIdentity {
            tenant_hash: TENANT.0,
            shard: 0,
            writer_id: *Uuid::from_u128(0x7300_0100 + obj_idx as u128).as_bytes(),
            writer_epoch: 1,
            writer_seq,
        };
        let mut w = RspanWriter::new(cfg, identity);
        for r in chunk {
            w.push(r.clone());
        }
        let bytes = w.finish().expect("finish object");
        let size = bytes.len() as u64;
        let key = format!("spans/obj-{obj_idx}.rspan");
        config
            .store
            .put(&key, bytes::Bytes::from(bytes), PutOptions::default())
            .await
            .expect("put object");

        let min = chunk.iter().map(|r| r.start_ts_ns).min().expect("nonempty");
        let max = chunk.iter().map(|r| r.end_ts_ns).max().expect("nonempty");
        segments.push(SegmentRef {
            data_object_key: key,
            object_size: size,
            min_event_ts_ns: min,
            max_event_ts_ns: max,
            ingest_hour_bucket: 0,
            sample_count: chunk.len() as u64,
            series_count: 0,
            shard: 0,
            content_hash: [0u8; 32],
            writer_id: Uuid::from_u128(0x7300_0100 + obj_idx as u128),
            writer_epoch: 1,
            writer_seq,
            created_unix_ns: 0,
            level: SegmentLevel::L0,
            segment_format_version: u32::from(ravel_rspan::footer::VERSION),
            declared_column_stats: Default::default(),
        });
    }
    let objects = segments.len();
    (segments, objects)
}

/// The scan's partition metrics after a drained execution.
struct PathMetrics {
    columnar_batches: usize,
    rowpath_batches: usize,
    pages_decoded: usize,
    pages_skipped: usize,
}

fn read_metrics(scan: &SpansScanExec) -> PathMetrics {
    let metrics = scan.metrics().expect("scan metrics");
    let count = |name: &str| metrics.sum_by_name(name).map(|v| v.as_usize()).unwrap_or(0);
    PathMetrics {
        columnar_batches: count("columnar_batches"),
        rowpath_batches: count("rowpath_batches"),
        pages_decoded: count("pages_decoded"),
        pages_skipped: count("pages_skipped"),
    }
}

/// Build a single-partition `SpansScanExec` over `segments` with `projection`
/// and no pending erasure, matching the whole ts range. A single partition keeps
/// the emitted order deterministic and the two shapes comparable. `accounting`
/// is threaded in so the caller can read the scan's `page_bytes_*` fold after
/// the drain.
fn build_scan(
    store: Arc<dyn ObjectStoreBackend>,
    segments: &[SegmentRef],
    projection: Vec<usize>,
    accounting: QueryAccounting,
) -> SpansScanExec {
    SpansScanExec::new(
        TENANT,
        SpanSegmentFetcher::new(store),
        segments,
        1,
        SpanQuery::ts_range(i64::MIN, i64::MAX),
        None,
        None,
        None,
        None,
        Arc::new(Vec::new()),
        Some(projection),
        accounting,
    )
    .expect("build scan")
}

fn percentile(sorted_ns: &[u64], pct: f64) -> u64 {
    if sorted_ns.is_empty() {
        return 0;
    }
    let rank = ((sorted_ns.len() - 1) as f64 * pct).round() as usize;
    sorted_ns[rank.min(sorted_ns.len() - 1)]
}

fn stddev_ns(samples_ns: &[u64]) -> f64 {
    if samples_ns.len() < 2 {
        return 0.0;
    }
    let n = samples_ns.len() as f64;
    let mean = samples_ns.iter().map(|&v| v as f64).sum::<f64>() / n;
    let variance = samples_ns
        .iter()
        .map(|&v| {
            let d = v as f64 - mean;
            d * d
        })
        .sum::<f64>()
        / (n - 1.0);
    variance.sqrt()
}

/// Time `runs` fresh-scan executions of one shape and read the deterministic
/// path metrics from the first. Each iteration builds a fresh scan so no run is
/// served warm by a previous one's state.
async fn measure_shape(
    store: Arc<dyn ObjectStoreBackend>,
    segments: &[SegmentRef],
    shape: &str,
    projection: Vec<usize>,
    includes_attrs: bool,
    runs: usize,
) -> ShapeResult {
    let mut samples_ns = Vec::with_capacity(runs.max(1));
    let mut rows = 0usize;
    let mut metrics = PathMetrics {
        columnar_batches: 0,
        rowpath_batches: 0,
        pages_decoded: 0,
        pages_skipped: 0,
    };
    let mut page_bytes_fetched = 0u64;
    let mut page_bytes_decoded = 0u64;

    for i in 0..runs.max(1) {
        // A fresh accounting handle per run; only the first run's snapshot is
        // reported (the fold is deterministic across identical runs).
        let accounting = QueryAccounting::new();
        let scan = build_scan(
            Arc::clone(&store),
            segments,
            projection.clone(),
            accounting.clone(),
        );
        let ctx = Arc::new(TaskContext::default());
        let start = Instant::now();
        let mut stream = scan.execute(0, ctx).expect("execute scan");
        let mut n = 0usize;
        while let Some(next) = stream.next().await {
            n += next.expect("batch").num_rows();
        }
        samples_ns.push(start.elapsed().as_nanos() as u64);
        drop(stream);
        if i == 0 {
            rows = n;
            metrics = read_metrics(&scan);
            let snap = accounting.snapshot();
            page_bytes_fetched = snap.page_bytes_fetched;
            page_bytes_decoded = snap.page_bytes_decoded;
        } else {
            assert_eq!(n, rows, "spans scan row count is deterministic across runs");
        }
    }

    let runs_taken = samples_ns.len();
    samples_ns.sort_unstable();
    let median_ns = percentile(&samples_ns, 0.50);
    let min_ns = samples_ns.first().copied().unwrap_or(0);
    let max_ns = samples_ns.last().copied().unwrap_or(0);
    let stddev = stddev_ns(&samples_ns);
    let rows_per_sec = if median_ns == 0 {
        0.0
    } else {
        rows as f64 / (median_ns as f64 / 1e9)
    };

    ShapeResult {
        shape: shape.to_string(),
        projection,
        includes_attrs,
        columnar_batches: metrics.columnar_batches,
        rowpath_batches: metrics.rowpath_batches,
        pages_decoded: metrics.pages_decoded,
        pages_skipped: metrics.pages_skipped,
        page_bytes_fetched,
        page_bytes_decoded,
        rows,
        runs_taken,
        min_ms: min_ns as f64 / 1e6,
        median_ms: median_ns as f64 / 1e6,
        max_ms: max_ns as f64 / 1e6,
        stddev_ms: stddev / 1e6,
        samples_ms: samples_ns.iter().map(|&ns| ns as f64 / 1e6).collect(),
        rows_per_sec,
    }
}

fn build_profile() -> &'static str {
    if cfg!(debug_assertions) {
        "debug"
    } else {
        "release"
    }
}

fn available_cores() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
}

/// Build the corpus once, measure both eligibility shapes over it, and return
/// the full report. Asserts that each shape actually took the path it claims:
/// the attrs-free shape must run columnar with no row-path fallback, and the
/// attrs-including shape the reverse. A lane that measured the same path twice
/// reports a number that reads as a comparison but is not one.
pub async fn run(config: &SpansScanConfig) -> Report {
    assert!(
        config.attrs_per_span > 0 && config.events_per_span > 0,
        "the corpus must carry attributes and events, or both shapes decode the \
         same pages and the ratio is a vacuous 1.0 (attrs_per_span={}, events_per_span={})",
        config.attrs_per_span,
        config.events_per_span,
    );

    let (segments, objects) = build_corpus(config).await;

    let attrs_free = measure_shape(
        Arc::clone(&config.store),
        &segments,
        "attrs-free (columnar)",
        attrs_free_projection(),
        false,
        config.runs,
    )
    .await;
    let attrs_including = measure_shape(
        Arc::clone(&config.store),
        &segments,
        "attrs-including (row)",
        attrs_including_projection(),
        true,
        config.runs,
    )
    .await;

    // The lane asserts it exercised both paths. Without this, a regression that
    // routed both shapes down one path would still emit a plausible ratio.
    assert!(
        attrs_free.columnar_batches > 0 && attrs_free.rowpath_batches == 0,
        "attrs-free shape must take the columnar fast path only \
         (columnar_batches={}, rowpath_batches={})",
        attrs_free.columnar_batches,
        attrs_free.rowpath_batches,
    );
    assert!(
        attrs_including.rowpath_batches > 0 && attrs_including.columnar_batches == 0,
        "attrs-including shape must take the row path only \
         (columnar_batches={}, rowpath_batches={})",
        attrs_including.columnar_batches,
        attrs_including.rowpath_batches,
    );

    let page_bytes_decoded_ratio = if attrs_free.page_bytes_decoded == 0 {
        0.0
    } else {
        attrs_including.page_bytes_decoded as f64 / attrs_free.page_bytes_decoded as f64
    };
    let rows_per_sec_ratio = if attrs_including.rows_per_sec == 0.0 {
        0.0
    } else {
        attrs_free.rows_per_sec / attrs_including.rows_per_sec
    };

    Report {
        config: ReportConfig {
            store: config.store_label.clone(),
            runs: config.runs,
            cores: available_cores(),
            profile: build_profile().to_string(),
        },
        corpus: CorpusShape {
            spans: config.spans,
            attrs_per_span: config.attrs_per_span,
            events_per_span: config.events_per_span,
            objects,
            records_per_object: config.records_per_object,
            block_target_records: config.block_target_records,
        },
        attrs_free,
        attrs_including,
        page_bytes_decoded_ratio,
        rows_per_sec_ratio,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ravel_object_store::memory::MemoryStore;

    /// The acceptance test (issue #641): the lane's configuration emits both
    /// eligibility shapes, and its corpus generator produces spans carrying both
    /// attributes and events.
    ///
    /// The eligibility axis is the presence of [`SPAN_COL_ATTRS`] in the
    /// projection: the attrs-free projection must exclude it (columnar path), the
    /// attrs-including projection must include it (row path). The corpus must
    /// carry real attributes AND events, or there are no dynamic-attribute or
    /// event pages to skip and the two shapes decode identically.
    ///
    /// To see this fail, point the generator at an attribute-free corpus by
    /// flipping the `attrs_per_span: 4` line in the `config` below to
    /// `attrs_per_span: 0`: the "carries attributes" assertion (`real_attrs > 0`)
    /// goes red on span 0. Flipping `events_per_span: 2` to `0` reddens the
    /// "carries events" assertion the same way. The assertions use hard `> 0`
    /// thresholds, not `>= config.*`, precisely so a zeroed config cannot make
    /// them vacuously pass. Restore to the positive values to make it pass.
    #[test]
    fn lane_covers_both_eligibility_shapes() {
        // Both shapes: the eligibility axis is SPAN_COL_ATTRS.
        let free = attrs_free_projection();
        let including = attrs_including_projection();
        assert!(
            !free.contains(&SPAN_COL_ATTRS),
            "the attrs-free (columnar) shape must exclude the attrs column"
        );
        assert!(
            including.contains(&SPAN_COL_ATTRS),
            "the attrs-including (row) shape must include the attrs column"
        );
        assert_ne!(
            free, including,
            "the two shapes must differ, or the lane measures one path twice"
        );

        // Corpus generator: every span carries attributes and events.
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let config = SpansScanConfig {
            attrs_per_span: 4,
            events_per_span: 2,
            ..SpansScanConfig::smoke(store, "memory")
        };
        let spans = generate_spans(&config);
        assert_eq!(spans.len(), config.spans, "generator honors the span count");
        for (i, s) in spans.iter().enumerate() {
            // A real attribute, distinct from service.name and _events_raw.
            let real_attrs = s
                .attrs
                .iter()
                .filter(|(k, _)| k != "service.name" && k != EVENTS_RAW_KEY)
                .count();
            assert!(
                real_attrs > 0,
                "span {i} must carry attributes (got {real_attrs})"
            );
            assert_eq!(
                real_attrs, config.attrs_per_span,
                "generator honors the attribute count for span {i}"
            );
            // Events, encoded through _events_raw and parseable back to a
            // non-empty event set.
            let raw = s
                .attrs
                .iter()
                .find(|(k, _)| k == EVENTS_RAW_KEY)
                .map(|(_, v)| v.clone());
            let events = raw
                .as_deref()
                .and_then(ravel_rspan::record::parse_events)
                .unwrap_or_default();
            assert!(!events.is_empty(), "span {i} must carry events");
            assert_eq!(
                events.len(),
                config.events_per_span,
                "generator honors the event count for span {i}"
            );
        }
    }

    /// End-to-end: the lane's smoke run drives both paths over the same corpus,
    /// the path metrics prove which ran, and the columnar shape decodes strictly
    /// fewer page BYTES than the row shape (ADR-0110 decision 7's quantity),
    /// while both fetch the same page bytes (decision 2).
    #[tokio::test]
    async fn smoke_run_exercises_both_paths_and_skips_pages() {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let config = SpansScanConfig::smoke(store, "memory");
        let report = run(&config).await;

        assert!(report.attrs_free.columnar_batches > 0);
        assert_eq!(report.attrs_free.rowpath_batches, 0);
        assert!(report.attrs_including.rowpath_batches > 0);
        assert_eq!(report.attrs_including.columnar_batches, 0);

        assert_eq!(
            report.attrs_free.rows, report.attrs_including.rows,
            "both shapes return the same rows"
        );
        // The `pages_decoded`/`pages_skipped` partition metrics, on both paths
        // (#669). Both shapes scan the same blocks over the same corpus, so the
        // row shape's decoded count equals the columnar shape's decoded plus
        // skipped: the same pages, split differently by the projection.
        assert!(
            report.attrs_free.pages_skipped > 0,
            "the columnar shape must skip attribute/event pages"
        );
        assert_eq!(
            report.attrs_including.pages_skipped, 0,
            "the row shape requests every column, so it skips no page"
        );
        // Holds because this corpus's attrs-including shape takes the row path
        // directly. A shape whose columnar attempt fell back on an `attrs_raw`
        // block would carry that attempt's counts too and read higher (#669).
        assert_eq!(
            report.attrs_including.pages_decoded,
            report.attrs_free.pages_decoded + report.attrs_free.pages_skipped,
            "the row shape decodes exactly the pages the columnar shape decoded \
             plus the ones it skipped"
        );
        assert!(
            report.attrs_including.pages_decoded > 0,
            "the row shape must report the pages it decoded, not an unwritten 0"
        );

        // The cross-path decoded quantity is `page_bytes_decoded` (QueryAccounting),
        // recorded on both paths: equal fetched bytes, strictly fewer decoded on
        // the columnar shape.
        assert_eq!(
            report.attrs_free.page_bytes_fetched, report.attrs_including.page_bytes_fetched,
            "both shapes fetch the same page bytes (ADR-0110 decision 2)"
        );
        assert!(
            report.attrs_including.page_bytes_decoded > report.attrs_free.page_bytes_decoded,
            "the row shape decodes strictly more page bytes than the columnar \
             shape (row={}, columnar={})",
            report.attrs_including.page_bytes_decoded,
            report.attrs_free.page_bytes_decoded,
        );
        assert!(report.page_bytes_decoded_ratio > 1.0);
    }
}
