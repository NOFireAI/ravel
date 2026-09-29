//! Acceptance coverage for `ravel-cli export --signal metrics` (ADR-1751
//! decisions 4 and 5, issue #1712).
//!
//! Drives the real `load_metrics` and `export_metrics` entry points in-process
//! against a shared `MemoryStore`, for the reason `tests/export_logs.rs` gives:
//! a subprocess against `--store memory` gets its own empty store. What the
//! store holds is read back through ravel-query's own `SegmentFetcher`, so the
//! assertions compare stored series ids and sample bit patterns, not a
//! rendering of them.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use arrow::array::{Array, ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::record_batch::RecordBatch;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_cli::erase;
use ravel_cli::export::{self, MetricsExportReport};
use ravel_cli::load::{self, MetricsMapping};
use ravel_cli::maintain::SignalArg;
use ravel_cli::store::{StoreKind, StoreSelection};
use ravel_ingest::{
    Clock, IngestConfig, IngestPoint, IngestRouter, IngestValue, SystemClock, WriteMode,
};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::instrument::{InstrumentedStore, StoreMetricsSnapshot};
use ravel_object_store::memory::MemoryStore;
use ravel_query::SegmentFetcher;
use ravel_query::http::{AppState, StaticBearerTokenResolver, router};
use ravel_query::{EngineConfig, QueryEngine};
use ravel_segment::{HistogramCounts, HistogramSample, HistogramSpan, HistogramValue, ResetHint};
use ravel_types::{LabelSet, METRIC_NAME_LABEL, SeriesId, Signal, TenantId, TimeRange};
use serde_json::Value;
use tower::ServiceExt;
use uuid::Uuid;

/// A fixed, plausible base clock, as in `tests/export_logs.rs`: the RSEG
/// flush buckets by this reading, so a window near it reaches a known hour.
const BASE_NS: i64 = 1_700_000_000_000_000_000; // 2023-11-14T22:13:20Z
const ONE_SEC_NS: i64 = 1_000_000_000;
const T0: i64 = BASE_NS;
const T1: i64 = BASE_NS + ONE_SEC_NS;
const T2: i64 = BASE_NS + 2 * ONE_SEC_NS;
const T_OUT: i64 = BASE_NS - 10 * ONE_SEC_NS;
/// The clock every fixed-clock load and export runs at: after every event
/// time above, well inside the future-skew bound.
const LOAD_NS: i64 = BASE_NS + 10 * ONE_SEC_NS;
/// A NaN with a payload, which a value path that canonicalizes NaN loses.
const NAN_PAYLOAD_BITS: u64 = 0x7ff8_0000_0000_1234;
const TOKEN: &str = "test-token";

struct FixedClock(i64);
impl Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        self.0
    }
}

fn write_parquet(path: &Path, batch: &RecordBatch) {
    let file = std::fs::File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(batch).expect("write batch");
    writer.close().expect("close writer");
}

fn read_parquet(path: &Path) -> RecordBatch {
    let file = std::fs::File::open(path).expect("open exported parquet");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("reader builder")
        .build()
        .expect("build reader");
    let batches: Vec<RecordBatch> = reader.map(|b| b.expect("read batch")).collect();
    assert_eq!(batches.len(), 1, "expected exactly one output batch");
    batches.into_iter().next().expect("one batch")
}

fn metrics_mapping(text: &str) -> MetricsMapping {
    load::parse_metrics_mapping(text).expect("valid metrics mapping")
}

/// One source row: `(name, ts_ns, value, job, host)`.
type SourceRow<'a> = (&'a str, i64, f64, &'a str, &'a str);

/// Writes `rows` as a Parquet file with `name`, `ts`, `value`, `job_col` and
/// `host_col` columns and loads it into `tenant` under `mapping`.
async fn load_rows(
    store: &Arc<dyn ObjectStoreBackend>,
    dir: &Path,
    tenant: &str,
    mapping: &MetricsMapping,
    rows: &[SourceRow<'_>],
    load_now_ns: i64,
    clock: Arc<dyn Clock>,
) {
    let batch = RecordBatch::try_from_iter(vec![
        (
            "name".to_string(),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "ts".to_string(),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "value".to_string(),
            Arc::new(Float64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "job_col".to_string(),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.3).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "host_col".to_string(),
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.4).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
    ])
    .expect("source batch");
    let path = dir.join(format!("source-{tenant}-{load_now_ns}.parquet"));
    write_parquet(&path, &batch);
    load_file(store, &path, tenant, mapping, load_now_ns, clock).await;
}

async fn load_file(
    store: &Arc<dyn ObjectStoreBackend>,
    path: &Path,
    tenant: &str,
    mapping: &MetricsMapping,
    load_now_ns: i64,
    clock: Arc<dyn Clock>,
) -> u64 {
    let report = load::load_metrics(
        Arc::clone(store),
        path,
        tenant,
        mapping,
        1,
        10_000,
        0,
        1,
        1,
        1,
        None,
        load_now_ns,
        clock,
    )
    .await
    .expect("load succeeds");
    assert_eq!(
        report.rows_processed, report.file_total_rows,
        "every source row is loaded"
    );
    report.points_written
}

#[allow(clippy::too_many_arguments)]
async fn export_window(
    store: &Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    start_ns: i64,
    end_ns: i64,
    mapping: &MetricsMapping,
    out: &Path,
    now_ns: i64,
) -> anyhow::Result<MetricsExportReport> {
    export::export_metrics(
        Arc::clone(store),
        StoreSelection::explicit(StoreKind::Memory),
        tenant,
        start_ns,
        end_ns,
        mapping,
        out,
        1,
        None,
        now_ns,
    )
    .await
}

/// A series' full label set, `__name__` included, sorted by label name.
type LabelKey = Vec<(String, String)>;

fn label_key(labels: &LabelSet) -> LabelKey {
    labels
        .iter()
        .map(|l| (l.name.clone(), l.value.clone()))
        .collect()
}

/// What a tenant's metrics catalog holds over `[start_ns, end_ns)`, read
/// through ravel-query's fetcher with no deduplication: each series' stored
/// id, and every stored sample's value bits by `(series, ts)`.
struct Stored {
    ids: BTreeMap<LabelKey, SeriesId>,
    samples: BTreeMap<(LabelKey, i64), Vec<u64>>,
}

async fn stored(
    store: &Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    start_ns: i64,
    end_ns: i64,
    now_ns: i64,
) -> Stored {
    let catalog = Catalog::new(
        Arc::clone(store),
        CatalogConfig {
            shard_count: 1,
            ..CatalogConfig::default()
        },
    )
    .expect("catalog");
    let hash = TenantId::new(tenant).hash();
    let snapshot = catalog
        .resolve(
            &hash,
            Signal::Metrics,
            TimeRange { start_ns, end_ns },
            &[],
            now_ns,
        )
        .await
        .expect("resolve");
    let fetcher = SegmentFetcher::new(Arc::clone(store));
    let mut out = Stored {
        ids: BTreeMap::new(),
        samples: BTreeMap::new(),
    };
    for seg_ref in &snapshot.segments {
        let (runs, _) = fetcher.fetch_soa(hash, seg_ref, &[]).await.expect("fetch");
        for run in runs {
            let key = label_key(&run.labels);
            out.ids.insert(key.clone(), run.series_id);
            for (ts, value) in run.timestamps.iter().zip(&run.values) {
                if *ts >= start_ns && *ts < end_ns {
                    out.samples
                        .entry((key.clone(), *ts))
                        .or_default()
                        .push(value.to_bits());
                }
            }
        }
    }
    for bits in out.samples.values_mut() {
        bits.sort_unstable();
    }
    out
}

fn i64_values(batch: &RecordBatch, column: &str) -> Vec<i64> {
    batch
        .column_by_name(column)
        .unwrap_or_else(|| panic!("no {column} column"))
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap_or_else(|| panic!("{column} is not Int64"))
        .values()
        .to_vec()
}

fn f64_bits(batch: &RecordBatch, column: &str) -> Vec<u64> {
    let col = batch
        .column_by_name(column)
        .unwrap_or_else(|| panic!("no {column} column"))
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap_or_else(|| panic!("{column} is not Float64"));
    assert_eq!(col.null_count(), 0, "{column} has no nulls");
    col.values().iter().map(|v| v.to_bits()).collect()
}

fn str_values(batch: &RecordBatch, column: &str) -> Vec<Option<String>> {
    let col = batch
        .column_by_name(column)
        .unwrap_or_else(|| panic!("no {column} column"))
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap_or_else(|| panic!("{column} is not Utf8"));
    (0..col.len())
        .map(|i| (!col.is_null(i)).then(|| col.value(i).to_string()))
        .collect()
}

fn some(values: &[&str]) -> Vec<Option<String>> {
    values.iter().map(|v| Some((*v).to_string())).collect()
}

fn column_names(batch: &RecordBatch) -> Vec<String> {
    batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect()
}

/// A gauge mapping with a unit, so every stored name carries the `_seconds`
/// suffix, and a label name the sanitizer rewrites (`host.name`).
const GAUGE_MAPPING: &str = r#"
[metrics]
name_column = "name"
value_column = "value"
ts_column = "ts"
ts_unit = "nanos"
unit = "s"

[[metrics.label]]
name = "job"
column = "job_col"

[[metrics.label]]
name = "host.name"
column = "host_col"
"#;

/// Loads `rows` into tenant `alpha` under `mapping_text`, exports
/// `[T0, T2 + 1)` with the same mapping, loads the export into tenant `beta`,
/// and asserts both tenants hold the same series and the same sample bits.
/// Returns the export report and the exported batch for shape assertions.
async fn round_trip(
    mapping_text: &str,
    rows: &[SourceRow<'_>],
) -> (MetricsExportReport, RecordBatch) {
    let dir = tempfile::tempdir().expect("tempdir");
    let export_pq = dir.path().join("export.parquet");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let mapping = metrics_mapping(mapping_text);

    load_rows(
        &store,
        dir.path(),
        "alpha",
        &mapping,
        rows,
        LOAD_NS,
        Arc::new(FixedClock(LOAD_NS)),
    )
    .await;
    let report = export_window(&store, "alpha", T0, T2 + 1, &mapping, &export_pq, LOAD_NS)
        .await
        .expect("export succeeds");
    let points = load_file(
        &store,
        &export_pq,
        "beta",
        &mapping,
        LOAD_NS + ONE_SEC_NS,
        Arc::new(FixedClock(LOAD_NS + ONE_SEC_NS)),
    )
    .await;
    assert_eq!(
        points, report.rows_written,
        "every exported row re-loads as one point"
    );

    let now = LOAD_NS + ONE_SEC_NS;
    let alpha = stored(&store, "alpha", T0, T2 + 1, now).await;
    let beta = stored(&store, "beta", T0, T2 + 1, now).await;
    assert_eq!(
        beta.samples, alpha.samples,
        "the re-loaded tenant holds the same (series, ts) samples, bit for bit"
    );
    assert_eq!(
        beta.ids.keys().collect::<Vec<_>>(),
        alpha.ids.keys().collect::<Vec<_>>(),
        "the re-loaded tenant holds the same label sets"
    );
    let alpha_tenant = TenantId::new("alpha");
    for (key, beta_id) in &beta.ids {
        let labels = LabelSet::new(
            key.iter()
                .map(|(name, value)| ravel_types::Label {
                    name: name.clone(),
                    value: value.clone(),
                })
                .collect(),
        )
        .expect("label set");
        let name = labels.get(METRIC_NAME_LABEL).expect("__name__");
        // SeriesId hashes the tenant, so the id the re-loaded series would
        // carry in the source tenant is what must match the source's own id.
        assert_eq!(
            SeriesId::compute(&alpha_tenant, name, &labels).expect("series id"),
            alpha.ids[key],
            "series {key:?} re-loads onto the source's SeriesId"
        );
        assert_eq!(
            *beta_id,
            SeriesId::compute(&TenantId::new("beta"), name, &labels).expect("series id"),
            "the re-loaded tenant's stored id is the canonical one"
        );
    }
    let batch = read_parquet(&export_pq);
    (report, batch)
}

/// `load(export(window))` with the same mapping reproduces every stored
/// series id and every sample's bits, NaN payload and signed zero included,
/// under a gauge mapping whose unit suffix every stored name already carries.
#[tokio::test]
async fn load_then_export_round_trips_metrics_series_and_sample_bits() {
    let nan = f64::from_bits(NAN_PAYLOAD_BITS);
    // Shuffled so a passing test proves the export sorts by event time.
    let rows: [SourceRow<'_>; 8] = [
        ("mem", T2, 42.0, "db", "h1"),
        ("cpu.usage", T1, nan, "api", "h1"),
        ("cpu.usage", T0, -0.0, "api", "h2"),
        ("latency_seconds", T2, 0.25, "api", "h1"),
        ("cpu.usage", T_OUT, 9.0, "api", "h1"),
        ("mem", T0, f64::INFINITY, "", "h1"),
        ("cpu.usage", T0, 1.5, "api", "h1"),
        ("cpu.usage", T1, 0.0, "api", "h2"),
    ];
    let (report, batch) = round_trip(GAUGE_MAPPING, &rows).await;

    assert_eq!(
        report,
        MetricsExportReport {
            rows_written: 7,
            series_written: 5,
            series_skipped: 0,
            segments_read: 1,
            segments_pruned: 0,
            erasure_predicates: 0,
            samples_deduplicated: 0,
        }
    );
    assert_eq!(
        column_names(&batch),
        vec!["ts", "name", "value", "job_col", "host_col"],
        "exactly the mapped columns"
    );
    assert_eq!(i64_values(&batch, "ts"), vec![T0, T0, T0, T1, T1, T2, T2]);
    assert_eq!(
        str_values(&batch, "name"),
        some(&[
            "cpu_usage_seconds",
            "cpu_usage_seconds",
            "mem_seconds",
            "cpu_usage_seconds",
            "cpu_usage_seconds",
            "latency_seconds",
            "mem_seconds",
        ]),
        "each name is the stored one, which a load under unit = \"s\" leaves unchanged"
    );
    assert_eq!(
        f64_bits(&batch, "value"),
        vec![
            1.5f64.to_bits(),
            (-0.0f64).to_bits(),
            f64::INFINITY.to_bits(),
            NAN_PAYLOAD_BITS,
            0.0f64.to_bits(),
            0.25f64.to_bits(),
            42.0f64.to_bits(),
        ]
    );
    assert_eq!(
        str_values(&batch, "job_col"),
        vec![
            Some("api".to_string()),
            Some("api".to_string()),
            None,
            Some("api".to_string()),
            Some("api".to_string()),
            Some("api".to_string()),
            Some("db".to_string()),
        ],
        "the empty job cell was dropped at load, so it exports as null"
    );
    assert_eq!(
        str_values(&batch, "host_col"),
        some(&["h1", "h2", "h1", "h1", "h2", "h1", "h1"])
    );
}

/// Under `kind = "counter"` with a unit the stored name ends in
/// `<unit>_total`, which a load would suffix again; the export writes the
/// name less `_total`, and the re-load lands on the same series.
#[tokio::test]
async fn a_counter_with_a_unit_exports_the_name_less_total_and_round_trips() {
    const COUNTER_MAPPING: &str = r#"
[metrics]
name_column = "name"
value_column = "value"
ts_column = "ts"
ts_unit = "nanos"
unit = "By"
kind = "counter"

[[metrics.label]]
name = "job"
column = "job_col"

[[metrics.label]]
name = "host"
column = "host_col"
"#;
    let rows: [SourceRow<'_>; 3] = [
        ("net.rx", T0, 10.0, "api", "h1"),
        ("net_tx_bytes", T1, 20.0, "api", "h1"),
        ("net.rx", T2, 30.0, "api", "h1"),
    ];
    let (report, batch) = round_trip(COUNTER_MAPPING, &rows).await;

    assert_eq!(report.rows_written, 3);
    assert_eq!(report.series_written, 2);
    assert_eq!(
        str_values(&batch, "name"),
        some(&["net_rx_bytes", "net_tx_bytes", "net_rx_bytes"]),
        "stored net_rx_bytes_total and net_tx_bytes_total are written less _total"
    );
}

/// A `name` literal mapping writes no name column and exports only the
/// series the literal loads as; another metric in the window is skipped and
/// counted rather than written under the literal's name.
#[tokio::test]
async fn a_name_literal_mapping_exports_only_its_own_metric() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    load_rows(
        &store,
        dir.path(),
        "alpha",
        &metrics_mapping(GAUGE_MAPPING),
        &[
            ("cpu.usage", T0, 1.0, "api", "h1"),
            ("mem", T0, 2.0, "api", "h1"),
            ("cpu.usage", T1, 3.0, "api", "h1"),
        ],
        LOAD_NS,
        Arc::new(FixedClock(LOAD_NS)),
    )
    .await;
    let literal = metrics_mapping(
        r#"
[metrics]
name = "cpu.usage"
value_column = "value"
ts_column = "ts"
ts_unit = "nanos"
unit = "s"

[[metrics.label]]
name = "job"
column = "job_col"

[[metrics.label]]
name = "host.name"
column = "host_col"
"#,
    );
    let export_pq = dir.path().join("export.parquet");
    let report = export_window(&store, "alpha", T0, T2, &literal, &export_pq, LOAD_NS)
        .await
        .expect("export succeeds");
    assert_eq!(report.rows_written, 2);
    assert_eq!(report.series_written, 1);
    assert_eq!(report.series_skipped, 1, "mem_seconds is not the literal's");
    let batch = read_parquet(&export_pq);
    assert_eq!(
        column_names(&batch),
        vec!["ts", "value", "job_col", "host_col"],
        "a literal name writes no name column"
    );
    assert_eq!(
        f64_bits(&batch, "value"),
        vec![1.0f64.to_bits(), 3.0f64.to_bits()]
    );
}

/// Wall clock floored to a whole second, as in `tests/load_metrics.rs`: the
/// query handler's listing window runs to the system clock, so the loads that
/// the query path reads back must be bucketed near it.
fn wall_now_ns() -> i64 {
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch");
    let ns = i64::try_from(dur.as_nanos()).expect("time overflow");
    (ns / ONE_SEC_NS) * ONE_SEC_NS
}

fn query_app(store: Arc<dyn ObjectStoreBackend>, tenant: &TenantId) -> Router {
    let catalog = Arc::new(
        Catalog::new(
            Arc::clone(&store),
            CatalogConfig {
                shard_count: 1,
                ..CatalogConfig::default()
            },
        )
        .expect("catalog"),
    );
    let engine = Arc::new(QueryEngine::new(catalog, store, EngineConfig::default()));
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), tenant.clone());
    router(AppState::new(
        engine,
        Arc::new(StaticBearerTokenResolver::new(tokens)),
    ))
}

/// The value the query path serves for `query` at exactly `at_ns`.
async fn served_value(app: &Router, query: &str, at_ns: i64) -> f64 {
    let secs = at_ns / ONE_SEC_NS;
    let request = Request::builder()
        .method("GET")
        .uri(format!(
            "/api/v1/query_range?query={query}&start={secs}&end={secs}&step=1s"
        ))
        .header("authorization", format!("Bearer {TOKEN}"))
        .body(Body::empty())
        .expect("build request");
    let response = app.clone().oneshot(request).await.expect("oneshot");
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body");
    let json: Value = serde_json::from_slice(&body).expect("json");
    assert_eq!(status, StatusCode::OK, "query_range failed: {json}");
    let result = json["data"]["result"].as_array().expect("result array");
    assert_eq!(result.len(), 1, "one series in {json}");
    let values = result[0]["values"].as_array().expect("values array");
    assert_eq!(values.len(), 1, "one step in {json}");
    values[0][1]
        .as_str()
        .expect("value string")
        .parse::<f64>()
        .expect("value parses")
}

/// Two loads of the same `(series, ts)` export as one row, whether the two
/// samples carry the same bits or different ones, and the row carries the
/// bits the query path serves: the later write, under the provenance order.
/// The later write carries the smaller value, so a winner chosen by value
/// bits rather than provenance exports the earlier one.
#[tokio::test]
async fn duplicate_samples_export_as_the_one_the_query_path_serves() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let mapping = metrics_mapping(GAUGE_MAPPING);
    let now = wall_now_ns();
    let same_ts = now - 60 * ONE_SEC_NS;
    let differing_ts = now - 59 * ONE_SEC_NS;

    load_rows(
        &store,
        dir.path(),
        "alpha",
        &mapping,
        &[
            ("cpu", same_ts, 1.5, "api", "h1"),
            ("cpu", differing_ts, 3.5, "api", "h1"),
        ],
        now,
        Arc::new(SystemClock),
    )
    .await;
    load_rows(
        &store,
        dir.path(),
        "alpha",
        &mapping,
        &[
            ("cpu", same_ts, 1.5, "api", "h1"),
            ("cpu", differing_ts, 2.5, "api", "h1"),
        ],
        now + 1,
        Arc::new(SystemClock),
    )
    .await;

    let export_now = wall_now_ns() + ONE_SEC_NS;
    let window = (now - 120 * ONE_SEC_NS, now);
    let raw = stored(&store, "alpha", window.0, window.1, export_now).await;
    assert_eq!(
        raw.samples.values().map(Vec::len).sum::<usize>(),
        4,
        "the store holds both loads' samples before deduplication"
    );

    let export_pq = dir.path().join("export.parquet");
    let report = export_window(
        &store, "alpha", window.0, window.1, &mapping, &export_pq, export_now,
    )
    .await
    .expect("export succeeds");
    assert_eq!(report.rows_written, 2, "one row per (series, ts)");
    assert_eq!(report.samples_deduplicated, 2, "one loser per timestamp");
    assert_eq!(report.segments_read, 2, "one object per load");
    let batch = read_parquet(&export_pq);
    assert_eq!(i64_values(&batch, "ts"), vec![same_ts, differing_ts]);
    assert_eq!(
        f64_bits(&batch, "value"),
        vec![1.5f64.to_bits(), 2.5f64.to_bits()],
        "identical duplicates collapse, and the later, smaller write wins a differing pair"
    );

    let app = query_app(Arc::clone(&store), &TenantId::new("alpha"));
    for (ts, exported) in [(same_ts, 1.5f64), (differing_ts, 2.5f64)] {
        assert_eq!(
            served_value(&app, "cpu_seconds", ts).await.to_bits(),
            exported.to_bits(),
            "the exported sample at {ts} is the one the query path serves"
        );
    }
}

/// A pending selective-erasure request over one label value excludes that
/// series' samples from the export, although the raw object still holds them.
#[tokio::test]
async fn an_erased_series_is_not_exported() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let mapping = metrics_mapping(GAUGE_MAPPING);
    load_rows(
        &store,
        dir.path(),
        "alpha",
        &mapping,
        &[
            ("cpu", T0, 1.0, "keep", "h1"),
            ("cpu", T0, 2.0, "erase-me", "h1"),
            ("cpu", T1, 3.0, "keep", "h1"),
            ("cpu", T1, 4.0, "erase-me", "h1"),
        ],
        LOAD_NS,
        Arc::new(FixedClock(LOAD_NS)),
    )
    .await;
    erase::submit(
        Arc::clone(&store),
        "alpha",
        SignalArg::Metrics,
        vec![("job".to_string(), "erase-me".to_string())],
        0,
        0,
        "issue #1712 metrics erasure test".to_string(),
        Uuid::from_u128(0x1712_0003),
        LOAD_NS,
    )
    .await
    .expect("the erasure request is recorded");

    let export_pq = dir.path().join("export.parquet");
    let report = export_window(&store, "alpha", T0, T2, &mapping, &export_pq, LOAD_NS)
        .await
        .expect("export succeeds");
    assert_eq!(report.erasure_predicates, 1);
    assert_eq!(report.rows_written, 2, "both erased samples are excluded");
    assert_eq!(report.series_written, 1);
    let batch = read_parquet(&export_pq);
    assert_eq!(str_values(&batch, "job_col"), some(&["keep", "keep"]));
    assert_eq!(
        f64_bits(&batch, "value"),
        vec![1.0f64.to_bits(), 3.0f64.to_bits()]
    );
}

/// The window is half-open by event time: a sample at exactly `start` is
/// exported and one at exactly `end` is not.
#[tokio::test]
async fn a_sample_at_start_is_exported_and_one_at_end_is_not() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let mapping = metrics_mapping(GAUGE_MAPPING);
    load_rows(
        &store,
        dir.path(),
        "alpha",
        &mapping,
        &[
            ("cpu", T0 - 1, 1.0, "api", "h1"),
            ("cpu", T0, 2.0, "api", "h1"),
            ("cpu", T1 - 1, 3.0, "api", "h1"),
            ("cpu", T1, 4.0, "api", "h1"),
        ],
        LOAD_NS,
        Arc::new(FixedClock(LOAD_NS)),
    )
    .await;
    let export_pq = dir.path().join("export.parquet");
    let report = export_window(&store, "alpha", T0, T1, &mapping, &export_pq, LOAD_NS)
        .await
        .expect("export succeeds");
    assert_eq!(report.rows_written, 2);
    let batch = read_parquet(&export_pq);
    assert_eq!(i64_values(&batch, "ts"), vec![T0, T1 - 1]);
    assert_eq!(
        f64_bits(&batch, "value"),
        vec![2.0f64.to_bits(), 3.0f64.to_bits()]
    );
}

/// A `[metrics.histogram]` mapping is refused by name before any object-store
/// request.
#[tokio::test]
async fn a_classic_histogram_mapping_is_refused_before_any_store_request() {
    let dir = tempfile::tempdir().expect("tempdir");
    let instrumented = Arc::new(InstrumentedStore::new(MemoryStore::new()));
    let metrics = instrumented.metrics();
    let store: Arc<dyn ObjectStoreBackend> = instrumented;
    let mapping = metrics_mapping(
        r#"
[metrics]
name = "rpc_duration"
value_column = "value"
ts_column = "ts"
ts_unit = "nanos"

[metrics.histogram]
le_column = "le"
sum_column = "sum"
count_column = "count"
"#,
    );
    let out = dir.path().join("out.parquet");
    let err = export_window(&store, "alpha", T0, T1, &mapping, &out, LOAD_NS)
        .await
        .expect_err("a histogram mapping is refused");
    assert_eq!(
        err.to_string(),
        "export --signal metrics cannot write a mapping with [metrics.histogram]: a load explodes \
         each of its rows into _bucket, _sum and _count series and accumulates the bucket \
         counts, and the stored series do not record which of them were one data point, so no \
         file in that shape re-loads onto the same series. Export the exploded series with a \
         scalar mapping instead (no [metrics.histogram], no unit, no kind, name_column for the \
         metric name and a [[metrics.label]] for le); loading that file with the same scalar \
         mapping reproduces the same series and samples."
    );
    assert_eq!(err.to_string(), export::HISTOGRAM_MAPPING_REFUSAL);
    assert_eq!(metrics.snapshot(), StoreMetricsSnapshot::default());
    assert!(!out.exists(), "a refused export writes no file");
}

/// Loads one `cpu` sample under a mapping with no unit, then exports it under
/// `export_mapping`, returning the refusal.
async fn refused_export(export_mapping: &str, ts_ns: i64) -> String {
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let plain = metrics_mapping(
        r#"
[metrics]
name_column = "name"
value_column = "value"
ts_column = "ts"
ts_unit = "nanos"

[[metrics.label]]
name = "job"
column = "job_col"

[[metrics.label]]
name = "host"
column = "host_col"
"#,
    );
    load_rows(
        &store,
        dir.path(),
        "alpha",
        &plain,
        &[("cpu", ts_ns, 1.0, "api", "h1")],
        LOAD_NS,
        Arc::new(FixedClock(LOAD_NS)),
    )
    .await;
    let out = dir.path().join("out.parquet");
    let err = export_window(
        &store,
        "alpha",
        T0,
        T2,
        &metrics_mapping(export_mapping),
        &out,
        LOAD_NS,
    )
    .await
    .expect_err("the export is refused");
    assert!(!out.exists(), "a refused export writes no file");
    err.to_string()
}

/// A stored name no `name_column` value loads back as under the export's
/// unit and kind is refused, naming the series and what a load would do.
#[tokio::test]
async fn a_name_the_mapping_would_suffix_again_is_refused() {
    let err = refused_export(
        r#"
[metrics]
name_column = "name"
value_column = "value"
ts_column = "ts"
ts_unit = "nanos"
unit = "s"

[[metrics.label]]
name = "job"
column = "job_col"

[[metrics.label]]
name = "host"
column = "host_col"
"#,
        T0,
    )
    .await;
    assert_eq!(
        err,
        "series cpu{host=\"h1\", job=\"api\"} cannot be exported under this mapping: no \
         name_column value loads back as \"cpu\" with unit = \"s\" and kind = \"gauge\" (written \
         as \"cpu\", a load names it \"cpu_seconds\"), so the exported file would re-load onto a \
         different series. Export it with the unit and kind it was loaded with; a mapping with \
         neither loads every sanitized metric name back unchanged."
    );
}

/// A stored label the mapping does not name is refused, since the re-load
/// would drop it.
#[tokio::test]
async fn a_label_the_mapping_does_not_name_is_refused() {
    let err = refused_export(
        r#"
[metrics]
name_column = "name"
value_column = "value"
ts_column = "ts"
ts_unit = "nanos"

[[metrics.label]]
name = "job"
column = "job_col"
"#,
        T0,
    )
    .await;
    assert_eq!(
        err,
        "series cpu{host=\"h1\", job=\"api\"} carries the label \"host\", which no \
         [[metrics.label]] in the mapping names; a load of the exported file would drop it and \
         land the samples on a different series. Add a [[metrics.label]] for it."
    );
}

/// A sample whose timestamp is not a whole number of the mapping's `ts_unit`
/// is refused rather than truncated onto another timestamp.
#[tokio::test]
async fn a_sample_finer_than_the_ts_unit_is_refused() {
    let err = refused_export(
        r#"
[metrics]
name_column = "name"
value_column = "value"
ts_column = "ts"
ts_unit = "millis"

[[metrics.label]]
name = "job"
column = "job_col"

[[metrics.label]]
name = "host"
column = "host_col"
"#,
        T0 + 1,
    )
    .await;
    assert_eq!(
        err,
        format!(
            "a sample of series cpu{{host=\"h1\", job=\"api\"}} is at {} ns, which is not a whole \
             number of millis (the mapping's ts_unit); writing it in millis would move it onto a \
             different timestamp. Export with a finer ts_unit.",
            T0 + 1
        )
    );
}

/// Under `ts_unit = "millis"` the export writes each timestamp in
/// milliseconds, and the re-load lands every sample on its stored ns time.
#[tokio::test]
async fn a_millis_mapping_writes_millisecond_timestamps_and_round_trips() {
    const MILLIS_MAPPING: &str = r#"
[metrics]
name_column = "name"
value_column = "value"
ts_column = "ts"
ts_unit = "millis"

[[metrics.label]]
name = "job"
column = "job_col"

[[metrics.label]]
name = "host"
column = "host_col"
"#;
    const MS: i64 = 1_000_000;
    let rows: [SourceRow<'_>; 3] = [
        ("cpu", T2 / MS, 3.0, "api", "h1"),
        ("cpu", T0 / MS, 1.0, "api", "h1"),
        ("cpu", (T1 + 7 * MS) / MS, 2.0, "api", "h1"),
    ];
    let (report, batch) = round_trip(MILLIS_MAPPING, &rows).await;

    assert_eq!(report.rows_written, 3);
    assert_eq!(
        i64_values(&batch, "ts"),
        vec![T0 / MS, (T1 + 7 * MS) / MS, T2 / MS],
        "each ts is the stored ns time in milliseconds"
    );
    assert_eq!(
        f64_bits(&batch, "value"),
        vec![1.0f64.to_bits(), 2.0f64.to_bits(), 3.0f64.to_bits()]
    );
}

/// A raw name that fits the 512-byte metric-name cap but whose stored name,
/// with the unit suffix and `_total` a load appended, does not: the export
/// writes the name less both suffixes, which a load with the same mapping
/// accepts and suffixes back onto the stored name.
#[tokio::test]
async fn a_counter_name_suffixed_past_the_length_cap_round_trips() {
    const COUNTER_MAPPING: &str = r#"
[metrics]
name_column = "name"
value_column = "value"
ts_column = "ts"
ts_unit = "nanos"
unit = "s"
kind = "counter"

[[metrics.label]]
name = "job"
column = "job_col"

[[metrics.label]]
name = "host"
column = "host_col"
"#;
    let raw = "n".repeat(510);
    let rows: [SourceRow<'_>; 2] = [
        (raw.as_str(), T0, 1.0, "api", "h1"),
        (raw.as_str(), T1, 2.0, "api", "h1"),
    ];
    let (report, batch) = round_trip(COUNTER_MAPPING, &rows).await;

    assert_eq!(report.series_written, 1);
    assert_eq!(
        str_values(&batch, "name"),
        some(&[raw.as_str(), raw.as_str()]),
        "the stored {raw}_seconds_total is 524 bytes, so the name is written less both suffixes"
    );
}

/// The gauge form of the case above: the stored name carries only the unit
/// suffix, and the export writes it less that suffix.
#[tokio::test]
async fn a_gauge_name_suffixed_past_the_length_cap_round_trips() {
    let raw = "g".repeat(510);
    let rows: [SourceRow<'_>; 1] = [(raw.as_str(), T0, 1.0, "api", "h1")];
    let (report, batch) = round_trip(GAUGE_MAPPING, &rows).await;

    assert_eq!(report.series_written, 1);
    assert_eq!(
        str_values(&batch, "name"),
        some(&[raw.as_str()]),
        "the stored {raw}_seconds is 518 bytes, so the name is written less the unit suffix"
    );
}

/// A mapping that writes two fields to one output column is refused before
/// any object-store request.
#[tokio::test]
async fn a_shared_output_column_is_refused_before_any_store_request() {
    let dir = tempfile::tempdir().expect("tempdir");
    let instrumented = Arc::new(InstrumentedStore::new(MemoryStore::new()));
    let metrics = instrumented.metrics();
    let store: Arc<dyn ObjectStoreBackend> = instrumented;
    let mapping = metrics_mapping(
        r#"
[metrics]
name_column = "name"
value_column = "value"
ts_column = "ts"
ts_unit = "nanos"

[[metrics.label]]
name = "job"
column = "value"
"#,
    );
    let out = dir.path().join("out.parquet");
    let err = export_window(&store, "alpha", T0, T1, &mapping, &out, LOAD_NS)
        .await
        .expect_err("a shared output column is refused");
    assert_eq!(
        err.to_string(),
        "the mapping writes two different fields to the output column \"value\"; give each one \
         its own column name"
    );
    assert_eq!(metrics.snapshot(), StoreMetricsSnapshot::default());
    assert!(!out.exists(), "a refused export writes no file");
}

/// A native-histogram series with samples in the window refuses the whole
/// export by name. Native histograms are refused at wire admission, so the
/// sample is written through `IngestRouter::write_values` directly, as
/// ravel-ingest's own histogram read-back test does.
#[tokio::test]
async fn a_native_histogram_series_in_the_window_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("alpha");
    let labels = LabelSet::new(vec![
        ravel_types::Label {
            name: METRIC_NAME_LABEL.to_string(),
            value: "req_latency".to_string(),
        },
        ravel_types::Label {
            name: "job".to_string(),
            value: "api".to_string(),
        },
    ])
    .expect("label set");
    let series_id = SeriesId::compute(&tenant, "req_latency", &labels).expect("series id");
    let router = IngestRouter::new(
        IngestConfig {
            shard_count: 1,
            target_bytes: 8,
            max_flush_delay: Duration::from_secs(3600),
            flush_tick: Duration::from_millis(20),
            ..IngestConfig::default()
        },
        Arc::clone(&store),
        Signal::Metrics,
        Arc::new(FixedClock(LOAD_NS)),
    );
    router
        .write_values(
            tenant.clone(),
            vec![IngestPoint {
                series_id,
                labels: Arc::new(labels),
                value: IngestValue::Histogram(HistogramSample {
                    ts_ns: T1,
                    value: HistogramValue {
                        scale: 2,
                        zero_threshold: 1e-9,
                        sum: Some(42.5),
                        custom_values: None,
                        positive_spans: vec![HistogramSpan {
                            offset: 0,
                            length: 3,
                        }],
                        negative_spans: vec![],
                        counts: HistogramCounts::Int {
                            zero_count: 1,
                            count: 7,
                            positive: vec![2, 3, 1],
                            negative: vec![],
                        },
                        reset_hint: ResetHint::Yes,
                    },
                }),
            }],
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("the histogram sample is written");
    router.shutdown().await;

    let out = dir.path().join("out.parquet");
    let err = export_window(
        &store,
        "alpha",
        T0,
        T2,
        &metrics_mapping(GAUGE_MAPPING),
        &out,
        LOAD_NS,
    )
    .await
    .expect_err("a native-histogram series is refused");
    assert_eq!(
        err.to_string(),
        format!(
            "series req_latency{{job=\"api\"}} holds native (exponential) histogram samples in \
             [{T0}, {T2}), which a [metrics] mapping cannot carry: native histograms are not \
             mappable in this version, and an export that left them out would not round-trip \
             the window. Export a window that holds none, or name one metric with a name \
             literal."
        )
    );
    assert!(!out.exists(), "a refused export writes no file");
}
