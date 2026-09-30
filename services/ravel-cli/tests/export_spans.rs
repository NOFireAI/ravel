//! Acceptance coverage for `ravel-cli export --signal spans` (ADR-1751
//! decisions 4 and 5).
//!
//! Drives the real `load_spans` and `export_spans` entry points in-process
//! against a shared `MemoryStore`, for the reason `tests/export_logs.rs` gives:
//! a subprocess against `--store memory` gets its own empty store. What the
//! store holds is read back through ravel-query's own `SpanSegmentFetcher`, so
//! the round trip compares stored `SpanRecord`s field for field, not a
//! rendering of them.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::Path;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray, Float64Array, Int64Array,
    StringArray,
};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_cli::erase;
use ravel_cli::export::{self, SpansExportReport};
use ravel_cli::load::{self, SpansMapping};
use ravel_cli::maintain::SignalArg;
use ravel_cli::store::{StoreKind, StoreSelection};
use ravel_ingest::Clock;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::instrument::{InstrumentedStore, StoreMetricsSnapshot};
use ravel_object_store::memory::MemoryStore;
use ravel_query::SpanSegmentFetcher;
use ravel_rspan::{SpanQuery, SpanRecord, StatusCode};
use ravel_types::accounting::QueryAccounting;
use ravel_types::{Signal, TenantId, TimeRange};
use uuid::Uuid;

/// A fixed, plausible base clock, as in `tests/export_metrics.rs`: the RSPAN
/// flush buckets by the load clock, so a window near it reaches a known hour.
const BASE_NS: i64 = 1_700_000_000_000_000_000; // 2023-11-14T22:13:20Z
const ONE_SEC_NS: i64 = 1_000_000_000;
const ONE_MS_NS: i64 = 1_000_000;
const T0: i64 = BASE_NS;
const T1: i64 = BASE_NS + ONE_SEC_NS;
/// The clock every load and export runs at: after every event time above,
/// well inside the future-skew bound.
const LOAD_NS: i64 = BASE_NS + 10 * ONE_SEC_NS;

struct FixedClock(i64);
impl Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        self.0
    }
}

/// Every mappable span field, with `start_ts` and `end_ts` in different
/// units, and one attribute of each declared type.
const SPANS_MAPPING: &str = r#"
[spans]
trace_id_column       = "trace_id"
span_id_column        = "span_id"
parent_span_id_column = "parent_span_id"
name_column           = "name"
start_ts_column       = "start_ns"
start_ts_unit         = "nanos"
end_ts_column         = "end_us"
end_ts_unit           = "micros"
status_code_column    = "status_code"
status_message_column = "status_message"

[[spans.resource_attribute]]
key = "service.name"
column = "svc"
type = "str"

[[spans.attribute]]
key = "http.method"
column = "method"
type = "str"

[[spans.attribute]]
key = "http.status_code"
column = "http_status"
type = "i64"

[[spans.attribute]]
key = "queue.seconds"
column = "queue_seconds"
type = "f64"

[[spans.attribute]]
key = "cache.hit"
column = "cache_hit"
type = "bool"

[[spans.attribute]]
key = "request.digest"
column = "digest"
type = "bytes"
"#;

/// [`SPANS_MAPPING`] plus one attribute it does not name, so a tenant loaded
/// under it stores an attribute the export's mapping leaves unmapped.
fn wide_mapping() -> SpansMapping {
    spans_mapping(&format!(
        "{SPANS_MAPPING}\n[[spans.attribute]]\nkey = \"tenant.tier\"\ncolumn = \"tier\"\n\
         type = \"str\"\n"
    ))
}

fn spans_mapping(text: &str) -> SpansMapping {
    load::parse_spans_mapping(text).expect("valid spans mapping")
}

/// One source row, in [`SPANS_MAPPING`]'s fields plus the unmapped `tier`.
#[derive(Clone)]
struct Src {
    trace_id: [u8; 16],
    span_id: [u8; 8],
    parent: Option<[u8; 8]>,
    name: &'static str,
    start_ns: i64,
    end_ns: i64,
    status_code: Option<i64>,
    status_message: Option<&'static str>,
    svc: Option<&'static str>,
    method: Option<&'static str>,
    http_status: Option<i64>,
    queue_seconds: Option<f64>,
    cache_hit: Option<bool>,
    digest: Option<Vec<u8>>,
    tier: Option<&'static str>,
}

/// A span with only its required fields set.
fn bare(trace: u8, span: u8, name: &'static str, start_ns: i64, end_ns: i64) -> Src {
    Src {
        trace_id: [trace; 16],
        span_id: [span; 8],
        parent: None,
        name,
        start_ns,
        end_ns,
        status_code: None,
        status_message: None,
        svc: None,
        method: None,
        http_status: None,
        queue_seconds: None,
        cache_hit: None,
        digest: None,
        tier: None,
    }
}

fn source_batch(rows: &[Src]) -> RecordBatch {
    let bin = |values: Vec<Option<Vec<u8>>>| -> ArrayRef {
        let refs: Vec<Option<&[u8]>> = values.iter().map(|v| v.as_deref()).collect();
        Arc::new(BinaryArray::from(refs))
    };
    RecordBatch::try_from_iter(vec![
        (
            "trace_id",
            bin(rows.iter().map(|r| Some(r.trace_id.to_vec())).collect()),
        ),
        (
            "span_id",
            bin(rows.iter().map(|r| Some(r.span_id.to_vec())).collect()),
        ),
        (
            "parent_span_id",
            bin(rows.iter().map(|r| r.parent.map(|p| p.to_vec())).collect()),
        ),
        (
            "name",
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.name).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "start_ns",
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.start_ns).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "end_us",
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.end_ns / 1_000).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "status_code",
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.status_code).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "status_message",
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.status_message).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "svc",
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.svc).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "method",
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.method).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "http_status",
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.http_status).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "queue_seconds",
            Arc::new(Float64Array::from(
                rows.iter().map(|r| r.queue_seconds).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "cache_hit",
            Arc::new(BooleanArray::from(
                rows.iter().map(|r| r.cache_hit).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "digest",
            bin(rows.iter().map(|r| r.digest.clone()).collect()),
        ),
        (
            "tier",
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.tier).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
    ])
    .expect("source batch")
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

/// Loads the Parquet file at `path` into `tenant` under `mapping` and returns
/// the number of spans loaded.
async fn load_file(
    store: &Arc<dyn ObjectStoreBackend>,
    path: &Path,
    tenant: &str,
    mapping: &SpansMapping,
    load_now_ns: i64,
) -> u64 {
    let report = load::load_spans(
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
        Arc::new(FixedClock(load_now_ns)),
    )
    .await
    .expect("the spans load succeeds");
    assert_eq!(
        report.rows_processed, report.file_total_rows,
        "every source row is loaded"
    );
    report.rows_processed
}

async fn load_rows(
    store: &Arc<dyn ObjectStoreBackend>,
    dir: &Path,
    tenant: &str,
    mapping: &SpansMapping,
    rows: &[Src],
) {
    let path = dir.join(format!("source-{tenant}.parquet"));
    write_parquet(&path, &source_batch(rows));
    load_file(store, &path, tenant, mapping, LOAD_NS).await;
}

async fn export_window(
    store: &Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    start_ns: i64,
    end_ns: i64,
    mapping: &SpansMapping,
    out: &Path,
) -> anyhow::Result<SpansExportReport> {
    export::export_spans(
        Arc::clone(store),
        StoreSelection::explicit(StoreKind::Memory),
        tenant,
        start_ns,
        end_ns,
        mapping,
        out,
        1,
        None,
        LOAD_NS,
    )
    .await
}

/// Every span a tenant stores that overlaps `[start_ns, end_ns]`, read through
/// ravel-query's fetcher, in `(trace_id, span_id)` order.
async fn stored(
    store: &Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    start_ns: i64,
    end_ns: i64,
    now_ns: i64,
) -> Vec<SpanRecord> {
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
            Signal::Spans,
            TimeRange { start_ns, end_ns },
            &[],
            now_ns,
        )
        .await
        .expect("resolve");
    let fetcher = SpanSegmentFetcher::new(Arc::clone(store));
    let query = SpanQuery::ts_range(start_ns, end_ns);
    let mut out = Vec::new();
    for seg_ref in &snapshot.segments {
        let fetched = fetcher
            .fetch_accounted(
                seg_ref,
                hash,
                &query,
                None,
                None,
                &[],
                &QueryAccounting::new(),
            )
            .await
            .expect("span fetch");
        if let Some(output) = fetched {
            out.extend(output.records.into_iter().map(|row| row.record));
        }
    }
    out.sort_by_key(|record| (record.trace_id, record.span_id));
    out
}

fn column_names(batch: &RecordBatch) -> Vec<String> {
    batch
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect()
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

fn fixed_values(batch: &RecordBatch, column: &str) -> Vec<Option<Vec<u8>>> {
    let col = batch
        .column_by_name(column)
        .unwrap_or_else(|| panic!("no {column} column"))
        .as_any()
        .downcast_ref::<FixedSizeBinaryArray>()
        .unwrap_or_else(|| panic!("{column} is not FixedSizeBinary"));
    (0..col.len())
        .map(|i| (!col.is_null(i)).then(|| col.value(i).to_vec()))
        .collect()
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

/// Four spans varying every mapped field: a root with every optional field
/// set and an unmapped attribute, its child, and two spans of other traces
/// that start at the same instant, so the output order also pins the
/// `(trace_id, span_id)` tiebreak. The source order is not the output order.
fn round_trip_rows() -> Vec<Src> {
    let root = Src {
        parent: None,
        status_code: Some(1),
        status_message: Some("fine"),
        svc: Some("checkout"),
        method: Some(""),
        http_status: Some(200),
        queue_seconds: Some(0.125),
        cache_hit: Some(true),
        digest: Some(vec![0xde, 0xad, 0xbe, 0xef]),
        tier: Some("gold"),
        ..bare(1, 0x11, "GET /checkout", T0, T0 + 250 * ONE_MS_NS)
    };
    let child = Src {
        parent: Some([0x11; 8]),
        status_code: Some(2),
        status_message: Some("deadlock detected"),
        svc: Some("orders-db"),
        method: Some("POST"),
        http_status: Some(-1),
        queue_seconds: Some(f64::INFINITY),
        cache_hit: Some(false),
        digest: Some(Vec::new()),
        ..bare(
            1,
            0x22,
            "SELECT orders",
            T0 + ONE_MS_NS + 7,
            T0 + 9 * ONE_MS_NS,
        )
    };
    let later_trace = Src {
        svc: Some("sweeper"),
        tier: Some("bronze"),
        ..bare(3, 0x44, "sweep b", T0 + 2 * ONE_MS_NS, T0 + 2 * ONE_MS_NS)
    };
    let earlier_trace = bare(2, 0x33, "sweep a", T0 + 2 * ONE_MS_NS, T0 + 3 * ONE_MS_NS);
    vec![later_trace, child, earlier_trace, root]
}

/// `load(export(window))` with the same mapping reproduces every mapped field
/// of every span: the ids, a parent and no parent, the name, both
/// timestamps, the status, and each mapped attribute of each declared type.
/// The source tenant also stores an attribute the export's mapping does not
/// name, which the re-loaded spans do not carry; nothing else differs.
#[tokio::test]
async fn load_then_export_then_load_round_trips_every_mapped_span_field() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let mapping = spans_mapping(SPANS_MAPPING);
    load_rows(
        &store,
        dir.path(),
        "alpha",
        &wide_mapping(),
        &round_trip_rows(),
    )
    .await;

    let export_pq = dir.path().join("export.parquet");
    let report = export_window(&store, "alpha", T0, T1, &mapping, &export_pq)
        .await
        .expect("export succeeds");
    assert_eq!(report.rows_written, 4);
    assert_eq!(report.erasure_predicates, 0);

    let batch = read_parquet(&export_pq);
    assert_eq!(
        column_names(&batch),
        vec![
            "trace_id",
            "span_id",
            "parent_span_id",
            "name",
            "start_ns",
            "end_us",
            "status_code",
            "status_message",
            "svc",
            "method",
            "http_status",
            "queue_seconds",
            "cache_hit",
            "digest",
        ],
        "exactly the mapped columns, in mapping order"
    );
    assert_eq!(
        i64_values(&batch, "start_ns"),
        vec![
            T0,
            T0 + ONE_MS_NS + 7,
            T0 + 2 * ONE_MS_NS,
            T0 + 2 * ONE_MS_NS
        ],
        "sorted by start_ts, written in nanos"
    );
    assert_eq!(
        i64_values(&batch, "end_us"),
        vec![
            (T0 + 250 * ONE_MS_NS) / 1_000,
            (T0 + 9 * ONE_MS_NS) / 1_000,
            (T0 + 3 * ONE_MS_NS) / 1_000,
            (T0 + 2 * ONE_MS_NS) / 1_000
        ],
        "end_ts written in micros"
    );
    assert_eq!(
        fixed_values(&batch, "trace_id"),
        vec![
            Some(vec![1; 16]),
            Some(vec![1; 16]),
            Some(vec![2; 16]),
            Some(vec![3; 16])
        ],
        "two spans starting together are ordered by trace_id"
    );
    assert_eq!(
        fixed_values(&batch, "parent_span_id"),
        vec![None, Some(vec![0x11; 8]), None, None]
    );
    assert_eq!(
        str_values(&batch, "status_message"),
        vec![
            Some("fine".to_string()),
            Some("deadlock detected".to_string()),
            None,
            None
        ]
    );

    let loaded = load_file(&store, &export_pq, "beta", &mapping, LOAD_NS + ONE_SEC_NS).await;
    assert_eq!(loaded, report.rows_written, "every exported row re-loads");

    let now = LOAD_NS + ONE_SEC_NS;
    let alpha = stored(&store, "alpha", T0, T1, now).await;
    let beta = stored(&store, "beta", T0, T1, now).await;
    assert_eq!(alpha.len(), 4);
    assert!(
        alpha
            .iter()
            .any(|span| span.attrs.iter().any(|(key, _)| key == "tenant.tier")),
        "the source tenant stores the unmapped attribute"
    );
    let expected: Vec<SpanRecord> = alpha
        .into_iter()
        .map(|mut span| {
            span.attrs.retain(|(key, _)| key != "tenant.tier");
            span
        })
        .collect();
    assert_eq!(
        beta, expected,
        "the re-loaded tenant holds the same spans, less the attribute the mapping does not name"
    );
    let child = beta
        .iter()
        .find(|span| span.span_id == [0x22; 8])
        .expect("child span");
    assert_eq!(child.parent_span_id, Some([0x11; 8]));
    assert_eq!(child.status_code, StatusCode::Error);
    assert_eq!(
        child.attrs,
        vec![
            ("cache.hit".to_string(), "false".to_string()),
            ("http.method".to_string(), "POST".to_string()),
            ("http.status_code".to_string(), "-1".to_string()),
            ("queue.seconds".to_string(), "+Inf".to_string()),
            ("request.digest".to_string(), String::new()),
            ("service.name".to_string(), "orders-db".to_string()),
        ]
    );
}

/// The window is half-open on the span's start: a span starting at exactly
/// `start` is exported, one starting at exactly `end` is not, and one that
/// started before `start` is not exported although it is still open inside
/// the window.
#[tokio::test]
async fn a_span_starting_at_end_is_not_exported() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let mapping = spans_mapping(SPANS_MAPPING);
    load_rows(
        &store,
        dir.path(),
        "alpha",
        &mapping,
        &[
            bare(1, 1, "before", T0 - 1, T0 + 5 * ONE_MS_NS),
            bare(1, 2, "at start", T0, T0 + ONE_MS_NS),
            bare(1, 3, "last", T1 - 1, T1 + ONE_MS_NS),
            bare(1, 4, "at end", T1, T1 + ONE_MS_NS),
        ],
    )
    .await;
    let export_pq = dir.path().join("export.parquet");
    let report = export_window(&store, "alpha", T0, T1, &mapping, &export_pq)
        .await
        .expect("export succeeds");
    assert_eq!(report.rows_written, 2);
    let batch = read_parquet(&export_pq);
    assert_eq!(i64_values(&batch, "start_ns"), vec![T0, T1 - 1]);
    assert_eq!(
        str_values(&batch, "name"),
        vec![Some("at start".to_string()), Some("last".to_string())]
    );
}

/// A pending selective-erasure request over one attribute value excludes the
/// spans carrying it, although the raw object still holds them.
#[tokio::test]
async fn an_erased_span_is_not_exported() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let mapping = spans_mapping(SPANS_MAPPING);
    load_rows(
        &store,
        dir.path(),
        "alpha",
        &mapping,
        &[
            Src {
                svc: Some("keep"),
                ..bare(1, 1, "kept a", T0, T0 + ONE_MS_NS)
            },
            Src {
                svc: Some("erase-me"),
                ..bare(1, 2, "erased", T0 + ONE_MS_NS, T0 + 2 * ONE_MS_NS)
            },
            Src {
                svc: Some("keep"),
                ..bare(1, 3, "kept b", T0 + 2 * ONE_MS_NS, T0 + 3 * ONE_MS_NS)
            },
        ],
    )
    .await;
    erase::submit(
        Arc::clone(&store),
        "alpha",
        SignalArg::Spans,
        vec![("service.name".to_string(), "erase-me".to_string())],
        0,
        0,
        "spans export erasure test".to_string(),
        Uuid::from_u128(0x2213_0001),
        LOAD_NS,
    )
    .await
    .expect("the erasure request is recorded");
    assert_eq!(
        stored(&store, "alpha", T0, T1, LOAD_NS).await.len(),
        3,
        "the raw objects still hold the erased span"
    );

    let export_pq = dir.path().join("export.parquet");
    let report = export_window(&store, "alpha", T0, T1, &mapping, &export_pq)
        .await
        .expect("export succeeds");
    assert_eq!(report.erasure_predicates, 1);
    assert_eq!(report.rows_written, 2);
    let batch = read_parquet(&export_pq);
    assert_eq!(
        str_values(&batch, "name"),
        vec![Some("kept a".to_string()), Some("kept b".to_string())]
    );
    assert_eq!(
        str_values(&batch, "svc"),
        vec![Some("keep".to_string()), Some("keep".to_string())]
    );
}

/// Loads `rows` into tenant `alpha` under [`SPANS_MAPPING`], exports
/// `[T0, T1)` under `export_mapping`, and returns the refusal.
async fn refused_export(export_mapping: &str, rows: &[Src]) -> String {
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    load_rows(
        &store,
        dir.path(),
        "alpha",
        &spans_mapping(SPANS_MAPPING),
        rows,
    )
    .await;
    let out = dir.path().join("out.parquet");
    let err = export_window(
        &store,
        "alpha",
        T0,
        T1,
        &spans_mapping(export_mapping),
        &out,
    )
    .await
    .expect_err("the export is refused");
    assert!(!out.exists(), "a refused export writes no file");
    err.to_string()
}

/// [`SPANS_MAPPING`] with `start_ts_unit` in millis.
fn millis_start_mapping() -> String {
    SPANS_MAPPING.replace(
        "start_ts_unit         = \"nanos\"",
        "start_ts_unit         = \"millis\"",
    )
}

/// A span whose start is not a whole number of the mapping's `start_ts_unit`
/// is refused rather than truncated onto another start.
#[tokio::test]
async fn a_span_start_finer_than_the_start_ts_unit_is_refused() {
    let err = refused_export(
        &millis_start_mapping(),
        &[bare(1, 0x11, "fine grained", T0 + 1, T0 + ONE_MS_NS)],
    )
    .await;
    assert_eq!(
        err,
        format!(
            "export --signal spans refused on 1 spans for this reason; first: span \"fine \
             grained\" (trace_id 01010101010101010101010101010101, span_id 1111111111111111) has \
             start_ts {} ns, which is not a whole number of millis (the mapping's start_ts_unit); \
             writing it in millis would move it onto a different timestamp. Export with a finer \
             start_ts_unit.",
            T0 + 1
        )
    );
}

/// Several refused spans are refused once, naming the count and the first
/// offender in output order. The spans are loaded in the reverse of that
/// order, and the first offender is sub-unit in both its start and its end,
/// which counts it once.
#[tokio::test]
async fn two_refused_spans_name_the_count_and_the_first_in_output_order() {
    let mapping = millis_start_mapping().replace(
        "end_ts_unit           = \"micros\"",
        "end_ts_unit           = \"millis\"",
    );
    let err = refused_export(
        &mapping,
        &[
            bare(
                2,
                0x22,
                "second",
                T0 + 5 * ONE_MS_NS + 2,
                T0 + 6 * ONE_MS_NS,
            ),
            bare(1, 0x11, "first", T0 + 3, T0 + 4 * ONE_MS_NS + 1_000),
            bare(3, 0x33, "whole", T0 + 7 * ONE_MS_NS, T0 + 8 * ONE_MS_NS),
        ],
    )
    .await;
    assert_eq!(
        err,
        format!(
            "export --signal spans refused on 2 spans for this reason; first: span \"first\" \
             (trace_id 01010101010101010101010101010101, span_id 1111111111111111) has start_ts \
             {} ns, which is not a whole number of millis (the mapping's start_ts_unit); writing \
             it in millis would move it onto a different timestamp. Export with a finer \
             start_ts_unit.",
            T0 + 3
        )
    );
}

/// A mapped attribute whose stored string the declared type does not read
/// back is refused by name: `"007"` loads from an `i64` column as `"7"`.
#[tokio::test]
async fn an_attribute_the_declared_type_does_not_read_back_is_refused() {
    let as_str = SPANS_MAPPING.replace(
        "column = \"http_status\"\ntype = \"i64\"",
        "column = \"method\"\ntype = \"str\"",
    );
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    // Loaded with `http.status_code` read from the `method` string column, so
    // the stored value is the string "007".
    load_rows(
        &store,
        dir.path(),
        "alpha",
        &spans_mapping(&as_str),
        &[Src {
            method: Some("007"),
            ..bare(1, 0x11, "padded", T0, T0 + ONE_MS_NS)
        }],
    )
    .await;
    let out = dir.path().join("out.parquet");
    let err = export_window(&store, "alpha", T0, T1, &spans_mapping(SPANS_MAPPING), &out)
        .await
        .expect_err("the export is refused");
    assert!(!out.exists(), "a refused export writes no file");
    assert_eq!(
        err.to_string(),
        "export --signal spans refused on 1 spans for this reason; first: span \"padded\" \
         (trace_id 01010101010101010101010101010101, span_id 1111111111111111) carries the \
         attribute \"http.status_code\" with the stored value \"007\", which the mapping declares \
         i64; no i64 cell loads back as that string, so the exported file would not re-load as \
         the same span. Declare the attribute as str."
    );
}

/// Two fields sharing one output column are refused before any object-store
/// request.
#[tokio::test]
async fn two_fields_sharing_one_output_column_are_refused_before_any_store_request() {
    let dir = tempfile::tempdir().expect("tempdir");
    let instrumented = Arc::new(InstrumentedStore::new(MemoryStore::new()));
    let metrics = instrumented.metrics();
    let store: Arc<dyn ObjectStoreBackend> = instrumented;
    let mapping = spans_mapping(&SPANS_MAPPING.replace(
        "status_message_column = \"status_message\"",
        "status_message_column = \"name\"",
    ));
    let out = dir.path().join("out.parquet");
    let err = export_window(&store, "alpha", T0, T1, &mapping, &out)
        .await
        .expect_err("a duplicate output column is refused");
    assert_eq!(
        err.to_string(),
        "the mapping writes two different fields to the output column \"name\"; give each one \
         its own column name"
    );
    assert_eq!(metrics.snapshot(), StoreMetricsSnapshot::default());
    assert!(!out.exists(), "a refused export writes no file");
}

/// `export::run` sends `--signal spans` to the spans export: the mapping's
/// `[spans]` section is parsed and the spans export's own checks run.
#[tokio::test]
async fn run_dispatches_signal_spans_to_the_spans_export() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mapping_path = dir.path().join("mapping.toml");
    std::fs::write(
        &mapping_path,
        SPANS_MAPPING.replace(
            "status_message_column = \"status_message\"",
            "status_message_column = \"name\"",
        ),
    )
    .expect("write mapping");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let out = dir.path().join("out.parquet");
    let err = export::run(
        store,
        StoreSelection::explicit(StoreKind::Memory),
        "acme",
        SignalArg::Spans,
        T0,
        T1,
        &mapping_path,
        &out,
        1,
        None,
        LOAD_NS,
    )
    .await
    .expect_err("the duplicate output column is refused");
    assert_eq!(
        err.to_string(),
        "the mapping writes two different fields to the output column \"name\"; give each one \
         its own column name"
    );
}
