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
use std::time::Duration;

use arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BooleanArray, FixedSizeBinaryArray, Float64Array,
    Int64Array, MapBuilder, StringArray, StringBuilder,
};
use arrow::record_batch::RecordBatch;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_cli::erase;
use ravel_cli::export::{self, SpansExportReport};
use ravel_cli::load::{self, SpansMapping};
use ravel_cli::maintain::SignalArg;
use ravel_cli::store::{StoreKind, StoreSelection};
use ravel_ingest::{Clock, IngestConfig, SpanIngestRouter, WriteMode};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::instrument::{InstrumentedStore, StoreMetricsSnapshot};
use ravel_object_store::memory::MemoryStore;
use ravel_otlp::{SpanIngestLimits, normalize_traces};
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
    assert_eq!(
        report.spans_with_unwritten_data, 2,
        "the root and the later trace carry the unmapped tenant.tier"
    );

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

/// Stores `spans` in `tenant` the way an OTLP-ingested span is stored:
/// through `normalize_traces` and the span ingest router, so the store holds
/// the reserved attributes (`_kind` and the rest) a Parquet load cannot write.
async fn ingest_otlp(store: &Arc<dyn ObjectStoreBackend>, tenant: &str, spans: Vec<Span>) {
    let out = normalize_traces(
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: None,
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans,
                    ..Default::default()
                }],
                ..Default::default()
            }],
        },
        &SpanIngestLimits::default(),
        LOAD_NS,
    );
    assert!(out.rejected.is_empty(), "rejected: {:?}", out.rejected);
    let router = SpanIngestRouter::new(
        IngestConfig {
            shard_count: 1,
            target_bytes: 1,
            ..IngestConfig::default()
        },
        Arc::clone(store),
        Arc::new(FixedClock(LOAD_NS)),
    );
    router
        .write(
            TenantId::new(tenant),
            out.spans,
            WriteMode::Strict,
            Duration::from_secs(5),
        )
        .await
        .expect("the OTLP spans are written");
    router.shutdown().await;
}

fn otlp_span(trace: u8, span: u8, name: &str, start_ns: i64, end_ns: i64) -> Span {
    Span {
        trace_id: vec![trace; 16],
        span_id: vec![span; 8],
        name: name.to_string(),
        start_time_unix_nano: start_ns as u64,
        end_time_unix_nano: end_ns as u64,
        ..Default::default()
    }
}

/// A stored span carrying an attribute the file has no column for, a reserved
/// `_kind` or a key the mapping does not name, is exported without it and
/// counted: the re-loaded tenant holds the same spans less exactly those
/// attributes, and the report counts the two spans that carried one.
#[tokio::test]
async fn attributes_the_file_does_not_carry_are_counted_per_span() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let mapping = spans_mapping(SPANS_MAPPING);
    load_rows(
        &store,
        dir.path(),
        "alpha",
        &mapping,
        &[Src {
            svc: Some("checkout"),
            ..bare(1, 0x11, "mapped only", T0, T0 + ONE_MS_NS)
        }],
    )
    .await;
    let server = Span {
        kind: 2,
        ..otlp_span(2, 0x22, "server", T0 + 2 * ONE_MS_NS, T0 + 3 * ONE_MS_NS)
    };
    let tiered = Span {
        attributes: vec![KeyValue {
            key: "tenant.tier".to_string(),
            value: Some(AnyValue {
                value: Some(AnyValueVariant::StringValue("gold".to_string())),
            }),
            ..Default::default()
        }],
        ..otlp_span(3, 0x33, "tiered", T0 + 4 * ONE_MS_NS, T0 + 5 * ONE_MS_NS)
    };
    ingest_otlp(&store, "alpha", vec![server, tiered]).await;

    let alpha = stored(&store, "alpha", T0, T1, LOAD_NS).await;
    let attrs_of = |spans: &[SpanRecord], span_id: [u8; 8]| -> Vec<(String, String)> {
        spans
            .iter()
            .find(|span| span.span_id == span_id)
            .expect("span stored")
            .attrs
            .clone()
    };
    assert_eq!(
        attrs_of(&alpha, [0x22; 8]),
        vec![("_kind".to_string(), "server".to_string())],
        "the OTLP path stores the span kind as a reserved attribute"
    );
    assert_eq!(
        attrs_of(&alpha, [0x33; 8]),
        vec![("tenant.tier".to_string(), "gold".to_string())]
    );

    let export_pq = dir.path().join("export.parquet");
    let report = export_window(&store, "alpha", T0, T1, &mapping, &export_pq)
        .await
        .expect("export succeeds");
    assert_eq!(report.rows_written, 3);
    assert_eq!(
        report.segments_read, 2,
        "one object from the file load, one from the OTLP ingest"
    );
    assert_eq!(report.segments_pruned, 0);
    assert_eq!(
        report.spans_with_unwritten_data, 2,
        "the _kind span and the tenant.tier span; the mapped-only span lost nothing"
    );

    load_file(&store, &export_pq, "beta", &mapping, LOAD_NS + ONE_SEC_NS).await;
    let beta = stored(&store, "beta", T0, T1, LOAD_NS + ONE_SEC_NS).await;
    let expected: Vec<SpanRecord> = alpha
        .into_iter()
        .map(|mut span| {
            span.attrs
                .retain(|(key, _)| key != "_kind" && key != "tenant.tier");
            span
        })
        .collect();
    assert_eq!(
        beta, expected,
        "the re-loaded spans lack exactly the attributes the file does not carry"
    );
    assert!(attrs_of(&beta, [0x22; 8]).is_empty());
    assert!(attrs_of(&beta, [0x33; 8]).is_empty());
}

/// A `[spans]` mapping naming only the required fields: no parent, status
/// code or status message column, and no attributes.
const REQUIRED_ONLY_MAPPING: &str = r#"
[spans]
trace_id_column = "trace_id"
span_id_column  = "span_id"
name_column     = "name"
start_ts_column = "start_ns"
start_ts_unit   = "nanos"
end_ts_column   = "end_us"
end_ts_unit     = "micros"
"#;

/// A parent id, a non-Unset status code and a status message the mapping has
/// no column for are not written, and each span that stored one is counted:
/// three of the four spans lost one field each, the bare span lost nothing.
#[tokio::test]
async fn span_fields_the_mapping_has_no_column_for_are_counted_per_span() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    load_rows(
        &store,
        dir.path(),
        "alpha",
        &spans_mapping(SPANS_MAPPING),
        &[
            Src {
                parent: Some([0x10; 8]),
                ..bare(1, 0x11, "child", T0, T0 + ONE_MS_NS)
            },
            Src {
                status_code: Some(2),
                ..bare(2, 0x22, "failed", T0 + 2 * ONE_MS_NS, T0 + 3 * ONE_MS_NS)
            },
            Src {
                status_message: Some("deadlock"),
                ..bare(3, 0x33, "explained", T0 + 4 * ONE_MS_NS, T0 + 5 * ONE_MS_NS)
            },
            bare(4, 0x44, "bare", T0 + 6 * ONE_MS_NS, T0 + 7 * ONE_MS_NS),
        ],
    )
    .await;
    let alpha = stored(&store, "alpha", T0, T1, LOAD_NS).await;
    let span = |spans: &[SpanRecord], span_id: [u8; 8]| -> SpanRecord {
        spans
            .iter()
            .find(|span| span.span_id == span_id)
            .expect("span stored")
            .clone()
    };
    assert_eq!(span(&alpha, [0x11; 8]).parent_span_id, Some([0x10; 8]));
    assert_eq!(span(&alpha, [0x22; 8]).status_code, StatusCode::Error);
    assert_eq!(
        span(&alpha, [0x33; 8]).status_message.as_deref(),
        Some("deadlock")
    );

    let mapping = spans_mapping(REQUIRED_ONLY_MAPPING);
    let export_pq = dir.path().join("export.parquet");
    let report = export_window(&store, "alpha", T0, T1, &mapping, &export_pq)
        .await
        .expect("export succeeds");
    assert_eq!(report.rows_written, 4);
    assert_eq!(
        report.spans_with_unwritten_data, 3,
        "the child's parent, the failed span's status and the explained span's message"
    );

    load_file(&store, &export_pq, "beta", &mapping, LOAD_NS + ONE_SEC_NS).await;
    let beta = stored(&store, "beta", T0, T1, LOAD_NS + ONE_SEC_NS).await;
    let expected: Vec<SpanRecord> = alpha
        .into_iter()
        .map(|mut span| {
            span.parent_span_id = None;
            span.status_code = StatusCode::Unset;
            span.status_message = None;
            span
        })
        .collect();
    assert_eq!(
        beta, expected,
        "the re-loaded spans lack exactly the fields the file does not carry"
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

/// [`SPANS_MAPPING`] with `attrs_map_column = "attrs"`.
fn attrs_map_mapping() -> SpansMapping {
    spans_mapping(&SPANS_MAPPING.replace(
        "status_message_column = \"status_message\"\n",
        "status_message_column = \"status_message\"\nattrs_map_column      = \"attrs\"\n",
    ))
}

/// One `attrs` map cell: `None` is a null cell, a `None` value a null value.
type MapCell<'a> = Option<Vec<(&'a str, Option<&'a str>)>>;

/// `batch` with an `attrs` map column appended, one cell per row.
fn with_attrs_map(batch: RecordBatch, cells: Vec<MapCell<'_>>) -> RecordBatch {
    let mut map = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    for cell in cells {
        match cell {
            None => map.append(false).expect("null map cell"),
            Some(entries) => {
                for (key, value) in entries {
                    map.keys().append_value(key);
                    map.values().append_option(value);
                }
                map.append(true).expect("map cell");
            }
        }
    }
    let mut columns: Vec<(String, ArrayRef)> = batch
        .schema()
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, column)| (field.name().clone(), Arc::clone(column)))
        .collect();
    columns.push(("attrs".to_string(), Arc::new(map.finish())));
    RecordBatch::try_from_iter(columns).expect("batch with an attrs map")
}

/// Each row's entries of the exported `attrs` map column, in stored order.
fn map_values(batch: &RecordBatch, column: &str) -> Vec<Vec<(String, String)>> {
    let map = batch
        .column_by_name(column)
        .unwrap_or_else(|| panic!("no {column} column"))
        .as_map();
    (0..map.len())
        .map(|row| {
            let entries = map.value(row);
            let keys = entries.column(0).as_string::<i32>();
            let values = entries.column(1).as_string::<i32>();
            (0..entries.len())
                .map(|i| (keys.value(i).to_string(), values.value(i).to_string()))
                .collect()
        })
        .collect()
}

fn otlp_attr(key: &str, value: AnyValueVariant) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue { value: Some(value) }),
        ..Default::default()
    }
}

fn pairs(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

/// With `attrs_map_column` set, every stored attribute the mapping does not
/// name travels through the map column: a load of the export stores every
/// span with every attribute at its exact stored string, and only the
/// reserved `_kind` is unwritten. Four of the six spans carry an unmapped
/// attribute and two carry none, so the same store exported without the map
/// column counts exactly four.
#[tokio::test]
async fn attrs_map_column_carries_every_unmapped_attribute_through_a_round_trip() {
    use AnyValueVariant::{BoolValue, BytesValue, DoubleValue, IntValue, StringValue};

    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    // The root and the later trace carry the unmapped `tenant.tier`; the child
    // and the earlier trace carry no unmapped attribute.
    load_rows(
        &store,
        dir.path(),
        "alpha",
        &wide_mapping(),
        &round_trip_rows(),
    )
    .await;
    let typed = Span {
        attributes: vec![
            otlp_attr("retries", IntValue(3)),
            otlp_attr("ratio", DoubleValue(0.1)),
            otlp_attr("sampled", BoolValue(true)),
            otlp_attr("blob", BytesValue(vec![0xab, 0xcd])),
            otlp_attr("empty", StringValue(String::new())),
            otlp_attr("padded", StringValue("007".to_string())),
            otlp_attr("http.method", StringValue("GET".to_string())),
        ],
        ..otlp_span(4, 0x55, "typed", T0 + 3 * ONE_MS_NS, T0 + 4 * ONE_MS_NS)
    };
    let server = Span {
        kind: 2,
        attributes: vec![otlp_attr("peer", StringValue("db".to_string()))],
        ..otlp_span(5, 0x66, "server", T0 + 5 * ONE_MS_NS, T0 + 6 * ONE_MS_NS)
    };
    ingest_otlp(&store, "alpha", vec![typed, server]).await;

    let alpha = stored(&store, "alpha", T0, T1, LOAD_NS).await;
    assert_eq!(alpha.len(), 6);
    let attrs_of = |spans: &[SpanRecord], span_id: [u8; 8]| -> Vec<(String, String)> {
        spans
            .iter()
            .find(|span| span.span_id == span_id)
            .expect("span stored")
            .attrs
            .clone()
    };
    assert_eq!(
        attrs_of(&alpha, [0x55; 8]),
        pairs(&[
            ("blob", "abcd"),
            ("empty", ""),
            ("http.method", "GET"),
            ("padded", "007"),
            ("ratio", "0.1"),
            ("retries", "3"),
            ("sampled", "true"),
        ])
    );
    assert_eq!(
        attrs_of(&alpha, [0x66; 8]),
        pairs(&[("_kind", "server"), ("peer", "db")])
    );

    let mapping = attrs_map_mapping();
    let export_pq = dir.path().join("export.parquet");
    let report = export_window(&store, "alpha", T0, T1, &mapping, &export_pq)
        .await
        .expect("export succeeds");
    assert_eq!(report.rows_written, 6);
    assert_eq!(
        report.spans_with_unwritten_data, 1,
        "only the server span's reserved _kind has no column"
    );
    let batch = read_parquet(&export_pq);
    assert_eq!(
        column_names(&batch).last().map(String::as_str),
        Some("attrs")
    );
    assert_eq!(
        map_values(&batch, "attrs"),
        vec![
            pairs(&[("tenant.tier", "gold")]),
            Vec::new(),
            Vec::new(),
            pairs(&[("tenant.tier", "bronze")]),
            pairs(&[
                ("blob", "abcd"),
                ("empty", ""),
                ("padded", "007"),
                ("ratio", "0.1"),
                ("retries", "3"),
                ("sampled", "true"),
            ]),
            pairs(&[("peer", "db")]),
        ],
        "every unmapped, unreserved attribute as stored, in output order"
    );

    load_file(&store, &export_pq, "beta", &mapping, LOAD_NS + ONE_SEC_NS).await;
    let beta = stored(&store, "beta", T0, T1, LOAD_NS + ONE_SEC_NS).await;
    let expected: Vec<SpanRecord> = alpha
        .iter()
        .cloned()
        .map(|mut span| {
            span.attrs.retain(|(key, _)| key != "_kind");
            span
        })
        .collect();
    assert_eq!(
        beta, expected,
        "the re-loaded spans carry every attribute at its stored string, less the reserved _kind"
    );

    let plain_pq = dir.path().join("plain.parquet");
    let plain = export_window(
        &store,
        "alpha",
        T0,
        T1,
        &spans_mapping(SPANS_MAPPING),
        &plain_pq,
    )
    .await
    .expect("export without the map column succeeds");
    assert_eq!(plain.rows_written, 6);
    assert_eq!(
        plain.spans_with_unwritten_data, 4,
        "the root, the later trace, the typed span and the server span; not the child or the \
         earlier trace"
    );
    assert!(
        !column_names(&read_parquet(&plain_pq)).contains(&"attrs".to_string()),
        "no map column without attrs_map_column"
    );
}

/// Writes `batch` and loads it into `alpha` under `mapping`, returning the
/// load's outcome rather than asserting it.
async fn try_load_batch(
    store: &Arc<dyn ObjectStoreBackend>,
    dir: &Path,
    mapping: &SpansMapping,
    batch: &RecordBatch,
) -> Result<load::SpansLoadReport, load::LoadError> {
    let path = dir.join("with-map.parquet");
    write_parquet(&path, batch);
    load::load_spans(
        Arc::clone(store),
        &path,
        "alpha",
        mapping,
        1,
        10_000,
        0,
        1,
        1,
        1,
        None,
        LOAD_NS,
        Arc::new(FixedClock(LOAD_NS)),
    )
    .await
}

/// A map key equal to a mapped attribute's key is refused on the row, as a
/// mapping naming one key twice is refused, whether or not the mapped cell
/// holds a value, and nothing is stored. The map's other keys are stored
/// exactly as written once the collision is gone.
#[tokio::test]
async fn an_attrs_map_key_that_a_mapped_attribute_names_is_refused() {
    for method in [Some("GET"), None] {
        let dir = tempfile::tempdir().expect("tempdir");
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let batch = with_attrs_map(
            source_batch(&[Src {
                method,
                ..bare(1, 0x11, "collides", T0, T0 + ONE_MS_NS)
            }]),
            vec![Some(vec![
                ("http.method", Some("POST")),
                ("peer", Some("db")),
            ])],
        );
        let err = try_load_batch(&store, dir.path(), &attrs_map_mapping(), &batch)
            .await
            .expect_err("the collision is refused");
        assert_eq!(
            err.to_string(),
            "row 0: attrs_map_column \"attrs\" holds the key \"http.method\", which the mapping \
             also reads from the column \"method\". A span carries one merged attrs map with \
             unique keys, so one of the two would never reach the record; drop the key from the \
             map or the entry from the mapping.",
            "method cell {method:?}"
        );
        assert!(
            stored(&store, "alpha", T0, T1, LOAD_NS).await.is_empty(),
            "a refused row stores no span (method cell {method:?})"
        );
    }

    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let batch = with_attrs_map(
        source_batch(&[Src {
            method: Some("GET"),
            ..bare(1, 0x11, "distinct", T0, T0 + ONE_MS_NS)
        }]),
        vec![Some(vec![
            ("http.verb", Some("POST")),
            ("peer", Some("db")),
        ])],
    );
    try_load_batch(&store, dir.path(), &attrs_map_mapping(), &batch)
        .await
        .expect("distinct keys load");
    let spans = stored(&store, "alpha", T0, T1, LOAD_NS).await;
    assert_eq!(spans.len(), 1);
    assert_eq!(
        spans[0].attrs,
        pairs(&[
            ("http.method", "GET"),
            ("http.verb", "POST"),
            ("peer", "db")
        ])
    );
}

/// A null map cell and a null map value carry no attribute; a map holding a
/// reserved key or one key twice is refused on its row, and a column that is
/// not a map of strings is refused by name.
#[tokio::test]
async fn attrs_map_column_cells_are_read_by_the_span_attribute_rules() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let batch = with_attrs_map(
        source_batch(&[
            bare(1, 0x11, "null cell", T0, T0 + ONE_MS_NS),
            bare(1, 0x22, "null value", T0 + ONE_MS_NS, T0 + 2 * ONE_MS_NS),
        ]),
        vec![None, Some(vec![("peer", None), ("zone", Some("eu"))])],
    );
    try_load_batch(&store, dir.path(), &attrs_map_mapping(), &batch)
        .await
        .expect("null cells and values load");
    let spans = stored(&store, "alpha", T0, T1, LOAD_NS).await;
    assert_eq!(spans.len(), 2);
    assert!(spans[0].attrs.is_empty());
    assert_eq!(spans[1].attrs, pairs(&[("zone", "eu")]));

    for (entries, want) in [
        (
            vec![("_kind", Some("server"))],
            "row 0: attrs_map_column \"attrs\" holds the reserved attribute key \"_kind\", which \
             holds a span field this version does not map",
        ),
        (
            vec![("zone", Some("eu")), ("zone", Some("us"))],
            "row 0: attrs_map_column \"attrs\" holds the key \"zone\" twice. A span carries one \
             merged attrs map with unique keys, so one of the two would never reach the record.",
        ),
    ] {
        let dir = tempfile::tempdir().expect("tempdir");
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let batch = with_attrs_map(
            source_batch(&[bare(1, 0x11, "refused", T0, T0 + ONE_MS_NS)]),
            vec![Some(entries)],
        );
        let err = try_load_batch(&store, dir.path(), &attrs_map_mapping(), &batch)
            .await
            .expect_err("the map is refused");
        assert_eq!(err.to_string(), want);
        assert!(stored(&store, "alpha", T0, T1, LOAD_NS).await.is_empty());
    }

    let mut not_a_map = attrs_map_mapping();
    not_a_map.attrs_map_column = Some("start_ns".to_string());
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let err = try_load_batch(
        &store,
        dir.path(),
        &not_a_map,
        &source_batch(&[bare(1, 0x11, "plain", T0, T0 + ONE_MS_NS)]),
    )
    .await
    .expect_err("an integer column is not a map");
    assert_eq!(
        err.to_string(),
        "attrs_map_column \"start_ns\" has type Int64; expected a map of string keys to string \
         values"
    );
}

/// A null map value is an attribute the row does not carry, so it is skipped
/// before the collision, reserved-key and duplicate checks: none of them
/// refuses a key whose value is null.
#[tokio::test]
async fn a_null_attrs_map_value_is_skipped_before_the_key_checks() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let batch = with_attrs_map(
        source_batch(&[Src {
            method: Some("GET"),
            ..bare(1, 0x11, "null values", T0, T0 + ONE_MS_NS)
        }]),
        vec![Some(vec![
            ("http.method", None),
            ("_kind", None),
            ("zone", Some("eu")),
            ("zone", None),
        ])],
    );
    try_load_batch(&store, dir.path(), &attrs_map_mapping(), &batch)
        .await
        .expect("null values under a mapped, reserved or repeated key load");
    let spans = stored(&store, "alpha", T0, T1, LOAD_NS).await;
    assert_eq!(spans.len(), 1);
    assert_eq!(
        spans[0].attrs,
        pairs(&[("http.method", "GET"), ("zone", "eu")])
    );
}

/// `count` distinct map keys `k0000`, `k0001`, ... in ascending order.
fn numbered_keys(count: usize) -> Vec<String> {
    (0..count).map(|i| format!("k{i:04}")).collect()
}

/// A row whose `[[spans.attribute]]` values and map entries together reach
/// the loader per-record cap loads; one more is refused by name and stores
/// nothing.
#[tokio::test]
async fn attrs_map_entries_count_toward_the_loader_per_record_cap() {
    let cap = load::LOADER_MAX_ATTRIBUTES_PER_RECORD;
    let keys = numbered_keys(cap);
    let entries: Vec<(&str, Option<&str>)> =
        keys.iter().map(|key| (key.as_str(), Some("v"))).collect();

    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let at_cap = with_attrs_map(
        source_batch(&[bare(1, 0x11, "at the cap", T0, T0 + ONE_MS_NS)]),
        vec![Some(entries.clone())],
    );
    try_load_batch(&store, dir.path(), &attrs_map_mapping(), &at_cap)
        .await
        .expect("a row at the cap loads");
    let spans = stored(&store, "alpha", T0, T1, LOAD_NS).await;
    assert_eq!(spans.len(), 1);
    assert_eq!(spans[0].attrs.len(), cap);

    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let over_cap = with_attrs_map(
        source_batch(&[Src {
            method: Some("GET"),
            ..bare(1, 0x11, "one over", T0, T0 + ONE_MS_NS)
        }]),
        vec![Some(entries)],
    );
    let err = try_load_batch(&store, dir.path(), &attrs_map_mapping(), &over_cap)
        .await
        .expect_err("a row one over the cap is refused");
    assert_eq!(
        err.to_string(),
        "row 0: span carries 1025 attributes with its attrs_map_column entries, more than the \
         loader per-record cap of 1024"
    );
    assert!(stored(&store, "alpha", T0, T1, LOAD_NS).await.is_empty());
}

/// A map key over the OTLP key-length cap and a map value over the
/// value-length cap each drop that one entry and are counted in
/// `attributes_dropped`; a key and a value exactly at their caps are stored.
#[tokio::test]
async fn an_over_cap_attrs_map_key_or_value_is_dropped_and_counted() {
    let limits = SpanIngestLimits::default();
    let key_at_cap = "k".repeat(limits.max_attribute_key_len);
    let key_over_cap = "k".repeat(limits.max_attribute_key_len + 1);
    let value_at_cap = "v".repeat(limits.max_attribute_value_len);
    let value_over_cap = "v".repeat(limits.max_attribute_value_len + 1);

    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let batch = with_attrs_map(
        source_batch(&[bare(1, 0x11, "long entries", T0, T0 + ONE_MS_NS)]),
        vec![Some(vec![
            (key_at_cap.as_str(), Some("short")),
            (key_over_cap.as_str(), Some("short")),
            ("value.at.cap", Some(value_at_cap.as_str())),
            ("value.over.cap", Some(value_over_cap.as_str())),
            ("zone", Some("eu")),
        ])],
    );
    let report = try_load_batch(&store, dir.path(), &attrs_map_mapping(), &batch)
        .await
        .expect("over-cap entries drop, the span loads");
    assert_eq!(
        report.attributes_dropped, 2,
        "the over-cap key and the over-cap value"
    );
    let spans = stored(&store, "alpha", T0, T1, LOAD_NS).await;
    assert_eq!(spans.len(), 1);
    assert_eq!(
        spans[0].attrs,
        pairs(&[
            (key_at_cap.as_str(), "short"),
            ("value.at.cap", value_at_cap.as_str()),
            ("zone", "eu"),
        ])
    );
}

/// An `attrs_map_column` naming another mapped field's output column is
/// refused as any two fields on one column are, before any store request.
#[tokio::test]
async fn an_attrs_map_column_sharing_a_mapped_column_is_refused() {
    let dir = tempfile::tempdir().expect("tempdir");
    let instrumented = Arc::new(InstrumentedStore::new(MemoryStore::new()));
    let metrics = instrumented.metrics();
    let store: Arc<dyn ObjectStoreBackend> = instrumented;
    let mut mapping = attrs_map_mapping();
    mapping.attrs_map_column = Some("method".to_string());
    let out = dir.path().join("out.parquet");
    let err = export_window(&store, "alpha", T0, T1, &mapping, &out)
        .await
        .expect_err("the shared output column is refused");
    assert_eq!(
        err.to_string(),
        "the mapping writes two different fields to the output column \"method\"; give each one \
         its own column name"
    );
    assert_eq!(metrics.snapshot(), StoreMetricsSnapshot::default());
    assert!(!out.exists(), "a refused export writes no file");
}

/// The load mapping of the cap tests below: two resource attributes `r0` and
/// `r1`, and one `[[spans.attribute]]` per key `a0000` .. `a1023`, the most a
/// mapping may declare, each read from the column of its own name.
fn full_width_mapping() -> SpansMapping {
    spans_mapping(&full_width_text(
        &["r0", "r1"],
        0..load::LOADER_MAX_ATTRIBUTES_PER_RECORD,
    ))
}

/// [`REQUIRED_ONLY_MAPPING`] with `attrs_map_column = "attrs"`.
fn required_only_with_attrs_map() -> String {
    REQUIRED_ONLY_MAPPING.replace(
        "end_ts_unit     = \"micros\"\n",
        "end_ts_unit     = \"micros\"\nattrs_map_column = \"attrs\"\n",
    )
}

/// [`REQUIRED_ONLY_MAPPING`] plus one `[[spans.resource_attribute]]` per key
/// in `resource_keys` and one `[[spans.attribute]]` `a{i:04}` per `i` in
/// `attributes`, each read from the column of its own name.
fn full_width_text(resource_keys: &[&str], attributes: std::ops::Range<usize>) -> String {
    let mut text = String::from(REQUIRED_ONLY_MAPPING);
    for key in resource_keys {
        text.push_str(&format!(
            "\n[[spans.resource_attribute]]\nkey = \"{key}\"\ncolumn = \"{key}\"\ntype = \"str\"\n"
        ));
    }
    for i in attributes {
        text.push_str(&format!(
            "\n[[spans.attribute]]\nkey = \"a{i:04}\"\ncolumn = \"a{i:04}\"\ntype = \"str\"\n"
        ));
    }
    text
}

/// The export mapping of the cap tests: `r0` and `a0000` mapped to columns
/// and every other attribute in the `attrs` map.
fn one_attribute_map_mapping() -> SpansMapping {
    spans_mapping(&full_width_text(&["r0"], 0..1).replacen(
        REQUIRED_ONLY_MAPPING,
        &required_only_with_attrs_map(),
        1,
    ))
}

/// One source row under [`full_width_mapping`]: every `a` attribute set to
/// `"v"`, `r0` set, and `r1` set when `with_r1`.
struct WideRow {
    span: u8,
    start_ns: i64,
    with_r1: bool,
}

fn full_width_batch(rows: &[WideRow]) -> RecordBatch {
    let bin = |values: Vec<Vec<u8>>| -> ArrayRef {
        let refs: Vec<&[u8]> = values.iter().map(Vec::as_slice).collect();
        Arc::new(BinaryArray::from(refs))
    };
    let mut columns: Vec<(String, ArrayRef)> = vec![
        (
            "trace_id".to_string(),
            bin(rows.iter().map(|_| vec![7u8; 16]).collect()),
        ),
        (
            "span_id".to_string(),
            bin(rows.iter().map(|r| vec![r.span; 8]).collect()),
        ),
        (
            "name".to_string(),
            Arc::new(StringArray::from(vec!["wide"; rows.len()])),
        ),
        (
            "start_ns".to_string(),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.start_ns).collect::<Vec<_>>(),
            )),
        ),
        (
            "end_us".to_string(),
            Arc::new(Int64Array::from(
                rows.iter()
                    .map(|r| (r.start_ns + ONE_MS_NS) / 1_000)
                    .collect::<Vec<_>>(),
            )),
        ),
        (
            "r0".to_string(),
            Arc::new(StringArray::from(vec![Some("x"); rows.len()])),
        ),
        (
            "r1".to_string(),
            Arc::new(StringArray::from(
                rows.iter()
                    .map(|r| r.with_r1.then_some("y"))
                    .collect::<Vec<_>>(),
            )),
        ),
    ];
    for i in 0..load::LOADER_MAX_ATTRIBUTES_PER_RECORD {
        columns.push((
            format!("a{i:04}"),
            Arc::new(StringArray::from(vec!["v"; rows.len()])),
        ));
    }
    RecordBatch::try_from_iter(columns).expect("full-width batch")
}

/// The map column holds at most the loader per-record cap less the row's
/// written `[[spans.attribute]]` values, so the file always re-loads. Both
/// spans store 1024 `a` attributes and `r0`; the second also stores `r1`.
/// The export maps `a0000` and `r0` to columns, which leaves 1023 map entries
/// for each span. The first span's candidates are `a0001` .. `a1023`, exactly
/// 1023, so it re-loads at the cap with every attribute. The second's are
/// those plus `r1`, one too many: the entries are kept in ascending key
/// order, so `r1` is the one not written, the span is counted, and the rest
/// re-loads as stored. `r0` has its own column and takes no map room, as
/// resource attributes do not count toward the load's cap.
#[tokio::test]
async fn a_spans_export_writes_no_more_map_entries_than_its_load_reads() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let source = dir.path().join("wide.parquet");
    write_parquet(
        &source,
        &full_width_batch(&[
            WideRow {
                span: 0x11,
                start_ns: T0,
                with_r1: false,
            },
            WideRow {
                span: 0x22,
                start_ns: T0 + 2 * ONE_MS_NS,
                with_r1: true,
            },
        ]),
    );
    load_file(&store, &source, "alpha", &full_width_mapping(), LOAD_NS).await;
    let alpha = stored(&store, "alpha", T0, T1, LOAD_NS).await;
    assert_eq!(
        alpha
            .iter()
            .map(|span| span.attrs.len())
            .collect::<Vec<_>>(),
        vec![1025, 1026]
    );

    let mapping = one_attribute_map_mapping();
    let export_pq = dir.path().join("export.parquet");
    let report = export_window(&store, "alpha", T0, T1, &mapping, &export_pq)
        .await
        .expect("export succeeds");
    assert_eq!(report.rows_written, 2);
    assert_eq!(
        report.spans_with_unwritten_data, 1,
        "only the span whose map entries passed the cap"
    );
    let maps = map_values(&read_parquet(&export_pq), "attrs");
    let expected_map: Vec<(String, String)> = (1..load::LOADER_MAX_ATTRIBUTES_PER_RECORD)
        .map(|i| (format!("a{i:04}"), "v".to_string()))
        .collect();
    assert_eq!(maps, vec![expected_map.clone(), expected_map]);

    load_file(&store, &export_pq, "beta", &mapping, LOAD_NS + ONE_SEC_NS).await;
    let beta = stored(&store, "beta", T0, T1, LOAD_NS + ONE_SEC_NS).await;
    let expected: Vec<SpanRecord> = alpha
        .into_iter()
        .map(|mut span| {
            span.attrs.retain(|(key, _)| key != "r1");
            span
        })
        .collect();
    assert_eq!(
        beta, expected,
        "the at-cap span re-loads with every attribute, the over-cap span without r1"
    );
}

/// A span whose `[[spans.attribute]]` values alone fill the loader per-record
/// cap leaves no room in the map: both spans store 1024 `a` attributes, all
/// mapped to columns, and the second also stores the unmapped `r1`. The export
/// writes an empty map for each, counts only the second, and the file re-loads
/// every span as stored less `r1`.
#[tokio::test]
async fn span_attributes_alone_at_the_cap_leave_no_map_room() {
    let cap = load::LOADER_MAX_ATTRIBUTES_PER_RECORD;
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let source = dir.path().join("wide.parquet");
    write_parquet(
        &source,
        &full_width_batch(&[
            WideRow {
                span: 0x11,
                start_ns: T0,
                with_r1: false,
            },
            WideRow {
                span: 0x22,
                start_ns: T0 + 2 * ONE_MS_NS,
                with_r1: true,
            },
        ]),
    );
    load_file(&store, &source, "alpha", &full_width_mapping(), LOAD_NS).await;
    let alpha = stored(&store, "alpha", T0, T1, LOAD_NS).await;
    assert_eq!(
        alpha
            .iter()
            .map(|span| span.attrs.len())
            .collect::<Vec<_>>(),
        vec![1025, 1026]
    );

    let mapping = spans_mapping(&full_width_text(&["r0"], 0..cap).replacen(
        REQUIRED_ONLY_MAPPING,
        &required_only_with_attrs_map(),
        1,
    ));
    let export_pq = dir.path().join("export.parquet");
    let report = export_window(&store, "alpha", T0, T1, &mapping, &export_pq)
        .await
        .expect("export succeeds");
    assert_eq!(report.rows_written, 2);
    assert_eq!(
        report.spans_with_unwritten_data, 1,
        "only the span storing r1, which has no room in the map"
    );
    assert_eq!(
        map_values(&read_parquet(&export_pq), "attrs"),
        vec![Vec::new(), Vec::new()],
        "the mapped attributes alone reach the cap, so neither map holds an entry"
    );

    load_file(&store, &export_pq, "beta", &mapping, LOAD_NS + ONE_SEC_NS).await;
    let beta = stored(&store, "beta", T0, T1, LOAD_NS + ONE_SEC_NS).await;
    let expected: Vec<SpanRecord> = alpha
        .into_iter()
        .map(|mut span| {
            span.attrs.retain(|(key, _)| key != "r1");
            span
        })
        .collect();
    assert_eq!(beta, expected, "every span re-loads as stored, less r1");
}

/// A mapped `[[spans.attribute]]` the span does not store writes a null cell,
/// which a load does not count toward the cap, so the map gets that slot back.
/// The span stores `r1` and `a0001` .. `a1023` but no `a0000`: under the
/// export mapping that maps `a0000` those are 1024 map candidates, exactly the
/// cap, so every one is written, nothing is counted, and the file re-loads the
/// span as stored.
#[tokio::test]
async fn a_null_mapped_attribute_gives_its_slot_back_to_the_map() {
    let dir = tempfile::tempdir().expect("tempdir");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let source = dir.path().join("wide.parquet");
    let wide = full_width_batch(&[WideRow {
        span: 0x11,
        start_ns: T0,
        with_r1: true,
    }]);
    let rows = wide.num_rows();
    let columns: Vec<(String, ArrayRef)> = wide
        .schema()
        .fields()
        .iter()
        .zip(wide.columns())
        .map(|(field, column)| {
            let column = if field.name() == "a0000" {
                Arc::new(StringArray::from(vec![None::<&str>; rows])) as ArrayRef
            } else {
                Arc::clone(column)
            };
            (field.name().clone(), column)
        })
        .collect();
    write_parquet(
        &source,
        &RecordBatch::try_from_iter(columns).expect("batch without a0000"),
    );
    load_file(&store, &source, "alpha", &full_width_mapping(), LOAD_NS).await;
    let alpha = stored(&store, "alpha", T0, T1, LOAD_NS).await;
    assert_eq!(alpha.len(), 1);
    assert_eq!(alpha[0].attrs.len(), 1025, "r0, r1 and a0001 .. a1023");
    assert!(!alpha[0].attrs.iter().any(|(key, _)| key == "a0000"));

    let mapping = one_attribute_map_mapping();
    let export_pq = dir.path().join("export.parquet");
    let report = export_window(&store, "alpha", T0, T1, &mapping, &export_pq)
        .await
        .expect("export succeeds");
    assert_eq!(report.rows_written, 1);
    assert_eq!(
        report.spans_with_unwritten_data, 0,
        "the null a0000 cell takes no map room, so r1 fits"
    );
    let mut expected_map: Vec<(String, String)> = (1..load::LOADER_MAX_ATTRIBUTES_PER_RECORD)
        .map(|i| (format!("a{i:04}"), "v".to_string()))
        .collect();
    expected_map.push(("r1".to_string(), "y".to_string()));
    assert_eq!(
        map_values(&read_parquet(&export_pq), "attrs"),
        vec![expected_map]
    );

    load_file(&store, &export_pq, "beta", &mapping, LOAD_NS + ONE_SEC_NS).await;
    let beta = stored(&store, "beta", T0, T1, LOAD_NS + ONE_SEC_NS).await;
    assert_eq!(beta, alpha, "the span re-loads with every attribute");
}
