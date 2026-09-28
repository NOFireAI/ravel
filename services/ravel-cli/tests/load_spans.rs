//! End-to-end coverage for `ravel-cli load --signal spans` (ADR-1751
//! decisions 1 and 2, follow-up task 2; issues #1751 and #1712).
//!
//! These drive the loader's real entry points in-process against a shared
//! `MemoryStore` and read the loaded spans back through
//! `SpanSegmentFetcher::fetch_accounted`, the funnel a production spans query
//! takes. A subprocess against `--store memory` cannot be used for the round
//! trip: each process gets its own empty in-memory store, so a second process
//! could never see the first's writes (the same reason `tests/load.rs` and
//! `tests/load_metrics.rs` drive the library entry points in-process).
//!
//! The differential test compares the STORED records against
//! `ravel_otlp::normalize_traces`'s own output for the same spans, rather than
//! against the loader's internal normalization: a comparison against the
//! loader's own helpers would assert nothing about parity.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use arrow::array::{
    ArrayRef, BinaryArray, BooleanArray, FixedSizeBinaryArray, Float64Array, Int64Array,
    StringArray,
};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;

use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_cli::load::{self, LoadError, SpansLoadReport};
use ravel_cli::maintain::SignalArg;
use ravel_ingest::SystemClock;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::fault::{FaultPlan, FaultStore, Op, ScriptedFault, Sequence};
use ravel_object_store::memory::MemoryStore;
use ravel_otlp::{NormalizedSpan, SpanIngestLimits, normalize_traces};
use ravel_query::SpanSegmentFetcher;
use ravel_rspan::{SpanQuery, SpanRecord, StatusCode};
use ravel_types::accounting::QueryAccounting;
use ravel_types::{CommitToken, Signal, TenantId, TimeRange};

const NS_PER_SEC: i64 = 1_000_000_000;
const NS_PER_MIN: i64 = 60 * NS_PER_SEC;
const NS_PER_DAY: i64 = 86_400 * NS_PER_SEC;

/// Wall clock floored to a whole second. Real wall time, not a pinned
/// constant: a span buckets by the flush-open clock, and the catalog listing
/// window a later resolve derives runs to `now`. Pinning the load clock to a
/// fixed past instant while the resolve reads the system clock would make that
/// window span the years between the two.
fn now_ns() -> i64 {
    let dur = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch");
    let ns = i64::try_from(dur.as_nanos()).expect("time overflow");
    (ns / NS_PER_SEC) * NS_PER_SEC
}

fn write_parquet(path: &Path, batch: &RecordBatch) {
    let file = std::fs::File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
    writer.write(batch).expect("write batch");
    writer.close().expect("close writer");
}

fn i64_col(vals: Vec<i64>) -> ArrayRef {
    Arc::new(Int64Array::from(vals))
}

fn opt_i64_col(vals: Vec<Option<i64>>) -> ArrayRef {
    Arc::new(Int64Array::from(vals))
}

fn f64_col(vals: Vec<Option<f64>>) -> ArrayRef {
    Arc::new(Float64Array::from(vals))
}

fn bool_col(vals: Vec<Option<bool>>) -> ArrayRef {
    Arc::new(BooleanArray::from(vals))
}

fn str_col(vals: Vec<&str>) -> ArrayRef {
    Arc::new(StringArray::from(vals))
}

fn opt_str_col(vals: Vec<Option<&str>>) -> ArrayRef {
    Arc::new(StringArray::from(vals))
}

/// A `Binary` column of optional byte values, the natural Parquet shape for an
/// id column whose rows may be absent (a root span's parent).
fn opt_bin_col(vals: Vec<Option<Vec<u8>>>) -> ArrayRef {
    let refs: Vec<Option<&[u8]>> = vals.iter().map(|v| v.as_deref()).collect();
    Arc::new(BinaryArray::from(refs))
}

fn bin_col(vals: Vec<Vec<u8>>) -> ArrayRef {
    opt_bin_col(vals.into_iter().map(Some).collect())
}

fn spans_mapping(text: &str) -> load::SpansMapping {
    load::parse_spans_mapping(text).expect("valid spans mapping")
}

/// The stored spans for `tenant`, read back through the same
/// `SpanSegmentFetcher` funnel a production spans query takes, over every
/// segment `Catalog::resolve` reports for the window.
async fn read_back_spans(
    store: &Arc<dyn ObjectStoreBackend>,
    tenant: &TenantId,
    range: TimeRange,
    min_tokens: &[CommitToken],
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
    let snapshot = catalog
        .resolve(&tenant.hash(), Signal::Spans, range, min_tokens, now_ns)
        .await
        .expect("catalog resolve");
    let fetcher = SpanSegmentFetcher::new(Arc::clone(store));
    let query = SpanQuery::ts_range(range.start_ns, range.end_ns);
    let mut out = Vec::new();
    for seg in &snapshot.segments {
        let fetched = fetcher
            .fetch_accounted(
                seg,
                tenant.hash(),
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

/// The mapping every test in this file loads through, naming every mappable
/// span field ADR-1751 decision 2 lists.
const FULL_MAPPING: &str = r#"
[spans]
trace_id_column       = "trace_id"
span_id_column        = "span_id"
parent_span_id_column = "parent_span_id"
name_column           = "name"
start_ts_column       = "start_ns"
start_ts_unit         = "nanos"
end_ts_column         = "end_ns"
end_ts_unit           = "nanos"
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

/// One fixture span, in the shape both sides of the differential test build
/// from: the Parquet row and the OTLP `Span` are generated from this, so
/// neither side can drift from the other by hand.
struct Fixture {
    trace_id: [u8; 16],
    span_id: [u8; 8],
    parent_span_id: Option<[u8; 8]>,
    name: &'static str,
    start_ns: i64,
    end_ns: i64,
    status_code: Option<i64>,
    status_message: Option<&'static str>,
    service: Option<&'static str>,
    method: Option<&'static str>,
    http_status: Option<i64>,
    queue_seconds: Option<f64>,
    cache_hit: Option<bool>,
    digest: Option<Vec<u8>>,
}

/// Fixtures that vary every mapped field, including an empty attribute value,
/// the non-string attribute columns, and a span with no parent.
fn fixtures(base_ns: i64) -> Vec<Fixture> {
    vec![
        // A root span: no parent, status ok, every attribute column set, and
        // an EMPTY string attribute value (which, unlike a metric label, is a
        // value OTLP stores rather than dropping).
        Fixture {
            trace_id: [1u8; 16],
            span_id: [0x11u8; 8],
            parent_span_id: None,
            name: "GET /checkout",
            start_ns: base_ns,
            end_ns: base_ns + 250 * 1_000_000,
            status_code: Some(1),
            status_message: Some("fine"),
            service: Some("checkout"),
            method: Some(""),
            http_status: Some(200),
            queue_seconds: Some(0.125),
            cache_hit: Some(true),
            digest: Some(vec![0xde, 0xad, 0xbe, 0xef]),
        },
        // A child span: status error, a negative integer, and an infinite
        // float, whose spelling is the one place `format_float` diverges from
        // Rust's own `Display` -- so the assertion below that both paths spell
        // it `+Inf` is evidence the loader goes through the shared formatter
        // rather than merely happening to agree.
        Fixture {
            trace_id: [1u8; 16],
            span_id: [0x22u8; 8],
            parent_span_id: Some([0x11u8; 8]),
            name: "SELECT orders",
            start_ns: base_ns + 1_000_000,
            end_ns: base_ns + 9_000_000,
            status_code: Some(2),
            status_message: Some("deadlock detected"),
            service: Some("orders-db"),
            method: Some("POST"),
            http_status: Some(-1),
            queue_seconds: Some(f64::INFINITY),
            cache_hit: Some(false),
            digest: Some(vec![]),
        },
        // A span with nothing optional set: no parent, a null status column
        // (which is Unset, as an OTLP span with no status is), a null status
        // message, and every attribute cell null (an attribute the row does
        // not carry, as an OTLP span that omits the key).
        Fixture {
            trace_id: [2u8; 16],
            span_id: [0x33u8; 8],
            parent_span_id: None,
            name: "background sweep",
            start_ns: base_ns + 2_000_000,
            end_ns: base_ns + 2_000_000,
            status_code: None,
            status_message: None,
            service: None,
            method: None,
            http_status: None,
            queue_seconds: None,
            cache_hit: None,
            digest: None,
        },
    ]
}

/// The Parquet batch for a fixture set, in [`FULL_MAPPING`]'s column names.
fn fixture_batch(fixtures: &[Fixture]) -> RecordBatch {
    RecordBatch::try_from_iter(vec![
        (
            "trace_id".to_string(),
            bin_col(fixtures.iter().map(|f| f.trace_id.to_vec()).collect()),
        ),
        (
            "span_id".to_string(),
            bin_col(fixtures.iter().map(|f| f.span_id.to_vec()).collect()),
        ),
        (
            "parent_span_id".to_string(),
            opt_bin_col(
                fixtures
                    .iter()
                    .map(|f| f.parent_span_id.map(|p| p.to_vec()))
                    .collect(),
            ),
        ),
        (
            "name".to_string(),
            str_col(fixtures.iter().map(|f| f.name).collect()),
        ),
        (
            "start_ns".to_string(),
            i64_col(fixtures.iter().map(|f| f.start_ns).collect()),
        ),
        (
            "end_ns".to_string(),
            i64_col(fixtures.iter().map(|f| f.end_ns).collect()),
        ),
        (
            "status_code".to_string(),
            opt_i64_col(fixtures.iter().map(|f| f.status_code).collect()),
        ),
        (
            "status_message".to_string(),
            opt_str_col(fixtures.iter().map(|f| f.status_message).collect()),
        ),
        (
            "svc".to_string(),
            opt_str_col(fixtures.iter().map(|f| f.service).collect()),
        ),
        (
            "method".to_string(),
            opt_str_col(fixtures.iter().map(|f| f.method).collect()),
        ),
        (
            "http_status".to_string(),
            opt_i64_col(fixtures.iter().map(|f| f.http_status).collect()),
        ),
        (
            "queue_seconds".to_string(),
            f64_col(fixtures.iter().map(|f| f.queue_seconds).collect()),
        ),
        (
            "cache_hit".to_string(),
            bool_col(fixtures.iter().map(|f| f.cache_hit).collect()),
        ),
        (
            "digest".to_string(),
            opt_bin_col(fixtures.iter().map(|f| f.digest.clone()).collect()),
        ),
    ])
    .expect("record batch")
}

fn kv(key: &str, value: AnyValueVariant) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue { value: Some(value) }),
        ..Default::default()
    }
}

/// The OTLP request for a fixture set: one `ResourceSpans` per span, because
/// the mapping carries `service.name` per row and a resource attribute set is
/// per `ResourceSpans` in OTLP. The scope is absent, so `normalize_traces`
/// adds no `otel.scope.*` attributes; kind, trace state, flags, events and
/// links are all left at their defaults, so it writes no reserved keys either
/// -- exactly the fields ADR-1751 decision 2 declines to map.
fn fixture_request(fixtures: &[Fixture]) -> ExportTraceServiceRequest {
    let resource_spans = fixtures
        .iter()
        .map(|f| {
            let mut attributes = Vec::new();
            if let Some(v) = f.method {
                attributes.push(kv(
                    "http.method",
                    AnyValueVariant::StringValue(v.to_string()),
                ));
            }
            if let Some(v) = f.http_status {
                attributes.push(kv("http.status_code", AnyValueVariant::IntValue(v)));
            }
            if let Some(v) = f.queue_seconds {
                attributes.push(kv("queue.seconds", AnyValueVariant::DoubleValue(v)));
            }
            if let Some(v) = f.cache_hit {
                attributes.push(kv("cache.hit", AnyValueVariant::BoolValue(v)));
            }
            if let Some(v) = &f.digest {
                attributes.push(kv("request.digest", AnyValueVariant::BytesValue(v.clone())));
            }
            let span = Span {
                trace_id: f.trace_id.to_vec(),
                span_id: f.span_id.to_vec(),
                parent_span_id: f.parent_span_id.map(|p| p.to_vec()).unwrap_or_default(),
                name: f.name.to_string(),
                start_time_unix_nano: f.start_ns as u64,
                end_time_unix_nano: f.end_ns as u64,
                attributes,
                status: f.status_code.map(|code| Status {
                    code: code as i32,
                    message: f.status_message.unwrap_or_default().to_string(),
                }),
                ..Default::default()
            };
            let resource_attrs = f
                .service
                .map(|svc| {
                    vec![kv(
                        "service.name",
                        AnyValueVariant::StringValue(svc.to_string()),
                    )]
                })
                .unwrap_or_default();
            ResourceSpans {
                resource: Some(Resource {
                    attributes: resource_attrs,
                    ..Default::default()
                }),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans: vec![span],
                    ..Default::default()
                }],
                ..Default::default()
            }
        })
        .collect();
    ExportTraceServiceRequest { resource_spans }
}

/// Run a spans load of `batch` into a fresh `MemoryStore` under `mapping`.
async fn load_batch(
    batch: &RecordBatch,
    mapping: &load::SpansMapping,
    load_ns: i64,
) -> (Arc<dyn ObjectStoreBackend>, SpansLoadReport) {
    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("spans.parquet");
    write_parquet(&pq, batch);
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let report = load::load_spans(
        Arc::clone(&store),
        &pq,
        "acme",
        mapping,
        1,
        10_000,
        0,
        1,
        1,
        1,
        None,
        load_ns,
        Arc::new(SystemClock),
    )
    .await
    .expect("the spans load succeeds");
    (store, report)
}

/// Acceptance (ADR-1751 follow-up task 2): one span loaded from Parquet
/// through the real `load_spans` entry point into a `MemoryStore` reads back
/// through the span fetch path a production query uses, with every mapped
/// field exact.
///
/// The thirty-day gap is part of the assertion: ADR-0089's past-event-time lag
/// relaxation (widened to every signal by ADR-1751 decision 1) is what admits
/// the span at all, and the object buckets by the load-time flush clock rather
/// than the event time, which is what keeps it discoverable.
#[tokio::test]
async fn a_loaded_span_reads_back_through_the_span_query_path() {
    let load_ns = now_ns();
    let event_ns = load_ns - 30 * NS_PER_DAY;

    let trace_id = [0xABu8; 16];
    let span_id = [0x01u8; 8];
    let parent_span_id = [0x02u8; 8];
    let batch = RecordBatch::try_from_iter(vec![
        ("trace_id".to_string(), bin_col(vec![trace_id.to_vec()])),
        ("span_id".to_string(), bin_col(vec![span_id.to_vec()])),
        (
            "parent_span_id".to_string(),
            opt_bin_col(vec![Some(parent_span_id.to_vec())]),
        ),
        ("name".to_string(), str_col(vec!["GET /checkout"])),
        ("start_ns".to_string(), i64_col(vec![event_ns])),
        (
            "end_ns".to_string(),
            i64_col(vec![event_ns + 250 * 1_000_000]),
        ),
        ("status_code".to_string(), opt_i64_col(vec![Some(2)])),
        (
            "status_message".to_string(),
            opt_str_col(vec![Some("upstream timeout")]),
        ),
        ("svc".to_string(), opt_str_col(vec![Some("checkout")])),
        ("method".to_string(), opt_str_col(vec![Some("GET")])),
        ("http_status".to_string(), opt_i64_col(vec![Some(504)])),
        ("queue_seconds".to_string(), f64_col(vec![Some(0.125)])),
        ("cache_hit".to_string(), bool_col(vec![Some(false)])),
        (
            "digest".to_string(),
            opt_bin_col(vec![Some(vec![0xde, 0xad])]),
        ),
    ])
    .expect("record batch");

    let mapping = spans_mapping(FULL_MAPPING);
    let (store, report) = load_batch(&batch, &mapping, load_ns).await;
    assert_eq!(report.rows_processed, 1, "one source row, one span");
    assert_eq!(report.objects_written(), 1, "one RSPAN object");

    let tenant = TenantId::new("acme");
    let records = read_back_spans(
        &store,
        &tenant,
        TimeRange {
            start_ns: event_ns - NS_PER_MIN,
            end_ns: event_ns + NS_PER_MIN,
        },
        &report.tokens,
        load_ns,
    )
    .await;

    assert_eq!(records.len(), 1, "exactly one span reads back");
    let got = &records[0];
    assert_eq!(got.trace_id, trace_id, "the trace id round-trips as bytes");
    assert_eq!(got.span_id, span_id, "the span id round-trips as bytes");
    assert_eq!(got.parent_span_id, Some(parent_span_id));
    assert_eq!(got.name, "GET /checkout");
    assert_eq!(
        got.start_ts_ns, event_ns,
        "the span keeps its own thirty-day-old start time, not load time"
    );
    assert_eq!(got.end_ts_ns, event_ns + 250 * 1_000_000);
    assert_eq!(got.status_code, StatusCode::Error);
    assert_eq!(got.status_message.as_deref(), Some("upstream timeout"));
    assert_eq!(
        got.attrs,
        vec![
            ("cache.hit".to_string(), "false".to_string()),
            ("http.method".to_string(), "GET".to_string()),
            ("http.status_code".to_string(), "504".to_string()),
            ("queue.seconds".to_string(), "0.125".to_string()),
            ("request.digest".to_string(), "dead".to_string()),
            ("service.name".to_string(), "checkout".to_string()),
        ],
        "every mapped attribute is stored under its mapped key, coerced to the string form OTLP \
         stores: an integer verbatim, a float through the Go-compatible formatter, a bool as \
         true/false, and bytes as lowercase hex"
    );
}

/// Differential (ADR-1751 decision 1's parity requirement): the same spans
/// loaded from Parquet and normalized from OTLP produce identical records,
/// field by field.
///
/// The comparison is against `ravel_otlp::normalize_traces`'s own output, not
/// against the loader's normalization helpers, and it runs over the STORED
/// records, so it covers the coercion, the resource-over-span merge and the
/// RSPAN round trip together.
#[tokio::test]
async fn loaded_spans_match_normalize_traces_field_for_field() {
    let load_ns = now_ns();
    // Recent, because the OTLP side of this comparison DOES enforce the
    // past-lag bound the loader relaxes: a thirty-day-old span would be
    // rejected there and admitted here, which is the subject of the
    // acceptance test above, not of this one.
    let base_ns = load_ns - 60 * NS_PER_SEC;
    let fixtures = fixtures(base_ns);

    let mapping = spans_mapping(FULL_MAPPING);
    let (store, report) = load_batch(&fixture_batch(&fixtures), &mapping, load_ns).await;
    assert_eq!(report.rows_processed, fixtures.len() as u64);

    let tenant = TenantId::new("acme");
    let loaded = read_back_spans(
        &store,
        &tenant,
        TimeRange {
            start_ns: base_ns - NS_PER_MIN,
            end_ns: base_ns + NS_PER_MIN,
        },
        &report.tokens,
        load_ns,
    )
    .await;

    let out = normalize_traces(
        fixture_request(&fixtures),
        &SpanIngestLimits::default(),
        load_ns,
    );
    assert!(
        out.rejected.is_empty(),
        "the OTLP side admits every fixture: {:?}",
        out.rejected
    );
    let mut expected: Vec<NormalizedSpan> = out.spans;
    expected.sort_by_key(|span| (span.trace_id, span.span_id));

    assert_eq!(
        loaded.len(),
        expected.len(),
        "both paths admit the same number of spans"
    );
    for (got, want) in loaded.iter().zip(&expected) {
        assert_eq!(got.trace_id, want.trace_id, "trace_id");
        assert_eq!(got.span_id, want.span_id, "span_id");
        assert_eq!(
            got.parent_span_id, want.parent_span_id,
            "parent_span_id for {}",
            want.name
        );
        assert_eq!(got.name, want.name, "name");
        assert_eq!(got.start_ts_ns, want.start_ts_ns, "start_ts_ns");
        assert_eq!(got.end_ts_ns, want.end_ts_ns, "end_ts_ns");
        assert_eq!(got.status_code, want.status_code, "status_code");
        assert_eq!(
            got.status_message, want.status_message,
            "status_message for {}",
            want.name
        );
        assert_eq!(got.attrs, want.attrs, "attrs for {}", want.name);
    }

    // Non-vacuity: the fixtures really do exercise the shapes the parity claim
    // rests on, so a generator that quietly stopped producing them would fail
    // here rather than leave the comparison trivially true.
    let all_attrs: Vec<&(String, String)> = expected.iter().flat_map(|s| s.attrs.iter()).collect();
    assert!(
        all_attrs
            .iter()
            .any(|(k, v)| k == "http.method" && v.is_empty()),
        "an empty attribute value is stored, not dropped"
    );
    assert!(
        all_attrs
            .iter()
            .any(|(k, v)| k == "queue.seconds" && v == "+Inf"),
        "a float attribute goes through the shared Go-compatible formatter on both paths, which \
         is the one place its spelling differs from Rust's own Display: {all_attrs:?}"
    );
    assert!(
        expected.iter().any(|s| s.parent_span_id.is_none())
            && expected.iter().any(|s| s.parent_span_id.is_some()),
        "the fixtures carry both a root span and a child span"
    );
    assert!(
        expected.iter().any(|s| s.status_code == StatusCode::Unset)
            && expected.iter().any(|s| s.status_code == StatusCode::Error),
        "the fixtures carry both an unset and an explicit status"
    );
}

/// A mapping that names span events or span links is refused by name, not as
/// a generic unknown-field typo (ADR-1751 decision 2).
#[test]
fn a_mapping_naming_events_or_links_is_refused() {
    for text in [
        r#"
        [spans]
        trace_id_column = "trace_id"
        span_id_column = "span_id"
        name_column = "name"
        start_ts_column = "start"
        start_ts_unit = "nanos"
        end_ts_column = "end"
        end_ts_unit = "nanos"

        [[spans.events]]
        name_column = "event_name"
        "#,
        r#"
        [spans]
        trace_id_column = "trace_id"
        span_id_column = "span_id"
        name_column = "name"
        start_ts_column = "start"
        start_ts_unit = "nanos"
        end_ts_column = "end"
        end_ts_unit = "nanos"

        [[spans.links]]
        trace_id_column = "linked_trace"
        "#,
        // The reserved attrs key the OTLP path stores events under: a mapped
        // column here could only fabricate a field this version does not map.
        r#"
        [spans]
        trace_id_column = "trace_id"
        span_id_column = "span_id"
        name_column = "name"
        start_ts_column = "start"
        start_ts_unit = "nanos"
        end_ts_column = "end"
        end_ts_unit = "nanos"

        [[spans.attribute]]
        key = "_events_raw"
        column = "events_blob"
        type = "str"
        "#,
    ] {
        let err = load::parse_spans_mapping(text).expect_err("events and links are not mappable");
        let LoadError::Setup(message) = err else {
            panic!("expected a setup error");
        };
        // Asserting on "events" alone would pass on serde's own unknown-field
        // error, which quotes the offending key and therefore contains the
        // word too. This phrase appears only in the named refusal.
        assert!(
            message.contains("which this version does not map"),
            "the named refusal, not a generic unknown-field error: {message}"
        );
        assert!(
            !message.contains("unknown field"),
            "events and links are refused by name, not as a typo: {message}"
        );
    }
}

/// An id column that cannot carry an id of the right width is refused with its
/// own message, before any row is decoded.
#[tokio::test]
async fn an_id_column_of_the_wrong_width_is_refused() {
    let load_ns = now_ns();
    let event_ns = load_ns - 60 * NS_PER_SEC;
    // A FixedSizeBinary(8) trace_id column: the schema itself says no row can
    // produce the 16 bytes a trace id is.
    let narrow: ArrayRef = Arc::new(
        FixedSizeBinaryArray::try_from_iter(vec![[0x11u8; 8]].into_iter()).expect("fixed binary"),
    );
    let batch = RecordBatch::try_from_iter(vec![
        ("trace_id".to_string(), narrow),
        ("span_id".to_string(), bin_col(vec![vec![0x22u8; 8]])),
        ("name".to_string(), str_col(vec!["op"])),
        ("start_ns".to_string(), i64_col(vec![event_ns])),
        ("end_ns".to_string(), i64_col(vec![event_ns])),
    ])
    .expect("record batch");

    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("narrow.parquet");
    write_parquet(&pq, &batch);
    let mapping = spans_mapping(MINIMAL_MAPPING);
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let err = load::load_spans(
        Arc::clone(&store),
        &pq,
        "acme",
        &mapping,
        1,
        10_000,
        0,
        1,
        1,
        1,
        None,
        load_ns,
        Arc::new(SystemClock),
    )
    .await
    .expect_err("a trace_id column of the wrong width cannot load");
    let LoadError::BatchFailed { reason, .. } = &err else {
        panic!("expected a batch failure, got {err}");
    };
    assert_eq!(
        reason,
        "id column \"trace_id\" is FixedSizeBinary(8), but this id is 16 bytes. Ravel never pads \
         or truncates an id, so no row of this column can produce one.",
        "the refusal names the column, the width it has, and the width it needs"
    );
}

/// The smallest legal spans mapping: every optional field omitted.
const MINIMAL_MAPPING: &str = r#"
[spans]
trace_id_column = "trace_id"
span_id_column  = "span_id"
name_column     = "name"
start_ts_column = "start_ns"
start_ts_unit   = "nanos"
end_ts_column   = "end_ns"
end_ts_unit     = "nanos"
"#;

/// A `[metrics]` section cannot serve `--signal spans`, and a `[spans]`
/// section cannot serve another signal (ADR-1751 decision 2).
#[test]
fn a_metrics_section_with_signal_spans_is_refused() {
    let metrics_section = r#"
        [metrics]
        name = "m"
        value_column = "v"
        ts_column = "ts"
        ts_unit = "nanos"
        "#;
    let err = load::parse_mapping_document(metrics_section, SignalArg::Spans)
        .expect_err("a [metrics] section cannot serve --signal spans");
    let LoadError::Setup(message) = err else {
        panic!("expected a setup error");
    };
    assert_eq!(
        message,
        "--mapping file declares a [metrics] section but --signal is spans. Exactly one section \
         must be present and it must match --signal (ADR-1751 decision 2).",
        "the refusal names both the section and the signal"
    );

    let err = load::parse_mapping_document(MINIMAL_MAPPING, SignalArg::Metrics)
        .expect_err("a [spans] section cannot serve --signal metrics");
    let LoadError::Setup(message) = err else {
        panic!("expected a setup error");
    };
    assert!(
        message.contains("[spans]") && message.contains("metrics"),
        "the refusal names both the section and the signal: {message}"
    );
}

/// The final drain keeps every durable commit token: when a later batch's
/// write fails, the tokens the earlier batches committed are reported with the
/// error rather than dropped with the report.
///
/// Driven by a scripted fault on the Nth data-object PUT, so the failure lands
/// after at least one batch has acked durable.
#[tokio::test]
async fn a_failed_spans_load_reports_the_tokens_that_landed() {
    let load_ns = now_ns();
    let base_ns = load_ns - 60 * NS_PER_SEC;
    // Four single-span batches (one span per batch at --batch-rows 1), all on
    // one trace so every write lands on the same shard.
    let mut fixtures = Vec::new();
    for i in 0..4u8 {
        fixtures.push(Fixture {
            trace_id: [9u8; 16],
            span_id: [i + 1; 8],
            parent_span_id: None,
            name: "op",
            start_ns: base_ns,
            end_ns: base_ns,
            status_code: None,
            status_message: None,
            service: Some("api"),
            method: None,
            http_status: None,
            queue_seconds: None,
            cache_hit: None,
            digest: None,
        });
    }
    let dir = tempfile::tempdir().expect("tempdir");
    let pq = dir.path().join("spans.parquet");
    write_parquet(&pq, &fixture_batch(&fixtures));

    // Fail the SECOND span data-object PUT and every retry of it, so the first
    // batch is durable and the second batch's flush is abandoned. The key
    // filter `/s/l0/` matches only span data objects, not the provisioning
    // record or commit records, so the first data PUT passes through.
    let fault = ScriptedFault::Transient("injected PUT failure".into());
    let mut seq = Sequence::new(Op::Put)
        .with_key_contains("/s/l0/")
        .then_passthrough();
    for _ in 0..8 {
        seq = seq.then_fault(fault.clone());
    }
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(FaultStore::new(
        MemoryStore::new(),
        FaultPlan::empty().with_sequence(seq),
    ));

    let mapping = spans_mapping(FULL_MAPPING);
    let err = load::load_spans(
        Arc::clone(&store),
        &pq,
        "acme",
        &mapping,
        1,
        1,
        0,
        1,
        1,
        1,
        None,
        load_ns,
        Arc::new(SystemClock),
    )
    .await
    .expect_err("the scripted PUT fault fails the load");

    assert!(
        matches!(err, LoadError::Flush { .. }),
        "expected a flush failure, got: {err}"
    );
    assert_eq!(
        err.durable_tokens().len(),
        1,
        "exactly the first batch was durable before the failure, and its token is reported with \
         the error rather than dropped with the report: {err}"
    );
    let resume = err
        .resume_figures()
        .expect("a flush failure carries figures");
    assert_eq!(
        (resume.rows_skipped, resume.rows_written),
        (0, 1),
        "one source row landed, so the resume offset is that row boundary"
    );
    assert_eq!(
        resume.next_skip_rows(),
        1,
        "the resume offset lands on a row boundary"
    );
}
