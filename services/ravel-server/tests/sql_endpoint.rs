//! End-to-end tests for `POST /api/v1/sql` (ticket B3, issue #22).
//!
//! Real RSEG segments, real commit records, a real catalog, and the route
//! driven through `tower::ServiceExt::oneshot`, so everything asserted here
//! is what a client would actually receive.
//!
//! The redaction tests are the reason this file exists rather than relying on
//! the unit tests in ravel-sql: `SqlError::client_message` can be correct
//! while the handler still formats `{err}` into the body somewhere, and only
//! an assertion over the real response bytes catches that.

#![cfg(feature = "sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_ingest::Clock;
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{
    AttrValue, LogRecord, Predicate, RlogConfig, RlogReader, RlogWriter, stream_attrs_bytes,
};
use ravel_maintain::QUERY_AUDIT_SHARD;
use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Rule, ScriptedFault};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, list_all};
use ravel_query::http::StaticBearerTokenResolver;
use ravel_query::{LogSegmentFetcher, SegmentFetcher};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_server::sql::{ARROW_STREAM_MEDIA_TYPE, SqlState, router};
use ravel_sql::{SqlConfig, SqlExecutor};
use ravel_types::logstream::log_stream_id;
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantId};
use serde_json::Value;
use tower::ServiceExt;
use uuid::Uuid;

const NS_PER_HOUR: i64 = 3_600_000_000_000;
/// Small on purpose: `Catalog::resolve` issues one LIST per (shard,
/// ingest-hour) pair across the window, so a wall-clock value would fan out
/// to hundreds of thousands of LISTs.
const NOW_NS: i64 = 4 * NS_PER_HOUR;

/// Raw backend text that must never appear in a client body.
const RAW_STORE_TEXT: &str = "bucket=prod-telemetry endpoint=s3.internal request-id=abc";

/// A clock frozen at [`NOW_NS`]: the endpoint reads it once per request and
/// threads the same value through resolution and the retry.
struct FixedClock;

impl Clock for FixedClock {
    fn now_ns(&self) -> i64 {
        NOW_NS
    }
}

fn labels_for(metric: &str) -> LabelSet {
    LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: metric.to_string(),
    }])
    .expect("valid labels")
}

/// Publish one real segment plus its commit record for `tenant`.
async fn publish_segment(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    index: usize,
    metric: &str,
    samples: &[(i64, f64)],
) {
    let tenant_hash = tenant.hash();
    let label_set = labels_for(metric);
    let series = vec![SeriesInput {
        series_id: SeriesId::compute(tenant, metric, &label_set).expect("series id"),
        labels: label_set,
        samples: samples
            .iter()
            .map(|(ts_ns, value)| Sample {
                ts_ns: *ts_ns,
                value: *value,
            })
            .collect(),
    }];

    let writer_id = Uuid::from_u128(2_000 + index as u128);
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: index as u64 + 1,
    };
    let written = SegmentWriter::write(
        series,
        identity,
        IngestBounds {
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 0,
        },
    )
    .expect("write segment");

    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: index as u64 + 1,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: 1,
        created_unix_ns: 10 + index as i64,
        ingest_hour_bucket: 0,
    })
    .expect("valid commit record");

    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    store
        .put(&data_key, written.bytes, PutOptions::default())
        .await
        .expect("put data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
}

/// One log record on the single-`service.name` stream `service`. Mirrors the
/// fixture builder in `crates/ravel-sql/tests/logs_provider.rs`, but this test
/// drives it through the real HTTP endpoint rather than the provider directly.
fn log_record(service: &str, ts: i64, body: &str) -> LogRecord {
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str(service.to_string()),
    )];
    LogRecord {
        stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: body.into(),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: Vec::new(),
    }
}

/// Publish one real RLOG object plus its `Signal::Logs` commit record for
/// `tenant`, exactly as `ravel-ingest`'s log shard actor does: distinct-stream
/// count as `series_count`, record count as `sample_count`, event-time bounds
/// from the records, and a blake3 content hash over the object bytes. The
/// object lands at the reconstructed data key, so `Catalog::resolve` finds it.
async fn publish_log_segment(
    store: &dyn ObjectStoreBackend,
    tenant: &TenantId,
    index: usize,
    records: &[LogRecord],
) {
    let tenant_hash = tenant.hash();

    let mut min_event_ts_ns = i64::MAX;
    let mut max_event_ts_ns = i64::MIN;
    let mut streams = std::collections::HashSet::new();
    for rec in records {
        min_event_ts_ns = min_event_ts_ns.min(rec.ts_ns);
        max_event_ts_ns = max_event_ts_ns.max(rec.ts_ns);
        streams.insert(rec.stream_id);
    }

    let writer_id = Uuid::from_u128(9_000 + index as u128);
    let identity = ObjectIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.into_bytes(),
        writer_epoch: 1,
        writer_seq: index as u64 + 1,
    };
    let mut writer = RlogWriter::new(RlogConfig::default(), identity);
    for rec in records {
        writer.push(rec.clone()).expect("push log record");
    }
    let bytes = writer.finish().expect("finish rlog object");
    let content_hash: [u8; 32] = *blake3::hash(&bytes).as_bytes();

    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Logs,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: index as u64 + 1,
        object_size: bytes.len() as u64,
        content_hash,
        sample_count: records.len() as u64,
        series_count: streams.len() as u64,
        min_event_ts_ns,
        max_event_ts_ns,
        min_ingest_ts_ns: min_event_ts_ns,
        max_ingest_ts_ns: max_event_ts_ns,
        segment_format_version: 1,
        created_unix_ns: 10 + index as i64,
        ingest_hour_bucket: 0,
    })
    .expect("valid log commit record");

    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    store
        .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put log data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish log commit");
}

fn build_router(store: Arc<dyn ObjectStoreBackend>, tokens: HashMap<String, TenantId>) -> Router {
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let executor = SqlExecutor::new(
        catalog,
        SegmentFetcher::new(store.clone()),
        LogSegmentFetcher::new(store.clone()),
        SqlConfig::default(),
        1 << 30,
    );
    router(SqlState {
        executor: Arc::new(executor),
        tenant_resolver: Arc::new(StaticBearerTokenResolver::new(tokens)),
        store,
        clock: Arc::new(FixedClock),
        max_deadline: Duration::from_secs(30),
    })
}

fn tokens(pairs: &[(&str, &str)]) -> HashMap<String, TenantId> {
    pairs
        .iter()
        .map(|(token, tenant)| (token.to_string(), TenantId::new(tenant.to_string())))
        .collect()
}

/// A request body with an explicit window covering every fixture sample.
fn body(query: &str) -> String {
    serde_json::json!({
        "query": query,
        "start": 0.0,
        "end": NOW_NS as f64 / 1_000_000_000.0,
    })
    .to_string()
}

async fn post(
    app: &Router,
    token: Option<&str>,
    accept: Option<&str>,
    payload: String,
) -> (StatusCode, Vec<u8>) {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/api/v1/sql")
        .header(header::CONTENT_TYPE, "application/json");
    if let Some(token) = token {
        builder = builder.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    if let Some(accept) = accept {
        builder = builder.header(header::ACCEPT, accept);
    }
    let request = builder.body(Body::from(payload)).expect("build request");
    let response = app
        .clone()
        .oneshot(request)
        .await
        .expect("oneshot is infallible");
    let status = response.status();
    let bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("read body")
        .to_vec();
    (status, bytes)
}

async fn post_json(app: &Router, token: &str, query: &str) -> (StatusCode, Value) {
    let (status, bytes) = post(app, Some(token), None, body(query)).await;
    let value = serde_json::from_slice(&bytes).unwrap_or_else(|e| {
        panic!(
            "body is not JSON ({e}): {}",
            String::from_utf8_lossy(&bytes)
        )
    });
    (status, value)
}

async fn one_tenant_app(metric: &str, samples: &[(i64, f64)]) -> Router {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    publish_segment(store.as_ref(), &tenant, 0, metric, samples).await;
    build_router(store, tokens(&[("acme-token", "acme")]))
}

// ---------------------------------------------------------------------------
// Happy paths
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_select_returns_json_rows() {
    let app = one_tenant_app("m", &[(100, 1.0), (200, 2.5)]).await;
    let (status, value) = post_json(
        &app,
        "acme-token",
        "SELECT ts, value FROM samples ORDER BY ts",
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value["status"], "success");
    let rows = value["data"]["rows"].as_array().expect("rows");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0][0], serde_json::json!(100));
    assert_eq!(rows[0][1], serde_json::json!(1.0));
    assert_eq!(rows[1][1], serde_json::json!(2.5));
}

/// NaN and the infinities have no JSON literal, so they arrive as the
/// Prometheus spellings rather than as `null` or a parse error.
#[tokio::test]
async fn json_encodes_non_finite_values_as_prometheus_strings() {
    let app = one_tenant_app(
        "m",
        &[(1, f64::NAN), (2, f64::INFINITY), (3, f64::NEG_INFINITY)],
    )
    .await;
    let (status, value) =
        post_json(&app, "acme-token", "SELECT value FROM samples ORDER BY ts").await;

    assert_eq!(status, StatusCode::OK, "{value}");
    let rows = value["data"]["rows"].as_array().expect("rows");
    assert_eq!(rows[0][0], serde_json::json!("NaN"));
    assert_eq!(rows[1][0], serde_json::json!("+Inf"));
    assert_eq!(rows[2][0], serde_json::json!("-Inf"));
}

/// The Arrow IPC encoding is selected by `Accept` and is bit-exact: a NaN
/// payload survives the round trip that JSON necessarily loses.
#[tokio::test]
async fn accept_arrow_stream_returns_a_bit_exact_ipc_stream() {
    use arrow::array::{Array, Float64Array};
    use arrow::ipc::reader::StreamReader;

    let payload = f64::from_bits(0x7ff8_0000_0000_00ab);
    let app = one_tenant_app("m", &[(1, payload), (2, -0.0)]).await;

    let (status, bytes) = post(
        &app,
        Some("acme-token"),
        Some(ARROW_STREAM_MEDIA_TYPE),
        body("SELECT value FROM samples ORDER BY ts"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    let reader = StreamReader::try_new(bytes.as_slice(), None).expect("ipc reader");
    let mut values = Vec::new();
    for batch in reader {
        let batch = batch.expect("batch");
        let col = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("float column");
        for i in 0..col.len() {
            values.push(col.value(i).to_bits());
        }
    }
    assert_eq!(values, vec![payload.to_bits(), (-0.0f64).to_bits()]);
}

#[tokio::test]
async fn an_unauthenticated_request_is_rejected() {
    let app = one_tenant_app("m", &[(1, 1.0)]).await;
    let (status, bytes) = post(&app, None, None, body("SELECT ts FROM samples")).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let value: Value = serde_json::from_slice(&bytes).expect("json");
    assert_eq!(value["errorType"], "unauthorized");
}

// ---------------------------------------------------------------------------
// Security invariant 1, over HTTP
// ---------------------------------------------------------------------------

#[tokio::test]
async fn rejected_statement_kinds_return_400_over_http() {
    let app = one_tenant_app("m", &[(1, 1.0)]).await;

    for (name, sql) in [
        (
            "create external table",
            "CREATE EXTERNAL TABLE evil (a INT) STORED AS PARQUET LOCATION 's3://evil/x'",
        ),
        ("copy to", "COPY (SELECT * FROM samples) TO 's3://evil/out'"),
        ("insert", "INSERT INTO samples VALUES (1, 2.0)"),
        ("set", "SET datafusion.execution.batch_size = 1"),
        ("multi statement", "SELECT 1; SELECT 2"),
        ("explain", "EXPLAIN SELECT ts FROM samples"),
    ] {
        let (status, value) = post_json(&app, "acme-token", sql).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{name}: {value}");
        assert_eq!(value["status"], "error", "{name}");
        assert_eq!(value["errorType"], "bad_data", "{name}");
    }
}

/// `avg`/`mean` are admitted (ADR-0022 decisions 3, 4, issue #172): the
/// sequential-fold UDAF now answers over HTTP like any other aggregate.
/// Bit-exactness against the reference fold is gated in
/// crates/ravel-sql/tests/differential.rs; this only checks the endpoint
/// wires the result through instead of rejecting it.
#[tokio::test]
async fn avg_returns_a_json_result() {
    let app = one_tenant_app("m", &[(1, 1.0), (2, 2.0), (3, 3.0)]).await;
    let (status, value) = post_json(&app, "acme-token", "SELECT avg(value) FROM samples").await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value["status"], "success");
    let rows = value["data"]["rows"].as_array().expect("rows");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], serde_json::json!(2.0));
}

// ---------------------------------------------------------------------------
// Security invariant 2, over HTTP
// ---------------------------------------------------------------------------

#[tokio::test]
async fn tenants_cannot_read_each_others_rows_over_http() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let a = TenantId::new("tenant-a".to_string());
    let b = TenantId::new("tenant-b".to_string());
    publish_segment(store.as_ref(), &a, 0, "shared", &[(1, 1.0), (2, 2.0)]).await;
    publish_segment(
        store.as_ref(),
        &b,
        1,
        "shared",
        &[(1, 900.0), (2, 901.0), (3, 902.0)],
    )
    .await;

    let app = build_router(
        store,
        tokens(&[("a-token", "tenant-a"), ("b-token", "tenant-b")]),
    );
    let sql = "SELECT count(value), sum(value) FROM samples";

    let (status_a, value_a) = post_json(&app, "a-token", sql).await;
    assert_eq!(status_a, StatusCode::OK, "{value_a}");
    assert_eq!(value_a["data"]["rows"][0][0], serde_json::json!(2));
    assert_eq!(value_a["data"]["rows"][0][1], serde_json::json!(3.0));

    let (status_b, value_b) = post_json(&app, "b-token", sql).await;
    assert_eq!(status_b, StatusCode::OK, "{value_b}");
    assert_eq!(value_b["data"]["rows"][0][0], serde_json::json!(3));
    assert_eq!(value_b["data"]["rows"][0][1], serde_json::json!(2703.0));
}

// ---------------------------------------------------------------------------
// Error redaction (a7-F02, second boundary)
// ---------------------------------------------------------------------------

/// The assertion set the PromQL boundary's tests use, applied to the raw
/// response body rather than to a message string.
fn assert_body_redacted(body: &[u8], tenant: &TenantId) {
    let text = String::from_utf8_lossy(body);
    let tenant_hex = tenant.hash().to_hex();
    assert!(!text.contains(".rseg"), "leaked the segment suffix: {text}");
    assert!(
        !text.contains(&tenant_hex),
        "leaked the tenant hash {tenant_hex}: {text}"
    );
    assert!(!text.contains("t/"), "leaked the object key prefix: {text}");
    assert!(
        !text.contains(RAW_STORE_TEXT),
        "leaked raw backend text: {text}"
    );
}

/// A storage-layer fault: every segment read fails with a permanent store
/// error whose `Display` carries the object key and raw backend text.
#[tokio::test]
async fn a_storage_fault_body_carries_no_object_key_or_tenant_hash() {
    let plan = FaultPlan::empty().with_rule(
        Rule::new(
            Op::Get,
            ScriptedFault::Permanent(RAW_STORE_TEXT.to_string()),
        )
        .with_key_contains(".rseg"),
    );
    let store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let backend: Arc<dyn ObjectStoreBackend> = Arc::clone(&store) as Arc<dyn ObjectStoreBackend>;
    let tenant = TenantId::new("acme".to_string());
    publish_segment(backend.as_ref(), &tenant, 0, "m", &[(1, 1.0), (2, 2.0)]).await;

    let app = build_router(backend, tokens(&[("acme-token", "acme")]));
    let (status, bytes) = post(
        &app,
        Some("acme-token"),
        None,
        body("SELECT ts, value FROM samples"),
    )
    .await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_body_redacted(&bytes, &tenant);

    let value: Value = serde_json::from_slice(&bytes).expect("json");
    assert_eq!(value["errorType"], "unavailable");
    assert_eq!(value["error"], ravel_sql::MSG_UNAVAILABLE);
}

/// A vanished pinned segment: after the one allowed retry the endpoint
/// answers `SnapshotInvalidated`, redacted to the same transient class.
#[tokio::test]
async fn a_vanished_segment_body_carries_no_object_key() {
    let plan = FaultPlan::empty()
        .with_rule(Rule::new(Op::Get, ScriptedFault::NotFoundBlip).with_key_contains(".rseg"));
    let store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let backend: Arc<dyn ObjectStoreBackend> = Arc::clone(&store) as Arc<dyn ObjectStoreBackend>;
    let tenant = TenantId::new("acme".to_string());
    publish_segment(backend.as_ref(), &tenant, 0, "m", &[(1, 1.0)]).await;

    let app = build_router(backend, tokens(&[("acme-token", "acme")]));
    let (status, bytes) = post(
        &app,
        Some("acme-token"),
        None,
        body("SELECT ts, value FROM samples"),
    )
    .await;

    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_body_redacted(&bytes, &tenant);
}

/// A DataFusion planning error. Its `Display` names the unknown column and
/// lists the table's valid fields; none of that may reach the body.
#[tokio::test]
async fn a_datafusion_plan_error_body_carries_no_schema_detail() {
    let app = one_tenant_app("m", &[(1, 1.0)]).await;
    let tenant = TenantId::new("acme".to_string());

    let (status, bytes) = post(
        &app,
        Some("acme-token"),
        None,
        body("SELECT no_such_column FROM samples"),
    )
    .await;

    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_body_redacted(&bytes, &tenant);

    let text = String::from_utf8_lossy(&bytes);
    assert!(
        !text.contains("no_such_column"),
        "leaked the resolved plan fragment: {text}"
    );
    assert!(
        !text.contains("series_id"),
        "leaked the table schema: {text}"
    );

    let value: Value = serde_json::from_slice(&bytes).expect("json");
    assert_eq!(value["errorType"], "execution");
    assert_eq!(value["error"], ravel_sql::MSG_PLAN);
}

/// A DataFusion type error is planning-class too, and equally redacted.
#[tokio::test]
async fn a_datafusion_type_error_body_is_redacted() {
    let app = one_tenant_app("m", &[(1, 1.0)]).await;
    let tenant = TenantId::new("acme".to_string());

    let (status, bytes) = post(
        &app,
        Some("acme-token"),
        None,
        body("SELECT value FROM samples WHERE labels > 3"),
    )
    .await;

    assert!(
        status.is_client_error() || status == StatusCode::UNPROCESSABLE_ENTITY,
        "unexpected status {status}"
    );
    assert_body_redacted(&bytes, &tenant);
}

/// Validation errors are the one class that keeps its own text, because it
/// is derived only from the caller's own input. It still must not carry
/// server state.
#[tokio::test]
async fn validation_error_bodies_quote_only_the_callers_input() {
    let app = one_tenant_app("m", &[(1, 1.0)]).await;
    let tenant = TenantId::new("acme".to_string());

    let (status, bytes) = post(
        &app,
        Some("acme-token"),
        None,
        body("INSERT INTO samples VALUES (1, 2.0)"),
    )
    .await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_body_redacted(&bytes, &tenant);

    let value: Value = serde_json::from_slice(&bytes).expect("json");
    let message = value["error"].as_str().expect("message");
    assert!(
        message.contains("INSERT"),
        "a rejection must say what was rejected: {message}"
    );
}

// ---------------------------------------------------------------------------
// Request handling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn a_malformed_body_is_a_400_not_a_500() {
    let app = one_tenant_app("m", &[(1, 1.0)]).await;
    let (status, bytes) = post(&app, Some("acme-token"), None, "{ not json".to_string()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let value: Value = serde_json::from_slice(&bytes).expect("json");
    assert_eq!(value["errorType"], "bad_data");
}

#[tokio::test]
async fn min_commit_token_is_accepted_and_read_your_write_resolves() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    publish_segment(store.as_ref(), &tenant, 0, "m", &[(1, 1.0)]).await;
    let app = build_router(store, tokens(&[("acme-token", "acme")]));

    // A syntactically valid but unknown token must fail as unsatisfiable,
    // not be silently ignored: read-your-write is a durability contract.
    let payload = serde_json::json!({
        "query": "SELECT ts FROM samples",
        "start": 0.0,
        "end": NOW_NS as f64 / 1_000_000_000.0,
        "min_commit_token": ["not-a-real-token"],
    })
    .to_string();
    let (status, bytes) = post(&app, Some("acme-token"), None, payload).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let value: Value = serde_json::from_slice(&bytes).expect("json");
    assert!(
        value["error"]
            .as_str()
            .expect("message")
            .contains("min_commit_token"),
        "{value}"
    );
}

// ---------------------------------------------------------------------------
// The `logs` table over HTTP (ADR-0033, issue #240)
// ---------------------------------------------------------------------------

/// The epic's acceptance test for the endpoint wiring: a real `POST
/// /api/v1/sql` against `FROM logs` returns the expected rows. Unlike the
/// provider-level test in `crates/ravel-sql/tests/logs_provider.rs`, this
/// resolves a `Signal::Logs` snapshot through the real `Catalog` and drives the
/// query through the HTTP layer, proving the endpoint routes to the `logs`
/// table purely from the query's `FROM` clause (ADR-0033 decision D: no
/// protocol change, no second endpoint).
#[tokio::test]
async fn sql_query_against_logs_table_returns_rows() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());

    // Two streams, several records, one carrying the word "timeout".
    let records = vec![
        log_record("api", 100, "hello world"),
        log_record("api", 150, "connection timeout"),
        log_record("worker", 200, "shutdown ok"),
    ];
    publish_log_segment(store.as_ref(), &tenant, 0, &records).await;
    let app = build_router(store, tokens(&[("acme-token", "acme")]));

    // A plain ts-range scan returns every record in range, body included. The
    // `ts` column is `Timestamp(ns)`, so the bounds are TIMESTAMP literals (a
    // bare integer does not coerce), 100 ns and 200 ns past the epoch.
    let (status, value) = post_json(
        &app,
        "acme-token",
        "SELECT ts, body FROM logs \
         WHERE ts >= TIMESTAMP '1970-01-01 00:00:00.000000100' \
           AND ts <= TIMESTAMP '1970-01-01 00:00:00.000000200' \
         ORDER BY ts",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value["status"], "success");
    let rows = value["data"]["rows"].as_array().expect("rows");
    assert_eq!(rows.len(), 3, "{value}");
    assert_eq!(rows[0][0], serde_json::json!(100));
    assert_eq!(rows[0][1], serde_json::json!("hello world"));
    assert_eq!(rows[1][1], serde_json::json!("connection timeout"));
    assert_eq!(rows[2][1], serde_json::json!("shutdown ok"));

    // A `has_word` content predicate pushes down and returns only the match,
    // proving the `has_word` UDF is registered in the logs session and the
    // bloom-accelerated scan runs end to end over HTTP.
    let (status, value) = post_json(
        &app,
        "acme-token",
        "SELECT ts FROM logs WHERE has_word(body, 'timeout') ORDER BY ts",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let rows = value["data"]["rows"].as_array().expect("rows");
    assert_eq!(
        rows.len(),
        1,
        "only the 'connection timeout' record: {value}"
    );
    assert_eq!(rows[0][0], serde_json::json!(150));
}

/// A planning failure on a `logs` query returns the shared, redacted planning
/// message -- and that message must not tell the client the problem is about
/// the `samples` table (it named only `samples` before that fix).
///
/// The vehicle is an unregistered function, not the `attrs['k']` subscript
/// this change makes plannable. `attrs['k']` was the vehicle while it was
/// the documented logs planning gap; it is now the feature under test in
/// `a_logs_attrs_subscript_query_succeeds_over_http` below, and a test that
/// asserts a query fails is worthless once the query is meant to succeed.
/// What this test actually protects -- a `logs` planning failure not blaming
/// `samples` -- is independent of which construct failed, so it keeps its
/// value with any unplannable query.
#[tokio::test]
async fn a_logs_plan_error_does_not_blame_the_samples_table() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    publish_log_segment(
        store.as_ref(),
        &tenant,
        0,
        &[log_record("api", 100, "hello world")],
    )
    .await;
    let app = build_router(store, tokens(&[("acme-token", "acme")]));

    let (status, value) = post_json(
        &app,
        "acme-token",
        "SELECT no_such_function(body) FROM logs",
    )
    .await;

    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{value}");
    assert_eq!(value["errorType"], "execution");
    let error = value["error"].as_str().expect("error string");
    assert_eq!(error, ravel_sql::MSG_PLAN);
    // The message must be accurate for a `logs` query: it names `logs`, and it
    // must not tell the client the fix is about the `samples` table.
    assert!(
        error.contains("logs"),
        "planning message must be accurate for a logs query: {error}"
    );
    assert!(
        !error.contains("samples table"),
        "planning message must not blame the samples table: {error}"
    );
}

/// `attrs['k']` plans and answers over HTTP, which is the whole point of
/// registering the `ExprPlanner`: the crate-level tests in
/// `ravel_sql::logs_provider` prove the planner works against a session they
/// build themselves, and this proves the planner is actually reachable from
/// the endpoint a user posts SQL to.
///
/// That distinction has bitten this codebase repeatedly: a feature can be
/// complete, tested, and registered nowhere. Asserting the returned value
/// (not merely a 200) is what makes this a end-to-end proof.
#[tokio::test]
async fn a_logs_attrs_subscript_query_succeeds_over_http() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    publish_log_segment(
        store.as_ref(),
        &tenant,
        0,
        &[log_record("api", 100, "hello world")],
    )
    .await;
    let app = build_router(store, tokens(&[("acme-token", "acme")]));

    let (status, value) = post_json(
        &app,
        "acme-token",
        "SELECT ts FROM logs WHERE attrs['service.name'] = 'api'",
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{value}");
    let rows = value["data"]["rows"].as_array().expect("rows");
    assert_eq!(
        rows.len(),
        1,
        "the one record whose service.name is api: {value}"
    );
    assert_eq!(rows[0][0], serde_json::json!(100));

    // A key that no record carries returns zero rows, not an error: an absent
    // attribute is a normal query result, not a planning failure.
    let (status, value) = post_json(
        &app,
        "acme-token",
        "SELECT ts FROM logs WHERE attrs['no.such.key'] = 'x'",
    )
    .await;

    assert_eq!(status, StatusCode::OK, "{value}");
    assert!(
        value["data"]["rows"].as_array().expect("rows").is_empty(),
        "a missing attribute key yields no rows: {value}"
    );
}

/// A `samples`-only query must not trigger a `Signal::Logs` catalog resolve
/// (ADR-0033: resolve the logs snapshot only when the query references `logs`).
/// Proven by counting the LIST calls against the logs commit keyspace
/// (`.../l/c/...`) through a spying `ObjectStoreBackend`: a metrics query adds
/// zero such LISTs, while a logs query adds several.
///
/// `FaultStore` counts only faults it *injects*, not passthrough calls, so it
/// cannot answer "did this request list the logs keyspace at all"; a small
/// counting proxy is the honest tool here (the same conclusion the counting
/// store in `crates/ravel-sql/tests/util` reached).
#[tokio::test]
async fn a_samples_only_query_does_not_resolve_the_logs_signal() {
    let inner: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let spy = LogsCommitListSpy::new(inner);
    let backend: Arc<dyn ObjectStoreBackend> = spy.clone();
    let tenant = TenantId::new("acme".to_string());

    // Both signals have data, so either query can succeed on its own merits.
    publish_segment(backend.as_ref(), &tenant, 0, "m", &[(100, 1.0), (200, 2.5)]).await;
    publish_log_segment(
        backend.as_ref(),
        &tenant,
        0,
        &[
            log_record("api", 100, "hello"),
            log_record("api", 200, "world"),
        ],
    )
    .await;

    let app = build_router(Arc::clone(&backend), tokens(&[("acme-token", "acme")]));

    // Ignore whatever publishing did; measure only what the queries cause.
    spy.reset();
    let before = spy.logs_commit_lists();
    assert_eq!(before, 0, "counter reset before the queries");

    // A metrics-only query resolves Signal::Metrics and must never list the
    // logs commit keyspace.
    let (status, value) = post_json(&app, "acme-token", "SELECT count(value) FROM samples").await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let after_samples = spy.logs_commit_lists();
    assert_eq!(
        after_samples, before,
        "a samples-only query must trigger no Signal::Logs LISTs \
         (before={before}, after={after_samples})"
    );

    // A logs query, by contrast, does list the logs commit keyspace: this is
    // the "after" that makes the zero above meaningful rather than vacuous.
    let (status, value) = post_json(&app, "acme-token", "SELECT ts FROM logs").await;
    assert_eq!(status, StatusCode::OK, "{value}");
    let after_logs = spy.logs_commit_lists();
    assert!(
        after_logs > after_samples,
        "a logs query must resolve Signal::Logs and list its commit keyspace \
         (after_samples={after_samples}, after_logs={after_logs})"
    );
}

/// A pass-through `ObjectStoreBackend` that counts LIST calls whose prefix
/// addresses the logs commit keyspace (`t/<hash>/l/c/...`, the unit
/// `Catalog::resolve(_, Signal::Logs, _)` lists). Every other operation is a
/// plain delegate. This observes real calls, which `FaultStore`'s
/// injected-fault counters cannot.
struct LogsCommitListSpy {
    inner: Arc<dyn ObjectStoreBackend>,
    logs_commit_lists: AtomicU64,
}

impl LogsCommitListSpy {
    fn new(inner: Arc<dyn ObjectStoreBackend>) -> Arc<Self> {
        Arc::new(LogsCommitListSpy {
            inner,
            logs_commit_lists: AtomicU64::new(0),
        })
    }

    fn logs_commit_lists(&self) -> u64 {
        self.logs_commit_lists.load(Ordering::Acquire)
    }

    fn reset(&self) {
        self.logs_commit_lists.store(0, Ordering::Release);
    }

    /// The `l` signal segment in the commit-key layout
    /// (`t/<hash>/l/c/<shard>/<hour>/`, ravel-commit `keys`). Metrics is `/m/c/`.
    fn note(&self, prefix: &str) {
        if prefix.contains("/l/c/") {
            self.logs_commit_lists.fetch_add(1, Ordering::AcqRel);
        }
    }
}

#[async_trait::async_trait]
impl ObjectStoreBackend for LogsCommitListSpy {
    async fn put(
        &self,
        key: &str,
        data: bytes::Bytes,
        opts: PutOptions,
    ) -> Result<ravel_object_store::PutOutcome, ravel_object_store::StoreError> {
        self.inner.put(key, data, opts).await
    }

    async fn get(
        &self,
        key: &str,
        range: ravel_object_store::GetRange,
    ) -> Result<ravel_object_store::GetOutcome, ravel_object_store::StoreError> {
        self.inner.get(key, range).await
    }

    async fn head(
        &self,
        key: &str,
    ) -> Result<ravel_object_store::ObjectMeta, ravel_object_store::StoreError> {
        self.inner.head(key).await
    }

    async fn list(
        &self,
        prefix: &str,
        page: Option<ravel_object_store::PageToken>,
    ) -> Result<ravel_object_store::ListPage, ravel_object_store::StoreError> {
        self.note(prefix);
        self.inner.list(prefix, page).await
    }

    async fn list_delimited(
        &self,
        prefix: &str,
    ) -> Result<ravel_object_store::DelimitedList, ravel_object_store::StoreError> {
        self.note(prefix);
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), ravel_object_store::StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> ravel_object_store::Capabilities {
        // multipart: false to match the refusing default `put_multipart` this
        // double inherits (issue #298).
        ravel_object_store::Capabilities {
            multipart: false,
            ..self.inner.capabilities()
        }
    }
}

// ---------------------------------------------------------------------------
// Query-audit records (ADR-0042 decision 4, issue #391)
// ---------------------------------------------------------------------------

/// Read back every query-audit record (`kind=query`) the server has written to
/// the tenant's `Signal::Audit` shard, so a test can assert the record the
/// handler produced for a request.
async fn query_audit_records(store: &dyn ObjectStoreBackend, tenant: &TenantId) -> Vec<LogRecord> {
    let tenant_hash = tenant.hash();
    let prefix = keys::commit_shard_prefix(&tenant_hash, Signal::Audit, QUERY_AUDIT_SHARD)
        .expect("audit commit prefix");
    let metas = list_all(store, &prefix).await.expect("list audit commits");
    let cfg = RlogConfig::default();
    let mut out = Vec::new();
    for meta in metas {
        let got = store
            .get(&meta.key, GetRange::Full)
            .await
            .expect("get commit");
        let commit = record::decode(&got.data).expect("decode commit");
        let data_key = keys::reconstruct_data_key(&commit).expect("data key");
        let object = store
            .get(&data_key, GetRange::Full)
            .await
            .expect("get data");
        let reader = RlogReader::new(object.data.as_ref(), &cfg).expect("rlog reader");
        let (rows, _stats) = reader.scan(&Predicate::And(Vec::new())).expect("scan");
        for row in rows {
            if attr(&row, "kind") == Some("query") {
                out.push(row);
            }
        }
    }
    out
}

/// The value of a string `attrs` entry, or `None` if absent or non-string.
fn attr<'a>(row: &'a LogRecord, key: &str) -> Option<&'a str> {
    row.attrs
        .iter()
        .find(|(k, _)| k == key)
        .and_then(|(_, v)| match v {
            AttrValue::Str(s) => Some(s.as_str()),
            _ => None,
        })
}

/// (a) A successful SQL query produces exactly one `Signal::Audit` record with
/// `kind=query` and `query.status=ok`, attributed to the resolved tenant and
/// carrying the verbatim query text.
#[tokio::test]
async fn a_successful_query_writes_one_ok_audit_record() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    publish_segment(store.as_ref(), &tenant, 0, "m", &[(100, 1.0), (200, 2.5)]).await;
    let app = build_router(Arc::clone(&store), tokens(&[("acme-token", "acme")]));

    let sql = "SELECT ts, value FROM samples ORDER BY ts";
    let (status, value) = post_json(&app, "acme-token", sql).await;
    assert_eq!(status, StatusCode::OK, "{value}");

    let records = query_audit_records(store.as_ref(), &tenant).await;
    assert_eq!(records.len(), 1, "exactly one query-audit record");
    let row = &records[0];
    assert_eq!(attr(row, "kind"), Some("query"));
    assert_eq!(attr(row, "query.language"), Some("sql"));
    assert_eq!(attr(row, "query.status"), Some("ok"));
    assert_eq!(
        attr(row, "query.tenant"),
        Some(tenant.hash().to_hex().as_str())
    );
    // `body()` sends an explicit `start: 0.0, end: NOW_NS`, so the resolved
    // window is exactly that, not the absent-window default.
    assert_eq!(attr(row, "query.window_start_ns"), Some("0"));
    assert_eq!(
        attr(row, "query.window_end_ns"),
        Some(NOW_NS.to_string().as_str())
    );
    assert_eq!(attr(row, "query.text"), Some(sql));
    assert_eq!(row.severity_text, "INFO");
    assert_eq!(row.ts_ns, NOW_NS);
}

/// (b) A query that fails still produces exactly one audit record, with
/// `query.status=error` and `ERROR` severity. The audit trail records the
/// attempt, not only successful queries.
#[tokio::test]
async fn a_failed_query_writes_one_error_audit_record() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    publish_segment(store.as_ref(), &tenant, 0, "m", &[(1, 1.0)]).await;
    let app = build_router(Arc::clone(&store), tokens(&[("acme-token", "acme")]));

    // An unknown column reaches the executor and fails to plan.
    let sql = "SELECT no_such_column FROM samples";
    let (status, value) = post_json(&app, "acme-token", sql).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{value}");

    let records = query_audit_records(store.as_ref(), &tenant).await;
    assert_eq!(
        records.len(),
        1,
        "a failed query is still audited exactly once"
    );
    let row = &records[0];
    assert_eq!(attr(row, "query.status"), Some("error"));
    assert_eq!(attr(row, "query.text"), Some(sql));
    assert_eq!(row.severity_text, "ERROR");
}

/// A request rejected before it reaches the executor for a resolved tenant is
/// not audited: there is no executed query to attribute. A malformed body is
/// rejected at parse time, so it writes no audit record.
#[tokio::test]
async fn a_request_rejected_before_execution_is_not_audited() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let tenant = TenantId::new("acme".to_string());
    publish_segment(store.as_ref(), &tenant, 0, "m", &[(1, 1.0)]).await;
    let app = build_router(Arc::clone(&store), tokens(&[("acme-token", "acme")]));

    let (status, _bytes) = post(&app, Some("acme-token"), None, "{ not json".to_string()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    let records = query_audit_records(store.as_ref(), &tenant).await;
    assert!(
        records.is_empty(),
        "a body rejected before execution writes no audit record"
    );
}

/// (c) An audit-write failure must not fail the client response. Every PUT to
/// the `Signal::Audit` keyspace is faulted, so the audit record cannot be
/// written; the query still succeeds and the client still gets its rows.
#[tokio::test]
async fn an_audit_write_failure_does_not_fail_the_query_response() {
    let plan = FaultPlan::empty().with_rule(
        Rule::new(
            Op::Put,
            ScriptedFault::Permanent("audit store down".to_string()),
        )
        .with_key_contains("/u/"),
    );
    let store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let backend: Arc<dyn ObjectStoreBackend> = Arc::clone(&store) as Arc<dyn ObjectStoreBackend>;
    let tenant = TenantId::new("acme".to_string());
    // Metric segments live under "/m/", so setup is unaffected by the audit fault.
    publish_segment(backend.as_ref(), &tenant, 0, "m", &[(100, 1.0), (200, 2.5)]).await;
    let app = build_router(Arc::clone(&backend), tokens(&[("acme-token", "acme")]));

    let (status, value) = post_json(
        &app,
        "acme-token",
        "SELECT ts, value FROM samples ORDER BY ts",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{value}");
    assert_eq!(value["status"], "success");

    // The audit PUT was faulted, so no record persisted: the failure was
    // swallowed (and logged), never turned into a 5xx.
    let records = query_audit_records(backend.as_ref(), &tenant).await;
    assert!(
        records.is_empty(),
        "the audit PUT was faulted, so nothing persisted, yet the query still succeeded"
    );
    // The fault really fired, proving the assertion above is not vacuous.
    assert!(
        store.fault_count(Op::Put, ravel_object_store::fault::FaultKind::Permanent) > 0,
        "the audit PUT fault must have fired"
    );
}
