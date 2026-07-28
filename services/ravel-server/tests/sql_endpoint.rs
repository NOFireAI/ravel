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
use std::time::Duration;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_ingest::Clock;
use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Rule, ScriptedFault};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::SegmentFetcher;
use ravel_query::http::StaticBearerTokenResolver;
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_server::sql::{ARROW_STREAM_MEDIA_TYPE, SqlState, router};
use ravel_sql::{SqlConfig, SqlExecutor};
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

fn build_router(store: Arc<dyn ObjectStoreBackend>, tokens: HashMap<String, TenantId>) -> Router {
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let executor = SqlExecutor::new(
        catalog,
        SegmentFetcher::new(store),
        SqlConfig::default(),
        1 << 30,
    );
    router(SqlState {
        executor: Arc::new(executor),
        tenant_resolver: Arc::new(StaticBearerTokenResolver::new(tokens)),
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

#[tokio::test]
async fn avg_is_rejected_with_a_message_naming_the_workaround() {
    let app = one_tenant_app("m", &[(1, 1.0)]).await;
    let (status, value) = post_json(&app, "acme-token", "SELECT avg(value) FROM samples").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let message = value["error"].as_str().expect("message");
    assert!(message.contains("sum"), "{message}");
    assert!(message.contains("count"), "{message}");
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
