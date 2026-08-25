//! End-to-end coverage for ADR-0085 through the real HTTP server: OTLP and
//! Remote-Write v1 ingest capture per-metric type/help/unit, the OTLP name gets
//! its Prometheus-style unit and counter suffixes, and `/api/v1/metadata` serves
//! the captured record back over the same running binary. This is the epic's
//! closing loop: nothing here is a crate-only capability, every assertion is
//! made against a real `MemoryStore`-backed `ravel_server::start` process.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::{
    AggregationTemporality, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use ravel_object_store::memory::MemoryStore;
use ravel_remote_write::proto::prometheus::metric_metadata::MetricType;
use ravel_remote_write::proto::prometheus::{
    Label as ProtoLabelV1, MetricMetadata as ProtoMetricMetadataV1, Sample as ProtoSampleV1,
    TimeSeries as ProtoTimeSeriesV1, WriteRequest as ProtoWriteRequestV1,
};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::TenantId;

const TOKEN: &str = "testtoken";

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_millis() as i64
}

fn string_kv(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(AnyValueVariant::StringValue(value.to_string())),
        }),
        ..Default::default()
    }
}

/// A monotonic cumulative `Sum` metric named `metric_name`, with `unit` and
/// `description` set so the metadata capture and the OTLP suffix pass both have
/// something to read. A monotonic Sum with `unit: "By"` named `foo` ingests as
/// the family `foo_bytes_total` (ADR-0085 decision 2).
fn export_counter_request(
    metric_name: &str,
    job: &str,
    unit: &str,
    description: &str,
    value: f64,
    ts_ns: i64,
) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", job)],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: metric_name.to_string(),
                    description: description.to_string(),
                    unit: unit.to_string(),
                    data: Some(MetricData::Sum(Sum {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: ts_ns as u64,
                            value: Some(NumberValue::AsDouble(value)),
                            ..Default::default()
                        }],
                        aggregation_temporality: AggregationTemporality::Cumulative as i32,
                        is_monotonic: true,
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn compress(bytes: &[u8]) -> Vec<u8> {
    snap::raw::Encoder::new()
        .compress_vec(bytes)
        .expect("compress")
}

/// An RW1 body for one series named `metric`, carrying one
/// `prometheus.MetricMetadata` entry describing that family (ADR-0085 decision
/// 1: RW1's family key is `metric_family_name` as parsed).
fn rw1_body_with_metadata(
    metric: &str,
    job: &str,
    value: f64,
    ts_ms: i64,
    meta_type: MetricType,
    help: &str,
    unit: &str,
) -> Vec<u8> {
    let req = ProtoWriteRequestV1 {
        timeseries: vec![ProtoTimeSeriesV1 {
            labels: vec![
                ProtoLabelV1 {
                    name: "__name__".to_string(),
                    value: metric.to_string(),
                },
                ProtoLabelV1 {
                    name: "job".to_string(),
                    value: job.to_string(),
                },
            ],
            samples: vec![ProtoSampleV1 {
                value,
                timestamp: ts_ms,
            }],
            exemplars: vec![],
            histograms: vec![],
        }],
        metadata: vec![ProtoMetricMetadataV1 {
            r#type: meta_type as i32,
            metric_family_name: metric.to_string(),
            help: help.to_string(),
            unit: unit.to_string(),
        }],
    };
    compress(&req.encode_to_vec())
}

async fn start_test_server() -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new("acme"));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let store = Arc::new(MemoryStore::new());
    let config = ServerConfig {
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        adaptive_flush_delay: false,
        max_flush_delay: std::time::Duration::from_secs(2),
        max_flush_delay_idle: std::time::Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
        mode: Mode::All,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: 1,
        tenant_resolver,
        mtls_listener: None,
        fold_tenants: Vec::new(),
        fold: FoldTaskConfig {
            enabled: false,
            ..FoldTaskConfig::default()
        },
        maintain: ravel_server::MaintenanceTaskConfig::default(),
        alerting: ravel_server::AlertEvalConfig::default(),
        oidc_refresh: None,
        otap: false,
        metrics_tenant_labels: false,
        limits: ravel_server::LimitsConfig::default(),
        deployment_key: None,
        gc: ravel_maintain::GcConfigValues::maintain_defaults(),
        query_deadline: ravel_query::EngineConfig::default().deadline,
        store_probe_interval: ravel_server::store_probe::DEFAULT_STORE_PROBE_INTERVAL,
        admission_reconcile_interval: ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL,
        query_concurrency_limit: ravel_query::QueryConcurrencyLimit::Unlimited,
        max_s3_requests: ravel_query::EngineConfig::default().max_s3_requests,
        scrub_period: std::time::Duration::from_secs(7 * 86_400),
        indexed_fields: Default::default(),
        typed_attr_columns: Default::default(),
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        cache_dir: None,
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: std::time::Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
    };
    ravel_server::start(
        config,
        store.clone(),
        store.clone(),
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts")
}

/// Assert the query for `metric` (constrained to `commit_token`'s visibility)
/// returns exactly one series with that `__name__`.
async fn assert_query_one(base: &str, client: &reqwest::Client, metric: &str, commit_token: &str) {
    let query_response = client
        .get(format!("{base}/api/v1/query"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .query(&[("query", metric), ("min_commit_token", commit_token)])
        .send()
        .await
        .expect("query request succeeds");

    assert_eq!(query_response.status(), 200, "query should succeed");
    let body: serde_json::Value = query_response.json().await.expect("query response is JSON");
    assert_eq!(body["status"], "success");
    let result = body["data"]["result"]
        .as_array()
        .expect("result is an array");
    assert_eq!(result.len(), 1, "expected exactly one series: {body}");
    assert_eq!(result[0]["metric"]["__name__"], metric);
}

/// The acceptance test: a unit-suffixed OTLP counter and an RW1 series with
/// metadata are ingested over the real HTTP surfaces, their metadata is flushed
/// durably, and `/api/v1/metadata` serves both back with the OTLP name suffixed;
/// `/api/v1/query` resolves the suffixed name, proving it is what the query path
/// actually stores. The anonymous probe still gets a `200` with an empty object.
///
/// prove-the-test: dropping the `flush_once` call below (so the durable record is
/// never written before the metadata GET) makes the `data["foo_bytes_total"]`
/// assertion fail with the response's `data` observed as `{}` (an empty object
/// where the suffixed entry was expected). Independently, omitting
/// `.with_metadata_cache` from `build_app_state` makes the same GET serve the
/// pre-ADR empty object even after the flush, failing identically.
#[tokio::test]
async fn otlp_counter_is_suffixed_and_metadata_visible_over_http() {
    let running = start_test_server().await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    // 1. OTLP: a monotonic Sum named `foo`, unit `By`, description `bytes sent`.
    //    It ingests as the family `foo_bytes_total` (unit suffix `_bytes`, then
    //    `_total` for the monotonic counter).
    let otlp = export_counter_request("foo", "demo", "By", "bytes sent", 42.0, now_ns());
    let otlp_response = client
        .post(format!("{base}/v1/metrics"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .body(otlp.encode_to_vec())
        .send()
        .await
        .expect("OTLP export succeeds");
    assert_eq!(otlp_response.status(), 200, "OTLP export should succeed");
    let otlp_commit_token = otlp_response
        .headers()
        .get("x-ravel-commit-token")
        .expect("commit token header present")
        .to_str()
        .expect("commit token header is ascii")
        .to_string();

    // 2. RW1: a series named `bar` carrying a Gauge metadata entry.
    let rw1 = rw1_body_with_metadata(
        "bar",
        "demo",
        7.0,
        now_ms(),
        MetricType::Gauge,
        "a gauge",
        "seconds",
    );
    let rw1_response = client
        .post(format!("{base}/api/v1/write"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header(
            "content-type",
            "application/x-protobuf;proto=prometheus.WriteRequest",
        )
        .body(rw1)
        .send()
        .await
        .expect("RW1 write succeeds");
    assert_eq!(rw1_response.status(), 204, "RW1 write should succeed");

    // 3. Flush the metadata sink deterministically (no real 30 s window wait):
    //    this writes the durable `t/<tenant_hash>/m/meta` record both surfaces'
    //    `observe` calls populated. The time-injection testing pattern applied
    //    end to end, exactly as it is for a unit test.
    running
        .metadata_sink
        .as_ref()
        .expect("metadata sink present in Mode::All")
        .flush_once(now_ns())
        .await;

    // 4. GET /api/v1/metadata with the bearer: both families are visible, with
    //    the OTLP name suffixed and its unit mapped to the Prometheus word.
    let meta_response = client
        .get(format!("{base}/api/v1/metadata"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .send()
        .await
        .expect("metadata request succeeds");
    assert_eq!(meta_response.status(), 200, "metadata GET should succeed");
    let meta: serde_json::Value = meta_response.json().await.expect("metadata JSON");
    assert_eq!(meta["status"], "success");
    let data = meta["data"].as_object().expect("data is an object");

    let foo = &data["foo_bytes_total"];
    assert!(
        foo.is_array(),
        "the OTLP counter is visible under its suffixed family name: {meta}"
    );
    assert_eq!(foo[0]["type"], "counter", "monotonic Sum -> counter");
    assert_eq!(foo[0]["help"], "bytes sent", "OTLP description is the help");
    assert_eq!(
        foo[0]["unit"], "bytes",
        "By -> bytes (mapped, not raw UCUM)"
    );

    let bar = &data["bar"];
    assert!(bar.is_array(), "the RW1 series is visible: {meta}");
    assert_eq!(bar[0]["type"], "gauge");
    assert_eq!(bar[0]["help"], "a gauge");
    assert_eq!(bar[0]["unit"], "seconds");

    // 5. The OTLP-suffixed name is what the query path resolves, not just what
    //    the metadata record claims.
    assert_query_one(&base, &client, "foo_bytes_total", &otlp_commit_token).await;

    // 6. The anonymous probe never becomes a 401: no bearer -> 200 with an empty
    //    object, through the real router (ADR-0085 read path contract).
    let anon = client
        .get(format!("{base}/api/v1/metadata"))
        .send()
        .await
        .expect("anonymous metadata request completes");
    assert_eq!(anon.status(), 200, "metadata is never a 401");
    let anon_body: serde_json::Value = anon.json().await.expect("anonymous metadata JSON");
    assert_eq!(
        anon_body["data"],
        serde_json::json!({}),
        "an unresolved tenant gets the empty object"
    );

    running.shutdown().await.expect("graceful shutdown");
}

/// The read side degrades gracefully with no metadata yet: a tenant that has
/// ingested nothing gets a `200` with an empty `data` object, not an error.
#[tokio::test]
async fn metadata_for_tenant_with_no_ingest_is_empty_not_an_error() {
    let running = start_test_server().await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let response = client
        .get(format!("{base}/api/v1/metadata"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .send()
        .await
        .expect("metadata request succeeds");
    assert_eq!(response.status(), 200, "an empty tenant is still a 200");
    let body: serde_json::Value = response.json().await.expect("metadata JSON");
    assert_eq!(body["status"], "success");
    assert_eq!(
        body["data"],
        serde_json::json!({}),
        "no metadata ingested yet -> empty data object, no error"
    );

    running.shutdown().await.expect("graceful shutdown");
}
