//! End-to-end coverage for `POST /api/v1/write`: real RW1 and RW2 wire
//! payloads (snappy + protobuf) against an in-process server backed by
//! `MemoryStore`, followed by a query for the ingested sample
//! (read-your-write via the returned commit token).

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use prost::Message;
use ravel_ingest::{AdmissionLimits, CountLimit, RateLimit};
use ravel_object_store::memory::MemoryStore;
use ravel_remote_write::proto::prometheus::{
    Label as ProtoLabelV1, Sample as ProtoSampleV1, TimeSeries as ProtoTimeSeriesV1,
    WriteRequest as ProtoWriteRequestV1,
};
use ravel_remote_write::proto::write_v2::histogram::{Count, ZeroCount};
use ravel_remote_write::proto::write_v2::{
    BucketSpan, Histogram as ProtoHistogramV2, Request as ProtoRequestV2, Sample as ProtoSampleV2,
    TimeSeries as ProtoTimeSeriesV2,
};
use ravel_server::{FoldTaskConfig, LimitsConfig, Mode, ServerConfig};
use ravel_types::TenantId;

const TOKEN: &str = "testtoken";

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_millis() as i64
}

fn compress(bytes: &[u8]) -> Vec<u8> {
    snap::raw::Encoder::new()
        .compress_vec(bytes)
        .expect("compress")
}

fn rw1_body(metric: &str, job: &str, value: f64, ts_ms: i64) -> Vec<u8> {
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
        metadata: vec![],
    };
    compress(&req.encode_to_vec())
}

/// `symbols[0]` is conventionally the empty string per the RW2 spec.
fn rw2_body(metric: &str, job: &str, value: f64, ts_ms: i64) -> Vec<u8> {
    let symbols = vec![
        String::new(),
        "__name__".to_string(),
        metric.to_string(),
        "job".to_string(),
        job.to_string(),
    ];
    let req = ProtoRequestV2 {
        symbols,
        timeseries: vec![ProtoTimeSeriesV2 {
            labels_refs: vec![1, 2, 3, 4],
            samples: vec![ProtoSampleV2 {
                value,
                timestamp: ts_ms,
                start_timestamp: 0,
            }],
            histograms: vec![],
            exemplars: vec![],
            metadata: None,
        }],
    };
    compress(&req.encode_to_vec())
}

/// An RW2 body carrying one well-formed integer native histogram (no scalar
/// samples): one positive span of three buckets whose deltas accumulate to
/// 2, 5, 6, plus a zero bucket, all covered by `count`.
fn rw2_histogram_body(metric: &str, job: &str, ts_ms: i64) -> Vec<u8> {
    let symbols = vec![
        String::new(),
        "__name__".to_string(),
        metric.to_string(),
        "job".to_string(),
        job.to_string(),
    ];
    let req = ProtoRequestV2 {
        symbols,
        timeseries: vec![ProtoTimeSeriesV2 {
            labels_refs: vec![1, 2, 3, 4],
            samples: vec![],
            histograms: vec![ProtoHistogramV2 {
                count: Some(Count::CountInt(14)),
                zero_count: Some(ZeroCount::ZeroCountInt(1)),
                sum: 42.5,
                schema: 2,
                zero_threshold: 1e-9,
                positive_spans: vec![BucketSpan {
                    offset: 0,
                    length: 3,
                }],
                positive_deltas: vec![2, 3, 1],
                negative_spans: vec![],
                negative_deltas: vec![],
                positive_counts: vec![],
                negative_counts: vec![],
                reset_hint: 0,
                timestamp: ts_ms,
                start_timestamp: 0,
                custom_values: vec![],
            }],
            exemplars: vec![],
            metadata: None,
        }],
    };
    compress(&req.encode_to_vec())
}

/// An RW2 body carrying `metrics.len()` distinct series (distinct
/// `__name__`, same job), one scalar sample each. Symbol table layout:
/// `[0]=""`, `[1]="__name__"`, `[2]="job"`, `[3]=job value`, then one symbol
/// per metric name starting at `[4]`.
fn rw2_multi_series_body(metrics: &[&str], job: &str, value: f64, ts_ms: i64) -> Vec<u8> {
    let mut symbols = vec![
        String::new(),
        "__name__".to_string(),
        "job".to_string(),
        job.to_string(),
    ];
    let timeseries = metrics
        .iter()
        .map(|metric| {
            let name_ref = symbols.len() as u32;
            symbols.push(metric.to_string());
            ProtoTimeSeriesV2 {
                labels_refs: vec![1, name_ref, 2, 3],
                samples: vec![ProtoSampleV2 {
                    value,
                    timestamp: ts_ms,
                    start_timestamp: 0,
                }],
                histograms: vec![],
                exemplars: vec![],
                metadata: None,
            }
        })
        .collect();
    let req = ProtoRequestV2 {
        symbols,
        timeseries,
    };
    compress(&req.encode_to_vec())
}

async fn start_test_server_with_limits(tenant_limits: AdmissionLimits) -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new("acme"));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let store = Arc::new(MemoryStore::new());
    let mut tenants = HashMap::new();
    tenants.insert(TenantId::new("acme"), tenant_limits);
    let config = ServerConfig {
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
        limits: LimitsConfig {
            defaults: ravel_server::config::limits::shipped_defaults(),
            tenants,
            ..LimitsConfig::default()
        },
        deployment_key: None,
        gc: ravel_maintain::GcConfigValues::maintain_defaults(),
        query_deadline: ravel_query::EngineConfig::default().deadline,
        store_probe_interval: ravel_server::store_probe::DEFAULT_STORE_PROBE_INTERVAL,
        admission_reconcile_interval: ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL,
        query_concurrency_limit: ravel_query::QueryConcurrencyLimit::Unlimited,
        scrub_period: std::time::Duration::from_secs(7 * 86_400),
        indexed_fields: Default::default(),
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
    };
    ravel_server::start(
        config,
        store,
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts")
}

async fn start_test_server() -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new("acme"));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let store = Arc::new(MemoryStore::new());
    let config = ServerConfig {
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
        scrub_period: std::time::Duration::from_secs(7 * 86_400),
        indexed_fields: Default::default(),
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
    };
    ravel_server::start(
        config,
        store,
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts")
}

async fn query_one(base: &str, client: &reqwest::Client, metric: &str, commit_token: &str) {
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

#[tokio::test]
async fn rw1_write_then_query_round_trips_sample() {
    let running = start_test_server().await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let body = rw1_body("rw1_metric", "demo", 42.5, now_ms());
    let response = client
        .post(format!("{base}/api/v1/write"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header(
            "content-type",
            "application/x-protobuf;proto=prometheus.WriteRequest",
        )
        .body(body)
        .send()
        .await
        .expect("write request succeeds");

    assert_eq!(response.status(), 204, "RW1 write should succeed");
    // RW1 responses carry no written-count stats headers (RW2-only, ADR-0015).
    assert!(
        response
            .headers()
            .get("x-prometheus-remote-write-samples-written")
            .is_none()
    );
    let commit_token = response
        .headers()
        .get("x-ravel-commit-token")
        .expect("commit token header present")
        .to_str()
        .expect("commit token header is ascii")
        .to_string();

    query_one(&base, &client, "rw1_metric", &commit_token).await;

    running.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn rw2_write_then_query_round_trips_sample_with_stats_headers() {
    let running = start_test_server().await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let body = rw2_body("rw2_metric", "demo", 7.0, now_ms());
    let response = client
        .post(format!("{base}/api/v1/write"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header(
            "content-type",
            "application/x-protobuf;proto=io.prometheus.write.v2.Request",
        )
        .body(body)
        .send()
        .await
        .expect("write request succeeds");

    assert_eq!(response.status(), 204, "RW2 write should succeed");
    let commit_token = response
        .headers()
        .get("x-ravel-commit-token")
        .expect("commit token header present")
        .to_str()
        .expect("commit token header is ascii")
        .to_string();
    assert_eq!(
        response
            .headers()
            .get("x-prometheus-remote-write-samples-written")
            .expect("samples-written header present")
            .to_str()
            .expect("ascii"),
        "1"
    );
    assert_eq!(
        response
            .headers()
            .get("x-prometheus-remote-write-histograms-written")
            .expect("histograms-written header present")
            .to_str()
            .expect("ascii"),
        "0"
    );
    assert_eq!(
        response
            .headers()
            .get("x-prometheus-remote-write-exemplars-written")
            .expect("exemplars-written header present")
            .to_str()
            .expect("ascii"),
        "0"
    );

    query_one(&base, &client, "rw2_metric", &commit_token).await;

    running.shutdown().await.expect("graceful shutdown");
}

/// A native histogram admitted over RW2 is reported as one written
/// histogram and zero written samples, the split fixed for phase C8: before
/// it, the histograms-written header was a hardcoded zero and an admitted
/// histogram would have inflated samples-written.
#[tokio::test]
async fn rw2_native_histogram_reports_histograms_written_not_samples() {
    let running = start_test_server().await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let body = rw2_histogram_body("rw2_hist", "demo", now_ms());
    let response = client
        .post(format!("{base}/api/v1/write"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header(
            "content-type",
            "application/x-protobuf;proto=io.prometheus.write.v2.Request",
        )
        .body(body)
        .send()
        .await
        .expect("write request succeeds");

    assert_eq!(response.status(), 204, "RW2 histogram write should succeed");
    let header = |name: &str| {
        response
            .headers()
            .get(name)
            .unwrap_or_else(|| panic!("{name} header present"))
            .to_str()
            .expect("ascii")
            .to_string()
    };
    assert_eq!(
        header("x-prometheus-remote-write-histograms-written"),
        "1",
        "one native histogram was admitted and written"
    );
    assert_eq!(
        header("x-prometheus-remote-write-samples-written"),
        "0",
        "a native histogram is not counted as a written scalar sample"
    );

    running.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn version_header_negotiates_when_content_type_is_generic() {
    let running = start_test_server().await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let body = rw2_body("rw2_header_negotiated", "demo", 3.0, now_ms());
    let response = client
        .post(format!("{base}/api/v1/write"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .header("x-prometheus-remote-write-version", "2.0.0")
        .body(body)
        .send()
        .await
        .expect("write request succeeds");

    assert_eq!(
        response.status(),
        204,
        "header-negotiated RW2 write should succeed"
    );
    assert!(
        response
            .headers()
            .get("x-prometheus-remote-write-samples-written")
            .is_some(),
        "header negotiation should still select RW2"
    );

    running.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn unknown_content_type_yields_415() {
    let running = start_test_server().await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let body = rw1_body("unused_metric", "demo", 1.0, now_ms());
    let response = client
        .post(format!("{base}/api/v1/write"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await
        .expect("write request completes");

    assert_eq!(
        response.status(),
        415,
        "unknown content type must be rejected"
    );

    running.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn malformed_body_yields_400() {
    let running = start_test_server().await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let response = client
        .post(format!("{base}/api/v1/write"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header(
            "content-type",
            "application/x-protobuf;proto=prometheus.WriteRequest",
        )
        .body(vec![0xff, 0xff, 0xff])
        .send()
        .await
        .expect("write request completes");

    assert_eq!(
        response.status(),
        400,
        "corrupt snappy body must be rejected as malformed"
    );

    running.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn missing_credentials_yield_401() {
    let running = start_test_server().await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let body = rw1_body("unused_metric", "demo", 1.0, now_ms());
    let response = client
        .post(format!("{base}/api/v1/write"))
        .header(
            "content-type",
            "application/x-protobuf;proto=prometheus.WriteRequest",
        )
        .body(body)
        .send()
        .await
        .expect("write request completes");

    assert_eq!(
        response.status(),
        401,
        "missing bearer token should be rejected"
    );

    running.shutdown().await.expect("graceful shutdown");
}

#[tokio::test]
async fn buffered_mode_header_is_refused_and_write_is_still_strict() {
    let running = start_test_server().await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let body = rw1_body("strict_only_metric", "demo", 5.0, now_ms());
    let response = client
        .post(format!("{base}/api/v1/write"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header(
            "content-type",
            "application/x-protobuf;proto=prometheus.WriteRequest",
        )
        .header("x-ravel-ingest-mode", "buffered")
        .body(body)
        .send()
        .await
        .expect("write request succeeds");

    // The RW surface never reads x-ravel-ingest-mode: a commit token must
    // still come back, proving the write went through strict mode and was
    // durable by the time the response was sent (ADR-0015).
    assert_eq!(response.status(), 204);
    assert!(
        response.headers().get("x-ravel-commit-token").is_some(),
        "strict-mode commit token must be present even with the buffered-mode header set"
    );

    running.shutdown().await.expect("graceful shutdown");
}

/// Layer 2 (ADR-0051 section 1): a tenant whose ingest byte-rate bucket
/// starts (and stays) empty gets 429 with `Retry-After`, not the malformed-
/// body 400 or the auth-failure 401 this file covers elsewhere.
#[tokio::test]
async fn byte_rate_exceeded_yields_429_with_retry_after() {
    let running = start_test_server_with_limits(AdmissionLimits {
        ingest_byte_rate: RateLimit::Bounded {
            per_sec: 1,
            burst: 1,
        },
        ..AdmissionLimits::default()
    })
    .await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let body = rw1_body("rate_limited_metric", "demo", 1.0, now_ms());
    let response = client
        .post(format!("{base}/api/v1/write"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header(
            "content-type",
            "application/x-protobuf;proto=prometheus.WriteRequest",
        )
        .body(body)
        .send()
        .await
        .expect("write request completes");

    assert_eq!(
        response.status(),
        429,
        "a byte-rate breach must reject the whole request"
    );
    assert!(
        response.headers().get("retry-after").is_some(),
        "429 must carry Retry-After"
    );

    running.shutdown().await.expect("graceful shutdown");
}

/// Layer 4 (ADR-0051 section 1): an active-series-cap breach on Remote
/// Write is pinned to 2xx with the true written count, never a 429 — 429 is
/// reserved for the rate-limit rows only. One series is already active from
/// a first write; the second distinct series in a follow-up request finds
/// the cap full and is silently dropped from the written count, with no
/// partial-success message (Remote Write has none).
#[tokio::test]
async fn series_cap_breach_is_still_2xx_with_reduced_written_count() {
    let running = start_test_server_with_limits(AdmissionLimits {
        max_active_series: CountLimit::Bounded(1),
        ..AdmissionLimits::default()
    })
    .await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let body = rw2_multi_series_body(&["series_cap_a", "series_cap_b"], "demo", 1.0, now_ms());
    let response = client
        .post(format!("{base}/api/v1/write"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header(
            "content-type",
            "application/x-protobuf;proto=io.prometheus.write.v2.Request",
        )
        .body(body)
        .send()
        .await
        .expect("write request succeeds");

    assert_eq!(
        response.status(),
        204,
        "an active-series-cap breach must never be reported as a 4xx"
    );
    assert_eq!(
        response
            .headers()
            .get("x-prometheus-remote-write-samples-written")
            .expect("samples-written header present")
            .to_str()
            .expect("ascii"),
        "1",
        "only the series the cap admitted should be counted as written"
    );

    running.shutdown().await.expect("graceful shutdown");
}
