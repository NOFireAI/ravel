//! Phase 3 gateway wiring (issue #12): drive one encoded OTAP metrics batch
//! through the `otap`-gated `ArrowMetricsService` in-process and assert the
//! strict ack carries a commit token, then that the stored, queryable data
//! matches what the equivalent OTLP request produces on the same server.
//!
//! Compiled only under the `otap` feature (the whole gRPC service is), so this
//! file is empty in the default build.
#![cfg(feature = "otap")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use prost::Message;
use ravel_ingest::{AdmissionLimits, CountLimit, RateLimit};
use ravel_object_store::memory::MemoryStore;
use ravel_otap::encode::{DataPointRow, MetricKind, MetricRow, MetricsStreamEncoder};
use ravel_otap::proto::experimental::arrow::v1::arrow_metrics_service_client::ArrowMetricsServiceClient;
use ravel_otap::proto::experimental::arrow::v1::{BatchArrowRecords, BatchStatus, StatusCode};
use ravel_server::{FoldTaskConfig, LimitsConfig, Mode, ServerConfig};
use ravel_types::TenantId;

const TOKEN: &str = "testtoken";
const VALUE: f64 = 42.5;

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
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
        otap: true,
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

/// Like [`start_test_server`] but with `acme`'s admission limits overridden to
/// `tenant_limits`; every other layer keeps this service's shipped defaults.
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
        otap: true,
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

/// One encoded OTAP batch: a single gauge `metric_name` with one point at
/// `ts_ns` valued [`VALUE`] and no attributes. This is the columnar
/// equivalent of [`otlp_gauge_request`] below.
fn otap_gauge_batch(
    metric_name: &str,
    ts_ns: i64,
) -> ravel_otap::proto::experimental::arrow::v1::BatchArrowRecords {
    let metrics = vec![MetricRow {
        name: metric_name.to_string(),
        kind: MetricKind::Gauge,
        data_points: vec![DataPointRow {
            time_unix_nano: ts_ns,
            value: VALUE,
            flags: 0,
            exemplars: vec![],
            attrs: vec![],
        }],
    }];
    let mut encoder = MetricsStreamEncoder::new("otap-grpc-test").expect("new encoder");
    encoder.encode_batch(0, &metrics).expect("encode batch")
}

/// The OTLP equivalent: one gauge, one point, no resource (so no synthesized
/// `job`/`instance`), matching the OTAP encoder's known scope (it emits no
/// RESOURCE_ATTRS). The two paths must therefore produce byte-identical
/// series identity and value.
fn otlp_gauge_request(metric_name: &str, ts_ns: i64) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: metric_name.to_string(),
                    data: Some(MetricData::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: ts_ns as u64,
                            value: Some(NumberValue::AsDouble(VALUE)),
                            ..Default::default()
                        }],
                    })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

/// Encode a sequence of batches through one stateful [`MetricsStreamEncoder`],
/// so batch N>0 references the schema/dictionaries batch 0 established -- i.e.
/// the same IPC-stream statefulness the server-side decoder tracks. Each spec
/// is the list of distinct gauge names (one point each) for that batch.
fn otap_stream_batches(specs: &[&[&str]], ts_ns: i64) -> Vec<BatchArrowRecords> {
    let mut encoder = MetricsStreamEncoder::new("otap-grpc-test").expect("new encoder");
    specs
        .iter()
        .enumerate()
        .map(|(i, names)| {
            let metrics: Vec<MetricRow> = names
                .iter()
                .map(|name| MetricRow {
                    name: (*name).to_string(),
                    kind: MetricKind::Gauge,
                    data_points: vec![DataPointRow {
                        time_unix_nano: ts_ns,
                        value: VALUE,
                        flags: 0,
                        exemplars: vec![],
                        attrs: vec![],
                    }],
                })
                .collect();
            encoder
                .encode_batch(i as i64, &metrics)
                .expect("encode batch")
        })
        .collect()
}

/// Encode a single-series gauge batch of `count` points onto `encoder` as
/// batch `batch_id`. Timestamps and values are distinct per point so the batch
/// does not compress away, letting the wire size scale with `count` -- used to
/// build a batch that exceeds a byte-rate burst on size alone. One metric name
/// keeps the name dictionary from overflowing its key type.
fn encode_point_batch(
    encoder: &mut MetricsStreamEncoder,
    batch_id: i64,
    name: &str,
    count: usize,
    ts_ns: i64,
) -> BatchArrowRecords {
    let data_points: Vec<DataPointRow> = (0..count)
        .map(|i| DataPointRow {
            time_unix_nano: ts_ns + i as i64,
            value: VALUE + i as f64,
            flags: 0,
            exemplars: vec![],
            attrs: vec![],
        })
        .collect();
    encoder
        .encode_batch(
            batch_id,
            &[MetricRow {
                name: name.to_string(),
                kind: MetricKind::Gauge,
                data_points,
            }],
        )
        .expect("encode batch")
}

/// Opens one `ArrowMetrics` stream, sends every batch in `batches`, and
/// collects every `BatchStatus` the server replies with until the response
/// stream ends. A shorter reply than `batches` means the server tore the
/// stream down before consuming the rest.
async fn open_stream_send(
    grpc_addr: std::net::SocketAddr,
    batches: Vec<BatchArrowRecords>,
) -> Vec<BatchStatus> {
    let mut client = ArrowMetricsServiceClient::connect(format!("http://{grpc_addr}"))
        .await
        .expect("connect to gRPC listener");
    let mut request = tonic::Request::new(futures::stream::iter(batches));
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {TOKEN}").parse().expect("valid metadata"),
    );
    let mut inbound = client
        .arrow_metrics(request)
        .await
        .expect("arrow_metrics stream opens")
        .into_inner();
    let mut statuses = Vec::new();
    while let Some(status) = inbound.message().await.expect("read batch status") {
        statuses.push(status);
    }
    statuses
}

/// Sends one OTAP batch over the bidirectional `ArrowMetrics` stream and
/// returns the single `BatchStatus` the server replies with. Default write
/// mode is Strict (no `x-ravel-ingest-mode` header), so the ack blocks until
/// the batch is durable and carries commit tokens.
async fn send_one_batch(
    grpc_addr: std::net::SocketAddr,
    batch: ravel_otap::proto::experimental::arrow::v1::BatchArrowRecords,
) -> ravel_otap::proto::experimental::arrow::v1::BatchStatus {
    let mut client = ArrowMetricsServiceClient::connect(format!("http://{grpc_addr}"))
        .await
        .expect("connect to gRPC listener");
    let mut request = tonic::Request::new(futures::stream::iter(vec![batch]));
    request.metadata_mut().insert(
        "authorization",
        format!("Bearer {TOKEN}").parse().expect("valid metadata"),
    );
    let mut inbound = client
        .arrow_metrics(request)
        .await
        .expect("arrow_metrics stream opens")
        .into_inner();
    inbound
        .message()
        .await
        .expect("read batch status")
        .expect("exactly one batch status")
}

/// Instant query for `metric_name`, returning the single result series as
/// JSON. `min_commit_token` pins read-your-write visibility.
async fn query_one_series(
    http_addr: std::net::SocketAddr,
    metric_name: &str,
    min_commit_token: &str,
) -> serde_json::Value {
    let client = reqwest::Client::new();
    let response = client
        .get(format!("http://{http_addr}/api/v1/query"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .query(&[
            ("query", metric_name),
            ("min_commit_token", min_commit_token),
        ])
        .send()
        .await
        .expect("query request succeeds");
    assert_eq!(response.status(), 200, "query should succeed");
    let body: serde_json::Value = response.json().await.expect("query response is JSON");
    assert_eq!(body["status"], "success", "query body: {body}");
    let result = body["data"]["result"]
        .as_array()
        .expect("result is an array")
        .clone();
    assert_eq!(result.len(), 1, "expected exactly one series: {body}");
    result.into_iter().next().unwrap()
}

fn series_value(series: &serde_json::Value) -> f64 {
    series["value"][1]
        .as_str()
        .expect("value is a string")
        .parse()
        .expect("value parses as f64")
}

/// A single OTAP batch acks OK with a non-empty commit token, and the sample
/// it carried is queryable at that token with the right series and value.
#[tokio::test]
async fn otap_batch_acks_with_commit_token_and_round_trips() {
    let running = start_test_server().await;
    let grpc_addr = running.grpc_addr.expect("gateway binds gRPC");

    let ts = now_ns();
    let status = send_one_batch(grpc_addr, otap_gauge_batch("otap_gauge", ts)).await;

    assert_eq!(status.batch_id, 0, "ack names the batch it acks");
    assert_eq!(
        status.status_code,
        StatusCode::Ok as i32,
        "strict OTAP write should ack OK, got message: {}",
        status.status_message
    );
    assert!(
        !status.status_message.is_empty(),
        "strict ack must carry a commit token"
    );

    let series = query_one_series(running.http_addr, "otap_gauge", &status.status_message).await;
    assert_eq!(series["metric"]["__name__"], "otap_gauge");
    assert!(
        (series_value(&series) - VALUE).abs() < f64::EPSILON,
        "unexpected value: {series}"
    );

    running.shutdown().await.expect("graceful shutdown");
}

/// The stored, queryable result of an OTAP batch is identical to the OTLP
/// request's for the same logical metric: same series identity (the whole
/// `metric` label map) and same value. This is the phase-2 differential gate
/// (docs/otap-ingest.md) observed end to end through the gateway rather than
/// at the normalizer.
#[tokio::test]
async fn otap_and_otlp_produce_identical_stored_series() {
    let running = start_test_server().await;
    let grpc_addr = running.grpc_addr.expect("gateway binds gRPC");
    let http_base = format!("http://{}", running.http_addr);
    let ts = now_ns();

    // OTAP path over gRPC.
    let otap_status = send_one_batch(grpc_addr, otap_gauge_batch("gauge_otap", ts)).await;
    assert_eq!(otap_status.status_code, StatusCode::Ok as i32);

    // OTLP path over HTTP, equivalent input.
    let http = reqwest::Client::new();
    let otlp_response = http
        .post(format!("{http_base}/v1/metrics"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .body(otlp_gauge_request("gauge_otlp", ts).encode_to_vec())
        .send()
        .await
        .expect("OTLP export succeeds");
    assert_eq!(otlp_response.status(), 200);
    let otlp_token = otlp_response
        .headers()
        .get("x-ravel-commit-token")
        .expect("OTLP commit token header")
        .to_str()
        .expect("token header is ascii")
        .to_string();

    let otap_series =
        query_one_series(running.http_addr, "gauge_otap", &otap_status.status_message).await;
    let otlp_series = query_one_series(running.http_addr, "gauge_otlp", &otlp_token).await;

    // Values match.
    assert!((series_value(&otap_series) - series_value(&otlp_series)).abs() < f64::EPSILON);

    // Series identity matches: both `metric` maps carry only `__name__` (their
    // own metric name) and no other labels. Comparing the maps with the name
    // removed proves the OTAP path admitted the same label set OTLP did.
    let strip_name = |mut m: serde_json::Value| {
        let obj = m.as_object_mut().expect("metric is an object");
        obj.remove("__name__");
        m
    };
    assert_eq!(
        strip_name(otap_series["metric"].clone()),
        strip_name(otlp_series["metric"].clone()),
        "OTAP and OTLP series carry different labels"
    );

    running.shutdown().await.expect("graceful shutdown");
}

/// Finding 1 (checkpoint review of #524): a byte-rate rejection mid-stream
/// tears the gRPC stream down instead of keeping it alive. Keeping it alive
/// would leave the stateful per-`DecoderKey` Arrow IPC decoder desynced,
/// because the rejected (pre-decode) batch's Schema/DictionaryBatch messages
/// never reached it. Proven two ways: the stream ends right after the
/// rejection (a third queued batch never gets a status), and a fresh
/// reconnect -- which gets a fresh `StreamState` -- ingests cleanly.
#[tokio::test]
async fn byte_rate_rejection_tears_down_stream_and_reconnect_is_clean() {
    let ts = now_ns();

    // One stateful encoder for the three in-stream batches: a small first
    // batch, a big second batch, and a third that must never be processed.
    // The big batch's wire size must exceed the burst so its rejection is a
    // function of size alone (a request larger than the burst can never hold
    // enough tokens, regardless of refill) -- no dependence on wall-clock
    // refill timing. It rides the same encoder as the small first batch, so it
    // carries no schema/dictionaries of its own; its point count alone must
    // carry it past the burst.
    let mut encoder = MetricsStreamEncoder::new("otap-grpc-test").expect("new encoder");
    let small0 = encode_point_batch(&mut encoder, 0, "byte_rate_ok", 1, ts);
    let big1 = encode_point_batch(&mut encoder, 1, "byte_rate_big", 20_000, ts);
    let after2 = encode_point_batch(&mut encoder, 2, "byte_rate_after", 1, ts);
    let small_len = small0.encoded_len() as u64;
    let big_len = big1.encoded_len() as u64;
    let batches = vec![small0, big1, after2];

    // Burst admits two small batches (the first in-stream batch and, later, the
    // reconnect batch) with headroom to spare, but is smaller than the big
    // batch. per_sec is minimal so the outcome does not lean on refill.
    let burst = small_len * 3;
    assert!(
        big_len > burst,
        "big batch ({big_len}) must exceed the burst ({burst}) so its rejection is size-deterministic"
    );

    let running = start_test_server_with_limits(AdmissionLimits {
        ingest_byte_rate: RateLimit::Bounded { per_sec: 1, burst },
        ..AdmissionLimits::default()
    })
    .await;
    let grpc_addr = running.grpc_addr.expect("gateway binds gRPC");

    let statuses = open_stream_send(grpc_addr, batches).await;
    assert_eq!(
        statuses.len(),
        2,
        "byte-rate rejection must tear the stream down: the third batch must never get a status, got {statuses:?}"
    );
    assert_eq!(
        statuses[0].status_code,
        StatusCode::Ok as i32,
        "first (in-budget) batch acks OK: {}",
        statuses[0].status_message
    );
    assert_eq!(
        statuses[1].status_code,
        StatusCode::ResourceExhausted as i32,
        "over-rate batch is RESOURCE_EXHAUSTED, then ends the stream"
    );

    // Reconnect on a fresh stream: it gets a fresh `StreamState` decoder and
    // ingests cleanly. The burst headroom (3x a small batch, only one consumed
    // so far) covers this without waiting on refill.
    let reconnect = open_stream_send(
        grpc_addr,
        otap_stream_batches(&[&["byte_rate_reconnect"]], ts),
    )
    .await;
    assert_eq!(
        reconnect.len(),
        1,
        "reconnect delivers one status: {reconnect:?}"
    );
    assert_eq!(
        reconnect[0].status_code,
        StatusCode::Ok as i32,
        "a fresh reconnect must start clean: {}",
        reconnect[0].status_message
    );

    running.shutdown().await.expect("graceful shutdown");
}

/// Finding 2 (checkpoint review of #524): when the active-series cap drops
/// some of a batch's points, the ack stays OK (ADR-0051 layer 4 is "OK +
/// partial success") but must signal the drop, not read as a clean ack. The
/// `status_message` leads with a `partial-success:` line carrying the exact
/// dropped-point count.
#[tokio::test]
async fn active_series_cap_partial_success_reports_drop_count() {
    const CAP: usize = 2;
    const FLOOD: usize = 5;

    let running = start_test_server_with_limits(AdmissionLimits {
        max_active_series: CountLimit::Bounded(CAP as u64),
        ..AdmissionLimits::default()
    })
    .await;
    let grpc_addr = running.grpc_addr.expect("gateway binds gRPC");

    let ts = now_ns();
    let names: Vec<String> = (0..FLOOD).map(|i| format!("otap_cap_{i}")).collect();
    let refs: Vec<&str> = names.iter().map(String::as_str).collect();
    let batches = otap_stream_batches(&[&refs], ts);
    let batch = batches.into_iter().next().expect("one batch");

    let status = send_one_batch(grpc_addr, batch).await;

    assert_eq!(
        status.status_code,
        StatusCode::Ok as i32,
        "a cap breach is a partial success, still OK: {}",
        status.status_message
    );
    let dropped = FLOOD - CAP;
    assert!(
        status.status_message.contains("partial-success"),
        "ack must signal the drop rather than read as a fully-accepted batch: {:?}",
        status.status_message
    );
    assert!(
        status
            .status_message
            .contains(&format!("{dropped} data points rejected")),
        "ack must report the dropped-point count ({dropped}): {:?}",
        status.status_message
    );

    running.shutdown().await.expect("graceful shutdown");
}
