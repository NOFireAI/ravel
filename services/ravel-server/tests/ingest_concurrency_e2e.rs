//! End-to-end coverage for the process-wide in-flight ingest-request ceiling:
//! a real HTTP or gRPC export blocked mid-flush, over
//! `--max-inflight-ingest-requests`, is shed immediately rather than queued.
//!
//! Every test here holds the shard's data-object PUT open with a
//! `FaultStore` hold gate (ADR-0059 decision 5) so an admitted request stays
//! genuinely in-flight -- awaiting its flush's ack -- for the whole window in
//! which the test proves the next request is shed.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use ravel_object_store::fault::{FaultStore, GateHandle, Occurrence, Op};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, list_all};
use ravel_server::ingest_concurrency::IngestConcurrencyLimit;
use ravel_server::{FoldTaskConfig, LimitsConfig, Mode, ServerConfig};
use ravel_types::TenantId;

const TOKEN: &str = "testtoken";
const TENANT: &str = "acme";

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
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

fn metrics_export_request(metric_name: &str, ts_ns: i64) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", "concurrency-test")],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: metric_name.to_string(),
                    data: Some(MetricData::Gauge(Gauge {
                        data_points: vec![NumberDataPoint {
                            time_unix_nano: ts_ns as u64,
                            value: Some(NumberValue::AsDouble(1.0)),
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

/// Starts a full server against `store`, with the process-wide in-flight
/// ingest ceiling set to `limit`. One shard, so every metrics write from
/// every test here lands on the same shard actor and the same flush.
async fn start_test_server(
    store: Arc<dyn ObjectStoreBackend>,
    limit: IngestConcurrencyLimit,
) -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let config = ServerConfig {
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
        limits: LimitsConfig::default(),
        deployment_key: None,
        gc: ravel_maintain::GcConfigValues::maintain_defaults(),
        query_deadline: ravel_query::EngineConfig::default().deadline,
        store_probe_interval: ravel_server::store_probe::DEFAULT_STORE_PROBE_INTERVAL,
        admission_reconcile_interval: ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL,
        query_concurrency_limit: ravel_query::QueryConcurrencyLimit::Unlimited,
        max_s3_requests: ravel_query::EngineConfig::default().max_s3_requests,
        scrub_period: Duration::from_secs(7 * 86_400),
        indexed_fields: Default::default(),
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: std::time::Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
        ingest_concurrency_limit: limit,
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

/// Releases every currently held call on `gate`, repeatedly, until every
/// spawned task in `tasks` has completed. A held flush's data-object PUT is
/// only armed once the flush actually fires (the shard's own
/// `max_flush_delay`), so a single release pass can race a later flush; this
/// keeps draining until nothing is left in flight.
async fn drain_until_done<T>(gate: &GateHandle, tasks: &[tokio::task::JoinHandle<T>]) {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            for id in gate.held() {
                gate.release(id);
            }
            if tasks.iter().all(|t| t.is_finished()) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("held requests drain within 10s");
}

async fn durable_metric_object_count(store: &dyn ObjectStoreBackend) -> usize {
    let prefix = format!("t/{}/m/l0/", TenantId::new(TENANT).hash().to_hex());
    list_all(store, &prefix)
        .await
        .expect("list metrics data objects")
        .len()
}

/// (a) HTTP: with the ceiling at 2, two concurrent exports are admitted and
/// held mid-flush; a third is shed with 429 and `Retry-After`.
/// (b) the shed request buffers nothing and returns no commit token.
/// (d) the shed counter increments, observable on `/metrics`.
#[tokio::test]
async fn http_third_concurrent_request_over_limit_is_shed() {
    let fault = Arc::new(FaultStore::new(MemoryStore::new(), Default::default()));
    let store: Arc<dyn ObjectStoreBackend> = fault.clone();
    let running = start_test_server(store.clone(), IngestConcurrencyLimit::Bounded(2)).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let data_key_prefix = format!("t/{}/m/l0/", TenantId::new(TENANT).hash().to_hex());
    let gate = fault.hold(Op::Put, Some(data_key_prefix), Occurrence::Always);

    let ts_ns = now_ns();
    let held_tasks: Vec<_> = ["held_a", "held_b"]
        .iter()
        .map(|name| {
            let client = client.clone();
            let base = base.clone();
            let request = metrics_export_request(name, ts_ns);
            tokio::spawn(async move {
                client
                    .post(format!("{base}/v1/metrics"))
                    .header("authorization", format!("Bearer {TOKEN}"))
                    .header("content-type", "application/x-protobuf")
                    .body(request.encode_to_vec())
                    .send()
                    .await
                    .expect("held export completes once released")
            })
        })
        .collect();

    // Give the two admitted requests time to pass admission, enqueue on the
    // shard, and (once the shard's flush timer fires) reach the held PUT --
    // all far faster than it takes for either task to finish, since finishing
    // requires this test to release the gate first.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        held_tasks.iter().all(|t| !t.is_finished()),
        "both admitted requests must still be in flight, blocked on the held flush"
    );

    let third_request = metrics_export_request("shed", ts_ns);
    let shed_response = client
        .post(format!("{base}/v1/metrics"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .body(third_request.encode_to_vec())
        .send()
        .await
        .expect("shed request still gets an HTTP response, not a connection error");

    assert_eq!(
        shed_response.status(),
        429,
        "a request over the in-flight ceiling must be shed with 429"
    );
    assert_eq!(
        shed_response
            .headers()
            .get("retry-after")
            .expect("a shed response carries Retry-After")
            .to_str()
            .expect("ascii header value"),
        "1",
    );
    assert!(
        shed_response
            .headers()
            .get("x-ravel-commit-token")
            .is_none(),
        "a shed request must never carry a commit token: it never buffered anything"
    );

    let metrics_body = client
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("metrics scrape completes")
        .text()
        .await
        .expect("metrics body is text");
    assert!(
        metrics_body.contains("ravel_ingest_concurrency_shed_total{mode=\"all\"} 1"),
        "shed counter must read exactly 1 after exactly one shed request:\n{metrics_body}"
    );

    drain_until_done(&gate, &held_tasks).await;
    for task in held_tasks {
        let response = task.await.expect("task joins");
        assert_eq!(
            response.status(),
            200,
            "an admitted request must succeed once released"
        );
    }

    assert_eq!(
        durable_metric_object_count(store.as_ref()).await,
        1,
        "only the two admitted requests' single combined flush is durable; the shed request wrote nothing"
    );

    running.shutdown().await.expect("graceful shutdown");
}

/// (a) gRPC: the same ceiling sheds a third concurrent gRPC export as
/// `RESOURCE_EXHAUSTED`, proving the limit is process-wide across transports,
/// not per-transport.
#[tokio::test]
async fn grpc_third_concurrent_request_over_limit_is_shed() {
    use opentelemetry_proto::tonic::collector::metrics::v1::metrics_service_client::MetricsServiceClient;

    let fault = Arc::new(FaultStore::new(MemoryStore::new(), Default::default()));
    let store: Arc<dyn ObjectStoreBackend> = fault.clone();
    let running = start_test_server(store.clone(), IngestConcurrencyLimit::Bounded(2)).await;
    let grpc_addr = running.grpc_addr.expect("gateway mode binds gRPC");

    let data_key_prefix = format!("t/{}/m/l0/", TenantId::new(TENANT).hash().to_hex());
    let gate = fault.hold(Op::Put, Some(data_key_prefix), Occurrence::Always);

    let ts_ns = now_ns();
    let held_tasks: Vec<_> = ["held_a", "held_b"]
        .iter()
        .map(|name| {
            let request = metrics_export_request(name, ts_ns);
            tokio::spawn(async move {
                let mut client = MetricsServiceClient::connect(format!("http://{grpc_addr}"))
                    .await
                    .expect("gRPC client connects");
                let mut tonic_request = tonic::Request::new(request);
                tonic_request.metadata_mut().insert(
                    "authorization",
                    format!("Bearer {TOKEN}").parse().expect("ascii metadata"),
                );
                client
                    .export(tonic_request)
                    .await
                    .expect("held export completes once released")
            })
        })
        .collect();

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        held_tasks.iter().all(|t| !t.is_finished()),
        "both admitted gRPC requests must still be in flight, blocked on the held flush"
    );

    let mut client = MetricsServiceClient::connect(format!("http://{grpc_addr}"))
        .await
        .expect("gRPC client connects");
    let mut third_request = tonic::Request::new(metrics_export_request("shed", ts_ns));
    third_request.metadata_mut().insert(
        "authorization",
        format!("Bearer {TOKEN}").parse().expect("ascii metadata"),
    );
    let status = client
        .export(third_request)
        .await
        .expect_err("a request over the in-flight ceiling must be shed");
    assert_eq!(status.code(), tonic::Code::ResourceExhausted);

    drain_until_done(&gate, &held_tasks).await;
    for task in held_tasks {
        task.await.expect("task joins");
    }

    running.shutdown().await.expect("graceful shutdown");
}

/// (c) `0` disables the ceiling: many more concurrent requests than any
/// bounded default would admit all succeed, none shed.
#[tokio::test]
async fn zero_limit_disables_shedding() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_test_server(store, IngestConcurrencyLimit::Unlimited).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let ts_ns = now_ns();
    let tasks: Vec<_> = (0..50)
        .map(|i| {
            let client = client.clone();
            let base = base.clone();
            let request = metrics_export_request(&format!("unlimited_{i}"), ts_ns);
            tokio::spawn(async move {
                client
                    .post(format!("{base}/v1/metrics"))
                    .header("authorization", format!("Bearer {TOKEN}"))
                    .header("content-type", "application/x-protobuf")
                    .body(request.encode_to_vec())
                    .send()
                    .await
                    .expect("export request completes")
            })
        })
        .collect();

    for task in tasks {
        let response = task.await.expect("task joins");
        assert_eq!(
            response.status(),
            200,
            "with the ceiling disabled, no concurrent request is ever shed"
        );
    }

    running.shutdown().await.expect("graceful shutdown");
}
