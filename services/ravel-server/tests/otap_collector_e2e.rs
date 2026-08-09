//! Real-OTel-Collector-in-Docker e2e for the `otap` `ArrowMetricsService`
//! (ADR-0011, issue #12 phase 3).
//!
//! This is the "against a real sender" half of phase 3: a pinned OTel
//! Collector Contrib binary whose `otelarrow` exporter encodes OTAP with the
//! upstream Go otel-arrow library (not `ravel-otap`'s own encoder), streaming
//! `BatchArrowRecords` into an in-process `ravel-server` built with `--otap`
//! and backed by `MemoryStore`, with the exported sample read back via
//! `/api/v1/query`. The in-process `tests/otap_grpc.rs` suite already drives
//! the same decode/normalize/strict-commit/read-your-write path using
//! `ravel-otap`'s encoder as the client; this file is the piece that
//! substitutes a real, independent OTAP producer for that synthetic one, the
//! interop bar ADR-0011 sets ("the OTel collector ecosystem speaks OTAP").
//!
//! Flow: the test pushes one OTLP/HTTP metric into the collector's `otlp`
//! receiver; the collector's metrics pipeline re-exports it via `otelarrow`
//! (Arrow, plaintext, bearer auth in a gRPC header) to this server's gRPC
//! listener; the server decodes, normalizes, strict-commits, and the sample
//! becomes queryable.
//!
//! Pinned image: `otel/opentelemetry-collector-contrib:0.120.0`. The
//! `otelarrowexporter`/`otelarrowreceiver` components have shipped in the
//! Contrib distribution since v0.104.0 (the core distribution does NOT bundle
//! them), so a Contrib tag is required. The vendored protos here are
//! open-telemetry/otel-arrow tag v0.50.0 (proto/otel-arrow/README.md); the
//! exact Contrib tag whose bundled `otelarrow` component was built against
//! otel-arrow v0.50.0 should be confirmed against that tag's
//! `exporter/otelarrowexporter/go.mod` when this test is actually executed
//! with registry access. OTAP guarantees lossless OTLP<->OTAP convertibility
//! across versions, so a nearby Contrib tag is expected to interoperate; the
//! pin exists so a run is reproducible, not because this exact digest was
//! validated here (see below).
//!
//! `#[ignore]`d by default because it needs a working Docker daemon (the
//! executing user in the docker group or root, a reachable
//! `/var/run/docker.sock`) and the ability to pull or already have the pinned
//! image cached locally. Run explicitly with:
//!
//! ```text
//! cargo test -p ravel-server --features otap --test otap_collector_e2e -- --ignored
//! ```
//!
//! Like `tests/remote_write_prometheus_e2e.rs`, this was written but not
//! executed in the environment it was implemented in: the fleet executor's
//! user is not in the `docker` group and the host has no egress to pull the
//! image. The in-process `tests/otap_grpc.rs` suite covers the same server
//! path with real OTAP wire bytes (produced by `ravel-otap`'s encoder) and
//! does run in CI; this file swaps that encoder for a real OTel Collector.
#![cfg(feature = "otap")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::io::Write;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use prost::Message;
use ravel_object_store::instrument::StoreMetrics;
use ravel_object_store::memory::MemoryStore;
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::TenantId;

const TOKEN: &str = "testtoken";
const COLLECTOR_IMAGE: &str = "otel/opentelemetry-collector-contrib:0.120.0";
const METRIC_NAME: &str = "otap_collector_e2e";
const VALUE: f64 = 42.5;
/// The collector's OTLP/HTTP receiver port on the host (`--network host`).
const COLLECTOR_OTLP_HTTP_PORT: u16 = 4318;

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
        max_inflight_flushes: 1,
        adaptive_flush_delay: false,
        mode: Mode::All,
        // Bound to loopback but reachable from the collector container via
        // `--network host` (Linux only; no Docker Desktop fallback).
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
        // The `otap` feature links the service; this is the runtime opt-in
        // the `--otap` flag drives in the binary. Without it the
        // ArrowMetricsService is not registered and the collector's stream
        // would be UNIMPLEMENTED.
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
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
    };
    ravel_server::start(config, store, Arc::new(StoreMetrics::default()), None)
        .await
        .expect("server starts")
}

/// A collector config with an `otlp` receiver (the test pushes to it) and an
/// `otelarrow` exporter that streams OTAP to `ravel-server`'s gRPC listener.
/// `tls.insecure` because the listener is plaintext tonic; the bearer token
/// rides a gRPC header, exactly as `ravel-server` resolves the tenant from
/// stream-open metadata.
fn collector_config(ravel_grpc_port: u16) -> String {
    format!(
        r#"
receivers:
  otlp:
    protocols:
      http:
        endpoint: 127.0.0.1:{COLLECTOR_OTLP_HTTP_PORT}

exporters:
  otelarrow:
    endpoint: 127.0.0.1:{ravel_grpc_port}
    tls:
      insecure: true
    headers:
      authorization: 'Bearer {TOKEN}'
    arrow:
      num_streams: 1

service:
  telemetry:
    logs:
      level: info
  pipelines:
    metrics:
      receivers: [otlp]
      exporters: [otelarrow]
"#
    )
}

/// One OTLP gauge, one point, no resource: the same minimal shape
/// `tests/otap_grpc.rs` uses, so the series identity the collector re-encodes
/// as OTAP is `__name__` only.
fn otlp_gauge_request(ts_ns: i64) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: None,
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: METRIC_NAME.to_string(),
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

fn tempfile_dir() -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("ravel-otap-e2e-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("create temp config dir");
    dir
}

/// Query `METRIC_NAME` and return the single result series, or `None` while
/// the collector pipeline has not delivered it yet. No commit token is
/// available on this path (the strict ack goes to the collector, not the
/// test), so visibility is polled rather than pinned.
async fn try_query_series(
    client: &reqwest::Client,
    http_addr: std::net::SocketAddr,
) -> Option<serde_json::Value> {
    let response = client
        .get(format!("http://{http_addr}/api/v1/query"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .query(&[("query", METRIC_NAME)])
        .send()
        .await
        .expect("query request succeeds");
    assert_eq!(response.status(), 200, "query should succeed");
    let body: serde_json::Value = response.json().await.expect("query response is JSON");
    assert_eq!(body["status"], "success", "query body: {body}");
    let result = body["data"]["result"].as_array().expect("result array");
    result.first().cloned()
}

/// A metric sent OTLP -> collector -> `otelarrow` (real OTAP) -> `ravel-server`
/// round-trips into a queryable series with the right value.
#[tokio::test]
#[ignore = "requires a Docker daemon reachable by this user and the otel/opentelemetry-collector-contrib:0.120.0 image"]
async fn real_collector_otelarrow_export_is_queryable() {
    let running = start_test_server().await;
    let grpc_addr = running.grpc_addr.expect("gateway binds gRPC");
    let http_addr = running.http_addr;

    let config_dir = tempfile_dir();
    let config_path = config_dir.join("collector.yaml");
    std::fs::File::create(&config_path)
        .expect("create collector.yaml")
        .write_all(collector_config(grpc_addr.port()).as_bytes())
        .expect("write collector.yaml");

    let container_name = format!("ravel-otap-e2e-{}", std::process::id());

    let status = Command::new("docker")
        .args([
            "run",
            "--rm",
            "-d",
            "--network",
            "host",
            "--name",
            &container_name,
            "-v",
            &format!(
                "{}:/etc/otelcol-contrib/config.yaml:ro",
                config_path.display()
            ),
            COLLECTOR_IMAGE,
            "--config=/etc/otelcol-contrib/config.yaml",
        ])
        .status()
        .expect("docker must be runnable in an environment that executes this ignored test");
    assert!(status.success(), "docker run failed to start the collector");

    // Give the collector time to start its receiver and open the OTAP stream.
    tokio::time::sleep(Duration::from_secs(5)).await;

    let client = reqwest::Client::new();

    // Push one OTLP metric into the collector's receiver; it re-exports via
    // otelarrow. Retried because the receiver may not be listening for the
    // first cycle after container start.
    let payload = otlp_gauge_request(now_ns()).encode_to_vec();
    let mut pushed = false;
    for _ in 0..10 {
        let sent = client
            .post(format!(
                "http://127.0.0.1:{COLLECTOR_OTLP_HTTP_PORT}/v1/metrics"
            ))
            .header("content-type", "application/x-protobuf")
            .body(payload.clone())
            .send()
            .await;
        if matches!(sent, Ok(ref r) if r.status().is_success()) {
            pushed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(pushed, "collector otlp receiver never accepted the metric");

    // Poll until the exported sample lands (strict commit through the OTAP
    // path makes it durable + visible), bounded so a broken pipeline fails
    // rather than hangs.
    let mut series = None;
    for _ in 0..20 {
        if let Some(s) = try_query_series(&client, http_addr).await {
            series = Some(s);
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }

    let _ = Command::new("docker")
        .args(["stop", &container_name])
        .status();

    let series = series.expect("collector-exported OTAP sample became queryable");
    assert_eq!(series["metric"]["__name__"], METRIC_NAME);
    let got: f64 = series["value"][1]
        .as_str()
        .expect("value is a string")
        .parse()
        .expect("value parses as f64");
    assert!(
        (got - VALUE).abs() < f64::EPSILON,
        "unexpected value: {series}"
    );

    running.shutdown().await.expect("graceful shutdown");
}
