//! ADR-1702 task 9 reachability: a server built by [`ravel_server::start`] runs
//! an OTLP-HTTP gzip inflate, a Remote Write snappy decode and the metrics
//! shard flush encode on its write CPU gate.
//!
//! The server keeps the gate's default 256 KiB inline floor, so each request
//! body is built from pseudo-random sample values that neither gzip nor snappy
//! can shrink below the floor, and the test asserts that premise before it
//! reads anything. Both requests are strict, so each returns only after the
//! flush of its own points has published, and the flush buffers are far over
//! the floor. The write gate's per-site counters on `/metrics` are read once,
//! after both requests.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::io::Write as _;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use flate2::Compression;
use flate2::write::GzEncoder;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use prost::Message;
use ravel_cpu_gate::DEFAULT_INLINE_FLOOR_BYTES;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_remote_write::proto::write_v2::{
    Request as ProtoRequestV2, Sample as ProtoSampleV2, TimeSeries as ProtoTimeSeriesV2,
};
use ravel_server::ingest_concurrency::IngestConcurrencyLimit;
use ravel_server::{FoldTaskConfig, LimitsConfig, Mode, ServerConfig};
use ravel_types::TenantId;

const TOKEN: &str = "testtoken";
const TENANT: &str = "acme";
/// Samples per request: under the OTLP per-request cap of 100,000 points, and
/// enough pseudo-random doubles that each compressed body clears the floor.
const POINTS: usize = 60_000;

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
}

/// Deterministic xorshift values, so the bodies are reproducible and their
/// doubles carry close to 64 bits of entropy each.
fn values(seed: u64, n: usize) -> Vec<f64> {
    let mut x = seed;
    (0..n)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            f64::from_bits((x >> 12) | 0x3ff0_0000_0000_0000)
        })
        .collect()
}

fn otlp_gzip_body() -> Vec<u8> {
    let end_ns = now_ns() as u64;
    let data_points = values(0x9E37_79B9_7F4A_7C15, POINTS)
        .into_iter()
        .enumerate()
        .map(|(i, value)| NumberDataPoint {
            time_unix_nano: end_ns - (POINTS - i) as u64 * 1_000_000,
            value: Some(NumberValue::AsDouble(value)),
            ..Default::default()
        })
        .collect();
    let request = ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            scope_metrics: vec![ScopeMetrics {
                metrics: vec![Metric {
                    name: "gate_e2e_otlp".to_string(),
                    data: Some(MetricData::Gauge(Gauge { data_points })),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder
        .write_all(&request.encode_to_vec())
        .expect("gzip write");
    encoder.finish().expect("gzip finish")
}

fn remote_write_snappy_body() -> Vec<u8> {
    let end_ms = now_ns() / 1_000_000;
    let request = ProtoRequestV2 {
        symbols: vec![
            String::new(),
            "__name__".to_string(),
            "gate_e2e_rw".to_string(),
        ],
        timeseries: vec![ProtoTimeSeriesV2 {
            labels_refs: vec![1, 2],
            samples: values(0xD1B5_4A32_D192_ED03, POINTS)
                .into_iter()
                .enumerate()
                .map(|(i, value)| ProtoSampleV2 {
                    value,
                    timestamp: end_ms - (POINTS - i) as i64,
                    start_timestamp: 0,
                })
                .collect(),
            histograms: vec![],
            exemplars: vec![],
            metadata: None,
        }],
    };
    snap::raw::Encoder::new()
        .compress_vec(&request.encode_to_vec())
        .expect("snappy compress")
}

async fn start_ingest_server(store: Arc<dyn ObjectStoreBackend>) -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let config = ServerConfig {
        audit_pipeline: Default::default(),
        audit_text: Default::default(),
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        max_queued_flushes: 8,
        adaptive_flush_delay: false,
        max_flush_delay: Duration::from_millis(200),
        max_flush_delay_idle: Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
        idle_flush_byte_floor: 0,
        mode: Mode::Gateway,
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
        max_ingest_lag: ravel_server::DEFAULT_MAX_INGEST_LAG,
        deployment_key: None,
        gc: ravel_maintain::GcConfigValues::maintain_defaults(),
        query_deadline: ravel_query::EngineConfig::default().deadline,
        store_probe_interval: ravel_server::store_probe::DEFAULT_STORE_PROBE_INTERVAL,
        admission_reconcile_interval: ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL,
        query_concurrency_limit: ravel_query::QueryConcurrencyLimit::Unlimited,
        max_s3_requests: ravel_query::EngineConfig::default().max_s3_requests,
        scrub_period: Duration::from_secs(7 * 86_400),
        indexed_fields: Default::default(),
        typed_attr_columns: Default::default(),
        parquet_profiles: None,
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        catalog_cache_max_bytes: 256 * 1024 * 1024,
        process_memory_budget_bytes: u64::MAX,
        process_memory_budget_is_fallback: false,
        cache_dir: None,
        catalog_resolve_concurrency: None,
        cpu_gate_permits: ravel_server::config::CpuGatePermits { read: 2, write: 2 },
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
        shutdown_timeout: ravel_server::DEFAULT_SHUTDOWN_TIMEOUT,
        drain_settle_interval: Duration::ZERO,
        ingest_concurrency_limit: IngestConcurrencyLimit::Unlimited,
    };
    ravel_server::start(
        config,
        store.clone(),
        store,
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts")
}

/// The write gate's `(jobs, inline)` counts for `site` on `/metrics`, each
/// required to render exactly once.
fn write_gate_site(body: &str, site: &str) -> (u64, u64) {
    let figure = |family: &str| {
        let name = format!("ravel_cpu_gate_{family}{{");
        let labels = format!("gate=\"write\",site=\"{site}\"}} ");
        let samples: Vec<&str> = body
            .lines()
            .filter(|line| line.starts_with(name.as_str()))
            .filter_map(|line| line.split_once(labels.as_str()).map(|(_, value)| value))
            .collect();
        assert_eq!(
            samples.len(),
            1,
            "{name}..{labels} must render once:\n{body}"
        );
        samples[0].parse::<u64>().expect("counter value")
    };
    (figure("jobs_total"), figure("inline_total"))
}

/// One gzip export and one Remote Write request are each one gated decode job
/// at their own site, and the two strict writes flush in two flushes, each one
/// gated `metrics_flush` job. Nothing at these sites runs inline: every unit is
/// over the default floor.
///
/// Fails with the write gate left off the gateway state, off the Remote Write
/// state or off the metrics router: that site then reads `(0, 0)`, since its
/// work runs without consulting any gate.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_path_runs_on_the_servers_write_gate() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_ingest_server(store).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let gzip_body = otlp_gzip_body();
    let snappy_body = remote_write_snappy_body();
    for (what, len) in [("gzip", gzip_body.len()), ("snappy", snappy_body.len())] {
        assert!(
            len as u64 >= DEFAULT_INLINE_FLOOR_BYTES,
            "the {what} body ({len} bytes) must clear the gate's inline floor"
        );
    }

    let otlp = client
        .post(format!("{base}/v1/metrics"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .header("content-encoding", "gzip")
        .body(gzip_body)
        .send()
        .await
        .expect("OTLP export completes");
    let status = otlp.status();
    assert_eq!(status, 200, "{}", otlp.text().await.unwrap_or_default());

    let rw = client
        .post(format!("{base}/api/v1/write"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header(
            "content-type",
            "application/x-protobuf;proto=io.prometheus.write.v2.Request",
        )
        .body(snappy_body)
        .send()
        .await
        .expect("remote write completes");
    let status = rw.status();
    assert!(
        status.is_success(),
        "{status}: {}",
        rw.text().await.unwrap_or_default()
    );

    let body = client
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("metrics scrape completes")
        .text()
        .await
        .expect("metrics body is text");
    assert_eq!(
        (
            write_gate_site(&body, "otlp_http_gzip"),
            write_gate_site(&body, "remote_write_snappy"),
            write_gate_site(&body, "metrics_flush"),
        ),
        ((1, 0), (1, 0), (2, 0)),
        "(otlp_http_gzip, remote_write_snappy, metrics_flush) as (jobs, inline)"
    );

    running.shutdown().await.expect("server shuts down");
}
