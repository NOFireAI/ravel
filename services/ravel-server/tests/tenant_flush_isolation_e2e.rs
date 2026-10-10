//! One tenant's hung flushes must not starve a co-resident tenant's flush on
//! the same shard (issue #1921, ADR-2708 D3), through the real server: real
//! `ravel_server::start`, real OTLP HTTP exports, and a `FaultStore` hold on
//! tenant A's metrics prefix standing in for a stalled S3 prefix.
//!
//! The server takes `ravel_ingest::IngestConfig`'s own default permit count
//! and leaves the per-tenant share unset, so this pins the shipped defaults:
//! A fills its share (N-1 permits), its next trigger is deferred, and tenant
//! B's strict write still commits on the permit A's share leaves free.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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

const TOKEN_A: &str = "token-a";
const TOKEN_B: &str = "token-b";
const TENANT_A: &str = "stalled-tenant";
const TENANT_B: &str = "healthy-tenant";

/// The server's strict-write ack deadline (`DEFAULT_ACK_DEADLINE` in
/// services/ravel-server/src/lib.rs). B's write must return inside it.
const STRICT_ACK_DEADLINE: Duration = Duration::from_secs(10);

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
}

fn export_request(metric_name: &str, ts_ns: i64) -> ExportMetricsServiceRequest {
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(AnyValueVariant::StringValue("isolation".to_string())),
                    }),
                    ..Default::default()
                }],
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

/// The tenant's level-0 metrics data prefix, where a flush's first PUT lands.
fn data_prefix(tenant: &str) -> String {
    format!("t/{}/m/l0/", TenantId::new(tenant).hash().to_hex())
}

/// One shard, so both tenants share one shard actor and its permits.
async fn start_server(store: Arc<dyn ObjectStoreBackend>) -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN_A.to_string(), TenantId::new(TENANT_A));
    tokens.insert(TOKEN_B.to_string(), TenantId::new(TENANT_B));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let config = ServerConfig {
        audit_pipeline: Default::default(),
        audit_text: Default::default(),
        query_budgets: Default::default(),
        max_inflight_flushes: ravel_ingest::IngestConfig::default().max_inflight_flushes,
        max_inflight_flushes_per_tenant: None,
        max_queued_flushes: 8,
        adaptive_flush_delay: false,
        // Fast enough that each strict write's flush fires within a few
        // hundred milliseconds.
        max_flush_delay: Duration::from_millis(250),
        max_flush_delay_idle: Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
        idle_flush_byte_floor: 0,
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
        cpu_gate_permits: Default::default(),
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
        shutdown_timeout: ravel_server::DEFAULT_SHUTDOWN_TIMEOUT,
        drain_settle_interval: Duration::ZERO,
        ingest_concurrency_limit: IngestConcurrencyLimit::Bounded(1024),
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

/// Held calls on `prefix`. The held registry is store-wide, so a count taken
/// from the gate alone would include every other gate's calls.
fn held_on(gate: &GateHandle, prefix: &str) -> usize {
    gate.held_details()
        .iter()
        .filter(|(_, op, key)| *op == Op::Put && key.starts_with(prefix))
        .count()
}

/// The metrics-signal sample of `family` on a `/metrics` scrape.
async fn scrape_metrics_sample(client: &reqwest::Client, base: &str, family: &str) -> u64 {
    let body = client
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("metrics scrape completes")
        .text()
        .await
        .expect("metrics body is text");
    body.lines()
        .find(|line| {
            line.starts_with(&format!("{family}{{")) && line.contains("signal=\"metrics\"")
        })
        .and_then(|line| line.rsplit(' ').next())
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(0)
}

async fn eventually(what: &str, mut check: impl AsyncFnMut() -> bool) {
    for _ in 0..1_000 {
        if check().await {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("timed out waiting for {what}");
}

fn spawn_export(
    client: &reqwest::Client,
    base: &str,
    token: &'static str,
    metric_name: &str,
) -> tokio::task::JoinHandle<reqwest::StatusCode> {
    let client = client.clone();
    let url = format!("{base}/v1/metrics");
    let body = export_request(metric_name, now_ns()).encode_to_vec();
    tokio::spawn(async move {
        client
            .post(url)
            .header("authorization", format!("Bearer {token}"))
            .header("content-type", "application/x-protobuf")
            .body(body)
            .send()
            .await
            .expect("export gets an HTTP response")
            .status()
    })
}

#[tokio::test]
async fn a_stalled_tenant_leaves_a_permit_for_a_coresident_strict_write() {
    let permits = ravel_ingest::IngestConfig::default().max_inflight_flushes as usize;
    let share = permits.saturating_sub(1).max(1);
    assert_eq!(
        (permits, share),
        (4, 3),
        "shipped defaults: 4 permits per shard, a tenant share of N-1"
    );

    let fault = Arc::new(FaultStore::new(MemoryStore::new(), Default::default()));
    let store: Arc<dyn ObjectStoreBackend> = fault.clone();
    let running = start_server(store.clone()).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let prefix_a = data_prefix(TENANT_A);
    let prefix_b = data_prefix(TENANT_B);
    let gate = fault.hold(Op::Put, Some(prefix_a.clone()), Occurrence::Always);

    // One write at a time, each waited onto its own held PUT, so A ends up
    // with exactly `share` flushes holding permits.
    let mut a_writes = Vec::new();
    for i in 0..share {
        a_writes.push(spawn_export(&client, &base, TOKEN_A, &format!("a_{i}")));
        let want = i + 1;
        eventually("A's flush to park on its held PUT", async || {
            held_on(&gate, &prefix_a) >= want
        })
        .await;
    }

    // A is at its share now: its next trigger is deferred, not spawned.
    let deferred_before =
        scrape_metrics_sample(&client, &base, "ravel_ingest_flush_trigger_deferred_total").await;
    a_writes.push(spawn_export(&client, &base, TOKEN_A, "a_over_share"));
    eventually("A's over-share trigger to be deferred", async || {
        scrape_metrics_sample(&client, &base, "ravel_ingest_flush_trigger_deferred_total").await
            > deferred_before
    })
    .await;

    let started = Instant::now();
    let b_status = tokio::time::timeout(
        STRICT_ACK_DEADLINE,
        spawn_export(&client, &base, TOKEN_B, "b_healthy"),
    )
    .await
    .expect("B's strict write returns inside its ack deadline")
    .expect("B's export task completes");
    let elapsed = started.elapsed();
    assert_eq!(
        b_status, 200,
        "B's strict write commits on the permit A's share left free"
    );
    assert!(
        elapsed < STRICT_ACK_DEADLINE,
        "B returned in {elapsed:?}, not inside the {STRICT_ACK_DEADLINE:?} deadline"
    );

    // The hold fired on A and only on A: exactly `share` PUTs parked on A's
    // prefix (the over-share flush never reached one), none on B's, and B's
    // object is durable.
    assert_eq!(
        held_on(&gate, &prefix_a),
        share,
        "A holds exactly its share of permits, all parked on the hold"
    );
    assert_eq!(held_on(&gate, &prefix_b), 0, "the hold never touched B");
    assert!(
        !list_all(store.as_ref(), &prefix_b)
            .await
            .expect("list B's objects")
            .is_empty(),
        "B's flush published its data object"
    );
    assert!(
        a_writes.iter().all(|w| !w.is_finished()),
        "A's writes are all still waiting on the stalled prefix"
    );

    // Release A so the server drains cleanly.
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            for id in gate.held() {
                gate.release(id);
            }
            if a_writes.iter().all(|w| w.is_finished()) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("A's writes finish once its prefix is released");
    running.shutdown().await.expect("graceful shutdown");
}
