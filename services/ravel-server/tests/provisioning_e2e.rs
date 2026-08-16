//! Durable `shard_count` acceptance test (ADR-0050 section 5, EC5; loosened by
//! ADR-0082). Ingest across four shards through a real in-process server
//! (which writes the provisioning record with shard_count=4), then simulate a
//! restart configured for two shards and assert the process STARTS (ADR-0082:
//! a present, decodable record is accepted regardless of what its gen-0
//! shard_count says), and that a query at the new, lower configured
//! shard_count still serves the COMPLETE original 4-shard data -- routing
//! comes entirely from the record's own generation history, never from the
//! live config default, so a lowered fleet-wide default never truncates a
//! tenant already provisioned at a higher count.
//!
//! The startup path itself lives in `main.rs`
//! (`ravel_server::provisioning::validate_static_provisioning`, called before
//! any listener binds), not in `ravel_server::start`, so the "restart"
//! phase calls that exact function the binary calls, against the same store the
//! first process wrote. The query-path routing-correctness claim is exercised
//! by actually restarting a real server (`ravel_server::start`, which wires
//! its catalog through `query::build_catalog` with provisioning enforcement
//! on) and querying it over HTTP, so a lower-shard_count query still resolves
//! every series, including ones that hashed to shard indices at or above the
//! new, lower configured value.

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
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{Signal, TenantId};

const TOKEN: &str = "testtoken";
const TENANT: &str = "acme";
const INGEST_SHARDS: u32 = 4;
const RESTART_SHARDS: u32 = 2;

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

/// A request carrying several distinct series so points route across shards.
fn export_request(ts_ns: i64) -> ExportMetricsServiceRequest {
    let metrics: Vec<Metric> = (0..12)
        .map(|i| Metric {
            name: format!("cpu_usage_{i}"),
            data: Some(MetricData::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    time_unix_nano: ts_ns as u64,
                    value: Some(NumberValue::AsDouble(i as f64)),
                    ..Default::default()
                }],
            })),
            ..Default::default()
        })
        .collect();
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: vec![string_kv("service.name", "demo")],
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                metrics,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

async fn start_server(
    store: Arc<dyn ObjectStoreBackend>,
    shard_count: u32,
) -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let config = ServerConfig {
        max_inflight_flushes: 1,
        adaptive_flush_delay: false,
        mode: Mode::All,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count,
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
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
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

/// A PromQL selector matching every series `export_request` writes, so a query
/// result count directly proves how many of the 12 series a resolve served --
/// the routing-correctness observable this test hinges on.
const ALL_SERIES_QUERY: &str = "{__name__=~\"cpu_usage_.*\"}";

async fn query_result_count(client: &reqwest::Client, base: &str) -> usize {
    let resp = client
        .get(format!("{base}/api/v1/query"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .query(&[("query", ALL_SERIES_QUERY)])
        .send()
        .await
        .expect("query request succeeds");
    assert_eq!(resp.status(), 200, "query should succeed");
    let body: serde_json::Value = resp.json().await.expect("query response is JSON");
    assert_eq!(body["status"], "success", "query body: {body}");
    body["data"]["result"]
        .as_array()
        .expect("result is an array")
        .len()
}

/// Ingest across 4 shards, restart at 2, assert the restart STARTS (ADR-0082:
/// a present, decodable record is accepted regardless of what its gen-0
/// shard_count says), and that a query against the restarted, lower-configured
/// server still serves the complete original 4-shard data -- routing comes
/// entirely from the record's own generation history, never truncating to the
/// new, lower configured value.
#[tokio::test]
async fn restart_at_lower_shard_count_serves_complete_data() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());

    // Phase 1: ingest across 4 shards through the real HTTP handler. This writes
    // the provisioning record with shard_count=4 (on the first write) and lands
    // segment data across however many of those 4 shards the 12 series hash to.
    let running = start_server(store.clone(), INGEST_SHARDS).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();
    let response = client
        .post(format!("{base}/v1/metrics"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .body(export_request(now_ns()).encode_to_vec())
        .send()
        .await
        .expect("export request succeeds");
    assert_eq!(response.status(), 200, "export at 4 shards should succeed");

    // The record was written under shard_count=4.
    let record_key =
        ravel_catalog::provisioning_key(&TenantId::new(TENANT).hash(), Signal::Metrics);
    store
        .get(&record_key, ravel_object_store::GetRange::Full)
        .await
        .expect("provisioning record exists after first ingest");

    // A query at the correct shard_count=4 serves all 12 series.
    let before_restart = query_result_count(&client, &base).await;
    assert_eq!(
        before_restart, 12,
        "all 12 series must be visible at the shard_count the data was written under"
    );

    running.shutdown().await.expect("graceful shutdown");

    // Phase 2: simulate a restart configured for 2 shards. This is exactly what
    // `main.rs` runs before binding any listener: the static tenant ("acme",
    // from `--tenant-token`) is validated against its record. ADR-0082: a
    // present, decodable record no longer refuses startup on disagreement, so
    // this now succeeds instead of erroring.
    let static_tenants = vec![TenantId::new(TENANT).hash()];
    ravel_server::provisioning::validate_static_provisioning(
        store.as_ref(),
        &static_tenants,
        RESTART_SHARDS,
        now_ns(),
    )
    .await
    .expect(
        "a disagreeing but decodable record must not refuse startup (ADR-0082); \
         the process starts and routing still comes from the generation history",
    );

    // Phase 3: actually restart a server against the same store at the lower
    // shard_count, and prove the query path never serves a truncated shard
    // range. `build_catalog` turns on provisioning enforcement exactly as the
    // real binary does, so this exercises the same enforced resolve path a
    // production restart would.
    let restarted = start_server(store.clone(), RESTART_SHARDS).await;
    let restarted_base = format!("http://{}", restarted.http_addr);
    let after_restart = query_result_count(&client, &restarted_base).await;
    assert_eq!(
        after_restart, 12,
        "a restart at a lower configured shard_count must still serve every series: \
         routing comes from the record's own generation history, not the live config default"
    );

    restarted.shutdown().await.expect("graceful shutdown");
}

/// The fresh-deployment guarantee at the server layer: a
/// brand-new tenant with no prior writes and no provisioning record does not
/// fail startup. This is the exact shape of a fresh operator-managed cluster
/// (configured tenant tokens, zero data).
#[tokio::test]
async fn fresh_tenant_with_no_prior_data_starts_cleanly() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let static_tenants = vec![
        TenantId::new("brand-new-a").hash(),
        TenantId::new("brand-new-b").hash(),
    ];
    ravel_server::provisioning::validate_static_provisioning(
        store.as_ref(),
        &static_tenants,
        INGEST_SHARDS,
        now_ns(),
    )
    .await
    .expect("a fresh tenant with no record and no data must not fail startup");
}
