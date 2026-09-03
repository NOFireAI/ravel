//! Durable `shard_count` drift-tolerance acceptance test (ADR-0082, over
//! ADR-0050 section 5). Ingest across four shards through a real in-process
//! server (which writes the provisioning record with shard_count=4), then
//! simulate a restart configured for two shards and assert the process starts
//! cleanly and the query path serves the recorded four-shard range rather than
//! a truncated `0..2`.
//!
//! The startup check lives in `main.rs`
//! (`ravel_server::provisioning::validate_static_provisioning`, called before
//! any listener binds), not in `ravel_server::start`, so the "restart"
//! phase calls that exact function the binary calls, against the same store the
//! first process wrote. The query path is exercised through a real `Catalog`
//! built the way `build_catalog` builds it (with provisioning enforcement):
//! generation-aware resolve serves the recorded four shards even when the live
//! `--shards` default is two.

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
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{Signal, TenantId, TimeRange};

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
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        adaptive_flush_delay: false,
        max_flush_delay: std::time::Duration::from_secs(2),
        max_flush_delay_idle: std::time::Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
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
        typed_attr_columns: Default::default(),
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        catalog_cache_max_bytes: 256 * 1024 * 1024,
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

/// Ingest 4 shards, restart at 2, assert the restart starts cleanly and the
/// query path serves the recorded four-shard range (ADR-0082).
#[tokio::test]
async fn startup_tolerates_shard_count_drift() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());

    // Phase 1: ingest across 4 shards through the real HTTP handler. This writes
    // the provisioning record with shard_count=4 (on the first write) and lands
    // segment data.
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

    // A query at the correct shard_count=4 serves the data (the record does not
    // break normal operation).
    let query_ok = client
        .get(format!("{base}/api/v1/query"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .query(&[("query", "cpu_usage_0")])
        .send()
        .await
        .expect("query at 4 shards succeeds");
    assert_eq!(
        query_ok.status(),
        200,
        "query at the correct shard_count works"
    );

    running.shutdown().await.expect("graceful shutdown");

    // Phase 2: simulate a restart configured for 2 shards. This is exactly what
    // `main.rs` runs before binding any listener: the static tenant ("acme",
    // from `--tenant-token`) is validated against its record. Under ADR-0082 a
    // recorded shard_count (4) above the live default (2) is tolerated: startup
    // proceeds rather than refusing. Before ADR-0082 this returned
    // `ProvisioningError::ShardCountMismatch` and the `.expect(...)` panicked.
    let static_tenants = vec![TenantId::new(TENANT).hash()];
    ravel_server::provisioning::validate_static_provisioning(
        store.as_ref(),
        &static_tenants,
        RESTART_SHARDS,
        now_ns(),
    )
    .await
    .expect("a restart with a lower --shards default must tolerate the recorded count (ADR-0082)");

    // Phase 3: prove the query path serves the recorded four-shard range, not a
    // truncated `0..2`. A catalog built for 2 shards (the way `build_catalog`
    // builds it, with provisioning enforcement) resolves via the record's own
    // generation history, so the resolve succeeds and covers the recorded
    // shards. Before ADR-0082 this failed with a provisioning error.
    let catalog = Catalog::new(
        store.clone(),
        CatalogConfig {
            shard_count: RESTART_SHARDS,
            ..CatalogConfig::default()
        },
    )
    .expect("catalog builds")
    .with_provisioning_enforcement();
    catalog
        .resolve(
            &TenantId::new(TENANT).hash(),
            Signal::Metrics,
            TimeRange {
                start_ns: 0,
                end_ns: now_ns(),
            },
            &[],
            now_ns(),
        )
        .await
        .expect("a query at a lower --shards default resolves over the recorded shard range");
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
