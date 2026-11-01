//! `--decoded-cache-max-bytes` through the server's own wiring: at 0 the SQL
//! response carries no `stats.decodedCache`, and above 0 it carries the field
//! exactly once per call, with every counter named, and for a statement with
//! a `LIKE '%...%'` arm the serve-if-fits estimate and verdict (ADR-3326).
//!
//! Two servers share one store holding one ingested log object and differ
//! only in the flag, and in the read cache the enabled one carries: with it
//! the logs scan stripes over more partitions than the one segment, so a
//! `LIKE`-only scan runs the plan pass that makes the verdict.

#![cfg(feature = "sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use ravel_catalog::{DeclaredColumnType, DeclaredTypedColumn, TenantConfig, TenantLifecycleState};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_server::config::QueryBudgets;
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{TenantHash, TenantId};
use serde_json::Value;

const TOKEN: &str = "acme-token";
const TENANT: &str = "acme";
const NS_PER_HOUR: i64 = 3_600_000_000_000;
const ROWS: usize = 500;
const DECODED_CACHE: u64 = 64 * 1024 * 1024;
const STATEMENT: &str = "SELECT count(*) AS n FROM logs WHERE severity_text = 'INFO'";
/// A declared `Str` attribute every row carries, so `LIKE` on it is a pushed
/// `Contains` arm.
const URL: &str = "url";
const LIKE_STATEMENT: &str = "SELECT count(*) AS n FROM logs WHERE url LIKE '%example%'";
const FIELDS: [&str; 10] = [
    "hits",
    "misses",
    "admissions",
    "evictions",
    "ghostHits",
    "marginSkips",
    "residentBytes",
    "pressureEvictions",
    "pressureFreedBytes",
    "deferredServed",
];

fn tenant_hash() -> TenantHash {
    TenantId::new(TENANT).hash()
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
}

async fn start(
    store: Arc<dyn ObjectStoreBackend>,
    decoded_cache_max_bytes: u64,
) -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    ravel_server::start(
        server_config(tokens, decoded_cache_max_bytes),
        store.clone(),
        store,
        Arc::new(ravel_object_store::StoreMetrics::default()),
        (decoded_cache_max_bytes > 0).then(|| {
            let bytes = 64 * 1024 * 1024;
            ravel_query::ReadCache::Ram(Arc::new(ravel_cache::Cache::new(
                ravel_cache::CacheLimits::new(bytes, 16_384, bytes),
            )))
        }),
    )
    .await
    .expect("server starts")
}

async fn ingest(client: &reqwest::Client, base: &str, base_ts_ns: i64) {
    let log_records = (0..ROWS)
        .map(|i| {
            let ts = (base_ts_ns + i as i64) as u64;
            LogRecord {
                time_unix_nano: ts,
                observed_time_unix_nano: ts,
                severity_number: 9,
                severity_text: "INFO".to_string(),
                body: Some(AnyValue {
                    value: Some(AnyValueVariant::StringValue(format!("request {i}"))),
                }),
                attributes: vec![KeyValue {
                    key: URL.to_string(),
                    value: Some(AnyValue {
                        value: Some(AnyValueVariant::StringValue(format!(
                            "https://example.com/page/{}",
                            i % 7
                        ))),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }
        })
        .collect();
    let request = ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(AnyValueVariant::StringValue("web".to_string())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records,
                schema_url: String::new(),
            }],
            ..Default::default()
        }],
    };
    let response = client
        .post(format!("{base}/v1/logs"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .body(request.encode_to_vec())
        .send()
        .await
        .expect("export sent");
    assert_eq!(response.status(), 200, "export accepted");
}

/// Run `statement` and return the raw response body and its `stats` object.
async fn run(
    client: &reqwest::Client,
    base: &str,
    base_ts_ns: i64,
    statement: &str,
) -> (String, Value) {
    let start = (base_ts_ns - NS_PER_HOUR) as f64 / 1e9;
    let end = (base_ts_ns + NS_PER_HOUR) as f64 / 1e9;
    let response = client
        .post(format!("{base}/api/v1/sql"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/json")
        .body(serde_json::json!({ "query": statement, "start": start, "end": end }).to_string())
        .send()
        .await
        .expect("sql request sent");
    let status = response.status();
    let body = response.text().await.expect("sql response body");
    let value: Value = serde_json::from_str(&body).expect("sql response is JSON");
    assert_eq!(status, 200, "{value}");
    assert_eq!(value["data"]["rows"][0][0], ROWS as u64, "{value}");
    (body, value["stats"].clone())
}

/// FLIP: dropping the `decodedCache` insert in `stats_json`
/// (`services/ravel-server/src/sql.rs`) fails the enabled half; inserting it
/// unconditionally fails the disabled half.
///
/// The `LIKE` statement's object carries `deferredSetEstimateBytes` too, a
/// positive estimate at most 0.95 of the cap, with `deferredServed` true;
/// the filtered statement's carries `deferredServed` false and no estimate.
#[tokio::test]
async fn decoded_cache_stats_appear_once_when_enabled_and_never_at_zero() {
    let memory = Arc::new(MemoryStore::new());
    let config = TenantConfig {
        typed_attr_columns: Some(vec![DeclaredTypedColumn {
            key: URL.to_string(),
            ty: DeclaredColumnType::Str,
        }]),
        ..TenantConfig::new(TenantLifecycleState::Active)
    };
    ravel_catalog::set_tenant_config(memory.as_ref(), &tenant_hash(), &config, now_ns())
        .await
        .expect("tenant config");
    let store: Arc<dyn ObjectStoreBackend> = memory;
    let client = reqwest::Client::new();
    let base_ts_ns = now_ns();

    let disabled = start(Arc::clone(&store), 0).await;
    let disabled_base = format!("http://{}", disabled.http_addr);
    ingest(&client, &disabled_base, base_ts_ns).await;
    let mut off = Vec::new();
    for _ in 0..2 {
        off.push(run(&client, &disabled_base, base_ts_ns, STATEMENT).await);
    }
    off.push(run(&client, &disabled_base, base_ts_ns, LIKE_STATEMENT).await);
    let off_metrics = scrape(&client, &disabled_base).await;
    disabled
        .shutdown()
        .await
        .expect("disabled server shuts down");

    let enabled = start(store, DECODED_CACHE).await;
    let enabled_base = format!("http://{}", enabled.http_addr);
    let mut on = Vec::new();
    for _ in 0..3 {
        on.push(run(&client, &enabled_base, base_ts_ns, STATEMENT).await);
    }
    let like = run(&client, &enabled_base, base_ts_ns, LIKE_STATEMENT).await;
    let on_metrics = scrape(&client, &enabled_base).await;
    enabled.shutdown().await.expect("enabled server shuts down");

    for name in METRIC_FAMILIES {
        assert_eq!(
            off_metrics.matches(&format!("# HELP {name} ")).count(),
            0,
            "{name} at cap 0"
        );
        assert_eq!(
            on_metrics.matches(&format!("# HELP {name} ")).count(),
            1,
            "{name} enabled"
        );
    }
    for name in [
        "ravel_decoded_cache_hits_total",
        "ravel_decoded_cache_admissions_total",
        "ravel_decoded_cache_resident_bytes",
    ] {
        assert!(
            sample(&on_metrics, name) > 0,
            "{name} must move after three runs:\n{on_metrics}"
        );
    }
    assert_eq!(
        sample(&on_metrics, "ravel_decoded_cache_max_bytes"),
        DECODED_CACHE
    );

    for (body, stats) in &off {
        assert_eq!(body.matches("\"decodedCache\"").count(), 0, "{body}");
        assert!(stats.get("decodedCache").is_none(), "{stats}");
    }
    for (body, stats) in &on {
        assert_eq!(body.matches("\"decodedCache\"").count(), 1, "{body}");
        let decoded = stats["decodedCache"]
            .as_object()
            .unwrap_or_else(|| panic!("stats.decodedCache: {stats}"));
        assert_eq!(decoded.len(), FIELDS.len(), "{stats}");
        for field in FIELDS {
            if field == "deferredServed" {
                assert_eq!(decoded[field], Value::Bool(false), "{stats}");
            } else {
                assert!(
                    decoded[field].is_u64(),
                    "stats.decodedCache.{field}: {stats}"
                );
            }
        }
    }
    assert_eq!(like.0.matches("\"decodedCache\"").count(), 1, "{}", like.0);
    let decoded = like.1["decodedCache"]
        .as_object()
        .unwrap_or_else(|| panic!("stats.decodedCache: {}", like.1));
    assert_eq!(decoded.len(), FIELDS.len() + 1, "{}", like.1);
    for field in FIELDS {
        if field == "deferredServed" {
            assert_eq!(decoded[field], Value::Bool(true), "{}", like.1);
        } else {
            assert!(
                decoded[field].is_u64(),
                "stats.decodedCache.{field}: {}",
                like.1
            );
        }
    }
    let estimate = decoded["deferredSetEstimateBytes"]
        .as_u64()
        .unwrap_or_else(|| panic!("deferredSetEstimateBytes: {}", like.1));
    assert!(estimate > 0, "{}", like.1);
    assert!(estimate * 20 <= DECODED_CACHE * 19, "{}", like.1);
    // The filtered statement is interactive, so the first run admits and the
    // later runs are served.
    let first = &on[0].1["decodedCache"];
    assert!(first["admissions"].as_u64().unwrap() > 0, "{first}");
    for (_, stats) in &on[1..] {
        let hot = &stats["decodedCache"];
        assert!(hot["hits"].as_u64().unwrap() > 0, "{hot}");
        assert_eq!(hot["misses"].as_u64(), Some(0), "{hot}");
        assert!(hot["residentBytes"].as_u64().unwrap() > 0, "{hot}");
    }
}

const METRIC_FAMILIES: [&str; 11] = [
    "ravel_decoded_cache_resident_bytes",
    "ravel_decoded_cache_max_bytes",
    "ravel_decoded_cache_entries",
    "ravel_decoded_cache_hits_total",
    "ravel_decoded_cache_misses_total",
    "ravel_decoded_cache_admissions_total",
    "ravel_decoded_cache_evictions_total",
    "ravel_decoded_cache_ghost_hits_total",
    "ravel_decoded_cache_margin_skips_total",
    "ravel_decoded_cache_pressure_evictions_total",
    "ravel_decoded_cache_expired_drops_total",
];

async fn scrape(client: &reqwest::Client, base: &str) -> String {
    client
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("scrape completes")
        .text()
        .await
        .expect("scrape body")
}

/// The value of the single `{mode}`-labelled sample of `name`.
fn sample(body: &str, name: &str) -> u64 {
    let lines: Vec<&str> = body
        .lines()
        .filter(|l| l.starts_with(&format!("{name}{{")))
        .collect();
    assert_eq!(lines.len(), 1, "{name}: {lines:?}");
    lines[0]
        .rsplit(' ')
        .next()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| panic!("{name}: {}", lines[0]))
}

fn server_config(tokens: HashMap<String, TenantId>, decoded_cache_max_bytes: u64) -> ServerConfig {
    ServerConfig {
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        audit_pipeline: None,
        audit_text: Default::default(),
        query_budgets: QueryBudgets {
            decoded_cache_max_bytes,
            ..QueryBudgets::default()
        },
        max_inflight_flushes: 1,
        max_inflight_flushes_per_tenant: None,
        max_queued_flushes: 8,
        adaptive_flush_delay: false,
        max_flush_delay: Duration::from_millis(50),
        max_flush_delay_idle: Duration::from_millis(50),
        min_flush_bytes: 256 * 1024,
        idle_flush_byte_floor: 0,
        mode: Mode::All,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: 1,
        tenant_resolver: ravel_server::tenant::build_resolver(tokens, false),
        mtls_listener: None,
        fold_tenants: vec![tenant_hash()],
        fold: FoldTaskConfig {
            enabled: false,
            ..FoldTaskConfig::default()
        },
        maintain: ravel_server::MaintenanceTaskConfig::default(),
        alerting: ravel_server::AlertEvalConfig::default(),
        oidc_refresh: None,
        otap: false,
        otap_max_decoded_batch_bytes: ravel_server::config::DEFAULT_OTAP_MAX_DECODED_BATCH_BYTES,
        metrics_tenant_labels: false,
        limits: ravel_server::LimitsConfig::default(),
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
        disable_cache: true,
        cache_max_bytes: 0,
        catalog_cache_max_bytes: 256 * 1024 * 1024,
        process_memory_budget_bytes: u64::MAX,
        process_memory_budget_is_fallback: false,
        cache_dir: None,
        catalog_resolve_concurrency: None,
        cpu_gate_permits: Default::default(),
        max_pending_strict_acks: 8_192,
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
        shutdown_timeout: ravel_server::DEFAULT_SHUTDOWN_TIMEOUT,
        drain_settle_interval: Duration::ZERO,
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
    }
}
