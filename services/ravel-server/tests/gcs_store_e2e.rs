//! ADR-3456 D5: a server started with `--store gcs` writes and reads through
//! the GCS store, end to end, against an in-process fake GCS endpoint.
//!
//! The bucket is qualified first through `ravel_object_store::conformance`,
//! as `ravel-cli store qualify` would, then the server is built from the
//! parsed command line the way `main` builds it: `build_store`, the
//! qualification gate, then `ravel_server::start`.

#![cfg(feature = "sql")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use ravel_object_store::PutOptions;
use ravel_object_store::conformance::{
    CONFORMANCE_SUITE_VERSION, QUALIFICATION_KEY, QualificationRecord, run_conformance_suite,
};
use ravel_object_store::gcs::GcsConfig;
use ravel_object_store::gcs::fake::FakeGcs;
use ravel_server::config::{QueryBudgets, StoreKind};
use ravel_server::qualification::QualificationError;
use ravel_server::store::{BuiltStore, build_store};
use ravel_server::{Cli, FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{TenantHash, TenantId};
use serde_json::{Value, json};

const BUCKET: &str = "ravel-e2e";
const TOKEN: &str = "acme-token";
const TENANT: &str = "acme";
const NS_PER_HOUR: i64 = 3_600_000_000_000;
const CACHE_MAX_BYTES: u64 = 64 * 1024 * 1024;
const BODIES: [&str; 3] = ["gcs line 0", "gcs line 1", "gcs line 2"];

fn tenant_hash() -> TenantHash {
    TenantId::new(TENANT).hash()
}

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
}

/// The command line under test, validated the way `main` validates it.
fn gcs_cli(fake: &FakeGcs) -> Cli {
    Cli::parse_validated_from([
        "ravel-server",
        "--store",
        "gcs",
        "--gcs-bucket",
        BUCKET,
        "--gcs-endpoint",
        fake.endpoint(),
    ])
    .expect("--store gcs with a bucket and a loopback endpoint is accepted")
}

/// `path` with every `%XX` escape decoded.
fn percent_decoded(path: &str) -> String {
    let bytes = path.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%'
            && let Some(byte) = path
                .get(i + 1..i + 3)
                .and_then(|hex| u8::from_str_radix(hex, 16).ok())
        {
            out.push(byte);
            i += 3;
            continue;
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8(out).expect("decoded path is UTF-8")
}

/// Runs the conformance suite through `built`'s store and writes the
/// qualification record `ravel-cli store qualify` writes on a pass.
async fn qualify(built: &BuiltStore, backend_identity: &str) {
    let store = built.foreground.as_ref();
    let report = run_conformance_suite(
        store,
        "sys/qualify/e2e/",
        GcsConfig::new(BUCKET).list_page_size,
    )
    .await;
    assert!(
        report.passed(),
        "the fake GCS bucket passes the conformance suite: {:?}",
        report.failures().collect::<Vec<_>>()
    );
    let record = QualificationRecord {
        suite_version: CONFORMANCE_SUITE_VERSION,
        backend_identity: backend_identity.to_string(),
        qualified_unix_ns: now_ns(),
        passed_properties: report
            .results
            .iter()
            .map(|r| r.property.name().to_string())
            .collect(),
    };
    store
        .put(
            QUALIFICATION_KEY,
            Bytes::from(serde_json::to_vec_pretty(&record).expect("encode record")),
            PutOptions::create_if_absent(),
        )
        .await
        .expect("qualification record written");
}

async fn ingest(client: &reqwest::Client, base: &str, base_ts_ns: i64) {
    let log_records = BODIES
        .iter()
        .enumerate()
        .map(|(i, body)| {
            let ts = (base_ts_ns + i as i64) as u64;
            LogRecord {
                time_unix_nano: ts,
                observed_time_unix_nano: ts,
                severity_number: 9,
                severity_text: "INFO".to_string(),
                body: Some(AnyValue {
                    value: Some(AnyValueVariant::StringValue(body.to_string())),
                }),
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

async fn sql(client: &reqwest::Client, base: &str, base_ts_ns: i64, statement: &str) -> Value {
    let start = (base_ts_ns - NS_PER_HOUR) as f64 / 1e9;
    let end = (base_ts_ns + NS_PER_HOUR) as f64 / 1e9;
    let response = client
        .post(format!("{base}/api/v1/sql"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/json")
        .body(json!({ "query": statement, "start": start, "end": end }).to_string())
        .send()
        .await
        .expect("sql request sent");
    let status = response.status();
    let value: Value = response.json().await.expect("sql response is JSON");
    assert_eq!(status, 200, "{value}");
    value
}

/// The `ravel_store_attempts_total{op="put"}` sample on a `/metrics` scrape.
async fn put_attempts(client: &reqwest::Client, base: &str) -> u64 {
    let body = client
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("metrics scrape completes")
        .text()
        .await
        .expect("metrics body is text");
    let samples: Vec<u64> = body
        .lines()
        .filter(|line| {
            line.starts_with("ravel_store_attempts_total{") && line.contains("op=\"put\"")
        })
        .map(|line| {
            line.rsplit(' ')
                .next()
                .and_then(|value| value.parse().ok())
                .unwrap_or_else(|| panic!("unparseable sample: {line}"))
        })
        .collect();
    assert_eq!(samples.len(), 1, "one put attempts sample: {body}");
    samples[0]
}

/// FLIP: building a `MemoryStore` for `StoreKind::Gcs` in `build_store`
/// leaves the fake with no PUT under the tenant's prefix, and returning the
/// `s3://` form from `Cli::backend_identity` for it fails the identity
/// assertion.
#[tokio::test]
async fn server_on_gcs_ingests_and_reads_logs() {
    let fake = FakeGcs::start(BUCKET).await.expect("fake GCS starts");
    let cli = gcs_cli(&fake);
    let identity = format!("gs://{BUCKET}@{}", fake.endpoint());
    assert_eq!(cli.backend_identity().as_deref(), Some(identity.as_str()));

    let built = build_store(&cli, CACHE_MAX_BYTES).expect("the gcs store builds");
    qualify(&built, &identity).await;
    ravel_server::qualification::enforce(
        built.foreground.as_ref(),
        cli.store,
        cli.backend_identity().as_deref(),
    )
    .await
    .expect("the qualified bucket passes the startup gate");
    fake.clear_requests();

    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    let server = ravel_server::start(
        server_config(tokens),
        built.foreground,
        built.background,
        built.metrics,
        built.cache,
    )
    .await
    .expect("server starts");
    let base = format!("http://{}", server.http_addr);
    let client = reqwest::Client::new();
    let base_ts_ns = now_ns();

    let puts_before = put_attempts(&client, &base).await;
    ingest(&client, &base, base_ts_ns).await;

    let count = sql(&client, &base, base_ts_ns, "SELECT count(*) AS n FROM logs").await;
    assert_eq!(count["data"]["rows"], json!([[BODIES.len()]]), "{count}");
    let bodies = sql(
        &client,
        &base,
        base_ts_ns,
        "SELECT body FROM logs ORDER BY ts",
    )
    .await;
    assert_eq!(
        bodies["data"]["rows"],
        json!(BODIES.map(|body| [body])),
        "{bodies}"
    );

    let tenant_prefix = format!("/{BUCKET}/t/{}/", tenant_hash().to_hex());
    let tenant_puts: Vec<String> = fake
        .requests()
        .into_iter()
        .filter(|request| request.method == "PUT")
        .map(|request| percent_decoded(&request.path))
        .filter(|path| path.starts_with(&tenant_prefix))
        .collect();
    assert!(
        !tenant_puts.is_empty(),
        "the fake saw no PUT under {tenant_prefix}: {:?}",
        fake.requests()
    );

    let puts_after = put_attempts(&client, &base).await;
    assert!(
        puts_after > puts_before,
        "ravel_store_attempts_total{{op=\"put\"}} did not move: {puts_before} -> {puts_after}"
    );

    server.shutdown().await.expect("server shuts down");
}

/// The startup gate refuses a `--store gcs` bucket that was never qualified.
#[tokio::test]
async fn server_on_gcs_refuses_an_unqualified_bucket() {
    let fake = FakeGcs::start(BUCKET).await.expect("fake GCS starts");
    let cli = gcs_cli(&fake);
    assert_eq!(cli.store, StoreKind::Gcs);
    let built = build_store(&cli, CACHE_MAX_BYTES).expect("the gcs store builds");
    let err = ravel_server::qualification::enforce(
        built.foreground.as_ref(),
        cli.store,
        cli.backend_identity().as_deref(),
    )
    .await
    .expect_err("an unqualified gcs bucket refuses startup");
    assert!(matches!(err, QualificationError::Absent), "{err}");
    assert!(
        fake.requests()
            .iter()
            .any(|request| request.method == "GET" && request.is_object(BUCKET, QUALIFICATION_KEY)),
        "the gate read the record from the fake: {:?}",
        fake.requests()
    );
}

fn server_config(tokens: HashMap<String, TenantId>) -> ServerConfig {
    ServerConfig {
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        audit_pipeline: None,
        audit_text: Default::default(),
        query_budgets: QueryBudgets::default(),
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
        disable_cache: false,
        cache_max_bytes: CACHE_MAX_BYTES,
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
