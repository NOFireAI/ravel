//! Probe for issue #4573: does a read carrying `min_commit_token` return a
//! log row whose (shard, ingest_hour) bucket a retention tombstone has
//! retired, while the token's commit record and level-0 object still exist
//! and the tenant has no erasure request?
//!
//! The tombstone is published straight into the store, exactly as the
//! maintain retention task writes it, with no sweep and no retention delete,
//! so the commit record and the L0 object stay in place.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs, SeverityNumber};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message;
use ravel_commit::keys::{self, COMMIT_SUFFIX, ParsedCommitKey};
use ravel_commit::signal;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions, list_all};
use ravel_proto::commit::v1::RetentionTombstone;
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{Signal, TenantId};
use serde_json::Value as JsonValue;

const TOKEN: &str = "testtoken";
const TENANT: &str = "acme";
const NS: i64 = 1_000_000_000;
const JOB: &str = "probe4573";
const BODY: &str = "probe 4573 retired row";
const QUERY: &str = r#"sum(count_over_time(ravel_log_lines{job="probe4573"}[1h]))"#;

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
}

fn export_request(ts_ns: i64) -> ExportLogsServiceRequest {
    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(AnyValueVariant::StringValue(JOB.to_string())),
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records: vec![LogRecord {
                    time_unix_nano: ts_ns as u64,
                    observed_time_unix_nano: ts_ns as u64,
                    severity_number: SeverityNumber::Info as i32,
                    severity_text: "INFO".to_string(),
                    body: Some(AnyValue {
                        value: Some(AnyValueVariant::StringValue(BODY.to_string())),
                    }),
                    ..Default::default()
                }],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

async fn start_test_server() -> (ravel_server::Running, Arc<MemoryStore>) {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let store = Arc::new(MemoryStore::new());
    let config = ServerConfig {
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        audit_pipeline: Default::default(),
        audit_text: Default::default(),
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        max_inflight_flushes_per_tenant: None,
        flush_object_backstop_multiple: 4,
        max_queued_flushes: 8,
        adaptive_flush_delay: false,
        max_flush_delay: std::time::Duration::from_secs(2),
        max_flush_delay_idle: std::time::Duration::from_secs(40),
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
        scrub_period: std::time::Duration::from_secs(7 * 86_400),
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
        ingest_hold: Default::default(),
        max_pending_strict_acks: 8_192,
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: std::time::Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
        shutdown_timeout: ravel_server::DEFAULT_SHUTDOWN_TIMEOUT,
        drain_settle_interval: std::time::Duration::ZERO,
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
    };
    let running = ravel_server::start(
        config,
        store.clone(),
        store.clone(),
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts");
    (running, store)
}

struct Ingested {
    running: ravel_server::Running,
    store: Arc<MemoryStore>,
    base: String,
    client: reqwest::Client,
    commit_token: String,
    commit_key: ParsedCommitKey,
    eval_time: String,
}

/// Starts a server, exports one log record and locates the commit record the
/// server wrote for it, so the bucket comes from the store, not from an
/// assumption about how the ingest path buckets a flush.
async fn ingest_one_record() -> Ingested {
    let (running, store) = start_test_server().await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();
    let ts_ns = (now_ns() / NS) * NS;

    let response = client
        .post(format!("{base}/v1/logs"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/x-protobuf")
        .body(export_request(ts_ns).encode_to_vec())
        .send()
        .await
        .expect("logs export request succeeds");
    assert_eq!(response.status(), 200, "logs export should succeed");
    let commit_token = response
        .headers()
        .get("x-ravel-commit-token")
        .expect("commit token header present")
        .to_str()
        .expect("commit token header is ascii")
        .to_string();

    let commit_keys: Vec<ParsedCommitKey> =
        list_all(store.as_ref() as &dyn ObjectStoreBackend, "t/")
            .await
            .expect("list store")
            .into_iter()
            .filter(|meta| meta.key.ends_with(&format!(".{COMMIT_SUFFIX}")))
            .filter_map(|meta| keys::parse_commit_key(&meta.key).ok())
            .filter(|parsed| parsed.signal == Signal::Logs)
            .collect();
    assert_eq!(
        commit_keys.len(),
        1,
        "expected exactly one logs commit record: {commit_keys:?}"
    );
    let commit_key = commit_keys.into_iter().next().unwrap();

    Ingested {
        running,
        store,
        base,
        client,
        commit_token,
        commit_key,
        eval_time: (ts_ns / NS + 20).to_string(),
    }
}

impl Ingested {
    /// Rows the PromQL logs surface counts for the probe job, with or without
    /// `min_commit_token`. An empty vector result counts as zero rows.
    async fn rows(&self, with_token: bool) -> u64 {
        let mut params = vec![("query", QUERY), ("time", self.eval_time.as_str())];
        if with_token {
            params.push(("min_commit_token", self.commit_token.as_str()));
        }
        let response = self
            .client
            .get(format!("{}/api/v1/query", self.base))
            .header("authorization", format!("Bearer {TOKEN}"))
            .query(&params)
            .send()
            .await
            .expect("query request succeeds");
        let status = response.status();
        let body: JsonValue = response.json().await.expect("query response is JSON");
        assert_eq!(status, 200, "body: {body}");
        assert_eq!(body["status"], "success", "body: {body}");
        let result = body["data"]["result"]
            .as_array()
            .expect("result is an array");
        match result.as_slice() {
            [] => 0,
            [sample] => sample["value"][1]
                .as_str()
                .expect("value is a string")
                .parse::<f64>()
                .expect("value parses as f64") as u64,
            _ => panic!("expected at most one aggregated series: {body}"),
        }
    }

    /// Publish a retention tombstone for the record's (shard, ingest_hour)
    /// bucket at its `retire.tmb` key, as the maintain retention task writes
    /// it. Returns the key written.
    async fn publish_tombstone(&self) -> String {
        let record = RetentionTombstone {
            format_version: 1,
            tenant_hash: self.commit_key.tenant_hash.0.to_vec(),
            signal: signal::to_proto(Signal::Logs).into(),
            shard: self.commit_key.shard,
            ingest_hour_bucket: self.commit_key.ingest_hour_bucket,
            retired_at_ns: 0,
            retention_window_ns: 0,
            record_count_observed: 0,
        };
        let key = keys::retention_tombstone_key_for(&record).expect("tombstone key");
        self.store
            .put(
                &key,
                bytes::Bytes::from(record.encode_to_vec()),
                PutOptions::create_if_absent(),
            )
            .await
            .expect("put tombstone");
        key
    }
}

#[tokio::test]
async fn control_token_read_returns_the_row() {
    let probe = ingest_one_record().await;
    let token = probe.rows(true).await;
    let plain = probe.rows(false).await;
    println!("PROBE4573 control plain={plain} token={token}");
    assert_eq!(token, 1, "token read must return the ingested row");
    drop(probe.running);
}

#[tokio::test]
async fn token_read_over_a_tombstoned_bucket_returns_the_retired_row() {
    let probe = ingest_one_record().await;
    let tombstone_key = probe.publish_tombstone().await;
    println!(
        "PROBE4573 tombstone key={tombstone_key} shard={} hour={}",
        probe.commit_key.shard, probe.commit_key.ingest_hour_bucket
    );

    let plain = probe.rows(false).await;
    let token = probe.rows(true).await;
    println!("PROBE4573 plain={plain} token={token}");

    assert_eq!(
        plain, 0,
        "a tombstoned bucket is excluded from a plain read"
    );
    assert_eq!(token, 1, "the token read returns the retired row");
    assert_ne!(plain, token, "token and plain reads disagree (#4573)");
    drop(probe.running);
}
