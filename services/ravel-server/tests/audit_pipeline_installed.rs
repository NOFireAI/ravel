//! Reachability proof for the query-audit pipeline installed into the
//! shipping `ravel-server` binary's start path (issue #1378): a real
//! [`ravel_server::start`] server, not a hand-built router, spawns one
//! `ravel_maintain::AuditPipeline` in every query-serving mode and installs
//! its sink on every query surface (SQL, PromQL, and -- by construction,
//! since `Maintain`/`Gateway` mount none of them -- neither).
//!
//! The SQL test lives behind the `sql` feature; every other test uses PromQL,
//! which is always compiled in, so the file builds and this suite's other
//! four tests run under both the default feature set and `--features sql`.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;

use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::{AttrValue, LogRecord, Predicate, RlogConfig, RlogReader};
use ravel_maintain::QUERY_AUDIT_SHARD;
use ravel_object_store::fault::{FaultPlan, FaultStore, Op, Rule, ScriptedFault};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, PutOptions, list_all};
use ravel_segment::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantId};
use uuid::Uuid;

const TOKEN: &str = "acme-token";
const TENANT: &str = "acme";
const NS_PER_HOUR: i64 = 3_600_000_000_000;
/// Small so `Catalog::resolve` issues few LISTs, matching `sql_endpoint.rs`
/// and `otlp_trace_export_e2e.rs`.
const NOW_NS: i64 = 4 * NS_PER_HOUR;
/// `/api/v1/query_range`'s `start`/`end` are Unix seconds, not nanoseconds.
const NOW_S: i64 = NOW_NS / 1_000_000_000;

/// One real segment plus its commit record for `TENANT` (mirrors
/// `otlp_trace_export_e2e.rs::publish_segment`), so a query against it
/// returns a clean 200 through the real router.
async fn publish_segment(store: &dyn ObjectStoreBackend, metric: &str, samples: &[(i64, f64)]) {
    let tenant = TenantId::new(TENANT);
    let tenant_hash = tenant.hash();
    let label_set = LabelSet::new(vec![Label {
        name: "__name__".to_string(),
        value: metric.to_string(),
    }])
    .expect("labels");
    let series = vec![SeriesInput {
        series_id: SeriesId::compute(&tenant, metric, &label_set).expect("series id"),
        labels: label_set,
        samples: samples
            .iter()
            .map(|(ts_ns, value)| Sample {
                ts_ns: *ts_ns,
                value: *value,
            })
            .collect(),
    }];
    let writer_id = Uuid::from_u128(2_000);
    let identity = SegmentIdentity {
        tenant_hash: tenant_hash.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    };
    let written = SegmentWriter::write(
        series,
        identity,
        IngestBounds {
            min_ingest_ts_ns: 0,
            max_ingest_ts_ns: 0,
        },
    )
    .expect("write segment");
    let rec = record::build(NewCommitRecord {
        tenant_hash,
        signal: Signal::Metrics,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: written.summary.min_event_ts_ns,
        max_ingest_ts_ns: written.summary.max_event_ts_ns,
        segment_format_version: 1,
        created_unix_ns: 10,
        ingest_hour_bucket: 0,
    })
    .expect("commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    store
        .put(&data_key, written.bytes, PutOptions::default())
        .await
        .expect("put data");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish");
}

fn server_config(
    tokens: HashMap<String, TenantId>,
    mode: Mode,
    audit_pipeline: ravel_maintain::AuditPipelineConfig,
) -> ServerConfig {
    ServerConfig {
        audit_pipeline,
        audit_text: Default::default(),
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        adaptive_flush_delay: false,
        max_flush_delay: std::time::Duration::from_secs(2),
        max_flush_delay_idle: std::time::Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
        mode,
        listen_http: "127.0.0.1:0".parse().expect("valid loopback addr"),
        listen_grpc: "127.0.0.1:0".parse().expect("valid loopback addr"),
        shard_count: 1,
        tenant_resolver: ravel_server::tenant::build_resolver(tokens, false),
        mtls_listener: None,
        fold_tenants: vec![TenantId::new(TENANT).hash()],
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
        catalog_resolve_concurrency: None,
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: std::time::Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
    }
}

/// Starts a real server (the binary's own `ravel_server::start` path) over
/// `store`, in `mode`, with `audit_pipeline` as its resolved
/// `--audit-mode`/`--audit-max-batch`/`--audit-max-age` config.
async fn start_server(
    store: Arc<dyn ObjectStoreBackend>,
    mode: Mode,
    audit_pipeline: ravel_maintain::AuditPipelineConfig,
) -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    ravel_server::start(
        server_config(tokens, mode, audit_pipeline),
        store.clone(),
        store.clone(),
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts")
}

/// Every `kind=query` record written to `TENANT`'s query-audit shard
/// (mirrors `sql_endpoint.rs::query_audit_records`; integration test binaries
/// cannot share private helpers across files).
async fn query_audit_records(store: &dyn ObjectStoreBackend, tenant: &TenantId) -> Vec<LogRecord> {
    let tenant_hash = tenant.hash();
    let prefix = keys::commit_shard_prefix(&tenant_hash, Signal::Audit, QUERY_AUDIT_SHARD)
        .expect("audit commit prefix");
    let metas = list_all(store, &prefix).await.expect("list audit commits");
    let cfg = RlogConfig::default();
    let mut out = Vec::new();
    for meta in metas {
        let got = store
            .get(&meta.key, GetRange::Full)
            .await
            .expect("get commit");
        let commit = record::decode(&got.data).expect("decode commit");
        let data_key = keys::reconstruct_data_key(&commit).expect("data key");
        let object = store
            .get(&data_key, GetRange::Full)
            .await
            .expect("get data");
        let reader = RlogReader::new(object.data.as_ref(), &cfg).expect("rlog reader");
        let (rows, _stats) = reader.scan(&Predicate::And(Vec::new())).expect("scan");
        for row in rows {
            if attr(&row, "kind") == Some("query") {
                out.push(row);
            }
        }
    }
    out
}

/// The value of a string `attrs` entry, or `None` if absent or non-string.
fn attr<'a>(row: &'a LogRecord, key: &str) -> Option<&'a str> {
    row.attrs
        .iter()
        .find(|(k, _)| k == key)
        .and_then(|(_, v)| match v {
            AttrValue::Str(s) => Some(s.as_str()),
            _ => None,
        })
}

/// `POST /api/v1/sql` against a real started server writes exactly one
/// `kind=query` audit record with `query.language=sql`.
#[cfg(feature = "sql")]
#[tokio::test]
async fn sql_query_writes_one_query_audit_record() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    publish_segment(store.as_ref(), "m", &[(100, 1.0), (200, 2.5)]).await;
    let running = start_server(store.clone(), Mode::All, Default::default()).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let sql = "SELECT ts, value FROM samples ORDER BY ts";
    let response = client
        .post(format!("{base}/api/v1/sql"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .header("content-type", "application/json")
        .body(
            serde_json::json!({
                "query": sql,
                "start": 0.0,
                "end": NOW_NS as f64 / 1_000_000_000.0,
            })
            .to_string(),
        )
        .send()
        .await
        .expect("sql request sent");
    assert_eq!(response.status(), 200, "the sql query should succeed");

    let tenant = TenantId::new(TENANT);
    let records = query_audit_records(store.as_ref(), &tenant).await;
    assert_eq!(records.len(), 1, "exactly one query-audit record");
    let row = &records[0];
    assert_eq!(attr(row, "query.language"), Some("sql"));
    assert_eq!(attr(row, "query.status"), Some("ok"));
    assert_eq!(
        attr(row, "query.tenant"),
        Some(tenant.hash().to_hex().as_str())
    );

    running.shutdown().await.expect("graceful shutdown");
}

/// `GET /api/v1/query_range` against a real started server writes exactly one
/// `kind=query` audit record with `query.language=promql`.
#[tokio::test]
async fn promql_query_writes_one_query_audit_record() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    publish_segment(store.as_ref(), "m", &[(100, 1.0), (200, 2.5)]).await;
    let running = start_server(store.clone(), Mode::All, Default::default()).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let response = client
        .get(format!("{base}/api/v1/query_range"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .query(&[
            ("query", "m".to_string()),
            ("start", "0".to_string()),
            ("end", NOW_S.to_string()),
            ("step", "60s".to_string()),
        ])
        .send()
        .await
        .expect("query_range request sent");
    assert_eq!(response.status(), 200, "the promql query should succeed");

    let tenant = TenantId::new(TENANT);
    let records = query_audit_records(store.as_ref(), &tenant).await;
    assert_eq!(records.len(), 1, "exactly one query-audit record");
    let row = &records[0];
    assert_eq!(attr(row, "query.language"), Some("promql"));
    assert_eq!(attr(row, "query.status"), Some("ok"));
    assert_eq!(
        attr(row, "query.tenant"),
        Some(tenant.hash().to_hex().as_str())
    );

    running.shutdown().await.expect("graceful shutdown");
}

/// `audit_mode=required` (the default): faulting every PUT into the audit
/// keyspace fails the query closed with 503, after usage was already
/// recorded (T2a order) -- the query does not outlive its trail.
#[tokio::test]
async fn required_mode_fails_closed_on_audit_write_fault() {
    let plan = FaultPlan::empty().with_rule(
        Rule::new(
            Op::Put,
            ScriptedFault::Permanent("audit store down".to_string()),
        )
        .with_key_contains("/u/"),
    );
    let store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let backend: Arc<dyn ObjectStoreBackend> = Arc::clone(&store) as Arc<dyn ObjectStoreBackend>;
    // Metric segments live under "/m/", so setup is unaffected by the fault.
    publish_segment(backend.as_ref(), "m", &[(100, 1.0), (200, 2.5)]).await;
    let running = start_server(backend.clone(), Mode::All, Default::default()).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let response = client
        .get(format!("{base}/api/v1/query_range"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .query(&[
            ("query", "m".to_string()),
            ("start", "0".to_string()),
            ("end", NOW_S.to_string()),
            ("step", "60s".to_string()),
        ])
        .send()
        .await
        .expect("query_range request sent");
    assert_eq!(
        response.status(),
        503,
        "a required-mode audit flush failure must fail the query closed"
    );
    let body = response.text().await.expect("response body readable");
    assert!(
        body.contains("query audit is temporarily unavailable"),
        "503 body must carry the audit-unavailable message, got: {body}"
    );

    assert_eq!(
        store.fault_count(Op::Put, ravel_object_store::fault::FaultKind::Permanent),
        1,
        "exactly one faulted audit PUT"
    );

    let metrics = client
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("metrics request sent")
        .text()
        .await
        .expect("metrics body readable");
    assert!(
        metrics.contains("ravel_query_queries_total"),
        "usage must be recorded even though the audit write failed"
    );

    running.shutdown().await.expect("graceful shutdown");
}

/// `audit_mode=best-effort`: the same fault logs and counts the failure
/// instead of failing the response.
#[tokio::test]
async fn best_effort_mode_serves_the_response_and_counts_the_failure() {
    let plan = FaultPlan::empty().with_rule(
        Rule::new(
            Op::Put,
            ScriptedFault::Permanent("audit store down".to_string()),
        )
        .with_key_contains("/u/"),
    );
    let store = Arc::new(FaultStore::new(MemoryStore::new(), plan));
    let backend: Arc<dyn ObjectStoreBackend> = Arc::clone(&store) as Arc<dyn ObjectStoreBackend>;
    publish_segment(backend.as_ref(), "m", &[(100, 1.0), (200, 2.5)]).await;
    let audit_pipeline = ravel_maintain::AuditPipelineConfig {
        audit_mode: ravel_maintain::AuditMode::BestEffort,
        ..Default::default()
    };
    let running = start_server(backend.clone(), Mode::All, audit_pipeline).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let response = client
        .get(format!("{base}/api/v1/query_range"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .query(&[
            ("query", "m".to_string()),
            ("start", "0".to_string()),
            ("end", NOW_S.to_string()),
            ("step", "60s".to_string()),
        ])
        .send()
        .await
        .expect("query_range request sent");
    assert_eq!(
        response.status(),
        200,
        "a best-effort-mode audit flush failure must not fail the query"
    );

    assert_eq!(
        store.fault_count(Op::Put, ravel_object_store::fault::FaultKind::Permanent),
        1,
        "exactly one faulted audit PUT"
    );
    assert_eq!(
        running.audit_write_failures(),
        1,
        "exactly one counted best-effort flush failure"
    );

    running.shutdown().await.expect("graceful shutdown");
}

/// `Mode::Maintain` and `Mode::Gateway` serve no query surface, so `start`
/// installs no `AuditPipeline` for them: a request either mode does serve
/// writes no object into the query-audit shard.
#[tokio::test]
async fn maintain_and_gateway_modes_install_no_pipeline() {
    for mode in [Mode::Maintain, Mode::Gateway] {
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let running = start_server(store.clone(), mode, Default::default()).await;
        let base = format!("http://{}", running.http_addr);
        let client = reqwest::Client::new();

        let response = client
            .get(format!("{base}/healthz"))
            .send()
            .await
            .expect("healthz request sent");
        assert_eq!(response.status(), 200, "mode {mode:?} must serve healthz");

        assert!(
            !running.has_audit_pipeline(),
            "mode {mode:?} must install no pipeline at all"
        );

        let tenant = TenantId::new(TENANT);
        let tenant_hash = tenant.hash();
        let prefix = keys::commit_shard_prefix(&tenant_hash, Signal::Audit, QUERY_AUDIT_SHARD)
            .expect("audit commit prefix");
        let metas = list_all(store.as_ref(), &prefix)
            .await
            .expect("list audit commits");
        assert_eq!(
            metas.len(),
            0,
            "mode {mode:?} must write zero audit objects"
        );

        running.shutdown().await.expect("graceful shutdown");
    }
}
