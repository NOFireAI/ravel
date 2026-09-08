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
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantHash, TenantId};
use uuid::Uuid;

const TOKEN: &str = "acme-token";
const TENANT: &str = "acme";
/// A second tenant, for the per-tenant audit-routing tests. Distinct token and
/// distinct tenant id, so the two resolve to different `TenantHash`es.
const TOKEN_B: &str = "beta-token";
const TENANT_B: &str = "beta";
/// The audit tokenization key the redaction tests run under, in the hex
/// spelling `RAVEL_AUDIT_TOKEN_KEY` takes: 32 bytes of `0x01`. A fixed key
/// makes every token below a constant, so a pinned `query.text` catches a
/// changed token shape or a literal position that stopped being redacted.
const AUDIT_TOKEN_KEY_HEX: &str =
    "0101010101010101010101010101010101010101010101010101010101010101";
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
    publish_segment_for(store, TENANT, metric, samples).await
}

/// [`publish_segment`] for an arbitrary tenant name, so a multi-tenant test can
/// give each of its tenants real data to query.
async fn publish_segment_for(
    store: &dyn ObjectStoreBackend,
    tenant_name: &str,
    metric: &str,
    samples: &[(i64, f64)],
) {
    let tenant = TenantId::new(tenant_name);
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
    fold_tenants: Vec<TenantHash>,
    audit_text: ravel_maintain::AuditTextPolicy,
) -> ServerConfig {
    ServerConfig {
        audit_pipeline,
        audit_text,
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
        fold_tenants,
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
    start_server_with(
        store,
        mode,
        audit_pipeline,
        &[(TOKEN, TENANT)],
        vec![TenantId::new(TENANT).hash()],
        Default::default(),
    )
    .await
}

/// The `--audit-text redacted` posture under a fixed tokenization key,
/// resolved through the same `config` entry point `main` uses, so the tokens
/// the redaction tests pin are constants rather than values discovered at run
/// time.
fn redacted_text_policy() -> ravel_maintain::AuditTextPolicy {
    ravel_server::config::resolve_audit_text_policy(
        ravel_server::config::AuditTextArg::Redacted,
        Some(AUDIT_TOKEN_KEY_HEX),
        None,
    )
    .expect("an explicit key resolves the redacted posture")
}

/// The `--audit-text plaintext` posture, resolved through the same entry point
/// as [`redacted_text_policy`] so the flag-to-posture mapping is under test
/// rather than assumed.
fn plaintext_text_policy() -> ravel_maintain::AuditTextPolicy {
    ravel_server::config::resolve_audit_text_policy(
        ravel_server::config::AuditTextArg::Plaintext,
        None,
        None,
    )
    .expect("the plaintext posture resolves without a key")
}

/// [`start_server`] with an explicit bearer-token table and `fold_tenants`
/// list, so a test can serve several tenants and can start a server with no
/// static tenant list at all (the OIDC/mTLS deployment shape).
async fn start_server_with(
    store: Arc<dyn ObjectStoreBackend>,
    mode: Mode,
    audit_pipeline: ravel_maintain::AuditPipelineConfig,
    token_pairs: &[(&str, &str)],
    fold_tenants: Vec<TenantHash>,
    audit_text: ravel_maintain::AuditTextPolicy,
) -> ravel_server::Running {
    let mut tokens = HashMap::new();
    for (token, tenant) in token_pairs {
        tokens.insert((*token).to_string(), TenantId::new(*tenant));
    }
    ravel_server::start(
        server_config(tokens, mode, audit_pipeline, fold_tenants, audit_text),
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

/// Every L0 data-object key on any tenant's [`Signal::Audit`] prefix. The
/// signal prefix for `Signal::Audit` is `u`, so `t/<hex>/u/l0/` selects audit
/// data objects and excludes their commit records and every other signal.
#[cfg(feature = "sql")]
async fn audit_data_object_keys(store: &dyn ObjectStoreBackend) -> Vec<String> {
    list_all(store, "t/")
        .await
        .expect("list every tenant prefix")
        .into_iter()
        .map(|meta| meta.key)
        .filter(|key| key.contains("/u/l0/"))
        .collect()
}

/// Runs one SQL statement as `token` over an explicit event-time window (Unix
/// seconds) and returns the response status and decoded JSON body.
#[cfg(feature = "sql")]
async fn run_sql(
    client: &reqwest::Client,
    base: &str,
    token: &str,
    sql: &str,
    start_s: i64,
    end_s: i64,
) -> (reqwest::StatusCode, serde_json::Value) {
    let response = client
        .post(format!("{base}/api/v1/sql"))
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(
            serde_json::json!({
                "query": sql,
                "start": start_s as f64,
                "end": end_s as f64,
            })
            .to_string(),
        )
        .send()
        .await
        .expect("sql request sent");
    let status = response.status();
    let value: serde_json::Value = response.json().await.expect("sql response is JSON");
    (status, value)
}

/// The wall-clock second the server's own clock is reading. `ravel_server::start`
/// takes no injected clock, so a query-audit record it writes is stamped with
/// real time; an `audit` statement therefore needs a window around real now
/// rather than around the 1970 timestamps the sample segments use.
#[cfg(feature = "sql")]
fn now_seconds() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock is after the unix epoch")
        .as_secs() as i64
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

/// The shared body of the two per-tenant routing tests: two tenants each run
/// one SQL statement through one real server, and each reads its own
/// query-audit trail back and only its own.
///
/// One pipeline serves both tenants, so this is the assertion that the
/// pipeline routes per event rather than per construction. Every count is
/// exact: "at least one row" would pass on a store holding both tenants'
/// records under one prefix, which is the disclosure being ruled out.
#[cfg(feature = "sql")]
async fn two_tenants_each_read_only_their_own_audit(fold_tenants: Vec<TenantHash>) {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    publish_segment_for(store.as_ref(), TENANT, "m", &[(100, 1.0), (200, 2.5)]).await;
    publish_segment_for(store.as_ref(), TENANT_B, "m", &[(300, 3.5)]).await;
    let running = start_server_with(
        store.clone(),
        Mode::All,
        Default::default(),
        &[(TOKEN, TENANT), (TOKEN_B, TENANT_B)],
        fold_tenants,
        Default::default(),
    )
    .await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let hash_a = TenantId::new(TENANT).hash();
    let hash_b = TenantId::new(TENANT_B).hash();
    assert_ne!(hash_a, hash_b, "the two tenants must hash differently");

    // One statement each. The texts differ so a mis-attributed record is
    // identifiable, not just miscounted.
    let (status, value) = run_sql(
        &client,
        &base,
        TOKEN,
        "SELECT ts, value FROM samples ORDER BY ts",
        0,
        NOW_S,
    )
    .await;
    assert_eq!(status, 200, "tenant a's statement should succeed: {value}");
    let (status, value) = run_sql(
        &client,
        &base,
        TOKEN_B,
        "SELECT value FROM samples ORDER BY ts",
        0,
        NOW_S,
    )
    .await;
    assert_eq!(status, 200, "tenant b's statement should succeed: {value}");

    // The store now holds exactly one audit data object per tenant, each under
    // that tenant's own prefix and nowhere else. Asserted before either audit
    // statement runs, since those add a record of their own.
    let keys = audit_data_object_keys(store.as_ref()).await;
    let prefix_a = format!("t/{}/u/l0/", hash_a.to_hex());
    let prefix_b = format!("t/{}/u/l0/", hash_b.to_hex());
    let under_a = keys.iter().filter(|key| key.starts_with(&prefix_a)).count();
    let under_b = keys.iter().filter(|key| key.starts_with(&prefix_b)).count();
    assert_eq!(
        under_a, 1,
        "exactly one audit object under {prefix_a}: {keys:?}"
    );
    assert_eq!(
        under_b, 1,
        "exactly one audit object under {prefix_b}: {keys:?}"
    );
    assert_eq!(
        keys.len(),
        2,
        "two audit objects in total, so none landed under a third prefix \
         (an all-zero hash, or one tenant's prefix holding both): {keys:?}"
    );

    // Each tenant reads its own trail: one row, its own hash, never the
    // other's. A window around real time, because `start` takes no injected
    // clock.
    let now_s = now_seconds();
    let audit_sql = "SELECT attrs['query.tenant'] FROM audit WHERE attrs['kind'] = 'query'";
    for (token, own, other) in [(TOKEN, hash_a, hash_b), (TOKEN_B, hash_b, hash_a)] {
        let (status, value) = run_sql(
            &client,
            &base,
            token,
            audit_sql,
            now_s - 3_600,
            now_s + 3_600,
        )
        .await;
        assert_eq!(status, 200, "the audit statement should succeed: {value}");
        let rows = value["data"]["rows"].as_array().expect("rows");
        assert_eq!(
            rows.len(),
            1,
            "exactly the one statement this tenant ran: {value}"
        );
        assert_eq!(
            rows[0][0],
            serde_json::json!(own.to_hex()),
            "the row must carry this tenant's own hash: {value}"
        );
        assert_ne!(
            rows[0][0],
            serde_json::json!(other.to_hex()),
            "the row must never carry the other tenant's hash: {value}"
        );
    }

    running.shutdown().await.expect("graceful shutdown");
}

/// Two tenants, one process-wide pipeline: each tenant's query-audit record is
/// written under its own audit prefix, so each reads exactly its own statement
/// back through `audit` and never the other's.
#[cfg(feature = "sql")]
#[tokio::test]
async fn two_tenants_read_only_their_own_query_audit() {
    two_tenants_each_read_only_their_own_audit(vec![TenantId::new(TENANT).hash()]).await;
}

/// The same guarantee with no `--fold-tenant` list at all, which is the
/// OIDC/mTLS deployment shape: the pipeline has no static tenant list to fall
/// back on, and must still route each record by the tenant its request
/// resolved to.
#[cfg(feature = "sql")]
#[tokio::test]
async fn empty_fold_tenants_still_routes_audit_per_tenant() {
    two_tenants_each_read_only_their_own_audit(Vec::new()).await;
}

/// [`start_server`] for `TENANT` alone under an explicit `--audit-text`
/// posture, which is the only variable the three redaction tests below change.
async fn start_server_text(
    store: Arc<dyn ObjectStoreBackend>,
    audit_text: ravel_maintain::AuditTextPolicy,
) -> ravel_server::Running {
    start_server_with(
        store,
        Mode::All,
        Default::default(),
        &[(TOKEN, TENANT)],
        vec![TenantId::new(TENANT).hash()],
        audit_text,
    )
    .await
}

/// Runs one PromQL instant query as `TOKEN` and returns the response status.
async fn run_promql(client: &reqwest::Client, base: &str, query: &str) -> reqwest::StatusCode {
    client
        .get(format!("{base}/api/v1/query"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .query(&[("query", query), ("time", "0")])
        .send()
        .await
        .expect("query request sent")
        .status()
}

/// The single `query.text` attribute of the single `kind=query` record
/// `TENANT`'s audit shard holds. Both counts are exact: a second record, or a
/// record without the attribute, is a failure rather than a silent `None`.
async fn only_audit_query_text(store: &dyn ObjectStoreBackend) -> String {
    let records = query_audit_records(store, &TenantId::new(TENANT)).await;
    assert_eq!(records.len(), 1, "exactly one query-audit record");
    attr(&records[0], "query.text")
        .expect("the record carries a string query.text")
        .to_string()
}

/// `--audit-text redacted`: the `query.text` stored for a SQL statement is the
/// structure-preserving keyed tokenization, pinned exactly. Table name, column
/// names, the function name, the operator, and `ORDER BY` stay readable; both
/// string literals become tokens; neither literal appears anywhere in the
/// record.
#[cfg(feature = "sql")]
#[tokio::test]
async fn redacted_mode_tokenizes_sql_string_literals_and_keeps_structure() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    publish_segment(store.as_ref(), "m", &[(100, 1.0), (200, 2.5)]).await;
    let running = start_server_text(store.clone(), redacted_text_policy()).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let sql = "SELECT ts, value FROM samples WHERE label(labels, '__name__') = 'm' ORDER BY ts";
    let (status, value) = run_sql(&client, &base, TOKEN, sql, 0, NOW_S).await;
    assert_eq!(status, 200, "the sql query should succeed: {value}");

    let text = only_audit_query_text(store.as_ref()).await;
    assert_eq!(
        text,
        "SELECT ts, value FROM samples WHERE label(labels, 'tok_0d2d088dbe1acaea') \
         = 'tok_784cf4b979581a8d' ORDER BY ts",
        "the stored text must be the tokenized statement"
    );

    running.shutdown().await.expect("graceful shutdown");
}

/// `--audit-text redacted`: the `query.text` stored for a PromQL expression
/// tokenizes the label-matcher value and leaves the metric name, the matcher's
/// label name, and the operator readable.
#[tokio::test]
async fn redacted_mode_tokenizes_promql_matcher_values() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    publish_segment(store.as_ref(), "m", &[(100, 1.0), (200, 2.5)]).await;
    let running = start_server_text(store.clone(), redacted_text_policy()).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let status = run_promql(&client, &base, "m{job=\"checkout\"}").await;
    assert_eq!(status, 200, "the promql query should succeed");

    let text = only_audit_query_text(store.as_ref()).await;
    assert_eq!(
        text, "m{job=\"tok_f43bf8c54925b9cc\"}",
        "the stored text must be the tokenized expression"
    );

    running.shutdown().await.expect("graceful shutdown");
}

/// `--audit-text plaintext`: the opt-in posture stores the expression the
/// caller sent, byte for byte, with no token anywhere in it.
#[tokio::test]
async fn plaintext_mode_writes_verbatim() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    publish_segment(store.as_ref(), "m", &[(100, 1.0), (200, 2.5)]).await;
    let running = start_server_text(store.clone(), plaintext_text_policy()).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let query = "m{job=\"checkout\"}";
    let status = run_promql(&client, &base, query).await;
    assert_eq!(status, 200, "the promql query should succeed");

    let text = only_audit_query_text(store.as_ref()).await;
    assert_eq!(text, query, "the stored text must be verbatim");
    assert!(
        !text.contains("tok_"),
        "the plaintext posture must introduce no token: {text}"
    );

    running.shutdown().await.expect("graceful shutdown");
}
