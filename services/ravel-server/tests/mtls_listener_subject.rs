//! `--mtls-subject-header` names the caller on the mTLS listener only
//! (ADR-3541 decision 3, ADR-0050 section 1).
//!
//! The resolver chain is built by the same `build_auth_resolver_with_principals`
//! `main` calls, so the mTLS resolver carries the subject header exactly as a
//! deployment that sets the flag does, and the public chain is whatever that
//! function puts there.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;

use ravel_commit::keys;
use ravel_commit::record;
use ravel_logseg::{AttrValue, LogRecord, Predicate, RlogConfig, RlogReader};
use ravel_maintain::QUERY_AUDIT_SHARD;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, list_all};
use ravel_server::config::AuthResolverSettings;
use ravel_server::tenant::OidcCapabilityClaims;
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_tenant_resolve::{MtlsResolver, Principal};
use ravel_types::{Signal, TenantId};

const TENANT: &str = "acme";
const PUB: &str = "PUB";
const SUBJECT_HEADER: &str = "x-ravel-client-cert-subject";
const SUBJECT: &str = "CN=alice,O=Acme";
const SUBJECT_KEY: &str = "query.principal.subject";
const SOURCE_KEY: &str = "query.principal.subject_source";

/// A `Mode::All` server with the query audit in required mode, a public
/// static token `PUB` for `acme` with no subject, and an mTLS listener whose
/// resolver reads `x-ravel-client-cert-cn` for the tenant and
/// [`SUBJECT_HEADER`] for the subject.
async fn start_server(store: Arc<dyn ObjectStoreBackend>) -> ravel_server::Running {
    let mut principals = HashMap::new();
    principals.insert(
        PUB.to_string(),
        Principal::tenant_only(TenantId::new(TENANT)),
    );
    let bundle = ravel_server::tenant::build_auth_resolver_with_principals(
        principals,
        false,
        AuthResolverSettings {
            oidc: None,
            mtls_header: Some(MtlsResolver::DEFAULT_HEADER.to_string()),
            mtls_subject_header: Some(SUBJECT_HEADER.to_string()),
        },
        OidcCapabilityClaims::default(),
    )
    .expect("the resolver chain builds");
    let mtls_resolver = bundle
        .mtls_resolver
        .expect("--mtls-enabled builds an mTLS resolver");

    let config = ServerConfig {
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        audit_pipeline: Some(ravel_maintain::AuditPipelineConfig {
            audit_mode: ravel_maintain::AuditMode::Required,
            ..Default::default()
        }),
        audit_text: Default::default(),
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        max_inflight_flushes_per_tenant: None,
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
        tenant_resolver: bundle.resolver,
        mtls_listener: Some(ravel_server::MtlsListenerConfig {
            addr: "127.0.0.1:0".parse().expect("valid loopback addr"),
            resolver: mtls_resolver,
        }),
        fold_tenants: vec![TenantId::new(TENANT).hash()],
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

/// Every `kind=query` record written to `tenant`'s query-audit shard (mirrors
/// `audit_pipeline_installed.rs::query_audit_records`; integration test
/// binaries cannot share private helpers across files).
async fn query_audit_records(store: &dyn ObjectStoreBackend, tenant: &TenantId) -> Vec<LogRecord> {
    let prefix = keys::commit_shard_prefix(&tenant.hash(), Signal::Audit, QUERY_AUDIT_SHARD)
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
        out.extend(
            rows.into_iter()
                .filter(|row| attr(row, "kind") == Some("query")),
        );
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

/// Whether `row` carries `key` at all, of any value type.
fn has_attr(row: &LogRecord, key: &str) -> bool {
    row.attrs.iter().any(|(k, _)| k == key)
}

/// The query on the mTLS listener records the subject the proxy forwarded,
/// with source `mtls`. The same two headers on the public listener, under a
/// valid token, write a record with no subject, because no public resolver
/// reads the subject header; and with no token they authenticate nothing, so
/// the request is refused 401 and writes no record.
#[tokio::test]
async fn a_query_on_the_mtls_listener_records_the_proxy_set_subject_and_the_public_listener_ignores_the_header()
 {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_server(store.clone()).await;
    let tenant = TenantId::new(TENANT);

    let client = reqwest::Client::new();

    // The mTLS listener's own socket, carrying the two headers the
    // TLS-terminating proxy forwards.
    let mtls_addr = running.mtls_addr.expect("an mTLS listener was configured");
    let response = client
        .get(format!("http://{mtls_addr}/api/v1/query"))
        .header(MtlsResolver::DEFAULT_HEADER, TENANT)
        .header(SUBJECT_HEADER, SUBJECT)
        .query(&[("query", "m"), ("time", "0")])
        .send()
        .await
        .expect("request sent");
    let status = response.status();
    let body = response.text().await.expect("body readable");
    assert_eq!(
        status, 200,
        "the proxy headers authenticate on the mTLS listener: {body}"
    );

    let records = query_audit_records(store.as_ref(), &tenant).await;
    assert_eq!(records.len(), 1, "the mTLS query writes one record");
    assert_eq!(attr(&records[0], "query.language"), Some("promql"));
    assert_eq!(attr(&records[0], SUBJECT_KEY), Some(SUBJECT));
    assert_eq!(attr(&records[0], SOURCE_KEY), Some("mtls"));

    // The public listener, as PUB, with both proxy headers attached.
    let base = format!("http://{}", running.http_addr);
    let query = || {
        client
            .get(format!("{base}/api/v1/query"))
            .header(MtlsResolver::DEFAULT_HEADER, TENANT)
            .header(SUBJECT_HEADER, SUBJECT)
            .query(&[("query", "m"), ("time", "0")])
    };
    let response = query()
        .header("authorization", format!("Bearer {PUB}"))
        .send()
        .await
        .expect("request sent");
    let status = response.status();
    let body = response.text().await.expect("body readable");
    assert_eq!(
        status, 200,
        "PUB authenticates on the public listener: {body}"
    );
    let records = query_audit_records(store.as_ref(), &tenant).await;
    assert_eq!(records.len(), 2, "the public query writes one record");
    let public = records
        .iter()
        .find(|row| attr(row, SUBJECT_KEY) != Some(SUBJECT))
        .expect("one record is not the mTLS one");
    assert_eq!(attr(public, "query.language"), Some("promql"));
    assert!(
        !has_attr(public, SUBJECT_KEY),
        "the public record names no subject"
    );
    assert!(
        !has_attr(public, SOURCE_KEY),
        "the public record names no subject source"
    );

    // The same headers and no credential: nothing on the public chain reads
    // the mTLS headers, so the request is refused and audits nothing.
    let response = query().send().await.expect("request sent");
    assert_eq!(response.status(), 401);

    let records = query_audit_records(store.as_ref(), &tenant).await;
    assert_eq!(records.len(), 2, "exactly the two authenticated queries");
    assert_eq!(
        records
            .iter()
            .filter(|row| attr(row, SUBJECT_KEY) == Some(SUBJECT))
            .count(),
        1,
        "only the mTLS record names the subject"
    );

    running.shutdown().await.expect("graceful shutdown");
}
