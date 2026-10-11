//! A real server with alert rules configured keeps evaluating them when the
//! tenant's alert history holds an object written before an RLOG format bump
//! (ADR-2708 decision D6, issue #2713).
//!
//! The object is a real alert transition object, encoded by the same
//! `ravel_alerting::encode_record_object` the evaluator writes with, published
//! through a real commit record, and then rewritten to a trailer version this
//! build does not read. The server is started over that store and scraped over
//! HTTP until its alert loop has completed an evaluated tick.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use ravel_alerting::{AlertState, build_transition_record, encode_record_object};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_logseg::{ObjectIdentity, RlogConfig};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions, StoreMetrics};
use ravel_server::alerting::{ALERT_SHARD, parse_rules};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{Signal, TenantId};
use uuid::Uuid;

const TOKEN: &str = "testtoken";
const TENANT: &str = "acme";
const NS_PER_HOUR: i64 = 3_600 * 1_000_000_000;

const ALERT_RULES_JSON: &str = r#"{
  "rules": [
    {
      "tenant": "acme",
      "rule_id": "pre-bump-probe",
      "promql": "up",
      "condition": { "type": "threshold", "op": "gt", "value": 0.9 }
    }
  ]
}"#;

/// The RLOG trailer version written before the bump this build reads past.
const PRE_BUMP_VERSION: u16 = 3;

/// Publish one firing transition for the configured rule as an alert object
/// whose trailer says [`PRE_BUMP_VERSION`], with its commit record, the way an
/// evaluator built before the bump would have left it.
async fn seed_pre_bump_alert_object(store: &dyn ObjectStoreBackend) {
    let tenant = TenantId::new(TENANT).hash();
    let rules = parse_rules(ALERT_RULES_JSON).expect("rules parse");
    let rule = &rules[&tenant][0];
    // A recent stamp, so no retention window can count the object as expired.
    let ts_ns = i64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos(),
    )
    .expect("now fits i64")
        - NS_PER_HOUR;
    let alert = build_transition_record(rule, AlertState::Firing, 1, ts_ns);

    let writer_id = Uuid::from_u128(2_713);
    let identity = ObjectIdentity {
        tenant_hash: tenant.0,
        shard: ALERT_SHARD,
        writer_id: writer_id.into_bytes(),
        writer_epoch: 1,
        writer_seq: 1,
    };
    let mut bytes =
        encode_record_object(&alert, RlogConfig::default(), identity).expect("encode alert");
    ravel_logseg::footer::rewrite_trailer_version(&mut bytes, PRE_BUMP_VERSION)
        .expect("rewrite version");

    let ingest_hour = u32::try_from(ts_ns.div_euclid(NS_PER_HOUR)).expect("hour fits u32");
    let commit = record::build(NewCommitRecord {
        tenant_hash: tenant,
        signal: Signal::Alerts,
        shard: ALERT_SHARD,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: bytes.len() as u64,
        content_hash: *blake3::hash(&bytes).as_bytes(),
        sample_count: 1,
        series_count: 1,
        min_event_ts_ns: ts_ns,
        max_event_ts_ns: ts_ns,
        min_ingest_ts_ns: ts_ns,
        max_ingest_ts_ns: ts_ns,
        segment_format_version: u32::from(PRE_BUMP_VERSION),
        created_unix_ns: ts_ns,
        ingest_hour_bucket: ingest_hour,
    })
    .expect("valid commit record");
    let data_key = keys::reconstruct_data_key(&commit).expect("data key");
    store
        .put(&data_key, bytes.into(), PutOptions::default())
        .await
        .expect("put data object");
    publish::publish(store, &commit, &RetryPolicy::default())
        .await
        .expect("publish commit record");
}

/// The shape of `shipped_rules_name_emitted_metrics.rs`'s server, with alert
/// rules on and a short evaluation interval so the first tick comes quickly.
async fn start_server(store: Arc<MemoryStore>) -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let alerting = ravel_server::AlertEvalConfig {
        enabled: true,
        interval: Duration::from_millis(100),
        rules: Arc::new(parse_rules(ALERT_RULES_JSON).expect("rules parse")),
        ..ravel_server::AlertEvalConfig::default()
    };
    let config = ServerConfig {
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        audit_pipeline: Default::default(),
        audit_text: Default::default(),
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        max_inflight_flushes_per_tenant: None,
        max_queued_flushes: 8,
        adaptive_flush_delay: false,
        max_flush_delay: Duration::from_secs(2),
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
        alerting,
        oidc_refresh: None,
        otap: false,
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
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
    };
    ravel_server::start(
        config,
        store.clone(),
        store,
        Arc::new(StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts")
}

async fn scrape(running: &ravel_server::Running) -> String {
    reqwest::Client::new()
        .get(format!("http://{}/metrics", running.http_addr))
        .send()
        .await
        .expect("metrics request completes")
        .text()
        .await
        .expect("metrics body is text")
}

/// The value of the one sample whose name and labels are exactly `series`,
/// or `None` when the body carries no such line.
fn sample(body: &str, series: &str) -> Option<f64> {
    body.lines().find_map(|line| {
        line.strip_prefix(series)
            .and_then(|rest| rest.strip_prefix(' '))
            .map(|value| value.parse().expect("numeric sample"))
    })
}

const EVALUATED: &str = r#"ravel_alert_ticks_total{mode="all",outcome="evaluated"}"#;
const HISTORY_UNAVAILABLE: &str =
    r#"ravel_alert_ticks_total{mode="all",outcome="history_unavailable"}"#;
const SKIPPED: &str =
    r#"ravel_alert_history_records_skipped_total{mode="all",reason="unsupported_version"}"#;

/// Watch it fail: delete the `UnsupportedVersion` arm in
/// `AlertEvaluator::fold_commit_entries`. Every tick then ends in
/// `history_unavailable`, the evaluated count stays 0 and the poll runs out.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pre_bump_alert_object_does_not_stop_rule_evaluation() {
    let store = Arc::new(MemoryStore::new());
    seed_pre_bump_alert_object(store.as_ref()).await;
    let running = start_server(Arc::clone(&store)).await;

    // Bounded on the number of scrapes, not on elapsed time: 600 polls 50 ms
    // apart give a 100 ms evaluation interval far more than one tick.
    let mut body = String::new();
    for _ in 0..600 {
        body = scrape(&running).await;
        if sample(&body, EVALUATED).is_some_and(|evaluated| evaluated >= 1.0) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let evaluated = sample(&body, EVALUATED).expect("evaluated tick series rendered");
    assert!(
        evaluated >= 1.0,
        "a tick evaluated the rules despite the pre-bump object:\n{body}"
    );
    let skipped = sample(&body, SKIPPED).expect("skip counter rendered");
    assert!(
        skipped >= 1.0,
        "the pre-bump object was skipped and counted:\n{body}"
    );
    assert_eq!(
        sample(&body, HISTORY_UNAVAILABLE),
        Some(0.0),
        "no tick aborted on the pre-bump object:\n{body}"
    );

    running.shutdown().await.expect("graceful shutdown");
}
