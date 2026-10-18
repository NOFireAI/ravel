//! `GET /api/v1/rules` and `GET /api/v1/alerts` on a started server (issues
//! #1711 and #2665).
//!
//! `alerts_api`'s own unit tests drive the router directly. This file covers
//! what those cannot: that both routes are mounted on the shipping HTTP
//! listener, that they serve the rule set `ServerConfig::alerting` carries and
//! the state a real evaluator tick published into the server's own
//! [`ravel_server::Running::published_alerts`] handle, that they are scoped by
//! the listener's resolver over a real socket, and that they are mounted where
//! the alert evaluator runs (`all`/`query`) rather than in every mode.
//!
//! Outside one paused-time test that lets the server's own evaluator loop tick
//! on a short interval, that loop never ticks here ([`EVAL_INTERVAL`]). Each
//! other tick is driven by an `AlertEvaluator` the test builds over the
//! server's store and points at the server's handle, on a frozen clock, so
//! what the routes serve is exactly what that one tick observed.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use ravel_alerting::{AlertState, Rule, build_transition_record, encode_record_object};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_commit::publish::RetryPolicy;
use ravel_commit::record::NewCommitRecord;
use ravel_commit::{keys, publish, record};
use ravel_ingest::Clock;
use ravel_logseg::{ObjectIdentity, RlogConfig};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{EngineConfig, QueryEngine};
use ravel_server::alerting::{
    ALERT_SHARD, AlertEvaluator, AlertQueryEngines, AlertTickOutcome, PublishedAlerts, parse_rules,
};
use ravel_server::alerts_api::{NOT_EVALUATING, NOT_YET_EVALUATED};
use ravel_server::{AlertEvalConfig, FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{Signal, TenantId};
use serde_json::{Value, json};
use uuid::Uuid;

/// The tenant that has rules, and one that authenticates with no rules of its
/// own. The second is what makes a handler that ignores the resolved tenant
/// fail here rather than pass.
const ACME_TOKEN: &str = "acme-token";
const OTHER_TOKEN: &str = "other-token";

/// A long enough interval that the server's own evaluator never ticks during
/// a test, so every published snapshot comes from a tick the test drove. It is
/// also the `interval` the group renders.
const EVAL_INTERVAL: Duration = Duration::from_secs(3600);

const NS_PER_SEC: i64 = 1_000_000_000;
/// The frozen clock: half an hour past the epoch, inside the first ingest
/// hour, where the seeded alert record's commit lives.
const NOW_NS: i64 = 30 * 60 * NS_PER_SEC;
/// `NOW_NS` as the routes render it.
const NOW_RFC3339: &str = "1970-01-01T00:30:00.000000000Z";

const RULES: &str = r#"{
  "rules": [
    {
      "tenant": "acme",
      "rule_id": "cpu-hot",
      "promql": "max by (instance) (cpu_usage)",
      "condition": {"type": "threshold", "op": "gt", "value": 0.9},
      "for": "5m",
      "labels": {"severity": "page"},
      "annotations": {"summary": "CPU over 90% for five minutes"}
    }
  ]
}"#;

/// One acme rule per outcome a tick can leave a rule in, plus a rule for the
/// second tenant. `vector(N)` needs no ingested data, so each rule's outcome is
/// decided by its threshold and `for` alone:
///
/// - `fires` matches with no `for`, so it fires on the first tick;
/// - `waits` matches with a one-hour `for`, so it is pending after one tick;
/// - `quiet` does not match, so it stays inactive;
/// - `silenced` matches, but its alert's latest record is Suppressed, which
///   the state machine keeps;
/// - `broken` is not valid PromQL, so its query fails at evaluation time.
const TICK_RULES: &str = r#"{
  "rules": [
    {
      "tenant": "acme",
      "rule_id": "fires",
      "promql": "vector(2)",
      "condition": {"type": "threshold", "op": "gt", "value": 1},
      "labels": {"severity": "page"},
      "annotations": {"summary": "fires on the first tick"}
    },
    {
      "tenant": "acme",
      "rule_id": "waits",
      "promql": "vector(3)",
      "condition": {"type": "threshold", "op": "gt", "value": 1},
      "for": "1h",
      "labels": {"severity": "ticket"}
    },
    {
      "tenant": "acme",
      "rule_id": "quiet",
      "promql": "vector(0)",
      "condition": {"type": "threshold", "op": "gt", "value": 1}
    },
    {
      "tenant": "acme",
      "rule_id": "silenced",
      "promql": "vector(4)",
      "condition": {"type": "threshold", "op": "gt", "value": 1}
    },
    {
      "tenant": "acme",
      "rule_id": "broken",
      "promql": "vector(",
      "condition": {"type": "threshold", "op": "gt", "value": 1}
    },
    {
      "tenant": "other",
      "rule_id": "elsewhere",
      "promql": "vector(0)",
      "condition": {"type": "threshold", "op": "gt", "value": 1}
    }
  ]
}"#;

/// A clock that never moves: a tick reads `NOW_NS` everywhere it reads time.
struct FrozenClock(AtomicI64);

impl Clock for FrozenClock {
    fn now_ns(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

fn alerting_config(rules: &str, enabled: bool) -> AlertEvalConfig {
    AlertEvalConfig {
        enabled,
        interval: EVAL_INTERVAL,
        rules: Arc::new(parse_rules(rules).expect("valid rules")),
        ..AlertEvalConfig::default()
    }
}

async fn start_test_server(
    mode: Mode,
    alerting: AlertEvalConfig,
) -> (ravel_server::Running, Arc<MemoryStore>) {
    let tokens = HashMap::from([
        (ACME_TOKEN.to_string(), TenantId::new("acme")),
        (OTHER_TOKEN.to_string(), TenantId::new("other")),
    ]);
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let store = Arc::new(MemoryStore::new());
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
        mode,
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
        // The shape `main.rs` builds: the same map handed to both the
        // evaluator and the routes.
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

/// One GET over the real socket, returning the status and the parsed body
/// (`Value::Null` when the body is not JSON, as a 404's empty body is not).
async fn get(base: &str, path: &str, token: Option<&str>) -> (u16, Value) {
    let client = reqwest::Client::new();
    let mut request = client.get(format!("{base}{path}"));
    if let Some(token) = token {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    let response = request.send().await.expect("request completes");
    let status = response.status().as_u16();
    let bytes = response.bytes().await.expect("read body");
    let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, body)
}

/// An evaluator for `tenant` over `store`, on the frozen clock, publishing
/// into `published`: what `alerting::spawn` builds, minus the timer.
fn evaluator(
    store: &Arc<MemoryStore>,
    config: &AlertEvalConfig,
    tenant: &str,
    published: PublishedAlerts,
) -> AlertEvaluator {
    let store: Arc<dyn ObjectStoreBackend> = store.clone();
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&store), CatalogConfig::default()).expect("catalog"));
    let engine = QueryEngine::new(catalog, Arc::clone(&store), EngineConfig::default());
    let tenant = TenantId::new(tenant).hash();
    let rules = config.rules.get(&tenant).expect("tenant has rules").clone();
    AlertEvaluator::new(
        store,
        AlertQueryEngines {
            promql: Arc::new(engine),
            #[cfg(feature = "sql")]
            sql: None,
        },
        Arc::new(FrozenClock(AtomicI64::new(NOW_NS))),
        tenant,
        rules,
        config,
    )
    .expect("evaluator")
    .with_published(published)
}

/// Write a durable Suppressed record for `rule`'s alert one minute before
/// `NOW_NS`, through a real commit record, so the tick's history fold reads
/// it as the alert's latest state. This is the state a silence leaves.
async fn seed_suppressed(store: &MemoryStore, tenant: &TenantId, rule: &Rule) {
    let tenant_hash = tenant.hash();
    let ts_ns = NOW_NS - 60 * NS_PER_SEC;
    let writer_id = Uuid::from_u128(0x2665);
    let alert = build_transition_record(rule, AlertState::Suppressed, 1, ts_ns);
    let identity = ObjectIdentity {
        tenant_hash: tenant_hash.0,
        shard: ALERT_SHARD,
        writer_id: writer_id.into_bytes(),
        writer_epoch: 1,
        writer_seq: 1,
    };
    let bytes = encode_record_object(&alert, RlogConfig::default(), identity).expect("encode");
    let rec = record::build(NewCommitRecord {
        tenant_hash,
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
        segment_format_version: u32::from(ravel_ingest::LOG_SEGMENT_FORMAT_VERSION),
        created_unix_ns: ts_ns,
        ingest_hour_bucket: 0,
    })
    .expect("valid alert commit record");
    let data_key = keys::reconstruct_data_key(&rec).expect("data key");
    store
        .put(&data_key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put alert data object");
    publish::publish(store, &rec, &RetryPolicy::default())
        .await
        .expect("publish alert commit");
}

/// Start a server over [`TICK_RULES`], seed the silenced alert, and run one
/// tick for `other` and then one for `acme`, both publishing into the server's
/// handle. `other` ticks first so a route that served whichever snapshot was
/// published last, rather than the caller's, would hand `other` acme's alerts.
async fn server_after_one_tick() -> ravel_server::Running {
    let config = alerting_config(TICK_RULES, true);
    let (running, store) = start_test_server(Mode::All, config.clone()).await;
    let acme = TenantId::new("acme");
    let silenced = config.rules[&acme.hash()]
        .iter()
        .find(|rule| rule.rule_id == "silenced")
        .expect("silenced rule")
        .clone();
    seed_suppressed(&store, &acme, &silenced).await;

    let published = running.published_alerts.clone();
    let report = evaluator(&store, &config, "other", published.clone())
        .run_tick()
        .await;
    assert_eq!(report.outcome(), AlertTickOutcome::Evaluated);
    let report = evaluator(&store, &config, "acme", published)
        .run_tick()
        .await;
    assert_eq!(report.outcome(), AlertTickOutcome::Evaluated);
    // fires and waits each write their first record; silenced keeps its
    // Suppressed record and broken writes nothing.
    assert_eq!(
        (
            report.rules_evaluated,
            report.rules_failed,
            report.records_written
        ),
        (4, 1, 2)
    );
    running
}

/// The route serves the firing and pending alerts the evaluator's tick
/// published for the caller's tenant, and nothing else: not the inactive
/// rule, not the suppressed alert, not the failed rule, and not to a tenant
/// whose own evaluator found nothing.
///
/// Fails against a route that lists every folded alert (the suppressed one
/// appears), and against one that reads a single snapshot for every tenant
/// (`other` sees acme's two alerts).
#[tokio::test]
async fn alerts_route_serves_exactly_the_firing_alerts_published_by_the_evaluator() {
    let running = server_after_one_tick().await;
    let base = format!("http://{}", running.http_addr);

    let (status, body) = get(&base, "/api/v1/alerts", Some(ACME_TOKEN)).await;
    assert_eq!(status, 200);
    assert_eq!(
        body,
        json!({
            "status": "success",
            "data": {"alerts": [
                {
                    "labels": {"alertname": "fires", "severity": "page"},
                    "annotations": {"summary": "fires on the first tick"},
                    "state": "firing",
                    "activeAt": NOW_RFC3339,
                    "value": "2"
                },
                {
                    "labels": {"alertname": "waits", "severity": "ticket"},
                    "annotations": {},
                    "state": "pending",
                    "activeAt": NOW_RFC3339,
                    "value": "3"
                }
            ]}
        }),
        "exactly acme's firing and pending alerts"
    );

    let (status, body) = get(&base, "/api/v1/alerts", Some(OTHER_TOKEN)).await;
    assert_eq!(status, 200);
    assert_eq!(
        body,
        json!({"status": "success", "data": {"alerts": []}}),
        "the other tenant's own tick found nothing, and it never sees acme's"
    );

    let (status, _) = get(&base, "/api/v1/alerts", None).await;
    assert_eq!(status, 401);

    running.shutdown().await.expect("graceful shutdown");
}

/// One rule of acme's `/api/v1/rules` body, by name.
fn rule_named<'a>(body: &'a Value, name: &str) -> &'a Value {
    body["data"]["groups"][0]["rules"]
        .as_array()
        .expect("rules array")
        .iter()
        .find(|rule| rule["name"] == name)
        .unwrap_or_else(|| panic!("rule {name} rendered"))
}

/// Before the tenant's first tick every rule reads `unknown`; after it, each
/// rule carries the health and state that tick left it in, with its alerts.
#[tokio::test]
async fn rules_route_renders_health_and_state_from_the_last_tick() {
    let config = alerting_config(TICK_RULES, true);
    let (running, store) = start_test_server(Mode::All, config.clone()).await;
    let base = format!("http://{}", running.http_addr);

    let (status, before) = get(&base, "/api/v1/rules", Some(ACME_TOKEN)).await;
    assert_eq!(status, 200);
    for name in ["fires", "waits", "quiet", "silenced", "broken"] {
        let rule = rule_named(&before, name);
        assert_eq!(rule["health"], "unknown", "{name} before any tick");
        assert_eq!(rule["state"], "unknown", "{name} before any tick");
        assert!(rule.get("alerts").is_none(), "{name}: no alerts claimed");
        assert!(rule.get("lastEvaluation").is_none(), "{name}");
    }
    running.shutdown().await.expect("graceful shutdown");
    drop(store);

    let running = server_after_one_tick().await;
    let base = format!("http://{}", running.http_addr);
    let (status, after) = get(&base, "/api/v1/rules", Some(ACME_TOKEN)).await;
    assert_eq!(status, 200);

    for (name, state, alerts) in [
        ("fires", "firing", 1),
        ("waits", "pending", 1),
        ("quiet", "inactive", 0),
        ("silenced", "inactive", 0),
    ] {
        let rule = rule_named(&after, name);
        assert_eq!(rule["health"], "ok", "{name}");
        assert_eq!(rule["state"], state, "{name}");
        assert_eq!(rule["lastEvaluation"], NOW_RFC3339, "{name}");
        assert!(rule.get("lastError").is_none(), "{name}");
        assert_eq!(
            rule["alerts"].as_array().map(Vec::len),
            Some(alerts),
            "{name}"
        );
    }
    assert_eq!(
        rule_named(&after, "fires")["alerts"][0],
        json!({
            "labels": {"alertname": "fires", "severity": "page"},
            "annotations": {"summary": "fires on the first tick"},
            "state": "firing",
            "activeAt": NOW_RFC3339,
            "value": "2"
        })
    );

    let broken = rule_named(&after, "broken");
    assert_eq!(broken["health"], "err");
    assert_eq!(broken["lastEvaluation"], NOW_RFC3339);
    assert_eq!(
        broken["lastError"], "promql parse error: unclosed left parenthesis",
        "lastError is the failed query's own error"
    );
    assert_eq!(broken["state"], "inactive", "a failed rule raised nothing");

    running.shutdown().await.expect("graceful shutdown");
}

/// A process that runs no evaluator says so on `/api/v1/alerts` and claims no
/// state on `/api/v1/rules`, even when the shared store holds a firing alert
/// another process's evaluator wrote and published: neither route reads the
/// store or another process's state.
#[tokio::test]
async fn a_process_running_no_evaluator_serves_no_alert_state() {
    // Rules loaded, evaluation off: what `--disable-loop alert-eval` leaves.
    let (running, store) = start_test_server(Mode::All, alerting_config(TICK_RULES, false)).await;
    assert!(matches!(
        running.published_alerts.read(&TenantId::new("acme").hash()),
        ravel_server::alerting::PublishedRead::NotEvaluating
    ));

    // Another process: an evaluator over the same store, with its own handle.
    let elsewhere_config = alerting_config(TICK_RULES, true);
    let elsewhere = PublishedAlerts::for_config(&elsewhere_config);
    let report = evaluator(&store, &elsewhere_config, "acme", elsewhere.clone())
        .run_tick()
        .await;
    // No silence is seeded here, so `silenced` fires too.
    assert_eq!(
        report.records_written, 3,
        "fires, waits and silenced are durable"
    );
    assert!(matches!(
        elsewhere.read(&TenantId::new("acme").hash()),
        ravel_server::alerting::PublishedRead::Published(_)
    ));

    let base = format!("http://{}", running.http_addr);
    let (status, body) = get(&base, "/api/v1/alerts", Some(ACME_TOKEN)).await;
    assert_eq!(status, 503);
    assert_eq!(
        body,
        json!({"status": "error", "errorType": "unavailable", "error": NOT_EVALUATING})
    );

    let (status, rules) = get(&base, "/api/v1/rules", Some(ACME_TOKEN)).await;
    assert_eq!(status, 200);
    let fires = rule_named(&rules, "fires");
    assert_eq!(fires["health"], "unknown");
    assert_eq!(fires["state"], "unknown");
    assert!(fires.get("alerts").is_none());

    running.shutdown().await.expect("graceful shutdown");
}

/// The routes are reachable on `--listen-http` and serve the configured rule
/// set for the caller's tenant only. Before the server's evaluator has ticked,
/// `/api/v1/alerts` answers 503 rather than an empty list.
#[tokio::test]
async fn rules_route_is_served_on_the_http_listener() {
    let (running, _store) = start_test_server(Mode::All, alerting_config(RULES, true)).await;
    let base = format!("http://{}", running.http_addr);

    let (status, body) = get(&base, "/api/v1/rules", Some(ACME_TOKEN)).await;
    assert_eq!(status, 200, "the rules route must be mounted, not 404");
    assert_eq!(
        body,
        json!({
            "status": "success",
            "data": {
                "groups": [{
                    "name": "ravel-alert-rules",
                    "file": "",
                    "interval": 3600.0,
                    "rules": [{
                        "type": "alerting",
                        "name": "cpu-hot",
                        "query": "max by (instance) (cpu_usage) > 0.9",
                        "duration": 300.0,
                        "labels": {"severity": "page"},
                        "annotations": {"summary": "CPU over 90% for five minutes"},
                        "health": "unknown",
                        "state": "unknown"
                    }]
                }]
            }
        }),
        "the started server renders the rule set it was configured with"
    );

    // The other configured tenant authenticates and has no rules.
    let (status, body) = get(&base, "/api/v1/rules", Some(OTHER_TOKEN)).await;
    assert_eq!(status, 200);
    assert_eq!(
        body,
        json!({"status": "success", "data": {"groups": []}}),
        "a tenant with no rules must not see acme's"
    );

    // No credential is the same 401 the other tenant-scoped routes return.
    let (status, _) = get(&base, "/api/v1/rules", None).await;
    assert_eq!(status, 401);

    let (status, body) = get(&base, "/api/v1/alerts", Some(ACME_TOKEN)).await;
    assert_eq!(status, 503, "mounted, and honest that no tick has run");
    assert_eq!(
        body,
        json!({"status": "error", "errorType": "unavailable", "error": NOT_YET_EVALUATED})
    );

    running.shutdown().await.expect("graceful shutdown");
}

/// One acme rule that fires on its first tick, with no ingested data.
const FIRING_RULE: &str = r#"{
  "rules": [
    {
      "tenant": "acme",
      "rule_id": "fires",
      "promql": "vector(2)",
      "condition": {"type": "threshold", "op": "gt", "value": 1},
      "labels": {"severity": "page"},
      "annotations": {"summary": "fires on the first tick"}
    }
  ]
}"#;

/// The evaluator `ravel_server::start` spawns publishes into the handle the
/// routes read: once its own loop has ticked, `/api/v1/alerts` serves the
/// alert that tick fired. Time is paused, so the short interval elapses on
/// tokio's clock with no real sleep. The tick reads the system clock, so
/// `activeAt` is checked for shape only.
#[tokio::test(start_paused = true)]
async fn the_servers_own_evaluator_publishes_what_the_alerts_route_serves() {
    let interval = Duration::from_secs(60);
    let config = AlertEvalConfig {
        interval,
        ..alerting_config(FIRING_RULE, true)
    };
    let (running, _store) = start_test_server(Mode::All, config).await;
    let base = format!("http://{}", running.http_addr);

    let mut intervals = 0;
    let mut body = loop {
        let (status, body) = get(&base, "/api/v1/alerts", Some(ACME_TOKEN)).await;
        if status == 200 {
            break body;
        }
        assert_eq!(status, 503, "{body}");
        assert_eq!(body["error"], NOT_YET_EVALUATED);
        assert!(
            intervals < 5,
            "the server's evaluator published nothing the route reads after {intervals} intervals"
        );
        tokio::time::sleep(interval).await;
        intervals += 1;
    };

    let alert = &mut body["data"]["alerts"][0];
    assert!(alert["activeAt"].is_string(), "{alert}");
    alert
        .as_object_mut()
        .expect("alert object")
        .remove("activeAt");
    assert_eq!(
        body,
        json!({
            "status": "success",
            "data": {"alerts": [{
                "labels": {"alertname": "fires", "severity": "page"},
                "annotations": {"summary": "fires on the first tick"},
                "state": "firing",
                "value": "2"
            }]}
        })
    );

    running.shutdown().await.expect("graceful shutdown");
}

/// The mount sits inside the query-surface block, which is where the evaluator
/// itself runs. A gateway process has no evaluator, so it serves neither
/// route: were the merge moved above that block, these 404s would become
/// answers about rules nothing in the process evaluates.
#[tokio::test]
async fn gateway_mode_does_not_serve_the_rules_route() {
    let (running, _store) = start_test_server(Mode::Gateway, alerting_config(RULES, true)).await;
    let base = format!("http://{}", running.http_addr);

    let (status, _) = get(&base, "/api/v1/rules", Some(ACME_TOKEN)).await;
    assert_eq!(status, 404, "gateway mode mounts no query surface");
    let (status, _) = get(&base, "/api/v1/alerts", Some(ACME_TOKEN)).await;
    assert_eq!(status, 404, "gateway mode mounts no query surface");

    running.shutdown().await.expect("graceful shutdown");
}
