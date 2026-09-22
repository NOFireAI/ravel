//! `GET /api/v1/rules` on a started server (issue #1711).
//!
//! `alerts_api`'s own unit tests drive the router directly. This file covers
//! what those cannot: that the route is mounted on the shipping HTTP listener,
//! that it serves the rule set `ServerConfig::alerting` carries, that it is
//! scoped by the listener's resolver over a real socket, and that it is
//! mounted where the alert evaluator runs (`all`/`query`) rather than in every
//! mode. `GET /api/v1/alerts` is separate work, so its absence is asserted
//! here as the framework's 404 rather than left to be noticed by a client.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use ravel_object_store::memory::MemoryStore;
use ravel_server::alerting::parse_rules;
use ravel_server::{AlertEvalConfig, FoldTaskConfig, Mode, ServerConfig};
use ravel_types::TenantId;
use serde_json::{Value, json};

/// The tenant that has rules, and one that authenticates with no rules of its
/// own. The second is what makes a handler that ignores the resolved tenant
/// fail here rather than pass.
const ACME_TOKEN: &str = "acme-token";
const OTHER_TOKEN: &str = "other-token";

/// A long enough interval that no evaluation tick runs during the test, so the
/// assertions below depend on the loaded rule set and not on evaluator
/// activity. It is also the `interval` the group renders.
const EVAL_INTERVAL: Duration = Duration::from_secs(3600);

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

async fn start_test_server(mode: Mode) -> ravel_server::Running {
    let tokens = HashMap::from([
        (ACME_TOKEN.to_string(), TenantId::new("acme")),
        (OTHER_TOKEN.to_string(), TenantId::new("other")),
    ]);
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let store = Arc::new(MemoryStore::new());
    let rules = parse_rules(RULES).expect("valid rules");
    let config = ServerConfig {
        audit_pipeline: Default::default(),
        audit_text: Default::default(),
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        max_queued_flushes: 8,
        adaptive_flush_delay: false,
        max_flush_delay: Duration::from_secs(2),
        max_flush_delay_idle: Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
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
        // The shape `main.rs` builds: evaluation on because the rules file
        // held rules, and the same map handed to both the evaluator and the
        // route.
        alerting: AlertEvalConfig {
            enabled: true,
            interval: EVAL_INTERVAL,
            rules: Arc::new(rules),
            ..AlertEvalConfig::default()
        },
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
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        catalog_cache_max_bytes: 256 * 1024 * 1024,
        process_memory_budget_bytes: u64::MAX,
        process_memory_budget_is_fallback: false,
        cache_dir: None,
        catalog_resolve_concurrency: None,
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
        store.clone(),
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts")
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

/// The route is reachable on `--listen-http` and serves the configured rule
/// set for the caller's tenant only.
#[tokio::test]
async fn rules_route_is_served_on_the_http_listener() {
    let running = start_test_server(Mode::All).await;
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

    // `/api/v1/alerts` is a separate endpoint and is served nowhere.
    let (status, _) = get(&base, "/api/v1/alerts", Some(ACME_TOKEN)).await;
    assert_eq!(
        status, 404,
        "the alerts route is a documented absence, not a stub"
    );

    running.shutdown().await.expect("graceful shutdown");
}

/// The mount sits inside the query-surface block, which is where the evaluator
/// itself runs. A gateway process has no evaluator, so it serves no rules
/// route: were the merge moved above that block, this 404 would become a 200
/// reporting rules nothing in the process evaluates.
#[tokio::test]
async fn gateway_mode_does_not_serve_the_rules_route() {
    let running = start_test_server(Mode::Gateway).await;
    let base = format!("http://{}", running.http_addr);

    let (status, _) = get(&base, "/api/v1/rules", Some(ACME_TOKEN)).await;
    assert_eq!(status, 404, "gateway mode mounts no query surface");

    running.shutdown().await.expect("graceful shutdown");
}
