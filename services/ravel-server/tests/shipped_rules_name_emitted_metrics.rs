//! `deploy/prometheus/ravel.rules.yaml` ships alert rules an operator loads
//! verbatim. Every metric name in that file must name a series Ravel really
//! renders, or the rule matches nothing and the condition it covers pages
//! nobody, with no error at scrape time.
//!
//! The assertion is against a RENDERED `/metrics` body, obtained the way
//! `metrics_endpoint.rs` obtains one (an in-process server on `MemoryStore`,
//! scraped over HTTP), not against the string literals in
//! `services/ravel-server/src/metrics.rs`: a name that is declared in the
//! source but that nothing renders would pass a source scan and still leave
//! the shipped rule dead.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use ravel_object_store::StoreMetrics;
use ravel_object_store::memory::MemoryStore;
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::TenantId;

const TOKEN: &str = "testtoken";

/// The shipped rule file, relative to this crate's manifest directory.
const RULES_FILE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../deploy/prometheus/ravel.rules.yaml"
);

/// Distinct `ravel_*` metric names the shipped rule file references, counted
/// over expressions, annotations and comments alike.
///
/// A literal, not a figure derived from the same scan the assertions run over:
/// an extractor that silently matches nothing (a broken pattern, a path that
/// no longer resolves) would otherwise leave every assertion below passing
/// vacuously over an empty set. Adding a rule that names a new metric fails
/// here until this number is updated.
const EXPECTED_METRIC_NAMES: usize = 37;

/// Alert rules in the shipped file. Pinned for the same reason as the name
/// count: a file that stopped parsing, or that lost a group, must fail rather
/// than shrink the scan.
const EXPECTED_ALERTS: usize = 31;

/// One tenant, one trivially valid PromQL rule. Enough for `alerting::spawn`
/// to build an evaluator, which is what puts the whole `ravel_alert_*` family
/// on the exposition: a process that configured no alerting omits the family
/// by design, so a default test server cannot prove those names render.
const ALERT_RULES_JSON: &str = r#"{
  "rules": [
    {
      "tenant": "acme",
      "rule_id": "shipped-rules-probe",
      "promql": "up",
      "condition": { "type": "threshold", "op": "gt", "value": 0.9 }
    }
  ]
}"#;

/// Every `ravel_<segment>[_<segment>...]` token in `text`.
///
/// Hand-rolled rather than a regex so this test adds no dependency. A trailing
/// underscore is trimmed, so a family prefix written in prose (`ravel_scrub_`)
/// does not enter the set as a metric name, and a match that continues an
/// identifier is skipped.
fn metric_names(text: &str) -> BTreeSet<String> {
    const PREFIX: &str = "ravel_";
    let bytes = text.as_bytes();
    let mut out = BTreeSet::new();
    let mut cursor = 0usize;
    while let Some(offset) = text[cursor..].find(PREFIX) {
        let start = cursor + offset;
        let mut end = start + PREFIX.len();
        while end < bytes.len()
            && (bytes[end].is_ascii_lowercase()
                || bytes[end].is_ascii_digit()
                || bytes[end] == b'_')
        {
            end += 1;
        }
        let continues_identifier =
            start > 0 && (bytes[start - 1].is_ascii_alphanumeric() || bytes[start - 1] == b'_');
        let name = text[start..end].trim_end_matches('_');
        if !continues_identifier && name.len() > PREFIX.len() {
            out.insert(name.to_string());
        }
        cursor = end;
    }
    out
}

/// The families a rendered exposition really exposes, read off its `# TYPE`
/// lines.
///
/// Not [`metric_names`] over the whole body: a family's HELP text names other
/// families (`ravel_ingest_wire_bytes_total`'s names
/// `ravel_admission_admitted_bytes_total`), so a substring scan would accept a
/// rule whose metric only ever appears inside someone else's help string.
fn exposed_families(body: &str) -> BTreeSet<String> {
    body.lines()
        .filter_map(|line| line.strip_prefix("# TYPE "))
        .filter_map(|rest| rest.split_whitespace().next())
        .map(str::to_string)
        .collect()
}

/// Mirrors `metrics_endpoint.rs`'s helper, with the three knobs this test
/// needs: the fold task (the stamp-coverage families), a deployment key (the
/// durable auth family) and an alert rule set (the alerting family).
async fn start_test_server(
    mode: Mode,
    fold_enabled: bool,
    deployment_key: bool,
    alert_rules: bool,
) -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new("acme"));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let store = Arc::new(MemoryStore::new());
    let alerting = if alert_rules {
        ravel_server::AlertEvalConfig {
            enabled: true,
            rules: Arc::new(
                ravel_server::alerting::parse_rules(ALERT_RULES_JSON).expect("rules parse"),
            ),
            ..ravel_server::AlertEvalConfig::default()
        }
    } else {
        ravel_server::AlertEvalConfig::default()
    };
    let config = ServerConfig {
        audit_pipeline: Default::default(),
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
        tenant_resolver,
        mtls_listener: None,
        fold_tenants: Vec::new(),
        fold: FoldTaskConfig {
            enabled: fold_enabled,
            ..FoldTaskConfig::default()
        },
        maintain: ravel_server::MaintenanceTaskConfig::default(),
        alerting,
        oidc_refresh: None,
        otap: false,
        metrics_tenant_labels: false,
        limits: ravel_server::LimitsConfig::default(),
        max_ingest_lag: ravel_server::DEFAULT_MAX_INGEST_LAG,
        deployment_key: deployment_key.then(|| Box::new([7u8; 32])),
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
        process_memory_budget_bytes: u64::MAX,
        process_memory_budget_is_fallback: false,
        cache_dir: None,
        catalog_resolve_concurrency: None,
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
        Arc::new(StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts")
}

async fn scrape(running: &ravel_server::Running) -> String {
    let base = format!("http://{}", running.http_addr);
    reqwest::Client::new()
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("metrics request completes")
        .text()
        .await
        .expect("metrics body is text")
}

/// The union of the `/metrics` bodies of the two process shapes it takes to
/// render every family a shipped rule names.
///
/// No single process renders them all, and that is by design rather than an
/// accident of this test: the maintenance and scrub families exist only on a
/// `--mode maintain` process, while the ingest, fold stamp-coverage, durable
/// auth and alerting families exist only where those subsystems were built.
async fn rendered_bodies() -> String {
    let all = start_test_server(Mode::All, true, true, true).await;
    let mut body = scrape(&all).await;
    all.shutdown().await.expect("graceful shutdown");

    let maintain = start_test_server(Mode::Maintain, false, false, false).await;
    body.push_str(&scrape(&maintain).await);
    maintain.shutdown().await.expect("graceful shutdown");

    body
}

#[tokio::test]
async fn every_metric_named_by_a_shipped_rule_is_rendered() {
    let rules = std::fs::read_to_string(RULES_FILE)
        .unwrap_or_else(|e| panic!("shipped rule file {RULES_FILE} must be readable: {e}"));

    assert_eq!(
        rules.matches("\n      - alert: ").count(),
        EXPECTED_ALERTS,
        "shipped rule file must carry exactly {EXPECTED_ALERTS} alert rules"
    );

    let names = metric_names(&rules);
    assert_eq!(
        names.len(),
        EXPECTED_METRIC_NAMES,
        "shipped rule file must name exactly {EXPECTED_METRIC_NAMES} distinct Ravel metrics, \
         found {}: {names:?}",
        names.len()
    );

    let body = rendered_bodies().await;
    let rendered = exposed_families(&body);
    assert!(
        rendered.len() > EXPECTED_METRIC_NAMES,
        "the rendered bodies must expose more metrics than the rule file names; \
         found only {}",
        rendered.len()
    );

    let missing: Vec<&String> = names.iter().filter(|n| !rendered.contains(*n)).collect();
    assert!(
        missing.is_empty(),
        "shipped rules name metrics no /metrics body renders: {missing:?}"
    );
}
