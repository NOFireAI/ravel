//! ADR-2633 section 4: every memory gate family appears on a scraped
//! `/metrics`, reading the server's own process memory budget and the
//! sampler thread's stats. Before the sampler starts, both gate gauges read
//! the off state; once it runs, they carry its readings.
//!
//! The sampler starts at most once per process and runs for the process's
//! life, so this file holds one test.

#![cfg(not(target_env = "msvc"))]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ravel_object_store::StoreMetrics;
use ravel_object_store::memory::MemoryStore;
use ravel_server::memory_gate::{self, MemoryGate};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::TenantId;

async fn start_test_server() -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert("testtoken".to_string(), TenantId::new("acme"));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let store = Arc::new(MemoryStore::new());
    let config = ServerConfig {
        audit_pipeline: Default::default(),
        audit_text: Default::default(),
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        max_queued_flushes: 8,
        adaptive_flush_delay: false,
        max_flush_delay: std::time::Duration::from_secs(2),
        max_flush_delay_idle: std::time::Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
        idle_flush_byte_floor: 0,
        mode: Mode::Query,
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
        cpu_gate_permits: ravel_server::config::CpuGatePermits { read: 3, write: 2 },
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

async fn scrape(client: &reqwest::Client, base: &str) -> String {
    let response = client
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("metrics request completes");
    assert_eq!(response.status(), 200);
    response.text().await.expect("metrics body is text")
}

const FAMILIES: [(&str, &str); 8] = [
    ("ravel_memory_gate_resident_bytes", "gauge"),
    ("ravel_memory_gate_high_water_bytes", "gauge"),
    ("ravel_memory_gate_purges_total", "counter"),
    ("ravel_memory_gate_samples_total", "counter"),
    ("ravel_memory_gate_epoch_refresh_seconds_total", "counter"),
    ("ravel_memory_gate_purge_seconds", "histogram"),
    ("ravel_memory_gate_refusals_total", "counter"),
    ("ravel_memory_budget_refusals_total", "counter"),
];

/// The value of the one `name{mode="query"}` sample in `body`.
fn value(body: &str, name: &str) -> f64 {
    let prefix = format!("{name}{{mode=\"query\"}} ");
    let values: Vec<f64> = body
        .lines()
        .filter_map(|line| line.strip_prefix(&prefix))
        .map(|value| value.parse().expect("sample value parses"))
        .collect();
    assert_eq!(values.len(), 1, "one {name} sample:\n{body}");
    values[0]
}

fn assert_every_family(body: &str) {
    for (name, kind) in FAMILIES {
        assert_eq!(
            body.matches(&format!("# TYPE {name} {kind}\n")).count(),
            1,
            "{name}:\n{body}"
        );
    }
    for site in ["admission", "reserve", "grow"] {
        let sample = format!("ravel_memory_gate_refusals_total{{mode=\"query\",site=\"{site}\"}} ");
        assert_eq!(body.matches(&sample).count(), 1, "{sample}:\n{body}");
    }
    for site in ["reserve", "grow"] {
        let sample =
            format!("ravel_memory_budget_refusals_total{{mode=\"query\",site=\"{site}\"}} ");
        assert_eq!(body.matches(&sample).count(), 1, "{sample}:\n{body}");
    }
}

/// A mark of 1 byte puts every reading at or above it, so each sample
/// purges and the scrape shows the purge, the re-read, and the mark.
#[tokio::test]
async fn every_memory_gate_family_appears_on_the_scraped_metrics() {
    let running = start_test_server().await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let body = scrape(&client, &base).await;
    assert_every_family(&body);
    assert_eq!(value(&body, "ravel_memory_gate_resident_bytes"), 0.0);
    assert_eq!(value(&body, "ravel_memory_gate_high_water_bytes"), 0.0);

    let gate = MemoryGate {
        enabled: true,
        high_water_bytes: 1,
        high_water_percent: 1,
        memory_budget_bytes: 100,
        interval_ms: 10,
        wait_ms: memory_gate::DEFAULT_WAIT_MS,
        ingest_buffer_bytes: 0,
        source: memory_gate::SOURCE_FLAG,
    };
    let budget = running.process_memory_budget();
    assert!(memory_gate::start_sampler(&gate, budget.clone()).expect("sampler thread spawns"));

    let deadline = Instant::now() + Duration::from_secs(30);
    let body = loop {
        let body = scrape(&client, &base).await;
        // The sampler records the purge before it writes the gate, so a scrape
        // between the two reads the count with the gauges still at 0.
        if value(&body, "ravel_memory_gate_purge_seconds_count") >= 1.0
            && value(&body, "ravel_memory_gate_high_water_bytes") >= 1.0
        {
            break body;
        }
        assert!(Instant::now() < deadline, "no purge within 30 s:\n{body}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_every_family(&body);
    assert_eq!(value(&body, "ravel_memory_gate_high_water_bytes"), 1.0);
    assert!(
        value(&body, "ravel_memory_gate_resident_bytes") >= 1.0,
        "{body}"
    );
    assert!(
        value(&body, "ravel_memory_gate_purges_total") >= 1.0,
        "{body}"
    );
    assert!(
        value(&body, "ravel_memory_gate_samples_total") >= 1.0,
        "{body}"
    );
    assert!(
        value(&body, "ravel_memory_gate_epoch_refresh_seconds_total") > 0.0,
        "{body}"
    );
    assert!(!budget.gate_open());

    running.shutdown().await.expect("graceful shutdown");
}
