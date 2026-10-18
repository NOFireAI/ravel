//! ADR-2633 section 4, as amended for issues #2806 and #2838: every memory
//! gate family appears on a scraped `/metrics`, reading the server's own
//! process memory budget and the sampler thread's stats. Before the sampler
//! starts, all four gate gauges read the off state; once it runs, they
//! carry its readings, and the sub-floor sample counter moves with every
//! reading below the close floor.
//!
//! The sampler reads host available memory from `/proc/meminfo`, so this
//! runs on Linux only; on every other target the gate resolves off and the
//! sampler never starts. It starts at most once per process and runs for the
//! process's life, so this file holds one test.

#![cfg(target_os = "linux")]
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ravel_object_store::StoreMetrics;
use ravel_object_store::memory::MemoryStore;
use ravel_server::memory_gate::{self, HostMemorySource, MemoryGate};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::TenantId;

async fn start_test_server() -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert("testtoken".to_string(), TenantId::new("acme"));
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
        cpu_gate_permits: ravel_server::config::CpuGatePermits {
            read: 3,
            write: 2,
            ..Default::default()
        },
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

/// Each family, its type, and a phrase its HELP line must carry: the one
/// that tells it from its neighbour (the two floors, the two refusal
/// counters), so a renderer that swaps two families' text fails here.
const FAMILIES: [(&str, &str, &str); 11] = [
    (
        "ravel_memory_gate_available_bytes",
        "gauge",
        "Host available memory as the memory gate sampler last read it",
    ),
    (
        "ravel_memory_gate_available_floor_bytes",
        "gauge",
        "close floor (--memory-gate-min-available-bytes)",
    ),
    (
        "ravel_memory_gate_purge_floor_bytes",
        "gauge",
        "purge floor (--memory-gate-purge-available-bytes)",
    ),
    (
        "ravel_memory_gate_resident_bytes",
        "gauge",
        "jemalloc stats.resident as the memory gate sampler last read it",
    ),
    (
        "ravel_memory_gate_purges_total",
        "counter",
        "Forced jemalloc purges the memory gate sampler ran",
    ),
    (
        "ravel_memory_gate_samples_total",
        "counter",
        "Memory gate samples that read host available memory",
    ),
    (
        "ravel_memory_gate_samples_below_floor_total",
        "counter",
        "whether or not the sample closed the gate",
    ),
    (
        "ravel_memory_gate_epoch_refresh_seconds_total",
        "counter",
        "Wall time the memory gate sampler spent reading host available memory",
    ),
    (
        "ravel_memory_gate_purge_seconds",
        "histogram",
        "Wall time of each forced jemalloc purge",
    ),
    (
        "ravel_memory_gate_refusals_total",
        "counter",
        "refused because the memory gate was closed on host available memory",
    ),
    (
        "ravel_memory_budget_refusals_total",
        "counter",
        "refused because the process memory budget's ledger was full",
    ),
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
    for (name, kind, help_mentions) in FAMILIES {
        assert_eq!(
            body.matches(&format!("# TYPE {name} {kind}\n")).count(),
            1,
            "{name}:\n{body}"
        );
        let help_prefix = format!("# HELP {name} ");
        let help_lines: Vec<&str> = body
            .lines()
            .filter_map(|line| line.strip_prefix(&help_prefix))
            .collect();
        assert_eq!(help_lines.len(), 1, "{name} HELP:\n{body}");
        assert!(
            help_lines[0].contains(help_mentions),
            "{name} HELP {:?} does not mention {help_mentions:?}",
            help_lines[0]
        );
        assert!(
            body.contains(&format!(
                "{help_prefix}{}\n# TYPE {name} {kind}\n",
                help_lines[0]
            )),
            "{name} HELP is not directly above its TYPE line:\n{body}"
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

/// Floors above any host's available memory put every reading below both,
/// so every sample is below the close floor, the third consecutive one
/// closes the gate at the default `close_after_samples`, the first one
/// purges, and the scrape shows the purge, the readings, both floors and
/// the sub-floor sample count. The floors are 4 and 8 PiB: above any host,
/// and each exactly representable as an f64 and distinct from the other
/// once rendered, so a renderer that swaps the two floor families fails the
/// value assertions below.
#[tokio::test]
async fn every_memory_gate_family_appears_on_the_scraped_metrics() {
    const CLOSE_FLOOR: u64 = 1 << 52;
    const PURGE_FLOOR: u64 = 1 << 53;

    let running = start_test_server().await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let body = scrape(&client, &base).await;
    assert_every_family(&body);
    for gauge in [
        "ravel_memory_gate_available_bytes",
        "ravel_memory_gate_available_floor_bytes",
        "ravel_memory_gate_purge_floor_bytes",
        "ravel_memory_gate_resident_bytes",
    ] {
        assert_eq!(
            value(&body, gauge),
            0.0,
            "{gauge} before the sampler starts"
        );
    }

    let gate = MemoryGate {
        enabled: true,
        host_memory_bytes: 0,
        min_available_bytes: CLOSE_FLOOR,
        purge_available_bytes: PURGE_FLOOR,
        purge_min_interval_ms: memory_gate::MIN_PURGE_MIN_INTERVAL_MS,
        interval_ms: 10,
        close_after_samples: memory_gate::DEFAULT_CLOSE_AFTER_SAMPLES,
        wait_ms: memory_gate::DEFAULT_WAIT_MS,
        memory_budget_bytes: 100,
        available_source: HostMemorySource::MemInfo,
        source: memory_gate::SOURCE_FLAG,
    };
    let budget = running.process_memory_budget();
    assert!(memory_gate::start_sampler(&gate, budget.clone()).expect("sampler thread spawns"));

    let close_after = f64::from(memory_gate::DEFAULT_CLOSE_AFTER_SAMPLES);
    let deadline = Instant::now() + Duration::from_secs(30);
    let body = loop {
        let body = scrape(&client, &base).await;
        // The sampler records the purge before it writes the gate, so a scrape
        // between the two reads the count with the budget's gauges still at 0.
        // It writes the gate before it counts the sub-floor sample, but the
        // count's Relaxed fetch_add after the gate's Release store orders
        // nothing for a Relaxed reader, so a scrape may see the count at the
        // close threshold with the gate still open for one interval. Every
        // sample here is below the floor, so the close, once written, holds,
        // and it is waited for on the budget rather than inferred from the
        // count.
        if value(&body, "ravel_memory_gate_purge_seconds_count") >= 1.0
            && value(&body, "ravel_memory_gate_available_floor_bytes") >= 1.0
            && value(&body, "ravel_memory_gate_samples_below_floor_total") >= close_after
            && !budget.gate_open()
        {
            break body;
        }
        assert!(
            Instant::now() < deadline,
            "no purge, {close_after} sub-floor samples and a closed gate within 30 s:\n{body}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert_every_family(&body);
    // Each family's value: the two floors as set (and distinct from each
    // other), the two readings real figures below both floors, the counters
    // moved, and the purge histogram's count equal to the purge counter.
    assert_eq!(
        value(&body, "ravel_memory_gate_available_floor_bytes"),
        CLOSE_FLOOR as f64
    );
    assert_eq!(
        value(&body, "ravel_memory_gate_purge_floor_bytes"),
        PURGE_FLOOR as f64
    );
    assert_ne!(CLOSE_FLOOR as f64, PURGE_FLOOR as f64);
    let available = value(&body, "ravel_memory_gate_available_bytes");
    assert!(
        available >= 1.0 && available < CLOSE_FLOOR as f64,
        "available {available}:\n{body}"
    );
    let resident = value(&body, "ravel_memory_gate_resident_bytes");
    assert!(
        resident >= 1.0 && resident < CLOSE_FLOOR as f64,
        "resident {resident}:\n{body}"
    );
    let purges = value(&body, "ravel_memory_gate_purges_total");
    assert!(purges >= 1.0, "{body}");
    assert_eq!(
        value(&body, "ravel_memory_gate_purge_seconds_count"),
        purges,
        "{body}"
    );
    assert!(
        value(&body, "ravel_memory_gate_purge_seconds_sum") >= 0.0,
        "{body}"
    );
    let inf_bucket = "ravel_memory_gate_purge_seconds_bucket{mode=\"query\",le=\"+Inf\"} ";
    let inf_count: f64 = body
        .lines()
        .find_map(|line| line.strip_prefix(inf_bucket))
        .expect("the +Inf bucket renders")
        .parse()
        .expect("bucket count parses");
    assert_eq!(inf_count, purges, "{body}");
    let samples = value(&body, "ravel_memory_gate_samples_total");
    assert!(samples >= purges, "{body}");
    // Every sample is below the close floor, so the sub-floor count tracks
    // the sample count, within one either way: the scrape's snapshot loads
    // `samples` before `samples_below_floor` while the sampler keeps
    // running, so a sample that completes between the two loads shows on
    // the second counter and not the first, and one that is mid-sample at
    // the first load shows on the first and not the second.
    let below_floor = value(&body, "ravel_memory_gate_samples_below_floor_total");
    assert!(
        below_floor >= close_after && below_floor <= samples + 1.0 && below_floor >= samples - 1.0,
        "samples_below_floor {below_floor} against samples {samples}:\n{body}"
    );
    assert!(
        value(&body, "ravel_memory_gate_epoch_refresh_seconds_total") > 0.0,
        "{body}"
    );
    // The gate closed before the loop broke, and every later sample is
    // below the floor, so it is still closed here.
    assert!(
        !budget.gate_open(),
        "{close_after} consecutive sub-floor samples close the gate and later ones keep it closed"
    );
    assert_eq!(budget.gate_floor(), CLOSE_FLOOR);

    // A reservation against the closed gate counts on the reserve site of
    // the gate's refusal family, and not on the ledger's.
    let refused = budget
        .try_reserve(1)
        .expect_err("a closed gate refuses one byte");
    assert!(matches!(
        refused.cause,
        ravel_memory::ExhaustionCause::HostMemory {
            floor: CLOSE_FLOOR,
            ..
        }
    ));
    let body = scrape(&client, &base).await;
    let site = |family: &str, site: &str| -> f64 {
        let prefix = format!("{family}{{mode=\"query\",site=\"{site}\"}} ");
        body.lines()
            .find_map(|line| line.strip_prefix(&prefix))
            .unwrap_or_else(|| panic!("{prefix} in {body}"))
            .parse()
            .expect("refusal count parses")
    };
    assert!(site("ravel_memory_gate_refusals_total", "reserve") >= 1.0);
    assert_eq!(site("ravel_memory_budget_refusals_total", "reserve"), 0.0);

    running.shutdown().await.expect("graceful shutdown");
}
