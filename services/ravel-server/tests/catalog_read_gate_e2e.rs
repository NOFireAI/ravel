//! ADR-1702 decision 4 reachability: the catalog a started server builds runs
//! its snapshot part decode on the server's read CPU gate.
//!
//! The test folds a snapshot whose single part declares an uncompressed entry
//! body above the read gate's default inline floor, starts a query server on
//! that store, issues a query over the folded hour, and reads the part decode
//! back from `ravel_cpu_gate_jobs_total{gate="read",site="catalog_part"}` on
//! `/metrics`. A catalog built without the gate decodes on the resolving task
//! and leaves that counter at zero.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use ravel_catalog::{Catalog, CatalogConfig, PartLimits, decode_head, decode_part};
use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{GetRange, ObjectStoreBackend, StoreMetrics};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{Signal, TenantHash, TenantId};
use uuid::Uuid;

const TOKEN: &str = "testtoken";
const TENANT: &str = "acme";
const NS_PER_HOUR: i64 = 3_600_000_000_000;
/// Hours before the current one the seeded segments sit in: far enough back
/// that the fold seals the hour on its first run against the wall clock.
const SEALED_HOURS_AGO: i64 = 4;
/// Enough segments that the folded part's entry body clears the read gate's
/// default inline floor; the test asserts the size rather than trusting this.
const SEGMENTS: u64 = 3_000;

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
}

/// Publishes one durable L0 metric segment at `hour` through the ingest
/// flush's publish path. The payload is opaque: resolving a snapshot part
/// never decodes segment bytes.
async fn seed_metric_segment(store: &MemoryStore, tenant: &TenantHash, seq: u64, hour: u32) {
    let created_unix_ns = i64::from(hour) * NS_PER_HOUR + NS_PER_HOUR / 2;
    let writer_id = Uuid::new_v4();
    let payload = format!("seg-{writer_id}-{seq}-{hour}").into_bytes();
    let content_hash = *blake3::hash(&payload).as_bytes();
    let record = record::build(NewCommitRecord {
        tenant_hash: *tenant,
        signal: Signal::Metrics,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: seq,
        object_size: payload.len() as u64,
        content_hash,
        sample_count: 1,
        series_count: 1,
        min_event_ts_ns: created_unix_ns - 1_000,
        max_event_ts_ns: created_unix_ns,
        min_ingest_ts_ns: created_unix_ns - 1_000,
        max_ingest_ts_ns: created_unix_ns,
        segment_format_version: 1,
        created_unix_ns,
        ingest_hour_bucket: hour,
    })
    .expect("valid metric commit record");
    let data_key = keys::reconstruct_data_key(&record).expect("data key");
    publish::put_data_object(store, &data_key, bytes::Bytes::from(payload))
        .await
        .expect("put data object");
    publish::publish(store, &record, &RetryPolicy::default())
        .await
        .expect("publish commit record");
}

/// The `entries_uncompressed_len` of every part the metrics HEAD names.
async fn part_entry_lens(store: &MemoryStore, tenant: &TenantHash) -> Vec<u64> {
    let head_key = format!(
        "t/{}/catalog/{}/HEAD",
        tenant.to_hex(),
        Signal::Metrics.key_prefix()
    );
    let head_bytes = store
        .get(&head_key, GetRange::Full)
        .await
        .expect("HEAD present")
        .data;
    let head = decode_head(&head_bytes).expect("HEAD decodes");
    let mut lens = Vec::new();
    for part in &head.parts {
        let bytes = store
            .get(&part.key, GetRange::Full)
            .await
            .expect("part present")
            .data;
        let decoded = decode_part(&bytes, &PartLimits::default()).expect("part decodes");
        lens.push(decoded.header.entries_uncompressed_len);
    }
    lens
}

async fn start_query_server(store: Arc<MemoryStore>) -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let config = ServerConfig {
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
        cpu_gate_permits: ravel_server::config::CpuGatePermits { read: 2, write: 1 },
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
    let backend: Arc<dyn ObjectStoreBackend> = store;
    ravel_server::start(
        config,
        backend.clone(),
        backend,
        Arc::new(StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts")
}

/// The value of the one `ravel_cpu_gate_<family>` sample for the read gate's
/// `site`, asserting it renders exactly once.
fn read_site_figure(body: &str, family: &str, site: &str) -> u64 {
    let prefix =
        format!("ravel_cpu_gate_{family}{{mode=\"query\",gate=\"read\",site=\"{site}\"}} ");
    let samples: Vec<&str> = body
        .lines()
        .filter_map(|line| line.strip_prefix(prefix.as_str()))
        .collect();
    assert_eq!(samples.len(), 1, "{prefix:?} must render once:\n{body}");
    samples[0].parse().expect("counter value")
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn query_resolve_decodes_the_snapshot_part_on_the_server_read_gate() {
    let tenant = TenantId::new(TENANT).hash();
    let store = Arc::new(MemoryStore::new());
    let now = now_ns();
    let hour = u32::try_from(now / NS_PER_HOUR - SEALED_HOURS_AGO).expect("hour fits u32");
    for seq in 1..=SEGMENTS {
        seed_metric_segment(&store, &tenant, seq, hour).await;
    }

    let folder = Catalog::new(
        store.clone(),
        CatalogConfig {
            shard_count: 1,
            ..CatalogConfig::default()
        },
    )
    .expect("folding catalog");
    let report = folder
        .fold(&tenant, Signal::Metrics, Uuid::new_v4(), now, &[], None)
        .await
        .expect("fold");
    assert_eq!(report.entry_count, SEGMENTS, "every seeded segment folds");
    let lens = part_entry_lens(&store, &tenant).await;
    assert_eq!(lens.len(), 1, "one part: {lens:?}");
    assert!(
        lens[0] >= ravel_cpu_gate::DEFAULT_INLINE_FLOOR_BYTES,
        "the part's entry body is {} bytes, below the {} byte inline floor, so its \
         decode would run inline whatever gate the catalog holds",
        lens[0],
        ravel_cpu_gate::DEFAULT_INLINE_FLOOR_BYTES
    );

    let running = start_query_server(store.clone()).await;
    let before = scrape(&running).await;
    assert_eq!(read_site_figure(&before, "jobs_total", "catalog_part"), 0);

    let start_s = i64::from(hour) * 3_600;
    let end_s = start_s + 3_599;
    // The answer does not matter here (the seeded payloads are opaque): the
    // resolve that precedes any segment read is what decodes the part.
    let _ = reqwest::Client::new()
        .get(format!("http://{}/api/v1/query_range", running.http_addr))
        .header("authorization", format!("Bearer {TOKEN}"))
        .query(&[
            ("query", "cpu_usage"),
            ("start", start_s.to_string().as_str()),
            ("end", end_s.to_string().as_str()),
            ("step", "60"),
        ])
        .send()
        .await
        .expect("query request completes");

    let after = scrape(&running).await;
    assert_eq!(
        read_site_figure(&after, "jobs_total", "catalog_part"),
        1,
        "the resolve decodes the one part on the read gate, once:\n{after}"
    );
    assert_eq!(read_site_figure(&after, "inline_total", "catalog_part"), 0);

    running.shutdown().await.expect("graceful shutdown");
}
