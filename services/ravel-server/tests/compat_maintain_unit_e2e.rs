//! Issue #4248 reachability: a real [`ravel_server::start`] in `Mode::Maintain`
//! on a keyed bucket spawns the `sys/compat` maintenance unit, which runs the
//! late-write pass on a certified family, records its watermark in
//! `sys/compat`, and renders the certificate gauges on `/metrics`:
//! `ravel_compat_late_write_watermark_seconds{family}`,
//! `ravel_compat_late_write_watermark_age_seconds{family}`,
//! `ravel_compat_certificate_contradicted{family}` and
//! `ravel_compat_certify_finalization_pending{family}`.
//!
//! Each test seeds a certificate no pass has verified yet (the watermark is at
//! the census start, before `certified_unix_ns`), so the unit's first tick has
//! a pass due, and waits for the scrape to show the tick's result.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ravel_catalog::compat::{
    COMPAT_FORMAT_VERSION, CompatKeys, FAMILY_RSEG, create_compat, read_compat,
};
use ravel_catalog::{AbsentPolicy, raise_compat_fence, validate_or_adopt};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_proto::sys::v1 as sysproto;
use ravel_server::ingest_concurrency::IngestConcurrencyLimit;
use ravel_server::{FoldTaskConfig, LimitsConfig, Mode, ServerConfig};
use ravel_types::{Signal, TenantId};
use uuid::Uuid;

const TOKEN: &str = "testtoken";
const TENANT: &str = "acme";
const DEPLOYMENT_KEY: [u8; 32] = [0x4e; 32];
const NS_PER_HOUR: i64 = 3_600_000_000_000;
const NS_PER_SEC: i64 = 1_000_000_000;

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
}

fn keys() -> CompatKeys {
    CompatKeys::from_file_keys(&[DEPLOYMENT_KEY]).expect("key")
}

fn rseg_version() -> u32 {
    u32::from(ravel_segment::SegmentVersion::CURRENT.number())
}

/// `sys/compat` with `rseg` active at this build's version and certified at
/// `certified_unix_ns`, the watermark left at the census start a minute
/// earlier (what `certify` writes), and a fenced one-shard metrics bucket for
/// [`TENANT`].
async fn seed_certified(store: &MemoryStore, certified_unix_ns: i64) {
    let activated = certified_unix_ns - 10 * NS_PER_HOUR;
    create_compat(
        store,
        &keys(),
        sysproto::CompatRecord {
            format_version: COMPAT_FORMAT_VERSION,
            compat_epoch: 1,
            max_publish_lifetime_ns: NS_PER_HOUR,
            families: vec![sysproto::CompatFamily {
                family: FAMILY_RSEG.to_string(),
                active_write_version: rseg_version(),
                activated_epoch: 1,
                activated_unix_ns: activated,
                activated_by: "compat_maintain_unit_e2e".to_string(),
                certificate: Some(sysproto::Certificate {
                    version: rseg_version(),
                    certified_unix_ns,
                    ..Default::default()
                }),
                fence_complete_unix_ns: activated,
                late_writes_verified_through_unix_ns: certified_unix_ns - 60 * NS_PER_SEC,
                contradicted: false,
            }],
            ..Default::default()
        },
    )
    .await
    .expect("create sys/compat");
    let tenant = TenantId::new(TENANT).hash();
    validate_or_adopt(
        store,
        &tenant,
        Signal::Metrics,
        1,
        activated,
        AbsentPolicy::CreateFromConfig,
    )
    .await
    .expect("provision");
    raise_compat_fence(store, &tenant, Signal::Metrics, 1, activated)
        .await
        .expect("fence");
}

/// A metrics commit record one version below the certificate, landed now: a
/// late write.
async fn seed_late_commit(store: &MemoryStore) {
    let now = now_ns();
    store.set_clock_ms(u64::try_from(now / 1_000_000).expect("ms"));
    let commit = record::build(NewCommitRecord {
        tenant_hash: TenantId::new(TENANT).hash(),
        signal: Signal::Metrics,
        shard: 0,
        writer_id: Uuid::from_u128(1),
        writer_epoch: 1,
        writer_seq: 1,
        object_size: 1,
        content_hash: [1; 32],
        sample_count: 1,
        series_count: 1,
        min_event_ts_ns: now,
        max_event_ts_ns: now,
        min_ingest_ts_ns: now,
        max_ingest_ts_ns: now,
        segment_format_version: rseg_version() - 1,
        created_unix_ns: now,
        ingest_hour_bucket: u32::try_from(now.div_euclid(NS_PER_HOUR)).expect("hour"),
    })
    .expect("build commit");
    let key = ravel_commit::keys::commit_key_for_record(&commit).expect("commit key");
    store
        .put(&key, record::encode(&commit), PutOptions::default())
        .await
        .expect("put commit");
}

async fn rseg_entry(store: &dyn ObjectStoreBackend) -> sysproto::CompatFamily {
    read_compat(store, &keys())
        .await
        .expect("read")
        .expect("present")
        .0
        .families
        .into_iter()
        .find(|f| f.family == FAMILY_RSEG)
        .expect("rseg entry")
}

fn base_config() -> ServerConfig {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    ServerConfig {
        audit_pipeline: Default::default(),
        audit_text: Default::default(),
        query_budgets: Default::default(),
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        max_inflight_flushes: 1,
        max_inflight_flushes_per_tenant: None,
        flush_object_backstop_multiple: 4,
        max_queued_flushes: 8,
        adaptive_flush_delay: false,
        max_flush_delay: Duration::from_millis(200),
        max_flush_delay_idle: Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
        idle_flush_byte_floor: 0,
        mode: Mode::Maintain,
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
        maintain: ravel_server::MaintenanceTaskConfig {
            enabled: true,
            interval: Duration::from_secs(1),
            shard_count: 1,
            worker_record_keys: Some(
                ravel_maintain::worker_set::MaintainRecordKeys::from_file_keys(&[[0x5D; 32]])
                    .expect("one maintain key"),
            ),
            ..ravel_server::MaintenanceTaskConfig::default()
        },
        alerting: ravel_server::AlertEvalConfig::default(),
        oidc_refresh: None,
        otap: false,
        otap_max_decoded_batch_bytes: ravel_server::config::DEFAULT_OTAP_MAX_DECODED_BATCH_BYTES,
        metrics_tenant_labels: false,
        limits: LimitsConfig::default(),
        max_ingest_lag: ravel_server::DEFAULT_MAX_INGEST_LAG,
        deployment_key: Some(Box::new(DEPLOYMENT_KEY)),
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
        cpu_gate_permits: ravel_server::config::CpuGatePermits {
            read: 2,
            write: 2,
            ..Default::default()
        },
        ingest_hold: Default::default(),
        max_pending_strict_acks: 8_192,
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
        shutdown_timeout: ravel_server::DEFAULT_SHUTDOWN_TIMEOUT,
        drain_settle_interval: Duration::ZERO,
        ingest_concurrency_limit: IngestConcurrencyLimit::Unlimited,
    }
}

async fn start(store: Arc<dyn ObjectStoreBackend>) -> ravel_server::Running {
    ravel_server::start(
        base_config(),
        store.clone(),
        store,
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts")
}

async fn scrape(addr: std::net::SocketAddr) -> String {
    reqwest::Client::new()
        .get(format!("http://{addr}/metrics"))
        .send()
        .await
        .expect("metrics scrape completes")
        .text()
        .await
        .expect("metrics body is text")
}

/// The `rseg` sample of gauge `name`, `None` when absent. Panics when the
/// series appears more than once.
fn rseg_sample(body: &str, name: &str) -> Option<f64> {
    let prefix = format!("{name}{{mode=\"maintain\",family=\"rseg\"}} ");
    let values: Vec<f64> = body
        .lines()
        .filter_map(|line| line.strip_prefix(prefix.as_str()))
        .map(|value| value.trim().parse().expect("a numeric sample"))
        .collect();
    assert!(values.len() <= 1, "{name} rendered {values:?}");
    values.first().copied()
}

/// Scrape until `done` holds, for up to two minutes; returns the last body.
async fn scrape_until(addr: std::net::SocketAddr, done: impl Fn(&str) -> bool) -> String {
    let mut body = String::new();
    for _ in 0..1_200 {
        body = scrape(addr).await;
        if done(&body) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    body
}

/// A clean late-write pass advances the watermark past the certificate, which
/// clears finalization-pending, and `/metrics` shows the advanced watermark,
/// `contradicted` 0 and pending 0. A server that never spawned the unit (no
/// `sys/compat` keys on the maintenance config) leaves the watermark at the
/// census start and pending at 1.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_maintain_server_runs_the_late_write_pass_and_renders_the_gauges() {
    let store = Arc::new(MemoryStore::new());
    let certified = now_ns() - NS_PER_HOUR;
    seed_certified(&store, certified).await;
    let certified_seconds = (certified / NS_PER_SEC) as f64;

    let backend: Arc<dyn ObjectStoreBackend> = store.clone();
    let started_seconds = (now_ns() / NS_PER_SEC) as f64;
    let running = start(backend).await;
    let body = scrape_until(running.http_addr, |body| {
        rseg_sample(body, "ravel_compat_late_write_watermark_seconds")
            .is_some_and(|watermark| watermark > certified_seconds)
    })
    .await;
    running.shutdown().await.expect("graceful shutdown");
    let scraped_by_seconds = (now_ns() / NS_PER_SEC) as f64 + 1.0;

    let watermark = rseg_sample(&body, "ravel_compat_late_write_watermark_seconds")
        .expect("the watermark gauge renders");
    assert!(
        watermark > certified_seconds,
        "the pass advanced the watermark past the certificate:\n{body}"
    );
    // The age is the scrape time minus the watermark, so the two sum to a
    // time inside the scrape window.
    let age = rseg_sample(&body, "ravel_compat_late_write_watermark_age_seconds")
        .expect("the age gauge renders");
    assert!(age >= 0.0, "{body}");
    assert!(
        (started_seconds..=scraped_by_seconds).contains(&(watermark + age)),
        "watermark {watermark} + age {age} is not a scrape time in \
         [{started_seconds}, {scraped_by_seconds}]:\n{body}"
    );
    assert_eq!(
        rseg_sample(&body, "ravel_compat_certificate_contradicted"),
        Some(0.0),
        "{body}"
    );
    assert_eq!(
        rseg_sample(&body, "ravel_compat_certify_finalization_pending"),
        Some(0.0),
        "{body}"
    );
    let entry = rseg_entry(store.as_ref()).await;
    assert!(
        entry.late_writes_verified_through_unix_ns > certified,
        "the pass recorded its watermark in sys/compat: {entry:?}"
    );
    assert!(!entry.contradicted);
}

/// A record below the certified version landed after the census contradicts
/// the certificate: `/metrics` shows `contradicted` 1 and pending 0, and
/// `sys/compat` carries the flag.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_late_write_renders_the_certificate_contradicted() {
    let store = Arc::new(MemoryStore::new());
    let certified = now_ns() - NS_PER_HOUR;
    seed_certified(&store, certified).await;
    seed_late_commit(&store).await;

    let backend: Arc<dyn ObjectStoreBackend> = store.clone();
    let running = start(backend).await;
    let body = scrape_until(running.http_addr, |body| {
        rseg_sample(body, "ravel_compat_certificate_contradicted") == Some(1.0)
    })
    .await;
    running.shutdown().await.expect("graceful shutdown");

    assert_eq!(
        rseg_sample(&body, "ravel_compat_certificate_contradicted"),
        Some(1.0),
        "{body}"
    );
    assert_eq!(
        rseg_sample(&body, "ravel_compat_certify_finalization_pending"),
        Some(0.0),
        "{body}"
    );
    assert!(rseg_entry(store.as_ref()).await.contradicted);
}
