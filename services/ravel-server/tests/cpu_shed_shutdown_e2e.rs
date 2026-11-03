//! ADR-3718's CPU sampler background task and graceful shutdown.
//!
//! `Running::shutdown` best-effort-joins each ingest router's shard actors by
//! `Arc::try_unwrap`-ing it (`drain_router` in `services/ravel-server/src/
//! lib.rs`): that only succeeds once every other strong reference is gone.
//! The CPU-shed detector task, once spawned, must therefore be stopped
//! before that join runs, and must never itself hold a router `Arc` clone in
//! the first place. This file proves the join still happens with the
//! detector enabled, using a fake [`ravel_server::cpu_shed::CpuSource`] (via
//! [`ravel_server::start_for_test_with_cpu_source`], the `cpu-shed-test
//! -support`-gated seam) so the detector's sampling loop genuinely runs,
//! including off Linux where the real `/proc/self/stat` source always
//! answers `None`.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_server::config::CpuShedConfig;
use ravel_server::cpu_shed::CpuSource;
use ravel_server::health_listener::Heartbeat;
use ravel_server::ingest_concurrency::IngestConcurrencyLimit;
use ravel_server::{FoldTaskConfig, LimitsConfig, Mode, ServerConfig};
use ravel_types::TenantId;

const TOKEN: &str = "testtoken";
const TENANT: &str = "acme";

/// Always answers a fixed CPU time: enough for the detector's sampling loop
/// to run every tick (`Detector::run` only no-ops on `None`), with no need
/// to reach saturation for this file's assertion, which is about shutdown
/// joining the shard actors, not about shedding.
#[derive(Debug, Clone, Copy)]
struct FixedCpuSource;

impl CpuSource for FixedCpuSource {
    fn cpu_time(&self) -> Option<Duration> {
        Some(Duration::from_millis(1))
    }
}

/// Starts a full server with the ADR-3718 detector enabled
/// (`--ingest-cpu-shed-threshold 0.95`) and driven by [`FixedCpuSource`]
/// instead of `/proc/self/stat`.
async fn start_test_server_with_detector(
    store: Arc<dyn ObjectStoreBackend>,
) -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let config = ServerConfig {
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
        audit_pipeline: Default::default(),
        audit_text: Default::default(),
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        max_inflight_flushes_per_tenant: None,
        flush_object_backstop_multiple: 4,
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
        alerting: ravel_server::AlertEvalConfig::default(),
        oidc_refresh: None,
        otap: false,
        otap_max_decoded_batch_bytes: ravel_server::config::DEFAULT_OTAP_MAX_DECODED_BATCH_BYTES,
        metrics_tenant_labels: false,
        limits: LimitsConfig::default(),
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
        cpu_shed: CpuShedConfig {
            threshold: 0.95,
            max: 0.9,
            cores: 1,
        },
        max_pending_strict_acks: 8_192,
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
        shutdown_timeout: ravel_server::DEFAULT_SHUTDOWN_TIMEOUT,
        drain_settle_interval: Duration::ZERO,
        ingest_concurrency_limit: IngestConcurrencyLimit::Unlimited,
    };
    let heartbeat = Heartbeat::new(Arc::new(ravel_ingest::SystemClock));
    ravel_server::start_for_test_with_cpu_source(
        config,
        store.clone(),
        store.clone(),
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
        heartbeat,
        Box::new(FixedCpuSource),
    )
    .await
    .expect("server starts")
}

/// With the ADR-3718 detector enabled and genuinely sampling (not merely
/// configured), graceful shutdown still joins every ingest router's shard
/// actors.
///
/// Distinguishing test: before this fix, the detector's closure captured a
/// full `Arc<IngestRouter>` clone (not just a metrics handle) and its
/// `tokio::spawn` `JoinHandle` was never stored or stopped, so the clone it
/// held lived for as long as the (never-stopped) task ran -- past the point
/// `drain_router`'s `Arc::try_unwrap` needs sole ownership. Reverting the
/// spawn site in `services/ravel-server/src/lib.rs` to that shape (capturing
/// router `Arc` clones instead of `.metrics_handle()`, and never calling
/// `cpu_shed_detector_task.stop()` before the `drain_router` calls) flips
/// this assertion from pass to fail: `shard_actors_joined_handle()` reads
/// `false` instead of `true`.
#[tokio::test]
async fn shutdown_joins_shard_actors_with_the_detector_running() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_test_server_with_detector(store).await;

    // Give the detector's sampling loop a few real ticks (`SAMPLE_INTERVAL`
    // is 100ms) so it is genuinely running, not merely spawned and still on
    // its very first poll, before shutdown races it.
    tokio::time::sleep(Duration::from_millis(350)).await;

    let joined = running.shard_actors_joined_handle();
    running.shutdown().await.expect("graceful shutdown");

    assert!(
        joined.load(Ordering::SeqCst),
        "shutdown must join every ingest router's shard actors even while \
         the ADR-3718 CPU sampler was running"
    );
}
