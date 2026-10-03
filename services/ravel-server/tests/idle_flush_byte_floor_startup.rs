//! Startup validation of `--idle-flush-byte-floor` (ADR-1737 decision 1).
//!
//! The floor and `--min-flush-bytes` are one constraint across two flags: a
//! floor at or above `min_flush_bytes` leaves no idle tier between them, so
//! every buffer that is not already worth a PUT would take the hour-long
//! sub-floor hold, and the buffered-mode loss window an operator thought they
//! were scoping to near-empty tenants would cover all of them. `start` checks
//! the pair at its top, in every mode, before any pipeline is built, so that
//! combination refuses startup instead of running.
//!
//! Both halves are covered here: the refusal names the flags an operator set,
//! and the shipped default of 0 (and a legal non-zero floor) still start.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;

use ravel_object_store::memory::MemoryStore;
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::TenantId;

const TOKEN: &str = "testtoken";

/// The shipped `--min-flush-bytes`, which the floor is validated against.
const MIN_FLUSH_BYTES: usize = 256 * 1024;

/// A `ServerConfig` in `Mode::All` (which builds all three ingest routers, so
/// every pipeline's config is validated) with `idle_flush_byte_floor` set to
/// `floor` and everything else at the shipped cadence.
fn config_with_floor(floor: usize) -> ServerConfig {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new("acme"));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    ServerConfig {
        audit_pipeline: Default::default(),
        audit_text: Default::default(),
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        max_queued_flushes: 8,
        adaptive_flush_delay: false,
        max_flush_delay: std::time::Duration::from_secs(2),
        max_flush_delay_idle: std::time::Duration::from_secs(40),
        min_flush_bytes: MIN_FLUSH_BYTES,
        idle_flush_byte_floor: floor,
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
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: std::time::Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
        shutdown_timeout: ravel_server::DEFAULT_SHUTDOWN_TIMEOUT,
        drain_settle_interval: std::time::Duration::ZERO,
        ingest_concurrency_limit: ravel_server::ingest_concurrency::IngestConcurrencyLimit::Bounded(
            1024,
        ),
    }
}

/// Run `start` with the given floor, returning whatever it returned.
async fn start_with_floor(floor: usize) -> anyhow::Result<ravel_server::Running> {
    let store = Arc::new(MemoryStore::new());
    ravel_server::start(
        config_with_floor(floor),
        store.clone(),
        store.clone(),
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
}

/// A floor exactly at `--min-flush-bytes` is the boundary the constraint
/// rejects: at that value the idle tier is empty, since every buffer below
/// `min_flush_bytes` is also below the floor.
#[tokio::test]
async fn a_floor_at_min_flush_bytes_refuses_startup() {
    let err = start_with_floor(MIN_FLUSH_BYTES)
        .await
        .err()
        .expect("a floor at min_flush_bytes must refuse startup");
    let message = format!("{err:#}");
    assert!(
        message.contains("--idle-flush-byte-floor"),
        "the refusal must name the flag the operator set, got: {message}"
    );
    assert!(
        message.contains("--min-flush-bytes"),
        "the refusal must name the flag the floor is validated against, got: {message}"
    );
    assert!(
        message.contains(&MIN_FLUSH_BYTES.to_string()),
        "the refusal must carry the byte figures that conflict, got: {message}"
    );
}

/// Above the boundary too: this pins that the refusal reaches the operator
/// with both flags named, not which check raised it (the top-of-`start` check
/// fires before any pipeline's own validation).
#[tokio::test]
async fn a_floor_above_min_flush_bytes_refuses_startup() {
    let err = start_with_floor(MIN_FLUSH_BYTES + 1)
        .await
        .err()
        .expect("a floor above min_flush_bytes must refuse startup");
    let message = format!("{err:#}");
    assert!(
        message.contains("--idle-flush-byte-floor"),
        "the refusal must name the flag the operator set, got: {message}"
    );
}

/// The shipped default starts, so the validation cannot have been written as
/// "any floor is invalid": 0 disables the tier and is the value every existing
/// deployment runs.
#[tokio::test]
async fn the_disabled_default_floor_starts() {
    let running = start_with_floor(0)
        .await
        .expect("a floor of 0 disables the sub-floor hold and must start");
    running.shutdown().await.expect("clean shutdown");
}

/// And a legal non-zero floor starts, so an operator who accepts the window
/// can actually run with it.
#[tokio::test]
async fn a_floor_below_min_flush_bytes_starts() {
    let running = start_with_floor(MIN_FLUSH_BYTES - 1)
        .await
        .expect("a floor below min_flush_bytes must start");
    running.shutdown().await.expect("clean shutdown");
}
