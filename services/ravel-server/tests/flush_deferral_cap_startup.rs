//! Startup validation of the flush cadence against the read-side scan slack
//! (ADR-0076 decision 4, ADR-1642 deferral cap amendment), through the library
//! entry point.
//!
//! A cadence that spends the whole `FLUSH_BOUND_SLACK_HOURS` slack leaves the
//! flush deferral cap at 0, and a shard whose flush queue filled would then
//! refuse every write from the first trigger it defers. `start` refuses that
//! cadence for a `ServerConfig` built in code, in every mode, with the same
//! typed error `Cli::validate` returns; one second below the limit starts.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use ravel_object_store::memory::MemoryStore;
use ravel_server::{FlushCadenceError, FoldTaskConfig, Mode, ServerConfig};
use ravel_types::TenantId;

const TOKEN: &str = "testtoken";

/// A `ServerConfig` with a 1 s `max_flush_delay`, `max_flush_delay_idle` set
/// to `idle`, and everything else at the shipped cadence.
fn config_with_idle(mode: Mode, idle: Duration) -> ServerConfig {
    config_with_cadence(mode, Duration::from_secs(1), idle, false)
}

/// A `ServerConfig` with the given flush cadence and everything else at the
/// shipped defaults.
fn config_with_cadence(
    mode: Mode,
    fast: Duration,
    idle: Duration,
    adaptive_flush_delay: bool,
) -> ServerConfig {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new("acme"));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    ServerConfig {
        audit_pipeline: Default::default(),
        audit_text: Default::default(),
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        max_queued_flushes: 8,
        adaptive_flush_delay,
        max_flush_delay: fast,
        max_flush_delay_idle: idle,
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
    }
}

async fn start_with_idle(mode: Mode, idle: Duration) -> anyhow::Result<ravel_server::Running> {
    start_config(config_with_idle(mode, idle)).await
}

async fn start_config(config: ServerConfig) -> anyhow::Result<ravel_server::Running> {
    let store = Arc::new(MemoryStore::new());
    ravel_server::start(
        config,
        store.clone(),
        store.clone(),
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
}

/// A 3600 s idle delay plus the 3600 s flush lifetime is exactly the 7200 s
/// slack, which the slack check admits at equality, and leaves a deferral cap
/// of 0 even before the trigger bound adds its flush tick. Refused in a writing mode and in a
/// mode that builds no ingest router, since the CLI refuses the flags in every
/// mode.
#[tokio::test]
async fn a_cadence_leaving_no_deferral_cap_refuses_startup() {
    for mode in [Mode::All, Mode::Query] {
        let err = start_with_idle(mode, Duration::from_secs(3600))
            .await
            .err()
            .expect("a cadence leaving no deferral cap must refuse startup");
        assert!(
            matches!(
                err.downcast_ref::<FlushCadenceError>(),
                Some(FlushCadenceError::ZeroFlushDeferralCap { .. })
            ),
            "expected FlushCadenceError::ZeroFlushDeferralCap in {mode:?}, got: {err:#}"
        );
        let msg = format!("{err:#}");
        assert!(
            msg.contains("flush deferral cap at 0")
                && msg.contains("--max-flush-delay-idle")
                && msg.contains("--max-flush-delay ")
                && msg.contains("max_flush_lifetime")
                && msg.contains("FLUSH_BOUND_SLACK_HOURS"),
            "expected the deferral cap error naming the flags and terms, got: {msg}"
        );
    }
}

/// One second below the limit leaves 0.8 s of cap (1 s less the 200 ms flush
/// tick) and starts.
#[tokio::test]
async fn a_cadence_one_second_below_the_limit_starts() {
    let running = start_with_idle(Mode::All, Duration::from_secs(3599))
        .await
        .expect("a cadence leaving a positive deferral cap must start");
    running.shutdown().await.expect("clean shutdown");
}

/// An idle delay whose sum with the flush lifetime exceeds the slack is
/// refused by the slack check, which runs first, with its own variant.
#[tokio::test]
async fn an_idle_delay_past_the_scan_slack_refuses_startup() {
    let err = start_with_idle(Mode::All, Duration::from_secs(3601))
        .await
        .err()
        .expect("an idle delay past the scan slack must refuse startup");
    assert!(
        matches!(
            err.downcast_ref::<FlushCadenceError>(),
            Some(FlushCadenceError::FlushBoundExceedsSlack { .. })
        ),
        "expected FlushCadenceError::FlushBoundExceedsSlack, got: {err:#}"
    );
    let msg = format!("{err:#}");
    assert!(
        msg.contains("--max-flush-delay-idle") && msg.contains("FLUSH_BOUND_SLACK_HOURS"),
        "expected the slack error naming the flag and the slack, got: {msg}"
    );
}

/// The adaptive corridor's widest threshold, `max_flush_delay` plus
/// `STRICT_VISIBILITY_RESERVE_NS` (0.5 s) less `put_retry_base_delay` (0.1 s),
/// enters the trigger bound only with `adaptive_flush_delay` on. With fast =
/// idle = 3599.5 s that threshold is 3599.9 s, plus the 0.2 s flush tick is
/// 3600.1 s against the 3600 s the slack leaves after the flush lifetime: cap
/// 0, refused. With adaptive off the bound is 3599.7 s and the 0.3 s cap
/// starts. Without the reserve the widest threshold, 3599.4 s, falls below
/// fast and the adaptive case starts too, so this pins both terms. The CLI
/// cannot reach this cadence: its `MAX_STRICT_VISIBILITY_BUDGET_NS` check
/// refuses a 3599.5 s `--max-flush-delay` first.
#[tokio::test]
async fn the_adaptive_corridor_counts_against_the_deferral_cap() {
    let delay = Duration::from_millis(3_599_500);
    let err = start_config(config_with_cadence(Mode::All, delay, delay, true))
        .await
        .err()
        .expect("an adaptive cadence leaving no deferral cap must refuse startup");
    assert!(
        matches!(
            err.downcast_ref::<FlushCadenceError>(),
            Some(FlushCadenceError::ZeroFlushDeferralCap {
                adaptive_flush_delay: true,
                ..
            })
        ),
        "expected FlushCadenceError::ZeroFlushDeferralCap with the adaptive flag, got: {err:#}"
    );

    let running = start_config(config_with_cadence(Mode::All, delay, delay, false))
        .await
        .expect("the same cadence with adaptive off leaves a positive cap and must start");
    running.shutdown().await.expect("clean shutdown");
}
