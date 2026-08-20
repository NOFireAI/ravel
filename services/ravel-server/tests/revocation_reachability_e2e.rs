//! Reachability proof that an operator-
//! driven revoke of a tenant's durable `sys/auth` token actually reaches a
//! RUNNING server's [`ravel_server::lifecycle_refresh::DurableBearerResolver`]
//! within the ADR-0066 decision 6 staleness horizon.
//!
//! `services/ravel-operator/src/controller.rs`'s `reconcile_sys_auth` is the
//! real operator write path, but it is private and `ravel-operator` is
//! outside this task's crate allowlist. This test instead calls the exact
//! `ravel_catalog` primitives that function calls
//! (`upsert_token_owned`/`remove_tokens_by_tenant_owned_by`, both stamped
//! `MANAGED_BY_OPERATOR`) directly against the store, which is the operator's
//! actual write pattern and the only piece of it this crate can reach.
//!
//! `lifecycle_refresh.rs`'s own unit tests already prove `DurableAuthState`
//! resolves and revokes tokens correctly in isolation; none of them boots a
//! real HTTP server. This test drives a real request through a real,
//! running `ravel_server::start()` instance end to end -- the resolver chain
//! actually installed on the query listener, not `DurableAuthState` called
//! directly -- and uses `#[tokio::test(start_paused = true)]` plus
//! `tokio::time::advance` to cross the periodic-refresh horizon
//! deterministically, with no real sleep.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ravel_catalog::MANAGED_BY_OPERATOR;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};

const TENANT: &str = "acme";
const TOKEN: &str = "operator-issued-token";
const DEPLOYMENT_KEY: [u8; 32] = [0x37u8; 32];

fn now_ns() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before epoch")
        .as_nanos() as i64
}

/// A server with no static `--tenant-token` entries at all: the only
/// resolver that can ever authenticate `TOKEN` is the durable
/// `DurableBearerResolver` `ravel_server::start` installs when
/// `deployment_key` is `Some` (ADR-0066 decision 6). A 200 here is therefore
/// a real reachability proof, not a coincidence of some other resolver in
/// the chain matching.
async fn start_keyed_server(store: Arc<dyn ObjectStoreBackend>) -> ravel_server::Running {
    let tenant_resolver = ravel_server::tenant::build_resolver(HashMap::new(), false);
    let config = ServerConfig {
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        adaptive_flush_delay: false,
        max_flush_delay: std::time::Duration::from_secs(2),
        max_flush_delay_idle: std::time::Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
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
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
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

/// Query with `TOKEN` as a bearer token, returning the HTTP status.
async fn query_status(base: &str, client: &reqwest::Client) -> reqwest::StatusCode {
    client
        .get(format!("{base}/api/v1/query"))
        .header("authorization", format!("Bearer {TOKEN}"))
        .query(&[("query", "up")])
        .send()
        .await
        .expect("query request succeeds")
        .status()
}

/// The full reachability chain: seed `sys/auth` the way the operator does,
/// boot a real server, confirm the token authenticates, revoke it the way
/// the operator's reconcile remove-pass does, cross the periodic-refresh
/// horizon with a paused, manually advanced clock, then confirm the SAME
/// token is now rejected -- with no restart and no direct call into
/// `DurableAuthState`.
///
/// Flipped line: `services/ravel-server/src/lifecycle_refresh.rs`'s `spawn`
/// loop's horizon arm, `_ = tokio::time::sleep_until(next_refresh) => { ...
/// refresh_cycle(auth.as_deref(), ...).await }` -- specifically the
/// `auth.refresh(now_ns)` call inside `refresh_cycle` that re-reads
/// `sys/auth` from the store. If that call were removed, or the loop never
/// woke on the horizon (e.g. the horizon arm dropped from the `select!`,
/// mirroring the miss-starvation regression `token_misses_do_not_starve_
/// the_horizon_limits_refresh` guards against in the same file), the
/// in-memory `AuthTokenMap` this process cached at startup would never be
/// replaced, and the revoked token would keep resolving (`AuthResolution::
/// Resolved`) and answering 200 for as long as the process runs -- exactly
/// the bug this test would need to catch to be worth having.
#[tokio::test(start_paused = true)]
async fn operator_revoke_reaches_running_server_within_the_horizon() {
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());

    // Seed sys/auth the way the operator's reconcile upsert pass does: a
    // token owned by "operator", not the CLI or an unmanaged v1 entry.
    ravel_catalog::upsert_token_owned(
        store.as_ref(),
        &DEPLOYMENT_KEY,
        TOKEN.as_bytes(),
        TENANT,
        Some(MANAGED_BY_OPERATOR),
        now_ns(),
    )
    .await
    .expect("operator seeds the tenant's token");

    let running = start_keyed_server(store.clone()).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    // Pre-revoke: the durable resolver, reachable through no other resolver
    // in the chain, authenticates the operator-provisioned token.
    assert_eq!(
        query_status(&base, &client).await,
        200,
        "the operator-provisioned token must authenticate before any revoke"
    );

    // The operator's reconcile remove-pass: revoke exactly this tenant's
    // operator-owned tokens, leaving any CLI-provisioned or unmanaged entry
    // for another tenant untouched (ADR-0072 decision 4 amendment) --
    // irrelevant here since this bucket has only the one entry, but this is
    // the real scoped primitive, not a blunt whole-map wipe.
    ravel_catalog::remove_tokens_by_tenant_owned_by(
        store.as_ref(),
        &DEPLOYMENT_KEY,
        TENANT,
        MANAGED_BY_OPERATOR,
        now_ns(),
    )
    .await
    .expect("operator revokes the tenant's tokens");

    // Cross the periodic-refresh horizon (60s + up to 10% jitter = at most
    // 66s) on a paused, manually advanced clock -- no real sleep. Advanced
    // in small steps with `yield_now` interleaved (mirroring `maintain.rs`'s
    // `discovery_arm_fires_repeatedly_despite_a_shorter_heartbeat`), so the
    // paused-time runtime actually polls and wakes the spawned refresh task
    // between each step rather than one giant jump it might not schedule
    // around.
    for _ in 0..9 {
        tokio::time::advance(Duration::from_secs(10)).await;
        tokio::task::yield_now().await;
    }

    // Post-revoke, post-horizon: the SAME token, through the SAME running
    // process, with no restart, is now rejected.
    assert_eq!(
        query_status(&base, &client).await,
        401,
        "a token removed from sys/auth must stop authenticating once the running server's \
         durable resolver has refreshed past the staleness horizon"
    );

    running.shutdown().await.expect("clean shutdown");
}
