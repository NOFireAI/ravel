//! `/readyz` reflects store reachability with hysteresis, and `/healthz`
//! (liveness) never follows it down (ADR-0050 section 7, EC7).
//!
//! The probe's counting logic is unit-tested in isolation
//! (`store_probe::ProbeHysteresis`); these tests drive the cycles deterministically
//! against a real, running server, controlling each cycle via
//! `store_probe::run_probe_cycle` rather than sleeping through real
//! `--store-probe-interval` windows. The server's own background probe is
//! configured with a very long interval so it never ticks mid-test, leaving the
//! process-global reachability flag written only by the cycles these tests
//! drive explicitly.
//!
//! The two tests both mutate that one process-global flag, so they serialize on
//! a shared async lock to keep their windows from interleaving.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_server::store_probe::{self, K, ProbeHysteresis};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::TenantId;
use tokio::sync::Mutex;

/// Serializes the two tests: both drive the one process-global reachability
/// flag, so their outage windows must not interleave.
static PROBE_TEST_LOCK: Mutex<()> = Mutex::const_new(());

/// A `MemoryStore` whose GETs can be flipped to a hard error at runtime, to
/// simulate the object store becoming unreachable. Every other operation
/// delegates, so only reachability (which the probe measures with a GET) is
/// affected.
struct ToggleFailStore {
    inner: MemoryStore,
    fail_gets: Arc<AtomicBool>,
}

impl ToggleFailStore {
    fn new() -> (Self, Arc<AtomicBool>) {
        let fail_gets = Arc::new(AtomicBool::new(false));
        (
            ToggleFailStore {
                inner: MemoryStore::new(),
                fail_gets: fail_gets.clone(),
            },
            fail_gets,
        )
    }
}

#[async_trait]
impl ObjectStoreBackend for ToggleFailStore {
    async fn put(
        &self,
        key: &str,
        data: Bytes,
        opts: PutOptions,
    ) -> Result<PutOutcome, StoreError> {
        self.inner.put(key, data, opts).await
    }

    async fn get(&self, key: &str, range: GetRange) -> Result<GetOutcome, StoreError> {
        if self.fail_gets.load(Ordering::SeqCst) {
            return Err(StoreError::Transient("injected store outage".to_string()));
        }
        self.inner.get(key, range).await
    }

    async fn head(&self, key: &str) -> Result<ObjectMeta, StoreError> {
        self.inner.head(key).await
    }

    async fn list(&self, prefix: &str, page: Option<PageToken>) -> Result<ListPage, StoreError> {
        self.inner.list(prefix, page).await
    }

    async fn list_delimited(&self, prefix: &str) -> Result<DelimitedList, StoreError> {
        self.inner.list_delimited(prefix).await
    }

    async fn delete(&self, key: &str) -> Result<(), StoreError> {
        self.inner.delete(key).await
    }

    fn capabilities(&self) -> Capabilities {
        self.inner.capabilities()
    }
}

/// Start an in-process server over a real socket, backed by `store`, with the
/// background probe interval set long enough that it never ticks during a test.
async fn start_server(store: Arc<dyn ObjectStoreBackend>) -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert("testtoken".to_string(), TenantId::new("acme"));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let config = ServerConfig {
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
        deployment_key: None,
        gc: ravel_maintain::GcConfigValues::maintain_defaults(),
        query_deadline: ravel_query::EngineConfig::default().deadline,
        // Long enough never to tick during the test: the cycles are driven
        // explicitly via `run_probe_cycle` for determinism.
        store_probe_interval: Duration::from_secs(3600),
        admission_reconcile_interval: ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL,
        query_concurrency_limit: ravel_query::QueryConcurrencyLimit::Unlimited,
        max_s3_requests: ravel_query::EngineConfig::default().max_s3_requests,
        scrub_period: std::time::Duration::from_secs(7 * 86_400),
        indexed_fields: Default::default(),
        disable_cache: false,
        cache_max_bytes: 256 * 1024 * 1024,
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: std::time::Duration::from_secs(3600),
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

async fn status(client: &reqwest::Client, base: &str, path: &str) -> u16 {
    client
        .get(format!("{base}{path}"))
        .send()
        .await
        .expect("request completes")
        .status()
        .as_u16()
}

/// The named acceptance test: `/readyz` is 200 once startup completes, flips to
/// 503 only after K consecutive failed probes (not after one), then recovers to
/// 200 on a single subsequent success.
#[tokio::test]
async fn readyz_goes_unready_when_store_unreachable() {
    let _guard = PROBE_TEST_LOCK.lock().await;

    let (toggle, fail_gets) = ToggleFailStore::new();
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(toggle);
    let running = start_server(store.clone()).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    // Startup completed and the store is reachable: /readyz is 200.
    assert_eq!(
        status(&client, &base, "/readyz").await,
        200,
        "readyz must be 200 once startup completes and the store is reachable"
    );

    // The store becomes unreachable. Drive probe cycles deterministically.
    fail_gets.store(true, Ordering::SeqCst);
    let mut hysteresis = ProbeHysteresis::new();

    // The first K-1 failures must NOT flip readiness (asymmetric hysteresis:
    // flips only AT K, not after a single blip).
    for i in 1..K {
        store_probe::run_probe_cycle(store.as_ref(), &mut hysteresis).await;
        assert_eq!(
            status(&client, &base, "/readyz").await,
            200,
            "readyz must still be 200 after {i} consecutive failed probes (< K={K})"
        );
    }

    // The K-th consecutive failure flips readiness to 503.
    store_probe::run_probe_cycle(store.as_ref(), &mut hysteresis).await;
    assert_eq!(
        status(&client, &base, "/readyz").await,
        503,
        "readyz must be 503 after K={K} consecutive failed probes"
    );

    // A single successful probe recovers readiness immediately.
    fail_gets.store(false, Ordering::SeqCst);
    store_probe::run_probe_cycle(store.as_ref(), &mut hysteresis).await;
    assert_eq!(
        status(&client, &base, "/readyz").await,
        200,
        "a single successful probe must recover readyz to 200 immediately"
    );

    running.shutdown().await.expect("graceful shutdown");
}

/// Companion: liveness must never follow readiness down. Across the exact same
/// store outage that drives `/readyz` to 503, `/healthz` stays 200 throughout,
/// so a store outage never gets healthy processes killed and restarted
/// (ADR-0050 section 7's two documented objections).
#[tokio::test]
async fn healthz_stays_200_during_store_outage() {
    let _guard = PROBE_TEST_LOCK.lock().await;

    let (toggle, fail_gets) = ToggleFailStore::new();
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(toggle);
    let running = start_server(store.clone()).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    assert_eq!(status(&client, &base, "/healthz").await, 200);

    // Drive a full outage past K, checking healthz at each step.
    fail_gets.store(true, Ordering::SeqCst);
    let mut hysteresis = ProbeHysteresis::new();
    for _ in 0..=K {
        store_probe::run_probe_cycle(store.as_ref(), &mut hysteresis).await;
        assert_eq!(
            status(&client, &base, "/healthz").await,
            200,
            "healthz (liveness) must stay 200 during a store outage, never follow readiness down"
        );
    }
    // Readiness is down at this point; liveness is not.
    assert_eq!(status(&client, &base, "/readyz").await, 503);
    assert_eq!(status(&client, &base, "/healthz").await, 200);

    // Recover, and reset the global for any later test in this process.
    fail_gets.store(false, Ordering::SeqCst);
    store_probe::run_probe_cycle(store.as_ref(), &mut hysteresis).await;
    assert_eq!(status(&client, &base, "/readyz").await, 200);

    running.shutdown().await.expect("graceful shutdown");
}
