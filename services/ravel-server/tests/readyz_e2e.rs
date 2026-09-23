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
//! These tests all mutate that one process-global flag (and the probe's
//! process-global liveness gauge), so they serialize on a shared async lock to
//! keep their windows from interleaving.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use ravel_ingest::Clock;
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{
    Capabilities, DelimitedList, GetOutcome, GetRange, ListPage, ObjectMeta, ObjectStoreBackend,
    PageToken, PutOptions, PutOutcome, StoreError,
};
use ravel_server::store_probe::{self, K, ProbeHysteresis};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::TenantId;
use tokio::sync::Mutex;

/// Serializes the probe tests: they all drive the one process-global
/// reachability flag and liveness gauge, so their windows must not interleave.
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
        audit_pipeline: Default::default(),
        audit_text: Default::default(),
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        max_queued_flushes: 8,
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
        max_ingest_lag: ravel_server::DEFAULT_MAX_INGEST_LAG,
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
        store_probe::run_probe_cycle(store.as_ref(), &mut hysteresis, &ravel_ingest::SystemClock)
            .await;
        assert_eq!(
            status(&client, &base, "/readyz").await,
            200,
            "readyz must still be 200 after {i} consecutive failed probes (< K={K})"
        );
    }

    // The K-th consecutive failure flips readiness to 503.
    store_probe::run_probe_cycle(store.as_ref(), &mut hysteresis, &ravel_ingest::SystemClock).await;
    assert_eq!(
        status(&client, &base, "/readyz").await,
        503,
        "readyz must be 503 after K={K} consecutive failed probes"
    );

    // A single successful probe recovers readiness immediately.
    fail_gets.store(false, Ordering::SeqCst);
    store_probe::run_probe_cycle(store.as_ref(), &mut hysteresis, &ravel_ingest::SystemClock).await;
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
        store_probe::run_probe_cycle(store.as_ref(), &mut hysteresis, &ravel_ingest::SystemClock)
            .await;
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
    store_probe::run_probe_cycle(store.as_ref(), &mut hysteresis, &ravel_ingest::SystemClock).await;
    assert_eq!(status(&client, &base, "/readyz").await, 200);

    running.shutdown().await.expect("graceful shutdown");
}

/// A clock a test advances by hand, so the probe's liveness gauge can be
/// pinned to an exact value and driven stale deterministically, with no
/// wall-clock sleep.
#[derive(Clone)]
struct TestClock(Arc<AtomicI64>);

impl TestClock {
    fn at(now_ns: i64) -> TestClock {
        TestClock(Arc::new(AtomicI64::new(now_ns)))
    }

    fn advance(&self, by: Duration) {
        let by_ns = i64::try_from(by.as_nanos()).expect("test duration fits i64");
        self.0.fetch_add(by_ns, Ordering::SeqCst);
    }
}

impl Clock for TestClock {
    fn now_ns(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

/// Issue #1728: `ravel_store_reachable` alone reads healthy forever once
/// nothing updates it, because it (like `ravel_store_probe_failures_total`)
/// is written only while the probe task is alive. This is the exact
/// false-healthy state a dead probe task produces, and the one signal that
/// exposes it: `ravel_store_probe_last_run_timestamp_seconds` going stale
/// while `store_reachable()` still answers `true`.
#[tokio::test]
async fn store_probe_last_run_gauge_goes_stale_while_readyz_stays_green() {
    let _guard = PROBE_TEST_LOCK.lock().await;

    let (toggle, fail_gets) = ToggleFailStore::new();
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(toggle);
    let mut hysteresis = ProbeHysteresis::new();
    let clock = TestClock::at(1_700_000_000_000_000_000);

    // One successful cycle: the gauge must equal the injected clock's exact
    // value, not merely a non-zero or SystemTime::now() value (WRONG-2).
    store_probe::run_probe_cycle(store.as_ref(), &mut hysteresis, &clock).await;
    assert_eq!(
        store_probe::probe_last_run_unix_ns(),
        1_700_000_000_000_000_000,
        "gauge must equal the injected clock's exact value after one cycle"
    );
    assert!(store_probe::store_reachable(), "starts reachable");

    // Advance the clock with NO new cycle driven. This is the exact
    // false-healthy state: store_reachable() still says true, but the
    // liveness gauge is now stale, which is what a dead probe task looks like
    // from outside.
    clock.advance(Duration::from_secs(600));
    assert_eq!(
        store_probe::probe_last_run_unix_ns(),
        1_700_000_000_000_000_000,
        "gauge must not move on its own; only a completed cycle advances it"
    );
    assert!(
        store_probe::store_reachable(),
        "store_reachable() must still read true here: that is the false-healthy \
         state readable only through the liveness gauge's age, not its value"
    );

    // A FAILING cycle still advances the gauge (WRONG-1 sets it only on the
    // success branch, which would leave it stuck at the earlier value here).
    fail_gets.store(true, Ordering::SeqCst);
    for _ in 0..K {
        store_probe::run_probe_cycle(store.as_ref(), &mut hysteresis, &clock).await;
    }
    assert!(
        !store_probe::store_reachable(),
        "K consecutive failures must flip reachability"
    );
    assert_eq!(
        store_probe::probe_last_run_unix_ns(),
        clock.now_ns(),
        "a failing cycle must still advance the liveness gauge to the clock's current value"
    );

    // Recover, and reset the global for any later test in this process.
    fail_gets.store(false, Ordering::SeqCst);
    store_probe::run_probe_cycle(store.as_ref(), &mut hysteresis, &clock).await;
    assert!(store_probe::store_reachable(), "a single success recovers");
}

/// Issue #1728: `store_probe::spawn` stamps the liveness gauge before the
/// loop's first sleep, so `0` means only "no probe task was ever spawned in
/// this process". Without that stamp the gauge holds `0` for a whole jittered
/// interval plus one cycle after every start, and a task that dies inside that
/// window holds it forever, which no alert expression can tell apart from a
/// process that never spawned a probe. The interval here is long enough that
/// no cycle can complete during the test, so the value asserted can only have
/// come from the spawn stamp itself, and the clock is injected so the
/// assertion is an exact value rather than a wall-clock band.
#[tokio::test]
async fn store_probe_spawn_stamps_the_liveness_gauge_before_any_cycle_runs() {
    let _guard = PROBE_TEST_LOCK.lock().await;

    // A value no other test in this binary stamps, so a stale global cannot
    // pass this assertion for it.
    const SPAWN_NS: i64 = 1_600_000_000_123_456_789;

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let clock = TestClock::at(SPAWN_NS);
    let task = store_probe::spawn_with_clock(
        store,
        Duration::from_secs(86_400),
        Arc::new(clock.clone()) as Arc<dyn Clock>,
    );

    assert_eq!(
        store_probe::probe_last_run_unix_ns(),
        SPAWN_NS,
        "spawn must stamp the liveness gauge from the injected clock before the \
         task's first sleep, so the gauge is never left at its never-spawned 0 \
         by a process that did spawn a probe"
    );

    // The stamp is not a cycle: nothing ran, so reachability is untouched and
    // the gauge does not move on its own while the clock advances.
    clock.advance(Duration::from_secs(3_600));
    assert_eq!(
        store_probe::probe_last_run_unix_ns(),
        SPAWN_NS,
        "only a completed cycle (or a spawn) advances the gauge; a probe that \
         dies before its first cycle must age out from its spawn stamp"
    );

    task.shutdown().await;
}

/// Issue #1728: the nanoseconds-to-seconds conversion in
/// `render_store_probe_family` is what the gauge's name and every alert
/// threshold in docs/guides/observability.md rest on, and nothing pinned its
/// exact rendered value. `metrics_endpoint.rs`'s gauge test only asserts the
/// metric name is present on the scrape, never the value, and
/// `store_probe_last_run_gauge_goes_stale_while_readyz_stays_green` above only
/// asserts the raw nanosecond atomic, which never goes through that
/// conversion. This drives one cycle with a pinned `TestClock`, scrapes a
/// real `/metrics` response, and asserts the exact rendered seconds string.
#[tokio::test]
async fn metrics_store_probe_last_run_gauge_renders_exact_seconds_for_pinned_clock() {
    let _guard = PROBE_TEST_LOCK.lock().await;

    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let running = start_server(store.clone()).await;
    let base = format!("http://{}", running.http_addr);
    let client = reqwest::Client::new();

    let mut hysteresis = ProbeHysteresis::new();
    let clock = TestClock::at(1_700_000_000_500_000_000);
    store_probe::run_probe_cycle(store.as_ref(), &mut hysteresis, &clock).await;

    let body = client
        .get(format!("{base}/metrics"))
        .send()
        .await
        .expect("metrics request completes")
        .text()
        .await
        .expect("metrics body is text");

    assert!(
        body.contains("ravel_store_probe_last_run_timestamp_seconds{mode=\"all\"} 1700000000.5\n"),
        "gauge must render the injected clock's exact seconds value (1700000000.5) for the \
         pinned ns value 1_700_000_000_500_000_000; a wrong divisor would render a value off by \
         a factor of 1000 instead:\n{body}"
    );

    running.shutdown().await.expect("graceful shutdown");
}
