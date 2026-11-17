//! Issue #4325 end to end: a store that fails transiently while the server
//! starts delays startup instead of ending it.
//!
//! The startup reads run in `main`'s order through the same library calls
//! `main` makes (`startup_retry::enforce_qualification`,
//! `startup_retry::resolve_tenancy`, `startup_retry::validate_static_provisioning`,
//! `gc_config::bootstrap_at_startup`, then `start_with_heartbeat_and_ipcrypt`,
//! which reads the IPCrypt marker) over one `FaultStore` that fails the
//! `sys/qualification` GET, the spans `s/l0/` listing of provisioning
//! validation and the `sys/ipcrypt/policy` GET transiently a few times each.
//! Time is a gated virtual clock: every retry sleep stops until the test has
//! checked that the health listener's `/readyz` still answers 503.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use ravel_object_store::conformance::{
    CONFORMANCE_SUITE_VERSION, QUALIFICATION_KEY, QualificationRecord,
};
use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, ScriptedFault, Sequence};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions, StoreMetrics};
use ravel_server::config::StoreKind;
use ravel_server::health_listener::{HealthListener, Heartbeat};
use ravel_server::startup_retry::{self, StartupClock, StartupRetry};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{ConfiguredScheme, TenantHash, TenantId};
use tokio::sync::{Semaphore, mpsc};

const IPCRYPT_MARKER_KEY: &str = "sys/ipcrypt/policy";
const QUALIFICATION_FAILURES: usize = 3;
const LISTING_FAILURES: usize = 2;
const MARKER_FAILURES: usize = 2;

/// Virtual time whose every sleep reports itself and then waits for the test
/// to release it.
struct GatedClock {
    now: Mutex<Duration>,
    slept: mpsc::UnboundedSender<Duration>,
    release: Semaphore,
}

impl GatedClock {
    /// The virtual time the sleeps have advanced to.
    fn virtual_now(&self) -> Duration {
        *self.now.lock().unwrap()
    }
}

#[async_trait::async_trait]
impl StartupClock for GatedClock {
    fn elapsed(&self) -> Duration {
        *self.now.lock().unwrap()
    }

    async fn sleep(&self, duration: Duration) {
        *self.now.lock().unwrap() += duration;
        self.slept.send(duration).expect("test is listening");
        self.release
            .acquire()
            .await
            .expect("semaphore open")
            .forget();
    }
}

fn transient(op: Op, key: &str, failures: usize) -> Sequence {
    let mut sequence = Sequence::new(op).with_key_contains(key);
    for i in 0..failures {
        sequence = sequence.then_fault(ScriptedFault::Transient(format!("blip {i}")));
    }
    sequence.then_passthrough()
}

async fn seed_qualification(store: &dyn ObjectStoreBackend) {
    let record = QualificationRecord {
        suite_version: CONFORMANCE_SUITE_VERSION,
        backend_identity: "s3://ravel-test".to_string(),
        qualified_unix_ns: 1,
        passed_properties: vec!["conditional_write_create_if_absent".to_string()],
    };
    store
        .put(
            QUALIFICATION_KEY,
            Bytes::from(serde_json::to_vec(&record).expect("encode")),
            PutOptions::default(),
        )
        .await
        .expect("seed record");
}

fn server_config(
    gc: ravel_maintain::GcConfigValues,
    fold_tenants: Vec<TenantHash>,
) -> ServerConfig {
    let mut tokens = std::collections::HashMap::new();
    tokens.insert("testtoken".to_string(), TenantId::new("acme"));
    ServerConfig {
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
        tenant_resolver: ravel_server::tenant::build_resolver(tokens, false),
        mtls_listener: None,
        fold_tenants,
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
        limits: ravel_server::LimitsConfig::default(),
        max_ingest_lag: ravel_server::DEFAULT_MAX_INGEST_LAG,
        deployment_key: None,
        gc,
        query_deadline: ravel_query::EngineConfig::default().deadline,
        store_probe_interval: Duration::from_secs(3600),
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
        ingest_hold: Default::default(),
        max_pending_strict_acks: 8_192,
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

/// `main`'s startup store reads, in its order, then `start`.
async fn start_like_main(
    store: Arc<dyn ObjectStoreBackend>,
    retry: StartupRetry,
    heartbeat: Heartbeat,
) -> anyhow::Result<ravel_server::Running> {
    startup_retry::enforce_qualification(&retry, store.as_ref(), StoreKind::S3, None).await?;
    let tenancy =
        startup_retry::resolve_tenancy(&retry, store.as_ref(), ConfiguredScheme::Unkeyed, 1)
            .await?;
    ravel_types::install_tenant_hash_scheme(tenancy.scheme)
        .map_err(|_| anyhow::anyhow!("tenant-hash scheme was already installed"))?;
    let fold_tenants = vec![TenantId::new("acme").hash()];
    startup_retry::validate_static_provisioning(
        &retry,
        store.as_ref(),
        &fold_tenants,
        1,
        Mode::All,
        1,
    )
    .await?;
    let gc =
        ravel_server::gc_config::bootstrap_at_startup(store.clone(), 1, Mode::All, &retry).await?;
    ravel_server::start_with_heartbeat_and_ipcrypt(
        server_config(gc, fold_tenants),
        store.clone(),
        store,
        Arc::new(StoreMetrics::default()),
        None,
        heartbeat,
        ravel_server::ipcrypt::IpcryptPolicy::disabled(),
        retry,
    )
    .await
}

async fn status(client: &reqwest::Client, url: String) -> u16 {
    client
        .get(url)
        .send()
        .await
        .expect("request completes")
        .status()
        .as_u16()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transient_store_failures_at_startup_delay_it_and_the_server_then_serves_readyz() {
    let fault = Arc::new(FaultStore::new(
        MemoryStore::new(),
        FaultPlan::empty()
            .with_sequence(transient(
                Op::Get,
                QUALIFICATION_KEY,
                QUALIFICATION_FAILURES,
            ))
            .with_sequence(transient(Op::List, "/s/l0/", LISTING_FAILURES))
            .with_sequence(transient(Op::Get, IPCRYPT_MARKER_KEY, MARKER_FAILURES)),
    ));
    seed_qualification(fault.inner()).await;
    let store: Arc<dyn ObjectStoreBackend> = fault.clone();

    let (slept_tx, mut slept_rx) = mpsc::unbounded_channel();
    let clock = Arc::new(GatedClock {
        now: Mutex::new(Duration::ZERO),
        slept: slept_tx,
        release: Semaphore::new(0),
    });
    let retry = StartupRetry::new(clock.clone(), Duration::from_secs(15), 1);

    // `main` binds this listener later, after sys/gc, so during the earlier
    // reads nothing listens at all; bound here, it shows /readyz is 503 at
    // every retry, the marker retries inside `start` included.
    let heartbeat = Heartbeat::new(Arc::new(ravel_ingest::SystemClock));
    let health = HealthListener::bind("127.0.0.1:0".parse().expect("addr"), heartbeat.clone())
        .expect("health listener binds");
    let health_readyz = format!("http://{}/readyz", health.local_addr());
    let client = reqwest::Client::new();

    let startup = start_like_main(store, retry, heartbeat);
    tokio::pin!(startup);
    let mut retries = 0usize;
    let running = loop {
        tokio::select! {
            Some(_) = slept_rx.recv() => {
                retries += 1;
                assert_eq!(
                    status(&client, health_readyz.clone()).await,
                    503,
                    "/readyz must not be ready during startup retry {retries}"
                );
                clock.release.add_permits(1);
            }
            started = &mut startup => {
                break started.expect("server starts once the store recovers");
            }
        }
    };

    assert_eq!(
        fault.fault_count(Op::Get, FaultKind::Transient) as usize,
        QUALIFICATION_FAILURES + MARKER_FAILURES
    );
    assert_eq!(
        fault.fault_count(Op::List, FaultKind::Transient) as usize,
        LISTING_FAILURES
    );
    // Each sequence consumed its faults plus the passthrough: one attempt
    // more than its failures.
    assert_eq!(
        fault.sequence_progress(0) as usize,
        QUALIFICATION_FAILURES + 1
    );
    assert_eq!(fault.sequence_progress(1) as usize, LISTING_FAILURES + 1);
    assert_eq!(fault.sequence_progress(2) as usize, MARKER_FAILURES + 1);
    assert_eq!(
        retries,
        QUALIFICATION_FAILURES + LISTING_FAILURES + MARKER_FAILURES
    );
    assert!(clock.virtual_now() < Duration::from_secs(15));

    let readyz = format!("http://{}/readyz", running.http_addr);
    assert_eq!(status(&client, readyz).await, 200);
    health.attach_readiness(running.readiness());
    assert_eq!(status(&client, health_readyz).await, 200);

    running.shutdown().await.expect("clean shutdown");
    tokio::task::spawn_blocking(move || health.shutdown())
        .await
        .expect("join")
        .expect("health listener stops");
}
