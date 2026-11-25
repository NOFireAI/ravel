//! Issue #4325 end to end: a store that fails transiently while the server
//! starts delays startup instead of ending it.
//!
//! The test calls the function `main` calls for its startup store checks,
//! `startup_retry::run_startup_store_checks`, then
//! `start_with_heartbeat_and_ipcrypt`, which reads the IPCrypt marker. One
//! `FaultStore` fails every retried site a few times: `sys/qualification`
//! (with a transient error, a timeout and a throttle), the retired-reader
//! gate's HEAD of `sys/compat`, `sys/tenancy`, the
//! tenant key-epoch record, the spans `s/l0/` listing of provisioning
//! validation, `sys/gc` and `sys/ipcrypt/policy`. A site that stopped
//! retrying would refuse on its first fault and fail the test.
//!
//! Time is a gated virtual clock: every retry sleep stops until the test
//! releases it. The `--listen-health` listener binds where `main` binds it,
//! after the store checks and before `start`, and at every marker retry
//! inside `start` the test checks that its `/readyz` answers 503. Nothing
//! listens during the retries before that, in `main` or here, so those
//! retries have no `/readyz` to probe.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use ravel_object_store::conformance::{
    CONFORMANCE_SUITE_VERSION, QUALIFICATION_KEY, QualificationRecord,
};
use ravel_object_store::fault::{FaultKind, FaultPlan, FaultStore, Op, ScriptedFault, Sequence};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{KmsRoutingStore, ObjectStoreBackend, PutOptions, StoreMetrics};
use ravel_server::compat_gate::{CompatGateError, GateOutcome, RetiredReader};
use ravel_server::config::StoreKind;
use ravel_server::health_listener::{HealthListener, Heartbeat};
use ravel_server::startup_retry::{
    StartupClock, StartupRetry, StartupStoreInputs, StartupStoreState, run_startup_store_checks,
};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{ConfiguredScheme, TenantHash, TenantId};
use tokio::sync::{Semaphore, mpsc};

const IPCRYPT_MARKER_KEY: &str = "sys/ipcrypt/policy";
const COMPAT_KEY: &str = "sys/compat";
const TENANCY_KEY: &str = "sys/tenancy";
const GC_KEY: &str = "sys/gc";
const EPOCH_KEY: &str = "/enc";
const LISTING_KEY: &str = "/s/l0/";
const TENANCY_FAILURES: usize = 1;
const EPOCH_FAILURES: usize = 1;
const LISTING_FAILURES: usize = 2;
const GC_FAILURES: usize = 1;
const MARKER_FAILURES: usize = 2;
const COMPAT_FAILURES: usize = 1;
/// A retired set, so the gate reads the store: `RETIRED_READERS` is empty in
/// this build and sends no request.
const RETIRED: &[RetiredReader] = &[RetiredReader {
    family: "rseg",
    oldest_readable: 8,
}];
/// The qualification read fails once with each retryable kind.
fn qualification_faults() -> Vec<ScriptedFault> {
    vec![
        ScriptedFault::Transient("blip".into()),
        ScriptedFault::Timeout,
        ScriptedFault::Throttled {
            retry_after_ms: 1000,
        },
    ]
}

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

    /// No store call here hangs, so no attempt is ever cut off.
    async fn attempt_timer(&self, _duration: Duration) {
        std::future::pending::<()>().await;
    }
}

fn faults(op: Op, key: &str, faults: Vec<ScriptedFault>) -> Sequence {
    let mut sequence = Sequence::new(op).with_key_contains(key);
    for fault in faults {
        sequence = sequence.then_fault(fault);
    }
    sequence.then_passthrough()
}

fn transient(op: Op, key: &str, failures: usize) -> Sequence {
    faults(
        op,
        key,
        (0..failures)
            .map(|i| ScriptedFault::Transient(format!("blip {i}")))
            .collect(),
    )
}

fn kms_store() -> KmsRoutingStore {
    KmsRoutingStore::new(
        Arc::new(MemoryStore::new()),
        ravel_object_store::s3::S3Config {
            bucket: "ravel-test".to_string(),
            region: "us-east-1".to_string(),
            endpoint: Some("http://localhost:0".to_string()),
            access_key_id: "test".to_string(),
            secret_access_key: "test".to_string(),
            allow_http: true,
            force_path_style: true,
            kms_key_id: None,
            session_token: None,
            credentials_file: None,
            auth: Default::default(),
            instance_metadata_endpoint: None,
        },
        ravel_object_store::s3::S3HttpConfig::default(),
        Arc::new(StoreMetrics::default()),
    )
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
        emit_declared_stat_aggregates: true,
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

async fn status(client: &reqwest::Client, url: String) -> u16 {
    client
        .get(url)
        .send()
        .await
        .expect("request completes")
        .status()
        .as_u16()
}

/// Drive `future` to completion, releasing every retry sleep it reports.
/// `at_retry` runs before each release; returns the result and the number
/// of retries.
async fn drive<T>(
    future: impl std::future::Future<Output = T>,
    clock: &GatedClock,
    slept: &mut mpsc::UnboundedReceiver<Duration>,
    mut at_retry: impl AsyncFnMut(usize),
) -> (T, usize) {
    tokio::pin!(future);
    let mut retries = 0usize;
    loop {
        tokio::select! {
            Some(_) = slept.recv() => {
                retries += 1;
                at_retry(retries).await;
                clock.release.add_permits(1);
            }
            done = &mut future => return (done, retries),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transient_store_failures_at_startup_delay_it_and_the_server_then_serves_readyz() {
    let fault = Arc::new(FaultStore::new(
        MemoryStore::new(),
        FaultPlan::empty()
            .with_sequence(faults(Op::Get, QUALIFICATION_KEY, qualification_faults()))
            .with_sequence(transient(Op::Get, TENANCY_KEY, TENANCY_FAILURES))
            .with_sequence(transient(Op::Get, EPOCH_KEY, EPOCH_FAILURES))
            .with_sequence(transient(Op::List, LISTING_KEY, LISTING_FAILURES))
            .with_sequence(transient(Op::Get, GC_KEY, GC_FAILURES))
            .with_sequence(transient(Op::Get, IPCRYPT_MARKER_KEY, MARKER_FAILURES))
            // A second passthrough step, so a second run of the gate would
            // show as progress 3.
            .with_sequence(transient(Op::Head, COMPAT_KEY, COMPAT_FAILURES).then_passthrough()),
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

    let kms = kms_store();
    let kms_config = ravel_server::tenant_kms::parse_tenant_kms_config(
        "[tenants]\nacme = \"arn:aws:kms:us-east-1:111122223333:key/acme\"\n",
    )
    .expect("valid config");
    let checks = run_startup_store_checks(
        &retry,
        StartupStoreInputs {
            store: &store,
            store_kind: StoreKind::S3,
            backend_identity: None,
            require_bucket_protection: false,
            store_s3: None,
            tenancy: ConfiguredScheme::Unkeyed,
            retired_readers: RETIRED,
            tenant_kms: Some((&kms, &kms_config)),
            shard_count: 1,
            mode: Mode::All,
        },
        || vec![TenantId::new("acme").hash()],
        || 1,
    );
    let (checked, check_retries) = drive(checks, &clock, &mut slept_rx, async |_| {}).await;
    let StartupStoreState {
        retired_reader_gate,
        static_tenants,
        gc,
        ..
    } = checked.expect("the startup store checks pass once the store recovers");
    // The gate read the store as fresh, with no `sys/tenancy`: it ran before
    // tenancy pinning wrote the marker, which would have made it refuse.
    assert_eq!(retired_reader_gate, GateOutcome::Fresh);
    // One gate run: the faulted HEAD and its one retry, no third HEAD.
    assert_eq!(fault.sequence_progress(6) as usize, COMPAT_FAILURES + 1);
    assert_eq!(
        fault.fault_count(Op::Head, FaultKind::Transient) as usize,
        COMPAT_FAILURES
    );
    assert_eq!(
        check_retries,
        qualification_faults().len()
            + COMPAT_FAILURES
            + TENANCY_FAILURES
            + EPOCH_FAILURES
            + LISTING_FAILURES
            + GC_FAILURES
    );

    // Bound where `main` binds it: after the store checks, before `start`.
    let heartbeat = Heartbeat::new(Arc::new(ravel_ingest::SystemClock));
    let health = HealthListener::bind("127.0.0.1:0".parse().expect("addr"), heartbeat.clone())
        .expect("health listener binds");
    let health_readyz = format!("http://{}/readyz", health.local_addr());
    let client = reqwest::Client::new();

    let startup = ravel_server::start_with_heartbeat_and_ipcrypt(
        server_config(gc, static_tenants),
        store.clone(),
        store,
        Arc::new(StoreMetrics::default()),
        None,
        heartbeat,
        ravel_server::ipcrypt::IpcryptPolicy::disabled(),
        retry,
    );
    let (started, marker_retries) = drive(startup, &clock, &mut slept_rx, async |retry| {
        assert_eq!(
            status(&client, health_readyz.clone()).await,
            503,
            "/readyz must not be ready during marker retry {retry}"
        );
    })
    .await;
    let running = started.expect("server starts once the store recovers");
    assert_eq!(marker_retries, MARKER_FAILURES);

    assert_eq!(fault.fault_count(Op::Get, FaultKind::Timeout), 1);
    assert_eq!(fault.fault_count(Op::Get, FaultKind::Throttled), 1);
    assert_eq!(
        fault.fault_count(Op::Get, FaultKind::Transient) as usize,
        1 + TENANCY_FAILURES + EPOCH_FAILURES + GC_FAILURES + MARKER_FAILURES
    );
    assert_eq!(
        fault.fault_count(Op::List, FaultKind::Transient) as usize,
        LISTING_FAILURES
    );
    // Each sequence consumed its faults plus the passthrough: one attempt
    // more than its failures.
    let expected_progress = [
        qualification_faults().len(),
        TENANCY_FAILURES,
        EPOCH_FAILURES,
        LISTING_FAILURES,
        GC_FAILURES,
        MARKER_FAILURES,
    ];
    for (index, failures) in expected_progress.into_iter().enumerate() {
        assert_eq!(
            fault.sequence_progress(index) as usize,
            failures + 1,
            "sequence {index}"
        );
    }
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

/// A gate refusal ends the checks on its first attempt, before tenancy
/// pinning or any later check runs: no retry sleep, `sys/tenancy` is the one
/// seeded, and no `sys/gc` is written.
#[tokio::test]
async fn a_retired_reader_gate_refusal_refuses_at_once_before_tenancy() {
    let fault = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    seed_qualification(fault.inner()).await;
    fault
        .inner()
        .put(
            TENANCY_KEY,
            Bytes::from_static(b"marker"),
            PutOptions::default(),
        )
        .await
        .expect("seed sys/tenancy");
    let store: Arc<dyn ObjectStoreBackend> = fault.clone();

    let (slept_tx, mut slept_rx) = mpsc::unbounded_channel();
    let clock = Arc::new(GatedClock {
        now: Mutex::new(Duration::ZERO),
        slept: slept_tx,
        release: Semaphore::new(0),
    });
    let retry = StartupRetry::new(clock.clone(), Duration::from_secs(15), 1);
    let checks = run_startup_store_checks(
        &retry,
        StartupStoreInputs {
            store: &store,
            store_kind: StoreKind::S3,
            backend_identity: None,
            require_bucket_protection: false,
            store_s3: None,
            tenancy: ConfiguredScheme::Unkeyed,
            retired_readers: RETIRED,
            tenant_kms: None,
            shard_count: 1,
            mode: Mode::All,
        },
        Vec::new,
        || 1,
    );
    let (checked, retries) = drive(checks, &clock, &mut slept_rx, async |_| {}).await;
    let Err(err) = checked else {
        panic!("sys/tenancy without sys/compat refuses");
    };
    assert_eq!(
        err.to_string(),
        "retired-reader gate (sys/compat) refused startup"
    );
    assert!(matches!(
        err.downcast_ref::<CompatGateError>(),
        Some(CompatGateError::TenancyWithoutCompat)
    ));
    assert_eq!(retries, 0);
    assert_eq!(
        fault
            .inner()
            .get(TENANCY_KEY, ravel_object_store::GetRange::Full)
            .await
            .expect("the seeded marker")
            .data,
        Bytes::from_static(b"marker")
    );
    assert!(matches!(
        fault.inner().head(GC_KEY).await,
        Err(ravel_object_store::StoreError::NotFound)
    ));
}
