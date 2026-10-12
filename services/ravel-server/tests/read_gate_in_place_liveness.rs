//! Liveness under `--cpu-gate-read-dispatch in-place` (issue #2795, ADR-1702
//! dispatch amendment). In-place dispatch runs a gated read job on the worker
//! that asked, through `tokio::task::block_in_place`, which hands that
//! worker's queued tasks to another thread for the job's duration. So with
//! read jobs spinning on every worker of the main runtime, the main router's
//! `/healthz` still answers. A job run inline on the worker without
//! `block_in_place` would hold every worker, and the probe would wait for the
//! spin to end.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use clap::Parser;
use ravel_cpu_gate::{Dispatch, JobSize, ReadSite};
use ravel_object_store::memory::MemoryStore;
use ravel_server::config::{Cli, CpuGatePermits, HostProfile};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::TenantId;

/// Main-runtime workers, and read gate permits: one spinning job per worker.
const WORKERS: usize = 2;
/// How long each gated job spins unless the test stops it first.
const SPIN: Duration = Duration::from_secs(2);
/// The longest a `/healthz` probe may take while the jobs spin.
const PROBE_BOUND: Duration = Duration::from_millis(250);

async fn start_server(cpu_gate_permits: CpuGatePermits) -> ravel_server::Running {
    let mut tokens = HashMap::new();
    tokens.insert("testtoken".to_string(), TenantId::new("acme"));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let store = Arc::new(MemoryStore::new());
    let config = ServerConfig {
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Aged,
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
        cpu_gate_permits,
        max_pending_strict_acks: 8_192,
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

/// One `GET /healthz` over a blocking socket, so the client side depends on
/// no runtime thread. Returns how long the full response took.
fn probe_healthz(addr: SocketAddr) -> Duration {
    let started = Instant::now();
    let mut stream = TcpStream::connect(addr).expect("connect to the HTTP listener");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("set read timeout");
    stream
        .write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .expect("send the probe");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("read the probe response");
    let took = started.elapsed();
    assert!(
        response.starts_with("HTTP/1.1 200"),
        "/healthz must answer 200, got: {response:?}"
    );
    took
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn healthz_answers_while_in_place_read_jobs_spin_on_every_worker() {
    let cli = Cli::try_parse_from([
        "ravel-server",
        "--cpu-gate-read-dispatch",
        "in-place",
        "--cpu-gate-read-permits",
        "2",
    ])
    .expect("flags parse");
    let permits =
        cli.resolve_cpu_gate_permits(HostProfile::new(WORKERS, None, None, None, None, None));
    assert_eq!(permits.read_dispatch, Dispatch::InPlace);
    let running = start_server(permits).await;
    let gate = running.read_gate();
    assert_eq!(
        (gate.dispatch(), gate.permits()),
        (Dispatch::InPlace, WORKERS),
        "the flags reach the server's read gate"
    );
    let addr = running.http_addr;
    probe_healthz(addr);

    let began = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let jobs: Vec<_> = (0..WORKERS)
        .map(|_| {
            let gate = Arc::clone(&gate);
            let began = Arc::clone(&began);
            let stop = Arc::clone(&stop);
            tokio::spawn(async move {
                let caller = std::thread::current().id();
                let ran_on = gate
                    .run(ReadSite::LogBlock, JobSize::Bytes(u64::MAX), move || {
                        began.fetch_add(1, Ordering::AcqRel);
                        let spinning = Instant::now();
                        while !stop.load(Ordering::Acquire) && spinning.elapsed() < SPIN {
                            std::hint::spin_loop();
                        }
                        std::thread::current().id()
                    })
                    .await
                    .expect("the spinning job returns");
                (caller, ran_on)
            })
        })
        .collect();

    let deadline = Instant::now() + Duration::from_secs(10);
    while began.load(Ordering::Acquire) < WORKERS {
        assert!(
            Instant::now() < deadline,
            "both gated jobs must start within 10 s"
        );
        std::thread::sleep(Duration::from_millis(1));
    }

    let probes: Vec<Duration> = (0..3)
        .map(|_| {
            std::thread::spawn(move || probe_healthz(addr))
                .join()
                .unwrap()
        })
        .collect();
    let still_running = gate.running();
    stop.store(true, Ordering::Release);

    for (index, took) in probes.iter().enumerate() {
        // hygiene-allow: wall-clock -- the probe's response time is the
        // property under test: a server on real sockets with no injected
        // clock, against a bound eight times under the 2 s spin it must beat.
        assert!(
            *took < PROBE_BOUND,
            "/healthz probe {index} took {took:?} while {WORKERS} in-place read jobs spun, \
             over the {PROBE_BOUND:?} bound: every probe {probes:?}"
        );
    }
    assert_eq!(
        still_running, WORKERS as u64,
        "both jobs held a permit through every probe"
    );
    for job in jobs {
        let (caller, ran_on) = job.await.unwrap();
        assert_eq!(caller, ran_on, "an in-place job runs on its calling worker");
    }
    assert_eq!(gate.in_place_fallbacks(), 0);

    running.shutdown().await.expect("graceful shutdown");
}
