//! A full server for the strict flush policy tests (ADR-2793 decision 1):
//! `--strict-flush-policy` and the `x-ravel-ingest-mode` values
//! `strict-arrival`/`strict-aged`, driven through the shipping HTTP and gRPC
//! handlers. Every data PUT of one signal is held by a `FaultStore` gate, so
//! a test reads "this write opened a flush" as a held call, and the
//! `/metrics` exposition says which trigger opened it. Included with
//! `#[path]` by each test that needs it.
//!
//! `ravel_server::start` runs the ingest pipelines on the system clock, so the
//! age threshold cannot be held still. A test that asserts no flush opened
//! does so inside the first `MAX_FLUSH_DELAY` after the write reached its
//! buffer, and reads `flushes_by_age` as well, so an age flush that did fire
//! inside the window fails the test rather than passing as an arrival flush.

#![allow(dead_code, clippy::expect_used)]

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::fault::{FaultPlan, FaultStore, GateHandle, Occurrence, Op};
use ravel_object_store::memory::MemoryStore;
use ravel_server::config::StrictFlushPolicy;
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::TenantId;

pub const TOKEN: &str = "testtoken";
pub const TENANT: &str = "acme";

/// The strict-tier age threshold. Startup refuses a delay whose strict
/// visibility budget (this plus a 500 ms reserve) reaches 3 s, so 2 s is the
/// widest window an absence check can have.
pub const MAX_FLUSH_DELAY: Duration = Duration::from_secs(2);

/// How long an absence check waits after the write is visible in
/// `buffered_items_total` before it reads the flush counters and the gate.
/// An arrival flush is counted in the same actor turn that buffers the write,
/// and its PUT is issued by the task that turn spawns, so this only has to
/// cover that task being scheduled. It stays far inside `MAX_FLUSH_DELAY`.
const SETTLE: Duration = Duration::from_millis(200);

/// Bounds every wait in this module, so a write that never flushes fails the
/// test instead of hanging it.
const WAIT: Duration = Duration::from_secs(15);

/// The `signal` label value and the data-object key segment of one pipeline.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    Metrics,
    Logs,
    Spans,
}

impl Signal {
    pub fn label(self) -> &'static str {
        match self {
            Signal::Metrics => "metrics",
            Signal::Logs => "logs",
            Signal::Spans => "spans",
        }
    }

    fn key_segment(self) -> &'static str {
        match self {
            Signal::Metrics => "m",
            Signal::Logs => "l",
            Signal::Spans => "s",
        }
    }

    /// The prefix of this pipeline's level-0 data objects for [`TENANT`].
    pub fn data_prefix(self) -> String {
        format!(
            "t/{}/{}/l0/",
            TenantId::new(TENANT).hash().to_hex(),
            self.key_segment()
        )
    }
}

pub struct PolicyServer {
    pub running: ravel_server::Running,
    pub base: String,
    pub client: reqwest::Client,
    /// Holds every data PUT of the signal the server was started for.
    pub gate: GateHandle,
    pub signal: Signal,
}

/// Starts a server under `policy` with every `signal` data PUT held.
pub async fn start(policy: StrictFlushPolicy, signal: Signal) -> PolicyServer {
    start_with(policy, signal, |_| {}).await
}

/// [`start`], with `configure` applied to the configuration last.
pub async fn start_with(
    policy: StrictFlushPolicy,
    signal: Signal,
    configure: impl FnOnce(&mut ServerConfig),
) -> PolicyServer {
    let mut tokens = HashMap::new();
    tokens.insert(TOKEN.to_string(), TenantId::new(TENANT));
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let faults = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let gate = faults.hold(Op::Put, Some(signal.data_prefix()), Occurrence::Always);
    let store: Arc<dyn ObjectStoreBackend> = faults;
    let mut config = ServerConfig {
        strict_flush_policy: policy,
        audit_pipeline: Default::default(),
        audit_text: Default::default(),
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        max_inflight_flushes_per_tenant: None,
        max_queued_flushes: 8,
        adaptive_flush_delay: false,
        max_flush_delay: MAX_FLUSH_DELAY,
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
    };
    configure(&mut config);
    let running = ravel_server::start(
        config,
        store.clone(),
        store,
        Arc::new(ravel_object_store::StoreMetrics::default()),
        None,
    )
    .await
    .expect("server starts");
    let base = format!("http://{}", running.http_addr);
    PolicyServer {
        running,
        base,
        client: reqwest::Client::new(),
        gate,
        signal,
    }
}

/// Drives `fut` to completion, releasing every held call as it arrives.
pub async fn release_until<F: Future>(gate: &GateHandle, fut: F) -> F::Output {
    tokio::pin!(fut);
    tokio::time::timeout(WAIT, async {
        loop {
            tokio::select! {
                out = &mut fut => return out,
                () = gate.wait_until_held(1) => {
                    for id in gate.held() {
                        gate.release(id);
                    }
                }
            }
        }
    })
    .await
    .expect("the future completes once its held PUTs are released")
}

/// The value of the one sample of `family` labelled `signal="<signal>"` in a
/// `/metrics` body. Panics unless exactly one such sample is present.
pub fn sample(exposition: &str, family: &str, signal: &str) -> u64 {
    let label = format!("signal=\"{signal}\"");
    let values: Vec<u64> = exposition
        .lines()
        .filter(|line| {
            line.strip_prefix(family)
                .is_some_and(|rest| rest.starts_with('{'))
                && line.contains(&label)
        })
        .map(|line| {
            line.rsplit(' ')
                .next()
                .and_then(|value| value.parse().ok())
                .unwrap_or_else(|| panic!("unparsable sample: {line}"))
        })
        .collect();
    assert_eq!(
        values.len(),
        1,
        "expected exactly one {family}{{{label}}} sample in:\n{exposition}"
    );
    values[0]
}

/// The value of the one sample of a process-wide `family` (labelled by
/// `mode` only) in a `/metrics` body. Panics unless exactly one is present.
pub fn process_sample(exposition: &str, family: &str) -> u64 {
    let values: Vec<u64> = exposition
        .lines()
        .filter(|line| {
            line.strip_prefix(family)
                .is_some_and(|rest| rest.starts_with('{'))
        })
        .map(|line| {
            line.rsplit(' ')
                .next()
                .and_then(|value| value.parse().ok())
                .unwrap_or_else(|| panic!("unparsable sample: {line}"))
        })
        .collect();
    assert_eq!(
        values.len(),
        1,
        "expected exactly one {family} sample in:\n{exposition}"
    );
    values[0]
}

impl PolicyServer {
    pub async fn scrape(&self) -> String {
        self.client
            .get(format!("{}/metrics", self.base))
            .send()
            .await
            .expect("metrics request completes")
            .text()
            .await
            .expect("metrics body is text")
    }

    /// `family`'s sample for this server's signal.
    pub async fn counter(&self, family: &str) -> u64 {
        sample(&self.scrape().await, family, self.signal.label())
    }

    pub async fn flushes_by_arrival(&self) -> u64 {
        self.counter("ravel_ingest_flushes_by_arrival_total").await
    }

    pub async fn flushes_by_age(&self) -> u64 {
        self.counter("ravel_ingest_flushes_by_age_total").await
    }

    /// Waits until a data PUT is held, then asserts it is the only one.
    pub async fn wait_for_data_put(&self) {
        tokio::time::timeout(WAIT, self.gate.wait_until_held(1))
            .await
            .expect("the write issues its data PUT");
        let held = self.gate.held_details();
        assert_eq!(held.len(), 1, "exactly one data PUT is held: {held:?}");
    }

    /// Waits until `task` has buffered its item, so the pipeline holds at
    /// least `items`, while `task` is still waiting for its response. A task
    /// that finishes first was answered without being enqueued, and its
    /// response is in the panic message.
    pub async fn wait_for_buffered_while_pending<T: std::fmt::Debug>(
        &self,
        items: u64,
        task: &mut tokio::task::JoinHandle<T>,
    ) {
        let buffered = tokio::time::timeout(WAIT, async {
            loop {
                if self.counter("ravel_ingest_buffered_items_total").await >= items {
                    return true;
                }
                if task.is_finished() {
                    return false;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the write reaches its buffer or is answered");
        if !buffered {
            let response = task.await.expect("export task");
            panic!("the write was answered before it was enqueued: {response:?}");
        }
    }

    /// Waits until the pipeline has buffered at least `items` items, then
    /// settles once and asserts from one scrape that no flush has opened: no
    /// data PUT held, and neither the arrival nor the age counter moved.
    /// Callers send every write first and check once, so all of them share
    /// one window inside `MAX_FLUSH_DELAY`.
    pub async fn assert_no_flush_yet(&self, items: u64) {
        tokio::time::timeout(WAIT, async {
            while self.counter("ravel_ingest_buffered_items_total").await < items {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the writes reach their buffer");
        tokio::time::sleep(SETTLE).await;
        assert_eq!(self.gate.held_count(), 0, "no data PUT was issued");
        let exposition = self.scrape().await;
        let label = self.signal.label();
        assert_eq!(
            sample(&exposition, "ravel_ingest_flushes_by_arrival_total", label),
            0,
            "no arrival flush opened"
        );
        assert_eq!(
            sample(&exposition, "ravel_ingest_flushes_by_age_total", label),
            0,
            "no age flush opened inside the window either, or this check proves nothing"
        );
    }

    /// Shuts the server down, releasing the drain's held PUTs.
    pub async fn shutdown(self) {
        let gate = self.gate.clone();
        release_until(&gate, self.running.shutdown())
            .await
            .expect("graceful shutdown");
    }
}
