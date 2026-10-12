//! The server CPU one post-decode pending-ack refusal costs over one head
//! refusal (ADR-2793 decision 4), measured over real HTTP against an
//! in-process ingest server.
//!
//! The two refusals answer the same 429 from different places. The in-flight
//! ceiling (`--max-inflight-ingest-requests`) sheds at the request head,
//! before the body is read. The pending strict-ack bound
//! (`--max-pending-strict-acks`) is taken inside the router's `submit`, after
//! the Remote Write body has been snappy-inflated, protobuf-decoded,
//! normalized and admitted. Each arm saturates exactly one of the two and
//! sends the same K Remote Write 1.0 requests:
//!
//! - Arm P (post-decode): `max_pending_strict_acks` 1, in-flight ceiling
//!   unlimited. One strict request is parked on a held data PUT, so it holds
//!   the only pending-ack slot; `/metrics` must read
//!   `ravel_ingest_pending_strict_acks` 1 before the K requests start.
//! - Arm H (head): in-flight ceiling 1, `max_pending_strict_acks` 0
//!   (unlimited). One request is parked on a held read of the tenant's
//!   provisioning record, which the handler issues after admission and before
//!   enqueue, so it holds the only in-flight permit.
//!
//! Every arm run starts a fresh server over a fresh
//! `FaultStore<MemoryStore>` and serves a tenant no earlier run used. The
//! measured region is exactly the K requests: the process CPU clock and a
//! `/metrics` scrape are read immediately before and after it. The scrape
//! supplies four families off the real endpoint:
//! `ravel_ingest_pending_ack_shed_total`,
//! `ravel_ingest_concurrency_shed_total`, `ravel_ingest_pending_strict_acks`
//! and `ravel_store_calls_total` (`op="put"`), the last counted by an
//! `InstrumentedStore` sharing the server's `StoreMetrics`, as
//! `ravel-server`'s own store chain does.
//!
//! The CPU figure is the whole process: server, client and runtimes. Both
//! arms send the same bytes from the same client, so the per-request
//! difference of the two arms is the extra cost of refusing after decode
//! rather than at the head. That includes any client-side difference: a head
//! refusal answers before the body is read, so its connection may not be
//! reused.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use bytes::Bytes;
use ravel_ingest::{AdmissionLimits, CountLimit, RateLimit};
use ravel_object_store::fault::{FaultPlan, FaultStore, GateHandle, Occurrence, Op};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{InstrumentedStore, ObjectStoreBackend, StoreMetrics};
use ravel_server::ingest_concurrency::IngestConcurrencyLimit;
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{Signal, TenantId};
use serde::Serialize;

use crate::metrics_gen::Generator;
use crate::metrics_ingest::{LogicalSample, encode_rw1_body, parse_logical_stream};
use crate::metrics_workload::load_workload;
use crate::saturation_run::{scrape, tenant, token};

/// Requests the lane keeps in flight during the measured region, in both arms.
pub const CLIENT_CONCURRENCY: usize = 8;

/// The status Remote Write acknowledges a durable write with.
pub const REMOTE_WRITE_ACK: u16 = 204;

/// The body sizes measured when the caller names none.
pub const DEFAULT_SAMPLES_PER_REQUEST: [usize; 3] = [1, 1_000, 10_000];

/// A run's CPU delta in arm P must be at least this many times the CPU
/// clock's resolution, or the size's figure is refused.
pub const RESOLUTION_FACTOR: u64 = 100;

/// Bounds every wait on a gate or on the parked request, so a request that
/// never parks or never finishes fails the run instead of hanging it.
const WAIT: Duration = Duration::from_secs(15);

/// Per-request client timeout. Above the server's 10 s strict ack deadline,
/// so a request the server answers late is still answered.
const CLIENT_TIMEOUT: Duration = Duration::from_secs(60);

const SCRAPE_TIMEOUT: Duration = Duration::from_secs(15);

/// Pushes the fleet admission reconcile tick, which PUTs this process's
/// usage record, and the store probe out past any run's measured region.
const BACKGROUND_INTERVAL: Duration = Duration::from_secs(3600);

const WORKLOAD: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../benchmarks/metrics/workload.json"
);
const PROFILE: &str = "ci";

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum Arm {
    /// Refused by the pending strict-ack bound, after decode.
    PostDecode,
    /// Refused by the in-flight ceiling, at the request head.
    Head,
}

impl Arm {
    pub fn label(self) -> &'static str {
        match self {
            Arm::PostDecode => "P",
            Arm::Head => "H",
        }
    }

    fn max_pending_strict_acks(self) -> u64 {
        match self {
            Arm::PostDecode => 1,
            Arm::Head => 0,
        }
    }

    fn ingest_concurrency_limit(self) -> IngestConcurrencyLimit {
        match self {
            Arm::PostDecode => IngestConcurrencyLimit::Unlimited,
            Arm::Head => IngestConcurrencyLimit::Bounded(1),
        }
    }
}

/// One arm run: K requests against a fresh server, and what `/metrics` and
/// the CPU clock moved by across them.
#[derive(Clone, Debug, Serialize)]
pub struct ArmRun {
    pub arm: Arm,
    pub samples_per_request: usize,
    pub repeat: usize,
    pub requests: usize,
    pub client_429: u64,
    /// Responses other than 429, and transport errors.
    pub client_other: u64,
    pub first_other: Option<String>,
    pub pending_ack_shed_delta: u64,
    pub concurrency_shed_delta: u64,
    pub store_put_delta: u64,
    pub cpu_ns: u64,
    pub wall_secs: f64,
    /// `ravel_ingest_pending_strict_acks` in the scrape after the region.
    pub pending_strict_acks_after: u64,
    /// The parked request was still unanswered when the region ended.
    pub parked_unanswered_after: bool,
    /// The parked request's status once its gate was released.
    pub parked_status: u16,
}

impl ArmRun {
    pub fn cpu_secs(&self) -> f64 {
        self.cpu_ns as f64 / 1e9
    }
}

/// Process CPU time, user plus system over every thread, in nanoseconds.
pub fn process_cpu_ns() -> u64 {
    let ts = rustix::time::clock_gettime(rustix::time::ClockId::ProcessCPUTime);
    let secs = u64::try_from(ts.tv_sec).unwrap_or(0);
    let nanos = u64::try_from(ts.tv_nsec).unwrap_or(0);
    secs.saturating_mul(1_000_000_000).saturating_add(nanos)
}

/// The smallest non-zero step [`process_cpu_ns`] took over `reads`
/// back-to-back changes of its value.
pub fn cpu_clock_resolution_ns(reads: usize) -> u64 {
    let mut smallest = u64::MAX;
    for _ in 0..reads {
        let start = process_cpu_ns();
        let mut next = process_cpu_ns();
        while next <= start {
            next = process_cpu_ns();
        }
        smallest = smallest.min(next - start);
    }
    smallest
}

/// The first `count` valid scalar samples of the MetricsBench `ci` profile,
/// the newest scrape one interval before `now_ms`.
pub fn logical_samples(count: usize, now_ms: i64) -> anyhow::Result<Vec<LogicalSample>> {
    let workload = load_workload(WORKLOAD).with_context(|| format!("loading {WORKLOAD}"))?;
    let profile = workload
        .profile(PROFILE)
        .with_context(|| format!("{WORKLOAD} declares no profile `{PROFILE}`"))?;
    let interval_ms = i64::try_from(profile.scrape_interval_secs)? * 1_000;
    let mut steps = u64::try_from(count.div_ceil(usize::try_from(profile.active_series)?))? + 1;
    for _ in 0..4 {
        let base_ts_ms = now_ms - i64::try_from(steps)? * interval_ms;
        let (bytes, _) = Generator::new(&workload, PROFILE, base_ts_ms)?.generate_bytes(steps)?;
        let text = String::from_utf8(bytes)?;
        let mut samples: Vec<LogicalSample> = parse_logical_stream(&text)
            .into_iter()
            .filter(|s| s.validate().is_ok())
            .collect();
        if samples.len() >= count {
            samples.truncate(count);
            return Ok(samples);
        }
        steps *= 2;
    }
    bail!("the `{PROFILE}` profile yields fewer than {count} scalar samples in {steps} steps")
}

/// One Remote Write 1.0 body of `count` samples, encoded once.
pub fn encode_body(count: usize, now_ms: i64) -> anyhow::Result<Bytes> {
    let samples = logical_samples(count, now_ms)?;
    Ok(Bytes::from(encode_rw1_body(&samples)?))
}

/// The client every arm sends with.
pub fn lane_client() -> anyhow::Result<reqwest::Client> {
    Ok(reqwest::Client::builder().timeout(CLIENT_TIMEOUT).build()?)
}

fn unlimited_admission() -> AdmissionLimits {
    AdmissionLimits {
        max_active_series: CountLimit::Unlimited,
        max_active_streams: CountLimit::Unlimited,
        ingest_byte_rate: RateLimit::Unlimited,
        series_creation_rate: RateLimit::Unlimited,
    }
}

/// Starts a gateway-mode server over `store` serving `tenant_index`'s token,
/// with `arm`'s two admission bounds and the strict flush policy `arrival`,
/// so the parked request's data PUT is issued as soon as it is enqueued. The
/// per-tenant admission limits are unlimited: K bodies of 10,000 samples can
/// exceed the shipped 32 MiB/s byte rate, and its 429 is neither refusal the
/// lane measures. The store probe and the admission reconcile run hourly.
/// Every other field is `saturation_run::start_server`'s.
pub async fn start_ingest_server(
    store: Arc<dyn ObjectStoreBackend>,
    store_metrics: Arc<StoreMetrics>,
    tenant_index: usize,
    arm: Arm,
    cores: usize,
) -> anyhow::Result<ravel_server::Running> {
    let tokens: HashMap<String, TenantId> =
        HashMap::from([(token(tenant_index), tenant(tenant_index))]);
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let loopback: std::net::SocketAddr = "127.0.0.1:0".parse()?;
    let config = ServerConfig {
        strict_flush_policy: ravel_server::config::StrictFlushPolicy::Arrival,
        audit_pipeline: None,
        audit_text: Default::default(),
        query_budgets: Default::default(),
        max_inflight_flushes: 1,
        max_inflight_flushes_per_tenant: None,
        max_queued_flushes: 8,
        adaptive_flush_delay: false,
        max_flush_delay: Duration::from_secs(2),
        max_flush_delay_idle: Duration::from_secs(40),
        min_flush_bytes: 256 * 1024,
        idle_flush_byte_floor: 0,
        mode: Mode::Gateway,
        listen_http: loopback,
        listen_grpc: loopback,
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
        limits: ravel_server::LimitsConfig {
            defaults: unlimited_admission(),
            ..ravel_server::LimitsConfig::default()
        },
        max_ingest_lag: ravel_server::DEFAULT_MAX_INGEST_LAG,
        deployment_key: None,
        gc: ravel_maintain::GcConfigValues::maintain_defaults(),
        query_deadline: Duration::from_secs(120),
        store_probe_interval: BACKGROUND_INTERVAL,
        admission_reconcile_interval: BACKGROUND_INTERVAL,
        query_concurrency_limit: ravel_query::QueryConcurrencyLimit::Unlimited,
        max_s3_requests: ravel_query::EngineConfig::default().max_s3_requests,
        scrub_period: Duration::from_secs(7 * 86_400),
        indexed_fields: Default::default(),
        typed_attr_columns: Default::default(),
        parquet_profiles: None,
        disable_cache: true,
        cache_max_bytes: 0,
        catalog_cache_max_bytes: 0,
        process_memory_budget_bytes: u64::MAX,
        process_memory_budget_is_fallback: false,
        cache_dir: None,
        catalog_resolve_concurrency: None,
        cpu_gate_permits: ravel_server::config::CpuGatePermits {
            read: ravel_cpu_gate::default_read_permits(cores),
            write: ravel_cpu_gate::default_write_permits(cores),
        },
        max_pending_strict_acks: arm.max_pending_strict_acks(),
        ingest_buffer_budget_limit: ravel_server::IngestByteBudgetLimit::Unlimited,
        idle_tenant_state_ttl: Duration::from_secs(3600),
        distrib: None,
        remote_clusters: Vec::new(),
        shutdown_timeout: ravel_server::DEFAULT_SHUTDOWN_TIMEOUT,
        drain_settle_interval: Duration::ZERO,
        ingest_concurrency_limit: arm.ingest_concurrency_limit(),
    };
    ravel_server::start(config, store.clone(), store, store_metrics, None).await
}

/// Sends one Remote Write 1.0 request and reads its whole response.
async fn post_write(
    client: &reqwest::Client,
    url: &str,
    token: &str,
    body: Bytes,
) -> Result<reqwest::StatusCode, String> {
    let response = client
        .post(url)
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/x-protobuf")
        .header("content-encoding", "snappy")
        .header("x-prometheus-remote-write-version", "0.1.0")
        .body(body)
        .send()
        .await
        .map_err(|e| error_chain(&e))?;
    let status = response.status();
    response.bytes().await.map_err(|e| error_chain(&e))?;
    Ok(status)
}

/// `err` and every source under it, so a transport failure names its cause.
fn error_chain(err: &dyn std::error::Error) -> String {
    let mut out = err.to_string();
    let mut source = err.source();
    while let Some(cause) = source {
        out.push_str(": ");
        out.push_str(&cause.to_string());
        source = cause.source();
    }
    out
}

#[derive(Default)]
struct Outcomes {
    too_many: u64,
    other: u64,
    first_other: Option<String>,
}

/// Sends `requests` copies of `body`, [`CLIENT_CONCURRENCY`] in flight.
async fn send_requests(
    client: &reqwest::Client,
    url: &str,
    token: &str,
    body: &Bytes,
    requests: usize,
) -> anyhow::Result<Outcomes> {
    let next = Arc::new(AtomicUsize::new(0));
    let mut workers = tokio::task::JoinSet::new();
    for _ in 0..CLIENT_CONCURRENCY {
        let (client, url, token, body, next) = (
            client.clone(),
            url.to_string(),
            token.to_string(),
            body.clone(),
            next.clone(),
        );
        workers.spawn(async move {
            let mut outcomes = Outcomes::default();
            while next.fetch_add(1, Ordering::Relaxed) < requests {
                match post_write(&client, &url, &token, body.clone()).await {
                    Ok(reqwest::StatusCode::TOO_MANY_REQUESTS) => outcomes.too_many += 1,
                    other => {
                        outcomes.other += 1;
                        outcomes
                            .first_other
                            .get_or_insert_with(|| format!("{other:?}"));
                    }
                }
            }
            outcomes
        });
    }
    let mut total = Outcomes::default();
    while let Some(joined) = workers.join_next().await {
        let outcomes = joined.context("request worker")?;
        total.too_many += outcomes.too_many;
        total.other += outcomes.other;
        if total.first_other.is_none() {
            total.first_other = outcomes.first_other;
        }
    }
    Ok(total)
}

/// The value of the one sample of `family` whose labels contain every entry
/// of `labels`. Absent or present more than once is an error, never a zero.
pub fn sample_value(exposition: &str, family: &str, labels: &[&str]) -> anyhow::Result<u64> {
    let values: Vec<&str> = exposition
        .lines()
        .filter_map(|line| {
            let rest = line.strip_prefix(family)?.strip_prefix('{')?;
            let (label_set, value) = rest.rsplit_once("} ")?;
            labels
                .iter()
                .all(|label| label_set.contains(label))
                .then_some(value)
        })
        .collect();
    match values.as_slice() {
        [value] => {
            let parsed: f64 = value
                .trim()
                .parse()
                .with_context(|| format!("{family} sample `{value}`"))?;
            if !(parsed >= 0.0 && parsed.fract() == 0.0) {
                bail!("{family} sample {parsed} is not a count");
            }
            Ok(parsed as u64)
        }
        [] => bail!("/metrics carries no {family} sample matching {labels:?}"),
        many => bail!(
            "/metrics carries {} {family} samples matching {labels:?}",
            many.len()
        ),
    }
}

const PENDING_ACK_SHED: &str = "ravel_ingest_pending_ack_shed_total";
const CONCURRENCY_SHED: &str = "ravel_ingest_concurrency_shed_total";
const PENDING_STRICT_ACKS: &str = "ravel_ingest_pending_strict_acks";
const STORE_CALLS: &str = "ravel_store_calls_total";

struct Counters {
    pending_ack_shed: u64,
    concurrency_shed: u64,
    store_puts: u64,
    pending_strict_acks: u64,
}

impl Counters {
    fn read(exposition: &str) -> anyhow::Result<Self> {
        Ok(Counters {
            pending_ack_shed: sample_value(exposition, PENDING_ACK_SHED, &[])?,
            concurrency_shed: sample_value(exposition, CONCURRENCY_SHED, &[])?,
            store_puts: sample_value(exposition, STORE_CALLS, &["op=\"put\""])?,
            pending_strict_acks: sample_value(exposition, PENDING_STRICT_ACKS, &[])?,
        })
    }
}

fn delta(family: &str, before: u64, after: u64) -> anyhow::Result<u64> {
    after
        .checked_sub(before)
        .with_context(|| format!("{family} went backwards: {before} then {after}"))
}

/// Releases every call `gate` holds, as each arrives, until `task` finishes.
async fn release_until<T>(
    gate: &GateHandle,
    task: &mut tokio::task::JoinHandle<T>,
) -> anyhow::Result<T> {
    tokio::time::timeout(WAIT, async {
        loop {
            tokio::select! {
                out = &mut *task => return out.context("parked request task"),
                () = gate.wait_until_held(1) => {
                    for id in gate.held() {
                        gate.release(id);
                    }
                }
            }
        }
    })
    .await
    .context("the parked request did not finish once its gate was released")?
}

/// Releases every call `gate` holds, as each arrives, until aborted.
async fn release_forever(gate: GateHandle) {
    loop {
        gate.wait_until_held(1).await;
        for id in gate.held() {
            gate.release(id);
        }
    }
}

/// Drives `make`'s future to completion on `server` from a blocking thread of
/// the caller's runtime. The server's start and shutdown futures are not
/// `Send`, so they cannot be spawned onto it.
async fn on_server<F, T>(
    server: &tokio::runtime::Handle,
    make: impl FnOnce() -> F + Send + 'static,
) -> anyhow::Result<T>
where
    F: std::future::Future<Output = anyhow::Result<T>>,
    T: Send + 'static,
{
    let server = server.clone();
    tokio::task::spawn_blocking(move || server.block_on(make()))
        .await
        .context("server task")?
}

/// One arm run. The server starts and stops on `server`; the requests, the
/// scrapes and the parked request run on the caller's runtime.
#[allow(clippy::too_many_arguments)]
pub async fn measure_arm(
    server: &tokio::runtime::Handle,
    client: &reqwest::Client,
    arm: Arm,
    body: &Bytes,
    samples_per_request: usize,
    requests: usize,
    repeat: usize,
    tenant_index: usize,
) -> anyhow::Result<ArmRun> {
    let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
    let fault = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
    let store_metrics = Arc::new(StoreMetrics::default());
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(InstrumentedStore::with_metrics(
        fault.clone(),
        store_metrics.clone(),
    ));
    let running = on_server(server, move || {
        start_ingest_server(store, store_metrics, tenant_index, arm, cores)
    })
    .await?;
    let base = format!("http://{}", running.http_addr);
    let url = format!("{base}/api/v1/write");
    let token = token(tenant_index);
    let tenant_hash = tenant(tenant_index).hash();

    let gate = match arm {
        Arm::PostDecode => fault.hold(
            Op::Put,
            Some(format!(
                "t/{}/{}/l0/",
                tenant_hash.to_hex(),
                Signal::Metrics.key_prefix()
            )),
            Occurrence::Always,
        ),
        Arm::Head => fault.hold(
            Op::Get,
            Some(ravel_catalog::provisioning_key(
                &tenant_hash,
                Signal::Metrics,
            )),
            Occurrence::Always,
        ),
    };
    let mut parked = {
        let (client, url, token, body) = (client.clone(), url.clone(), token.clone(), body.clone());
        tokio::spawn(async move { post_write(&client, &url, &token, body).await })
    };
    tokio::time::timeout(WAIT, gate.wait_until_held(1))
        .await
        .with_context(|| {
            format!(
                "arm {}: the parked request never reached its gate",
                arm.label()
            )
        })?;
    if parked.is_finished() {
        let answer = (&mut parked).await;
        bail!(
            "arm {}: the parked request was answered before the K requests: {answer:?}",
            arm.label()
        );
    }
    if arm == Arm::PostDecode {
        let parked_slots =
            Counters::read(&scrape(client, running.http_addr, SCRAPE_TIMEOUT).await?)?
                .pending_strict_acks;
        if parked_slots != 1 {
            bail!(
                "arm P: {PENDING_STRICT_ACKS} reads {parked_slots} with one request parked, not 1"
            );
        }
    }

    let before = Counters::read(&scrape(client, running.http_addr, SCRAPE_TIMEOUT).await?)?;
    let cpu_before = process_cpu_ns();
    let wall_start = Instant::now();
    let outcomes = send_requests(client, &url, &token, body, requests).await?;
    let wall_secs = wall_start.elapsed().as_secs_f64();
    let cpu_after = process_cpu_ns();
    let after = Counters::read(&scrape(client, running.http_addr, SCRAPE_TIMEOUT).await?)?;
    let parked_unanswered_after = !parked.is_finished();

    let parked_status = match release_until(&gate, &mut parked).await? {
        Ok(status) => status.as_u16(),
        Err(err) => bail!("arm {}: the parked request failed: {err}", arm.label()),
    };
    let releaser = tokio::spawn(release_forever(gate));
    let shutdown = on_server(server, move || running.shutdown()).await;
    releaser.abort();
    shutdown?;

    Ok(ArmRun {
        arm,
        samples_per_request,
        repeat,
        requests,
        client_429: outcomes.too_many,
        client_other: outcomes.other,
        first_other: outcomes.first_other,
        pending_ack_shed_delta: delta(
            PENDING_ACK_SHED,
            before.pending_ack_shed,
            after.pending_ack_shed,
        )?,
        concurrency_shed_delta: delta(
            CONCURRENCY_SHED,
            before.concurrency_shed,
            after.concurrency_shed,
        )?,
        store_put_delta: delta(STORE_CALLS, before.store_puts, after.store_puts)?,
        cpu_ns: delta("process CPU clock", cpu_before, cpu_after)?,
        wall_secs,
        pending_strict_acks_after: after.pending_strict_acks,
        parked_unanswered_after,
        parked_status,
    })
}

/// Every count band `run` misses, in a fixed order: the 429 count, the two
/// shed deltas, the store PUT delta, then whether the parked request held its
/// slot through the region and was acknowledged once released.
pub fn count_band_misses(run: &ArmRun) -> Vec<String> {
    let k = run.requests as u64;
    let (pending_want, concurrency_want) = match run.arm {
        Arm::PostDecode => (k, 0),
        Arm::Head => (0, k),
    };
    let mut misses = Vec::new();
    let mut check = |name: &str, got: u64, want: u64| {
        if got != want {
            misses.push(format!("{name} = {got}, band == {want}"));
        }
    };
    check("client 429 count", run.client_429, k);
    check(
        "pending_ack_shed delta",
        run.pending_ack_shed_delta,
        pending_want,
    );
    check(
        "concurrency_shed delta",
        run.concurrency_shed_delta,
        concurrency_want,
    );
    check("store PUT delta", run.store_put_delta, 0);
    if run.arm == Arm::PostDecode {
        check(
            "pending_strict_acks after the region",
            run.pending_strict_acks_after,
            1,
        );
    }
    check(
        "parked request unanswered after the region",
        u64::from(run.parked_unanswered_after),
        1,
    );
    check(
        "parked request status once released",
        u64::from(run.parked_status),
        u64::from(REMOTE_WRITE_ACK),
    );
    misses
}

/// The pre-registered band for the median extra CPU per post-decode refusal,
/// in microseconds. `lo..=hi` is the prediction; `miss_below`/`miss_above`
/// bound what counts as a miss of it.
#[derive(Clone, Copy, Debug, Serialize)]
pub struct CpuBand {
    pub lo_us: f64,
    pub hi_us: f64,
    pub miss_below_us: Option<f64>,
    pub miss_above_us: f64,
}

pub fn cpu_band(samples_per_request: usize) -> Option<CpuBand> {
    match samples_per_request {
        1 => Some(CpuBand {
            lo_us: 5.0,
            hi_us: 100.0,
            miss_below_us: Some(-50.0),
            miss_above_us: 1_000.0,
        }),
        1_000 => Some(CpuBand {
            lo_us: 100.0,
            hi_us: 1_000.0,
            miss_below_us: None,
            miss_above_us: 10_000.0,
        }),
        10_000 => Some(CpuBand {
            lo_us: 1_000.0,
            hi_us: 10_000.0,
            miss_below_us: None,
            miss_above_us: 100_000.0,
        }),
        _ => None,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum CpuVerdict {
    InBand,
    /// Outside the prediction, inside the miss bounds.
    OutsidePrediction,
    Miss,
    /// No band is registered for this body size.
    NoBand,
}

impl CpuVerdict {
    pub fn of(band: Option<CpuBand>, median_us: f64) -> Self {
        let Some(band) = band else {
            return CpuVerdict::NoBand;
        };
        if median_us > band.miss_above_us || band.miss_below_us.is_some_and(|lo| median_us < lo) {
            CpuVerdict::Miss
        } else if median_us < band.lo_us || median_us > band.hi_us {
            CpuVerdict::OutsidePrediction
        } else {
            CpuVerdict::InBand
        }
    }

    /// Whether the figure fails the bin: anything outside the prediction.
    pub fn fails(self) -> bool {
        matches!(self, CpuVerdict::OutsidePrediction | CpuVerdict::Miss)
    }
}

/// The median of `values`; the mean of the middle two for an even count.
pub fn median(values: &[f64]) -> Option<f64> {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let n = sorted.len();
    match n {
        0 => None,
        _ if n % 2 == 1 => Some(sorted[n / 2]),
        _ => Some((sorted[n / 2 - 1] + sorted[n / 2]) / 2.0),
    }
}

/// `(cpu_P - cpu_H) / K` in microseconds, for one pair of runs.
pub fn extra_cpu_per_refusal_us(p: &ArmRun, h: &ArmRun) -> f64 {
    (p.cpu_ns as f64 - h.cpu_ns as f64) / p.requests as f64 / 1_000.0
}

/// Every run of one body size, and the figure the size reports.
#[derive(Clone, Debug, Serialize)]
pub struct SizeReport {
    pub samples_per_request: usize,
    pub body_bytes: usize,
    pub runs: Vec<ArmRun>,
    /// One per repeat, arm P's run paired with arm H's of the same repeat.
    /// Empty when [`Self::resolution_refusal`] is set.
    pub extra_cpu_per_refusal_us: Vec<f64>,
    /// Absent when [`Self::resolution_refusal`] is set.
    pub extra_cpu_per_refusal_us_median: Option<f64>,
    pub band: Option<CpuBand>,
    pub verdict: Option<CpuVerdict>,
    pub count_misses: Vec<String>,
    /// Why the size's figure is withheld: an arm P run whose CPU delta is
    /// under [`RESOLUTION_FACTOR`] times the clock's resolution.
    pub resolution_refusal: Option<String>,
}

impl SizeReport {
    /// `pairs` holds one (arm P, arm H) run per repeat.
    pub fn build(
        samples_per_request: usize,
        body_bytes: usize,
        pairs: Vec<(ArmRun, ArmRun)>,
        resolution_ns: u64,
    ) -> Self {
        let floor_ns = resolution_ns.saturating_mul(RESOLUTION_FACTOR);
        let resolution_refusal = pairs
            .iter()
            .find(|(p, _)| p.cpu_ns < floor_ns)
            .map(|(p, _)| {
                format!(
                    "arm P repeat {} moved the CPU clock {} ns, under {RESOLUTION_FACTOR} x \
                     the {resolution_ns} ns resolution ({floor_ns} ns): raise --requests",
                    p.repeat, p.cpu_ns
                )
            });
        let extra: Vec<f64> = match resolution_refusal {
            Some(_) => Vec::new(),
            None => pairs
                .iter()
                .map(|(p, h)| extra_cpu_per_refusal_us(p, h))
                .collect(),
        };
        let mut count_misses = Vec::new();
        let mut runs = Vec::with_capacity(pairs.len() * 2);
        for (p, h) in pairs {
            for run in [p, h] {
                count_misses.extend(count_band_misses(&run).into_iter().map(|miss| {
                    format!(
                        "{samples_per_request} samples, arm {} repeat {}: {miss}",
                        run.arm.label(),
                        run.repeat
                    )
                }));
                runs.push(run);
            }
        }
        let band = cpu_band(samples_per_request);
        let median_us = median(&extra);
        SizeReport {
            samples_per_request,
            body_bytes,
            runs,
            extra_cpu_per_refusal_us: extra,
            extra_cpu_per_refusal_us_median: median_us,
            band,
            verdict: median_us.map(|m| CpuVerdict::of(band, m)),
            count_misses,
            resolution_refusal,
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    const K: usize = 50;
    const SAMPLES: usize = 1_000;

    fn assert_count_bands(run: &ArmRun) {
        let k = K as u64;
        let label = run.arm.label();
        assert_eq!(run.client_429, k, "arm {label}: client 429 count: {run:?}");
        let (pending_want, concurrency_want) = match run.arm {
            Arm::PostDecode => (k, 0),
            Arm::Head => (0, k),
        };
        assert_eq!(
            run.pending_ack_shed_delta, pending_want,
            "arm {label}: pending_ack_shed delta: {run:?}"
        );
        assert_eq!(
            run.concurrency_shed_delta, concurrency_want,
            "arm {label}: concurrency_shed delta: {run:?}"
        );
        assert_eq!(
            run.store_put_delta, 0,
            "arm {label}: store PUT delta: {run:?}"
        );
        assert!(
            run.parked_unanswered_after,
            "arm {label}: the parked request held through the region: {run:?}"
        );
        assert_eq!(
            run.parked_status, REMOTE_WRITE_ACK,
            "arm {label}: parked status: {run:?}"
        );
        assert_eq!(
            count_band_misses(run),
            Vec::<String>::new(),
            "arm {label}: the bin's band check agrees"
        );
    }

    /// Both arms reach their refusal through the shipping Remote Write
    /// handler over HTTP: every request answers 429, and the real `/metrics`
    /// counts each refusal under its own family and no other, with no store
    /// PUT completing inside the region.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn lane_counts_close_at_small_k() {
        let now_ms = crate::saturation_run::wall_now_ns() / 1_000_000;
        let body = encode_body(SAMPLES, now_ms).expect("body encodes");
        let client = lane_client().expect("client builds");
        let server = tokio::runtime::Handle::current();
        for (tenant_index, arm) in [Arm::PostDecode, Arm::Head].into_iter().enumerate() {
            let run = measure_arm(&server, &client, arm, &body, SAMPLES, K, 0, tenant_index)
                .await
                .expect("arm runs");
            assert_count_bands(&run);
        }
    }

    #[test]
    fn sample_value_refuses_an_absent_or_repeated_sample() {
        let body = "# HELP x\nravel_x{mode=\"gateway\",op=\"put\"} 3\n\
                    ravel_x{mode=\"gateway\",op=\"get\"} 9\nravel_xy{mode=\"gateway\"} 4\n";
        assert_eq!(sample_value(body, "ravel_x", &["op=\"put\""]).unwrap(), 3);
        assert!(sample_value(body, "ravel_x", &[]).is_err(), "two samples");
        assert!(sample_value(body, "ravel_z", &[]).is_err(), "absent");
        assert_eq!(sample_value(body, "ravel_xy", &[]).unwrap(), 4);
    }

    #[test]
    fn cpu_verdicts_follow_the_registered_bands() {
        let one = cpu_band(1);
        assert_eq!(CpuVerdict::of(one, 50.0), CpuVerdict::InBand);
        assert_eq!(CpuVerdict::of(one, 200.0), CpuVerdict::OutsidePrediction);
        assert_eq!(CpuVerdict::of(one, 1_001.0), CpuVerdict::Miss);
        assert_eq!(CpuVerdict::of(one, -51.0), CpuVerdict::Miss);
        assert_eq!(
            CpuVerdict::of(cpu_band(1_000), 20.0),
            CpuVerdict::OutsidePrediction
        );
        assert_eq!(
            CpuVerdict::of(cpu_band(10_000), 100_001.0),
            CpuVerdict::Miss
        );
        assert_eq!(CpuVerdict::of(cpu_band(7), 1.0), CpuVerdict::NoBand);
        assert!(!CpuVerdict::NoBand.fails());
    }

    #[test]
    fn median_of_odd_and_even_counts() {
        assert_eq!(median(&[3.0, 1.0, 2.0]), Some(2.0));
        assert_eq!(median(&[4.0, 1.0, 2.0, 3.0]), Some(2.5));
        assert_eq!(median(&[]), None);
    }
}
