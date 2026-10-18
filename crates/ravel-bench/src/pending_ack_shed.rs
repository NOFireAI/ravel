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
//! A body is (series, samples per series): synthetic samples with one
//! distinct label set per series, timestamps one second apart ending at the
//! run's start, encoded once and decoded once to prove the server will see
//! the requested series count. Before any run the decoded body is also put
//! through `normalize_resolved` with the tenant, limits and clock reading the
//! handler would use, and must keep every sample: a refusal of a body whose
//! points normalization drops is not the refusal of the body the stamp names.
//!
//! Every arm run starts a fresh server over a fresh
//! `FaultStore<MemoryStore>` and serves a tenant no earlier run used. The
//! measured region is exactly the K requests: the process CPU clock and a
//! `/metrics` scrape are read immediately before and after it. The scrape
//! supplies four families off the real endpoint, each matched on its exact
//! label set: `ravel_ingest_pending_ack_shed_total`,
//! `ravel_ingest_concurrency_shed_total`, `ravel_ingest_pending_strict_acks`
//! and `ravel_store_calls_total` (`op="put"`), the last counted by an
//! `InstrumentedStore` sharing the server's `StoreMetrics`, as
//! `ravel-server`'s own store chain does.
//!
//! The parked request waits under the server's strict-ack deadline, so the
//! region must end inside it: the time from the parked request's submit to
//! the region's end is banded below [`PARKED_WAIT_BAND`], a count band miss
//! that names K as too large for the body on the host.
//!
//! The client opens a fresh connection for every request in both arms: a head
//! refusal of an unread body closes the connection, so a pooled client's next
//! request on it can fail in transport, which is neither refusal.
//!
//! The CPU figure is the whole process: server, client and runtimes. Both
//! arms send the same bytes from the same client over the same connection
//! pattern, so the per-request difference of the two arms is the extra cost
//! of refusing after decode rather than at the head.

use std::collections::HashMap;
use std::fmt;
use std::ops::Range;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use anyhow::{Context, bail};
use bytes::Bytes;
use ravel_ingest::{AdmissionLimits, CountLimit, RateLimit};
use ravel_object_store::fault::{FaultPlan, FaultStore, GateHandle, Occurrence, Op};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{InstrumentedStore, ObjectStoreBackend, StoreMetrics};
use ravel_otlp::IngestLimits;
use ravel_remote_write::ResolvedRequest;
use ravel_server::ingest_concurrency::IngestConcurrencyLimit;
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{Signal, TenantId};
use serde::Serialize;

use crate::metrics_ingest::{LogicalSample, encode_rw1_body};
use crate::saturation_run::{scrape, tenant, token};

/// Requests the lane keeps in flight during the measured region, in both arms.
pub const CLIENT_CONCURRENCY: usize = 8;

/// The status Remote Write acknowledges a durable write with.
pub const REMOTE_WRITE_ACK: u16 = 204;

/// How the lane's client connects, for the stamp.
pub const CONNECTION_MODE: &str = "fresh per request";

/// The bodies measured when the caller names none.
pub const DEFAULT_BODIES: [BodySpec; 4] = [
    BodySpec::new(1, 1),
    BodySpec::new(1_000, 1),
    BodySpec::new(1_000, 10),
    BodySpec::new(10_000, 1),
];

/// `|cpu_P - cpu_H|` of every repeat must be at least this many times the CPU
/// clock's resolution, or the body's figure is withheld.
pub const RESOLUTION_FACTOR: u64 = 100;

/// Bounds every wait on a gate or on the parked request, so a request that
/// never parks or never finishes fails the run instead of hanging it.
const WAIT: Duration = Duration::from_secs(15);

/// The server's fixed strict-ack deadline: `DEFAULT_ACK_DEADLINE` in
/// `ravel-server`'s `lib.rs`, which is not public.
pub const STRICT_ACK_DEADLINE: Duration = Duration::from_secs(10);

/// The parked request's wait from its submit to the region's end must stay
/// under this: [`STRICT_ACK_DEADLINE`] less a 2 s margin.
pub const PARKED_WAIT_BAND: Duration = STRICT_ACK_DEADLINE.saturating_sub(Duration::from_secs(2));

/// Per-request client timeout. Above [`STRICT_ACK_DEADLINE`], so a request
/// the server answers late is still answered.
const CLIENT_TIMEOUT: Duration = Duration::from_secs(60);

const SCRAPE_TIMEOUT: Duration = Duration::from_secs(15);

/// Pushes the fleet admission reconcile tick, which PUTs this process's
/// usage record, and the store probe out past any run's measured region.
const BACKGROUND_INTERVAL: Duration = Duration::from_secs(3600);

/// The server's post-Snappy Remote Write cap, used for the body check.
const DECODE_LIMIT_BYTES: usize = 64 * 1024 * 1024;

/// The `--max-ingest-lag` the lane's server is started with.
const MAX_INGEST_LAG: Duration = ravel_server::DEFAULT_MAX_INGEST_LAG;

const BODY_METRIC: &str = "ravel_bench_shed_load";
const SAMPLE_STEP_MS: i64 = 1_000;

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

/// The arms of `repeat` in the order they run: P then H on an even repeat, H
/// then P on an odd one.
pub fn arms_in_order(repeat: usize) -> [Arm; 2] {
    if repeat.is_multiple_of(2) {
        [Arm::PostDecode, Arm::Head]
    } else {
        [Arm::Head, Arm::PostDecode]
    }
}

/// [`arms_in_order`] as the label the report prints, `PH` or `HP`.
pub fn order_label(repeat: usize) -> &'static str {
    match arms_in_order(repeat) {
        [Arm::PostDecode, _] => "PH",
        _ => "HP",
    }
}

/// A body's shape: distinct series, and samples in each.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub struct BodySpec {
    pub series: usize,
    pub samples_per_series: usize,
}

impl BodySpec {
    pub const fn new(series: usize, samples_per_series: usize) -> Self {
        BodySpec {
            series,
            samples_per_series,
        }
    }
}

impl fmt::Display for BodySpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}x{}", self.series, self.samples_per_series)
    }
}

impl FromStr for BodySpec {
    type Err = String;

    /// `SERIESxSAMPLES`, both at least 1, e.g. `1000x10`.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (series, samples) = s
            .split_once('x')
            .ok_or_else(|| format!("`{s}` is not SERIESxSAMPLES"))?;
        let parse = |part: &str| match part.parse::<usize>() {
            Ok(n) if n >= 1 => Ok(n),
            _ => Err(format!("`{part}` in `{s}` is not a count of at least 1")),
        };
        Ok(BodySpec::new(parse(series)?, parse(samples)?))
    }
}

/// The samples of a `spec` body: series `s` carries the labels
/// `instance="shed-bench"` and `series` set to `s` zero-padded to six digits
/// (`series="s000042"` for series 42), and its samples are
/// [`SAMPLE_STEP_MS`] apart, the newest at `now_ms`.
pub fn body_samples(spec: BodySpec, now_ms: i64) -> anyhow::Result<Vec<LogicalSample>> {
    let per_series = spec.samples_per_series;
    let mut samples = Vec::with_capacity(spec.series.saturating_mul(per_series));
    for s in 0..spec.series {
        for j in 0..per_series {
            let age = i64::try_from(per_series - 1 - j)?;
            samples.push(LogicalSample {
                metric: BODY_METRIC.to_string(),
                labels: vec![
                    ("instance".to_string(), "shed-bench".to_string()),
                    ("series".to_string(), format!("s{s:06}")),
                ],
                ts_ms: now_ms - age * SAMPLE_STEP_MS,
                value: (s * per_series + j) as f64,
            });
        }
    }
    Ok(samples)
}

/// One Remote Write 1.0 body, encoded once, with what decoding it found.
#[derive(Clone, Debug)]
pub struct Body {
    pub spec: BodySpec,
    pub bytes: Bytes,
    pub decoded_series: usize,
    pub decoded_samples: usize,
    /// The decoded request, kept for [`Body::normalize`].
    pub resolved: ResolvedRequest,
}

impl Body {
    /// Encodes `spec`'s samples and decodes the result with the server's
    /// Remote Write 1.0 decoder, refusing a body that does not decode to
    /// exactly `spec`'s series and samples.
    pub fn build(spec: BodySpec, now_ms: i64) -> anyhow::Result<Self> {
        let bytes = Bytes::from(encode_rw1_body(&body_samples(spec, now_ms)?)?);
        let decoded = ravel_remote_write::decode_write_request(&bytes, DECODE_LIMIT_BYTES)
            .with_context(|| format!("decoding the {spec} body"))?;
        let decoded_series = decoded.series.len();
        let decoded_samples: usize = decoded.series.iter().map(|s| s.samples.len()).sum();
        if decoded_series != spec.series {
            bail!("the {spec} body decodes to {decoded_series} series");
        }
        if decoded
            .series
            .iter()
            .any(|s| s.samples.len() != spec.samples_per_series)
        {
            bail!(
                "a series of the {spec} body does not decode to {} samples",
                spec.samples_per_series
            );
        }
        Ok(Body {
            spec,
            bytes,
            decoded_series,
            decoded_samples,
            resolved: decoded,
        })
    }

    /// What the Remote Write handler's `normalize_resolved` call makes of
    /// this body for `tenant` under `limits` at the ingest clock reading
    /// `now_ns`.
    pub fn normalize(&self, tenant: &TenantId, limits: &IngestLimits, now_ns: i64) -> Normalized {
        let out = ravel_remote_write::normalize_resolved(
            tenant,
            self.resolved.clone(),
            limits,
            &ravel_ipcrypt::rules::TenantRules::none(),
            now_ns,
        );
        Normalized {
            points: out.points.len() + out.histogram_points.len(),
            rejections: out.rejected.iter().map(|r| r.to_string()).collect(),
            exemplars_dropped: out.exemplars_dropped,
        }
    }
}

/// The limits the lane's server normalizes Remote Write with: the defaults,
/// with the admission lag `ravel_server::start` derives from the
/// `max_ingest_lag` [`start_ingest_server`] passes.
pub fn server_ingest_limits() -> anyhow::Result<IngestLimits> {
    Ok(IngestLimits {
        max_ingest_lag_ns: ravel_server::resolve_ingest_lag(MAX_INGEST_LAG)?.admission_lag_ns,
        ..IngestLimits::default()
    })
}

/// One body normalized as the handler normalizes it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Normalized {
    /// Scalar and histogram points normalization kept.
    pub points: usize,
    /// Every rejection, as its message.
    pub rejections: Vec<String>,
    pub exemplars_dropped: usize,
}

impl Normalized {
    /// Every way normalization did not keep exactly `spec`'s
    /// series x samples per series: one entry per rejection, then the point
    /// count, then dropped exemplars.
    pub fn misses(&self, spec: BodySpec) -> Vec<String> {
        let want = spec.series.saturating_mul(spec.samples_per_series);
        let mut misses: Vec<String> = self
            .rejections
            .iter()
            .map(|r| format!("normalize rejected: {r}"))
            .collect();
        if self.points != want {
            misses.push(format!(
                "normalized points = {}, band == {want}",
                self.points
            ));
        }
        if self.exemplars_dropped != 0 {
            misses.push(format!(
                "normalize dropped {} exemplars, band == 0",
                self.exemplars_dropped
            ));
        }
        misses
    }
}

/// The tenant indices the runs of body `body_index` use, two per repeat, in
/// the order the runs take them.
pub fn body_tenants(body_index: usize, repeats: usize) -> Range<usize> {
    let per_body = repeats.saturating_mul(2);
    body_index.saturating_mul(per_body)..(body_index + 1).saturating_mul(per_body)
}

/// One arm run: K requests against a fresh server, and what `/metrics` and
/// the CPU clock moved by across them.
#[derive(Clone, Debug, Serialize)]
pub struct ArmRun {
    pub arm: Arm,
    /// The repeat's arm order, `PH` or `HP`.
    pub order: &'static str,
    pub series: usize,
    pub samples_per_series: usize,
    pub body_bytes: usize,
    pub repeat: usize,
    pub requests: usize,
    pub client_429: u64,
    /// Answers with a status other than 429.
    pub client_other_status: u64,
    /// The status of the earliest of those answers, in the order the client
    /// got them across every worker.
    pub first_other_status: Option<u16>,
    /// Requests that got no answer: the send or the response read failed.
    pub transport_errors: u64,
    /// The earliest transport error, in the order the client saw them across
    /// every worker, with its whole cause chain.
    pub first_transport_error: Option<String>,
    pub pending_ack_shed_delta: u64,
    pub concurrency_shed_delta: u64,
    /// Store PUTs that completed inside the region.
    pub store_put_delta: u64,
    /// Calls the arm's gate held right after the region.
    pub held_after_region: usize,
    /// `ravel_store_calls_total{op="put"}` right before the region.
    pub store_puts_before_region: u64,
    /// The same counter once the parked request has been released and answered.
    pub store_puts_after_release: u64,
    pub cpu_ns: u64,
    pub wall_secs: f64,
    /// Wall seconds from the parked request's submit to the region's start.
    pub parked_submit_to_region_secs: f64,
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

    /// CPU seconds over wall seconds: cores busy on average across the
    /// region. `None` for a region that measured no wall time.
    pub fn cores_busy(&self) -> Option<f64> {
        (self.wall_secs > 0.0).then(|| self.cpu_secs() / self.wall_secs)
    }

    /// `None` for a region that measured no wall time.
    pub fn requests_per_sec(&self) -> Option<f64> {
        (self.wall_secs > 0.0).then(|| self.requests as f64 / self.wall_secs)
    }

    /// Wall seconds from the parked request's submit to the region's end:
    /// how long it had waited under the strict-ack deadline by then.
    pub fn parked_wait_secs(&self) -> f64 {
        self.parked_submit_to_region_secs + self.wall_secs
    }
}

/// A timespec as nanoseconds; a negative field is an error, never a zero.
fn timespec_ns(sec: i64, nsec: i64) -> anyhow::Result<u64> {
    let secs = u64::try_from(sec).with_context(|| format!("timespec seconds {sec} < 0"))?;
    let nanos = u64::try_from(nsec).with_context(|| format!("timespec nanoseconds {nsec} < 0"))?;
    Ok(secs.saturating_mul(1_000_000_000).saturating_add(nanos))
}

/// Process CPU time, user plus system over every thread, in nanoseconds.
pub fn process_cpu_ns() -> anyhow::Result<u64> {
    let ts = rustix::time::clock_gettime(rustix::time::ClockId::ProcessCPUTime);
    timespec_ns(ts.tv_sec, ts.tv_nsec).context("process CPU clock")
}

/// The smallest non-zero step [`process_cpu_ns`] took over `reads`
/// back-to-back changes of its value.
pub fn cpu_clock_resolution_ns(reads: usize) -> anyhow::Result<u64> {
    let mut smallest = u64::MAX;
    for _ in 0..reads {
        let start = process_cpu_ns()?;
        let mut next = process_cpu_ns()?;
        while next <= start {
            next = process_cpu_ns()?;
        }
        smallest = smallest.min(next - start);
    }
    Ok(smallest)
}

/// The client every arm sends with. It keeps no idle connection and asks the
/// server to close each one, so no request rides a connection an earlier
/// refusal closed.
pub fn lane_client() -> anyhow::Result<reqwest::Client> {
    let mut headers = reqwest::header::HeaderMap::new();
    headers.insert(
        reqwest::header::CONNECTION,
        reqwest::header::HeaderValue::from_static("close"),
    );
    Ok(reqwest::Client::builder()
        .timeout(CLIENT_TIMEOUT)
        .pool_max_idle_per_host(0)
        .default_headers(headers)
        .build()?)
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
/// per-tenant admission limits are unlimited: K bodies of 10,000 series can
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
        max_ingest_lag: MAX_INGEST_LAG,
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

/// Sends one Remote Write 1.0 request and reads its whole response. `Err` is
/// a transport failure with its whole cause chain.
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

/// What K requests got back. Each "first" is held with its answer's sequence
/// number, the order answers arrived across every worker, so a merge keeps
/// the earliest rather than whichever worker finished first.
#[derive(Default)]
struct Outcomes {
    too_many: u64,
    other_status: u64,
    first_other_status: Option<(usize, u16)>,
    transport_errors: u64,
    first_transport_error: Option<(usize, String)>,
}

/// The one of `a` and `b` with the lower sequence number.
fn earlier<T>(a: Option<(usize, T)>, b: Option<(usize, T)>) -> Option<(usize, T)> {
    match (a, b) {
        (Some(a), Some(b)) => Some(if b.0 < a.0 { b } else { a }),
        (a, b) => a.or(b),
    }
}

impl Outcomes {
    /// Records the answer that arrived `seq`th.
    fn record(&mut self, seq: usize, answer: Result<reqwest::StatusCode, String>) {
        match answer {
            Ok(reqwest::StatusCode::TOO_MANY_REQUESTS) => self.too_many += 1,
            Ok(status) => {
                self.other_status += 1;
                self.first_other_status =
                    earlier(self.first_other_status.take(), Some((seq, status.as_u16())));
            }
            Err(chain) => {
                self.transport_errors += 1;
                self.first_transport_error =
                    earlier(self.first_transport_error.take(), Some((seq, chain)));
            }
        }
    }

    fn merge(&mut self, other: Outcomes) {
        self.too_many += other.too_many;
        self.other_status += other.other_status;
        self.transport_errors += other.transport_errors;
        self.first_other_status = earlier(self.first_other_status.take(), other.first_other_status);
        self.first_transport_error = earlier(
            self.first_transport_error.take(),
            other.first_transport_error,
        );
    }
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
    let answered = Arc::new(AtomicUsize::new(0));
    let mut workers = tokio::task::JoinSet::new();
    for _ in 0..CLIENT_CONCURRENCY {
        let (client, url, token, body, next, answered) = (
            client.clone(),
            url.to_string(),
            token.to_string(),
            body.clone(),
            next.clone(),
            answered.clone(),
        );
        workers.spawn(async move {
            let mut outcomes = Outcomes::default();
            while next.fetch_add(1, Ordering::Relaxed) < requests {
                let answer = post_write(&client, &url, &token, body.clone()).await;
                outcomes.record(answered.fetch_add(1, Ordering::Relaxed), answer);
            }
            outcomes
        });
    }
    let mut total = Outcomes::default();
    while let Some(joined) = workers.join_next().await {
        total.merge(joined.context("request worker")?);
    }
    Ok(total)
}

/// The label pairs of one exposition label set (the text between `{` and
/// `}`), in order. `None` when the text is not a well-formed label set.
fn parse_label_set(text: &str) -> Option<Vec<(String, String)>> {
    let mut pairs = Vec::new();
    let mut rest = text;
    while !rest.is_empty() {
        let (name, quoted) = rest.split_once("=\"")?;
        let mut value = String::new();
        let mut chars = quoted.char_indices();
        let end = loop {
            match chars.next()? {
                (_, '\\') => match chars.next()? {
                    (_, 'n') => value.push('\n'),
                    (_, escaped) => value.push(escaped),
                },
                (i, '"') => break i,
                (_, c) => value.push(c),
            }
        };
        pairs.push((name.to_string(), value));
        rest = &quoted[end + 1..];
        if let Some(after_comma) = rest.strip_prefix(',') {
            rest = after_comma;
        } else if !rest.is_empty() {
            return None;
        }
    }
    Some(pairs)
}

/// The value of the one sample of `family` whose label set is exactly
/// `labels`, in any order. Absent, present more than once, or a label set
/// that does not parse is an error, never a zero.
pub fn sample_value(
    exposition: &str,
    family: &str,
    labels: &[(&str, &str)],
) -> anyhow::Result<u64> {
    let mut want: Vec<(String, String)> = labels
        .iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect();
    want.sort();
    let mut values = Vec::new();
    for line in exposition.lines() {
        let Some(rest) = line
            .strip_prefix(family)
            .and_then(|rest| rest.strip_prefix('{'))
        else {
            continue;
        };
        let (label_text, value) = rest
            .rsplit_once("} ")
            .with_context(|| format!("{family} line `{line}` has no closed label set"))?;
        let mut got = parse_label_set(label_text)
            .with_context(|| format!("{family} line `{line}` has a malformed label set"))?;
        got.sort();
        if got == want {
            values.push(value);
        }
    }
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
        [] => bail!("/metrics carries no {family} sample with labels {labels:?}"),
        many => bail!(
            "/metrics carries {} {family} samples with labels {labels:?}",
            many.len()
        ),
    }
}

const PENDING_ACK_SHED: &str = "ravel_ingest_pending_ack_shed_total";
const CONCURRENCY_SHED: &str = "ravel_ingest_concurrency_shed_total";
const PENDING_STRICT_ACKS: &str = "ravel_ingest_pending_strict_acks";
const STORE_CALLS: &str = "ravel_store_calls_total";
const MODE_LABEL: (&str, &str) = ("mode", "gateway");

struct Counters {
    pending_ack_shed: u64,
    concurrency_shed: u64,
    store_puts: u64,
    pending_strict_acks: u64,
}

impl Counters {
    fn read(exposition: &str) -> anyhow::Result<Self> {
        Ok(Counters {
            pending_ack_shed: sample_value(exposition, PENDING_ACK_SHED, &[MODE_LABEL])?,
            concurrency_shed: sample_value(exposition, CONCURRENCY_SHED, &[MODE_LABEL])?,
            store_puts: sample_value(exposition, STORE_CALLS, &[MODE_LABEL, ("op", "put")])?,
            pending_strict_acks: sample_value(exposition, PENDING_STRICT_ACKS, &[MODE_LABEL])?,
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
/// scrapes and the parked request run on the caller's runtime. On every path
/// out after the server starts, the parked request is released and joined
/// and the server is shut down.
pub async fn measure_arm(
    server: &tokio::runtime::Handle,
    client: &reqwest::Client,
    arm: Arm,
    body: &Body,
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
    let http_addr = running.http_addr;
    let url = format!("http://{http_addr}/api/v1/write");
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
    let parked_submit = Instant::now();
    let mut parked = {
        let (client, url, token, bytes) = (
            client.clone(),
            url.clone(),
            token.clone(),
            body.bytes.clone(),
        );
        tokio::spawn(async move { post_write(&client, &url, &token, bytes).await })
    };

    let measured = async {
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
            let parked_slots = Counters::read(&scrape(client, http_addr, SCRAPE_TIMEOUT).await?)?
                .pending_strict_acks;
            if parked_slots != 1 {
                bail!(
                    "arm P: {PENDING_STRICT_ACKS} reads {parked_slots} with one request parked, not 1"
                );
            }
        }

        let before = Counters::read(&scrape(client, http_addr, SCRAPE_TIMEOUT).await?)?;
        let cpu_before = process_cpu_ns()?;
        let wall_start = Instant::now();
        let parked_submit_to_region_secs = wall_start.duration_since(parked_submit).as_secs_f64();
        let outcomes = send_requests(client, &url, &token, &body.bytes, requests).await?;
        let wall_secs = wall_start.elapsed().as_secs_f64();
        let cpu_after = process_cpu_ns()?;
        let held_after_region = gate.held().len();
        let after = Counters::read(&scrape(client, http_addr, SCRAPE_TIMEOUT).await?)?;
        let parked_unanswered_after = !parked.is_finished();

        let parked_status = match release_until(&gate, &mut parked).await? {
            Ok(status) => status.as_u16(),
            Err(err) => bail!("arm {}: the parked request failed: {err}", arm.label()),
        };
        let released = Counters::read(&scrape(client, http_addr, SCRAPE_TIMEOUT).await?)?;

        Ok(ArmRun {
            arm,
            order: order_label(repeat),
            series: body.spec.series,
            samples_per_series: body.spec.samples_per_series,
            body_bytes: body.bytes.len(),
            repeat,
            requests,
            client_429: outcomes.too_many,
            client_other_status: outcomes.other_status,
            first_other_status: outcomes.first_other_status.map(|(_, status)| status),
            transport_errors: outcomes.transport_errors,
            first_transport_error: outcomes.first_transport_error.map(|(_, chain)| chain),
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
            held_after_region,
            store_puts_before_region: before.store_puts,
            store_puts_after_release: released.store_puts,
            cpu_ns: delta("process CPU clock", cpu_before, cpu_after)?,
            wall_secs,
            parked_submit_to_region_secs,
            pending_strict_acks_after: after.pending_strict_acks,
            parked_unanswered_after,
            parked_status,
        })
    }
    .await;

    let releaser = tokio::spawn(release_forever(gate));
    if !parked.is_finished() && tokio::time::timeout(WAIT, &mut parked).await.is_err() {
        parked.abort();
    }
    let shutdown = on_server(server, move || running.shutdown()).await;
    releaser.abort();
    let run = measured?;
    shutdown?;
    Ok(run)
}

/// Every count band `run` misses, in a fixed order: the 429 count, transport
/// errors, the two shed deltas, completed PUTs inside the region, arm P's
/// pending slot, the calls the gate held after the region (exactly the
/// parked one in either arm), the PUT counter's liveness, whether the parked
/// request held its slot through the region and was acknowledged once
/// released, then the parked request's wait against [`PARKED_WAIT_BAND`].
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
    check("client transport errors", run.transport_errors, 0);
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
    check("store PUTs completed in the region", run.store_put_delta, 0);
    if run.arm == Arm::PostDecode {
        check(
            "pending_strict_acks after the region",
            run.pending_strict_acks_after,
            1,
        );
    }
    check(
        "gate held calls after the region",
        run.held_after_region as u64,
        1,
    );
    check(
        "store PUT counter rose once the parked request answered",
        u64::from(run.store_puts_after_release > run.store_puts_before_region),
        1,
    );
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
    let waited = run.parked_wait_secs();
    let band = PARKED_WAIT_BAND.as_secs_f64();
    if waited.is_nan() || waited >= band {
        misses.push(format!(
            "parked request waited {waited:.3} s from its submit to the region end \
             ({:.3} s to the region start, then the {:.3} s region), band < {band} s under \
             the server's {} s strict-ack deadline: K is too large for this body on this host",
            run.parked_submit_to_region_secs,
            run.wall_secs,
            STRICT_ACK_DEADLINE.as_secs_f64()
        ));
    }
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

pub fn cpu_band(spec: BodySpec) -> Option<CpuBand> {
    let band = |lo_us, hi_us, miss_below_us, miss_above_us| {
        Some(CpuBand {
            lo_us,
            hi_us,
            miss_below_us,
            miss_above_us,
        })
    };
    match (spec.series, spec.samples_per_series) {
        (1, 1) => band(3.0, 20.0, Some(-50.0), 200.0),
        (1_000, 1) => band(500.0, 2_500.0, None, 10_000.0),
        (1_000, 10) => band(700.0, 3_500.0, None, 15_000.0),
        (10_000, 1) => band(5_000.0, 25_000.0, Some(2_000.0), 100_000.0),
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
    /// No band is registered for this body.
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

    /// The bin's exit code for this verdict alone: 5 past a miss bound, 3
    /// outside the prediction, 0 otherwise.
    pub fn exit_code(self) -> u8 {
        match self {
            CpuVerdict::Miss => 5,
            CpuVerdict::OutsidePrediction => 3,
            CpuVerdict::InBand | CpuVerdict::NoBand => 0,
        }
    }
}

/// An exit code's precedence: 1, then 5, 3, 2, and 0 last.
fn exit_severity(code: u8) -> u8 {
    match code {
        1 => 4,
        5 => 3,
        3 => 2,
        2 => 1,
        _ => 0,
    }
}

/// The bin's exit code over every body: the most severe of
/// [`BodyReport::exit_code`], so one body's outcome never masks a worse one
/// on another.
pub fn exit_code(reports: &[BodyReport]) -> u8 {
    reports
        .iter()
        .map(BodyReport::exit_code)
        .max_by_key(|&code| exit_severity(code))
        .unwrap_or(0)
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

/// Why a body's figure is withheld: the first repeat whose `|cpu_P - cpu_H|`
/// is under [`RESOLUTION_FACTOR`] times the clock's resolution. The
/// magnitude is compared, so a large negative difference is a figure too.
pub fn resolution_refusal(pairs: &[(ArmRun, ArmRun)], resolution_ns: u64) -> Option<String> {
    let floor_ns = resolution_ns.saturating_mul(RESOLUTION_FACTOR);
    pairs.iter().find_map(|(p, h)| {
        let difference_ns = p.cpu_ns.abs_diff(h.cpu_ns);
        (difference_ns < floor_ns).then(|| {
            format!(
                "repeat {}: |cpu_P - cpu_H| = {difference_ns} ns, under {RESOLUTION_FACTOR} x \
                 the {resolution_ns} ns resolution ({floor_ns} ns): raise --requests",
                p.repeat
            )
        })
    })
}

/// Every run of one body, and the figure the body reports.
#[derive(Clone, Debug, Serialize)]
pub struct BodyReport {
    pub series: usize,
    pub samples_per_series: usize,
    pub body_bytes: usize,
    pub decoded_series: usize,
    pub decoded_samples: usize,
    pub runs: Vec<ArmRun>,
    /// One per repeat, arm P's run paired with arm H's of the same repeat.
    /// Empty when [`Self::resolution_refusal`] is set.
    pub extra_cpu_per_refusal_us: Vec<f64>,
    /// Absent when [`Self::resolution_refusal`] is set.
    pub extra_cpu_per_refusal_us_median: Option<f64>,
    pub band: Option<CpuBand>,
    pub verdict: Option<CpuVerdict>,
    pub count_misses: Vec<String>,
    /// Why the body's figure is withheld; see [`resolution_refusal`].
    pub resolution_refusal: Option<String>,
}

impl BodyReport {
    /// `pairs` holds one (arm P, arm H) run per repeat, whatever order the
    /// two ran in.
    pub fn build(body: &Body, pairs: Vec<(ArmRun, ArmRun)>, resolution_ns: u64) -> Self {
        let resolution_refusal = resolution_refusal(&pairs, resolution_ns);
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
                        "{} body, arm {} repeat {}: {miss}",
                        body.spec,
                        run.arm.label(),
                        run.repeat
                    )
                }));
                runs.push(run);
            }
        }
        let band = cpu_band(body.spec);
        let median_us = median(&extra);
        BodyReport {
            series: body.spec.series,
            samples_per_series: body.spec.samples_per_series,
            body_bytes: body.bytes.len(),
            decoded_series: body.decoded_series,
            decoded_samples: body.decoded_samples,
            runs,
            extra_cpu_per_refusal_us: extra,
            extra_cpu_per_refusal_us_median: median_us,
            band,
            verdict: median_us.map(|m| CpuVerdict::of(band, m)),
            count_misses,
            resolution_refusal,
        }
    }

    pub fn spec(&self) -> BodySpec {
        BodySpec::new(self.series, self.samples_per_series)
    }

    /// This body's exit code: 1 on any count miss; otherwise its CPU
    /// verdict's ([`CpuVerdict::exit_code`]); 2 when it has no verdict: the
    /// figure was withheld, or no repeat ran.
    pub fn exit_code(&self) -> u8 {
        if !self.count_misses.is_empty() {
            return 1;
        }
        match self.verdict {
            Some(verdict) => verdict.exit_code(),
            None => 2,
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    const K: usize = 50;
    const SPEC: BodySpec = BodySpec::new(1_000, 1);

    fn assert_count_bands(run: &ArmRun) {
        let k = K as u64;
        let label = run.arm.label();
        assert_eq!(run.client_429, k, "arm {label}: client 429 count: {run:?}");
        assert_eq!(
            run.transport_errors, 0,
            "arm {label}: client transport errors: {run:?}"
        );
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
            "arm {label}: store PUTs completed in the region: {run:?}"
        );
        assert_eq!(
            run.held_after_region, 1,
            "arm {label}: the gate holds exactly the parked call after the region: {run:?}"
        );
        assert!(
            run.parked_wait_secs() < PARKED_WAIT_BAND.as_secs_f64(),
            "arm {label}: the parked request's wait stays inside the strict-ack deadline: {run:?}"
        );
        assert!(
            run.store_puts_after_release > run.store_puts_before_region,
            "arm {label}: the PUT counter rose once the parked request answered: {run:?}"
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
    /// handler over HTTP on fresh connections: every request answers 429 with
    /// no transport error, the real `/metrics` counts each refusal under its
    /// own family and no other, no store PUT completes inside the region,
    /// each arm's gate holds exactly the parked call, the parked request's
    /// wait stays inside the strict-ack deadline, and the PUT counter rises
    /// once the parked request is released. Every `/metrics` read goes
    /// through `sample_value`, so each family the lane reads appears exactly
    /// once with its exact label set on the real endpoint.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn lane_counts_close_at_small_k() {
        let now_ns = crate::saturation_run::wall_now_ns();
        let body = Body::build(SPEC, now_ns / 1_000_000).expect("body encodes");
        let limits = server_ingest_limits().expect("limits resolve");
        let client = lane_client().expect("client builds");
        let server = tokio::runtime::Handle::current();
        for (tenant_index, arm) in arms_in_order(0).into_iter().enumerate() {
            assert_eq!(
                body.normalize(&tenant(tenant_index), &limits, now_ns)
                    .misses(SPEC),
                Vec::<String>::new(),
                "normalize keeps every sample of the body"
            );
            let run = measure_arm(&server, &client, arm, &body, K, 0, tenant_index)
                .await
                .expect("arm runs");
            assert_eq!(run.order, "PH");
            assert_count_bands(&run);
        }
    }

    #[test]
    fn bodies_decode_to_the_requested_series_and_samples() {
        for spec in [BodySpec::new(1, 1), BodySpec::new(3, 4), SPEC] {
            let body = Body::build(spec, 1_760_000_000_000).unwrap();
            assert_eq!(body.decoded_series, spec.series, "{spec}");
            assert_eq!(
                body.decoded_samples,
                spec.series * spec.samples_per_series,
                "{spec}"
            );
        }
        let wide = body_samples(BodySpec::new(43, 1), 10_000).unwrap();
        assert_eq!(
            wide[42].labels,
            [
                ("instance".to_string(), "shed-bench".to_string()),
                ("series".to_string(), "s000042".to_string())
            ]
        );
        let samples = body_samples(BodySpec::new(2, 3), 10_000).unwrap();
        let ts: Vec<i64> = samples.iter().map(|s| s.ts_ms).collect();
        assert_eq!(ts, [8_000, 9_000, 10_000, 8_000, 9_000, 10_000]);
        assert!(samples.iter().all(|s| s.validate().is_ok()));
    }

    #[test]
    fn body_specs_parse_and_refuse() {
        assert_eq!("1000x10".parse(), Ok(BodySpec::new(1_000, 10)));
        assert_eq!(BodySpec::new(10_000, 1).to_string(), "10000x1");
        for bad in ["1000", "0x1", "1x0", "ax1", "1x", "x1"] {
            assert!(bad.parse::<BodySpec>().is_err(), "{bad}");
        }
    }

    #[test]
    fn arm_order_alternates_per_repeat() {
        assert_eq!(arms_in_order(0), [Arm::PostDecode, Arm::Head]);
        assert_eq!(arms_in_order(1), [Arm::Head, Arm::PostDecode]);
        assert_eq!(arms_in_order(2), [Arm::PostDecode, Arm::Head]);
        assert_eq!(
            [order_label(0), order_label(1), order_label(2)],
            ["PH", "HP", "PH"]
        );
    }

    #[test]
    fn sample_value_matches_the_exact_label_set_once() {
        let body = "# HELP x\n\
                    ravel_x{mode=\"gateway\",op=\"put\"} 3\n\
                    ravel_x{mode=\"gateway\",op=\"get\"} 9\n\
                    ravel_x{mode=\"gateway\",xop=\"put\"} 5\n\
                    ravel_x{mode=\"gateway\",op=\"put\",extra=\"1\"} 6\n\
                    ravel_xy{mode=\"gateway\"} 4\n";
        let put = [("mode", "gateway"), ("op", "put")];
        assert_eq!(sample_value(body, "ravel_x", &put).unwrap(), 3);
        assert_eq!(
            sample_value(body, "ravel_x", &[("op", "put"), ("mode", "gateway")]).unwrap(),
            3,
            "label order does not matter"
        );
        assert_eq!(
            sample_value(body, "ravel_x", &[("mode", "gateway"), ("xop", "put")]).unwrap(),
            5
        );
        assert!(
            sample_value(body, "ravel_x", &[("op", "put")]).is_err(),
            "a subset of the labels matches nothing"
        );
        assert!(sample_value(body, "ravel_z", &[]).is_err(), "absent");
        assert_eq!(
            sample_value(body, "ravel_xy", &[("mode", "gateway")]).unwrap(),
            4
        );
        let twice = "ravel_x{mode=\"gateway\"} 1\nravel_x{mode=\"gateway\"} 2\n";
        assert!(
            sample_value(twice, "ravel_x", &[("mode", "gateway")]).is_err(),
            "two samples"
        );
        let malformed = "ravel_x{mode=\"gateway} 1\n";
        assert!(sample_value(malformed, "ravel_x", &[("mode", "gateway")]).is_err());
    }

    #[test]
    fn label_sets_parse_escapes_and_refuse_garbage() {
        assert_eq!(
            parse_label_set(r#"a="x\"y",b="z\\""#),
            Some(vec![
                ("a".to_string(), "x\"y".to_string()),
                ("b".to_string(), "z\\".to_string())
            ])
        );
        assert_eq!(parse_label_set(""), Some(vec![]));
        assert_eq!(parse_label_set(r#"a="x"b="y""#), None);
        assert_eq!(parse_label_set(r#"a="x"#), None);
    }

    #[test]
    fn timespec_refuses_a_negative_field() {
        assert_eq!(timespec_ns(2, 5).unwrap(), 2_000_000_005);
        assert!(timespec_ns(-1, 0).is_err());
        assert!(timespec_ns(0, -1).is_err());
    }

    /// A run that holds every count band.
    fn synthetic_run(arm: Arm, repeat: usize, cpu_ns: u64) -> ArmRun {
        let (pending_ack_shed_delta, concurrency_shed_delta) = match arm {
            Arm::PostDecode => (1_000, 0),
            Arm::Head => (0, 1_000),
        };
        ArmRun {
            arm,
            order: order_label(repeat),
            series: SPEC.series,
            samples_per_series: SPEC.samples_per_series,
            body_bytes: 0,
            repeat,
            requests: 1_000,
            client_429: 1_000,
            client_other_status: 0,
            first_other_status: None,
            transport_errors: 0,
            first_transport_error: None,
            pending_ack_shed_delta,
            concurrency_shed_delta,
            store_put_delta: 0,
            held_after_region: 1,
            store_puts_before_region: 0,
            store_puts_after_release: 1,
            cpu_ns,
            wall_secs: 1.0,
            parked_submit_to_region_secs: 0.1,
            pending_strict_acks_after: 1,
            parked_unanswered_after: true,
            parked_status: REMOTE_WRITE_ACK,
        }
    }

    fn synthetic_body() -> Body {
        Body {
            spec: SPEC,
            bytes: Bytes::new(),
            decoded_series: SPEC.series,
            decoded_samples: SPEC.series,
            resolved: ResolvedRequest::default(),
        }
    }

    fn synthetic_pair(repeat: usize, p_ns: u64, h_ns: u64) -> (ArmRun, ArmRun) {
        (
            synthetic_run(Arm::PostDecode, repeat, p_ns),
            synthetic_run(Arm::Head, repeat, h_ns),
        )
    }

    /// The lane's client opens a new TCP connection for every request,
    /// against a server whose answers would let a pooling client reuse one:
    /// HTTP/1.1, an empty body with its length, and no `Connection: close`.
    #[tokio::test]
    async fn lane_client_opens_a_fresh_connection_per_request() {
        const REQUESTS: usize = 5;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let accepts = Arc::new(AtomicUsize::new(0));
        let acceptor = tokio::spawn({
            let accepts = accepts.clone();
            async move {
                while let Ok((socket, _)) = listener.accept().await {
                    accepts.fetch_add(1, Ordering::SeqCst);
                    tokio::spawn(answer_every_request(socket));
                }
            }
        });
        let client = lane_client().expect("client builds");
        for _ in 0..REQUESTS {
            let response = client
                .get(format!("http://{addr}/"))
                .send()
                .await
                .expect("answered");
            assert_eq!(response.status(), reqwest::StatusCode::OK);
            response.bytes().await.expect("body read");
        }
        acceptor.abort();
        assert_eq!(accepts.load(Ordering::SeqCst), REQUESTS);
    }

    /// Answers every request head on `socket` with an empty 200 that leaves
    /// the connection open, until the client closes it.
    async fn answer_every_request(mut socket: tokio::net::TcpStream) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut pending = Vec::new();
        let mut buf = [0u8; 4096];
        loop {
            while let Some(end) = pending.windows(4).position(|w| w == b"\r\n\r\n") {
                pending.drain(..end + 4);
                if socket
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
                    .await
                    .is_err()
                {
                    return;
                }
            }
            match socket.read(&mut buf).await {
                Ok(0) | Err(_) => return,
                Ok(n) => pending.extend_from_slice(&buf[..n]),
            }
        }
    }

    /// The pre-run check binds: a body whose oldest sample is past the
    /// server's ingest-lag bound at the check's clock reading loses that
    /// sample in normalize, and the check reports the rejection and the short
    /// point count.
    #[test]
    fn a_sample_past_the_ingest_lag_bound_fails_the_normalize_check() {
        let limits = server_ingest_limits().unwrap();
        assert_eq!(
            limits.max_ingest_lag_ns,
            i64::try_from(MAX_INGEST_LAG.as_nanos()).unwrap()
        );
        let spec = BodySpec::new(1, 3);
        let now_ms = 1_760_000_000_000;
        let body = Body::build(spec, now_ms).unwrap();
        let tenant = tenant(0);

        let fresh = body.normalize(&tenant, &limits, now_ms * 1_000_000);
        assert_eq!(fresh.points, 3);
        assert_eq!(fresh.misses(spec), Vec::<String>::new());

        // The samples are 2 s, 1 s and 0 s old at `now_ms`; this reading puts
        // the oldest 0.5 s past the bound and the other two inside it.
        let late_ns = now_ms * 1_000_000 + limits.max_ingest_lag_ns - 1_500_000_000;
        let late = body.normalize(&tenant, &limits, late_ns);
        assert_eq!(late.points, 2);
        assert_eq!(late.rejections.len(), 1, "{late:?}");
        let misses = late.misses(spec);
        assert_eq!(misses.len(), 2, "{misses:?}");
        assert!(misses[0].starts_with("normalize rejected: "), "{misses:?}");
        assert_eq!(misses[1], "normalized points = 2, band == 3");
    }

    #[test]
    fn body_tenants_match_the_run_order() {
        assert_eq!(body_tenants(0, 3), 0..6);
        assert_eq!(body_tenants(1, 3), 6..12);
        assert_eq!(body_tenants(2, 1), 4..6);
    }

    /// A merge keeps the answer that arrived first, whichever worker's
    /// tally is merged first.
    #[test]
    fn outcomes_keep_the_earliest_answer() {
        let status = |code| Ok(reqwest::StatusCode::from_u16(code).unwrap());
        let mut early = Outcomes::default();
        early.record(2, status(503));
        early.record(5, Err("late".to_string()));
        early.record(6, status(500));
        let mut late = Outcomes::default();
        late.record(3, Err("early".to_string()));
        late.record(4, status(502));
        late.record(7, status(429));

        let mut total = Outcomes::default();
        total.merge(late);
        total.merge(early);
        assert_eq!(total.first_other_status, Some((2, 503)));
        assert_eq!(total.first_transport_error, Some((3, "early".to_string())));
        assert_eq!(
            (total.too_many, total.other_status, total.transport_errors),
            (1, 3, 2)
        );
    }

    /// Arm H's gate holds exactly the parked call too, and a parked wait at
    /// or past the band is a count miss naming K and the measured seconds.
    #[test]
    fn count_bands_hold_the_gate_and_the_parked_wait() {
        for arm in [Arm::PostDecode, Arm::Head] {
            assert_eq!(
                count_band_misses(&synthetic_run(arm, 0, 0)),
                Vec::<String>::new()
            );
            let mut idle_gate = synthetic_run(arm, 0, 0);
            idle_gate.held_after_region = 0;
            assert_eq!(
                count_band_misses(&idle_gate),
                ["gate held calls after the region = 0, band == 1"]
            );

            let mut inside = synthetic_run(arm, 0, 0);
            inside.parked_submit_to_region_secs = 7.0;
            inside.wall_secs = 0.9;
            assert_eq!(count_band_misses(&inside), Vec::<String>::new());

            let mut late = synthetic_run(arm, 0, 0);
            late.parked_submit_to_region_secs = 7.5;
            late.wall_secs = 0.6;
            let misses = count_band_misses(&late);
            assert_eq!(misses.len(), 1, "{misses:?}");
            assert!(
                misses[0].starts_with("parked request waited 8.100 s")
                    && misses[0].ends_with("K is too large for this body on this host"),
                "{misses:?}"
            );
        }
    }

    #[test]
    fn rates_refuse_a_zero_wall_time() {
        let mut run = synthetic_run(Arm::Head, 0, 2_000_000_000);
        assert_eq!(run.cores_busy(), Some(2.0));
        assert_eq!(run.requests_per_sec(), Some(1_000.0));
        run.wall_secs = 0.0;
        assert_eq!(run.cores_busy(), None);
        assert_eq!(run.requests_per_sec(), None);
    }

    /// The rule compares the arms' difference, not arm P's total: a large arm
    /// P total with a difference under 100 x the resolution is withheld, and
    /// a difference above it reports, in either sign.
    #[test]
    fn resolution_rule_withholds_a_small_difference() {
        let body = synthetic_body();
        let resolution_ns = 1_000;
        let pair = synthetic_pair;

        let small = BodyReport::build(
            &body,
            vec![
                pair(0, 5_000_000_000, 4_000_000_000),
                pair(1, 5_000_099_999, 5_000_000_000),
            ],
            resolution_ns,
        );
        let why = small.resolution_refusal.expect("withheld");
        assert!(
            why.starts_with("repeat 1: |cpu_P - cpu_H| = 99999 ns"),
            "{why}"
        );
        assert!(small.extra_cpu_per_refusal_us.is_empty());
        assert_eq!(small.extra_cpu_per_refusal_us_median, None);
        assert_eq!(small.verdict, None);

        let large = BodyReport::build(
            &body,
            vec![
                pair(0, 5_000_100_000, 5_000_000_000),
                pair(1, 4_000_000_000, 5_000_000_000),
            ],
            resolution_ns,
        );
        assert_eq!(large.resolution_refusal, None);
        assert_eq!(large.extra_cpu_per_refusal_us, [0.1, -1_000.0]);
        assert!(large.extra_cpu_per_refusal_us_median.is_some());
    }

    #[test]
    fn cpu_verdicts_follow_the_registered_bands() {
        let one = cpu_band(BodySpec::new(1, 1));
        assert_eq!(CpuVerdict::of(one, 10.0), CpuVerdict::InBand);
        assert_eq!(CpuVerdict::of(one, 2.0), CpuVerdict::OutsidePrediction);
        assert_eq!(CpuVerdict::of(one, 21.0), CpuVerdict::OutsidePrediction);
        assert_eq!(CpuVerdict::of(one, 201.0), CpuVerdict::Miss);
        assert_eq!(CpuVerdict::of(one, -51.0), CpuVerdict::Miss);
        let thousand = cpu_band(BodySpec::new(1_000, 1));
        assert_eq!(CpuVerdict::of(thousand, 1_000.0), CpuVerdict::InBand);
        assert_eq!(
            CpuVerdict::of(thousand, -1_000.0),
            CpuVerdict::OutsidePrediction
        );
        assert_eq!(CpuVerdict::of(thousand, 10_001.0), CpuVerdict::Miss);
        let ten_each = cpu_band(BodySpec::new(1_000, 10));
        assert_eq!(CpuVerdict::of(ten_each, 3_500.0), CpuVerdict::InBand);
        assert_eq!(CpuVerdict::of(ten_each, 15_001.0), CpuVerdict::Miss);
        let wide = cpu_band(BodySpec::new(10_000, 1));
        assert_eq!(CpuVerdict::of(wide, 25_000.0), CpuVerdict::InBand);
        assert_eq!(CpuVerdict::of(wide, 3_000.0), CpuVerdict::OutsidePrediction);
        assert_eq!(CpuVerdict::of(wide, 1_999.0), CpuVerdict::Miss);
        assert_eq!(CpuVerdict::of(wide, 100_001.0), CpuVerdict::Miss);
        assert_eq!(
            CpuVerdict::of(cpu_band(BodySpec::new(7, 1)), 1.0),
            CpuVerdict::NoBand
        );
        assert_eq!(CpuVerdict::Miss.exit_code(), 5);
        assert_eq!(CpuVerdict::OutsidePrediction.exit_code(), 3);
        assert_eq!(CpuVerdict::InBand.exit_code(), 0);
        assert_eq!(CpuVerdict::NoBand.exit_code(), 0);
    }

    /// Exit codes are decided per body and the most severe wins, in the
    /// order 1, 5, 3, 2: a withheld figure on one body never masks a miss on
    /// another, in either body order.
    #[test]
    fn exit_code_takes_the_most_severe_body() {
        let body = synthetic_body();
        let resolution_ns = 1_000;
        // 1000x1 band: 500 to 2,500 us, miss above 10,000 us; K = 1,000.
        let report = |p_ns: u64| {
            BodyReport::build(
                &body,
                vec![synthetic_pair(0, p_ns, 1_000_000_000)],
                resolution_ns,
            )
        };
        let withheld = report(1_000_000_001);
        let in_band = report(2_000_000_000);
        let outside = report(5_000_000_000);
        let cpu_miss = report(21_000_000_000);
        let count_miss = {
            let (mut p, h) = synthetic_pair(0, 2_000_000_000, 1_000_000_000);
            p.client_429 -= 1;
            BodyReport::build(&body, vec![(p, h)], resolution_ns)
        };
        assert_eq!(
            [&withheld, &in_band, &outside, &cpu_miss, &count_miss].map(BodyReport::exit_code),
            [2, 0, 3, 5, 1]
        );

        assert_eq!(exit_code(&[]), 0);
        assert_eq!(exit_code(std::slice::from_ref(&in_band)), 0);
        for (other, want) in [
            (&in_band, 2),
            (&outside, 3),
            (&cpu_miss, 5),
            (&count_miss, 1),
        ] {
            assert_eq!(exit_code(&[withheld.clone(), other.clone()]), want);
            assert_eq!(exit_code(&[other.clone(), withheld.clone()]), want);
        }
        assert_eq!(exit_code(&[cpu_miss.clone(), count_miss.clone()]), 1);
        assert_eq!(exit_code(&[outside.clone(), cpu_miss.clone()]), 5);
    }

    #[test]
    fn median_of_odd_and_even_counts() {
        assert_eq!(median(&[3.0, 1.0, 2.0]), Some(2.0));
        assert_eq!(median(&[4.0, 1.0, 2.0, 3.0]), Some(2.5));
        assert_eq!(median(&[]), None);
    }
}
