//! The server, fixture and probe plumbing the two ADR-1702 task 11 saturation
//! bins share (issue #2670). Band logic lives in [`crate::saturation`].
//!
//! The server is started in the same call order as `ravel-server`'s own
//! `main`: a [`Heartbeat`] built and spawned on the server runtime, a
//! [`HealthListener`] bound on it before [`ravel_server::start_with_heartbeat`],
//! and the readiness handle attached once that returns. The server runtime is
//! a multi-thread runtime with tokio's default worker count, one per core, the
//! same as the binary's bare `#[tokio::main]`. Each bin installs jemalloc as
//! its global allocator and calls [`configure_allocator`] before the server
//! starts, as `main` does before it binds a listener. Probes and load run on other runtimes and
//! threads, so a probe never waits for a server runtime worker to be free
//! before it is sent.
//!
//! Fixtures are RSEG objects written with the same [`SegmentWriter`] call and
//! the same commit record fields the ingest flush uses, published as L0
//! commits to a [`MemoryStore`], so a query resolves and decodes them through
//! the server's real read path.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

pub use crate::saturation::Probe;
use crate::saturation::{ProberStep, prober_step, slots_due};
use anyhow::{Context, bail};
use ravel_commit::keys;
use ravel_commit::publish::{self, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, StoreMetrics};
use ravel_segment::{
    Footer, IngestBounds, ReaderLimits, SegmentIdentity, SegmentWriter, SeriesInputV3,
    SeriesValues, WrittenSegment, open_from_full, parse_series_idx,
};
use ravel_server::health_listener::{HealthListener, Heartbeat};
use ravel_server::{FoldTaskConfig, Mode, ServerConfig};
use ravel_types::{Label, LabelSet, Sample, SeriesId, Signal, TenantHash, TenantId};
use uuid::Uuid;

/// Fixture tenant `i`.
pub fn tenant(i: usize) -> TenantId {
    TenantId::new(format!("saturation-{i}"))
}

/// The bearer token [`start_server`] maps to [`tenant`] `i`.
pub fn token(i: usize) -> String {
    format!("saturation-token-{i}")
}

pub const NS_PER_HOUR: i64 = 3_600_000_000_000;
/// Per-probe deadline. A probe that has not answered by then counts as
/// unanswered.
pub const PROBE_TIMEOUT: Duration = Duration::from_secs(5);
/// Deadline for a heartbeat `/metrics` scrape. Above the 10 s ADR bound, so a
/// stall of 10 s to 15 s, which misses that bound, still returns an age
/// rather than failing the scrape first.
pub const HEARTBEAT_SCRAPE_TIMEOUT: Duration = Duration::from_secs(15);

// RSEG section kinds (docs/segment-format.md), the catalog sections the query
// fetcher charges a catalog decode for.
const LABEL_DICT: u32 = 1;
const SERIES_IDS: u32 = 5;
const SERIES_META: u32 = 6;
const SERIES_IDX: u32 = 8;
const SERIES_META_CHUNKS: u32 = 9;
/// `Compression.COMPRESSION_NONE` from proto/ravel/segment.proto.
const COMPRESSION_NONE: i32 = 0;

pub fn wall_now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
        .unwrap_or(0)
}

/// The ingest hour two hours before now: old enough that nothing still
/// writes into it, recent enough for any lookback default.
pub fn fixture_hour(now_ns: i64) -> i64 {
    now_ns / NS_PER_HOUR - 2
}

pub fn labels(pairs: &[(&str, &str)]) -> anyhow::Result<LabelSet> {
    Ok(LabelSet::new(
        pairs
            .iter()
            .map(|(name, value)| Label {
                name: (*name).to_string(),
                value: (*value).to_string(),
            })
            .collect(),
    )?)
}

/// One scalar series of `metric` named by `labels` (which must carry
/// `__name__ = metric`).
pub fn series(
    tenant: &TenantId,
    metric: &str,
    labels: LabelSet,
    samples: Vec<Sample>,
) -> anyhow::Result<SeriesInputV3> {
    Ok(SeriesInputV3 {
        series_id: SeriesId::compute(tenant, metric, &labels)?,
        labels,
        values: SeriesValues::Scalar(samples),
    })
}

/// Encodes `series` into one RSEG object for `tenant` at `hour`, as the
/// ingest flush does.
pub fn write_segment(
    tenant: &TenantHash,
    writer_id: Uuid,
    hour: i64,
    series: Vec<SeriesInputV3>,
) -> anyhow::Result<WrittenSegment> {
    let flush_ns = hour * NS_PER_HOUR + NS_PER_HOUR - 1;
    let identity = SegmentIdentity {
        tenant_hash: tenant.0,
        shard: 0,
        writer_id: writer_id.to_string(),
        writer_epoch: 1,
        writer_seq: 1,
    };
    let bounds = IngestBounds {
        min_ingest_ts_ns: flush_ns,
        max_ingest_ts_ns: flush_ns,
    };
    SegmentWriter::write_histograms(series, identity, bounds).context("encode fixture segment")
}

/// Publishes `written` as an L0 metrics commit for `tenant` at `hour`, with
/// the commit record fields the ingest flush sets.
pub async fn publish_segment(
    store: &MemoryStore,
    tenant: &TenantHash,
    writer_id: Uuid,
    hour: i64,
    written: &WrittenSegment,
) -> anyhow::Result<()> {
    let flush_ns = hour * NS_PER_HOUR + NS_PER_HOUR - 1;
    let record = record::build(NewCommitRecord {
        tenant_hash: *tenant,
        signal: Signal::Metrics,
        shard: 0,
        writer_id,
        writer_epoch: 1,
        writer_seq: 1,
        object_size: written.bytes.len() as u64,
        content_hash: written.summary.blake3,
        sample_count: written.summary.sample_count,
        series_count: written.summary.series_count,
        min_event_ts_ns: written.summary.min_event_ts_ns,
        max_event_ts_ns: written.summary.max_event_ts_ns,
        min_ingest_ts_ns: flush_ns,
        max_ingest_ts_ns: flush_ns,
        segment_format_version: u32::from(ravel_ingest::SEGMENT_FORMAT_VERSION),
        created_unix_ns: flush_ns,
        ingest_hour_bucket: u32::try_from(hour).context("hour fits u32")?,
    })
    .context("build fixture commit record")?;
    let data_key = keys::reconstruct_data_key(&record).context("fixture data key")?;
    publish::put_data_object(store, &data_key, written.bytes.clone())
        .await
        .context("put fixture data object")?;
    publish::publish(store, &record, &RetryPolicy::default())
        .await
        .context("publish fixture commit record")?;
    Ok(())
}

/// The bytes the query fetcher reserves, and hands the read gate as the job
/// size, for one whole-catalog decode of `bytes`: every catalog section's
/// uncompressed length, with the chunked SERIES_META counted at its inflated
/// length. Mirrors `catalog_decode_len` in `ravel-query`'s fetcher.
pub fn catalog_decode_bytes(bytes: &[u8]) -> anyhow::Result<u64> {
    let limits = ReaderLimits::default();
    let located = open_from_full(bytes, limits).context("open fixture segment")?;
    Ok(catalog_decode_len(&located.footer, bytes, limits))
}

/// [`catalog_decode_bytes`] over an already parsed `footer` of `bytes`.
pub fn catalog_decode_len(footer: &Footer, bytes: &[u8], limits: ReaderLimits) -> u64 {
    let inflated_chunks = meta_chunks_inflated_len(footer, bytes, limits);
    footer
        .sections
        .iter()
        .filter(|s| {
            matches!(
                s.kind,
                LABEL_DICT | SERIES_IDS | SERIES_META | SERIES_IDX | SERIES_META_CHUNKS
            )
        })
        .map(|s| match (s.kind, inflated_chunks) {
            (SERIES_META_CHUNKS, Some(inflated)) => inflated,
            _ => ravel_memory::decoded_charge(
                s.uncompressed_len,
                limits.max_section_uncompressed_bytes,
            ),
        })
        .fold(0, u64::saturating_add)
}

/// The inflated SERIES_META chunk length the fetcher's
/// `meta_chunks_inflated_len` charges, with its filters: `None`, and so the
/// footer figure, when there is no SERIES_IDX, the section is stored
/// compressed, its stored bytes are out of bounds or fail the section crc32c,
/// or the directory does not parse.
fn meta_chunks_inflated_len(footer: &Footer, bytes: &[u8], limits: ReaderLimits) -> Option<u64> {
    let idx = footer
        .sections
        .iter()
        .find(|s| s.kind == SERIES_IDX)
        .filter(|s| s.comp == COMPRESSION_NONE)?;
    let start = usize::try_from(idx.offset).ok()?;
    let end = start.checked_add(usize::try_from(idx.len).ok()?)?;
    let section = bytes.get(start..end)?;
    if crc32c::crc32c(section) != idx.crc32c {
        return None;
    }
    let index = parse_series_idx(section).ok()?;
    Some(
        index
            .chunk_frame_uncompressed_lens()
            .map(|len| ravel_memory::decoded_charge(len, limits.max_section_uncompressed_bytes))
            .fold(0u64, u64::saturating_add),
    )
}

/// A started server with its health listener and heartbeat task.
pub struct StartedServer {
    pub running: ravel_server::Running,
    pub health: HealthListener,
    heartbeat_task: tokio::task::JoinHandle<()>,
}

impl StartedServer {
    pub fn http_addr(&self) -> SocketAddr {
        self.running.http_addr
    }

    pub fn health_addr(&self) -> SocketAddr {
        self.health.local_addr()
    }

    /// Drains the server, stops the heartbeat and joins the health thread, in
    /// `main`'s order.
    pub async fn shutdown(self) -> anyhow::Result<()> {
        self.running.shutdown().await?;
        self.heartbeat_task.abort();
        let health = self.health;
        tokio::task::spawn_blocking(move || health.shutdown())
            .await
            .context("health listener shutdown task")??;
        Ok(())
    }
}

/// Starts a query-mode server over `store`, the way `ravel-server`'s `main`
/// does, with `--listen-health 127.0.0.1:0`, the default read and write gate
/// permits for `cores`, the read cache off (`disable_cache`), no process
/// memory budget ceiling, and the default query budgets, which keep the
/// per-tenant and per-query SQL memory ceilings. Tenants `0..tenants` are
/// served, each with its own [`token`]. Must run on the server runtime.
pub async fn start_server(
    store: Arc<MemoryStore>,
    cores: usize,
    tenants: usize,
) -> anyhow::Result<StartedServer> {
    let tokens: HashMap<String, TenantId> = (0..tenants).map(|i| (token(i), tenant(i))).collect();
    let tenant_resolver = ravel_server::tenant::build_resolver(tokens, false);
    let loopback: SocketAddr = "127.0.0.1:0".parse()?;
    let config = ServerConfig {
        audit_pipeline: Default::default(),
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
        mode: Mode::Query,
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
        limits: ravel_server::LimitsConfig::default(),
        max_ingest_lag: ravel_server::DEFAULT_MAX_INGEST_LAG,
        deployment_key: None,
        gc: ravel_maintain::GcConfigValues::maintain_defaults(),
        query_deadline: Duration::from_secs(120),
        store_probe_interval: ravel_server::store_probe::DEFAULT_STORE_PROBE_INTERVAL,
        admission_reconcile_interval: ravel_ingest::DEFAULT_ADMISSION_RECONCILE_INTERVAL,
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
    let heartbeat = Heartbeat::new(Arc::new(ravel_ingest::SystemClock));
    let heartbeat_task = heartbeat.spawn();
    let health = HealthListener::bind(loopback, heartbeat.clone())?;
    let backend: Arc<dyn ObjectStoreBackend> = store;
    let running = ravel_server::start_with_heartbeat(
        config,
        backend.clone(),
        backend,
        Arc::new(StoreMetrics::default()),
        None,
        heartbeat,
    )
    .await?;
    health.attach_readiness(running.readiness());
    Ok(StartedServer {
        running,
        health,
        heartbeat_task,
    })
}

/// Percent-encodes a query parameter value.
pub fn encode_param(value: &str) -> String {
    let mut out = String::with_capacity(value.len() * 3);
    for b in value.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(char::from(b));
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

pub async fn scrape(
    client: &reqwest::Client,
    http: SocketAddr,
    timeout: Duration,
) -> anyhow::Result<String> {
    let response = client
        .get(format!("http://{http}/metrics"))
        .timeout(timeout)
        .send()
        .await
        .context("/metrics request")?;
    if !response.status().is_success() {
        bail!("/metrics answered {}", response.status());
    }
    response.text().await.context("/metrics body")
}

/// Fails, naming every missing family, when the server's `/metrics` lacks one
/// of [`crate::saturation::REQUIRED_FAMILIES`] by exact name, and naming the
/// count when the heartbeat family does not carry exactly one sample.
pub fn require_families(body: &str) -> anyhow::Result<()> {
    crate::saturation::check_start_families(body).map_err(anyhow::Error::msg)
}

/// Whether a query API answer is a success: HTTP 200 and, for a
/// Prometheus-shaped body, `"status":"success"`.
pub async fn query_succeeded(response: reqwest::Response) -> Result<(), String> {
    let code = response.status();
    let body = response.text().await.map_err(|e| e.to_string())?;
    if !code.is_success() {
        return Err(format!("HTTP {code}: {}", truncate(&body)));
    }
    if body.contains("\"status\":\"error\"") {
        return Err(format!("error body: {}", truncate(&body)));
    }
    Ok(())
}

fn truncate(body: &str) -> &str {
    let end = body.char_indices().nth(300).map_or(body.len(), |(i, _)| i);
    &body[..end]
}

/// Ends a measured window: the prober stops before the first slot due after
/// it, and every other loop stops at its next check.
#[derive(Debug)]
pub struct WindowStop {
    /// The last slot due in the window, `u64::MAX` while it is open.
    last_slot: AtomicU64,
}

impl WindowStop {
    pub fn new() -> Arc<Self> {
        Arc::new(WindowStop {
            last_slot: AtomicU64::new(u64::MAX),
        })
    }

    /// Closes the window at `window` from its start.
    pub fn close(&self, window: Duration) {
        self.last_slot.store(slots_due(window), Ordering::Release);
    }

    pub fn is_closed(&self) -> bool {
        self.last_slot.load(Ordering::Acquire) != u64::MAX
    }

    fn last_slot(&self) -> u64 {
        self.last_slot.load(Ordering::Acquire)
    }
}

/// A prober thread and the window start it took once it was ready.
pub struct Prober {
    handle: std::thread::JoinHandle<anyhow::Result<Vec<Probe>>>,
    /// The window start: taken by the prober thread after its runtime and
    /// HTTP client are built, so its setup is not counted in the window.
    pub start: Instant,
}

/// Probes `url` sequentially, one probe per slot, until `stop` closes the
/// window, on a dedicated thread with its own `current_thread` runtime.
/// Returns once the prober is ready to send its first probe, with the window
/// start it took then. Slot `k` is due at `start + k * period`. Which slot
/// comes next, and when the loop stops, is [`prober_step`]: a probe whose own
/// duration crossed later slots' due times skips them, and a prober that
/// wakes late skips nothing. Every probe sent is returned; whether it covered
/// a slot is decided afterwards by [`crate::saturation::slot_coverage`],
/// which refuses one sent more than a period late or after the window.
pub fn spawn_prober(
    url: String,
    period: Duration,
    stop: Arc<WindowStop>,
) -> anyhow::Result<Prober> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel::<Instant>(1);
    let handle = std::thread::Builder::new()
        .name("saturation-prober".to_string())
        .spawn(move || {
            runtime.block_on(async move {
                let client = reqwest::Client::builder().timeout(PROBE_TIMEOUT).build()?;
                let start = Instant::now();
                ready_tx
                    .send(start)
                    .map_err(|_| anyhow::anyhow!("prober start not received"))?;
                let mut probes: Vec<Probe> = Vec::new();
                while let ProberStep::Send(slot) =
                    prober_step(probes.last(), period, stop.last_slot())
                {
                    let offset = period
                        .checked_mul(u32::try_from(slot)?)
                        .context("probe slot offset")?;
                    tokio::time::sleep_until((start + offset).into()).await;
                    // The window can close during the sleep.
                    if prober_step(probes.last(), period, stop.last_slot()) == ProberStep::Stop {
                        break;
                    }
                    let issued = Instant::now();
                    let status = match client.get(&url).send().await {
                        Ok(response) => {
                            let code = response.status().as_u16();
                            // Read the whole answer before the probe counts as answered.
                            response.bytes().await.ok().map(|_| code)
                        }
                        Err(_) => None,
                    };
                    let probe = Probe {
                        slot,
                        issued_at: issued.duration_since(start),
                        latency: issued.elapsed(),
                        status,
                    };
                    probes.push(probe);
                }
                Ok(probes)
            })
        })?;
    match ready_rx.recv() {
        Ok(start) => Ok(Prober { handle, start }),
        // The thread dropped its sender before it was ready: its error says why.
        Err(_) => match join_prober(Prober {
            handle,
            start: Instant::now(),
        }) {
            Err(err) => Err(err.context("prober setup")),
            Ok(_) => bail!("prober exited before it was ready"),
        },
    }
}

/// Joins a prober thread.
pub fn join_prober(prober: Prober) -> anyhow::Result<Vec<Probe>> {
    prober
        .handle
        .join()
        .map_err(|_| anyhow::anyhow!("prober thread panicked"))?
}

/// Enables jemalloc's background purge thread unless `_RJEM_MALLOC_CONF`
/// sets it, as `ravel-server`'s `main` does before it binds a listener, and
/// prints the state read back from the allocator.
pub fn configure_allocator() {
    let malloc_conf = std::env::var(ravel_server::mem_stats::MALLOC_CONF_ENV).ok();
    let state = ravel_server::mem_stats::configure_background_thread(malloc_conf.as_deref());
    println!(
        "allocator: jemalloc background_thread enabled={} source={}",
        state.enabled, state.source
    );
}

/// Builds the multi-thread runtime the server runs on: tokio's default worker
/// count, which is one per core, as `#[tokio::main]` builds it.
pub fn server_runtime() -> anyhow::Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .thread_name("ravel-server-rt")
        .build()?)
}

/// A small runtime for the load generators and scrapers, apart from the
/// server's.
pub fn client_runtime() -> anyhow::Result<tokio::runtime::Runtime> {
    Ok(tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .thread_name("saturation-client")
        .build()?)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    /// A segment with enough series that the writer emits the chunked
    /// catalog (SERIES_IDX and SERIES_META_CHUNKS).
    fn chunked_segment() -> (Vec<u8>, Footer) {
        let tenant = tenant(0);
        let hour = 400_000;
        let input = (0..ravel_segment::V5_SPARSE_THRESHOLD)
            .map(|i| {
                let instance = format!("i-{i:06}");
                series(
                    &tenant,
                    "sat_t",
                    labels(&[("__name__", "sat_t"), ("instance", &instance)]).unwrap(),
                    vec![Sample {
                        ts_ns: hour * NS_PER_HOUR + 1,
                        value: 1.0,
                    }],
                )
                .unwrap()
            })
            .collect();
        let written = write_segment(&tenant.hash(), Uuid::nil(), hour, input).unwrap();
        let bytes = written.bytes.to_vec();
        let footer = open_from_full(&bytes, ReaderLimits::default())
            .unwrap()
            .footer;
        (bytes, footer)
    }

    fn section(footer: &Footer, kind: u32) -> usize {
        footer
            .sections
            .iter()
            .position(|s| s.kind == kind)
            .expect("section present")
    }

    /// The SERIES_META_CHUNKS figure at its footer length.
    fn chunks_footer_charge(footer: &Footer, limits: ReaderLimits) -> u64 {
        let chunks = &footer.sections[section(footer, SERIES_META_CHUNKS)];
        ravel_memory::decoded_charge(
            chunks.uncompressed_len,
            limits.max_section_uncompressed_bytes,
        )
    }

    /// The footer figures of the other catalog sections.
    fn other_catalog_charge(footer: &Footer, limits: ReaderLimits) -> u64 {
        footer
            .sections
            .iter()
            .filter(|s| matches!(s.kind, LABEL_DICT | SERIES_IDS | SERIES_META | SERIES_IDX))
            .map(|s| {
                ravel_memory::decoded_charge(
                    s.uncompressed_len,
                    limits.max_section_uncompressed_bytes,
                )
            })
            .sum()
    }

    #[test]
    fn uncompressed_series_idx_charges_the_inflated_chunks() {
        let limits = ReaderLimits::default();
        let (bytes, footer) = chunked_segment();
        let inflated = meta_chunks_inflated_len(&footer, &bytes, limits).expect("inflated");
        assert_ne!(inflated, chunks_footer_charge(&footer, limits));
        assert_eq!(
            catalog_decode_bytes(&bytes).unwrap(),
            other_catalog_charge(&footer, limits) + inflated
        );
    }

    /// The fetcher reads the chunk directory only from an uncompressed
    /// SERIES_IDX; a compressed one takes the footer figure.
    #[test]
    fn compressed_series_idx_takes_the_footer_figure() {
        let limits = ReaderLimits::default();
        let (bytes, footer) = chunked_segment();
        let rest = other_catalog_charge(&footer, limits);
        let mut compressed = footer.clone();
        compressed.sections[section(&footer, SERIES_IDX)].comp = 1;
        assert_eq!(meta_chunks_inflated_len(&compressed, &bytes, limits), None);
        assert_eq!(
            catalog_decode_len(&compressed, &bytes, limits),
            rest + chunks_footer_charge(&footer, limits)
        );
    }

    #[test]
    fn series_idx_failing_its_crc_takes_the_footer_figure() {
        let limits = ReaderLimits::default();
        let (bytes, footer) = chunked_segment();
        let mut bad_crc = footer.clone();
        let idx = section(&footer, SERIES_IDX);
        bad_crc.sections[idx].crc32c ^= 1;
        assert_eq!(meta_chunks_inflated_len(&bad_crc, &bytes, limits), None);
        assert!(meta_chunks_inflated_len(&footer, &bytes, limits).is_some());
    }
}
