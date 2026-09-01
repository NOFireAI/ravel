//! Local-vs-distributed read crossover benchmark core (ADR-0071 distributed
//! read fan-out). Every other query bench in this crate measures
//! one isolated stage or the single-process query path; this one measures the
//! same fixed PromQL query over the same fixed corpus twice -- once through the
//! local `QueryEngine` and once through the ADR-0071 distributed fan-out -- and
//! reports the wall-time crossover plus the bytes-moved and S3-request counts
//! the distributed path already accounts (`QueryStats::accounting`).
//!
//! # What "distributed" means here
//!
//! The distributed path is the real ADR-0071 machinery, not a mock: the engine
//! resolves ONE pinned snapshot, the cost gate (forced open with zero
//! thresholds so any corpus distributes) trips, `Distributed::fetch` partitions
//! the snapshot shard-major, and each slice is dispatched to a worker over the
//! frozen `SeriesFetch` gRPC surface. The workers are in-process `tonic`
//! loopback servers (`127.0.0.1:0`), exactly the shape the ADR-0071 acceptance
//! test (`ravel_query::distrib::tests`) uses, each holding the full segment set
//! (any worker can serve any slice; a `SnapshotSegmentResolver` maps a shipped
//! identity to its ref by content hash). A round-robin [`SliceFetcher`] spreads
//! slices across the `worker_count` workers.
//!
//! # What the numbers do and do not say
//!
//! The store is an in-process `MemoryStore` and the workers are loopback: there
//! is no network round trip and no second NIC, so this panel measures the
//! *overhead* the fan-out adds (partition, per-slice gRPC serialize, loopback
//! transport, coordinator fold and merge) against a zero-latency store. It is a
//! lower bound on the crossover: ADR-0071's whole premise is that distribution
//! pays off once real object-store latency and extra fetch NICs dominate, which
//! a `MemoryStore`/loopback shape cannot exhibit. An object-store-backed
//! crossover panel (real S3/MinIO, workers on separate hosts) is the explicit
//! follow-up. The S3-request counts and bytes-moved, by
//! contrast, are host-independent: ADR-0071 makes them identical to local
//! execution for a scalar query, and this bench reports both so that invariant
//! is visible rather than asserted.
//!
//! Lives in the lib (not the bin) so `tests/distrib_crossover_smoke.rs` can
//! exercise the same path the `distrib_crossover_bench` bin runs, mirroring
//! `e2e.rs`/`query_latency.rs`. Report-only: it never changes
//! ravel-ingest/ravel-catalog/ravel-query behavior, only measures it.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use ravel_catalog::{Catalog, CatalogConfig, Snapshot};
use ravel_ingest::{Clock, IngestConfig, IngestRouter, SystemClock, WriteMode};
use ravel_object_store::ObjectStoreBackend;
use ravel_promql::Value;
use ravel_proto::queryfrag::v1 as pb;
use ravel_query::distrib::Distributed;
use ravel_query::distrib::client::{DistribError, RemoteSliceFetcher, SliceFetcher, SliceResponse};
use ravel_query::distrib::partition::DistribThresholds;
use ravel_query::distrib::service::{SeriesFetchService, SnapshotSegmentResolver};
use ravel_query::{EngineConfig, QueryEngine, QueryStats, SegmentFetcher};
use ravel_types::accounting::AccountedOp;
use ravel_types::{Signal, TenantId, TimeRange};
use serde::Serialize;
use tokio::task::JoinHandle;
use tonic::transport::server::TcpIncoming;
use tonic::transport::{Channel, Server};

use crate::generator::{BatchSizeDistribution, WorkloadConfig, generate_batches};

/// Inputs for one crossover run. Corpus knobs deliberately default small so the
/// smoke target stays fast; a recorded run passes larger values on the command
/// line.
pub struct CrossoverConfig {
    pub store: Arc<dyn ObjectStoreBackend>,
    pub store_label: String,
    pub shards: u32,
    pub target_series: usize,
    pub samples_per_series: usize,
    pub batch_size: usize,
    pub ack_timeout_secs: u64,
    /// PromQL selector evaluated by every timed query; must match the workload
    /// generator's metric name (`bench_gauge`).
    pub query: String,
    /// Corpus time span in seconds: the wall-clock window the generated samples
    /// spread across (`samples_per_series` points at `span / samples` spacing).
    /// A wider span with the same per-series sample count flushes more distinct
    /// segments, so this is the lever that grows a corpus past the gate
    /// constants (256 MiB / 256 segments) without changing series cardinality.
    /// The instant query and the snapshot resolve both cover this whole window.
    pub corpus_span_secs: u64,
    /// Timed instant queries per path, for the wall-time percentiles.
    pub query_reps: usize,
    /// Worker counts the distributed path is measured at. One distributed panel
    /// per entry; `max_parallel_slices` is set to the same value so the fan-out
    /// width matches the worker count.
    pub worker_counts: Vec<usize>,
}

impl CrossoverConfig {
    /// A fast, deterministic default corpus for the smoke target: a handful of
    /// segments across four shards, queried at worker counts 1 and 4.
    pub fn smoke(store: Arc<dyn ObjectStoreBackend>, store_label: String) -> Self {
        CrossoverConfig {
            store,
            store_label,
            shards: 4,
            target_series: 64,
            samples_per_series: 8,
            batch_size: 128,
            ack_timeout_secs: 10,
            query: "bench_gauge".to_string(),
            corpus_span_secs: 1,
            query_reps: 5,
            worker_counts: vec![1, 4],
        }
    }
}

/// Coarse process CPU time (user + system, summed over every thread in this
/// process) in milliseconds, read from `/proc/self/stat`. The distributed
/// coordinator, the local engine, and the in-process loopback workers all run
/// in this one process, so a delta of this figure around a timed loop is the
/// total CPU the whole crossover consumed, not a single coordinator thread's
/// share. Granularity is one `USER_HZ` clock tick (10 ms on Linux); a short
/// loop can read back 0. Returns 0.0 off Linux (the reference host is Linux).
#[cfg(target_os = "linux")]
fn process_cpu_ms() -> f64 {
    // /proc/self/stat: "pid (comm) state ...". `comm` may contain spaces and
    // parens, so split after the final ')': the remaining whitespace tokens
    // begin at field 3 (state). utime is field 14, stime field 15, i.e.
    // offsets 11 and 12 in that post-')' token list.
    let Ok(stat) = std::fs::read_to_string("/proc/self/stat") else {
        return 0.0;
    };
    let Some(rest) = stat.rsplit_once(')').map(|(_, r)| r) else {
        return 0.0;
    };
    let fields: Vec<&str> = rest.split_whitespace().collect();
    let (Some(utime), Some(stime)) = (
        fields.get(11).and_then(|v| v.parse::<u64>().ok()),
        fields.get(12).and_then(|v| v.parse::<u64>().ok()),
    ) else {
        return 0.0;
    };
    // USER_HZ is 100 on effectively every Linux target (10 ms per tick); the
    // panel documents this granularity rather than linking libc for sysconf.
    const USER_HZ: f64 = 100.0;
    (utime + stime) as f64 * 1000.0 / USER_HZ
}

#[cfg(not(target_os = "linux"))]
fn process_cpu_ms() -> f64 {
    0.0
}

/// Wall-time percentiles (ms) plus the cost counters the query accounting
/// exposes, for one measured path (local, or distributed at a given worker
/// count).
#[derive(Serialize, Clone)]
pub struct PanelReport {
    /// `"local"` or `"distributed"`.
    pub path: String,
    /// Worker count for a distributed panel; 0 for the local panel.
    pub worker_count: usize,
    pub wall_ms_p50: f64,
    pub wall_ms_p95: f64,
    pub wall_ms_p99: f64,
    pub wall_ms_max: f64,
    /// Total process CPU time (user + system, all threads) consumed across the
    /// timed reps, in milliseconds, from a `/proc/self/stat` delta around the
    /// timed loop (see [`process_cpu_ms`]). Report-only and coarse: one
    /// `USER_HZ` tick (10 ms) of granularity, so a fast panel can read 0. It is
    /// whole-process, not a single coordinator thread, because the loopback
    /// workers share this process; on the distributed panels it therefore
    /// includes the in-process workers' fetch/serialize CPU.
    pub coordinator_cpu_ms: f64,
    /// Matched series in the query's instant vector (must equal the local
    /// panel's: distribution changes where bytes are fetched, never what the
    /// query computes).
    pub matched_series: usize,
    /// Segments the resolved snapshot fetched (`QueryStats::segments_fetched`).
    pub segments_fetched: u64,
    /// Total store GET/LIST/HEAD requests the query issued
    /// (`accounting.total_s3_requests`). Identical local vs distributed for a
    /// scalar query (ADR-0071 performance section).
    pub s3_requests: u64,
    /// Store GET requests alone (the fetch path's ranged reads).
    pub s3_get_requests: u64,
    /// Total bytes moved out of the store (`accounting.total_s3_bytes`): the
    /// bytes-moved figure. Host-independent and identical local vs distributed.
    pub s3_bytes: u64,
    /// Decoded sample footprint (`accounting.decompressed_bytes`): the bytes the
    /// fetch path materialized, distinct from the raw store bytes above.
    pub decompressed_bytes: u64,
}

#[derive(Serialize)]
pub struct ReportConfig {
    pub store: String,
    pub shards: u32,
    pub target_series: usize,
    pub samples_per_series: usize,
    pub batch_size: usize,
    pub query: String,
    pub corpus_span_secs: u64,
    pub query_reps: usize,
    pub worker_counts: Vec<usize>,
}

#[derive(Serialize)]
pub struct Report {
    pub config: ReportConfig,
    pub accepted_points: u64,
    /// Segments in the pinned snapshot the corpus resolved to (the fan-out's
    /// input size).
    pub corpus_segments: usize,
    /// Distinct ingest shards those segments span (the partition's slice
    /// ceiling).
    pub corpus_shards: usize,
    /// The local baseline panel, then one distributed panel per worker count,
    /// in `worker_counts` order.
    pub panels: Vec<PanelReport>,
}

/// Nearest-rank percentile over pre-sorted samples, matching the convention the
/// other bench cores in this crate use (`e2e.rs`, `query_latency.rs`).
fn percentile(sorted_ns: &[u64], pct: f64) -> u64 {
    if sorted_ns.is_empty() {
        return 0;
    }
    let rank = ((sorted_ns.len() - 1) as f64 * pct).round() as usize;
    sorted_ns[rank.min(sorted_ns.len() - 1)]
}

/// A [`SliceFetcher`] that spreads each slice across a fixed set of loopback
/// workers round-robin. With `max_parallel_slices` equal to the worker count,
/// concurrent slices land on distinct workers, modelling an N-worker fan-out.
struct RoundRobinFetcher {
    workers: Vec<RemoteSliceFetcher>,
    next: AtomicUsize,
}

#[async_trait::async_trait]
impl SliceFetcher for RoundRobinFetcher {
    async fn fetch(&self, request: pb::FetchRequest) -> Result<SliceResponse, DistribError> {
        let idx = self.next.fetch_add(1, Ordering::Relaxed) % self.workers.len();
        self.workers[idx].fetch(request).await
    }
}

/// Starts one `tonic` `SeriesFetch` worker on `127.0.0.1:0` over `store` and the
/// full segment set, returning a client fetcher plus the server task handle
/// (abort it to shut the worker down). Mirrors the acceptance test's
/// `spawn_worker`.
async fn spawn_worker(
    store: Arc<dyn ObjectStoreBackend>,
    segments: Vec<ravel_catalog::SegmentRef>,
) -> (RemoteSliceFetcher, JoinHandle<()>) {
    let fetcher = SegmentFetcher::new(store);
    let resolver = Arc::new(SnapshotSegmentResolver::new(segments));
    let service = SeriesFetchService::new(fetcher, resolver).into_server();

    let incoming = TcpIncoming::bind("127.0.0.1:0".parse().expect("addr")).expect("bind worker");
    let addr = incoming.local_addr().expect("worker local addr");
    let handle = tokio::spawn(async move {
        Server::builder()
            .add_service(service)
            .serve_with_incoming(incoming)
            .await
            .expect("serve worker");
    });
    let channel = Channel::from_shared(format!("http://{addr}"))
        .expect("worker endpoint")
        .connect_lazy();
    (RemoteSliceFetcher::new(channel), handle)
}

/// Runs `reps` timed instant queries against `engine`, returning the wall-time
/// percentiles and the query's stats (accounting, matched series). Stats are
/// deterministic across reps (the same pinned snapshot and fetch), so the first
/// rep's stats stand for the panel.
async fn measure_panel(
    engine: &QueryEngine,
    tenant_hash: ravel_types::TenantHash,
    query: &str,
    t_ms: i64,
    now_ns: i64,
    reps: usize,
) -> (Vec<u64>, usize, QueryStats, f64) {
    let deadline = Duration::from_secs(30);
    let mut wall_ns = Vec::with_capacity(reps);
    let mut matched = 0usize;
    let mut stats = QueryStats::default();
    let cpu_start_ms = process_cpu_ms();
    for i in 0..reps {
        let start = Instant::now();
        let (value, s) = engine
            .instant_with_stats(tenant_hash, query, t_ms, &[], now_ns, deadline)
            .await
            .expect("instant query");
        wall_ns.push(start.elapsed().as_nanos() as u64);
        if i == 0 {
            matched = match value {
                Value::Vector(v) => v.len(),
                _ => 0,
            };
            stats = s;
        }
    }
    let cpu_ms = (process_cpu_ms() - cpu_start_ms).max(0.0);
    (wall_ns, matched, stats, cpu_ms)
}

fn panel_report(
    path: &str,
    worker_count: usize,
    wall_ns: Vec<u64>,
    matched_series: usize,
    stats: &QueryStats,
    coordinator_cpu_ms: f64,
) -> PanelReport {
    let mut sorted = wall_ns;
    sorted.sort_unstable();
    let acct = &stats.accounting;
    PanelReport {
        path: path.to_string(),
        worker_count,
        wall_ms_p50: percentile(&sorted, 0.50) as f64 / 1e6,
        wall_ms_p95: percentile(&sorted, 0.95) as f64 / 1e6,
        wall_ms_p99: percentile(&sorted, 0.99) as f64 / 1e6,
        wall_ms_max: sorted.last().copied().unwrap_or(0) as f64 / 1e6,
        coordinator_cpu_ms,
        matched_series,
        segments_fetched: stats.segments_fetched,
        s3_requests: acct.total_s3_requests(),
        s3_get_requests: acct.s3_requests(AccountedOp::Get),
        s3_bytes: acct.total_s3_bytes(),
        decompressed_bytes: acct.decompressed_bytes,
    }
}

pub async fn run(config: &CrossoverConfig) -> Report {
    let store = Arc::clone(&config.store);
    let tenant = TenantId::new(format!("bench-tenant-{}", uuid::Uuid::new_v4()));
    let tenant_hash = tenant.hash();
    let signal = Signal::Metrics;

    let ingest_config = IngestConfig {
        shard_count: config.shards,
        ..IngestConfig::default()
    };
    let clock: Arc<dyn Clock> = Arc::new(SystemClock);
    let router = Arc::new(IngestRouter::new(
        ingest_config,
        Arc::clone(&store),
        signal,
        Arc::clone(&clock),
    ));
    let catalog = Arc::new(
        Catalog::new(
            Arc::clone(&store),
            CatalogConfig {
                shard_count: config.shards,
                ..CatalogConfig::default()
            },
        )
        .expect("catalog config"),
    );

    // Build and ingest a fixed corpus, then flush it durable so the resolved
    // snapshot below sees every accepted point (mirrors `query_latency::run`).
    let run_start_ns = clock.now_ns();
    let duration_ns = Duration::from_secs(config.corpus_span_secs.max(1)).as_nanos() as i64;
    let interval_ns = (duration_ns / config.samples_per_series.max(1) as i64).max(1);
    let workload = WorkloadConfig {
        tenant: tenant.as_str().to_string(),
        series_count: config.target_series,
        samples_per_series: config.samples_per_series,
        start_ts_ns: run_start_ns,
        interval_ns,
        batch_size: BatchSizeDistribution::fixed(config.batch_size),
        ..WorkloadConfig::default()
    };
    let batches: Vec<Vec<_>> = generate_batches(&workload).expect("generate workload");

    let ack_deadline = Duration::from_secs(config.ack_timeout_secs);
    let mut handles = Vec::with_capacity(batches.len());
    for batch in batches {
        let router = Arc::clone(&router);
        let tenant = tenant.clone();
        let batch_len = batch.len() as u64;
        handles.push(tokio::spawn(async move {
            let result = router
                .write(tenant, batch, WriteMode::Strict, ack_deadline)
                .await;
            (batch_len, result)
        }));
    }
    let mut accepted_points: u64 = 0;
    for handle in handles {
        let (batch_len, result) = handle.await.expect("join write task");
        match result {
            Ok(_) => accepted_points += batch_len,
            Err(err) => eprintln!("write error: {err}"),
        }
    }
    router.flush_all().await;

    // Resolve the pinned snapshot once over the whole corpus window. Its
    // segments seed every worker's resolver (any worker can serve any slice),
    // and its shape (segment/shard counts) is the fan-out's input size.
    let query_now_ns = clock.now_ns();
    let resolve_range = TimeRange {
        start_ns: run_start_ns,
        end_ns: query_now_ns,
    };
    let snapshot: Snapshot = catalog
        .resolve(&tenant_hash, signal, resolve_range, &[], query_now_ns)
        .await
        .expect("resolve corpus snapshot");
    let corpus_segments = snapshot.segments.len();
    let corpus_shards = {
        let mut shards: Vec<u32> = snapshot.segments.iter().map(|s| s.shard).collect();
        shards.sort_unstable();
        shards.dedup();
        shards.len()
    };

    let instant_t_ms = (run_start_ns + duration_ns) / 1_000_000;

    let mut panels = Vec::with_capacity(1 + config.worker_counts.len());

    // Local baseline: a plain engine, no distributed context, runs the
    // byte-identical local path.
    {
        let engine = QueryEngine::new(
            Arc::clone(&catalog),
            Arc::clone(&store),
            EngineConfig::default(),
        );
        let (wall_ns, matched, stats, cpu_ms) = measure_panel(
            &engine,
            tenant_hash,
            &config.query,
            instant_t_ms,
            query_now_ns,
            config.query_reps,
        )
        .await;
        panels.push(panel_report("local", 0, wall_ns, matched, &stats, cpu_ms));
    }

    // One distributed panel per worker count. Thresholds are zeroed so the cost
    // gate always trips and every query fans out, regardless of corpus size.
    for &worker_count in &config.worker_counts {
        let workers = worker_count.max(1);
        let mut fetchers = Vec::with_capacity(workers);
        let mut servers = Vec::with_capacity(workers);
        for _ in 0..workers {
            let (fetcher, server) =
                spawn_worker(Arc::clone(&store), snapshot.segments.clone()).await;
            fetchers.push(fetcher);
            servers.push(server);
        }
        let routing = RoundRobinFetcher {
            workers: fetchers,
            next: AtomicUsize::new(0),
        };
        let distributed = Distributed::new(
            Arc::new(routing),
            DistribThresholds {
                min_store_bytes: 0,
                min_segments: 0,
                max_parallel_slices: workers,
            },
        );
        let engine = QueryEngine::new(
            Arc::clone(&catalog),
            Arc::clone(&store),
            EngineConfig::default(),
        )
        .with_distributed(Arc::new(distributed));

        let (wall_ns, matched, stats, cpu_ms) = measure_panel(
            &engine,
            tenant_hash,
            &config.query,
            instant_t_ms,
            query_now_ns,
            config.query_reps,
        )
        .await;
        panels.push(panel_report(
            "distributed",
            worker_count,
            wall_ns,
            matched,
            &stats,
            cpu_ms,
        ));

        for server in servers {
            server.abort();
        }
    }

    Report {
        config: ReportConfig {
            store: config.store_label.clone(),
            shards: config.shards,
            target_series: config.target_series,
            samples_per_series: config.samples_per_series,
            batch_size: config.batch_size,
            query: config.query.clone(),
            corpus_span_secs: config.corpus_span_secs.max(1),
            query_reps: config.query_reps,
            worker_counts: config.worker_counts.clone(),
        },
        accepted_points,
        corpus_segments,
        corpus_shards,
        panels,
    }
}
