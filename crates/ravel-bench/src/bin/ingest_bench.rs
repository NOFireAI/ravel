//! End-to-end ingest benchmark: drives `IngestRouter` directly (no HTTP)
//! against a real or in-memory object store (docs/benchmarking.md
//! "End-to-end"). Report-only: never changes ravel-ingest/ravel-catalog
//! behavior, only measures it.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use clap::{Parser, ValueEnum};
use ravel_bench::generator::{BatchSizeDistribution, WorkloadConfig, generate_batches};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_ingest::{
    Clock, IngestConfig, IngestRouter, SEGMENT_FORMAT_V1, SEGMENT_FORMAT_V2, SystemClock, WriteMode,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::s3::{S3Config, S3Store};
use ravel_object_store::{ObjectStoreBackend, list_all};
use ravel_types::{Signal, TenantId, TimeRange};
use serde::Serialize;

/// Bytes on the wire per logical sample: `ts_ns: i64` + `value: f64`
/// (docs/benchmarking.md "write amplification (bytes stored / bytes
/// ingested logical)").
const LOGICAL_BYTES_PER_SAMPLE: u64 = 16;
const VISIBILITY_POLL_INTERVAL: Duration = Duration::from_secs(1);
const VISIBILITY_POLL_MAX_ROUNDS: u32 = 30;

#[derive(Copy, Clone, Debug, ValueEnum)]
enum StoreKind {
    Memory,
    S3,
}

/// Which RSEG trailer version the ingest router writes for this run
/// (`IngestConfig::segment_format_version`, docs/rseg-v2-plan.md P6). A CLI
/// choice, not a default change: `IngestConfig::default()` still ships v1;
/// this flag exists so the orchestrator can point this same binary at v2 on
/// comparable hardware without a code change.
#[derive(Copy, Clone, Debug, ValueEnum)]
enum SegmentFormat {
    V1,
    V2,
}

impl From<SegmentFormat> for u16 {
    fn from(f: SegmentFormat) -> u16 {
        match f {
            SegmentFormat::V1 => SEGMENT_FORMAT_V1,
            SegmentFormat::V2 => SEGMENT_FORMAT_V2,
        }
    }
}

#[derive(Parser, Debug)]
#[command(about = "End-to-end ravel-ingest benchmark (Phase 1, docs/benchmarking.md)")]
struct Args {
    #[arg(long, value_enum, default_value_t = StoreKind::Memory)]
    store: StoreKind,
    #[arg(long, default_value_t = 4)]
    shards: u32,
    #[arg(long, default_value_t = 1_000)]
    target_series: usize,
    #[arg(long, default_value_t = 50_000)]
    points_per_sec: u64,
    #[arg(long, default_value_t = 10)]
    duration_secs: u64,
    #[arg(long, default_value_t = 200)]
    batch_size: usize,
    #[arg(long, default_value_t = 5)]
    ack_timeout_secs: u64,
    #[arg(long, value_enum, default_value_t = SegmentFormat::V1)]
    segment_format: SegmentFormat,
}

impl std::fmt::Display for StoreKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StoreKind::Memory => write!(f, "memory"),
            StoreKind::S3 => write!(f, "s3"),
        }
    }
}

impl std::fmt::Display for SegmentFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SegmentFormat::V1 => write!(f, "v1"),
            SegmentFormat::V2 => write!(f, "v2"),
        }
    }
}

fn s3_config_from_env() -> S3Config {
    let get = |key: &str| std::env::var(key).unwrap_or_default();
    S3Config {
        bucket: get("RAVEL_S3_BUCKET"),
        region: get("RAVEL_S3_REGION"),
        endpoint: std::env::var("RAVEL_S3_ENDPOINT").ok(),
        access_key_id: get("RAVEL_S3_ACCESS_KEY_ID"),
        secret_access_key: get("RAVEL_S3_SECRET_ACCESS_KEY"),
        allow_http: std::env::var("RAVEL_S3_ALLOW_HTTP").as_deref() == Ok("true"),
        force_path_style: std::env::var("RAVEL_S3_FORCE_PATH_STYLE").as_deref() != Ok("false"),
    }
}

fn percentile(sorted_ns: &[u64], pct: f64) -> u64 {
    if sorted_ns.is_empty() {
        return 0;
    }
    let rank = ((sorted_ns.len() - 1) as f64 * pct).round() as usize;
    sorted_ns[rank.min(sorted_ns.len() - 1)]
}

struct LatencyStats {
    p50_ns: u64,
    p95_ns: u64,
    p99_ns: u64,
    max_ns: u64,
    count: usize,
}

fn latency_stats(mut samples_ns: Vec<u64>) -> LatencyStats {
    samples_ns.sort_unstable();
    LatencyStats {
        p50_ns: percentile(&samples_ns, 0.50),
        p95_ns: percentile(&samples_ns, 0.95),
        p99_ns: percentile(&samples_ns, 0.99),
        max_ns: samples_ns.last().copied().unwrap_or(0),
        count: samples_ns.len(),
    }
}

#[derive(Serialize)]
struct Report {
    config: ReportConfig,
    accepted_points_per_sec: f64,
    accepted_points: u64,
    ack_latency_ms: LatencyReport,
    flushes_by_size: u64,
    flushes_by_age: u64,
    flushes_manual: u64,
    put_retries: u64,
    abandoned_flushes: u64,
    acks_ok: u64,
    acks_err: u64,
    /// Derived: one data-object PUT and one commit-record PUT per flush
    /// (`ravel_commit::publish::publish`), excluding retries.
    estimated_put_count: u64,
    bytes_written: u64,
    logical_bytes: u64,
    write_amplification: f64,
    visibility_lag_ms: VisibilityReport,
}

#[derive(Serialize)]
struct ReportConfig {
    store: String,
    shards: u32,
    target_series: usize,
    points_per_sec: u64,
    duration_secs: u64,
    batch_size: usize,
    /// Which RSEG trailer version produced these numbers ("v1" or "v2"):
    /// provenance so a later report comparing runs doesn't have to guess.
    segment_format: String,
}

#[derive(Serialize)]
struct LatencyReport {
    p50: f64,
    p95: f64,
    p99: f64,
    max: f64,
    count: usize,
}

impl From<LatencyStats> for LatencyReport {
    fn from(s: LatencyStats) -> Self {
        LatencyReport {
            p50: s.p50_ns as f64 / 1e6,
            p95: s.p95_ns as f64 / 1e6,
            p99: s.p99_ns as f64 / 1e6,
            max: s.max_ns as f64 / 1e6,
            count: s.count,
        }
    }
}

#[derive(Serialize)]
struct VisibilityReport {
    resolved_count: usize,
    unresolved_count: usize,
    avg: f64,
    max: f64,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let report = run(&args).await;

    println!(
        "{}",
        serde_json::to_string_pretty(&report).expect("serialize report")
    );
    print_human_table(&report);
}

async fn run(args: &Args) -> Report {
    let store: Arc<dyn ObjectStoreBackend> = match args.store {
        StoreKind::Memory => Arc::new(MemoryStore::new()),
        StoreKind::S3 => {
            Arc::new(S3Store::new(s3_config_from_env()).expect("build S3Store from RAVEL_S3_* env"))
        }
    };

    let tenant = TenantId::new("bench-tenant");
    let tenant_hash = tenant.hash();
    let signal = Signal::Metrics;

    let ingest_config = IngestConfig {
        shard_count: args.shards,
        segment_format_version: args.segment_format.into(),
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
                shard_count: args.shards,
                ..CatalogConfig::default()
            },
        )
        .expect("catalog config"),
    );

    let total_points = args.points_per_sec * args.duration_secs;
    let samples_per_series = (total_points as usize / args.target_series.max(1)).max(1);
    let run_start_ns = clock.now_ns();
    // Event timestamps must land within the real wall-clock span of the run:
    // the catalog's listing-based visibility poll below filters by event-time
    // overlap against a range anchored on `run_start_ns`, so samples dated
    // far outside that span would never resolve.
    let duration_ns = Duration::from_secs(args.duration_secs.max(1)).as_nanos() as i64;
    let interval_ns = (duration_ns / samples_per_series as i64).max(1);
    let workload = WorkloadConfig {
        tenant: tenant.as_str().to_string(),
        series_count: args.target_series,
        samples_per_series,
        start_ts_ns: run_start_ns,
        interval_ns,
        batch_size: BatchSizeDistribution::fixed(args.batch_size),
        ..WorkloadConfig::default()
    };
    let batches = generate_batches(&workload).expect("generate workload");

    let ack_deadline = Duration::from_secs(args.ack_timeout_secs);
    let pacing_interval = if args.points_per_sec == 0 {
        Duration::ZERO
    } else {
        Duration::from_secs_f64(args.batch_size as f64 / args.points_per_sec as f64)
    };

    // Visibility lag is tracked concurrently with the write phase below, not
    // as a separate pass afterward: acks span the whole run, so measuring
    // them only once every write has landed would charge early acks for the
    // rest of the run's wall time instead of their real ack-to-queryable gap.
    let pending: Arc<Mutex<HashMap<String, i64>>> = Arc::new(Mutex::new(HashMap::new()));
    let lags_ns: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let writes_done = Arc::new(AtomicBool::new(false));

    let poller = tokio::spawn({
        let catalog = Arc::clone(&catalog);
        let clock = Arc::clone(&clock);
        let pending = Arc::clone(&pending);
        let lags_ns = Arc::clone(&lags_ns);
        let writes_done = Arc::clone(&writes_done);
        async move {
            for _ in 0..VISIBILITY_POLL_MAX_ROUNDS {
                tokio::time::sleep(VISIBILITY_POLL_INTERVAL).await;
                let now_ns = clock.now_ns();
                let listing_range = TimeRange {
                    start_ns: run_start_ns,
                    end_ns: now_ns,
                };
                match catalog
                    .resolve(&tenant_hash, signal, listing_range, &[], now_ns)
                    .await
                {
                    Ok(snapshot) => {
                        let mut pending = pending.lock().expect("pending lock");
                        let mut lags_ns = lags_ns.lock().expect("lags lock");
                        for seg in snapshot.segments {
                            if let Some(ack_wall_ns) = pending.remove(&seg.data_object_key) {
                                lags_ns.push((now_ns - ack_wall_ns).max(0) as u64);
                            }
                        }
                        if writes_done.load(Ordering::Acquire) && pending.is_empty() {
                            break;
                        }
                    }
                    Err(err) => eprintln!("visibility: listing resolve failed: {err}"),
                }
            }
        }
    });

    let mut handles = Vec::with_capacity(batches.len());
    let mut next_dispatch = tokio::time::Instant::now();
    for batch in batches {
        if pacing_interval > Duration::ZERO {
            tokio::time::sleep_until(next_dispatch).await;
            next_dispatch += pacing_interval;
        }
        let router = Arc::clone(&router);
        let clock = Arc::clone(&clock);
        let catalog = Arc::clone(&catalog);
        let pending = Arc::clone(&pending);
        let tenant = tenant.clone();
        let batch_len = batch.len() as u64;
        handles.push(tokio::spawn(async move {
            let start = std::time::Instant::now();
            let result = router
                .write(tenant, batch, WriteMode::Strict, ack_deadline)
                .await;
            let latency_ns = start.elapsed().as_nanos() as u64;
            let ack_wall_ns = clock.now_ns();
            if let Ok(receipt) = &result {
                // Resolve this ack's exact segment (read-your-write min-token
                // GET) right away, concurrently with every other in-flight
                // batch, so the poller above can see it on its very next
                // tick rather than only after the whole write phase ends.
                for token in &receipt.tokens {
                    let exact_range = TimeRange {
                        start_ns: ack_wall_ns,
                        end_ns: ack_wall_ns,
                    };
                    match catalog
                        .resolve(
                            &tenant_hash,
                            signal,
                            exact_range,
                            std::slice::from_ref(token),
                            ack_wall_ns,
                        )
                        .await
                    {
                        Ok(snapshot) => {
                            let mut pending = pending.lock().expect("pending lock");
                            for seg in snapshot.segments {
                                pending.entry(seg.data_object_key).or_insert(ack_wall_ns);
                            }
                        }
                        Err(err) => eprintln!("visibility: min-token resolve failed: {err}"),
                    }
                }
            }
            (batch_len, latency_ns, result)
        }));
    }

    let wall_start = std::time::Instant::now();
    let mut latencies_ns = Vec::with_capacity(handles.len());
    let mut accepted_points: u64 = 0;
    for handle in handles {
        let (batch_len, latency_ns, result) = handle.await.expect("join write task");
        latencies_ns.push(latency_ns);
        match result {
            Ok(_) => accepted_points += batch_len,
            Err(err) => eprintln!("write error: {err}"),
        }
    }
    let elapsed_secs = wall_start.elapsed().as_secs_f64().max(1e-9);

    router.flush_all().await;
    writes_done.store(true, Ordering::Release);
    poller.await.expect("join visibility poller");

    let visibility = {
        let pending = pending.lock().expect("pending lock");
        let lags_ns = lags_ns.lock().expect("lags lock");
        let unresolved_count = pending.len();
        let resolved_count = lags_ns.len();
        let avg = if lags_ns.is_empty() {
            0.0
        } else {
            lags_ns.iter().sum::<u64>() as f64 / lags_ns.len() as f64 / 1e6
        };
        let max = lags_ns.iter().max().copied().unwrap_or(0) as f64 / 1e6;
        VisibilityReport {
            resolved_count,
            unresolved_count,
            avg,
            max,
        }
    };

    let metrics = router.metrics().snapshot();
    let objects = list_all(store.as_ref(), "")
        .await
        .expect("list all objects");
    let bytes_written: u64 = objects.iter().map(|o| o.size).sum();
    let logical_bytes = accepted_points * LOGICAL_BYTES_PER_SAMPLE;
    let write_amplification = if logical_bytes == 0 {
        0.0
    } else {
        bytes_written as f64 / logical_bytes as f64
    };
    let estimated_put_count =
        2 * (metrics.flushes_by_size + metrics.flushes_by_age + metrics.flushes_manual);

    Report {
        config: ReportConfig {
            store: args.store.to_string(),
            shards: args.shards,
            target_series: args.target_series,
            points_per_sec: args.points_per_sec,
            duration_secs: args.duration_secs,
            batch_size: args.batch_size,
            segment_format: args.segment_format.to_string(),
        },
        accepted_points_per_sec: accepted_points as f64 / elapsed_secs,
        accepted_points,
        ack_latency_ms: latency_stats(latencies_ns).into(),
        flushes_by_size: metrics.flushes_by_size,
        flushes_by_age: metrics.flushes_by_age,
        flushes_manual: metrics.flushes_manual,
        put_retries: metrics.put_retries,
        abandoned_flushes: metrics.abandoned_flushes,
        acks_ok: metrics.acks_ok,
        acks_err: metrics.acks_err,
        estimated_put_count,
        bytes_written,
        logical_bytes,
        write_amplification,
        visibility_lag_ms: visibility,
    }
}

fn print_human_table(report: &Report) {
    println!("\ningest_bench report");
    println!("  store             : {}", report.config.store);
    println!("  shards            : {}", report.config.shards);
    println!("  target_series     : {}", report.config.target_series);
    println!("  points_per_sec cfg: {}", report.config.points_per_sec);
    println!("  duration_secs     : {}", report.config.duration_secs);
    println!("  batch_size        : {}", report.config.batch_size);
    println!("  segment_format    : {}", report.config.segment_format);
    println!(
        "  accepted points/s : {:.1}",
        report.accepted_points_per_sec
    );
    println!("  accepted points   : {}", report.accepted_points);
    println!(
        "  ack latency ms    : p50={:.3} p95={:.3} p99={:.3} max={:.3} (n={})",
        report.ack_latency_ms.p50,
        report.ack_latency_ms.p95,
        report.ack_latency_ms.p99,
        report.ack_latency_ms.max,
        report.ack_latency_ms.count
    );
    println!(
        "  flushes           : size={} age={} manual={}",
        report.flushes_by_size, report.flushes_by_age, report.flushes_manual
    );
    println!("  put_retries       : {}", report.put_retries);
    println!("  abandoned_flushes : {}", report.abandoned_flushes);
    println!(
        "  acks ok/err       : {}/{}",
        report.acks_ok, report.acks_err
    );
    println!("  estimated PUTs    : {}", report.estimated_put_count);
    println!("  bytes_written     : {}", report.bytes_written);
    println!("  logical_bytes     : {}", report.logical_bytes);
    println!("  write_amplification: {:.3}", report.write_amplification);
    println!(
        "  visibility lag ms : avg={:.3} max={:.3} resolved={} unresolved={}",
        report.visibility_lag_ms.avg,
        report.visibility_lag_ms.max,
        report.visibility_lag_ms.resolved_count,
        report.visibility_lag_ms.unresolved_count
    );
}
