//! End-to-end ingest benchmark core: drives `IngestRouter` directly (no HTTP)
//! against a real or in-memory
//! object store. Lives in the lib (not the `ingest_bench` bin) so
//! `tests/ingest_smoke.rs` can exercise the same flags-to-report path the
//! bin runs, matching how `ravel_bench::e2e` and `ravel_bench::concurrent`
//! are tested directly rather than through a bin. Report-only: never changes
//! ravel-ingest/ravel-catalog behavior, only measures it.
//!
//! The measurement panel sweeps this binary over pipeline depth {1,2,3,4} x
//! flush-delay policy {fixed, adaptive} (ADR-0067). The two knobs are
//! `--max-inflight-flushes` and `--flush-delay-policy`; their defaults
//! (depth 1, fixed) reproduce production defaults.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use clap::{Parser, ValueEnum};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_ingest::{Clock, IngestConfig, IngestRouter, SystemClock, WriteMode};
use ravel_object_store::{ObjectStoreBackend, list_all};
use ravel_types::{Signal, TenantId, TimeRange};
use serde::Serialize;

use crate::generator::{BatchSizeDistribution, WorkloadConfig, generate_batches};
use crate::harness::{StoreKind, store_from_env};

/// Bytes on the wire per logical sample: `ts_ns: i64` + `value: f64`. Used as
/// the denominator of write amplification (bytes stored / bytes ingested
/// logical).
const LOGICAL_BYTES_PER_SAMPLE: u64 = 16;
const VISIBILITY_POLL_INTERVAL: Duration = Duration::from_secs(1);
const VISIBILITY_POLL_MAX_ROUNDS: u32 = 30;
/// How often the in-flight depth sampler reads the per-shard gauge. Cheap: a
/// single `Mutex<HashMap>` read of `IngestMetrics::in_flight_flushes_by_shard`
/// per tick, no allocation on the flush path. 25 ms keeps ~40 samples/s, dense
/// enough to catch a depth spike a MinIO/real-S3 flush (tens to hundreds of
/// ms) holds open, without loading the actor.
const DEPTH_SAMPLE_INTERVAL: Duration = Duration::from_millis(25);

/// Flush-delay policy selected on the command line (ADR-0067 decision 3).
/// `Fixed` (the default) keeps the fixed `max_flush_delay` age trigger;
/// `Adaptive` enables the per-(shard, tenant) adaptive corridor. Wires
/// straight to `IngestConfig::adaptive_flush_delay`.
#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum FlushDelayPolicy {
    Fixed,
    Adaptive,
}

impl FlushDelayPolicy {
    fn adaptive(self) -> bool {
        matches!(self, FlushDelayPolicy::Adaptive)
    }
}

impl std::fmt::Display for FlushDelayPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FlushDelayPolicy::Fixed => write!(f, "fixed"),
            FlushDelayPolicy::Adaptive => write!(f, "adaptive"),
        }
    }
}

/// Command-line surface of the `ingest_bench` bin. Lives here (not the bin)
/// so a test can parse an argv, `validate` it, and `to_config` it exactly as
/// the bin does, mirroring how `services/ravel-server`'s `Cli` is validated
/// in its own crate.
#[derive(Parser, Debug)]
#[command(about = "End-to-end ravel-ingest benchmark")]
pub struct IngestBenchArgs {
    #[arg(long, value_enum, default_value_t = StoreKind::Memory)]
    pub store: StoreKind,
    #[arg(long, default_value_t = 4)]
    pub shards: u32,
    #[arg(long, default_value_t = 1_000)]
    pub target_series: usize,
    #[arg(long, default_value_t = 50_000)]
    pub points_per_sec: u64,
    #[arg(long, default_value_t = 10)]
    pub duration_secs: u64,
    #[arg(long, default_value_t = 200)]
    pub batch_size: usize,
    #[arg(long, default_value_t = 5)]
    pub ack_timeout_secs: u64,
    /// Upper bound on concurrently in-flight flush tasks per shard
    /// (ADR-0067 decision 2). Passed straight through to
    /// `IngestConfig::max_inflight_flushes`. Default 1 reproduces today's
    /// one-flush-at-a-time behavior; the measurement panel sweeps {1,2,3,4}.
    #[arg(long, default_value_t = 1)]
    pub max_inflight_flushes: u32,
    /// Flush-delay policy (ADR-0067 decision 3): `fixed` keeps the fixed
    /// `max_flush_delay` age trigger, `adaptive` enables the adaptive
    /// corridor (`IngestConfig::adaptive_flush_delay`). Default `fixed`
    /// matches production.
    #[arg(long, value_enum, default_value_t = FlushDelayPolicy::Fixed)]
    pub flush_delay_policy: FlushDelayPolicy,
}

impl IngestBenchArgs {
    /// Rejects the flag combinations the ingest pipeline cannot honor, with
    /// the same reasoning `services/ravel-server`'s `Cli::validate` uses so a
    /// panel operator gets the identical error whichever entry point they
    /// drive. `max_inflight_flushes == 0` deadlocks every flush: a shard
    /// could never acquire a semaphore permit to run one.
    pub fn validate(&self) -> Result<(), String> {
        if self.max_inflight_flushes == 0 {
            return Err(
                "--max-inflight-flushes '0' would deadlock every flush: a shard \
                 could never acquire a permit to run one. Set a positive count (1 keeps \
                 today's non-pipelined behavior)."
                    .to_string(),
            );
        }
        Ok(())
    }

    /// Builds the runtime config, constructing the backing store from the
    /// `--store` flag (and `RAVEL_S3_*` env for `s3`). Call `validate` first.
    pub fn to_config(&self) -> IngestBenchConfig {
        IngestBenchConfig {
            store: store_from_env(self.store),
            store_label: self.store.to_string(),
            shards: self.shards,
            target_series: self.target_series,
            points_per_sec: self.points_per_sec,
            duration_secs: self.duration_secs,
            batch_size: self.batch_size,
            ack_timeout_secs: self.ack_timeout_secs,
            max_inflight_flushes: self.max_inflight_flushes,
            flush_delay_policy: self.flush_delay_policy,
        }
    }
}

/// Inputs for one ingest run. `store_label` is display-only (the `--store`
/// flag's name); the actual backend is `store`.
pub struct IngestBenchConfig {
    pub store: Arc<dyn ObjectStoreBackend>,
    pub store_label: String,
    pub shards: u32,
    pub target_series: usize,
    pub points_per_sec: u64,
    pub duration_secs: u64,
    pub batch_size: usize,
    pub ack_timeout_secs: u64,
    pub max_inflight_flushes: u32,
    pub flush_delay_policy: FlushDelayPolicy,
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

/// Builds the in-flight depth histogram from the per-tick samples of the
/// per-shard gauge (each sample is the max depth observed across shards at
/// that tick). This is a **real periodic sample** of
/// `IngestMetrics::in_flight_flushes_by_shard`, not the max-observed
/// fallback the issue permitted: the gauge is cheap to read, so the panel
/// gets the full occupancy distribution rather than a single high-water mark.
fn depth_report(samples: Vec<u64>) -> DepthReport {
    let mut counts: HashMap<u64, usize> = HashMap::new();
    let mut max_observed = 0u64;
    for depth in &samples {
        *counts.entry(*depth).or_insert(0) += 1;
        max_observed = max_observed.max(*depth);
    }
    let mut histogram: Vec<DepthBucket> = counts
        .into_iter()
        .map(|(depth, samples)| DepthBucket { depth, samples })
        .collect();
    histogram.sort_unstable_by_key(|b| b.depth);
    DepthReport {
        source: "sampled per-shard in-flight gauge @25ms".to_string(),
        max_observed,
        samples: samples.len(),
        histogram,
    }
}

#[derive(Serialize)]
pub struct Report {
    pub config: ReportConfig,
    pub accepted_points_per_sec: f64,
    pub accepted_points: u64,
    pub ack_latency_ms: LatencyReport,
    pub flushes_by_size: u64,
    pub flushes_by_age: u64,
    /// Flushes opened by the adaptive age corridor (ADR-0067 decision 3);
    /// zero when `--flush-delay-policy fixed`. Reported alongside the fixed
    /// counters so the A/B panel can attribute the flush-cause shift.
    pub flushes_by_age_adaptive: u64,
    pub flushes_manual: u64,
    /// In-flight flush-depth occupancy, sampled over the run.
    pub in_flight_depth: DepthReport,
    pub put_retries: u64,
    pub abandoned_retry_exhausted: u64,
    pub abandoned_input_rejected: u64,
    pub acks_ok: u64,
    pub acks_err: u64,
    /// Derived: one data-object PUT and one commit-record PUT per flush
    /// (`ravel_commit::publish::publish`), excluding retries. Includes the
    /// adaptive-age flushes so the estimate stays correct with the adaptive
    /// policy on; unchanged from before at the default policy where
    /// `flushes_by_age_adaptive` is always zero.
    pub estimated_put_count: u64,
    pub bytes_written: u64,
    pub logical_bytes: u64,
    pub write_amplification: f64,
    pub visibility_lag_ms: VisibilityReport,
}

#[derive(Serialize)]
pub struct ReportConfig {
    pub store: String,
    pub shards: u32,
    pub target_series: usize,
    pub points_per_sec: u64,
    pub duration_secs: u64,
    pub batch_size: usize,
    pub max_inflight_flushes: u32,
    pub flush_delay_policy: String,
}

#[derive(Serialize)]
pub struct LatencyReport {
    pub p50: f64,
    pub p95: f64,
    pub p99: f64,
    pub max: f64,
    pub count: usize,
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

/// One in-flight-depth histogram bucket: `samples` ticks observed exactly
/// `depth` flushes in flight (max across shards at that tick).
#[derive(Serialize)]
pub struct DepthBucket {
    pub depth: u64,
    pub samples: usize,
}

#[derive(Serialize)]
pub struct DepthReport {
    /// How the histogram was obtained: a real periodic sample, versus the
    /// max-observed fallback the issue allowed if a sample were not cheap.
    pub source: String,
    pub max_observed: u64,
    pub samples: usize,
    pub histogram: Vec<DepthBucket>,
}

#[derive(Serialize)]
pub struct VisibilityReport {
    pub resolved_count: usize,
    pub unresolved_count: usize,
    pub avg: f64,
    pub max: f64,
}

pub async fn run(config: &IngestBenchConfig) -> Report {
    let store = Arc::clone(&config.store);

    let tenant = TenantId::new("bench-tenant");
    let tenant_hash = tenant.hash();
    let signal = Signal::Metrics;

    let ingest_config = IngestConfig {
        shard_count: config.shards,
        max_inflight_flushes: config.max_inflight_flushes,
        adaptive_flush_delay: config.flush_delay_policy.adaptive(),
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

    let total_points = config.points_per_sec * config.duration_secs;
    let samples_per_series = (total_points as usize / config.target_series.max(1)).max(1);
    let run_start_ns = clock.now_ns();
    // Event timestamps must land within the real wall-clock span of the run:
    // the catalog's listing-based visibility poll below filters by event-time
    // overlap against a range anchored on `run_start_ns`, so samples dated
    // far outside that span would never resolve.
    let duration_ns = Duration::from_secs(config.duration_secs.max(1)).as_nanos() as i64;
    let interval_ns = (duration_ns / samples_per_series as i64).max(1);
    let workload = WorkloadConfig {
        tenant: tenant.as_str().to_string(),
        series_count: config.target_series,
        samples_per_series,
        start_ts_ns: run_start_ns,
        interval_ns,
        batch_size: BatchSizeDistribution::fixed(config.batch_size),
        ..WorkloadConfig::default()
    };
    let batches = generate_batches(&workload).expect("generate workload");

    let ack_deadline = Duration::from_secs(config.ack_timeout_secs);
    let pacing_interval = if config.points_per_sec == 0 {
        Duration::ZERO
    } else {
        Duration::from_secs_f64(config.batch_size as f64 / config.points_per_sec as f64)
    };

    // Visibility lag is tracked concurrently with the write phase below, not
    // as a separate pass afterward: acks span the whole run, so measuring
    // them only once every write has landed would charge early acks for the
    // rest of the run's wall time instead of their real ack-to-queryable gap.
    let pending: Arc<Mutex<HashMap<String, i64>>> = Arc::new(Mutex::new(HashMap::new()));
    let lags_ns: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let writes_done = Arc::new(AtomicBool::new(false));
    // Set after the flush drain and the poller both finish, so the depth
    // sampler covers the whole run including the trailing manual flushes.
    let run_done = Arc::new(AtomicBool::new(false));

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

    // Real periodic sample of the per-shard in-flight-flush gauge (ADR-0067
    // decision 2 consequence). Each tick records the max depth across shards;
    // `depth_report` turns the series into an occupancy histogram plus the
    // high-water mark. Samples at least once even for a sub-tick run because
    // it reads before it sleeps.
    let depth_samples: Arc<Mutex<Vec<u64>>> = Arc::new(Mutex::new(Vec::new()));
    let sampler = tokio::spawn({
        let router = Arc::clone(&router);
        let run_done = Arc::clone(&run_done);
        let depth_samples = Arc::clone(&depth_samples);
        async move {
            loop {
                let max_depth = router
                    .metrics()
                    .in_flight_flushes_by_shard()
                    .into_iter()
                    .map(|(_, count)| count)
                    .max()
                    .unwrap_or(0);
                depth_samples.lock().expect("depth lock").push(max_depth);
                if run_done.load(Ordering::Acquire) {
                    break;
                }
                tokio::time::sleep(DEPTH_SAMPLE_INTERVAL).await;
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
    run_done.store(true, Ordering::Release);
    sampler.await.expect("join depth sampler");

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

    let depth = {
        let samples = depth_samples.lock().expect("depth lock");
        depth_report(samples.clone())
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
    let estimated_put_count = 2
        * (metrics.flushes_by_size
            + metrics.flushes_by_age
            + metrics.flushes_by_age_adaptive
            + metrics.flushes_manual);

    Report {
        config: ReportConfig {
            store: config.store_label.clone(),
            shards: config.shards,
            target_series: config.target_series,
            points_per_sec: config.points_per_sec,
            duration_secs: config.duration_secs,
            batch_size: config.batch_size,
            max_inflight_flushes: config.max_inflight_flushes,
            flush_delay_policy: config.flush_delay_policy.to_string(),
        },
        accepted_points_per_sec: accepted_points as f64 / elapsed_secs,
        accepted_points,
        ack_latency_ms: latency_stats(latencies_ns).into(),
        flushes_by_size: metrics.flushes_by_size,
        flushes_by_age: metrics.flushes_by_age,
        flushes_by_age_adaptive: metrics.flushes_by_age_adaptive,
        flushes_manual: metrics.flushes_manual,
        in_flight_depth: depth,
        put_retries: metrics.put_retries,
        abandoned_retry_exhausted: metrics.abandoned_retry_exhausted,
        abandoned_input_rejected: metrics.abandoned_input_rejected,
        acks_ok: metrics.acks_ok,
        acks_err: metrics.acks_err,
        estimated_put_count,
        bytes_written,
        logical_bytes,
        write_amplification,
        visibility_lag_ms: visibility,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn depth_report_builds_sorted_histogram_and_high_water_mark() {
        let report = depth_report(vec![0, 2, 2, 1, 3, 0]);
        assert_eq!(report.samples, 6);
        assert_eq!(report.max_observed, 3);
        let buckets: Vec<(u64, usize)> = report
            .histogram
            .iter()
            .map(|b| (b.depth, b.samples))
            .collect();
        assert_eq!(buckets, vec![(0, 2), (1, 1), (2, 2), (3, 1)]);
    }

    #[test]
    fn empty_depth_samples_report_zero() {
        let report = depth_report(vec![]);
        assert_eq!(report.samples, 0);
        assert_eq!(report.max_observed, 0);
        assert!(report.histogram.is_empty());
    }
}
