//! Concurrent readers-and-writers benchmark core, plus a cold-vs-warm query
//! cache comparison (docs/benchmarking.md "End-to-end", issue #269).
//!
//! `ravel_bench::e2e::run` drives `IngestRouter` and `QueryEngine` through
//! one randomized-tenant ingest-then-query cycle, sequentially: it is not a
//! model for real concurrent access to shared data, since every call gets
//! its own tenant and no two calls ever touch the same series at the same
//! time. This module instead builds one shared store, tenant,
//! `IngestRouter`, `Catalog`, and `QueryEngine`, then spawns writer tasks
//! that call the shared router's `write()` concurrently (each on its own
//! disjoint slice of series identity, via `WorkloadConfig::series_idx_offset`,
//! so writers never collide on the same series) and reader tasks that call
//! the shared engine's `.instant()` concurrently for the whole run, so the
//! report can show whether readers and writers measurably slow each other
//! down instead of only a blended average.
//!
//! Lives in the lib (not the `concurrent_bench` bin), mirroring `e2e.rs`,
//! so `tests/concurrent_smoke.rs` can exercise the same path the bin runs.
//! Report-only: never changes ravel-ingest/ravel-catalog/ravel-query
//! behavior, only measures it.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use ravel_catalog::{Catalog, CatalogConfig};
use ravel_ingest::{Clock, IngestConfig, IngestRouter, SystemClock, WriteMode};
use ravel_object_store::ObjectStoreBackend;
use ravel_otlp::NormalizedPoint;
use ravel_promql::Value;
use ravel_query::{EngineConfig, QueryEngine};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Signal, TenantId};
use serde::Serialize;

use crate::generator::{BatchSizeDistribution, WorkloadConfig, generate_batches};

/// Same rationale and shape as `ravel_bench::e2e`'s private helper of the
/// same name: `generate_batches` derives each series' identity from a
/// metric name but never stores that name as a `__name__` label, so a
/// PromQL selector matches nothing without it. Duplicated rather than
/// shared -- see the final report for why extracting it was not worth a
/// third copy-site for two lines of struct-building.
fn with_metric_name_label(point: NormalizedPoint) -> NormalizedPoint {
    let metric_name = if point.is_monotonic_sum {
        "bench_counter_total"
    } else {
        "bench_gauge"
    };
    let mut labels: Vec<Label> = point.labels.iter().cloned().collect();
    labels.push(Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: metric_name.to_string(),
    });
    NormalizedPoint {
        labels: LabelSet::new(labels).expect("add __name__ label"),
        ..point
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

#[derive(Serialize, Clone)]
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

/// Inputs for one concurrent readers-and-writers run.
pub struct ConcurrentConfig {
    pub store: Arc<dyn ObjectStoreBackend>,
    pub store_label: String,
    pub shards: u32,
    pub writers: usize,
    pub readers: usize,
    pub target_series: usize,
    pub points_per_sec: u64,
    pub duration_secs: u64,
    pub batch_size: usize,
    pub ack_timeout_secs: u64,
    /// PromQL instant selector every reader task (and the cold/warm phase)
    /// repeats; must match the workload generator's metric name.
    pub query: String,
}

#[derive(Serialize)]
pub struct ReportConfig {
    pub store: String,
    pub shards: u32,
    pub writers: usize,
    pub readers: usize,
    pub target_series: usize,
    pub points_per_sec: u64,
    pub duration_secs: u64,
    pub batch_size: usize,
    pub query: String,
}

#[derive(Serialize)]
pub struct WriterReport {
    pub writer_id: usize,
    pub accepted_points: u64,
    pub batches_ok: u64,
    pub batches_err: u64,
    pub latency_ms: LatencyReport,
}

#[derive(Serialize)]
pub struct ReaderReport {
    pub reader_id: usize,
    pub queries_run: u64,
    pub non_empty_results: u64,
    pub latency_ms: LatencyReport,
}

/// Cold-vs-warm instant-query comparison against a fresh `Catalog`/
/// `QueryEngine` built right after the concurrent ingest phase completes.
/// "Cold" is the first instant query issued against that fresh pair; "warm"
/// is the same query repeated immediately after, against the same pair.
///
/// `ravel-query` itself has no segment-footer or page cache yet
/// (docs/benchmarking.md "Profiling"), but the fresh `Catalog` this
/// comparison builds owns five caches on the resolve path (commit
/// records, compaction records, HEAD, snapshot parts, postings --
/// `ravel_catalog::catalog::Catalog`), all cold at construction. The
/// cold query populates them; the warm query hits them. So this *is*
/// a cache-hit-rate measurement, of the catalog's caches, not of
/// segment-level caching (which does not exist) or of object-store/OS
/// effects.
#[derive(Serialize)]
pub struct ColdWarmReport {
    pub cold_latency_ms: f64,
    pub cold_matched_series: usize,
    pub warm_latency_ms: f64,
    pub warm_matched_series: usize,
}

#[derive(Serialize)]
pub struct Report {
    pub config: ReportConfig,
    pub writers: Vec<WriterReport>,
    pub readers: Vec<ReaderReport>,
    /// Latency across every writer's batches combined, in addition to each
    /// writer's own percentiles above: a system-wide summary alongside the
    /// per-role breakdown this bench exists to keep distinct.
    pub writer_latency_ms: LatencyReport,
    pub reader_latency_ms: LatencyReport,
    pub writer_throughput_points_per_sec: f64,
    pub reader_throughput_queries_per_sec: f64,
    pub cache: ColdWarmReport,
}

struct WriterOutcome {
    writer_id: usize,
    accepted_points: u64,
    batches_ok: u64,
    batches_err: u64,
    latencies_ns: Vec<u64>,
}

struct ReaderOutcome {
    reader_id: usize,
    queries_run: u64,
    non_empty_results: u64,
    latencies_ns: Vec<u64>,
}

#[allow(clippy::too_many_arguments)]
async fn run_writer(
    writer_id: usize,
    router: Arc<IngestRouter>,
    tenant: TenantId,
    series_idx_offset: usize,
    target_series: usize,
    points_per_sec: u64,
    duration_secs: u64,
    batch_size: usize,
    start_ts_ns: i64,
    duration_ns: i64,
    ack_deadline: Duration,
) -> WriterOutcome {
    let total_points = points_per_sec * duration_secs;
    let samples_per_series = (total_points as usize / target_series.max(1)).max(1);
    let interval_ns = (duration_ns / samples_per_series as i64).max(1);
    let workload = WorkloadConfig {
        seed: writer_id as u64,
        tenant: tenant.as_str().to_string(),
        series_count: target_series,
        samples_per_series,
        start_ts_ns,
        interval_ns,
        batch_size: BatchSizeDistribution::fixed(batch_size),
        series_idx_offset,
        ..WorkloadConfig::default()
    };
    let batches: Vec<Vec<NormalizedPoint>> = generate_batches(&workload)
        .expect("generate writer workload")
        .into_iter()
        .map(|batch| batch.into_iter().map(with_metric_name_label).collect())
        .collect();

    let pacing_interval = if points_per_sec == 0 {
        Duration::ZERO
    } else {
        Duration::from_secs_f64(batch_size as f64 / points_per_sec as f64)
    };

    // Each batch's `write()` is its own spawned task, joined only after
    // every batch has been dispatched (mirrors `ravel_bench::e2e::run`):
    // under `WriteMode::Strict`, an ack does not complete until the shard
    // has flushed, which at this bench's modest per-writer point rate is
    // paced by `IngestConfig::max_flush_delay` (500ms), not by batch
    // arrival. Awaiting each batch inline before dispatching the next
    // would serialize every batch behind its own flush tick instead of
    // letting many batches share one.
    let mut handles = Vec::with_capacity(batches.len());
    let mut next_dispatch = tokio::time::Instant::now();
    for batch in batches {
        if pacing_interval > Duration::ZERO {
            tokio::time::sleep_until(next_dispatch).await;
            next_dispatch += pacing_interval;
        }
        let router = Arc::clone(&router);
        let tenant = tenant.clone();
        let batch_len = batch.len() as u64;
        handles.push(tokio::spawn(async move {
            let start = Instant::now();
            let result = router
                .write(tenant, batch, WriteMode::Strict, ack_deadline)
                .await;
            (batch_len, start.elapsed().as_nanos() as u64, result)
        }));
    }

    let mut accepted_points = 0u64;
    let mut batches_ok = 0u64;
    let mut batches_err = 0u64;
    let mut latencies_ns = Vec::with_capacity(handles.len());
    for handle in handles {
        let (batch_len, latency_ns, result) = handle.await.expect("join writer batch task");
        latencies_ns.push(latency_ns);
        match result {
            Ok(_) => {
                accepted_points += batch_len;
                batches_ok += 1;
            }
            Err(err) => {
                eprintln!("writer {writer_id}: write error: {err}");
                batches_err += 1;
            }
        }
    }

    WriterOutcome {
        writer_id,
        accepted_points,
        batches_ok,
        batches_err,
        latencies_ns,
    }
}

#[allow(clippy::too_many_arguments)]
async fn run_reader(
    reader_id: usize,
    engine: Arc<QueryEngine>,
    clock: Arc<dyn Clock>,
    tenant_hash: ravel_types::TenantHash,
    query: String,
    query_t_ms: i64,
    run_deadline: Instant,
    query_deadline: Duration,
    writers_flushed: Arc<AtomicBool>,
) -> ReaderOutcome {
    let mut queries_run = 0u64;
    let mut non_empty_results = 0u64;
    let mut latencies_ns = Vec::new();
    while Instant::now() < run_deadline {
        let now_ns = clock.now_ns();
        let start = Instant::now();
        let result = engine
            .instant(tenant_hash, &query, query_t_ms, &[], now_ns, query_deadline)
            .await;
        latencies_ns.push(start.elapsed().as_nanos() as u64);
        queries_run += 1;
        match result {
            Ok(Value::Vector(v)) if !v.is_empty() => non_empty_results += 1,
            Ok(_) => {}
            Err(err) => eprintln!("reader {reader_id}: query error: {err}"),
        }
    }
    // `run`'s main task joins every writer, then calls `router.flush_all()`,
    // then flips `writers_flushed` -- only after that point is every accepted
    // point in this run guaranteed durable and query-able. Under scheduler
    // contention the writer/flush phase can itself take longer than
    // `run_deadline` allowed for, in which case the loop above may have
    // exited before any commit ever landed; without this, this reader would
    // report zero non-empty results regardless of real ingest success. This
    // final query runs only once the flag is set, so it always observes the
    // fully-flushed run.
    while !writers_flushed.load(Ordering::Acquire) {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let now_ns = clock.now_ns();
    let start = Instant::now();
    let result = engine
        .instant(tenant_hash, &query, query_t_ms, &[], now_ns, query_deadline)
        .await;
    latencies_ns.push(start.elapsed().as_nanos() as u64);
    queries_run += 1;
    match result {
        Ok(Value::Vector(v)) if !v.is_empty() => non_empty_results += 1,
        Ok(_) => {}
        Err(err) => eprintln!("reader {reader_id}: final query error: {err}"),
    }
    ReaderOutcome {
        reader_id,
        queries_run,
        non_empty_results,
        latencies_ns,
    }
}

pub async fn run(config: &ConcurrentConfig) -> Report {
    let store = Arc::clone(&config.store);
    // Unique per run, like `ravel_bench::e2e::run`'s tenant, so consecutive
    // local runs against the same bucket never share a prefix.
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
    let engine = Arc::new(QueryEngine::new(
        Arc::clone(&catalog),
        Arc::clone(&store),
        EngineConfig::default(),
    ));

    let writers = config.writers.max(1);
    let readers = config.readers.max(1);
    let duration_secs = config.duration_secs.max(1);
    let run_start_ns = clock.now_ns();
    let duration_ns = Duration::from_secs(duration_secs).as_nanos() as i64;
    let series_per_writer = config.target_series.max(writers).div_ceil(writers);
    let points_per_sec_per_writer = (config.points_per_sec / writers as u64).max(1);
    let ack_deadline = Duration::from_secs(config.ack_timeout_secs);
    // Fixed at the run's planned end: every writer's samples are generated
    // up front spanning [run_start_ns, run_start_ns + duration_ns), so a
    // reader querying at this instant matches whatever slice of that span
    // has landed by the time it runs -- from nothing (before the first
    // flush) to everything (after `flush_all` below), by construction.
    let query_t_ms = (run_start_ns + duration_ns) / 1_000_000;
    let query_deadline = Duration::from_secs(30);
    let run_deadline = Instant::now() + Duration::from_secs(duration_secs);

    let mut writer_handles = Vec::with_capacity(writers);
    for writer_id in 0..writers {
        writer_handles.push(tokio::spawn(run_writer(
            writer_id,
            Arc::clone(&router),
            tenant.clone(),
            writer_id * series_per_writer,
            series_per_writer,
            points_per_sec_per_writer,
            duration_secs,
            config.batch_size,
            run_start_ns,
            duration_ns,
            ack_deadline,
        )));
    }

    let writers_flushed = Arc::new(AtomicBool::new(false));
    let mut reader_handles = Vec::with_capacity(readers);
    for reader_id in 0..readers {
        reader_handles.push(tokio::spawn(run_reader(
            reader_id,
            Arc::clone(&engine),
            Arc::clone(&clock),
            tenant_hash,
            config.query.clone(),
            query_t_ms,
            run_deadline,
            query_deadline,
            Arc::clone(&writers_flushed),
        )));
    }

    let wall_start = Instant::now();
    let mut writer_outcomes = Vec::with_capacity(writers);
    for handle in writer_handles {
        writer_outcomes.push(handle.await.expect("join writer task"));
    }
    let write_elapsed_secs = wall_start.elapsed().as_secs_f64().max(1e-9);

    // Catches any writer's still-buffered tail so the cold/warm phase below,
    // and every reader's guaranteed final query (see `run_reader`), sees
    // every accepted point, not just what auto-flush already published.
    router.flush_all().await;
    writers_flushed.store(true, Ordering::Release);

    let mut reader_outcomes = Vec::with_capacity(readers);
    for handle in reader_handles {
        reader_outcomes.push(handle.await.expect("join reader task"));
    }

    // Cold vs warm: a fresh Catalog/QueryEngine built right after ingest
    // completes, distinct from the shared `engine` the concurrent readers
    // above used for the whole run (which by then has already resolved
    // this tenant's snapshot many times over).
    let cold_catalog = Arc::new(
        Catalog::new(
            Arc::clone(&store),
            CatalogConfig {
                shard_count: config.shards,
                ..CatalogConfig::default()
            },
        )
        .expect("cold catalog config"),
    );
    let cold_engine = QueryEngine::new(cold_catalog, Arc::clone(&store), EngineConfig::default());
    let query_now_ns = clock.now_ns();

    let cold_start = Instant::now();
    let cold_value = cold_engine
        .instant(
            tenant_hash,
            &config.query,
            query_t_ms,
            &[],
            query_now_ns,
            query_deadline,
        )
        .await
        .expect("cold instant query");
    let cold_latency_ms = cold_start.elapsed().as_secs_f64() * 1e3;
    let cold_matched_series = match cold_value {
        Value::Vector(v) => v.len(),
        _ => 0,
    };

    let warm_start = Instant::now();
    let warm_value = cold_engine
        .instant(
            tenant_hash,
            &config.query,
            query_t_ms,
            &[],
            query_now_ns,
            query_deadline,
        )
        .await
        .expect("warm instant query");
    let warm_latency_ms = warm_start.elapsed().as_secs_f64() * 1e3;
    let warm_matched_series = match warm_value {
        Value::Vector(v) => v.len(),
        _ => 0,
    };

    let total_accepted: u64 = writer_outcomes.iter().map(|w| w.accepted_points).sum();
    let total_queries: u64 = reader_outcomes.iter().map(|r| r.queries_run).sum();
    let all_writer_latencies_ns: Vec<u64> = writer_outcomes
        .iter()
        .flat_map(|w| w.latencies_ns.iter().copied())
        .collect();
    let all_reader_latencies_ns: Vec<u64> = reader_outcomes
        .iter()
        .flat_map(|r| r.latencies_ns.iter().copied())
        .collect();

    let writers_report: Vec<WriterReport> = writer_outcomes
        .into_iter()
        .map(|w| WriterReport {
            writer_id: w.writer_id,
            accepted_points: w.accepted_points,
            batches_ok: w.batches_ok,
            batches_err: w.batches_err,
            latency_ms: latency_stats(w.latencies_ns).into(),
        })
        .collect();
    let readers_report: Vec<ReaderReport> = reader_outcomes
        .into_iter()
        .map(|r| ReaderReport {
            reader_id: r.reader_id,
            queries_run: r.queries_run,
            non_empty_results: r.non_empty_results,
            latency_ms: latency_stats(r.latencies_ns).into(),
        })
        .collect();

    Report {
        config: ReportConfig {
            store: config.store_label.clone(),
            shards: config.shards,
            writers,
            readers,
            target_series: config.target_series,
            points_per_sec: config.points_per_sec,
            duration_secs: config.duration_secs,
            batch_size: config.batch_size,
            query: config.query.clone(),
        },
        writers: writers_report,
        readers: readers_report,
        writer_latency_ms: latency_stats(all_writer_latencies_ns).into(),
        reader_latency_ms: latency_stats(all_reader_latencies_ns).into(),
        writer_throughput_points_per_sec: total_accepted as f64 / write_elapsed_secs,
        reader_throughput_queries_per_sec: total_queries as f64 / duration_secs as f64,
        cache: ColdWarmReport {
            cold_latency_ms,
            cold_matched_series,
            warm_latency_ms,
            warm_matched_series,
        },
    }
}
