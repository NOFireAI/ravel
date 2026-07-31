//! End-to-end PromQL query-latency benchmark core (docs/benchmarking.md
//! "End-to-end", issue #270): every other bench in this crate measures one
//! isolated stage (fetch, decode, merge -- `read_accounting`,
//! `codecs`, `section_accounting`) or the full ingest path
//! (`ravel_bench::e2e`, `ravel_bench::concurrent`), but none of them time
//! the full PromQL evaluator itself. This module ingests a fixed dataset,
//! flushes it durable, then repeatedly runs a PromQL instant query and a
//! PromQL range query against it, reporting latency percentiles separately
//! for each shape.
//!
//! Unlike `ravel_bench::concurrent`, there is no live write phase running
//! concurrently with the measured queries: ingest happens once, up front,
//! followed by `router.flush_all()`, so a plain flush-then-query sequence
//! (mirroring `ravel_bench::e2e`'s shape) is sufficient -- there is nothing
//! for the queries to race.
//!
//! Lives in the lib (not the `query_latency_bench` bin), mirroring `e2e.rs`
//! and `concurrent.rs`, so `tests/query_latency_smoke.rs` can exercise the
//! same path the bin runs. Report-only: never changes
//! ravel-ingest/ravel-catalog/ravel-query behavior, only measures it.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
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

/// Same rationale and shape as `ravel_bench::e2e`'s and
/// `ravel_bench::concurrent`'s private helper of the same name:
/// `generate_batches` derives each series' identity from a metric name but
/// never stores that name as a `__name__` label, so a PromQL selector
/// matches nothing without it. Duplicated rather than shared -- see the
/// final report for why a third copy-site for two lines of struct-building
/// was not worth extracting.
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

/// Duplicated from `ravel_bench::e2e`/`ravel_bench::concurrent` rather than
/// extracted to a shared module: the task that added this bench was scoped
/// to add-only (new files, no edits to landed `e2e.rs`/`concurrent.rs`), and
/// `concurrent.rs` already established the precedent of duplicating this
/// exact percentile math at its second copy-site. Behavior must not diverge
/// from theirs -- same nearest-rank percentile, same sort-then-index.
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

/// Inputs for one query-latency run.
pub struct QueryLatencyConfig {
    pub store: Arc<dyn ObjectStoreBackend>,
    pub store_label: String,
    pub shards: u32,
    pub target_series: usize,
    pub points_per_sec: u64,
    pub duration_secs: u64,
    pub batch_size: usize,
    pub ack_timeout_secs: u64,
    /// PromQL selector both the instant and range query phases evaluate;
    /// must match the workload generator's metric name.
    pub query: String,
    /// Number of repeated instant queries run for the instant latency
    /// percentiles.
    pub instant_query_count: usize,
    /// Number of repeated range queries run for the range latency
    /// percentiles.
    pub range_query_count: usize,
    /// Number of steps the range query covers across the ingested window.
    pub range_steps: usize,
}

#[derive(Serialize)]
pub struct ReportConfig {
    pub store: String,
    pub shards: u32,
    pub target_series: usize,
    pub points_per_sec: u64,
    pub duration_secs: u64,
    pub batch_size: usize,
    pub query: String,
    pub instant_query_count: usize,
    pub range_query_count: usize,
    pub range_steps: usize,
}

#[derive(Serialize)]
pub struct Report {
    pub config: ReportConfig,
    pub accepted_points: u64,
    /// Matched series in the first instant query's result vector. Zero
    /// (rather than the query phase being skipped) if the selector matched
    /// nothing -- see `ravel_bench::e2e::Report::query_matched_series` for
    /// why this is left visible rather than folded away.
    pub instant_matched_series: usize,
    pub instant_latency_ms: LatencyReport,
    /// Matched series in the first range query's result matrix.
    pub range_matched_series: usize,
    pub range_latency_ms: LatencyReport,
}

pub async fn run(config: &QueryLatencyConfig) -> Report {
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

    let total_points = config.points_per_sec * config.duration_secs;
    let samples_per_series = (total_points as usize / config.target_series.max(1)).max(1);
    let run_start_ns = clock.now_ns();
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
    let batches: Vec<Vec<_>> = generate_batches(&workload)
        .expect("generate workload")
        .into_iter()
        .map(|batch| batch.into_iter().map(with_metric_name_label).collect())
        .collect();

    let ack_deadline = Duration::from_secs(config.ack_timeout_secs);

    // Every batch is dispatched concurrently and joined, with no pacing: this
    // bench measures query latency, not ingest throughput or ack latency, so
    // (unlike `e2e::run`) there is no reason to spread dispatch over
    // `duration_secs` of real wall time. `router.flush_all()` below is what
    // actually guarantees every accepted point is durable and query-able
    // before the measured phase starts, regardless of how fast dispatch ran.
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

    // A fresh `QueryEngine` over the now-durable dataset, timed with
    // `Instant::now()` bracketing exactly the `engine.instant()`/
    // `engine.range()` call and nothing else.
    let engine = QueryEngine::new(
        Arc::clone(&catalog),
        Arc::clone(&store),
        EngineConfig::default(),
    );
    let query_now_ns = clock.now_ns();
    let query_deadline = Duration::from_secs(30);
    let instant_t_ms = (run_start_ns + duration_ns) / 1_000_000;
    let range_start_ms = run_start_ns / 1_000_000;
    let range_end_ms = instant_t_ms;
    let duration_ms = (duration_ns / 1_000_000).max(1);
    let range_step_ms = (duration_ms / config.range_steps.max(1) as i64).max(1);

    let mut instant_latencies_ns = Vec::with_capacity(config.instant_query_count);
    let mut instant_matched_series = 0usize;
    for i in 0..config.instant_query_count {
        let start = Instant::now();
        let value = engine
            .instant(
                tenant_hash,
                &config.query,
                instant_t_ms,
                &[],
                query_now_ns,
                query_deadline,
            )
            .await
            .expect("instant query");
        instant_latencies_ns.push(start.elapsed().as_nanos() as u64);
        if i == 0 {
            instant_matched_series = match value {
                Value::Vector(v) => v.len(),
                _ => 0,
            };
        }
    }

    let mut range_latencies_ns = Vec::with_capacity(config.range_query_count);
    let mut range_matched_series = 0usize;
    for i in 0..config.range_query_count {
        let start = Instant::now();
        let value = engine
            .range(
                tenant_hash,
                &config.query,
                range_start_ms,
                range_end_ms,
                range_step_ms,
                &[],
                query_now_ns,
                query_deadline,
            )
            .await
            .expect("range query");
        range_latencies_ns.push(start.elapsed().as_nanos() as u64);
        if i == 0 {
            range_matched_series = match value {
                Value::Matrix(m) => m.len(),
                _ => 0,
            };
        }
    }

    Report {
        config: ReportConfig {
            store: config.store_label.clone(),
            shards: config.shards,
            target_series: config.target_series,
            points_per_sec: config.points_per_sec,
            duration_secs: config.duration_secs,
            batch_size: config.batch_size,
            query: config.query.clone(),
            instant_query_count: config.instant_query_count,
            range_query_count: config.range_query_count,
            range_steps: config.range_steps,
        },
        accepted_points,
        instant_matched_series,
        instant_latency_ms: latency_stats(instant_latencies_ns).into(),
        range_matched_series,
        range_latency_ms: latency_stats(range_latencies_ns).into(),
    }
}
