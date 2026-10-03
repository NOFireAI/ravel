//! Report-only measurement for issue #2445 (epic #2425): what share of an
//! instant PromQL query's end-to-end wall time is spent inside the
//! aggregation operator (`sum by`) and inside one-to-one vector matching
//! (`+`). Reuses the same ingest-then-flush-then-query shape
//! `ravel_bench::query_latency` uses, with an in-memory object store, but
//! builds its own exact label layout instead of the generic workload
//! generator: the question needs an exact group count and an exact matched-
//! series count, which `ravel_bench::generator`'s random label assignment
//! does not guarantee.
//!
//! Never changes ravel-ingest/ravel-catalog/ravel-promql/ravel-query
//! behavior, only measures it. Reads `ravel_promql::op_timers`'s always-on
//! atomic counters, which this bin is the only consumer of outside
//! `ravel-promql` itself.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use ravel_bench::harness::{StoreKind, store_from_env};
use ravel_catalog::{Catalog, CatalogConfig};
use ravel_ingest::{Clock, IngestConfig, IngestRouter, SystemClock, WriteMode};
use ravel_otlp::NormalizedPoint;
use ravel_promql::Value;
use ravel_promql::op_timers;
use ravel_query::{EngineConfig, QueryEngine};
use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, TenantId};

/// Series per metric. Two metrics (A, B), [`SERIES_PER_METRIC`] each, for
/// 200,000 series total.
const SERIES_PER_METRIC: usize = 100_000;
/// `sum by (grp_major, grp_minor) (metric_a)` must produce exactly this many
/// groups: `grp_major`/`grp_minor` are assigned from `i % TOTAL_GROUPS`, so
/// every one of the 100,000 series of metric A lands in one of exactly this
/// many (group-label) combinations, each hit `SERIES_PER_METRIC /
/// TOTAL_GROUPS` times.
const TOTAL_GROUPS: usize = 10_000;
/// Number of metric-A series (the first `MATCHED` by index) that carry an
/// identical non-`__name__` label set to a metric-B series, and hence have a
/// one-to-one match; the rest of each metric has none.
const MATCHED: usize = 50_000;

const METRIC_A: &str = "promql_op_share_a";
const METRIC_B: &str = "promql_op_share_b";
const GROUP_LABEL_1: &str = "grp_major";
const GROUP_LABEL_2: &str = "grp_minor";

const Q_AGG: &str = "sum by (grp_major, grp_minor) (promql_op_share_a)";
const Q_MATCH: &str = "promql_op_share_a + promql_op_share_b";

const START_TS_NS: i64 = 1_700_000_000_000_000_000;
const WARMUPS: usize = 2;
const RUNS: usize = 5;
const QUERIES_PER_RUN: usize = 5;

/// Build one series' label set and `SeriesId`. `i` ranges over
/// `0..SERIES_PER_METRIC`, independently for each metric: the group-label
/// pair depends only on `i % TOTAL_GROUPS`, so metric A's grouping is exact
/// regardless of the match partition below. `series_key` is what actually
/// decides matching: for `i < MATCHED` it is identical across A and B (a
/// one-to-one partner exists), and for `i >= MATCHED` it is tagged by metric
/// so it can equal no label set on the other side.
fn build_series(tenant: &TenantId, metric: &str, i: usize) -> (SeriesId, LabelSet) {
    let group = i % TOTAL_GROUPS;
    let grp_major = group % 100;
    let grp_minor = group / 100;
    let series_key = if i < MATCHED {
        format!("paired-{i:06}")
    } else if metric == METRIC_A {
        format!("solo-a-{i:06}")
    } else {
        format!("solo-b-{i:06}")
    };
    let labels = LabelSet::new(vec![
        Label {
            name: METRIC_NAME_LABEL.to_string(),
            value: metric.to_string(),
        },
        Label {
            name: GROUP_LABEL_1.to_string(),
            value: format!("major-{grp_major:03}"),
        },
        Label {
            name: GROUP_LABEL_2.to_string(),
            value: format!("minor-{grp_minor:03}"),
        },
        Label {
            name: "dc_zone".to_string(),
            value: "zone-use1".to_string(),
        },
        Label {
            name: "host_pool".to_string(),
            value: "pool-general".to_string(),
        },
        Label {
            name: "svc_tier".to_string(),
            value: "tier-standard".to_string(),
        },
        Label {
            name: "series_key".to_string(),
            value: series_key,
        },
    ])
    .expect("label set");
    let series_id = SeriesId::compute(tenant, metric, &labels).expect("series id");
    (series_id, labels)
}

fn build_points(tenant: &TenantId) -> Vec<NormalizedPoint> {
    let mut points = Vec::with_capacity(SERIES_PER_METRIC * 2);
    for metric in [METRIC_A, METRIC_B] {
        for i in 0..SERIES_PER_METRIC {
            let (series_id, labels) = build_series(tenant, metric, i);
            points.push(NormalizedPoint {
                series_id,
                labels: Arc::new(labels),
                sample: Sample {
                    ts_ns: START_TS_NS,
                    value: 1.0,
                },
                is_monotonic_sum: false,
            });
        }
    }
    points
}

struct RunStats {
    min: f64,
    median: f64,
    max: f64,
}

/// Median/min/max over 5 per-run means (not over the 25 individual query
/// samples): each run's mean is one data point, so a run slowed by one
/// outlier query is not allowed to dominate the summary the way averaging
/// all 25 raw samples together would.
fn run_stats(per_run_means: &mut [f64]) -> RunStats {
    per_run_means.sort_by(|a, b| a.partial_cmp(b).expect("no NaN in timings"));
    let n = per_run_means.len();
    RunStats {
        min: per_run_means[0],
        median: per_run_means[n / 2],
        max: per_run_means[n - 1],
    }
}

fn mean(values: &[u64]) -> f64 {
    values.iter().sum::<u64>() as f64 / values.len() as f64
}

/// One query's full measured sample set: wall time and the operator-counter
/// deltas straddling each of the `RUNS * QUERIES_PER_RUN` executions.
#[derive(Default)]
struct QuerySamples {
    wall_ns: Vec<u64>,
    agg_ns: Vec<u64>,
    agg_calls: Vec<u64>,
    agg_sort_ns: Vec<u64>,
    match_ns: Vec<u64>,
    match_calls: Vec<u64>,
}

async fn run_instant(
    engine: &QueryEngine,
    tenant_hash: ravel_types::TenantHash,
    query: &str,
    t_ms: i64,
    now_ns: i64,
) -> (usize, u64, u64, u64, u64, u64, u64) {
    let agg_ns_before = op_timers::AGG_NS.load(Ordering::Relaxed);
    let agg_calls_before = op_timers::AGG_CALLS.load(Ordering::Relaxed);
    let agg_sort_ns_before = op_timers::AGG_SORT_NS.load(Ordering::Relaxed);
    let match_ns_before = op_timers::MATCH_NS.load(Ordering::Relaxed);
    let match_calls_before = op_timers::MATCH_CALLS.load(Ordering::Relaxed);

    let start = Instant::now();
    let (value, _coverage) = engine
        .instant(
            tenant_hash,
            query,
            t_ms,
            &[],
            now_ns,
            Duration::from_secs(30),
        )
        .await
        .expect("instant query");
    let wall_ns = start.elapsed().as_nanos() as u64;

    let matched = match value {
        Value::Vector(v) => v.len(),
        _ => 0,
    };
    let agg_ns = op_timers::AGG_NS.load(Ordering::Relaxed) - agg_ns_before;
    let agg_calls = op_timers::AGG_CALLS.load(Ordering::Relaxed) - agg_calls_before;
    let agg_sort_ns = op_timers::AGG_SORT_NS.load(Ordering::Relaxed) - agg_sort_ns_before;
    let match_ns = op_timers::MATCH_NS.load(Ordering::Relaxed) - match_ns_before;
    let match_calls = op_timers::MATCH_CALLS.load(Ordering::Relaxed) - match_calls_before;
    (
        matched,
        wall_ns,
        agg_ns,
        agg_calls,
        agg_sort_ns,
        match_ns,
        match_calls,
    )
}

#[tokio::main]
async fn main() {
    let store = store_from_env(StoreKind::Memory);
    let tenant = TenantId::new(format!("promql-op-share-{}", uuid::Uuid::new_v4()));
    let tenant_hash = tenant.hash();
    let signal = ravel_types::Signal::Metrics;

    let shard_count = 4;
    let ingest_config = IngestConfig {
        shard_count,
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
                shard_count,
                ..CatalogConfig::default()
            },
        )
        .expect("catalog config"),
    );

    let points = build_points(&tenant);
    let batch_size = 2_000;
    let ack_deadline = Duration::from_secs(30);
    let mut handles = Vec::new();
    for batch in points.chunks(batch_size) {
        let router = Arc::clone(&router);
        let tenant = tenant.clone();
        let batch = batch.to_vec();
        let batch_len = batch.len() as u64;
        handles.push(tokio::spawn(async move {
            let result = router
                .write(tenant, batch, WriteMode::Strict, ack_deadline)
                .await;
            (batch_len, result)
        }));
    }
    let mut accepted: u64 = 0;
    for handle in handles {
        let (batch_len, result) = handle.await.expect("join write task");
        match result {
            Ok(_) => accepted += batch_len,
            Err(err) => panic!("ingest write error: {err}"),
        }
    }

    // Ingest, through `IngestRouter::write` (the normal ingest path, same
    // entry point `ravel_bench::query_latency` uses), lands in the shard's
    // live write-ahead buffer first. `flush_all()` is what seals that buffer
    // into durable RSEG segments in the object store, so every query below
    // reads flushed, stored segments -- never the unflushed buffer.
    router.flush_all().await;

    // `EngineConfig::default()`'s `max_series` (10_000, `DEFAULT_MAX_SERIES`)
    // caps the raw series a fetch may return before aggregation collapses
    // them; Q_AGG's selector alone matches all `SERIES_PER_METRIC` series of
    // metric A pre-aggregation, so the cap must be raised past that for this
    // bench's known volume, not past Q_MATCH's combined selector need.
    let engine = QueryEngine::new(
        Arc::clone(&catalog),
        Arc::clone(&store),
        EngineConfig {
            max_series: SERIES_PER_METRIC * 2 + 1,
            ..EngineConfig::default()
        },
    );
    let query_now_ns = clock.now_ns();
    let t_ms = START_TS_NS / 1_000_000;

    // Warm-ups: discarded, not recorded into any sample vector.
    for _ in 0..WARMUPS {
        run_instant(&engine, tenant_hash, Q_AGG, t_ms, query_now_ns).await;
        run_instant(&engine, tenant_hash, Q_MATCH, t_ms, query_now_ns).await;
    }

    let mut agg_samples = QuerySamples::default();
    let mut match_samples = QuerySamples::default();
    let mut agg_run_means = Vec::with_capacity(RUNS);
    let mut match_run_means = Vec::with_capacity(RUNS);
    let mut agg_matched_count = None;
    let mut match_matched_count = None;

    for _run in 0..RUNS {
        let mut agg_run_wall = Vec::with_capacity(QUERIES_PER_RUN);
        let mut match_run_wall = Vec::with_capacity(QUERIES_PER_RUN);
        for _q in 0..QUERIES_PER_RUN {
            let (matched, wall_ns, agg_ns, agg_calls, agg_sort_ns, match_ns, match_calls) =
                run_instant(&engine, tenant_hash, Q_AGG, t_ms, query_now_ns).await;
            agg_matched_count.get_or_insert(matched);
            agg_samples.wall_ns.push(wall_ns);
            agg_samples.agg_ns.push(agg_ns);
            agg_samples.agg_calls.push(agg_calls);
            agg_samples.agg_sort_ns.push(agg_sort_ns);
            agg_samples.match_ns.push(match_ns);
            agg_samples.match_calls.push(match_calls);
            agg_run_wall.push(wall_ns);

            let (matched, wall_ns, agg_ns, agg_calls, agg_sort_ns, match_ns, match_calls) =
                run_instant(&engine, tenant_hash, Q_MATCH, t_ms, query_now_ns).await;
            match_matched_count.get_or_insert(matched);
            match_samples.wall_ns.push(wall_ns);
            match_samples.agg_ns.push(agg_ns);
            match_samples.agg_calls.push(agg_calls);
            match_samples.agg_sort_ns.push(agg_sort_ns);
            match_samples.match_ns.push(match_ns);
            match_samples.match_calls.push(match_calls);
            match_run_wall.push(wall_ns);
        }
        agg_run_means.push(mean(&agg_run_wall));
        match_run_means.push(mean(&match_run_wall));
    }

    let agg_matched = agg_matched_count.expect("at least one Q_AGG run");
    let match_matched = match_matched_count.expect("at least one Q_MATCH run");

    // --- Assertions (exit non-zero on violation) ---
    let mut failures = Vec::new();
    if agg_matched != TOTAL_GROUPS {
        failures.push(format!(
            "Q_AGG matched {agg_matched} series, want exactly {TOTAL_GROUPS}"
        ));
    }
    if match_matched != MATCHED {
        failures.push(format!(
            "Q_MATCH matched {match_matched} series, want exactly {MATCHED}"
        ));
    }
    if agg_samples.agg_calls.iter().any(|&c| c != 1) {
        failures.push("AGG_CALLS did not advance by exactly 1 per Q_AGG execution".to_string());
    }
    if agg_samples.match_calls.iter().any(|&c| c != 0) {
        failures.push("MATCH_CALLS advanced on a Q_AGG execution (want 0)".to_string());
    }
    if match_samples.match_calls.iter().any(|&c| c != 1) {
        failures.push("MATCH_CALLS did not advance by exactly 1 per Q_MATCH execution".to_string());
    }
    if match_samples.agg_calls.iter().any(|&c| c != 0) {
        failures.push("AGG_CALLS advanced on a Q_MATCH execution (want 0)".to_string());
    }
    if !failures.is_empty() {
        eprintln!("promql_operator_share: assertion failures:");
        for f in &failures {
            eprintln!("  - {f}");
        }
        std::process::exit(1);
    }

    // Per-run means, in run order (index r = run r), kept unsorted so the
    // operator-share ratio below pairs each run's operator time with that
    // SAME run's total time, not an independently-sorted one.
    let agg_op_run_means: Vec<f64> = (0..RUNS)
        .map(|r| mean(&agg_samples.agg_ns[r * QUERIES_PER_RUN..(r + 1) * QUERIES_PER_RUN]))
        .collect();
    let match_op_run_means: Vec<f64> = (0..RUNS)
        .map(|r| mean(&match_samples.match_ns[r * QUERIES_PER_RUN..(r + 1) * QUERIES_PER_RUN]))
        .collect();
    let agg_share_run: Vec<f64> = (0..RUNS)
        .map(|r| 100.0 * agg_op_run_means[r] / agg_run_means[r])
        .collect();
    let match_share_run: Vec<f64> = (0..RUNS)
        .map(|r| 100.0 * match_op_run_means[r] / match_run_means[r])
        .collect();

    let agg_total = run_stats(&mut agg_run_means.clone());
    let match_total = run_stats(&mut match_run_means.clone());
    let agg_op = run_stats(&mut agg_op_run_means.clone());
    let match_op = run_stats(&mut match_op_run_means.clone());
    let agg_share = run_stats(&mut agg_share_run.clone());
    let match_share = run_stats(&mut match_share_run.clone());

    let agg_sort_total_ns: u64 = agg_samples.agg_sort_ns.iter().sum();
    let agg_sort_mean_ns = mean(&agg_samples.agg_sort_ns);

    // Raw per-run figures (5 run-means each), printed before the aggregated
    // stats below so the report's appendix can quote them verbatim.
    println!("promql_operator_share raw per-run means (run order, ns unless noted)");
    println!("  Q_AGG   wall_ns per run   : {agg_run_means:?}");
    println!("  Q_AGG   op_ns per run     : {agg_op_run_means:?}");
    println!("  Q_AGG   share_pct per run : {agg_share_run:?}");
    println!("  Q_MATCH wall_ns per run   : {match_run_means:?}");
    println!("  Q_MATCH op_ns per run     : {match_op_run_means:?}");
    println!("  Q_MATCH share_pct per run : {match_share_run:?}");
    println!(
        "  Q_AGG   sort_ns per query : {:?}",
        agg_samples.agg_sort_ns
    );

    println!("promql_operator_share report");
    println!("  accepted_points   : {accepted}");
    println!("  Q_AGG matched     : {agg_matched} (want {TOTAL_GROUPS})");
    println!("  Q_MATCH matched   : {match_matched} (want {MATCHED})");
    println!(
        "  Q_AGG   total_ns  : min={:.0} median={:.0} max={:.0}",
        agg_total.min, agg_total.median, agg_total.max
    );
    println!(
        "  Q_AGG   op_ns     : min={:.0} median={:.0} max={:.0}",
        agg_op.min, agg_op.median, agg_op.max
    );
    println!("  Q_AGG   sort_ns   : total={agg_sort_total_ns} mean_per_call={agg_sort_mean_ns:.0}");
    println!(
        "  Q_MATCH total_ns  : min={:.0} median={:.0} max={:.0}",
        match_total.min, match_total.median, match_total.max
    );
    println!(
        "  Q_MATCH op_ns     : min={:.0} median={:.0} max={:.0}",
        match_op.min, match_op.median, match_op.max
    );
    println!(
        "  Q_AGG   OPERATOR_SHARE % : min={:.3} median={:.3} max={:.3}",
        agg_share.min, agg_share.median, agg_share.max
    );
    println!(
        "  Q_MATCH OPERATOR_SHARE % : min={:.3} median={:.3} max={:.3}",
        match_share.min, match_share.median, match_share.max
    );
}
