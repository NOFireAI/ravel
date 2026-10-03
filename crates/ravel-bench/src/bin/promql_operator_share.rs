//! Report-only measurement for issue #2445 (epic #2425) and its stage 0
//! follow-up, issue #2468: what share of an instant PromQL query's
//! end-to-end wall time is spent inside the aggregation operator
//! (`sum by`), inside one-to-one vector matching (`+`), and -- the
//! #2468 extension -- inside catalog resolve, object fetch, segment
//! decode, series materialisation, and result assembly. Reuses the same
//! ingest-then-flush-then-query shape `ravel_bench::query_latency` uses,
//! with an in-memory object store, but builds its own exact label layout
//! instead of the generic workload generator: the question needs an exact
//! group count and an exact matched-series count, which
//! `ravel_bench::generator`'s random label assignment does not guarantee.
//!
//! Never changes ravel-ingest/ravel-catalog/ravel-promql/ravel-query
//! behavior, only measures it. Reads `ravel_promql::op_timers`'s and
//! `ravel_query::phase_timers`'s always-on atomic counters, which this bin
//! is the only consumer of outside those crates themselves.
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
use ravel_query::{EngineConfig, QueryEngine, QueryPhase, phase_timers};
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

/// Pre-registered share-of-wall bands (lo%, hi%), issue #2468. A `< X%`
/// expectation is `(0.0, X)`. Checked against the MEDIAN share across the 5
/// runs; never tuned after the fact.
struct Band {
    lo: f64,
    hi: f64,
}
const BAND_RESOLVE: Band = Band { lo: 0.0, hi: 5.0 };
const BAND_FETCH: Band = Band { lo: 0.0, hi: 15.0 };
const BAND_DECODE_AGG: Band = Band { lo: 25.0, hi: 50.0 };
const BAND_DECODE_MATCH: Band = Band { lo: 20.0, hi: 40.0 };
const BAND_MATERIALIZE: Band = Band { lo: 25.0, hi: 50.0 };
const BAND_OPERATOR_AGG: Band = Band { lo: 4.0, hi: 5.0 };
const BAND_OPERATOR_MATCH: Band = Band { lo: 11.0, hi: 13.0 };
const BAND_ASSEMBLY: Band = Band { lo: 0.0, hi: 10.0 };
const BAND_REMAINDER: Band = Band { lo: 0.0, hi: 5.0 };

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

/// Every figure one instant-query execution produces: wall time, the
/// operator-counter deltas (issue #2445), and the phase-timer deltas
/// (issue #2468), plus the per-query (not diffed) fetch request/byte
/// counts and segment count `QueryStats` already carries.
struct QueryInstantSample {
    matched: usize,
    wall_ns: u64,
    agg_ns: u64,
    agg_calls: u64,
    agg_sort_ns: u64,
    match_ns: u64,
    match_calls: u64,
    resolve_ns: u64,
    resolve_calls: u64,
    fetch_ns: u64,
    fetch_calls: u64,
    decode_ns: u64,
    decode_calls: u64,
    fetch_decode_wall_ns: u64,
    fetch_decode_wall_calls: u64,
    materialize_ns: u64,
    materialize_calls: u64,
    result_assembly_ns: u64,
    result_assembly_calls: u64,
    segments_fetched: u64,
    fetch_requests: u64,
    fetch_bytes: u64,
}

async fn run_instant(
    engine: &QueryEngine,
    tenant_hash: ravel_types::TenantHash,
    query: &str,
    t_ms: i64,
    now_ns: i64,
) -> QueryInstantSample {
    let agg_ns_before = op_timers::AGG_NS.load(Ordering::Relaxed);
    let agg_calls_before = op_timers::AGG_CALLS.load(Ordering::Relaxed);
    let agg_sort_ns_before = op_timers::AGG_SORT_NS.load(Ordering::Relaxed);
    let match_ns_before = op_timers::MATCH_NS.load(Ordering::Relaxed);
    let match_calls_before = op_timers::MATCH_CALLS.load(Ordering::Relaxed);
    let resolve_ns_before = phase_timers::RESOLVE_NS.load(Ordering::Relaxed);
    let resolve_calls_before = phase_timers::RESOLVE_CALLS.load(Ordering::Relaxed);
    let fetch_ns_before = phase_timers::FETCH_NS.load(Ordering::Relaxed);
    let fetch_calls_before = phase_timers::FETCH_CALLS.load(Ordering::Relaxed);
    let decode_ns_before = phase_timers::DECODE_NS.load(Ordering::Relaxed);
    let decode_calls_before = phase_timers::DECODE_CALLS.load(Ordering::Relaxed);
    let fdw_ns_before = phase_timers::FETCH_DECODE_WALL_NS.load(Ordering::Relaxed);
    let fdw_calls_before = phase_timers::FETCH_DECODE_WALL_CALLS.load(Ordering::Relaxed);
    let materialize_ns_before = phase_timers::MATERIALIZE_NS.load(Ordering::Relaxed);
    let materialize_calls_before = phase_timers::MATERIALIZE_CALLS.load(Ordering::Relaxed);
    let assembly_ns_before = phase_timers::RESULT_ASSEMBLY_NS.load(Ordering::Relaxed);
    let assembly_calls_before = phase_timers::RESULT_ASSEMBLY_CALLS.load(Ordering::Relaxed);

    let start = Instant::now();
    let (value, stats) = engine
        .instant_with_stats(
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

    // "Object fetch" request/byte figures for this one query, straight off
    // `QueryStats` (computed fresh per query, so no before/after diff is
    // needed here unlike the atomics above): Plan (footer/skip-index probe)
    // + Probe (catalog section fetch) + Scan (page fetch) sum to every GET
    // this query issued against segment data, excluding Resolve's own
    // catalog-listing requests.
    let fetch_requests = stats.phase_accounting.phase(QueryPhase::Plan).total_s3_requests()
        + stats
            .phase_accounting
            .phase(QueryPhase::Probe)
            .total_s3_requests()
        + stats
            .phase_accounting
            .phase(QueryPhase::Scan)
            .total_s3_requests();
    let fetch_bytes = stats.phase_accounting.phase(QueryPhase::Plan).total_s3_bytes()
        + stats.phase_accounting.phase(QueryPhase::Probe).total_s3_bytes()
        + stats.phase_accounting.phase(QueryPhase::Scan).total_s3_bytes();

    QueryInstantSample {
        matched,
        wall_ns,
        agg_ns: op_timers::AGG_NS.load(Ordering::Relaxed) - agg_ns_before,
        agg_calls: op_timers::AGG_CALLS.load(Ordering::Relaxed) - agg_calls_before,
        agg_sort_ns: op_timers::AGG_SORT_NS.load(Ordering::Relaxed) - agg_sort_ns_before,
        match_ns: op_timers::MATCH_NS.load(Ordering::Relaxed) - match_ns_before,
        match_calls: op_timers::MATCH_CALLS.load(Ordering::Relaxed) - match_calls_before,
        resolve_ns: phase_timers::RESOLVE_NS.load(Ordering::Relaxed) - resolve_ns_before,
        resolve_calls: phase_timers::RESOLVE_CALLS.load(Ordering::Relaxed) - resolve_calls_before,
        fetch_ns: phase_timers::FETCH_NS.load(Ordering::Relaxed) - fetch_ns_before,
        fetch_calls: phase_timers::FETCH_CALLS.load(Ordering::Relaxed) - fetch_calls_before,
        decode_ns: phase_timers::DECODE_NS.load(Ordering::Relaxed) - decode_ns_before,
        decode_calls: phase_timers::DECODE_CALLS.load(Ordering::Relaxed) - decode_calls_before,
        fetch_decode_wall_ns: phase_timers::FETCH_DECODE_WALL_NS.load(Ordering::Relaxed)
            - fdw_ns_before,
        fetch_decode_wall_calls: phase_timers::FETCH_DECODE_WALL_CALLS.load(Ordering::Relaxed)
            - fdw_calls_before,
        materialize_ns: phase_timers::MATERIALIZE_NS.load(Ordering::Relaxed)
            - materialize_ns_before,
        materialize_calls: phase_timers::MATERIALIZE_CALLS.load(Ordering::Relaxed)
            - materialize_calls_before,
        result_assembly_ns: phase_timers::RESULT_ASSEMBLY_NS.load(Ordering::Relaxed)
            - assembly_ns_before,
        result_assembly_calls: phase_timers::RESULT_ASSEMBLY_CALLS.load(Ordering::Relaxed)
            - assembly_calls_before,
        segments_fetched: stats.segments_fetched,
        fetch_requests,
        fetch_bytes,
    }
}

fn per_run_u64_means(samples: &[QueryInstantSample], f: impl Fn(&QueryInstantSample) -> u64) -> Vec<f64> {
    (0..RUNS)
        .map(|r| {
            let slice = &samples[r * QUERIES_PER_RUN..(r + 1) * QUERIES_PER_RUN];
            let vals: Vec<u64> = slice.iter().map(|s| f(s)).collect();
            mean(&vals)
        })
        .collect()
}

fn share_of(field_means: &[f64], wall_means: &[f64]) -> Vec<f64> {
    (0..RUNS)
        .map(|r| 100.0 * field_means[r] / wall_means[r])
        .collect()
}

fn band_verdict(value: f64, band: &Band) -> &'static str {
    if value < band.lo {
        "below band"
    } else if value <= band.hi {
        "inside band"
    } else {
        "above band"
    }
}

struct PhaseReport {
    time: RunStats,
    share: RunStats,
    verdict: &'static str,
}

fn phase_report(
    samples: &[QueryInstantSample],
    wall_run_means: &[f64],
    f: impl Fn(&QueryInstantSample) -> u64,
    band: &Band,
) -> PhaseReport {
    let field_means = per_run_u64_means(samples, f);
    let share_run = share_of(&field_means, wall_run_means);
    let time = run_stats(&mut field_means.clone());
    let share = run_stats(&mut share_run.clone());
    let verdict = band_verdict(share.median, band);
    PhaseReport {
        time,
        share,
        verdict,
    }
}

fn print_phase(label: &str, r: &PhaseReport) {
    println!(
        "  {label:<34}: time_ns min={:.0} median={:.0} max={:.0} | share% min={:.3} median={:.3} max={:.3} -> {}",
        r.time.min, r.time.median, r.time.max, r.share.min, r.share.median, r.share.max, r.verdict
    );
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

    let mut agg_samples: Vec<QueryInstantSample> = Vec::with_capacity(RUNS * QUERIES_PER_RUN);
    let mut match_samples: Vec<QueryInstantSample> = Vec::with_capacity(RUNS * QUERIES_PER_RUN);
    let mut agg_run_means = Vec::with_capacity(RUNS);
    let mut match_run_means = Vec::with_capacity(RUNS);
    let mut agg_matched_count = None;
    let mut match_matched_count = None;

    for _run in 0..RUNS {
        let mut agg_run_wall = Vec::with_capacity(QUERIES_PER_RUN);
        let mut match_run_wall = Vec::with_capacity(QUERIES_PER_RUN);
        for _q in 0..QUERIES_PER_RUN {
            let sample = run_instant(&engine, tenant_hash, Q_AGG, t_ms, query_now_ns).await;
            agg_matched_count.get_or_insert(sample.matched);
            agg_run_wall.push(sample.wall_ns);
            agg_samples.push(sample);

            let sample = run_instant(&engine, tenant_hash, Q_MATCH, t_ms, query_now_ns).await;
            match_matched_count.get_or_insert(sample.matched);
            match_run_wall.push(sample.wall_ns);
            match_samples.push(sample);
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
    if agg_samples.iter().any(|s| s.agg_calls != 1) {
        failures.push("AGG_CALLS did not advance by exactly 1 per Q_AGG execution".to_string());
    }
    if agg_samples.iter().any(|s| s.match_calls != 0) {
        failures.push("MATCH_CALLS advanced on a Q_AGG execution (want 0)".to_string());
    }
    if match_samples.iter().any(|s| s.match_calls != 1) {
        failures.push("MATCH_CALLS did not advance by exactly 1 per Q_MATCH execution".to_string());
    }
    if match_samples.iter().any(|s| s.agg_calls != 0) {
        failures.push("AGG_CALLS advanced on a Q_MATCH execution (want 0)".to_string());
    }
    // Issue #2468: every phase timer must fire at least once per query.
    // `fetch_decode_wall`/`materialize`/`result_assembly`/`resolve` each wrap
    // exactly one call site per query attempt (no retry expected against
    // this bench's in-memory store), so they must fire exactly once;
    // `fetch`/`decode` fire once per segment/run and so are only checked for
    // "at least once".
    for (label, samples) in [("Q_AGG", &agg_samples), ("Q_MATCH", &match_samples)] {
        for s in samples.iter() {
            if s.resolve_calls != 1 {
                failures.push(format!(
                    "{label}: RESOLVE_CALLS was {} (want exactly 1)",
                    s.resolve_calls
                ));
            }
            if s.fetch_calls < 1 {
                failures.push(format!("{label}: FETCH_CALLS did not fire"));
            }
            if s.decode_calls < 1 {
                failures.push(format!("{label}: DECODE_CALLS did not fire"));
            }
            if s.fetch_decode_wall_calls != 1 {
                failures.push(format!(
                    "{label}: FETCH_DECODE_WALL_CALLS was {} (want exactly 1)",
                    s.fetch_decode_wall_calls
                ));
            }
            if s.materialize_calls != 1 {
                failures.push(format!(
                    "{label}: MATERIALIZE_CALLS was {} (want exactly 1)",
                    s.materialize_calls
                ));
            }
            if s.result_assembly_calls != 1 {
                failures.push(format!(
                    "{label}: RESULT_ASSEMBLY_CALLS was {} (want exactly 1)",
                    s.result_assembly_calls
                ));
            }
        }
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
    let agg_op_run_means: Vec<f64> = per_run_u64_means(&agg_samples, |s| s.agg_ns);
    let match_op_run_means: Vec<f64> = per_run_u64_means(&match_samples, |s| s.match_ns);
    let agg_share_run: Vec<f64> = share_of(&agg_op_run_means, &agg_run_means);
    let match_share_run: Vec<f64> = share_of(&match_op_run_means, &match_run_means);

    let agg_total = run_stats(&mut agg_run_means.clone());
    let match_total = run_stats(&mut match_run_means.clone());
    let agg_op = run_stats(&mut agg_op_run_means.clone());
    let match_op = run_stats(&mut match_op_run_means.clone());
    let agg_share = run_stats(&mut agg_share_run.clone());
    let match_share = run_stats(&mut match_share_run.clone());

    let agg_sort_total_ns: u64 = agg_samples.iter().map(|s| s.agg_sort_ns).sum();
    let agg_sort_mean_ns = mean(
        &agg_samples
            .iter()
            .map(|s| s.agg_sort_ns)
            .collect::<Vec<_>>(),
    );

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
        agg_samples.iter().map(|s| s.agg_sort_ns).collect::<Vec<_>>()
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
        "  Q_AGG   OPERATOR_SHARE % : min={:.3} median={:.3} max={:.3} -> {}",
        agg_share.min,
        agg_share.median,
        agg_share.max,
        band_verdict(agg_share.median, &BAND_OPERATOR_AGG)
    );
    println!(
        "  Q_MATCH OPERATOR_SHARE % : min={:.3} median={:.3} max={:.3} -> {}",
        match_share.min,
        match_share.median,
        match_share.max,
        band_verdict(match_share.median, &BAND_OPERATOR_MATCH)
    );

    // --- Issue #2468 stage 0: per-phase report ---
    println!("promql_phase_share report (issue #2468)");

    println!("Q_AGG phases:");
    print_phase(
        "catalog_resolve",
        &phase_report(&agg_samples, &agg_run_means, |s| s.resolve_ns, &BAND_RESOLVE),
    );
    print_phase(
        "object_fetch (summed)",
        &phase_report(&agg_samples, &agg_run_means, |s| s.fetch_ns, &BAND_FETCH),
    );
    print_phase(
        "segment_decode (summed)",
        &phase_report(
            &agg_samples,
            &agg_run_means,
            |s| s.decode_ns,
            &BAND_DECODE_AGG,
        ),
    );
    let agg_fdw = phase_report(
        &agg_samples,
        &agg_run_means,
        |s| s.fetch_decode_wall_ns,
        &BAND_FETCH, // informational only; not a pre-registered phase on its own
    );
    print_phase("fetch+decode (wall span, informational)", &agg_fdw);
    print_phase(
        "series_materialisation",
        &phase_report(
            &agg_samples,
            &agg_run_means,
            |s| s.materialize_ns,
            &BAND_MATERIALIZE,
        ),
    );
    print_phase(
        "result_assembly",
        &phase_report(
            &agg_samples,
            &agg_run_means,
            |s| s.result_assembly_ns,
            &BAND_ASSEMBLY,
        ),
    );
    print_phase(
        "unattributed_remainder",
        &phase_report(
            &agg_samples,
            &agg_run_means,
            |s| {
                s.wall_ns.saturating_sub(
                    s.resolve_ns + s.fetch_decode_wall_ns + s.materialize_ns + s.agg_ns + s.result_assembly_ns,
                )
            },
            &BAND_REMAINDER,
        ),
    );
    let agg_fetch_requests = run_stats(&mut per_run_u64_means(&agg_samples, |s| s.fetch_requests));
    let agg_fetch_bytes = run_stats(&mut per_run_u64_means(&agg_samples, |s| s.fetch_bytes));
    let agg_segments = run_stats(&mut per_run_u64_means(&agg_samples, |s| s.segments_fetched));
    println!(
        "  object_fetch requests/bytes      : requests median={:.1} bytes median={:.0} | segments_fetched median={:.1}",
        agg_fetch_requests.median, agg_fetch_bytes.median, agg_segments.median
    );

    println!("Q_MATCH phases:");
    print_phase(
        "catalog_resolve",
        &phase_report(
            &match_samples,
            &match_run_means,
            |s| s.resolve_ns,
            &BAND_RESOLVE,
        ),
    );
    print_phase(
        "object_fetch (summed)",
        &phase_report(&match_samples, &match_run_means, |s| s.fetch_ns, &BAND_FETCH),
    );
    print_phase(
        "segment_decode (summed)",
        &phase_report(
            &match_samples,
            &match_run_means,
            |s| s.decode_ns,
            &BAND_DECODE_MATCH,
        ),
    );
    let match_fdw = phase_report(
        &match_samples,
        &match_run_means,
        |s| s.fetch_decode_wall_ns,
        &BAND_FETCH,
    );
    print_phase("fetch+decode (wall span, informational)", &match_fdw);
    print_phase(
        "series_materialisation",
        &phase_report(
            &match_samples,
            &match_run_means,
            |s| s.materialize_ns,
            &BAND_MATERIALIZE,
        ),
    );
    print_phase(
        "result_assembly",
        &phase_report(
            &match_samples,
            &match_run_means,
            |s| s.result_assembly_ns,
            &BAND_ASSEMBLY,
        ),
    );
    print_phase(
        "unattributed_remainder",
        &phase_report(
            &match_samples,
            &match_run_means,
            |s| {
                s.wall_ns.saturating_sub(
                    s.resolve_ns
                        + s.fetch_decode_wall_ns
                        + s.materialize_ns
                        + s.match_ns
                        + s.result_assembly_ns,
                )
            },
            &BAND_REMAINDER,
        ),
    );
    let match_fetch_requests =
        run_stats(&mut per_run_u64_means(&match_samples, |s| s.fetch_requests));
    let match_fetch_bytes = run_stats(&mut per_run_u64_means(&match_samples, |s| s.fetch_bytes));
    let match_segments = run_stats(&mut per_run_u64_means(&match_samples, |s| s.segments_fetched));
    println!(
        "  object_fetch requests/bytes      : requests median={:.1} bytes median={:.0} | segments_fetched median={:.1}",
        match_fetch_requests.median, match_fetch_bytes.median, match_segments.median
    );
}
