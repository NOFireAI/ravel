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

/// Issue #2479 stage 0b: pre-registered bands for Q_AGG's **fan-out**
/// (per-segment future) unattributed time share, i.e. share of the sum of
/// `FUTURE_NS` -- not share of wall, which the stage 0 bands above use.
/// `BAND_MATCHER_EVAL` has no step to check it against (see the report's
/// `matcher_evaluation` line) and is kept only so the pre-registration is
/// visible in one place.
const BAND_LABEL_CLONE: Band = Band { lo: 40.0, hi: 70.0 };
const BAND_MAP_SET_INSERTS: Band = Band { lo: 10.0, hi: 30.0 };
#[allow(dead_code)]
const BAND_MATCHER_EVAL: Band = Band { lo: 5.0, hi: 20.0 };
const BAND_SAMPLE_ASSEMBLY_FUTURE: Band = Band { lo: 0.0, hi: 15.0 };
const BAND_LIMITER_PLAN_FUTURE: Band = Band { lo: 0.0, hi: 5.0 };
const BAND_FUTURE_REMAINDER: Band = Band { lo: 0.0, hi: 5.0 };

/// Labels per series `build_series` emits (`__name__`, the two group labels,
/// `dc_zone`, `host_pool`, `svc_tier`, `series_key`); used to turn a
/// label-SET clone count into a label-STRING allocation count for the
/// multiplier report.
const LABELS_PER_SERIES: u64 = 7;

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
    // --- Issue #2479 stage 0b: per-segment future tiling and the two gaps
    // outside it (see `phase_timers.rs`'s stage 0b doc comments).
    future_ns: u64,
    future_calls: u64,
    limiter_wait_ns: u64,
    limiter_wait_calls: u64,
    footer_parse_ns: u64,
    footer_parse_calls: u64,
    catalog_retain_ns: u64,
    catalog_retain_calls: u64,
    page_plan_ns: u64,
    page_plan_calls: u64,
    run_plan_lookup_ns: u64,
    run_plan_lookup_calls: u64,
    label_clone_ns: u64,
    label_clone_calls: u64,
    sample_assembly_ns: u64,
    sample_assembly_calls: u64,
    pre_fanout_ns: u64,
    pre_fanout_calls: u64,
    post_fanout_ns: u64,
    post_fanout_calls: u64,
    /// Found during remainder investigation (issue #2479): the
    /// `scalar`/`histogram` filter-collect pair in
    /// `fetch_runs_and_histograms`, between `decode_selected` returning and
    /// `fetch_pages` starting.
    selected_split_ns: u64,
    selected_split_calls: u64,
    /// Found during remainder investigation (issue #2479): the
    /// `into_soa`/`into_fetched` conversion pair in
    /// `fetch_soa_and_histograms_phase_accounted`, after
    /// `fetch_runs_and_histograms` returns but still inside `FUTURE_NS`.
    soa_convert_ns: u64,
    soa_convert_calls: u64,
    /// Engine-cumulative `SegmentFetcher::label_sets_materialized` diff
    /// across this one query (catalog-decode-time materialisation, distinct
    /// from `label_clone_calls`'s later per-run clone -- see the multiplier
    /// report).
    label_sets_materialized: u64,
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
    let future_ns_before = phase_timers::FUTURE_NS.load(Ordering::Relaxed);
    let future_calls_before = phase_timers::FUTURE_CALLS.load(Ordering::Relaxed);
    let limiter_wait_ns_before = phase_timers::LIMITER_WAIT_NS.load(Ordering::Relaxed);
    let limiter_wait_calls_before = phase_timers::LIMITER_WAIT_CALLS.load(Ordering::Relaxed);
    let footer_parse_ns_before = phase_timers::FOOTER_PARSE_NS.load(Ordering::Relaxed);
    let footer_parse_calls_before = phase_timers::FOOTER_PARSE_CALLS.load(Ordering::Relaxed);
    let catalog_retain_ns_before = phase_timers::CATALOG_RETAIN_NS.load(Ordering::Relaxed);
    let catalog_retain_calls_before = phase_timers::CATALOG_RETAIN_CALLS.load(Ordering::Relaxed);
    let page_plan_ns_before = phase_timers::PAGE_PLAN_NS.load(Ordering::Relaxed);
    let page_plan_calls_before = phase_timers::PAGE_PLAN_CALLS.load(Ordering::Relaxed);
    let run_plan_lookup_ns_before = phase_timers::RUN_PLAN_LOOKUP_NS.load(Ordering::Relaxed);
    let run_plan_lookup_calls_before = phase_timers::RUN_PLAN_LOOKUP_CALLS.load(Ordering::Relaxed);
    let label_clone_ns_before = phase_timers::LABEL_CLONE_NS.load(Ordering::Relaxed);
    let label_clone_calls_before = phase_timers::LABEL_CLONE_CALLS.load(Ordering::Relaxed);
    let sample_assembly_ns_before = phase_timers::SAMPLE_ASSEMBLY_NS.load(Ordering::Relaxed);
    let sample_assembly_calls_before = phase_timers::SAMPLE_ASSEMBLY_CALLS.load(Ordering::Relaxed);
    let pre_fanout_ns_before = phase_timers::PRE_FANOUT_NS.load(Ordering::Relaxed);
    let pre_fanout_calls_before = phase_timers::PRE_FANOUT_CALLS.load(Ordering::Relaxed);
    let post_fanout_ns_before = phase_timers::POST_FANOUT_NS.load(Ordering::Relaxed);
    let post_fanout_calls_before = phase_timers::POST_FANOUT_CALLS.load(Ordering::Relaxed);
    let selected_split_ns_before = phase_timers::SELECTED_SPLIT_NS.load(Ordering::Relaxed);
    let selected_split_calls_before = phase_timers::SELECTED_SPLIT_CALLS.load(Ordering::Relaxed);
    let soa_convert_ns_before = phase_timers::SOA_CONVERT_NS.load(Ordering::Relaxed);
    let soa_convert_calls_before = phase_timers::SOA_CONVERT_CALLS.load(Ordering::Relaxed);
    let label_sets_materialized_before = engine.label_sets_materialized();

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
        future_ns: phase_timers::FUTURE_NS.load(Ordering::Relaxed) - future_ns_before,
        future_calls: phase_timers::FUTURE_CALLS.load(Ordering::Relaxed) - future_calls_before,
        limiter_wait_ns: phase_timers::LIMITER_WAIT_NS.load(Ordering::Relaxed)
            - limiter_wait_ns_before,
        limiter_wait_calls: phase_timers::LIMITER_WAIT_CALLS.load(Ordering::Relaxed)
            - limiter_wait_calls_before,
        footer_parse_ns: phase_timers::FOOTER_PARSE_NS.load(Ordering::Relaxed)
            - footer_parse_ns_before,
        footer_parse_calls: phase_timers::FOOTER_PARSE_CALLS.load(Ordering::Relaxed)
            - footer_parse_calls_before,
        catalog_retain_ns: phase_timers::CATALOG_RETAIN_NS.load(Ordering::Relaxed)
            - catalog_retain_ns_before,
        catalog_retain_calls: phase_timers::CATALOG_RETAIN_CALLS.load(Ordering::Relaxed)
            - catalog_retain_calls_before,
        page_plan_ns: phase_timers::PAGE_PLAN_NS.load(Ordering::Relaxed) - page_plan_ns_before,
        page_plan_calls: phase_timers::PAGE_PLAN_CALLS.load(Ordering::Relaxed)
            - page_plan_calls_before,
        run_plan_lookup_ns: phase_timers::RUN_PLAN_LOOKUP_NS.load(Ordering::Relaxed)
            - run_plan_lookup_ns_before,
        run_plan_lookup_calls: phase_timers::RUN_PLAN_LOOKUP_CALLS.load(Ordering::Relaxed)
            - run_plan_lookup_calls_before,
        label_clone_ns: phase_timers::LABEL_CLONE_NS.load(Ordering::Relaxed)
            - label_clone_ns_before,
        label_clone_calls: phase_timers::LABEL_CLONE_CALLS.load(Ordering::Relaxed)
            - label_clone_calls_before,
        sample_assembly_ns: phase_timers::SAMPLE_ASSEMBLY_NS.load(Ordering::Relaxed)
            - sample_assembly_ns_before,
        sample_assembly_calls: phase_timers::SAMPLE_ASSEMBLY_CALLS.load(Ordering::Relaxed)
            - sample_assembly_calls_before,
        pre_fanout_ns: phase_timers::PRE_FANOUT_NS.load(Ordering::Relaxed) - pre_fanout_ns_before,
        pre_fanout_calls: phase_timers::PRE_FANOUT_CALLS.load(Ordering::Relaxed)
            - pre_fanout_calls_before,
        post_fanout_ns: phase_timers::POST_FANOUT_NS.load(Ordering::Relaxed)
            - post_fanout_ns_before,
        post_fanout_calls: phase_timers::POST_FANOUT_CALLS.load(Ordering::Relaxed)
            - post_fanout_calls_before,
        selected_split_ns: phase_timers::SELECTED_SPLIT_NS.load(Ordering::Relaxed)
            - selected_split_ns_before,
        selected_split_calls: phase_timers::SELECTED_SPLIT_CALLS.load(Ordering::Relaxed)
            - selected_split_calls_before,
        soa_convert_ns: phase_timers::SOA_CONVERT_NS.load(Ordering::Relaxed) - soa_convert_ns_before,
        soa_convert_calls: phase_timers::SOA_CONVERT_CALLS.load(Ordering::Relaxed)
            - soa_convert_calls_before,
        label_sets_materialized: engine.label_sets_materialized() - label_sets_materialized_before,
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

/// Same time/share computation as [`phase_report`], for a step with no
/// pre-registered band (issue #2479 stage 0b's `footer_parse`/
/// `catalog_retain`, and the `pre_fanout`/`post_fanout` gaps): always reports
/// "not pre-registered" rather than a band verdict.
fn phase_report_unregistered(
    samples: &[QueryInstantSample],
    denom_run_means: &[f64],
    f: impl Fn(&QueryInstantSample) -> u64,
) -> PhaseReport {
    let field_means = per_run_u64_means(samples, f);
    let share_run = share_of(&field_means, denom_run_means);
    let time = run_stats(&mut field_means.clone());
    let share = run_stats(&mut share_run.clone());
    PhaseReport {
        time,
        share,
        verdict: "not pre-registered",
    }
}

fn print_phase(label: &str, r: &PhaseReport) {
    println!(
        "  {label:<34}: time_ns min={:.0} median={:.0} max={:.0} | share% min={:.3} median={:.3} max={:.3} -> {}",
        r.time.min, r.time.median, r.time.max, r.share.min, r.share.median, r.share.max, r.verdict
    );
}

/// Like [`phase_report`]/[`phase_report_unregistered`], picking the banded
/// form when `band` is `Some` (issue #2479 stage 0b: only the steps the task
/// pre-registered a Q_AGG fan-out-share band for pass one in).
fn fanout_step(
    samples: &[QueryInstantSample],
    denom_run_means: &[f64],
    f: impl Fn(&QueryInstantSample) -> u64,
    band: Option<&Band>,
) -> PhaseReport {
    match band {
        Some(b) => phase_report(samples, denom_run_means, f, b),
        None => phase_report_unregistered(samples, denom_run_means, f),
    }
}

/// Issue #2479 stage 0b report: per-segment future tiling, the two gaps
/// outside the fan-out, and the multiplier counts (deliverable 4). `bands`
/// applies the task's pre-registered Q_AGG fan-out-share bands when true
/// (Q_MATCH has none, so every step there prints "not pre-registered").
/// `ground_truth_distinct` is the compile-time-known count of distinct
/// series this query's selector(s) touch before any per-segment
/// multiplication, used to compute the "built k times or once" multiplier.
fn print_stage0b_fanout(
    query_label: &str,
    samples: &[QueryInstantSample],
    wall_run_means: &[f64],
    apply_bands: bool,
    ground_truth_distinct: u64,
) {
    println!("{query_label} stage 0b fan-out (issue #2479):");

    let future_means = per_run_u64_means(samples, |s| s.future_ns);
    let future_share_wall = share_of(&future_means, wall_run_means);
    let future_time = run_stats(&mut future_means.clone());
    let future_share = run_stats(&mut future_share_wall.clone());
    let fdw_means = per_run_u64_means(samples, |s| s.fetch_decode_wall_ns);
    let fdw_time = run_stats(&mut fdw_means.clone());
    println!(
        "  future_ns (sum of per-segment futures)   : min={:.0} median={:.0} max={:.0} ns | share-of-wall% median={:.2} (informational: futures run concurrently inside fetch_decode_wall, so > 100% of it is expected, not a bug)",
        future_time.min, future_time.median, future_time.max, future_share.median
    );
    println!(
        "  fetch_decode_wall_ns (fan-out wall span) : median={:.0} ns | concurrency_factor = future_sum/wall = {:.2}",
        fdw_time.median,
        future_time.median / fdw_time.median.max(1.0)
    );

    let limiter_plan = fanout_step(
        samples,
        &future_means,
        |s| s.limiter_wait_ns + s.page_plan_ns,
        apply_bands.then_some(&BAND_LIMITER_PLAN_FUTURE),
    );
    print_phase("limiter_wait+page_plan (share of future)", &limiter_plan);
    print_phase(
        "  limiter_wait alone (share of future)",
        &fanout_step(samples, &future_means, |s| s.limiter_wait_ns, None),
    );
    print_phase(
        "  page_plan alone (share of future)",
        &fanout_step(samples, &future_means, |s| s.page_plan_ns, None),
    );

    println!(
        "  matcher_evaluation (share of future)     : not separately instrumentable from ravel-query -- executes inside ravel_segment::decode_catalog_matching_v4 (out of this crate's scope), counted inside DECODE_NS alongside page decode; pre-registered band {:.0}-{:.0}% cannot be checked here",
        BAND_MATCHER_EVAL.lo, BAND_MATCHER_EVAL.hi
    );

    print_phase(
        "run_plan_lookup (map/set-insert analog, share of future)",
        &fanout_step(
            samples,
            &future_means,
            |s| s.run_plan_lookup_ns,
            apply_bands.then_some(&BAND_MAP_SET_INSERTS),
        ),
    );
    print_phase(
        "label_clone (label-set construction, share of future)",
        &fanout_step(
            samples,
            &future_means,
            |s| s.label_clone_ns,
            apply_bands.then_some(&BAND_LABEL_CLONE),
        ),
    );
    print_phase(
        "sample_assembly (share of future)",
        &fanout_step(
            samples,
            &future_means,
            |s| s.sample_assembly_ns,
            apply_bands.then_some(&BAND_SAMPLE_ASSEMBLY_FUTURE),
        ),
    );
    print_phase(
        "footer_parse (share of future, not pre-registered)",
        &fanout_step(samples, &future_means, |s| s.footer_parse_ns, None),
    );
    print_phase(
        "catalog_retain (share of future, not pre-registered)",
        &fanout_step(samples, &future_means, |s| s.catalog_retain_ns, None),
    );
    print_phase(
        "selected_split (share of future, not pre-registered; found during remainder investigation)",
        &fanout_step(samples, &future_means, |s| s.selected_split_ns, None),
    );
    print_phase(
        "soa_convert (share of future, not pre-registered; found during remainder investigation)",
        &fanout_step(samples, &future_means, |s| s.soa_convert_ns, None),
    );
    print_phase(
        "remainder_inside_futures (share of future)",
        &fanout_step(
            samples,
            &future_means,
            |s| {
                let steps = s.limiter_wait_ns
                    + s.footer_parse_ns
                    + s.catalog_retain_ns
                    + s.page_plan_ns
                    + s.run_plan_lookup_ns
                    + s.label_clone_ns
                    + s.sample_assembly_ns
                    + s.selected_split_ns
                    + s.soa_convert_ns
                    + s.fetch_ns
                    + s.decode_ns;
                s.future_ns.saturating_sub(steps)
            },
            apply_bands.then_some(&BAND_FUTURE_REMAINDER),
        ),
    );

    print_phase(
        "pre_fanout_gap (share of wall, not pre-registered)",
        &phase_report_unregistered(samples, wall_run_means, |s| s.pre_fanout_ns),
    );
    print_phase(
        "post_fanout_gap (share of wall, not pre-registered)",
        &phase_report_unregistered(samples, wall_run_means, |s| s.post_fanout_ns),
    );
    print_phase(
        "remainder_outside_stage0b (share of wall; tightens stage 0's own remainder by also subtracting pre/post_fanout)",
        &phase_report_unregistered(samples, wall_run_means, |s| {
            s.wall_ns.saturating_sub(
                s.resolve_ns
                    + s.fetch_decode_wall_ns
                    + s.materialize_ns
                    + s.result_assembly_ns
                    + s.pre_fanout_ns
                    + s.post_fanout_ns,
            )
        }),
    );

    // --- Multiplier counts (deliverable 4) ---
    let n = samples.len() as f64;
    let label_clone_calls_total: u64 = samples.iter().map(|s| s.label_clone_calls).sum();
    let run_plan_lookup_calls_total: u64 = samples.iter().map(|s| s.run_plan_lookup_calls).sum();
    let label_sets_materialized_total: u64 =
        samples.iter().map(|s| s.label_sets_materialized).sum();
    let decode_calls_total: u64 = samples.iter().map(|s| s.decode_calls).sum();
    let label_clone_calls_per_query = label_clone_calls_total as f64 / n;
    let multiplier = label_clone_calls_per_query / ground_truth_distinct as f64;
    println!("  {query_label} multiplier counts (issue #2479 deliverable 4):");
    println!(
        "    series-run materialisations (label_clone_calls), per-query mean : {label_clone_calls_per_query:.1} (= sum over fetched segments of series matched in that segment)"
    );
    println!(
        "    run_plan_lookup_calls, per-query mean                            : {:.1} (want == label_clone_calls; cross-checked by assertion above)",
        run_plan_lookup_calls_total as f64 / n
    );
    println!(
        "    label_sets_materialized (engine-cumulative, catalog-decode path), per-query mean: {:.1}",
        label_sets_materialized_total as f64 / n
    );
    println!("    ground-truth distinct series this query's selector(s) touch     : {ground_truth_distinct}");
    println!(
        "    multiplier = label_clone_calls / ground_truth_distinct           : {multiplier:.3} -> {}",
        if multiplier > 1.05 {
            "ABOVE 1x: a series is materialised MORE THAN ONCE per query (once per segment it is matched in, not once per query) -- see stage0b-promql-fanout.md"
        } else if multiplier < 0.95 {
            "BELOW 1x: fewer label-set clones than ground-truth distinct series; unexpected, investigate"
        } else {
            "approximately 1x: each series materialised once per query (no per-segment multiplier observed for this query/shard layout)"
        }
    );
    println!(
        "    label strings allocated (label_clone_calls * {LABELS_PER_SERIES} labels/series), per-query mean: {:.0}",
        label_clone_calls_per_query * LABELS_PER_SERIES as f64
    );
    println!(
        "    samples decoded: NOT separately instrumented (no per-sample counter exists in ravel-query); decode_calls per-query mean={:.1} is a per-run-call proxy (one call per series-run, not per sample within it) -- documented deviation, see stage0b-promql-fanout.md",
        decode_calls_total as f64 / n
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
    // Issue #2479 stage 0b: every new per-segment-future step timer must
    // fire at least once per query, and `FUTURE_CALLS`/`CATALOG_RETAIN_CALLS`
    // exactly once per fetched segment (one per-segment future, one
    // `decode_selected` call, each), and `PRE_FANOUT_CALLS`/
    // `POST_FANOUT_CALLS` exactly twice per query (two disjoint regions per
    // attempt, no retry expected against this bench's in-memory store).
    // `run_plan_lookup`/`label_clone`/`sample_assembly` all fire once per
    // (series, run) pair in the same per-series loop, so their call counts
    // must agree with each other exactly.
    // Q_MATCH (`promql_op_share_a + promql_op_share_b`) has two distinct
    // matcher sets (one per metric), so the OUTER per-distinct-matcher-set
    // fan-out in `prefetch_metric_plans` runs the entire INNER per-segment
    // fan-out once per matcher set: `FUTURE_CALLS`/`CATALOG_RETAIN_CALLS`
    // legitimately equal `segments_fetched * matcher_set_count`, not
    // `segments_fetched` alone. `stats.segments_fetched` itself reports the
    // distinct segment count, not multiplied by matcher-set passes. Q_AGG
    // has one matcher set (single metric), so its multiplier is 1 and this
    // reduces to the original exact-equality check.
    let matcher_set_count = |label: &str| -> u64 {
        match label {
            "Q_AGG" => 1,
            "Q_MATCH" => 2,
            other => panic!("unknown query label {other}"),
        }
    };
    for (label, samples) in [("Q_AGG", &agg_samples), ("Q_MATCH", &match_samples)] {
        let want_multiplier = matcher_set_count(label);
        for s in samples.iter() {
            let want_future_calls = s.segments_fetched * want_multiplier;
            if s.future_calls != want_future_calls {
                failures.push(format!(
                    "{label}: FUTURE_CALLS was {} (want exactly segments_fetched={} * matcher_set_count={want_multiplier} = {want_future_calls})",
                    s.future_calls, s.segments_fetched
                ));
            }
            if s.catalog_retain_calls != want_future_calls {
                failures.push(format!(
                    "{label}: CATALOG_RETAIN_CALLS was {} (want exactly segments_fetched={} * matcher_set_count={want_multiplier} = {want_future_calls})",
                    s.catalog_retain_calls, s.segments_fetched
                ));
            }
            if s.selected_split_calls < 1 {
                failures.push(format!("{label}: SELECTED_SPLIT_CALLS did not fire"));
            }
            if s.soa_convert_calls < 1 {
                failures.push(format!("{label}: SOA_CONVERT_CALLS did not fire"));
            }
            if s.limiter_wait_calls < 1 {
                failures.push(format!("{label}: LIMITER_WAIT_CALLS did not fire"));
            }
            if s.footer_parse_calls < 1 {
                failures.push(format!("{label}: FOOTER_PARSE_CALLS did not fire"));
            }
            if s.page_plan_calls < 1 {
                failures.push(format!("{label}: PAGE_PLAN_CALLS did not fire"));
            }
            if s.run_plan_lookup_calls < 1 {
                failures.push(format!("{label}: RUN_PLAN_LOOKUP_CALLS did not fire"));
            }
            if s.label_clone_calls < 1 {
                failures.push(format!("{label}: LABEL_CLONE_CALLS did not fire"));
            }
            if s.sample_assembly_calls < 1 {
                failures.push(format!("{label}: SAMPLE_ASSEMBLY_CALLS did not fire"));
            }
            if s.run_plan_lookup_calls != s.label_clone_calls
                || s.label_clone_calls != s.sample_assembly_calls
            {
                failures.push(format!(
                    "{label}: per-series-run call counts disagree: run_plan_lookup={} label_clone={} sample_assembly={} (want all equal)",
                    s.run_plan_lookup_calls, s.label_clone_calls, s.sample_assembly_calls
                ));
            }
            if s.pre_fanout_calls != 2 {
                failures.push(format!(
                    "{label}: PRE_FANOUT_CALLS was {} (want exactly 2)",
                    s.pre_fanout_calls
                ));
            }
            if s.post_fanout_calls != 2 {
                failures.push(format!(
                    "{label}: POST_FANOUT_CALLS was {} (want exactly 2)",
                    s.post_fanout_calls
                ));
            }
            // Inside the per-segment futures, the tiled steps
            // (limiter_wait, footer_parse, catalog_retain, page_plan,
            // run_plan_lookup, label_clone, sample_assembly, plus the
            // existing fetch/decode) must sum to within 5% of FUTURE_NS's
            // own sum; a wider gap means a statement inside the future is
            // not yet attributed to any step.
            let steps_sum = s.limiter_wait_ns
                + s.footer_parse_ns
                + s.catalog_retain_ns
                + s.page_plan_ns
                + s.run_plan_lookup_ns
                + s.label_clone_ns
                + s.sample_assembly_ns
                + s.selected_split_ns
                + s.soa_convert_ns
                + s.fetch_ns
                + s.decode_ns;
            // Deliverable 6's gate is explicitly an OR: steps_sum within 5%
            // of FUTURE_NS, OR the bin prints the remainder row and the
            // report names which statements could not be placed. A
            // steps_sum that OVERSHOOTS FUTURE_NS is a different, always-real
            // bug (double-counted time) and stays a hard failure; a
            // steps_sum that falls short takes the documented-remainder
            // branch instead of a hard failure, printed here so it is
            // visible even on a run that fails for an unrelated reason.
            if s.future_ns > 0 {
                let remainder = s.future_ns.abs_diff(steps_sum.min(s.future_ns));
                let remainder_pct = 100.0 * remainder as f64 / s.future_ns as f64;
                if steps_sum > s.future_ns && remainder_pct > 5.0 {
                    failures.push(format!(
                        "{label}: in-future steps_sum={steps_sum}ns exceeds FUTURE_NS={}ns by {remainder_pct:.1}% (want <=5%)",
                        s.future_ns
                    ));
                } else if remainder_pct > 5.0 {
                    eprintln!(
                        "promql_operator_share: {label}: in-future unattributed remainder is {remainder_pct:.1}% of FUTURE_NS={}ns (steps_sum={steps_sum}ns, over 5%); documented in stage0b-promql-fanout.md per deliverable 6's OR-clause, not treated as a hard failure",
                        s.future_ns
                    );
                }
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

    // --- Issue #2479 stage 0b: per-segment fan-out report ---
    println!("promql_fanout_share report (issue #2479 stage 0b)");
    print_stage0b_fanout(
        "Q_AGG",
        &agg_samples,
        &agg_run_means,
        true,
        SERIES_PER_METRIC as u64,
    );
    print_stage0b_fanout(
        "Q_MATCH",
        &match_samples,
        &match_run_means,
        false,
        (SERIES_PER_METRIC * 2) as u64,
    );
}
