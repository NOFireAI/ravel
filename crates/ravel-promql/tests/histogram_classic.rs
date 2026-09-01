//! Integration coverage for `histogram_quantile`/`histogram_fraction`
//! through the public `Evaluator` API: parsing and registry dispatch,
//! grouping by labels-minus-`le` end to end, nested composition with
//! another function's output (the plan's "rate-of-buckets" shape), and
//! range-query top-level evaluation (the canonical Grafana p99 shape), which
//! routes through `eval_instant_over_grid` since neither function fits
//! `eval_range`'s matrix-reducing shape. `histogram_classic.rs`'s own unit
//! tests already pin the bucket math itself; these tests exercise the wiring
//! around it.
#![allow(clippy::expect_used)]

use ravel_promql::{Evaluator, testsource::TestSource};

fn two_jobs_bucket_source() -> TestSource {
    TestSource::new()
        .with_series(
            &[
                ("__name__", "http_request_duration_seconds_bucket"),
                ("job", "a"),
                ("le", "0.1"),
            ],
            // A third point at 30s guarantees two samples strictly inside
            // the left-open `[1m]` window ending at 60s (0s itself sits
            // exactly on the excluded left boundary).
            &[(0, 10.0), (30_000_000_000, 15.0), (60_000_000_000, 20.0)],
        )
        .expect("valid series")
        .with_series(
            &[
                ("__name__", "http_request_duration_seconds_bucket"),
                ("job", "a"),
                ("le", "1"),
            ],
            &[(0, 30.0), (30_000_000_000, 40.0), (60_000_000_000, 50.0)],
        )
        .expect("valid series")
        .with_series(
            &[
                ("__name__", "http_request_duration_seconds_bucket"),
                ("job", "a"),
                ("le", "+Inf"),
            ],
            &[(0, 40.0), (30_000_000_000, 60.0), (60_000_000_000, 80.0)],
        )
        .expect("valid series")
        .with_series(
            &[
                ("__name__", "http_request_duration_seconds_bucket"),
                ("job", "b"),
                ("le", "0.1"),
            ],
            &[(0, 5.0), (60_000_000_000, 5.0)],
        )
        .expect("valid series")
        .with_series(
            &[
                ("__name__", "http_request_duration_seconds_bucket"),
                ("job", "b"),
                ("le", "+Inf"),
            ],
            &[(0, 5.0), (60_000_000_000, 5.0)],
        )
        .expect("valid series")
}

#[test]
fn instant_histogram_quantile_dispatches_and_groups_by_job() {
    let source = two_jobs_bucket_source();
    let mut result = Evaluator::new()
        .instant(
            &source,
            "histogram_quantile(0.9, http_request_duration_seconds_bucket)",
            0,
        )
        .expect("evaluates");
    result.sort_by(|a, b| a.labels.get("job").cmp(&b.labels.get("job")));
    assert_eq!(result.len(), 2);
    assert_eq!(result[0].labels.get("job"), Some("a"));
    assert_eq!(
        result[0].labels.get("__name__"),
        None,
        "metric name dropped"
    );
    assert_eq!(result[1].labels.get("job"), Some("b"));
}

#[test]
fn instant_histogram_fraction_dispatches_and_groups_by_job() {
    let source = two_jobs_bucket_source();
    let result = Evaluator::new()
        .instant(
            &source,
            "histogram_fraction(0, 0.1, http_request_duration_seconds_bucket{job=\"a\"})",
            0,
        )
        .expect("evaluates");
    assert_eq!(result.len(), 1);
    // 10 of 40 total observations are at or below 0.1.
    assert_eq!(result[0].value, 0.25);
}

/// The plan's canonical composition: `histogram_quantile` over the
/// per-second bucket rate rather than the raw cumulative counters. Only
/// reachable through `eval_expr`'s general recursion (`vector_arg` calling
/// back into it), not through `eval_range_call`'s matrix-reducing shape.
#[test]
fn histogram_quantile_composes_with_rate_of_buckets() {
    let source = two_jobs_bucket_source();
    let result = Evaluator::new()
        .instant(
            &source,
            "histogram_quantile(0.9, rate(http_request_duration_seconds_bucket{job=\"a\"}[1m]))",
            60_000,
        )
        .expect("evaluates");
    assert_eq!(result.len(), 1);
    // rate() scales every bucket by the same 1/60s factor, which cancels
    // out of the quantile's ratios entirely: the result equals the
    // instant(0.9, raw counters) quantile at the same instant.
    let raw = Evaluator::new()
        .instant(
            &source,
            "histogram_quantile(0.9, http_request_duration_seconds_bucket{job=\"a\"})",
            60_000,
        )
        .expect("evaluates");
    assert_eq!(result[0].value, raw[0].value);
}

/// The canonical Grafana p99 shape: `histogram_quantile` at a range-query
/// top level, no wrapping arithmetic. It now routes through
/// `eval_instant_over_grid` (the same generalization `VectorMap`/`Instant`
/// use), evaluating the whole call fresh at each grid step and stitching the
/// per-step instant vectors into one matrix. The per-step values match the
/// instant evaluation at that step's timestamp.
#[test]
fn histogram_quantile_evaluates_in_the_range_call_path() {
    let source = two_jobs_bucket_source();
    let query = "histogram_quantile(0.9, http_request_duration_seconds_bucket)";
    let mut matrix = Evaluator::new()
        .range(&source, query, 0, 60_000, 60_000)
        .expect("evaluates at the range-query top level");
    matrix.sort_by(|a, b| a.0.get("job").cmp(&b.0.get("job")));
    // Two grid steps (0s, 60s) over two jobs (a, b).
    assert_eq!(matrix.len(), 2);
    assert_eq!(matrix[0].0.get("job"), Some("a"));
    assert_eq!(matrix[0].0.get("__name__"), None, "metric name dropped");
    assert_eq!(matrix[1].0.get("job"), Some("b"));
    for (labels, samples) in &matrix {
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].ts_ns, 0);
        assert_eq!(samples[1].ts_ns, 60_000_000_000);
        for step_ms in [0_i64, 60_000] {
            let instant = Evaluator::new()
                .instant(&source, query, step_ms)
                .expect("evaluates");
            let expected = instant
                .iter()
                .find(|s| s.labels.get("job") == labels.get("job"))
                .expect("job present at this step");
            let step_ns = step_ms * 1_000_000;
            let got = samples
                .iter()
                .find(|s| s.ts_ns == step_ns)
                .expect("sample at this step");
            assert_eq!(got.value.to_bits(), expected.value.to_bits());
        }
    }
}

#[test]
fn histogram_fraction_evaluates_in_the_range_call_path() {
    let source = two_jobs_bucket_source();
    let query = "histogram_fraction(0, 0.1, http_request_duration_seconds_bucket{job=\"a\"})";
    let matrix = Evaluator::new()
        .range(&source, query, 0, 60_000, 60_000)
        .expect("evaluates at the range-query top level");
    assert_eq!(matrix.len(), 1);
    let (labels, samples) = &matrix[0];
    assert_eq!(labels.get("job"), Some("a"));
    assert_eq!(samples.len(), 2);
    // 10 of 40 total observations are at or below 0.1 at t=0s.
    assert_eq!(samples[0].ts_ns, 0);
    assert_eq!(samples[0].value.to_bits(), 0.25_f64.to_bits());
    // At t=60s: 20 of 80 total observations are at or below 0.1.
    assert_eq!(samples[1].ts_ns, 60_000_000_000);
    assert_eq!(samples[1].value.to_bits(), 0.25_f64.to_bits());
}
