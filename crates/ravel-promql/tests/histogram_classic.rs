//! Integration coverage for `histogram_quantile`/`histogram_fraction`
//! through the public `Evaluator` API: parsing and registry dispatch,
//! grouping by labels-minus-`le` end to end, nested composition with
//! another function's output (the plan's "rate-of-buckets" shape), and the
//! typed error a range-query top-level call gets since neither function
//! fits `eval_range`'s matrix-reducing shape. `histogram_classic.rs`'s own
//! unit tests already pin the bucket math itself; these tests exercise the
//! wiring around it.
#![allow(clippy::expect_used)]

use ravel_promql::{Error, Evaluator, testsource::TestSource};

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

#[test]
fn histogram_quantile_is_rejected_in_the_range_call_path() {
    let source = two_jobs_bucket_source();
    let err = Evaluator::new()
        .range(
            &source,
            "histogram_quantile(0.9, http_request_duration_seconds_bucket)",
            0,
            60_000,
            60_000,
        )
        .expect_err("not supported at the range-query top level");
    assert!(matches!(err, Error::Unsupported { .. }));
    assert!(err.to_string().contains("histogram_quantile"));
}

#[test]
fn histogram_fraction_is_rejected_in_the_range_call_path() {
    let source = two_jobs_bucket_source();
    let err = Evaluator::new()
        .range(
            &source,
            "histogram_fraction(0, 1, http_request_duration_seconds_bucket)",
            0,
            60_000,
            60_000,
        )
        .expect_err("not supported at the range-query top level");
    assert!(matches!(err, Error::Unsupported { .. }));
    assert!(err.to_string().contains("histogram_fraction"));
}
