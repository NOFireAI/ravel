//! Integration coverage for the P5 `_over_time` family's dispatch wiring
//! through the public `Evaluator` API: instant vs. range call dispatch for
//! the plain `RangeVector` shape, `quantile_over_time`'s reversed
//! (scalar-first) argument order (`FunctionKind::ScalarRangeVector`), and
//! `absent_over_time`'s special whole-matrix-emptiness handling in both the
//! instant and range call paths, including its selector-derived label
//! synthesis. `over_time.rs`'s own unit tests already pin every function's
//! math bit-exactly against hand-derived Prometheus formulas; these tests
//! instead exercise the wiring around that math.
#![allow(clippy::expect_used)]

use ravel_promql::{Evaluator, testsource::TestSource};

fn three_sample_gauge() -> TestSource {
    TestSource::new()
        .with_series(
            &[("__name__", "temp"), ("job", "api")],
            &[(0, 1.0), (60_000_000_000, 2.0), (120_000_000_000, 3.0)],
        )
        .expect("valid series")
}

#[test]
fn instant_avg_over_time_dispatches_to_the_registered_function() {
    let source = three_sample_gauge();
    let result = Evaluator::new()
        .instant(&source, "avg_over_time(temp[5m])", 120_000)
        .expect("evaluates");
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].value, 2.0);
}

#[test]
fn range_query_sum_over_time_advances_the_cursor_across_steps() {
    let source = three_sample_gauge();
    let result = Evaluator::new()
        .range(&source, "sum_over_time(temp[2m])", 0, 120_000, 60_000)
        .expect("evaluates");
    assert_eq!(result.len(), 1);
    let (_, samples) = &result[0];
    let values: Vec<f64> = samples.iter().map(|s| s.value).collect();
    // t=0: window (-2m,0] -> [1.0] -> 1.0; t=60s: window (-1m,60s] ->
    // [1.0, 2.0] -> 3.0; t=120s: window (0,120s] excludes the sample at
    // exactly ts=0 (left-open) -> [2.0, 3.0] -> 5.0.
    assert_eq!(values, vec![1.0, 3.0, 5.0]);
}

/// `quantile_over_time(q, v)` puts the scalar argument first, the mirror
/// image of `predict_linear(v, t)`; this is the only P5 shape that does,
/// dispatched through `FunctionKind::ScalarRangeVector`.
#[test]
fn quantile_over_time_scalar_first_argument_dispatches_correctly() {
    let source = three_sample_gauge();
    let result = Evaluator::new()
        .instant(&source, "quantile_over_time(0.5, temp[5m])", 120_000)
        .expect("evaluates");
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].value, 2.0);
}

#[test]
fn range_query_quantile_over_time_advances_the_cursor_across_steps() {
    let source = three_sample_gauge();
    let result = Evaluator::new()
        .range(
            &source,
            "quantile_over_time(1, temp[2m])",
            0,
            120_000,
            60_000,
        )
        .expect("evaluates");
    let (_, samples) = &result[0];
    let values: Vec<f64> = samples.iter().map(|s| s.value).collect();
    assert_eq!(values, vec![1.0, 2.0, 3.0]);
}

#[test]
fn absent_over_time_instant_is_empty_when_the_matrix_selector_matches() {
    let source = three_sample_gauge();
    let result = Evaluator::new()
        .instant(&source, r#"absent_over_time(temp{job="api"}[5m])"#, 120_000)
        .expect("evaluates");
    assert!(result.is_empty());
}

/// No series matches the selector at all, so `absent_over_time` synthesizes
/// one output series from the selector's own equality matchers.
#[test]
fn absent_over_time_instant_synthesizes_labels_when_the_matrix_is_empty() {
    let source = three_sample_gauge();
    let result = Evaluator::new()
        .instant(
            &source,
            r#"absent_over_time(missing{job="api"}[5m])"#,
            120_000,
        )
        .expect("evaluates");
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].value, 1.0);
    assert_eq!(result[0].labels.get("job"), Some("api"));
    assert_eq!(result[0].labels.get("__name__"), None);
}

/// Over a range query, `absent_over_time` evaluates absence independently
/// at each grid step, so its single synthesized series carries a sample
/// only where the window was empty, not at every step.
#[test]
fn absent_over_time_range_reports_a_sample_only_at_absent_steps() {
    let source = TestSource::new()
        .with_series(&[("__name__", "temp")], &[(60_000_000_000, 1.0)])
        .expect("valid series");
    let result = Evaluator::new()
        .range(&source, "absent_over_time(temp[1m])", 0, 120_000, 60_000)
        .expect("evaluates");
    assert_eq!(result.len(), 1);
    let (_, samples) = &result[0];
    let ts_ms: Vec<i64> = samples.iter().map(|s| s.ts_ns / 1_000_000).collect();
    // Present at t=60s (sample falls in the left-open (0,60s] window);
    // absent at t=0 (nothing at or before 0 in a left-open window starting
    // before the series' only sample) and at t=120s (window (60s,120s]
    // holds nothing after the single sample at exactly 60s).
    assert_eq!(ts_ms, vec![0, 120_000]);
}

#[test]
fn unregistered_over_time_function_is_still_rejected() {
    let source = three_sample_gauge();
    let err = Evaluator::new()
        .instant(&source, "median_over_time(temp[5m])", 120_000)
        .expect_err("median_over_time is not a real function");
    assert!(err.to_string().contains("median_over_time"));
}
