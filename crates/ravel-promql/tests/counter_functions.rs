//! Integration coverage for the P4 function-call dispatch path itself
//! (`crate::functions::eval_call`/`eval_range_call`,
//! `Evaluator::eval_range_matrix_reduction`'s per-series cursor, and
//! `negate_matrix`) through the public `Evaluator` API. `rate.rs`'s own
//! unit tests already pin every function's math bit-exactly against
//! hand-derived Prometheus formulas; these tests instead exercise the
//! wiring around that math: instant vs. range call dispatch, the cursor
//! advancing correctly across a multi-step grid, `offset`, unary minus over
//! a range-call result, a scalar-typed function argument, and the
//! unregistered-function error in both the instant and range call paths.
#![allow(clippy::expect_used)]

use ravel_promql::{Error, Evaluator, testsource::TestSource};

/// A counter that increases by exactly 1 per second: sample at `t` seconds
/// carries value `t`. Its true rate is 1.0/s everywhere the window has at
/// least two samples, which keeps every hand-derived expectation below
/// exact instead of merely approximate.
fn one_per_second_counter() -> TestSource {
    TestSource::new()
        .with_series(
            &[("__name__", "up")],
            &[
                (0, 0.0),
                (60_000_000_000, 60.0),
                (120_000_000_000, 120.0),
                (180_000_000_000, 180.0),
                (240_000_000_000, 240.0),
                (300_000_000_000, 300.0),
            ],
        )
        .expect("valid series")
}

#[test]
fn instant_rate_call_dispatches_to_the_registered_function() {
    let source = one_per_second_counter();
    let result = Evaluator::new()
        .instant(&source, "rate(up[5m])", 300_000)
        .expect("evaluates");
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].value, 1.0);
}

/// The window at each step differs (an extrapolation clamp fires only once
/// the in-window first sample is no longer the series' very first sample),
/// so a wrong cursor would either replay one step's window at every step or
/// desync `lo`/`hi` across the grid. Values hand-derived from
/// `extrapolatedRate` at each step's own window.
#[test]
fn range_query_rate_advances_the_cursor_across_steps() {
    let source = one_per_second_counter();
    let result = Evaluator::new()
        .range(&source, "rate(up[5m])", 180_000, 300_000, 60_000)
        .expect("evaluates");
    assert_eq!(result.len(), 1);
    let (_, samples) = &result[0];
    let values: Vec<f64> = samples.iter().map(|s| s.value).collect();
    assert_eq!(values, vec![0.6, 0.8, 1.0]);
}

#[test]
fn unary_minus_over_a_range_call_negates_every_step() {
    let source = one_per_second_counter();
    let result = Evaluator::new()
        .range(&source, "-rate(up[5m])", 180_000, 300_000, 60_000)
        .expect("evaluates");
    let (_, samples) = &result[0];
    let values: Vec<f64> = samples.iter().map(|s| s.value).collect();
    assert_eq!(values, vec![-0.6, -0.8, -1.0]);
}

#[test]
fn offset_shifts_the_selector_window_the_function_operates_over() {
    let source = one_per_second_counter();
    let result = Evaluator::new()
        .instant(&source, "rate(up[5m] offset 60s)", 360_000)
        .expect("evaluates");
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].value, 1.0);
}

/// `predict_linear`'s second argument is a scalar, dispatched through
/// `FunctionKind::RangeVectorScalar`; this is the only P4 shape that takes
/// one.
#[test]
fn predict_linear_scalar_argument_dispatches_correctly() {
    let source = one_per_second_counter();
    let result = Evaluator::new()
        .instant(&source, "predict_linear(up[5m], 60)", 300_000)
        .expect("evaluates");
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].value, 360.0);
}

#[test]
fn unregistered_function_is_rejected_in_the_instant_call_path() {
    let source = one_per_second_counter();
    let err = Evaluator::new()
        .instant(&source, "sort_by_label(up)", 300_000)
        .expect_err("sort_by_label is not yet registered");
    assert!(matches!(err, Error::Unsupported { .. }));
    assert!(err.to_string().contains("sort_by_label"));
}

#[test]
fn unregistered_function_is_rejected_in_the_range_call_path() {
    let source = one_per_second_counter();
    let err = Evaluator::new()
        .range(&source, "sort_by_label(up)", 0, 300_000, 60_000)
        .expect_err("sort_by_label is not yet registered");
    assert!(matches!(err, Error::Unsupported { .. }));
    assert!(err.to_string().contains("sort_by_label"));
}
