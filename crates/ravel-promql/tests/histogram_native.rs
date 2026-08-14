//! Integration tests for the native-histogram function forms
//! driven end to end through the `Evaluator` over a `TestSource` that carries
//! injected native-histogram series. These exercise the wiring (selector ->
//! histogram element -> function/aggregation) that the per-module unit tests
//! do not, complementing the algorithm fixtures in `src/histogram.rs`.
//!
//! Every value here is hand-computed from Prometheus' documented algorithms;
//! the live differential gate cannot run for native histograms yet (no read
//! path feeds them into Ravel), so these fixtures are the acceptance bar.

#![allow(clippy::expect_used)]

use ravel_promql::histogram::{FloatHistogram, ResetHint, Span};
use ravel_promql::testsource::TestSource;
use ravel_promql::{Evaluator, InstantSample};

/// A positive-only histogram at scale 0, one span from index 1, `counts`
/// consecutive buckets. Bucket i (1-based) covers `(2^(i-1), 2^i]`.
fn hist(counts: &[f64], sum: f64) -> FloatHistogram {
    let total: f64 = counts.iter().sum();
    FloatHistogram {
        counter_reset_hint: ResetHint::Unknown,
        scale: 0,
        zero_threshold: 0.0,
        zero_count: 0.0,
        count: total,
        sum,
        positive_spans: vec![Span {
            offset: 1,
            length: counts.len() as u32,
        }],
        negative_spans: Vec::new(),
        positive_buckets: counts.to_vec(),
        negative_buckets: Vec::new(),
        custom_values: Vec::new(),
    }
}

fn sec_ns(s: i64) -> i64 {
    s * 1_000_000_000
}

fn floats(v: &[InstantSample]) -> Vec<f64> {
    v.iter().map(|s| s.value).collect()
}

#[test]
fn native_histogram_quantile_over_a_selector() {
    let src = TestSource::new()
        .with_histogram_series(&[("__name__", "h")], &[(0, hist(&[2.0, 2.0], 0.0))])
        .expect("valid series");
    // Buckets (1,2]:2 (2,4]:2. Median rank 2 -> upper bound of first bucket 2.
    let got = Evaluator::new()
        .instant(&src, "histogram_quantile(0.5, h)", 0)
        .expect("evaluates");
    assert_eq!(floats(&got), vec![2.0]);
    // The metric name is dropped (function result); no `le` label appears.
    assert!(got[0].labels.get("__name__").is_none());
    assert!(got[0].labels.get("le").is_none());
}

#[test]
fn histogram_count_of_rate_is_a_float() {
    // A counter native histogram observed at 150s and 300s, no reset. The
    // query window at t=5m over [5m] is the left-open (0, 300s]; both samples
    // fall inside. sampled_interval = 150s, duration_to_start = 150s (< the
    // 1.1x threshold 165s), duration_to_end = 0, so
    // extrapolate_to_interval = 150 + 150 = 300s and the factor is 2. rate
    // divides by the 300s range: count = (8-2) * 2 / 300 = 0.04.
    let src = TestSource::new()
        .with_histogram_series(
            &[("__name__", "h")],
            &[
                (sec_ns(150), hist(&[2.0], 4.0)),
                (sec_ns(300), hist(&[8.0], 16.0)),
            ],
        )
        .expect("valid series");
    let got = Evaluator::new()
        .instant(&src, "histogram_count(rate(h[5m]))", 5 * 60_000)
        .expect("evaluates");
    assert_eq!(got.len(), 1);
    assert!((got[0].value - 0.04).abs() < 1e-12, "got {}", got[0].value);
}

#[test]
fn increase_over_a_native_histogram_counts_without_per_second() {
    let src = TestSource::new()
        .with_histogram_series(
            &[("__name__", "h")],
            &[
                (sec_ns(150), hist(&[2.0], 4.0)),
                (sec_ns(300), hist(&[8.0], 16.0)),
            ],
        )
        .expect("valid series");
    let got = Evaluator::new()
        .instant(&src, "histogram_sum(increase(h[5m]))", 5 * 60_000)
        .expect("evaluates");
    // Same factor 2 as the rate case, without the per-second division:
    // increase sum = (16 - 4) * 2 = 24.
    assert_eq!(got.len(), 1);
    assert!((got[0].value - 24.0).abs() < 1e-9, "got {}", got[0].value);
}

#[test]
fn sum_aggregation_of_native_histograms() {
    let src = TestSource::new()
        .with_histogram_series(
            &[("__name__", "h"), ("i", "1")],
            &[(0, hist(&[1.0, 2.0], 3.0))],
        )
        .expect("valid")
        .with_histogram_series(
            &[("__name__", "h"), ("i", "2")],
            &[(0, hist(&[4.0, 5.0], 9.0))],
        )
        .expect("valid");
    // sum(h) merges the two histograms; histogram_count of the result is the
    // summed total count 3 + 9 = 12.
    let got = Evaluator::new()
        .instant(&src, "histogram_count(sum(h))", 0)
        .expect("evaluates");
    assert_eq!(floats(&got), vec![12.0]);
    let got_sum = Evaluator::new()
        .instant(&src, "histogram_sum(sum(h))", 0)
        .expect("evaluates");
    assert_eq!(floats(&got_sum), vec![12.0]);
}

#[test]
fn avg_aggregation_of_native_histograms() {
    let src = TestSource::new()
        .with_histogram_series(&[("__name__", "h"), ("i", "1")], &[(0, hist(&[2.0], 4.0))])
        .expect("valid")
        .with_histogram_series(&[("__name__", "h"), ("i", "2")], &[(0, hist(&[4.0], 8.0))])
        .expect("valid");
    let got = Evaluator::new()
        .instant(&src, "histogram_avg(avg(h))", 0)
        .expect("evaluates");
    // avg(h) has count (2+4)/2 = 3, sum (4+8)/2 = 6; histogram_avg = 6/3 = 2.
    assert_eq!(floats(&got), vec![2.0]);
}

#[test]
fn mixed_float_and_histogram_group_is_dropped_by_sum() {
    // One float series and one histogram series collapse into the same (empty)
    // group under `sum(...)`; the mixed group is omitted (Prometheus warns and
    // drops it). The result is an empty vector rather than a wrong sum.
    let src = TestSource::new()
        .with_series(&[("__name__", "m"), ("i", "1")], &[(0, 5.0)])
        .expect("valid")
        .with_histogram_series(&[("__name__", "m"), ("i", "2")], &[(0, hist(&[3.0], 3.0))])
        .expect("valid");
    let got = Evaluator::new()
        .instant(&src, "sum(m)", 0)
        .expect("evaluates");
    assert!(got.is_empty(), "mixed group must be dropped, got {got:?}");
}

#[test]
fn rate_over_a_native_histogram_compensates_a_counter_reset() {
    // counts 10 -> 2 (reset) -> 5 at 100s, 200s, 300s. Window at t=5m over
    // [5m] is (0, 300s]; all three samples fall inside. The reset (explicit
    // Yes hint on the middle sample) makes the compensated increase
    // (5 - 10) + 10 = 5. sampled_interval = 200s, average = 100s,
    // duration_to_start = 100s (< threshold 110s) so extrapolate = 200 + 100
    // = 300s, factor = 1.5. rate divides by 300s: count = 5 * 1.5 / 300.
    let src = TestSource::new()
        .with_histogram_series(
            &[("__name__", "h")],
            &[
                (sec_ns(100), {
                    let mut h = hist(&[10.0], 20.0);
                    h.counter_reset_hint = ResetHint::No;
                    h
                }),
                (sec_ns(200), {
                    let mut h = hist(&[2.0], 4.0);
                    h.counter_reset_hint = ResetHint::Yes;
                    h
                }),
                (sec_ns(300), {
                    let mut h = hist(&[5.0], 10.0);
                    h.counter_reset_hint = ResetHint::No;
                    h
                }),
            ],
        )
        .expect("valid series");
    let got = Evaluator::new()
        .instant(&src, "histogram_count(rate(h[5m]))", 5 * 60_000)
        .expect("evaluates");
    assert_eq!(got.len(), 1);
    assert!(
        (got[0].value - (5.0 * 1.5 / 300.0)).abs() < 1e-12,
        "got {}",
        got[0].value
    );
}

#[test]
fn histogram_fraction_native_over_a_selector() {
    let src = TestSource::new()
        .with_histogram_series(&[("__name__", "h")], &[(0, hist(&[2.0, 2.0], 0.0))])
        .expect("valid series");
    // Half the mass is at or below 2 (the median), so the fraction below 2 is
    // 0.5 and the full-range fraction is 1.
    let got = Evaluator::new()
        .instant(&src, "histogram_fraction(-Inf, 2, h)", 0)
        .expect("evaluates");
    assert_eq!(floats(&got), vec![0.5]);
}
