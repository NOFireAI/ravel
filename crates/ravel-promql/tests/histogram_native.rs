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
use ravel_promql::{Evaluator, InstantSample, Value};

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
fn mixed_float_and_histogram_group_warns_when_dropped_by_sum_and_avg() {
    // A float series and a histogram series collapse into one group under
    // sum()/avg(); the type-incompatible group is dropped WITH a warning
    // (Prometheus' MixedFloatsHistogramsAggWarning), never silently. Before the
    // #649 fix the group was dropped with no annotation at all, so this
    // assertion on a non-empty `warnings()` failed. The annotation is a
    // WARNING, not the float-only aggregators' "ignored histogram" info.
    for op in ["sum", "avg"] {
        let src = TestSource::new()
            .with_series(&[("__name__", "m"), ("i", "1")], &[(0, 5.0)])
            .expect("valid")
            .with_histogram_series(&[("__name__", "m"), ("i", "2")], &[(0, hist(&[3.0], 3.0))])
            .expect("valid");
        let (value, annos) = Evaluator::new()
            .eval_instant_annotated(&src, &format!("{op}(m)"), 0)
            .expect("evaluates");
        let Value::Vector(out) = value else {
            panic!("{op} is a vector");
        };
        assert!(
            out.is_empty(),
            "{op}: mixed group must be dropped, got {out:?}"
        );
        assert!(
            annos
                .warnings()
                .iter()
                .any(|w| w.contains("mix of histograms and floats")),
            "{op}: dropping a mixed group must warn, warnings={:?}",
            annos.warnings()
        );
        assert!(
            annos.infos().is_empty(),
            "{op}: the drop is a warning, not an info: {:?}",
            annos.infos()
        );
    }
}

#[test]
fn irate_over_a_native_histogram_yields_a_histogram_element() {
    // Two histogram samples 30s apart in the (0, 300s] window, counts 2 then 8,
    // no reset. irate divides the raw difference (count 6, sum 30) by the 30s
    // interval: count 0.2, sum 1.0. Before the #650 fix the histogram window
    // routed to Drop and the query returned empty.
    let src = TestSource::new()
        .with_histogram_series(
            &[("__name__", "h")],
            &[
                (sec_ns(270), hist(&[2.0], 10.0)),
                (sec_ns(300), hist(&[8.0], 40.0)),
            ],
        )
        .expect("valid series");
    let got = Evaluator::new()
        .instant(&src, "irate(h[5m])", 5 * 60_000)
        .expect("evaluates");
    assert_eq!(got.len(), 1);
    let h = got[0]
        .histogram
        .as_ref()
        .expect("irate over histograms yields a histogram element, not empty");
    assert!((h.count - 0.2).abs() < 1e-12, "count {}", h.count);
    assert!((h.sum - 1.0).abs() < 1e-12, "sum {}", h.sum);
}

#[test]
fn idelta_over_a_native_histogram_yields_a_histogram_element() {
    // idelta is the raw last-minus-previous difference with no per-second
    // division (and no reset detection): counts 8 then 2 gives count -6,
    // sum -30. Before the #650 fix this returned empty.
    let src = TestSource::new()
        .with_histogram_series(
            &[("__name__", "h")],
            &[
                (sec_ns(270), hist(&[8.0], 40.0)),
                (sec_ns(300), hist(&[2.0], 10.0)),
            ],
        )
        .expect("valid series");
    let got = Evaluator::new()
        .instant(&src, "idelta(h[5m])", 5 * 60_000)
        .expect("evaluates");
    assert_eq!(got.len(), 1);
    let h = got[0]
        .histogram
        .as_ref()
        .expect("idelta over histograms yields a histogram element, not empty");
    assert!((h.count - (-6.0)).abs() < 1e-12, "count {}", h.count);
    assert!((h.sum - (-30.0)).abs() < 1e-12, "sum {}", h.sum);
}

#[test]
fn resets_and_changes_over_a_native_histogram_are_floats() {
    // counts 2 -> 8 -> 5 across three in-window samples: resets counts the one
    // downward step (8->5) as a float 1; changes counts both differing adjacent
    // pairs as a float 2. Both are floats, never histogram elements. Before the
    // #650 fix each returned empty.
    let src = TestSource::new()
        .with_histogram_series(
            &[("__name__", "h")],
            &[
                (sec_ns(60), hist(&[2.0], 10.0)),
                (sec_ns(180), hist(&[8.0], 40.0)),
                (sec_ns(300), hist(&[5.0], 25.0)),
            ],
        )
        .expect("valid series");
    let resets = Evaluator::new()
        .instant(&src, "resets(h[5m])", 5 * 60_000)
        .expect("evaluates");
    assert_eq!(resets.len(), 1);
    assert!(resets[0].histogram.is_none(), "resets is a float");
    assert_eq!(resets[0].value, 1.0);

    let changes = Evaluator::new()
        .instant(&src, "changes(h[5m])", 5 * 60_000)
        .expect("evaluates");
    assert_eq!(changes.len(), 1);
    assert!(changes[0].histogram.is_none(), "changes is a float");
    assert_eq!(changes[0].value, 2.0);
}

#[test]
fn deriv_over_a_pure_histogram_window_drops_with_no_annotation() {
    // #650's deriv framing corrected: Prometheus' funcDeriv takes its
    // `len(Floats) < 2` early return for a pure-histogram window, dropping with
    // NO annotation. This matches Ravel's existing behavior, so deriv needs no
    // code change. A single Ravel series is monotype (float OR histogram), so
    // deriv can never see a mixed float+histogram window here at all.
    let src = TestSource::new()
        .with_histogram_series(
            &[("__name__", "h")],
            &[
                (sec_ns(180), hist(&[2.0], 10.0)),
                (sec_ns(300), hist(&[8.0], 40.0)),
            ],
        )
        .expect("valid series");
    let (value, annos) = Evaluator::new()
        .eval_instant_annotated(&src, "deriv(h[5m])", 5 * 60_000)
        .expect("evaluates");
    let Value::Vector(out) = value else {
        panic!("deriv is a vector");
    };
    assert!(
        out.is_empty(),
        "deriv drops a pure-histogram window: {out:?}"
    );
    assert!(
        annos.is_empty(),
        "deriv's histogram drop carries no annotation: {annos:?}"
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
