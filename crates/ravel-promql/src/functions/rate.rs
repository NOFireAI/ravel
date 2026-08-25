//! Counter/regression function family: `rate`, `irate`,
//! `increase`, `delta`, `idelta`, `resets`, `changes`, `deriv`,
//! `predict_linear`. Every formula is a direct, bit-exact-oriented port of
//! Prometheus' own `promql/functions.go` (`extrapolatedRate`,
//! `instantValue`, `linearRegression`, `funcResets`, `funcChanges`), down to
//! its operation order and the millisecond-domain duration arithmetic
//! (Prometheus samples and range/matrix bounds are millisecond-quantized;
//! this evaluator's nanoseconds are always an exact multiple of that, so
//! converting to milliseconds before dividing to seconds reproduces
//! Prometheus' own rounding instead of introducing new rounding of its own).
//!
//! One exception: a *duration* literal (e.g. `[5m]`) is a Go
//! `time.Duration`, already nanosecond-valued, and Prometheus takes its
//! `.Seconds()` directly (`ns / 1e9`), never routed through milliseconds;
//! `rate`'s final per-second division mirrors that directly.

use ravel_types::Sample;

use crate::histogram::{self, FloatHistogram, TimedHistogram};

use super::{FunctionDef, FunctionKind, RangeWindow};

pub(crate) const FUNCTIONS: &[FunctionDef] = &[
    FunctionDef {
        name: "rate",
        kind: FunctionKind::RangeVectorFloatOrHist {
            float: rate,
            hist: rate_hist,
        },
    },
    FunctionDef {
        name: "irate",
        kind: FunctionKind::RangeVector(irate),
    },
    FunctionDef {
        name: "increase",
        kind: FunctionKind::RangeVectorFloatOrHist {
            float: increase,
            hist: increase_hist,
        },
    },
    FunctionDef {
        name: "delta",
        kind: FunctionKind::RangeVectorFloatOrHist {
            float: delta,
            hist: delta_hist,
        },
    },
    FunctionDef {
        name: "idelta",
        kind: FunctionKind::RangeVector(idelta),
    },
    FunctionDef {
        name: "resets",
        kind: FunctionKind::RangeVector(resets),
    },
    FunctionDef {
        name: "changes",
        kind: FunctionKind::RangeVector(changes),
    },
    FunctionDef {
        name: "deriv",
        kind: FunctionKind::RangeVector(deriv),
    },
    FunctionDef {
        name: "predict_linear",
        kind: FunctionKind::RangeVectorScalar(predict_linear),
    },
];

fn rate(samples: &[Sample], w: RangeWindow) -> Option<f64> {
    extrapolated_rate(samples, w, true, true)
}

fn increase(samples: &[Sample], w: RangeWindow) -> Option<f64> {
    extrapolated_rate(samples, w, true, false)
}

fn delta(samples: &[Sample], w: RangeWindow) -> Option<f64> {
    extrapolated_rate(samples, w, false, false)
}

/// `rate` over a native-histogram window: the counter-reset-compensated
/// window reduction ([`histogram::histogram_rate`]) scaled by the same
/// boundary-extrapolation-and-per-second factor the float path uses.
fn rate_hist(samples: &[TimedHistogram], w: RangeWindow) -> Option<FloatHistogram> {
    histogram_extrapolated_rate(samples, w, true, true)
}

/// `increase` over a native-histogram window: like [`rate_hist`] without the
/// final per-second division.
fn increase_hist(samples: &[TimedHistogram], w: RangeWindow) -> Option<FloatHistogram> {
    histogram_extrapolated_rate(samples, w, true, false)
}

/// `delta` over a native-histogram (gauge) window: the plain windowed
/// difference with boundary extrapolation, no counter-reset compensation.
fn delta_hist(samples: &[TimedHistogram], w: RangeWindow) -> Option<FloatHistogram> {
    histogram_extrapolated_rate(samples, w, false, false)
}

/// The native-histogram counterpart of [`extrapolated_rate`] (Prometheus'
/// `extrapolatedRate` histogram branch): reduce the window to one histogram
/// via [`histogram::histogram_rate`], then multiply by the identical boundary-
/// extrapolation factor (`isRate` also dividing by the range in seconds). The
/// counter zero-floor clamp is float-only in Prometheus and is not applied
/// here.
fn histogram_extrapolated_rate(
    samples: &[TimedHistogram],
    w: RangeWindow,
    is_counter: bool,
    is_rate: bool,
) -> Option<FloatHistogram> {
    let mut reduced = histogram::histogram_rate(samples, is_counter)?;

    let first_ts = samples[0].0;
    let last_ts = samples[samples.len() - 1].0;

    let duration_to_start = ns_diff_to_seconds(first_ts, w.start_ns);
    let duration_to_end = ns_diff_to_seconds(w.end_ns, last_ts);
    let sampled_interval = ns_diff_to_seconds(last_ts, first_ts);
    let average_duration_between_samples = sampled_interval / (samples.len() - 1) as f64;

    let extrapolation_threshold = average_duration_between_samples * 1.1;
    let mut extrapolate_to_interval = sampled_interval;
    if duration_to_start < extrapolation_threshold {
        extrapolate_to_interval += duration_to_start;
    } else {
        extrapolate_to_interval += average_duration_between_samples / 2.0;
    }
    if duration_to_end < extrapolation_threshold {
        extrapolate_to_interval += duration_to_end;
    } else {
        extrapolate_to_interval += average_duration_between_samples / 2.0;
    }
    let mut factor = extrapolate_to_interval / sampled_interval;
    if is_rate {
        factor /= w.range_ns as f64 / 1_000_000_000.0;
    }
    reduced.mul(factor);
    Some(reduced)
}

fn irate(samples: &[Sample], _w: RangeWindow) -> Option<f64> {
    instant_value(samples, true)
}

fn idelta(samples: &[Sample], _w: RangeWindow) -> Option<f64> {
    instant_value(samples, false)
}

fn resets(samples: &[Sample], _w: RangeWindow) -> Option<f64> {
    let mut resets = 0i64;
    let mut prev_value = samples[0].value;
    for s in &samples[1..] {
        if s.value < prev_value {
            resets += 1;
        }
        prev_value = s.value;
    }
    Some(resets as f64)
}

fn changes(samples: &[Sample], _w: RangeWindow) -> Option<f64> {
    let mut changes = 0i64;
    let mut prev_value = samples[0].value;
    for s in &samples[1..] {
        let current = s.value;
        // Prometheus compares with plain IEEE `!=` here (not the bit-pattern
        // equality this crate otherwise favors), so `-0.0`/`0.0` are NOT a
        // change, exactly like upstream; only same-ness of NaN is special
        // cased so two different NaN payloads still count as no change.
        if current != prev_value && !(current.is_nan() && prev_value.is_nan()) {
            changes += 1;
        }
        prev_value = current;
    }
    Some(changes as f64)
}

fn deriv(samples: &[Sample], _w: RangeWindow) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    // Anchored to the window's first sample (not the query's evaluation
    // instant) purely for floating-point accuracy: Prometheus does the same
    // to keep `x` values small (prometheus/prometheus#2674). `predict_linear`
    // anchors to the evaluation instant instead, since its whole point is
    // projecting forward from "now".
    let (slope, _intercept) = linear_regression(samples, samples[0].ts_ns);
    Some(slope)
}

fn predict_linear(samples: &[Sample], w: RangeWindow, duration_seconds: f64) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let (slope, intercept) = linear_regression(samples, w.eval_ts_ns);
    Some(slope * duration_seconds + intercept)
}

/// Port of Prometheus' `extrapolatedRate` (`rate`/`increase`/`delta`):
/// boundary extrapolation with the 1.1x average-interval cap, plus (for
/// `isCounter`) counter-reset compensation and the zero-floor clamp that
/// keeps extrapolation from projecting a counter below zero. Fewer than two
/// samples in the window has no defined rate (Prometheus drops the series);
/// callers get `None`.
fn extrapolated_rate(
    samples: &[Sample],
    w: RangeWindow,
    is_counter: bool,
    is_rate: bool,
) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let first = samples[0];
    let last = samples[samples.len() - 1];

    let mut result_value = last.value - first.value;
    if is_counter {
        let mut last_value = 0.0_f64;
        for s in samples {
            if s.value < last_value {
                result_value += last_value;
            }
            last_value = s.value;
        }
    }

    let mut duration_to_start = ns_diff_to_seconds(first.ts_ns, w.start_ns);
    let duration_to_end = ns_diff_to_seconds(w.end_ns, last.ts_ns);

    let sampled_interval = ns_diff_to_seconds(last.ts_ns, first.ts_ns);
    let average_duration_between_samples = sampled_interval / (samples.len() - 1) as f64;

    if is_counter && result_value > 0.0 && first.value >= 0.0 {
        // Counters cannot be negative: if there is any slope at all, the
        // zero point of the counter can be extrapolated, and if that zero
        // point is closer than `duration_to_start`, the extrapolation is
        // clamped there instead, so it never projects to a negative value.
        let duration_to_zero = sampled_interval * (first.value / result_value);
        if duration_to_zero < duration_to_start {
            duration_to_start = duration_to_zero;
        }
    }

    let extrapolation_threshold = average_duration_between_samples * 1.1;
    let mut extrapolate_to_interval = sampled_interval;

    if duration_to_start < extrapolation_threshold {
        extrapolate_to_interval += duration_to_start;
    } else {
        extrapolate_to_interval += average_duration_between_samples / 2.0;
    }
    if duration_to_end < extrapolation_threshold {
        extrapolate_to_interval += duration_to_end;
    } else {
        extrapolate_to_interval += average_duration_between_samples / 2.0;
    }
    result_value *= extrapolate_to_interval / sampled_interval;
    if is_rate {
        result_value /= w.range_ns as f64 / 1_000_000_000.0;
    }
    Some(result_value)
}

/// Port of Prometheus' `instantValue` (`irate`/`idelta`): only the last two
/// samples in the window matter. `is_rate` (irate) also detects a counter
/// reset between them and converts to per-second; a zero sampled interval
/// (two samples at the same timestamp) has no defined result and is
/// dropped, exactly like Prometheus (`sampledInterval == 0`).
fn instant_value(samples: &[Sample], is_rate: bool) -> Option<f64> {
    if samples.len() < 2 {
        return None;
    }
    let last = samples[samples.len() - 1];
    let previous = samples[samples.len() - 2];

    let mut result_value = if is_rate && last.value < previous.value {
        last.value
    } else {
        last.value - previous.value
    };

    let sampled_interval_ns = last.ts_ns - previous.ts_ns;
    if sampled_interval_ns == 0 {
        return None;
    }

    if is_rate {
        result_value /= ms_to_seconds(ns_to_ms(sampled_interval_ns));
    }
    Some(result_value)
}

/// Port of Prometheus' `linearRegression`: ordinary least squares over the
/// window's samples, `x` measured in seconds from `intercept_time_ns`. All
/// samples having the same value short-circuits to a slope of zero (and,
/// for a non-finite constant value, `NaN`/`NaN`), matching Prometheus'
/// `constY` special case, which also avoids a `varX == 0` division by zero
/// when every sample shares one timestamp.
fn linear_regression(samples: &[Sample], intercept_time_ns: i64) -> (f64, f64) {
    let mut n = 0.0_f64;
    let mut sum_x = 0.0_f64;
    let mut sum_y = 0.0_f64;
    let mut sum_xy = 0.0_f64;
    let mut sum_x2 = 0.0_f64;
    let init_y = samples[0].value;
    let mut const_y = true;

    for (i, s) in samples.iter().enumerate() {
        if const_y && i > 0 && s.value != init_y {
            const_y = false;
        }
        n += 1.0;
        let x = ms_to_seconds(ns_to_ms(s.ts_ns - intercept_time_ns));
        sum_x += x;
        sum_y += s.value;
        sum_xy += x * s.value;
        sum_x2 += x * x;
    }

    if const_y {
        if init_y.is_infinite() {
            return (f64::NAN, f64::NAN);
        }
        return (0.0, init_y);
    }

    let cov_xy = sum_xy - sum_x * sum_y / n;
    let var_x = sum_x2 - sum_x * sum_x / n;

    let slope = cov_xy / var_x;
    let intercept = sum_y / n - slope * sum_x / n;
    (slope, intercept)
}

/// `(a - b)` converted from nanoseconds to seconds via an intermediate
/// millisecond value, matching Prometheus' own millisecond-timestamp
/// duration arithmetic exactly (see the module doc).
fn ns_diff_to_seconds(a: i64, b: i64) -> f64 {
    ms_to_seconds(ns_to_ms(a - b))
}

/// Exact (the evaluator's own invariant: every nanosecond value it produces
/// is already an exact multiple of one millisecond).
fn ns_to_ms(ns: i64) -> i64 {
    ns / 1_000_000
}

fn ms_to_seconds(ms: i64) -> f64 {
    ms as f64 / 1000.0
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn ms(n: i64) -> i64 {
        n * 1_000_000
    }

    fn sample(ts_ms: i64, value: f64) -> Sample {
        Sample {
            ts_ns: ms(ts_ms),
            value,
        }
    }

    fn window(start_ms: i64, end_ms: i64, range_ms: i64) -> RangeWindow {
        RangeWindow {
            start_ns: ms(start_ms),
            end_ns: ms(end_ms),
            range_ns: ms(range_ms),
            eval_ts_ns: ms(end_ms),
        }
    }

    #[test]
    fn rate_single_sample_is_none() {
        let samples = [sample(0, 1.0)];
        assert_eq!(rate(&samples, window(0, 300_000, 300_000)), None);
    }

    #[test]
    fn rate_extrapolates_to_the_window_boundary() {
        // Four samples, evenly spaced 60s apart, starting exactly at the
        // window's left edge and ending exactly at its right edge: no
        // extrapolation headroom is needed, so `rate` is exactly
        // `(last - first) / range_seconds`.
        let samples = [
            sample(0, 10.0),
            sample(60_000, 20.0),
            sample(120_000, 30.0),
            sample(180_000, 40.0),
        ];
        let w = window(0, 180_000, 180_000);
        let got = rate(&samples, w).expect("2+ samples");
        assert_eq!(got, 30.0 / 180.0);
    }

    #[test]
    fn increase_extrapolates_half_average_interval_past_the_first_sample() {
        // Samples start 90s after the window's left edge (duration_to_start
        // = 90s) with a 60s average spacing: 90s exceeds the 1.1x
        // extrapolation threshold (66s), so the extrapolation on that side
        // is capped at half the average interval (30s), not the full 90s.
        // (`first.value` is a large nonzero baseline so the counter
        // zero-floor clamp, covered separately below, does not also fire
        // here: duration_to_zero = 60*(1000/60) = 1000s, well past
        // duration_to_start, so it never overrides it.)
        let samples = [sample(90_000, 1_000.0), sample(150_000, 1_060.0)];
        let w = window(0, 150_000, 150_000);
        let got = increase(&samples, w).expect("2+ samples");
        // sampled_interval=60s, extrapolate_to_interval = 60 + 30 (start,
        // capped) + 0 (end, exact boundary) = 90s.
        assert_eq!(got, 60.0 * (90.0 / 60.0));
    }

    #[test]
    fn increase_extrapolates_the_full_gap_when_inside_the_threshold() {
        // duration_to_start = 5s, well inside the 1.1x*60s=66s threshold, so
        // the full 5s gap is added rather than being capped at 30s. (See the
        // note above: nonzero baseline keeps the zero-floor clamp out of
        // this test.)
        let samples = [sample(5_000, 1_000.0), sample(65_000, 1_060.0)];
        let w = window(0, 65_000, 65_000);
        let got = increase(&samples, w).expect("2+ samples");
        assert_eq!(got, 60.0 * (65.0 / 60.0));
    }

    #[test]
    fn increase_compensates_for_a_counter_reset() {
        // Counter resets from 10 down to 2 partway through the window.
        let samples = [sample(0, 0.0), sample(60_000, 10.0), sample(120_000, 2.0)];
        let w = window(0, 120_000, 120_000);
        let got = increase(&samples, w).expect("2+ samples");
        // Raw resultValue = 2 - 0 = 2, plus 10 compensated at the reset = 12.
        // sampled_interval=120s, average=60s; both boundaries are exact
        // (duration_to_start=0, duration_to_end=0), so no extrapolation
        // beyond the sampled interval itself.
        assert_eq!(got, 12.0);
    }

    #[test]
    fn increase_zero_floor_clamp_prevents_negative_counter_extrapolation() {
        // First sample is 30s after the window start; slope is 1/s, so a
        // naive extrapolation of the full 30s gap would imply the counter
        // was at -30 at the window's start, an impossible negative counter
        // value. The zero-floor clamp caps `duration_to_start` at the
        // extrapolated zero-crossing (10s: first.value / resultValue * 60s
        // = 10/60*60) instead of the raw 30s gap.
        let samples = [sample(30_000, 10.0), sample(90_000, 70.0)];
        let w = window(0, 90_000, 90_000);
        let got = increase(&samples, w).expect("2+ samples");
        // resultValue=60, sampled_interval=60s, duration_to_zero =
        // 60*(10/60)=10s < duration_to_start(30s) -> clamped to 10s.
        // 10s is inside the 66s threshold, so it is added in full;
        // duration_to_end=0 exact boundary. extrapolate_to_interval =
        // 60+10+0=70s.
        assert_eq!(got, 60.0 * (70.0 / 60.0));
    }

    #[test]
    fn delta_does_not_compensate_for_counter_resets() {
        // Same samples as the increase-reset-compensation case, but delta
        // must NOT add back the drop: it is meant for gauges.
        let samples = [sample(0, 0.0), sample(60_000, 10.0), sample(120_000, 2.0)];
        let w = window(0, 120_000, 120_000);
        let got = delta(&samples, w).expect("2+ samples");
        assert_eq!(got, 2.0);
    }

    #[test]
    fn irate_single_sample_is_none() {
        let samples = [sample(0, 1.0)];
        assert_eq!(irate(&samples, window(0, 0, 300_000)), None);
    }

    #[test]
    fn irate_uses_only_the_last_two_samples() {
        let samples = [sample(0, 100.0), sample(30_000, 5.0), sample(60_000, 25.0)];
        let got = irate(&samples, window(0, 60_000, 300_000)).expect("2+ samples");
        assert_eq!(got, 20.0 / 30.0);
    }

    #[test]
    fn irate_detects_counter_reset_between_last_two_samples() {
        let samples = [sample(0, 100.0), sample(30_000, 5.0)];
        let got = irate(&samples, window(0, 30_000, 300_000)).expect("2+ samples");
        // Counter reset: resultValue is just the last (post-reset) value.
        assert_eq!(got, 5.0 / 30.0);
    }

    #[test]
    fn idelta_does_not_detect_counter_reset() {
        let samples = [sample(0, 100.0), sample(30_000, 5.0)];
        let got = idelta(&samples, window(0, 30_000, 300_000)).expect("2+ samples");
        assert_eq!(got, 5.0 - 100.0);
    }

    #[test]
    fn zero_sampled_interval_drops_the_series_for_irate_and_idelta() {
        let samples = [sample(10_000, 1.0), sample(10_000, 2.0)];
        assert_eq!(irate(&samples, window(0, 10_000, 300_000)), None);
        assert_eq!(idelta(&samples, window(0, 10_000, 300_000)), None);
    }

    #[test]
    fn resets_counts_downward_transitions() {
        let samples = [
            sample(0, 1.0),
            sample(1_000, 2.0),
            sample(2_000, 1.0),
            sample(3_000, 3.0),
            sample(4_000, 0.5),
        ];
        assert_eq!(
            resets(&samples, window(0, 4_000, 4_000)),
            Some(2.0),
            "two downward transitions: 2->1 and 3->0.5"
        );
    }

    #[test]
    fn resets_of_a_single_sample_is_zero() {
        let samples = [sample(0, 5.0)];
        assert_eq!(resets(&samples, window(0, 0, 300_000)), Some(0.0));
    }

    #[test]
    fn changes_counts_value_transitions() {
        let samples = [
            sample(0, 1.0),
            sample(1_000, 1.0),
            sample(2_000, 2.0),
            sample(3_000, 2.0),
            sample(4_000, 3.0),
        ];
        assert_eq!(changes(&samples, window(0, 4_000, 4_000)), Some(2.0));
    }

    #[test]
    fn changes_treats_negative_and_positive_zero_as_no_change() {
        let samples = [sample(0, 0.0), sample(1_000, -0.0)];
        assert_eq!(changes(&samples, window(0, 1_000, 1_000)), Some(0.0));
    }

    #[test]
    fn changes_treats_consecutive_nan_of_any_payload_as_no_change() {
        let nan_a = f64::from_bits(0x7ff8_0000_0000_0001);
        let nan_b = f64::from_bits(0x7ff8_0000_0000_0002);
        let samples = [sample(0, nan_a), sample(1_000, nan_b)];
        assert_eq!(changes(&samples, window(0, 1_000, 1_000)), Some(0.0));
    }

    #[test]
    fn deriv_single_sample_is_none() {
        let samples = [sample(0, 1.0)];
        assert_eq!(deriv(&samples, window(0, 0, 300_000)), None);
    }

    #[test]
    fn deriv_of_a_straight_line_is_its_slope() {
        let samples = [sample(0, 10.0), sample(10_000, 20.0), sample(20_000, 30.0)];
        let got = deriv(&samples, window(0, 20_000, 20_000)).expect("2+ samples");
        assert_eq!(got, 1.0, "10 units per 10s = 1 unit/s");
    }

    #[test]
    fn deriv_of_a_constant_series_is_zero() {
        let samples = [sample(0, 7.0), sample(10_000, 7.0), sample(20_000, 7.0)];
        let got = deriv(&samples, window(0, 20_000, 20_000)).expect("2+ samples");
        assert_eq!(got, 0.0);
    }

    #[test]
    fn predict_linear_projects_the_fitted_line_forward() {
        let samples = [sample(0, 10.0), sample(10_000, 20.0), sample(20_000, 30.0)];
        let w = window(0, 20_000, 20_000);
        // Slope 1 unit/s, anchored at the window's own eval instant (20s):
        // predicting 30s further out from t=20s (value 30) gives 60.
        let got = predict_linear(&samples, w, 30.0).expect("2+ samples");
        assert_eq!(got, 60.0);
    }

    #[test]
    fn predict_linear_single_sample_is_none() {
        let samples = [sample(0, 1.0)];
        assert_eq!(predict_linear(&samples, window(0, 0, 300_000), 60.0), None);
    }

    #[test]
    fn nan_and_inf_samples_propagate_through_delta() {
        let samples = [sample(0, f64::NAN), sample(60_000, 1.0)];
        let got = delta(&samples, window(0, 60_000, 60_000)).expect("2+ samples");
        assert!(got.is_nan());

        let samples = [sample(0, f64::INFINITY), sample(60_000, f64::INFINITY)];
        let got = delta(&samples, window(0, 60_000, 60_000)).expect("2+ samples");
        assert!(got.is_nan(), "Inf - Inf is NaN");

        let samples = [sample(0, 1.0), sample(60_000, f64::INFINITY)];
        let got = delta(&samples, window(0, 60_000, 60_000)).expect("2+ samples");
        assert_eq!(got, f64::INFINITY);
    }

    /// A scale-0 counter native histogram carrying `count`/`sum` in one
    /// positive bucket, for the range/instant parity test below.
    fn nh_counter(count: f64, sum: f64) -> FloatHistogram {
        use crate::histogram::{ResetHint, Span};
        FloatHistogram {
            counter_reset_hint: ResetHint::Unknown,
            scale: 0,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count,
            sum,
            positive_spans: vec![Span {
                offset: 1,
                length: 1,
            }],
            negative_spans: Vec::new(),
            positive_buckets: vec![count],
            negative_buckets: Vec::new(),
            custom_values: Vec::new(),
        }
    }

    #[test]
    fn range_rate_over_native_histogram_matches_the_instant_arm() {
        use crate::eval::{RangeValue, Value};
        use crate::testsource::TestSource;

        // A native-histogram counter with two increasing samples in the
        // window. The instant arm already reduces it through
        // `histogram_extrapolated_rate`; the range arm must dispatch to the
        // same reducer over the same window and produce the bit-identical
        // histogram element. Before item 1, the range arm ignored `hist: _`
        // and the histogram series vanished, so the range matrix was empty:
        // the flipped assertion is the `range_hist == instant_hist` equality
        // (pre-fix the range side had no histogram element at all).
        // Samples land inside the left-open window `(0, 120s]`: the 2m window
        // at t=120s excludes an edge sample at exactly t=0.
        let source = TestSource::new()
            .with_histogram_series(
                &[("__name__", "h")],
                &[
                    (ms(60_000), nh_counter(2.0, 10.0)),
                    (ms(120_000), nh_counter(6.0, 30.0)),
                ],
            )
            .expect("valid histogram series");
        let evaluator = crate::eval::Evaluator::new();

        let instant = evaluator
            .eval_instant(&source, "rate(h[2m])", 120_000)
            .expect("instant evaluates");
        let Value::Vector(instant) = instant else {
            panic!("rate is a vector");
        };
        assert_eq!(instant.len(), 1, "one histogram series");
        let instant_hist = instant[0]
            .histogram
            .as_ref()
            .expect("instant rate over a histogram yields a histogram element");

        let (range, _annos) = evaluator
            .eval_range_hist_annotated(&source, "rate(h[2m])", 120_000, 120_000, 60_000)
            .expect("range evaluates");
        let RangeValue::Matrix(matrix) = range else {
            panic!("range rate is a matrix");
        };
        assert_eq!(matrix.len(), 1, "one histogram series");
        let samples = &matrix[0].1;
        assert_eq!(samples.len(), 1, "one grid step at t=120s");
        let range_hist = samples[0]
            .histogram
            .as_ref()
            .expect("range rate over a histogram yields a histogram element, not an empty series");

        assert_eq!(
            range_hist, instant_hist,
            "the range arm must reduce through the same histogram reducer as the instant arm"
        );
    }
}
