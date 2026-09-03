//! Counter/regression function family: `rate`, `irate`,
//! `increase`, `delta`, `idelta`, `resets`, `changes`, `deriv`,
//! `predict_linear`. Every formula is a direct, bit-exact-oriented port of
//! Prometheus' own `promql/functions.go` (`extrapolatedRate`,
//! `instantValue`, `linearRegression`, `funcResets`, `funcChanges`), down to
//! its operation order.
//!
//! Timestamps are nanoseconds and every interval this family divides by is
//! computed directly in nanoseconds (`(a - b) as f64 / 1e9`), never via an
//! intermediate millisecond value: a log-derived series (ADR-1103) can
//! carry two distinct samples less than one millisecond apart, and flooring
//! through milliseconds first turns a nonzero nanosecond gap into a zero
//! divisor. For a millisecond-quantized timestamp pair (every value
//! Prometheus itself produces), `(a - b)` is already an exact multiple of
//! 1_000_000, so this direct formula is the same real-valued quotient as
//! the old two-step one and, since both are single correctly-rounded IEEE
//! 754 divisions of exactly-representable operands, bit-identical to it
//! (see `ms_quantized_inputs_are_unchanged` below).

use ravel_types::Sample;

use crate::histogram::{self, FloatHistogram, ResetHint, TimedHistogram};

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
    let first_ts = samples[0].0;
    let last_ts = samples[samples.len() - 1].0;
    let sampled_interval = ns_diff_to_seconds(last_ts, first_ts);
    // See the matching guard in `extrapolated_rate`: a zero-duration window
    // has no defined rate and returns no sample rather than a
    // NaN-producing division. Guarding on `sampled_interval` itself (rather
    // than `last_ts == first_ts`) ties the guard to the exact quantity the
    // division below uses, so the two can never disagree.
    if sampled_interval == 0.0 {
        return None;
    }

    let mut reduced = histogram::histogram_rate(samples, is_counter)?;

    let duration_to_start = ns_diff_to_seconds(first_ts, w.start_ns);
    let duration_to_end = ns_diff_to_seconds(w.end_ns, last_ts);
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
    // ADR-1103: every sample sharing one timestamp makes every regression `x`
    // value identical, so `var_x` is exactly zero and the slope is a 0.0/0.0
    // NaN rather than a defined value; standard Prometheus storage dedups to
    // one sample per timestamp so this state has no oracle behavior to match
    // (same reasoning as `extrapolated_rate`'s zero-duration guard), so this
    // drops the window instead of returning NaN.
    if samples[samples.len() - 1].ts_ns == samples[0].ts_ns {
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
    // See `deriv`'s matching guard: a zero-duration window has no defined
    // slope.
    if samples[samples.len() - 1].ts_ns == samples[0].ts_ns {
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
    let sampled_interval = ns_diff_to_seconds(last.ts_ns, first.ts_ns);
    // ADR-1103: a log-derived series can deliver every sample in the window
    // at one timestamp (`first == last`). Standard Prometheus storage dedups
    // to one sample per timestamp, so this state cannot arise there and the
    // ported formula below (which divides by `sampled_interval`) has no
    // oracle behavior to match; a zero-duration window has no defined rate,
    // mirroring `instant_value`'s existing `sampledInterval == 0` drop, so
    // this returns no sample rather than the 0.0/0.0 NaN the division would
    // otherwise produce. Guarding on `sampled_interval` itself (rather than
    // `last.ts_ns == first.ts_ns`) ties the guard to the exact quantity the
    // division below uses, so the two can never disagree: #1136 found two
    // distinct timestamps less than a millisecond apart used to floor to a
    // zero millisecond interval and pass this guard while still dividing by
    // zero.
    if sampled_interval == 0.0 {
        return None;
    }

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
        result_value /= sampled_interval_ns as f64 / 1_000_000_000.0;
    }
    Some(result_value)
}

/// `irate`/`idelta` over a native-histogram window (Prometheus' `instantValue`
/// histogram branch): only the last two histograms matter. `idelta`
/// (`is_rate == false`) always takes the raw difference `last - previous`;
/// `irate` (`is_rate == true`) skips the subtraction when the pair is a counter
/// reset (leaving the later histogram, mirroring the float path where a reset
/// makes the result the last value alone) and then divides by the sampled
/// interval in seconds. Two samples at the same timestamp have no defined
/// result and drop, exactly like the float path (`sampledInterval == 0`). The
/// result is marked gauge-typed, matching Prometheus' `Compact`-then-set-hint
/// step (the trailing `Compact(0)` only removes empty buckets, which the JSON
/// renderer already omits, so it is not reproduced here).
///
/// The two samples are adjacent samples of the same series but can carry
/// different schemas (RSEG down-converts a flush whose bucket count exceeds
/// its limit, so two flushes of one series can persist at different scales).
/// The reset check is directional (oracle-verified against three
/// adjacent-schema shapes, see `functions::rate::tests` and
/// `tests/histogram_native.rs`): a schema INCREASE is always a reset, since
/// Prometheus never risks comparing a coarser earlier histogram against
/// newly-visible finer detail, while a schema DECREASE is only a reset if the
/// buckets, once aligned to the common coarser schema, actually shrank. That
/// direction now lives where Prometheus keeps it, in
/// [`FloatHistogram::detect_reset`] (issue #679), so `irate` calls it on the
/// raw pair the way Prometheus' `instantValue` does rather than carrying its
/// own copy of the rule. `idelta` has no reset escape at all (always aligns
/// and subtracts) once a schema-type mismatch is ruled out; the
/// increase/decrease asymmetry is irate-only.
///
/// An exponential-schema sample paired with a custom-buckets one is a
/// separate case `copy_to_scale` cannot bridge (a custom-buckets operand
/// reports a schema sentinel far outside the exponential range, and rescaling
/// the other operand to it overflows the bucket-index shift). Prometheus
/// refuses to combine the two: `irate` treats the mismatch as a reset (`Sub`
/// never runs, the later histogram is returned alone) and `idelta` (which
/// always subtracts) has no reset escape, so it returns no value at all,
/// alongside [`mixed_exponential_custom_schemas_warning`]. Detect this before
/// `min_scale`/`copy_to_scale` are ever computed, not by hardening
/// `copy_to_scale`'s shift arithmetic: even a bounded shift would silently
/// combine bucket layouts Prometheus does not consider comparable.
pub(crate) fn instant_value_hist(
    samples: &[TimedHistogram],
    is_rate: bool,
) -> Option<FloatHistogram> {
    if samples.len() < 2 {
        return None;
    }
    let (last_ts, last) = &samples[samples.len() - 1];
    let (prev_ts, previous) = &samples[samples.len() - 2];
    let sampled_interval_ns = last_ts - prev_ts;
    if sampled_interval_ns == 0 {
        return None;
    }

    let schema_type_mismatch = previous.uses_custom_buckets() != last.uses_custom_buckets();
    if schema_type_mismatch && !is_rate {
        // idelta cannot combine an exponential-schema histogram with a
        // custom-buckets one and has no reset escape hatch; the warning is
        // raised by instant_value_hist_type_warning's caller.
        return None;
    }

    let mut result = last.clone();
    if is_rate {
        // `detect_reset` carries the whole directional rule (#679), so this is
        // Prometheus' `instantValue` shape: subtract unless the pair is a
        // reset. The `schema_type_mismatch` short-circuit stays ahead of it
        // because an explicit `No`/`Gauge` hint would otherwise send an
        // exponential/custom-buckets pair into `copy_to_scale`, whose
        // bucket-index shift cannot bridge the custom-buckets sentinel.
        let is_reset = schema_type_mismatch || last.detect_reset(previous);
        if !is_reset {
            let min_scale = previous.scale.min(last.scale);
            result = result.copy_to_scale(min_scale);
            result.sub_assign(&previous.copy_to_scale(min_scale));
        }
    } else {
        // idelta always subtracts (no reset escape at all) once a schema-type
        // mismatch has been ruled out above; the schema-increase/decrease
        // asymmetry above is irate-only.
        let min_scale = previous.scale.min(last.scale);
        result = result.copy_to_scale(min_scale);
        result.sub_assign(&previous.copy_to_scale(min_scale));
    }
    result.counter_reset_hint = ResetHint::Gauge;
    if is_rate {
        result.div(sampled_interval_ns as f64 / 1_000_000_000.0);
    }
    Some(result)
}

/// Which type-mismatch warning an `irate`/`idelta` over a native-histogram
/// pair triggers, mirroring Prometheus' `instantValue` histogram branch:
/// `irate` (a counter operation) over a gauge-typed pair warns the metric is
/// not a counter; `idelta` (a gauge operation) over a pair that is not
/// gauge-typed warns the metric is not a gauge; either function over a pair
/// mixing an exponential-schema histogram with a custom-buckets one warns
/// the schemas don't match (only reachable for `idelta` -- `irate` treats
/// that mismatch as a silent reset, see [`instant_value_hist`]).
pub(crate) enum InstantHistTypeWarning {
    /// `irate` was asked to treat a gauge-typed native histogram as a counter.
    NotCounter,
    /// `idelta` was asked to treat a non-gauge native histogram as a gauge.
    NotGauge,
    /// One operand is custom-buckets and the other exponential-schema.
    MixedSchemas,
}

/// Whether `irate`/`idelta` over the last two histograms in `samples` should
/// raise a type-mismatch warning (Prometheus' `instantValue` histogram
/// branch: [`InstantHistTypeWarning::MixedSchemas`] for an exponential/
/// custom-buckets pair, `NativeHistogramNotCounter` for `irate` over a
/// gauge-hinted pair, `NativeHistogramNotGauge` for `idelta` over a pair that
/// is not both gauge-hinted). The schema-type check runs first since it is
/// unrelated to counter/gauge hints and `irate` never warns on it (the
/// mismatch is a silent reset there, not a value-suppressing warning). The
/// counter/gauge check reads each histogram's own `counter_reset_hint` on
/// entry, before [`instant_value_hist`] overwrites the result's hint with
/// [`ResetHint::Gauge`]. Returns `None` when no warning applies, or when no
/// result is produced (fewer than two samples, or a zero sampled interval),
/// so a warning fires only alongside a value or alongside `idelta`'s
/// mismatch-driven empty result, exactly as Prometheus' own early returns
/// gate it.
pub(crate) fn instant_value_hist_type_warning(
    samples: &[TimedHistogram],
    is_rate: bool,
) -> Option<InstantHistTypeWarning> {
    if samples.len() < 2 {
        return None;
    }
    let (last_ts, last) = &samples[samples.len() - 1];
    let (prev_ts, previous) = &samples[samples.len() - 2];
    if last_ts - prev_ts == 0 {
        return None;
    }
    if previous.uses_custom_buckets() != last.uses_custom_buckets() {
        return if is_rate {
            None
        } else {
            Some(InstantHistTypeWarning::MixedSchemas)
        };
    }
    let last_gauge = last.counter_reset_hint == ResetHint::Gauge;
    let prev_gauge = previous.counter_reset_hint == ResetHint::Gauge;
    if is_rate {
        (last_gauge || prev_gauge).then_some(InstantHistTypeWarning::NotCounter)
    } else {
        (!last_gauge || !prev_gauge).then_some(InstantHistTypeWarning::NotGauge)
    }
}

/// Prometheus' `NativeHistogramNotCounterWarning` message: `irate` (a counter
/// operation) was applied to a gauge-typed native histogram. Like Ravel's other
/// ported annotations (`invalid_quantile_warning`,
/// `mixed_floats_histograms_agg_warning`), this carries the core Prometheus
/// wording without the `PromQL warning:` prefix, source position, or trailing
/// metric-name clause Prometheus appends; the differential comparator matches
/// annotation presence, not text.
pub(crate) fn native_histogram_not_counter_warning() -> String {
    "this native histogram metric is not a counter".to_string()
}

/// Prometheus' `NativeHistogramNotGaugeWarning` message: `idelta` (a gauge
/// operation) was applied to a native histogram that is not gauge-typed. Same
/// text convention as [`native_histogram_not_counter_warning`].
pub(crate) fn native_histogram_not_gauge_warning() -> String {
    "this native histogram metric is not a gauge".to_string()
}

/// Prometheus' mixed-schema warning: an exponential-schema histogram and a
/// custom-buckets one appear adjacent in one series' window. Same text
/// convention as [`native_histogram_not_counter_warning`].
pub(crate) fn mixed_exponential_custom_schemas_warning() -> String {
    "vector contains a mix of histograms with exponential and custom buckets schemas".to_string()
}

/// `resets` over a native-histogram window (Prometheus' `funcResets` histogram
/// path): count adjacent pairs where the later histogram is a counter reset
/// relative to the earlier one ([`FloatHistogram::detect_reset`]). A float, not
/// a histogram, exactly like the float `resets`.
pub(crate) fn resets_hist(samples: &[TimedHistogram]) -> f64 {
    let mut resets = 0i64;
    for pair in samples.windows(2) {
        if pair[1].1.detect_reset(&pair[0].1) {
            resets += 1;
        }
    }
    resets as f64
}

/// `changes` over a native-histogram window (Prometheus' `funcChanges`
/// histogram path): count adjacent pairs of unequal histograms (Prometheus'
/// data-equality `!Equals`, [`FloatHistogram::equals`]). A float, like the
/// float `changes`.
pub(crate) fn changes_hist(samples: &[TimedHistogram]) -> f64 {
    let mut changes = 0i64;
    for pair in samples.windows(2) {
        if !pair[1].1.equals(&pair[0].1) {
            changes += 1;
        }
    }
    changes as f64
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
        let x = (s.ts_ns - intercept_time_ns) as f64 / 1_000_000_000.0;
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

/// `(a - b)` converted from nanoseconds to seconds directly, with no
/// intermediate millisecond value: timestamps are nanoseconds, and every
/// interval this family divides by is computed in nanoseconds so a
/// sub-millisecond, non-equal-timestamp pair (ADR-1103 log-derived series)
/// never floors to a zero-length interval. Millisecond-quantized inputs
/// (everything Prometheus itself produces) are unaffected: see the module
/// doc and `ms_quantized_inputs_are_unchanged`.
fn ns_diff_to_seconds(a: i64, b: i64) -> f64 {
    (a - b) as f64 / 1_000_000_000.0
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

    fn sample_ns(ts_ns: i64, value: f64) -> Sample {
        Sample { ts_ns, value }
    }

    fn window_ns(start_ns: i64, end_ns: i64, range_ns: i64, eval_ts_ns: i64) -> RangeWindow {
        RangeWindow {
            start_ns,
            end_ns,
            range_ns,
            eval_ts_ns,
        }
    }

    #[test]
    fn rate_single_sample_is_none() {
        let samples = [sample(0, 1.0)];
        assert_eq!(rate(&samples, window(0, 300_000, 300_000)), None);
    }

    #[test]
    fn rate_over_all_equal_timestamp_samples_is_none_not_nan() {
        // ADR-1103: a log-derived series can deliver every sample in the
        // window at one timestamp. Standard Prometheus storage dedups to one
        // sample per timestamp, so `first == last` cannot arise there and
        // the ported formula (which divides by `sampled_interval`) has no
        // oracle behavior to match; a zero-duration window is defined here
        // to have no rate, mirroring `instant_value`'s existing
        // `sampledInterval == 0` drop. Demonstrated failing by deleting the
        // `if last.ts_ns == first.ts_ns { return None; }` guard at the top
        // of `extrapolated_rate` in this file: without it, `last.value -
        // first.value == 0.0` divided by a zero `sampled_interval` produces
        // `NaN`, not a dropped sample.
        let samples = [
            sample(60_000, 1.0),
            sample(60_000, 2.0),
            sample(60_000, 3.0),
        ];
        let w = window(0, 300_000, 300_000);
        assert_eq!(rate(&samples, w), None);
        assert_eq!(increase(&samples, w), None);
        assert_eq!(delta(&samples, w), None);
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
    fn deriv_and_predict_linear_over_all_equal_timestamp_samples_are_none_not_nan() {
        // ADR-1103 audit finding beyond the named test list: every sample
        // sharing one timestamp makes every regression `x` value identical,
        // so `var_x` (and `cov_xy`) are exactly zero and the slope is a
        // 0.0/0.0 NaN, not caught by `linear_regression`'s existing
        // `const_y` special case (which only handles identical *values*, not
        // identical *timestamps*). Demonstrated failing by deleting the
        // `if samples[samples.len() - 1].ts_ns == samples[0].ts_ns { return
        // None; }` guard in `deriv` (and the matching one in
        // `predict_linear`) in this file.
        let samples = [
            sample(60_000, 1.0),
            sample(60_000, 2.0),
            sample(60_000, 3.0),
        ];
        let w = window(0, 300_000, 300_000);
        assert_eq!(deriv(&samples, w), None);
        assert_eq!(predict_linear(&samples, w, 60.0), None);
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

    #[test]
    fn irate_hist_takes_last_two_and_divides_by_interval() {
        // Two histogram samples 30s apart, counts 2 then 8, no reset. irate
        // takes the raw difference (count 6, sum 30) and divides by the 30s
        // interval in seconds: 6/30 and 30/30. Result is gauge-typed.
        let samples = [
            (ms(0), nh_counter(2.0, 10.0)),
            (ms(30_000), nh_counter(8.0, 40.0)),
        ];
        let got = instant_value_hist(&samples, true).expect("2 samples");
        assert_eq!(got.count.to_bits(), (6.0_f64 / 30.0).to_bits());
        assert_eq!(got.sum.to_bits(), (30.0_f64 / 30.0).to_bits());
        assert_eq!(got.counter_reset_hint, histogram::ResetHint::Gauge);
    }

    #[test]
    fn idelta_hist_is_the_raw_difference_without_division_or_reset_detection() {
        // idelta does not divide and does not detect resets: last - previous,
        // even when the counter went down.
        let samples = [
            (ms(0), nh_counter(8.0, 40.0)),
            (ms(30_000), nh_counter(2.0, 10.0)),
        ];
        let got = instant_value_hist(&samples, false).expect("2 samples");
        assert_eq!(got.count.to_bits(), (2.0_f64 - 8.0).to_bits());
        assert_eq!(got.sum.to_bits(), (10.0_f64 - 40.0).to_bits());
    }

    #[test]
    fn irate_idelta_hist_fewer_than_two_or_zero_interval_is_none() {
        let one = [(ms(0), nh_counter(1.0, 1.0))];
        assert!(instant_value_hist(&one, true).is_none());
        let same_ts = [
            (ms(10_000), nh_counter(1.0, 1.0)),
            (ms(10_000), nh_counter(2.0, 2.0)),
        ];
        assert!(instant_value_hist(&same_ts, true).is_none());
        assert!(instant_value_hist(&same_ts, false).is_none());
    }

    #[test]
    fn resets_and_changes_over_histograms_count_transitions() {
        // counts 2 -> 8 -> 5: one reset (the 8->5 shrink) and two changes (both
        // adjacent pairs differ).
        let samples = [
            (ms(0), nh_counter(2.0, 10.0)),
            (ms(30_000), nh_counter(8.0, 40.0)),
            (ms(60_000), nh_counter(5.0, 25.0)),
        ];
        assert_eq!(resets_hist(&samples), 1.0, "one downward step 8->5");
        assert_eq!(changes_hist(&samples), 2.0, "both adjacent pairs differ");

        // A constant series has no resets and no changes.
        let constant = [
            (ms(0), nh_counter(3.0, 12.0)),
            (ms(30_000), nh_counter(3.0, 12.0)),
        ];
        assert_eq!(resets_hist(&constant), 0.0);
        assert_eq!(changes_hist(&constant), 0.0);
    }

    /// #1136: two DISTINCT timestamps less than one millisecond apart used
    /// to floor to a zero-length interval (`ns_to_ms` truncates), so
    /// `sampled_interval == 0.0` while `last.ts_ns != first.ts_ns`: the
    /// equal-timestamp guard did not fire and every division below produced
    /// NaN (`rate`/`increase`/`delta`/`deriv`/`predict_linear`) or Inf
    /// (`irate`/`idelta`). Demonstrated failing against the pre-fix code by
    /// reverting `ns_diff_to_seconds` to `ms_to_seconds(ns_to_ms(a - b))`
    /// and the two inlined ms-based divisions this test also exercises (in
    /// `instant_value` and `linear_regression`'s `x` computation): every
    /// `assert!(...is_finite())` below then fails.
    #[test]
    fn sub_millisecond_windows_never_yield_nan_or_inf() {
        check_sub_millisecond_pair(0, 500_000); // 500 microseconds apart
        check_sub_millisecond_pair(0, 1); // 1 nanosecond apart: the tightest possible gap
    }

    fn check_sub_millisecond_pair(first_ts: i64, last_ts: i64) {
        let samples = [sample_ns(first_ts, 1.0), sample_ns(last_ts, 2.0)];
        let range_ns = last_ts - first_ts;
        let w = window_ns(first_ts, last_ts, range_ns, last_ts);
        let interval_s = range_ns as f64 / 1_000_000_000.0;

        let got_rate = rate(&samples, w).expect("2 distinct-timestamp samples");
        assert!(got_rate.is_finite(), "rate must not be NaN/Inf: {got_rate}");
        assert_eq!(got_rate, 1.0 / interval_s);

        let got_increase = increase(&samples, w).expect("2 distinct-timestamp samples");
        assert!(got_increase.is_finite(), "increase must not be NaN/Inf");
        assert_eq!(got_increase, 1.0);

        let got_delta = delta(&samples, w).expect("2 distinct-timestamp samples");
        assert!(got_delta.is_finite(), "delta must not be NaN/Inf");
        assert_eq!(got_delta, 1.0);

        let got_irate = irate(&samples, w).expect("2 distinct-timestamp samples");
        assert!(
            got_irate.is_finite(),
            "irate must not be NaN/Inf: {got_irate}"
        );
        assert_eq!(got_irate, 1.0 / interval_s);

        let got_idelta = idelta(&samples, w).expect("2 distinct-timestamp samples");
        assert!(got_idelta.is_finite(), "idelta must not be NaN/Inf");
        assert_eq!(got_idelta, 1.0);

        let got_deriv = deriv(&samples, w).expect("2 distinct-timestamp samples");
        assert!(
            got_deriv.is_finite(),
            "deriv must not be NaN/Inf: {got_deriv}"
        );
        let x0 = 0.0_f64;
        let x1 = (last_ts - first_ts) as f64 / 1_000_000_000.0;
        let expected_deriv = {
            let sum_x = x0 + x1;
            let sum_y = 1.0 + 2.0;
            let sum_xy = x0 * 1.0 + x1 * 2.0;
            let sum_x2 = x0 * x0 + x1 * x1;
            let n = 2.0;
            let cov_xy = sum_xy - sum_x * sum_y / n;
            let var_x = sum_x2 - sum_x * sum_x / n;
            cov_xy / var_x
        };
        assert_eq!(got_deriv, expected_deriv);

        let got_predict = predict_linear(&samples, w, 1.0).expect("2 distinct-timestamp samples");
        assert!(
            got_predict.is_finite(),
            "predict_linear must not be NaN/Inf: {got_predict}"
        );
        let x0p = (first_ts - last_ts) as f64 / 1_000_000_000.0;
        let x1p = 0.0_f64;
        let expected_predict = {
            let sum_x = x0p + x1p;
            let sum_y = 1.0 + 2.0;
            let sum_xy = x0p * 1.0 + x1p * 2.0;
            let sum_x2 = x0p * x0p + x1p * x1p;
            let n = 2.0;
            let cov_xy = sum_xy - sum_x * sum_y / n;
            let var_x = sum_x2 - sum_x * sum_x / n;
            let slope = cov_xy / var_x;
            let intercept = sum_y / n - slope * sum_x / n;
            slope * 1.0 + intercept
        };
        assert_eq!(got_predict, expected_predict);
    }

    /// #1136: the new nanosecond-direct interval formula must not move any
    /// conformance row for millisecond-quantized timestamps (everything
    /// Prometheus' own storage produces). This pins the algebraic argument
    /// in the module doc with a bit-for-bit check against the old two-step
    /// (nanoseconds -> milliseconds -> seconds) formula, reusing the
    /// millisecond-quantized `(a, b)` pairs that already drive
    /// `rate_extrapolates_to_the_window_boundary` and
    /// `increase_compensates_for_a_counter_reset` above.
    #[test]
    fn ms_quantized_inputs_are_unchanged() {
        fn old_ns_diff_to_seconds(a: i64, b: i64) -> f64 {
            ((a - b) / 1_000_000) as f64 / 1000.0
        }

        let pairs = [
            (ms(180_000), 0),
            (ms(180_000), ms(60_000)),
            (ms(120_000), ms(0)),
            (ms(150_000), ms(90_000)),
            (ms(65_000), ms(5_000)),
            (ms(120_000), ms(120_000)),
            (ms(300_000), ms(0)),
        ];
        for (a, b) in pairs {
            let old = old_ns_diff_to_seconds(a, b);
            let new = ns_diff_to_seconds(a, b);
            assert_eq!(
                old.to_bits(),
                new.to_bits(),
                "ns_diff_to_seconds({a}, {b}): old={old:?} new={new:?}"
            );
        }

        // End-to-end: the same millisecond-quantized samples used by
        // `rate_extrapolates_to_the_window_boundary` and
        // `increase_compensates_for_a_counter_reset`, checked bit-for-bit
        // against those tests' own already-pinned expected values.
        let samples = [
            sample(0, 10.0),
            sample(60_000, 20.0),
            sample(120_000, 30.0),
            sample(180_000, 40.0),
        ];
        let w = window(0, 180_000, 180_000);
        let got = rate(&samples, w).expect("2+ samples");
        assert_eq!(got.to_bits(), (30.0_f64 / 180.0).to_bits());

        let samples = [sample(0, 0.0), sample(60_000, 10.0), sample(120_000, 2.0)];
        let w = window(0, 120_000, 120_000);
        let got = increase(&samples, w).expect("2+ samples");
        assert_eq!(got.to_bits(), 12.0_f64.to_bits());

        let samples = [sample(0, 10.0), sample(10_000, 20.0), sample(20_000, 30.0)];
        let got = deriv(&samples, window(0, 20_000, 20_000)).expect("2+ samples");
        assert_eq!(got.to_bits(), 1.0_f64.to_bits());

        let samples = [sample(0, 10.0), sample(10_000, 20.0), sample(20_000, 30.0)];
        let w = window(0, 20_000, 20_000);
        let got = predict_linear(&samples, w, 30.0).expect("2+ samples");
        assert_eq!(got.to_bits(), 60.0_f64.to_bits());

        let samples = [sample(0, 100.0), sample(30_000, 5.0), sample(60_000, 25.0)];
        let got = irate(&samples, window(0, 60_000, 300_000)).expect("2+ samples");
        assert_eq!(got.to_bits(), (20.0_f64 / 30.0).to_bits());
    }

    /// #1136: two histogram samples 500 microseconds apart is the native-
    /// histogram counterpart of `sub_millisecond_windows_never_yield_nan_or_inf`
    /// above. Demonstrated failing the same way: pre-fix, `histogram_extrapolated_rate`
    /// guarded on `last_ts == first_ts` while dividing by a `sampled_interval`
    /// that floored to zero for this pair, producing NaN buckets via `mul`.
    #[test]
    fn histogram_rate_over_sub_millisecond_window_is_finite() {
        let samples = [
            (0i64, nh_counter(2.0, 10.0)),
            (500_000i64, nh_counter(6.0, 30.0)),
        ];
        let w = window_ns(0, 500_000, 500_000, 500_000);
        let got = rate_hist(&samples, w).expect("2 histogram samples, distinct timestamps");
        assert!(got.count.is_finite(), "count must not be NaN/Inf");
        assert!(got.sum.is_finite(), "sum must not be NaN/Inf");
        for b in &got.positive_buckets {
            assert!(b.is_finite(), "positive bucket must not be NaN/Inf");
        }
        for b in &got.negative_buckets {
            assert!(b.is_finite(), "negative bucket must not be NaN/Inf");
        }
    }
}
