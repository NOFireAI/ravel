//! `_over_time` aggregation family: `avg`, `min`, `max`,
//! `sum`, `count`, `stddev`, `stdvar`, `quantile`, `present`, `last`, and
//! `absent` `_over_time`. Every accumulation is a direct, bit-exact-oriented
//! port of Prometheus' own `promql/functions.go` and `promql/quantile.go`:
//! Kahan-compensated summation (`kahanSumInc`) for `sum`/`avg_over_time`,
//! Welford's online variance (also Kahan-compensated on both the running
//! mean and the running sum of squared deviations) for `stddev`/
//! `stdvar_over_time`, and the same NaN-sorts-smallest, edge-clamped
//! quantile helper `quantile_over_time` shares with `quantile()` the
//! aggregation operator.
//!
//! `min`/`max_over_time` use plain IEEE `<`/`>` (never bit-pattern
//! equality), seeded from the window's first sample and re-scanning every
//! sample including that first one: a NaN accumulator is always overwritten
//! by the next non-NaN sample, so the result is only NaN when every sample
//! in the window is NaN, and a `-0.0` never displaces an already-seen
//! `0.0` (`-0.0 < 0.0` is false), matching Prometheus' own comparison
//! exactly, tie quirk included.

use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample};

use crate::eval::{Error, Evaluator, InstantSample, InstantVector, QueryWindow, RangeMatrix};
use crate::source::SeriesSource;

use super::{FunctionDef, FunctionKind, MatrixArg, RangeWindow};

pub(crate) const FUNCTIONS: &[FunctionDef] = &[
    FunctionDef {
        name: "avg_over_time",
        kind: FunctionKind::RangeVector(avg_over_time),
    },
    FunctionDef {
        name: "min_over_time",
        kind: FunctionKind::RangeVector(min_over_time),
    },
    FunctionDef {
        name: "max_over_time",
        kind: FunctionKind::RangeVector(max_over_time),
    },
    FunctionDef {
        name: "sum_over_time",
        kind: FunctionKind::RangeVector(sum_over_time),
    },
    FunctionDef {
        name: "count_over_time",
        kind: FunctionKind::RangeVector(count_over_time),
    },
    FunctionDef {
        name: "stddev_over_time",
        kind: FunctionKind::RangeVector(stddev_over_time),
    },
    FunctionDef {
        name: "stdvar_over_time",
        kind: FunctionKind::RangeVector(stdvar_over_time),
    },
    FunctionDef {
        name: "quantile_over_time",
        kind: FunctionKind::ScalarRangeVector(quantile_over_time),
    },
    FunctionDef {
        name: "present_over_time",
        kind: FunctionKind::RangeVector(present_over_time),
    },
    FunctionDef {
        name: "last_over_time",
        kind: FunctionKind::RangeVector(last_over_time),
    },
    FunctionDef {
        name: "absent_over_time",
        kind: FunctionKind::AbsentOverTime,
    },
];

fn sum_over_time(samples: &[Sample], _w: RangeWindow) -> Option<f64> {
    let mut sum = 0.0_f64;
    let mut c = 0.0_f64;
    for s in samples {
        (sum, c) = kahan_sum_inc(s.value, sum, c);
    }
    Some(if sum.is_infinite() { sum } else { sum + c })
}

/// Port of Prometheus' `funcAvgOverTime`: an incremental mean, each step
/// re-based as `f/count - mean/count` and folded through the same Kahan
/// compensation as `sum_over_time`, rather than `sum_over_time(...) /
/// count`, so a window whose plain sum would overflow to `+-Inf` still
/// averages correctly. Once the running mean has gone infinite, a further
/// sample is skipped (not folded in) whenever it cannot change the sign or
/// finiteness of that infinity: a same-signed infinite sample, or any
/// finite, non-NaN sample. A NaN sample, or an oppositely-signed infinite
/// sample, still gets folded in, which is what brings the mean back out of
/// its infinite state (to NaN).
fn avg_over_time(samples: &[Sample], _w: RangeWindow) -> Option<f64> {
    let mut mean = 0.0_f64;
    let mut c = 0.0_f64;
    let mut count = 0.0_f64;
    for s in samples {
        count += 1.0;
        if mean.is_infinite() {
            let same_sign_infinite = s.value.is_infinite() && (mean > 0.0) == (s.value > 0.0);
            let finite_and_not_nan = !s.value.is_infinite() && !s.value.is_nan();
            if same_sign_infinite || finite_and_not_nan {
                continue;
            }
        }
        (mean, c) = kahan_sum_inc(s.value / count - mean / count, mean, c);
    }
    Some(if mean.is_infinite() { mean } else { mean + c })
}

fn count_over_time(samples: &[Sample], _w: RangeWindow) -> Option<f64> {
    Some(samples.len() as f64)
}

fn min_over_time(samples: &[Sample], _w: RangeWindow) -> Option<f64> {
    let mut min = samples[0].value;
    for s in samples {
        if s.value < min || min.is_nan() {
            min = s.value;
        }
    }
    Some(min)
}

fn max_over_time(samples: &[Sample], _w: RangeWindow) -> Option<f64> {
    let mut max = samples[0].value;
    for s in samples {
        if s.value > max || max.is_nan() {
            max = s.value;
        }
    }
    Some(max)
}

fn stddev_over_time(samples: &[Sample], _w: RangeWindow) -> Option<f64> {
    Some(welford_variance(samples).sqrt())
}

fn stdvar_over_time(samples: &[Sample], _w: RangeWindow) -> Option<f64> {
    Some(welford_variance(samples))
}

/// Port of Prometheus' shared `stddev`/`stdvar_over_time` accumulator:
/// Welford's online variance, with Kahan compensation folded into both the
/// running mean and the running sum of squared deviations (`m2`), each
/// updated from the same pre-update `delta` (the sample minus the
/// *previous* compensated mean).
fn welford_variance(samples: &[Sample]) -> f64 {
    let mut count = 0.0_f64;
    let mut mean = 0.0_f64;
    let mut c_mean = 0.0_f64;
    let mut m2 = 0.0_f64;
    let mut c_m2 = 0.0_f64;
    for s in samples {
        count += 1.0;
        let delta = s.value - (mean + c_mean);
        (mean, c_mean) = kahan_sum_inc(delta / count, mean, c_mean);
        (m2, c_m2) = kahan_sum_inc(delta * (s.value - (mean + c_mean)), m2, c_m2);
    }
    (m2 + c_m2) / count
}

fn present_over_time(_samples: &[Sample], _w: RangeWindow) -> Option<f64> {
    Some(1.0)
}

fn last_over_time(samples: &[Sample], _w: RangeWindow) -> Option<f64> {
    Some(samples[samples.len() - 1].value)
}

fn quantile_over_time(q: f64, samples: &[Sample], _w: RangeWindow) -> Option<f64> {
    let values: Vec<f64> = samples.iter().map(|s| s.value).collect();
    Some(quantile(q, values))
}

/// Port of Prometheus' `quantile()` (`promql/quantile.go`), shared by the
/// `quantile` aggregation operator and `quantile_over_time`: `q < 0` and
/// `q > 1` clamp to `-Inf`/`+Inf` rather than erroring (Prometheus emits a
/// warning annotation alongside; `quantile_over_time`'s dispatch raises the
/// matching `InvalidQuantileWarning` via
/// [`super::maybe_warn_invalid_quantile`], since this pure numeric helper has
/// no annotation channel of its own), `q` itself being NaN produces NaN, and
/// otherwise linear
/// interpolation between the two nearest ranks after sorting with NaN
/// values treated as smaller than every real number.
pub(crate) fn quantile(q: f64, mut values: Vec<f64>) -> f64 {
    if values.is_empty() {
        return f64::NAN;
    }
    if q.is_nan() {
        return f64::NAN;
    }
    if q < 0.0 {
        return f64::NEG_INFINITY;
    }
    if q > 1.0 {
        return f64::INFINITY;
    }
    values.sort_by(|a, b| {
        if a.is_nan() {
            return std::cmp::Ordering::Less;
        }
        if b.is_nan() {
            return std::cmp::Ordering::Greater;
        }
        a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
    });

    let n = values.len() as f64;
    let rank = q * (n - 1.0);
    let lower_index = rank.floor().max(0.0);
    let upper_index = (lower_index + 1.0).min(n - 1.0);
    let weight = rank - rank.floor();
    let lo = values[lower_index as usize];
    let hi = values[upper_index as usize];
    lo * (1.0 - weight) + hi * weight
}

/// Port of Prometheus' `kahanSumInc`: adds `inc` to `sum` (compensated by
/// running error term `c`), returning the new `(sum, c)` pair. The result
/// itself is `sum`; `c` is folded back in by the caller only once, after
/// the whole window has been accumulated (adding it at every step would
/// just re-derive plain summation).
pub(crate) fn kahan_sum_inc(inc: f64, sum: f64, c: f64) -> (f64, f64) {
    let t = sum + inc;
    let new_c = if t.is_infinite() {
        0.0
    } else if sum.abs() >= inc.abs() {
        c + ((sum - t) + inc)
    } else {
        c + ((inc - t) + sum)
    };
    (t, new_c)
}

/// `absent_over_time` at one instant: unlike every other function in this
/// family, it is not a per-series reduction of `matrix`'s rows. It reports
/// whether the whole range vector argument matched nothing at all, and if
/// so synthesizes exactly one output series from `labels`
/// ([`matrix_arg_absent_labels`]).
/// `histogram_present`: whether native-histogram data also matched the
/// same window (issue #525). `matrix` only ever carries float samples
/// (`eval_matrix_arg`'s `Selector` case is float-only), so without this a
/// series reporting purely through native histograms would look absent and
/// fire a false liveness alert while its data keeps flowing -- wrong under
/// either of #524's two policies (typed rejection would ALSO be wrong
/// here: a liveness check that errors on live histogram data is just as
/// broken as one that misreports it absent), so this is a presence fix,
/// not a guard.
pub(crate) fn absent_over_time_instant(
    labels: LabelSet,
    matrix: RangeMatrix,
    eval_ts_ns: i64,
    histogram_present: bool,
) -> InstantVector {
    if !matrix.is_empty() || histogram_present {
        return Vec::new();
    }
    vec![InstantSample {
        labels,
        ts_ns: eval_ts_ns,
        orig_sample_ts_ns: eval_ts_ns,
        value: 1.0,
        histogram: None,
    }]
}

/// `absent_over_time` over a range query's grid: absence is evaluated
/// independently at each step (exactly like every other range-call
/// function), so the single synthesized output series carries a sample
/// only at the steps whose window matched nothing. This re-queries the
/// matrix-typed argument once per grid step rather than sharing
/// [`Evaluator::eval_range_matrix_reduction`]'s single-query cursor, since
/// that helper is built around reducing each matched series' own rows and
/// has no notion of a synthetic series representing the matrix's
/// emptiness; for a subquery argument there is no single raw sample stream
/// to cursor over in the first place.
pub(crate) fn absent_over_time_range(
    evaluator: &Evaluator,
    source: &dyn SeriesSource,
    arg: MatrixArg,
    start_ns: i64,
    end_ns: i64,
    step_ns: i64,
    ctx: &QueryWindow,
) -> Result<RangeMatrix, Error> {
    let labels = matrix_arg_absent_labels(arg);
    let mut out_samples = Vec::new();
    let mut t = start_ns;
    while t <= end_ns {
        // `histogram_present`: see `absent_over_time_instant`'s doc comment
        // (issue #525) -- native-histogram data at this step's window
        // counts as present, never as absent, regardless of whether the
        // (float-only) `matrix` below matched anything.
        let (matrix, histogram_present) = match arg {
            MatrixArg::Selector(ms) => {
                let matrix = evaluator.eval_matrix_selector(source, ms, t, ctx)?;
                let matchers = crate::eval::build_matchers(&ms.vs)?;
                let sel_ts_ns = crate::eval::selector_eval_ts(&ms.vs, t, ctx)?;
                let range_ns = crate::eval::duration_to_ns(ms.range)?;
                let window_start = sel_ts_ns.checked_sub(range_ns).ok_or(Error::TimeOverflow)?;
                let histogram_present = crate::eval::has_histogram_samples(
                    source,
                    &matchers,
                    ravel_types::TimeRange {
                        start_ns: window_start,
                        end_ns: sel_ts_ns,
                    },
                )?;
                (matrix, histogram_present)
            }
            MatrixArg::Subquery(sq) => (evaluator.eval_subquery_matrix(source, sq, t, ctx)?, false),
        };
        if matrix.is_empty() && !histogram_present {
            out_samples.push(Sample {
                ts_ns: t,
                value: 1.0,
            });
        }
        t = t.checked_add(step_ns).ok_or(Error::TimeOverflow)?;
    }
    if out_samples.is_empty() {
        Ok(Vec::new())
    } else {
        Ok(vec![(labels, out_samples)])
    }
}

/// Derive `absent_over_time`'s synthesized label set for a matrix-typed
/// argument ([`matrix_arg`](super::matrix_arg)'s result): a matrix selector
/// derives it from its own selector's equality matchers, exactly Prometheus'
/// `createLabelsForAbsentFunction` ([`absent_labels`]). Prometheus'
/// `createLabelsForAbsentFunction` has no subquery arm at all; a subquery
/// argument falls through to its default, an empty label set, regardless of
/// what the subquery's inner expression looks like.
pub(crate) fn matrix_arg_absent_labels(arg: MatrixArg) -> LabelSet {
    match arg {
        MatrixArg::Selector(ms) => absent_labels(&ms.vs),
        MatrixArg::Subquery(_) => LabelSet::default(),
    }
}

/// Port of Prometheus' `createLabelsForAbsentFunction`: walk the selector's
/// matchers left to right, keeping a name set to the value of an equality
/// matcher only while no later matcher (equal or not) has touched that same
/// name since it was last set. Concretely: an equality matcher sets the
/// name only if it is not currently set; any other matcher (or a repeated
/// equality matcher for an already-set name) clears it. A name can be set,
/// cleared, and set again by a later matcher, so this is not simply "first
/// equality wins" — it is exactly the builder replay Prometheus performs.
/// `__name__` is always skipped.
fn absent_labels(vs: &promql_parser::parser::VectorSelector) -> LabelSet {
    use promql_parser::label::MatchOp;
    let mut present: Vec<(String, String)> = Vec::new();
    for m in &vs.matchers.matchers {
        if m.name == METRIC_NAME_LABEL {
            continue;
        }
        let already_set = present.iter().any(|(name, _)| *name == m.name);
        if matches!(m.op, MatchOp::Equal) && !already_set {
            present.push((m.name.clone(), m.value.clone()));
        } else {
            present.retain(|(name, _)| *name != m.name);
        }
    }
    let labels: Vec<Label> = present
        .into_iter()
        .map(|(name, value)| Label { name, value })
        .collect();
    LabelSet::new(labels).unwrap_or_default()
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
    fn sum_over_time_adds_every_sample() {
        let samples = [sample(0, 1.0), sample(1_000, 2.0), sample(2_000, 3.5)];
        let got = sum_over_time(&samples, window(0, 2_000, 2_000)).expect("non-empty");
        assert_eq!(got, 6.5);
    }

    #[test]
    fn sum_over_time_of_all_negative_zero_is_positive_zero() {
        // Kahan summation starts from a plain `0.0` accumulator, same as
        // Prometheus' own `var sum float64`; `0.0 + (-0.0)` is `0.0` in
        // IEEE 754, so the sum of any number of `-0.0` samples is `0.0`,
        // never `-0.0`, bit-exactly matching Prometheus.
        let samples = [sample(0, -0.0), sample(1_000, -0.0)];
        let got = sum_over_time(&samples, window(0, 1_000, 1_000)).expect("non-empty");
        assert_eq!(got.to_bits(), 0.0_f64.to_bits());
    }

    #[test]
    fn sum_over_time_kahan_compensation_recovers_precision_a_plain_sum_loses() {
        // A huge value followed by many small increments: naive summation
        // (`sum += inc` with no compensation) loses every increment to
        // rounding once `sum` dwarfs `inc`; Kahan compensation recovers it.
        let mut samples = vec![sample(0, 1.0e16)];
        for i in 1..=1000 {
            samples.push(sample(i, 1.0));
        }
        let got = sum_over_time(&samples, window(0, 1_000, 1_000)).expect("non-empty");
        assert_eq!(got, 1.0e16 + 1000.0);
    }

    #[test]
    fn avg_over_time_of_the_constant_series_is_that_constant() {
        let samples = [sample(0, 5.0), sample(1_000, 5.0), sample(2_000, 5.0)];
        let got = avg_over_time(&samples, window(0, 2_000, 2_000)).expect("non-empty");
        assert_eq!(got, 5.0);
    }

    #[test]
    fn avg_over_time_matches_hand_computed_mean() {
        let samples = [sample(0, 1.0), sample(1_000, 2.0), sample(2_000, 3.0)];
        let got = avg_over_time(&samples, window(0, 2_000, 2_000)).expect("non-empty");
        assert_eq!(got, 2.0);
    }

    #[test]
    fn avg_over_time_skips_finite_samples_once_the_mean_is_infinite() {
        // Mean goes to `+Inf` after the first sample; the second, finite
        // sample must be skipped (not folded in, which would produce
        // `Inf/2 + finite/2`, still `Inf` but via a different, non-exact
        // path) rather than changing the result. `+Inf` is the exact
        // expected output either way, so this pins the *skip*, not just the
        // final value: a same-signed-infinite third sample must also still
        // be skipped, and does not flip the mean to NaN.
        let samples = [
            sample(0, f64::INFINITY),
            sample(1_000, 1.0),
            sample(2_000, f64::INFINITY),
        ];
        let got = avg_over_time(&samples, window(0, 2_000, 2_000)).expect("non-empty");
        assert_eq!(got, f64::INFINITY);
    }

    #[test]
    fn avg_over_time_of_opposite_signed_infinities_is_nan() {
        let samples = [sample(0, f64::INFINITY), sample(1_000, f64::NEG_INFINITY)];
        let got = avg_over_time(&samples, window(0, 1_000, 1_000)).expect("non-empty");
        assert!(got.is_nan());
    }

    #[test]
    fn count_over_time_counts_samples_not_the_window_duration() {
        let samples = [sample(0, 1.0), sample(1_000, 2.0)];
        assert_eq!(
            count_over_time(&samples, window(0, 300_000, 300_000)),
            Some(2.0)
        );
    }

    #[test]
    fn min_over_time_skips_nan_unless_every_sample_is_nan() {
        let samples = [sample(0, f64::NAN), sample(1_000, 3.0), sample(2_000, 1.0)];
        let got = min_over_time(&samples, window(0, 2_000, 2_000)).expect("non-empty");
        assert_eq!(got, 1.0);

        let all_nan = [sample(0, f64::NAN), sample(1_000, f64::NAN)];
        let got = min_over_time(&all_nan, window(0, 1_000, 1_000)).expect("non-empty");
        assert!(got.is_nan());
    }

    #[test]
    fn min_over_time_does_not_let_negative_zero_displace_a_seen_positive_zero() {
        // Plain IEEE `<`, not bit-pattern comparison: `-0.0 < 0.0` is
        // false, so once `0.0` has been seen as the running minimum, a
        // later `-0.0` sample does not replace it.
        let samples = [sample(0, 0.0), sample(1_000, -0.0)];
        let got = min_over_time(&samples, window(0, 1_000, 1_000)).expect("non-empty");
        assert_eq!(got.to_bits(), 0.0_f64.to_bits());
    }

    #[test]
    fn max_over_time_skips_nan_unless_every_sample_is_nan() {
        let samples = [sample(0, f64::NAN), sample(1_000, 3.0), sample(2_000, 5.0)];
        let got = max_over_time(&samples, window(0, 2_000, 2_000)).expect("non-empty");
        assert_eq!(got, 5.0);

        let all_nan = [sample(0, f64::NAN)];
        let got = max_over_time(&all_nan, window(0, 0, 300_000)).expect("non-empty");
        assert!(got.is_nan());
    }

    #[test]
    fn stdvar_and_stddev_over_time_match_hand_computed_population_variance() {
        // 2, 4, 4, 4, 5, 5, 7, 9: population variance is 4, stddev 2
        // (textbook Welford example).
        let values = [2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0];
        let samples: Vec<Sample> = values
            .iter()
            .enumerate()
            .map(|(i, v)| sample(i as i64 * 1_000, *v))
            .collect();
        let w = window(0, 7_000, 7_000);
        let variance = stdvar_over_time(&samples, w).expect("non-empty");
        assert_eq!(variance, 4.0);
        let stddev = stddev_over_time(&samples, w).expect("non-empty");
        assert_eq!(stddev, 2.0);
    }

    #[test]
    fn stdvar_over_time_of_a_single_sample_is_zero() {
        let samples = [sample(0, 42.0)];
        let got = stdvar_over_time(&samples, window(0, 0, 300_000)).expect("non-empty");
        assert_eq!(got, 0.0);
    }

    #[test]
    fn present_over_time_is_always_one_for_a_non_empty_window() {
        let samples = [sample(0, f64::NAN)];
        assert_eq!(
            present_over_time(&samples, window(0, 0, 300_000)),
            Some(1.0)
        );
    }

    #[test]
    fn last_over_time_returns_the_final_sample_in_window_order() {
        let samples = [sample(0, 1.0), sample(1_000, 2.0), sample(2_000, 3.0)];
        let got = last_over_time(&samples, window(0, 2_000, 2_000)).expect("non-empty");
        assert_eq!(got, 3.0);
    }

    #[test]
    fn quantile_over_time_interpolates_between_ranks() {
        let samples = [
            sample(0, 1.0),
            sample(1_000, 2.0),
            sample(2_000, 3.0),
            sample(3_000, 4.0),
        ];
        let w = window(0, 3_000, 3_000);
        // rank = 0.5 * 3 = 1.5 -> interpolate halfway between index 1 (2.0)
        // and index 2 (3.0).
        let got = quantile_over_time(0.5, &samples, w).expect("non-empty");
        assert_eq!(got, 2.5);
        // Exact rank (q=0 -> index 0, q=1 -> last index): no interpolation.
        assert_eq!(quantile_over_time(0.0, &samples, w), Some(1.0));
        assert_eq!(quantile_over_time(1.0, &samples, w), Some(4.0));
    }

    #[test]
    fn quantile_over_time_out_of_range_q_clamps_to_infinity() {
        let samples = [sample(0, 1.0)];
        let w = window(0, 0, 300_000);
        assert_eq!(
            quantile_over_time(-0.5, &samples, w),
            Some(f64::NEG_INFINITY)
        );
        assert_eq!(quantile_over_time(1.5, &samples, w), Some(f64::INFINITY));
    }

    #[test]
    fn quantile_over_time_of_nan_q_is_nan() {
        let samples = [sample(0, 1.0)];
        let w = window(0, 0, 300_000);
        assert!(
            quantile_over_time(f64::NAN, &samples, w)
                .expect("non-empty")
                .is_nan()
        );
    }

    #[test]
    fn quantile_over_time_sorts_nan_samples_as_smaller_than_every_real_number() {
        let samples = [sample(0, f64::NAN), sample(1_000, 1.0), sample(2_000, 2.0)];
        let w = window(0, 2_000, 2_000);
        // Sorted: [NaN, 1.0, 2.0]. q=0 selects the smallest rank, the NaN.
        assert!(
            quantile_over_time(0.0, &samples, w)
                .expect("non-empty")
                .is_nan()
        );
        // q=1 selects the largest rank, unaffected by the NaN sorting first.
        assert_eq!(quantile_over_time(1.0, &samples, w), Some(2.0));
    }

    fn matchers_of(query: &str) -> promql_parser::parser::VectorSelector {
        let expr = promql_parser::parser::parse(query).expect("valid query");
        match expr {
            promql_parser::parser::Expr::MatrixSelector(ms) => ms.vs,
            promql_parser::parser::Expr::VectorSelector(vs) => vs,
            other => panic!("expected a selector, got {other:?}"),
        }
    }

    #[test]
    fn absent_labels_keeps_a_single_equality_matcher() {
        let vs = matchers_of(r#"up{job="api"}[5m]"#);
        let got = absent_labels(&vs);
        assert_eq!(got.iter().count(), 1);
        let label = got.iter().next().expect("one label");
        assert_eq!(label.name, "job");
        assert_eq!(label.value, "api");
    }

    #[test]
    fn absent_labels_drops_a_name_matched_more_than_once() {
        // Second matcher for `job`, even though it is also equality,
        // deletes the name entirely (Prometheus' builder replay: `Has`
        // is already true from the first `Set`, so the second matcher
        // falls into the `Del` branch).
        let vs = matchers_of(r#"up{job="api", job="web"}[5m]"#);
        let got = absent_labels(&vs);
        assert_eq!(got.iter().count(), 0);
    }

    #[test]
    fn absent_labels_can_reset_after_a_non_equal_matcher_clears_it() {
        // job=a (sets it) -> job=~b.* (Has(job) is true, so this is a Del,
        // not a second Set) -> job=c (Has(job) is now false again, so this
        // Set takes effect): final label is job=c.
        let vs = matchers_of(r#"up{job="a", job=~"b.*", job="c"}[5m]"#);
        let got = absent_labels(&vs);
        assert_eq!(got.iter().count(), 1);
        let label = got.iter().next().expect("one label");
        assert_eq!(label.name, "job");
        assert_eq!(label.value, "c");
    }

    #[test]
    fn absent_labels_skips_a_bare_regex_matcher_entirely() {
        let vs = matchers_of(r#"up{job=~"a.*"}[5m]"#);
        let got = absent_labels(&vs);
        assert_eq!(got.iter().count(), 0);
    }

    #[test]
    fn absent_labels_never_includes_the_metric_name() {
        let vs = matchers_of(r#"up{job="api"}[5m]"#);
        let got = absent_labels(&vs);
        assert!(got.iter().all(|l| l.name != METRIC_NAME_LABEL));
    }

    #[test]
    fn matrix_arg_absent_labels_is_always_empty_for_a_subquery() {
        // Prometheus' `createLabelsForAbsentFunction` has no subquery arm at
        // all, so any subquery argument (whatever its inner expression looks
        // like) falls through to an empty label set, never the inner
        // selector's matchers.
        let sq = matchers_of_subquery(r#"nosuch{job="x"}[10m:1m]"#);
        let got = matrix_arg_absent_labels(MatrixArg::Subquery(&sq));
        assert_eq!(got.iter().count(), 0);
    }

    #[test]
    fn absent_over_time_over_a_subquery_returns_empty_label_set_end_to_end() {
        use crate::eval::Evaluator;
        use crate::testsource::TestSource;

        let source = TestSource::new();
        let result = Evaluator::new()
            .instant(&source, r#"absent_over_time(nosuch{job="x"}[10m:1m])"#, 0)
            .expect("evaluates");
        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0].labels.iter().count(),
            0,
            "absent_over_time over a subquery must not synthesize labels from \
             the inner selector's matchers"
        );
    }

    fn matchers_of_subquery(query: &str) -> promql_parser::parser::SubqueryExpr {
        let expr = promql_parser::parser::parse(query).expect("valid query");
        match expr {
            promql_parser::parser::Expr::Subquery(sq) => sq,
            other => panic!("expected a subquery, got {other:?}"),
        }
    }
}
