//! The exact-acceleration operator matrix of ADR-3694 decision 8, as a
//! differential test against the evaluator as it stands. Nothing here is
//! library code: every per-hour statistic and every combination is computed in
//! this file from the raw fixture samples, and the combined value is compared
//! with `Evaluator::instant` over the same window by bit pattern.
//!
//! A window `(eval - range, eval]` is split into hour segments
//! `(kH, (k+1)H]`, left-open like the matrix window itself; when the window is
//! not hour-aligned its partial edge hours are summarised from the raw samples
//! inside the window with the same statistic. The verdict of each row lives in
//! the "Exact acceleration matrix" section of `docs/query-engine.md`, which
//! names the test pinning it.
#![allow(clippy::expect_used)]

use ravel_promql::Evaluator;
use ravel_promql::histogram::{FloatHistogram, ResetHint, Span, histogram_quantile};
use ravel_promql::testsource::TestSource;

const MIN_NS: i64 = 60_000_000_000;
const HOUR_NS: i64 = 60 * MIN_NS;
/// The staleness marker bit pattern the evaluator drops from every matrix
/// selection (`eval.rs`, `STALE_NAN_BITS`).
const STALE_BITS: u64 = 0x7ff0_0000_0000_0002;
const NAN_A: u64 = 0x7ff8_0000_0000_0001;
const NAN_B: u64 = 0x7ff8_0000_0000_beef;

fn stale() -> f64 {
    f64::from_bits(STALE_BITS)
}

fn nan_a() -> f64 {
    f64::from_bits(NAN_A)
}

fn nan_b() -> f64 {
    f64::from_bits(NAN_B)
}

#[derive(Clone, Copy, Debug)]
struct Window {
    label: &'static str,
    eval_min: i64,
    range_min: i64,
}

impl Window {
    fn start_ns(self) -> i64 {
        (self.eval_min - self.range_min) * MIN_NS
    }

    fn end_ns(self) -> i64 {
        self.eval_min * MIN_NS
    }

    fn range_ns(self) -> i64 {
        self.range_min * MIN_NS
    }

    fn eval_ms(self) -> i64 {
        self.eval_min * 60_000
    }
}

const WINDOWS: &[Window] = &[
    Window {
        label: "(0m,180m]",
        eval_min: 180,
        range_min: 180,
    },
    Window {
        label: "(0m,120m]",
        eval_min: 120,
        range_min: 120,
    },
    Window {
        label: "(34m,167m]",
        eval_min: 167,
        range_min: 133,
    },
    Window {
        label: "(25m,50m]",
        eval_min: 50,
        range_min: 25,
    },
    Window {
        label: "(60m,120m]",
        eval_min: 120,
        range_min: 60,
    },
    Window {
        label: "(35m,95m]",
        eval_min: 95,
        range_min: 60,
    },
    Window {
        label: "(85m,95m]",
        eval_min: 95,
        range_min: 10,
    },
    Window {
        label: "(70m,130m]",
        eval_min: 130,
        range_min: 60,
    },
];

fn window(label: &str) -> Window {
    *WINDOWS
        .iter()
        .find(|w| w.label == label)
        .expect("known window label")
}

struct Fixture {
    name: &'static str,
    samples: Vec<(i64, f64)>,
}

fn at(minutes: &[(i64, f64)]) -> Vec<(i64, f64)> {
    minutes.iter().map(|&(m, v)| (m * MIN_NS, v)).collect()
}

fn float_fixtures() -> Vec<Fixture> {
    vec![
        Fixture {
            // Two distinct NaN payloads; hour (60m,120m] holds nothing else.
            name: "nan_payloads",
            samples: at(&[
                (10, 1.5),
                (30, nan_a()),
                (50, 2.5),
                (60, nan_b()),
                (70, nan_a()),
                (90, nan_b()),
                (120, nan_a()),
                (130, 3.0),
                (150, nan_b()),
                (170, 0.5),
            ]),
        },
        Fixture {
            // 0.0 and -0.0 on both sides of every hour boundary.
            name: "signed_zero",
            samples: at(&[
                (10, 0.0),
                (30, -0.0),
                (60, 0.0),
                (70, -0.0),
                (100, 0.0),
                (120, -0.0),
                (130, 1.0),
                (150, -0.0),
                (170, 0.0),
            ]),
        },
        Fixture {
            // Markers between a reset pair (3.0, 2.0) and between equal values
            // (5.0, 5.0); `(85m,95m]` holds the 90m marker and nothing else.
            name: "staleness",
            samples: at(&[
                (10, 1.0),
                (20, 3.0),
                (30, stale()),
                (40, 2.0),
                (60, 4.0),
                (70, 5.0),
                (90, stale()),
                (100, 5.0),
                (120, 7.0),
                (130, 8.0),
                (150, 9.0),
                (170, 10.0),
            ]),
        },
        Fixture {
            // A reset inside (0m,60m], one across the 60m boundary, and one
            // landing exactly on the 120m boundary.
            name: "resets",
            samples: at(&[
                (10, 10.1),
                (20, 20.2),
                (30, 5.3),
                (45, 15.4),
                (60, 25.7),
                (75, 3.3),
                (90, 13.9),
                (110, 23.1),
                (120, 2.2),
                (130, 12.7),
                (150, 22.9),
                (170, 33.1),
            ]),
        },
        Fixture {
            // Values whose floating-point sum depends on association.
            name: "rounding",
            samples: rounding_samples(),
        },
        Fixture {
            // Two counter resets inside (0m,60m].
            name: "two_resets_one_hour",
            samples: two_resets_samples(),
        },
    ]
}

fn rounding_samples() -> Vec<(i64, f64)> {
    let hours = [
        [0.1, 0.2, 0.2, 0.3],
        [99.9, 0.7, 0.3, 0.1],
        [1.1, 7.77, 0.1, 2.5],
    ];
    let mut out = Vec::new();
    for (h, values) in hours.iter().enumerate() {
        for (i, v) in values.iter().enumerate() {
            let minute = 60 * h as i64 + 15 * (i as i64 + 1);
            out.push((minute * MIN_NS, *v));
        }
    }
    out
}

fn two_resets_samples() -> Vec<(i64, f64)> {
    at(&[
        (10, 1.1),
        (20, 7.77),
        (30, 7.77),
        (40, 0.2),
        (50, 0.1),
        (60, 0.3),
        (90, 1.3),
        (120, 2.3),
    ])
}

fn fixture(name: &str) -> Fixture {
    float_fixtures()
        .into_iter()
        .find(|f| f.name == name)
        .expect("known fixture")
}

/// Whether the staleness marker is dropped while segmenting, as the
/// evaluator's matrix selection does, or kept as a sample (mutant (b)).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Stale {
    Drop,
    Keep,
}

/// The window's samples, split into left-open hour segments in time order.
fn hour_segments<T: Clone>(
    samples: &[(i64, T)],
    w: Window,
    is_stale: impl Fn(&T) -> bool,
    stale: Stale,
) -> Vec<Vec<(i64, T)>> {
    let mut segments: Vec<(i64, Vec<(i64, T)>)> = Vec::new();
    for (ts, v) in samples {
        if *ts <= w.start_ns() || *ts > w.end_ns() {
            continue;
        }
        if stale == Stale::Drop && is_stale(v) {
            continue;
        }
        let hour = (*ts - 1).div_euclid(HOUR_NS);
        match segments.last_mut() {
            Some((h, seg)) if *h == hour => seg.push((*ts, v.clone())),
            _ => segments.push((hour, vec![(*ts, v.clone())])),
        }
    }
    segments.into_iter().map(|(_, seg)| seg).collect()
}

fn float_segments(samples: &[(i64, f64)], w: Window, stale: Stale) -> Vec<Vec<(i64, f64)>> {
    hour_segments(samples, w, |v| v.to_bits() == STALE_BITS, stale)
}

/// Every per-hour statistic the float rows read. Each row's combination reads
/// only the fields its matrix row names.
#[derive(Clone, Debug)]
struct FloatHour {
    n: usize,
    first: (i64, f64),
    last: (i64, f64),
    /// The sample before `last`, when the hour holds two or more.
    before_last: Option<(i64, f64)>,
    /// The evaluator's own `min_over_time` fold over the hour.
    min: f64,
    /// The evaluator's own `max_over_time` fold over the hour.
    max: f64,
    /// Adjacent pairs inside the hour with `b < a`.
    resets: u64,
    /// Adjacent pairs inside the hour with `b != a`, two NaNs being equal.
    changes: u64,
    /// The `a` of every in-hour reset pair, summed in time order.
    correction: Option<f64>,
}

impl FloatHour {
    fn of(seg: &[(i64, f64)]) -> FloatHour {
        let n = seg.len();
        let mut min = seg[0].1;
        let mut max = seg[0].1;
        for &(_, v) in seg {
            if v < min || min.is_nan() {
                min = v;
            }
            if v > max || max.is_nan() {
                max = v;
            }
        }
        let mut resets = 0;
        let mut changes = 0;
        let mut correction: Option<f64> = None;
        for pair in seg.windows(2) {
            let (a, b) = (pair[0].1, pair[1].1);
            if b < a {
                resets += 1;
                correction = Some(correction.map_or(a, |c| c + a));
            }
            if b != a && !(a.is_nan() && b.is_nan()) {
                changes += 1;
            }
        }
        FloatHour {
            n,
            first: seg[0],
            last: seg[n - 1],
            before_last: n.checked_sub(2).map(|i| seg[i]),
            min,
            max,
            resets,
            changes,
            correction,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Float {
    Count,
    Min,
    Max,
    Last,
    Present,
    Absent,
    Resets,
    Changes,
    Irate,
    Idelta,
    Delta,
    Rate,
    Increase,
}

impl Float {
    fn query(self, w: Window) -> String {
        let name = match self {
            Float::Count => "count_over_time",
            Float::Min => "min_over_time",
            Float::Max => "max_over_time",
            Float::Last => "last_over_time",
            Float::Present => "present_over_time",
            Float::Absent => "absent_over_time",
            Float::Resets => "resets",
            Float::Changes => "changes",
            Float::Irate => "irate",
            Float::Idelta => "idelta",
            Float::Delta => "delta",
            Float::Rate => "rate",
            Float::Increase => "increase",
        };
        format!("{name}(m[{}m])", w.range_min)
    }
}

/// How the hours are combined: the combination the matrix documents, or one of
/// the wrong ones each EXACT row is shown to reject.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Combine {
    Exact,
    /// Mutant (a): a floating-point sum reassociated across hours. For
    /// `delta`/`rate`/`increase`, the per-hour deltas and boundary gaps
    /// summed (telescoped); for `count_over_time`, the hour counts summed as
    /// `f64`.
    Reassociate,
    /// `min`/`max` combined under `f64::total_cmp`, the ordering ADR-0103's
    /// amendment excluded from pushdown.
    TotalOrder,
    /// `resets`/`changes` without the pair straddling each hour boundary.
    DropBoundaryPairs,
    /// `irate`/`idelta` read from the final hour only.
    FinalHourOnly,
}

fn eval_float(samples: &[(i64, f64)], query: &str, w: Window) -> Option<f64> {
    let source = TestSource::new()
        .with_series(&[("__name__", "m")], samples)
        .expect("valid series");
    let result = Evaluator::new()
        .instant(&source, query, w.eval_ms())
        .expect("evaluates");
    assert!(result.len() <= 1, "{query}: one series in, at most one out");
    result.first().map(|s| s.value)
}

fn combine_float(f: Float, hours: &[FloatHour], w: Window, how: Combine) -> Option<f64> {
    if f == Float::Absent {
        return hours.is_empty().then_some(1.0);
    }
    let last_hour = hours.last()?;
    match f {
        Float::Count => Some(if how == Combine::Reassociate {
            hours.iter().fold(0.0, |acc, h| acc + h.n as f64)
        } else {
            hours.iter().map(|h| h.n).sum::<usize>() as f64
        }),
        Float::Min | Float::Max => {
            let values = hours
                .iter()
                .map(|h| if f == Float::Min { h.min } else { h.max });
            if how == Combine::TotalOrder {
                return if f == Float::Min {
                    values.min_by(f64::total_cmp)
                } else {
                    values.max_by(f64::total_cmp)
                };
            }
            let mut acc = hours[0].min;
            if f == Float::Max {
                acc = hours[0].max;
            }
            for v in values {
                let better = if f == Float::Min { v < acc } else { v > acc };
                if better || acc.is_nan() {
                    acc = v;
                }
            }
            Some(acc)
        }
        Float::Last => Some(last_hour.last.1),
        Float::Present => Some(1.0),
        Float::Resets | Float::Changes => {
            let mut total = 0u64;
            for (k, h) in hours.iter().enumerate() {
                total += if f == Float::Resets {
                    h.resets
                } else {
                    h.changes
                };
                if k == 0 || how == Combine::DropBoundaryPairs {
                    continue;
                }
                let (a, b) = (hours[k - 1].last.1, h.first.1);
                let counted = if f == Float::Resets {
                    b < a
                } else {
                    b != a && !(a.is_nan() && b.is_nan())
                };
                if counted {
                    total += 1;
                }
            }
            Some(total as f64)
        }
        Float::Irate | Float::Idelta => {
            let last = last_hour.last;
            let previous = match last_hour.before_last {
                Some(p) => p,
                None if how == Combine::FinalHourOnly || hours.len() < 2 => return None,
                None => hours[hours.len() - 2].last,
            };
            let is_rate = f == Float::Irate;
            let mut result = if is_rate && last.1 < previous.1 {
                last.1
            } else {
                last.1 - previous.1
            };
            let interval_ns = last.0 - previous.0;
            if interval_ns == 0 {
                return None;
            }
            if is_rate {
                result /= ns_to_seconds(interval_ns);
            }
            Some(result)
        }
        Float::Delta | Float::Rate | Float::Increase => {
            let is_counter = f != Float::Delta;
            let is_rate = f == Float::Rate;
            let first = hours[0].first;
            let last = last_hour.last;
            let n: usize = hours.iter().map(|h| h.n).sum();
            let result = if how == Combine::Reassociate {
                telescoped(hours, is_counter)
            } else {
                let mut result = last.1 - first.1;
                if is_counter {
                    // The evaluator compares the window's first sample with 0.0.
                    if first.1 < 0.0 {
                        result += 0.0;
                    }
                    for (k, h) in hours.iter().enumerate() {
                        if k > 0 && h.first.1 < hours[k - 1].last.1 {
                            result += hours[k - 1].last.1;
                        }
                        if let Some(c) = h.correction {
                            result += c;
                        }
                    }
                }
                result
            };
            extrapolate(first, last, n, result, w, is_counter, is_rate)
        }
        Float::Absent => None,
    }
}

/// Mutant (a) for the extrapolated-rate rows: each hour's own increase plus
/// the gap from the previous hour, summed across hours.
fn telescoped(hours: &[FloatHour], is_counter: bool) -> f64 {
    let mut total = 0.0;
    for (k, h) in hours.iter().enumerate() {
        let mut inc = h.last.1 - h.first.1;
        if k > 0 {
            let prev = hours[k - 1].last.1;
            let gap = if is_counter && h.first.1 < prev {
                h.first.1
            } else {
                h.first.1 - prev
            };
            inc += gap;
        }
        if is_counter && let Some(c) = h.correction {
            inc += c;
        }
        total += inc;
    }
    total
}

/// The boundary extrapolation of `extrapolated_rate` (`functions/rate.rs`),
/// fed from the combined first sample, last sample and sample count.
fn extrapolate(
    first: (i64, f64),
    last: (i64, f64),
    n: usize,
    mut result: f64,
    w: Window,
    is_counter: bool,
    is_rate: bool,
) -> Option<f64> {
    if n < 2 {
        return None;
    }
    let sampled_interval = ns_to_seconds(last.0 - first.0);
    if sampled_interval == 0.0 {
        return None;
    }
    let mut duration_to_start = ns_to_seconds(first.0 - w.start_ns());
    let duration_to_end = ns_to_seconds(w.end_ns() - last.0);
    let average = sampled_interval / (n - 1) as f64;
    if is_counter && result > 0.0 && first.1 >= 0.0 {
        let duration_to_zero = sampled_interval * (first.1 / result);
        if duration_to_zero < duration_to_start {
            duration_to_start = duration_to_zero;
        }
    }
    let threshold = average * 1.1;
    let mut extrapolate_to = sampled_interval;
    if duration_to_start < threshold {
        extrapolate_to += duration_to_start;
    } else {
        extrapolate_to += average / 2.0;
    }
    if duration_to_end < threshold {
        extrapolate_to += duration_to_end;
    } else {
        extrapolate_to += average / 2.0;
    }
    result *= extrapolate_to / sampled_interval;
    if is_rate {
        result /= ns_to_seconds(w.range_ns());
    }
    Some(result)
}

/// Window-factor-only variant of [`extrapolate`] for native histograms, which
/// skip the counter zero-floor clamp (`histogram_extrapolated_rate`).
fn histogram_factor(first_ns: i64, last_ns: i64, n: usize, w: Window, is_rate: bool) -> f64 {
    let sampled_interval = ns_to_seconds(last_ns - first_ns);
    let duration_to_start = ns_to_seconds(first_ns - w.start_ns());
    let duration_to_end = ns_to_seconds(w.end_ns() - last_ns);
    let average = sampled_interval / (n - 1) as f64;
    let threshold = average * 1.1;
    let mut extrapolate_to = sampled_interval;
    if duration_to_start < threshold {
        extrapolate_to += duration_to_start;
    } else {
        extrapolate_to += average / 2.0;
    }
    if duration_to_end < threshold {
        extrapolate_to += duration_to_end;
    } else {
        extrapolate_to += average / 2.0;
    }
    let mut factor = extrapolate_to / sampled_interval;
    if is_rate {
        factor /= ns_to_seconds(w.range_ns());
    }
    factor
}

fn ns_to_seconds(ns: i64) -> f64 {
    if ns % 1_000_000 == 0 {
        (ns / 1_000_000) as f64 / 1000.0
    } else {
        ns as f64 / 1_000_000_000.0
    }
}

fn bits(v: Option<f64>) -> Option<u64> {
    v.map(f64::to_bits)
}

fn combined(f: Float, fx: &Fixture, w: Window, stale: Stale, how: Combine) -> Option<f64> {
    let hours: Vec<FloatHour> = float_segments(&fx.samples, w, stale)
        .iter()
        .map(|seg| FloatHour::of(seg))
        .collect();
    combine_float(f, &hours, w, how)
}

/// "At most one counter reset inside each hour segment": the condition under
/// which the summed in-hour correction equals the evaluator's sequential one.
fn one_reset_per_hour(fx: &Fixture, w: Window) -> bool {
    float_segments(&fx.samples, w, Stale::Drop)
        .iter()
        .all(|seg| FloatHour::of(seg).resets <= 1)
}

/// Compares the combination with the evaluator on every fixture and window
/// where `holds` says the row's condition holds. Returns the number of windows
/// that produced a value, after asserting every fixture had at least one
/// window compared.
fn assert_float_row_exact(f: Float, holds: impl Fn(&Fixture, Window) -> bool) -> usize {
    let mut produced = 0;
    for fx in float_fixtures() {
        let mut compared = 0;
        for &w in WINDOWS {
            if !holds(&fx, w) {
                continue;
            }
            let expected = bits(eval_float(&fx.samples, &f.query(w), w));
            let got = bits(combined(f, &fx, w, Stale::Drop, Combine::Exact));
            assert_eq!(
                got, expected,
                "{f:?} on {} over {}: combined {got:x?}, evaluator {expected:x?}",
                fx.name, w.label
            );
            compared += 1;
            if expected.is_some() {
                produced += 1;
            }
        }
        assert!(compared > 0, "{f:?}: no window compared on {}", fx.name);
    }
    produced
}

fn exact_everywhere(_: &Fixture, _: Window) -> bool {
    true
}

/// Asserts that a wrong combination disagrees with the evaluator on the named
/// fixture and window while the documented combination agrees there.
fn assert_rejects(
    f: Float,
    fx: &str,
    w: &str,
    stale: Stale,
    how: Combine,
) -> (Option<u64>, Option<u64>) {
    let fx = fixture(fx);
    let w = window(w);
    let expected = bits(eval_float(&fx.samples, &f.query(w), w));
    let exact = bits(combined(f, &fx, w, Stale::Drop, Combine::Exact));
    let wrong = bits(combined(f, &fx, w, stale, how));
    assert_eq!(exact, expected, "{f:?}: documented combination must agree");
    assert_ne!(
        wrong, expected,
        "{f:?} on {} over {}: the wrong combination agrees ({wrong:x?})",
        fx.name, w.label
    );
    (expected, wrong)
}

fn rejects_counted_marker(f: Float, w: &str) -> (Option<u64>, Option<u64>) {
    assert_rejects(f, "staleness", w, Stale::Keep, Combine::Exact)
}

// ---- EXACT rows -----------------------------------------------------------

#[test]
fn count_over_time_exact_from_hour_counts() {
    assert!(assert_float_row_exact(Float::Count, exact_everywhere) > 0);
}

#[test]
fn count_over_time_rejects_counted_staleness_marker() {
    // The marker alone: the evaluator selects nothing, the mutant counts one.
    let (expected, wrong) = rejects_counted_marker(Float::Count, "(85m,95m]");
    assert_eq!((expected, wrong), (None, Some(1.0_f64.to_bits())));
}

#[test]
fn count_over_time_reassociated_f64_sum_is_harmless() {
    // Hour counts are small integers, so summing them as f64 in any grouping
    // is exact: mutant (a) has no float rounding to expose here.
    for fx in float_fixtures() {
        for &w in WINDOWS {
            assert_eq!(
                bits(combined(
                    Float::Count,
                    &fx,
                    w,
                    Stale::Drop,
                    Combine::Reassociate
                )),
                bits(combined(Float::Count, &fx, w, Stale::Drop, Combine::Exact)),
            );
        }
    }
}

#[test]
fn min_over_time_exact_from_hour_fold() {
    assert!(assert_float_row_exact(Float::Min, exact_everywhere) > 0);
}

#[test]
fn min_over_time_rejects_total_order_combination() {
    // Hour minima 0.0, -0.0, -0.0: the fold keeps the first zero, total_cmp
    // picks -0.0.
    let (expected, wrong) = assert_rejects(
        Float::Min,
        "signed_zero",
        "(0m,180m]",
        Stale::Drop,
        Combine::TotalOrder,
    );
    assert_eq!(expected, Some(0.0_f64.to_bits()));
    assert_eq!(wrong, Some((-0.0_f64).to_bits()));
}

#[test]
fn min_over_time_rejects_counted_staleness_marker() {
    let (expected, wrong) = rejects_counted_marker(Float::Min, "(85m,95m]");
    assert_eq!((expected, wrong), (None, Some(STALE_BITS)));
}

#[test]
fn max_over_time_exact_from_hour_fold() {
    assert!(assert_float_row_exact(Float::Max, exact_everywhere) > 0);
}

#[test]
fn max_over_time_rejects_total_order_combination() {
    // total_cmp ranks a positive NaN above every number, the fold displaces it.
    let (expected, wrong) = assert_rejects(
        Float::Max,
        "nan_payloads",
        "(0m,180m]",
        Stale::Drop,
        Combine::TotalOrder,
    );
    assert_eq!(expected, Some(3.0_f64.to_bits()));
    assert_eq!(wrong, Some(NAN_A));
}

#[test]
fn max_over_time_rejects_counted_staleness_marker() {
    let (expected, wrong) = rejects_counted_marker(Float::Max, "(85m,95m]");
    assert_eq!((expected, wrong), (None, Some(STALE_BITS)));
}

#[test]
fn last_over_time_exact_from_hour_last_sample() {
    assert!(assert_float_row_exact(Float::Last, exact_everywhere) > 0);
}

#[test]
fn last_over_time_rejects_counted_staleness_marker() {
    let (expected, wrong) = rejects_counted_marker(Float::Last, "(35m,95m]");
    assert_eq!(
        (expected, wrong),
        (Some(5.0_f64.to_bits()), Some(STALE_BITS))
    );
}

#[test]
fn present_over_time_exact_from_hour_presence() {
    assert!(assert_float_row_exact(Float::Present, exact_everywhere) > 0);
}

#[test]
fn present_over_time_rejects_counted_staleness_marker() {
    let (expected, wrong) = rejects_counted_marker(Float::Present, "(85m,95m]");
    assert_eq!((expected, wrong), (None, Some(1.0_f64.to_bits())));
}

#[test]
fn absent_over_time_exact_from_hour_presence() {
    // Empty windows: the marker-only one on `staleness`, and the same
    // interval on `signed_zero`, which has no sample there.
    assert_eq!(assert_float_row_exact(Float::Absent, exact_everywhere), 2);
}

#[test]
fn absent_over_time_rejects_counted_staleness_marker() {
    let (expected, wrong) = rejects_counted_marker(Float::Absent, "(85m,95m]");
    assert_eq!((expected, wrong), (Some(1.0_f64.to_bits()), None));
}

#[test]
fn resets_exact_from_hour_pairs_and_edges() {
    assert!(assert_float_row_exact(Float::Resets, exact_everywhere) > 0);
}

#[test]
fn resets_rejects_dropped_boundary_pair() {
    // The 25.7 -> 3.3 reset straddles the 60m boundary.
    let (expected, wrong) = assert_rejects(
        Float::Resets,
        "resets",
        "(0m,180m]",
        Stale::Drop,
        Combine::DropBoundaryPairs,
    );
    assert_eq!(expected, Some(3.0_f64.to_bits()));
    assert_eq!(wrong, Some(2.0_f64.to_bits()));
}

#[test]
fn resets_rejects_counted_staleness_marker() {
    // 3.0, marker, 2.0: the marker hides the reset from `b < a`.
    let (expected, wrong) = rejects_counted_marker(Float::Resets, "(0m,180m]");
    assert_eq!(expected, Some(1.0_f64.to_bits()));
    assert_eq!(wrong, Some(0.0_f64.to_bits()));
}

#[test]
fn changes_exact_from_hour_pairs_and_edges() {
    assert!(assert_float_row_exact(Float::Changes, exact_everywhere) > 0);
}

#[test]
fn changes_rejects_dropped_boundary_pair() {
    let (expected, wrong) = assert_rejects(
        Float::Changes,
        "resets",
        "(0m,180m]",
        Stale::Drop,
        Combine::DropBoundaryPairs,
    );
    assert_eq!(expected, Some(11.0_f64.to_bits()));
    assert_eq!(wrong, Some(9.0_f64.to_bits()));
}

#[test]
fn changes_rejects_counted_staleness_marker() {
    // 5.0, marker, 5.0 is no change; with the marker it is two.
    let (expected, wrong) = rejects_counted_marker(Float::Changes, "(0m,180m]");
    assert_eq!(expected, Some(8.0_f64.to_bits()));
    assert_eq!(wrong, Some(11.0_f64.to_bits()));
}

#[test]
fn irate_exact_from_hour_last_two_samples() {
    assert!(assert_float_row_exact(Float::Irate, exact_everywhere) > 0);
}

#[test]
fn irate_rejects_final_hour_only() {
    // (120m,130m] holds one sample; the pair straddles the 120m boundary.
    assert_rejects(
        Float::Irate,
        "resets",
        "(70m,130m]",
        Stale::Drop,
        Combine::FinalHourOnly,
    );
}

#[test]
fn irate_rejects_counted_staleness_marker() {
    let (_, wrong) = rejects_counted_marker(Float::Irate, "(35m,95m]");
    assert!(wrong.is_some_and(|b| f64::from_bits(b).is_nan()));
}

#[test]
fn idelta_exact_from_hour_last_two_samples() {
    assert!(assert_float_row_exact(Float::Idelta, exact_everywhere) > 0);
}

#[test]
fn idelta_rejects_final_hour_only() {
    assert_rejects(
        Float::Idelta,
        "resets",
        "(70m,130m]",
        Stale::Drop,
        Combine::FinalHourOnly,
    );
}

#[test]
fn idelta_rejects_counted_staleness_marker() {
    let (expected, wrong) = rejects_counted_marker(Float::Idelta, "(35m,95m]");
    assert_eq!(expected, Some(1.0_f64.to_bits()));
    assert!(wrong.is_some_and(|b| f64::from_bits(b).is_nan()));
}

#[test]
fn delta_exact_from_hour_endpoints() {
    assert!(assert_float_row_exact(Float::Delta, exact_everywhere) > 0);
}

#[test]
fn delta_rejects_telescoped_hour_sum() {
    let (expected, wrong) = assert_rejects(
        Float::Delta,
        "rounding",
        "(0m,120m]",
        Stale::Drop,
        Combine::Reassociate,
    );
    assert_eq!(expected, Some(0x0));
    assert_eq!(wrong, Some(0xbced_6db6_db6d_b6db));
}

#[test]
fn delta_rejects_counted_staleness_marker() {
    let (_, wrong) = rejects_counted_marker(Float::Delta, "(35m,95m]");
    assert!(wrong.is_some_and(|b| f64::from_bits(b).is_nan()));
}

// ---- EXACT UNDER CONDITION rows ---------------------------------------------

#[test]
fn increase_exact_with_one_reset_per_hour() {
    assert!(assert_float_row_exact(Float::Increase, one_reset_per_hour) > 0);
}

#[test]
fn rate_exact_with_one_reset_per_hour() {
    assert!(assert_float_row_exact(Float::Rate, one_reset_per_hour) > 0);
}

#[test]
fn increase_condition_fails_on_two_resets_in_one_hour() {
    let fx = fixture("two_resets_one_hour");
    let w = window("(0m,120m]");
    assert!(!one_reset_per_hour(&fx, w));
    let expected = bits(eval_float(&fx.samples, &Float::Increase.query(w), w));
    let got = bits(combined(
        Float::Increase,
        &fx,
        w,
        Stale::Drop,
        Combine::Exact,
    ));
    assert_eq!(expected, Some(0x4024_01dc_a01d_ca00));
    assert_eq!(got, Some(0x4024_01dc_a01d_ca01));
}

#[test]
fn rate_condition_fails_on_two_resets_in_one_hour() {
    let fx = fixture("two_resets_one_hour");
    let w = window("(0m,120m]");
    let expected = bits(eval_float(&fx.samples, &Float::Rate.query(w), w));
    let got = bits(combined(Float::Rate, &fx, w, Stale::Drop, Combine::Exact));
    assert_eq!(expected, Some(0x3f56_c38a_61fb_357a));
    assert_eq!(got, Some(0x3f56_c38a_61fb_357b));
}

#[test]
fn increase_rejects_telescoped_hour_sum() {
    let (expected, wrong) = assert_rejects(
        Float::Increase,
        "resets",
        "(0m,120m]",
        Stale::Drop,
        Combine::Reassociate,
    );
    assert_eq!(expected, Some(0x4050_a9e4_129e_412a));
    assert_eq!(wrong, Some(0x4050_a9e4_129e_4129));
}

#[test]
fn rate_rejects_telescoped_hour_sum() {
    let (expected, wrong) = assert_rejects(
        Float::Rate,
        "resets",
        "(0m,120m]",
        Stale::Drop,
        Combine::Reassociate,
    );
    assert_eq!(expected, Some(0x3f82_f5a2_c8f3_c629));
    assert_eq!(wrong, Some(0x3f82_f5a2_c8f3_c628));
}

#[test]
fn rate_rejects_counted_staleness_marker() {
    for f in [Float::Rate, Float::Increase] {
        let (_, wrong) = rejects_counted_marker(f, "(35m,95m]");
        assert!(wrong.is_some_and(|b| f64::from_bits(b).is_nan()));
    }
}

// ---- RAW ONLY rows ------------------------------------------------------------

fn rounding_hours() -> Vec<Vec<f64>> {
    float_segments(&rounding_samples(), window("(0m,180m]"), Stale::Drop)
        .iter()
        .map(|seg| seg.iter().map(|&(_, v)| v).collect())
        .collect()
}

fn eval_rounding(query: &str) -> u64 {
    let w = window("(0m,180m]");
    eval_float(&rounding_samples(), query, w)
        .expect("evaluates to a value")
        .to_bits()
}

fn kahan_inc(inc: f64, sum: f64, c: f64) -> (f64, f64) {
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

fn kahan_sum(values: &[f64]) -> f64 {
    let (mut sum, mut c) = (0.0, 0.0);
    for &v in values {
        (sum, c) = kahan_inc(v, sum, c);
    }
    if sum.is_infinite() { sum } else { sum + c }
}

fn incremental_mean(values: &[f64]) -> f64 {
    let (mut mean, mut c, mut count) = (0.0_f64, 0.0, 0.0);
    for &v in values {
        count += 1.0;
        (mean, c) = kahan_inc(v / count - mean / count, mean, c);
    }
    if mean.is_infinite() { mean } else { mean + c }
}

/// `(count, mean, m2)` of the evaluator's compensated Welford pass.
fn welford(values: &[f64]) -> (f64, f64, f64) {
    let (mut count, mut mean, mut c_mean, mut m2, mut c_m2) = (0.0, 0.0, 0.0, 0.0, 0.0);
    for &v in values {
        count += 1.0;
        let delta = v - (mean + c_mean);
        (mean, c_mean) = kahan_inc(delta / count, mean, c_mean);
        (m2, c_m2) = kahan_inc(delta * (v - (mean + c_mean)), m2, c_m2);
    }
    (count, mean + c_mean, m2 + c_m2)
}

/// Chan et al.'s pairwise merge of per-hour `(count, mean, m2)`.
fn chan_variance(hours: &[(f64, f64, f64)]) -> f64 {
    let (mut n, mut mean, mut m2) = hours[0];
    for &(nb, mb, m2b) in &hours[1..] {
        let total = n + nb;
        let d = mb - mean;
        mean += d * nb / total;
        m2 = m2 + m2b + d * d * n * nb / total;
        n = total;
    }
    m2 / n
}

fn median(values: &[f64]) -> f64 {
    let mut v = values.to_vec();
    v.sort_by(f64::total_cmp);
    let rank = 0.5 * (v.len() as f64 - 1.0);
    let lo = rank.floor();
    let hi = (lo + 1.0).min(v.len() as f64 - 1.0);
    let weight = rank - lo;
    v[lo as usize] * (1.0 - weight) + v[hi as usize] * weight
}

/// `(n, Σx, Σy, Σxy, Σx²)` over one hour, `x` in seconds from `anchor_ns`.
fn regression_sums(seg: &[(i64, f64)], anchor_ns: i64) -> [f64; 5] {
    let mut s = [0.0; 5];
    for &(ts, v) in seg {
        let x = ns_to_seconds(ts - anchor_ns);
        s[0] += 1.0;
        s[1] += x;
        s[2] += v;
        s[3] += x * v;
        s[4] += x * x;
    }
    s
}

fn regression_from_hours(anchor_ns: i64) -> (f64, f64) {
    let mut s = [0.0; 5];
    for seg in float_segments(&rounding_samples(), window("(0m,180m]"), Stale::Drop) {
        let h = regression_sums(&seg, anchor_ns);
        for (total, part) in s.iter_mut().zip(h) {
            *total += part;
        }
    }
    let [n, sx, sy, sxy, sx2] = s;
    let slope = (sxy - sx * sy / n) / (sx2 - sx * sx / n);
    (slope, sy / n - slope * sx / n)
}

#[test]
fn sum_over_time_raw_only_compensated_hour_sums_differ() {
    let per_hour: Vec<f64> = rounding_hours().iter().map(|h| kahan_sum(h)).collect();
    let combined = kahan_sum(&per_hour).to_bits();
    assert_eq!(
        eval_rounding("sum_over_time(m[180m])"),
        0x405c_5147_ae14_7ae2
    );
    assert_eq!(combined, 0x405c_5147_ae14_7ae1);
}

#[test]
fn avg_over_time_raw_only_weighted_hour_means_differ() {
    let hours = rounding_hours();
    let n: usize = hours.iter().map(Vec::len).sum();
    let weighted: Vec<f64> = hours
        .iter()
        .map(|h| incremental_mean(h) * h.len() as f64)
        .collect();
    let combined = (kahan_sum(&weighted) / n as f64).to_bits();
    assert_eq!(
        eval_rounding("avg_over_time(m[180m])"),
        0x4022_e0da_740d_a742
    );
    assert_eq!(combined, 0x4022_e0da_740d_a741);
}

#[test]
fn stdvar_over_time_raw_only_merged_hour_moments_differ() {
    let moments: Vec<(f64, f64, f64)> = rounding_hours().iter().map(|h| welford(h)).collect();
    let combined = chan_variance(&moments).to_bits();
    assert_eq!(
        eval_rounding("stdvar_over_time(m[180m])"),
        0x4087_624c_8ae6_6094
    );
    assert_eq!(combined, 0x4087_624c_8ae6_6095);
}

#[test]
fn stddev_over_time_raw_only_merged_hour_moments_differ() {
    let moments: Vec<(f64, f64, f64)> = rounding_hours().iter().map(|h| welford(h)).collect();
    let combined = chan_variance(&moments).sqrt().to_bits();
    assert_eq!(
        eval_rounding("stddev_over_time(m[180m])"),
        0x403b_5ad6_eadf_df79
    );
    assert_eq!(combined, 0x403b_5ad6_eadf_df7a);
}

#[test]
fn quantile_over_time_raw_only_median_of_hour_medians_differs() {
    let medians: Vec<f64> = rounding_hours().iter().map(|h| median(h)).collect();
    let combined = median(&medians).to_bits();
    assert_eq!(
        eval_rounding("quantile_over_time(0.5, m[180m])"),
        0x3fd3_3333_3333_3333
    );
    assert_eq!(combined, 0x3fe0_0000_0000_0000);
}

#[test]
fn predict_linear_raw_only_summed_hour_moments_differ() {
    let (slope, intercept) = regression_from_hours(window("(0m,180m]").end_ns());
    let combined = (slope * 600.0 + intercept).to_bits();
    assert_eq!(
        eval_rounding("predict_linear(m[180m], 600)"),
        0x4013_03a9_f975_b340
    );
    assert_eq!(combined, 0x4013_03a9_f975_b348);
}

#[test]
fn deriv_raw_only_summed_hour_moments_differ() {
    let first_ts = rounding_samples()[0].0;
    let (slope, _) = regression_from_hours(first_ts);
    assert_eq!(eval_rounding("deriv(m[180m])"), 0xbf4b_aa15_7caa_451d);
    assert_eq!(slope.to_bits(), 0xbf4b_aa15_7caa_4526);
}

// ---- Native histograms --------------------------------------------------------

fn hist(scale: i32, first_index: i32, counts: &[f64], sum: f64) -> FloatHistogram {
    FloatHistogram {
        counter_reset_hint: ResetHint::Unknown,
        scale,
        zero_threshold: 0.0,
        zero_count: 0.0,
        count: counts.iter().sum(),
        sum,
        positive_spans: vec![Span {
            offset: first_index,
            length: counts.len() as u32,
        }],
        negative_spans: Vec::new(),
        positive_buckets: counts.to_vec(),
        negative_buckets: Vec::new(),
        custom_values: Vec::new(),
    }
}

struct HistFixture {
    name: &'static str,
    samples: Vec<(i64, FloatHistogram)>,
}

fn hist_fixtures() -> Vec<HistFixture> {
    let two_resets = |counts: fn(f64) -> Vec<f64>| -> Vec<(i64, FloatHistogram)> {
        two_resets_samples()
            .into_iter()
            .map(|(ts, c)| (ts, hist(0, 1, &counts(c), c)))
            .collect()
    };
    vec![
        HistFixture {
            // Scale 1 -> 0 inside (0m,60m] (a decrease, not a reset), 0 -> 1
            // across the 60m boundary (a reset), a bucket reset inside
            // (60m,120m], 1 -> 0 across 120m; NaN sums with two payloads.
            name: "hist_schema_change",
            samples: vec![
                (10 * MIN_NS, hist(1, 1, &[1.0, 1.0, 1.0, 1.0], 4.0)),
                (30 * MIN_NS, hist(1, 1, &[2.0, 2.0, 2.0, 2.0], 8.5)),
                (50 * MIN_NS, hist(0, 1, &[5.0, 5.0], 11.25)),
                (60 * MIN_NS, hist(0, 1, &[6.0, 6.0], 13.0)),
                (70 * MIN_NS, hist(1, 1, &[4.0, 4.0, 4.0, 4.0], 16.0)),
                (90 * MIN_NS, hist(1, 1, &[5.0, 5.0, 5.0, 5.0], 20.5)),
                (110 * MIN_NS, hist(1, 1, &[1.0, 1.0, 1.0, 1.0], 4.25)),
                (120 * MIN_NS, hist(1, 1, &[2.0, 2.0, 2.0, 2.0], nan_a())),
                (130 * MIN_NS, hist(0, 1, &[5.0, 5.0], 11.0)),
                (150 * MIN_NS, hist(0, 1, &[7.0, 7.0], 15.5)),
                (170 * MIN_NS, hist(0, 1, &[9.0, 9.0], nan_b())),
            ],
        },
        HistFixture {
            // Two bucket resets inside (0m,60m], buckets [c, c].
            name: "hist_two_resets_one_hour",
            samples: two_resets(|c| vec![c, c]),
        },
        HistFixture {
            // The same resets, buckets [c, c, 1.0].
            name: "hist_two_resets_one_hour_offset",
            samples: two_resets(|c| vec![c, c, 1.0]),
        },
    ]
}

/// Per-hour statistic for the native-histogram rows.
struct HistHour {
    n: usize,
    first: (i64, FloatHistogram),
    last: (i64, FloatHistogram),
    min_scale: i32,
    resets: usize,
    /// The earlier histogram of every in-hour reset pair, summed in time order.
    correction: Option<FloatHistogram>,
}

impl HistHour {
    fn of(seg: &[(i64, FloatHistogram)]) -> HistHour {
        let mut resets = 0;
        let mut correction: Option<FloatHistogram> = None;
        for pair in seg.windows(2) {
            let prev = &pair[0].1;
            if pair[1].1.detect_reset(prev) {
                resets += 1;
                correction = Some(match correction {
                    None => prev.clone(),
                    Some(c) => {
                        let scale = c.scale.min(prev.scale);
                        let mut c = c.copy_to_scale(scale);
                        c.add_assign(&prev.copy_to_scale(scale));
                        c
                    }
                });
            }
        }
        HistHour {
            n: seg.len(),
            first: seg[0].clone(),
            last: seg[seg.len() - 1].clone(),
            min_scale: seg.iter().map(|(_, h)| h.scale).min().unwrap_or(i32::MAX),
            resets,
            correction,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Hist {
    Count,
    Sum,
    Quantile,
}

impl Hist {
    fn query(self, w: Window) -> String {
        match self {
            Hist::Count => format!("histogram_count(increase(h[{}m]))", w.range_min),
            Hist::Sum => format!("histogram_sum(rate(h[{}m]))", w.range_min),
            Hist::Quantile => format!("histogram_quantile(0.9, rate(h[{}m]))", w.range_min),
        }
    }

    fn read(self, h: &FloatHistogram) -> f64 {
        match self {
            Hist::Count => h.observation_count(),
            Hist::Sum => h.observation_sum(),
            Hist::Quantile => histogram_quantile(0.9, h),
        }
    }
}

fn hist_hours(fx: &HistFixture, w: Window) -> Vec<HistHour> {
    hour_segments(&fx.samples, w, |_| false, Stale::Drop)
        .iter()
        .map(|seg| HistHour::of(seg))
        .collect()
}

/// The windowed reduction of `histogram_rate` plus the extrapolation factor of
/// `histogram_extrapolated_rate`, rebuilt from the hour statistics.
fn combine_hist(f: Hist, hours: &[HistHour], w: Window) -> Option<f64> {
    let first = &hours.first()?.first;
    let last = &hours.last()?.last;
    let n: usize = hours.iter().map(|h| h.n).sum();
    if n < 2 || ns_to_seconds(last.0 - first.0) == 0.0 {
        return None;
    }
    let scale = hours.iter().map(|h| h.min_scale).min()?;
    let mut reduced = last.1.copy_to_scale(scale);
    reduced.sub_assign(&first.1.copy_to_scale(scale));
    for (k, h) in hours.iter().enumerate() {
        if k > 0 {
            let prev = &hours[k - 1].last.1;
            if h.first.1.detect_reset(prev) {
                reduced.add_assign(&prev.copy_to_scale(scale));
            }
        }
        if let Some(c) = &h.correction {
            reduced.add_assign(&c.copy_to_scale(scale));
        }
    }
    reduced.counter_reset_hint = ResetHint::Gauge;
    reduced.mul(histogram_factor(first.0, last.0, n, w, f != Hist::Count));
    Some(f.read(&reduced))
}

fn eval_hist(fx: &HistFixture, f: Hist, w: Window) -> Option<f64> {
    let source = TestSource::new()
        .with_histogram_series(&[("__name__", "h")], &fx.samples)
        .expect("valid series");
    let result = Evaluator::new()
        .instant(&source, &f.query(w), w.eval_ms())
        .expect("evaluates");
    assert!(result.len() <= 1);
    result.first().map(|s| s.value)
}

fn assert_hist_row_exact(f: Hist) {
    let mut produced = 0;
    for fx in hist_fixtures() {
        let mut compared = 0;
        for &w in WINDOWS {
            let hours = hist_hours(&fx, w);
            if hours.iter().any(|h| h.resets > 1) {
                continue;
            }
            let expected = bits(eval_hist(&fx, f, w));
            let got = bits(combine_hist(f, &hours, w));
            assert_eq!(
                got, expected,
                "{f:?} on {} over {}: combined {got:x?}, evaluator {expected:x?}",
                fx.name, w.label
            );
            compared += 1;
            if expected.is_some() {
                produced += 1;
            }
        }
        assert!(compared > 0, "{f:?}: no window compared on {}", fx.name);
    }
    assert!(produced > 0);
}

#[test]
fn histogram_count_exact_with_one_reset_per_hour() {
    assert_hist_row_exact(Hist::Count);
}

#[test]
fn histogram_sum_exact_with_one_reset_per_hour() {
    assert_hist_row_exact(Hist::Sum);
}

#[test]
fn histogram_quantile_exact_with_one_reset_per_hour() {
    assert_hist_row_exact(Hist::Quantile);
}

#[test]
fn histogram_rows_condition_fails_on_two_resets_in_one_hour() {
    let w = window("(0m,120m]");
    let mut found = Vec::new();
    for fx in hist_fixtures() {
        if !fx.name.starts_with("hist_two_resets") {
            continue;
        }
        let hours = hist_hours(&fx, w);
        assert_eq!(hours[0].resets, 2);
        for f in [Hist::Count, Hist::Sum, Hist::Quantile] {
            let expected = bits(eval_hist(&fx, f, w)).expect("evaluates");
            let got = bits(combine_hist(f, &hours, w)).expect("combines");
            found.push((fx.name, f, expected, got));
        }
    }
    // The summed in-hour correction moves the last bit of every row on at
    // least one of the two fixtures; equal pairs are rounding coincidences.
    assert_eq!(
        found,
        vec![
            (
                "hist_two_resets_one_hour",
                Hist::Count,
                0x4034_01dc_a01d_ca00,
                0x4034_01dc_a01d_ca01
            ),
            (
                "hist_two_resets_one_hour",
                Hist::Sum,
                0x3f56_c38a_61fb_357b,
                0x3f56_c38a_61fb_357c
            ),
            (
                "hist_two_resets_one_hour",
                Hist::Quantile,
                0x400b_db8c_dadb_e120,
                0x400b_db8c_dadb_e120
            ),
            (
                "hist_two_resets_one_hour_offset",
                Hist::Count,
                0x4036_3068_4306_842f,
                0x4036_3068_4306_842f
            ),
            (
                "hist_two_resets_one_hour_offset",
                Hist::Sum,
                0x3f56_c38a_61fb_357b,
                0x3f56_c38a_61fb_357c
            ),
            (
                "hist_two_resets_one_hour_offset",
                Hist::Quantile,
                0x400f_eaf9_3725_73ad,
                0x400f_eaf9_3725_73ab
            ),
        ]
    );
}

#[test]
fn schema_change_fixture_exercises_both_scale_directions() {
    let fx = hist_fixtures()
        .into_iter()
        .find(|f| f.name == "hist_schema_change")
        .expect("fixture");
    let hours = hist_hours(&fx, window("(0m,180m]"));
    let scales: Vec<i32> = hours.iter().map(|h| h.min_scale).collect();
    assert_eq!(scales, vec![0, 1, 0]);
    // The 60m -> 70m scale increase is a reset across the boundary.
    assert!(hours[1].first.1.detect_reset(&hours[0].last.1));
    assert_eq!(
        hours.iter().map(|h| h.resets).collect::<Vec<_>>(),
        vec![0, 1, 0]
    );
}
