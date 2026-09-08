//! Elementwise math, clamping, and instant-vector reordering function family:
//! `abs`, `ceil`, `floor`, `round`, `sqrt`, `exp`, `ln`,
//! `log2`, `log10`, `sgn`, the trig family, `deg`, `rad`, `pi`, `clamp`,
//! `clamp_min`, `clamp_max`, `sort`, `sort_desc`. Formulas and the
//! metric-name-dropping rule per function are direct ports of Prometheus'
//! own `promql/functions.go`: every elementwise math function (including
//! `round`, `clamp*`) routes through `enh.DropMetricName`, matching
//! Prometheus' `simpleFunc`/`funcClampImpl` helpers; `sort`/`sort_desc` do
//! not, since they only reorder the input vector's own samples rather than
//! deriving new series from it (Prometheus' `funcSort`/`funcSortDesc` never
//! call `DropMetricName`).
//!
//! Every function here also resets `orig_sample_ts_ns` to the sample's own
//! `ts_ns` (the evaluation instant), even `sort`/`sort_desc`, which otherwise
//! pass the input samples through unchanged: this is what makes
//! `timestamp(sort(x))` report the evaluation instant rather than leaking
//! the underlying selector's original sample timestamp through an
//! intervening function call, matching Prometheus (only a bare vector
//! selector argument to `timestamp()` recovers a real stored timestamp).

use std::cmp::Ordering;

use crate::eval::{
    Error, Evaluator, InstantSample, InstantVector, QueryWindow, Value, drop_metric_name,
};
use crate::source::SeriesSource;

use super::{FunctionDef, FunctionKind, scalar_arg, vector_arg};

pub(crate) const FUNCTIONS: &[FunctionDef] = &[
    FunctionDef {
        name: "abs",
        kind: FunctionKind::VectorMap(|v| elementwise(v, f64::abs)),
    },
    FunctionDef {
        name: "ceil",
        kind: FunctionKind::VectorMap(|v| elementwise(v, f64::ceil)),
    },
    FunctionDef {
        name: "floor",
        kind: FunctionKind::VectorMap(|v| elementwise(v, f64::floor)),
    },
    FunctionDef {
        name: "sqrt",
        kind: FunctionKind::VectorMap(|v| elementwise(v, f64::sqrt)),
    },
    FunctionDef {
        name: "exp",
        kind: FunctionKind::VectorMap(|v| elementwise(v, f64::exp)),
    },
    FunctionDef {
        name: "ln",
        kind: FunctionKind::VectorMap(|v| elementwise(v, f64::ln)),
    },
    FunctionDef {
        name: "log2",
        kind: FunctionKind::VectorMap(|v| elementwise(v, f64::log2)),
    },
    FunctionDef {
        name: "log10",
        kind: FunctionKind::VectorMap(|v| elementwise(v, f64::log10)),
    },
    FunctionDef {
        name: "sgn",
        kind: FunctionKind::VectorMap(|v| elementwise(v, sgn)),
    },
    FunctionDef {
        name: "sin",
        kind: FunctionKind::VectorMap(|v| elementwise(v, f64::sin)),
    },
    FunctionDef {
        name: "cos",
        kind: FunctionKind::VectorMap(|v| elementwise(v, f64::cos)),
    },
    FunctionDef {
        name: "tan",
        kind: FunctionKind::VectorMap(|v| elementwise(v, f64::tan)),
    },
    FunctionDef {
        name: "asin",
        kind: FunctionKind::VectorMap(|v| elementwise(v, f64::asin)),
    },
    FunctionDef {
        name: "acos",
        kind: FunctionKind::VectorMap(|v| elementwise(v, f64::acos)),
    },
    FunctionDef {
        name: "atan",
        kind: FunctionKind::VectorMap(|v| elementwise(v, f64::atan)),
    },
    FunctionDef {
        name: "sinh",
        kind: FunctionKind::VectorMap(|v| elementwise(v, f64::sinh)),
    },
    FunctionDef {
        name: "cosh",
        kind: FunctionKind::VectorMap(|v| elementwise(v, f64::cosh)),
    },
    FunctionDef {
        name: "tanh",
        kind: FunctionKind::VectorMap(|v| elementwise(v, f64::tanh)),
    },
    FunctionDef {
        name: "asinh",
        kind: FunctionKind::VectorMap(|v| elementwise(v, f64::asinh)),
    },
    FunctionDef {
        name: "acosh",
        kind: FunctionKind::VectorMap(|v| elementwise(v, f64::acosh)),
    },
    FunctionDef {
        name: "atanh",
        kind: FunctionKind::VectorMap(|v| elementwise(v, f64::atanh)),
    },
    FunctionDef {
        name: "deg",
        kind: FunctionKind::VectorMap(|v| elementwise(v, f64::to_degrees)),
    },
    FunctionDef {
        name: "rad",
        kind: FunctionKind::VectorMap(|v| elementwise(v, f64::to_radians)),
    },
    FunctionDef {
        name: "pi",
        kind: FunctionKind::Instant(pi),
    },
    FunctionDef {
        name: "round",
        kind: FunctionKind::Instant(round),
    },
    FunctionDef {
        name: "clamp",
        kind: FunctionKind::Instant(clamp),
    },
    FunctionDef {
        name: "clamp_min",
        kind: FunctionKind::Instant(clamp_min),
    },
    FunctionDef {
        name: "clamp_max",
        kind: FunctionKind::Instant(clamp_max),
    },
    FunctionDef {
        name: "sort",
        kind: FunctionKind::VectorMap(|v| reorder(v, false)),
    },
    FunctionDef {
        name: "sort_desc",
        kind: FunctionKind::VectorMap(|v| reorder(v, true)),
    },
];

/// `math.Signbit`-free sign: -1/+1 for negative/positive values, the value
/// itself unchanged for zero (preserving `-0.0`) and NaN, matching Go's
/// `funcSgn`.
fn sgn(v: f64) -> f64 {
    if v.is_nan() || v == 0.0 {
        v
    } else if v < 0.0 {
        -1.0
    } else {
        1.0
    }
}

/// Apply `op` to every sample's value, dropping `__name__` and resetting
/// `orig_sample_ts_ns` (Prometheus' `simpleFunc`).
fn elementwise(v: InstantVector, op: impl Fn(f64) -> f64) -> InstantVector {
    v.into_iter()
        .map(|s| InstantSample {
            labels: drop_metric_name(s.labels),
            ts_ns: s.ts_ns,
            orig_sample_ts_ns: s.ts_ns,
            value: op(s.value),
            histogram: None,
        })
        .collect()
}

/// `sort`/`sort_desc`: reorder by value only, keeping every sample's labels
/// (including `__name__`) unchanged, but still resetting
/// `orig_sample_ts_ns` (see module doc). NaN is defined here to sort last
/// regardless of direction ("NaN should sort to the bottom", matching the
/// comment in both of Prometheus' `funcSort`/`funcSortDesc`); this crate
/// cannot verify tie-breaking for repeated NaNs or equal values against a
/// live Prometheus in this sandbox (no network egress), so corpus entries
/// for these functions use series with distinct, non-NaN values wherever
/// order must be asserted.
fn reorder(v: InstantVector, desc: bool) -> InstantVector {
    let mut out: InstantVector = v
        .into_iter()
        .map(|s| InstantSample {
            labels: s.labels,
            ts_ns: s.ts_ns,
            orig_sample_ts_ns: s.ts_ns,
            value: s.value,
            histogram: None,
        })
        .collect();
    out.sort_by(|a, b| cmp_by_value(a.value, b.value, desc));
    out
}

fn cmp_by_value(a: f64, b: f64, desc: bool) -> Ordering {
    match (a.is_nan(), b.is_nan()) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Greater,
        (false, true) => Ordering::Less,
        (false, false) => {
            let ord = if a < b {
                Ordering::Less
            } else if a > b {
                Ordering::Greater
            } else {
                Ordering::Equal
            };
            if desc { ord.reverse() } else { ord }
        }
    }
}

/// Go's `math.Max`/`math.Min`: unlike Rust's `f64::max`/`f64::min`, either
/// operand being NaN makes the result NaN.
fn go_max(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() {
        f64::NAN
    } else {
        a.max(b)
    }
}

fn go_min(a: f64, b: f64) -> f64 {
    if a.is_nan() || b.is_nan() {
        f64::NAN
    } else {
        a.min(b)
    }
}

fn pi(
    _evaluator: &Evaluator,
    _source: &dyn SeriesSource,
    _call: &promql_parser::parser::Call,
    _eval_ts_ns: i64,
    _ctx: &QueryWindow,
) -> Result<Value, Error> {
    Ok(Value::Scalar(std::f64::consts::PI))
}

/// `round(v instant-vector, to_nearest=1 scalar)`: Prometheus' exact
/// formula is `math.Floor(v/toNearest + 0.5) * toNearest`, computed as
/// `math.Floor(v*toNearestInverse + 0.5) / toNearestInverse` ("invert as it
/// seems to cause fewer floating point accuracy issues" per Prometheus'
/// own comment) — not Rust's `f64::round()`, which breaks negative-value
/// ties the other way.
fn round(
    evaluator: &Evaluator,
    source: &dyn SeriesSource,
    call: &promql_parser::parser::Call,
    eval_ts_ns: i64,
    ctx: &QueryWindow,
) -> Result<Value, Error> {
    let v = vector_arg(evaluator, source, &call.args.args[0], eval_ts_ns, ctx)?;
    let to_nearest = if call.args.args.len() >= 2 {
        scalar_arg(evaluator, source, &call.args.args[1], eval_ts_ns, ctx)?
    } else {
        1.0
    };
    let inv = 1.0 / to_nearest;
    Ok(Value::Vector(elementwise(v, move |x| {
        (x * inv + 0.5).floor() / inv
    })))
}

fn clamp(
    evaluator: &Evaluator,
    source: &dyn SeriesSource,
    call: &promql_parser::parser::Call,
    eval_ts_ns: i64,
    ctx: &QueryWindow,
) -> Result<Value, Error> {
    let v = vector_arg(evaluator, source, &call.args.args[0], eval_ts_ns, ctx)?;
    let min = scalar_arg(evaluator, source, &call.args.args[1], eval_ts_ns, ctx)?;
    let max = scalar_arg(evaluator, source, &call.args.args[2], eval_ts_ns, ctx)?;
    if max < min {
        return Ok(Value::Vector(Vec::new()));
    }
    Ok(Value::Vector(elementwise(v, move |x| {
        go_max(min, go_min(max, x))
    })))
}

fn clamp_min(
    evaluator: &Evaluator,
    source: &dyn SeriesSource,
    call: &promql_parser::parser::Call,
    eval_ts_ns: i64,
    ctx: &QueryWindow,
) -> Result<Value, Error> {
    let v = vector_arg(evaluator, source, &call.args.args[0], eval_ts_ns, ctx)?;
    let min = scalar_arg(evaluator, source, &call.args.args[1], eval_ts_ns, ctx)?;
    Ok(Value::Vector(elementwise(v, move |x| go_max(min, x))))
}

fn clamp_max(
    evaluator: &Evaluator,
    source: &dyn SeriesSource,
    call: &promql_parser::parser::Call,
    eval_ts_ns: i64,
    ctx: &QueryWindow,
) -> Result<Value, Error> {
    let v = vector_arg(evaluator, source, &call.args.args[0], eval_ts_ns, ctx)?;
    let max = scalar_arg(evaluator, source, &call.args.args[1], eval_ts_ns, ctx)?;
    Ok(Value::Vector(elementwise(v, move |x| go_min(max, x))))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use ravel_types::{Label, LabelSet};

    use super::*;

    fn sample(value: f64) -> InstantSample {
        InstantSample {
            labels: LabelSet::new(vec![Label {
                name: "__name__".to_string(),
                value: "m".to_string(),
            }])
            .expect("single label is trivially unique"),
            ts_ns: 1_000,
            orig_sample_ts_ns: 500,
            value,
            histogram: None,
        }
    }

    #[test]
    fn abs_drops_metric_name_and_resets_orig_ts() {
        let out = elementwise(vec![sample(-4.0)], f64::abs);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].value, 4.0);
        assert_eq!(out[0].orig_sample_ts_ns, out[0].ts_ns);
        assert_eq!(out[0].labels.get("__name__"), None);
    }

    #[test]
    fn sgn_preserves_zero_sign_and_nan() {
        assert_eq!(sgn(0.0).to_bits(), 0.0f64.to_bits());
        assert_eq!(sgn(-0.0).to_bits(), (-0.0f64).to_bits());
        assert!(sgn(f64::NAN).is_nan());
        assert_eq!(sgn(5.0), 1.0);
        assert_eq!(sgn(-5.0), -1.0);
    }

    #[test]
    fn round_matches_floor_half_up_formula() {
        // Prometheus' formula, not Rust's f64::round(): -0.5 rounds to 0
        // (Floor(-0.5 + 0.5) = Floor(0) = 0), not -1.
        let out = elementwise(vec![sample(-0.5)], |x| (x + 0.5).floor());
        assert_eq!(out[0].value, 0.0);
    }

    #[test]
    fn clamp_propagates_nan_from_bounds() {
        assert!(go_max(f64::NAN, 3.0).is_nan());
        assert!(go_min(3.0, f64::NAN).is_nan());
        assert_eq!(go_max(1.0, 3.0), 3.0);
        assert_eq!(go_min(1.0, 3.0), 1.0);
    }

    #[test]
    fn sort_orders_ascending_with_nan_last() {
        let v = vec![sample(3.0), sample(f64::NAN), sample(1.0), sample(2.0)];
        let out = reorder(v, false);
        let values: Vec<f64> = out.iter().map(|s| s.value).collect();
        assert_eq!(&values[..3], &[1.0, 2.0, 3.0]);
        assert!(values[3].is_nan());
    }

    #[test]
    fn sort_desc_orders_descending_with_nan_last() {
        let v = vec![sample(3.0), sample(f64::NAN), sample(1.0), sample(2.0)];
        let out = reorder(v, true);
        let values: Vec<f64> = out.iter().map(|s| s.value).collect();
        assert_eq!(&values[..3], &[3.0, 2.0, 1.0]);
        assert!(values[3].is_nan());
    }

    #[test]
    fn sort_keeps_metric_name() {
        let out = reorder(vec![sample(1.0)], false);
        assert_eq!(out[0].labels.get("__name__"), Some("m"));
    }
}
