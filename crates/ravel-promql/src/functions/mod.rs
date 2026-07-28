//! Function registry and dispatch (plan section 7's parallelization note).
//! Each function family lives in its own module exposing a `const
//! FUNCTIONS: &[FunctionDef]`; [`FAMILIES`] is the single place that
//! aggregates them. A later phase adding a disjoint family (e.g.
//! `aggregate_over_time`, label functions, aggregation operators) adds its
//! own module file plus one entry in `FAMILIES`; it never has to touch an
//! existing family's file or a shared match statement, so independent
//! families can be implemented and merged in parallel.
//!
//! [`FunctionKind`] is a closed enum of plain `fn` pointers rather than
//! `Box<dyn Fn>`, so [`FunctionDef`] and every family's table stay `Copy`
//! `const` arrays with no allocation or dynamic dispatch.

mod label;
mod rate;
mod time;
mod transform;

use ravel_types::{LabelSet, Sample};

use crate::eval::{
    Error, Evaluator, InstantSample, InstantVector, QueryWindow, RangeMatrix, Value,
    drop_metric_name, duration_to_ns, selector_eval_ts,
};
use crate::source::SeriesSource;

/// One registered function: its promql-parser name and evaluation shape.
#[derive(Clone, Copy)]
pub(crate) struct FunctionDef {
    pub(crate) name: &'static str,
    pub(crate) kind: FunctionKind,
}

/// A function's evaluation shape.
///
/// * `RangeVector`/`RangeVectorScalar` (P4): `f(v range-vector) ->
///   instant-vector`, reduced from one series' matrix window per step
///   (`predict_linear` additionally takes a scalar).
/// * `VectorMap` (P6): `f(v instant-vector) -> instant-vector`, the whole
///   evaluated argument vector in, a whole vector out (elementwise math,
///   `sort`/`sort_desc`, `timestamp`). No other arguments.
/// * `Instant` (P6): every other shape (extra scalar/string arguments,
///   optional arguments, zero vector arguments, or access to the call's own
///   AST for argument introspection like `absent`). Given the evaluator and
///   full call context directly so each function evaluates its own
///   arguments however its shape requires.
#[derive(Clone, Copy)]
pub(crate) enum FunctionKind {
    RangeVector(fn(&[Sample], RangeWindow) -> Option<f64>),
    RangeVectorScalar(fn(&[Sample], RangeWindow, f64) -> Option<f64>),
    VectorMap(fn(InstantVector) -> InstantVector),
    Instant(InstantFn),
}

/// An [`FunctionKind::Instant`] function: given the evaluator and full call
/// context, evaluates its own arguments and produces a [`Value`] however its
/// shape requires (factored out of the enum variant purely to keep the type
/// short enough for clippy's `type_complexity` lint).
pub(crate) type InstantFn = fn(
    &Evaluator,
    &dyn SeriesSource,
    &promql_parser::parser::Call,
    i64,
    &QueryWindow,
) -> Result<Value, Error>;

/// The window bounds a range-vector function needs beyond the raw samples:
/// the left-open window's exclusive start and inclusive end (matching
/// [`Evaluator::eval_matrix_selector`]'s own window), the range literal's
/// own duration (`rate`'s per-second divisor), and the un-shifted
/// evaluation instant for this step (`predict_linear`'s intercept anchor,
/// which is the query's own instant, not the offset/`@`-shifted lookup
/// time `end_ns` may be).
#[derive(Clone, Copy)]
pub(crate) struct RangeWindow {
    pub(crate) start_ns: i64,
    pub(crate) end_ns: i64,
    pub(crate) range_ns: i64,
    pub(crate) eval_ts_ns: i64,
}

/// All registered function families, aggregated into one lookup table.
const FAMILIES: &[&[FunctionDef]] = &[
    rate::FUNCTIONS,
    transform::FUNCTIONS,
    label::FUNCTIONS,
    time::FUNCTIONS,
];

fn lookup(name: &str) -> Option<FunctionDef> {
    FAMILIES
        .iter()
        .flat_map(|family| family.iter())
        .find(|f| f.name == name)
        .copied()
}

/// The [`Error::Unsupported`] for a function name not in [`FAMILIES`],
/// naming the call exactly as `eval::unsupported_construct_error` used to
/// before function dispatch existed.
fn unregistered_function_error(name: &str) -> Error {
    Error::Unsupported {
        construct: format!("function call: {name}"),
    }
}

/// Evaluate a top-level function call at one instant (`eval_expr`'s
/// `Expr::Call` arm).
pub(crate) fn eval_call(
    evaluator: &Evaluator,
    source: &dyn SeriesSource,
    call: &promql_parser::parser::Call,
    eval_ts_ns: i64,
    ctx: &QueryWindow,
) -> Result<Value, Error> {
    let def = lookup(call.func.name).ok_or_else(|| unregistered_function_error(call.func.name))?;
    match def.kind {
        FunctionKind::RangeVector(f) => {
            let ms = matrix_arg(&call.args.args[0])?;
            let window = range_window(ms, eval_ts_ns, ctx)?;
            let matrix = evaluator.eval_matrix_selector(source, ms, eval_ts_ns, ctx)?;
            Ok(Value::Vector(apply_reduce(matrix, eval_ts_ns, |samples| {
                f(samples, window)
            })))
        }
        FunctionKind::RangeVectorScalar(f) => {
            let ms = matrix_arg(&call.args.args[0])?;
            let t = scalar_arg(evaluator, source, &call.args.args[1], eval_ts_ns, ctx)?;
            let window = range_window(ms, eval_ts_ns, ctx)?;
            let matrix = evaluator.eval_matrix_selector(source, ms, eval_ts_ns, ctx)?;
            Ok(Value::Vector(apply_reduce(matrix, eval_ts_ns, |samples| {
                f(samples, window, t)
            })))
        }
        FunctionKind::VectorMap(f) => {
            let v = vector_arg(evaluator, source, &call.args.args[0], eval_ts_ns, ctx)?;
            Ok(Value::Vector(f(v)))
        }
        FunctionKind::Instant(f) => f(evaluator, source, call, eval_ts_ns, ctx),
    }
}

/// Evaluate a top-level function call as a range matrix (`eval_range`'s
/// `RangeCore::Call` arm). One `source.query` call sized to cover the whole
/// grid's windows, then [`Evaluator::eval_range_matrix_reduction`] slices
/// each step's own window and reduces it, matching the single-query
/// discipline `eval_range_selector` already uses for plain selectors.
#[allow(clippy::too_many_arguments)]
pub(crate) fn eval_range_call(
    evaluator: &Evaluator,
    source: &dyn SeriesSource,
    call: &promql_parser::parser::Call,
    start_ns: i64,
    end_ns: i64,
    step_ns: i64,
    points: u64,
    ctx: &QueryWindow,
) -> Result<RangeMatrix, Error> {
    let def = lookup(call.func.name).ok_or_else(|| unregistered_function_error(call.func.name))?;
    match def.kind {
        FunctionKind::RangeVector(f) => {
            let ms = matrix_arg(&call.args.args[0])?;
            evaluator.eval_range_matrix_reduction(
                source,
                ms,
                start_ns,
                end_ns,
                step_ns,
                points,
                ctx,
                |samples, window_start_ns, sel_ts_ns, range_ns, reported_ts_ns| {
                    f(
                        samples,
                        RangeWindow {
                            start_ns: window_start_ns,
                            end_ns: sel_ts_ns,
                            range_ns,
                            eval_ts_ns: reported_ts_ns,
                        },
                    )
                },
            )
        }
        FunctionKind::RangeVectorScalar(f) => {
            let ms = matrix_arg(&call.args.args[0])?;
            // The scalar argument is evaluated once, at the query's own
            // (un-shifted) instant, same as Prometheus evaluates a
            // non-selector argument expression once per step using that
            // step's own `enh.ts`; since P4's only such argument is a
            // constant/scalar-only expression tree (no selectors, so no
            // per-step variance), it is resolved up front rather than once
            // per grid point.
            let t = scalar_arg(evaluator, source, &call.args.args[1], start_ns, ctx)?;
            evaluator.eval_range_matrix_reduction(
                source,
                ms,
                start_ns,
                end_ns,
                step_ns,
                points,
                ctx,
                move |samples, window_start_ns, sel_ts_ns, range_ns, reported_ts_ns| {
                    f(
                        samples,
                        RangeWindow {
                            start_ns: window_start_ns,
                            end_ns: sel_ts_ns,
                            range_ns,
                            eval_ts_ns: reported_ts_ns,
                        },
                        t,
                    )
                },
            )
        }
        FunctionKind::VectorMap(f) => eval_instant_over_grid(start_ns, end_ns, step_ns, |t| {
            let v = vector_arg(evaluator, source, &call.args.args[0], t, ctx)?;
            Ok(Value::Vector(f(v)))
        }),
        FunctionKind::Instant(f) => eval_instant_over_grid(start_ns, end_ns, step_ns, |t| {
            f(evaluator, source, call, t, ctx)
        }),
    }
}

/// Evaluate an [`FunctionKind::VectorMap`] or [`FunctionKind::Instant`]
/// function over a range query's grid by calling `step_fn` once per
/// timestamp and merging each step's `Value` into a matrix, exactly the
/// generalization Prometheus itself applies to instant-only function
/// expressions inside a range query: evaluate the whole expression fresh at
/// every grid point and stitch the per-step instant vectors (or the single
/// no-label series a per-step scalar becomes) into one range matrix by label
/// set. Kept local to this module (not a method on `Evaluator`) since this
/// generalization applies uniformly to every [`FunctionKind::VectorMap`]/
/// [`FunctionKind::Instant`] function without needing anything from
/// `eval.rs`'s own selector/matrix machinery beyond the `Value` it already
/// returns.
fn eval_instant_over_grid(
    start_ns: i64,
    end_ns: i64,
    step_ns: i64,
    mut step_fn: impl FnMut(i64) -> Result<Value, Error>,
) -> Result<RangeMatrix, Error> {
    let mut series: Vec<(LabelSet, Vec<Sample>)> = Vec::new();
    let mut t = start_ns;
    while t <= end_ns {
        match step_fn(t)? {
            Value::Vector(v) => {
                for s in v {
                    match series.iter_mut().find(|(labels, _)| *labels == s.labels) {
                        Some((_, samples)) => samples.push(Sample {
                            ts_ns: t,
                            value: s.value,
                        }),
                        None => series.push((
                            s.labels,
                            vec![Sample {
                                ts_ns: t,
                                value: s.value,
                            }],
                        )),
                    }
                }
            }
            Value::Scalar(x) => {
                let labels = LabelSet::default();
                match series.iter_mut().find(|(l, _)| *l == labels) {
                    Some((_, samples)) => samples.push(Sample { ts_ns: t, value: x }),
                    None => series.push((labels, vec![Sample { ts_ns: t, value: x }])),
                }
            }
            other => {
                return Err(Error::WrongType {
                    expected: "instant vector or scalar",
                    got: other.type_name(),
                });
            }
        }
        t = t.checked_add(step_ns).ok_or(Error::TimeOverflow)?;
    }
    Ok(series)
}

/// Evaluate a function argument known (by promql-parser's parse-time type
/// check) to be Vector-typed.
fn vector_arg(
    evaluator: &Evaluator,
    source: &dyn SeriesSource,
    expr: &promql_parser::parser::Expr,
    eval_ts_ns: i64,
    ctx: &QueryWindow,
) -> Result<InstantVector, Error> {
    match evaluator.eval_expr(source, expr, eval_ts_ns, ctx)? {
        Value::Vector(v) => Ok(v),
        other => Err(Error::WrongType {
            expected: "instant vector",
            got: other.type_name(),
        }),
    }
}

/// Evaluate a function argument known (by promql-parser's parse-time type
/// check) to be String-typed (a quoted literal; promql-parser rejects any
/// other String-typed expression at parse time).
pub(crate) fn string_arg(
    evaluator: &Evaluator,
    source: &dyn SeriesSource,
    expr: &promql_parser::parser::Expr,
    eval_ts_ns: i64,
    ctx: &QueryWindow,
) -> Result<String, Error> {
    match evaluator.eval_expr(source, expr, eval_ts_ns, ctx)? {
        Value::String(s) => Ok(s),
        other => Err(Error::WrongType {
            expected: "string",
            got: other.type_name(),
        }),
    }
}

/// Compute a matrix selector's window bounds the same way
/// [`Evaluator::eval_matrix_selector`] does internally, for the instant-call
/// path (the range-call path gets these per grid step from
/// [`Evaluator::eval_range_matrix_reduction`] instead).
fn range_window(
    ms: &promql_parser::parser::MatrixSelector,
    eval_ts_ns: i64,
    ctx: &QueryWindow,
) -> Result<RangeWindow, Error> {
    let sel_ts_ns = selector_eval_ts(&ms.vs, eval_ts_ns, ctx)?;
    let range_ns = duration_to_ns(ms.range)?;
    let start_ns = sel_ts_ns.checked_sub(range_ns).ok_or(Error::TimeOverflow)?;
    Ok(RangeWindow {
        start_ns,
        end_ns: sel_ts_ns,
        range_ns,
        eval_ts_ns,
    })
}

/// Extract the matrix selector from a function argument known (by
/// promql-parser's own parse-time type check) to be Matrix-typed:
/// `MatrixSelector` directly, `Paren` wrapping one, or `Subquery` (which
/// parses but is not yet supported by this evaluator). Any other node is
/// unreachable, since the parser rejects the query before this evaluator
/// ever sees it otherwise.
fn matrix_arg(
    expr: &promql_parser::parser::Expr,
) -> Result<&promql_parser::parser::MatrixSelector, Error> {
    use promql_parser::parser::Expr;
    let mut cur = expr;
    loop {
        match cur {
            Expr::Paren(p) => cur = &p.expr,
            Expr::MatrixSelector(ms) => return Ok(ms),
            Expr::Subquery(_) => {
                return Err(Error::Unsupported {
                    construct: "subquery".to_string(),
                });
            }
            _ => unreachable!(
                "promql-parser only ever type-checks a Matrix-typed argument to one of these \
                 forms before this evaluator sees it"
            ),
        }
    }
}

/// Evaluate a function argument known (by promql-parser's parse-time type
/// check) to be Scalar-typed, via the general recursive evaluator so any
/// scalar-producing construct works, including ones a later phase adds.
fn scalar_arg(
    evaluator: &Evaluator,
    source: &dyn SeriesSource,
    expr: &promql_parser::parser::Expr,
    eval_ts_ns: i64,
    ctx: &QueryWindow,
) -> Result<f64, Error> {
    match evaluator.eval_expr(source, expr, eval_ts_ns, ctx)? {
        Value::Scalar(v) => Ok(v),
        other => Err(Error::WrongType {
            expected: "scalar",
            got: other.type_name(),
        }),
    }
}

/// Reduce each matched series' matrix window through `reduce`, dropping
/// `__name__` (Prometheus drops the metric name on the result of any
/// function call) and omitting series for which `reduce` returns `None`
/// (too few samples, a zero sampled interval, etc.).
fn apply_reduce(
    matrix: RangeMatrix,
    eval_ts_ns: i64,
    reduce: impl Fn(&[Sample]) -> Option<f64>,
) -> InstantVector {
    let mut out = Vec::with_capacity(matrix.len());
    for (labels, samples) in matrix {
        if let Some(value) = reduce(&samples) {
            out.push(InstantSample {
                labels: drop_metric_name(labels),
                ts_ns: eval_ts_ns,
                orig_sample_ts_ns: eval_ts_ns,
                value,
            });
        }
    }
    out
}
