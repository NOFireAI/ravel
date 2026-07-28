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

mod over_time;
mod rate;

use ravel_types::Sample;

use crate::eval::{
    Error, Evaluator, InstantVector, QueryWindow, RangeMatrix, Value, drop_metric_name,
    duration_to_ns, selector_eval_ts,
};
use crate::source::SeriesSource;

/// One registered function: its promql-parser name and evaluation shape.
#[derive(Clone, Copy)]
pub(crate) struct FunctionDef {
    pub(crate) name: &'static str,
    pub(crate) kind: FunctionKind,
}

/// A function's evaluation shape. Every P4 function is `RangeVector`
/// (`f(v range-vector) -> instant-vector`, reduced from one series' matrix
/// window per step) except `predict_linear`, which additionally takes a
/// scalar (`RangeVectorScalar`).
#[derive(Clone, Copy)]
pub(crate) enum FunctionKind {
    RangeVector(fn(&[Sample], RangeWindow) -> Option<f64>),
    RangeVectorScalar(fn(&[Sample], RangeWindow, f64) -> Option<f64>),
    /// `f(q, v range-vector) -> instant-vector`: the scalar comes first
    /// (`quantile_over_time(q, v)`), the mirror image of
    /// `RangeVectorScalar`'s argument order (`predict_linear(v, t)`). P5's
    /// only member of this shape.
    ScalarRangeVector(fn(f64, &[Sample], RangeWindow) -> Option<f64>),
    /// `absent_over_time`: not a per-series reduction of the matrix
    /// argument's rows like every other member of this enum. It reports
    /// whether the *whole* range vector matched anything at all,
    /// synthesizing its own single output series from the selector's
    /// equality matchers when it did not (the same label-derivation rule
    /// `absent()` uses, duplicated in `over_time.rs` rather than shared:
    /// P6's `functions/transform.rs`, the home for `absent()`, lands in
    /// parallel and this phase must not touch that file). Carries no
    /// function pointer; `eval_call`/`eval_range_call` special-case it
    /// directly.
    AbsentOverTime,
}

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
const FAMILIES: &[&[FunctionDef]] = &[rate::FUNCTIONS, over_time::FUNCTIONS];

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
        FunctionKind::ScalarRangeVector(f) => {
            let q = scalar_arg(evaluator, source, &call.args.args[0], eval_ts_ns, ctx)?;
            let ms = matrix_arg(&call.args.args[1])?;
            let window = range_window(ms, eval_ts_ns, ctx)?;
            let matrix = evaluator.eval_matrix_selector(source, ms, eval_ts_ns, ctx)?;
            Ok(Value::Vector(apply_reduce(matrix, eval_ts_ns, |samples| {
                f(q, samples, window)
            })))
        }
        FunctionKind::AbsentOverTime => {
            let ms = matrix_arg(&call.args.args[0])?;
            let matrix = evaluator.eval_matrix_selector(source, ms, eval_ts_ns, ctx)?;
            Ok(Value::Vector(over_time::absent_over_time_instant(
                &ms.vs, matrix, eval_ts_ns,
            )))
        }
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
        FunctionKind::ScalarRangeVector(f) => {
            let ms = matrix_arg(&call.args.args[1])?;
            // Same up-front, once-per-call resolution as
            // `RangeVectorScalar` above, and for the same reason: P5's only
            // such argument is a constant/scalar-only expression tree, so
            // it cannot vary per grid step.
            let q = scalar_arg(evaluator, source, &call.args.args[0], start_ns, ctx)?;
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
                        q,
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
        FunctionKind::AbsentOverTime => {
            let ms = matrix_arg(&call.args.args[0])?;
            over_time::absent_over_time_range(evaluator, source, ms, start_ns, end_ns, step_ns, ctx)
        }
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
            out.push(crate::eval::InstantSample {
                labels: drop_metric_name(labels),
                ts_ns: eval_ts_ns,
                value,
            });
        }
    }
    out
}
