//! Function registry and dispatch. Each function family lives in its own
//! module exposing a `const
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

mod histogram_classic;
mod histogram_native;
pub(crate) mod label;
pub(crate) mod over_time;
mod rate;
mod time;
mod transform;

use ravel_types::{LabelSet, Sample};

use crate::eval::{
    Error, Evaluator, InstantSample, InstantVector, QueryWindow, RangeMatrix, Value,
    drop_metric_name, duration_to_ns, possible_non_counter_info, resolve_eval_ts, selector_eval_ts,
};
use crate::histogram::{FloatHistogram, TimedHistogram};
use crate::source::SeriesSource;

/// One registered function: its promql-parser name and evaluation shape.
#[derive(Clone, Copy)]
pub(crate) struct FunctionDef {
    pub(crate) name: &'static str,
    pub(crate) kind: FunctionKind,
}

/// A function's evaluation shape.
///
/// * `RangeVector`/`RangeVectorScalar`: `f(v range-vector) ->
///   instant-vector`, reduced from one series' matrix window per step
///   (`predict_linear` additionally takes a scalar).
/// * `VectorMap`: `f(v instant-vector) -> instant-vector`, the whole
///   evaluated argument vector in, a whole vector out (elementwise math,
///   `sort`/`sort_desc`, `timestamp`). No other arguments.
/// * `Instant`: every other shape (extra scalar/string arguments,
///   optional arguments, zero vector arguments, or access to the call's own
///   AST for argument introspection like `absent`). Given the evaluator and
///   full call context directly so each function evaluates its own
///   arguments however its shape requires.
#[derive(Clone, Copy)]
pub(crate) enum FunctionKind {
    RangeVector(fn(&[Sample], RangeWindow) -> Option<f64>),
    RangeVectorScalar(fn(&[Sample], RangeWindow, f64) -> Option<f64>),
    /// `rate`/`increase`/`delta`: a float window reducer plus a native-
    /// histogram window reducer, so one registration serves both sample
    /// kinds. In an instant query both the float and histogram matrix
    /// selectors are evaluated and their outputs unioned; in a range query
    /// only the float reducer runs (a histogram-valued range result has no
    /// JSON rendering and no read path to feed it yet).
    RangeVectorFloatOrHist {
        float: fn(&[Sample], RangeWindow) -> Option<f64>,
        hist: fn(&[TimedHistogram], RangeWindow) -> Option<FloatHistogram>,
    },
    /// `f(phi, v instant-vector) -> instant-vector`: many-to-fewer,
    /// grouping `v` by its own labels rather than reducing one series'
    /// matrix window, so it does not fit either `RangeVector` shape above
    /// (`histogram_quantile`). Takes `&QueryWindow` so it can raise
    /// warning/info annotations (out-of-range `phi`, malformed buckets,
    /// forced monotonicity).
    HistogramQuantile(HistogramQuantileFn),
    /// `f(lower, upper, v instant-vector) -> instant-vector`, the two-scalar
    /// counterpart of `HistogramQuantile` (`histogram_fraction`). Takes
    /// `&QueryWindow` for the same annotation reason.
    HistogramFraction(HistogramFractionFn),
    /// `f(q, v range-vector) -> instant-vector`: the scalar comes first
    /// (`quantile_over_time(q, v)`), the mirror image of
    /// `RangeVectorScalar`'s argument order (`predict_linear(v, t)`). The
    /// only member of this shape.
    ScalarRangeVector(fn(f64, &[Sample], RangeWindow) -> Option<f64>),
    /// `absent_over_time`: not a per-series reduction of the matrix
    /// argument's rows like every other member of this enum. It reports
    /// whether the *whole* range vector matched anything at all,
    /// synthesizing its own single output series from the selector's
    /// equality matchers when it did not (the same label-derivation rule
    /// `absent()` uses, duplicated in `over_time.rs` rather than shared with
    /// `functions/transform.rs`, the home for `absent()`). Carries no
    /// function pointer; `eval_call`/`eval_range_call` special-case it
    /// directly.
    AbsentOverTime,
    /// `f(v instant-vector) -> instant-vector`: the whole evaluated
    /// argument vector in, a whole vector out (elementwise math,
    /// `sort`/`sort_desc`, `timestamp`). No other arguments.
    VectorMap(fn(InstantVector) -> InstantVector),
    /// Every other shape (extra scalar/string arguments, optional
    /// arguments, zero vector arguments, or access to the call's own AST
    /// for argument introspection like `absent`). Given the evaluator and
    /// full call context directly so each function evaluates its own
    /// arguments however its shape requires.
    Instant(InstantFn),
}

/// An [`FunctionKind::HistogramQuantile`] function pointer, aliased to keep
/// the enum variant under clippy's `type_complexity` threshold now that it
/// also takes a `&QueryWindow` for annotations.
pub(crate) type HistogramQuantileFn = fn(f64, InstantVector, &QueryWindow) -> Vec<(LabelSet, f64)>;

/// An [`FunctionKind::HistogramFraction`] function pointer, aliased for the
/// same reason as [`HistogramQuantileFn`].
pub(crate) type HistogramFractionFn =
    fn(f64, f64, InstantVector, &QueryWindow) -> Vec<(LabelSet, f64)>;

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
    histogram_classic::FUNCTIONS,
    histogram_native::FUNCTIONS,
    over_time::FUNCTIONS,
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
            let arg = matrix_arg(&call.args.args[0])?;
            let window = range_window(arg, eval_ts_ns, ctx)?;
            let matrix = eval_matrix_arg(evaluator, source, arg, eval_ts_ns, ctx)?;
            Ok(Value::Vector(apply_reduce(matrix, eval_ts_ns, |samples| {
                f(samples, window)
            })))
        }
        FunctionKind::RangeVectorScalar(f) => {
            let arg = matrix_arg(&call.args.args[0])?;
            let t = scalar_arg(evaluator, source, &call.args.args[1], eval_ts_ns, ctx)?;
            let window = range_window(arg, eval_ts_ns, ctx)?;
            let matrix = eval_matrix_arg(evaluator, source, arg, eval_ts_ns, ctx)?;
            Ok(Value::Vector(apply_reduce(matrix, eval_ts_ns, |samples| {
                f(samples, window, t)
            })))
        }
        FunctionKind::RangeVectorFloatOrHist { float, hist } => {
            let arg = matrix_arg(&call.args.args[0])?;
            let window = range_window(arg, eval_ts_ns, ctx)?;
            let matrix = eval_matrix_arg(evaluator, source, arg, eval_ts_ns, ctx)?;
            let mut out = apply_reduce(matrix, eval_ts_ns, |samples| float(samples, window));
            // Gated on the *reduced* output, not the raw matrix: Prometheus'
            // own check only runs once a rate/increase value was actually
            // computed (its early return for fewer than two samples happens
            // first), so a single-sample selector match must not raise this
            // either.
            maybe_info_non_counter_selector_name(call.func.name, arg, !out.is_empty(), ctx);
            // Native-histogram series matching the same selector: reduce each
            // window to a histogram and emit it as a histogram element. A
            // subquery argument no longer reaches this histogram branch (it is
            // gated to a matrix selector below): when a subquery's inner
            // expression matches histogram data, `eval_subquery_matrix` errors
            // upstream with `Error::Unsupported` rather than silently dropping
            // the histogram series here.
            if let MatrixArg::Selector(ms) = arg {
                let hmatrix =
                    evaluator.eval_histogram_matrix_selector(source, ms, eval_ts_ns, ctx)?;
                for (labels, samples) in hmatrix {
                    if let Some(h) = hist(&samples, window) {
                        out.push(InstantSample::histogram(labels, eval_ts_ns, eval_ts_ns, h));
                    }
                }
            }
            Ok(Value::Vector(out))
        }
        FunctionKind::HistogramQuantile(f) => {
            let phi = scalar_arg(evaluator, source, &call.args.args[0], eval_ts_ns, ctx)?;
            let vector = vector_arg(evaluator, source, &call.args.args[1], eval_ts_ns, ctx)?;
            Ok(Value::Vector(to_instant_vector(
                f(phi, vector, ctx),
                eval_ts_ns,
            )))
        }
        FunctionKind::HistogramFraction(f) => {
            let lower = scalar_arg(evaluator, source, &call.args.args[0], eval_ts_ns, ctx)?;
            let upper = scalar_arg(evaluator, source, &call.args.args[1], eval_ts_ns, ctx)?;
            let vector = vector_arg(evaluator, source, &call.args.args[2], eval_ts_ns, ctx)?;
            Ok(Value::Vector(to_instant_vector(
                f(lower, upper, vector, ctx),
                eval_ts_ns,
            )))
        }
        FunctionKind::ScalarRangeVector(f) => {
            let q = scalar_arg(evaluator, source, &call.args.args[0], eval_ts_ns, ctx)?;
            let arg = matrix_arg(&call.args.args[1])?;
            let window = range_window(arg, eval_ts_ns, ctx)?;
            let matrix = eval_matrix_arg(evaluator, source, arg, eval_ts_ns, ctx)?;
            Ok(Value::Vector(apply_reduce(matrix, eval_ts_ns, |samples| {
                f(q, samples, window)
            })))
        }
        FunctionKind::AbsentOverTime => {
            let arg = matrix_arg(&call.args.args[0])?;
            let matrix = eval_matrix_arg(evaluator, source, arg, eval_ts_ns, ctx)?;
            Ok(Value::Vector(over_time::absent_over_time_instant(
                over_time::matrix_arg_absent_labels(arg),
                matrix,
                eval_ts_ns,
            )))
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
            let arg = matrix_arg(&call.args.args[0])?;
            eval_matrix_arg_range_reduction(
                evaluator,
                source,
                arg,
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
            let arg = matrix_arg(&call.args.args[0])?;
            let scalar_expr = &call.args.args[1];
            // Prometheus evaluates a non-selector argument expression once
            // per step using that step's own `enh.ts`; the scalar argument
            // can itself vary per step (`scalar()`/`time()`), so it is
            // resolved fresh inside the reduce closure, at each step's own
            // `reported_ts_ns`, rather than once up front. `reduce` must
            // return `Option<f64>`, so a scalar-evaluation error is stashed
            // in `err` and re-raised after the reduction completes.
            let err = std::cell::RefCell::new(None);
            let matrix = eval_matrix_arg_range_reduction(
                evaluator,
                source,
                arg,
                start_ns,
                end_ns,
                step_ns,
                points,
                ctx,
                |samples, window_start_ns, sel_ts_ns, range_ns, reported_ts_ns| match scalar_arg(
                    evaluator,
                    source,
                    scalar_expr,
                    reported_ts_ns,
                    ctx,
                ) {
                    Ok(t) => f(
                        samples,
                        RangeWindow {
                            start_ns: window_start_ns,
                            end_ns: sel_ts_ns,
                            range_ns,
                            eval_ts_ns: reported_ts_ns,
                        },
                        t,
                    ),
                    Err(e) => {
                        *err.borrow_mut() = Some(e);
                        None
                    }
                },
            )?;
            if let Some(e) = err.into_inner() {
                return Err(e);
            }
            Ok(matrix)
        }
        FunctionKind::RangeVectorFloatOrHist { float, hist: _ } => {
            // Range queries reduce only the float series: a histogram-valued
            // range result cannot be rendered by the HTTP JSON layer and no
            // read path feeds native histograms into a range query yet.
            // The float reduction is identical to `RangeVector`.
            let arg = matrix_arg(&call.args.args[0])?;
            let result = eval_matrix_arg_range_reduction(
                evaluator,
                source,
                arg,
                start_ns,
                end_ns,
                step_ns,
                points,
                ctx,
                |samples, window_start_ns, sel_ts_ns, range_ns, reported_ts_ns| {
                    float(
                        samples,
                        RangeWindow {
                            start_ns: window_start_ns,
                            end_ns: sel_ts_ns,
                            range_ns,
                            eval_ts_ns: reported_ts_ns,
                        },
                    )
                },
            )?;
            maybe_info_non_counter_selector_name(call.func.name, arg, !result.is_empty(), ctx);
            Ok(result)
        }
        // `histogram_quantile`/`histogram_fraction` group and reduce a whole
        // instant vector at once, so they have no per-series matrix to reduce
        // per grid step the way `resolve_range_core` reduces a matrix window.
        // Instead they route through `eval_instant_over_grid`, the same
        // generalization `VectorMap`/`Instant` use below: evaluate the whole
        // call fresh at every grid point (identical to the `eval_call` arm
        // above) and stitch the per-step instant vectors into one matrix.
        // This makes the canonical Grafana p99 pattern
        // (`histogram_quantile(0.99, <native-histogram-selector>)`) evaluate
        // at a range-query top level, matching what it already did nested in
        // an arithmetic identity.
        FunctionKind::HistogramQuantile(f) => {
            eval_instant_over_grid(start_ns, end_ns, step_ns, |t| {
                ctx.check_deadline()?;
                let phi = scalar_arg(evaluator, source, &call.args.args[0], t, ctx)?;
                let vector = vector_arg(evaluator, source, &call.args.args[1], t, ctx)?;
                Ok(Value::Vector(to_instant_vector(f(phi, vector, ctx), t)))
            })
        }
        FunctionKind::HistogramFraction(f) => {
            eval_instant_over_grid(start_ns, end_ns, step_ns, |t| {
                ctx.check_deadline()?;
                let lower = scalar_arg(evaluator, source, &call.args.args[0], t, ctx)?;
                let upper = scalar_arg(evaluator, source, &call.args.args[1], t, ctx)?;
                let vector = vector_arg(evaluator, source, &call.args.args[2], t, ctx)?;
                Ok(Value::Vector(to_instant_vector(
                    f(lower, upper, vector, ctx),
                    t,
                )))
            })
        }
        FunctionKind::ScalarRangeVector(f) => {
            let scalar_expr = &call.args.args[0];
            let arg = matrix_arg(&call.args.args[1])?;
            // Same per-step resolution as `RangeVectorScalar` above, and
            // for the same reason: the scalar argument can vary per step
            // (`scalar()`/`time()`), so it is resolved fresh inside the
            // reduce closure at each step's own `reported_ts_ns`. `reduce`
            // must return `Option<f64>`, so a scalar-evaluation error is
            // stashed in `err` and re-raised after the reduction completes.
            let err = std::cell::RefCell::new(None);
            let matrix = eval_matrix_arg_range_reduction(
                evaluator,
                source,
                arg,
                start_ns,
                end_ns,
                step_ns,
                points,
                ctx,
                |samples, window_start_ns, sel_ts_ns, range_ns, reported_ts_ns| match scalar_arg(
                    evaluator,
                    source,
                    scalar_expr,
                    reported_ts_ns,
                    ctx,
                ) {
                    Ok(q) => f(
                        q,
                        samples,
                        RangeWindow {
                            start_ns: window_start_ns,
                            end_ns: sel_ts_ns,
                            range_ns,
                            eval_ts_ns: reported_ts_ns,
                        },
                    ),
                    Err(e) => {
                        *err.borrow_mut() = Some(e);
                        None
                    }
                },
            )?;
            if let Some(e) = err.into_inner() {
                return Err(e);
            }
            Ok(matrix)
        }
        FunctionKind::AbsentOverTime => {
            let arg = matrix_arg(&call.args.args[0])?;
            over_time::absent_over_time_range(
                evaluator, source, arg, start_ns, end_ns, step_ns, ctx,
            )
        }
        FunctionKind::VectorMap(f) => eval_instant_over_grid(start_ns, end_ns, step_ns, |t| {
            ctx.check_deadline()?;
            let v = vector_arg(evaluator, source, &call.args.args[0], t, ctx)?;
            Ok(Value::Vector(f(v)))
        }),
        FunctionKind::Instant(f) => eval_instant_over_grid(start_ns, end_ns, step_ns, |t| {
            ctx.check_deadline()?;
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
pub(crate) fn eval_instant_over_grid(
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

/// Evaluate a matrix-typed argument's reduction across a whole range query's
/// grid, dispatching on which kind of matrix-typed node it is. A matrix
/// selector reuses [`Evaluator::eval_range_matrix_reduction`]'s single-query,
/// forward-only-cursor discipline. A subquery has no single raw sample
/// stream to cursor over (each grid step's inner expression is its own
/// recursive evaluation, per [`Evaluator::eval_subquery_matrix`]), so it is
/// re-evaluated fully at each outer step instead: exactly the "no cross-step
/// caching" discipline, generalized to the
/// two-grids-deep case a subquery nested in a range query produces.
#[allow(clippy::too_many_arguments)]
fn eval_matrix_arg_range_reduction(
    evaluator: &Evaluator,
    source: &dyn SeriesSource,
    arg: MatrixArg,
    start_ns: i64,
    end_ns: i64,
    step_ns: i64,
    points: u64,
    ctx: &QueryWindow,
    reduce: impl Fn(&[Sample], i64, i64, i64, i64) -> Option<f64>,
) -> Result<RangeMatrix, Error> {
    match arg {
        MatrixArg::Selector(ms) => evaluator.eval_range_matrix_reduction(
            source, ms, start_ns, end_ns, step_ns, points, ctx, reduce,
        ),
        MatrixArg::Subquery(sq) => {
            let range_ns = duration_to_ns(sq.range)?;
            let mut series: Vec<(LabelSet, Vec<Sample>)> = Vec::new();
            let mut t = start_ns;
            while t <= end_ns {
                ctx.check_deadline()?;
                let matrix = evaluator.eval_subquery_matrix(source, sq, t, ctx)?;
                let sel_ts_ns = resolve_eval_ts(sq.offset.as_ref(), sq.at.as_ref(), t, ctx)?;
                let window_start_ns = sel_ts_ns.checked_sub(range_ns).ok_or(Error::TimeOverflow)?;
                for (labels, samples) in matrix {
                    if samples.is_empty() {
                        continue;
                    }
                    if let Some(value) = reduce(&samples, window_start_ns, sel_ts_ns, range_ns, t) {
                        let labels = drop_metric_name(labels);
                        match series.iter_mut().find(|(l, _)| *l == labels) {
                            Some((_, out)) => out.push(Sample { ts_ns: t, value }),
                            None => series.push((labels, vec![Sample { ts_ns: t, value }])),
                        }
                    }
                }
                t = t.checked_add(step_ns).ok_or(Error::TimeOverflow)?;
            }
            Ok(series)
        }
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

/// A function's Matrix-typed argument: either a matrix selector (`x[5m]`) or
/// a subquery (`x[5m:1m]`). Every call site downstream of [`matrix_arg`] is
/// agnostic to which one it got: [`range_window`] computes the same
/// [`RangeWindow`] shape for either, and [`Evaluator::eval_matrix_selector`]/
/// [`Evaluator::eval_subquery_matrix`] produce the same [`RangeMatrix`]
/// shape for a reducer to consume.
#[derive(Clone, Copy)]
pub(crate) enum MatrixArg<'a> {
    Selector(&'a promql_parser::parser::MatrixSelector),
    Subquery(&'a promql_parser::parser::SubqueryExpr),
}

/// Compute a matrix-typed argument's nominal window bounds: the left-open
/// window's exclusive start and inclusive end, and the range literal's own
/// duration (`rate`'s per-second divisor). For a subquery this is the
/// *declared* window (`end - range`), not the epoch-aligned grid
/// [`Evaluator::eval_subquery_matrix`] actually steps over internally
/// (which starts no earlier than this bound, but may start later): the
/// window a reducer like `rate` extrapolates against is the query's own
/// declared range, matching Prometheus, which computes `rate`'s boundary
/// extrapolation from the subquery's nominal range regardless of alignment.
fn range_window(arg: MatrixArg, eval_ts_ns: i64, ctx: &QueryWindow) -> Result<RangeWindow, Error> {
    match arg {
        MatrixArg::Selector(ms) => {
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
        MatrixArg::Subquery(sq) => {
            let end_ns = resolve_eval_ts(sq.offset.as_ref(), sq.at.as_ref(), eval_ts_ns, ctx)?;
            let range_ns = duration_to_ns(sq.range)?;
            let start_ns = end_ns.checked_sub(range_ns).ok_or(Error::TimeOverflow)?;
            Ok(RangeWindow {
                start_ns,
                end_ns,
                range_ns,
                eval_ts_ns,
            })
        }
    }
}

/// `rate()`/`increase()` (never `delta()`, which targets gauges and has no
/// counterpart Prometheus check) over a literal-named vector selector whose
/// name lacks a Prometheus counter-naming suffix: raises
/// [`possible_non_counter_info`]. The check runs against the
/// argument's own selector name, not each matched series' `__name__` label,
/// so a
/// label-matcher-only selector (no literal name) or a subquery's wrapped
/// expression has nothing to check here. `produced_a_value` gates it to only
/// fire once a rate/increase value was actually computed for at least one
/// series, matching Prometheus: its own check sits after the "fewer than two
/// samples" early return, so a selector that matched a series but produced
/// no value (too few samples in the window) must not raise this either.
fn maybe_info_non_counter_selector_name(
    func_name: &str,
    arg: MatrixArg,
    produced_a_value: bool,
    ctx: &QueryWindow,
) {
    if !produced_a_value || (func_name != "rate" && func_name != "increase") {
        return;
    }
    let MatrixArg::Selector(ms) = arg else {
        return;
    };
    let Some(name) = ms.vs.name.as_deref() else {
        return;
    };
    if !has_counter_suffix(name) {
        ctx.info(possible_non_counter_info(name));
    }
}

/// Prometheus' counter-naming convention (`funcRate`'s own suffix check): a
/// metric name ending in one of these is expected to be a counter.
fn has_counter_suffix(name: &str) -> bool {
    name.ends_with("_total")
        || name.ends_with("_sum")
        || name.ends_with("_count")
        || name.ends_with("_bucket")
}

/// Evaluate a matrix-typed argument at one instant into a [`RangeMatrix`],
/// dispatching on which kind of matrix-typed node it is.
fn eval_matrix_arg(
    evaluator: &Evaluator,
    source: &dyn SeriesSource,
    arg: MatrixArg,
    eval_ts_ns: i64,
    ctx: &QueryWindow,
) -> Result<RangeMatrix, Error> {
    match arg {
        MatrixArg::Selector(ms) => evaluator.eval_matrix_selector(source, ms, eval_ts_ns, ctx),
        MatrixArg::Subquery(sq) => evaluator.eval_subquery_matrix(source, sq, eval_ts_ns, ctx),
    }
}

/// Extract the matrix-typed node from a function argument known (by
/// promql-parser's own parse-time type check) to be Matrix-typed:
/// `MatrixSelector`, `Subquery`, or `Paren` wrapping either. Any other node
/// is unreachable, since the parser rejects the query before this evaluator
/// ever sees it otherwise.
fn matrix_arg(expr: &promql_parser::parser::Expr) -> Result<MatrixArg<'_>, Error> {
    use promql_parser::parser::Expr;
    let mut cur = expr;
    loop {
        match cur {
            Expr::Paren(p) => cur = &p.expr,
            Expr::MatrixSelector(ms) => return Ok(MatrixArg::Selector(ms)),
            Expr::Subquery(sq) => return Ok(MatrixArg::Subquery(sq)),
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

/// Evaluate a function argument known (by promql-parser's parse-time type
/// check) to be Vector-typed, via the general recursive evaluator so any
/// vector-producing construct works, including a nested function call
/// (e.g. `histogram_quantile(0.9, rate(h_bucket[5m]))`).
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

/// Wrap a many-to-fewer function's grouped output (already reduced to one
/// value per output group, with `le`/`__name__` already stripped from its
/// labels by the grouping itself) into an [`InstantVector`] at the query's
/// evaluation instant.
fn to_instant_vector(groups: Vec<(LabelSet, f64)>, eval_ts_ns: i64) -> InstantVector {
    groups
        .into_iter()
        .map(|(labels, value)| InstantSample {
            labels,
            ts_ns: eval_ts_ns,
            orig_sample_ts_ns: eval_ts_ns,
            value,
            histogram: None,
        })
        .collect()
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
                histogram: None,
            });
        }
    }
    out
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use crate::eval::Evaluator;
    use crate::histogram::{FloatHistogram, ResetHint, Span};
    use crate::testsource::TestSource;

    fn ms_ns(ms: i64) -> i64 {
        ms * 1_000_000
    }

    /// The `diff_native_hist` counter sample at difftest sample index `i`
    /// (generator.rs `native_histogram_families`): schema 0, three positive
    /// buckets from index 1 (`[2*step, 3*step, step]`, `step = i + 1`), zero
    /// bucket populated. Reproduced here so this crate can pin the range path
    /// against the instant path without depending on ravel-promql-difftest.
    fn diff_native_hist(i: usize) -> FloatHistogram {
        let step = (i + 1) as f64;
        let positive_buckets = vec![2.0 * step, 3.0 * step, step];
        let bucket_total: f64 = positive_buckets.iter().sum();
        FloatHistogram {
            counter_reset_hint: if i > 0 {
                ResetHint::No
            } else {
                ResetHint::Unknown
            },
            scale: 0,
            zero_threshold: 1e-9,
            zero_count: 1.0,
            count: bucket_total + 1.0,
            sum: positive_buckets
                .iter()
                .enumerate()
                .map(|(b, &c)| c * 2f64.powi(b as i32))
                .sum(),
            positive_spans: vec![Span {
                offset: 1,
                length: 3,
            }],
            negative_spans: Vec::new(),
            positive_buckets,
            negative_buckets: Vec::new(),
            custom_values: Vec::new(),
        }
    }

    #[test]
    fn range_and_instant_histogram_quantile_agree_bit_exact() {
        // The range-query top-level `histogram_quantile` arm routes through
        // `eval_instant_over_grid`, running the identical
        // scalar_arg/vector_arg/interpolate_value computation the instant arm
        // runs at the same timestamp. This pins that they produce bit-exact
        // f64s, so the promql-difftest p99 corpus entry's residual drift from
        // Prometheus is purely backend libm (`powf` vs Go `math.Pow`) and not
        // a range-vs-instant divergence introduced by the shared helper. The
        // +360s and +420s grid points are the two the CI difftest flagged as
        // 1-2 ULP off Prometheus; every point here must be Ravel-self-exact.
        let samples: Vec<(i64, FloatHistogram)> = (0..=20)
            .map(|i| (ms_ns(i as i64 * 30_000), diff_native_hist(i as usize)))
            .collect();
        let source = TestSource::new()
            .with_histogram_series(&[("__name__", "diff_native_hist")], &samples)
            .expect("valid histogram series");

        let range = Evaluator::new()
            .range(
                &source,
                "histogram_quantile(0.99, diff_native_hist)",
                300_000,
                600_000,
                60_000,
            )
            .expect("range evaluates");
        assert_eq!(range.len(), 1, "one series");
        let range_samples = &range[0].1;

        for s in range_samples {
            let ts_ms = s.ts_ns / 1_000_000;
            let instant = Evaluator::new()
                .instant(&source, "histogram_quantile(0.99, diff_native_hist)", ts_ms)
                .expect("instant evaluates");
            assert_eq!(instant.len(), 1, "one instant sample at {ts_ms}ms");
            assert_eq!(
                instant[0].value.to_bits(),
                s.value.to_bits(),
                "range and instant histogram_quantile must be bit-exact at {ts_ms}ms",
            );
        }
    }

    #[test]
    fn range_query_quantile_over_time_scalar_argument_is_re_evaluated_at_each_step() {
        // `quantile_over_time`'s scalar `q` argument is `scalar(qm)`, which
        // varies by evaluation instant: `qm` has no sample within `q`'s
        // default 5m lookback at the grid's first step (600s), so
        // `scalar(qm)` is NaN there, but its one sample falls inside the
        // lookback by the second step (660s). Hoisting the scalar argument
        // once at the grid's start_ns (the pre-fix bug) would keep it NaN
        // at every step, including the second.
        let source = TestSource::new()
            .with_series(
                &[("__name__", "g")],
                &[
                    (ms_ns(0), 10.0),
                    (ms_ns(300_000), 20.0),
                    (ms_ns(590_000), 30.0),
                ],
            )
            .expect("valid series")
            .with_series(&[("__name__", "qm")], &[(ms_ns(650_000), 1.0)])
            .expect("valid series");

        let result = Evaluator::new()
            .range(
                &source,
                "quantile_over_time(scalar(qm), g[20m])",
                600_000,
                660_000,
                60_000,
            )
            .expect("evaluates");

        assert_eq!(result.len(), 1);
        let samples = &result[0].1;
        assert_eq!(samples.len(), 2);
        assert!(
            samples[0].value.is_nan(),
            "first step: qm is absent from its own lookback, so scalar(qm) is NaN"
        );
        assert_eq!(
            samples[1].value, 30.0,
            "second step: qm=1.0 is now in lookback, quantile(1.0, [10,20,30]) is the max"
        );
    }

    #[test]
    fn range_query_predict_linear_scalar_argument_is_re_evaluated_at_each_step() {
        // Same bug, the `RangeVectorScalar` shape: `predict_linear`'s
        // duration argument `scalar(tm)` is NaN at the grid's first step
        // (tm has no sample in lookback yet) and a real number by the
        // second step. A duration hoisted once at start_ns would stay NaN
        // at every step instead of reflecting each step's own lookback.
        let source = TestSource::new()
            .with_series(
                &[("__name__", "h")],
                &[
                    (ms_ns(0), 10.0),
                    (ms_ns(300_000), 20.0),
                    (ms_ns(590_000), 30.0),
                ],
            )
            .expect("valid series")
            .with_series(&[("__name__", "tm")], &[(ms_ns(650_000), 5.0)])
            .expect("valid series");

        let result = Evaluator::new()
            .range(
                &source,
                "predict_linear(h[20m], scalar(tm))",
                600_000,
                660_000,
                60_000,
            )
            .expect("evaluates");

        assert_eq!(result.len(), 1);
        let samples = &result[0].1;
        assert_eq!(samples.len(), 2);
        assert!(
            samples[0].value.is_nan(),
            "first step: tm is absent from its own lookback, so scalar(tm) is NaN"
        );
        assert!(
            samples[1].value.is_finite(),
            "second step: tm=5.0 is now in lookback, predict_linear must use this step's own duration"
        );
    }
}
