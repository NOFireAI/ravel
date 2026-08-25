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
    Error, Evaluator, HistogramAwareMatrix, InstantSample, InstantVector, QueryWindow, RangeMatrix,
    RangeSample, Value, drop_metric_name, duration_to_ns, float_matrix_into_hist,
    hist_matrix_into_float, invalid_quantile_warning, possible_non_counter_info, resolve_eval_ts,
    selector_eval_ts,
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
pub struct RangeWindow {
    pub start_ns: i64,
    pub end_ns: i64,
    pub range_ns: i64,
    pub eval_ts_ns: i64,
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
            let keep_name = range_vector_keeps_metric_name(call.func.name);
            // Aggregation-pushdown fast path (ADR-0103 amendment): for a
            // `count_over_time` over a literal matrix selector, ask the source
            // for a precomputed per-series count over this exact window before
            // fetching and reducing raw samples. Restricted to a literal
            // selector: a subquery's window is not a simple raw-sample count of
            // the outer range, so this mechanism must never be asked to serve
            // one. `Ok(Some(..))` takes the fast path; `Ok(None)` (no
            // precomputed answer) and `Err(..)` (the optional lookup faulted)
            // both fall through to the unchanged fetch-and-reduce path, so a
            // fault forgoes the fast path rather than failing the query.
            if call.func.name == "count_over_time"
                && let MatrixArg::Selector(ms) = arg
            {
                let matchers = crate::eval::build_matchers(&ms.vs)?;
                if let Ok(Some(results)) =
                    source.query_precomputed_count(&matchers, window.start_ns, window.end_ns)
                {
                    let out = results
                        .into_iter()
                        .map(|(labels, count)| InstantSample {
                            labels: if keep_name {
                                labels
                            } else {
                                drop_metric_name(labels)
                            },
                            ts_ns: eval_ts_ns,
                            orig_sample_ts_ns: eval_ts_ns,
                            value: count as f64,
                            histogram: None,
                        })
                        .collect();
                    return Ok(Value::Vector(out));
                }
            }
            let matrix = eval_matrix_arg(evaluator, source, arg, eval_ts_ns, ctx)?;
            let mut out = apply_reduce(matrix, eval_ts_ns, keep_name, |samples| f(samples, window));
            // Histogram-only series matching the same selector: reduced by the
            // shared over_time histogram policy so the instant endpoint answers
            // the same class as the range endpoint (ADR-0108 decision 5, the
            // instant/range parity fix). Float-only members drop here exactly
            // as the oracle does.
            append_histogram_over_time(
                evaluator,
                source,
                arg,
                call.func.name,
                eval_ts_ns,
                keep_name,
                ctx,
                &mut out,
            )?;
            Ok(Value::Vector(out))
        }
        FunctionKind::RangeVectorScalar(f) => {
            let arg = matrix_arg(&call.args.args[0])?;
            let t = scalar_arg(evaluator, source, &call.args.args[1], eval_ts_ns, ctx)?;
            let window = range_window(arg, eval_ts_ns, ctx)?;
            let matrix = eval_matrix_arg(evaluator, source, arg, eval_ts_ns, ctx)?;
            let mut out = apply_reduce(matrix, eval_ts_ns, false, |samples| f(samples, window, t));
            // `predict_linear`: float-only, so any histogram-only series is
            // dropped by the shared policy (no annotation), matching the oracle.
            append_histogram_over_time(
                evaluator,
                source,
                arg,
                call.func.name,
                eval_ts_ns,
                false,
                ctx,
                &mut out,
            )?;
            Ok(Value::Vector(out))
        }
        FunctionKind::RangeVectorFloatOrHist { float, hist } => {
            let arg = matrix_arg(&call.args.args[0])?;
            let window = range_window(arg, eval_ts_ns, ctx)?;
            let matrix = eval_matrix_arg(evaluator, source, arg, eval_ts_ns, ctx)?;
            let mut out = apply_reduce(matrix, eval_ts_ns, false, |samples| float(samples, window));
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
                    evaluator.eval_histogram_matrix_selector(source, ms, eval_ts_ns, ctx, false)?;
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
            // `quantile_over_time` clamps a q outside [0, 1] to +-Inf and warns,
            // exactly like `quantile()`/`histogram_quantile` (Prometheus'
            // `funcQuantileOverTime` raises `InvalidQuantileWarning`). The warn
            // sits inside the reduce closure so it fires once per matched series
            // (deduped by the annotation sink) and, like Prometheus, not at all
            // when no series matched.
            let mut out = apply_reduce(matrix, eval_ts_ns, false, |samples| {
                maybe_warn_invalid_quantile(q, ctx);
                f(q, samples, window)
            });
            // `quantile_over_time`: float-only, so a histogram-only series is
            // dropped by the shared policy with no annotation (its out-of-range
            // `q` warning likewise fires only for float windows), matching the
            // oracle's `len(Floats) == 0` early return.
            append_histogram_over_time(
                evaluator,
                source,
                arg,
                call.func.name,
                eval_ts_ns,
                false,
                ctx,
                &mut out,
            )?;
            Ok(Value::Vector(out))
        }
        FunctionKind::AbsentOverTime => {
            let arg = matrix_arg(&call.args.args[0])?;
            Ok(Value::Vector(over_time::absent_over_time_instant(
                evaluator, source, arg, eval_ts_ns, ctx,
            )?))
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
) -> Result<HistogramAwareMatrix, Error> {
    let def = lookup(call.func.name).ok_or_else(|| unregistered_function_error(call.func.name))?;
    match def.kind {
        FunctionKind::RangeVector(f) => {
            let arg = matrix_arg(&call.args.args[0])?;
            let keep_name = range_vector_keeps_metric_name(call.func.name);
            let name = call.func.name;
            eval_matrix_arg_range_reduction(
                evaluator,
                source,
                arg,
                start_ns,
                end_ns,
                step_ns,
                points,
                ctx,
                keep_name,
                |samples, hists, window_start_ns, sel_ts_ns, range_ns, reported_ts_ns| {
                    if !hists.is_empty() {
                        // A histogram-only window: the float reducer `f` has no
                        // histogram form, so route by member name to the shared
                        // over_time histogram policy (ADR-0108 decision 5).
                        // `irate`/`idelta` over a histogram pair whose
                        // counter/gauge typing does not match the operation
                        // raises the same warning as the instant arm; the
                        // annotation sink de-duplicates across grid steps.
                        if let Some(warning) = maybe_hist_type_warning(name, hists) {
                            ctx.warn(warning);
                        }
                        return histogram_range_element(
                            over_time::histogram_over_time(name, hists),
                            reported_ts_ns,
                        );
                    }
                    f(
                        samples,
                        RangeWindow {
                            start_ns: window_start_ns,
                            end_ns: sel_ts_ns,
                            range_ns,
                            eval_ts_ns: reported_ts_ns,
                        },
                    )
                    .map(|v| RangeSample::scalar(reported_ts_ns, v))
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
                false,
                |samples, hists, window_start_ns, sel_ts_ns, range_ns, reported_ts_ns| {
                    // `predict_linear` is float-only in Prometheus: a
                    // histogram-only window takes its `len(Floats) == 0`
                    // early return, dropping with no annotation.
                    if !hists.is_empty() {
                        return None;
                    }
                    match scalar_arg(evaluator, source, scalar_expr, reported_ts_ns, ctx) {
                        Ok(t) => f(
                            samples,
                            RangeWindow {
                                start_ns: window_start_ns,
                                end_ns: sel_ts_ns,
                                range_ns,
                                eval_ts_ns: reported_ts_ns,
                            },
                            t,
                        )
                        .map(|v| RangeSample::scalar(reported_ts_ns, v)),
                        Err(e) => {
                            *err.borrow_mut() = Some(e);
                            None
                        }
                    }
                },
            )?;
            if let Some(e) = err.into_inner() {
                return Err(e);
            }
            Ok(matrix)
        }
        FunctionKind::RangeVectorFloatOrHist { float, hist } => {
            // `rate`/`increase`/`delta`: the float reducer serves float series
            // and the native-histogram reducer serves histogram series, exactly
            // mirroring the instant arm's dispatch. A histogram window reduces
            // to one histogram element (ADR-0108 decision 4).
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
                false,
                |samples, hists, window_start_ns, sel_ts_ns, range_ns, reported_ts_ns| {
                    let window = RangeWindow {
                        start_ns: window_start_ns,
                        end_ns: sel_ts_ns,
                        range_ns,
                        eval_ts_ns: reported_ts_ns,
                    };
                    if !hists.is_empty() {
                        hist(hists, window).map(|h| RangeSample::histogram(reported_ts_ns, h))
                    } else {
                        float(samples, window).map(|v| RangeSample::scalar(reported_ts_ns, v))
                    }
                },
            )?;
            // Gated on the reduced FLOAT output, matching the instant arm and
            // Prometheus (the check follows the too-few-samples early return,
            // and lives in the float branch: a native histogram carries its own
            // counter-reset hint, so the metric-name heuristic does not apply
            // to it). The instant arm gets this for free because it reduces
            // floats and histograms into separate values; here both land in one
            // matrix, so the float elements have to be counted explicitly.
            let produced_a_float = result
                .iter()
                .any(|(_, samples)| samples.iter().any(|s| s.histogram.is_none()));
            maybe_info_non_counter_selector_name(call.func.name, arg, produced_a_float, ctx);
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
            // Quantile/fraction results are float-valued, so the grid produces
            // only float elements; the histogram-aware matrix carries them
            // unchanged.
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
                false,
                |samples, hists, window_start_ns, sel_ts_ns, range_ns, reported_ts_ns| {
                    // `quantile_over_time` is float-only in Prometheus: a
                    // histogram-only window takes its `len(Floats) == 0` early
                    // return, dropping the series with no annotation. The
                    // out-of-range-`q` warning fires there only after a float
                    // element exists, so it is likewise skipped here.
                    if !hists.is_empty() {
                        return None;
                    }
                    match scalar_arg(evaluator, source, scalar_expr, reported_ts_ns, ctx) {
                        Ok(q) => {
                            maybe_warn_invalid_quantile(q, ctx);
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
                            .map(|v| RangeSample::scalar(reported_ts_ns, v))
                        }
                        Err(e) => {
                            *err.borrow_mut() = Some(e);
                            None
                        }
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
            .map(float_matrix_into_hist)
        }
        // A top-level `VectorMap`/`Instant` function call is a float-only range
        // result today: any histogram element the per-step evaluation carries
        // is projected away rather than rendered as a `0.0` float (the grid
        // path that preserves histograms is `RangeCore::Generic`, top-level
        // aggregates and binary expressions, handled in `eval.rs`). Histogram
        // semantics for these function families are ADR-0108 decisions 4/5,
        // out of this task's scope; the round-trip through
        // `hist_matrix_into_float` drops the histogram elements explicitly
        // rather than emitting them as `0.0` floats.
        FunctionKind::VectorMap(f) => eval_instant_over_grid(start_ns, end_ns, step_ns, |t| {
            ctx.check_deadline()?;
            let v = vector_arg(evaluator, source, &call.args.args[0], t, ctx)?;
            Ok(Value::Vector(f(v)))
        })
        .map(|m| float_matrix_into_hist(hist_matrix_into_float(m))),
        FunctionKind::Instant(f) => eval_instant_over_grid(start_ns, end_ns, step_ns, |t| {
            ctx.check_deadline()?;
            f(evaluator, source, call, t, ctx)
        })
        .map(|m| float_matrix_into_hist(hist_matrix_into_float(m))),
    }
}

/// The counter/gauge type-mismatch warning `irate`/`idelta` over a native-
/// histogram window raises (Prometheus' `instantValue` histogram branch), or
/// `None` for any other function or a type-matched pair. Shared by the instant
/// dispatch ([`append_histogram_over_time`]) and the range dispatch
/// ([`eval_range_call`]'s `RangeVector` arm) so both endpoints annotate
/// identically. Only `irate`/`idelta` reach this: every other range-vector
/// member routes its histogram window through the same
/// [`over_time::histogram_over_time`] policy without a counter/gauge assumption.
fn maybe_hist_type_warning(name: &str, hists: &[TimedHistogram]) -> Option<String> {
    let is_rate = match name {
        "irate" => true,
        "idelta" => false,
        _ => return None,
    };
    match rate::instant_value_hist_type_warning(hists, is_rate)? {
        rate::InstantHistTypeWarning::NotCounter => {
            Some(rate::native_histogram_not_counter_warning())
        }
        rate::InstantHistTypeWarning::NotGauge => Some(rate::native_histogram_not_gauge_warning()),
    }
}

/// Map a native-histogram [`over_time::HistOverTime`] outcome to a range
/// element at `reported_ts_ns` (ADR-0108 decision 5): a float or histogram
/// result becomes the matching [`RangeSample`], a drop becomes `None`.
fn histogram_range_element(
    outcome: over_time::HistOverTime,
    reported_ts_ns: i64,
) -> Option<RangeSample> {
    match outcome {
        over_time::HistOverTime::Float(v) => Some(RangeSample::scalar(reported_ts_ns, v)),
        over_time::HistOverTime::Histogram(h) => Some(RangeSample::histogram(reported_ts_ns, h)),
        over_time::HistOverTime::Drop => None,
    }
}

/// Append native-histogram `_over_time` results to an instant range-vector
/// function's output (ADR-0108 decision 5, the instant/range parity fix): a
/// histogram-only series matching the same selector is reduced by the shared
/// [`over_time::histogram_over_time`] policy so both endpoints answer the same
/// class of value. Only a matrix selector can carry histograms into this
/// family (a subquery over native histograms errors upstream in
/// `eval_subquery_matrix`), so a subquery argument contributes nothing.
#[allow(clippy::too_many_arguments)]
fn append_histogram_over_time(
    evaluator: &Evaluator,
    source: &dyn SeriesSource,
    arg: MatrixArg,
    name: &str,
    eval_ts_ns: i64,
    keep_metric_name: bool,
    ctx: &QueryWindow,
    out: &mut InstantVector,
) -> Result<(), Error> {
    let MatrixArg::Selector(ms) = arg else {
        return Ok(());
    };
    let hmatrix =
        evaluator.eval_histogram_matrix_selector(source, ms, eval_ts_ns, ctx, keep_metric_name)?;
    for (labels, samples) in hmatrix {
        if let Some(warning) = maybe_hist_type_warning(name, &samples) {
            ctx.warn(warning);
        }
        match over_time::histogram_over_time(name, &samples) {
            over_time::HistOverTime::Float(v) => {
                out.push(InstantSample::scalar(labels, eval_ts_ns, eval_ts_ns, v));
            }
            over_time::HistOverTime::Histogram(h) => {
                out.push(InstantSample::histogram(labels, eval_ts_ns, eval_ts_ns, h));
            }
            over_time::HistOverTime::Drop => {}
        }
    }
    Ok(())
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
) -> Result<HistogramAwareMatrix, Error> {
    let mut series: Vec<(LabelSet, Vec<RangeSample>)> = Vec::new();
    let mut t = start_ns;
    while t <= end_ns {
        match step_fn(t)? {
            Value::Vector(v) => {
                for s in v {
                    // Preserve the element type per step: a histogram element
                    // is materialized as a histogram range element, not
                    // collapsed to its meaningless `0.0` placeholder float
                    // (ADR-0108 decision 2, the silent-zeros fix). This keeps
                    // the grid path (top-level aggregates and binary
                    // expressions) faithful to the instant path it re-runs at
                    // each step.
                    let element = match s.histogram {
                        Some(h) => RangeSample::histogram(t, h),
                        None => RangeSample::scalar(t, s.value),
                    };
                    match series.iter_mut().find(|(labels, _)| *labels == s.labels) {
                        Some((_, samples)) => samples.push(element),
                        None => series.push((s.labels, vec![element])),
                    }
                }
            }
            Value::Scalar(x) => {
                let labels = LabelSet::default();
                let element = RangeSample::scalar(t, x);
                match series.iter_mut().find(|(l, _)| *l == labels) {
                    Some((_, samples)) => samples.push(element),
                    None => series.push((labels, vec![element])),
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
    keep_metric_name: bool,
    reduce: impl Fn(&[Sample], &[TimedHistogram], i64, i64, i64, i64) -> Option<RangeSample>,
) -> Result<HistogramAwareMatrix, Error> {
    match arg {
        MatrixArg::Selector(ms) => evaluator.eval_range_matrix_reduction(
            source,
            ms,
            start_ns,
            end_ns,
            step_ns,
            points,
            ctx,
            keep_metric_name,
            reduce,
        ),
        MatrixArg::Subquery(sq) => {
            // A subquery yields only float samples: `eval_subquery_matrix`
            // rejects native histograms upstream with `Error::Unsupported`
            // rather than dropping them, so the histogram slice handed to
            // `reduce` is always empty here.
            let range_ns = duration_to_ns(sq.range)?;
            let mut series: Vec<(LabelSet, Vec<RangeSample>)> = Vec::new();
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
                    if let Some(element) =
                        reduce(&samples, &[], window_start_ns, sel_ts_ns, range_ns, t)
                    {
                        let labels = if keep_metric_name {
                            labels
                        } else {
                            drop_metric_name(labels)
                        };
                        match series.iter_mut().find(|(l, _)| *l == labels) {
                            Some((_, out)) => out.push(element),
                            None => series.push((labels, vec![element])),
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
pub enum MatrixArg<'a> {
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
pub fn range_window(
    arg: MatrixArg,
    eval_ts_ns: i64,
    ctx: &QueryWindow,
) -> Result<RangeWindow, Error> {
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
pub fn matrix_arg(expr: &promql_parser::parser::Expr) -> Result<MatrixArg<'_>, Error> {
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
/// (too few samples, a zero sampled interval, etc.). `keep_metric_name`
/// suppresses that drop for the one range-vector function that preserves the
/// input series' identity verbatim (`last_over_time`; see
/// [`range_vector_keeps_metric_name`]).
fn apply_reduce(
    matrix: RangeMatrix,
    eval_ts_ns: i64,
    keep_metric_name: bool,
    reduce: impl Fn(&[Sample]) -> Option<f64>,
) -> InstantVector {
    let mut out = Vec::with_capacity(matrix.len());
    for (labels, samples) in matrix {
        if let Some(value) = reduce(&samples) {
            out.push(InstantSample {
                labels: if keep_metric_name {
                    labels
                } else {
                    drop_metric_name(labels)
                },
                ts_ns: eval_ts_ns,
                orig_sample_ts_ns: eval_ts_ns,
                value,
                histogram: None,
            });
        }
    }
    out
}

/// PromQL drops `__name__` from a function call's result, since the output is
/// a computed value rather than a literal sample. The range-vector family has
/// one exception: `last_over_time` returns the input sample verbatim, so it
/// preserves every label including `__name__`, matching Prometheus'
/// `funcLastOverTime` (which appends `el.Metric` unchanged, unlike its
/// siblings that call `DropMetricName`).
fn range_vector_keeps_metric_name(func_name: &str) -> bool {
    func_name == "last_over_time"
}

/// Raise Prometheus' `InvalidQuantileWarning` when a quantile argument falls
/// outside `[0, 1]` (NaN included, since it is in no range). Shared by
/// `quantile_over_time`'s instant and range dispatch arms; the annotation
/// sink de-duplicates, so a per-series or per-step repeat is reported once.
fn maybe_warn_invalid_quantile(q: f64, ctx: &QueryWindow) {
    if !(0.0..=1.0).contains(&q) {
        ctx.warn(invalid_quantile_warning(q));
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use ravel_types::{Label, LabelSet};

    use crate::eval::Evaluator;
    use crate::histogram::{FloatHistogram, ResetHint, Span};
    use crate::source::{HistogramSeriesData, LabelMatcher, SeriesData, SeriesSource, SourceError};
    use crate::testsource::TestSource;

    fn ms_ns(ms: i64) -> i64 {
        ms * 1_000_000
    }

    fn labels(pairs: &[(&str, &str)]) -> LabelSet {
        LabelSet::new(
            pairs
                .iter()
                .map(|(name, value)| Label {
                    name: (*name).to_string(),
                    value: (*value).to_string(),
                })
                .collect(),
        )
        .expect("valid labels")
    }

    /// What a [`MockSource::query_precomputed_count`] returns.
    enum Precomputed {
        Some(Vec<(LabelSet, u64)>),
        None,
        Err,
    }

    /// A `SeriesSource` wrapping a [`TestSource`] for the raw fetch path, with
    /// a configurable `query_precomputed_count` result and call counters so a
    /// test can assert WHICH path actually ran, not just the final value.
    struct MockSource {
        inner: TestSource,
        precomputed: Precomputed,
        query_calls: AtomicUsize,
        hist_calls: AtomicUsize,
        precomputed_calls: AtomicUsize,
        precomputed_window: Mutex<Option<(i64, i64)>>,
    }

    impl MockSource {
        fn new(inner: TestSource, precomputed: Precomputed) -> Self {
            MockSource {
                inner,
                precomputed,
                query_calls: AtomicUsize::new(0),
                hist_calls: AtomicUsize::new(0),
                precomputed_calls: AtomicUsize::new(0),
                precomputed_window: Mutex::new(None),
            }
        }
    }

    impl SeriesSource for MockSource {
        fn query(
            &self,
            matchers: &[LabelMatcher],
            window: ravel_types::TimeRange,
        ) -> Result<Vec<SeriesData>, SourceError> {
            self.query_calls.fetch_add(1, Ordering::Relaxed);
            self.inner.query(matchers, window)
        }

        fn query_histograms(
            &self,
            matchers: &[LabelMatcher],
            window: ravel_types::TimeRange,
        ) -> Result<Vec<HistogramSeriesData>, SourceError> {
            self.hist_calls.fetch_add(1, Ordering::Relaxed);
            self.inner.query_histograms(matchers, window)
        }

        fn query_precomputed_count(
            &self,
            _matchers: &[LabelMatcher],
            start_ns: i64,
            end_ns: i64,
        ) -> Result<Option<Vec<(LabelSet, u64)>>, SourceError> {
            self.precomputed_calls.fetch_add(1, Ordering::Relaxed);
            *self.precomputed_window.lock().expect("lock") = Some((start_ns, end_ns));
            match &self.precomputed {
                Precomputed::Some(v) => Ok(Some(v.clone())),
                Precomputed::None => Ok(None),
                Precomputed::Err => Err(SourceError::Backend("boom".to_string())),
            }
        }
    }

    /// `(k-label, value)` pairs from an instant vector, sorted, so a test can
    /// compare results independent of series order. `count_over_time` drops
    /// `__name__`, so series are keyed on their remaining `k` label here.
    fn by_k(v: &[crate::eval::InstantSample]) -> Vec<(String, f64)> {
        let mut out: Vec<(String, f64)> = v
            .iter()
            .map(|s| (s.labels.get("k").unwrap_or_default().to_string(), s.value))
            .collect();
        out.sort_by(|a, b| a.0.cmp(&b.0));
        out
    }

    #[test]
    fn count_over_time_takes_precomputed_fast_path_without_fetching_samples() {
        // Mock serves a precomputed count for both series; assert the exact
        // values come back AND that the raw `query` path was never touched, so
        // the fast path was genuinely taken and not just coincidentally right.
        let inner = TestSource::new()
            .with_series(
                &[("__name__", "m"), ("k", "a")],
                &[(ms_ns(0), 1.0), (ms_ns(60_000), 2.0)],
            )
            .expect("series a")
            .with_series(&[("__name__", "m"), ("k", "b")], &[(ms_ns(0), 1.0)])
            .expect("series b");
        let source = MockSource::new(
            inner,
            Precomputed::Some(vec![
                (labels(&[("__name__", "m"), ("k", "a")]), 3),
                (labels(&[("__name__", "m"), ("k", "b")]), 7),
            ]),
        );

        let got = Evaluator::new()
            .instant(&source, "count_over_time(m[5m])", 60_000)
            .expect("evaluates");

        assert_eq!(
            by_k(&got),
            vec![("a".to_string(), 3.0), ("b".to_string(), 7.0)],
        );
        assert_eq!(
            source.query_calls.load(Ordering::Relaxed),
            0,
            "fast path must not fetch raw samples"
        );
        assert_eq!(source.precomputed_calls.load(Ordering::Relaxed), 1);
        // The pushed-down window is `count_over_time`'s own left-open window
        // (eval_ts - 5m, eval_ts], in ns.
        assert_eq!(
            *source.precomputed_window.lock().expect("lock"),
            Some((ms_ns(60_000) - ms_ns(300_000), ms_ns(60_000))),
        );
        // `count_over_time` drops `__name__`, so the fast path output must too.
        assert!(got.iter().all(|s| s.labels.get("__name__").is_none()));
    }

    #[test]
    fn count_over_time_falls_back_on_none_and_matches_plain_source() {
        // `Ok(None)` means "no precomputed answer": the query must fetch and
        // reduce exactly as a source with no override would, and `query` must
        // actually be called.
        let raw = &[(ms_ns(0), 1.0), (ms_ns(60_000), 2.0)];
        let plain = TestSource::new()
            .with_series(&[("__name__", "m")], raw)
            .expect("series");
        let expected = Evaluator::new()
            .instant(&plain, "count_over_time(m[5m])", 60_000)
            .expect("plain evaluates");

        let source = MockSource::new(
            TestSource::new()
                .with_series(&[("__name__", "m")], raw)
                .expect("series"),
            Precomputed::None,
        );
        let got = Evaluator::new()
            .instant(&source, "count_over_time(m[5m])", 60_000)
            .expect("evaluates");

        assert_eq!(got.len(), 1);
        assert_eq!(got[0].value, 2.0);
        assert_eq!(got[0].value, expected[0].value);
        assert_eq!(got[0].labels, expected[0].labels);
        assert!(
            source.query_calls.load(Ordering::Relaxed) >= 1,
            "fallback must fetch raw samples"
        );
    }

    #[test]
    fn precomputed_omitting_a_series_yields_no_sample_for_it_not_zero() {
        // A series present in the raw data but absent from the precomputed
        // result must produce NO output sample (the absence-not-zero rule),
        // never a `0.0` point.
        let inner = TestSource::new()
            .with_series(&[("__name__", "m"), ("k", "x")], &[(ms_ns(60_000), 1.0)])
            .expect("series x")
            .with_series(&[("__name__", "m"), ("k", "y")], &[(ms_ns(60_000), 1.0)])
            .expect("series y");
        let source = MockSource::new(
            inner,
            Precomputed::Some(vec![(labels(&[("__name__", "m"), ("k", "x")]), 5)]),
        );

        let got = Evaluator::new()
            .instant(&source, "count_over_time(m[5m])", 60_000)
            .expect("evaluates");

        assert_eq!(by_k(&got), vec![("x".to_string(), 5.0)]);
        assert!(
            got.iter().all(|s| s.labels.get("k") != Some("y")),
            "omitted series must not appear, not even as 0.0"
        );
        assert_eq!(source.query_calls.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn subquery_argument_never_asks_for_a_precomputed_count() {
        // A subquery argument is structurally excluded: the precomputed hook
        // must not be consulted even when it would return `Some`.
        let inner = TestSource::new()
            .with_series(
                &[("__name__", "m")],
                &[
                    (ms_ns(0), 1.0),
                    (ms_ns(300_000), 2.0),
                    (ms_ns(600_000), 3.0),
                ],
            )
            .expect("series");
        let source = MockSource::new(
            inner,
            Precomputed::Some(vec![(labels(&[("__name__", "m")]), 99)]),
        );

        let _ = Evaluator::new()
            .instant(&source, "count_over_time(m[1h:5m])", 3_600_000)
            .expect("evaluates");

        assert_eq!(
            source.precomputed_calls.load(Ordering::Relaxed),
            0,
            "subquery exclusion is structural, not a None default"
        );
    }

    #[test]
    fn count_over_time_falls_back_on_error_rather_than_failing() {
        // An `Err` from the optional precomputed lookup must never fail the
        // query: it falls back to fetch-and-reduce exactly like `Ok(None)`.
        let raw = &[(ms_ns(0), 1.0), (ms_ns(60_000), 2.0)];
        let plain = TestSource::new()
            .with_series(&[("__name__", "m")], raw)
            .expect("series");
        let expected = Evaluator::new()
            .instant(&plain, "count_over_time(m[5m])", 60_000)
            .expect("plain evaluates");

        let source = MockSource::new(
            TestSource::new()
                .with_series(&[("__name__", "m")], raw)
                .expect("series"),
            Precomputed::Err,
        );
        let got = Evaluator::new()
            .instant(&source, "count_over_time(m[5m])", 60_000)
            .expect("error must not fail the query");

        assert_eq!(got.len(), 1);
        assert_eq!(got[0].value, expected[0].value);
        assert_eq!(got[0].labels, expected[0].labels);
        assert!(
            source.query_calls.load(Ordering::Relaxed) >= 1,
            "error falls back to raw fetch"
        );
    }

    #[test]
    fn default_source_with_no_override_counts_normally() {
        // Regression: a source that never overrides `query_precomputed_count`
        // (default `Ok(None)`) counts exactly as before this mechanism existed.
        let source = TestSource::new()
            .with_series(
                &[("__name__", "m")],
                &[(ms_ns(0), 1.0), (ms_ns(30_000), 2.0), (ms_ns(60_000), 3.0)],
            )
            .expect("series");
        let got = Evaluator::new()
            .instant(&source, "count_over_time(m[5m])", 60_000)
            .expect("evaluates");
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].value, 3.0);
    }

    #[test]
    fn last_over_time_preserves_metric_name_while_siblings_drop_it() {
        // `last_over_time` returns the input sample verbatim, so it keeps every
        // label including `__name__`; every other `_over_time` function drops
        // `__name__` (the output is a computed value). This pins Prometheus'
        // one-function exception in both the instant and range dispatch paths,
        // which are distinct code (`apply_reduce` vs
        // `eval_range_matrix_reduction`). Pre-fix, both paths dropped
        // `__name__` unconditionally.
        let source = TestSource::new()
            .with_series(
                &[("__name__", "diff_boundary"), ("shape", "boundary")],
                &[(ms_ns(0), 1.0), (ms_ns(60_000), 2.0)],
            )
            .expect("valid series");

        // Instant path.
        let got = Evaluator::new()
            .instant(&source, "last_over_time(diff_boundary[5m])", 60_000)
            .expect("evaluates");
        assert_eq!(got.len(), 1);
        assert_eq!(
            got[0].labels.get("__name__"),
            Some("diff_boundary"),
            "last_over_time keeps __name__"
        );
        assert_eq!(got[0].labels.get("shape"), Some("boundary"));

        // Range path.
        let range = Evaluator::new()
            .range(
                &source,
                "last_over_time(diff_boundary[5m])",
                60_000,
                60_000,
                60_000,
            )
            .expect("evaluates");
        assert_eq!(range.len(), 1);
        assert_eq!(
            range[0].0.get("__name__"),
            Some("diff_boundary"),
            "range last_over_time keeps __name__ too"
        );

        // A sibling function drops __name__ but keeps the other labels.
        let avg = Evaluator::new()
            .instant(&source, "avg_over_time(diff_boundary[5m])", 60_000)
            .expect("evaluates");
        assert_eq!(avg.len(), 1);
        assert_eq!(
            avg[0].labels.get("__name__"),
            None,
            "avg_over_time drops __name__"
        );
        assert_eq!(avg[0].labels.get("shape"), Some("boundary"));
    }

    #[test]
    fn quantile_over_time_out_of_range_q_raises_the_invalid_quantile_warning() {
        // A q outside [0, 1] clamps to +-Inf and, matching Prometheus'
        // `funcQuantileOverTime`, raises an `InvalidQuantileWarning`. Pre-fix
        // Ravel clamped correctly but emitted no warning, the divergence the
        // over_time difftest corpus surfaced. Both directions warn; a q in
        // range does not.
        let source = TestSource::new()
            .with_series(
                &[("__name__", "g")],
                &[(ms_ns(0), 1.0), (ms_ns(60_000), 2.0)],
            )
            .expect("valid series");

        for q in ["1.5", "-0.5"] {
            let query = format!("quantile_over_time({q}, g[5m])");
            let (_value, annotations) = Evaluator::new()
                .eval_instant_annotated(&source, &query, 60_000)
                .expect("evaluates");
            assert_eq!(
                annotations.warnings().len(),
                1,
                "q={q} out of range warns once: {annotations:?}"
            );
            assert!(
                annotations.warnings()[0].contains("quantile value should be between 0 and 1"),
                "warning is the invalid-quantile one: {:?}",
                annotations.warnings()
            );
        }

        // In-range q: no annotation at all.
        let (_value, annotations) = Evaluator::new()
            .eval_instant_annotated(&source, "quantile_over_time(0.5, g[5m])", 60_000)
            .expect("evaluates");
        assert!(
            annotations.is_empty(),
            "an in-range quantile_over_time is annotation-free: {annotations:?}"
        );
    }

    #[test]
    fn quantile_over_time_out_of_range_q_does_not_warn_when_no_series_match() {
        // Prometheus calls the range-vector function once per matched series,
        // so a q out of range over a selector that matches nothing raises no
        // warning (the function never runs). The warn lives inside the reduce
        // closure precisely so it inherits this per-series gating.
        let source = TestSource::new()
            .with_series(&[("__name__", "g")], &[(ms_ns(0), 1.0)])
            .expect("valid series");
        let (_value, annotations) = Evaluator::new()
            .eval_instant_annotated(&source, "quantile_over_time(1.5, nosuch[5m])", 60_000)
            .expect("evaluates");
        assert!(
            annotations.is_empty(),
            "no matched series means no warning: {annotations:?}"
        );
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

    /// A simple scale-0 native histogram carrying `count`/`sum`, one positive
    /// bucket, for the range-grid aggregation tests below.
    fn nh(count: f64, sum: f64) -> FloatHistogram {
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
    fn range_grid_keeps_histogram_elements_from_top_level_sum() {
        // A top-level `sum` over native-histogram series is a `RangeCore::Generic`
        // grid path. Before the fix `eval_instant_over_grid` collapsed each
        // `InstantSample` into `Sample { ts_ns, value }`, discarding the
        // histogram and emitting the meaningless `0.0` placeholder; the flipped
        // assertion is `samples[0].histogram.is_some()` (it was `None`, value
        // `0.0`, pre-fix).
        let source = TestSource::new()
            .with_histogram_series(&[("__name__", "h"), ("i", "1")], &[(0, nh(2.0, 10.0))])
            .expect("valid histogram series")
            .with_histogram_series(&[("__name__", "h"), ("i", "2")], &[(0, nh(2.0, 10.0))])
            .expect("valid histogram series");
        let (value, _annotations) = Evaluator::new()
            .eval_range_hist_annotated(&source, "sum(h)", 0, 60_000, 60_000)
            .expect("range evaluates");
        let crate::eval::RangeValue::Matrix(matrix) = value else {
            panic!("sum over histograms must be a matrix");
        };
        assert_eq!(matrix.len(), 1, "one aggregated group");
        let samples = &matrix[0].1;
        assert!(!samples.is_empty(), "at least one grid step");
        for s in samples {
            let h = s
                .histogram
                .as_ref()
                .expect("sum over histograms yields a histogram element, not a 0.0 float");
            assert_eq!(h.count.to_bits(), 4.0_f64.to_bits(), "2 + 2 populations");
            assert_eq!(h.sum.to_bits(), 20.0_f64.to_bits(), "10 + 10 sums");
        }
    }

    #[test]
    fn range_grid_count_yields_floats_for_histogram_inputs() {
        // `count` over histogram inputs is a float, per the ADR-0108 per-operator
        // table. The float value (2) was already correct pre-fix; what the new
        // histogram-aware channel makes assertable is that this element is a
        // float element (`histogram: None`), never routed through histogram
        // aggregation. The flipped line is `samples[0].histogram.is_none()`
        // alongside the value: it is only observable through
        // `eval_range_hist_annotated`, which did not exist pre-fix.
        let source = TestSource::new()
            .with_histogram_series(&[("__name__", "h"), ("i", "1")], &[(0, nh(2.0, 10.0))])
            .expect("valid histogram series")
            .with_histogram_series(&[("__name__", "h"), ("i", "2")], &[(0, nh(3.0, 12.0))])
            .expect("valid histogram series");
        let (value, _annotations) = Evaluator::new()
            .eval_range_hist_annotated(&source, "count(h)", 0, 60_000, 60_000)
            .expect("range evaluates");
        let crate::eval::RangeValue::Matrix(matrix) = value else {
            panic!("count over histograms must be a matrix");
        };
        assert_eq!(matrix.len(), 1, "one group");
        let samples = &matrix[0].1;
        assert!(!samples.is_empty(), "at least one grid step");
        for s in samples {
            assert!(
                s.histogram.is_none(),
                "count is a float element, never a histogram"
            );
            assert_eq!(
                s.value.to_bits(),
                2.0_f64.to_bits(),
                "two histogram members"
            );
        }
    }

    #[test]
    fn range_grid_undefined_aggregations_drop_histogram_inputs() {
        // `quantile` has no defined native-histogram semantics: Prometheus drops
        // histogram elements. Before the fix `eval_quantile` grouped on the
        // meaningless `0.0` placeholder float of each histogram element, so a
        // histogram-only group produced a bogus `0.0` float series; the flipped
        // assertion is `matrix.is_empty()` (it had one `0.0`-valued series
        // pre-fix).
        let source = TestSource::new()
            .with_histogram_series(&[("__name__", "h"), ("i", "1")], &[(0, nh(2.0, 10.0))])
            .expect("valid histogram series")
            .with_histogram_series(&[("__name__", "h"), ("i", "2")], &[(0, nh(3.0, 12.0))])
            .expect("valid histogram series");
        let (value, _annotations) = Evaluator::new()
            .eval_range_hist_annotated(&source, "quantile(0.5, h)", 0, 60_000, 60_000)
            .expect("range evaluates");
        let crate::eval::RangeValue::Matrix(matrix) = value else {
            panic!("quantile must be a matrix");
        };
        assert!(
            matrix.is_empty(),
            "a histogram-only quantile group is dropped, not emitted as a 0.0 float: {matrix:?}"
        );
    }

    #[test]
    fn range_grid_undefined_aggregations_annotate_the_drop() {
        // The drop of a histogram element by `min`/`max`/`stddev`/`stdvar`/
        // `quantile`/`topk`/`bottomk` must carry Prometheus'
        // `HistogramIgnoredInAggregationInfo`, not be silent (ADR-0108
        // decision 6a; the difftest comparator checks annotation presence).
        // Before item 6a these drops emitted no annotation; the flipped
        // assertion is the non-empty `infos()` naming the aggregation.
        let source = TestSource::new()
            .with_histogram_series(&[("__name__", "h"), ("i", "1")], &[(0, nh(2.0, 10.0))])
            .expect("valid histogram series")
            .with_histogram_series(&[("__name__", "h"), ("i", "2")], &[(0, nh(3.0, 12.0))])
            .expect("valid histogram series");
        for (op, expected) in [
            ("min(h)", "ignored histogram in min aggregation"),
            ("max(h)", "ignored histogram in max aggregation"),
            ("stddev(h)", "ignored histogram in stddev aggregation"),
            ("stdvar(h)", "ignored histogram in stdvar aggregation"),
            (
                "quantile(0.5, h)",
                "ignored histogram in quantile aggregation",
            ),
            ("topk(1, h)", "ignored histogram in topk aggregation"),
            ("bottomk(1, h)", "ignored histogram in bottomk aggregation"),
        ] {
            let (_value, annotations) = Evaluator::new()
                .eval_range_hist_annotated(&source, op, 0, 60_000, 60_000)
                .expect("range evaluates");
            assert!(
                annotations.infos().iter().any(|m| m == expected),
                "{op} must annotate the histogram drop with {expected:?}, got {:?}",
                annotations.infos()
            );
        }
    }

    #[test]
    fn grouping_by_preserves_histogram_element_type() {
        // `sum by (i)` over histogram series keeps each group's element a
        // histogram end to end (through both grid materialization and the
        // grouping rebuild), AND carries the correct per-group population: each
        // `i` label groups a single distinct series, so its summed histogram
        // must be exactly that series' own `count`/`sum`, not a `0.0`
        // placeholder and not another group's value. Before the fix the grid
        // dropped the histogram field, so every group's element was a `0.0`
        // float; the flipped assertions are `s.histogram.is_some()` and the
        // exact `count`/`sum` bit-patterns per group.
        let source = TestSource::new()
            .with_histogram_series(&[("__name__", "h"), ("i", "1")], &[(0, nh(2.0, 10.0))])
            .expect("valid histogram series")
            .with_histogram_series(&[("__name__", "h"), ("i", "2")], &[(0, nh(5.0, 25.0))])
            .expect("valid histogram series");
        let (value, _annotations) = Evaluator::new()
            .eval_range_hist_annotated(&source, "sum by (i) (h)", 0, 60_000, 60_000)
            .expect("range evaluates");
        let crate::eval::RangeValue::Matrix(matrix) = value else {
            panic!("grouped sum must be a matrix");
        };
        assert_eq!(matrix.len(), 2, "one group per distinct i label");
        for (labels, samples) in &matrix {
            let i = labels
                .iter()
                .find(|l| l.name == "i")
                .map(|l| l.value.clone())
                .expect("group keeps its `i` label");
            // `sum` drops `__name__`; the only surviving label is the group key.
            assert!(
                labels
                    .iter()
                    .all(|l| l.name != ravel_types::METRIC_NAME_LABEL),
                "group {labels:?} drops __name__"
            );
            let (want_count, want_sum) = match i.as_str() {
                "1" => (2.0_f64, 10.0_f64),
                "2" => (5.0_f64, 25.0_f64),
                other => panic!("unexpected group i={other}"),
            };
            assert!(!samples.is_empty(), "each group has at least one grid step");
            for s in samples {
                let h = s
                    .histogram
                    .as_ref()
                    .unwrap_or_else(|| panic!("group i={i} preserves its histogram element type"));
                assert_eq!(
                    h.count.to_bits(),
                    want_count.to_bits(),
                    "group i={i} keeps its own population count"
                );
                assert_eq!(
                    h.sum.to_bits(),
                    want_sum.to_bits(),
                    "group i={i} keeps its own observation sum"
                );
            }
        }
    }
}
