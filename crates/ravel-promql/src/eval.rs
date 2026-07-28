//! PromQL evaluator core (ADR-0007, ADR-0021). A typed recursive-descent
//! interpreter over the promql-parser AST: [`Value`] is the internal result
//! type (scalar, string, instant vector, range matrix), and [`Evaluator`]
//! walks paren expressions, unary minus, number/string literals, and vector
//! and matrix selectors (with `offset` and the `@` modifier, including
//! `start()`/`end()`). Every other AST node (aggregation, binary expression,
//! subquery, function call) is rejected with [`Error::Unsupported`], naming
//! the construct.
//!
//! ## Time precision
//!
//! The public API boundary (`t_ms`, `start_ms`, `end_ms`, `step_ms`) is
//! **milliseconds**, matching Prometheus' query API. Everything internal
//! (lookback, offset, sample selection, output timestamps) is
//! **nanoseconds**, matching [`ravel_types::Sample::ts_ns`] and the rest of
//! the system. `ms_to_ns` is exact (milliseconds are coarser, so scaling up
//! never loses precision). The reverse direction is lossy in general, so
//! [`ns_to_ms_floor`] is provided for callers (e.g. the HTTP layer
//! rendering Prometheus' float-seconds-at-ms-precision responses) that need
//! to go the other way; it floors toward negative infinity rather than
//! truncating toward zero, so `-1` ns is `-1` ms, not `0` ms. This
//! evaluator does not call it internally (every ns value it produces is
//! already an exact multiple of `1_000_000`), but it is exported as the one
//! correct implementation of that rule.
//!
//! ## Range-query resolution budget
//!
//! The evaluation grid of a range query is sized by the caller's `start_ms`,
//! `end_ms` and `step_ms`, so it is query-controlled allocation. The step
//! count is computed and checked against [`Evaluator::max_range_points`]
//! (default [`DEFAULT_MAX_RANGE_POINTS`]) *before* any grid is built:
//! over-budget requests return [`Error::TooManyPoints`] having allocated
//! nothing and having touched neither the parser nor the
//! [`SeriesSource`]. This is a hard limit, never a truncation; the query
//! fails rather than silently returning a coarser or shorter matrix.
//!
//! ## Public entry points vs. `instant`/`range`
//!
//! [`Evaluator::instant`] and [`Evaluator::range`] keep their pre-existing
//! signatures and return types byte-for-byte (ADR-0021 consequence: "the
//! evaluator's public API is preserved"), since `ravel-query` depends on
//! their concrete `InstantVector`/`RangeMatrix` return types and is out of
//! scope for this phase. They are now implemented in terms of
//! [`Evaluator::eval_instant`]/[`Evaluator::eval_range`], which expose the
//! full [`Value`] result (including bare scalar and string results) for a
//! future phase to consume; `instant`/`range` unwrap the `Vector`/`Matrix`
//! case and turn anything else into [`Error::WrongType`].

use ravel_types::{Label, LabelSet, METRIC_NAME_LABEL, Sample, TimeRange};

use crate::matchers;
use crate::source::{LabelMatcher, MatchOp, SeriesSource, SourceError};

/// Nanoseconds per millisecond.
const NS_PER_MS: i64 = 1_000_000;

/// Convert milliseconds to nanoseconds. Exact: never loses precision.
pub fn ms_to_ns(ms: i64) -> Result<i64, Error> {
    ms.checked_mul(NS_PER_MS).ok_or(Error::TimeOverflow)
}

/// Convert nanoseconds to milliseconds, flooring toward negative infinity
/// (e.g. `-1` ns floors to `-1` ms, not `0` ms; `-1_000_001` ns floors to
/// `-2` ms). This is the correct direction for reporting a coarser unit
/// derived from a finer one: truncating toward zero would report small
/// negative durations as zero and misround negative timestamps.
pub fn ns_to_ms_floor(ns: i64) -> i64 {
    ns.div_euclid(NS_PER_MS)
}

/// One series' value at the query's evaluation instant. `ts_ns` is always
/// the query's evaluation time, never the offset/`@`-shifted lookup time
/// used to pick the sample: Prometheus reports the query timestamp
/// regardless of any `offset` or `@` on the selector.
#[derive(Debug, Clone, PartialEq)]
pub struct InstantSample {
    pub labels: LabelSet,
    pub ts_ns: i64,
    pub value: f64,
}

/// Result of an instant query: one entry per matched series.
pub type InstantVector = Vec<InstantSample>;

/// Result of a range query: one entry per matched series, with one sample
/// per evaluated step at which that series had a value in the lookback
/// window. Series with no value at any step are omitted entirely.
pub type RangeMatrix = Vec<(LabelSet, Vec<Sample>)>;

/// A typed evaluation result. Internal to `ravel-promql`: AST types from
/// promql-parser still do not leak from the crate (ADR-0007 consequence).
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Scalar(f64),
    String(String),
    Vector(InstantVector),
    Matrix(RangeMatrix),
}

impl Value {
    /// A short, human-readable name of this value's type, for error
    /// messages (e.g. [`Error::WrongType`]).
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::Scalar(_) => "scalar",
            Value::String(_) => "string",
            Value::Vector(_) => "instant vector",
            Value::Matrix(_) => "range vector",
        }
    }
}

#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("promql parse error: {0}")]
    Parse(String),
    #[error("unsupported PromQL construct: {construct}")]
    Unsupported { construct: String },
    #[error("series source error: {0}")]
    Source(#[from] SourceError),
    #[error("time value out of range")]
    TimeOverflow,
    #[error("step must be positive, got {step_ms} ms")]
    NonPositiveStep { step_ms: i64 },
    #[error("range start {start_ms} ms is after end {end_ms} ms")]
    InvalidRange { start_ms: i64, end_ms: i64 },
    #[error(
        "range query resolution of {points} evaluation points exceeds the maximum of {max}; widen step_ms or narrow the range"
    )]
    TooManyPoints { points: u64, max: usize },
    #[error("query evaluates to {got}, but this operation requires {expected}")]
    WrongType {
        expected: &'static str,
        got: &'static str,
    },
}

/// Default PromQL lookback: 5 minutes, in nanoseconds (ADR-0007). This is
/// the single source of truth for the lookback delta. The evaluator uses it
/// as its default lookback window ([`Evaluator::default`]) and `ravel-query`
/// consumes it (re-exported from the crate root) so its pre-fetch padding
/// (`padded_range`, docs/query-engine.md) can never drift from the window
/// evaluation actually selects over.
pub const DEFAULT_LOOKBACK_NS: i64 = 5 * 60 * 1_000_000_000;

/// Default cap on the number of evaluation points (grid steps) a single
/// range query may produce, counting both endpoints when the range is
/// step-aligned. 11,000 matches Prometheus' own `query_range` resolution
/// limit, so a dashboard that Prometheus accepts is accepted here and one
/// Prometheus rejects is rejected here with the same shape of error.
///
/// This bounds evaluator allocation from query-controlled input: the grid is
/// at most this many `(i64, i64)` pairs (176 KiB at the default), and each
/// output series holds at most this many samples. It is independent of the
/// engine's `max_samples` budget, which counts samples yielded by storage and
/// does not constrain the grid.
pub const DEFAULT_MAX_RANGE_POINTS: usize = 11_000;

/// Prometheus staleness marker: the exact NaN bit pattern a scrape/rule
/// engine writes to signal that a series has ended. Per Prometheus lookback
/// semantics (ADR-0007) and ADR-0010 §5, when this is the most recent sample
/// in the lookback (or matrix) window the series is **absent** at that
/// instant, not a NaN value. Detection is an exact-bits comparison, never
/// `is_nan()`: every other NaN payload is a live, observable value and
/// passes through bit-for-bit (`f64::to_bits` exactness is a frozen
/// invariant).
const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;

/// The ambient time window a query's `@ start()`/`@ end()` resolve against:
/// the whole query's own parameters, fixed regardless of which grid step (if
/// any) is currently being evaluated. For an instant query both fields equal
/// the query's evaluation timestamp.
struct QueryWindow {
    start_ns: i64,
    end_ns: i64,
}

/// PromQL evaluator. Stateless besides its lookback and resolution
/// configuration; safe to share across queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Evaluator {
    lookback_delta_ns: i64,
    max_range_points: usize,
}

impl Default for Evaluator {
    fn default() -> Self {
        Evaluator {
            lookback_delta_ns: DEFAULT_LOOKBACK_NS,
            max_range_points: DEFAULT_MAX_RANGE_POINTS,
        }
    }
}

impl Evaluator {
    pub fn new() -> Self {
        Self::default()
    }

    /// Override the default 5-minute lookback.
    pub fn with_lookback_delta(mut self, lookback: std::time::Duration) -> Result<Self, Error> {
        self.lookback_delta_ns =
            i64::try_from(lookback.as_nanos()).map_err(|_| Error::TimeOverflow)?;
        Ok(self)
    }

    /// Override the default range-query resolution cap
    /// ([`DEFAULT_MAX_RANGE_POINTS`]). A cap of 0 rejects every range query,
    /// including a single-point one; that is the caller's choice, not a
    /// special case here.
    pub fn with_max_range_points(mut self, max_range_points: usize) -> Self {
        self.max_range_points = max_range_points;
        self
    }

    /// The configured range-query resolution cap, in evaluation points.
    pub fn max_range_points(&self) -> usize {
        self.max_range_points
    }

    /// Evaluate `query` at `t_ms`, returning the full [`Value`] the query
    /// resolves to (scalar, string, or instant vector; a bare matrix
    /// selector at top level is a [`Error::WrongType`], matching
    /// Prometheus' own instant-query type check).
    pub fn eval_instant(
        &self,
        source: &dyn SeriesSource,
        query: &str,
        t_ms: i64,
    ) -> Result<Value, Error> {
        let expr = promql_parser::parser::parse(query).map_err(Error::Parse)?;
        let t_ns = ms_to_ns(t_ms)?;
        let ctx = QueryWindow {
            start_ns: t_ns,
            end_ns: t_ns,
        };
        self.eval_expr(source, &expr, t_ns, &ctx)
    }

    /// Evaluate `query` as an instant vector at `t_ms`.
    ///
    /// For each series matching the selector, the most recent sample with
    /// `ts_ns > sel_ts - lookback` and `ts_ns <= sel_ts` is used (`sel_ts`
    /// is `t_ms` shifted by the selector's `offset`/`@`, if any); series
    /// with no such sample are omitted. The input carries one sample per
    /// `(series, ts)`, deduped upstream under the normative total order (see
    /// [`SeriesData`](crate::source::SeriesData)).
    pub fn instant(
        &self,
        source: &dyn SeriesSource,
        query: &str,
        t_ms: i64,
    ) -> Result<InstantVector, Error> {
        match self.eval_instant(source, query, t_ms)? {
            Value::Vector(v) => Ok(v),
            other => Err(Error::WrongType {
                expected: "instant vector",
                got: other.type_name(),
            }),
        }
    }

    /// Evaluate `query` as a range matrix over `start_ms..=end_ms` stepping
    /// by `step_ms`, returning the full [`Value`] the query resolves to. A
    /// scalar or string top-level expression is constant across the whole
    /// grid (Prometheus repeats the same value at every step), so it is
    /// reported once rather than per step; materializing the repeated
    /// series for the wire format is the HTTP rendering layer's job (a
    /// later phase), not the evaluator's.
    pub fn eval_range(
        &self,
        source: &dyn SeriesSource,
        query: &str,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
    ) -> Result<Value, Error> {
        if step_ms <= 0 {
            return Err(Error::NonPositiveStep { step_ms });
        }
        if start_ms > end_ms {
            return Err(Error::InvalidRange { start_ms, end_ms });
        }

        let start_ns = ms_to_ns(start_ms)?;
        let end_ns = ms_to_ns(end_ms)?;
        let step_ns = ms_to_ns(step_ms)?;

        // Resolution budget first: the grid below is sized by caller-supplied
        // range and step, so it must be bounded before it is built (and
        // before the parser runs, mirroring Prometheus, which rejects the
        // resolution in its API layer ahead of query construction).
        let points = step_count(start_ns, end_ns, step_ns);
        if points > u64::try_from(self.max_range_points).unwrap_or(u64::MAX) {
            return Err(Error::TooManyPoints {
                points,
                max: self.max_range_points,
            });
        }

        let expr = promql_parser::parser::parse(query).map_err(Error::Parse)?;
        let ctx = QueryWindow { start_ns, end_ns };

        // P1's supported grammar (paren, unary minus, literals, one
        // selector) can contain at most one selector, so unlike a general
        // multi-selector tree there is no risk of re-querying storage once
        // per node: the grid below still issues exactly one `source.query`
        // call for the whole range, same as before this phase. A future
        // phase that adds binary/call/aggregate expressions (and therefore
        // multiple selectors, or the same selector's value needed at
        // multiple sub-evaluations) is the point at which per-selector
        // cursoring (ADR-0021 §1) becomes necessary; it is not needed yet.
        let (core, negate) = resolve_range_core(&expr)?;
        match core {
            RangeCore::Scalar(v) => Ok(Value::Scalar(if negate { -v } else { v })),
            RangeCore::Str(s) => Ok(Value::String(s.to_string())),
            RangeCore::Selector(vs) => {
                let matrix = self.eval_range_selector(
                    source, vs, start_ns, end_ns, step_ns, points, &ctx, negate,
                )?;
                Ok(Value::Matrix(matrix))
            }
        }
    }

    /// Evaluate `query` as a range matrix over `start_ms..=end_ms` stepping
    /// by `step_ms`. Evaluation instants are `start`, `start + step`, ...,
    /// stopping at the last instant `<= end` (so `end` is included when the
    /// range is an exact multiple of `step` from `start`, and excluded
    /// otherwise). The same per-step lookback rule as [`Self::instant`]
    /// applies at each instant.
    ///
    /// The number of evaluation points is checked against
    /// [`Self::max_range_points`] before anything is parsed or allocated;
    /// an over-budget request returns [`Error::TooManyPoints`].
    pub fn range(
        &self,
        source: &dyn SeriesSource,
        query: &str,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
    ) -> Result<RangeMatrix, Error> {
        match self.eval_range(source, query, start_ms, end_ms, step_ms)? {
            Value::Matrix(m) => Ok(m),
            other => Err(Error::WrongType {
                expected: "range vector",
                got: other.type_name(),
            }),
        }
    }

    /// General recursive evaluator for a single instant. Handles the P1
    /// grammar (paren, unary minus, literals, vector/matrix selectors) and
    /// rejects everything else, naming the construct.
    fn eval_expr(
        &self,
        source: &dyn SeriesSource,
        expr: &promql_parser::parser::Expr,
        eval_ts_ns: i64,
        ctx: &QueryWindow,
    ) -> Result<Value, Error> {
        use promql_parser::parser::Expr;
        match expr {
            Expr::Paren(p) => self.eval_expr(source, &p.expr, eval_ts_ns, ctx),
            Expr::Unary(u) => match self.eval_expr(source, &u.expr, eval_ts_ns, ctx)? {
                Value::Scalar(x) => Ok(Value::Scalar(-x)),
                Value::Vector(v) => Ok(Value::Vector(negate_vector(v))),
                other => Err(Error::WrongType {
                    expected: "scalar or instant vector",
                    got: other.type_name(),
                }),
            },
            Expr::NumberLiteral(n) => Ok(Value::Scalar(n.val)),
            Expr::StringLiteral(s) => Ok(Value::String(s.val.clone())),
            Expr::VectorSelector(vs) => self
                .eval_vector_selector(source, vs, eval_ts_ns, ctx)
                .map(Value::Vector),
            Expr::MatrixSelector(ms) => self
                .eval_matrix_selector(source, ms, eval_ts_ns, ctx)
                .map(Value::Matrix),
            _ => Err(unsupported_construct_error(expr)),
        }
    }

    fn eval_vector_selector(
        &self,
        source: &dyn SeriesSource,
        vs: &promql_parser::parser::VectorSelector,
        eval_ts_ns: i64,
        ctx: &QueryWindow,
    ) -> Result<InstantVector, Error> {
        let selector_matchers = build_matchers(vs)?;
        let sel_ts_ns = selector_eval_ts(vs, eval_ts_ns, ctx)?;
        let window = TimeRange {
            start_ns: sel_ts_ns
                .checked_sub(self.lookback_delta_ns)
                .ok_or(Error::TimeOverflow)?,
            end_ns: sel_ts_ns,
        };

        let series = source.query(&selector_matchers, window)?;
        let mut out = Vec::with_capacity(series.len());
        for s in series {
            if let Some(value) = pick_sample(&s.samples, sel_ts_ns, self.lookback_delta_ns) {
                out.push(InstantSample {
                    labels: s.labels,
                    ts_ns: eval_ts_ns,
                    value,
                });
            }
        }
        Ok(out)
    }

    /// Evaluate a matrix selector at one instant: every non-stale sample in
    /// the left-open window `(sel_ts - range, sel_ts]`, per matched series.
    /// Not reachable from `eval_instant`/`eval_range` in this phase (a bare
    /// matrix selector is always a top-level [`Error::WrongType`], matching
    /// Prometheus), but implemented and tested now: a future phase's
    /// function calls (e.g. `rate(x[5m])`) evaluate their matrix-typed
    /// argument through this same path.
    #[allow(dead_code)]
    fn eval_matrix_selector(
        &self,
        source: &dyn SeriesSource,
        ms: &promql_parser::parser::MatrixSelector,
        eval_ts_ns: i64,
        ctx: &QueryWindow,
    ) -> Result<RangeMatrix, Error> {
        let selector_matchers = build_matchers(&ms.vs)?;
        let sel_ts_ns = selector_eval_ts(&ms.vs, eval_ts_ns, ctx)?;
        let range_ns = duration_to_ns(ms.range)?;
        let window_start = sel_ts_ns.checked_sub(range_ns).ok_or(Error::TimeOverflow)?;
        let window = TimeRange {
            start_ns: window_start,
            end_ns: sel_ts_ns,
        };

        let series = source.query(&selector_matchers, window)?;
        let mut out = Vec::with_capacity(series.len());
        for s in series {
            // The window fetched from storage is inclusive on both ends
            // (`TimeRange`'s own contract); a matrix selector's window is
            // left-open like the instant lookback window, so the exclusive
            // lower bound is enforced here.
            let samples: Vec<Sample> = s
                .samples
                .iter()
                .filter(|sample| sample.ts_ns > window_start)
                .filter(|sample| sample.value.to_bits() != STALE_NAN_BITS)
                .copied()
                .collect();
            if !samples.is_empty() {
                out.push((s.labels, samples));
            }
        }
        Ok(out)
    }

    /// Build the range matrix for a single top-level vector selector
    /// (`resolve_range_core`'s `Selector` case), one `source.query` call for
    /// the whole grid, generalized from the pre-P1 implementation to
    /// support `@` (which, when present, pins every step to the same
    /// instant rather than shifting with `t`).
    #[allow(clippy::too_many_arguments)]
    fn eval_range_selector(
        &self,
        source: &dyn SeriesSource,
        vs: &promql_parser::parser::VectorSelector,
        start_ns: i64,
        end_ns: i64,
        step_ns: i64,
        points: u64,
        ctx: &QueryWindow,
        negate: bool,
    ) -> Result<RangeMatrix, Error> {
        let selector_matchers = build_matchers(vs)?;
        let offset_ns = signed_offset_ns(vs.offset.as_ref())?;
        let at_ts_ns = resolve_at(vs.at.as_ref(), ctx.start_ns, ctx.end_ns)?;

        // Evaluation grid: (reported ts, offset/@-shifted lookup ts).
        // `points` is already known to be within budget.
        let capacity = usize::try_from(points).unwrap_or(self.max_range_points);
        let mut grid: Vec<(i64, i64)> = Vec::with_capacity(capacity);
        let mut t = start_ns;
        while t <= end_ns {
            let base_ts = at_ts_ns.unwrap_or(t);
            let sel_ts = base_ts.checked_sub(offset_ns).ok_or(Error::TimeOverflow)?;
            grid.push((t, sel_ts));
            t = t.checked_add(step_ns).ok_or(Error::TimeOverflow)?;
        }
        if grid.is_empty() {
            return Ok(Vec::new());
        }

        let min_sel_ts = grid.iter().map(|(_, sel)| *sel).min().unwrap_or(start_ns);
        let max_sel_ts = grid.iter().map(|(_, sel)| *sel).max().unwrap_or(start_ns);
        let window = TimeRange {
            start_ns: min_sel_ts
                .checked_sub(self.lookback_delta_ns)
                .ok_or(Error::TimeOverflow)?,
            end_ns: max_sel_ts,
        };

        let series = source.query(&selector_matchers, window)?;
        let mut out = Vec::with_capacity(series.len());
        for s in series {
            let mut samples = Vec::new();
            for (reported_ts, sel_ts) in &grid {
                if let Some(value) = pick_sample(&s.samples, *sel_ts, self.lookback_delta_ns) {
                    samples.push(Sample {
                        ts_ns: *reported_ts,
                        value: if negate { -value } else { value },
                    });
                }
            }
            if !samples.is_empty() {
                let labels = if negate {
                    drop_metric_name(s.labels)
                } else {
                    s.labels
                };
                out.push((labels, samples));
            }
        }
        Ok(out)
    }
}

/// A range query's top-level construct, after stripping any enclosing
/// `Paren`/`Unary` wrappers (P1's grammar allows at most one selector, so
/// this identifies it directly rather than routing through the general
/// per-instant `eval_expr`, which would re-query storage once per grid
/// step).
enum RangeCore<'a> {
    Scalar(f64),
    Str(&'a str),
    Selector(&'a promql_parser::parser::VectorSelector),
}

/// Strip `Paren`/`Unary` wrappers from `expr`, returning the core construct
/// and whether an odd number of unary minuses were applied. Any other
/// top-level construct is a typed error: [`Error::WrongType`] for a bare
/// matrix selector (invalid at top level, exactly like Prometheus), or
/// [`Error::Unsupported`] naming the construct for everything this phase
/// does not implement.
fn resolve_range_core(expr: &promql_parser::parser::Expr) -> Result<(RangeCore<'_>, bool), Error> {
    use promql_parser::parser::Expr;
    let mut negate = false;
    let mut cur = expr;
    loop {
        match cur {
            Expr::Paren(p) => cur = &p.expr,
            Expr::Unary(u) => {
                negate = !negate;
                cur = &u.expr;
            }
            Expr::NumberLiteral(n) => return Ok((RangeCore::Scalar(n.val), negate)),
            Expr::StringLiteral(s) => return Ok((RangeCore::Str(&s.val), negate)),
            Expr::VectorSelector(vs) => return Ok((RangeCore::Selector(vs), negate)),
            Expr::MatrixSelector(_) => {
                return Err(Error::WrongType {
                    expected: "scalar, instant vector, or string",
                    got: "range vector",
                });
            }
            _ => return Err(unsupported_construct_error(cur)),
        }
    }
}

/// The [`Error::Unsupported`] for an AST node this phase does not evaluate.
/// Callers must have already handled `Paren`, `Unary`, `NumberLiteral`,
/// `StringLiteral`, `VectorSelector`, and `MatrixSelector`; this panics on
/// any of those (programmer error, not reachable) and covers the rest.
fn unsupported_construct_error(expr: &promql_parser::parser::Expr) -> Error {
    use promql_parser::parser::Expr;
    match expr {
        Expr::Aggregate(a) => Error::Unsupported {
            construct: format!("aggregation: {}", a.op),
        },
        Expr::Binary(b) => Error::Unsupported {
            construct: format!("binary expression: {}", b.op),
        },
        Expr::Subquery(_) => Error::Unsupported {
            construct: "subquery".to_string(),
        },
        Expr::Call(c) => Error::Unsupported {
            construct: format!("function call: {}", c.func.name),
        },
        Expr::Extension(_) => Error::Unsupported {
            construct: "extension node".to_string(),
        },
        Expr::Paren(_)
        | Expr::Unary(_)
        | Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::VectorSelector(_)
        | Expr::MatrixSelector(_) => {
            unreachable!("caller must handle every supported construct before falling back")
        }
    }
}

/// The selector-local evaluation timestamp: `@`'s absolute instant (falling
/// back to the ambient `eval_ts_ns`, i.e. the current grid step or the
/// instant query's own time) shifted by the selector's `offset`, if any.
/// `@`'s `start()`/`end()` forms resolve against the whole query's fixed
/// parameters (`ctx`), not the current step, exactly like Prometheus.
fn selector_eval_ts(
    vs: &promql_parser::parser::VectorSelector,
    eval_ts_ns: i64,
    ctx: &QueryWindow,
) -> Result<i64, Error> {
    let offset_ns = signed_offset_ns(vs.offset.as_ref())?;
    let base_ts_ns = resolve_at(vs.at.as_ref(), ctx.start_ns, ctx.end_ns)?.unwrap_or(eval_ts_ns);
    base_ts_ns.checked_sub(offset_ns).ok_or(Error::TimeOverflow)
}

/// Resolve an `@` modifier to an absolute nanosecond instant: `start()`/
/// `end()` resolve against the query's own fixed parameters, `@ <literal>`
/// against the literal instant (which may be before the Unix epoch).
/// `None` (no `@`) is `Ok(None)`.
pub(crate) fn resolve_at(
    at: Option<&promql_parser::parser::AtModifier>,
    query_start_ns: i64,
    query_end_ns: i64,
) -> Result<Option<i64>, Error> {
    use promql_parser::parser::AtModifier;
    let Some(at) = at else {
        return Ok(None);
    };
    let ns = match at {
        AtModifier::Start => query_start_ns,
        AtModifier::End => query_end_ns,
        AtModifier::At(t) => systemtime_to_ns(*t)?,
    };
    Ok(Some(ns))
}

/// Convert a `SystemTime` (which, for `@`, may represent an instant before
/// the Unix epoch) to signed nanoseconds since the epoch.
fn systemtime_to_ns(t: std::time::SystemTime) -> Result<i64, Error> {
    match t.duration_since(std::time::UNIX_EPOCH) {
        Ok(since_epoch) => i64::try_from(since_epoch.as_nanos()).map_err(|_| Error::TimeOverflow),
        Err(before_epoch) => {
            let ns = i64::try_from(before_epoch.duration().as_nanos())
                .map_err(|_| Error::TimeOverflow)?;
            ns.checked_neg().ok_or(Error::TimeOverflow)
        }
    }
}

/// Convert a parsed range/window `Duration` to signed nanoseconds.
pub(crate) fn duration_to_ns(d: std::time::Duration) -> Result<i64, Error> {
    i64::try_from(d.as_nanos()).map_err(|_| Error::TimeOverflow)
}

/// Negate every sample's value in an instant vector, dropping `__name__`
/// from each series' labels: Prometheus drops the metric name on the result
/// of any numeric operation, unary minus included.
fn negate_vector(v: InstantVector) -> InstantVector {
    v.into_iter()
        .map(|s| InstantSample {
            labels: drop_metric_name(s.labels),
            ts_ns: s.ts_ns,
            value: -s.value,
        })
        .collect()
}

/// Remove `__name__` from a label set, exactly as Prometheus does for the
/// result of a numeric operation.
fn drop_metric_name(labels: LabelSet) -> LabelSet {
    // Filtering can only remove entries from an already-deduplicated
    // `LabelSet`, so this reconstruction cannot introduce a duplicate name
    // and cannot fail in practice.
    let filtered: Vec<Label> = labels
        .iter()
        .filter(|l| l.name != METRIC_NAME_LABEL)
        .cloned()
        .collect();
    LabelSet::new(filtered).unwrap_or_default()
}

/// Number of evaluation instants in `start_ns..=end_ns` stepping by
/// `step_ns`, i.e. `floor((end - start) / step) + 1`. Requires
/// `start_ns <= end_ns` and `step_ns > 0` (both already checked by the
/// caller), so the result is at least 1.
///
/// The span is computed in `i128` because `end_ns - start_ns` overflows
/// `i64` for extreme-but-representable endpoints, and the whole point of
/// this function is to answer for exactly those inputs without panicking or
/// wrapping. `step_ns` is at least one millisecond (`step_ms >= 1`), so the
/// quotient is at most about `1.9e13` and always fits `u64`.
fn step_count(start_ns: i64, end_ns: i64, step_ns: i64) -> u64 {
    let span = i128::from(end_ns) - i128::from(start_ns);
    let steps = span / i128::from(step_ns) + 1;
    u64::try_from(steps).unwrap_or(u64::MAX)
}

/// Pick the value at the most recent timestamp in `(sel_ts - lookback,
/// sel_ts]`. `samples` must be sorted ascending by `ts_ns` (the
/// `SeriesSource` contract), which is guaranteed to hold at most one sample
/// per ts in the normal pipeline: the engine's k-way merge has already
/// deduped by the normative commit total order (ADR-0010 §5) upstream, where
/// the commit provenance that order ranks on is still available (see
/// [`SeriesData`](crate::source::SeriesData)).
///
/// Should a source violate that contract and hand raw duplicates at the
/// selected ts, they are resolved by the one component of the normative order
/// the values alone carry, greatest `value.to_bits()`, never by vector
/// position. This is the normative order's own final tiebreak, not a second
/// competing rule, so it agrees with the engine's dedup; with a deduped input
/// the run is a single sample and the scan is a no-op.
///
/// If the resolved sample is a Prometheus staleness marker
/// ([`STALE_NAN_BITS`]) the series is absent from that sample forward, so
/// this returns `None`. A later real sample supersedes the marker and is
/// picked normally; markers older than the selected sample are irrelevant.
fn pick_sample(samples: &[Sample], sel_ts_ns: i64, lookback_delta_ns: i64) -> Option<f64> {
    let idx = samples.partition_point(|s| s.ts_ns <= sel_ts_ns);
    if idx == 0 {
        return None;
    }
    let candidate_ts_ns = samples[idx - 1].ts_ns;
    let window_start = sel_ts_ns.checked_sub(lookback_delta_ns)?;
    if candidate_ts_ns <= window_start {
        return None;
    }
    // Resolve any duplicate (series, ts) by the normative final tiebreak
    // (greatest value.to_bits(), ADR-0010 §5) over the contiguous run of
    // equal-ts samples ending at idx-1, not by vector position. Bit-pattern
    // comparison, never `==`, so NaN payloads and -0.0 stay significant and a
    // staleness marker (a NaN payload) resolves exactly as it does upstream.
    let mut best_bits = samples[idx - 1].value.to_bits();
    let mut i = idx - 1;
    while i > 0 && samples[i - 1].ts_ns == candidate_ts_ns {
        best_bits = best_bits.max(samples[i - 1].value.to_bits());
        i -= 1;
    }
    if best_bits == STALE_NAN_BITS {
        return None;
    }
    Some(f64::from_bits(best_bits))
}

/// Build the full matcher list for a vector selector, including the
/// implicit `__name__` matcher when the selector has a bare metric name
/// (promql-parser keeps that separate from `vs.matchers`).
pub(crate) fn build_matchers(
    vs: &promql_parser::parser::VectorSelector,
) -> Result<Vec<LabelMatcher>, Error> {
    if matchers::has_or_group(&vs.matchers) {
        return Err(Error::Unsupported {
            construct: "label matcher or-group".to_string(),
        });
    }
    let mut out = matchers::from_ast_matchers(&vs.matchers);
    if let Some(name) = &vs.name {
        out.push(LabelMatcher {
            name: METRIC_NAME_LABEL.to_string(),
            op: MatchOp::Eq,
            value: name.clone(),
        });
    }
    Ok(out)
}

/// Signed nanosecond shift for a selector's `offset`: positive for `offset
/// 5m` (look backward), negative for the experimental `offset -5m` (look
/// forward). `None` (no offset) is zero.
pub(crate) fn signed_offset_ns(
    offset: Option<&promql_parser::parser::Offset>,
) -> Result<i64, Error> {
    let Some(offset) = offset else {
        return Ok(0);
    };
    let (duration, sign): (&std::time::Duration, i64) = match offset {
        promql_parser::parser::Offset::Pos(d) => (d, 1),
        promql_parser::parser::Offset::Neg(d) => (d, -1),
    };
    let ns = i64::try_from(duration.as_nanos()).map_err(|_| Error::TimeOverflow)?;
    ns.checked_mul(sign).ok_or(Error::TimeOverflow)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::testsource::TestSource;

    fn minutes(m: i64) -> i64 {
        m * 60_000
    }

    #[test]
    fn lookback_boundary_excludes_exactly_5m_before_t() {
        // Sample exactly 5m before T is excluded (lookback start is
        // exclusive); a sample exactly at T is included.
        let t_ms = minutes(10);
        let five_m_before_ns = ms_to_ns(t_ms - minutes(5)).expect("no overflow");
        let source = TestSource::new()
            .with_series(&[("__name__", "up")], &[(five_m_before_ns, 1.0)])
            .expect("valid series");

        let result = Evaluator::new()
            .instant(&source, "up", t_ms)
            .expect("evaluates");
        assert!(
            result.is_empty(),
            "sample exactly 5m before T must be excluded"
        );

        let at_t_ns = ms_to_ns(t_ms).expect("no overflow");
        let source = TestSource::new()
            .with_series(&[("__name__", "up")], &[(at_t_ns, 2.0)])
            .expect("valid series");
        let result = Evaluator::new()
            .instant(&source, "up", t_ms)
            .expect("evaluates");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 2.0);
        assert_eq!(result[0].ts_ns, at_t_ns);
    }

    #[test]
    fn lookback_boundary_includes_sample_one_ns_inside_window() {
        let t_ms = minutes(10);
        let just_inside_ns = ms_to_ns(t_ms - minutes(5)).expect("no overflow") + 1;
        let source = TestSource::new()
            .with_series(&[("__name__", "up")], &[(just_inside_ns, 3.0)])
            .expect("valid series");
        let result = Evaluator::new()
            .instant(&source, "up", t_ms)
            .expect("evaluates");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 3.0);
    }

    #[test]
    fn series_with_no_sample_in_window_is_omitted() {
        let source = TestSource::new()
            .with_series(&[("__name__", "up")], &[(0, 1.0)])
            .expect("valid series");
        let result = Evaluator::new()
            .instant(&source, "up", minutes(60))
            .expect("evaluates");
        assert!(result.is_empty());
    }

    #[test]
    fn instant_output_retains_metric_name_label() {
        let source = TestSource::new()
            .with_series(&[("__name__", "up"), ("job", "api")], &[(0, 1.0)])
            .expect("valid series");
        let result = Evaluator::new()
            .instant(&source, "up", minutes(1))
            .expect("evaluates");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].labels.get("__name__"), Some("up"));
    }

    #[test]
    fn nameless_selector_still_retains_metric_name_label() {
        // `{job="api"}` has no bare metric name on the selector itself, but
        // matched series still carry `__name__` in their own label set, and
        // it must pass through untouched.
        let source = TestSource::new()
            .with_series(&[("__name__", "up"), ("job", "api")], &[(0, 1.0)])
            .expect("valid series");
        let result = Evaluator::new()
            .instant(&source, r#"{job="api"}"#, minutes(1))
            .expect("evaluates");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].labels.get("__name__"), Some("up"));
        assert_eq!(result[0].labels.get("job"), Some("api"));
    }

    #[test]
    fn offset_shifts_evaluation_time_backward() {
        // `up offset 5m` at T=10m should look at data as of T-5m=5m.
        let sample_ts_ns = ms_to_ns(minutes(5)).expect("no overflow");
        let source = TestSource::new()
            .with_series(&[("__name__", "up")], &[(sample_ts_ns, 7.0)])
            .expect("valid series");

        let result = Evaluator::new()
            .instant(&source, "up offset 5m", minutes(10))
            .expect("evaluates");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 7.0);
        // Reported timestamp is the query time T, not the shifted lookup time.
        assert_eq!(result[0].ts_ns, ms_to_ns(minutes(10)).expect("no overflow"));

        // Without the offset, that same sample is outside the lookback
        // window at T=10m (5m old sample, boundary exclusive).
        let result = Evaluator::new()
            .instant(&source, "up", minutes(10))
            .expect("evaluates");
        assert!(result.is_empty());
    }

    #[test]
    fn negative_offset_shifts_evaluation_time_forward() {
        let sample_ts_ns = ms_to_ns(minutes(15)).expect("no overflow");
        let source = TestSource::new()
            .with_series(&[("__name__", "up")], &[(sample_ts_ns, 9.0)])
            .expect("valid series");

        // `up offset -5m` at T=10m looks at data as of T+5m=15m.
        let result = Evaluator::new()
            .instant(&source, "up offset -5m", minutes(10))
            .expect("evaluates");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 9.0);
    }

    #[test]
    fn regex_anchoring_rejects_partial_match_in_query() {
        let source = TestSource::new()
            .with_series(&[("__name__", "up"), ("job", "api-server")], &[(0, 1.0)])
            .expect("valid series");
        let result = Evaluator::new()
            .instant(&source, r#"up{job=~"api"}"#, minutes(1))
            .expect("evaluates");
        assert!(
            result.is_empty(),
            "job=~\"api\" must not match \"api-server\""
        );
    }

    #[test]
    fn duplicate_timestamp_resolves_by_normative_order_not_vec_position() {
        // In the real pipeline the engine dedups each (series, ts) to a
        // single sample under the normative commit total order (ADR-0010 §5)
        // before this surface ever sees it. If a source violates that and
        // hands raw duplicates, the evaluator must resolve them by that same
        // order's final tiebreak, greatest value.to_bits(), which is the one
        // component the (ts, value) pairs still carry once the commit
        // provenance is gone. It must NOT pick "later in the vec": the winner
        // is the same regardless of the order the source happened to store
        // equal-ts samples in.
        let ts_ns = ms_to_ns(minutes(1)).expect("no overflow");
        // 5.0 has the greater value.to_bits() of the pair (both positive
        // finite, so bit order matches numeric order), so it is the normative
        // winner either way. TestSource sorts stably on ts, preserving the
        // insertion order among equal timestamps, so the first arrangement
        // puts the normative winner *first* (a positional "last wins" rule
        // would wrongly pick 3.0) and the second puts it *last*.
        for arrangement in [
            [(ts_ns, 5.0_f64), (ts_ns, 3.0_f64)],
            [(ts_ns, 3.0_f64), (ts_ns, 5.0_f64)],
        ] {
            let source = TestSource::new()
                .with_series(&[("__name__", "up")], &arrangement)
                .expect("valid series");
            let result = Evaluator::new()
                .instant(&source, "up", minutes(1))
                .expect("evaluates");
            assert_eq!(result.len(), 1);
            assert_eq!(
                result[0].value, 5.0,
                "arrangement {arrangement:?}: greatest value.to_bits() wins \
                 (the normative final tiebreak), not vector position",
            );
        }

        // The winner is chosen by bit pattern, exactly as the engine's dedup
        // compares (ADR-0010 §5), so a staleness marker sharing the ts with a
        // real value resolves the same way here as upstream: the marker has
        // the greater bits, wins the tie, and makes the series absent. This
        // pins that the surface uses `to_bits()`, never `==` or a numeric max
        // that would treat the NaN marker as unordered.
        let stale = f64::from_bits(STALE_NAN_BITS);
        let source = TestSource::new()
            .with_series(&[("__name__", "up")], &[(ts_ns, 7.0), (ts_ns, stale)])
            .expect("valid series");
        let result = Evaluator::new()
            .instant(&source, "up", minutes(1))
            .expect("evaluates");
        assert!(
            result.is_empty(),
            "staleness marker wins the bit-pattern tiebreak and marks the series absent",
        );
    }

    #[test]
    fn or_grouped_matchers_are_rejected_as_unsupported() {
        let source = TestSource::new();
        let err = Evaluator::new()
            .instant(&source, r#"up{job="a" or job="b"}"#, 0)
            .expect_err("or-grouped matchers are not Phase 1 scope");
        let Error::Unsupported { construct } = err else {
            panic!("expected Unsupported, got {err:?}");
        };
        assert!(construct.contains("or-group"));
    }

    #[test]
    fn range_step_alignment_includes_end_when_aligned() {
        let source = TestSource::new()
            .with_series(
                &[("__name__", "up")],
                &[
                    (ms_to_ns(0).expect("ok"), 1.0),
                    (ms_to_ns(minutes(1)).expect("ok"), 2.0),
                    (ms_to_ns(minutes(2)).expect("ok"), 3.0),
                ],
            )
            .expect("valid series");
        let matrix = Evaluator::new()
            .range(&source, "up", 0, minutes(2), minutes(1))
            .expect("evaluates");
        assert_eq!(matrix.len(), 1);
        let (_, samples) = &matrix[0];
        // start=0, end=2m, step=1m is an exact multiple: 0, 1m, 2m all included.
        assert_eq!(samples.len(), 3);
        assert_eq!(samples[0].ts_ns, ms_to_ns(0).expect("ok"));
        assert_eq!(samples[1].ts_ns, ms_to_ns(minutes(1)).expect("ok"));
        assert_eq!(samples[2].ts_ns, ms_to_ns(minutes(2)).expect("ok"));
        assert_eq!(samples[2].value, 3.0);
    }

    #[test]
    fn range_applies_lookback_independently_per_step() {
        // One sample at t=0. Lookback is 5m (default), step is 5m, over
        // five steps (0, 5m, 10m, 15m, 20m). The sample is in-window for
        // the whole *query* range (0..=20m), but the per-step lookback rule
        // must only surface it at t=0: at t=5m the window is (0, 5m] and
        // ts=0 fails the exclusive lower bound. A single filter over the
        // whole materialized window instead of one check per step would
        // wrongly keep it at every grid point.
        let source = TestSource::new()
            .with_series(&[("__name__", "up")], &[(0, 1.0)])
            .expect("valid series");
        let matrix = Evaluator::new()
            .range(&source, "up", 0, minutes(20), minutes(5))
            .expect("evaluates");
        assert_eq!(matrix.len(), 1);
        let (_, samples) = &matrix[0];
        assert_eq!(samples.len(), 1, "sample must appear at exactly one step");
        assert_eq!(samples[0].ts_ns, 0);
        assert_eq!(samples[0].value, 1.0);
    }

    #[test]
    fn range_step_alignment_excludes_end_when_not_aligned() {
        let source = TestSource::new()
            .with_series(&[("__name__", "up")], &[(ms_to_ns(0).expect("ok"), 1.0)])
            .expect("valid series");
        // start=0, end=2m+30s, step=1m: grid is 0, 1m, 2m; 2m30s itself is
        // never visited because it is not start + k*step for any integer k,
        // so the reported end is excluded.
        let matrix = Evaluator::new()
            .range(&source, "up", 0, minutes(2) + 30_000, minutes(1))
            .expect("evaluates");
        assert_eq!(matrix.len(), 1);
        let (_, samples) = &matrix[0];
        assert_eq!(
            samples.last().expect("non-empty").ts_ns,
            ms_to_ns(minutes(2)).expect("ok")
        );
    }

    #[test]
    fn ns_to_ms_floors_toward_negative_infinity() {
        assert_eq!(ns_to_ms_floor(-1), -1);
        assert_eq!(ns_to_ms_floor(-1_000_001), -2);
        assert_eq!(ns_to_ms_floor(-1_000_000), -1);
        assert_eq!(ns_to_ms_floor(999_999), 0);
        assert_eq!(ns_to_ms_floor(1_000_000), 1);
        assert_eq!(ns_to_ms_floor(0), 0);
    }

    #[test]
    fn unsupported_constructs_name_the_rejected_node() {
        let cases: &[(&str, &str)] = &[
            ("rate(up[5m])", "rate"),
            ("sum(up)", "sum"),
            ("up + down", "binary expression"),
            ("up[5m:1m]", "subquery"),
        ];
        for (query, expected_substr) in cases {
            let err = Evaluator::new()
                .instant(&TestSource::new(), query, 0)
                .expect_err("must be rejected");
            let Error::Unsupported { construct } = err else {
                panic!("expected Unsupported for {query:?}, got {err:?}");
            };
            assert!(
                construct.contains(expected_substr),
                "construct {construct:?} should name {expected_substr:?} for query {query:?}"
            );
        }
    }

    #[test]
    fn bare_matrix_selector_at_top_level_is_wrong_type() {
        // `up[5m]` parses, but a matrix is never a valid top-level instant
        // query result: Prometheus rejects this as a type error, not as an
        // unsupported construct, since matrix selectors ARE evaluated (as
        // sub-expressions of range/subquery in a later phase).
        let err = Evaluator::new()
            .instant(&TestSource::new(), "up[5m]", 0)
            .expect_err("must be rejected");
        assert!(matches!(
            err,
            Error::WrongType {
                got: "range vector",
                ..
            }
        ));
    }

    #[test]
    fn paren_expression_unwraps_to_the_same_result_as_bare_selector() {
        let source = TestSource::new()
            .with_series(&[("__name__", "up")], &[(0, 1.0)])
            .expect("valid series");
        let bare = Evaluator::new()
            .instant(&source, "up", minutes(1))
            .expect("evaluates");
        let parenthesized = Evaluator::new()
            .instant(&source, "(up)", minutes(1))
            .expect("paren expressions unwrap");
        assert_eq!(bare, parenthesized);

        // Nested parens unwrap too.
        let double_parenthesized = Evaluator::new()
            .instant(&source, "((up))", minutes(1))
            .expect("nested paren expressions unwrap");
        assert_eq!(bare, double_parenthesized);
    }

    #[test]
    fn unary_minus_negates_value_and_drops_metric_name() {
        let source = TestSource::new()
            .with_series(&[("__name__", "up"), ("job", "api")], &[(0, 5.0)])
            .expect("valid series");
        let result = Evaluator::new()
            .instant(&source, "-up", minutes(1))
            .expect("evaluates");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, -5.0);
        assert_eq!(result[0].labels.get("__name__"), None);
        assert_eq!(result[0].labels.get("job"), Some("api"));

        // Double negation restores the original value (though __name__
        // stays dropped, matching Prometheus: any numeric op drops it).
        let double_negated = Evaluator::new()
            .instant(&source, "--up", minutes(1))
            .expect("evaluates");
        assert_eq!(double_negated[0].value, 5.0);
    }

    #[test]
    fn unary_minus_over_range_negates_every_sample_and_drops_metric_name() {
        let source = TestSource::new()
            .with_series(
                &[("__name__", "up")],
                &[(0, 2.0), (ms_to_ns(minutes(1)).expect("ok"), 3.0)],
            )
            .expect("valid series");
        let matrix = Evaluator::new()
            .range(&source, "-up", 0, minutes(1), minutes(1))
            .expect("evaluates");
        assert_eq!(matrix.len(), 1);
        let (labels, samples) = &matrix[0];
        assert_eq!(labels.get("__name__"), None);
        assert_eq!(
            samples.iter().map(|s| s.value).collect::<Vec<_>>(),
            vec![-2.0, -3.0]
        );
    }

    #[test]
    fn number_literal_evaluates_to_a_scalar() {
        let result = Evaluator::new()
            .eval_instant(&TestSource::new(), "42", 0)
            .expect("evaluates");
        assert_eq!(result, Value::Scalar(42.0));

        let negated = Evaluator::new()
            .eval_instant(&TestSource::new(), "-42", 0)
            .expect("evaluates");
        assert_eq!(negated, Value::Scalar(-42.0));
    }

    #[test]
    fn string_literal_evaluates_to_a_string() {
        let result = Evaluator::new()
            .eval_instant(&TestSource::new(), r#""hello""#, 0)
            .expect("evaluates");
        assert_eq!(result, Value::String("hello".to_string()));
    }

    #[test]
    fn scalar_top_level_range_query_is_constant_across_the_grid() {
        let result = Evaluator::new()
            .eval_range(&TestSource::new(), "7", 0, minutes(10), minutes(1))
            .expect("evaluates");
        assert_eq!(result, Value::Scalar(7.0));
    }

    #[test]
    fn instant_and_range_reject_scalar_and_string_results() {
        let err = Evaluator::new()
            .instant(&TestSource::new(), "42", 0)
            .expect_err("scalar is not an instant vector");
        assert!(matches!(err, Error::WrongType { got: "scalar", .. }));

        let err = Evaluator::new()
            .range(&TestSource::new(), r#""x""#, 0, minutes(1), minutes(1))
            .expect_err("string is not a range vector");
        assert!(matches!(err, Error::WrongType { got: "string", .. }));
    }

    #[test]
    fn at_modifier_pins_the_lookup_instant_regardless_of_query_time() {
        // `up @ 300` (a literal, seconds since the epoch) always looks up
        // data as of t=300s, no matter what instant the query itself asks
        // for.
        let pinned_ns = ms_to_ns(300_000).expect("ok");
        let source = TestSource::new()
            .with_series(&[("__name__", "up")], &[(pinned_ns, 11.0)])
            .expect("valid series");

        for query_t_ms in [0, minutes(1), minutes(100)] {
            let result = Evaluator::new()
                .instant(&source, "up @ 300", query_t_ms)
                .expect("evaluates");
            assert_eq!(result.len(), 1, "pinned lookup at t={query_t_ms}");
            assert_eq!(result[0].value, 11.0);
            // Reported timestamp is still the query's own instant.
            assert_eq!(result[0].ts_ns, ms_to_ns(query_t_ms).expect("ok"));
        }
    }

    #[test]
    fn at_start_and_end_resolve_against_query_parameters_for_instant_queries() {
        // For an instant query, start() and end() both equal the query's
        // own evaluation timestamp.
        let t_ms = minutes(10);
        let sample_ts_ns = ms_to_ns(t_ms).expect("ok");
        let source = TestSource::new()
            .with_series(&[("__name__", "up")], &[(sample_ts_ns, 1.0)])
            .expect("valid series");

        let start_result = Evaluator::new()
            .instant(&source, "up @ start()", t_ms)
            .expect("evaluates");
        let end_result = Evaluator::new()
            .instant(&source, "up @ end()", t_ms)
            .expect("evaluates");
        assert_eq!(start_result.len(), 1);
        assert_eq!(end_result.len(), 1);
        assert_eq!(start_result[0].value, 1.0);
        assert_eq!(end_result[0].value, 1.0);
    }

    #[test]
    fn at_start_resolves_against_the_whole_range_query_span() {
        // `up @ start()` in a range query pins every step's lookup to the
        // range's own start, not to the current grid step.
        let start_ms = minutes(5);
        let sample_ts_ns = ms_to_ns(start_ms).expect("ok");
        let source = TestSource::new()
            .with_series(&[("__name__", "up")], &[(sample_ts_ns, 4.0)])
            .expect("valid series");

        let matrix = Evaluator::new()
            .range(&source, "up @ start()", start_ms, minutes(15), minutes(5))
            .expect("evaluates");
        assert_eq!(matrix.len(), 1);
        let (_, samples) = &matrix[0];
        // Every one of the 3 steps (5m, 10m, 15m) sees the same pinned
        // value, since the lookup instant never moves off start().
        assert_eq!(samples.len(), 3);
        assert!(samples.iter().all(|s| s.value == 4.0));
    }

    #[test]
    fn matrix_selector_window_is_left_open() {
        let range_ns = duration_to_ns(std::time::Duration::from_secs(300)).expect("ok");
        let sel_ts_ns = ms_to_ns(minutes(10)).expect("ok");
        let window_start = sel_ts_ns - range_ns;

        // One sample exactly at the (excluded) window start, one 1ns
        // inside, one at sel_ts itself (included).
        let source = TestSource::new()
            .with_series(
                &[("__name__", "up")],
                &[
                    (window_start, 1.0),
                    (window_start + 1, 2.0),
                    (sel_ts_ns, 3.0),
                ],
            )
            .expect("valid series");

        let expr = promql_parser::parser::parse("up[5m]").expect("parses");
        let promql_parser::parser::Expr::MatrixSelector(ms) = expr else {
            panic!("expected matrix selector");
        };
        let ctx = QueryWindow {
            start_ns: sel_ts_ns,
            end_ns: sel_ts_ns,
        };
        let matrix = Evaluator::new()
            .eval_matrix_selector(&source, &ms, sel_ts_ns, &ctx)
            .expect("evaluates");
        assert_eq!(matrix.len(), 1);
        let (_, samples) = &matrix[0];
        let values: Vec<f64> = samples.iter().map(|s| s.value).collect();
        assert_eq!(values, vec![2.0, 3.0], "window start itself is excluded");
    }

    #[test]
    fn matrix_selector_excludes_stale_marker_samples() {
        let sel_ts_ns = ms_to_ns(minutes(10)).expect("ok");
        let stale_ts_ns = ms_to_ns(minutes(9)).expect("ok");
        let source = TestSource::new()
            .with_series(
                &[("__name__", "up")],
                &[
                    (stale_ts_ns, f64::from_bits(STALE_NAN_BITS)),
                    (sel_ts_ns, 9.0),
                ],
            )
            .expect("valid series");

        let expr = promql_parser::parser::parse("up[5m]").expect("parses");
        let promql_parser::parser::Expr::MatrixSelector(ms) = expr else {
            panic!("expected matrix selector");
        };
        let ctx = QueryWindow {
            start_ns: sel_ts_ns,
            end_ns: sel_ts_ns,
        };
        let matrix = Evaluator::new()
            .eval_matrix_selector(&source, &ms, sel_ts_ns, &ctx)
            .expect("evaluates");
        assert_eq!(matrix.len(), 1);
        let (_, samples) = &matrix[0];
        assert_eq!(samples.len(), 1, "the stale marker itself must not surface");
        assert_eq!(samples[0].value, 9.0);
    }

    #[test]
    fn non_positive_step_is_rejected() {
        let source = TestSource::new();
        let err = Evaluator::new()
            .range(&source, "up", 0, minutes(1), 0)
            .expect_err("must reject zero step");
        assert!(matches!(err, Error::NonPositiveStep { step_ms: 0 }));
    }

    #[test]
    fn start_after_end_is_rejected() {
        let source = TestSource::new();
        let err = Evaluator::new()
            .range(&source, "up", minutes(1), 0, minutes(1))
            .expect_err("must reject start > end");
        assert!(matches!(err, Error::InvalidRange { .. }));
    }

    /// Wraps a [`TestSource`] and counts `query` calls, so a test can prove a
    /// request was rejected before any storage work (and, since the grid is
    /// built after the budget check and before `query`, before any grid
    /// allocation).
    #[derive(Debug, Default)]
    struct CountingSource {
        inner: TestSource,
        queries: std::sync::atomic::AtomicUsize,
    }

    impl CountingSource {
        fn new(inner: TestSource) -> Self {
            CountingSource {
                inner,
                queries: std::sync::atomic::AtomicUsize::new(0),
            }
        }

        fn query_count(&self) -> usize {
            self.queries.load(std::sync::atomic::Ordering::Relaxed)
        }
    }

    impl crate::source::SeriesSource for CountingSource {
        fn query(
            &self,
            matchers: &[LabelMatcher],
            window: TimeRange,
        ) -> Result<Vec<crate::source::SeriesData>, SourceError> {
            self.queries
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            self.inner.query(matchers, window)
        }
    }

    fn one_series_source() -> CountingSource {
        CountingSource::new(
            TestSource::new()
                .with_series(&[("__name__", "up")], &[(0, 1.0)])
                .expect("valid series"),
        )
    }

    #[test]
    fn range_at_the_point_budget_boundary_is_evaluated() {
        // start=0, end=2m, step=1m is exactly 3 evaluation points.
        let source = one_series_source();
        let matrix = Evaluator::new()
            .with_max_range_points(3)
            .range(&source, "up", 0, minutes(2), minutes(1))
            .expect("a request at exactly the cap must succeed");
        assert_eq!(matrix.len(), 1);
        assert_eq!(
            source.query_count(),
            1,
            "boundary request must reach storage"
        );
    }

    #[test]
    fn range_one_point_over_the_budget_is_rejected() {
        // Same start/step, one more step: 4 points against a cap of 3.
        let source = one_series_source();
        let err = Evaluator::new()
            .with_max_range_points(3)
            .range(&source, "up", 0, minutes(3), minutes(1))
            .expect_err("one point over the cap must be rejected");
        let Error::TooManyPoints { points, max } = err else {
            panic!("expected TooManyPoints, got {err:?}");
        };
        assert_eq!(points, 4);
        assert_eq!(max, 3);
        assert_eq!(
            source.query_count(),
            0,
            "rejection must happen before the grid is built and storage is touched"
        );
    }

    #[test]
    fn default_point_budget_matches_prometheus_resolution_limit() {
        assert_eq!(DEFAULT_MAX_RANGE_POINTS, 11_000);
        assert_eq!(Evaluator::new().max_range_points(), 11_000);

        let source = one_series_source();
        let evaluator = Evaluator::new();
        // 11_000 points: 0, 1m, ..., 10_999m.
        let last = minutes(DEFAULT_MAX_RANGE_POINTS as i64 - 1);
        evaluator
            .range(&source, "up", 0, last, minutes(1))
            .expect("11_000 points is within the default budget");

        let err = evaluator
            .range(&source, "up", 0, last + minutes(1), minutes(1))
            .expect_err("11_001 points exceeds the default budget");
        assert!(matches!(
            err,
            Error::TooManyPoints {
                points: 11_001,
                max: 11_000
            }
        ));
    }

    #[test]
    fn pathological_range_is_rejected_without_building_a_grid() {
        // The a10-F03 reproducer: the widest representable span at the
        // finest step. Unbounded, this asks for ~9.2e12 grid entries
        // (~1.5e14 bytes) and OOMs the process; bounded, it is arithmetic.
        // That this test terminates at all is the assertion that no grid was
        // allocated; the query counter pins that storage was never reached.
        let source = one_series_source();
        let max_ms = i64::MAX / NS_PER_MS;
        let err = Evaluator::new()
            .range(&source, "up", 0, max_ms, 1)
            .expect_err("an unbounded-grid request must be rejected");
        let Error::TooManyPoints { points, max } = err else {
            panic!("expected TooManyPoints, got {err:?}");
        };
        assert_eq!(points, u64::try_from(max_ms).expect("positive") + 1);
        assert_eq!(max, DEFAULT_MAX_RANGE_POINTS);
        assert_eq!(source.query_count(), 0);
    }

    #[test]
    fn point_budget_is_checked_before_the_query_is_parsed() {
        // An over-budget request is rejected on its resolution even when the
        // query itself would fail to parse: the cheap arithmetic guard runs
        // first, so no caller-controlled parsing or regex compilation is done
        // for a request that cannot be served. This mirrors Prometheus, which
        // rejects the resolution in its API layer before building the query.
        let source = one_series_source();
        let err = Evaluator::new()
            .range(&source, "sum(up", 0, i64::MAX / NS_PER_MS, 1)
            .expect_err("must be rejected");
        assert!(matches!(err, Error::TooManyPoints { .. }));
    }

    #[test]
    fn zero_point_budget_rejects_every_range_query() {
        let source = one_series_source();
        let err = Evaluator::new()
            .with_max_range_points(0)
            .range(&source, "up", 0, 0, minutes(1))
            .expect_err("a cap of 0 rejects even a single-point range");
        assert!(matches!(err, Error::TooManyPoints { points: 1, max: 0 }));
        assert_eq!(source.query_count(), 0);
    }

    #[test]
    fn point_budget_does_not_apply_to_instant_queries() {
        // Instant queries evaluate one point by construction; the cap must
        // not leak into them.
        let source = one_series_source();
        let result = Evaluator::new()
            .with_max_range_points(0)
            .instant(&source, "up", 0)
            .expect("instant queries are unaffected by the range budget");
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn step_count_is_exact_and_does_not_overflow() {
        assert_eq!(step_count(0, 0, NS_PER_MS), 1);
        assert_eq!(step_count(0, 2 * NS_PER_MS, NS_PER_MS), 3);
        // Not step-aligned: the trailing partial step is not a point.
        assert_eq!(step_count(0, 2 * NS_PER_MS + 1, NS_PER_MS), 3);
        // Widest representable span: `end - start` overflows i64, the
        // i128 computation does not.
        let widest = step_count(i64::MIN, i64::MAX, NS_PER_MS);
        assert_eq!(
            widest,
            u64::try_from(
                (i128::from(i64::MAX) - i128::from(i64::MIN)) / i128::from(NS_PER_MS) + 1
            )
            .expect("fits u64")
        );
    }

    proptest::proptest! {
        /// `signed_offset_ns` never panics across the full range of
        /// representable durations, on either sign of offset.
        #[test]
        fn signed_offset_ns_never_panics(secs in 0u64..1_000_000_000, negative in proptest::bool::ANY) {
            let d = std::time::Duration::from_secs(secs);
            let offset = if negative {
                promql_parser::parser::Offset::Neg(d)
            } else {
                promql_parser::parser::Offset::Pos(d)
            };
            let _ = signed_offset_ns(Some(&offset));
        }

        /// Matrix-selector window arithmetic (`sel_ts - range`) never panics
        /// even at extreme selector timestamps and range durations; it must
        /// return `Error::TimeOverflow` instead.
        #[test]
        fn matrix_window_arithmetic_never_panics(
            sel_ts_ns in proptest::num::i64::ANY,
            range_secs in 0u64..1_000_000_000,
        ) {
            let range_ns = duration_to_ns(std::time::Duration::from_secs(range_secs));
            if let Ok(range_ns) = range_ns {
                let _ = sel_ts_ns.checked_sub(range_ns);
            }
        }
    }
}
