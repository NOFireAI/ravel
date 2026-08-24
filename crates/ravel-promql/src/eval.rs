//! PromQL evaluator core (ADR-0007, ADR-0021). A typed recursive-descent
//! interpreter over the promql-parser AST: [`Value`] is the internal result
//! type (scalar, string, instant vector, range matrix), and [`Evaluator`]
//! walks paren expressions, unary minus, number/string literals, vector and
//! matrix selectors (with `offset` and the `@` modifier, including
//! `start()`/`end()`), function calls (`crate::functions`), binary
//! expressions (`crate::binop`), aggregations (`crate::aggregate`), and
//! subqueries (`expr[range:step]`, recursively evaluated at epoch-aligned
//! steps). Every other AST node is rejected with
//! [`Error::Unsupported`], naming the construct.
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

use std::cell::{Cell, RefCell};
use std::time::Instant;

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
///
/// `orig_sample_ts_ns` is the timestamp the underlying sample actually
/// carries in storage. It equals `ts_ns` everywhere except a direct vector
/// selector read (`Evaluator::eval_vector_selector`), where it is the
/// picked sample's own (possibly offset/`@`-shifted-lookback, possibly
/// older-than-`ts_ns`) timestamp. Every function that builds its own output
/// samples resets it back to `ts_ns` (`timestamp()` is the only function
/// that reads it), matching Prometheus, which only special-cases
/// `timestamp()` applied directly to a bare vector selector and otherwise
/// always reports the evaluation time.
#[derive(Debug, Clone, PartialEq)]
pub struct InstantSample {
    pub labels: LabelSet,
    pub ts_ns: i64,
    pub orig_sample_ts_ns: i64,
    pub value: f64,
    /// The native histogram this element carries, when the underlying series
    /// is a native (exponential) histogram. `None` for an ordinary
    /// float sample, which is every element produced for float-only queries.
    /// When `Some`, `value` is not meaningful and is left `0.0`; histogram-
    /// aware functions (`histogram_count`/`_sum`/`_avg`, native
    /// `histogram_quantile`/`_fraction`, `rate`/`sum`/`avg` over histograms)
    /// read this field instead. A float function applied to a histogram
    /// element ignores it, matching Prometheus dropping histogram samples from
    /// float-only operations.
    pub histogram: Option<crate::histogram::FloatHistogram>,
}

impl InstantSample {
    /// A plain float element (`histogram: None`): the constructor every
    /// float-only code path uses so adding the histogram field stayed a
    /// single-line change at each call site.
    pub(crate) fn scalar(labels: LabelSet, ts_ns: i64, orig_sample_ts_ns: i64, value: f64) -> Self {
        InstantSample {
            labels,
            ts_ns,
            orig_sample_ts_ns,
            value,
            histogram: None,
        }
    }

    /// A native-histogram element: carries the histogram value with a
    /// placeholder `value` of `0.0`.
    pub(crate) fn histogram(
        labels: LabelSet,
        ts_ns: i64,
        orig_sample_ts_ns: i64,
        histogram: crate::histogram::FloatHistogram,
    ) -> Self {
        InstantSample {
            labels,
            ts_ns,
            orig_sample_ts_ns,
            value: 0.0,
            histogram: Some(histogram),
        }
    }
}

/// Result of an instant query: one entry per matched series.
pub type InstantVector = Vec<InstantSample>;

/// Result of a range query: one entry per matched series, with one sample
/// per evaluated step at which that series had a value in the lookback
/// window. Series with no value at any step are omitted entirely.
pub type RangeMatrix = Vec<(LabelSet, Vec<Sample>)>;

/// The native-histogram counterpart of [`RangeMatrix`] for one instant: one
/// entry per matched histogram series, each carrying that series' in-window
/// histogram samples. Only used internally by the histogram
/// `rate`/`increase`/`delta` path; native histograms have no range-query
/// result rendering yet.
pub(crate) type HistogramMatrix = Vec<(LabelSet, Vec<crate::histogram::TimedHistogram>)>;

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

/// Non-fatal diagnostics an otherwise-successful query evaluation
/// accumulates, mirroring the two separate fields real Prometheus attaches
/// to a query response envelope (`promql/parser` `Annotations`, rendered as
/// the top-level `warnings` and `infos` arrays).
///
/// `warnings` and `infos` are kept **distinct**, exactly as Prometheus keeps
/// them: a warning flags a result that is very likely not what the user
/// wanted (a quantile argument outside `[0, 1]` clamped to an infinity, a
/// classic histogram without enough well-formed buckets), while an info
/// flags a result that is probably fine but worth knowing about (a classic
/// histogram whose buckets had to be nudged back into monotonic order, a
/// `rate()` over a metric whose name suggests it is not a counter). Collapsing
/// the two would lose that severity distinction, so they never share a
/// channel here either.
///
/// Each channel de-duplicates by exact message text: an annotation raised at
/// every step of a range query, or once per matched series, is reported once.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Annotations {
    warnings: Vec<String>,
    infos: Vec<String>,
}

impl Annotations {
    /// The accumulated warning messages, in first-seen order.
    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }

    /// The accumulated info messages, in first-seen order.
    pub fn infos(&self) -> &[String] {
        &self.infos
    }

    /// True when neither channel carries anything.
    pub fn is_empty(&self) -> bool {
        self.warnings.is_empty() && self.infos.is_empty()
    }

    /// Consume into the two owned channel vectors, for a caller wiring them
    /// into a response envelope.
    pub fn into_parts(self) -> (Vec<String>, Vec<String>) {
        (self.warnings, self.infos)
    }

    /// Record a warning, ignoring an exact-text duplicate already present.
    pub(crate) fn add_warning(&mut self, message: impl Into<String>) {
        let message = message.into();
        if !self.warnings.contains(&message) {
            self.warnings.push(message);
        }
    }

    /// Record an info, ignoring an exact-text duplicate already present.
    pub(crate) fn add_info(&mut self, message: impl Into<String>) {
        let message = message.into();
        if !self.infos.contains(&message) {
            self.infos.push(message);
        }
    }
}

/// Prometheus' `InvalidQuantileWarning` message: a quantile argument (`phi`
/// for `histogram_quantile`, `q` for the `quantile` aggregator and
/// `quantile_over_time`) fell outside `[0, 1]` and was clamped to an
/// infinity. A warning, not an info: the clamped result is almost never what
/// the caller intended.
pub(crate) fn invalid_quantile_warning(q: f64) -> String {
    format!("quantile value should be between 0 and 1, got {q}")
}

/// Prometheus' bad-bucket warning for a classic histogram whose bucket set
/// is degenerate: fewer than two usable buckets, so no quantile/fraction can
/// be computed and the group evaluates to `NaN`. A warning: the `NaN` result
/// signals a malformed input, not a benign one. Note this fires only for the
/// too-few-buckets case; a group that is merely missing its `+Inf` top bucket
/// is NaN with no annotation, matching the pinned Prometheus binary.
pub(crate) fn classic_histogram_bad_buckets_warning() -> String {
    "input to histogram function was not a valid classic histogram: it needs \
     at least two buckets, the highest being a +Inf bucket"
        .to_string()
}

/// Prometheus' `HistogramQuantileForcedMonotonicityInfo` message: a classic
/// histogram's cumulative bucket counts were not monotonic and had to be
/// clamped to a running maximum before interpolation. An info, not a
/// warning: the fixed-up result is usually close to correct, the source data
/// was merely slightly inconsistent.
pub(crate) fn forced_monotonicity_info() -> String {
    "input to histogram function needed to be fixed for monotonicity of \
     bucket counts (see https://prometheus.io/docs/practices/histograms/)"
        .to_string()
}

/// Prometheus' `PossibleNonCounterInfo` message: `rate()`/`increase()` are
/// meant for counters, and Prometheus' counter-naming convention names one
/// with a `_total`, `_sum`, `_count`, or `_bucket` suffix. A name with none
/// of those probably was not meant to be used this way. An info, not a
/// warning: the computed value is unaffected, this only flags a likely
/// naming/usage mismatch.
pub(crate) fn possible_non_counter_info(metric_name: &str) -> String {
    format!(
        "metric might not be a counter, name does not end in _total/_sum/_count/_bucket: \
         {metric_name:?}"
    )
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
    #[error(
        "nested subquery evaluation touched {touched} evaluation points across every nesting level, exceeding the shared budget of {max}; narrow the range, widen the step, or reduce subquery nesting"
    )]
    EvalBudgetExhausted { touched: u64, max: u64 },
    #[error("evaluation exceeded its deadline before finishing")]
    DeadlineExceeded,
    #[error("query evaluates to {got}, but this operation requires {expected}")]
    WrongType {
        expected: &'static str,
        got: &'static str,
    },
    #[error("invalid destination label name {label:?}")]
    InvalidLabelName { label: String },
    #[error("invalid regular expression {pattern:?}: {reason}")]
    InvalidRegex { pattern: String, reason: String },
    #[error("multiple matches for labels: {detail}")]
    AmbiguousMatch { detail: String },
    #[error(transparent)]
    TooComplex(#[from] crate::complexity_guard::QueryTooComplex),
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

/// Default shared budget on total evaluation-grid points touched across
/// *every* subquery nesting level of one query. Unlike
/// [`DEFAULT_MAX_RANGE_POINTS`], which caps a single subquery/range node's
/// own grid independently, this one counter accumulates across the whole
/// recursive evaluation tree ([`QueryWindow::charge_budget`], charged by
/// [`Evaluator::eval_subquery_matrix`] on every call): a subquery nested
/// inside another subquery, or re-evaluated once per enclosing grid step,
/// cannot multiply its cost past any single node's own cap. 1,000,000
/// comfortably covers a legitimate multi-thousand-step range query wrapping
/// a modest subquery, while still stopping a multi-level nested blowup
/// within a handful of nesting levels.
pub const DEFAULT_MAX_TOTAL_EVAL_POINTS: u64 = 1_000_000;

/// Default step for a subquery that does not specify its own (`expr[5m:]`),
/// in nanoseconds: 1 minute, matching Prometheus' global `evaluation_interval`
/// default (`NoStepSubqueryIntervalFn`). `ravel-query`'s engine config carries
/// its own `default_evaluation_interval` knob and applies it via
/// [`Evaluator::with_default_step`]; a bare `Evaluator::new()` (e.g. in a
/// test, or a caller with no engine-level config) gets this default.
pub const DEFAULT_SUBQUERY_STEP_NS: i64 = 60 * 1_000_000_000;

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
pub struct QueryWindow {
    start_ns: i64,
    end_ns: i64,
    /// Shared cross-level evaluation-grid-point budget, charged
    /// down by [`Self::charge_budget`] on every subquery-grid evaluation
    /// ([`Evaluator::eval_subquery_matrix`]) regardless of nesting depth or
    /// how many times an enclosing grid re-evaluates it from scratch.
    budget: Cell<u64>,
    /// Wall-clock instant after which evaluation must stop with
    /// [`Error::DeadlineExceeded`], checked by [`Self::check_deadline`] once
    /// per subquery grid step: the evaluator is otherwise fully
    /// synchronous with no yield point for `ravel-query`'s
    /// `tokio::time::timeout` to preempt at, so a runaway nested evaluation
    /// would otherwise always run to completion before the timeout could
    /// fire. `None` (the default, [`Evaluator::new`]) never cancels.
    deadline: Option<Instant>,
    /// Non-fatal diagnostics accumulated across the whole evaluation
    /// (`quantile` out-of-range clamps, classic-histogram bucket problems,
    /// forced monotonicity). Threaded through every `eval_*` call via this
    /// shared context rather than woven into each `Value`, so no per-node
    /// return type changes; [`Evaluator::eval_instant_annotated`]/
    /// [`Evaluator::eval_range_annotated`] hand it back to the caller once
    /// evaluation finishes. `RefCell` because evaluation borrows `ctx`
    /// immutably everywhere and this is the one interior-mutable sink, like
    /// `budget` above.
    annotations: RefCell<Annotations>,
}

impl QueryWindow {
    /// A bare context for callers outside this crate that invoke a
    /// window-resolution helper such as [`crate::functions::range_window`]
    /// directly: the query's `[start_ns, end_ns]` parameters (consulted only to
    /// resolve `@ start()`/`@ end()`), an effectively unbounded evaluation
    /// budget, no deadline, and an empty annotation sink. Exposed so a
    /// distributed-pushdown coordinator can assert its independently-derived
    /// reduction bounds against the evaluator's own `range_window`.
    pub fn bare(start_ns: i64, end_ns: i64) -> Self {
        QueryWindow {
            start_ns,
            end_ns,
            budget: Cell::new(u64::MAX),
            deadline: None,
            annotations: RefCell::new(Annotations::default()),
        }
    }

    /// Test-only bare context: instant window at time zero, effectively
    /// unbounded budget, no deadline, empty annotation sink. For unit tests
    /// that call an internal `eval_*`/annotation helper directly instead of
    /// going through [`Evaluator::eval_instant`]/[`Evaluator::eval_range`].
    #[cfg(test)]
    pub(crate) fn for_test() -> Self {
        QueryWindow {
            start_ns: 0,
            end_ns: 0,
            budget: Cell::new(u64::MAX),
            deadline: None,
            annotations: RefCell::new(Annotations::default()),
        }
    }

    /// Record an evaluation warning (see [`Annotations`]).
    pub(crate) fn warn(&self, message: impl Into<String>) {
        self.annotations.borrow_mut().add_warning(message);
    }

    /// Record an evaluation info (see [`Annotations`]).
    pub(crate) fn info(&self, message: impl Into<String>) {
        self.annotations.borrow_mut().add_info(message);
    }

    /// Charge `points` (one subquery-grid evaluation's own size) against the
    /// shared cross-level budget. `max` is the configured ceiling
    /// ([`Evaluator::max_total_eval_points`]), reported back in the error so
    /// callers can name it; `touched` is the running total that would have
    /// been reached had this charge been allowed, so it always exceeds `max`
    /// when this returns an error.
    fn charge_budget(&self, points: u64, max: u64) -> Result<(), Error> {
        let remaining = self.budget.get();
        if points > remaining {
            let already_touched = max.saturating_sub(remaining);
            return Err(Error::EvalBudgetExhausted {
                touched: already_touched.saturating_add(points),
                max,
            });
        }
        self.budget.set(remaining - points);
        Ok(())
    }

    /// `Err(Error::DeadlineExceeded)` once [`Self::deadline`] has passed;
    /// `Ok(())` otherwise, including when no deadline was set.
    pub(crate) fn check_deadline(&self) -> Result<(), Error> {
        match self.deadline {
            Some(deadline) if Instant::now() >= deadline => Err(Error::DeadlineExceeded),
            _ => Ok(()),
        }
    }
}

/// PromQL evaluator. Stateless besides its lookback and resolution
/// configuration; safe to share across queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Evaluator {
    lookback_delta_ns: i64,
    max_range_points: usize,
    default_step_ns: i64,
    max_total_eval_points: u64,
    deadline: Option<Instant>,
}

impl Default for Evaluator {
    fn default() -> Self {
        Evaluator {
            lookback_delta_ns: DEFAULT_LOOKBACK_NS,
            max_range_points: DEFAULT_MAX_RANGE_POINTS,
            default_step_ns: DEFAULT_SUBQUERY_STEP_NS,
            max_total_eval_points: DEFAULT_MAX_TOTAL_EVAL_POINTS,
            deadline: None,
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

    /// Override the default step (1 minute, [`DEFAULT_SUBQUERY_STEP_NS`]) a
    /// subquery uses when it does not specify its own (`expr[5m:]`).
    pub fn with_default_step(mut self, step: std::time::Duration) -> Result<Self, Error> {
        self.default_step_ns = i64::try_from(step.as_nanos()).map_err(|_| Error::TimeOverflow)?;
        Ok(self)
    }

    /// The configured default subquery step, in nanoseconds.
    pub fn default_step_ns(&self) -> i64 {
        self.default_step_ns
    }

    /// Override the default shared cross-level evaluation budget
    /// ([`DEFAULT_MAX_TOTAL_EVAL_POINTS`]): the total number of evaluation-
    /// grid points a query may touch across every subquery nesting level,
    /// as opposed to [`Self::with_max_range_points`]'s per-node cap.
    pub fn with_max_total_eval_points(mut self, max_total_eval_points: u64) -> Self {
        self.max_total_eval_points = max_total_eval_points;
        self
    }

    /// The configured shared cross-level evaluation budget, in grid points.
    pub fn max_total_eval_points(&self) -> u64 {
        self.max_total_eval_points
    }

    /// Set a wall-clock deadline: once past, evaluation stops with
    /// [`Error::DeadlineExceeded`] the next time a subquery grid step checks
    /// it, rather than running to completion regardless of an
    /// enclosing caller's own timeout. `ravel-query`'s `QueryEngine` derives
    /// this from its own `deadline: Duration` parameter
    /// (`Instant::now() + deadline`) before evaluating. Unset by default
    /// ([`Evaluator::new`]), so an `Evaluator` built without this call never
    /// self-cancels.
    pub fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = Some(deadline);
        self
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
        Ok(self.eval_instant_annotated(source, query, t_ms)?.0)
    }

    /// Like [`Self::eval_instant`], but also returns the [`Annotations`]
    /// (warnings and infos) evaluation accumulated. `eval_instant` is the
    /// thin wrapper that discards them; a caller rendering a Prometheus
    /// response envelope (`ravel-query`) uses this to surface both the
    /// separate `warnings` and `infos` fields real Prometheus emits.
    pub fn eval_instant_annotated(
        &self,
        source: &dyn SeriesSource,
        query: &str,
        t_ms: i64,
    ) -> Result<(Value, Annotations), Error> {
        crate::complexity_guard::check(query)?;
        let expr = promql_parser::parser::parse(query).map_err(Error::Parse)?;
        let t_ns = ms_to_ns(t_ms)?;
        let ctx = QueryWindow {
            start_ns: t_ns,
            end_ns: t_ns,
            budget: Cell::new(self.max_total_eval_points),
            deadline: self.deadline,
            annotations: RefCell::new(Annotations::default()),
        };
        let value = self.eval_expr(source, &expr, t_ns, &ctx)?;
        Ok((value, ctx.annotations.into_inner()))
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
        Ok(self
            .eval_range_annotated(source, query, start_ms, end_ms, step_ms)?
            .0)
    }

    /// Like [`Self::eval_range`], but also returns the [`Annotations`]
    /// (warnings and infos) evaluation accumulated over the whole grid
    /// (de-duplicated by message text, so a per-step annotation is reported
    /// once). `eval_range` is the thin wrapper that discards them.
    pub fn eval_range_annotated(
        &self,
        source: &dyn SeriesSource,
        query: &str,
        start_ms: i64,
        end_ms: i64,
        step_ms: i64,
    ) -> Result<(Value, Annotations), Error> {
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

        crate::complexity_guard::check(query)?;
        let expr = promql_parser::parser::parse(query).map_err(Error::Parse)?;
        let ctx = QueryWindow {
            start_ns,
            end_ns,
            budget: Cell::new(self.max_total_eval_points),
            deadline: self.deadline,
            annotations: RefCell::new(Annotations::default()),
        };

        // The supported grammar (paren, unary minus, literals, one
        // selector) can contain at most one selector, so unlike a general
        // multi-selector tree there is no risk of re-querying storage once
        // per node: the grid below still issues exactly one `source.query`
        // call for the whole range, same as before this phase. A future
        // phase that adds binary/call/aggregate expressions (and therefore
        // multiple selectors, or the same selector's value needed at
        // multiple sub-evaluations) is the point at which per-selector
        // cursoring (ADR-0021 §1) becomes necessary; it is not needed yet.
        let (core, negate) = resolve_range_core(&expr)?;
        let value = match core {
            RangeCore::Scalar(v) => Value::Scalar(if negate { -v } else { v }),
            RangeCore::Str(s) => Value::String(s.to_string()),
            RangeCore::Selector(vs) => {
                let matrix = self.eval_range_selector(
                    source, vs, start_ns, end_ns, step_ns, points, &ctx, negate,
                )?;
                Value::Matrix(matrix)
            }
            RangeCore::Call(call) => {
                let matrix = crate::functions::eval_range_call(
                    self, source, call, start_ns, end_ns, step_ns, points, &ctx,
                )?;
                let matrix = if negate {
                    negate_matrix(matrix)
                } else {
                    matrix
                };
                Value::Matrix(matrix)
            }
            RangeCore::Generic(e) => {
                let matrix =
                    crate::functions::eval_instant_over_grid(start_ns, end_ns, step_ns, |t| {
                        ctx.check_deadline()?;
                        self.eval_expr(source, e, t, &ctx)
                    })?;
                Value::Matrix(matrix)
            }
        };
        Ok((value, ctx.annotations.into_inner()))
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

    /// General recursive evaluator for a single instant. Handles the
    /// grammar (paren, unary minus, literals, vector/matrix selectors),
    /// registered function calls, and rejects everything else, naming the
    /// construct.
    pub(crate) fn eval_expr(
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
            Expr::Call(c) => crate::functions::eval_call(self, source, c, eval_ts_ns, ctx),
            Expr::Binary(b) => crate::binop::eval_binary(self, source, b, eval_ts_ns, ctx),
            Expr::Aggregate(a) => {
                crate::aggregate::eval_aggregate(self, source, a, eval_ts_ns, ctx)
            }
            Expr::Subquery(sq) => self
                .eval_subquery_matrix(source, sq, eval_ts_ns, ctx)
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
            if let Some((value, orig_sample_ts_ns)) =
                pick_sample(&s.samples, sel_ts_ns, self.lookback_delta_ns)
            {
                out.push(InstantSample::scalar(
                    s.labels,
                    eval_ts_ns,
                    orig_sample_ts_ns,
                    value,
                ));
            }
        }

        // Native-histogram series matching the same selector. A series
        // is either float or histogram in storage, so these never collide with
        // the float results above; the lookback pick is the same left-open
        // `(sel_ts - lookback, sel_ts]` rule.
        let hist_series = source.query_histograms(&selector_matchers, window)?;
        for s in hist_series {
            if let Some((value, orig_sample_ts_ns)) =
                pick_histogram(&s.samples, sel_ts_ns, self.lookback_delta_ns)
            {
                out.push(InstantSample::histogram(
                    s.labels,
                    eval_ts_ns,
                    orig_sample_ts_ns,
                    value,
                ));
            }
        }
        Ok(out)
    }

    /// Evaluate a matrix selector at one instant: every non-stale sample in
    /// the left-open window `(sel_ts - range, sel_ts]`, per matched series.
    /// Not reachable from a bare top-level expression (a bare matrix
    /// selector is always a top-level [`Error::WrongType`], matching
    /// Prometheus); reached through a registered function's matrix-typed
    /// argument instead (`crate::functions`).
    pub(crate) fn eval_matrix_selector(
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

    /// Evaluate a subquery (`expr[range:step]`) at one instant: the inner
    /// `expr` re-evaluated, fully recursively, at every epoch-aligned step in
    /// the subquery's own window. Not reachable from a
    /// bare top-level expression (a bare subquery is always a top-level
    /// [`Error::WrongType`], matching Prometheus, same as a bare matrix
    /// selector); reached through `eval_expr`'s own `Expr::Subquery` arm
    /// (producing a top-level [`Value::Matrix`], invalid as a final result
    /// but exercised the same way a bare matrix selector is) or through a
    /// registered function's matrix-typed argument (`crate::functions`).
    ///
    /// The grid's end is the subquery's own `offset`/`@`-shifted instant
    /// (relative to `eval_ts_ns`, the ambient step); its start is the
    /// smallest epoch-aligned multiple of the step that is strictly
    /// greater than `end - range` ([`align_up_to_step`]), left-open like
    /// this crate's matrix-selector window. The step count is checked
    /// against [`Self::max_range_points`] *before* any grid is built or the
    /// inner `expr` is evaluated even once, exactly like a top-level range
    /// query's own budget check: a subquery nested inside a range query or
    /// another subquery re-derives this same check at every enclosing grid
    /// step, so an inner grid that is itself over budget is rejected on the
    /// very first attempt to build it rather than after the outer grid has
    /// multiplied it out (no cross-step caching beyond cursors, so this
    /// check, like every other
    /// part of subquery evaluation, is deliberately redone from scratch at
    /// each enclosing step rather than memoized).
    pub(crate) fn eval_subquery_matrix(
        &self,
        source: &dyn SeriesSource,
        sq: &promql_parser::parser::SubqueryExpr,
        eval_ts_ns: i64,
        ctx: &QueryWindow,
    ) -> Result<RangeMatrix, Error> {
        let end_ns = resolve_eval_ts(sq.offset.as_ref(), sq.at.as_ref(), eval_ts_ns, ctx)?;
        let range_ns = duration_to_ns(sq.range)?;
        let step_ns = match sq.step {
            Some(d) => duration_to_ns(d)?,
            None => self.default_step_ns,
        };
        if step_ns <= 0 {
            return Err(Error::NonPositiveStep {
                step_ms: ns_to_ms_floor(step_ns),
            });
        }

        let target_ns = end_ns.checked_sub(range_ns).ok_or(Error::TimeOverflow)?;
        let start_ns = align_up_to_step(target_ns, step_ns)?;
        if start_ns > end_ns {
            return Ok(Vec::new());
        }

        let points = step_count(start_ns, end_ns, step_ns);
        if points > u64::try_from(self.max_range_points).unwrap_or(u64::MAX) {
            return Err(Error::TooManyPoints {
                points,
                max: self.max_range_points,
            });
        }
        ctx.charge_budget(points, self.max_total_eval_points)?;

        crate::functions::eval_instant_over_grid(start_ns, end_ns, step_ns, |t| {
            ctx.check_deadline()?;
            let value = self.eval_expr(source, &sq.expr, t, ctx)?;
            // Native-histogram series inside a subquery window are not yet
            // supported. The subquery grid reducer (`eval_instant_over_grid`)
            // keeps only the float value of each instant sample, so any
            // histogram element the inner expression produces would be
            // silently dropped and the subquery would return a wrong (empty)
            // answer for that series. Detect the actual presence of matched
            // histogram data and reject it as a typed `Error::Unsupported`
            // (HTTP 422) instead. The trigger is real histogram data in the
            // fetched window, not the subquery's syntactic shape: a float-only
            // subquery (including `rate(x[5m:1m])` over float series) sees no
            // histogram element here and keeps working exactly as before.
            // Full histogram subquery support is not yet implemented.
            if let Value::Vector(ref v) = value
                && v.iter().any(|s| s.histogram.is_some())
            {
                return Err(Error::Unsupported {
                    construct: "subquery over native histograms".to_string(),
                });
            }
            Ok(value)
        })
    }

    /// The native-histogram counterpart of [`Self::eval_matrix_selector`] at
    /// one instant: every native histogram in the left-open window
    /// `(sel_ts - range, sel_ts]`, per matched histogram series, with
    /// `__name__` dropped (every function result drops it). Used by the
    /// histogram `rate`/`increase`/`delta` path; the float and histogram
    /// matrix selectors are queried independently, so a series contributes to
    /// exactly one of them.
    pub(crate) fn eval_histogram_matrix_selector(
        &self,
        source: &dyn SeriesSource,
        ms: &promql_parser::parser::MatrixSelector,
        eval_ts_ns: i64,
        ctx: &QueryWindow,
    ) -> Result<HistogramMatrix, Error> {
        let selector_matchers = build_matchers(&ms.vs)?;
        let sel_ts_ns = selector_eval_ts(&ms.vs, eval_ts_ns, ctx)?;
        let range_ns = duration_to_ns(ms.range)?;
        let window_start = sel_ts_ns.checked_sub(range_ns).ok_or(Error::TimeOverflow)?;
        let window = TimeRange {
            start_ns: window_start,
            end_ns: sel_ts_ns,
        };

        let series = source.query_histograms(&selector_matchers, window)?;
        let mut out = Vec::with_capacity(series.len());
        for s in series {
            let samples: Vec<crate::histogram::TimedHistogram> = s
                .samples
                .into_iter()
                .filter(|sample| sample.ts_ns > window_start)
                .map(|sample| (sample.ts_ns, sample.value))
                .collect();
            if !samples.is_empty() {
                out.push((drop_metric_name(s.labels), samples));
            }
        }
        Ok(out)
    }

    /// Build the range matrix for a single top-level vector selector
    /// (`resolve_range_core`'s `Selector` case), one `source.query` call for
    /// the whole grid, generalized to
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
                if let Some((value, _orig_sample_ts_ns)) =
                    pick_sample(&s.samples, *sel_ts, self.lookback_delta_ns)
                {
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

    /// Build the range matrix for a top-level function call over a matrix
    /// selector argument (`crate::functions::eval_range_call`'s shared
    /// helper): one `source.query` call sized to the whole grid's combined
    /// window, then `reduce` is applied to each step's own window slice.
    ///
    /// A matrix selector's per-step window bounds (`window_start`, `sel_ts`)
    /// are both monotonically non-decreasing as the grid's reported
    /// timestamp increases, whether or not `offset`/`@` is present (`@`
    /// pins every step to the same instant, so its bounds are constant,
    /// which is still non-decreasing). So for each series a single forward-
    /// only `(lo, hi)` index cursor spans every step, giving O(samples +
    /// steps) per series instead of re-scanning the
    /// series' samples from scratch per step.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn eval_range_matrix_reduction(
        &self,
        source: &dyn SeriesSource,
        ms: &promql_parser::parser::MatrixSelector,
        start_ns: i64,
        end_ns: i64,
        step_ns: i64,
        points: u64,
        ctx: &QueryWindow,
        keep_metric_name: bool,
        reduce: impl Fn(&[Sample], i64, i64, i64, i64) -> Option<f64>,
    ) -> Result<RangeMatrix, Error> {
        let selector_matchers = build_matchers(&ms.vs)?;
        let offset_ns = signed_offset_ns(ms.vs.offset.as_ref())?;
        let at_ts_ns = resolve_at(ms.vs.at.as_ref(), ctx.start_ns, ctx.end_ns)?;
        let range_ns = duration_to_ns(ms.range)?;

        // Evaluation grid: (reported ts, offset/@-shifted window end, window
        // start). `points` is already known to be within budget.
        let capacity = usize::try_from(points).unwrap_or(self.max_range_points);
        let mut grid: Vec<(i64, i64, i64)> = Vec::with_capacity(capacity);
        let mut t = start_ns;
        while t <= end_ns {
            let base_ts = at_ts_ns.unwrap_or(t);
            let sel_ts = base_ts.checked_sub(offset_ns).ok_or(Error::TimeOverflow)?;
            let window_start = sel_ts.checked_sub(range_ns).ok_or(Error::TimeOverflow)?;
            grid.push((t, sel_ts, window_start));
            t = t.checked_add(step_ns).ok_or(Error::TimeOverflow)?;
        }
        if grid.is_empty() {
            return Ok(Vec::new());
        }

        let min_window_start = grid.iter().map(|(_, _, w)| *w).min().unwrap_or(start_ns);
        let max_sel_ts = grid
            .iter()
            .map(|(_, sel, _)| *sel)
            .max()
            .unwrap_or(start_ns);
        let window = TimeRange {
            start_ns: min_window_start,
            end_ns: max_sel_ts,
        };

        let series = source.query(&selector_matchers, window)?;
        let mut out = Vec::with_capacity(series.len());
        for s in series {
            // Staleness does not depend on the current step's window, so it
            // is filtered once per series up front; the cursor below then
            // only has to track the left-open/right-closed window bounds.
            let live: Vec<Sample> = s
                .samples
                .iter()
                .filter(|sample| sample.value.to_bits() != STALE_NAN_BITS)
                .copied()
                .collect();

            let mut lo = 0usize;
            let mut hi = 0usize;
            let mut out_samples = Vec::new();
            for (reported_ts, sel_ts, window_start) in &grid {
                while lo < live.len() && live[lo].ts_ns <= *window_start {
                    lo += 1;
                }
                while hi < live.len() && live[hi].ts_ns <= *sel_ts {
                    hi += 1;
                }
                let window_samples = &live[lo..hi];
                if window_samples.is_empty() {
                    continue;
                }
                if let Some(value) = reduce(
                    window_samples,
                    *window_start,
                    *sel_ts,
                    range_ns,
                    *reported_ts,
                ) {
                    out_samples.push(Sample {
                        ts_ns: *reported_ts,
                        value,
                    });
                }
            }
            if !out_samples.is_empty() {
                let labels = if keep_metric_name {
                    s.labels
                } else {
                    drop_metric_name(s.labels)
                };
                out.push((labels, out_samples));
            }
        }
        Ok(out)
    }
}

/// A range query's top-level construct, after stripping any enclosing
/// `Paren`/`Unary` wrappers (the grammar allows at most one selector, so
/// this identifies it directly rather than routing through the general
/// per-instant `eval_expr`, which would re-query storage once per grid
/// step).
enum RangeCore<'a> {
    Scalar(f64),
    Str(&'a str),
    Selector(&'a promql_parser::parser::VectorSelector),
    Call(&'a promql_parser::parser::Call),
    /// A binary expression: re-evaluated per grid step through the general
    /// `eval_expr` (not a single selector, so unlike the other arms it
    /// cannot be range-fetched in one storage call). Holds the outermost
    /// `expr` passed to `resolve_range_core`, not the `Paren`/`Unary`-
    /// peeled `cur`, so any enclosing negation or parens are re-applied by
    /// `eval_expr` itself at each step rather than needing a second
    /// `negate` bit here.
    Generic(&'a promql_parser::parser::Expr),
}

/// Strip `Paren`/`Unary` wrappers from `expr`, returning the core construct
/// and whether an odd number of unary minuses were applied. Any other
/// top-level construct is a typed error: [`Error::WrongType`] for a bare
/// matrix selector or subquery (both produce a range vector, invalid at top
/// level, exactly like Prometheus), or [`Error::Unsupported`] naming the
/// construct for everything this phase does not implement.
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
            Expr::MatrixSelector(_) | Expr::Subquery(_) => {
                return Err(Error::WrongType {
                    expected: "scalar, instant vector, or string",
                    got: "range vector",
                });
            }
            // Whether `c.func.name` is actually registered is decided by
            // `crate::functions::eval_range_call`, not here: unlike every
            // other arm, a `Call` is not itself a self-describing construct
            // (an unregistered name still needs the same
            // "function call: {name}" error a future family's addition
            // would otherwise have to update this match to keep producing).
            Expr::Call(c) => return Ok((RangeCore::Call(c), negate)),
            Expr::Binary(_) => return Ok((RangeCore::Generic(expr), false)),
            Expr::Aggregate(_) => return Ok((RangeCore::Generic(expr), false)),
            _ => return Err(unsupported_construct_error(cur)),
        }
    }
}

/// The [`Error::Unsupported`] for an AST node this phase does not evaluate.
/// Callers must have already handled `Paren`, `Unary`, `NumberLiteral`,
/// `StringLiteral`, `VectorSelector`, `MatrixSelector`, `Call` (which always
/// dispatches to `crate::functions`, producing its own "function call:
/// {name}" [`Error::Unsupported`] for an unregistered name), `Binary`,
/// `Aggregate` (dispatching to `crate::binop`/`crate::aggregate`), and
/// `Subquery` (`eval_expr`'s own arm, or `resolve_range_core`'s
/// [`Error::WrongType`] for one at top level of a range query); this panics
/// on any of those (programmer error, not reachable) and covers the rest.
fn unsupported_construct_error(expr: &promql_parser::parser::Expr) -> Error {
    use promql_parser::parser::Expr;
    match expr {
        Expr::Extension(_) => Error::Unsupported {
            construct: "extension node".to_string(),
        },
        Expr::Paren(_)
        | Expr::Unary(_)
        | Expr::NumberLiteral(_)
        | Expr::StringLiteral(_)
        | Expr::VectorSelector(_)
        | Expr::MatrixSelector(_)
        | Expr::Call(_)
        | Expr::Binary(_)
        | Expr::Aggregate(_)
        | Expr::Subquery(_) => {
            unreachable!("caller must handle every supported construct before falling back")
        }
    }
}

/// The selector-local evaluation timestamp: `@`'s absolute instant (falling
/// back to the ambient `eval_ts_ns`, i.e. the current grid step or the
/// instant query's own time) shifted by the selector's `offset`, if any.
/// `@`'s `start()`/`end()` forms resolve against the whole query's fixed
/// parameters (`ctx`), not the current step, exactly like Prometheus.
pub(crate) fn selector_eval_ts(
    vs: &promql_parser::parser::VectorSelector,
    eval_ts_ns: i64,
    ctx: &QueryWindow,
) -> Result<i64, Error> {
    resolve_eval_ts(vs.offset.as_ref(), vs.at.as_ref(), eval_ts_ns, ctx)
}

/// The general form of [`selector_eval_ts`]: `@`'s absolute instant (falling
/// back to the ambient `eval_ts_ns`) shifted by `offset`, if any. Shared by
/// vector/matrix selectors and subqueries, whose `offset`/`@` resolve exactly
/// the same way.
pub(crate) fn resolve_eval_ts(
    offset: Option<&promql_parser::parser::Offset>,
    at: Option<&promql_parser::parser::AtModifier>,
    eval_ts_ns: i64,
    ctx: &QueryWindow,
) -> Result<i64, Error> {
    let offset_ns = signed_offset_ns(offset)?;
    let base_ts_ns = resolve_at(at, ctx.start_ns, ctx.end_ns)?.unwrap_or(eval_ts_ns);
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
        .map(|s| {
            // Unary minus over a native-histogram element negates every
            // population (Prometheus multiplies the histogram by -1); a float
            // element negates its value.
            match s.histogram {
                Some(mut h) => {
                    h.mul(-1.0);
                    InstantSample::histogram(drop_metric_name(s.labels), s.ts_ns, s.ts_ns, h)
                }
                None => {
                    InstantSample::scalar(drop_metric_name(s.labels), s.ts_ns, s.ts_ns, -s.value)
                }
            }
        })
        .collect()
}

/// Negate every sample's value in a range matrix. Unlike [`negate_vector`],
/// `__name__` is not dropped here: a function call's result labels already
/// had it dropped unconditionally by
/// [`Evaluator::eval_range_matrix_reduction`], before negation is even
/// considered.
fn negate_matrix(m: RangeMatrix) -> RangeMatrix {
    m.into_iter()
        .map(|(labels, samples)| {
            let negated = samples
                .into_iter()
                .map(|s| Sample {
                    ts_ns: s.ts_ns,
                    value: -s.value,
                })
                .collect();
            (labels, negated)
        })
        .collect()
}

/// Remove `__name__` from a label set, exactly as Prometheus does for the
/// result of a numeric operation.
pub(crate) fn drop_metric_name(labels: LabelSet) -> LabelSet {
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

/// The smallest multiple of `step_ns`, measured from the Unix epoch (time
/// zero), that is strictly greater than `target_ns`. Left-open, exactly like
/// this crate's matrix-selector window ([`Evaluator::eval_matrix_selector`]):
/// a grid point sitting exactly at `target_ns` (a subquery's `end - range`)
/// is excluded, not admitted, matching Prometheus' own subquery alignment
/// (`promql/engine.go`'s `evalSubquery`). Subquery grids are otherwise
/// aligned to epoch-relative step boundaries (`0, step, 2*step, ...`), not to
/// the subquery's own window start or the outer query's step boundaries, so
/// two subqueries with the same step line up on the same instants regardless
/// of where their windows happen to start.
///
/// Computed in `i128` for the same reason [`step_count`] is: `target_ns`'s
/// floor division by `step_ns` must not overflow for extreme-but-
/// representable inputs, and `div_euclid` (floor, not truncating) division is
/// required so a negative `target_ns` (a window before the Unix epoch) aligns
/// the same way Prometheus' Go integer-division-with-manual-correction does.
/// The floor is always `<= target_ns` by construction, so advancing one more
/// step always lands strictly above `target_ns`: no separate `==` case is
/// needed.
fn align_up_to_step(target_ns: i64, step_ns: i64) -> Result<i64, Error> {
    let target = i128::from(target_ns);
    let step = i128::from(step_ns);
    let floor = target.div_euclid(step) * step;
    let aligned = floor + step;
    i64::try_from(aligned).map_err(|_| Error::TimeOverflow)
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
///
/// Returns `(value, orig_sample_ts_ns)`: the picked value alongside the
/// picked sample's own timestamp (`timestamp()`'s
/// [`InstantSample::orig_sample_ts_ns`] needs the real stored timestamp,
/// which may differ from `sel_ts_ns` since the lookback rule picks the most
/// recent sample at or before it, not necessarily one exactly at it).
fn pick_sample(samples: &[Sample], sel_ts_ns: i64, lookback_delta_ns: i64) -> Option<(f64, i64)> {
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
    Some((f64::from_bits(best_bits), candidate_ts_ns))
}

/// Pick the native histogram at the most recent timestamp in `(sel_ts -
/// lookback, sel_ts]`, the histogram counterpart of [`pick_sample`]. Native
/// histograms carry no staleness-marker bit pattern (that is a float NaN
/// payload), so there is no stale-drop here; the most recent in-window sample
/// wins. Returns `(value, orig_sample_ts_ns)`.
fn pick_histogram(
    samples: &[crate::source::HistogramSample],
    sel_ts_ns: i64,
    lookback_delta_ns: i64,
) -> Option<(crate::histogram::FloatHistogram, i64)> {
    let idx = samples.partition_point(|s| s.ts_ns <= sel_ts_ns);
    if idx == 0 {
        return None;
    }
    let candidate = &samples[idx - 1];
    let window_start = sel_ts_ns.checked_sub(lookback_delta_ns)?;
    if candidate.ts_ns <= window_start {
        return None;
    }
    Some((candidate.value.clone(), candidate.ts_ns))
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

    // --- Warning/info annotations ---

    #[test]
    fn quantile_out_of_range_surfaces_a_warning() {
        // quantile(1.5, ...) clamps to +Inf; the value is correct but the
        // out-of-range argument earns a warning, and no info.
        let source = TestSource::new()
            .with_series(&[("__name__", "m"), ("i", "1")], &[(0, 1.0)])
            .expect("valid series")
            .with_series(&[("__name__", "m"), ("i", "2")], &[(0, 2.0)])
            .expect("valid series");
        let (value, annotations) = Evaluator::new()
            .eval_instant_annotated(&source, "quantile(1.5, m)", 0)
            .expect("evaluates");
        match value {
            Value::Vector(v) => assert_eq!(v[0].value, f64::INFINITY),
            other => panic!("expected vector, got {}", other.type_name()),
        }
        assert_eq!(annotations.warnings().len(), 1, "one warning");
        assert!(
            annotations.warnings()[0].contains("quantile value should be between 0 and 1"),
            "warning names the out-of-range quantile: {:?}",
            annotations.warnings()
        );
        assert!(annotations.infos().is_empty(), "no info for this case");
    }

    #[test]
    fn quantile_in_range_surfaces_no_annotations() {
        let source = TestSource::new()
            .with_series(&[("__name__", "m"), ("i", "1")], &[(0, 1.0)])
            .expect("valid series")
            .with_series(&[("__name__", "m"), ("i", "2")], &[(0, 2.0)])
            .expect("valid series");
        let (_value, annotations) = Evaluator::new()
            .eval_instant_annotated(&source, "quantile(0.5, m)", 0)
            .expect("evaluates");
        assert!(
            annotations.is_empty(),
            "a well-formed quantile query is annotation-free: {annotations:?}"
        );
    }

    #[test]
    fn histogram_quantile_missing_inf_bucket_is_nan_without_a_warning() {
        // A classic histogram with two or more buckets but no +Inf top bucket
        // (an incomplete shape, e.g. a `le!="+Inf"` matcher) is NaN, and,
        // matching the pinned Prometheus binary, raises NO warning: its
        // `bucketQuantile` returns NaN silently for that shape.
        // Before this fix Ravel emitted an extra bad-buckets warning here,
        // which is the divergence the over_time/histogram_classic difftest
        // corpora surfaced.
        let source = TestSource::new()
            .with_series(&[("__name__", "http_bucket"), ("le", "0.1")], &[(0, 10.0)])
            .expect("valid series")
            .with_series(&[("__name__", "http_bucket"), ("le", "0.5")], &[(0, 20.0)])
            .expect("valid series");
        let (value, annotations) = Evaluator::new()
            .eval_instant_annotated(&source, "histogram_quantile(0.9, http_bucket)", 0)
            .expect("evaluates");
        match value {
            Value::Vector(v) => assert!(v[0].value.is_nan(), "incomplete histogram is NaN"),
            other => panic!("expected vector, got {}", other.type_name()),
        }
        assert!(
            annotations.is_empty(),
            "a missing-+Inf classic histogram raises no annotation, matching \
             Prometheus: {annotations:?}"
        );
    }

    #[test]
    fn histogram_quantile_single_bucket_still_surfaces_a_bad_buckets_warning() {
        // A genuinely degenerate group (fewer than two usable buckets) is a
        // distinct case from the missing-+Inf shape above: it keeps the
        // bad-buckets warning. This pins that the missing-+Inf narrowing
        // touched only that reason, not the too-few-buckets one.
        let source = TestSource::new()
            .with_series(&[("__name__", "http_bucket"), ("le", "+Inf")], &[(0, 10.0)])
            .expect("valid series");
        let (value, annotations) = Evaluator::new()
            .eval_instant_annotated(&source, "histogram_quantile(0.9, http_bucket)", 0)
            .expect("evaluates");
        match value {
            Value::Vector(v) => assert!(v[0].value.is_nan(), "degenerate histogram is NaN"),
            other => panic!("expected vector, got {}", other.type_name()),
        }
        assert_eq!(annotations.warnings().len(), 1, "one bad-buckets warning");
        assert!(
            annotations.warnings()[0].contains("classic histogram"),
            "warning names the malformed classic histogram: {:?}",
            annotations.warnings()
        );
    }

    #[test]
    fn histogram_quantile_non_monotonic_buckets_surface_an_info() {
        // Cumulative counts dip (0.5 bucket below the 0.1 bucket): the fixup
        // is an info, not a warning, and the result is still produced.
        let source = TestSource::new()
            .with_series(&[("__name__", "hb"), ("le", "0.1")], &[(0, 10.0)])
            .expect("valid series")
            .with_series(&[("__name__", "hb"), ("le", "0.5")], &[(0, 5.0)])
            .expect("valid series")
            .with_series(&[("__name__", "hb"), ("le", "+Inf")], &[(0, 20.0)])
            .expect("valid series");
        let (_value, annotations) = Evaluator::new()
            .eval_instant_annotated(&source, "histogram_quantile(0.5, hb)", 0)
            .expect("evaluates");
        assert!(
            annotations.warnings().is_empty(),
            "a monotonicity fixup is not a warning: {:?}",
            annotations.warnings()
        );
        assert_eq!(annotations.infos().len(), 1, "one forced-monotonicity info");
        assert!(
            annotations.infos()[0].contains("monotonicity"),
            "info names the monotonicity fixup: {:?}",
            annotations.infos()
        );
    }

    #[test]
    fn range_query_deduplicates_a_per_step_warning() {
        // A range query re-evaluates the quantile at every grid step, but the
        // identical out-of-range warning is reported once, not per step.
        let source = TestSource::new()
            .with_series(&[("__name__", "m"), ("i", "1")], &[(0, 1.0)])
            .expect("valid series")
            .with_series(&[("__name__", "m"), ("i", "2")], &[(0, 2.0)])
            .expect("valid series");
        let (_value, annotations) = Evaluator::new()
            .eval_range_annotated(&source, "quantile(1.5, m)", 0, minutes(5), minutes(1))
            .expect("evaluates");
        assert_eq!(
            annotations.warnings().len(),
            1,
            "the per-step warning is de-duplicated to one: {:?}",
            annotations.warnings()
        );
    }

    #[test]
    fn rate_over_a_non_counter_named_metric_surfaces_an_info() {
        // `http_requests` has none of the counter-naming
        // suffixes (`_total`/`_sum`/`_count`/`_bucket`), so `rate()` over it
        // should raise `PossibleNonCounterInfo` even though the computed
        // value is correct.
        // Both samples fall strictly inside the window (0, 5m] (its start is
        // exclusive), so `rate()` computes a value and the check runs.
        let source = TestSource::new()
            .with_series(
                &[("__name__", "http_requests")],
                &[
                    (minutes(1) * NS_PER_MS, 1.0),
                    (minutes(4) * NS_PER_MS, 61.0),
                ],
            )
            .expect("valid series");
        let (_value, annotations) = Evaluator::new()
            .eval_instant_annotated(&source, "rate(http_requests[5m])", minutes(5))
            .expect("evaluates");
        assert!(
            annotations.warnings().is_empty(),
            "not a counter is an info, not a warning: {:?}",
            annotations.warnings()
        );
        assert_eq!(annotations.infos().len(), 1, "one non-counter-name info");
        assert!(
            annotations.infos()[0].contains("might not be a counter"),
            "info names the non-counter-suffixed metric: {:?}",
            annotations.infos()
        );
    }

    #[test]
    fn increase_over_a_non_counter_named_metric_surfaces_an_info() {
        // Same check as `rate()`, since both are counter-oriented.
        let source = TestSource::new()
            .with_series(
                &[("__name__", "http_requests")],
                &[
                    (minutes(1) * NS_PER_MS, 1.0),
                    (minutes(4) * NS_PER_MS, 61.0),
                ],
            )
            .expect("valid series");
        let (_value, annotations) = Evaluator::new()
            .eval_instant_annotated(&source, "increase(http_requests[5m])", minutes(5))
            .expect("evaluates");
        assert_eq!(annotations.infos().len(), 1, "one non-counter-name info");
    }

    #[test]
    fn rate_over_a_counter_named_metric_surfaces_no_info() {
        let source = TestSource::new()
            .with_series(
                &[("__name__", "http_requests_total")],
                &[
                    (minutes(1) * NS_PER_MS, 1.0),
                    (minutes(4) * NS_PER_MS, 61.0),
                ],
            )
            .expect("valid series");
        let (_value, annotations) = Evaluator::new()
            .eval_instant_annotated(&source, "rate(http_requests_total[5m])", minutes(5))
            .expect("evaluates");
        assert!(
            annotations.is_empty(),
            "a _total-suffixed name needs no info: {annotations:?}"
        );
    }

    #[test]
    fn rate_over_a_single_sample_window_surfaces_no_info() {
        // Only one sample falls in the window: `rate()` produces no value
        // (fewer than two samples), and Prometheus' own check sits after
        // that early return, so no info either, despite the non-counter name.
        let source = TestSource::new()
            .with_series(&[("__name__", "http_requests")], &[(0, 1.0)])
            .expect("valid series");
        let (_value, annotations) = Evaluator::new()
            .eval_instant_annotated(&source, "rate(http_requests[5m])", minutes(5))
            .expect("evaluates");
        assert!(
            annotations.is_empty(),
            "no computed value means no info: {annotations:?}"
        );
    }

    #[test]
    fn delta_never_surfaces_the_non_counter_info() {
        // `delta()` targets gauges; Prometheus has no counter-naming check
        // for it at all, unlike `rate()`/`increase()`.
        let source = TestSource::new()
            .with_series(
                &[("__name__", "http_requests")],
                &[
                    (minutes(1) * NS_PER_MS, 1.0),
                    (minutes(4) * NS_PER_MS, 61.0),
                ],
            )
            .expect("valid series");
        let (_value, annotations) = Evaluator::new()
            .eval_instant_annotated(&source, "delta(http_requests[5m])", minutes(5))
            .expect("evaluates");
        assert!(
            annotations.is_empty(),
            "delta() never raises the non-counter info: {annotations:?}"
        );
    }

    #[test]
    fn rate_over_a_label_matcher_only_selector_surfaces_no_info() {
        // `{job="x"}` has no literal selector name for the check to inspect
        // (see `maybe_info_non_counter_selector_name`'s doc comment): the
        // fix's scope is the selector's own literal name, not each matched
        // series' `__name__` label.
        let source = TestSource::new()
            .with_series(
                &[("__name__", "http_requests"), ("job", "x")],
                &[
                    (minutes(1) * NS_PER_MS, 1.0),
                    (minutes(4) * NS_PER_MS, 61.0),
                ],
            )
            .expect("valid series");
        let (_value, annotations) = Evaluator::new()
            .eval_instant_annotated(&source, "rate({job=\"x\"}[5m])", minutes(5))
            .expect("evaluates");
        assert!(
            annotations.is_empty(),
            "a name-less selector has nothing to check: {annotations:?}"
        );
    }

    #[test]
    fn range_query_dedupes_the_non_counter_info_per_step() {
        // Two samples per 5m step window (the window is left-open: `(t-5m,
        // t]`), so both grid steps (5m, 10m) independently produce a rate
        // and raise the identical info text.
        let source = TestSource::new()
            .with_series(
                &[("__name__", "http_requests")],
                &[
                    (minutes(1) * NS_PER_MS, 1.0),
                    (minutes(4) * NS_PER_MS, 2.0),
                    (minutes(6) * NS_PER_MS, 3.0),
                    (minutes(9) * NS_PER_MS, 4.0),
                ],
            )
            .expect("valid series");
        let (_value, annotations) = Evaluator::new()
            .eval_range_annotated(
                &source,
                "rate(http_requests[5m])",
                minutes(5),
                minutes(10),
                minutes(5),
            )
            .expect("evaluates");
        assert_eq!(
            annotations.infos().len(),
            1,
            "the per-step info is de-duplicated to one: {:?}",
            annotations.infos()
        );
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
            .expect_err("or-grouped matchers are not in scope");
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
            ("sort_by_label(up)", "sort_by_label"),
            ("mad_over_time(up[5m])", "mad_over_time"),
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
    fn bare_subquery_at_top_level_is_wrong_type() {
        // `up[5m:1m]` parses and its grid is evaluated, but a matrix is never
        // a valid top-level instant query result, same as a bare matrix
        // selector.
        let err = Evaluator::new()
            .instant(&TestSource::new(), "up[5m:1m]", 0)
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
    fn align_up_to_step_rounds_up_to_the_next_epoch_multiple() {
        // 90 is not a multiple of 60; the next one up is 120.
        assert_eq!(align_up_to_step(90, 60).expect("fits"), 120);
        // Already a multiple: left-open, so it still advances past it
        // rather than returning it.
        assert_eq!(align_up_to_step(120, 60).expect("fits"), 180);
    }

    #[test]
    fn align_up_to_step_floors_towards_negative_infinity_before_epoch() {
        // -90 sits between -120 and -60; the next multiple up is -60, not
        // -120 (a truncating, non-floor division would wrongly land there).
        assert_eq!(align_up_to_step(-90, 60).expect("fits"), -60);
        // -120 is itself a multiple; left-open means the result still
        // advances past it, landing on the same -60 as the -90 case above.
        assert_eq!(align_up_to_step(-120, 60).expect("fits"), -60);
    }

    #[test]
    fn subquery_grid_is_epoch_aligned_not_window_relative() {
        // `up[3m:1m]` at t=90s: the nominal window is [-90s, 90s], but the
        // grid start is the smallest 1m-multiple from the Unix epoch that is
        // >= -90s, which is -60s, not the window's own start. The sample
        // sits at -60s so every grid point (-60s, 0s, 60s) finds it within
        // the default 5m lookback.
        let source = TestSource::new()
            .with_series(&[("__name__", "up")], &[(-60_000_000_000, 1.0)])
            .expect("valid series");
        let value = Evaluator::new()
            .eval_instant(&source, "up[3m:1m]", 90_000)
            .expect("subquery evaluates");
        let Value::Matrix(matrix) = value else {
            panic!("expected a matrix, got {value:?}");
        };
        assert_eq!(matrix.len(), 1);
        let (_, samples) = &matrix[0];
        let timestamps_ms: Vec<i64> = samples.iter().map(|s| s.ts_ns / NS_PER_MS).collect();
        assert_eq!(timestamps_ms, vec![-60_000, 0, 60_000]);
    }

    #[test]
    fn subquery_without_its_own_step_uses_the_evaluator_default() {
        let source = TestSource::new()
            .with_series(&[("__name__", "up")], &[(0, 1.0)])
            .expect("valid series");
        let with_default = Evaluator::new()
            .with_default_step(std::time::Duration::from_secs(30))
            .expect("valid step")
            .eval_instant(&source, "up[2m:]", 0)
            .expect("subquery evaluates");
        let with_explicit = Evaluator::new()
            .eval_instant(&source, "up[2m:30s]", 0)
            .expect("subquery evaluates");
        assert_eq!(with_default, with_explicit);
    }

    #[test]
    fn nested_subquery_recurses_through_an_intervening_function() {
        // The inner subquery `up[2m:1m]` feeds `count_over_time`, whose
        // instant-vector result is itself subqueried by the outer
        // `[2m:1m]`. At eval_ts=0 both the outer grid's own target
        // (0 - 2m = -120s) and, at every outer step, the inner grid's own
        // target land exactly on a 1m step multiple; the left-open rule
        // excludes that boundary point from each grid, so
        // both grids have 2 points instead of 3. The single sample sits far
        // enough in the past (-240s) that every remaining inner grid point,
        // at every outer step, still finds it within the default 5m
        // lookback, so `count_over_time` over the always-2-point inner grid
        // is 2 at every outer step.
        let source = TestSource::new()
            .with_series(&[("__name__", "up")], &[(-240 * NS_PER_MS * 1000, 1.0)])
            .expect("valid series");
        let value = Evaluator::new()
            .eval_instant(&source, "count_over_time(up[2m:1m])[2m:1m]", 0)
            .expect("nested subquery evaluates");
        let Value::Matrix(matrix) = value else {
            panic!("expected a matrix, got {value:?}");
        };
        assert_eq!(matrix.len(), 1);
        let (_, samples) = &matrix[0];
        assert_eq!(samples.len(), 2);
        assert!(samples.iter().all(|s| s.value == 2.0));
    }

    #[test]
    fn range_function_over_subquery_is_an_instant_vector_inside_an_outer_subquery() {
        // Regression: the exact nesting shape
        // `max_over_time(rate(<selector>[2m])[5m:1m])[10m:2m]`. The inner
        // expression of the outer subquery is itself a range-vector-consuming
        // function (`max_over_time`) wrapped around a subquery whose own inner
        // body is another range function over a matrix selector (`rate(..[2m])`).
        // Each outer grid step must resolve that inner expression to an
        // *instant vector* (not a range vector), and the whole outer subquery
        // must therefore resolve to a matrix. This exercises the deeper nesting
        // the pre-existing `nested_subquery_recurses_through_an_intervening_
        // function` test does not: there the innermost body is a bare selector,
        // here it is a `rate` over a matrix selector.
        //
        // A regularly-sampled monotonic counter every 30s: `rate` over any 2m
        // window is a constant 1 unit / 30s = 1/30 per second, so
        // `max_over_time` of that subquery grid is 1/30 at every step.
        let mut samples = Vec::new();
        let mut value = 0.0;
        let mut t_ms = 0i64;
        while t_ms <= minutes(20) {
            samples.push((ms_to_ns(t_ms).expect("no overflow"), value));
            value += 1.0;
            t_ms += 30_000;
        }
        let source = TestSource::new()
            .with_series(
                &[("__name__", "diff_counter_total"), ("shape", "reset")],
                &samples,
            )
            .expect("valid series");

        // The inner expression alone (an over-time function wrapping a
        // subquery) resolves to an instant vector, the per-step value type the
        // outer subquery consumes.
        let inner = Evaluator::new()
            .eval_instant(
                &source,
                r#"max_over_time(rate(diff_counter_total{shape="reset"}[2m])[5m:1m])"#,
                minutes(18),
            )
            .expect("inner range-function-over-subquery evaluates");
        let Value::Vector(vector) = inner else {
            panic!("inner expression must be an instant vector, got {inner:?}");
        };
        assert_eq!(vector.len(), 1);
        assert!((vector[0].value - 1.0 / 30.0).abs() < 1e-9);

        // Wrapping that instant vector in the outer `[10m:2m]` subquery must
        // succeed and resolve to a matrix (Prometheus returns `resultType:
        // matrix` for this instant query), not raise a range-vector type error.
        let outer = Evaluator::new()
            .eval_instant(
                &source,
                r#"max_over_time(rate(diff_counter_total{shape="reset"}[2m])[5m:1m])[10m:2m]"#,
                minutes(18),
            )
            .expect("outer subquery over the range function evaluates");
        let Value::Matrix(matrix) = outer else {
            panic!("outer subquery must resolve to a matrix, got {outer:?}");
        };
        assert_eq!(matrix.len(), 1);
        let (_, samples) = &matrix[0];
        // Outer grid: end 18m, range 10m, step 2m -> target is 8m exactly,
        // excluded by the left-open grid-start rule, so the
        // grid starts at the next step multiple: 10m, 12m, 14m, 16m, 18m.
        assert_eq!(samples.len(), 5);
        assert!(samples.iter().all(|s| (s.value - 1.0 / 30.0).abs() < 1e-9));
    }

    #[test]
    fn subquery_offset_and_at_shift_the_grids_own_end() {
        // `up[2m:1m] offset 1m` at t=0 ends its grid at -1m (the grid start
        // is then epoch-aligned from that shifted end), not at t=0 itself.
        // The target (-1m - 2m = -180s) lands exactly on a 1m step
        // multiple, so the left-open rule excludes it: the
        // grid starts at -120s, not -180s, one point short of the
        // pre-fix grid. The sample sits at -180s, still within lookback of
        // every remaining grid point exercised below, in both the offset
        // and the `@` case.
        let source = TestSource::new()
            .with_series(&[("__name__", "up")], &[(-180_000_000_000, 1.0)])
            .expect("valid series");
        let offset = Evaluator::new()
            .eval_instant(&source, "up[2m:1m] offset 1m", 0)
            .expect("subquery evaluates");
        let Value::Matrix(matrix) = &offset else {
            panic!("expected a matrix, got {offset:?}");
        };
        let (_, samples) = &matrix[0];
        let timestamps_ms: Vec<i64> = samples.iter().map(|s| s.ts_ns / NS_PER_MS).collect();
        assert_eq!(timestamps_ms, vec![-120_000, -60_000]);

        // `@ 0` pins the grid's end to t=0 regardless of the query's own
        // evaluation timestamp.
        let at = Evaluator::new()
            .eval_instant(&source, "up[2m:1m] @ 0", 999_999)
            .expect("subquery evaluates");
        let bare_at_zero = Evaluator::new()
            .eval_instant(&source, "up[2m:1m]", 0)
            .expect("subquery evaluates");
        assert_eq!(at, bare_at_zero, "@0 pins the grid as if evaluated at t=0");
    }

    /// One native histogram, mirroring the fixture in
    /// `functions::histogram_native`'s tests, for the subquery-over-histogram
    /// rejection cases below.
    fn nh(count: f64, sum: f64) -> crate::histogram::FloatHistogram {
        crate::histogram::FloatHistogram {
            counter_reset_hint: crate::histogram::ResetHint::Unknown,
            scale: 0,
            zero_threshold: 0.0,
            zero_count: 0.0,
            count,
            sum,
            positive_spans: vec![crate::histogram::Span {
                offset: 1,
                length: 1,
            }],
            negative_spans: Vec::new(),
            positive_buckets: vec![count],
            negative_buckets: Vec::new(),
            custom_values: Vec::new(),
        }
    }

    fn expect_unsupported(err: Error, query: &str) {
        let Error::Unsupported { construct } = err else {
            panic!("expected Unsupported for {query:?}, got {err:?}");
        };
        assert!(
            construct.contains("subquery over native histograms"),
            "construct {construct:?} should name the histogram-subquery construct for {query:?}"
        );
    }

    #[test]
    fn subquery_over_native_histogram_is_unsupported_in_an_instant_query() {
        // `rate(h[10m:1m])`: the inner selector `h` matches a native-histogram
        // series, so the subquery grid would otherwise silently drop it and
        // return a wrong (empty) answer. It must be a typed Unsupported error
        // instead.
        let source = TestSource::new()
            .with_histogram_series(&[("__name__", "h"), ("job", "a")], &[(0, nh(6.0, 42.0))])
            .expect("valid histogram series");
        let err = Evaluator::new()
            .instant(&source, "rate(h[10m:1m])", 0)
            .expect_err("subquery over a histogram series must be rejected");
        expect_unsupported(err, "rate(h[10m:1m])");
    }

    #[test]
    fn subquery_over_native_histogram_is_unsupported_in_a_range_query() {
        // Same shape inside a range query: the rejection is inherited by the
        // range-dispatch subquery path, not special-cased per entry point.
        let source = TestSource::new()
            .with_histogram_series(&[("__name__", "h"), ("job", "a")], &[(0, nh(6.0, 42.0))])
            .expect("valid histogram series");
        let err = Evaluator::new()
            .eval_range(&source, "rate(h[10m:1m])", 0, minutes(5), minutes(1))
            .expect_err("subquery over a histogram series must be rejected");
        expect_unsupported(err, "rate(h[10m:1m]) (range)");
    }

    #[test]
    fn float_only_subquery_over_the_same_source_shape_is_unaffected() {
        // A float series queried through the identical subquery shape keeps
        // working exactly as before: detection triggers on the actual presence
        // of histogram data, never on the syntactic subquery form.
        //
        // Samples f=2 at t=0 and f=8 at t=60s. At t=120s the `[10m:1m]`
        // subquery grid steps every 60s from -480s to 120s; only the steps at
        // 0s, 60s, 120s pick a sample within the default 5m lookback (2, 8, 8),
        // so the inner matrix carries exactly three points. `count_over_time`
        // of that matrix is a concrete 3, independent of any extrapolation
        // math, and is unchanged by this task.
        let source = TestSource::new()
            .with_series(
                &[("__name__", "f"), ("job", "a")],
                &[(0, 2.0), (60_000_000_000, 8.0)],
            )
            .expect("valid float series");
        let counted = Evaluator::new()
            .instant(&source, "count_over_time(f[10m:1m])", 120_000)
            .expect("float-only subquery still evaluates");
        assert_eq!(counted.len(), 1);
        assert_eq!(counted[0].value.to_bits(), 3.0_f64.to_bits());
        assert!(counted[0].histogram.is_none());

        // `rate(f[10m:1m])` over the same float series (the shape the decision
        // note calls out explicitly) still produces one finite float series,
        // never an error and never a histogram element.
        let rated = Evaluator::new()
            .instant(&source, "rate(f[10m:1m])", 120_000)
            .expect("float-only rate subquery still evaluates");
        assert_eq!(rated.len(), 1);
        assert!(rated[0].value.is_finite());
        assert!(rated[0].histogram.is_none());
    }

    #[test]
    fn mixed_float_and_histogram_subquery_is_unsupported() {
        // The inner selector `{job="a"}` matches both a float series and a
        // histogram series. Presence of any matched histogram data rejects the
        // whole subquery (detect-and-reject), even alongside float series.
        let source = TestSource::new()
            .with_series(&[("__name__", "f"), ("job", "a")], &[(0, 2.0)])
            .expect("valid float series")
            .with_histogram_series(&[("__name__", "h"), ("job", "a")], &[(0, nh(6.0, 42.0))])
            .expect("valid histogram series");
        let err = Evaluator::new()
            .instant(&source, r#"rate({job="a"}[10m:1m])"#, 0)
            .expect_err("a mixed subquery must be rejected");
        expect_unsupported(err, r#"rate({job="a"}[10m:1m])"#);
    }

    #[test]
    fn subquery_grid_excludes_a_boundary_point_exactly_at_end_minus_range() {
        // Regression test: the subquery grid's start must be
        // left-open (`> end - range`), matching this crate's own
        // matrix-selector windows, not closed (`>= end - range`).
        //
        // Samples every 30s from 0 to 120s. `count_over_time(x[2m:1m])`
        // evaluated at t=120s (a step-aligned instant): range=2m, so
        // target = end - range = 120s - 120s = 0s, which is itself a
        // multiple of the 1m step. Before the fix, the closed rule admitted
        // 0s into the grid (points 0, 60, 120 -> count 3); the corrected
        // left-open rule excludes it (points 60, 120 -> count 2), one fewer
        // point than before the fix.
        let source = TestSource::new()
            .with_series(
                &[("__name__", "x")],
                &[
                    (0, 1.0),
                    (30_000_000_000, 1.0),
                    (60_000_000_000, 1.0),
                    (90_000_000_000, 1.0),
                    (120_000_000_000, 1.0),
                ],
            )
            .expect("valid series");
        let result = Evaluator::new()
            .instant(&source, "count_over_time(x[2m:1m])", 120_000)
            .expect("subquery evaluates");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 2.0);
    }

    #[test]
    fn subquery_grid_is_unchanged_when_the_boundary_is_not_step_aligned() {
        // Regression guard: when `end - range` does NOT land on a step
        // multiple, the left-open fix must not change the
        // grid at all, since `align_up_to_step` already rounded strictly
        // past a non-aligned target before and after the fix.
        //
        // Same series as above, but evaluated at t=135s: range=2m (120s)
        // gives target = 135s - 120s = 15s, which is not a multiple of the
        // 1m step. The smallest 1m-multiple strictly greater than 15s is
        // 60s, exactly what the pre-fix `>=` rule would also have produced
        // (60s was already `> 15s`, so old and new rule agree here).
        let source = TestSource::new()
            .with_series(
                &[("__name__", "x")],
                &[
                    (0, 1.0),
                    (30_000_000_000, 1.0),
                    (60_000_000_000, 1.0),
                    (90_000_000_000, 1.0),
                    (120_000_000_000, 1.0),
                ],
            )
            .expect("valid series");
        let result = Evaluator::new()
            .instant(&source, "count_over_time(x[2m:1m])", 135_000)
            .expect("subquery evaluates");
        assert_eq!(result.len(), 1);
        // Grid: 60s, 120s (start_ns=60s from align_up_to_step(15s, 60s),
        // end_ns=135s) -> 2 points, both find a sample.
        assert_eq!(result[0].value, 2.0);
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

    /// A query built to exceed [`crate::complexity_guard::MAX_QUERY_COMPLEXITY`]
    /// must fail with a typed error, not attempt to parse (issue #529). The
    /// query used here is a flat operator chain with no parens at all: it is
    /// the construct that proved a bracket/unary-only guard insufficient, and
    /// it stays orders of magnitude below the depth that would actually
    /// crash `promql_parser::parser::parse` (see the module docs on
    /// `complexity_guard` for the measured crash thresholds) -- this test
    /// proves the guard rejects it, not that the unguarded parser survives it.
    #[test]
    fn overly_complex_instant_query_is_rejected_not_parsed() {
        let mut query = String::from("1");
        for _ in 0..300 {
            query.push_str("+1");
        }
        let err = Evaluator::new()
            .eval_instant(&TestSource::new(), &query, 0)
            .expect_err("must be rejected before parsing");
        assert!(
            matches!(err, Error::TooComplex(_)),
            "expected Error::TooComplex, got {err:?}"
        );
    }

    #[test]
    fn overly_complex_range_query_is_rejected_not_parsed() {
        let mut query = String::from("1");
        for _ in 0..300 {
            query.push_str("+1");
        }
        let err = Evaluator::new()
            .eval_range(&TestSource::new(), &query, 0, minutes(1), minutes(1))
            .expect_err("must be rejected before parsing");
        assert!(
            matches!(err, Error::TooComplex(_)),
            "expected Error::TooComplex, got {err:?}"
        );
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
            budget: Cell::new(DEFAULT_MAX_TOTAL_EVAL_POINTS),
            deadline: None,
            annotations: RefCell::new(Annotations::default()),
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
            budget: Cell::new(DEFAULT_MAX_TOTAL_EVAL_POINTS),
            deadline: None,
            annotations: RefCell::new(Annotations::default()),
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
        // Pathological input: the widest representable span at the
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
    fn over_wide_nested_subquery_is_rejected_without_large_allocation() {
        // Outer subquery: ~60 points (1m at 1s step), each step evaluating
        // `max_over_time(up[10d:1s])`. The inner subquery alone asks for
        // 10 days at a 1s step. Every outer grid step lands on a whole
        // second, so the inner target (`t - 10d`) is always an exact
        // multiple of the inner 1s step; the left-open rule
        // excludes that boundary point, giving 864_000 points, not
        // 864_001, still far over the default 11_000 cap. The very first
        // outer step must already trip the inner cap check before building
        // the inner grid or touching storage, so this test terminates
        // immediately rather than attempting an 864_000-entry allocation
        // (repeated across outer steps).
        let source = one_series_source();
        let err = Evaluator::new()
            .instant(&source, "max_over_time(up[10d:1s])[1m:1s]", 0)
            .expect_err("an over-wide nested subquery grid must be rejected");
        let Error::TooManyPoints { points, max } = err else {
            panic!("expected TooManyPoints, got {err:?}");
        };
        assert_eq!(points, 864_000);
        assert_eq!(max, DEFAULT_MAX_RANGE_POINTS);
        assert_eq!(
            source.query_count(),
            0,
            "rejection must happen before any grid step reaches storage"
        );
    }

    #[test]
    fn default_shared_eval_budget_has_the_documented_value() {
        assert_eq!(DEFAULT_MAX_TOTAL_EVAL_POINTS, 1_000_000);
        assert_eq!(
            Evaluator::new().max_total_eval_points(),
            DEFAULT_MAX_TOTAL_EVAL_POINTS
        );
    }

    #[test]
    fn repeated_nested_subquery_reevaluation_is_rejected_by_shared_budget() {
        // A range query over `max_over_time(up[5m:1m])` re-evaluates the
        // inner `up[5m:1m]` subquery from scratch at every one of its 6
        // outer grid steps: each step's own inner grid is only
        // 5 points (one `source.query` call apiece, since the inner
        // expression is the bare selector `up`), far under any per-node
        // `max_range_points` cap, so `TooManyPoints` never fires no matter
        // how many outer steps run. Left uncapped, the full query would
        // touch 6 * 5 = 30 evaluation points and issue 30 storage queries.
        // The shared cross-level budget must still catch the multiplied
        // cost: with a budget of 10, exactly the first two outer steps'
        // worth of points (10) are charged before the third step's charge
        // (which would bring the running total to 15) is rejected.
        let source = one_series_source();
        let err = Evaluator::new()
            .with_max_total_eval_points(10)
            .range(
                &source,
                "max_over_time(up[5m:1m])",
                minutes(5),
                minutes(10),
                minutes(1),
            )
            .expect_err("shared cross-level budget must be exhausted");
        let Error::EvalBudgetExhausted { touched, max } = err else {
            panic!("expected EvalBudgetExhausted, got {err:?}");
        };
        assert_eq!(max, 10);
        assert_eq!(touched, 15);
        assert_eq!(
            source.query_count(),
            10,
            "only the first two outer steps' worth of points may reach storage \
             before the shared budget rejects the third"
        );
        assert!(
            source.query_count() < 30,
            "rejection must happen well before the outer grid's full 30-point \
             cost (6 outer steps * 5 inner points each) is reached; got {} \
             storage queries",
            source.query_count()
        );
    }

    #[test]
    fn short_deadline_cancels_a_long_running_nested_subquery_evaluation() {
        // `max_over_time(up[1000s:1s])` over a 1000-step outer range (1000
        // points, 1s step) re-evaluates its ~1000-point inner subquery from
        // scratch at every outer step: ~1,000,000 total
        // evaluation points, comfortably under both the per-node
        // `max_range_points` cap and the shared `max_total_eval_points`
        // budget, so without a deadline this runs to completion -- measured
        // separately at just over 1.5s in this same debug build. With a
        // 20ms deadline, the evaluator's own `check_deadline` (fired inside
        // the per-outer-step subquery re-evaluation loop, once per inner
        // grid point too) must notice and stop well before that, not after
        // running the full ~1,000,000-point computation and only then
        // reporting a timeout: the assertion on `elapsed` is what tells the
        // two apart, since a bug that dropped every `check_deadline` call
        // would still return `DeadlineExceeded` eventually (via a future
        // outer wrapper) but only after the full ~1.5s of work.
        let source = one_series_source();
        let deadline = Instant::now() + std::time::Duration::from_millis(20);
        let start = Instant::now();
        let err = Evaluator::new()
            .with_deadline(deadline)
            .range(
                &source,
                "max_over_time(up[1000s:1s])",
                1_000_000,
                1_000_000 + 999_000,
                1_000,
            )
            .expect_err("a deadline in the past relative to the workload must fire");
        let elapsed = start.elapsed();
        assert!(matches!(err, Error::DeadlineExceeded));
        assert!(
            elapsed < std::time::Duration::from_millis(500),
            "evaluation must be cancelled soon after the 20ms deadline, not \
             after running the full ~1,000,000-point computation (which \
             takes over 1.5s in this same debug build); took {elapsed:?}"
        );
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
