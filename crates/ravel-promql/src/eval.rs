//! PromQL evaluator core (ADR-0007). Phase 1 scope: instant and range
//! evaluation of a single vector selector (matchers + offset). Every other
//! AST node is rejected with [`Error::Unsupported`], naming the construct.
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

use ravel_types::{LabelSet, METRIC_NAME_LABEL, Sample, TimeRange};

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
/// the query's evaluation time, never the offset-shifted lookup time used
/// to pick the sample: Prometheus reports the query timestamp regardless of
/// any `offset` on the selector.
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
}

/// Default PromQL lookback: 5 minutes, in nanoseconds.
const DEFAULT_LOOKBACK_NS: i64 = 5 * 60 * 1_000_000_000;

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
/// in the lookback window the series is **absent** at that instant, not a
/// NaN value. Detection is an exact-bits comparison, never `is_nan()`: every
/// other NaN payload is a live, observable value and passes through
/// bit-for-bit (`f64::to_bits` exactness is a frozen invariant).
const STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;

/// PromQL evaluator for a single vector selector. Stateless besides its
/// lookback and resolution configuration; safe to share across queries.
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

    /// Evaluate `query` as an instant vector at `t_ms`.
    ///
    /// For each series matching the selector, the most recent sample with
    /// `ts_ns > sel_ts - lookback` and `ts_ns <= sel_ts` is used (`sel_ts`
    /// is `t_ms` shifted by the selector's `offset`, if any); series with no
    /// such sample are omitted. Ties at the same timestamp resolve to the
    /// sample stored last (see [`SeriesData`](crate::source::SeriesData)).
    pub fn instant(
        &self,
        source: &dyn SeriesSource,
        query: &str,
        t_ms: i64,
    ) -> Result<InstantVector, Error> {
        let vs = parse_vector_selector(query)?;
        let selector_matchers = build_matchers(&vs)?;
        let offset_ns = signed_offset_ns(vs.offset.as_ref())?;

        let t_ns = ms_to_ns(t_ms)?;
        let sel_ts_ns = t_ns.checked_sub(offset_ns).ok_or(Error::TimeOverflow)?;
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
                    ts_ns: t_ns,
                    value,
                });
            }
        }
        Ok(out)
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

        let vs = parse_vector_selector(query)?;
        let selector_matchers = build_matchers(&vs)?;
        let offset_ns = signed_offset_ns(vs.offset.as_ref())?;

        // Evaluation grid: (reported ts, offset-shifted lookup ts). `points`
        // is now known to be within the budget, so this is a single bounded
        // allocation.
        let capacity = usize::try_from(points).unwrap_or(self.max_range_points);
        let mut grid: Vec<(i64, i64)> = Vec::with_capacity(capacity);
        let mut t = start_ns;
        while t <= end_ns {
            let sel_ts = t.checked_sub(offset_ns).ok_or(Error::TimeOverflow)?;
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
                        value,
                    });
                }
            }
            if !samples.is_empty() {
                out.push((s.labels, samples));
            }
        }
        Ok(out)
    }
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

/// Pick the most recent sample in `(sel_ts - lookback, sel_ts]`. `samples`
/// must be sorted ascending by `ts_ns` (the `SeriesSource` contract); among
/// duplicate timestamps, the one later in the vec wins, which falls out of
/// `partition_point` for free given that ordering.
///
/// If the selected sample is a Prometheus staleness marker
/// ([`STALE_NAN_BITS`]) the series is absent from that sample forward, so
/// this returns `None`. A later real sample supersedes the marker and is
/// picked normally; markers older than the selected sample are irrelevant.
fn pick_sample(samples: &[Sample], sel_ts_ns: i64, lookback_delta_ns: i64) -> Option<f64> {
    let idx = samples.partition_point(|s| s.ts_ns <= sel_ts_ns);
    if idx == 0 {
        return None;
    }
    let candidate = &samples[idx - 1];
    let window_start = sel_ts_ns.checked_sub(lookback_delta_ns)?;
    if candidate.ts_ns <= window_start {
        return None;
    }
    if candidate.value.to_bits() == STALE_NAN_BITS {
        return None;
    }
    Some(candidate.value)
}

/// Parse `query` and reject every AST node except a bare vector selector
/// (optionally with `offset`), and reject the selector itself if it carries
/// an `@` modifier (not yet supported).
fn parse_vector_selector(query: &str) -> Result<promql_parser::parser::VectorSelector, Error> {
    let expr = promql_parser::parser::parse(query).map_err(Error::Parse)?;
    match expr {
        promql_parser::parser::Expr::VectorSelector(vs) => {
            if vs.at.is_some() {
                return Err(Error::Unsupported {
                    construct: "@".to_string(),
                });
            }
            Ok(vs)
        }
        promql_parser::parser::Expr::Aggregate(a) => Err(Error::Unsupported {
            construct: format!("aggregation: {}", a.op),
        }),
        promql_parser::parser::Expr::Unary(_) => Err(Error::Unsupported {
            construct: "unary expression".to_string(),
        }),
        promql_parser::parser::Expr::Binary(b) => Err(Error::Unsupported {
            construct: format!("binary expression: {}", b.op),
        }),
        promql_parser::parser::Expr::Paren(_) => Err(Error::Unsupported {
            construct: "paren expression".to_string(),
        }),
        promql_parser::parser::Expr::Subquery(_) => Err(Error::Unsupported {
            construct: "subquery".to_string(),
        }),
        promql_parser::parser::Expr::NumberLiteral(_) => Err(Error::Unsupported {
            construct: "number literal".to_string(),
        }),
        promql_parser::parser::Expr::StringLiteral(_) => Err(Error::Unsupported {
            construct: "string literal".to_string(),
        }),
        promql_parser::parser::Expr::MatrixSelector(_) => Err(Error::Unsupported {
            construct: "matrix selector".to_string(),
        }),
        promql_parser::parser::Expr::Call(c) => Err(Error::Unsupported {
            construct: format!("function call: {}", c.func.name),
        }),
        promql_parser::parser::Expr::Extension(_) => Err(Error::Unsupported {
            construct: "extension node".to_string(),
        }),
    }
}

/// Build the full matcher list for a vector selector, including the
/// implicit `__name__` matcher when the selector has a bare metric name
/// (promql-parser keeps that separate from `vs.matchers`).
fn build_matchers(vs: &promql_parser::parser::VectorSelector) -> Result<Vec<LabelMatcher>, Error> {
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
fn signed_offset_ns(offset: Option<&promql_parser::parser::Offset>) -> Result<i64, Error> {
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
    fn duplicate_timestamp_last_sample_in_vec_wins() {
        let ts_ns = ms_to_ns(minutes(1)).expect("no overflow");
        let source = TestSource::new()
            .with_series(&[("__name__", "up")], &[(ts_ns, 1.0), (ts_ns, 2.0)])
            .expect("valid series");
        let result = Evaluator::new()
            .instant(&source, "up", minutes(1))
            .expect("evaluates");
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].value, 2.0);
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
            ("up @ 100", "@"),
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
    fn paren_and_matrix_selector_are_also_unsupported() {
        for query in ["(up)", "up[5m]"] {
            let err = Evaluator::new()
                .instant(&TestSource::new(), query, 0)
                .expect_err("must be rejected");
            assert!(matches!(err, Error::Unsupported { .. }));
        }
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
}
