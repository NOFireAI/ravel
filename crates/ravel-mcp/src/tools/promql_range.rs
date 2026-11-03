//! The PromQL range arguments `ravel_query_promql` and
//! `ravel_analyze_timeseries` share (ADR-1374 D4 "Time", the query-promql
//! amendment items 2 to 4 and 10).
//!
//! A range is a half-open `time_range` and a `step`. Both bounds and the
//! step must be whole milliseconds, the step a positive duration with a
//! unit, the end after the start, and the steps at most
//! [`MAX_RANGE_POINTS`]. Each tool calls [`eval_range`] before its backend
//! call, so both refuse the same inputs with the same messages.

use crate::envelope::{Failure, FailureClass, NextStep, TimeRange};

/// The most steps one range evaluation may have. The evaluator owns the
/// figure (`ravel_promql::DEFAULT_MAX_RANGE_POINTS`) and refuses past it as
/// `unsupported` with no next step; this crate does not depend on the
/// evaluator, so this copy refuses first as `invalid_argument` with a next
/// step, and the server's MCP tests pin the two equal.
pub const MAX_RANGE_POINTS: usize = 11_000;

pub(crate) const NS_PER_MS: i64 = 1_000_000;

/// A refusal before the backend call: the failure and its next steps.
pub(crate) type Refusal = (Failure, Vec<NextStep>);

/// A validated range in nanoseconds, every figure a whole millisecond and
/// `end_ns` after `start_ns`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct EvalRange {
    pub start_ns: i64,
    pub end_ns: i64,
    pub step_ns: i64,
}

impl EvalRange {
    /// The last step the range evaluates: the greatest `start + k * step`
    /// below `end`.
    pub fn last_step_ns(&self) -> i64 {
        self.start_ns + (self.end_ns - 1 - self.start_ns) / self.step_ns * self.step_ns
    }

    pub fn start_ms(&self) -> i64 {
        self.start_ns / NS_PER_MS
    }

    /// The engine's range is closed, so the half-open end becomes the
    /// millisecond before it. Both bounds are whole milliseconds and the end
    /// is after the start, so that is never before the start.
    pub fn closed_end_ms(&self) -> i64 {
        self.end_ns / NS_PER_MS - 1
    }

    pub fn step_ms(&self) -> i64 {
        self.step_ns / NS_PER_MS
    }
}

/// Validate a `time_range` and `step` for `tool`, whose name the step-limit
/// next step carries.
pub(crate) fn eval_range(tool: &str, range: &TimeRange, step: &str) -> Result<EvalRange, Refusal> {
    let start_ns = whole_ms(
        "time_range.start_ns",
        crate::time::to_ns("time_range.start_ns", &range.start_ns),
    )?;
    let end_ns = whole_ms(
        "time_range.end_ns",
        crate::time::to_ns("time_range.end_ns", &range.end_ns),
    )?;
    if end_ns <= start_ns {
        return Err((
            invalid(
                "time_range.end_ns must be after time_range.start_ns: the range is half-open, \
                 so an empty one has no step to evaluate"
                    .to_string(),
            ),
            Vec::new(),
        ));
    }
    let step_ns = whole_ms("step", step_ns(step))?;
    let span_ns = i128::from(end_ns) - i128::from(start_ns);
    let steps = (span_ns - 1) / i128::from(step_ns) + 1;
    if steps > MAX_RANGE_POINTS as i128 {
        // The smallest whole-millisecond step that brings the range under the
        // limit: ceil(span / limit), since the steps number ceil(span / step).
        let limit = MAX_RANGE_POINTS as i128;
        let min_step_ms = (span_ns / i128::from(NS_PER_MS) + limit - 1) / limit;
        return Err((
            invalid(format!(
                "time_range and step evaluate {steps} steps, over the {MAX_RANGE_POINTS} a \
                 range evaluation may have"
            )),
            vec![NextStep {
                action: tool.to_string(),
                detail: format!(
                    "widen step to at least {min_step_ms}ms, or narrow time_range, so the \
                     range evaluates at most {MAX_RANGE_POINTS} steps"
                ),
            }],
        ));
    }
    Ok(EvalRange {
        start_ns,
        end_ns,
        step_ns,
    })
}

/// A step is a positive duration with a unit (`15s`, `1m`, `1h30m`). A bare
/// number is refused: Prometheus reads it as seconds and every other D4 time
/// input as nanoseconds, so either reading would surprise someone.
fn step_ns(raw: &str) -> Result<i64, Failure> {
    let duration = humantime::parse_duration(raw).map_err(|err| {
        invalid(format!(
            "step {raw:?} is not a duration with a unit, such as 30s or 1m: {err}"
        ))
    })?;
    if duration.is_zero() {
        return Err(invalid(format!("step {raw:?} must be positive")));
    }
    i64::try_from(duration.as_nanos())
        .map_err(|_| invalid(format!("step {raw:?} does not fit an i64 nanosecond count")))
}

/// Refuse a figure that is not a whole millisecond, naming it in
/// nanoseconds so the caller sees exactly what was left over.
pub(crate) fn whole_ms(name: &str, ns: Result<i64, Failure>) -> Result<i64, Refusal> {
    let ns = ns.map_err(|failure| (failure, Vec::new()))?;
    if ns.rem_euclid(NS_PER_MS) != 0 {
        return Err((
            invalid(format!(
                "{name} is {ns} ns, not a whole millisecond: PromQL evaluates at millisecond \
                 resolution, and rounding would evaluate a time the call did not name"
            )),
            Vec::new(),
        ));
    }
    Ok(ns)
}

fn invalid(message: String) -> Failure {
    Failure {
        class: FailureClass::InvalidArgument,
        message,
        counter: None,
    }
}
