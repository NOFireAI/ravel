//! `ravel_query_promql`: one PromQL evaluation, instant or range (ADR-1374
//! D2 and D4 "Time").
//!
//! Two modes, chosen by the arguments:
//!
//! - `evaluation_time` alone: an instant evaluation at that one instant,
//!   through [`QueryBackend::promql_instant`].
//! - `time_range` with `step`: a range evaluation through
//!   [`QueryBackend::promql_range`]. The range is half-open like every other
//!   D4 `time_range`: the steps are `start + k * step` for every `k >= 0`
//!   with `start + k * step < end`, so `end` itself is never evaluated.
//!
//! Both modes, or neither, is `invalid_argument`, and so is a range without
//! its step or a step without its range. Every refusal answers before the
//! backend is called.
//!
//! The engine evaluates at millisecond resolution, so an instant, a bound or
//! a step that is not a whole millisecond is refused as `invalid_argument`
//! rather than rounded: rounding would evaluate an instant or a window the
//! caller did not write.
//!
//! The port returns one row per `(series, step)` before any cap. This body
//! groups those rows into one row per series, holding the series' labels and
//! its samples, and caps the series count at `max_rows`. A capped result is
//! `ok_bounded`: a PromQL result has no order a cursor could resume, and the
//! ADR gives this tool no cursor.

use ravel_query::RequestBudgets;
use ravel_query::http::{InstantRequest, RangeRequest};
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::catalog::QueryPromqlInput;
use crate::envelope::{
    Cell, Column, Data, Envelope, Failure, FailureClass, NextStep, Row, TimeRange,
};
use crate::service::QueryBackend;

use super::ToolContext;
use super::capabilities::refused_with;

const TOOL: &str = "ravel_query_promql";

/// The PromQL lookback: an instant selector evaluated at `t` reads the
/// latest sample in `(t - LOOKBACK_NS, t]`. The server's evaluator owns the
/// figure (`ravel_promql::DEFAULT_LOOKBACK_NS`); this copy is what the scope
/// block reports, and the server's MCP tests pin the two equal.
pub const LOOKBACK_NS: i64 = 5 * 60 * 1_000_000_000;

const NS_PER_MS: i64 = 1_000_000;

/// The column names of the port's per-sample rows, in order.
const SAMPLE_COLUMNS: [&str; 4] = ["series", "ts_ns", "value", "histogram"];

/// Which evaluation a call asked for, in nanoseconds, every figure a whole
/// millisecond.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Instant { at_ns: i64 },
    Range { start_ns: i64, end_ns: i64, step_ns: i64 },
}

impl Mode {
    /// The last step a range evaluates: the greatest `start + k * step`
    /// below `end`.
    fn last_step_ns(start_ns: i64, end_ns: i64, step_ns: i64) -> i64 {
        start_ns + (end_ns - 1 - start_ns) / step_ns * step_ns
    }
}

#[derive(Debug, Clone)]
struct Plan {
    query: String,
    mode: Mode,
    allow_partial: bool,
}

type Refusal = (Failure, Vec<NextStep>);

/// Run the tool. Every argument refusal answers before `backend` is called.
pub async fn run<B: QueryBackend>(args: Value, ctx: &ToolContext<'_>, backend: &B) -> Envelope {
    let plan = match plan(args) {
        Ok(plan) => plan,
        Err((failure, next_steps)) => return refused_with(failure, next_steps, ctx),
    };
    let query = &ctx.budgets.query;
    let budgets = Some(RequestBudgets {
        max_bytes_scanned: Some(query.max_bytes_scanned),
        max_store_requests: Some(query.max_store_requests),
        max_segments: Some(query.max_segments),
    });
    let answered = match plan.mode {
        Mode::Instant { at_ns } => {
            let request = InstantRequest {
                query: plan.query.clone(),
                time_ms: at_ns / NS_PER_MS,
                min_tokens: Vec::new(),
                deadline: ctx.budgets.deadline,
                allow_partial: plan.allow_partial,
                now_ns: ctx.now_ns,
                budgets,
            };
            backend.promql_instant(ctx.tenant_hash, &request).await
        }
        Mode::Range {
            start_ns,
            end_ns,
            step_ns,
        } => {
            // The engine's range is closed, so the half-open end becomes the
            // millisecond before it. Both bounds are whole milliseconds and
            // the end is after the start, so that is never before the start.
            let request = RangeRequest {
                query: plan.query.clone(),
                start_ms: start_ns / NS_PER_MS,
                end_ms: end_ns / NS_PER_MS - 1,
                step_ms: step_ns / NS_PER_MS,
                min_tokens: Vec::new(),
                deadline: ctx.budgets.deadline,
                allow_partial: plan.allow_partial,
                now_ns: ctx.now_ns,
                budgets,
            };
            backend.promql_range(ctx.tenant_hash, &request).await
        }
    };
    match answered.and_then(|envelope| finish(envelope, &plan, ctx)) {
        Ok(envelope) => envelope,
        Err(failure) => backend_refusal(failure, &plan, ctx),
    }
}

fn plan(args: Value) -> Result<Plan, Refusal> {
    if args.get("query").is_none_or(Value::is_null) {
        return Err(refusal(
            FailureClass::MissingArgument,
            format!("{TOOL} requires query"),
        ));
    }
    let input = QueryPromqlInput::deserialize(args).map_err(|err| {
        refusal(
            FailureClass::InvalidArgument,
            format!("{TOOL} arguments: {err}"),
        )
    })?;
    if input.evidence_ref.is_some() {
        return Err(refusal(
            FailureClass::Unsupported,
            format!("evidence_ref is not served by {TOOL} in this build"),
        ));
    }
    if input.query.trim().is_empty() {
        return Err(refusal(
            FailureClass::InvalidArgument,
            "query must not be empty".to_string(),
        ));
    }
    let mode = match (&input.evaluation_time, &input.time_range, &input.step) {
        (Some(at), None, None) => Mode::Instant {
            at_ns: whole_ms("evaluation_time", crate::time::to_ns("evaluation_time", at))?,
        },
        (None, Some(range), Some(step)) => range_mode(range, step)?,
        (None, None, None) => {
            return Err(mode_refusal("neither an instant nor a range was given"));
        }
        (None, Some(_), None) => {
            return Err(mode_refusal("time_range was given without step"));
        }
        (None, None, Some(_)) => {
            return Err(mode_refusal("step was given without time_range"));
        }
        (Some(_), _, _) => {
            return Err(mode_refusal(
                "evaluation_time was given together with time_range or step",
            ));
        }
    };
    Ok(Plan {
        query: input.query,
        mode,
        allow_partial: input.allow_partial_coverage,
    })
}

fn range_mode(range: &TimeRange, step: &str) -> Result<Mode, Refusal> {
    let start_ns = whole_ms(
        "time_range.start_ns",
        crate::time::to_ns("time_range.start_ns", &range.start_ns),
    )?;
    let end_ns = whole_ms(
        "time_range.end_ns",
        crate::time::to_ns("time_range.end_ns", &range.end_ns),
    )?;
    if end_ns <= start_ns {
        return Err(refusal(
            FailureClass::InvalidArgument,
            "time_range.end_ns must be after time_range.start_ns: the range is half-open, \
             so an empty one has no step to evaluate"
                .to_string(),
        ));
    }
    let step_ns = whole_ms("step", step_ns(step))?;
    Ok(Mode::Range {
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
fn whole_ms(name: &str, ns: Result<i64, Failure>) -> Result<i64, Refusal> {
    let ns = ns.map_err(|failure| (failure, Vec::new()))?;
    if ns.rem_euclid(NS_PER_MS) != 0 {
        return Err(refusal(
            FailureClass::InvalidArgument,
            format!(
                "{name} is {ns} ns, not a whole millisecond: PromQL evaluates at millisecond \
                 resolution, and rounding would evaluate a time the call did not name"
            ),
        ));
    }
    Ok(ns)
}

fn mode_refusal(what: &str) -> Refusal {
    (
        invalid(format!(
            "{TOOL} takes exactly one mode, and {what}: evaluation_time alone for an \
             instant evaluation, or time_range with step for a range evaluation"
        )),
        vec![NextStep {
            action: TOOL.to_string(),
            detail: "send evaluation_time alone for one instant, or time_range and step \
                     (a duration such as 30s) for a range"
                .to_string(),
        }],
    )
}

/// Group, then cap, then fit: the order that makes `max_rows` a series count.
fn finish(mut envelope: Envelope, plan: &Plan, ctx: &ToolContext<'_>) -> Result<Envelope, Failure> {
    if envelope.coverage.partial && !plan.allow_partial {
        return Err(Failure {
            class: FailureClass::Unavailable,
            message: "partial results: coverage is incomplete; set allow_partial_coverage: \
                      true to receive partial results"
                .to_string(),
            counter: None,
        });
    }
    report_scope(&mut envelope, plan.mode);

    let names: Vec<&str> = envelope
        .data
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    if names == SAMPLE_COLUMNS {
        envelope.data = series_data(std::mem::take(&mut envelope.data.rows))?;
    }

    let produced = envelope.data.rows.len();
    let max_rows = ctx.budgets.max_rows as usize;
    let row_cap_omitted = produced.saturating_sub(max_rows) as u64;
    envelope.data.rows.truncate(max_rows);
    envelope.presentation.max_rows = ctx.budgets.max_rows;

    let mut fitted = envelope.fit(ctx.budgets.max_response_bytes);
    fitted.presentation.rows_omitted += row_cap_omitted;
    fitted.presentation.row_cap_hit |= row_cap_omitted > 0;
    fitted.data.row_count = produced as u64;
    Ok(fitted.finish(false))
}

/// State what was evaluated: the instant or the half-open range in
/// nanosecond strings, the step alignment, and the lookback.
fn report_scope(envelope: &mut Envelope, mode: Mode) {
    let predicates = &mut envelope.scope.predicates_applied;
    match mode {
        Mode::Instant { at_ns } => {
            envelope.scope.time_range = Some(TimeRange {
                start_ns: at_ns.to_string(),
                end_ns: at_ns.to_string(),
            });
            predicates.push(format!("instant evaluation at {at_ns} ns"));
        }
        Mode::Range {
            start_ns,
            end_ns,
            step_ns,
        } => {
            envelope.scope.time_range = Some(TimeRange {
                start_ns: start_ns.to_string(),
                end_ns: end_ns.to_string(),
            });
            let last_ns = Mode::last_step_ns(start_ns, end_ns, step_ns);
            predicates.push(format!(
                "steps aligned to time_range.start_ns: {start_ns} + k * {step_ns} ns, every \
                 step before {end_ns} ns, the last at {last_ns} ns"
            ));
        }
    }
    predicates.push(format!(
        "lookback {LOOKBACK_NS} ns (5m): an instant selector at step t reads the latest \
         sample in (t - 5m, t]"
    ));
}

/// One row per series from the port's one row per `(series, step)`. The
/// port emits each series' steps consecutively, so a series boundary is a
/// change in the series cell.
fn series_data(rows: Vec<Row>) -> Result<Data, Failure> {
    let mut grouped: Vec<(Cell, Series)> = Vec::new();
    let mut current: Option<(Cell, Series)> = None;
    for row in rows {
        let [series, ts, value, histogram]: [Cell; 4] = row.try_into().map_err(|row: Row| {
            internal(format!(
                "the PromQL port returned a row of {} cells, not 4",
                row.len()
            ))
        })?;
        let Cell::Timestamp(ts_ns) = ts else {
            return Err(internal(format!(
                "the PromQL port returned a ts_ns cell that is not a timestamp: {ts:?}"
            )));
        };
        if current.as_ref().is_some_and(|(last, _)| *last != series) {
            grouped.extend(current.take());
        }
        let (_, samples) = current.get_or_insert_with(|| (series, Series::default()));
        samples.ts_ns.push(Value::String(ts_ns.to_string()));
        samples.values.push(cell_json(&value)?);
        samples.any_histogram |= histogram != Cell::Null;
        samples.histograms.push(cell_json(&histogram)?);
    }
    grouped.extend(current);
    let rows = grouped
        .into_iter()
        .map(|(series, samples)| vec![series, Cell::Map(samples.into_map())])
        .collect();
    Ok(Data {
        columns: vec![column("series"), column("samples")],
        row_count: 0,
        rows,
    })
}

/// One series' samples as parallel arrays. `histogram` is carried only when
/// a step of the series is a native histogram; such a step's `value` is
/// `null`, as the port reports it.
#[derive(Default)]
struct Series {
    ts_ns: Vec<Value>,
    values: Vec<Value>,
    histograms: Vec<Value>,
    any_histogram: bool,
}

impl Series {
    fn into_map(self) -> Map<String, Value> {
        let mut map = Map::new();
        map.insert("ts_ns".to_string(), Value::Array(self.ts_ns));
        map.insert("value".to_string(), Value::Array(self.values));
        if self.any_histogram {
            map.insert("histogram".to_string(), Value::Array(self.histograms));
        }
        map
    }
}

/// A cell as the envelope serializes it, so a sample value follows the D4
/// precision rule (`NaN`, `+Inf`, `-Inf` as strings, the sign of zero kept).
fn cell_json(cell: &Cell) -> Result<Value, Failure> {
    serde_json::to_value(cell)
        .map_err(|err| internal(format!("a PromQL sample did not serialize: {err}")))
}

fn column(name: &str) -> Column {
    Column {
        name: name.to_string(),
        r#type: "json".to_string(),
    }
}

/// A failure from the backend, or from this body's own partial gate. A
/// partial-coverage refusal is rewritten to name this tool's consent
/// argument rather than the HTTP route's, and says how to consent.
fn backend_refusal(mut failure: Failure, plan: &Plan, ctx: &ToolContext<'_>) -> Envelope {
    let mut next_steps = Vec::new();
    if failure.class == FailureClass::Unavailable
        && failure.message.starts_with("partial results:")
        && !plan.allow_partial
    {
        failure.message = failure
            .message
            .replace("allow_partial=true", "allow_partial_coverage: true");
        next_steps.push(NextStep {
            action: TOOL.to_string(),
            detail: "resend with allow_partial_coverage: true to receive the partial result, \
                     reported with coverage.partial: true"
                .to_string(),
        });
    }
    refused_with(failure, next_steps, ctx)
}

fn invalid(message: String) -> Failure {
    Failure {
        class: FailureClass::InvalidArgument,
        message,
        counter: None,
    }
}

fn internal(message: String) -> Failure {
    Failure {
        class: FailureClass::Internal,
        message,
        counter: None,
    }
}

fn refusal(class: FailureClass, message: String) -> Refusal {
    (
        Failure {
            class,
            message,
            counter: None,
        },
        Vec::new(),
    )
}
