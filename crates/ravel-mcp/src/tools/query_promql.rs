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
//!
//! `max_rows` cannot bound one series' samples, so two rules do. A range of
//! more than [`MAX_RANGE_POINTS`] steps is `invalid_argument` before the
//! backend is called. And a series whose samples do not fit the byte cap
//! keeps a prefix of them as an object of parallel arrays of one length,
//! with `samples_omitted` counting the rest. The cut happens before
//! [`Envelope::fit`] runs, which would otherwise serialize the object to
//! text and cut that.

use ravel_query::RequestBudgets;
use ravel_query::http::{InstantRequest, RangeRequest};
use serde::Deserialize;
use serde_json::{Map, Value};

use crate::catalog::QueryPromqlInput;
use crate::envelope::{
    Cell, Column, Data, Envelope, Failure, FailureClass, NextStep, Row, TimeRange, fit_cap,
    serialized_len,
};
use crate::service::QueryBackend;

use super::ToolContext;
use super::capabilities::refused_with;
use super::promql_range::{EvalRange, NS_PER_MS, Refusal, eval_range, whole_ms};

const TOOL: &str = "ravel_query_promql";

/// The PromQL lookback: an instant selector evaluated at `t` reads the
/// latest sample in `(t - LOOKBACK_NS, t]`. The server's evaluator owns the
/// figure (`ravel_promql::DEFAULT_LOOKBACK_NS`); this copy is what the scope
/// block reports, and the server's MCP tests pin the two equal.
pub const LOOKBACK_NS: i64 = 5 * 60 * 1_000_000_000;

pub use super::promql_range::MAX_RANGE_POINTS;

/// Room left below the fit cap for the presentation counters `fit` and this
/// body set after the series are measured: four `u64` counters, zero when
/// measured and at most 20 digits each, and two flags one byte longer when
/// set.
const COUNTER_SLACK: usize = 128;

/// The column names of the port's per-sample rows, in order.
const SAMPLE_COLUMNS: [&str; 4] = ["series", "ts_ns", "value", "histogram"];

/// Which evaluation a call asked for, in nanoseconds, every figure a whole
/// millisecond.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    Instant { at_ns: i64 },
    Range(EvalRange),
}

#[derive(Debug, Clone)]
struct Plan {
    query: String,
    mode: Mode,
    allow_partial: bool,
}

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
        Mode::Range(range) => {
            let request = RangeRequest {
                query: plan.query.clone(),
                start_ms: range.start_ms(),
                end_ms: range.closed_end_ms(),
                step_ms: range.step_ms(),
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
        (None, Some(range), Some(step)) => Mode::Range(eval_range(TOOL, range, step)?),
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
    let grouped = names == SAMPLE_COLUMNS;
    if grouped {
        envelope.data = series_data(std::mem::take(&mut envelope.data.rows))?;
    }

    let produced = envelope.data.rows.len();
    let max_rows = ctx.budgets.max_rows as usize;
    let row_cap_omitted = produced.saturating_sub(max_rows) as u64;
    envelope.data.rows.truncate(max_rows);
    envelope.presentation.max_rows = ctx.budgets.max_rows;

    let cut = if grouped {
        fit_series(&mut envelope, ctx.budgets.max_response_bytes, plan.mode)?
    } else {
        SeriesCut::default()
    };
    let mut fitted = envelope.fit(ctx.budgets.max_response_bytes);
    fitted.presentation.rows_omitted += row_cap_omitted + cut.rows_omitted;
    fitted.presentation.cells_truncated += cut.samples_cut;
    fitted.presentation.bytes_cap_hit |= cut.rows_omitted > 0 || cut.samples_cut > 0;
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
            predicates.push(lookback_predicate());
        }
        Mode::Range(range) => report_range_scope(envelope, range),
    }
}

/// State a range evaluation's half-open range in nanosecond strings, its
/// step alignment, and the lookback. `ravel_analyze_timeseries` reports its
/// range with the same lines.
pub(crate) fn report_range_scope(envelope: &mut Envelope, range: EvalRange) {
    let EvalRange {
        start_ns,
        end_ns,
        step_ns,
    } = range;
    envelope.scope.time_range = Some(TimeRange {
        start_ns: start_ns.to_string(),
        end_ns: end_ns.to_string(),
    });
    let last_ns = range.last_step_ns();
    let predicates = &mut envelope.scope.predicates_applied;
    predicates.push(format!(
        "steps aligned to time_range.start_ns: {start_ns} + k * {step_ns} ns, every \
         step before {end_ns} ns, the last at {last_ns} ns"
    ));
    predicates.push(lookback_predicate());
}

fn lookback_predicate() -> String {
    format!(
        "lookback {LOOKBACK_NS} ns (5m): an instant selector at step t reads the latest \
         sample in (t - 5m, t]"
    )
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

/// What [`fit_series`] took to bring the series under the byte cap.
#[derive(Debug, Default)]
struct SeriesCut {
    /// Series dropped whole, after the one cut.
    rows_omitted: u64,
    /// Series kept with a prefix of their samples: zero or one.
    samples_cut: u64,
}

/// Fit the series rows under the byte cap before [`Envelope::fit`] sees
/// them, so a samples cell stays an object of parallel arrays: `fit` would
/// serialize an oversized one to text and cut that. Series that fit whole
/// are kept whole, in order. The first that does not is kept with the
/// longest prefix of its samples that fits, every array cut to the same
/// length and `samples_omitted` counting the samples left out; it is dropped
/// instead when no sample of it fits and a series before it was kept. Every
/// series after it is dropped.
fn fit_series(
    envelope: &mut Envelope,
    requested_max_response_bytes: u64,
    mode: Mode,
) -> Result<SeriesCut, Failure> {
    let cap = fit_cap(requested_max_response_bytes).saturating_sub(COUNTER_SLACK);
    if serialized_len(envelope) <= cap {
        return Ok(SeriesCut::default());
    }
    envelope.next_steps.push(bytes_next_step(mode));
    let mut rows = std::mem::take(&mut envelope.data.rows).into_iter();
    let mut used = serialized_len(envelope);
    let mut cut = SeriesCut::default();
    for row in rows.by_ref() {
        let separator = usize::from(!envelope.data.rows.is_empty());
        let room = cap.saturating_sub(used + separator);
        let len = row_len(&row);
        if len <= room {
            used += separator + len;
            envelope.data.rows.push(row);
            continue;
        }
        let (prefix, kept) = samples_prefix(row, room)?;
        if kept > 0 || envelope.data.rows.is_empty() {
            envelope.data.rows.push(prefix);
            cut.samples_cut = 1;
        } else {
            cut.rows_omitted = 1;
        }
        break;
    }
    cut.rows_omitted += rows.count() as u64;
    if cut.rows_omitted == 0 && cut.samples_cut == 0 {
        envelope.next_steps.pop();
    }
    Ok(cut)
}

fn bytes_next_step(mode: Mode) -> NextStep {
    let detail = match mode {
        Mode::Range(_) => {
            "the series did not fit max_response_bytes: widen step or narrow time_range for \
             fewer samples per series, or narrow the query for fewer series"
        }
        Mode::Instant { .. } => {
            "the series did not fit max_response_bytes: narrow the query for fewer series"
        }
    };
    NextStep {
        action: TOOL.to_string(),
        detail: detail.to_string(),
    }
}

fn row_len(row: &Row) -> usize {
    serde_json::to_string(row)
        .map(|text| text.len())
        .unwrap_or(usize::MAX)
}

/// The series row with the longest prefix of its samples whose serialized
/// row fits `room`, and how many samples that prefix holds. Every array is
/// cut to the same length and `samples_omitted` counts the rest, as a
/// string like every other D4 integer. A row's length only grows with the
/// prefix (each sample adds a `ts_ns` string of at least 3 bytes, and the
/// count loses at most one digit), so a binary search finds it.
fn samples_prefix(row: Row, room: usize) -> Result<(Row, usize), Failure> {
    let [series, samples]: [Cell; 2] = row
        .try_into()
        .map_err(|_| internal("a series row is not a label set and samples".to_string()))?;
    let Cell::Map(samples) = samples else {
        return Err(internal("a series' samples are not an object".to_string()));
    };
    let arrays: Vec<(String, Vec<Value>)> = samples
        .into_iter()
        .map(|(name, value)| match value {
            Value::Array(items) => Ok((name, items)),
            other => Err(internal(format!(
                "a series' {name} is not an array: {other}"
            ))),
        })
        .collect::<Result<_, _>>()?;
    let total = arrays
        .iter()
        .map(|(_, items)| items.len())
        .min()
        .unwrap_or(0);
    let build = |kept: usize| -> Row {
        let mut map = Map::new();
        for (name, items) in &arrays {
            map.insert(
                name.clone(),
                Value::Array(items.iter().take(kept).cloned().collect()),
            );
        }
        map.insert(
            "samples_omitted".to_string(),
            Value::String((total - kept).to_string()),
        );
        vec![series.clone(), Cell::Map(map)]
    };
    // The largest prefix shorter than the whole series that fits, or none.
    let (mut fits, mut over) = (0usize, total);
    while over - fits > 1 {
        let mid = fits + (over - fits) / 2;
        if row_len(&build(mid)) <= room {
            fits = mid;
        } else {
            over = mid;
        }
    }
    Ok((build(fits), fits))
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

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::cursor::{CURSOR_KEY_LEN, CursorKey};
    use crate::envelope::Status;
    use crate::tools::tests::{Stub, block_on, test_context};

    /// 1000 s and 1060 s after the epoch, in nanoseconds.
    const START_NS: i64 = 1_000_000_000_000;
    const END_NS: i64 = 1_060_000_000_000;

    fn key() -> CursorKey {
        CursorKey::from_process_secret([0x7au8; CURSOR_KEY_LEN])
    }

    fn range_args(start_ns: i64, end_ns: i64, step: &str) -> Value {
        json!({
            "query": "m",
            "time_range": {"start_ns": start_ns.to_string(), "end_ns": end_ns.to_string()},
            "step": step,
        })
    }

    fn labels(name: &str) -> Cell {
        let mut map = Map::new();
        map.insert("__name__".to_string(), json!(name));
        Cell::Map(map)
    }

    /// What the port returns for a vector or a matrix: one row per
    /// `(series, step)`, each series' steps consecutive.
    fn sample_rows(rows: &[(&str, i64, f64)]) -> Envelope {
        let mut envelope = Envelope::default();
        envelope.data.columns = SAMPLE_COLUMNS
            .iter()
            .map(|name| Column {
                name: (*name).to_string(),
                r#type: "json".to_string(),
            })
            .collect();
        envelope.data.rows = rows
            .iter()
            .map(|&(series, ts_ns, value)| {
                vec![
                    labels(series),
                    Cell::Timestamp(ts_ns),
                    Cell::Float(value),
                    Cell::Null,
                ]
            })
            .collect();
        envelope.data.row_count = rows.len() as u64;
        envelope.coverage.complete = true;
        envelope
    }

    fn answering(answers: Vec<Result<Envelope, Failure>>) -> Stub {
        let stub = Stub::default();
        stub.promql.borrow_mut().extend(answers);
        stub
    }

    fn failure_of(envelope: &Envelope) -> &Failure {
        assert_eq!(envelope.status, Status::Error, "{envelope:?}");
        envelope.failure.as_ref().expect("an error carries one")
    }

    /// Both modes, neither, and each half of the range pair alone are
    /// `invalid_argument`, decided before the backend runs: the default
    /// `Stub` panics on either PromQL method.
    ///
    /// FLIP: validating the mode after the backend call (or letting the
    /// engine refuse it) panics the stub on the first case.
    #[test]
    fn both_modes_or_neither_is_refused_before_the_backend() {
        let key = key();
        let ctx = test_context(&key);
        let stub = Stub::default();
        let range = json!({"start_ns": START_NS.to_string(), "end_ns": END_NS.to_string()});
        for (what, args) in [
            ("neither", json!({"query": "m"})),
            (
                "both",
                json!({"query": "m", "evaluation_time": "1000000000000",
                       "time_range": range, "step": "30s"}),
            ),
            (
                "instant with a step",
                json!({"query": "m", "evaluation_time": "1000000000000", "step": "30s"}),
            ),
            (
                "instant with a range",
                json!({"query": "m", "evaluation_time": "1000000000000", "time_range": range}),
            ),
            (
                "range without step",
                json!({"query": "m", "time_range": range}),
            ),
            ("step without range", json!({"query": "m", "step": "30s"})),
        ] {
            let envelope = block_on(run(args, &ctx, &stub));
            let failure = failure_of(&envelope);
            assert_eq!(failure.class, FailureClass::InvalidArgument, "{what}");
            assert!(
                failure
                    .message
                    .starts_with("ravel_query_promql takes exactly one mode"),
                "{what}: {}",
                failure.message
            );
            assert_eq!(envelope.next_steps.len(), 1, "{what}");
        }
    }

    /// A step that does not parse, is not positive, or has no unit, and a
    /// range whose end is not after its start, are `invalid_argument` before
    /// the backend runs.
    ///
    /// FLIP: handing the step to the engine unchecked (which refuses a
    /// non-positive step itself) panics the stub; so does a range check of
    /// `start > end`, which lets the empty range `start == end` through.
    #[test]
    fn a_bad_step_or_an_empty_range_is_refused_before_the_backend() {
        let key = key();
        let ctx = test_context(&key);
        let stub = Stub::default();
        for step in ["0s", "30", "-1s", "soon", ""] {
            let envelope = block_on(run(range_args(START_NS, END_NS, step), &ctx, &stub));
            let failure = failure_of(&envelope);
            assert_eq!(failure.class, FailureClass::InvalidArgument, "{step:?}");
            assert!(failure.message.starts_with("step "), "{}", failure.message);
        }
        for (start, end) in [(START_NS, START_NS), (END_NS, START_NS)] {
            let envelope = block_on(run(range_args(start, end, "30s"), &ctx, &stub));
            let failure = failure_of(&envelope);
            assert_eq!(failure.class, FailureClass::InvalidArgument);
            assert!(
                failure
                    .message
                    .starts_with("time_range.end_ns must be after time_range.start_ns"),
                "{}",
                failure.message
            );
        }
    }

    /// An instant, a bound or a step that is not a whole millisecond is
    /// refused as `invalid_argument` naming the figure, before the backend.
    ///
    /// FLIP: silently truncating to the millisecond (`ns / 1_000_000`)
    /// reaches the backend, and the panicking stub fails the test.
    #[test]
    fn sub_millisecond_inputs_are_refused_not_truncated() {
        let key = key();
        let ctx = test_context(&key);
        let stub = Stub::default();
        for (name, args) in [
            (
                "evaluation_time",
                json!({"query": "m", "evaluation_time": "1000000000000500"}),
            ),
            (
                "evaluation_time",
                json!({"query": "m", "evaluation_time": "2026-09-12T10:00:00.0000005Z"}),
            ),
            (
                "time_range.start_ns",
                range_args(START_NS + 1, END_NS, "30s"),
            ),
            ("time_range.end_ns", range_args(START_NS, END_NS - 1, "30s")),
            ("step", range_args(START_NS, END_NS, "1500us")),
        ] {
            let envelope = block_on(run(args, &ctx, &stub));
            let failure = failure_of(&envelope);
            assert_eq!(failure.class, FailureClass::InvalidArgument, "{name}");
            assert!(
                failure.message.starts_with(&format!("{name} is ")),
                "{name}: {}",
                failure.message
            );
            assert!(
                failure.message.contains("not a whole millisecond"),
                "{}",
                failure.message
            );
        }
    }

    /// An instant call carries the instant in milliseconds, the call's
    /// budgets and deadline, and no consent; the scope reports the instant
    /// and the lookback in nanoseconds.
    ///
    /// FLIP: passing seconds or nanoseconds as `time_ms`, or leaving
    /// `budgets` as `None`, fails the request assertions; dropping the
    /// lookback entry fails the scope assertion.
    #[test]
    fn an_instant_call_carries_milliseconds_budgets_and_its_scope() {
        let key = key();
        let ctx = test_context(&key);
        let stub = answering(vec![Ok(sample_rows(&[(
            "m",
            1_789_207_200_123_000_000,
            4.5,
        )]))]);

        let envelope = block_on(run(
            json!({"query": "m", "evaluation_time": "2026-09-12T10:00:00.123Z"}),
            &ctx,
            &stub,
        ));

        assert_eq!(envelope.status, Status::Ok, "{envelope:?}");
        let request = stub.instants.borrow()[0].clone();
        assert_eq!(request.time_ms, 1_789_207_200_123);
        assert_eq!(request.query, "m");
        assert!(!request.allow_partial);
        assert_eq!(request.deadline, ctx.budgets.deadline);
        assert_eq!(request.now_ns, ctx.now_ns);
        let budgets = request.budgets.expect("the call's budgets are forwarded");
        assert_eq!(
            budgets.max_bytes_scanned,
            Some(ctx.budgets.query.max_bytes_scanned)
        );
        assert_eq!(
            budgets.max_store_requests,
            Some(ctx.budgets.query.max_store_requests)
        );
        assert_eq!(budgets.max_segments, Some(ctx.budgets.query.max_segments));

        let value = serde_json::to_value(&envelope).expect("envelope serializes");
        assert_eq!(
            value["scope"]["time_range"],
            json!({"start_ns": "1789207200123000000", "end_ns": "1789207200123000000"})
        );
        assert_eq!(
            value["scope"]["predicates_applied"],
            json!([
                "instant evaluation at 1789207200123000000 ns",
                "lookback 300000000000 ns (5m): an instant selector at step t reads the \
                 latest sample in (t - 5m, t]"
            ])
        );
        assert_eq!(
            value["data"]["rows"],
            json!([[{"__name__": "m"},
                    {"ts_ns": ["1789207200123000000"], "value": [4.5]}]])
        );
    }

    /// A range call sends the half-open range as the closed millisecond
    /// range the engine takes, and the scope states the caller's range, the
    /// step and the last step evaluated.
    ///
    /// FLIP: sending `end_ms` as the caller's end evaluates a step at `end`,
    /// outside the half-open range; computing the last step as
    /// `end - step` reports 1030 s for the 1060 s range below.
    #[test]
    fn a_range_call_is_half_open_and_reports_its_step_alignment() {
        let key = key();
        let ctx = test_context(&key);
        let stub = answering(vec![
            Ok(sample_rows(&[("m", START_NS, 1.0)])),
            Ok(sample_rows(&[("m", START_NS, 1.0)])),
        ]);

        let envelope = block_on(run(range_args(START_NS, END_NS, "30s"), &ctx, &stub));
        assert_eq!(envelope.status, Status::Ok, "{envelope:?}");
        let request = stub.ranges.borrow()[0].clone();
        assert_eq!(
            (request.start_ms, request.end_ms, request.step_ms),
            (1_000_000, 1_059_999, 30_000)
        );
        assert_eq!(
            envelope.scope.predicates_applied[0],
            "steps aligned to time_range.start_ns: 1000000000000 + k * 30000000000 ns, \
             every step before 1060000000000 ns, the last at 1030000000000 ns"
        );
        let range = envelope.scope.time_range.expect("a range scope");
        assert_eq!(
            (range.start_ns.as_str(), range.end_ns.as_str()),
            ("1000000000000", "1060000000000")
        );

        let envelope = block_on(run(
            range_args(START_NS, END_NS + 1_000_000_000, "1m"),
            &ctx,
            &stub,
        ));
        assert_eq!(stub.ranges.borrow()[1].end_ms, 1_060_999);
        assert!(
            envelope.scope.predicates_applied[0].ends_with("the last at 1060000000000 ns"),
            "{:?}",
            envelope.scope.predicates_applied
        );
    }

    /// Sample rows become one row per series, timestamps as nanosecond
    /// strings and values under the D4 precision rule: `NaN`, `+Inf` and
    /// `-Inf` as strings, and `-0.0` keeping its sign.
    ///
    /// FLIP: serializing values with `serde_json::json!(f64)` turns NaN and
    /// the infinities into `null`; a grouping that keyed on the first series
    /// only folds `b` into `a`.
    #[test]
    fn series_rows_group_samples_and_keep_float_precision() {
        let key = key();
        let ctx = test_context(&key);
        let stub = answering(vec![Ok(sample_rows(&[
            ("a", START_NS, f64::NAN),
            ("a", START_NS + 30_000_000_000, -0.0),
            ("b", START_NS, f64::INFINITY),
            ("b", START_NS + 30_000_000_000, f64::NEG_INFINITY),
        ]))]);

        let envelope = block_on(run(range_args(START_NS, END_NS, "30s"), &ctx, &stub));

        assert_eq!(envelope.status, Status::Ok, "{envelope:?}");
        assert_eq!(envelope.data.row_count, 2);
        let text = serde_json::to_string(&envelope.data).expect("data serializes");
        assert!(
            text.contains(
                r#"[{"__name__":"a"},{"ts_ns":["1000000000000","1030000000000"],"value":["NaN",-0.0]}]"#
            ),
            "{text}"
        );
        assert!(
            text.contains(
                r#"[{"__name__":"b"},{"ts_ns":["1000000000000","1030000000000"],"value":["+Inf","-Inf"]}]"#
            ),
            "{text}"
        );
        assert_eq!(
            envelope
                .data
                .columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>(),
            ["series", "samples"]
        );
    }

    /// `max_rows` caps series, not samples: three series of four steps with
    /// `max_rows: 2` answer the first two series whole, count the third as
    /// omitted, and report `ok_bounded` with no cursor.
    ///
    /// FLIP: capping the port's sample rows at 2 answers one series with two
    /// samples; reporting the capped result as a page (`finish(true)` with a
    /// cursor) answers `ok_page`.
    #[test]
    fn max_rows_caps_series_and_the_result_is_ok_bounded() {
        let key = key();
        let mut ctx = test_context(&key);
        ctx.budgets.max_rows = 2;
        let mut rows = Vec::new();
        for series in ["a", "b", "c"] {
            for k in 0..4 {
                rows.push((series, START_NS + k * 15_000_000_000, k as f64));
            }
        }
        let stub = answering(vec![Ok(sample_rows(&rows))]);

        let envelope = block_on(run(range_args(START_NS, END_NS, "15s"), &ctx, &stub));

        assert_eq!(envelope.status, Status::OkBounded, "{envelope:?}");
        assert!(envelope.presentation.cursor.is_none());
        assert!(envelope.presentation.row_cap_hit);
        assert_eq!(envelope.presentation.max_rows, 2);
        assert_eq!(envelope.presentation.rows_omitted, 1);
        assert_eq!(envelope.data.row_count, 3);
        assert_eq!(envelope.data.rows.len(), 2);
        for (row, name) in envelope.data.rows.iter().zip(["a", "b"]) {
            assert_eq!(row[0], labels(name));
            let Cell::Map(samples) = &row[1] else {
                panic!("samples is a map: {row:?}");
            };
            assert_eq!(samples["value"], json!([0.0, 1.0, 2.0, 3.0]));
        }
    }

    /// A range one step over [`MAX_RANGE_POINTS`] is `invalid_argument`
    /// before the backend runs, with a next step naming the smallest step
    /// that fits; a range of exactly the limit reaches the backend. The step
    /// count is the half-open one, so a range one millisecond past the limit
    /// at 1 s steps is over it.
    ///
    /// FLIP: refusing at `steps >= MAX_RANGE_POINTS` refuses the range at
    /// the limit; counting `span / step` without the partial last step lets
    /// the 1 ms-over range reach the panicking stub.
    #[test]
    fn a_range_over_the_step_limit_is_refused_before_the_backend() {
        let key = key();
        let ctx = test_context(&key);
        let limit = MAX_RANGE_POINTS as i64;
        let second = 1_000_000_000;
        let stub = Stub::default();
        for end_ns in [
            START_NS + (limit + 1) * second,
            START_NS + limit * second + NS_PER_MS,
        ] {
            let envelope = block_on(run(range_args(START_NS, end_ns, "1s"), &ctx, &stub));
            let failure = failure_of(&envelope);
            assert_eq!(failure.class, FailureClass::InvalidArgument);
            assert_eq!(
                failure.message,
                "time_range and step evaluate 11001 steps, over the 11000 a range evaluation \
                 may have"
            );
            assert_eq!(envelope.next_steps.len(), 1);
            assert!(
                envelope.next_steps[0]
                    .detail
                    .starts_with("widen step to at least 1001ms, or narrow time_range"),
                "{:?}",
                envelope.next_steps
            );
        }

        let stub = answering(vec![Ok(sample_rows(&[("m", START_NS, 1.0)]))]);
        let envelope = block_on(run(
            range_args(START_NS, START_NS + limit * second, "1s"),
            &ctx,
            &stub,
        ));
        assert_eq!(envelope.status, Status::Ok, "{envelope:?}");
        assert_eq!(stub.ranges.borrow().len(), 1);
    }

    /// `n` consecutive one-second samples of `name` from [`START_NS`].
    fn long_series(
        name: &'static str,
        n: i64,
        value: impl Fn(i64) -> f64,
    ) -> Vec<(&'static str, i64, f64)> {
        (0..n)
            .map(|k| (name, START_NS + k * 1_000_000_000, value(k)))
            .collect()
    }

    /// Eleven digits after the point, so each sample costs about 34 B and
    /// 11,000 of them are well over the 256 KiB floor cap.
    fn wide(k: i64) -> f64 {
        k as f64 + 0.123_456_789_01
    }

    /// The samples object of `row`, its two arrays, and its omitted count.
    fn samples_of(row: &Row) -> (&Map<String, Value>, &Vec<Value>, &Vec<Value>) {
        let Cell::Map(samples) = &row[1] else {
            panic!("samples is an object, not {:?}", row[1]);
        };
        let ts = samples["ts_ns"].as_array().expect("ts_ns is an array");
        let values = samples["value"].as_array().expect("value is an array");
        (samples, ts, values)
    }

    /// 11,000 one-second steps, the most a range may have.
    fn range_at_the_step_limit() -> Value {
        range_args(
            START_NS,
            START_NS + MAX_RANGE_POINTS as i64 * 1_000_000_000,
            "1s",
        )
    }

    /// One series whose samples are over the byte cap keeps an aligned
    /// prefix: `samples` stays an object, `ts_ns` and `value` hold the same
    /// first `k` samples of the series, `samples_omitted` counts the rest,
    /// the cut is one truncated cell, the result is `ok_bounded`, and a next
    /// step says to widen the step or narrow the range. The envelope still
    /// fits the cap.
    ///
    /// FLIP: leaving the cell to `Envelope::fit` (the code before this rule)
    /// answers a string; cutting `ts_ns` alone leaves arrays of different
    /// lengths.
    #[test]
    fn a_series_over_the_byte_cap_keeps_an_aligned_prefix() {
        let key = key();
        let mut ctx = test_context(&key);
        ctx.budgets.max_response_bytes = 1;
        let full = long_series("m", 11_000, wide);
        let stub = answering(vec![Ok(sample_rows(&full))]);

        let envelope = block_on(run(range_at_the_step_limit(), &ctx, &stub));

        assert_eq!(
            envelope.status,
            Status::OkBounded,
            "{:?}",
            envelope.presentation
        );
        assert!(envelope.presentation.bytes_cap_hit);
        assert_eq!(envelope.presentation.cells_truncated, 1);
        assert_eq!(envelope.presentation.rows_omitted, 0);
        assert_eq!(envelope.data.row_count, 1);
        assert_eq!(envelope.data.rows.len(), 1);
        let (samples, ts, values) = samples_of(&envelope.data.rows[0]);
        let kept = ts.len();
        assert_eq!(values.len(), kept);
        assert!(kept > 0 && kept < full.len(), "{kept}");
        for (k, &(_, ts_ns, value)) in full[..kept].iter().enumerate() {
            assert_eq!(ts[k], json!(ts_ns.to_string()));
            assert_eq!(values[k], json!(value));
        }
        assert_eq!(
            samples["samples_omitted"],
            json!((full.len() - kept).to_string())
        );
        assert!(
            envelope
                .next_steps
                .iter()
                .any(|step| step.detail.contains("widen step or narrow time_range")),
            "{:?}",
            envelope.next_steps
        );
        let size = serde_json::to_vec(&envelope).expect("serializes").len() as u64;
        let cap = envelope.presentation.effective_max_response_bytes;
        assert!(size <= cap, "{size} over {cap}");
        // A prefix far shorter than the room (a fixed small cut) is not the
        // longest that fits: the envelope ends within 2 KiB of the cap.
        assert!(size + 2048 > cap, "{size} leaves more than 2 KiB of {cap}");
    }

    /// With several series and the second over the remaining room, the
    /// first is kept whole, the second is cut to a prefix, and the third is
    /// dropped and counted.
    ///
    /// FLIP: cutting the first series rather than the one that does not fit
    /// keeps `a` alone, with fewer than 10 samples, and drops `b`.
    #[test]
    fn the_series_that_does_not_fit_is_cut_and_the_ones_before_stay_whole() {
        let key = key();
        let mut ctx = test_context(&key);
        ctx.budgets.max_response_bytes = 1;
        let mut rows = long_series("a", 10, wide);
        rows.extend(long_series("b", 11_000, wide));
        rows.extend(long_series("c", 10, wide));
        let stub = answering(vec![Ok(sample_rows(&rows))]);

        let envelope = block_on(run(range_at_the_step_limit(), &ctx, &stub));

        assert_eq!(envelope.status, Status::OkBounded);
        assert_eq!(envelope.data.row_count, 3);
        assert_eq!(envelope.data.rows.len(), 2);
        assert_eq!(envelope.presentation.cells_truncated, 1);
        assert_eq!(envelope.presentation.rows_omitted, 1);
        let (a, a_ts, a_values) = samples_of(&envelope.data.rows[0]);
        assert_eq!(envelope.data.rows[0][0], labels("a"));
        assert_eq!((a_ts.len(), a_values.len()), (10, 10));
        assert!(!a.contains_key("samples_omitted"), "{a:?}");
        let (b, b_ts, b_values) = samples_of(&envelope.data.rows[1]);
        assert_eq!(envelope.data.rows[1][0], labels("b"));
        assert_eq!(b_ts.len(), b_values.len());
        assert_eq!(
            b["samples_omitted"],
            json!((11_000 - b_ts.len()).to_string())
        );
    }

    /// `NaN`, the infinities and `-0.0` inside a cut prefix keep the D4
    /// precision encoding.
    ///
    /// FLIP: a cut that turns the string-encoded values into `null` (what
    /// rebuilding the arrays through `json!(f64)` does) fails the match.
    #[test]
    fn a_cut_prefix_keeps_float_precision() {
        let key = key();
        let mut ctx = test_context(&key);
        ctx.budgets.max_response_bytes = 1;
        let special = [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -0.0];
        let full = long_series("m", 11_000, |k| {
            special.get(k as usize).copied().unwrap_or_else(|| wide(k))
        });
        let stub = answering(vec![Ok(sample_rows(&full))]);

        let envelope = block_on(run(range_at_the_step_limit(), &ctx, &stub));

        assert_eq!(envelope.presentation.cells_truncated, 1);
        let text = serde_json::to_string(&envelope.data).expect("data serializes");
        assert!(
            text.contains(r#""value":["NaN","+Inf","-Inf",-0.0,4.12345678901,"#),
            "{}",
            &text[..text.len().min(400)]
        );
    }

    /// Without consent a partial result is refused as `unavailable`, the
    /// class the HTTP route's 503 maps to, with a next step naming the
    /// consent argument; with consent the request carries it and the result
    /// comes back with `coverage.partial` set.
    ///
    /// FLIP: ignoring the flag (always sending `allow_partial: false`) fails
    /// the consenting request assertion; dropping the body's own gate
    /// answers the unconsented partial result as `ok`.
    #[test]
    fn partial_coverage_is_refused_without_consent_and_returned_with_it() {
        let key = key();
        let ctx = test_context(&key);
        let partial = || {
            let mut envelope = sample_rows(&[("m", START_NS, 1.0)]);
            envelope.coverage.complete = false;
            envelope.coverage.partial = true;
            envelope
        };
        let stub = answering(vec![Ok(partial()), Ok(partial())]);
        let instant = json!({"query": "m", "evaluation_time": START_NS.to_string()});

        let refused = block_on(run(instant.clone(), &ctx, &stub));
        let failure = failure_of(&refused);
        assert_eq!(failure.class, FailureClass::Unavailable);
        assert!(failure.message.starts_with("partial results:"));
        assert!(
            refused.next_steps[0]
                .detail
                .contains("allow_partial_coverage: true")
        );
        assert!(!stub.instants.borrow()[0].allow_partial);

        let mut consenting = instant;
        consenting["allow_partial_coverage"] = json!(true);
        let answered = block_on(run(consenting, &ctx, &stub));
        assert_eq!(answered.status, Status::Ok, "{answered:?}");
        assert!(answered.coverage.partial);
        assert!(!answered.coverage.complete);
        assert!(stub.instants.borrow()[1].allow_partial);
    }

    /// The service's own partial refusal names the HTTP parameter; the tool
    /// rewrites it to its own argument and adds the next step.
    ///
    /// FLIP: passing the failure through unchanged tells an MCP caller to
    /// set `allow_partial=true`, which no tool argument is called.
    #[test]
    fn the_services_partial_refusal_names_this_tools_argument() {
        let key = key();
        let ctx = test_context(&key);
        let stub = answering(vec![Err(Failure {
            class: FailureClass::Unavailable,
            message: ravel_query::http::partial_refusal_message(&["cluster eu: timeout".into()]),
            counter: None,
        })]);

        let envelope = block_on(run(
            json!({"query": "m", "evaluation_time": START_NS.to_string()}),
            &ctx,
            &stub,
        ));

        let failure = failure_of(&envelope);
        assert_eq!(failure.class, FailureClass::Unavailable);
        assert_eq!(
            failure.message,
            "partial results: coverage is incomplete (cluster eu: timeout); set \
             allow_partial_coverage: true to receive partial results"
        );
        assert_eq!(envelope.next_steps.len(), 1);
    }

    /// `evidence_ref` is not served, and a value for it is `unsupported`
    /// before the backend; an absent query is `missing_argument`.
    ///
    /// FLIP: ignoring the field reaches the panicking stub.
    #[test]
    fn evidence_ref_is_unsupported_and_query_is_required() {
        let key = key();
        let ctx = test_context(&key);
        let stub = Stub::default();
        let envelope = block_on(run(
            json!({"query": "m", "evaluation_time": "0", "evidence_ref": "e"}),
            &ctx,
            &stub,
        ));
        assert_eq!(failure_of(&envelope).class, FailureClass::Unsupported);

        let envelope = block_on(run(json!({"evaluation_time": "0"}), &ctx, &stub));
        assert_eq!(failure_of(&envelope).class, FailureClass::MissingArgument);
    }

    /// A scalar result is the port's one-row, one-column shape, unchanged.
    ///
    /// FLIP: forcing every result through the series grouping fails on the
    /// one-cell row with an internal failure.
    #[test]
    fn a_scalar_result_passes_through() {
        let key = key();
        let ctx = test_context(&key);
        let mut scalar = Envelope::default();
        scalar.data.columns = vec![Column {
            name: "value".to_string(),
            r#type: "double".to_string(),
        }];
        scalar.data.rows = vec![vec![Cell::Float(2.0)]];
        scalar.data.row_count = 1;
        let stub = answering(vec![Ok(scalar)]);

        let envelope = block_on(run(
            json!({"query": "1 + 1", "evaluation_time": "0"}),
            &ctx,
            &stub,
        ));

        assert_eq!(envelope.status, Status::Ok, "{envelope:?}");
        assert_eq!(envelope.data.rows, vec![vec![Cell::Float(2.0)]]);
        assert_eq!(envelope.data.row_count, 1);
    }
}
