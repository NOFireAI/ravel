//! `ravel_analyze_timeseries`: `change_point` or `summary` over a PromQL
//! range result (ADR-1374 D2, and the analyze-timeseries amendment).
//!
//! The range arguments follow `ravel_query_promql`'s rules exactly, through
//! the same [`eval_range`]: whole milliseconds, a positive step with a unit,
//! the end after the start, and at most
//! [`MAX_RANGE_POINTS`](super::promql_range::MAX_RANGE_POINTS) steps. Every
//! argument refusal answers before the backend is called.
//!
//! The call crosses the port as one [`AnalyticsCall`], which the server's
//! adapter deserializes into its `AnalyticsRequest` and sends through
//! `QueryService::analytics`, the same method `POST /api/v1/analytics` calls.
//! Admission, the deadline clamp, the usage record, the audit event and the
//! partial-coverage gate happen there. The adapter then runs the op over each
//! series and returns one row per series, in [`AnalyzeOp::columns`] order,
//! before any cap.
//!
//! Both ops are reported as heuristics: `accuracy.exact` is `false` and
//! `accuracy.approximation` names the method, the minimum point count, and
//! how many series were downsampled. A series with too few points is a row
//! with `too_few_points: true`, not a failed call.
//!
//! The tool takes no partial-coverage consent, so a read that skipped a
//! federated remote is always refused as `unavailable`, with next steps to
//! retry or to read the partial series through `ravel_query_promql`.

use std::time::Duration;

use ravel_query::http::service::PARTIAL_REFUSAL_PREFIX;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::catalog::{AnalyzeOp, AnalyzeTimeseriesInput};
use crate::envelope::{AnyJson, Cell, Envelope, Failure, FailureClass, NextStep};
use crate::service::QueryBackend;

use super::ToolContext;
use super::capabilities::refused_with;
use super::promql_range::{EvalRange, Refusal, eval_range};
use super::query_promql::{report_range_scope, rewrite_partial_refusal};

const TOOL: &str = "ravel_analyze_timeseries";

/// The fewest non-NaN points `change_point` classifies; a series with fewer
/// is `indeterminable` (ravel-analytics `MIN_POINTS`, ADR-0028 decision 3).
pub const CHANGE_POINT_MIN_POINTS: u64 = 22;

/// The most points `change_point` runs over. A longer series is
/// bucket-averaged down to this many (ravel-analytics `MAX_POINTS`,
/// ADR-0028 decision 4).
pub const CHANGE_POINT_MAX_POINTS: u64 = 2000;

/// The fewest non-NaN points `summary` summarizes.
pub const SUMMARY_MIN_POINTS: u64 = 1;

/// One `change_point` row per series, in this order.
pub const CHANGE_POINT_COLUMNS: [&str; 9] = [
    "series",
    "kind",
    "ts_ns",
    "score",
    "points",
    "nan_excluded",
    "downsampled",
    "min_points",
    "too_few_points",
];

/// One `summary` row per series, in this order.
pub const SUMMARY_COLUMNS: [&str; 10] = [
    "series",
    "median",
    "mad",
    "stddev",
    "variance",
    "points",
    "nan_excluded",
    "downsampled",
    "min_points",
    "too_few_points",
];

/// The arguments a per-call query budget would arrive under. The analytics
/// service call takes no per-request scan budget, so a value here could not
/// be applied and is refused rather than reported as in force.
const UNAPPLIED_BUDGETS: [&str; 3] = ["max_bytes_scanned", "max_store_requests", "max_segments"];

impl AnalyzeOp {
    pub fn name(self) -> &'static str {
        match self {
            AnalyzeOp::ChangePoint => "change_point",
            AnalyzeOp::Summary => "summary",
        }
    }

    /// The row shape the adapter returns for this op.
    pub fn columns(self) -> &'static [&'static str] {
        match self {
            AnalyzeOp::ChangePoint => &CHANGE_POINT_COLUMNS,
            AnalyzeOp::Summary => &SUMMARY_COLUMNS,
        }
    }

    pub fn min_points(self) -> u64 {
        match self {
            AnalyzeOp::ChangePoint => CHANGE_POINT_MIN_POINTS,
            AnalyzeOp::Summary => SUMMARY_MIN_POINTS,
        }
    }
}

/// `ravel_analyze_timeseries`'s request as it crosses
/// [`QueryBackend::analytics`]: the JSON the server's adapter converts into
/// its `AnalyticsRequest`, whose own type `ravel-mcp` cannot name.
///
/// `start_ms`, `end_ms` and `step_ms` map to the request's fields of the
/// same names; `end_ms` is closed, like the engine's range, so it is the
/// millisecond before the tool's half-open end. `min_tokens` are encoded
/// commit tokens, `deadline_ms` lowers the adapter's clamped deadline, and
/// `allow_partial` is the request's partial-coverage consent. `op` selects
/// the analytic the adapter runs per series: `change_point` with
/// downsampling on, or `summary` with no percentiles.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AnalyticsCall {
    pub query: String,
    pub start_ms: i64,
    pub end_ms: i64,
    pub step_ms: i64,
    pub op: AnalyzeOp,
    #[serde(default)]
    pub min_tokens: Vec<String>,
    #[serde(default)]
    pub deadline_ms: Option<u64>,
    #[serde(default)]
    pub allow_partial: bool,
}

impl AnalyticsCall {
    /// The call for a validated range. The tool never consents to partial
    /// coverage and requires no commit tokens.
    fn new(query: String, range: EvalRange, op: AnalyzeOp, deadline: Duration) -> Self {
        AnalyticsCall {
            query,
            start_ms: range.start_ms(),
            end_ms: range.closed_end_ms(),
            step_ms: range.step_ms(),
            op,
            min_tokens: Vec::new(),
            deadline_ms: Some(u64::try_from(deadline.as_millis()).unwrap_or(u64::MAX)),
            allow_partial: false,
        }
    }

    pub fn to_json(&self) -> Result<AnyJson, Failure> {
        serde_json::to_value(self)
            .map(AnyJson)
            .map_err(|err| internal(format!("{TOOL} call did not serialize: {err}")))
    }

    /// The call a port implementation received. A shape it cannot read is
    /// `invalid_argument`, naming the field serde names.
    pub fn from_json(request: &AnyJson) -> Result<Self, Failure> {
        serde_json::from_value(request.0.clone())
            .map_err(|err| invalid(format!("{TOOL} arguments: {err}")))
    }
}

#[derive(Debug, Clone)]
struct Plan {
    query: String,
    range: EvalRange,
    op: AnalyzeOp,
}

/// Run the tool. Every argument refusal answers before `backend` is called.
pub async fn run<B: QueryBackend>(args: Value, ctx: &ToolContext<'_>, backend: &B) -> Envelope {
    let plan = match plan(args) {
        Ok(plan) => plan,
        Err((failure, next_steps)) => return refused_with(failure, next_steps, ctx),
    };
    let call = AnalyticsCall::new(
        plan.query.clone(),
        plan.range,
        plan.op,
        ctx.budgets.deadline,
    );
    let answered = match call.to_json() {
        Ok(request) => backend.analytics(ctx.tenant_hash, &request).await,
        Err(failure) => Err(failure),
    };
    match answered.and_then(|envelope| finish(envelope, &plan, ctx)) {
        Ok(envelope) => envelope,
        Err(failure) => backend_refusal(failure, ctx),
    }
}

fn plan(args: Value) -> Result<Plan, Refusal> {
    for name in ["query", "time_range", "step", "op"] {
        if args.get(name).is_none_or(Value::is_null) {
            return Err(refusal(
                FailureClass::MissingArgument,
                format!("{TOOL} requires {name}"),
            ));
        }
    }
    if let Some(name) = UNAPPLIED_BUDGETS
        .iter()
        .find(|name| args.get(**name).is_some_and(|value| !value.is_null()))
    {
        return Err(refusal(
            FailureClass::Unsupported,
            format!(
                "{name} is not applied by {TOOL} in this build: the analytics service call \
                 takes no per-request scan budget, so the server's ceilings bound it"
            ),
        ));
    }
    let input = AnalyzeTimeseriesInput::deserialize(args).map_err(|err| {
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
    let range = eval_range(TOOL, &input.time_range, &input.step)?;
    Ok(Plan {
        query: input.query,
        range,
        op: input.op,
    })
}

/// Check the rows' shape, state the scope and the heuristic, then cap series
/// at `max_rows` and fit.
fn finish(mut envelope: Envelope, plan: &Plan, ctx: &ToolContext<'_>) -> Result<Envelope, Failure> {
    if envelope.coverage.partial {
        return Err(partial_refusal());
    }
    let columns = plan.op.columns();
    let names: Vec<&str> = envelope
        .data
        .columns
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    if names != columns {
        return Err(internal(format!(
            "the analytics port returned columns {names:?} for {}, not {columns:?}",
            plan.op.name()
        )));
    }
    report_range_scope(&mut envelope, plan.range);

    let downsampled_at = column_index(columns, "downsampled")?;
    let mut downsampled = 0u64;
    for row in &envelope.data.rows {
        match row.get(downsampled_at) {
            Some(Cell::Bool(true)) => downsampled += 1,
            Some(Cell::Bool(false)) => {}
            other => {
                return Err(internal(format!(
                    "the analytics port returned a downsampled cell that is not a bool: {other:?}"
                )));
            }
        }
    }
    let produced = envelope.data.rows.len();
    envelope.accuracy.exact = false;
    envelope.accuracy.approximation = Some(approximation(plan.op, downsampled, produced));

    let max_rows = ctx.budgets.max_rows as usize;
    let row_cap_omitted = produced.saturating_sub(max_rows) as u64;
    envelope.data.rows.truncate(max_rows);
    envelope.presentation.max_rows = ctx.budgets.max_rows;
    envelope.data.row_count = produced as u64;
    Ok(envelope
        .fit_row_capped(ctx.budgets.max_response_bytes, row_cap_omitted)
        .finish(false))
}

/// What `accuracy.approximation` says: the word heuristic, the method, the
/// minimum point count, and the downsampled series out of all of them.
fn approximation(op: AnalyzeOp, downsampled: u64, series: usize) -> String {
    match op {
        AnalyzeOp::ChangePoint => format!(
            "heuristic: change_point segments each series by PELT under a BIC penalty and \
             classifies its top boundary by rule; needs {CHANGE_POINT_MIN_POINTS} \
             non-NaN points per series, fewer is too_few_points; over \
             {CHANGE_POINT_MAX_POINTS} points is bucket-averaged to {CHANGE_POINT_MAX_POINTS}; \
             {downsampled} of {series} series downsampled"
        ),
        AnalyzeOp::Summary => format!(
            "heuristic: summary is median, MAD, population variance and stddev over the step \
             values (latest sample in the lookback), not every stored sample; needs \
             {SUMMARY_MIN_POINTS} non-NaN point per series; summary never downsamples; \
             {downsampled} of {series} series downsampled"
        ),
    }
}

fn column_index(columns: &[&str], name: &str) -> Result<usize, Failure> {
    columns
        .iter()
        .position(|column| *column == name)
        .ok_or_else(|| internal(format!("{name} is not an analytics column")))
}

/// The service's own refusal names the HTTP route's consent parameter; this
/// tool has none, so the remedy is replaced by the rule.
fn no_partial_consent() -> String {
    format!("{TOOL} takes no partial-coverage consent, so it does not analyze a partial read")
}

fn partial_refusal() -> Failure {
    Failure {
        class: FailureClass::Unavailable,
        message: format!(
            "{PARTIAL_REFUSAL_PREFIX} coverage is incomplete; {}",
            no_partial_consent()
        ),
        counter: None,
    }
}

/// A failure from the backend, or from this body's own checks. A
/// partial-coverage refusal names this tool's rule rather than the HTTP
/// route's `allow_partial`, and says where a partial read can be had.
fn backend_refusal(mut failure: Failure, ctx: &ToolContext<'_>) -> Envelope {
    let mut next_steps = Vec::new();
    if rewrite_partial_refusal(&mut failure, &no_partial_consent()) {
        next_steps.push(NextStep {
            action: TOOL.to_string(),
            detail: "retry once the skipped remote is reachable again".to_string(),
        });
        next_steps.push(NextStep {
            action: "ravel_query_promql".to_string(),
            detail: "send the same query, time_range and step with allow_partial_coverage: \
                     true to read the partial series, reported with coverage.partial: true \
                     and accuracy.exact: false"
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
    use serde_json::{Map, json};

    use super::*;
    use crate::cursor::{CURSOR_KEY_LEN, CursorKey};
    use crate::envelope::row_cap_sweep::assert_finished_inside_cap;
    use crate::envelope::{Column, MAX_RESPONSE_BYTES_FLOOR, Row, Status};
    use crate::tools::promql_range::MAX_RANGE_POINTS;
    use crate::tools::tests::{Stub, block_on, test_context};

    /// 1000 s and 1060 s after the epoch, in nanoseconds.
    const START_NS: i64 = 1_000_000_000_000;
    const END_NS: i64 = 1_060_000_000_000;

    fn key() -> CursorKey {
        CursorKey::from_process_secret([0x5cu8; CURSOR_KEY_LEN])
    }

    fn args(start_ns: i64, end_ns: i64, step: &str, op: &str) -> Value {
        json!({
            "query": "m",
            "time_range": {"start_ns": start_ns.to_string(), "end_ns": end_ns.to_string()},
            "step": step,
            "op": op,
        })
    }

    fn labels(name: &str) -> Cell {
        let mut map = Map::new();
        map.insert("__name__".to_string(), json!(name));
        Cell::Map(map)
    }

    /// What the adapter returns for `change_point`: one row per series,
    /// `(name, points, downsampled)`, too few points below the minimum.
    fn change_point_rows(series: &[(&str, u64, bool)]) -> Envelope {
        let rows = series
            .iter()
            .map(|&(name, points, downsampled)| {
                let too_few = points < CHANGE_POINT_MIN_POINTS;
                vec![
                    labels(name),
                    Cell::Str(
                        if too_few {
                            "indeterminable"
                        } else {
                            "step_change"
                        }
                        .to_string(),
                    ),
                    if too_few {
                        Cell::Null
                    } else {
                        Cell::Timestamp(START_NS)
                    },
                    if too_few {
                        Cell::Null
                    } else {
                        Cell::Float(4.5)
                    },
                    Cell::Int(i128::from(points)),
                    Cell::Int(0),
                    Cell::Bool(downsampled),
                    Cell::Int(i128::from(CHANGE_POINT_MIN_POINTS)),
                    Cell::Bool(too_few),
                ]
            })
            .collect();
        answer(&CHANGE_POINT_COLUMNS, rows)
    }

    fn answer(columns: &[&str], rows: Vec<Row>) -> Envelope {
        let mut envelope = Envelope::default();
        envelope.data.columns = columns
            .iter()
            .map(|name| Column {
                name: (*name).to_string(),
                r#type: "json".to_string(),
            })
            .collect();
        envelope.data.row_count = rows.len() as u64;
        envelope.data.rows = rows;
        envelope.coverage.complete = true;
        envelope.accuracy.exact = true;
        envelope
    }

    fn answering(answers: Vec<Result<Envelope, Failure>>) -> Stub {
        let stub = Stub::default();
        stub.analytics.borrow_mut().extend(answers);
        stub
    }

    fn failure_of(envelope: &Envelope) -> &Failure {
        assert_eq!(envelope.status, Status::Error, "{envelope:?}");
        envelope.failure.as_ref().expect("an error carries one")
    }

    /// A bad step, a sub-millisecond bound or step, an empty range, too many
    /// steps, and an unknown op are each `invalid_argument` before the
    /// backend runs, and `ravel_query_promql` refuses every range case with
    /// the same class, message and next steps but the tool name. The default
    /// `Stub` panics on every PromQL and analytics method, so reaching either
    /// backend fails the test.
    ///
    /// FLIP: a copy of the range checks in this body that drifts from
    /// `ravel_query_promql`'s (say, accepting a bare `30` as seconds, or
    /// refusing at `steps >= MAX_RANGE_POINTS`) fails the comparison; checks
    /// left to the engine panic the stub.
    #[test]
    fn both_tools_refuse_the_same_bad_ranges_before_the_backend() {
        let key = key();
        let ctx = test_context(&key);
        let stub = Stub::default();
        let second = 1_000_000_000i64;
        let limit = MAX_RANGE_POINTS as i64;
        let cases: Vec<(&str, i64, i64, &str)> = vec![
            ("zero step", START_NS, END_NS, "0s"),
            ("bare number step", START_NS, END_NS, "30"),
            ("negative step", START_NS, END_NS, "-1s"),
            ("unparsable step", START_NS, END_NS, "soon"),
            ("sub-ms step", START_NS, END_NS, "1500us"),
            ("sub-ms start", START_NS + 1, END_NS, "30s"),
            ("sub-ms end", START_NS, END_NS + 1, "30s"),
            ("empty range", START_NS, START_NS, "30s"),
            ("reversed range", END_NS, START_NS, "30s"),
            (
                "one step over the limit",
                START_NS,
                START_NS + limit * second + 1_000_000,
                "1s",
            ),
        ];
        for (what, start, end, step) in cases {
            let analyzed = block_on(run(args(start, end, step, "summary"), &ctx, &stub));
            let mut promql_args = args(start, end, step, "summary");
            promql_args.as_object_mut().expect("an object").remove("op");
            let evaluated = block_on(super::super::query_promql::run(promql_args, &ctx, &stub));

            let analyzed_failure = failure_of(&analyzed);
            let evaluated_failure = failure_of(&evaluated);
            assert_eq!(
                analyzed_failure.class,
                FailureClass::InvalidArgument,
                "{what}"
            );
            assert_eq!(analyzed_failure.class, evaluated_failure.class, "{what}");
            assert_eq!(
                analyzed_failure.message, evaluated_failure.message,
                "{what}"
            );
            assert_eq!(
                analyzed_failure.counter, evaluated_failure.counter,
                "{what}"
            );
            let steps = |envelope: &Envelope| {
                envelope
                    .next_steps
                    .iter()
                    .map(|step| step.detail.clone())
                    .collect::<Vec<_>>()
            };
            assert_eq!(steps(&analyzed), steps(&evaluated), "{what}");
            assert!(
                analyzed.next_steps.iter().all(|step| step.action == TOOL),
                "{what}: {:?}",
                analyzed.next_steps
            );
        }
        // The limit case carries its next step; the comparison above is not
        // vacuous over an empty list.
        let over = block_on(run(
            args(
                START_NS,
                START_NS + limit * second + 1_000_000,
                "1s",
                "summary",
            ),
            &ctx,
            &stub,
        ));
        assert_eq!(over.next_steps.len(), 1);

        let unknown_op = block_on(run(args(START_NS, END_NS, "30s", "forecast"), &ctx, &stub));
        let failure = failure_of(&unknown_op);
        assert_eq!(failure.class, FailureClass::InvalidArgument);
        assert!(
            failure.message.contains("unknown variant `forecast`"),
            "{}",
            failure.message
        );
    }

    /// Exactly `MAX_RANGE_POINTS` steps reach the backend.
    ///
    /// FLIP: refusing at `steps >= MAX_RANGE_POINTS` refuses this call.
    #[test]
    fn exactly_the_step_limit_reaches_the_backend() {
        let key = key();
        let ctx = test_context(&key);
        let stub = answering(vec![Ok(change_point_rows(&[]))]);
        let end = START_NS + MAX_RANGE_POINTS as i64 * 1_000_000_000;
        let envelope = block_on(run(args(START_NS, end, "1s", "change_point"), &ctx, &stub));
        assert_eq!(envelope.status, Status::Ok, "{envelope:?}");
        assert_eq!(stub.analyzed.borrow().len(), 1);
    }

    /// Each required argument missing is `missing_argument` naming it, and
    /// `evidence_ref` or a scan budget the service cannot apply is
    /// `unsupported`, all before the backend.
    ///
    /// FLIP: accepting `max_bytes_scanned` and leaving it unapplied panics
    /// the stub, as does forwarding `evidence_ref`.
    #[test]
    fn missing_and_unsupported_arguments_are_refused_before_the_backend() {
        let key = key();
        let ctx = test_context(&key);
        let stub = Stub::default();
        for name in ["query", "time_range", "step", "op"] {
            let mut call = args(START_NS, END_NS, "30s", "summary");
            call.as_object_mut().expect("an object").remove(name);
            let envelope = block_on(run(call, &ctx, &stub));
            let failure = failure_of(&envelope);
            assert_eq!(failure.class, FailureClass::MissingArgument, "{name}");
            assert_eq!(failure.message, format!("{TOOL} requires {name}"));
        }
        for (name, value) in [
            ("evidence_ref", json!("ref")),
            ("max_bytes_scanned", json!(1024)),
            ("max_store_requests", json!(5)),
            ("max_segments", json!(5)),
        ] {
            let mut call = args(START_NS, END_NS, "30s", "summary");
            call.as_object_mut()
                .expect("an object")
                .insert(name.to_string(), value);
            let envelope = block_on(run(call, &ctx, &stub));
            let failure = failure_of(&envelope);
            assert_eq!(failure.class, FailureClass::Unsupported, "{name}");
            assert!(failure.message.starts_with(name), "{}", failure.message);
        }
    }

    /// The port receives one [`AnalyticsCall`]: the half-open range as the
    /// engine's closed one, the step in milliseconds, the op, the call's
    /// clamped deadline, and no partial consent. It reads back through
    /// [`AnalyticsCall::from_json`] unchanged, and a field the adapter does
    /// not know is refused.
    ///
    /// FLIP: sending `end_ns` as the closed end evaluates one step more than
    /// `ravel_query_promql` would for the same range, and fails `end_ms`.
    #[test]
    fn the_port_receives_one_typed_call() {
        let key = key();
        let ctx = test_context(&key);
        let stub = answering(vec![Ok(change_point_rows(&[]))]);
        let envelope = block_on(run(
            args(START_NS, END_NS, "15s", "change_point"),
            &ctx,
            &stub,
        ));
        assert_eq!(envelope.status, Status::Ok, "{envelope:?}");
        let sent = stub.analyzed.borrow()[0].clone();
        let expected = AnalyticsCall {
            query: "m".to_string(),
            start_ms: 1_000_000,
            end_ms: 1_059_999,
            step_ms: 15_000,
            op: AnalyzeOp::ChangePoint,
            min_tokens: Vec::new(),
            deadline_ms: Some(ctx.budgets.deadline.as_millis() as u64),
            allow_partial: false,
        };
        assert_eq!(
            sent.0,
            json!({
                "query": "m", "start_ms": 1_000_000, "end_ms": 1_059_999, "step_ms": 15_000,
                "op": "change_point", "min_tokens": [],
                "deadline_ms": ctx.budgets.deadline.as_millis() as u64, "allow_partial": false,
            })
        );
        assert_eq!(
            AnalyticsCall::from_json(&sent).expect("reads back"),
            expected
        );

        let mut unknown = sent.0.clone();
        unknown
            .as_object_mut()
            .expect("an object")
            .insert("percentiles".to_string(), json!([0.5]));
        let refused = AnalyticsCall::from_json(&AnyJson(unknown)).expect_err("unknown field");
        assert_eq!(refused.class, FailureClass::InvalidArgument);
    }

    /// The result is never exact: `accuracy.exact` is `false` and the
    /// approximation says heuristic, names the method and the minimum point
    /// count, and counts the downsampled series. A series below the minimum
    /// is a row of the `ok` result, not a failure, and the scope carries
    /// the range lines `ravel_query_promql` reports.
    ///
    /// FLIP: passing the port's `exact: true` through fails the first
    /// assertion; counting downsampled series after the row cap reports
    /// "1 of 3"; refusing a too-few series fails the status.
    #[test]
    fn the_result_is_a_heuristic_with_too_few_points_per_row() {
        let key = key();
        let mut ctx = test_context(&key);
        ctx.budgets.max_rows = 2;
        let stub = answering(vec![Ok(change_point_rows(&[
            ("short", CHANGE_POINT_MIN_POINTS - 1, false),
            ("plain", 60, false),
            ("long", 2500, true),
        ]))]);
        let envelope = block_on(run(
            args(START_NS, END_NS, "15s", "change_point"),
            &ctx,
            &stub,
        ));
        assert_eq!(envelope.status, Status::OkBounded, "{envelope:?}");
        assert!(!envelope.accuracy.exact);
        let approximation = envelope.accuracy.approximation.clone().expect("named");
        assert!(
            approximation.starts_with("heuristic: change_point"),
            "{approximation}"
        );
        assert!(approximation.contains("PELT"), "{approximation}");
        assert!(
            approximation.contains("needs 22 non-NaN points"),
            "{approximation}"
        );
        assert!(
            approximation.ends_with("1 of 3 series downsampled"),
            "{approximation}"
        );

        assert_eq!(envelope.data.row_count, 3);
        assert_eq!(envelope.data.rows.len(), 2);
        assert_eq!(envelope.presentation.rows_omitted, 1);
        assert!(envelope.presentation.row_cap_hit);
        let short = &envelope.data.rows[0];
        assert_eq!(short[1], Cell::Str("indeterminable".to_string()));
        assert_eq!(short[7], Cell::Int(22));
        assert_eq!(short[8], Cell::Bool(true));
        assert_eq!(envelope.data.rows[1][8], Cell::Bool(false));

        let predicates = &envelope.scope.predicates_applied;
        assert!(
            predicates.iter().any(|line| line.starts_with(&format!(
                "steps aligned to time_range.start_ns: {START_NS} + k * 15000000000 ns"
            ))),
            "{predicates:?}"
        );
        assert!(predicates.iter().any(|line| line.starts_with("lookback ")));
        let range = envelope.scope.time_range.clone().expect("a range");
        assert_eq!(range.end_ns, END_NS.to_string());
    }

    /// `summary` names its own heuristic and says it never downsamples.
    ///
    /// FLIP: one approximation text for both ops names PELT for a summary.
    #[test]
    fn summary_names_its_heuristic_and_never_downsamples() {
        let key = key();
        let ctx = test_context(&key);
        let row = vec![
            labels("m"),
            Cell::Float(2.5),
            Cell::Float(1.0),
            Cell::Float(1.25f64.sqrt()),
            Cell::Float(1.25),
            Cell::Int(4),
            Cell::Int(0),
            Cell::Bool(false),
            Cell::Int(i128::from(SUMMARY_MIN_POINTS)),
            Cell::Bool(false),
        ];
        let stub = answering(vec![Ok(answer(&SUMMARY_COLUMNS, vec![row]))]);
        let envelope = block_on(run(args(START_NS, END_NS, "15s", "summary"), &ctx, &stub));
        assert_eq!(envelope.status, Status::Ok, "{envelope:?}");
        assert!(!envelope.accuracy.exact);
        let approximation = envelope.accuracy.approximation.clone().expect("named");
        assert!(
            approximation.starts_with("heuristic: summary"),
            "{approximation}"
        );
        assert!(!approximation.contains("PELT"), "{approximation}");
        assert!(
            approximation.contains("summary never downsamples"),
            "{approximation}"
        );
        assert!(
            approximation.ends_with("0 of 1 series downsampled"),
            "{approximation}"
        );
    }

    /// Both approximation texts fit the envelope's 256-byte approximation
    /// bound at the series cap, so the downsampled count at their end is
    /// never the part `fit` shortens.
    ///
    /// FLIP: a longer text loses its count to the truncation marker at 1000
    /// series.
    #[test]
    fn the_approximation_fits_its_bound_at_the_series_cap() {
        for op in [AnalyzeOp::ChangePoint, AnalyzeOp::Summary] {
            let text = approximation(op, 1000, 1000);
            let serialized = serde_json::to_string(&text).expect("a string serializes");
            assert!(
                serialized.len() <= 256,
                "{} bytes: {text}",
                serialized.len()
            );
            assert!(text.ends_with("1000 of 1000 series downsampled"), "{text}");
        }
    }

    /// Thirteen series cut to one by `max_rows`, the kept one named with
    /// `len` bytes, packed right up to the byte cap: the response stays
    /// inside the cap once the 12 omitted series, `row_cap_hit` and the
    /// `ok_bounded` status they make are written.
    ///
    /// FLIP: fitting with `fit` and folding the row cap in afterwards puts
    /// the widest of these 8 B over the cap: `rows_omitted` goes from `0` to
    /// `12` and the status from `ok` to `ok_bounded`.
    #[test]
    fn a_row_capped_result_at_its_byte_cap_finishes_inside_it() {
        let key = key();
        let mut ctx = test_context(&key);
        ctx.budgets.max_rows = 1;
        ctx.budgets.max_response_bytes = MAX_RESPONSE_BYTES_FLOOR;
        let run_with = |len: usize| {
            let names: Vec<String> = (0..13)
                .map(|n| if n == 0 { "x".repeat(len) } else { format!("s{n:02}") })
                .collect();
            let series: Vec<(&str, u64, bool)> =
                names.iter().map(|name| (name.as_str(), 60, false)).collect();
            let mut answer = change_point_rows(&series);
            answer.visibility.resolved_no_segments = true;
            let stub = answering(vec![Ok(answer)]);
            block_on(run(
                args(START_NS, END_NS, "15s", "change_point"),
                &ctx,
                &stub,
            ))
        };
        assert_finished_inside_cap(MAX_RESPONSE_BYTES_FLOOR, 12, run_with);
    }

    /// Partial coverage is refused: the service's refusal is rewritten to
    /// say this tool takes no consent, keeping the skipped clusters it
    /// named, with next steps to retry or to read the partial series through
    /// `ravel_query_promql`; and an answer that reports partial coverage
    /// anyway is refused the same way.
    ///
    /// FLIP: passing the service's message through names `allow_partial`, a
    /// parameter this tool does not take; returning a partial answer as
    /// `ok` fails the second call's status.
    #[test]
    fn partial_coverage_is_refused_with_a_route_to_the_partial_read() {
        let key = key();
        let ctx = test_context(&key);
        let mut partial = change_point_rows(&[("m", 60, false)]);
        partial.coverage.partial = true;
        partial.coverage.complete = false;
        let stub = answering(vec![Err(service_partial_refusal()), Ok(partial)]);
        for _ in 0..2 {
            let envelope = block_on(run(
                args(START_NS, END_NS, "15s", "change_point"),
                &ctx,
                &stub,
            ));
            let failure = failure_of(&envelope);
            assert_eq!(failure.class, FailureClass::Unavailable);
            assert!(
                !failure.message.contains("allow_partial"),
                "{}",
                failure.message
            );
            assert!(
                failure.message.ends_with(
                    "takes no partial-coverage consent, so it does not analyze a partial read"
                ),
                "{}",
                failure.message
            );
            let actions: Vec<&str> = envelope
                .next_steps
                .iter()
                .map(|step| step.action.as_str())
                .collect();
            assert_eq!(actions, vec![TOOL, "ravel_query_promql"]);
            assert!(
                envelope.next_steps[1]
                    .detail
                    .contains("allow_partial_coverage: true")
            );
        }
        // The clusters the service named survive the rewrite.
        let stub = answering(vec![Err(service_partial_refusal())]);
        let envelope = block_on(run(
            args(START_NS, END_NS, "15s", "change_point"),
            &ctx,
            &stub,
        ));
        assert!(failure_of(&envelope).message.contains("(eu: unreachable)"));
    }

    /// The service's partial refusal, built by the function the HTTP route's
    /// gate calls rather than copied from it.
    fn service_partial_refusal() -> Failure {
        Failure {
            class: FailureClass::Unavailable,
            message: ravel_query::http::partial_refusal_message(&["eu: unreachable".into()]),
            counter: None,
        }
    }

    /// The rewrite matches the service's own remedy clause: over
    /// `partial_refusal_message` the whole clause is replaced by this tool's
    /// rule, nothing of `allow_partial` is left in the message, and the next
    /// steps name `ravel_query_promql`'s `allow_partial_coverage`.
    ///
    /// FLIP: matching a hardcoded copy of the remedy clause that differs
    /// from `PARTIAL_REFUSAL_REMEDY` (the copy after the service rewords
    /// it) replaces nothing and leaves `set allow_partial=true` in the
    /// message; recognising the refusal by a prefix other than
    /// `PARTIAL_REFUSAL_PREFIX` skips the rewrite and the next steps.
    #[test]
    fn the_partial_rewrite_follows_the_services_own_wording() {
        let service = service_partial_refusal().message;
        assert!(service.starts_with(PARTIAL_REFUSAL_PREFIX), "{service}");
        assert!(
            service.ends_with(ravel_query::http::service::PARTIAL_REFUSAL_REMEDY),
            "{service}"
        );

        let key = key();
        let ctx = test_context(&key);
        let stub = answering(vec![Err(service_partial_refusal())]);
        let envelope = block_on(run(
            args(START_NS, END_NS, "15s", "change_point"),
            &ctx,
            &stub,
        ));
        let failure = failure_of(&envelope);
        assert_eq!(
            failure.message,
            format!(
                "{PARTIAL_REFUSAL_PREFIX} coverage is incomplete (eu: unreachable); {}",
                no_partial_consent()
            )
        );
        assert!(
            !failure.message.contains("allow_partial"),
            "{}",
            failure.message
        );
        let actions: Vec<&str> = envelope
            .next_steps
            .iter()
            .map(|step| step.action.as_str())
            .collect();
        assert_eq!(actions, vec![TOOL, "ravel_query_promql"]);
        assert!(
            envelope.next_steps[1]
                .detail
                .contains("allow_partial_coverage: true")
        );
    }

    /// A port answer in another row shape is an internal failure rather than
    /// rows read under the wrong column names.
    ///
    /// FLIP: skipping the column check reports a summary's rows as
    /// change-point rows.
    #[test]
    fn a_row_shape_for_the_other_op_is_internal() {
        let key = key();
        let ctx = test_context(&key);
        let stub = answering(vec![Ok(change_point_rows(&[("m", 60, false)]))]);
        let envelope = block_on(run(args(START_NS, END_NS, "15s", "summary"), &ctx, &stub));
        assert_eq!(failure_of(&envelope).class, FailureClass::Internal);
    }
}
