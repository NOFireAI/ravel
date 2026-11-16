//! `ravel_explain_query`: validate one SQL statement, resolve its snapshot,
//! and compare the cost estimate against the call's effective budget, without
//! scanning anything (ADR-1374 D2, D6).
//!
//! The statement is SQL only (2026-09-12 amendment, item 2). Validation and
//! the one-table rule both read the statement text alone, so a statement
//! either refuses before the port call or reaches
//! [`QueryBackend::sql_explain`] exactly once.
//!
//! The explain runs with no per-request budgets, under the server's own
//! ceilings: the D6 comparison below is the check against the call's
//! effective budget, and an engine refusal at a lowered `max_segments` would
//! answer the same question with no narrowing factor.

use ravel_query::{ByteLimit, RequestLimit};
use ravel_sql::{ErrorClass, SqlError, SqlRequest, TargetSignal, ValidationError};
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::catalog::ExplainQueryInput;
use crate::draft::{EstimateComponent, ExplainOutcome};
use crate::envelope::{
    AnyJson, Budget, Column, Envelope, Failure, FailureClass, NextStep, Status, TimeRange,
};
use crate::service::QueryBackend;

use super::ToolContext;
use super::capabilities::{budget_ceilings, refused_with};

/// Run the tool. Every refusal is an envelope; only a statement that passes
/// validation and the one-table rule reaches `backend`.
pub async fn run<B: QueryBackend>(args: Value, ctx: &ToolContext<'_>, backend: &B) -> Envelope {
    let request = match request(args, ctx) {
        Ok(request) => request,
        Err((failure, next_steps)) => return refused_with(failure, next_steps, ctx),
    };
    match backend.sql_explain(ctx.tenant_hash, &request).await {
        Ok(outcome) => render(&request, outcome, ctx),
        Err(failure) => refused_with(failure, Vec::new(), ctx),
    }
}

type Refusal = (Failure, Vec<NextStep>);

fn request(args: Value, ctx: &ToolContext<'_>) -> Result<SqlRequest, Refusal> {
    for name in ["query", "time_range"] {
        if args.get(name).is_none_or(Value::is_null) {
            return Err((
                failure(
                    FailureClass::MissingArgument,
                    format!("ravel_explain_query requires {name}"),
                ),
                Vec::new(),
            ));
        }
    }
    let input = ExplainQueryInput::deserialize(args).map_err(|err| {
        (
            failure(
                FailureClass::InvalidArgument,
                format!("ravel_explain_query arguments: {err}"),
            ),
            Vec::new(),
        )
    })?;
    let window = crate::time::window(&input.time_range).map_err(|f| (f, Vec::new()))?;
    validate(&input.query)?;
    ravel_sql::refuse_several_tables(&input.query).map_err(|error| {
        (
            sql_failure(&error),
            vec![step(
                "split_the_statement",
                "explain one table per statement; each statement's estimate is \
                 compared against the budget on its own",
            )],
        )
    })?;
    Ok(SqlRequest {
        sql: input.query,
        window,
        min_tokens: Vec::new(),
        now_ns: ctx.now_ns,
        deadline: ctx.budgets.deadline,
        row_window: true,
        max_rows: None,
        budgets: None,
    })
}

/// A write or any other non-query statement is `unsupported`; any other
/// validation failure is `validation` with the engine's text, which may be a
/// PromQL expression sent to the SQL tool.
fn validate(sql: &str) -> Result<(), Refusal> {
    match ravel_sql::validate_query(sql) {
        Ok(()) => Ok(()),
        Err(
            error @ (ValidationError::NotReadOnly { .. } | ValidationError::WriteInQuery { .. }),
        ) => Err((
            failure(
                FailureClass::Unsupported,
                format!("ravel_explain_query explains a read-only SELECT only: {error}"),
            ),
            Vec::new(),
        )),
        Err(error) => Err((
            failure(FailureClass::Validation, error.to_string()),
            vec![step(
                "ravel_query_promql",
                "ravel_explain_query takes SQL only; a PromQL expression runs through \
                 ravel_query_promql",
            )],
        )),
    }
}

/// The D4 class of an engine refusal, the same mapping the server's SQL
/// surface applies, with the engine's client-safe text.
pub(super) fn sql_failure(error: &SqlError) -> Failure {
    let class = match error.class() {
        ErrorClass::BadRequest => FailureClass::InvalidArgument,
        ErrorClass::Unsupported => FailureClass::Unsupported,
        ErrorClass::Unavailable => FailureClass::Unavailable,
        ErrorClass::Timeout => FailureClass::Deadline,
        ErrorClass::Internal => FailureClass::Internal,
    };
    failure(class, error.client_message())
}

/// One estimate figure against the ceiling that bounds it.
#[derive(Debug, Clone, Copy, PartialEq)]
struct Comparison {
    /// The `budget.estimate` key.
    estimate: &'static str,
    /// The `budget.effective` key.
    ceiling: &'static str,
    /// `None` when the estimator cannot bound the figure.
    estimated: Option<u64>,
    /// `None` when the ceiling is unlimited.
    limit: Option<u64>,
}

impl Comparison {
    /// Over the ceiling. An unbounded figure is never read as zero: against
    /// a ceiling it is over (2026-09-12 amendment, item 1).
    fn exceeds(&self) -> bool {
        match (self.estimated, self.limit) {
            (_, None) => false,
            (None, Some(_)) => true,
            (Some(estimated), Some(limit)) => estimated > limit,
        }
    }

    /// How many times over its ceiling a bounded figure is. `None` when it
    /// is not over, is unbounded, or the ceiling is zero.
    fn ratio(&self) -> Option<f64> {
        match (self.estimated, self.limit) {
            (Some(estimated), Some(limit)) if limit > 0 && estimated > limit => {
                Some(estimated as f64 / limit as f64)
            }
            _ => None,
        }
    }

    fn to_json(self) -> Value {
        json!({
            "estimate": self.estimate,
            "ceiling": self.ceiling,
            "estimated": self.estimated,
            "limit": self.limit,
            "exceeds": self.exceeds(),
        })
    }
}

/// The three D6 comparisons, in the order `EffectiveBudgets` declares its
/// ceilings. Segments compare the admitted count, the one the engine checks
/// against `max_segments`; recent segments are exempt from that ceiling.
fn comparisons(outcome: &ExplainOutcome, ctx: &ToolContext<'_>) -> [Comparison; 3] {
    let bounded = |component: EstimateComponent, figure: u64| {
        (!outcome.unbounded.contains(&component)).then_some(figure)
    };
    let query = &ctx.budgets.query;
    [
        Comparison {
            estimate: "bytes_scanned",
            ceiling: "max_bytes_scanned",
            estimated: bounded(
                EstimateComponent::StoreBytes,
                outcome.estimate.estimated_store_bytes,
            ),
            limit: match query.max_bytes_scanned {
                ByteLimit::Bounded(bytes) => Some(bytes),
                ByteLimit::Unlimited => None,
            },
        },
        Comparison {
            estimate: "store_requests",
            ceiling: "max_store_requests",
            estimated: bounded(
                EstimateComponent::Requests,
                outcome.estimate.estimated_requests,
            ),
            limit: match query.max_store_requests {
                RequestLimit::Bounded(requests) => Some(requests),
                RequestLimit::Unlimited => None,
            },
        },
        Comparison {
            estimate: "segments_admitted",
            ceiling: "max_segments",
            estimated: Some(outcome.segments_admitted),
            limit: Some(query.max_segments as u64),
        },
    ]
}

/// The factor to narrow by: the largest ratio among the bounded figures over
/// their ceiling, since narrowing by less leaves that one still over.
fn narrowing_factor(comparisons: &[Comparison]) -> Option<f64> {
    comparisons
        .iter()
        .filter_map(Comparison::ratio)
        .reduce(f64::max)
}

/// Parquet's request and byte figures are unbounded and have ceilings, and
/// the Parquet reader checks both before every GET at scan time, so they
/// warn rather than refuse (ADR-1374 amendment 2026-11-02).
fn refuses(comparison: &Comparison, target: TargetSignal) -> bool {
    comparison.exceeds() && !(comparison.estimated.is_none() && target == TargetSignal::Parquet)
}

fn render(request: &SqlRequest, outcome: ExplainOutcome, ctx: &ToolContext<'_>) -> Envelope {
    let comparisons = comparisons(&outcome, ctx);
    let factor = narrowing_factor(&comparisons);
    let estimate = estimate_json(&outcome, &comparisons, factor);
    let upper_envelope = outcome.unbounded.is_empty();
    let unchecked: Vec<String> = comparisons
        .iter()
        .filter(|comparison| comparison.exceeds() && !refuses(comparison, outcome.target))
        .map(|comparison| {
            format!(
                "{} is not checked by this explain: the Parquet reader enforces it at \
                 scan time, refusing the read that would cross it",
                comparison.ceiling
            )
        })
        .collect();
    let over: Vec<&Comparison> = comparisons
        .iter()
        .filter(|comparison| refuses(comparison, outcome.target))
        .collect();
    if !over.is_empty() {
        return over_budget(&over, factor, estimate, upper_envelope, unchecked, ctx);
    }

    let (signal, table) = scope_names(outcome.target);
    let mut warnings = Vec::new();
    for component in &outcome.unbounded {
        warnings.push(format!(
            "{} is unbounded for the {table} table: the estimator cannot bound it, so \
             it is reported as null and not as an estimate of zero",
            component.field_name()
        ));
    }
    warnings.extend(unchecked);

    let mut envelope = Envelope::default();
    envelope.scope.signal = signal.to_string();
    envelope.scope.table = table.to_string();
    envelope.scope.time_range = Some(TimeRange {
        start_ns: request.window.start_ns.to_string(),
        end_ns: request.window.end_ns.to_string(),
    });
    envelope.scope.predicates_applied = outcome.window_predicate.iter().cloned().collect();
    envelope.data.columns = outcome
        .columns
        .iter()
        .map(|column| Column {
            name: column.name.clone(),
            r#type: column.data_type.clone(),
        })
        .collect();
    envelope.plan = Some(outcome.plan_text.clone());
    envelope.visibility.resolved_no_segments =
        outcome.segments_resolved == 0 && outcome.target != TargetSignal::Parquet;
    envelope.coverage.complete = true;
    envelope.accuracy.exact = true;
    envelope.budget = Budget {
        effective: AnyJson(budget_ceilings(ctx)),
        actual: AnyJson(Value::Object(Map::new())),
        estimate: AnyJson(estimate),
        estimate_is_upper_envelope: upper_envelope,
    };
    envelope.warnings = warnings;
    envelope.fit(ctx.budgets.max_response_bytes).finish(false)
}

fn over_budget(
    over: &[&Comparison],
    factor: Option<f64>,
    estimate: Value,
    upper_envelope: bool,
    warnings: Vec<String>,
    ctx: &ToolContext<'_>,
) -> Envelope {
    let figures: Vec<String> = over
        .iter()
        .map(
            |comparison| match (comparison.estimated, comparison.limit) {
                (Some(estimated), Some(limit)) => format!(
                    "{} {estimated} > {} {limit}",
                    comparison.estimate, comparison.ceiling
                ),
                _ => format!(
                    "{} is unbounded against {} {}",
                    comparison.estimate,
                    comparison.ceiling,
                    comparison.limit.unwrap_or_default()
                ),
            },
        )
        .collect();
    let narrow = match factor {
        Some(factor) => format!("narrow by a factor of at least {factor}"),
        None => "no narrowing factor bounds an unbounded figure or a zero ceiling".to_string(),
    };
    let envelope = Envelope {
        status: Status::Error,
        failure: Some(failure(
            FailureClass::BudgetEstimateExceedsCeiling,
            format!(
                "the estimate exceeds the effective budget: {}; {narrow}",
                figures.join(", ")
            ),
        )),
        budget: Budget {
            effective: AnyJson(budget_ceilings(ctx)),
            actual: AnyJson(Value::Object(Map::new())),
            estimate: AnyJson(estimate),
            estimate_is_upper_envelope: upper_envelope,
        },
        warnings,
        next_steps: vec![step(
            "narrow_the_query",
            "shorten time_range or add predicates on indexed columns, then explain again",
        )],
        ..Envelope::default()
    };
    envelope.fit(ctx.budgets.max_response_bytes).finish(false)
}

/// The estimate in the spelling the other tools' `budget.estimate` uses, with
/// an unbounded figure as `null`, plus the comparisons that were run.
fn estimate_json(
    outcome: &ExplainOutcome,
    comparisons: &[Comparison],
    factor: Option<f64>,
) -> Value {
    let bounded = |component: EstimateComponent, figure: u64| {
        (!outcome.unbounded.contains(&component)).then_some(figure)
    };
    json!({
        "bytes_scanned": bounded(EstimateComponent::StoreBytes, outcome.estimate.estimated_store_bytes),
        "store_requests": bounded(EstimateComponent::Requests, outcome.estimate.estimated_requests),
        "decompressed_bytes": bounded(
            EstimateComponent::DecompressedBytes,
            outcome.estimate.estimated_decompressed_bytes,
        ),
        "segments_read": outcome.estimate.segments,
        "series": outcome.estimate.series,
        "segments_resolved": outcome.segments_resolved,
        "segments_admitted": outcome.segments_admitted,
        "segments_recent_exempt": outcome.segments_recent_exempt,
        "unbounded": outcome
            .unbounded
            .iter()
            .map(|component| component.field_name())
            .collect::<Vec<_>>(),
        "comparisons": comparisons.iter().map(|comparison| comparison.to_json()).collect::<Vec<_>>(),
        "narrowing_factor": factor,
    })
}

fn scope_names(target: TargetSignal) -> (&'static str, &'static str) {
    match target {
        TargetSignal::Metrics => ("metrics", "samples"),
        TargetSignal::Logs => ("logs", "logs"),
        TargetSignal::Spans => ("traces", "spans"),
        TargetSignal::Alerts => ("alerts", "alerts"),
        TargetSignal::Audit => ("audit", "audit"),
        TargetSignal::Parquet => ("parquet", "parquet"),
    }
}

fn failure(class: FailureClass, message: String) -> Failure {
    Failure {
        class,
        message,
        counter: None,
    }
}

fn step(action: &str, detail: &str) -> NextStep {
    NextStep {
        action: action.to_string(),
        detail: detail.to_string(),
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use ravel_types::accounting::CostEstimate;

    use super::*;
    use crate::cursor::{CURSOR_KEY_LEN, CursorKey};
    use crate::draft::ExplainColumn;
    use crate::tools::tests::{Stub, block_on, test_context};

    fn outcome(target: TargetSignal, estimate: CostEstimate) -> ExplainOutcome {
        ExplainOutcome {
            target,
            plan_text: "ProjectionExec\n  RavelScan".to_string(),
            window_predicate: Some("ts >= 1000 AND ts < 2000".to_string()),
            segments_resolved: 4,
            segments_admitted: 3,
            segments_recent_exempt: 1,
            estimate,
            unbounded: Vec::new(),
            columns: vec![ExplainColumn {
                name: "ts".to_string(),
                data_type: "Timestamp(Nanosecond, None)".to_string(),
            }],
        }
    }

    fn args(query: &str) -> Value {
        json!({
            "query": query,
            "time_range": {"start_ns": "1000", "end_ns": "2000"},
        })
    }

    /// Run the tool with requests, bytes and segments bounded at 100, 1000
    /// and 50.
    fn explain(stub: &Stub, query: &str) -> Envelope {
        let key = CursorKey::from_process_secret([21u8; CURSOR_KEY_LEN]);
        let mut ctx = test_context(&key);
        ctx.budgets.query.max_store_requests = RequestLimit::Bounded(100);
        ctx.budgets.query.max_bytes_scanned = ByteLimit::Bounded(1_000);
        ctx.budgets.query.max_segments = 50;
        block_on(run(args(query), &ctx, stub))
    }

    fn failure_of(envelope: &Envelope) -> &Failure {
        assert_eq!(envelope.status, Status::Error);
        envelope
            .failure
            .as_ref()
            .expect("an error envelope carries one")
    }

    /// Two figures over their ceilings by different ratios: requests 300
    /// against 100 (3x) and store bytes 2000 against 1000 (2x). The factor
    /// is the larger ratio, 3, because narrowing by 2 leaves requests over.
    /// The second case makes bytes the larger ratio (5000 against 1000, 5x;
    /// requests 200, 2x), so a factor read from one fixed dimension fails
    /// one of the two.
    ///
    /// Every figure is bounded, so the refused estimate is still an upper
    /// envelope.
    ///
    /// FLIP: summing the ratios gives 5 and 7; taking the product gives 6
    /// and 10; taking the minimum gives 2 and 2; reading requests alone
    /// gives 3 and 2.
    /// FLIP: a refusal that hardcodes `estimate_is_upper_envelope: false`
    /// contradicts its own empty `budget.estimate.unbounded`.
    #[test]
    fn the_narrowing_factor_is_the_largest_ratio_over_its_ceiling() {
        for (requests, bytes, factor) in [(300, 2_000, 3.0), (200, 5_000, 5.0)] {
            let stub = Stub {
                explain: Some(outcome(
                    TargetSignal::Metrics,
                    CostEstimate::new(requests, bytes, 0, 3, 7),
                )),
                ..Stub::default()
            };
            let envelope = explain(&stub, "SELECT ts FROM samples");
            let failure = failure_of(&envelope);
            assert_eq!(failure.class, FailureClass::BudgetEstimateExceedsCeiling);
            assert!(
                failure
                    .message
                    .ends_with(&format!("narrow by a factor of at least {factor}")),
                "{}",
                failure.message
            );
            assert_eq!(
                envelope.budget.estimate.0["narrowing_factor"],
                json!(factor)
            );
            assert_eq!(envelope.next_steps[0].action, "narrow_the_query");
            assert_eq!(envelope.budget.estimate.0["unbounded"], json!([]));
            assert!(envelope.budget.estimate_is_upper_envelope);
            let exceeded: Vec<&str> = envelope.budget.estimate.0["comparisons"]
                .as_array()
                .expect("comparisons")
                .iter()
                .filter(|comparison| comparison["exceeds"] == json!(true))
                .map(|comparison| comparison["ceiling"].as_str().expect("ceiling"))
                .collect();
            assert_eq!(exceeded, ["max_bytes_scanned", "max_store_requests"]);
        }
    }

    /// Segments compare the admitted count against `max_segments`: 60
    /// admitted against 50 is a factor of 1.2 on its own.
    ///
    /// FLIP: comparing `segments_resolved` (10 here) passes this statement.
    #[test]
    fn admitted_segments_over_max_segments_refuse() {
        let mut over = outcome(TargetSignal::Metrics, CostEstimate::new(1, 1, 0, 60, 1));
        over.segments_resolved = 10;
        over.segments_admitted = 60;
        let stub = Stub {
            explain: Some(over),
            ..Stub::default()
        };
        let envelope = explain(&stub, "SELECT ts FROM samples");
        assert_eq!(
            failure_of(&envelope).class,
            FailureClass::BudgetEstimateExceedsCeiling
        );
        assert_eq!(envelope.budget.estimate.0["narrowing_factor"], json!(1.2));
    }

    /// A logs explain whose decompressed bytes the estimator cannot bound:
    /// no ceiling holds that component, so it is named unbounded, the
    /// estimate is not an upper envelope, and the call is not refused. The
    /// structural zero is reported as null.
    ///
    /// FLIP: treating every unbounded component as over a ceiling refuses
    /// this call; reading it as zero reports `decompressed_bytes: 0` and an
    /// upper envelope of true.
    #[test]
    fn a_logs_explain_with_unbounded_decompressed_bytes_does_not_refuse() {
        let mut logs = outcome(TargetSignal::Logs, CostEstimate::new(40, 900, 0, 3, 0));
        logs.unbounded = vec![EstimateComponent::DecompressedBytes];
        let stub = Stub {
            explain: Some(logs),
            ..Stub::default()
        };
        let envelope = explain(&stub, "SELECT ts FROM logs");

        assert_eq!(envelope.status, Status::Ok, "{:?}", envelope.failure);
        assert!(!envelope.budget.estimate_is_upper_envelope);
        let estimate = &envelope.budget.estimate.0;
        assert_eq!(estimate["decompressed_bytes"], Value::Null);
        assert_eq!(estimate["bytes_scanned"], json!(900));
        assert_eq!(estimate["store_requests"], json!(40));
        assert_eq!(
            estimate["unbounded"],
            json!(["estimated_decompressed_bytes"])
        );
        assert!(
            envelope
                .warnings
                .iter()
                .any(|w| w.starts_with("estimated_decompressed_bytes is unbounded for the logs")),
            "{:?}",
            envelope.warnings
        );
        assert_eq!(envelope.scope.table, "logs");
    }

    /// A Parquet explain: requests and store bytes are unbounded and both
    /// have ceilings, and the Parquet reader enforces those ceilings at scan
    /// time, so the explain names them and warns instead of refusing. Run
    /// through the stub backend: the e2e test's MemoryStore tenant has no
    /// Parquet table registered.
    ///
    /// FLIP: applying the unknown-with-a-ceiling refusal to Parquet returns
    /// `budget_estimate_exceeds_ceiling` here; dropping the warning leaves
    /// the ceilings unmentioned.
    #[test]
    fn a_parquet_explain_names_its_unbounded_figures_and_warns() {
        let mut parquet = outcome(TargetSignal::Parquet, CostEstimate::new(0, 0, 0, 0, 0));
        parquet.unbounded = vec![
            EstimateComponent::Requests,
            EstimateComponent::StoreBytes,
            EstimateComponent::DecompressedBytes,
        ];
        parquet.segments_resolved = 0;
        parquet.segments_admitted = 0;
        let stub = Stub {
            explain: Some(parquet),
            ..Stub::default()
        };
        let envelope = explain(&stub, "SELECT name FROM sales");

        assert_eq!(envelope.status, Status::Ok, "{:?}", envelope.failure);
        assert!(!envelope.budget.estimate_is_upper_envelope);
        let unbounded = &envelope.budget.estimate.0["unbounded"];
        assert_eq!(
            unbounded,
            &json!([
                "estimated_requests",
                "estimated_store_bytes",
                "estimated_decompressed_bytes"
            ])
        );
        assert_eq!(envelope.budget.estimate.0["store_requests"], Value::Null);
        assert_eq!(envelope.budget.estimate.0["bytes_scanned"], Value::Null);
        for ceiling in ["max_bytes_scanned", "max_store_requests"] {
            let expected = format!(
                "{ceiling} is not checked by this explain: the Parquet reader enforces it \
                 at scan time, refusing the read that would cross it"
            );
            assert!(
                envelope.warnings.contains(&expected),
                "{:?}",
                envelope.warnings
            );
        }
        assert!(!envelope.visibility.resolved_no_segments);
    }

    /// A Parquet explain refused for a second reason, admitted segments over
    /// `max_segments`, still names both unchecked Parquet ceilings, and its
    /// estimate is not an upper envelope.
    ///
    /// FLIP: returning the refusal before the unchecked-ceiling warnings are
    /// built leaves `warnings` empty.
    #[test]
    fn a_parquet_refusal_still_names_its_unchecked_ceilings() {
        let mut parquet = outcome(TargetSignal::Parquet, CostEstimate::new(0, 0, 0, 60, 0));
        parquet.unbounded = vec![EstimateComponent::Requests, EstimateComponent::StoreBytes];
        parquet.segments_admitted = 60;
        let stub = Stub {
            explain: Some(parquet),
            ..Stub::default()
        };
        let envelope = explain(&stub, "SELECT name FROM sales");
        let failure = failure_of(&envelope);
        assert_eq!(failure.class, FailureClass::BudgetEstimateExceedsCeiling);
        assert!(
            failure
                .message
                .contains("segments_admitted 60 > max_segments 50"),
            "{}",
            failure.message
        );
        assert!(!envelope.budget.estimate_is_upper_envelope);
        let expected: Vec<String> = ["max_bytes_scanned", "max_store_requests"]
            .iter()
            .map(|ceiling| {
                format!(
                    "{ceiling} is not checked by this explain: the Parquet reader enforces \
                     it at scan time, refusing the read that would cross it"
                )
            })
            .collect();
        assert_eq!(envelope.warnings, expected);
    }

    /// The same unbounded store-byte figure on a signal table, against a
    /// ceiling, refuses: only Parquet's scan-time enforcement exempts it.
    /// With a component unbounded, the refused estimate is not an upper
    /// envelope.
    ///
    /// FLIP: exempting every target refuses nothing here.
    /// FLIP: a refusal that reports `estimate_is_upper_envelope: true`
    /// regardless of `unbounded` fails the last assertion.
    #[test]
    fn an_unbounded_figure_against_a_ceiling_refuses_off_parquet() {
        let mut spans = outcome(TargetSignal::Spans, CostEstimate::new(1, 0, 0, 1, 0));
        spans.unbounded = vec![EstimateComponent::StoreBytes];
        let stub = Stub {
            explain: Some(spans),
            ..Stub::default()
        };
        let envelope = explain(&stub, "SELECT trace_id FROM spans");
        let failure = failure_of(&envelope);
        assert_eq!(failure.class, FailureClass::BudgetEstimateExceedsCeiling);
        assert!(
            failure
                .message
                .contains("bytes_scanned is unbounded against max_bytes_scanned 1000"),
            "{}",
            failure.message
        );
        assert!(!envelope.budget.estimate_is_upper_envelope);
    }

    /// A statement inside every ceiling: the schema is `data.columns` with
    /// no rows, the plan is `plan`, the estimate sits beside `effective`,
    /// and the request reached the port with `row_window` set and no
    /// per-request budgets. No rows are returned and none are capped, so
    /// `presentation.max_rows` is 0.
    ///
    /// FLIP: putting the plan into `data.rows` or dropping `row_window`
    /// fails the matching assertion.
    /// FLIP: reporting the effective `max_rows` (200) as
    /// `presentation.max_rows` advertises a row cap the explain never
    /// applies.
    #[test]
    fn an_explain_inside_the_budget_places_schema_plan_and_estimate() {
        let stub = Stub {
            explain: Some(outcome(
                TargetSignal::Metrics,
                CostEstimate::new(100, 1_000, 5_000, 3, 7),
            )),
            ..Stub::default()
        };
        let envelope = explain(&stub, "SELECT ts FROM samples");

        assert_eq!(envelope.status, Status::Ok, "{:?}", envelope.failure);
        assert_eq!(envelope.data.columns.len(), 1);
        assert_eq!(envelope.data.columns[0].name, "ts");
        assert!(envelope.data.rows.is_empty());
        assert_eq!(envelope.presentation.max_rows, 0);
        assert_eq!(
            envelope.plan.as_deref(),
            Some("ProjectionExec\n  RavelScan")
        );
        assert!(envelope.budget.estimate_is_upper_envelope);
        assert_eq!(
            envelope.budget.effective.0["max_store_requests"],
            json!(100)
        );
        assert_eq!(envelope.budget.estimate.0["store_requests"], json!(100));
        assert_eq!(envelope.budget.estimate.0["narrowing_factor"], Value::Null);
        assert_eq!(
            envelope.scope.predicates_applied,
            vec!["ts >= 1000 AND ts < 2000".to_string()]
        );
        assert!(!envelope.visibility.resolved_no_segments);

        let explained = stub.explained.borrow();
        assert_eq!(explained.len(), 1);
        assert!(explained[0].row_window);
        assert!(explained[0].budgets.is_none());
        assert_eq!(
            explained[0].window,
            ravel_types::TimeRange {
                start_ns: 1000,
                end_ns: 2000
            }
        );
    }

    /// A join of two signal tables, and of a signal table with another
    /// table, refuse with the engine's own class and text before the port
    /// is called: the stub panics if `sql_explain` is reached.
    ///
    /// FLIP: checking the table count after the port call (or not at all)
    /// panics in the stub.
    #[test]
    fn a_statement_naming_two_tables_refuses_before_the_port() {
        for (sql, error) in [
            (
                "SELECT * FROM logs JOIN spans ON logs.trace_id = spans.trace_id",
                SqlError::CrossSignalQuery,
            ),
            (
                "SELECT * FROM logs JOIN sales ON logs.body = sales.name",
                SqlError::OneTablePerStatement,
            ),
        ] {
            let stub = Stub::default();
            let envelope = explain(&stub, sql);
            let failure = failure_of(&envelope);
            assert_eq!(failure.class, FailureClass::InvalidArgument, "{sql}");
            assert_eq!(failure.message, error.client_message(), "{sql}");
            assert_eq!(envelope.next_steps[0].action, "split_the_statement");
            assert!(stub.explained.borrow().is_empty());
        }
    }

    /// A write is `unsupported`, before the port.
    ///
    /// FLIP: mapping every validation error to `validation` returns that
    /// class here.
    #[test]
    fn insert_is_unsupported() {
        let stub = Stub::default();
        let envelope = explain(&stub, "INSERT INTO logs (body) VALUES ('x')");
        assert_eq!(failure_of(&envelope).class, FailureClass::Unsupported);
    }

    /// A PromQL expression sent to the SQL tool fails validation, and the
    /// next step names the tool that runs it.
    ///
    /// FLIP: an empty `next_steps`, or one naming another tool, fails this.
    #[test]
    fn a_validation_failure_points_at_ravel_query_promql() {
        let stub = Stub::default();
        let envelope = explain(&stub, "rate(http_requests_total[5m])");
        let failure = failure_of(&envelope);
        assert_eq!(failure.class, FailureClass::Validation);
        assert_eq!(envelope.next_steps.len(), 1);
        assert_eq!(envelope.next_steps[0].action, "ravel_query_promql");
    }

    /// The window is the shared time parser's: an RFC 3339 window is
    /// accepted, and an absent one is `missing_argument`.
    #[test]
    fn time_range_is_parsed_by_the_shared_parser() {
        let key = CursorKey::from_process_secret([22u8; CURSOR_KEY_LEN]);
        let ctx = test_context(&key);
        let parsed = request(
            json!({
                "query": "SELECT ts FROM samples",
                "time_range": {
                    "start_ns": "2026-09-12T10:00:00Z",
                    "end_ns": "2026-09-12T10:00:01Z",
                },
            }),
            &ctx,
        )
        .expect("valid arguments");
        assert_eq!(parsed.window.start_ns, 1_789_207_200_000_000_000);
        assert_eq!(parsed.window.end_ns, 1_789_207_201_000_000_000);

        let (failure, _) =
            request(json!({"query": "SELECT ts FROM samples"}), &ctx).expect_err("refused");
        assert_eq!(failure.class, FailureClass::MissingArgument);
    }

    /// The Parquet exemption in [`refuses`] is the one ADR-1374's 2026-11-02
    /// amendment records: the amendment, its marker, both citations it rests
    /// on, and the index row that names it.
    ///
    /// FLIP: dropping the amendment, its marker or a citation, or leaving the
    /// index row unamended, fails the matching assertion.
    #[test]
    fn the_parquet_exemption_is_recorded_in_adr_1374() {
        let adr = include_str!("../../../../docs/adrs/1374-agent-mcp-server.md");
        let index = include_str!("../../../../docs/adrs/README.md");
        for needle in [
            "## Amendment (2026-11-02): a Parquet explain names its unbounded figures and does not refuse",
            "<!-- amendment-applies: sections=\"D6. Budgets|Consequences\" pointer=\"Parquet explain amendment\" -->",
            "`crates/ravel-parquet/src/limits.rs`, module doc lines 14 to 24, `admit`\n   at line 171",
            "`unbounded_estimate_components` in\n   `crates/ravel-sql/src/executor.rs`",
            "15,652 bytes with the nine tools and 23,541 bytes",
        ] {
            assert!(adr.contains(needle), "ADR-1374 lacks {needle:?}");
        }
        let row = index
            .lines()
            .find(|line| line.starts_with("| [1374](1374-agent-mcp-server.md) |"))
            .expect("the index has a row for ADR-1374");
        assert!(
            row.contains("amended 2026-11-02: a Parquet explain"),
            "{row}"
        );
    }
}
