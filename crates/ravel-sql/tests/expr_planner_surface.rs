//! The SQL syntax that DataFusion's expression planners admit, pinned per
//! syntax against the pinned DataFusion version (issue #2476).
//!
//! An `ExprPlanner` rewrites syntax into a function call built from the
//! function value, so the per-registry allowlist in
//! `ravel_sql::session::build_session` never sees the rewrite. The set of
//! registered planners is pinned by the session unit test
//! `registered_expr_planners_are_pinned_for_every_table`; this file pins what
//! that set does. Rust cannot list which trait methods a planner overrides, so
//! each `ExprPlanner` method that corresponds to SQL syntax gets one canary
//! statement here with the outcome it has today. A DataFusion upgrade that
//! teaches a registered planner a new syntax, or drops one, flips a row.
//!
//! Every statement goes through `SqlExecutor::execute`, the entry point an
//! `/api/v1/sql` request takes, and a refusal is matched on its `SqlError`
//! variant and `ErrorClass`, never on DataFusion's message text.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use datafusion::arrow::util::display::array_value_to_string;
use ravel_sql::{ErrorClass, SqlError, SqlOutcome};

use crate::util::{self, Fixture, SegSpec, SeriesSpec, tenant_id};

fn tenant() -> ravel_types::TenantId {
    tenant_id("expr-planner-surface-2476")
}

/// One metrics series, so a `samples` canary has rows to plan over.
async fn fixture() -> Fixture {
    let specs = vec![SegSpec::new(
        10,
        1,
        1,
        vec![SeriesSpec::new("a", vec![(1, 1.0), (2, 2.0)])],
    )];
    let tenant = tenant();
    Fixture::memory(&[(&tenant, specs.as_slice())]).await
}

async fn run(fixture: &Fixture, sql: &str) -> Result<SqlOutcome, SqlError> {
    fixture
        .executor
        .execute(tenant().hash(), &util::request(sql))
        .await
}

/// What a canary statement does today.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    /// Plans and executes.
    Plans,
    /// Refused while planning: `SqlError::Plan`, class `Unsupported` (422).
    RefusedAtPlan,
    /// Plans, then refused when the physical plan is built:
    /// `SqlError::Execution`, class `Unsupported` (422).
    RefusedAtExecution,
}

/// Classify a result. Any refusal outside the two expected variants, or with
/// a class other than `Unsupported`, is reported as itself so the table
/// mismatch names it.
fn outcome(result: &Result<SqlOutcome, SqlError>) -> Result<Outcome, String> {
    match result {
        Ok(_) => Ok(Outcome::Plans),
        Err(e) if e.class() != ErrorClass::Unsupported => {
            Err(format!("refused with class {:?}: {e}", e.class()))
        }
        Err(SqlError::Plan(_)) => Ok(Outcome::RefusedAtPlan),
        Err(SqlError::Execution(_)) => Ok(Outcome::RefusedAtExecution),
        Err(e) => Err(format!("refused with an unexpected variant: {e}")),
    }
}

/// The single cell of a one-row, one-column result, rendered as text.
fn single_cell(outcome: &SqlOutcome) -> String {
    assert_eq!(outcome.output.num_rows(), 1, "expected exactly one row");
    let batch = outcome
        .output
        .batches()
        .iter()
        .find(|b| b.num_rows() == 1)
        .expect("a one-row batch");
    assert_eq!(batch.num_columns(), 1, "expected exactly one column");
    array_value_to_string(batch.column(0), 0).expect("cell")
}

fn assert_refused_as_unsupported_plan(sql: &str, result: Result<SqlOutcome, SqlError>) {
    match result {
        Err(e @ SqlError::Plan(_)) => assert_eq!(
            e.class(),
            ErrorClass::Unsupported,
            "{sql}: a planning refusal must report the Unsupported class"
        ),
        Err(e) => panic!("{sql}: refused, but not as SqlError::Plan: {e}"),
        Ok(_) => panic!("{sql}: planned, but the syntax is refused (issue #2476)"),
    }
}

/// `POSITION(x IN y)` stays refused: no registered planner implements
/// `plan_position`. `strpos(y, x)`, the admitted function-call form, plans
/// through the same entry point and answers the 1-based index, so the refusal
/// is the syntax and not the statement shape.
#[tokio::test]
async fn position_in_syntax_is_refused_and_strpos_plans() {
    let f = fixture().await;

    let refused = "SELECT POSITION('a' IN 'ab')";
    assert_refused_as_unsupported_plan(refused, run(&f, refused).await);

    let call = "SELECT strpos('ab', 'a')";
    let planned = run(&f, call)
        .await
        .unwrap_or_else(|e| panic!("{call} must plan: {e}"));
    assert_eq!(single_cell(&planned), "1");
}

/// `SUBSTRING(x FROM y FOR z)` stays refused: no registered planner
/// implements `plan_substring`.
///
/// sqlparser reads an unquoted `substr(...)` or `substring(...)` call into the
/// same AST node as the `FROM ... FOR` form, so those spellings are refused
/// too (pinned in [`expr_planner_syntax_outcomes_are_pinned`]). The
/// double-quoted name `"substr"(x, y, z)` is parsed as an ordinary function
/// call and reaches the admitted `substr` scalar, which is what this control
/// runs.
#[tokio::test]
async fn substring_from_for_syntax_is_refused_and_quoted_substr_plans() {
    let f = fixture().await;

    let refused = "SELECT SUBSTRING('abc' FROM 1 FOR 2)";
    assert_refused_as_unsupported_plan(refused, run(&f, refused).await);

    let call = "SELECT \"substr\"('abc', 1, 2)";
    let planned = run(&f, call)
        .await
        .unwrap_or_else(|e| panic!("{call} must plan: {e}"));
    assert_eq!(single_cell(&planned), "ab");
}

/// One row per SQL syntax an `ExprPlanner` method can claim in DataFusion
/// 54.1, with the outcome each has today. The registered planners and their
/// targets are listed in `registered_expr_planners_are_pinned_for_every_table`.
const CANARIES: [(&str, &str, Outcome); 18] = [
    // plan_binary_op: TraceIdHexLiteralPlanner rewrites this comparison.
    (
        "plan_binary_op (trace_id hex literal)",
        "SELECT start_ts FROM spans WHERE trace_id = '00112233445566778899aabbccddeeff'",
        Outcome::Plans,
    ),
    // plan_binary_op: no registered planner rewrites `@>` (the nested
    // planner would, to `array_has_all`), and no physical operator runs it.
    (
        "plan_binary_op (@>)",
        "SELECT 1 @> 2",
        Outcome::RefusedAtExecution,
    ),
    // plan_field_access: MapFieldAccessPlanner, map subscript.
    (
        "plan_field_access (map subscript)",
        "SELECT attrs['k'] FROM logs",
        Outcome::Plans,
    ),
    // plan_field_access: MapFieldAccessPlanner, string key on a struct.
    (
        "plan_field_access (struct subscript)",
        "SELECT named_struct('a', 1)['a']",
        Outcome::Plans,
    ),
    // plan_field_access: list index; no registered planner handles it.
    (
        "plan_field_access (list index)",
        "SELECT 'abc'[1]",
        Outcome::RefusedAtPlan,
    ),
    // plan_array_literal: no registered planner.
    (
        "plan_array_literal",
        "SELECT [1, 2]",
        Outcome::RefusedAtPlan,
    ),
    // plan_position: no registered planner (UnicodeFunctionPlanner is not
    // registered).
    (
        "plan_position",
        "SELECT POSITION('a' IN 'ab')",
        Outcome::RefusedAtPlan,
    ),
    // plan_dictionary_literal: CoreFunctionPlanner, to `named_struct`.
    (
        "plan_dictionary_literal",
        "SELECT {'a': 1}",
        Outcome::Plans,
    ),
    // plan_extract: DatetimeFunctionPlanner, to `date_part`.
    (
        "plan_extract",
        "SELECT EXTRACT(minute FROM ts) FROM samples",
        Outcome::Plans,
    ),
    // plan_substring: no registered planner.
    (
        "plan_substring (FROM ... FOR)",
        "SELECT SUBSTRING('abc' FROM 1 FOR 2)",
        Outcome::RefusedAtPlan,
    ),
    // plan_substring: sqlparser reads an unquoted `substr(...)` call into the
    // same AST node, so it reaches the same missing planner.
    (
        "plan_substring (unquoted substr call)",
        "SELECT substr('abc', 1, 2)",
        Outcome::RefusedAtPlan,
    ),
    // plan_struct_literal: CoreFunctionPlanner, to `struct`.
    (
        "plan_struct_literal",
        "SELECT STRUCT(1, 2)",
        Outcome::Plans,
    ),
    // plan_struct_literal: CoreFunctionPlanner, to `named_struct`.
    (
        "plan_struct_literal (named)",
        "SELECT STRUCT(1 AS a)",
        Outcome::Plans,
    ),
    // plan_overlay: CoreFunctionPlanner, to `overlay`.
    (
        "plan_overlay",
        "SELECT OVERLAY('abc' PLACING 'x' FROM 2)",
        Outcome::Plans,
    ),
    // plan_make_map: no registered planner.
    (
        "plan_make_map",
        "SELECT MAP {'a': 1}",
        Outcome::RefusedAtPlan,
    ),
    // plan_compound_identifier: CoreFunctionPlanner, to `get_field`.
    (
        "plan_compound_identifier",
        "SELECT s.a FROM (SELECT named_struct('a', 1) AS s)",
        Outcome::Plans,
    ),
    // plan_aggregate: AggregateFunctionPlanner.
    (
        "plan_aggregate",
        "SELECT count() FROM samples",
        Outcome::Plans,
    ),
    // plan_window: WindowFunctionPlanner.
    (
        "plan_window",
        "SELECT count(*) OVER () FROM samples",
        Outcome::Plans,
    ),
];

/// Every canary in [`CANARIES`] has its pinned outcome. All mismatches are
/// collected first, so one upgrade that flips several rows names them all.
#[tokio::test]
async fn expr_planner_syntax_outcomes_are_pinned() {
    let f = fixture().await;

    let mut mismatches = Vec::new();
    for (syntax, sql, expected) in CANARIES {
        let result = run(&f, sql).await;
        match outcome(&result) {
            Ok(got) if got == expected => {}
            Ok(got) => mismatches.push(format!(
                "{syntax}: `{sql}` expected {expected:?}, got {got:?} ({:?})",
                result.err()
            )),
            Err(why) => mismatches.push(format!("{syntax}: `{sql}` {why}")),
        }
    }
    assert!(
        mismatches.is_empty(),
        "expression-planner syntax drifted from the pinned outcomes. A row that \
         now plans widens the SQL surface past the scalar allowlist; decide \
         whether to admit it before updating the table:\n  {}",
        mismatches.join("\n  ")
    );
}
