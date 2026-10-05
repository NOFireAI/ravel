//! The SQL syntax that DataFusion's expression planners admit, pinned per
//! syntax against the pinned DataFusion version (issues #2476 and #2583).
//!
//! Why a planner's rewrite bypasses the scalar allowlist is stated beside the
//! planner registrations in `ravel_sql::session::build_session`. The set of
//! registered planners is pinned by the session unit test
//! `registered_expr_planners_are_pinned_for_every_table`; this file pins what
//! that set does. Rust cannot list which trait methods a planner overrides, so
//! each `ExprPlanner` method that corresponds to SQL syntax gets at least one
//! canary statement here with the outcome it has today, and every canary that
//! plans also names the call its logical plan makes in place of the syntax. A
//! DataFusion upgrade that teaches a registered planner a new syntax under one
//! of these methods, drops one, or changes a rewrite target flips a row. The
//! table covers the 13 methods of DataFusion 54.1 and a test pins that count,
//! so an upgrade that grows the trait has to add a row and raise it.
//!
//! Every statement goes through `SqlExecutor::execute`, the entry point an
//! `/api/v1/sql` request takes, and a refusal is matched on its `SqlError`
//! variant, never on DataFusion's message text. Both refusal variants pinned
//! here, `SqlError::Plan` and `SqlError::Execution`, always carry the
//! `Unsupported` class, so the class check is not a second guard on them: it
//! only names a refusal that arrived as some other variant.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::arrow::util::display::array_value_to_string;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::expr::WindowFunctionDefinition;
use datafusion::logical_expr::{Expr, LogicalPlan};
use ravel_catalog::Snapshot;
use ravel_query::{LogSegmentFetcher, PhaseAccounting, SegmentFetcher};
use ravel_sql::{
    ADMITTED_SCALARS, CeilingBreach, ErrorClass, LogsTableProvider, RavelTableProvider,
    SessionTable, SpanSegmentFetcher, SpansTableProvider, SpillDecision, SqlConfig, SqlError,
    SqlOutcome, TenantDelegatingPool, TenantMemoryAccountant, build_session,
};
use ravel_types::accounting::QueryAccounting;

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

/// What a canary statement does today, as observed through
/// `SqlExecutor::execute`.
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

/// The kind of a function call in a logical plan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Kind {
    Scalar,
    Aggregate,
    /// A function evaluated over a window, aggregate or window UDF alike.
    Window,
}

/// A function call, named by the function value's own `name()`.
type Call = (Kind, &'static str);

/// What a canary row pins: the outcome and, for a statement that plans, the
/// call its syntax is rewritten to (`None` for a rewrite that calls nothing).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Expect {
    Plans(Option<Call>),
    RefusedAtPlan,
    RefusedAtExecution,
}

impl Expect {
    fn outcome(self) -> Outcome {
        match self {
            Expect::Plans(_) => Outcome::Plans,
            Expect::RefusedAtPlan => Outcome::RefusedAtPlan,
            Expect::RefusedAtExecution => Outcome::RefusedAtExecution,
        }
    }
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

/// Run each statement and assert its one-cell answer.
async fn assert_answers(fixture: &Fixture, cases: &[(&str, &str)]) {
    for (sql, want) in cases {
        let planned = run(fixture, sql)
            .await
            .unwrap_or_else(|e| panic!("{sql} must plan: {e}"));
        assert_eq!(&single_cell(&planned), want, "{sql}");
    }
}

/// `POSITION(x IN y)` plans through `UnicodeFunctionPlanner` (issue #2583)
/// and answers the 1-based index of `x` in `y`.
#[tokio::test]
async fn position_in_syntax_answers_the_index() {
    let f = fixture().await;
    assert_answers(&f, &[("SELECT POSITION('a' IN 'ab')", "1")]).await;
}

/// `SUBSTRING(x FROM y [FOR z])` and the unquoted `substr(x, y, z)` and
/// `substring(x, y, z)` calls plan through `UnicodeFunctionPlanner` (issue
/// #2583) and answer the substring.
#[tokio::test]
async fn substring_syntax_and_unquoted_calls_answer_the_substring() {
    let f = fixture().await;
    assert_answers(
        &f,
        &[
            ("SELECT SUBSTRING('abcdef' FROM 2 FOR 3)", "bcd"),
            ("SELECT SUBSTRING('abcdef' FROM 3)", "cdef"),
            ("SELECT substr('abcdef', 2, 3)", "bcd"),
            ("SELECT substring('abcdef', 2, 3)", "bcd"),
        ],
    )
    .await;
}

/// One row per SQL syntax an `ExprPlanner` method can claim in DataFusion
/// 54.1, with the outcome each has today and, for a row that plans, the call
/// its syntax is rewritten to. Each label starts with the method's name.
const CANARIES: [(&str, &str, Expect); 21] = [
    // plan_binary_op: TraceIdHexLiteralPlanner rewrites this comparison to a
    // `FixedSizeBinary(16)` literal, so the plan calls nothing.
    (
        "plan_binary_op (trace_id hex literal)",
        "SELECT start_ts FROM spans WHERE trace_id = '00112233445566778899aabbccddeeff'",
        Expect::Plans(None),
    ),
    // plan_binary_op: no registered planner rewrites `@>` (the nested
    // planner would, to `array_has_all`), and no physical operator runs it.
    (
        "plan_binary_op (@>)",
        "SELECT 1 @> 2",
        Expect::RefusedAtExecution,
    ),
    // plan_field_access: MapFieldAccessPlanner, map subscript.
    (
        "plan_field_access (map subscript)",
        "SELECT attrs['k'] FROM logs",
        Expect::Plans(Some((Kind::Scalar, "get_field"))),
    ),
    // plan_field_access: MapFieldAccessPlanner, string key on a struct.
    (
        "plan_field_access (struct subscript)",
        "SELECT named_struct('a', 1)['a']",
        Expect::Plans(Some((Kind::Scalar, "get_field"))),
    ),
    // plan_field_access: list index; no registered planner handles it.
    (
        "plan_field_access (list index)",
        "SELECT 'abc'[1]",
        Expect::RefusedAtPlan,
    ),
    // plan_array_literal: no registered planner.
    ("plan_array_literal", "SELECT [1, 2]", Expect::RefusedAtPlan),
    // plan_position: UnicodeFunctionPlanner.
    (
        "plan_position",
        "SELECT POSITION('a' IN 'ab')",
        Expect::Plans(Some((Kind::Scalar, "strpos"))),
    ),
    // plan_position: the comma form is an ordinary call, resolved by name
    // through the allowlist rather than by the planner; pinned beside the
    // planner row so the two spellings are seen to reach one function.
    (
        "plan_position (unquoted position call)",
        "SELECT position('ab', 'a')",
        Expect::Plans(Some((Kind::Scalar, "strpos"))),
    ),
    // plan_dictionary_literal: CoreFunctionPlanner.
    (
        "plan_dictionary_literal",
        "SELECT {'a': 1}",
        Expect::Plans(Some((Kind::Scalar, "named_struct"))),
    ),
    // plan_extract: DatetimeFunctionPlanner.
    (
        "plan_extract",
        "SELECT EXTRACT(minute FROM ts) FROM samples",
        Expect::Plans(Some((Kind::Scalar, "date_part"))),
    ),
    // plan_substring: UnicodeFunctionPlanner. sqlparser reads
    // `SUBSTRING(x FROM y [FOR z])` and the unquoted `substr(...)` and
    // `substring(...)` calls into one AST node, which only `plan_substring`
    // plans, so all four rows reach the same planner method. A double-quoted
    // name such as `"substr"(...)` is an ordinary call instead.
    (
        "plan_substring (FROM ... FOR)",
        "SELECT SUBSTRING('abc' FROM 1 FOR 2)",
        Expect::Plans(Some((Kind::Scalar, "substr"))),
    ),
    (
        "plan_substring (FROM)",
        "SELECT SUBSTRING('abc' FROM 2)",
        Expect::Plans(Some((Kind::Scalar, "substr"))),
    ),
    (
        "plan_substring (unquoted substr call)",
        "SELECT substr('abc', 1, 2)",
        Expect::Plans(Some((Kind::Scalar, "substr"))),
    ),
    (
        "plan_substring (unquoted substring call)",
        "SELECT substring('abc', 1, 2)",
        Expect::Plans(Some((Kind::Scalar, "substr"))),
    ),
    // plan_struct_literal: CoreFunctionPlanner.
    (
        "plan_struct_literal",
        "SELECT STRUCT(1, 2)",
        Expect::Plans(Some((Kind::Scalar, "struct"))),
    ),
    (
        "plan_struct_literal (named)",
        "SELECT STRUCT(1 AS a)",
        Expect::Plans(Some((Kind::Scalar, "named_struct"))),
    ),
    // plan_overlay: CoreFunctionPlanner.
    (
        "plan_overlay",
        "SELECT OVERLAY('abc' PLACING 'x' FROM 2)",
        Expect::Plans(Some((Kind::Scalar, "overlay"))),
    ),
    // plan_make_map: no registered planner.
    (
        "plan_make_map",
        "SELECT MAP {'a': 1}",
        Expect::RefusedAtPlan,
    ),
    // plan_compound_identifier: CoreFunctionPlanner.
    (
        "plan_compound_identifier",
        "SELECT s.a FROM (SELECT named_struct('a', 1) AS s)",
        Expect::Plans(Some((Kind::Scalar, "get_field"))),
    ),
    // plan_aggregate: AggregateFunctionPlanner.
    (
        "plan_aggregate",
        "SELECT count() FROM samples",
        Expect::Plans(Some((Kind::Aggregate, "count"))),
    ),
    // plan_window: WindowFunctionPlanner.
    (
        "plan_window",
        "SELECT count(*) OVER () FROM samples",
        Expect::Plans(Some((Kind::Window, "count"))),
    ),
];

/// The `ExprPlanner` methods of DataFusion 54.1, each of which [`CANARIES`]
/// must cover with at least one row.
const EXPR_PLANNER_METHODS: usize = 13;

/// Every canary in [`CANARIES`] has its pinned outcome. All mismatches are
/// collected first, so one upgrade that flips several rows names them all.
#[tokio::test]
async fn expr_planner_syntax_outcomes_are_pinned() {
    let f = fixture().await;

    let mut mismatches = Vec::new();
    for (syntax, sql, expected) in CANARIES {
        let expected = expected.outcome();
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

/// [`CANARIES`] has a row for each of the 13 `ExprPlanner` methods, counted by
/// the method name each label starts with.
#[test]
fn canaries_cover_every_expr_planner_method() {
    let methods: BTreeSet<&str> = CANARIES
        .iter()
        .map(|(label, _, _)| label.split(' ').next().unwrap_or(label))
        .collect();
    assert_eq!(
        methods.len(),
        EXPR_PLANNER_METHODS,
        "CANARIES covers {methods:?}. An `ExprPlanner` method has lost its row, \
         or a DataFusion upgrade grew the trait: each new method that \
         corresponds to SQL syntax needs a row here and a raised count"
    );
}

/// An empty snapshot, enough to plan against: the target check never executes.
fn empty_snapshot() -> Snapshot {
    Snapshot {
        segments: Vec::new(),
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    }
}

/// A session over the one table `sql` reads, built by `build_session` exactly
/// as the executor builds one, so it carries the same planners and allowlist.
fn session_for(fixture: &Fixture, sql: &str) -> datafusion::prelude::SessionContext {
    let store = Arc::clone(&fixture.store);
    let hash = tenant().hash();
    let table = if sql.contains("FROM logs") {
        SessionTable::Logs(Arc::new(LogsTableProvider::new(
            empty_snapshot(),
            hash,
            LogSegmentFetcher::new(store),
            PhaseAccounting::new(),
        )))
    } else if sql.contains("FROM spans") {
        SessionTable::Spans(Arc::new(SpansTableProvider::new(
            empty_snapshot(),
            hash,
            SpanSegmentFetcher::new(store),
            QueryAccounting::new(),
        )))
    } else {
        SessionTable::Metrics(Arc::new(RavelTableProvider::new(
            empty_snapshot(),
            hash,
            SegmentFetcher::new(store),
            SqlConfig::default(),
            PhaseAccounting::new(),
        )))
    };
    let pool = Arc::new(TenantDelegatingPool::new(
        1 << 30,
        TenantMemoryAccountant::new(1 << 30),
        CeilingBreach::new(),
        QueryAccounting::new(),
    ));
    build_session(
        &SqlConfig::default(),
        pool,
        table,
        false,
        SpillDecision::Disabled,
    )
    .expect("session builds")
}

/// Every function call in `plan` and the plans beneath it, read from the
/// function values in the expressions, never from display text.
fn calls(plan: &LogicalPlan) -> BTreeSet<(Kind, String)> {
    let mut found = BTreeSet::new();
    plan.apply_with_subqueries(|node| {
        node.apply_expressions(|expr| {
            expr.apply(|e| {
                let call = match e {
                    Expr::ScalarFunction(f) => Some((Kind::Scalar, f.func.name())),
                    Expr::AggregateFunction(f) => Some((Kind::Aggregate, f.func.name())),
                    Expr::WindowFunction(w) => Some((
                        Kind::Window,
                        match &w.fun {
                            WindowFunctionDefinition::AggregateUDF(u) => u.name(),
                            WindowFunctionDefinition::WindowUDF(u) => u.name(),
                        },
                    )),
                    _ => None,
                };
                if let Some((kind, name)) = call {
                    found.insert((kind, name.to_string()));
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })
    })
    .expect("plan walk");
    found
}

/// Every canary that plans calls the function its row names, read from the
/// unoptimized logical plan, where a planner's rewrite is visible before any
/// analyzer or optimizer rule touches it. Every scalar such a plan calls is
/// in `ADMITTED_SCALARS`, and every aggregate or window function it calls is
/// still registered after the session's allowlist gate.
#[tokio::test]
async fn planner_rewrite_targets_are_pinned_and_admitted() {
    let f = fixture().await;

    let mut mismatches = Vec::new();
    for (syntax, sql, expected) in CANARIES {
        let Expect::Plans(target) = expected else {
            continue;
        };
        let ctx = session_for(&f, sql);
        let state = ctx.state();
        let plan = match state.create_logical_plan(sql).await {
            Ok(plan) => plan,
            Err(e) => {
                mismatches.push(format!("{syntax}: `{sql}` did not plan: {e}"));
                continue;
            }
        };
        let called = calls(&plan);
        match target {
            Some((kind, name)) if !called.contains(&(kind, name.to_string())) => {
                mismatches.push(format!(
                    "{syntax}: `{sql}` expected a {kind:?} call to {name}, the plan calls \
                     {called:?}"
                ))
            }
            None if !called.is_empty() => mismatches.push(format!(
                "{syntax}: `{sql}` expected no call, the plan calls {called:?}"
            )),
            _ => {}
        }
        for (kind, name) in &called {
            let admitted = match kind {
                Kind::Scalar => ADMITTED_SCALARS.contains(&name.as_str()),
                Kind::Aggregate => state.aggregate_functions().contains_key(name),
                Kind::Window => {
                    state.aggregate_functions().contains_key(name)
                        || state.window_functions().contains_key(name)
                }
            };
            if !admitted {
                mismatches.push(format!(
                    "{syntax}: `{sql}` calls {kind:?} {name}, which the allowlist does not \
                     admit"
                ));
            }
        }
    }
    assert!(
        mismatches.is_empty(),
        "an expression planner's rewrite target drifted, or reaches a function \
         outside the allowlist:\n  {}",
        mismatches.join("\n  ")
    );
}
