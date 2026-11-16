//! Types, operators, aggregates, sort order and the implicit limit.

use std::sync::Arc;

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::functions_aggregate::average::avg_udaf;
use datafusion::functions_aggregate::min_max::{max_udaf, min_udaf};
use datafusion::logical_expr::{AggregateUDF, Expr, LogicalPlan};
use datafusion::prelude::SessionContext;

use crate::lowering::fetches_around_aggregate;
use crate::support::{columns, fixture, optimized, prepare, rows, run, session};

async fn cells(source: &str) -> Vec<Vec<String>> {
    let fixture = fixture().await;
    let outcome = run(&fixture, source)
        .await
        .unwrap_or_else(|e| panic!("{source}: {e}"));
    rows(&outcome)
}

#[tokio::test]
async fn a_null_operand_gives_null_through_each_operator() {
    let source = "ROW n = null, a = 1 + null, b = null - 2.5, c = 3 * null, d = null / 4, \
                  e = null % 5, f = null > 1, g = 1 == null, h = NOT null, i = null AND true, \
                  j = null OR false, k = -null, l = null IN (1, 2), m = null IS NULL, \
                  o = null AND false, p = null OR true";
    let fixture = fixture().await;
    let outcome = run(&fixture, source).await.expect("runs");
    let types: Vec<String> = columns(&outcome).into_iter().map(|(_, t)| t).collect();
    assert_eq!(
        types,
        [
            "null", "null", "null", "null", "null", "null", "boolean", "boolean", "null",
            "boolean", "boolean", "null", "boolean", "boolean", "boolean", "boolean"
        ]
    );
    assert_eq!(
        rows(&outcome),
        vec![vec![
            "null", "null", "null", "null", "null", "null", "null", "null", "null", "null", "null",
            "null", "null", "true", "false", "true"
        ]]
    );
    // A null column value, not only a null literal.
    assert_eq!(
        cells(
            "FROM logs | WHERE message == \"c\" | EVAL a = dur + 1, b = dur * 1.5, c = dur > 0, \
             d = -dur | KEEP a, b, c, d"
        )
        .await,
        vec![vec!["null", "null", "null", "null"]]
    );
}

#[tokio::test]
async fn integer_division_by_zero_and_overflow_give_null() {
    assert_eq!(
        cells(
            "ROW a = 5 / 0, b = 5 % 0, c = -9223372036854775808 / -1, \
             d = 9223372036854775807 + 1, e = 7 / 2, f = -7 % 3, g = 7 % -1, h = -7 / -1, \
             i = -9223372036854775808 - 1, j = 4611686018427387904 * 2, k = 1.0 / 0"
        )
        .await,
        vec![vec![
            "null", "null", "null", "null", "3", "-1", "0", "7", "null", "null", "null"
        ]]
    );
    assert_eq!(
        cells(
            "FROM logs | WHERE message == \"a\" | EVAL q = dur / 0, r = dur % 0, s = dur / 3 \
             | KEEP q, r, s"
        )
        .await,
        vec![vec!["null", "null", "3"]]
    );
}

/// Every aggregate function in `plan`.
fn aggregate_functions(plan: &LogicalPlan) -> Vec<Arc<AggregateUDF>> {
    let mut found = Vec::new();
    plan.apply(|node| {
        node.apply_expressions(|expr| {
            expr.apply(|expr| {
                if let Expr::AggregateFunction(call) = expr {
                    found.push(Arc::clone(&call.func));
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })
    })
    .expect("walk");
    found
}

/// The aggregates `source` lowers to in `session`, against the instances
/// `session` registers under each of `names`.
async fn lowered_aggregates(
    session: &SessionContext,
    source: &str,
    names: &[&str],
) -> (Vec<Arc<AggregateUDF>>, Vec<Arc<AggregateUDF>>) {
    let plan = prepare(source)
        .expect("prepares")
        .optimized_plan(session)
        .await
        .expect("plans");
    let found = aggregate_functions(&plan);
    assert_eq!(found.len(), names.len(), "{plan}");
    let state = session.state();
    let registered = names
        .iter()
        .map(|name| {
            state
                .aggregate_functions()
                .get(*name)
                .cloned()
                .unwrap_or_else(|| panic!("{name} registered"))
        })
        .collect();
    (found, registered)
}

#[tokio::test]
async fn avg_of_a_long_uses_the_sessions_sequential_avg() {
    let fixture = fixture().await;
    let session = session(&fixture.store);
    let (found, registered) = lowered_aggregates(
        &session,
        "FROM logs | STATS a = AVG(severity_num)",
        &["avg"],
    )
    .await;
    assert!(Arc::ptr_eq(&found[0], &registered[0]));
    assert!(!Arc::ptr_eq(&found[0], &avg_udaf()));
    assert_eq!(
        cells("FROM logs | STATS a = AVG(severity_num)").await,
        vec![vec!["13.0"]]
    );
}

#[tokio::test]
async fn min_and_max_use_the_total_order_replacements() {
    let fixture = fixture().await;
    let session = session(&fixture.store);
    let (found, registered) = lowered_aggregates(
        &session,
        "FROM logs | STATS lo = MIN(dur), hi = MAX(dur)",
        &["min", "max"],
    )
    .await;
    for (i, builtin) in [min_udaf(), max_udaf()].into_iter().enumerate() {
        assert!(
            Arc::ptr_eq(&found[i], &registered[i]),
            "{}",
            found[i].name()
        );
        assert!(!Arc::ptr_eq(&found[i], &builtin), "{}", found[i].name());
    }
    assert_eq!(
        cells("FROM logs | STATS lo = MIN(dur), hi = MAX(dur), t = MAX(@timestamp)").await,
        vec![vec!["1", "20", "2023-11-14T22:15:20"]]
    );
}

#[tokio::test]
async fn sort_places_nulls_last_ascending_and_first_descending() {
    assert_eq!(
        cells("FROM logs | KEEP message, dur | SORT dur | KEEP message").await,
        vec![
            vec!["f"],
            vec!["d"],
            vec!["e"],
            vec!["a"],
            vec!["b"],
            vec!["c"]
        ]
    );
    assert_eq!(
        cells("FROM logs | KEEP message, dur | SORT dur DESC | KEEP message").await,
        vec![
            vec!["c"],
            vec!["b"],
            vec!["a"],
            vec!["e"],
            vec!["d"],
            vec!["f"]
        ]
    );
    assert_eq!(
        cells("FROM logs | KEEP message, dur | SORT dur ASC NULLS FIRST | LIMIT 1 | KEEP message")
            .await,
        vec![vec!["c"]]
    );
}

#[tokio::test]
async fn the_implicit_limit_applies_to_the_final_stage_only() {
    let fixture = fixture().await;
    let plan = optimized(&fixture, "FROM logs | STATS c = COUNT(*) BY service").await;
    let (above, below) = fetches_around_aggregate(&plan);
    assert_eq!(below, Vec::<usize>::new(), "{plan}");
    assert_eq!(above, vec![1000], "{plan}");

    let plan = optimized(
        &fixture,
        "FROM logs | LIMIT 2000 | STATS c = COUNT(*) | LIMIT 5",
    )
    .await;
    let (above, below) = fetches_around_aggregate(&plan);
    assert_eq!(below, vec![2000], "{plan}");
    assert_eq!(
        above,
        vec![5],
        "a statement ending in LIMIT gets no implicit one: {plan}"
    );
}
