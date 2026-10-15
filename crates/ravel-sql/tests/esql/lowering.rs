//! The plan shape a pipeline lowers to, the rows it returns, and plan
//! admission of the lowered plans.

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::{FetchType, LogicalPlan};
use ravel_sql::esql::EsqlError;

use crate::support::{self, columns, fixture, optimized, prepare, rows, run};

/// The row count a `Limit` or a `Sort` node stops at, if it has one.
pub fn fetch(node: &LogicalPlan) -> Option<usize> {
    match node {
        LogicalPlan::Limit(limit) => match limit.get_fetch_type().expect("literal fetch") {
            FetchType::Literal(fetch) => fetch,
            FetchType::UnsupportedExpr => panic!("a lowered limit is a literal"),
        },
        LogicalPlan::Sort(sort) => sort.fetch,
        _ => None,
    }
}

/// Every fetch in `plan`'s nodes beneath its first `Aggregate`, and every
/// fetch above it.
pub fn fetches_around_aggregate(plan: &LogicalPlan) -> (Vec<usize>, Vec<usize>) {
    let mut above = Vec::new();
    let mut below = Vec::new();
    let mut seen_aggregate = false;
    plan.apply(|node| {
        if matches!(node, LogicalPlan::Aggregate(_)) {
            seen_aggregate = true;
        }
        if let Some(n) = fetch(node) {
            if seen_aggregate {
                below.push(n);
            } else {
                above.push(n);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("walk");
    assert!(seen_aggregate, "the plan holds an aggregate:\n{plan}");
    (above, below)
}

#[tokio::test]
async fn limit_before_stats_stays_under_the_aggregate_in_the_optimized_plan() {
    let fixture = fixture().await;
    let source = "FROM logs | SORT @timestamp | LIMIT 3 | STATS c = COUNT(*)";
    let plan = optimized(&fixture, source).await;
    let (above, below) = fetches_around_aggregate(&plan);
    assert_eq!(
        below,
        vec![3],
        "LIMIT 3 bounds the aggregate's input:\n{plan}"
    );
    assert_eq!(
        above,
        vec![1000],
        "only the implicit limit is above it:\n{plan}"
    );

    let outcome = run(&fixture, source).await.expect("runs");
    assert_eq!(rows(&outcome), vec![vec!["3".to_string()]]);

    // The two latest rows are e (22:15:10) and f (22:15:20); a LIMIT moved
    // above the aggregate would read a (22:13:00) instead.
    let outcome = run(
        &fixture,
        "FROM logs | SORT @timestamp DESC | LIMIT 2 | STATS lo = MIN(@timestamp)",
    )
    .await
    .expect("runs");
    assert_eq!(
        rows(&outcome),
        vec![vec!["2023-11-14T22:15:10".to_string()]]
    );
}

#[tokio::test]
async fn filtered_count_by_time_bucket_and_service_matches_hand_computed_rows() {
    let fixture = fixture().await;
    let outcome = run(
        &fixture,
        r#"FROM logs
           | WHERE @timestamp >= "2023-11-14T22:13:00Z"
           | STATS c = COUNT(*) WHERE severity_num >= 13 BY b = BUCKET(@timestamp, 1 minute), service
           | SORT b, service"#,
    )
    .await
    .expect("runs");
    assert_eq!(
        columns(&outcome),
        vec![
            ("c".to_string(), "long".to_string()),
            ("b".to_string(), "date_nanos".to_string()),
            ("service".to_string(), "keyword".to_string()),
        ]
    );
    // Severities per minute: [9 api, 13 api], [17 web, 13 api], [5 web, 21 -].
    let expected = [
        ["1", "2023-11-14T22:13:00", "api"],
        ["1", "2023-11-14T22:14:00", "api"],
        ["1", "2023-11-14T22:14:00", "web"],
        ["0", "2023-11-14T22:15:00", "web"],
        ["1", "2023-11-14T22:15:00", "null"],
    ];
    assert_eq!(
        rows(&outcome),
        expected
            .iter()
            .map(|row| row.iter().map(|cell| cell.to_string()).collect::<Vec<_>>())
            .collect::<Vec<_>>()
    );
}

#[tokio::test]
async fn esql_plans_pass_admission_through_the_executor() {
    let fixture = fixture().await;
    for source in [
        "FROM logs",
        "FROM logs | WHERE service == \"api\" AND dur > 5 | KEEP message, dur",
        "FROM logs | EVAL x = dur * 2 + 1, y = dur / 3, z = dur % 4 | SORT x DESC | LIMIT 2",
        "FROM logs | STATS n = COUNT(dur), s = SUM(dur), lo = MIN(dur), hi = MAX(dur), m = AVG(dur) BY service",
        "FROM logs | WHERE @timestamp >= NOW() - 1 hour | STATS c = COUNT(*) BY b = BUCKET(@timestamp, 30 seconds)",
        "FROM logs | KEEP trace_id, span_id, message | WHERE trace_id IS NOT NULL",
    ] {
        run(&fixture, source)
            .await
            .unwrap_or_else(|e| panic!("{source}: {e}"));
    }
}

/// A sort on a key a sort below it also sorts on plans and runs, ordered by
/// the last sort.
#[tokio::test]
async fn a_sort_repeating_an_earlier_key_runs() {
    let fixture = fixture().await;
    for (source, expected) in [
        (
            "FROM logs | SORT dur + 1 | SORT dur + 1 | KEEP message",
            ["f", "d", "e", "a", "b", "c"].as_slice(),
        ),
        (
            "FROM logs | SORT dur + 1 DESC | SORT dur + 1 | KEEP message",
            ["f", "d", "e", "a", "b", "c"].as_slice(),
        ),
        (
            "FROM logs | SORT dur > 5, dur | SORT dur > 5, dur DESC | KEEP message",
            ["d", "f", "b", "a", "e", "c"].as_slice(),
        ),
        (
            "FROM logs | WHERE dur > 5 | SORT dur > 5, dur DESC | KEEP message",
            ["b", "a", "e"].as_slice(),
        ),
    ] {
        let outcome = run(&fixture, source)
            .await
            .unwrap_or_else(|e| panic!("{source}: {e}"));
        let found: Vec<Vec<String>> = rows(&outcome);
        let expected: Vec<Vec<String>> = expected.iter().map(|m| vec![m.to_string()]).collect();
        assert_eq!(found, expected, "{source}");
    }
}

#[tokio::test]
async fn row_runs_without_the_catalog() {
    let fixture = fixture().await;
    let prepared = prepare("ROW a = 1, b = \"x\" | EVAL c = a + 1").expect("prepares");
    assert!(!prepared.needs_catalog());
    assert_eq!(prepared.window(), None);
    let outcome = support::run_prepared(&fixture, &prepared)
        .await
        .expect("runs");
    assert_eq!(
        columns(&outcome),
        vec![
            ("a".to_string(), "long".to_string()),
            ("b".to_string(), "keyword".to_string()),
            ("c".to_string(), "long".to_string()),
        ]
    );
    assert_eq!(rows(&outcome), vec![vec!["1", "x", "2"]]);
    assert!(matches!(outcome.run, ravel_sql::esql::EsqlRun::Row(_)));
}

/// A result column named like the binder's internal names leaves the final
/// projection a column under its own name, which runs.
#[tokio::test]
async fn a_result_column_named_like_an_internal_name_runs() {
    let fixture = fixture().await;
    let outcome = run(&fixture, "ROW _e1 = 1").await.expect("runs");
    assert_eq!(
        columns(&outcome),
        vec![("_e1".to_string(), "long".to_string())]
    );
    assert_eq!(rows(&outcome), vec![vec!["1"]]);

    let outcome = run(
        &fixture,
        "FROM logs | SORT @timestamp | LIMIT 1 | EVAL _e1 = 1 | KEEP _e1",
    )
    .await
    .expect("runs");
    assert_eq!(rows(&outcome), vec![vec!["1"]]);
}

#[tokio::test]
async fn row_errors_class_as_bad_request_before_execution() {
    let err = prepare("ROW a = 1 | WHERE a").expect_err("a long is not a condition");
    assert!(matches!(err, EsqlError::Verification { .. }), "{err}");
    assert_eq!(err.class(), ravel_sql::ErrorClass::BadRequest);
}
