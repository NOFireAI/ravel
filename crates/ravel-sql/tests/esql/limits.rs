//! The result size ceiling, the caller's row cap, and the bound expression
//! budget.

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::LogicalPlan;
use ravel_sql::esql::{EsqlError, EsqlRun, MAX_BOUND_NODES, MAX_LIMIT};

use crate::lowering::fetch;
use crate::support::{fixture, optimized, prepare, request, rows, run, run_with};

/// Every fetch in `plan`, outermost first.
fn fetches(plan: &LogicalPlan) -> Vec<usize> {
    let mut found = Vec::new();
    plan.apply(|node| {
        found.extend(fetch(node));
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("walk");
    found
}

#[tokio::test]
async fn a_final_limit_above_the_ceiling_is_lowered_to_it() {
    let fixture = fixture().await;
    let source = "FROM logs | LIMIT 2000000000";
    let plan = optimized(&fixture, source).await;
    assert_eq!(fetches(&plan), vec![10_000], "{plan}");
    assert_eq!(MAX_LIMIT, 10_000);
    assert_eq!(
        prepare(source).expect("prepares").clamped_limit(),
        Some(2_000_000_000)
    );
    let outcome = run(&fixture, source).await.expect("runs");
    assert_eq!(outcome.clamped_limit, Some(2_000_000_000));
    assert_eq!(rows(&outcome).len(), 6);
}

#[tokio::test]
async fn a_final_limit_under_the_ceiling_is_unchanged() {
    let fixture = fixture().await;
    let source = "FROM logs | LIMIT 5";
    let plan = optimized(&fixture, source).await;
    assert_eq!(fetches(&plan), vec![5], "{plan}");
    assert_eq!(prepare(source).expect("prepares").clamped_limit(), None);
    let outcome = run(&fixture, source).await.expect("runs");
    assert_eq!(outcome.clamped_limit, None);
    assert_eq!(rows(&outcome).len(), 5);

    // Only the final stage's own LIMIT is lowered.
    let plan = optimized(&fixture, "FROM logs | LIMIT 20000 | LIMIT 3").await;
    assert_eq!(fetches(&plan), vec![3], "{plan}");
    assert_eq!(
        prepare("FROM logs | LIMIT 20000 | KEEP message")
            .expect("prepares")
            .clamped_limit(),
        None
    );
}

#[tokio::test]
async fn more_rows_than_the_callers_cap_report_the_cap_hit() {
    let fixture = fixture().await;
    let prepared = prepare("FROM logs").expect("prepares");
    let mut capped = request();
    capped.max_rows = Some(2);
    let outcome = run_with(&fixture, &prepared, &capped).await.expect("runs");
    let EsqlRun::Catalog(sql) = &outcome.run else {
        panic!("a FROM statement runs through the executor");
    };
    assert!(sql.stats.row_cap_hit);
    assert_eq!(
        rows(&outcome).len(),
        3,
        "the cap plus the one row that shows it"
    );

    let mut roomy = request();
    roomy.max_rows = Some(6);
    let outcome = run_with(&fixture, &prepared, &roomy).await.expect("runs");
    let EsqlRun::Catalog(sql) = &outcome.run else {
        panic!("a FROM statement runs through the executor");
    };
    assert!(!sql.stats.row_cap_hit);
    assert_eq!(rows(&outcome).len(), 6);
}

#[tokio::test]
async fn a_chain_of_twenty_five_double_terms_runs_to_its_exact_value() {
    let fixture = fixture().await;
    let source = format!("ROW x = 1.0{}", " + 1".repeat(24));
    let outcome = run(&fixture, &source).await.expect("runs");
    assert_eq!(rows(&outcome), vec![vec!["25.0"]]);

    let source = format!(
        "ROW a = 1000000000{}, b = 7{} % 4, c = 9{} / 0",
        " / 2".repeat(25),
        " * -1".repeat(25),
        " / 1".repeat(25)
    );
    let outcome = run(&fixture, &source).await.expect("runs");
    assert_eq!(rows(&outcome), vec![vec!["29", "-3", "null"]]);
}

#[tokio::test]
async fn shared_operands_keep_the_value_in_each_stage() {
    let fixture = fixture().await;
    let divided = format!("dur{}", " / 1".repeat(25));
    let source = format!(
        "FROM logs | WHERE {divided} > 1 | EVAL q = {divided}, d = dur * 1.0{} \
         | SORT {divided} DESC | KEEP dur, q, d",
        " + 0".repeat(25)
    );
    let outcome = run(&fixture, &source).await.expect("runs");
    let plain = run(
        &fixture,
        "FROM logs | WHERE dur > 1 | EVAL q = dur, d = dur * 1.0 | SORT dur DESC \
         | KEEP dur, q, d",
    )
    .await
    .expect("runs");
    assert!(!rows(&plain).is_empty());
    assert_eq!(rows(&outcome), rows(&plain));

    let source = format!(
        "FROM logs | STATS a = SUM(severity_num){}, b = SUM(severity_num) * 1.0{} \
         WHERE severity_num{} > 0 BY service | SORT service",
        " / 1".repeat(25),
        " + 0".repeat(25),
        " / 1".repeat(25)
    );
    let outcome = run(&fixture, &source).await.expect("runs");
    let plain = run(
        &fixture,
        "FROM logs | STATS a = SUM(severity_num), b = SUM(severity_num) * 1.0 \
         WHERE severity_num > 0 BY service | SORT service",
    )
    .await
    .expect("runs");
    assert!(!rows(&plain).is_empty());
    assert_eq!(rows(&outcome), rows(&plain));
}

#[test]
fn a_statement_past_the_node_budget_is_refused() {
    // Each field's projection carries every column defined before it.
    let fields: Vec<String> = (0..2000).map(|i| format!("a{i} = 1")).collect();
    let source = format!("ROW {}", fields.join(", "));
    match prepare(&source) {
        Err(EsqlError::Verification { message, line, .. }) => {
            assert_eq!(
                message,
                format!("the statement binds to more than {MAX_BOUND_NODES} expression nodes")
            );
            assert_eq!(line, 1);
        }
        other => panic!("expected a verification error, got {other:?}"),
    }
    let fields: Vec<String> = (0..100).map(|i| format!("a{i} = 1")).collect();
    prepare(&format!("ROW {}", fields.join(", "))).expect("prepares");
}
