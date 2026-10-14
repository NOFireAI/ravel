//! The result size ceiling, the caller's row cap, and the bound expression
//! budget, depth and stage limits.

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::LogicalPlan;
use ravel_sql::esql::{
    self, EsqlError, EsqlRun, MAX_BOUND_NODES, MAX_EXPRESSION_DEPTH, MAX_LIMIT, MAX_PLAN_STAGES,
};

use crate::lowering::fetch;
use crate::support::{
    WORKER_STACK, context, fixture, on_stack, optimized, prepare, request, rows, run, run_with,
};

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
async fn the_deepest_chains_run_to_their_exact_value() {
    let fixture = fixture().await;
    let source = format!("ROW x = 1.0{}", " + 1".repeat(19));
    let outcome = run(&fixture, &source).await.expect("runs");
    assert_eq!(rows(&outcome), vec![vec!["20.0"]]);

    let source = format!(
        "ROW a = 1000000000{}, b = 7{} % 4, c = 9{} / 0",
        " / 2".repeat(7),
        " * -1".repeat(5),
        " / 1".repeat(6)
    );
    let outcome = run(&fixture, &source).await.expect("runs");
    assert_eq!(rows(&outcome), vec![vec!["7812500", "-3", "null"]]);
}

/// Each operation keeps its own null result: an overflowing intermediate
/// reads null through every later operation, including a division that would
/// bring the exact value back into range, and a non-finite double divisor
/// reads null rather than giving `0` or the dividend.
#[tokio::test]
async fn an_intermediate_overflow_reads_null_through_later_operations() {
    let fixture = fixture().await;
    let outcome = run(
        &fixture,
        "ROW a = 9223372036854775807 * 2 / 2, b = (9223372036854775807 + 1) / 2, \
         c = (-9223372036854775807 - 1) / -1, d = (-9223372036854775807 - 1) % -1, \
         e = 1999999999999999999 / 2000000000000000000, \
         f = -1999999999999999999 / 2000000000000000000, g = -7 / 2, h = 7 % -2, \
         i = -7 % 2, j = 5 % 0",
    )
    .await
    .expect("runs");
    assert_eq!(
        rows(&outcome),
        vec![vec![
            "null", "null", "null", "0", "0", "0", "-3", "1", "-1", "null"
        ]]
    );

    let outcome = run(
        &fixture,
        "ROW a = 1e308 * 10 / 10, b = 1.0 / (1e308 * 10), c = 1.0 % (1e308 * 10), \
         d = 1e308 * 10 - 1e308 * 10, e = 1e308 * 10 * 0, f = -(1e308 * 10) / 1e308, \
         g = 1e308 * 10 / 1e308 + 1.0, h = 1.5 % 1.0",
    )
    .await
    .expect("runs");
    assert_eq!(
        rows(&outcome),
        vec![vec![
            "null", "null", "null", "null", "null", "null", "null", "0.5"
        ]]
    );

    // The same over a column, so nothing folds before execution.
    let outcome = run(
        &fixture,
        "FROM logs | WHERE message == \"a\" | EVAL o = dur * 922337203685477581 / 10, \
         p = dur * 92233720368547758 / 10, z = 1.0 / (dur * 1e308 * 1e308), \
         y = 1.0 / (dur * 1e307) | KEEP o, p, z, y",
    )
    .await
    .expect("runs");
    assert_eq!(
        rows(&outcome),
        vec![vec!["null", "92233720368547758", "null", "1e-308"]]
    );
}

/// Shared operands at the deepest admitted chains keep each stage's value,
/// on a thread with a server worker's stack.
#[test]
fn shared_operands_keep_the_value_in_each_stage() {
    on_stack(WORKER_STACK, || async {
        let fixture = fixture().await;
        let divided = format!("dur{}", " / 1".repeat(7));
        let source = format!(
            "FROM logs | WHERE {divided} > 1 | EVAL q = {divided}, d = dur * 1.0{} \
             | SORT {divided} DESC | KEEP dur, q, d",
            " + 0".repeat(17)
        );
        let outcome = run(&fixture, &source).await.expect("runs");
        assert_eq!(
            rows(&outcome),
            vec![
                vec!["20", "20", "20.0"],
                vec!["10", "10", "10.0"],
                vec!["7", "7", "7.0"],
                vec!["5", "5", "5.0"],
            ]
        );

        let source = format!(
            "FROM logs | STATS a = SUM(severity_num){}, b = SUM(severity_num) * 1.0{} \
             WHERE severity_num{} > 0 BY service | SORT service",
            " / 1".repeat(7),
            " + 0".repeat(17),
            " / 1".repeat(7)
        );
        let outcome = run(&fixture, &source).await.expect("runs");
        assert_eq!(
            rows(&outcome),
            vec![
                vec!["35", "35.0", "api"],
                vec!["22", "22.0", "web"],
                vec!["21", "21.0", "null"],
            ]
        );
    });
}

/// A statement whose bound nodes pass [`MAX_BOUND_NODES`] needs more tokens
/// than the default limits admit, so the refusal is reached under larger
/// ones.
#[test]
fn a_statement_past_the_node_budget_is_refused() {
    let limits = ravel_esql::Limits {
        max_statement_bytes: 16 << 20,
        max_tokens: 4 << 20,
        ..ravel_esql::Limits::default()
    };
    // A list's items each bind to a node without adding depth, so a few
    // fields reach the budget with no deep or numerous values.
    let row = |items: usize| {
        let list: Vec<String> = (0..items).map(|i| i.to_string()).collect();
        let field = |i: usize| format!("a{i} = 1 IN ({})", list.join(", "));
        format!("ROW {}", (0..4).map(field).collect::<Vec<_>>().join(", "))
    };
    match esql::prepare(&row(MAX_BOUND_NODES / 4), &limits, &context()) {
        Err(EsqlError::Verification { message, line, .. }) => {
            assert_eq!(
                message,
                format!("the statement binds to more than {MAX_BOUND_NODES} expression nodes")
            );
            assert_eq!(line, 1);
        }
        Err(other) => panic!("expected a verification error, got {other:?}"),
        Ok(_) => panic!("expected a verification error, the statement prepared"),
    }
    assert!(esql::prepare(&row(MAX_BOUND_NODES / 8), &limits, &context()).is_ok());
}

#[test]
fn an_expression_past_the_depth_limit_is_refused() {
    assert_eq!(MAX_EXPRESSION_DEPTH, 24);
    let refusal = |source: &str| match prepare(source) {
        Err(EsqlError::Verification {
            message,
            line,
            column,
        }) => (message, line, column),
        other => panic!("expected a verification error, got {other:?}"),
    };
    // A value deeper than the limit, refused where it is bound.
    assert_eq!(
        refusal(&format!("ROW x = 9{}", " / 1".repeat(8))),
        (
            "[9 / 1 / 1 / 1 / 1 / 1 / 1 / 1 / 1] binds to an expression 26 levels deep, \
             more than the limit of 24"
                .to_string(),
            1,
            9
        )
    );
    prepare(&format!("ROW x = 9{}", " / 1".repeat(7))).expect("prepares");
    // A column counts as the depth of what computes it.
    assert_eq!(
        refusal(&format!("ROW x = 9{} | EVAL y = x / 1", " / 1".repeat(7))).0,
        "[x / 1] binds to an expression 26 levels deep, more than the limit of 24"
    );
    // A value within the limit that its stage makes deeper: a long sum casts
    // its argument to a decimal.
    let argument = "1 / (1 / (1 / (1 / (1 / dur)))) / 1";
    prepare(&format!("FROM logs | EVAL s = {argument}")).expect("prepares");
    assert_eq!(
        refusal(&format!("FROM logs | STATS s = SUM({argument})")),
        (
            "the statement binds to an expression 25 levels deep, more than the limit of 24"
                .to_string(),
            1,
            13
        )
    );
}

#[test]
fn a_statement_past_the_stage_limit_is_refused() {
    assert_eq!(MAX_PLAN_STAGES, 12);
    // Each WHERE is a stage, and so are the output projection and the limit.
    let filters = |count: usize| format!("FROM logs{}", " | WHERE dur > 0".repeat(count));
    prepare(&filters(10)).expect("prepares");
    match prepare(&filters(11)) {
        Err(EsqlError::Verification { message, .. }) => {
            assert_eq!(message, "the statement binds to more than 12 plan stages");
        }
        other => panic!("expected a verification error, got {other:?}"),
    }
}
