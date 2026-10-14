//! The result size ceiling, the caller's row cap, and the bound expression
//! budget, depth and stage limits.

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::LogicalPlan;
use ravel_sql::esql::{
    self, EsqlError, EsqlRun, MAX_BOUND_NODES, MAX_EXPRESSION_DEPTH, MAX_LIMIT, MAX_PLAN_STAGES,
    PrepareContext,
};

use crate::lowering::fetch;
use crate::support::{
    BASE, NOW, SECOND, WORKER_STACK, context, fixture, on_stack, optimized, prepare, request, rows,
    run, run_with,
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

/// The message, line and column of the verification error refusing `source`.
fn refusal(source: &str) -> (String, usize, usize) {
    refusal_in(source, &context())
}

fn refusal_in(source: &str, ctx: &PrepareContext) -> (String, usize, usize) {
    match esql::prepare(source, &ravel_esql::Limits::default(), ctx) {
        Err(EsqlError::Verification {
            message,
            line,
            column,
        }) => (message, line, column),
        other => panic!("expected a verification error, got {other:?}"),
    }
}

/// How the depth limit is counted, as every depth refusal states it.
const DEPTH_RULE: &str = "each operator, comparison and function call is a level, long \
    arithmetic takes three levels per operator, and a column computed by ROW, EVAL or STATS \
    ... BY counts as the expression computing it, so arithmetic chained through EVAL columns \
    adds up";

#[test]
fn an_expression_past_the_depth_limit_is_refused() {
    assert_eq!(MAX_EXPRESSION_DEPTH, 24);
    // A value deeper than the limit, refused at its command.
    assert_eq!(
        refusal(&format!("ROW x = 9{}", " / 1".repeat(8))),
        (
            format!(
                "ROW expression [9 / 1 / 1 / 1 / 1 / 1 / 1 / 1 / 1] is 26 levels deep, past \
                 the limit of 24: {DEPTH_RULE}"
            ),
            1,
            1
        )
    );
    prepare(&format!("ROW x = 9{}", " / 1".repeat(7))).expect("prepares");
    // A column counts as the depth of what computes it.
    assert_eq!(
        refusal(&format!("ROW x = 9{}\n| EVAL y = x / 1", " / 1".repeat(7))),
        (
            format!(
                "EVAL expression [x / 1] is 26 levels deep, past the limit of 24: {DEPTH_RULE}"
            ),
            2,
            3
        )
    );
    // A value within the limit that its stage makes deeper: a long sum casts
    // its argument to a decimal.
    let argument = "1 / (1 / (1 / (1 / (1 / dur)))) / 1";
    prepare(&format!("FROM logs | EVAL s = {argument}")).expect("prepares");
    assert_eq!(
        refusal(&format!("FROM logs | STATS s = SUM({argument})")),
        (
            format!(
                "STATS argument of [SUM({argument})], cast to a decimal to sum, is 25 levels \
                 deep, past the limit of 24: {DEPTH_RULE}"
            ),
            1,
            13
        )
    );
}

/// Every `WHERE` up to a `LIMIT` runs as one condition, refused once that
/// condition is past the depth limit.
#[test]
fn where_conditions_past_the_depth_limit_together_are_refused() {
    let conditions = |from: usize, count: usize| {
        (from..from + count)
            .map(|j| format!("dur != {j}"))
            .collect::<Vec<_>>()
            .join(" AND ")
    };
    let statement = |second: usize| {
        format!(
            "FROM logs\n| WHERE {}\n| WHERE {}",
            conditions(0, 12),
            conditions(12, second)
        )
    };
    prepare(&statement(11)).expect("prepares");
    assert_eq!(
        refusal(&statement(12)),
        (
            format!(
                "WHERE brings the statement to 24 conditions joined by AND, the deepest 2 \
                 levels deep, which run as one condition 25 levels deep, past the limit of 24: \
                 the conditions of every WHERE since the source command or the last LIMIT run \
                 as one, and each condition joined by AND adds a level; {DEPTH_RULE}"
            ),
            3,
            3
        )
    );
    // A LIMIT between them keeps the two conditions apart.
    prepare(&format!(
        "FROM logs | WHERE {} | LIMIT 100 | WHERE {}",
        conditions(0, 12),
        conditions(12, 12)
    ))
    .expect("prepares");
}

/// How the stage limit is counted, as the stage refusal states it.
const STAGE_RULE: &str = "each WHERE, SORT, LIMIT, KEEP and DROP is one stage and each STATS \
    two or more; the columns ROW and EVAL fields define are computed in one stage ahead of the \
    next of those commands or the end of the statement, plus one for each level of them \
    reading another; FROM, the request time range and the implicit LIMIT do not count";

#[test]
fn a_statement_past_the_stage_limit_is_refused() {
    assert_eq!(MAX_PLAN_STAGES, 10);
    let filters = |count: usize| format!("FROM logs{}", " | WHERE dur > 0".repeat(count));
    prepare(&filters(10)).expect("prepares");
    assert_eq!(
        refusal(&filters(11)),
        (
            format!("WHERE takes the statement past 10 plan stages: {STAGE_RULE}"),
            1,
            173
        )
    );
    // The request time range is not a stage of the statement's own.
    let ranged = PrepareContext {
        start: Some(BASE - 60 * SECOND),
        end: Some(NOW + 1),
        ..context()
    };
    esql::prepare(&filters(10), &ravel_esql::Limits::default(), &ranged).expect("prepares");
    assert_eq!(refusal_in(&filters(11), &ranged), refusal(&filters(11)));
}

/// The most times `stage` follows `head` in a statement that prepares.
fn most(head: &str, stage: &str) -> usize {
    let mut n = 0;
    while prepare(&format!("{head}{}", stage.repeat(n + 1))).is_ok() {
        n += 1;
        assert!(n <= 2 * MAX_PLAN_STAGES, "{head}{stage} is never refused");
    }
    n
}

/// Each count the stage refusal states, one command at a time.
#[test]
fn the_stage_limit_counts_the_statements_own_commands() {
    for stage in [
        " | WHERE dur > 0",
        " | SORT dur",
        " | LIMIT 5",
        " | KEEP dur",
    ] {
        assert_eq!(most("FROM logs", stage), 10, "{stage}");
    }
    assert_eq!(most("FROM logs", " | STATS c = COUNT(*) BY service"), 5);
    // The columns EVAL defines take one stage, and one more per level of
    // them reading another.
    assert_eq!(most("FROM logs | EVAL x = dur + 1", " | SORT x"), 9);
    assert_eq!(
        most("FROM logs | EVAL x = dur + 1, y = x + 1", " | SORT y"),
        8
    );
    assert_eq!(most("ROW a = 1", " | SORT a"), 9);
    let drops = |count: usize| {
        let fields: Vec<String> = (1..=10).map(|i| format!("x{i} = dur + {i}")).collect();
        let drops: String = (1..=count).map(|i| format!(" | DROP x{i}")).collect();
        format!("FROM logs | EVAL {}{drops}", fields.join(", "))
    };
    prepare(&drops(9)).expect("prepares");
    assert_eq!(
        refusal(&drops(10)).0,
        format!("DROP takes the statement past 10 plan stages: {STAGE_RULE}")
    );
}
