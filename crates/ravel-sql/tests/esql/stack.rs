//! The worst statement the parser's default limits admit, per shape, run from
//! prepare through execution and drop on a thread with a server worker's
//! 2 MiB stack: each returns its exact rows, and one step past it is a typed
//! refusal.

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::utils::split_conjunction;
use datafusion::logical_expr::{Expr, LogicalPlan};
use ravel_sql::esql::{
    self, EsqlError, EsqlOutcome, MAX_EXPRESSION_DEPTH, MAX_PLAN_STAGES, PrepareContext,
    PreparedEsql,
};

use crate::support::{
    BASE, NOW, SECOND, WORKER_STACK, columns, context, fixture, on_stack, optimized, prepare, rows,
    run_prepared,
};

/// A request time range holding every fixture row.
const RANGE: (i64, i64) = (BASE - 60 * SECOND, NOW + 1);

/// `source` prepared with the request time range [`RANGE`] when `ranged`.
fn prepare_in(source: &str, ranged: bool) -> Result<PreparedEsql, EsqlError> {
    let ctx = PrepareContext {
        start: ranged.then_some(RANGE.0),
        end: ranged.then_some(RANGE.1),
        ..context()
    };
    esql::prepare(source, &ravel_esql::Limits::default(), &ctx)
}

/// The fixture's rows: message, service and dur.
const LOGS: [(&str, Option<&str>, Option<i64>); 6] = [
    ("a", Some("api"), Some(10)),
    ("b", Some("api"), Some(20)),
    ("c", Some("web"), None),
    ("d", Some("api"), Some(5)),
    ("e", Some("web"), Some(7)),
    ("f", None, Some(1)),
];

/// The arithmetic a shape's expressions use.
#[derive(Clone, Copy, Debug)]
enum Kind {
    Long,
    Double,
}

impl Kind {
    /// The integer `value` as a result of this kind renders.
    fn render(self, value: Option<i64>) -> String {
        match (self, value) {
            (_, None) => "null".to_string(),
            (Kind::Long, Some(value)) => value.to_string(),
            (Kind::Double, Some(value)) => format!("{value}.0"),
        }
    }

    /// `base` and `k` operators that keep its value: long division by one
    /// and remainder by `modulus`, alternating, which for a double start with
    /// a multiplication by `1.0`. `modulus` exceeds every value the fixture
    /// holds.
    fn expr(self, base: &str, k: usize, modulus: usize) -> String {
        let mut out = base.to_string();
        for i in 0..k {
            match (self, i) {
                (Kind::Double, 0) => out.push_str(" * 1.0"),
                (_, i) if i % 2 == 0 => out.push_str(" / 1"),
                _ => out.push_str(&format!(" % {modulus}")),
            }
        }
        out
    }
}

/// A statement shape: `k` operators in each expression, `n` repetitions.
#[derive(Clone, Copy, Debug)]
enum Shape {
    /// `ROW x1 = e`, then `| EVAL xi = e` per stage.
    Row,
    /// `FROM logs`, then `| WHERE e != -i` per stage `i`, each `e` with its
    /// own modulus, so no two conditions are equal.
    Where,
    /// `FROM logs`, then per stage a `WHERE` of `k + 1` `AND`ed comparisons
    /// `dur != -j`, each `j` its own.
    And,
    /// `FROM logs`, then `| EVAL xi = e` per stage.
    Eval,
    /// `FROM logs`, then `| EVAL xi = e` per stage over the column the stage
    /// before defined, `dur` for the first.
    Chain,
    /// `FROM logs | EVAL x1 = e, x2 = e, ...` with `n` fields.
    EvalFields,
    /// `FROM logs`, then `| SORT e` per stage, each with its own modulus.
    Sort,
    /// `FROM logs | STATS s = e BY service` over `SUM(severity_num)`, then
    /// the same over `SUM(s)` per stage.
    StatsField,
    /// `FROM logs | STATS s = SUM(severity_num) WHERE e >= 0 BY service`
    /// over `dur`, then the same over `s` per stage.
    StatsWhere,
    /// `FROM logs | STATS c = COUNT(*) BY g1 = e` over `dur`, then the same
    /// over the group before per stage, then `| WHERE e >= 0` over the last
    /// group.
    StatsGroup,
}

impl Shape {
    fn statement(self, kind: Kind, k: usize, n: usize) -> String {
        let e = |base: &str| kind.expr(base, k, 1000);
        let stages = |stage: &dyn Fn(usize) -> String| (1..=n).map(stage).collect::<String>();
        match self {
            Shape::Row => format!(
                "ROW x1 = {}{}",
                e("9"),
                stages(&|i| if i == 1 {
                    String::new()
                } else {
                    format!(" | EVAL x{i} = {}", e("9"))
                })
            ),
            Shape::Where => format!(
                "FROM logs{}",
                stages(&|i| format!(" | WHERE {} != -{i}", kind.expr("dur", k, 1000 + i)))
            ),
            Shape::And => format!(
                "FROM logs{}",
                stages(&|i| {
                    let and: Vec<String> = (1..=k + 1)
                        .map(|j| format!("dur != -{}", (i - 1) * (k + 1) + j))
                        .collect();
                    format!(" | WHERE {}", and.join(" AND "))
                })
            ),
            Shape::Eval => format!(
                "FROM logs{}",
                stages(&|i| format!(" | EVAL x{i} = {}", e("dur")))
            ),
            Shape::Chain => format!(
                "FROM logs{}",
                stages(&|i| {
                    let over = if i == 1 {
                        "dur".to_string()
                    } else {
                        format!("x{}", i - 1)
                    };
                    format!(" | EVAL x{i} = {}", e(&over))
                })
            ),
            Shape::EvalFields => format!(
                "FROM logs | EVAL {}",
                (1..=n)
                    .map(|i| format!("x{i} = {}", e("dur")))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Shape::Sort => format!(
                "FROM logs{}",
                stages(&|i| format!(" | SORT {}", kind.expr("dur", k, 1000 + i)))
            ),
            Shape::StatsField => format!(
                "FROM logs{}",
                stages(&|i| {
                    let sum = if i == 1 {
                        "SUM(severity_num)"
                    } else {
                        "SUM(s)"
                    };
                    format!(" | STATS s = {} BY service", e(sum))
                })
            ),
            Shape::StatsWhere => format!(
                "FROM logs{}",
                stages(&|i| {
                    let (sum, over) = if i == 1 {
                        ("severity_num", "dur")
                    } else {
                        ("s", "s")
                    };
                    format!(" | STATS s = SUM({sum}) WHERE {} >= 0 BY service", e(over))
                })
            ),
            Shape::StatsGroup => format!(
                "FROM logs{} | WHERE {} >= 0",
                stages(&|i| {
                    let over = if i == 1 {
                        "dur".to_string()
                    } else {
                        format!("g{}", i - 1)
                    };
                    format!(" | STATS c = COUNT(*) BY g{i} = {}", e(&over))
                }),
                e(&format!("g{n}"))
            ),
        }
    }

    /// The columns the test reads, their rows at any admitted `k` and `n`,
    /// and whether the rows' order is part of the result.
    fn expected(self, kind: Kind, n: usize) -> (Vec<String>, Vec<Vec<String>>, bool) {
        let names = |prefix: &str| (1..=n).map(|i| format!("{prefix}{i}")).collect();
        let message = || vec!["message".to_string()];
        let strings = |rows: &[[&str; 2]]| {
            rows.iter()
                .map(|row| row.iter().map(|cell| cell.to_string()).collect())
                .collect()
        };
        match self {
            Shape::Row => (names("x"), vec![vec![kind.render(Some(9)); n]], false),
            Shape::Where | Shape::And => (
                message(),
                LOGS.iter()
                    .filter(|(_, _, dur)| dur.is_some())
                    .map(|(message, _, _)| vec![message.to_string()])
                    .collect(),
                false,
            ),
            Shape::Eval | Shape::Chain | Shape::EvalFields => (
                names("x"),
                LOGS.iter()
                    .map(|(_, _, dur)| vec![kind.render(*dur); n])
                    .collect(),
                false,
            ),
            Shape::Sort => (
                message(),
                ["f", "d", "e", "a", "b", "c"]
                    .iter()
                    .map(|message| vec![message.to_string()])
                    .collect(),
                true,
            ),
            Shape::StatsField => (
                vec!["s".to_string(), "service".to_string()],
                [(35, "api"), (22, "web"), (21, "null")]
                    .iter()
                    .map(|(sum, service)| vec![kind.render(Some(*sum)), service.to_string()])
                    .collect(),
                false,
            ),
            Shape::StatsWhere => (
                vec!["s".to_string(), "service".to_string()],
                strings(&[["35", "api"], ["5", "web"], ["21", "null"]]),
                false,
            ),
            Shape::StatsGroup => (
                vec!["c".to_string(), format!("g{n}")],
                LOGS.iter()
                    .filter(|(_, _, dur)| dur.is_some())
                    .map(|(_, _, dur)| vec!["1".to_string(), kind.render(*dur)])
                    .collect(),
                false,
            ),
        }
    }
}

/// The cells of `names` in each row of `outcome`.
fn select(outcome: &EsqlOutcome, names: &[String]) -> Vec<Vec<String>> {
    let columns = columns(outcome);
    let at: Vec<usize> = names
        .iter()
        .map(|name| {
            columns
                .iter()
                .position(|(column, _)| column == name)
                .unwrap_or_else(|| panic!("no column {name}"))
        })
        .collect();
    rows(outcome)
        .into_iter()
        .map(|row| at.iter().map(|&i| row[i].clone()).collect())
        .collect()
}

/// The largest `n` from 1 up that `admitted` accepts, given that it accepts
/// 1 and accepts every `n` below one it accepts.
fn largest(admitted: impl Fn(usize) -> bool) -> usize {
    assert!(admitted(1));
    let mut high = 2;
    while admitted(high) {
        high *= 2;
    }
    let mut low = high / 2;
    while high - low > 1 {
        let mid = low + (high - low) / 2;
        if admitted(mid) {
            low = mid;
        } else {
            high = mid;
        }
    }
    low
}

/// What refused a statement.
#[derive(Debug, PartialEq, Eq)]
enum Refusal {
    /// One expression too deep.
    Depth,
    /// The `WHERE` conditions merged into one too deep.
    Merged,
    Stages,
    Parse,
}

fn refusal(err: &EsqlError) -> Refusal {
    let depth = format!("levels deep, past the limit of {MAX_EXPRESSION_DEPTH}:");
    match err {
        EsqlError::Parse(_) => Refusal::Parse,
        EsqlError::Verification { message, .. }
            if message.contains("joined by AND") && message.contains(&depth) =>
        {
            Refusal::Merged
        }
        EsqlError::Verification { message, .. } if message.contains(&depth) => Refusal::Depth,
        EsqlError::Verification { message, .. }
            if message.contains(&format!("past {MAX_PLAN_STAGES} plan stages:")) =>
        {
            Refusal::Stages
        }
        other => panic!("not a limit refusal: {other:?}"),
    }
}

/// On a 2 MiB thread: the deepest `k` the shape admits at one repetition, or
/// `depth` when given, the most repetitions `n` it admits at that `k`, the
/// statement at both run to its exact rows, and what refuses `n + 1`. A `k`
/// the search found is checked to refuse `k + 1` as too deep.
fn worst_at(kind: Kind, shape: Shape, depth: Option<usize>) -> (usize, usize, Refusal) {
    worst_in(kind, shape, depth, false)
}

/// [`worst_at`], each statement prepared with the request time range
/// [`RANGE`] when `ranged`.
fn worst_in(
    kind: Kind,
    shape: Shape,
    depth: Option<usize>,
    ranged: bool,
) -> (usize, usize, Refusal) {
    on_stack(WORKER_STACK, move || async move {
        let admitted = |k, n| prepare_in(&shape.statement(kind, k, n), ranged);
        let k = match depth {
            Some(k) => k,
            None => {
                let k = largest(|k| admitted(k, 1).is_ok());
                let deeper = admitted(k + 1, 1).expect_err("k + 1 is refused");
                assert_eq!(refusal(&deeper), Refusal::Depth, "{deeper}");
                k
            }
        };
        let n = largest(|n| admitted(k, n).is_ok());
        let longer = admitted(k, n + 1).expect_err("n + 1 is refused");

        let prepared = admitted(k, n).expect("prepares");
        let fixture = fixture().await;
        let outcome = run_prepared(&fixture, &prepared)
            .await
            .unwrap_or_else(|e| panic!("{kind:?} {shape:?} at k = {k}, n = {n}: {e}"));
        let (names, mut expected, ordered) = shape.expected(kind, n);
        let mut found = select(&outcome, &names);
        if !ordered {
            expected.sort();
            found.sort();
        }
        assert_eq!(found, expected, "{kind:?} {shape:?} at k = {k}, n = {n}");
        drop(outcome);
        drop(prepared);
        (k, n, refusal(&longer))
    })
}

fn worst(kind: Kind, shape: Shape) -> (usize, usize, Refusal) {
    worst_at(kind, shape, None)
}

#[test]
fn the_deepest_long_row_runs_on_a_worker_stack() {
    assert_eq!(worst(Kind::Long, Shape::Row), (7, 100, Refusal::Parse));
}

#[test]
fn the_deepest_double_row_runs_on_a_worker_stack() {
    assert_eq!(worst(Kind::Double, Shape::Row), (19, 100, Refusal::Parse));
}

/// The depth of `e`, counting `e` itself.
fn expr_depth(e: &Expr) -> usize {
    let mut deepest = 0;
    e.apply_children(|child| {
        deepest = deepest.max(expr_depth(child));
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("walk");
    deepest + 1
}

/// The deepest filter predicate in the optimized plan of `source`, on a
/// 2 MiB thread: its conjunct count and its depth.
fn merged_filter(source: String) -> (usize, usize) {
    on_stack(WORKER_STACK, move || async move {
        let fixture = fixture().await;
        let plan = optimized(&fixture, &source).await;
        let mut deepest = (0, 0);
        plan.apply(|node| {
            if let LogicalPlan::Filter(filter) = node {
                let depth = expr_depth(&filter.predicate);
                if depth > deepest.1 {
                    deepest = (split_conjunction(&filter.predicate).len(), depth);
                }
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .expect("walk");
        deepest
    })
}

/// DataFusion merges consecutive `WHERE` conditions into one `AND`, so the
/// merged condition is what is bounded: at the deepest condition a second
/// `WHERE` is refused. The optimized plan holds one conjunct per `WHERE`, and
/// after simplification it is shallower than the binder counts it.
#[test]
fn the_deepest_long_where_runs_on_a_worker_stack() {
    assert_eq!(worst(Kind::Long, Shape::Where), (7, 1, Refusal::Merged));
    assert_eq!(
        merged_filter(Shape::Where.statement(Kind::Long, 7, 1)),
        (1, 19)
    );
    assert_eq!(
        worst_at(Kind::Long, Shape::Where, Some(5)),
        (5, 7, Refusal::Merged)
    );
    assert_eq!(
        merged_filter(Shape::Where.statement(Kind::Long, 5, 7)),
        (7, 15)
    );
    assert_eq!(
        worst_at(Kind::Long, Shape::Where, Some(1)),
        (1, 10, Refusal::Stages)
    );
}

#[test]
fn the_deepest_double_where_runs_on_a_worker_stack() {
    assert_eq!(worst(Kind::Double, Shape::Where), (18, 1, Refusal::Merged));
    assert_eq!(
        worst_at(Kind::Double, Shape::Where, Some(17)),
        (17, 2, Refusal::Merged)
    );
}

/// Every conjunct is distinct, so the optimized plan keeps each of them in
/// one merged condition, and that condition is what is bounded.
#[test]
fn the_deepest_and_chain_runs_on_a_worker_stack() {
    assert_eq!(worst(Kind::Long, Shape::And), (22, 1, Refusal::Merged));
    assert_eq!(
        merged_filter(Shape::And.statement(Kind::Long, 22, 1)),
        (23, 24)
    );
    assert_eq!(
        worst_at(Kind::Long, Shape::And, Some(2)),
        (2, 7, Refusal::Merged)
    );
    assert_eq!(
        merged_filter(Shape::And.statement(Kind::Long, 2, 7)),
        (21, 22)
    );
    // A LIMIT keeps the conditions above it apart from those below.
    let and = |from: usize| {
        (from..from + 12)
            .map(|j| format!("dur != -{j}"))
            .collect::<Vec<_>>()
            .join(" AND ")
    };
    assert_eq!(
        merged_filter(format!(
            "FROM logs | WHERE {} | LIMIT 100 | WHERE {}",
            and(1),
            and(13)
        )),
        (12, 13)
    );
}

/// A group counts as the depth of the expression computing it, which a
/// `WHERE` over it inlines below the aggregate.
#[test]
fn the_deepest_long_stats_group_runs_on_a_worker_stack() {
    assert_eq!(worst(Kind::Long, Shape::StatsGroup), (3, 1, Refusal::Depth));
    assert_eq!(
        worst_at(Kind::Long, Shape::StatsGroup, Some(1)),
        (1, 4, Refusal::Stages)
    );
}

#[test]
fn the_deepest_double_stats_group_runs_on_a_worker_stack() {
    assert_eq!(
        worst(Kind::Double, Shape::StatsGroup),
        (7, 1, Refusal::Depth)
    );
    assert_eq!(
        worst_at(Kind::Double, Shape::StatsGroup, Some(1)),
        (1, 4, Refusal::Depth)
    );
}

/// Four groups each computed from the one before, then a `WHERE` over the
/// last: the second group already inlines past the depth limit.
#[test]
fn a_chain_of_groups_filtered_after_the_last_is_refused() {
    let chain = " / 1".repeat(7);
    let source = format!(
        "FROM logs | STATS c = COUNT(*) BY g1 = dur{chain} \
         | STATS c = COUNT(*) BY g2 = g1{chain} | STATS c = COUNT(*) BY g3 = g2{chain} \
         | STATS c = COUNT(*) BY g4 = g3{chain} | WHERE g4{chain} >= 0"
    );
    let found = on_stack(WORKER_STACK, move || async move {
        match prepare(&source) {
            Ok(prepared) => {
                let fixture = fixture().await;
                let outcome = run_prepared(&fixture, &prepared).await.expect("runs");
                Ok(rows(&outcome))
            }
            Err(err) => Err(err.to_string()),
        }
    });
    assert_eq!(
        found,
        Err(format!(
            "line 1:74: STATS expression [g1{chain}] is 44 levels deep, past the limit of 24: \
             each operator, comparison and function call is a level, long arithmetic takes \
             three levels per operator, and a column computed by ROW, EVAL or STATS ... BY \
             counts as the expression computing it, so arithmetic chained through EVAL columns \
             adds up"
        ))
    );
}

/// The request time range is a stage the statement does not write, so it
/// does not count against the stage limit, and the longest statements still
/// run with it, its conditions merged into the statement's own.
#[test]
fn the_longest_statements_run_with_a_request_range_on_a_worker_stack() {
    assert_eq!(
        worst_in(Kind::Long, Shape::Sort, Some(7), true),
        (7, 10, Refusal::Stages)
    );
    assert_eq!(
        worst_in(Kind::Double, Shape::Sort, Some(19), true),
        (19, 5, Refusal::Stages)
    );
    assert_eq!(
        worst_in(Kind::Long, Shape::Where, Some(7), true),
        (7, 1, Refusal::Merged)
    );
    assert_eq!(
        worst_in(Kind::Long, Shape::Where, Some(5), true),
        (5, 7, Refusal::Merged)
    );
    assert_eq!(
        worst_in(Kind::Long, Shape::Where, Some(1), true),
        (1, 10, Refusal::Stages)
    );
    assert_eq!(
        worst_in(Kind::Long, Shape::And, Some(22), true),
        (22, 1, Refusal::Merged)
    );
}

#[test]
fn the_longest_stats_run_with_a_request_range_on_a_worker_stack() {
    assert_eq!(
        worst_in(Kind::Long, Shape::StatsField, Some(7), true),
        (7, 5, Refusal::Stages)
    );
    assert_eq!(
        worst_in(Kind::Long, Shape::StatsGroup, Some(1), true),
        (1, 4, Refusal::Stages)
    );
    assert_eq!(
        worst_in(Kind::Long, Shape::Eval, Some(7), true),
        (7, 99, Refusal::Parse)
    );
}

#[test]
fn the_deepest_long_eval_runs_on_a_worker_stack() {
    assert_eq!(worst(Kind::Long, Shape::Eval), (7, 99, Refusal::Parse));
}

#[test]
fn the_deepest_double_eval_runs_on_a_worker_stack() {
    assert_eq!(worst(Kind::Double, Shape::Eval), (19, 99, Refusal::Parse));
}

/// A column counts as the depth of what computes it, so a chain of them is
/// refused once the expression it inlines to is too deep.
#[test]
fn the_deepest_long_chain_runs_on_a_worker_stack() {
    assert_eq!(worst(Kind::Long, Shape::Chain), (7, 1, Refusal::Depth));
    assert_eq!(
        worst_at(Kind::Long, Shape::Chain, Some(1)),
        (1, 7, Refusal::Depth)
    );
}

#[test]
fn the_deepest_double_chain_runs_on_a_worker_stack() {
    assert_eq!(worst(Kind::Double, Shape::Chain), (19, 1, Refusal::Depth));
    assert_eq!(
        worst_at(Kind::Double, Shape::Chain, Some(1)),
        (1, 5, Refusal::Depth)
    );
}

#[test]
fn the_widest_long_eval_runs_on_a_worker_stack() {
    assert_eq!(
        worst(Kind::Long, Shape::EvalFields),
        (7, 3640, Refusal::Parse)
    );
}

#[test]
fn the_widest_double_eval_runs_on_a_worker_stack() {
    assert_eq!(
        worst(Kind::Double, Shape::EvalFields),
        (19, 1560, Refusal::Parse)
    );
}

#[test]
fn the_deepest_long_sort_runs_on_a_worker_stack() {
    assert_eq!(worst(Kind::Long, Shape::Sort), (7, 10, Refusal::Stages));
}

#[test]
fn the_deepest_double_sort_runs_on_a_worker_stack() {
    assert_eq!(worst(Kind::Double, Shape::Sort), (19, 5, Refusal::Stages));
}

#[test]
fn the_deepest_long_stats_field_runs_on_a_worker_stack() {
    assert_eq!(
        worst(Kind::Long, Shape::StatsField),
        (7, 5, Refusal::Stages)
    );
}

/// A sum's result carries its own range or finite check, so an expression
/// over the `STATS` below it is deeper than the same over a column: at the
/// deepest double expression a second `STATS` is refused, and one operator
/// less admits more.
#[test]
fn the_deepest_double_stats_field_runs_on_a_worker_stack() {
    assert_eq!(
        worst(Kind::Double, Shape::StatsField),
        (18, 1, Refusal::Depth)
    );
    assert_eq!(
        worst_at(Kind::Double, Shape::StatsField, Some(17)),
        (17, 3, Refusal::Stages)
    );
}

#[test]
fn the_deepest_long_stats_where_runs_on_a_worker_stack() {
    assert_eq!(
        worst(Kind::Long, Shape::StatsWhere),
        (7, 3, Refusal::Stages)
    );
}

#[test]
fn the_deepest_double_stats_where_runs_on_a_worker_stack() {
    assert_eq!(
        worst(Kind::Double, Shape::StatsWhere),
        (18, 1, Refusal::Depth)
    );
    assert_eq!(
        worst_at(Kind::Double, Shape::StatsWhere, Some(17)),
        (17, 2, Refusal::Stages)
    );
}

/// A statement at a nesting count, and the cell `x` it returns when it is
/// admitted.
type Nested = (fn(usize) -> String, Option<&'static str>);

/// Each statement at the deepest nesting the parser admits, on a 2 MiB
/// thread: the rows it returns, or its depth refusal.
#[test]
fn the_parsers_deepest_statements_run_or_are_refused_on_a_worker_stack() {
    let shapes: [Nested; 9] = [
        (|c| format!("ROW x = 9{}", " / 1".repeat(c)), None),
        (|c| format!("ROW x = 1.0{}", " + 1".repeat(c)), None),
        (|c| format!("ROW x = {}true", "NOT ".repeat(c)), None),
        (|c| format!("ROW x = {}9", "- ".repeat(c)), None),
        (
            |c| format!("ROW x = {}9{}", "(".repeat(c), ")".repeat(c)),
            Some("9"),
        ),
        (
            |c| format!("ROW x = {}9{}", "-(".repeat(c), ")".repeat(c)),
            None,
        ),
        (
            |c| {
                format!(
                    "FROM logs | WHERE {}",
                    vec!["dur >= 0"; c + 1].join(" AND ")
                )
            },
            None,
        ),
        (
            |c| {
                format!(
                    "FROM logs | STATS x = SUM(severity_num){}",
                    " / 1".repeat(c)
                )
            },
            None,
        ),
        (
            |c| format!("FROM logs | SORT dur{}", " / 1".repeat(c)),
            None,
        ),
    ];
    for (statement, rows) in shapes {
        let outcome = on_stack(WORKER_STACK, move || async move {
            let parses = |c| !matches!(prepare(&statement(c)), Err(EsqlError::Parse(_)));
            let c = largest(parses);
            let source = statement(c);
            match prepare(&source) {
                Ok(prepared) => {
                    let fixture = fixture().await;
                    let outcome = run_prepared(&fixture, &prepared).await.expect("runs");
                    Ok(select(&outcome, &["x".to_string()]))
                }
                Err(err) => Err((c, refusal(&err))),
            }
        });
        match rows {
            Some(cell) => assert_eq!(outcome, Ok(vec![vec![cell.to_string()]])),
            None => assert!(
                matches!(outcome, Err((c, Refusal::Depth)) if c >= 63),
                "{outcome:?}"
            ),
        }
    }
}
