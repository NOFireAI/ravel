//! The worst statement the parser's default limits admit, per shape, run from
//! prepare through execution and drop on a thread with a server worker's
//! 2 MiB stack: each returns its exact rows, and one step past it is a typed
//! refusal. Each limit also carries the depth the binder counts for the
//! shape's deepest expression and the depth of the deepest expression in the
//! optimized plan that runs, every difference between the two named.

use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::logical_expr::utils::split_conjunction;
use datafusion::logical_expr::{Expr, LogicalPlan};
use ravel_sql::esql::{
    self, EsqlError, EsqlOutcome, MAX_EXPRESSION_DEPTH, MAX_PLAN_STAGES, PrepareContext,
    PreparedEsql,
};

use crate::support::{
    BASE, NOW, SECOND, WORKER_STACK, columns, context, fixture, on_stack, prepare, rows,
    run_prepared, session,
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

    /// `base` and `k` operators that keep its value: each a remainder by a
    /// modulus larger than every value the fixture holds, a long one for a
    /// long and a double one for a double. No modulus is one the simplifier
    /// can remove, and each operator of each `stage` has its own, so no two
    /// expressions of a statement share a subexpression.
    fn expr(self, base: &str, k: usize, stage: usize) -> String {
        let mut out = base.to_string();
        for j in 0..k {
            let modulus = 1000 + stage * k + j;
            match self {
                Kind::Long => out.push_str(&format!(" % {modulus}")),
                Kind::Double => out.push_str(&format!(" % {modulus}.0")),
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
    /// `FROM logs`, then `| WHERE e != -i` per stage `i`.
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
    /// `FROM logs`, then `| SORT e` per stage.
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
        let e = |base: &str, stage: usize| kind.expr(base, k, stage);
        let stages = |stage: &dyn Fn(usize) -> String| (1..=n).map(stage).collect::<String>();
        match self {
            Shape::Row => format!(
                "ROW x1 = {}{}",
                e("9", 1),
                stages(&|i| if i == 1 {
                    String::new()
                } else {
                    format!(" | EVAL x{i} = {}", e("9", i))
                })
            ),
            Shape::Where => format!(
                "FROM logs{}",
                stages(&|i| format!(" | WHERE {} != -{i}", e("dur", i)))
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
                stages(&|i| format!(" | EVAL x{i} = {}", e("dur", i)))
            ),
            Shape::Chain => format!(
                "FROM logs{}",
                stages(&|i| {
                    let over = if i == 1 {
                        "dur".to_string()
                    } else {
                        format!("x{}", i - 1)
                    };
                    format!(" | EVAL x{i} = {}", e(&over, i))
                })
            ),
            Shape::EvalFields => format!(
                "FROM logs | EVAL {}",
                (1..=n)
                    .map(|i| format!("x{i} = {}", e("dur", i)))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            Shape::Sort => format!(
                "FROM logs{}",
                stages(&|i| format!(" | SORT {}", e("dur", i)))
            ),
            Shape::StatsField => format!(
                "FROM logs{}",
                stages(&|i| {
                    let sum = if i == 1 {
                        "SUM(severity_num)"
                    } else {
                        "SUM(s)"
                    };
                    format!(" | STATS s = {} BY service", e(sum, i))
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
                    format!(
                        " | STATS s = SUM({sum}) WHERE {} >= 0 BY service",
                        e(over, i)
                    )
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
                    format!(" | STATS c = COUNT(*) BY g{i} = {}", e(&over, i))
                }),
                e(&format!("g{n}"), n + 1)
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

/// The depth the binder states refusing `err`: the refused expression's, or
/// the deepest condition's when it refuses merged `WHERE` conditions.
fn stated_depth(err: &EsqlError) -> usize {
    let message = match err {
        EsqlError::Verification { message, .. } => message,
        other => panic!("not a depth refusal: {other:?}"),
    };
    let depth = match message.split_once("the deepest ") {
        Some((_, rest)) => rest.split(' ').next(),
        None => message
            .split_once(" levels deep, past")
            .and_then(|(head, _)| head.rsplit(' ').next()),
    };
    depth
        .and_then(|depth| depth.parse().ok())
        .unwrap_or_else(|| panic!("no depth in {message}"))
}

/// The depth the binder counts for the deepest expression of `statement(k)`,
/// from the depths it states refusing `statement(k + 1)` and
/// `statement(k + 2)`: each step adds the same levels, which the refusal of
/// `statement(k + 3)` is checked to confirm.
fn counted_at(statement: impl Fn(usize) -> String, k: usize) -> usize {
    let stated = |k| stated_depth(&prepare(&statement(k)).expect_err("refused as too deep"));
    let (next, after, last) = (stated(k + 1), stated(k + 2), stated(k + 3));
    assert!(next > MAX_EXPRESSION_DEPTH, "{next}");
    assert!(after > next, "{next}, {after}");
    assert_eq!(after - next, last - after, "{next}, {after}, {last}");
    2 * next - after
}

/// The depth the binder counts for the condition `condition(0)`: the deepest
/// condition it states refusing a `WHERE` of it merged with one of
/// `condition(1) AND condition(2) AND ...`, as many as pass the limit, each
/// as deep as `condition(0)`.
fn counted_condition(condition: impl Fn(usize) -> String) -> usize {
    for m in 1..=MAX_EXPRESSION_DEPTH {
        let more: Vec<String> = (1..=m).map(&condition).collect();
        let source = format!(
            "FROM logs | WHERE {} | WHERE {}",
            condition(0),
            more.join(" AND ")
        );
        if let Err(err) = prepare(&source) {
            assert_eq!(refusal(&err), Refusal::Merged, "{err}");
            return stated_depth(&err);
        }
    }
    panic!("no merged refusal")
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

/// The depth of `e` as the binder counts it: an alias adds no level, and an
/// aggregate call is as deep as its deepest argument or filter.
fn counted_depth(e: &Expr) -> usize {
    match e {
        Expr::Alias(alias) => counted_depth(&alias.expr),
        Expr::AggregateFunction(call) => call
            .params
            .args
            .iter()
            .chain(call.params.filter.as_deref())
            .map(counted_depth)
            .max()
            .unwrap_or(0),
        other => expr_depth(other),
    }
}

/// `f` of the optimized plan of `source`, prepared with the request time
/// range [`RANGE`] when `ranged`, on a 2 MiB thread.
fn in_plan<T: Send + 'static>(
    source: String,
    ranged: bool,
    f: impl FnOnce(&LogicalPlan) -> T + Send + 'static,
) -> T {
    on_stack(WORKER_STACK, move || async move {
        let fixture = fixture().await;
        let prepared = prepare_in(&source, ranged).expect("prepares");
        let plan = prepared
            .optimized_plan(&session(&fixture.store))
            .await
            .expect("plans");
        f(&plan)
    })
}

/// The deepest expression in the optimized plan of `source`: the
/// projections, filters, sort keys, aggregate groups, arguments and filters,
/// and the filters pushed into the scan, measured by [`counted_depth`].
fn optimized_depth(source: String, ranged: bool) -> usize {
    in_plan(source, ranged, |plan| {
        let mut deepest = 0;
        plan.apply(|node| {
            if let LogicalPlan::TableScan(scan) = node {
                deepest = scan
                    .filters
                    .iter()
                    .map(counted_depth)
                    .fold(deepest, usize::max);
            }
            node.apply_expressions(|e| {
                deepest = deepest.max(counted_depth(e));
                Ok(TreeNodeRecursion::Continue)
            })
        })
        .expect("walk");
        deepest
    })
}

/// The deepest filter predicate in the optimized plan of `source`: its
/// conjunct count and its depth.
fn merged_filter(source: String, ranged: bool) -> (usize, usize) {
    in_plan(source, ranged, |plan| {
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

/// The filters the optimized plan of `source` pushes into its scan.
fn scan_filters(source: String, ranged: bool) -> Vec<String> {
    in_plan(source, ranged, |plan| {
        let mut filters = Vec::new();
        plan.apply(|node| {
            if let LogicalPlan::TableScan(scan) = node {
                filters.extend(scan.filters.iter().map(ToString::to_string));
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .expect("walk");
        filters
    })
}

/// A shape's limit: the most operators `k` and repetitions `n` it admits,
/// what refuses `n + 1`, the depth the binder counts for its deepest
/// expression when the search found `k`, and the depth of the deepest
/// expression in the optimized plan of the statement at `k` and `n`.
#[derive(Debug, PartialEq, Eq)]
struct Limit {
    k: usize,
    n: usize,
    longer: Refusal,
    counted: Option<usize>,
    optimized: usize,
}

/// Levels the simplifier removes from a long expression whose first
/// operator is over a column: the binder counts that operator's divisor as
/// `nullif(divisor, 0)` cast to a decimal, which the simplifier folds into a
/// literal. Measured by [`a_constant_divisor_folds_into_a_literal`].
const FOLDED_DIVISOR: usize = 1;

/// Levels the optimizer removes from a long expression compared with a
/// literal: it compares the decimal the expression computes in place of its
/// `TRY_CAST` to a long. Measured by
/// [`a_long_compared_with_a_literal_drops_its_checked_cast`].
const UNWRAPPED_CAST: usize = 1;

/// Levels common subexpression elimination removes from a double expression
/// outside a `WHERE`: it computes the finite check's operand, which the check
/// reads three times, in a projection below, and leaves the check's `CASE`,
/// `AND` and comparison above it.
const SHARED_FINITE: usize = 3;

/// Levels common subexpression elimination removes from the deepest of
/// several expressions of one stage that cast the same column, the fields of
/// an `EVAL` or the merged conditions of a filter: it casts the column once,
/// in a projection below.
const SHARED_CAST: usize = 1;

/// On a 2 MiB thread: the deepest `k` the shape admits at one repetition, or
/// `depth` when given, the most repetitions `n` it admits at that `k`, the
/// statement at both run to its exact rows, and what refuses `n + 1`. A `k`
/// the search found is checked to refuse `k + 1` as too deep, and the binder's
/// count is taken from that refusal: every stage of a shape counts as its
/// first, the `WHERE` conditions a shape merges aside, which its limit holds
/// to one stage.
fn worst_at(kind: Kind, shape: Shape, depth: Option<usize>) -> Limit {
    worst_in(kind, shape, depth, false)
}

/// [`worst_at`], each statement prepared with the request time range
/// [`RANGE`] when `ranged`.
fn worst_in(kind: Kind, shape: Shape, depth: Option<usize>, ranged: bool) -> Limit {
    let (k, n, longer, counted) = on_stack(WORKER_STACK, move || async move {
        let admitted = |k, n| prepare_in(&shape.statement(kind, k, n), ranged);
        let (k, counted) = match depth {
            Some(k) => (k, None),
            None => {
                let k = largest(|k| admitted(k, 1).is_ok());
                let deeper = admitted(k + 1, 1).expect_err("k + 1 is refused");
                assert_eq!(refusal(&deeper), Refusal::Depth, "{deeper}");
                (k, Some(counted_at(|k| shape.statement(kind, k, 1), k)))
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
        (k, n, refusal(&longer), counted)
    });
    let optimized = optimized_depth(shape.statement(kind, k, n), ranged);
    eprintln!(
        "{kind:?} {shape:?} ranged={ranged}: k = {k}, n = {n}, {longer:?}, counted {counted:?}, optimized {optimized}"
    );
    Limit {
        k,
        n,
        longer,
        counted,
        optimized,
    }
}

fn worst(kind: Kind, shape: Shape) -> Limit {
    worst_at(kind, shape, None)
}

/// One `EVAL` field, so nothing is shared: the folded divisor is the whole
/// difference.
#[test]
fn a_constant_divisor_folds_into_a_literal() {
    let eval = |k| Shape::Eval.statement(Kind::Long, k, 1);
    assert_eq!(counted_at(eval, 7), 23);
    let plan = in_plan(eval(7), false, |plan| plan.display_indent().to_string());
    assert!(!plan.contains("nullif"), "{plan}");
    assert_eq!(optimized_depth(eval(7), false), 23 - FOLDED_DIVISOR);
}

#[test]
fn a_long_compared_with_a_literal_drops_its_checked_cast() {
    let source = Shape::Where.statement(Kind::Long, 1, 1);
    assert_eq!(
        counted_condition(|j| format!("dur % {} != -1", 1000 + j)),
        6
    );
    let predicate = in_plan(source.clone(), false, |plan| {
        let mut predicate = None;
        plan.apply(|node| {
            if let LogicalPlan::Filter(filter) = node {
                predicate = Some(filter.predicate.clone());
            }
            Ok(TreeNodeRecursion::Continue)
        })
        .expect("walk");
        predicate.expect("a filter")
    });
    let Expr::BinaryExpr(comparison) = &predicate else {
        panic!("{predicate}");
    };
    assert!(!matches!(*comparison.left, Expr::TryCast(_)), "{predicate}");
    assert_eq!(
        merged_filter(source, false),
        (1, 6 - FOLDED_DIVISOR - UNWRAPPED_CAST)
    );
}

/// The simplifier folds the constant arithmetic to a literal.
#[test]
fn the_deepest_long_row_runs_on_a_worker_stack() {
    assert_eq!(
        worst(Kind::Long, Shape::Row),
        Limit {
            k: 7,
            n: 100,
            longer: Refusal::Parse,
            counted: Some(23),
            optimized: 1,
        }
    );
}

/// The simplifier folds the constant arithmetic to a literal, and the finite
/// check over it stays: `CASE`, `AND`, a comparison and the literal's column.
#[test]
fn the_deepest_double_row_runs_on_a_worker_stack() {
    assert_eq!(
        worst(Kind::Double, Shape::Row),
        Limit {
            k: 19,
            n: 100,
            longer: Refusal::Parse,
            counted: Some(24),
            optimized: 4,
        }
    );
}

/// DataFusion merges consecutive `WHERE` conditions into one `AND`, so the
/// merged condition is what is bounded: at the deepest condition a second
/// `WHERE` is refused. The optimized plan holds one conjunct per `WHERE`,
/// each [`FOLDED_DIVISOR`] and [`UNWRAPPED_CAST`] shallower than the binder
/// counts it, and [`SHARED_CAST`] shallower again when several share the
/// filter.
#[test]
fn the_deepest_long_where_runs_on_a_worker_stack() {
    let shallower = FOLDED_DIVISOR + UNWRAPPED_CAST;
    let merged = shallower + SHARED_CAST;
    assert_eq!(
        worst(Kind::Long, Shape::Where),
        Limit {
            k: 7,
            n: 1,
            longer: Refusal::Merged,
            counted: Some(24),
            optimized: 24 - shallower,
        }
    );
    assert_eq!(
        merged_filter(Shape::Where.statement(Kind::Long, 7, 1), false),
        (1, 24 - shallower)
    );
    // Seven conditions of 18 levels each run as one of 6 + 18.
    assert_eq!(
        worst_at(Kind::Long, Shape::Where, Some(5)),
        Limit {
            k: 5,
            n: 7,
            longer: Refusal::Merged,
            counted: None,
            optimized: 6 + 18 - merged,
        }
    );
    assert_eq!(
        merged_filter(Shape::Where.statement(Kind::Long, 5, 7), false),
        (7, 6 + 18 - merged)
    );
    assert_eq!(
        worst_at(Kind::Long, Shape::Where, Some(1)),
        Limit {
            k: 1,
            n: 10,
            longer: Refusal::Stages,
            counted: None,
            optimized: 9 + 6 - merged,
        }
    );
}

#[test]
fn the_deepest_double_where_runs_on_a_worker_stack() {
    assert_eq!(
        worst(Kind::Double, Shape::Where),
        Limit {
            k: 18,
            n: 1,
            longer: Refusal::Merged,
            counted: Some(24),
            optimized: 24,
        }
    );
    // Two conditions of 23 levels each run as one of 1 + 23.
    assert_eq!(
        worst_at(Kind::Double, Shape::Where, Some(17)),
        Limit {
            k: 17,
            n: 2,
            longer: Refusal::Merged,
            counted: None,
            optimized: 1 + 23 - SHARED_CAST,
        }
    );
}

/// Every conjunct is distinct, so the optimized plan keeps each of them in
/// one merged condition, and that condition is what is bounded.
#[test]
fn the_deepest_and_chain_runs_on_a_worker_stack() {
    assert_eq!(
        worst(Kind::Long, Shape::And),
        Limit {
            k: 22,
            n: 1,
            longer: Refusal::Merged,
            counted: Some(24),
            optimized: 24,
        }
    );
    assert_eq!(
        merged_filter(Shape::And.statement(Kind::Long, 22, 1), false),
        (23, 24)
    );
    assert_eq!(
        worst_at(Kind::Long, Shape::And, Some(2)),
        Limit {
            k: 2,
            n: 7,
            longer: Refusal::Merged,
            counted: None,
            optimized: 22,
        }
    );
    assert_eq!(
        merged_filter(Shape::And.statement(Kind::Long, 2, 7), false),
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
        merged_filter(
            format!(
                "FROM logs | WHERE {} | LIMIT 100 | WHERE {}",
                and(1),
                and(13)
            ),
            false
        ),
        (12, 13)
    );
}

/// A group counts as the depth of the expression computing it, which a
/// `WHERE` over it inlines below the aggregate.
#[test]
fn the_deepest_long_stats_group_runs_on_a_worker_stack() {
    let shallower = FOLDED_DIVISOR + UNWRAPPED_CAST;
    assert_eq!(
        worst(Kind::Long, Shape::StatsGroup),
        Limit {
            k: 3,
            n: 1,
            longer: Refusal::Depth,
            counted: Some(21),
            optimized: 21 - shallower,
        }
    );
    // Four groups of one operator each, inlined under the WHERE's own.
    assert_eq!(
        worst_at(Kind::Long, Shape::StatsGroup, Some(1)),
        Limit {
            k: 1,
            n: 4,
            longer: Refusal::Stages,
            counted: None,
            optimized: 5 * 3 + 2 + 1 - shallower,
        }
    );
}

/// The filter pushed into the scan holds the `WHERE` with every group
/// inlined, as the binder counts it.
#[test]
fn the_deepest_double_stats_group_runs_on_a_worker_stack() {
    assert_eq!(
        worst(Kind::Double, Shape::StatsGroup),
        Limit {
            k: 7,
            n: 1,
            longer: Refusal::Depth,
            counted: Some(23),
            optimized: 23,
        }
    );
    assert_eq!(
        worst_at(Kind::Double, Shape::StatsGroup, Some(1)),
        Limit {
            k: 1,
            n: 4,
            longer: Refusal::Depth,
            counted: None,
            optimized: 23,
        }
    );
    // The binder counts the four-group statement one level below the same
    // statement with one more double operator in its WHERE.
    let four = Shape::StatsGroup.statement(Kind::Double, 1, 4);
    let four = four.strip_suffix(" >= 0").expect("a WHERE").to_string();
    let more = move |j| format!("{four}{} >= 0", Kind::Double.expr("", j, 9));
    assert_eq!(counted_at(more, 1) - 1, 23);
}

/// The `WHERE` over the group runs as a filter below the aggregate, over the
/// column the group is computed from, as deep as the binder counts it less
/// the levels its long arithmetic sheds.
#[test]
fn a_where_on_a_group_runs_below_the_aggregate_on_the_groups_expression() {
    let shallower = FOLDED_DIVISOR + UNWRAPPED_CAST;
    let filters = |k, n| {
        in_plan(
            Shape::StatsGroup.statement(Kind::Long, k, n),
            false,
            |plan| {
                let mut filters = Vec::new();
                let mut below = false;
                plan.apply(|node| {
                    below |= matches!(node, LogicalPlan::Aggregate(_));
                    if let LogicalPlan::Filter(filter) = node {
                        let mut columns: Vec<String> = filter
                            .predicate
                            .column_refs()
                            .iter()
                            .map(|column| column.name.clone())
                            .collect();
                        columns.sort();
                        filters.push((below, columns, expr_depth(&filter.predicate)));
                    }
                    Ok(TreeNodeRecursion::Continue)
                })
                .expect("walk");
                filters
            },
        )
    };
    let counted = counted_at(|k| Shape::StatsGroup.statement(Kind::Long, k, 1), 3);
    assert_eq!(counted, 21);
    assert_eq!(
        filters(3, 1),
        vec![(true, vec!["dur".to_string()], counted - shallower)]
    );
    assert_eq!(
        filters(1, 4),
        vec![(true, vec!["dur".to_string()], 5 * 3 + 2 + 1 - shallower)]
    );
}

/// Four groups each computed from the one before, then a `WHERE` over the
/// last: the second group already inlines past the depth limit.
#[test]
fn a_chain_of_groups_filtered_after_the_last_is_refused() {
    let chain = |stage: usize| Kind::Long.expr("", 7, stage);
    let source = format!(
        "FROM logs | STATS c = COUNT(*) BY g1 = dur{} \
         | STATS c = COUNT(*) BY g2 = g1{} | STATS c = COUNT(*) BY g3 = g2{} \
         | STATS c = COUNT(*) BY g4 = g3{} | WHERE g4{} >= 0",
        chain(1),
        chain(2),
        chain(3),
        chain(4),
        chain(5)
    );
    let column = source
        .find("STATS c = COUNT(*) BY g2")
        .expect("a second STATS")
        + 1;
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
            "line 1:{column}: STATS expression [g1{}] is 44 levels deep, past the limit of 24: \
             each operator, comparison and function call is a level, long arithmetic takes \
             three levels per operator, and a column computed by ROW, EVAL or STATS ... BY \
             counts as the expression computing it, so arithmetic chained through EVAL columns \
             adds up",
            chain(2)
        ))
    );
}

/// The request time range is a stage the statement does not write, so it
/// does not count against the stage limit, and the longest statements still
/// run with it. Its conditions run in the scan apart from the statement's
/// own, so each optimized depth is the one without the range.
#[test]
fn the_longest_statements_run_with_a_request_range_on_a_worker_stack() {
    let shallower = FOLDED_DIVISOR + UNWRAPPED_CAST;
    let merged = shallower + SHARED_CAST;
    assert_eq!(
        worst_in(Kind::Long, Shape::Sort, Some(7), true),
        Limit {
            k: 7,
            n: 10,
            longer: Refusal::Stages,
            counted: None,
            optimized: 23 - FOLDED_DIVISOR,
        }
    );
    assert_eq!(
        worst_in(Kind::Double, Shape::Sort, Some(19), true),
        Limit {
            k: 19,
            n: 5,
            longer: Refusal::Stages,
            counted: None,
            optimized: 24 - SHARED_FINITE,
        }
    );
    assert_eq!(
        worst_in(Kind::Long, Shape::Where, Some(7), true),
        Limit {
            k: 7,
            n: 1,
            longer: Refusal::Merged,
            counted: None,
            optimized: 24 - shallower,
        }
    );
    assert_eq!(
        worst_in(Kind::Long, Shape::Where, Some(5), true),
        Limit {
            k: 5,
            n: 7,
            longer: Refusal::Merged,
            counted: None,
            optimized: 6 + 18 - merged,
        }
    );
    assert_eq!(
        worst_in(Kind::Long, Shape::Where, Some(1), true),
        Limit {
            k: 1,
            n: 10,
            longer: Refusal::Stages,
            counted: None,
            optimized: 9 + 6 - merged,
        }
    );
    assert_eq!(
        worst_in(Kind::Long, Shape::And, Some(22), true),
        Limit {
            k: 22,
            n: 1,
            longer: Refusal::Merged,
            counted: None,
            optimized: 24,
        }
    );
}

#[test]
fn the_longest_stats_run_with_a_request_range_on_a_worker_stack() {
    assert_eq!(
        worst_in(Kind::Long, Shape::StatsField, Some(7), true),
        Limit {
            k: 7,
            n: 5,
            longer: Refusal::Stages,
            counted: None,
            optimized: 23,
        }
    );
    assert_eq!(
        worst_in(Kind::Long, Shape::StatsGroup, Some(1), true),
        Limit {
            k: 1,
            n: 4,
            longer: Refusal::Stages,
            counted: None,
            optimized: 5 * 3 + 2 + 1 - FOLDED_DIVISOR - UNWRAPPED_CAST,
        }
    );
    assert_eq!(
        worst_in(Kind::Long, Shape::Eval, Some(7), true),
        Limit {
            k: 7,
            n: 99,
            longer: Refusal::Parse,
            counted: None,
            optimized: 23 - FOLDED_DIVISOR - SHARED_CAST,
        }
    );
}

/// The range's conditions run in the scan, apart from the conditions the
/// binder counts and merges, which run as deep with the range as without it.
#[test]
fn the_request_range_is_a_condition_apart_from_the_counted_ones() {
    let source = Shape::And.statement(Kind::Long, 22, 1);
    let without = scan_filters(source.clone(), false);
    let mut with = scan_filters(source.clone(), true);
    with.retain(|filter| !without.contains(filter));
    assert_eq!(with.len(), 2, "{with:?}");
    assert!(
        with.iter().all(|filter| filter.starts_with("logs.ts ")),
        "{with:?}"
    );
    assert_eq!(merged_filter(source.clone(), true), (23, 24));
    assert_eq!(optimized_depth(source, true), 24);
}

/// The analyzer casts a long compared with a double to a double, a level the
/// binder does not count.
#[test]
fn a_long_compared_with_a_double_gains_a_cast() {
    assert_eq!(counted_condition(|j| format!("dur > {j}.5")), 2);
    assert_eq!(
        merged_filter("FROM logs | WHERE dur > 1.5".to_string(), false),
        (1, 2 + 1)
    );
    assert_eq!(counted_condition(|j| format!("dur > {j}")), 2);
    assert_eq!(
        merged_filter("FROM logs | WHERE dur > 1".to_string(), false),
        (1, 2)
    );
}

/// The simplifier rewrites an `IN` list of three values as a chain of `OR`,
/// two levels deeper than the binder counts the list; a list of four stays a
/// list.
#[test]
fn a_short_in_list_runs_as_a_chain_of_or() {
    let three = |j: usize| format!("dur IN ({}, {}, {})", 3 * j + 1, 3 * j + 2, 3 * j + 3);
    assert_eq!(counted_condition(three), 2);
    assert_eq!(
        merged_filter(format!("FROM logs | WHERE {}", three(0)), false),
        (1, 2 + 2)
    );
    let four = |j: usize| {
        format!(
            "dur IN ({}, {}, {}, {})",
            4 * j + 1,
            4 * j + 2,
            4 * j + 3,
            4 * j + 4
        )
    };
    assert_eq!(counted_condition(four), 2);
    assert_eq!(
        merged_filter(format!("FROM logs | WHERE {}", four(0)), false),
        (1, 2)
    );
}

/// The operators these tests used before, `/ 1` and `% 1000` in turn, which
/// the simplifier removes or shares, measured against the remainders by
/// distinct moduli they use now: the binder counts both the same, and only
/// the second keeps its levels in the plan that runs.
#[test]
fn operators_the_simplifier_removes_hide_levels_the_measured_ones_keep() {
    let old = |k: usize| {
        (0..k)
            .map(|j| if j % 2 == 0 { " / 1" } else { " % 1000" })
            .collect::<String>()
    };
    let old_long = |k: usize| format!("FROM logs | WHERE dur{} != -1", old(k));
    let new_long = |k: usize| Shape::Where.statement(Kind::Long, k, 1);
    let old_double = |k: usize| format!("FROM logs | WHERE dur * 1.0{} != -1", old(k - 1));
    let new_double = |k: usize| Shape::Where.statement(Kind::Double, k, 1);
    let measured = [
        (counted_at(old_long, 7), optimized_depth(old_long(7), false)),
        (counted_at(new_long, 7), optimized_depth(new_long(7), false)),
        (
            counted_at(old_double, 18),
            optimized_depth(old_double(18), false),
        ),
        (
            counted_at(new_double, 18),
            optimized_depth(new_double(18), false),
        ),
    ];
    eprintln!("old long, new long, old double, new double: {measured:?}");
    assert_eq!(
        measured,
        [
            (24, 19),
            (24, 24 - FOLDED_DIVISOR - UNWRAPPED_CAST),
            (24, 14),
            (24, 24),
        ]
    );
}

#[test]
fn the_deepest_long_eval_runs_on_a_worker_stack() {
    assert_eq!(
        worst(Kind::Long, Shape::Eval),
        Limit {
            k: 7,
            n: 99,
            longer: Refusal::Parse,
            counted: Some(23),
            optimized: 23 - FOLDED_DIVISOR - SHARED_CAST,
        }
    );
}

#[test]
fn the_deepest_double_eval_runs_on_a_worker_stack() {
    assert_eq!(
        worst(Kind::Double, Shape::Eval),
        Limit {
            k: 19,
            n: 99,
            longer: Refusal::Parse,
            counted: Some(24),
            optimized: 24 - SHARED_FINITE - SHARED_CAST,
        }
    );
}

/// A column counts as the depth of what computes it, so a chain of them is
/// refused once the expression it inlines to is too deep.
#[test]
fn the_deepest_long_chain_runs_on_a_worker_stack() {
    assert_eq!(
        worst(Kind::Long, Shape::Chain),
        Limit {
            k: 7,
            n: 1,
            longer: Refusal::Depth,
            counted: Some(23),
            optimized: 23 - FOLDED_DIVISOR,
        }
    );
    // The statement returns every column of the chain, so the optimizer keeps
    // each in its own projection rather than inline it into the next: the
    // plan is as deep as one operator over a column.
    assert_eq!(
        worst_at(Kind::Long, Shape::Chain, Some(1)),
        Limit {
            k: 1,
            n: 7,
            longer: Refusal::Depth,
            counted: None,
            optimized: 5 - FOLDED_DIVISOR,
        }
    );
    // The refusal names the first stage past the limit, three levels deeper
    // than the seventh, which the binder counts at 2 + 7 * 3 = 23.
    for n in [8, 9] {
        let refused = prepare(&Shape::Chain.statement(Kind::Long, 1, n)).expect_err("refused");
        assert_eq!(stated_depth(&refused), 2 + 8 * 3);
    }
}

#[test]
fn the_deepest_double_chain_runs_on_a_worker_stack() {
    assert_eq!(
        worst(Kind::Double, Shape::Chain),
        Limit {
            k: 19,
            n: 1,
            longer: Refusal::Depth,
            counted: Some(24),
            optimized: 24 - SHARED_FINITE,
        }
    );
    // Each column in its own projection, as for a long chain: the deepest
    // expression is one stage's finite check over the operand below it.
    assert_eq!(
        worst_at(Kind::Double, Shape::Chain, Some(1)),
        Limit {
            k: 1,
            n: 5,
            longer: Refusal::Depth,
            counted: None,
            optimized: 4,
        }
    );
    // The refusal names the first stage past the limit, four levels deeper
    // than the fifth, which the binder counts at 2 + 5 * 4 = 22.
    for n in [6, 7] {
        let refused = prepare(&Shape::Chain.statement(Kind::Double, 1, n)).expect_err("refused");
        assert_eq!(stated_depth(&refused), 2 + 6 * 4);
    }
}

#[test]
fn the_widest_long_eval_runs_on_a_worker_stack() {
    assert_eq!(
        worst(Kind::Long, Shape::EvalFields),
        Limit {
            k: 7,
            n: 3640,
            longer: Refusal::Parse,
            counted: Some(23),
            optimized: 23 - FOLDED_DIVISOR - SHARED_CAST,
        }
    );
}

#[test]
fn the_widest_double_eval_runs_on_a_worker_stack() {
    assert_eq!(
        worst(Kind::Double, Shape::EvalFields),
        Limit {
            k: 19,
            n: 1340,
            longer: Refusal::Parse,
            counted: Some(24),
            optimized: 24 - SHARED_FINITE - SHARED_CAST,
        }
    );
}

#[test]
fn the_deepest_long_sort_runs_on_a_worker_stack() {
    assert_eq!(
        worst(Kind::Long, Shape::Sort),
        Limit {
            k: 7,
            n: 10,
            longer: Refusal::Stages,
            counted: Some(23),
            optimized: 23 - FOLDED_DIVISOR,
        }
    );
}

#[test]
fn the_deepest_double_sort_runs_on_a_worker_stack() {
    assert_eq!(
        worst(Kind::Double, Shape::Sort),
        Limit {
            k: 19,
            n: 5,
            longer: Refusal::Stages,
            counted: Some(24),
            optimized: 24 - SHARED_FINITE,
        }
    );
}

#[test]
fn the_deepest_long_stats_field_runs_on_a_worker_stack() {
    assert_eq!(
        worst(Kind::Long, Shape::StatsField),
        Limit {
            k: 7,
            n: 5,
            longer: Refusal::Stages,
            counted: Some(23),
            optimized: 23,
        }
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
        Limit {
            k: 18,
            n: 1,
            longer: Refusal::Depth,
            counted: Some(24),
            optimized: 24 - SHARED_FINITE,
        }
    );
    // A stage over a sum counts one level more than the first, as the
    // refusal of the second stage at 18 operators states, so 24 at 17.
    assert_eq!(
        worst_at(Kind::Double, Shape::StatsField, Some(17)),
        Limit {
            k: 17,
            n: 3,
            longer: Refusal::Stages,
            counted: None,
            optimized: 24 - SHARED_FINITE,
        }
    );
    let second = prepare(&Shape::StatsField.statement(Kind::Double, 18, 2)).expect_err("refused");
    assert_eq!(stated_depth(&second), 25);
}

#[test]
fn the_deepest_long_stats_where_runs_on_a_worker_stack() {
    assert_eq!(
        worst(Kind::Long, Shape::StatsWhere),
        Limit {
            k: 7,
            n: 3,
            longer: Refusal::Stages,
            counted: Some(24),
            optimized: 24 - FOLDED_DIVISOR - UNWRAPPED_CAST,
        }
    );
}

/// The finite check and the `>= 0` over it stay above the shared operand,
/// so the operand alone is the deepest: [`SHARED_FINITE`] and the comparison
/// shallower than the binder counts the condition.
#[test]
fn the_deepest_double_stats_where_runs_on_a_worker_stack() {
    assert_eq!(
        worst(Kind::Double, Shape::StatsWhere),
        Limit {
            k: 18,
            n: 1,
            longer: Refusal::Depth,
            counted: Some(24),
            optimized: 24 - SHARED_FINITE - 1,
        }
    );
    assert_eq!(
        worst_at(Kind::Double, Shape::StatsWhere, Some(17)),
        Limit {
            k: 17,
            n: 2,
            longer: Refusal::Stages,
            counted: None,
            optimized: 23 - SHARED_FINITE - 1,
        }
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
