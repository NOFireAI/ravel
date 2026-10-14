//! The worst statement the parser's default limits admit, per shape, run from
//! prepare through execution and drop on a thread with a server worker's
//! 2 MiB stack: each returns its exact rows, and one step past it is a typed
//! refusal.

use ravel_sql::esql::{EsqlError, EsqlOutcome, MAX_EXPRESSION_DEPTH};

use crate::support::{WORKER_STACK, columns, fixture, on_stack, prepare, rows, run_prepared};

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
    /// `FROM logs`, then `| WHERE e >= 0` per stage.
    Where,
    /// `FROM logs`, then per stage a `WHERE` of `k` `AND`ed comparisons.
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
                stages(&|_| format!(" | WHERE {} >= 0", e("dur")))
            ),
            Shape::And => {
                let and = vec!["dur >= 0"; k + 1].join(" AND ");
                format!("FROM logs{}", stages(&|_| format!(" | WHERE {and}")))
            }
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
    Depth,
    Stages,
    Parse,
}

fn refusal(err: &EsqlError) -> Refusal {
    match err {
        EsqlError::Parse(_) => Refusal::Parse,
        EsqlError::Verification { message, .. }
            if message.ends_with(&format!(
                "levels deep, more than the limit of {MAX_EXPRESSION_DEPTH}"
            )) =>
        {
            Refusal::Depth
        }
        EsqlError::Verification { message, .. } if message.contains("plan stages") => {
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
    on_stack(WORKER_STACK, move || async move {
        let admitted = |k, n| prepare(&shape.statement(kind, k, n));
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
        let outcome = run_prepared(&fixture, &prepared).await.expect("runs");
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

#[test]
fn the_deepest_long_where_runs_on_a_worker_stack() {
    assert_eq!(worst(Kind::Long, Shape::Where), (7, 10, Refusal::Stages));
}

#[test]
fn the_deepest_double_where_runs_on_a_worker_stack() {
    assert_eq!(worst(Kind::Double, Shape::Where), (18, 5, Refusal::Stages));
}

#[test]
fn the_deepest_and_chain_runs_on_a_worker_stack() {
    assert_eq!(worst(Kind::Long, Shape::And), (22, 10, Refusal::Stages));
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

/// Each statement at the deepest nesting the parser admits, on a 2 MiB
/// thread: the rows it returns, or its depth refusal.
#[test]
fn the_parsers_deepest_statements_run_or_are_refused_on_a_worker_stack() {
    let shapes: [(fn(usize) -> String, Option<&str>); 9] = [
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
