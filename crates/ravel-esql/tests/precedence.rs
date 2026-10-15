//! Comparison-level predicates (`IN`, `LIKE`, `RLIKE`, `IS [NOT] NULL`, `:`
//! and the comparison operators) are never the unparenthesised operand of an
//! arithmetic operator, a cast or another comparison. Expected trees are
//! built by hand and expected errors carry their exact span, line and column.

#![allow(clippy::expect_used, clippy::panic)]

use ravel_esql::ast::{
    Binary, BinaryOp, Command, Expr, InList, IsNull, Like, LikeKind, Literal, LiteralValue,
    NamePart, QualifiedName, Unary, UnaryOp,
};
use ravel_esql::{Error, Limits, LineCol, Span, parse, print};

const NO_SPAN: Span = Span::new(0, 0);

const AFTER_COMPARISON: &str = "`AND`, `OR`, `|` or end of input (a comparison, `:`, `IN`, \
     `LIKE`, `RLIKE` or `IS NULL` does not chain and is not an operand of arithmetic or `::` \
     unless parenthesised)";

/// The same refusal inside a `ROW` or `EVAL` field, where `,` may follow.
const AFTER_COMPARISON_IN_FIELD: &str = "`AND`, `OR`, `,`, `|` or end of input (a comparison, \
     `:`, `IN`, `LIKE`, `RLIKE` or `IS NULL` does not chain and is not an operand of arithmetic \
     or `::` unless parenthesised)";

fn col(text: &str) -> Expr {
    Expr::Field(QualifiedName {
        parts: vec![NamePart {
            text: text.to_owned(),
            quoted: false,
            span: NO_SPAN,
            param: None,
        }],
        span: NO_SPAN,
    })
}

fn lit(value: LiteralValue) -> Expr {
    Expr::Literal(Literal {
        value,
        span: NO_SPAN,
    })
}

fn int(text: &str) -> Expr {
    lit(LiteralValue::Integer(text.to_owned()))
}

fn string(text: &str) -> Expr {
    lit(LiteralValue::String(text.to_owned()))
}

fn bin(op: BinaryOp, left: Expr, right: Expr) -> Expr {
    Expr::Binary(Binary {
        op,
        left: Box::new(left),
        right: Box::new(right),
        span: NO_SPAN,
    })
}

fn not(operand: Expr) -> Expr {
    Expr::Unary(Unary {
        op: UnaryOp::Not,
        operand: Box::new(operand),
        span: NO_SPAN,
    })
}

fn like(expr: Expr, kind: LikeKind, pattern: &str) -> Expr {
    Expr::Like(Like {
        expr: Box::new(expr),
        kind,
        pattern: pattern.to_owned(),
        negated: false,
        span: NO_SPAN,
    })
}

fn in_list(expr: Expr, list: Vec<Expr>) -> Expr {
    Expr::In(InList {
        expr: Box::new(expr),
        list,
        negated: false,
        span: NO_SPAN,
    })
}

fn is_null(expr: Expr, negated: bool) -> Expr {
    Expr::IsNull(IsNull {
        expr: Box::new(expr),
        negated,
        span: NO_SPAN,
    })
}

/// The expression of `ROW x = ...` or the condition of `... | WHERE ...`.
fn only_expr(source: &str) -> Expr {
    let query = parse(source, &Limits::default())
        .unwrap_or_else(|error| panic!("{source:?} refused: {error}"));
    match (&query.source, query.processing.as_slice()) {
        (Command::Row(row), []) => row.fields[0].expr.clone(),
        (_, [Command::Where(w)]) => w.condition.clone(),
        other => panic!("unexpected shape {other:?}"),
    }
}

fn assert_expr(source: &str, expected: &Expr) {
    let actual = only_expr(source);
    assert!(
        actual.eq_ignoring_spans(expected),
        "{source}\nparsed:   {actual:#?}\nexpected: {expected:#?}"
    );
}

fn assert_refused(source: &str, found: &str, start: usize, column: usize) {
    let error = match parse(source, &Limits::default()) {
        Ok(query) => panic!("{source:?} parsed: {query:#?}"),
        Err(error) => error,
    };
    let expected = Error::UnexpectedToken {
        found: format!("`{found}`"),
        expected: if source.starts_with("ROW ") {
            AFTER_COMPARISON_IN_FIELD
        } else {
            AFTER_COMPARISON
        },
        span: Span::new(start, start + found.len()),
    };
    assert_eq!(error, expected, "{source:?}");
    assert_eq!(
        error.line_col(source),
        LineCol { line: 1, column },
        "{source:?}"
    );
}

#[test]
fn reported_inputs_are_refused_at_the_operator() {
    assert_refused("ROW x = a LIKE \"p\" + 1", "+", 19, 20);
    assert_refused("ROW x = a IN (1) * 2", "*", 17, 18);
    assert_refused("FROM t | WHERE f : \"x\" + 1", "+", 23, 24);
    assert_refused("FROM t | WHERE NOT a IS NULL + 1", "+", 29, 30);
    assert_refused("FROM t | WHERE a IS NULL + 1 > 0", "+", 25, 26);
    assert_refused("FROM t | WHERE a LIKE \"x\" - 1 LIKE \"y\"", "-", 26, 27);
}

#[test]
fn reported_inputs_parse_when_parenthesised() {
    assert_expr(
        "ROW x = (a LIKE \"p\") + 1",
        &bin(BinaryOp::Add, like(col("a"), LikeKind::Like, "p"), int("1")),
    );
    assert_expr(
        "ROW x = (a IN (1)) * 2",
        &bin(BinaryOp::Mul, in_list(col("a"), vec![int("1")]), int("2")),
    );
    assert_expr(
        "FROM t | WHERE (f : \"x\") + 1",
        &bin(
            BinaryOp::Add,
            bin(BinaryOp::Match, col("f"), string("x")),
            int("1"),
        ),
    );
    assert_expr(
        "FROM t | WHERE NOT (a IS NULL) + 1",
        &not(bin(BinaryOp::Add, is_null(col("a"), false), int("1"))),
    );
    assert_expr(
        "FROM t | WHERE (a IS NULL) + 1 > 0",
        &bin(
            BinaryOp::Gt,
            bin(BinaryOp::Add, is_null(col("a"), false), int("1")),
            int("0"),
        ),
    );
    assert_expr(
        "FROM t | WHERE (a LIKE \"x\") - 1 LIKE \"y\"",
        &like(
            bin(BinaryOp::Sub, like(col("a"), LikeKind::Like, "x"), int("1")),
            LikeKind::Like,
            "y",
        ),
    );
}

#[test]
fn arithmetic_before_a_predicate_binds_into_its_tested_value() {
    assert_expr(
        "ROW x = a + 1 LIKE \"p\"",
        &like(bin(BinaryOp::Add, col("a"), int("1")), LikeKind::Like, "p"),
    );
    assert_expr(
        "ROW x = a * 2 RLIKE \"p\"",
        &like(bin(BinaryOp::Mul, col("a"), int("2")), LikeKind::RLike, "p"),
    );
    assert_expr(
        "ROW x = a * 2 IN (1, 2 + 3)",
        &in_list(
            bin(BinaryOp::Mul, col("a"), int("2")),
            vec![int("1"), bin(BinaryOp::Add, int("2"), int("3"))],
        ),
    );
    assert_expr(
        "ROW x = a - 1 IS NOT NULL",
        &is_null(bin(BinaryOp::Sub, col("a"), int("1")), true),
    );
    assert_expr(
        "ROW x = a IS NULL AND b LIKE \"x\" OR c IN (1)",
        &bin(
            BinaryOp::Or,
            bin(
                BinaryOp::And,
                is_null(col("a"), false),
                like(col("b"), LikeKind::Like, "x"),
            ),
            in_list(col("c"), vec![int("1")]),
        ),
    );
}

/// Every predicate followed by every operator that binds at least as tightly
/// as a comparison is refused at that operator's first token.
#[test]
fn every_predicate_refuses_a_following_operator() {
    const PREFIX: &str = "FROM t | WHERE ";
    let predicates = [
        "a IN (1, 2)",
        "a NOT IN (1)",
        "a LIKE \"p*\"",
        "a NOT LIKE \"p\"",
        "a RLIKE \"p.*\"",
        "a NOT RLIKE \"p\"",
        "a IS NULL",
        "a IS NOT NULL",
        "a : \"x\"",
    ];
    let followers = [
        ("+", " 2"),
        ("-", " 2"),
        ("*", " 2"),
        ("/", " 2"),
        ("%", " 2"),
        ("::", "long"),
        ("==", " 2"),
        ("!=", " 2"),
        ("<", " 2"),
        (">=", " 2"),
        ("IN", " (1)"),
        ("LIKE", " \"y\""),
        ("RLIKE", " \"y\""),
        ("IS", " NULL"),
        (":", " \"y\""),
    ];
    for predicate in predicates {
        for (operator, rest) in followers {
            let source = format!("{PREFIX}{predicate} {operator}{rest}");
            let start = PREFIX.len() + predicate.len() + 1;
            assert_refused(&source, operator, start, start + 1);
        }
    }
    // A comparison operator's right side is a parsed operand, so arithmetic
    // and casts after it bind there; only a second comparison is refused.
    for comparison in ["a == 1", "a < 1"] {
        for (operator, rest) in &followers[6..] {
            let source = format!("{PREFIX}{comparison} {operator}{rest}");
            let start = PREFIX.len() + comparison.len() + 1;
            assert_refused(&source, operator, start, start + 1);
        }
    }
}

#[test]
fn arithmetic_after_a_comparison_operator_binds_into_its_right_operand() {
    assert_expr(
        "ROW x = a == 1 + 2",
        &bin(
            BinaryOp::Eq,
            col("a"),
            bin(BinaryOp::Add, int("1"), int("2")),
        ),
    );
    assert_expr(
        "ROW x = a < 1 * 2",
        &bin(
            BinaryOp::Lt,
            col("a"),
            bin(BinaryOp::Mul, int("1"), int("2")),
        ),
    );
}

#[test]
fn a_parenthesised_predicate_prints_with_its_parentheses_only() {
    for source in [
        "ROW x = (a LIKE \"p\") + 1",
        "ROW x = (a IN (1)) * 2",
        "ROW x = (a : \"x\") == true",
        "ROW x = NOT (a IS NULL) + 1",
        "ROW x = (a IS NOT NULL)::long",
        "ROW x = a + 1 LIKE \"p\"",
    ] {
        let query = parse(source, &Limits::default()).expect("parses");
        assert_eq!(print(&query, &Limits::default()).expect("prints"), source);
    }
}
