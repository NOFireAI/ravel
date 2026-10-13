//! Structure and spans of parsed pipelines, one case per command, and
//! operator precedence.

#![allow(clippy::expect_used, clippy::panic)]

use ravel_esql::ast::{
    Aggregate, ArrayLiteral, Binary, BinaryOp, Call, CallArgs, Cast, Command, DropCommand,
    EvalCommand, Expr, Field, FromCommand, InList, Interval, IntervalKind, IsNull, KeepCommand,
    Like, LikeKind, LimitCommand, Literal, LiteralValue, Metadata, NamePart, NullsOrder, ParamName,
    Parameter, QualifiedName, Query, RenameCommand, RenamePair, RowCommand, SortCommand, SortKey,
    SortOrder, SourcePattern, StatsCommand, Unary, UnaryOp, WhereCommand,
};
use ravel_esql::{Error, Limits, Span, parse, print};

const NO_SPAN: Span = Span::new(0, 0);

fn parsed(source: &str) -> Query {
    parse(source, &Limits::default()).expect("query parses")
}

fn slice(source: &str, span: Span) -> &str {
    span.slice(source)
        .expect("span falls on character boundaries")
}

fn name(dotted: &str) -> QualifiedName {
    QualifiedName {
        parts: dotted
            .split('.')
            .map(|part| NamePart {
                text: part.to_owned(),
                quoted: false,
                span: NO_SPAN,
            })
            .collect(),
        span: NO_SPAN,
    }
}

fn quoted(text: &str) -> NamePart {
    NamePart {
        text: text.to_owned(),
        quoted: true,
        span: NO_SPAN,
    }
}

fn col(dotted: &str) -> Expr {
    Expr::Field(name(dotted))
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

fn interval(value: &str, unit: &str) -> Expr {
    Expr::Interval(Interval {
        value: value.to_owned(),
        unit: unit.to_owned(),
        span: NO_SPAN,
    })
}

fn call(function: &str, args: Vec<Expr>) -> Expr {
    Expr::Call(Call {
        name: function.to_owned(),
        args: CallArgs::List(args),
        span: NO_SPAN,
    })
}

fn count_star() -> Expr {
    Expr::Call(Call {
        name: "COUNT".to_owned(),
        args: CallArgs::Star,
        span: NO_SPAN,
    })
}

fn bin(op: BinaryOp, left: Expr, right: Expr) -> Expr {
    Expr::Binary(Binary {
        op,
        left: Box::new(left),
        right: Box::new(right),
        span: NO_SPAN,
    })
}

fn unary(op: UnaryOp, operand: Expr) -> Expr {
    Expr::Unary(Unary {
        op,
        operand: Box::new(operand),
        span: NO_SPAN,
    })
}

fn field(assigned: Option<&str>, expr: Expr) -> Field {
    Field {
        name: assigned.map(name),
        expr,
        span: NO_SPAN,
    }
}

fn aggregate(assigned: Option<&str>, expr: Expr, filter: Option<Expr>) -> Aggregate {
    Aggregate {
        field: field(assigned, expr),
        filter,
        span: NO_SPAN,
    }
}

fn sort_key(expr: Expr, order: Option<SortOrder>, nulls: Option<NullsOrder>) -> SortKey {
    SortKey {
        expr,
        order,
        nulls,
        span: NO_SPAN,
    }
}

fn from(sources: &[&str], metadata: &[&str]) -> Command {
    Command::From(FromCommand {
        sources: sources
            .iter()
            .map(|s| SourcePattern {
                text: (*s).to_owned(),
                quoted: false,
                span: NO_SPAN,
            })
            .collect(),
        metadata: (!metadata.is_empty()).then(|| Metadata {
            fields: metadata.iter().map(|m| name(m)).collect(),
            span: NO_SPAN,
        }),
        span: NO_SPAN,
    })
}

fn query(source: Command, processing: Vec<Command>) -> Query {
    Query {
        source,
        processing,
        span: NO_SPAN,
    }
}

/// Asserts that `source` parses to `expected` ignoring spans, and that the
/// printed tree parses back to the same tree.
fn assert_tree(source: &str, expected: &Query) -> Query {
    let actual = parsed(source);
    assert!(
        actual.eq_ignoring_spans(expected),
        "{source}\nparsed:   {actual:#?}\nexpected: {expected:#?}"
    );
    let printed = print(&actual, &Limits::default()).expect("prints");
    let reparsed = parsed(&printed);
    assert!(
        reparsed.eq_ignoring_spans(&actual),
        "{source}\nprinted as {printed}"
    );
    actual
}

fn where_expr(source: &str) -> Expr {
    let query = parsed(source);
    match query.processing.first() {
        Some(Command::Where(w)) => w.condition.clone(),
        other => panic!("expected a WHERE, got {other:?}"),
    }
}

fn assert_where(condition: &str, expected: &Expr) {
    let source = format!("ROW a = 1 | WHERE {condition}");
    let actual = where_expr(&source);
    assert!(
        actual.eq_ignoring_spans(expected),
        "{condition}\nparsed:   {actual:#?}\nexpected: {expected:#?}"
    );
}

#[test]
fn parses_both_adr_acceptance_pipelines_with_spans() {
    let first = "FROM logs-prod-* METADATA _index
| WHERE @timestamp >= NOW() - 1 hour
| STATS errors = COUNT(*) WHERE log.level == \"ERROR\"
  BY window = BUCKET(@timestamp, 5 minutes), service.name
| SORT window ASC, service.name ASC";

    let expected = query(
        from(&["logs-prod-*"], &["_index"]),
        vec![
            Command::Where(WhereCommand {
                condition: bin(
                    BinaryOp::GtEq,
                    col("@timestamp"),
                    bin(BinaryOp::Sub, call("NOW", vec![]), interval("1", "hour")),
                ),
                span: NO_SPAN,
            }),
            Command::Stats(StatsCommand {
                aggregates: vec![aggregate(
                    Some("errors"),
                    count_star(),
                    Some(bin(BinaryOp::Eq, col("log.level"), string("ERROR"))),
                )],
                groupings: vec![
                    field(
                        Some("window"),
                        call("BUCKET", vec![col("@timestamp"), interval("5", "minutes")]),
                    ),
                    field(None, col("service.name")),
                ],
                span: NO_SPAN,
            }),
            Command::Sort(SortCommand {
                keys: vec![
                    sort_key(col("window"), Some(SortOrder::Asc), None),
                    sort_key(col("service.name"), Some(SortOrder::Asc), None),
                ],
                span: NO_SPAN,
            }),
        ],
    );
    let q = assert_tree(first, &expected);

    assert_eq!(slice(first, q.span), first);
    let Command::From(from_cmd) = &q.source else {
        panic!("expected FROM");
    };
    assert_eq!(
        slice(first, from_cmd.span),
        "FROM logs-prod-* METADATA _index"
    );
    assert_eq!(slice(first, from_cmd.sources[0].span), "logs-prod-*");
    let metadata = from_cmd.metadata.as_ref().expect("metadata");
    assert_eq!(slice(first, metadata.span), "METADATA _index");
    assert_eq!(slice(first, metadata.fields[0].span), "_index");

    let Command::Where(where_cmd) = &q.processing[0] else {
        panic!("expected WHERE");
    };
    assert_eq!(
        slice(first, where_cmd.span),
        "WHERE @timestamp >= NOW() - 1 hour"
    );
    assert_eq!(
        slice(first, where_cmd.condition.span()),
        "@timestamp >= NOW() - 1 hour"
    );
    let Expr::Binary(ge) = &where_cmd.condition else {
        panic!("expected >=");
    };
    assert_eq!(slice(first, ge.left.span()), "@timestamp");
    assert_eq!(slice(first, ge.right.span()), "NOW() - 1 hour");
    let Expr::Binary(sub) = ge.right.as_ref() else {
        panic!("expected -");
    };
    assert_eq!(slice(first, sub.left.span()), "NOW()");
    assert_eq!(slice(first, sub.right.span()), "1 hour");
    let Expr::Interval(hour) = sub.right.as_ref() else {
        panic!("expected an interval");
    };
    assert_eq!(hour.kind(), Some(IntervalKind::TimeDuration));

    let Command::Stats(stats) = &q.processing[1] else {
        panic!("expected STATS");
    };
    assert_eq!(
        slice(first, stats.span),
        "STATS errors = COUNT(*) WHERE log.level == \"ERROR\"
  BY window = BUCKET(@timestamp, 5 minutes), service.name"
    );
    let agg = &stats.aggregates[0];
    assert_eq!(
        slice(first, agg.span),
        "errors = COUNT(*) WHERE log.level == \"ERROR\""
    );
    assert_eq!(slice(first, agg.field.span), "errors = COUNT(*)");
    assert_eq!(slice(first, agg.field.expr.span()), "COUNT(*)");
    let filter = agg.filter.as_ref().expect("filter");
    assert_eq!(slice(first, filter.span()), "log.level == \"ERROR\"");
    let Expr::Binary(eq) = filter else {
        panic!("expected ==");
    };
    let Expr::Field(level) = eq.left.as_ref() else {
        panic!("expected a field");
    };
    assert_eq!(slice(first, level.span), "log.level");
    assert_eq!(slice(first, level.parts[0].span), "log");
    assert_eq!(slice(first, level.parts[1].span), "level");
    assert_eq!(slice(first, eq.right.span()), "\"ERROR\"");
    assert_eq!(
        slice(first, stats.groupings[0].span),
        "window = BUCKET(@timestamp, 5 minutes)"
    );
    let Expr::Call(bucket) = &stats.groupings[0].expr else {
        panic!("expected BUCKET");
    };
    let CallArgs::List(args) = &bucket.args else {
        panic!("expected arguments");
    };
    assert_eq!(slice(first, args[0].span()), "@timestamp");
    assert_eq!(slice(first, args[1].span()), "5 minutes");
    assert_eq!(slice(first, stats.groupings[1].span), "service.name");

    let Command::Sort(sort) = &q.processing[2] else {
        panic!("expected SORT");
    };
    assert_eq!(slice(first, sort.span), "SORT window ASC, service.name ASC");
    assert_eq!(slice(first, sort.keys[0].span), "window ASC");
    assert_eq!(slice(first, sort.keys[1].span), "service.name ASC");

    let second = "FROM logs-prod-*
| SORT @timestamp DESC
| LIMIT 100
| STATS recent_errors = COUNT(*) WHERE log.level == \"ERROR\"";

    let expected = query(
        from(&["logs-prod-*"], &[]),
        vec![
            Command::Sort(SortCommand {
                keys: vec![sort_key(col("@timestamp"), Some(SortOrder::Desc), None)],
                span: NO_SPAN,
            }),
            Command::Limit(LimitCommand {
                count: 100,
                span: NO_SPAN,
            }),
            Command::Stats(StatsCommand {
                aggregates: vec![aggregate(
                    Some("recent_errors"),
                    count_star(),
                    Some(bin(BinaryOp::Eq, col("log.level"), string("ERROR"))),
                )],
                groupings: vec![],
                span: NO_SPAN,
            }),
        ],
    );
    let q = assert_tree(second, &expected);
    assert_eq!(slice(second, q.span), second);
    assert_eq!(slice(second, q.source.span()), "FROM logs-prod-*");
    let spans: Vec<&str> = q
        .processing
        .iter()
        .map(|c| slice(second, c.span()))
        .collect();
    assert_eq!(
        spans,
        [
            "SORT @timestamp DESC",
            "LIMIT 100",
            "STATS recent_errors = COUNT(*) WHERE log.level == \"ERROR\"",
        ]
    );
    let Command::Stats(stats) = &q.processing[2] else {
        panic!("expected STATS");
    };
    assert_eq!(
        slice(
            second,
            stats.aggregates[0].filter.as_ref().expect("filter").span()
        ),
        "log.level == \"ERROR\""
    );
}

#[test]
fn per_aggregate_where_attaches_to_its_own_aggregate() {
    let source =
        "FROM t | STATS a = COUNT(*) WHERE x == 1, b = SUM(y), c = MAX(y) WHERE x == 2 BY g";
    let expected = query(
        from(&["t"], &[]),
        vec![Command::Stats(StatsCommand {
            aggregates: vec![
                aggregate(
                    Some("a"),
                    count_star(),
                    Some(bin(BinaryOp::Eq, col("x"), int("1"))),
                ),
                aggregate(Some("b"), call("SUM", vec![col("y")]), None),
                aggregate(
                    Some("c"),
                    call("MAX", vec![col("y")]),
                    Some(bin(BinaryOp::Eq, col("x"), int("2"))),
                ),
            ],
            groupings: vec![field(None, col("g"))],
            span: NO_SPAN,
        })],
    );
    let q = assert_tree(source, &expected);
    let Command::Stats(stats) = &q.processing[0] else {
        panic!("expected STATS");
    };
    let filters: Vec<Option<&str>> = stats
        .aggregates
        .iter()
        .map(|a| a.filter.as_ref().map(|f| slice(source, f.span())))
        .collect();
    assert_eq!(filters, [Some("x == 1"), None, Some("x == 2")]);
}

#[test]
fn stats_with_only_by_and_with_only_aggregates() {
    assert_tree(
        "FROM t | STATS BY host",
        &query(
            from(&["t"], &[]),
            vec![Command::Stats(StatsCommand {
                aggregates: vec![],
                groupings: vec![field(None, col("host"))],
                span: NO_SPAN,
            })],
        ),
    );
    assert_tree(
        "FROM t | STATS AVG(x), n = COUNT(x)",
        &query(
            from(&["t"], &[]),
            vec![Command::Stats(StatsCommand {
                aggregates: vec![
                    aggregate(None, call("AVG", vec![col("x")]), None),
                    aggregate(Some("n"), call("COUNT", vec![col("x")]), None),
                ],
                groupings: vec![],
                span: NO_SPAN,
            })],
        ),
    );
}

#[test]
fn row_command_with_literals() {
    let source =
        "ROW a = 1, b = 2.5, c = \"x\\ty\", d = null, e = TRUE, f = false, g = [1, -2, 3.0], 1e3";
    let q = assert_tree(
        source,
        &query(
            Command::Row(RowCommand {
                fields: vec![
                    field(Some("a"), int("1")),
                    field(Some("b"), lit(LiteralValue::Decimal("2.5".into()))),
                    field(Some("c"), string("x\ty")),
                    field(Some("d"), lit(LiteralValue::Null)),
                    field(Some("e"), lit(LiteralValue::Boolean(true))),
                    field(Some("f"), lit(LiteralValue::Boolean(false))),
                    field(
                        Some("g"),
                        Expr::Array(ArrayLiteral {
                            elements: vec![
                                int("1"),
                                unary(UnaryOp::Minus, int("2")),
                                lit(LiteralValue::Decimal("3.0".into())),
                            ],
                            span: NO_SPAN,
                        }),
                    ),
                    field(None, lit(LiteralValue::Decimal("1e3".into()))),
                ],
                span: NO_SPAN,
            }),
            vec![],
        ),
    );
    assert_eq!(slice(source, q.span), source);
    let Command::Row(row) = &q.source else {
        panic!("expected ROW");
    };
    assert_eq!(slice(source, row.fields[2].expr.span()), "\"x\\ty\"");
    assert_eq!(slice(source, row.fields[6].expr.span()), "[1, -2, 3.0]");
}

#[test]
fn from_command_forms() {
    let source = "from logs-*, \"other index\", remote:metrics-2026.10 metadata _index, _id";
    let q = assert_tree(
        source,
        &query(
            Command::From(FromCommand {
                sources: vec![
                    SourcePattern {
                        text: "logs-*".into(),
                        quoted: false,
                        span: NO_SPAN,
                    },
                    SourcePattern {
                        text: "other index".into(),
                        quoted: true,
                        span: NO_SPAN,
                    },
                    SourcePattern {
                        text: "remote:metrics-2026.10".into(),
                        quoted: false,
                        span: NO_SPAN,
                    },
                ],
                metadata: Some(Metadata {
                    fields: vec![name("_index"), name("_id")],
                    span: NO_SPAN,
                }),
                span: NO_SPAN,
            }),
            vec![],
        ),
    );
    let Command::From(f) = &q.source else {
        panic!("expected FROM");
    };
    assert_eq!(slice(source, f.sources[1].span), "\"other index\"");
    assert_eq!(
        slice(source, f.metadata.as_ref().expect("metadata").span),
        "metadata _index, _id"
    );
}

#[test]
fn a_source_named_metadata_is_a_source() {
    assert_tree("FROM metadata", &query(from(&["metadata"], &[]), vec![]));
    assert_tree(
        "FROM metadata METADATA _index",
        &query(from(&["metadata"], &["_index"]), vec![]),
    );
    assert_tree(
        "FROM a, METADATA metadata _id",
        &query(from(&["a", "METADATA"], &["_id"]), vec![]),
    );
    let quoted = query(
        Command::From(FromCommand {
            sources: vec![SourcePattern {
                text: "metadata".into(),
                quoted: true,
                span: NO_SPAN,
            }],
            metadata: None,
            span: NO_SPAN,
        }),
        vec![],
    );
    assert_tree("FROM \"metadata\"", &quoted);
    // After a source name, the keyword still starts the metadata list.
    assert!(matches!(
        parse("FROM metadata METADATA", &Limits::default()),
        Err(Error::UnexpectedToken { .. })
    ));
}

#[test]
fn eval_command_assigns_and_keeps_spelling() {
    let source = "FROM t | eval `odd name` = Length(msg), y = x::LONG, `a``b`.c";
    let mut odd = name("odd name");
    odd.parts = vec![quoted("odd name")];
    let mut ab = name("c");
    ab.parts.insert(0, quoted("a`b"));
    let q = assert_tree(
        source,
        &query(
            from(&["t"], &[]),
            vec![Command::Eval(EvalCommand {
                fields: vec![
                    Field {
                        name: Some(odd),
                        expr: call("Length", vec![col("msg")]),
                        span: NO_SPAN,
                    },
                    field(
                        Some("y"),
                        Expr::Cast(Cast {
                            expr: Box::new(col("x")),
                            type_name: "LONG".into(),
                            span: NO_SPAN,
                        }),
                    ),
                    field(None, Expr::Field(ab)),
                ],
                span: NO_SPAN,
            })],
        ),
    );
    let Command::Eval(eval) = &q.processing[0] else {
        panic!("expected EVAL");
    };
    assert_eq!(
        slice(source, eval.span),
        "eval `odd name` = Length(msg), y = x::LONG, `a``b`.c"
    );
    assert_eq!(slice(source, eval.fields[1].expr.span()), "x::LONG");
    let Expr::Field(qualified) = &eval.fields[2].expr else {
        panic!("expected a field");
    };
    assert_eq!(slice(source, qualified.parts[0].span), "`a``b`");
}

#[test]
fn keep_and_drop_accept_wildcards() {
    let q = assert_tree(
        "FROM t | KEEP @timestamp, service.*, *_count, `x` | DROP tmp*, a.b",
        &query(
            from(&["t"], &[]),
            vec![
                Command::Keep(KeepCommand {
                    patterns: vec![
                        name("@timestamp"),
                        name("service.*"),
                        name("*_count"),
                        QualifiedName {
                            parts: vec![quoted("x")],
                            span: NO_SPAN,
                        },
                    ],
                    span: NO_SPAN,
                }),
                Command::Drop(DropCommand {
                    patterns: vec![name("tmp*"), name("a.b")],
                    span: NO_SPAN,
                }),
            ],
        ),
    );
    assert_eq!(q.processing.len(), 2);
}

#[test]
fn rename_pairs() {
    let source = "FROM t | RENAME a AS b, x.y AS `z z`";
    let mut zz = name("z");
    zz.parts = vec![quoted("z z")];
    let q = assert_tree(
        source,
        &query(
            from(&["t"], &[]),
            vec![Command::Rename(RenameCommand {
                renames: vec![
                    RenamePair {
                        old: name("a"),
                        new: name("b"),
                        span: NO_SPAN,
                    },
                    RenamePair {
                        old: name("x.y"),
                        new: zz,
                        span: NO_SPAN,
                    },
                ],
                span: NO_SPAN,
            })],
        ),
    );
    let Command::Rename(r) = &q.processing[0] else {
        panic!("expected RENAME");
    };
    assert_eq!(slice(source, r.renames[1].span), "x.y AS `z z`");
}

#[test]
fn sort_orders_and_nulls() {
    assert_tree(
        "FROM t | SORT a, b DESC NULLS FIRST, c ASC NULLS LAST, d nulls first, e + 1",
        &query(
            from(&["t"], &[]),
            vec![Command::Sort(SortCommand {
                keys: vec![
                    sort_key(col("a"), None, None),
                    sort_key(col("b"), Some(SortOrder::Desc), Some(NullsOrder::First)),
                    sort_key(col("c"), Some(SortOrder::Asc), Some(NullsOrder::Last)),
                    sort_key(col("d"), None, Some(NullsOrder::First)),
                    sort_key(bin(BinaryOp::Add, col("e"), int("1")), None, None),
                ],
                span: NO_SPAN,
            })],
        ),
    );
}

#[test]
fn limit_and_where() {
    assert_tree(
        "FROM t | WHERE ok | LIMIT 18446744073709551615",
        &query(
            from(&["t"], &[]),
            vec![
                Command::Where(WhereCommand {
                    condition: col("ok"),
                    span: NO_SPAN,
                }),
                Command::Limit(LimitCommand {
                    count: u64::MAX,
                    span: NO_SPAN,
                }),
            ],
        ),
    );
}

#[test]
fn predicates_in_like_is_null_match_and_params() {
    assert_where(
        "a NOT IN (1, 2) AND b like \"x*\" OR c not rlike \"y.+\" AND d IS NOT NULL AND e IS NULL",
        &bin(
            BinaryOp::Or,
            bin(
                BinaryOp::And,
                Expr::In(InList {
                    expr: Box::new(col("a")),
                    list: vec![int("1"), int("2")],
                    negated: true,
                    span: NO_SPAN,
                }),
                Expr::Like(Like {
                    expr: Box::new(col("b")),
                    kind: LikeKind::Like,
                    pattern: "x*".into(),
                    negated: false,
                    span: NO_SPAN,
                }),
            ),
            bin(
                BinaryOp::And,
                bin(
                    BinaryOp::And,
                    Expr::Like(Like {
                        expr: Box::new(col("c")),
                        kind: LikeKind::RLike,
                        pattern: "y.+".into(),
                        negated: true,
                        span: NO_SPAN,
                    }),
                    Expr::IsNull(IsNull {
                        expr: Box::new(col("d")),
                        negated: true,
                        span: NO_SPAN,
                    }),
                ),
                Expr::IsNull(IsNull {
                    expr: Box::new(col("e")),
                    negated: false,
                    span: NO_SPAN,
                }),
            ),
        ),
    );
    assert_where(
        "message : \"timeout\" AND ?x == ?1 + ? AND f(??name)",
        &bin(
            BinaryOp::And,
            bin(
                BinaryOp::And,
                bin(BinaryOp::Match, col("message"), string("timeout")),
                bin(
                    BinaryOp::Eq,
                    Expr::Param(Parameter {
                        name: ParamName::Named("x".into()),
                        identifier: false,
                        span: NO_SPAN,
                    }),
                    bin(
                        BinaryOp::Add,
                        Expr::Param(Parameter {
                            name: ParamName::Numbered("1".into()),
                            identifier: false,
                            span: NO_SPAN,
                        }),
                        Expr::Param(Parameter {
                            name: ParamName::Positional,
                            identifier: false,
                            span: NO_SPAN,
                        }),
                    ),
                ),
            ),
            call(
                "f",
                vec![Expr::Param(Parameter {
                    name: ParamName::Named("name".into()),
                    identifier: true,
                    span: NO_SPAN,
                })],
            ),
        ),
    );
}

#[test]
fn intervals_have_kinds() {
    let expr = where_expr("ROW a = 1 | WHERE f(1 day, 2 WEEKS, 30 s, 3 mo)");
    let Expr::Call(c) = expr else {
        panic!("expected a call");
    };
    let CallArgs::List(args) = c.args else {
        panic!("expected arguments");
    };
    let kinds: Vec<Option<IntervalKind>> = args
        .iter()
        .map(|a| match a {
            Expr::Interval(i) => i.kind(),
            other => panic!("expected an interval, got {other:?}"),
        })
        .collect();
    assert_eq!(
        kinds,
        [
            Some(IntervalKind::DatePeriod),
            Some(IntervalKind::DatePeriod),
            Some(IntervalKind::TimeDuration),
            Some(IntervalKind::DatePeriod),
        ]
    );
}

#[test]
fn and_binds_tighter_than_or() {
    assert_where(
        "a OR b AND c",
        &bin(
            BinaryOp::Or,
            col("a"),
            bin(BinaryOp::And, col("b"), col("c")),
        ),
    );
    assert_where(
        "a AND b OR c",
        &bin(
            BinaryOp::Or,
            bin(BinaryOp::And, col("a"), col("b")),
            col("c"),
        ),
    );
}

#[test]
fn multiplication_binds_tighter_than_addition() {
    assert_where(
        "a + b * c",
        &bin(
            BinaryOp::Add,
            col("a"),
            bin(BinaryOp::Mul, col("b"), col("c")),
        ),
    );
    assert_where(
        "a * b + c % d",
        &bin(
            BinaryOp::Add,
            bin(BinaryOp::Mul, col("a"), col("b")),
            bin(BinaryOp::Mod, col("c"), col("d")),
        ),
    );
}

#[test]
fn not_binds_looser_than_comparison() {
    assert_where(
        "NOT a == b",
        &unary(UnaryOp::Not, bin(BinaryOp::Eq, col("a"), col("b"))),
    );
    assert_where(
        "NOT a AND b",
        &bin(BinaryOp::And, unary(UnaryOp::Not, col("a")), col("b")),
    );
}

#[test]
fn unary_minus_binds_tighter_than_multiplication_and_looser_than_cast() {
    assert_where(
        "-a * b",
        &bin(BinaryOp::Mul, unary(UnaryOp::Minus, col("a")), col("b")),
    );
    assert_where(
        "a - -b",
        &bin(BinaryOp::Sub, col("a"), unary(UnaryOp::Minus, col("b"))),
    );
    assert_where(
        "-a::long",
        &unary(
            UnaryOp::Minus,
            Expr::Cast(Cast {
                expr: Box::new(col("a")),
                type_name: "long".into(),
                span: NO_SPAN,
            }),
        ),
    );
}

#[test]
fn binary_operators_associate_left() {
    assert_where(
        "a - b - c",
        &bin(
            BinaryOp::Sub,
            bin(BinaryOp::Sub, col("a"), col("b")),
            col("c"),
        ),
    );
    assert_where(
        "a / b * c",
        &bin(
            BinaryOp::Mul,
            bin(BinaryOp::Div, col("a"), col("b")),
            col("c"),
        ),
    );
    assert_where(
        "a OR b OR c",
        &bin(
            BinaryOp::Or,
            bin(BinaryOp::Or, col("a"), col("b")),
            col("c"),
        ),
    );
}

#[test]
fn parentheses_override_precedence_and_widen_spans() {
    let source = "ROW a = 1 | WHERE (a OR b) AND c";
    let expr = where_expr(source);
    assert!(expr.eq_ignoring_spans(&bin(
        BinaryOp::And,
        bin(BinaryOp::Or, col("a"), col("b")),
        col("c"),
    )));
    let Expr::Binary(and) = &expr else {
        panic!("expected AND");
    };
    assert_eq!(slice(source, and.left.span()), "(a OR b)");
}

#[test]
fn comments_and_case_insensitive_keywords() {
    let source = "from t // trailing\n| /* outer /* nested */ still */ where A and not B";
    let q = parsed(source);
    let Command::Where(w) = &q.processing[0] else {
        panic!("expected WHERE");
    };
    assert!(w.condition.eq_ignoring_spans(&bin(
        BinaryOp::And,
        col("A"),
        unary(UnaryOp::Not, col("B"))
    )));
    assert_eq!(slice(source, w.span), "where A and not B");
}

#[test]
fn triple_quoted_string() {
    let expr = where_expr("ROW a = 1 | WHERE a == \"\"\"say \"hi\"\n\\n\"\"\"");
    assert!(expr.eq_ignoring_spans(&bin(BinaryOp::Eq, col("a"), string("say \"hi\"\n\\n"),)));
    // Quotes in the closing run beyond the last three belong to the body.
    let expr = where_expr("ROW a = 1 | WHERE a == \"\"\"x\"\"\"\"\"");
    assert!(expr.eq_ignoring_spans(&bin(BinaryOp::Eq, col("a"), string("x\"\""))));
}

#[test]
fn printer_emits_canonical_text() {
    let q = parsed(
        "from t metadata _id | where not (a or b) and -(x + 1)::double > 2 | stats c = count(*) where a by h | sort c desc nulls last | limit 5",
    );
    assert_eq!(
        print(&q, &Limits::default()).expect("prints"),
        "FROM t METADATA _id | WHERE NOT (a OR b) AND -(x + 1)::double > 2 | STATS c = count(*) WHERE a BY h | SORT c DESC NULLS LAST | LIMIT 5"
    );
}
