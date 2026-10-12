//! Property tests: any input yields a tree or a typed error with a span
//! inside the input; any tree the parser can produce prints to text that
//! parses back to the same tree when that text is within the limits; and no
//! comparison-level node is an operand of arithmetic, a cast or another
//! comparison unless the source parenthesised it.

#![allow(clippy::expect_used)]

use proptest::prelude::*;
use ravel_esql::ast::{
    Aggregate, ArrayLiteral, Binary, BinaryOp, Call, CallArgs, Cast, Command, DropCommand,
    EvalCommand, Expr, Field, FromCommand, InList, Interval, IsNull, KeepCommand, Like, LikeKind,
    LimitCommand, Literal, LiteralValue, Metadata, NamePart, NullsOrder, ParamName, Parameter,
    QualifiedName, Query, RenameCommand, RenamePair, RowCommand, SortCommand, SortKey, SortOrder,
    SourcePattern, StatsCommand, Unary, UnaryOp, WhereCommand,
};
use ravel_esql::lexer::tokenize;
use ravel_esql::{Error, LimitKind, Limits, Span, parse, print};

const NO_SPAN: Span = Span::new(0, 0);

const RESERVED: &[&str] = &[
    "and", "as", "asc", "by", "desc", "false", "in", "is", "like", "not", "null", "nulls", "or",
    "rlike", "true", "where",
];

const TYPES: &[&str] = &[
    "long", "double", "integer", "keyword", "text", "boolean", "datetime", "ip", "version",
];

const UNITS: &[&str] = &[
    "millisecond",
    "ms",
    "seconds",
    "s",
    "minute",
    "min",
    "hours",
    "h",
    "day",
    "d",
    "weeks",
    "month",
    "mo",
    "quarter",
    "q",
    "years",
    "yr",
    "y",
];

/// Statement fragments, valid and not, glued together with spaces.
const VOCABULARY: &[&str] = &[
    "FROM",
    "ROW",
    "WHERE",
    "EVAL",
    "KEEP",
    "DROP",
    "RENAME",
    "STATS",
    "SORT",
    "LIMIT",
    "BY",
    "AS",
    "METADATA",
    "ASC",
    "DESC",
    "NULLS",
    "FIRST",
    "LAST",
    "AND",
    "OR",
    "NOT",
    "IN",
    "LIKE",
    "RLIKE",
    "IS",
    "NULL",
    "true",
    "false",
    "GROK",
    "SHOW",
    "ENRICH",
    "|",
    ",",
    "(",
    ")",
    "[",
    "]",
    "=",
    "==",
    "!=",
    "<",
    "<=",
    ">",
    ">=",
    ":",
    "::",
    "+",
    "-",
    "*",
    "/",
    "%",
    ".",
    "?",
    "??",
    "?1",
    "?x",
    "a",
    "b.c",
    "@timestamp",
    "_id",
    "`q`",
    "`x``y`",
    "logs-*",
    "a*",
    "1",
    "2.5",
    ".5",
    "1e3",
    "18446744073709551616",
    "\"s\"",
    "\"\"\"t\"\"\"",
    "\"a\\n\"",
    "\"é\"",
    "count",
    "count(*)",
    "1 day",
    "long",
    "/* c */",
    "// c\n",
    "\n",
    "é",
    "ünï",
    "#",
    "$",
    "\"",
    "`",
    "/*",
];

fn is_reserved(word: &str) -> bool {
    RESERVED.iter().any(|r| r.eq_ignore_ascii_case(word))
}

fn word() -> impl Strategy<Value = String> {
    "[_@]?[a-z][a-z0-9_]{0,6}".prop_filter("reserved", |w| !is_reserved(w))
}

fn name_part() -> impl Strategy<Value = NamePart> {
    prop_oneof![
        3 => word().prop_map(|text| NamePart {
            text,
            quoted: false,
            span: NO_SPAN,
        }),
        1 => "[a-z `.é\n*|]{1,6}".prop_map(|text| NamePart {
            text,
            quoted: true,
            span: NO_SPAN,
        }),
    ]
}

fn name() -> impl Strategy<Value = QualifiedName> {
    prop::collection::vec(name_part(), 1..3).prop_map(|parts| QualifiedName {
        parts,
        span: NO_SPAN,
    })
}

fn pattern() -> impl Strategy<Value = QualifiedName> {
    let part = prop_oneof![
        2 => name_part(),
        1 => "[a-z*][a-z0-9_*]{0,5}"
            .prop_filter("reserved", |w| !is_reserved(w))
            .prop_map(|text| NamePart {
                text,
                quoted: false,
                span: NO_SPAN,
            }),
    ];
    prop::collection::vec(part, 1..3).prop_map(|parts| QualifiedName {
        parts,
        span: NO_SPAN,
    })
}

fn literal(value: LiteralValue) -> Expr {
    Expr::Literal(Literal {
        value,
        span: NO_SPAN,
    })
}

fn string_value() -> impl Strategy<Value = String> {
    "[a-zé \"\\\\\n\r\t`*|/]{0,8}"
}

fn number() -> impl Strategy<Value = LiteralValue> {
    prop_oneof![
        "[0-9]{1,24}".prop_map(LiteralValue::Integer),
        "[0-9]{1,3}\\.[0-9]{0,3}|\\.[0-9]{1,3}|[0-9]{1,2}[eE][+-]?[0-9]{1,2}"
            .prop_map(LiteralValue::Decimal),
    ]
}

fn signed(value: LiteralValue, sign: Option<UnaryOp>) -> Expr {
    let literal = literal(value);
    match sign {
        Some(op) => Expr::Unary(Unary {
            op,
            operand: Box::new(literal),
            span: NO_SPAN,
        }),
        None => literal,
    }
}

fn signed_number() -> impl Strategy<Value = Expr> {
    (
        number(),
        prop::option::of(prop_oneof![Just(UnaryOp::Minus), Just(UnaryOp::Plus)]),
    )
        .prop_map(|(value, sign)| signed(value, sign))
}

fn param(identifier: bool) -> impl Strategy<Value = Expr> {
    prop_oneof![
        Just(ParamName::Positional),
        "[0-9]{1,3}".prop_map(ParamName::Numbered),
        "[a-z_][a-z0-9_]{0,5}".prop_map(ParamName::Named),
    ]
    .prop_map(move |name| {
        Expr::Param(Parameter {
            name,
            identifier,
            span: NO_SPAN,
        })
    })
}

/// What may follow `:`: a signed number, a string, a boolean, `null` or a
/// value parameter.
fn scalar_constant() -> impl Strategy<Value = Expr> {
    prop_oneof![
        signed_number(),
        string_value().prop_map(|s| literal(LiteralValue::String(s))),
        any::<bool>().prop_map(|b| literal(LiteralValue::Boolean(b))),
        Just(literal(LiteralValue::Null)),
        param(false),
    ]
}

fn array() -> impl Strategy<Value = Expr> {
    let elements = prop_oneof![
        prop::collection::vec(signed_number(), 1..4),
        prop::collection::vec(
            string_value().prop_map(|s| literal(LiteralValue::String(s))),
            1..4
        ),
        prop::collection::vec(
            any::<bool>().prop_map(|b| literal(LiteralValue::Boolean(b))),
            1..4
        ),
    ];
    elements.prop_map(|elements| {
        Expr::Array(ArrayLiteral {
            elements,
            span: NO_SPAN,
        })
    })
}

fn cast(expr: Expr, type_name: &str) -> Expr {
    Expr::Cast(Cast {
        expr: Box::new(expr),
        type_name: type_name.to_owned(),
        span: NO_SPAN,
    })
}

fn match_expr() -> impl Strategy<Value = Expr> {
    let lhs = prop_oneof![
        name().prop_map(Expr::Field),
        (name(), prop::sample::select(TYPES)).prop_map(|(n, t)| cast(Expr::Field(n), t)),
    ];
    (lhs, scalar_constant()).prop_map(|(left, right)| {
        Expr::Binary(Binary {
            op: BinaryOp::Match,
            left: Box::new(left),
            right: Box::new(right),
            span: NO_SPAN,
        })
    })
}

fn function_name() -> impl Strategy<Value = String> {
    "[a-z][a-z_]{0,8}".prop_filter("reserved", |w| !is_reserved(w))
}

fn leaf() -> impl Strategy<Value = Expr> {
    prop_oneof![
        number().prop_map(literal),
        string_value().prop_map(|s| literal(LiteralValue::String(s))),
        any::<bool>().prop_map(|b| literal(LiteralValue::Boolean(b))),
        Just(literal(LiteralValue::Null)),
        ("[0-9]{1,4}", prop::sample::select(UNITS)).prop_map(|(value, unit)| {
            Expr::Interval(Interval {
                value,
                unit: unit.to_owned(),
                span: NO_SPAN,
            })
        }),
        name().prop_map(Expr::Field),
        name().prop_map(Expr::Field),
        param(false),
        param(true),
        function_name().prop_map(|name| Expr::Call(Call {
            name,
            args: CallArgs::Star,
            span: NO_SPAN,
        })),
        array(),
        match_expr(),
    ]
}

fn binary_op() -> impl Strategy<Value = BinaryOp> {
    prop::sample::select(vec![
        BinaryOp::Or,
        BinaryOp::And,
        BinaryOp::Eq,
        BinaryOp::NotEq,
        BinaryOp::Lt,
        BinaryOp::LtEq,
        BinaryOp::Gt,
        BinaryOp::GtEq,
        BinaryOp::Add,
        BinaryOp::Sub,
        BinaryOp::Mul,
        BinaryOp::Div,
        BinaryOp::Mod,
    ])
}

fn expr() -> impl Strategy<Value = Expr> {
    leaf().prop_recursive(5, 48, 3, |inner| {
        prop_oneof![
            (
                prop::sample::select(vec![UnaryOp::Not, UnaryOp::Minus, UnaryOp::Plus]),
                inner.clone()
            )
                .prop_map(|(op, operand)| Expr::Unary(Unary {
                    op,
                    operand: Box::new(operand),
                    span: NO_SPAN,
                })),
            (binary_op(), inner.clone(), inner.clone()).prop_map(|(op, left, right)| {
                Expr::Binary(Binary {
                    op,
                    left: Box::new(left),
                    right: Box::new(right),
                    span: NO_SPAN,
                })
            }),
            (
                inner.clone(),
                prop::collection::vec(inner.clone(), 1..3),
                any::<bool>()
            )
                .prop_map(|(expr, list, negated)| Expr::In(InList {
                    expr: Box::new(expr),
                    list,
                    negated,
                    span: NO_SPAN,
                })),
            (
                inner.clone(),
                prop::sample::select(vec![LikeKind::Like, LikeKind::RLike]),
                string_value(),
                any::<bool>()
            )
                .prop_map(|(expr, kind, pattern, negated)| Expr::Like(Like {
                    expr: Box::new(expr),
                    kind,
                    pattern,
                    negated,
                    span: NO_SPAN,
                })),
            (inner.clone(), any::<bool>()).prop_map(|(expr, negated)| Expr::IsNull(IsNull {
                expr: Box::new(expr),
                negated,
                span: NO_SPAN,
            })),
            (inner.clone(), prop::sample::select(TYPES)).prop_map(|(e, t)| cast(e, t)),
            (function_name(), prop::collection::vec(inner, 0..3)).prop_map(|(name, args)| {
                Expr::Call(Call {
                    name,
                    args: CallArgs::List(args),
                    span: NO_SPAN,
                })
            }),
        ]
    })
}

fn field(named: bool) -> impl Strategy<Value = Field> {
    let name = if named {
        name().prop_map(Some).boxed()
    } else {
        prop::option::of(name()).boxed()
    };
    (name, expr()).prop_map(|(name, expr)| Field {
        name,
        expr,
        span: NO_SPAN,
    })
}

fn source_command() -> impl Strategy<Value = Command> {
    let source = prop_oneof![
        prop_oneof![
            "[a-z*][a-z0-9_.*:-]{0,8}",
            Just("metadata".to_owned()),
            Just("METADATA".to_owned()),
        ]
        .prop_map(|text| SourcePattern {
            text,
            quoted: false,
            span: NO_SPAN,
        }),
        string_value().prop_map(|text| SourcePattern {
            text,
            quoted: true,
            span: NO_SPAN,
        }),
    ];
    let metadata = prop::collection::vec(name(), 1..3).prop_map(|fields| Metadata {
        fields,
        span: NO_SPAN,
    });
    prop_oneof![
        prop::collection::vec(field(true), 1..3).prop_map(|fields| Command::Row(RowCommand {
            fields,
            span: NO_SPAN,
        })),
        (
            prop::collection::vec(source, 1..3),
            prop::option::of(metadata)
        )
            .prop_map(|(sources, metadata)| Command::From(FromCommand {
                sources,
                metadata,
                span: NO_SPAN,
            })),
    ]
}

fn processing_command() -> impl Strategy<Value = Command> {
    let aggregate =
        (field(false), prop::option::of(expr())).prop_map(|(field, filter)| Aggregate {
            field,
            filter,
            span: NO_SPAN,
        });
    let sort_key = (
        expr(),
        prop::option::of(prop_oneof![Just(SortOrder::Asc), Just(SortOrder::Desc)]),
        prop::option::of(prop_oneof![Just(NullsOrder::First), Just(NullsOrder::Last)]),
    )
        .prop_map(|(expr, order, nulls)| SortKey {
            expr,
            order,
            nulls,
            span: NO_SPAN,
        });
    let rename = (name(), name()).prop_map(|(old, new)| RenamePair {
        old,
        new,
        span: NO_SPAN,
    });
    prop_oneof![
        expr().prop_map(|condition| Command::Where(WhereCommand {
            condition,
            span: NO_SPAN,
        })),
        prop::collection::vec(field(false), 1..3).prop_map(|fields| Command::Eval(EvalCommand {
            fields,
            span: NO_SPAN,
        })),
        prop::collection::vec(pattern(), 1..3).prop_map(|patterns| Command::Keep(KeepCommand {
            patterns,
            span: NO_SPAN,
        })),
        prop::collection::vec(pattern(), 1..3).prop_map(|patterns| Command::Drop(DropCommand {
            patterns,
            span: NO_SPAN,
        })),
        prop::collection::vec(rename, 1..3).prop_map(|renames| Command::Rename(RenameCommand {
            renames,
            span: NO_SPAN,
        })),
        (
            prop::collection::vec(aggregate, 0..3),
            prop::collection::vec(field(false), 1..3),
            any::<bool>()
        )
            .prop_map(|(aggregates, groupings, by)| {
                let groupings = if by || aggregates.is_empty() {
                    groupings
                } else {
                    Vec::new()
                };
                Command::Stats(StatsCommand {
                    aggregates,
                    groupings,
                    span: NO_SPAN,
                })
            }),
        prop::collection::vec(sort_key, 1..3).prop_map(|keys| Command::Sort(SortCommand {
            keys,
            span: NO_SPAN,
        })),
        any::<u64>().prop_map(|count| Command::Limit(LimitCommand {
            count,
            span: NO_SPAN,
        })),
    ]
}

fn query() -> impl Strategy<Value = Query> {
    (
        source_command(),
        prop::collection::vec(processing_command(), 0..4),
    )
        .prop_map(|(source, processing)| Query {
            source,
            processing,
            span: NO_SPAN,
        })
}

/// An expression with no grouping parentheses: prefix operators, operands,
/// predicate and cast suffixes, and infix operators, in grammar order. The
/// only parentheses are those of `IN` lists and calls, which the printer
/// reproduces, so a tree parsed from it prints with no more `(` than it has
/// unless the parser made a comparison-level node an operand.
fn paren_free_statement() -> impl Strategy<Value = String> {
    const COMMANDS: &[&str] = &[
        "ROW x = ",
        "FROM t | WHERE ",
        "FROM t | EVAL y = ",
        "FROM t | SORT ",
        "FROM t | STATS c = count(*) WHERE ",
    ];
    const OPERANDS: &[&str] = &[
        "a", "b.c", "1", "2.5", "\"s\"", "true", "null", "?p", "count(*)", "f(a)", "1 day",
    ];
    const PREFIXES: &[&str] = &["NOT", "-", "+"];
    const SUFFIXES: &[&str] = &[
        "IN (1, 2)",
        "NOT IN (a)",
        "LIKE \"p*\"",
        "NOT LIKE \"p\"",
        "RLIKE \"r\"",
        "NOT RLIKE \"r\"",
        "IS NULL",
        "IS NOT NULL",
        ": \"m\"",
        "::long",
    ];
    const INFIXES: &[&str] = &[
        "+", "-", "*", "/", "%", "==", "!=", "<", "<=", ">", ">=", "AND", "OR",
    ];
    let term = (
        prop::collection::vec(prop::sample::select(PREFIXES), 0..2),
        prop::sample::select(OPERANDS),
        prop::collection::vec(prop::sample::select(SUFFIXES), 0..3),
    )
        .prop_map(|(prefixes, operand, suffixes)| {
            let mut words = prefixes;
            words.push(operand);
            words.extend(suffixes);
            words.join(" ")
        });
    (
        prop::sample::select(COMMANDS),
        term.clone(),
        prop::collection::vec((prop::sample::select(INFIXES), term), 0..5),
    )
        .prop_map(|(command, first, rest)| {
            let mut text = format!("{command}{first}");
            for (infix, term) in rest {
                text.push_str(&format!(" {infix} {term}"));
            }
            text
        })
}

fn soup() -> impl Strategy<Value = String> {
    let prefix = prop::sample::select(vec![
        "",
        "FROM t | WHERE ",
        "ROW a = ",
        "FROM t | STATS ",
        "FROM t | SORT ",
        "FROM t | KEEP ",
    ]);
    (
        prefix,
        prop::collection::vec(prop::sample::select(VOCABULARY), 0..32),
    )
        .prop_map(|(prefix, words)| format!("{prefix}{}", words.join(" ")))
}

/// Parses `source`; an error must carry a span inside `source` on character
/// boundaries, and a tree must survive a print and reparse unchanged.
fn tree_or_typed_error(source: &str) -> Result<(), TestCaseError> {
    let limits = Limits::default();
    match parse(source, &limits) {
        Ok(query) => {
            let printed = print(&query, &limits).expect("a parsed tree prints");
            match parse(&printed, &limits) {
                Ok(reparsed) => prop_assert!(
                    reparsed.eq_ignoring_spans(&query),
                    "{source:?} printed as {printed:?}"
                ),
                // The printer adds spaces and escapes, so text printed from a
                // statement at a limit may exceed it; then, and only then,
                // the reparse is refused under that limit.
                Err(Error::LimitExceeded {
                    limit: LimitKind::StatementBytes,
                    ..
                }) => prop_assert!(printed.len() > limits.max_statement_bytes),
                Err(Error::LimitExceeded {
                    limit: LimitKind::Tokens,
                    ..
                }) => {
                    let unlimited = Limits {
                        max_tokens: usize::MAX,
                        ..limits
                    };
                    let tokens = tokenize(&printed, &unlimited).expect("printed text lexes");
                    prop_assert!(tokens.len() > limits.max_tokens);
                }
                Err(error) => {
                    return Err(TestCaseError::fail(format!(
                        "{source:?} printed as {printed:?}, which is refused: {error}"
                    )));
                }
            }
        }
        Err(error) => {
            let span = error.span();
            prop_assert!(span.start <= span.end && span.end <= source.len());
            prop_assert!(
                span.slice(source).is_some(),
                "{error:?} splits a character in {source:?}"
            );
            let at = error.line_col(source);
            prop_assert!(at.line >= 1 && at.column >= 1);
            prop_assert!(!error.to_string().is_empty());
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(2048))]

    #[test]
    fn arbitrary_input_yields_a_tree_or_a_typed_error(
        source in prop_oneof![any::<String>(), "\\PC{0,64}", soup()]
    ) {
        tree_or_typed_error(&source)?;
    }

    #[test]
    fn no_comparison_is_an_operand_unless_parenthesised(source in paren_free_statement()) {
        let limits = Limits::default();
        if let Ok(query) = parse(&source, &limits) {
            let printed = print(&query, &limits).expect("a parsed tree prints");
            let opened = |text: &str| text.matches('(').count();
            prop_assert!(
                opened(&printed) <= opened(&source),
                "{source:?} printed as {printed:?}"
            );
        }
    }

    #[test]
    fn printed_tree_parses_back_to_itself(query in query()) {
        let limits = Limits::default();
        let printed = print(&query, &limits).expect("a generated tree prints");
        let reparsed = match parse(&printed, &limits) {
            Ok(reparsed) => reparsed,
            Err(error) => {
                return Err(TestCaseError::fail(format!("{printed:?}: {error}")));
            }
        };
        prop_assert!(reparsed.eq_ignoring_spans(&query), "{printed}\n{reparsed:?}\n{query:?}");
        prop_assert_eq!(print(&reparsed, &limits).expect("reprints"), printed);
    }
}
