//! Every error variant from a concrete input, with its exact span and 1-based
//! line and column.

#![allow(clippy::expect_used)]

use ravel_esql::{Error, LimitKind, Limits, LineCol, Span, parse};

fn refuse(source: &str, limits: &Limits) -> Error {
    match parse(source, limits) {
        Ok(query) => panic!("{source:?} parsed: {query:?}"),
        Err(error) => error,
    }
}

fn assert_error(source: &str, limits: &Limits, expected: Error, line: usize, column: usize) {
    let error = refuse(source, limits);
    assert_eq!(error, expected, "{source:?}");
    assert_eq!(
        error.line_col(source),
        LineCol { line, column },
        "{source:?}: {error}"
    );
}

#[test]
fn limit_exceeded() {
    let limits = Limits {
        max_tokens: 3,
        ..Limits::default()
    };
    assert_error(
        "ROW a = 1",
        &limits,
        Error::LimitExceeded {
            limit: LimitKind::Tokens,
            max: 3,
            span: Span::new(8, 9),
        },
        1,
        9,
    );
    let error = refuse("ROW a = 1", &limits);
    assert_eq!(error.to_string(), "token limit of 3 exceeded at byte 8");
}

#[test]
fn unterminated_string() {
    assert_error(
        "ROW a = \"abc",
        &Limits::default(),
        Error::UnterminatedString {
            span: Span::new(8, 9),
        },
        1,
        9,
    );
    assert_error(
        "ROW a = \"ab\nc\"",
        &Limits::default(),
        Error::UnterminatedString {
            span: Span::new(8, 9),
        },
        1,
        9,
    );
    assert_error(
        "ROW a = \"\"\"abc\"\"",
        &Limits::default(),
        Error::UnterminatedString {
            span: Span::new(8, 11),
        },
        1,
        9,
    );
}

#[test]
fn unterminated_quoted_identifier() {
    assert_error(
        "ROW `abc = 1",
        &Limits::default(),
        Error::UnterminatedQuotedIdentifier {
            span: Span::new(4, 5),
        },
        1,
        5,
    );
}

#[test]
fn unterminated_comment() {
    assert_error(
        "ROW a = 1 /* x /* y */",
        &Limits::default(),
        Error::UnterminatedComment {
            span: Span::new(10, 12),
        },
        1,
        11,
    );
}

#[test]
fn invalid_escape() {
    assert_error(
        "ROW a = \"a\\qb\"",
        &Limits::default(),
        Error::InvalidEscape {
            span: Span::new(10, 12),
        },
        1,
        11,
    );
}

#[test]
fn unexpected_character() {
    assert_error(
        "ROW a = #",
        &Limits::default(),
        Error::UnexpectedCharacter {
            found: '#',
            span: Span::new(8, 9),
        },
        1,
        9,
    );
    assert_error(
        "ROW a = __",
        &Limits::default(),
        Error::UnexpectedCharacter {
            found: '_',
            span: Span::new(8, 9),
        },
        1,
        9,
    );
}

#[test]
fn unexpected_token() {
    assert_error(
        "ROW a = 1 2",
        &Limits::default(),
        Error::UnexpectedToken {
            found: "`2`".into(),
            expected: "`|` or end of input",
            span: Span::new(10, 11),
        },
        1,
        11,
    );
    assert_error(
        "ROW a =",
        &Limits::default(),
        Error::UnexpectedToken {
            found: "end of input".into(),
            expected: "an expression",
            span: Span::new(7, 7),
        },
        1,
        8,
    );
    assert_error(
        "FROM t | FROBNICATE x",
        &Limits::default(),
        Error::UnexpectedToken {
            found: "`FROBNICATE`".into(),
            expected: "a processing command",
            span: Span::new(9, 19),
        },
        1,
        10,
    );
    assert_error(
        "WHERE a",
        &Limits::default(),
        Error::UnexpectedToken {
            found: "`WHERE`".into(),
            expected: "a source command (`FROM` or `ROW`)",
            span: Span::new(0, 5),
        },
        1,
        1,
    );
    assert_error(
        "ROW a = 1 | WHERE a == b == c",
        &Limits::default(),
        Error::UnexpectedToken {
            found: "`==`".into(),
            expected: "`AND`, `OR`, `|` or end of input (a comparison, `:`, `IN`, `LIKE`, \
                       `RLIKE` or `IS NULL` does not chain and is not an operand of arithmetic \
                       or `::` unless parenthesised)",
            span: Span::new(25, 27),
        },
        1,
        26,
    );
}

#[test]
fn unexpected_token_on_line_three_counts_characters() {
    let source = "ROW\n  a = \"é\",\n  b = $";
    assert_error(
        source,
        &Limits::default(),
        Error::UnexpectedCharacter {
            found: '$',
            span: Span::new(22, 23),
        },
        3,
        7,
    );
    let source = "FROM t\n| WHERE a == \"ünï\"\n| EVAL x = 1 +";
    assert_error(
        source,
        &Limits::default(),
        Error::UnexpectedToken {
            found: "end of input".into(),
            expected: "an expression",
            span: Span::new(source.len(), source.len()),
        },
        3,
        15,
    );
}

#[test]
fn unexpected_token_text_is_truncated_to_forty_characters() {
    let token = format!("\"{}\"", "é".repeat(60));
    let source = format!("ROW a = 1 {token}");
    assert_error(
        &source,
        &Limits::default(),
        Error::UnexpectedToken {
            found: format!("`\"{}`", "é".repeat(39)),
            expected: "`|` or end of input",
            span: Span::new(10, 10 + token.len()),
        },
        1,
        11,
    );
}

#[test]
fn line_col_of_any_byte_offset() {
    let source = "aé\nb";
    let at = |offset| LineCol::at(source, offset);
    assert_eq!(at(0), LineCol { line: 1, column: 1 });
    assert_eq!(at(1), LineCol { line: 1, column: 2 });
    // Inside the two-byte `é`: that character's column.
    assert_eq!(at(2), LineCol { line: 1, column: 2 });
    assert_eq!(at(3), LineCol { line: 1, column: 3 });
    assert_eq!(at(4), LineCol { line: 2, column: 1 });
    // At or past the end: just after the last character.
    assert_eq!(at(5), LineCol { line: 2, column: 2 });
    assert_eq!(at(99), LineCol { line: 2, column: 2 });
}

#[test]
fn integer_out_of_range() {
    assert_error(
        "FROM t | LIMIT 18446744073709551616",
        &Limits::default(),
        Error::IntegerOutOfRange {
            text: "18446744073709551616".into(),
            span: Span::new(15, 35),
        },
        1,
        16,
    );
}

#[test]
fn unsupported_syntax_names_the_command() {
    let source = "FROM logs\n| WHERE level == \"ERROR\"\n| grok message \"%{WORD:verb}\"";
    assert_error(
        source,
        &Limits::default(),
        Error::UnsupportedSyntax {
            command: "GROK".into(),
            span: Span::new(37, 41),
        },
        3,
        3,
    );
    assert_error(
        "SHOW INFO",
        &Limits::default(),
        Error::UnsupportedSyntax {
            command: "SHOW".into(),
            span: Span::new(0, 4),
        },
        1,
        1,
    );
    let error = refuse(source, &Limits::default());
    assert_eq!(
        error.to_string(),
        "the GROK command is not supported yet (byte 37)"
    );
}

#[test]
fn every_known_unsupported_command_is_named_with_its_span() {
    let source_commands = ["EXPLAIN", "METRICS", "SET", "SHOW", "TS"];
    for command in source_commands {
        let source = format!("{command} x");
        assert_error(
            &source,
            &Limits::default(),
            Error::UnsupportedSyntax {
                command: command.into(),
                span: Span::new(0, command.len()),
            },
            1,
            1,
        );
    }
    // (as written, as reported)
    let processing_commands = [
        ("CHANGE_POINT", "CHANGE_POINT"),
        ("COMPLETION", "COMPLETION"),
        ("DISSECT", "DISSECT"),
        ("ENRICH", "ENRICH"),
        ("FORK", "FORK"),
        ("FUSE", "FUSE"),
        ("GROK", "GROK"),
        ("INLINE STATS", "INLINE STATS"),
        ("inline\n  Stats", "INLINE STATS"),
        ("INLINESTATS", "INLINESTATS"),
        ("JOIN", "JOIN"),
        ("LOOKUP JOIN", "LOOKUP JOIN"),
        ("lookup  join", "LOOKUP JOIN"),
        ("LOOKUP", "LOOKUP"),
        ("MV_EXPAND", "MV_EXPAND"),
        ("RERANK", "RERANK"),
        ("SAMPLE", "SAMPLE"),
    ];
    const PREFIX: &str = "FROM t | ";
    for (written, reported) in processing_commands {
        let source = format!("{PREFIX}{written} x");
        assert_error(
            &source,
            &Limits::default(),
            Error::UnsupportedSyntax {
                command: reported.into(),
                span: Span::new(PREFIX.len(), PREFIX.len() + written.len()),
            },
            1,
            PREFIX.len() + 1,
        );
    }
    // The first word of a two-word form alone is not that command.
    assert!(matches!(
        refuse("FROM t | INLINE x", &Limits::default()),
        Error::UnexpectedToken { .. }
    ));
}
