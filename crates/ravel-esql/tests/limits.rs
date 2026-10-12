//! Structural limits: each is accepted at its value and refused one past it,
//! and the refusal comes before parsing, so a syntax error placed ahead of
//! the breach does not mask it. The worst shapes the default limits admit
//! parse, print, compare with `==`, clone, format with `{:?}` and drop on a
//! 2 MiB stack.

#![allow(clippy::expect_used)]

use ravel_esql::{Error, LimitKind, Limits, Span, parse, print};

fn accepted(source: &str, limits: &Limits) {
    if let Err(error) = parse(source, limits) {
        panic!("refused at the limit: {error}\n{source}");
    }
}

fn refused(source: &str, limits: &Limits, kind: LimitKind) -> Span {
    match parse(source, limits) {
        Err(Error::LimitExceeded { limit, max, span }) => {
            assert_eq!(limit, kind, "{source}");
            let configured = match kind {
                LimitKind::StatementBytes => limits.max_statement_bytes,
                LimitKind::Tokens => limits.max_tokens,
                LimitKind::NestingDepth => limits.max_depth,
                LimitKind::PipelineStages => limits.max_pipeline_stages,
            };
            assert_eq!(max, configured);
            let message = Error::LimitExceeded { limit, max, span }.to_string();
            assert!(message.contains(&kind.to_string()), "{message}");
            span
        }
        other => panic!("expected the {kind} limit, got {other:?}\n{source}"),
    }
}

fn nested(open: &str, inner: &str, close: &str, levels: usize) -> String {
    format!("{}{inner}{}", open.repeat(levels), close.repeat(levels))
}

#[test]
fn each_structural_limit_refuses_before_parsing() {
    // Statement bytes: the length is checked before anything is lexed.
    let limits = Limits {
        max_statement_bytes: 64,
        ..Limits::default()
    };
    let at = format!("ROW a = {}", "1".repeat(56));
    assert_eq!(at.len(), 64);
    accepted(&at, &limits);
    let past = format!("ROW a = ) \"unterminated{}", "x".repeat(42));
    assert_eq!(past.len(), 65);
    assert_eq!(
        refused(&past, &limits, LimitKind::StatementBytes),
        Span::new(64, 65)
    );

    // Tokens: counted while lexing, before the parser sees the first one.
    let limits = Limits {
        max_tokens: 12,
        ..Limits::default()
    };
    let at = "ROW a = 1, b = 2, c = 3";
    accepted(at, &limits);
    // Whitespace and comments are not tokens.
    accepted(
        "ROW a = 1, /* x y z */ b = 2,\n  // p q r\n  c = 3",
        &limits,
    );
    let past = "ROW ) = 1, b = 2, c = 3 d";
    let span = refused(past, &limits, LimitKind::Tokens);
    assert_eq!(span.slice(past), Some("d"));

    // Pipeline stages: counted over the tokens before parsing.
    let limits = Limits {
        max_pipeline_stages: 5,
        ..Limits::default()
    };
    let at = format!("ROW a = 1{}", " | LIMIT 1".repeat(4));
    accepted(&at, &limits);
    let past = format!("ROW a = ){}", " | LIMIT 1".repeat(5));
    let span = refused(&past, &limits, LimitKind::PipelineStages);
    assert_eq!(span.start, past.rfind('|').expect("a pipe"));

    // Nesting depth from brackets: checked over the tokens before parsing.
    for max_depth in [2, 8, Limits::default().max_depth] {
        let limits = Limits {
            max_depth,
            ..Limits::default()
        };
        for (open, inner, close) in [("(", "1", ")"), ("f(", "1", ")"), ("[", "1", "]")] {
            if open == "[" && max_depth > 2 {
                continue;
            }
            let at = format!("ROW x = {}", nested(open, inner, close, max_depth - 1));
            accepted(&at, &limits);
            let past = format!(
                "ROW x = ) | EVAL y = {}",
                nested(open, inner, close, max_depth)
            );
            let span = refused(&past, &limits, LimitKind::NestingDepth);
            assert_eq!(
                span.start,
                past.rfind(open).expect("an opener") + open.len() - 1
            );
        }
    }

    // Nesting depth from operators: checked before the parser descends a
    // level or builds a node one level taller, so the syntax error after the
    // breach is never reached.
    for max_depth in [2, 8, Limits::default().max_depth] {
        let limits = Limits {
            max_depth,
            ..Limits::default()
        };
        let chains = [
            (|k: usize| format!("{}true", "NOT ".repeat(k))) as fn(usize) -> String,
            |k| format!("{}1", "-".repeat(k)),
            |k| format!("{}1", "+".repeat(k)),
            |k| format!("1{}", " + 1".repeat(k)),
            |k| format!("a{}", " OR a".repeat(k)),
            |k| format!("1{}", "::long".repeat(k)),
        ];
        for chain in chains {
            let at = format!("ROW x = {}", chain(max_depth - 1));
            accepted(&at, &limits);
            let past = format!("ROW x = {} )", chain(max_depth));
            refused(&past, &limits, LimitKind::NestingDepth);
        }
        // Each `1 + (` adds two levels: the addition and the parentheses.
        let levels = (max_depth - 1) / 2;
        let at = format!("ROW x = {}", nested("1 + (", "1", ")", levels));
        accepted(&at, &limits);
        let past = format!("ROW x = {} )", nested("1 + (", "1", ")", levels + 1));
        refused(&past, &limits, LimitKind::NestingDepth);
    }
}

#[test]
fn printer_refuses_a_tree_deeper_than_its_limits() {
    let deep = parse(
        &format!("ROW x = {}", "-".repeat(20)),
        &Limits {
            max_depth: 21,
            ..Limits::default()
        },
    );
    assert!(deep.is_err(), "20 minus signs and no operand do not parse");
    let query = parse(
        &format!("ROW x = {}1", "-".repeat(20)),
        &Limits {
            max_depth: 21,
            ..Limits::default()
        },
    )
    .expect("21 levels parse at a limit of 21");
    let shallow = Limits {
        max_depth: 20,
        ..Limits::default()
    };
    assert!(matches!(
        print(&query, &shallow),
        Err(Error::LimitExceeded {
            limit: LimitKind::NestingDepth,
            max: 20,
            ..
        })
    ));
    let printed = print(
        &query,
        &Limits {
            max_depth: 21,
            ..Limits::default()
        },
    )
    .expect("prints at its own limit");
    assert_eq!(printed, format!("ROW x = {}1", "-".repeat(20)));
}

#[test]
fn printed_text_past_the_byte_limit_is_refused_under_that_limit() {
    let limits = Limits {
        max_statement_bytes: 9,
        ..Limits::default()
    };
    let source = "ROW a=1+2";
    assert_eq!(source.len(), 9);
    let query = parse(source, &limits).expect("parses at the byte limit");
    let printed = print(&query, &limits).expect("prints");
    assert_eq!(printed, "ROW a = 1 + 2");
    assert_eq!(
        parse(&printed, &limits),
        Err(Error::LimitExceeded {
            limit: LimitKind::StatementBytes,
            max: 9,
            span: Span::new(9, 13),
        })
    );

    // Each raw tab in a string prints as the two-byte escape `\t`.
    let limits = Limits::default();
    let source = format!("ROW x = \"{}\"", "\t".repeat(140_000));
    assert_eq!(source.len(), 140_010);
    let query = parse(&source, &limits).expect("parses under the default limits");
    let printed = print(&query, &limits).expect("prints");
    assert_eq!(printed.len(), 280_010);
    assert_eq!(
        parse(&printed, &limits),
        Err(Error::LimitExceeded {
            limit: LimitKind::StatementBytes,
            max: limits.max_statement_bytes,
            span: Span::new(limits.max_statement_bytes, 280_010),
        })
    );
}

#[test]
fn nesting_depth_error_says_the_limit_bounds_tree_height() {
    let limits = Limits::default();
    let d = limits.max_depth;
    let at = format!("ROW x = a{}", " OR a".repeat(d - 1));
    accepted(&at, &limits);
    let past = format!("ROW x = a{}", " OR a".repeat(d));
    assert!(!past.contains('('));
    let span = refused(&past, &limits, LimitKind::NestingDepth);
    let error = Error::LimitExceeded {
        limit: LimitKind::NestingDepth,
        max: d,
        span,
    };
    assert_eq!(
        error.to_string(),
        format!(
            "nesting depth limit of 128 exceeded at byte {}; the depth limit bounds syntax \
             tree height, so a flat chain such as `a OR a OR ...` with more operands than the \
             limit is refused although it has no brackets",
            span.start
        )
    );
    // The other limits carry no note.
    let bytes = Error::LimitExceeded {
        limit: LimitKind::StatementBytes,
        max: 9,
        span: Span::new(9, 13),
    };
    assert_eq!(
        bytes.to_string(),
        "statement byte limit of 9 exceeded at byte 9"
    );
}

/// The deepest expression shapes the default limits admit, one per recursion
/// path through the parser and printer.
fn worst_admitted_inputs(limits: &Limits) -> Vec<String> {
    let d = limits.max_depth;
    vec![
        nested("(", "1", ")", d - 1),
        nested("f(", "1", ")", d - 1),
        nested("a IN (", "1", ")", d - 1),
        format!("{}true", "NOT ".repeat(d - 1)),
        format!("{}1", "-".repeat(d - 1)),
        format!("1{}", " + 1".repeat(d - 1)),
        format!("a{}", " OR a".repeat(d - 1)),
        format!("1{}", "::long".repeat(d - 1)),
        // Two levels per step, so one more pair of parentheses reaches `d`
        // when it is even.
        nested("(", &nested("1 + (", "1", ")", (d - 1) / 2), ")", 1 - d % 2),
        nested("(", &nested("-(", "1", ")", (d - 1) / 2), ")", 1 - d % 2),
        nested(
            "(",
            &nested("NOT (", "true", ")", (d - 1) / 2),
            ")",
            1 - d % 2,
        ),
    ]
}

#[test]
fn worst_admitted_input_fits_a_two_mebibyte_stack() {
    let handle = std::thread::Builder::new()
        .stack_size(2 * 1024 * 1024)
        .spawn(|| {
            let limits = Limits::default();
            for expr in worst_admitted_inputs(&limits) {
                let source = format!(
                    "ROW x = {expr} | WHERE {expr} | STATS y = {expr} WHERE {expr} BY {expr}"
                );
                let query = parse(&source, &limits).expect("admitted");
                let printed = print(&query, &limits).expect("prints");
                let reparsed = parse(&printed, &limits).expect("printed text parses");
                assert!(reparsed.eq_ignoring_spans(&query), "{printed}");
                let copy = query.clone();
                assert!(copy == query);
                assert_eq!(format!("{copy:?}"), format!("{query:?}"));
                drop(copy);
                drop(reparsed);
                drop(query);
                let one_deeper = format!("ROW x = ({expr})");
                assert!(
                    parse(&one_deeper, &limits).is_err(),
                    "one level past the limit"
                );
            }
        })
        .expect("spawn");
    handle.join().expect("no stack overflow");
}
