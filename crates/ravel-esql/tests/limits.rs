//! Structural limits: each is accepted at its value and refused one past it,
//! and the refusal comes before parsing, so a syntax error placed ahead of
//! the breach does not mask it. The worst shapes the default limits admit,
//! nested pipelines and option maps included, parse, print, compare with
//! `==`, clone, format
//! with `{:?}` and drop on a 2 MiB stack, and on a 1 MiB one for twice that
//! headroom.

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
        // An `IN` element is a value, so each step nests a parenthesised
        // `IN`: two levels per step, like the shapes below.
        nested(
            "(",
            &nested("a IN ((", "1", "))", (d - 1) / 2),
            ")",
            1 - d % 2,
        ),
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

/// `FROM t, (FROM t, ( ... (FROM t <rest>) ... ))`: `levels` subqueries, each
/// one stage and one nesting level.
fn from_subqueries(levels: usize, rest: &str) -> String {
    format!(
        "FROM {}t{rest}{}",
        "t, (FROM ".repeat(levels),
        ")".repeat(levels)
    )
}

/// `FROM t | FORK (FORK ( ... ) (WHERE true)) (WHERE true)`: `levels` `FORK`
/// commands, each nesting the next in its first branch.
fn nested_forks(levels: usize) -> String {
    nested_forks_around(levels, "WHERE true")
}

/// [`nested_forks`] with `branch` as the innermost `FORK`'s first branch.
fn nested_forks_around(levels: usize, branch: &str) -> String {
    let mut inner = format!("FORK ({branch}) (WHERE true)");
    for _ in 1..levels {
        inner = format!("FORK ({inner}) (WHERE true)");
    }
    format!("FROM t | {inner}")
}

/// `FROM t | WHERE a IN (FROM t | WHERE a IN ( ... (FROM t) ... ))` with
/// `levels` `IN` subqueries.
fn in_subqueries(levels: usize) -> String {
    in_subqueries_around(levels, "")
}

/// [`in_subqueries`] with `rest` after the innermost `FROM t`.
fn in_subqueries_around(levels: usize, rest: &str) -> String {
    let mut inner = format!("FROM t{rest}");
    for _ in 0..levels {
        inner = format!("FROM t | WHERE a IN ({inner})");
    }
    inner
}

/// `{"a": {"a": ... {"a": 1} ... }}` with `levels` maps.
fn nested_maps(levels: usize) -> String {
    nested("{\"a\": ", "1", "}", levels)
}

/// The deepest nested pipelines and option maps the default limits admit, as
/// whole statements, each with the statement one step past it and the limit
/// that refuses that one. Every command in a nested pipeline, a `FORK`
/// branch's included, is a stage, and every nested pipeline is a nesting
/// level.
fn worst_admitted_pipelines(limits: &Limits) -> Vec<(String, String, LimitKind)> {
    let d = limits.max_depth;
    let stages = limits.max_pipeline_stages;
    // The outer `FROM` and one per subquery.
    let from = stages - 1;
    // `FROM t | FORK` and, per `FORK`, its `(WHERE true)` branch; the first
    // `FORK` has two.
    let fork = (stages - 2) / 2;
    // The innermost `FROM t` and, per level, `FROM t | WHERE`.
    let in_list = (stages - 1) / 2;
    // Subqueries around a `WHERE` that spends the rest of the depth on calls:
    // one level per subquery, one for the condition and one per call.
    let mixed = stages - 2;
    let calls = |levels| format!(" | WHERE {}", nested("f(", "1", ")", levels));
    // A `FORK` branch condition sits one level inside its branch, like a
    // subquery's.
    let fork_where = |levels| format!("WHERE {}", nested("f(", "1", ")", levels));
    let completion = |levels| format!("COMPLETION y = \"p\" WITH {}", nested_maps(levels));
    vec![
        // The deepest `FORK` nesting, its innermost branch spending the rest
        // of the depth on calls, then on an option map.
        (
            nested_forks_around(fork, &fork_where(d - fork - 1)),
            nested_forks_around(fork, &fork_where(d - fork)),
            LimitKind::NestingDepth,
        ),
        (
            nested_forks_around(fork, &completion(d - fork - 1)),
            nested_forks_around(fork, &completion(d - fork)),
            LimitKind::NestingDepth,
        ),
        // The deepest `IN` subqueries with one more stage innermost, so the
        // statement holds every stage, and that stage's condition spending
        // the rest of the depth: two levels per subquery, one for the
        // condition and one per call.
        (
            in_subqueries_around(in_list, &calls(d - 2 * in_list - 1)),
            in_subqueries_around(in_list, &calls(d - 2 * in_list)),
            LimitKind::NestingDepth,
        ),
        // The deepest `FROM` subqueries with an option map spending the rest
        // of the depth.
        (
            from_subqueries(mixed, &format!(" | {}", completion(d - mixed - 1))),
            from_subqueries(mixed, &format!(" | {}", completion(d - mixed))),
            LimitKind::NestingDepth,
        ),
        // Option maps nested as deep as they go in a setting and as an
        // argument.
        (
            format!("SET a = {}; ROW x = 1", nested_maps(d - 1)),
            format!("SET a = {}; ROW x = 1", nested_maps(d)),
            LimitKind::NestingDepth,
        ),
        (
            format!("ROW x = f({})", nested_maps(d - 2)),
            format!("ROW x = f({})", nested_maps(d - 1)),
            LimitKind::NestingDepth,
        ),
        (
            from_subqueries(from, ""),
            from_subqueries(from + 1, ""),
            LimitKind::PipelineStages,
        ),
        (
            nested_forks(fork),
            nested_forks(fork + 1),
            LimitKind::PipelineStages,
        ),
        (
            in_subqueries(in_list),
            in_subqueries(in_list + 1),
            LimitKind::PipelineStages,
        ),
        (
            from_subqueries(mixed, &calls(d - mixed - 1)),
            from_subqueries(mixed, &calls(d - mixed)),
            LimitKind::NestingDepth,
        ),
    ]
}

#[test]
fn nested_pipelines_count_against_the_limits() {
    let limits = Limits::default();
    for (at, past, kind) in worst_admitted_pipelines(&limits) {
        accepted(&at, &limits);
        refused(&past, &limits, kind);
    }
    // The `IN` and `FORK` shapes that spend the rest of the depth also hold
    // every stage: one more command is refused.
    let in_list = (limits.max_pipeline_stages - 1) / 2;
    refused(
        &in_subqueries_around(in_list, " | WHERE true | LIMIT 1"),
        &limits,
        LimitKind::PipelineStages,
    );
    let fork = (limits.max_pipeline_stages - 2) / 2;
    refused(
        &nested_forks_around(fork, "WHERE true | LIMIT 1"),
        &limits,
        LimitKind::PipelineStages,
    );
}

/// Parses `source`, prints it, reparses the printed text and compares, clones,
/// compares with `==`, formats with `{:?}` and drops every tree.
fn exercise(source: &str, limits: &Limits) {
    let query = parse(source, limits).expect("admitted");
    let printed = print(&query, limits).expect("prints");
    let reparsed = parse(&printed, limits).expect("printed text parses");
    assert!(reparsed.eq_ignoring_spans(&query), "{printed}");
    let copy = query.clone();
    assert!(copy == query);
    assert_eq!(format!("{copy:?}"), format!("{query:?}"));
    drop(copy);
    drop(reparsed);
    drop(query);
}

/// Runs [`exercise`] over every worst admitted shape on a thread with
/// `stack_bytes` of stack.
fn worst_admitted_fit(stack_bytes: usize) {
    let handle = std::thread::Builder::new()
        .stack_size(stack_bytes)
        .spawn(|| {
            let limits = Limits::default();
            for expr in worst_admitted_inputs(&limits) {
                let source = format!(
                    "ROW x = {expr} | WHERE {expr} | STATS y = {expr} WHERE {expr} BY {expr}"
                );
                exercise(&source, &limits);
                let one_deeper = format!("ROW x = ({expr})");
                assert!(
                    parse(&one_deeper, &limits).is_err(),
                    "one level past the limit"
                );
            }
            for (source, _, _) in worst_admitted_pipelines(&limits) {
                exercise(&source, &limits);
            }
        })
        .expect("spawn");
    handle.join().expect("no stack overflow");
}

#[test]
fn worst_admitted_input_fits_a_two_mebibyte_stack() {
    worst_admitted_fit(2 * 1024 * 1024);
}

/// Half the 2 MiB budget, so the worst admitted input keeps twice its stack
/// as headroom in an unoptimised build.
#[test]
fn worst_admitted_input_fits_a_one_mebibyte_stack() {
    worst_admitted_fit(1024 * 1024);
}
