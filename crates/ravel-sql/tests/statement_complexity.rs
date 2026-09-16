//! The pre-parse structural-complexity gate on the SQL surface (issue #1680).
//!
//! `validate` is the single funnel every SQL surface reaches (the HTTP
//! handler, `get_flight_info_statement`, `do_get_statement`, and the page
//! plan), so these cases exercise the gate through `validate` itself rather
//! than through the guard module directly: what matters is that a statement
//! whose tree would abort the process never reaches `DFParser::parse_sql`, and
//! that an ordinary analytic statement still does.
//!
//! Every case runs on a thread with a 2 MiB stack, the size a `tokio::main`
//! worker thread gets, because that is the budget the production failure is
//! measured against. A test-harness thread's stack is larger, so running these
//! on it would prove nothing about the endpoint.
#![allow(clippy::expect_used, clippy::unwrap_used)]

use ravel_sql::{MAX_STATEMENT_COMPLEXITY, ValidationError, structural_count, validate};

/// A tokio worker's stack budget.
const WORKER_STACK_BYTES: usize = 2 << 20;

/// Run `body` on a thread with a worker-sized stack and return its value.
fn on_worker_stack<T: Send + 'static>(body: impl FnOnce() -> T + Send + 'static) -> T {
    std::thread::Builder::new()
        .stack_size(WORKER_STACK_BYTES)
        .spawn(body)
        .expect("spawn")
        .join()
        .expect("the guard must reject before any deep recursion")
}

/// The payload from the ticket: a 1 MiB body holding `SELECT 1` and `+1`
/// 500,000 times. Before the guard, `validate` parsed this and then walked the
/// 500,000-level tree, overflowing the worker stack and aborting the process.
/// It is now a typed rejection carrying both counts.
#[test]
fn the_half_million_operator_statement_is_rejected_with_the_typed_error() {
    let err = on_worker_stack(|| {
        let sql = format!("SELECT 1{}", "+1".repeat(500_000));
        validate(&sql).expect_err("must be rejected")
    });
    match err {
        ValidationError::TooComplex(too_complex) => {
            assert_eq!(too_complex.max, MAX_STATEMENT_COMPLEXITY);
            assert_eq!(too_complex.count, MAX_STATEMENT_COMPLEXITY + 1);
        }
        other => panic!("expected TooComplex, got {other:?}"),
    }
}

/// The same shape written with a different operator, so the rejection is not
/// tied to one token: a string-concatenation chain builds the same tree.
#[test]
fn a_concatenation_chain_is_rejected_too() {
    let err = on_worker_stack(|| {
        let sql = format!("SELECT 'a'{}", "||'a'".repeat(500_000));
        validate(&sql).expect_err("must be rejected")
    });
    assert!(
        matches!(err, ValidationError::TooComplex(_)),
        "expected TooComplex, got {err:?}"
    );
}

/// A boolean chain in a `WHERE` clause, which a query generator can emit by
/// accident, is the same tree and the same rejection.
#[test]
fn a_long_boolean_chain_is_rejected_too() {
    let err = on_worker_stack(|| {
        let sql = format!(
            "SELECT ts FROM samples WHERE 1=1{}",
            " AND 1=1".repeat(500_000)
        );
        validate(&sql).expect_err("must be rejected")
    });
    assert!(
        matches!(err, ValidationError::TooComplex(_)),
        "expected TooComplex, got {err:?}"
    );
}

/// The bound is not so low that it breaks real use: a statement a user would
/// actually write -- three joins, a projection with aggregates, a `WHERE`
/// clause with a 100-element `IN` list, grouping and ordering -- is accepted.
/// This is the case that fails if the bound is tightened without measuring.
#[test]
fn a_realistic_analytic_statement_is_accepted() {
    let sql = on_worker_stack(|| {
        let in_list: Vec<String> = (0..100).map(|i| format!("'series-{i:04}'")).collect();
        let sql = format!(
            "SELECT s.series_id, \
                    count(s.value) AS samples, \
                    min(s.value) AS lo, \
                    max(s.value) AS hi, \
                    avg(s.value) AS mean \
             FROM samples s \
             JOIN samples t ON s.series_id = t.series_id \
             JOIN samples u ON s.series_id = u.series_id \
             WHERE s.ts > 1735689600000000000 \
               AND s.ts < 1735693200000000000 \
               AND s.series_id IN ({}) \
               AND t.value > 0.5 \
               AND u.value < 99.5 \
             GROUP BY s.series_id \
             ORDER BY hi DESC \
             LIMIT 100",
            in_list.join(", ")
        );
        validate(&sql).expect("a statement a real user would write must be accepted");
        sql
    });
    // The statement is substantial, not a token gesture at one: pin its size
    // so a later edit cannot shrink it into a trivially-passing case.
    assert!(sql.len() > 1_500, "statement length {}", sql.len());
}

/// The bound is not set below what real analytic SQL needs. The ClickBench
/// corpus is the largest body of real statements this repository holds, read
/// from where the benchmarks keep it rather than copied, and every one of its
/// statements must pass the gate. Its largest statement (90 `SUM(col + n)`
/// terms) is the one that decides the bound, so its exact structural count is
/// pinned here: a later tightening of `MAX_STATEMENT_COMPLEXITY` fails this
/// test rather than a user's query.
#[test]
fn every_clickbench_corpus_statement_is_accepted() {
    const CLICKBENCH_CORPUS: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../benchmarks/clickbench/hits.corpus.json"
    ));
    let corpus: serde_json::Value = serde_json::from_str(CLICKBENCH_CORPUS).expect("corpus parses");
    let entries = corpus["entries"].as_array().expect("corpus has entries");
    assert_eq!(entries.len(), 43, "corpus statement count");

    let mut widest = (0usize, String::new());
    for entry in entries {
        let sql = entry["sql"].as_str().expect("entry has sql");
        let id = entry["id"].as_str().unwrap_or("<unnamed>").to_string();
        validate(sql).unwrap_or_else(|err| panic!("{id} must pass the gate: {err}"));
        let count = structural_count(sql);
        if count > widest.0 {
            widest = (count, id);
        }
    }

    assert_eq!(
        widest,
        (630, "q30_resolution_running_sums".to_string()),
        "the widest corpus statement, and its exact token count"
    );
    assert!(
        widest.0 < MAX_STATEMENT_COMPLEXITY,
        "the bound must admit the corpus: {} vs {MAX_STATEMENT_COMPLEXITY}",
        widest.0
    );
}

/// The parser's own recursion limit is the second bound, and it is pinned
/// here because the complexity gate cannot stand in for it: 200 nested
/// parentheses cost 400 structural tokens, well under the bound, and the
/// parser must refuse them on its own. A statement nested just under the limit
/// still parses, so this pins a limit rather than a blanket refusal.
#[test]
fn nested_parentheses_are_refused_by_the_parser_recursion_limit() {
    on_worker_stack(|| {
        let deep = format!("SELECT {}1{}", "(".repeat(200), ")".repeat(200));
        assert!(structural_count(&deep) < MAX_STATEMENT_COMPLEXITY);
        match validate(&deep).expect_err("must be refused") {
            ValidationError::Parse(message) => assert!(
                message.contains("Recursion"),
                "the parser's own limit must be what refuses it: {message}"
            ),
            other => panic!("expected a parse error, got {other:?}"),
        }

        validate("SELECT ((((((((((1))))))))))").expect("shallow nesting still parses");
    });
}

/// `structural_count` is the figure the gate decides on, so it is pinned
/// exactly, not just in relation to the bound: whitespace is free, every
/// token costs one however long it is, and a comment costs none.
///
/// `SELECT 1` is two tokens, not seven characters. The unit is the token
/// because the invariant the guard rests on is about tokens, and counting
/// characters made the bound depend on whether an author quoted an
/// identifier.
#[test]
fn the_structural_count_is_exact() {
    assert_eq!(structural_count("SELECT 1"), 2);
    assert_eq!(structural_count("SELECT   \n  1"), 2);
    assert_eq!(structural_count("SELECT 'aaaaaaaaaaaaaaaaaaaa'"), 2);
    assert_eq!(structural_count("SELECT 1 -- aaaaaaaaaaaaaaaaaaaa\n"), 2);
    assert_eq!(structural_count("SELECT 1 /* aaaa */"), 2);
    assert_eq!(structural_count("SELECT 1+1"), 4);

    // A long bare identifier is one token, exactly as its quoted form is.
    assert_eq!(structural_count("SELECT ResolutionWidth"), 2);
    assert_eq!(structural_count(r#"SELECT "ResolutionWidth""#), 2);

    // A hint comment is NOT a comment to this dialect, so its body counts:
    // SELECT, 1, then each of `/ * ! + 1 * /`. The same text as an ordinary
    // `/* +1 */` comment costs 2, which is the whole difference.
    assert_eq!(structural_count("SELECT 1 /*! +1 */"), 9);
    assert_eq!(structural_count("SELECT 1 /* +1 */"), 2);
}

/// The guard counts structure, not characters: a statement whose string
/// literal holds hundreds of thousands of operator characters is ordinary SQL
/// and is accepted, because a literal is one token to the parser and adds no
/// tree level however long it is.
#[test]
fn operator_characters_inside_a_string_literal_do_not_count() {
    on_worker_stack(|| {
        let payload = "+1".repeat(500_000);
        let sql = format!("SELECT ts FROM logs WHERE body = '{payload}'");
        validate(&sql).expect("a long literal is not structure");
    });
}

/// A comment is not a token at all, so a statement carrying a very long
/// comment is accepted on the strength of its actual structure.
#[test]
fn a_long_comment_does_not_count() {
    on_worker_stack(|| {
        let payload = "+1".repeat(500_000);
        validate(&format!("SELECT ts FROM logs -- {payload}\n"))
            .expect("a line comment is not structure");
        validate(&format!("SELECT ts FROM logs /* {payload} */"))
            .expect("a block comment is not structure");
    });
}

/// The boundary itself, through `validate` rather than through the guard's own
/// unit tests: a statement at the bound passes the gate and is accepted, and
/// two characters more are refused by the gate with the exact counts.
///
/// The padding is a flat projection list (`SELECT 1,1,1,...`), not an operator
/// chain, on purpose: an operator chain long enough to reach the bound builds
/// a tree deep enough to abort an unoptimized build on this 2 MiB stack, which
/// would take the whole test binary down. The gate counts the two shapes
/// identically at two characters apiece, so the boundary this pins is the same
/// one.
#[test]
fn the_gate_fires_one_character_past_the_bound() {
    on_worker_stack(|| {
        // `SELECT` and `1` are one token each, then two for every `,1`.
        // div_ceil so the count lands exactly on the bound rather than one
        // under it: with plain division an odd remainder left `at_bound` at
        // 999 and the message below overstated what the case pinned.
        let items = (MAX_STATEMENT_COMPLEXITY - 2).div_ceil(2);
        let at_bound = format!("SELECT 1{}", ",1".repeat(items));
        assert_eq!(
            ravel_sql::complexity_guard::structural_count(&at_bound),
            MAX_STATEMENT_COMPLEXITY,
            "the probe must sit exactly on the bound for this case to pin it"
        );
        validate(&at_bound).expect("a statement at the bound passes the gate");

        let over = format!("{at_bound},1");
        match validate(&over).expect_err("one item more is refused") {
            ValidationError::TooComplex(too_complex) => {
                assert_eq!(too_complex.count, MAX_STATEMENT_COMPLEXITY + 1);
                assert_eq!(too_complex.max, MAX_STATEMENT_COMPLEXITY);
            }
            other => panic!("expected TooComplex, got {other:?}"),
        }
    });
}
