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

use ravel_sql::{MAX_STATEMENT_COMPLEXITY, ValidationError, validate};

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
        // 7 characters for `SELECT1`, then two for each `,1`.
        let items = (MAX_STATEMENT_COMPLEXITY - 7) / 2;
        let at_bound = format!("SELECT 1{}", ",1".repeat(items));
        assert_eq!(at_bound.replace(' ', "").len(), 7 + 2 * items);
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
