//! The one-table rule of the MCP SQL tools (ADR-2904 decision 9 and its
//! #2915 amendment), through `ravel_sql::refuse_several_tables` as a caller
//! outside this crate reaches it.
//!
//! This file is a member of the `it` binary, which `cargo test -p ravel-sql`
//! builds under default features: a function that exists only behind
//! `flight-sql` does not compile here.

#![allow(clippy::expect_used)]

use ravel_sql::{SqlError, refuse_several_tables};

fn refused(sql: &str) -> SqlError {
    refuse_several_tables(sql).expect_err(sql)
}

/// The function takes the statement text and nothing else: no store, no
/// catalog and no executor, so it has nothing to issue a store or catalog
/// request through. That is a property of the signature, and this pins it.
///
/// FLIP: adding a store, catalog or executor parameter to
/// `refuse_several_tables` fails this to compile. Putting
/// `#[cfg(feature = "flight-sql")]` on the function or on its re-export fails
/// the whole file to compile under default features.
#[test]
fn takes_statement_text_alone_under_default_features() {
    let check: fn(&str) -> Result<(), SqlError> = refuse_several_tables;
    check("SELECT ts FROM logs").expect("one table");
}

/// Two or more signal tables are `CrossSignalQuery`, wherever the second one
/// sits: a join, a subquery in `WHERE`, a scalar subquery, a CTE body.
///
/// FLIP (subquery): a table walk that stops at a subquery expression misses
/// `spans` in the `IN (SELECT ...)` and scalar-subquery cases and returns Ok.
/// FLIP (variant): dropping the `refuse_cross_signal` call returns
/// `OneTablePerStatement` for every case here.
#[test]
fn two_signal_tables_are_cross_signal_query() {
    for sql in [
        "SELECT * FROM logs JOIN spans ON logs.trace_id = spans.trace_id",
        "SELECT * FROM samples, logs, spans",
        "SELECT body FROM logs WHERE trace_id IN (SELECT trace_id FROM spans)",
        "SELECT body, (SELECT count(*) FROM spans) AS n FROM logs",
        "WITH s AS (SELECT trace_id FROM spans) \
         SELECT body FROM logs JOIN s ON logs.trace_id = s.trace_id",
        "SELECT ts FROM alerts UNION ALL SELECT ts FROM audit",
    ] {
        let err = refused(sql);
        assert!(
            matches!(err, SqlError::CrossSignalQuery),
            "expected CrossSignalQuery for {sql:?}, got {err:?}"
        );
    }
}

/// A second table that is not a signal table is `OneTablePerStatement`: a
/// Parquet table beside a signal table, and two non-signal tables. Its text
/// names no table, unlike `CrossSignalQuery`'s, which names the five signal
/// tables and would misdescribe these.
///
/// FLIP (signal tables only): a check that counts signal tables alone and
/// skips the `names_several_tables` test returns Ok for every case here.
/// FLIP (subquery): a table walk that stops at a subquery expression misses
/// `hits` in the `IN (SELECT ...)` case and returns Ok.
#[test]
fn a_second_table_of_another_kind_is_one_table_per_statement() {
    for sql in [
        "SELECT * FROM logs JOIN hits ON logs.trace_id = hits.trace_id",
        "SELECT * FROM hits JOIN orders ON hits.id = orders.id",
        "SELECT body FROM logs WHERE trace_id IN (SELECT trace_id FROM hits)",
        "SELECT id FROM hits UNION ALL SELECT id FROM orders",
    ] {
        let err = refused(sql);
        assert!(
            matches!(err, SqlError::OneTablePerStatement),
            "expected OneTablePerStatement for {sql:?}, got {err:?}"
        );
        for table in ["logs", "hits", "orders", "samples"] {
            assert!(
                !err.client_message().contains(table),
                "the message must echo no table name: {err}"
            );
        }
    }
}

/// One table, no table, one table read twice, and a CTE over one table are
/// all admitted. A CTE name is not a table, including one spelled like a
/// signal table.
///
/// FLIP (CTE name counted): dropping the CTE-name subtraction in
/// `referenced_base_tables` makes `recent` a second table, and the CTE cases
/// are refused.
#[test]
fn one_table_or_none_is_admitted() {
    for sql in [
        "SELECT ts FROM logs",
        "SELECT * FROM hits",
        "SELECT 1",
        "SELECT a.body FROM logs a JOIN logs b ON a.trace_id = b.trace_id",
        "WITH recent AS (SELECT * FROM logs) SELECT count(*) FROM recent",
        "WITH recent AS (SELECT * FROM hits) SELECT * FROM recent",
        "WITH logs AS (SELECT value FROM samples) SELECT count(*) FROM logs",
        "SELECT body FROM logs WHERE trace_id IN (SELECT trace_id FROM logs)",
    ] {
        refuse_several_tables(sql)
            .unwrap_or_else(|err| panic!("{sql:?} names one table or none: {err:?}"));
    }
}
