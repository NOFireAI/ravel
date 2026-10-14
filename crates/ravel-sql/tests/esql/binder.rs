//! Name resolution and the column set each stage leaves (ADR-2887 D9, D10).

use ravel_sql::esql::EsqlError;

use crate::support::{fixture, prepare, rows, run};

fn names(source: &str) -> Vec<String> {
    prepare(source)
        .unwrap_or_else(|e| panic!("{source}: {e}"))
        .columns()
        .iter()
        .map(|column| column.name.clone())
        .collect()
}

fn typed(source: &str) -> Vec<(String, String)> {
    prepare(source)
        .unwrap_or_else(|e| panic!("{source}: {e}"))
        .columns()
        .iter()
        .map(|column| (column.name.clone(), column.ty.name().to_string()))
        .collect()
}

/// The verification message and its 1-based line and column.
fn verification(source: &str) -> (String, usize, usize) {
    match prepare(source) {
        Err(EsqlError::Verification {
            message,
            line,
            column,
        }) => (message, line, column),
        other => panic!("{source}: expected a verification error, got {other:?}"),
    }
}

fn unsupported(source: &str) -> (String, usize, usize) {
    match prepare(source) {
        Err(EsqlError::Unsupported { name, line, column }) => (name, line, column),
        other => panic!("{source}: expected an unsupported error, got {other:?}"),
    }
}

#[test]
fn columns_dropped_by_stats_do_not_resolve_afterwards() {
    assert_eq!(
        verification("FROM logs | STATS c = COUNT(*) BY service | WHERE message == \"a\""),
        ("Unknown column [message]".to_string(), 1, 51)
    );
    // Attribute keys no longer resolve either.
    assert_eq!(
        verification("FROM logs | STATS c = COUNT(*) | EVAL x = service").0,
        "Unknown column [service]"
    );
    assert_eq!(
        names("FROM logs | STATS c = COUNT(*) BY service | EVAL d = c + 1"),
        ["c", "service", "d"]
    );
}

#[test]
fn default_projection_is_the_native_columns_then_the_declared_ones() {
    assert_eq!(
        typed("FROM logs"),
        [
            ("@timestamp", "date_nanos"),
            ("observed_ts", "date_nanos"),
            ("severity_num", "long"),
            ("severity_text", "keyword"),
            ("message", "keyword"),
            ("trace_id", "keyword"),
            ("span_id", "keyword"),
            ("flags", "long"),
            ("dur", "long"),
            ("region", "keyword"),
        ]
        .map(|(n, t)| (n.to_string(), t.to_string()))
    );
    assert_eq!(
        verification("FROM logs | KEEP attrs"),
        ("Unknown column [attrs]".to_string(), 1, 18)
    );
}

#[test]
fn keep_and_drop_expand_wildcards_and_fix_column_order() {
    assert_eq!(
        names("FROM logs | KEEP s*, @timestamp"),
        ["severity_num", "severity_text", "span_id", "@timestamp"]
    );
    // A name is more specific than a wildcard, wherever it appears.
    assert_eq!(
        names("FROM logs | KEEP *, message")
            .last()
            .map(String::as_str),
        Some("message")
    );
    assert_eq!(
        names("FROM logs | KEEP message, *")
            .first()
            .map(String::as_str),
        Some("message")
    );
    assert_eq!(
        names("FROM logs | DROP *_id, observed_ts"),
        [
            "@timestamp",
            "severity_num",
            "severity_text",
            "message",
            "flags",
            "dur",
            "region"
        ]
    );
    assert_eq!(
        verification("FROM logs | KEEP message | KEEP x*").0,
        "No matches found for pattern [x*]"
    );
    assert_eq!(
        verification("FROM logs | KEEP message | DROP message").0,
        "Removing all fields is not allowed [DROP message]"
    );
}

#[tokio::test]
async fn eval_replacement_moves_the_column_to_the_end() {
    let source = "FROM logs | KEEP message, severity_num | EVAL message = \"x\" | SORT severity_num | LIMIT 1";
    assert_eq!(names(source), ["severity_num", "message"]);
    let fixture = fixture().await;
    let outcome = run(&fixture, source).await.expect("runs");
    assert_eq!(rows(&outcome), vec![vec!["5", "x"]]);
    // Each field of one EVAL sees the fields before it.
    assert_eq!(
        names("FROM logs | KEEP dur | EVAL a = dur + 1, dur = a * 2, a = 0"),
        ["dur", "a"]
    );
}

#[tokio::test]
async fn rename_onto_an_existing_name_replaces_that_column() {
    let source =
        "FROM logs | KEEP message, severity_num, flags | RENAME message AS flags | SORT flags";
    assert_eq!(
        typed(source),
        [("flags", "keyword"), ("severity_num", "long")]
            .map(|(n, t)| (n.to_string(), t.to_string()))
    );
    let fixture = fixture().await;
    let outcome = run(&fixture, &format!("{source} | LIMIT 1"))
        .await
        .expect("runs");
    assert_eq!(rows(&outcome), vec![vec!["a", "9"]]);
}

#[tokio::test]
async fn timestamp_and_message_map_to_ts_and_body() {
    let fixture = fixture().await;
    let outcome = run(
        &fixture,
        "FROM logs | WHERE body == \"a\" AND ts IS NOT NULL | KEEP @timestamp, message, trace_id",
    )
    .await
    .expect("runs");
    assert_eq!(
        rows(&outcome),
        vec![vec![
            "2023-11-14T22:13:00".to_string(),
            "a".to_string(),
            "ab".repeat(16),
        ]]
    );
    // `ts` and `body` name the same columns, under their ES|QL names.
    assert_eq!(
        names("FROM logs | KEEP ts, body"),
        ["@timestamp", "message"]
    );
}

#[tokio::test]
async fn a_declared_column_has_its_declared_type() {
    let source = "FROM logs | WHERE message == \"a\" | KEEP dur, region | EVAL d2 = dur * 2";
    assert_eq!(
        typed(source),
        [("dur", "long"), ("region", "keyword"), ("d2", "long")]
            .map(|(n, t)| (n.to_string(), t.to_string()))
    );
    let fixture = fixture().await;
    let outcome = run(&fixture, source).await.expect("runs");
    assert_eq!(rows(&outcome), vec![vec!["10", "eu", "20"]]);
}

#[tokio::test]
async fn an_attribute_reads_null_when_absent() {
    let source = "FROM logs | EVAL s = service, n = nope | KEEP message, s, n | SORT message";
    assert_eq!(
        typed(source),
        [("message", "keyword"), ("s", "keyword"), ("n", "keyword")]
            .map(|(n, t)| (n.to_string(), t.to_string()))
    );
    let fixture = fixture().await;
    let outcome = run(&fixture, source).await.expect("runs");
    assert_eq!(
        rows(&outcome),
        vec![
            vec!["a", "api", "null"],
            vec!["b", "api", "null"],
            vec!["c", "web", "null"],
            vec!["d", "api", "null"],
            vec!["e", "web", "null"],
            vec!["f", "null", "null"],
        ]
    );
    // KEEP of an attribute key materializes it in place.
    assert_eq!(
        names("FROM logs | KEEP service, message"),
        ["service", "message"]
    );
}

#[test]
fn from_of_another_source_is_refused_with_its_span() {
    assert_eq!(
        unsupported("FROM metrics"),
        ("source [metrics]".to_string(), 1, 6)
    );
    assert_eq!(unsupported("FROM logs*").0, "source pattern [logs*]");
    assert_eq!(
        unsupported("FROM logs, spans"),
        (
            "FROM over more than one source [logs, spans]".to_string(),
            1,
            6
        )
    );
    assert_eq!(
        unsupported("FROM remote:logs").0,
        "remote source [remote:logs]"
    );
    assert_eq!(
        unsupported("FROM logs METADATA _id"),
        ("METADATA [_id]".to_string(), 1, 11)
    );
}

#[test]
fn unsupported_forms_name_themselves() {
    assert_eq!(
        unsupported("FROM logs | WHERE message LIKE \"a*\"").0,
        "LIKE"
    );
    assert_eq!(
        unsupported("FROM logs | EVAL x = TO_UPPER(message)"),
        ("function [TO_UPPER]".to_string(), 1, 22)
    );
    assert_eq!(
        unsupported("FROM logs | WHERE @timestamp > NOW() - 1 month").0,
        "calendar interval [1 month]"
    );
}

#[test]
fn aggregates_bind_only_in_stats() {
    assert_eq!(
        verification("FROM logs | EVAL x = COUNT(*)").0,
        "aggregate function [COUNT(*)] not allowed outside STATS command"
    );
    assert_eq!(
        verification("FROM logs | STATS x = message").0,
        "column [message] must appear in the STATS BY clause or be used in an aggregate function"
    );
}
