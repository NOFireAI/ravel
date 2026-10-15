//! Name resolution and the column set each stage leaves (ADR-2887 D9, D10).

use ravel_sql::esql::{self, EsqlError, PrepareContext};
use ravel_sql::{DeclaredColumn, DeclaredType};

use crate::support::{columns, context, fixture, prepare, rows, run};

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

/// A name a `DROP` removed or a `RENAME` renamed away does not fall through
/// to an attribute key; neither does any attribute key after `KEEP` or
/// `STATS`. Before them, an unknown name is still an attribute key.
#[test]
fn a_removed_name_does_not_resolve_as_an_attribute_key() {
    assert_eq!(
        verification(r#"FROM logs | EVAL x = 1 | DROP x | WHERE x == "a""#),
        ("Unknown column [x]".to_string(), 1, 41)
    );
    assert_eq!(
        verification(r#"FROM logs | EVAL x = 1 | RENAME x AS y | WHERE x == "a""#),
        ("Unknown column [x]".to_string(), 1, 48)
    );
    // An attribute key renamed away or dropped stays gone.
    assert_eq!(
        verification(r#"FROM logs | RENAME service AS s | WHERE service == "a""#),
        ("Unknown column [service]".to_string(), 1, 41)
    );
    assert_eq!(
        verification(r#"FROM logs | DROP service | WHERE service == "a""#),
        ("Unknown column [service]".to_string(), 1, 34)
    );
    // A declared column dropped is not read back as an attribute key.
    assert_eq!(
        verification("FROM logs | DROP region | KEEP region"),
        ("Unknown column [region]".to_string(), 1, 32)
    );
    assert_eq!(
        verification(r#"FROM logs | KEEP message | WHERE service == "a""#),
        ("Unknown column [service]".to_string(), 1, 34)
    );
    assert_eq!(
        verification(r#"FROM logs | STATS c = COUNT(*) | WHERE service == "a""#),
        ("Unknown column [service]".to_string(), 1, 40)
    );

    // Before the first KEEP or STATS an unknown name is an attribute key,
    // and a name defined again after a DROP is visible again.
    assert_eq!(
        typed(r#"FROM logs | EVAL x = 1 | DROP x | EVAL y = service | KEEP y"#),
        [("y".to_string(), "keyword".to_string())]
    );
    assert_eq!(
        typed("FROM logs | EVAL x = 1 | DROP x | EVAL x = 2 | KEEP x"),
        [("x".to_string(), "long".to_string())]
    );
}

#[tokio::test]
async fn a_dropped_name_reads_no_attribute_at_run_time() {
    let fixture = fixture().await;
    let outcome = run(
        &fixture,
        r#"FROM logs | SORT @timestamp | EVAL service = "z" | DROP service | EVAL s = service | KEEP s"#,
    )
    .await;
    match outcome {
        Err(EsqlError::Verification { message, .. }) => {
            assert_eq!(message, "Unknown column [service]")
        }
        other => panic!("expected a verification error, got {other:?}"),
    }
}

/// A declared column keyed like a native or reserved name is not bound: the
/// name reads the native column, and the declared column is not visible.
#[test]
fn a_declared_column_colliding_with_a_reserved_name_yields_to_it() {
    let ctx = PrepareContext {
        declared: vec![
            DeclaredColumn::new("severity_text", DeclaredType::I64),
            DeclaredColumn::new("ts", DeclaredType::Bool),
            DeclaredColumn::new("attrs", DeclaredType::Str),
            DeclaredColumn::new("dur", DeclaredType::I64),
        ],
        ..context()
    };
    let prepared = esql::prepare(
        "FROM logs | KEEP severity_text, dur",
        &ravel_esql::Limits::default(),
        &ctx,
    )
    .expect("prepares");
    let columns: Vec<(String, String)> = prepared
        .columns()
        .iter()
        .map(|column| (column.name.clone(), column.ty.name().to_string()))
        .collect();
    assert_eq!(
        columns,
        [("severity_text", "keyword"), ("dur", "long")]
            .map(|(n, t)| (n.to_string(), t.to_string()))
    );
    let all = esql::prepare("FROM logs", &ravel_esql::Limits::default(), &ctx).expect("prepares");
    let names: Vec<&str> = all.columns().iter().map(|c| c.name.as_str()).collect();
    assert_eq!(
        names,
        [
            "@timestamp",
            "observed_ts",
            "severity_num",
            "severity_text",
            "message",
            "trace_id",
            "span_id",
            "flags",
            "dur",
        ]
    );
    match esql::prepare(
        "FROM logs | KEEP attrs",
        &ravel_esql::Limits::default(),
        &ctx,
    ) {
        Err(EsqlError::Verification { message, .. }) => {
            assert_eq!(message, "Unknown column [attrs]")
        }
        other => panic!("expected a verification error, got {other:?}"),
    }
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

/// Over a null-typed argument SUM, AVG, MIN and MAX return null and COUNT
/// returns 0: one row without BY, one per group with it.
#[tokio::test]
async fn stats_over_null_returns_a_row_per_group() {
    let fixture = fixture().await;
    for (function, value, ty) in [
        ("SUM", "null", "null"),
        ("AVG", "null", "null"),
        ("MIN", "null", "null"),
        ("MAX", "null", "null"),
        ("COUNT", "0", "long"),
    ] {
        let source = format!("FROM logs | STATS s = {function}(null)");
        let outcome = run(&fixture, &source)
            .await
            .unwrap_or_else(|e| panic!("{source}: {e}"));
        assert_eq!(
            columns(&outcome),
            vec![("s".to_string(), ty.to_string())],
            "{source}"
        );
        assert_eq!(rows(&outcome), vec![vec![value]], "{source}");

        let source = format!("FROM logs | STATS s = {function}(null) BY service | SORT service");
        let outcome = run(&fixture, &source)
            .await
            .unwrap_or_else(|e| panic!("{source}: {e}"));
        assert_eq!(
            rows(&outcome),
            vec![vec![value, "api"], vec![value, "web"], vec![value, "null"],],
            "{source}"
        );
    }
}

/// Without BY, a STATS field that calls no aggregate function is refused at
/// its own position, named by its text and its place among the fields.
#[test]
fn a_stats_field_without_an_aggregate_is_refused_without_by() {
    assert_eq!(
        verification("FROM logs | STATS s = 1"),
        (
            "STATS field 1 [s = 1] calls no aggregate function, which a STATS without BY requires"
                .to_string(),
            1,
            19
        )
    );
    assert_eq!(
        verification("FROM logs | STATS c = COUNT(*), n = NOW()"),
        (
            "STATS field 2 [n = NOW()] calls no aggregate function, which a STATS without BY requires"
                .to_string(),
            1,
            33
        )
    );
    assert_eq!(
        verification("FROM logs | STATS 1 + 2").0,
        "STATS field 1 [1 + 2] calls no aggregate function, which a STATS without BY requires"
    );
    // An aggregate over a null is still an aggregate.
    assert_eq!(names("FROM logs | STATS s = SUM(null)"), vec!["s"]);
}
