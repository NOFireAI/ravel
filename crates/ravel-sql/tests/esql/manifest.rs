//! The binder and executor states the release manifest
//! (`crates/ravel-esql/manifest/esql-9.5.toml`) records for each entry,
//! checked against `esql::prepare` and `esql::execute` on the fixture, and
//! the divergences the manifest cites this file for.

use ravel_esql::manifest::{self, Entry, Manifest, State};
use ravel_sql::esql::{EsqlError, EsqlOutcome, MAX_BOUND_NODES};

use crate::support::{
    SHADOWED_MESSAGE, columns, fixture, prepare, rows, run, run_prepared, shadow_fixture,
};

const MANIFEST: &str = include_str!("../../../ravel-esql/manifest/esql-9.5.toml");

fn load() -> Manifest {
    manifest::load(MANIFEST).expect("the manifest loads and validates")
}

fn describe(entry: &Entry) -> String {
    format!("{} {:?} ({:?})", entry.kind, entry.name, entry.example)
}

/// A refusal the binder or the parser names: a construct this lowering does
/// not execute, a statement that does not verify, or syntax the parser
/// recognizes and refuses.
fn is_typed_refusal(error: &EsqlError) -> bool {
    matches!(
        error,
        EsqlError::Unsupported { .. }
            | EsqlError::Verification { .. }
            | EsqlError::Parse(ravel_esql::Error::UnsupportedSyntax { .. })
    )
}

/// Every entry marked binder-supported prepares, and every entry marked
/// executor-supported also runs on the fixture.
#[tokio::test]
async fn every_executor_supported_entry_runs() {
    let manifest = load();
    let fixture = fixture().await;
    let mut wrong = Vec::new();
    let mut ran = 0;
    for entry in &manifest.entries {
        if entry.binder != State::Supported {
            continue;
        }
        let prepared = match prepare(&entry.example) {
            Ok(prepared) => prepared,
            Err(error) => {
                wrong.push(format!(
                    "{} is binder-supported but does not prepare: {error:?}",
                    describe(entry)
                ));
                continue;
            }
        };
        if entry.executor != State::Supported {
            continue;
        }
        match run_prepared(&fixture, &prepared).await {
            Ok(_) => ran += 1,
            Err(error) => wrong.push(format!(
                "{} is executor-supported but does not run: {error:?}",
                describe(entry)
            )),
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
    assert_eq!(ran, manifest.execution_compatibility().numerator);
}

/// Every entry marked refused at a layer is refused there with a typed
/// error, and every entry marked binder-absent fails to parse.
#[tokio::test]
async fn every_refused_entry_is_refused_with_a_typed_error() {
    let manifest = load();
    let fixture = fixture().await;
    let mut wrong = Vec::new();
    let mut refused = 0;
    for entry in &manifest.entries {
        let prepared = prepare(&entry.example);
        let holds = match (entry.parser, entry.binder, &prepared) {
            (State::Refused, _, Err(EsqlError::Parse(error))) => {
                matches!(error, ravel_esql::Error::UnsupportedSyntax { .. })
            }
            (State::Supported, State::Absent, _) => false,
            (_, State::Absent, Err(EsqlError::Parse(_))) => true,
            (State::Supported, State::Refused, Err(error)) => {
                matches!(
                    error,
                    EsqlError::Unsupported { .. } | EsqlError::Verification { .. }
                )
            }
            (State::Supported, State::Supported, Ok(prepared))
                if entry.executor == State::Refused =>
            {
                match run_prepared(&fixture, prepared).await {
                    Err(error) => is_typed_refusal(&error),
                    Ok(_) => false,
                }
            }
            (_, State::Supported, Ok(_)) if entry.executor == State::Supported => continue,
            _ => false,
        };
        if holds {
            refused += 1;
        } else {
            wrong.push(format!(
                "{} is marked parser = {}, binder = {}, executor = {} but prepares to {:?}",
                describe(entry),
                entry.parser,
                entry.binder,
                entry.executor,
                prepared.as_ref().map(|_| "a statement")
            ));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
    assert_eq!(
        refused,
        manifest
            .entries
            .iter()
            .filter(|e| e.executor != State::Supported)
            .count()
    );
}

/// `message` and `@timestamp` name the record's body and timestamp even
/// when the record carries attributes with those keys.
#[tokio::test]
async fn timestamp_and_message_shadow_attributes_of_the_same_name() {
    let fixture = shadow_fixture().await;
    let outcome = run(&fixture, "FROM logs | KEEP message, @timestamp")
        .await
        .expect("runs");
    assert_eq!(
        columns(&outcome),
        vec![
            ("message".to_string(), "keyword".to_string()),
            ("@timestamp".to_string(), "date_nanos".to_string())
        ]
    );
    let rows = rows(&outcome);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], "a");
    assert_ne!(rows[0][0], SHADOWED_MESSAGE);
    assert_ne!(rows[0][1], "not a time");
}

/// The manifest records no field-count divergence: one ROW or EVAL command
/// of 16,000 fields, and a RENAME of 8,000, which is as many as the default
/// 65,536-token limit admits, bind within the node budget.
#[test]
fn a_command_of_16000_fields_binds_within_the_node_budget() {
    let list = |n: usize, field: &dyn Fn(usize) -> String| {
        (0..n).map(field).collect::<Vec<_>>().join(", ")
    };
    let assign = |i: usize| format!("x{i} = 1");
    let rename = |i: usize| format!("x{i} AS y{i}");
    for source in [
        format!("ROW {}", list(16_000, &assign)),
        format!("FROM logs | EVAL {}", list(16_000, &assign)),
        format!(
            "ROW {} | RENAME {}",
            list(8_000, &assign),
            list(8_000, &rename)
        ),
    ] {
        let tokens = ravel_esql::lexer::tokenize(&source, &ravel_esql::Limits::default())
            .expect("lexes")
            .len();
        assert!(tokens > 60_000, "{tokens} tokens");
        if let Err(error) = prepare(&source) {
            panic!("{tokens} tokens: {error:?}");
        }
    }
    assert_eq!(MAX_BOUND_NODES, 1_048_576);
}

#[tokio::test]
async fn an_integer_literal_is_typed_long() {
    let outcome = run(&fixture().await, "ROW a = 1").await.expect("runs");
    assert_eq!(
        columns(&outcome),
        vec![("a".to_string(), "long".to_string())]
    );
    assert_eq!(rows(&outcome), vec![vec!["1"]]);
}

/// An overflow and a division by zero read null, and the outcome has no
/// field that could carry a warning about them.
#[tokio::test]
async fn an_overflow_gives_null_without_a_warning() {
    let outcome = run(
        &fixture().await,
        "ROW a = 9223372036854775807 + 1, b = 5 / 0",
    )
    .await
    .expect("runs");
    assert_eq!(rows(&outcome), vec![vec!["null", "null"]]);
    let EsqlOutcome {
        columns: _,
        run: _,
        clamped_limit,
    } = outcome;
    assert_eq!(clamped_limit, None);
}
