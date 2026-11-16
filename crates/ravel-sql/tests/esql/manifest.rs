//! The binder and executor states the release manifest
//! (`crates/ravel-esql/manifest/esql-9.5.toml`) records for each entry,
//! checked against `esql::prepare` and `esql::execute` on the fixture, the
//! answers it records against the answers Ravel gives, and the divergences
//! the manifest cites this file for.

use datafusion::arrow::array::{Array, ArrayRef, TimestampNanosecondArray};
use datafusion::arrow::util::display::array_value_to_string;
use ravel_esql::ast::{BinaryOp, Command, Expr, Query};
use ravel_esql::manifest::{self, Answer, Cell, Column, Entry, Expected, Manifest, State};
use ravel_esql::{Limits, Span};
use ravel_sql::esql::{EsqlError, EsqlOutcome, EsqlType, MAX_BOUND_NODES, PreparedEsql};

use crate::support::{
    BASE, Fixture, NOW, SECOND, SHADOWED_MESSAGE, columns, fixture, prepare, rows, run,
    run_prepared, shadow_fixture,
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

/// `ns` nanoseconds after the epoch as the release renders a `date_nanos`.
fn iso_nanos(ns: i64) -> String {
    const DAY: i64 = 86_400 * SECOND;
    let (days, rest) = (ns.div_euclid(DAY), ns.rem_euclid(DAY));
    // Days to a proleptic Gregorian civil date (Howard Hinnant's algorithm).
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    let secs = rest / SECOND;
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:09}Z",
        secs / 3_600,
        secs / 60 % 60,
        secs % 60,
        rest % SECOND
    )
}

fn cell(ty: &EsqlType, array: &ArrayRef, row: usize) -> Cell {
    // A `NullArray` has no validity buffer; its nulls are logical.
    if array
        .logical_nulls()
        .is_some_and(|nulls| nulls.is_null(row))
    {
        return Cell::Null;
    }
    let text = || array_value_to_string(array, row).expect("cell");
    match ty {
        EsqlType::Keyword => Cell::Text(text()),
        EsqlType::Long => Cell::Long(text().parse().expect("a long")),
        EsqlType::Double => Cell::Double(text().parse().expect("a double")),
        EsqlType::Boolean => Cell::Bool(text().parse().expect("a boolean")),
        EsqlType::DateNanos => Cell::Text(iso_nanos(
            array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("nanosecond timestamps")
                .value(row),
        )),
        EsqlType::Null => Cell::Null,
    }
}

/// Ravel's answer in the manifest's terms. The outcome has no field that
/// carries a warning (`an_overflow_gives_null_without_a_warning` destructures
/// it), so the warnings are always empty. Each batch must carry the
/// outcome's columns by name, in order, before cells pair by position.
fn answer(outcome: &EsqlOutcome) -> Answer {
    let mut rows = Vec::new();
    let names: Vec<&str> = outcome.columns.iter().map(|c| c.name.as_str()).collect();
    for batch in outcome.output().batches() {
        let schema = batch.schema();
        let batch_names: Vec<&str> = schema.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            batch_names, names,
            "a batch's columns are not the outcome's columns"
        );
        for row in 0..batch.num_rows() {
            rows.push(
                outcome
                    .columns
                    .iter()
                    .zip(batch.columns())
                    .map(|(column, array)| cell(&column.ty, array, row))
                    .collect(),
            );
        }
    }
    Answer {
        columns: outcome
            .columns
            .iter()
            .map(|column| Column {
                name: column.name.clone(),
                ty: column.ty.name().to_string(),
            })
            .collect(),
        rows,
        warnings: Vec::new(),
    }
}

/// Every entry marked binder-supported prepares, and every entry marked
/// executor-supported runs on the fixture and gives the answer the manifest
/// records for the release, column names, types, rows and warnings. An entry
/// that names a divergence gives that answer once exactly that divergence is
/// undone, and must not give it without the undo.
#[tokio::test]
async fn every_executor_supported_entry_gives_the_release_answer() {
    let manifest = load();
    let fixture = fixture().await;
    let mut wrong = Vec::new();
    let (mut compatible, mut divergent) = (0, 0);
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
        let outcome = match run_prepared(&fixture, &prepared).await {
            Ok(outcome) => outcome,
            Err(error) => {
                wrong.push(format!(
                    "{} is executor-supported but does not run: {error:?}",
                    describe(entry)
                ));
                continue;
            }
        };
        let Some(expected) = &entry.expected else {
            wrong.push(format!("{} records no answer", describe(entry)));
            continue;
        };
        let actual = answer(&outcome);
        match &entry.divergence {
            None => match expected.compare(&actual) {
                Ok(()) => compatible += 1,
                Err(difference) => wrong.push(format!(
                    "{} differs from the release with no named divergence: {difference}",
                    describe(entry)
                )),
            },
            Some(id) => match expected.compare_divergent(id, &actual) {
                Ok(()) => divergent += 1,
                Err(difference) => wrong.push(format!("{}: {difference}", describe(entry))),
            },
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
    assert_eq!(compatible, manifest.execution_compatibility().numerator);
    assert_eq!(divergent, manifest.divergent().numerator);
}

/// The text of `example` from 1-based `line` and `column` on.
fn text_at(example: &str, line: usize, column: usize) -> String {
    example
        .lines()
        .nth(line.saturating_sub(1))
        .map(|text| text.chars().skip(column.saturating_sub(1)).collect())
        .unwrap_or_default()
}

fn is_word(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// Whether `marker` occurs in `text` as a whole token: no word character
/// joins it on either side where its own edge is a word character, so
/// `LIKE` is not found inside `RLIKE`.
fn names_token(text: &str, marker: &str) -> bool {
    let starts_word = marker.chars().next().is_some_and(is_word);
    let ends_word = marker.chars().last().is_some_and(is_word);
    text.match_indices(marker).any(|(at, _)| {
        let before = text[..at].chars().next_back();
        let after = text[at + marker.len()..].chars().next();
        !(starts_word && before.is_some_and(is_word)) && !(ends_word && after.is_some_and(is_word))
    })
}

/// Whether `text` starts with `marker` as a whole token.
fn starts_with_token(text: &str, marker: &str) -> bool {
    text.strip_prefix(marker).is_some_and(|rest| {
        !(marker.chars().last().is_some_and(is_word) && rest.chars().next().is_some_and(is_word))
    })
}

/// The binder's name for an expression kind it has no binding for. It names
/// no construct, so a refusal by this name is matched by where the refused
/// expression lies, never by its name.
const ANY_EXPRESSION: &str = "this expression";

/// Every expression in the WHERE conditions of `query`, through the
/// operands of unary, binary and IN subquery expressions.
fn where_expressions(query: &Query) -> Vec<&Expr> {
    fn push<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
        out.push(expr);
        match expr {
            Expr::Unary(unary) => push(&unary.operand, out),
            Expr::Binary(binary) => {
                push(&binary.left, out);
                push(&binary.right, out);
            }
            Expr::InSubquery(in_subquery) => push(&in_subquery.expr, out),
            _ => {}
        }
    }
    let mut out = Vec::new();
    for command in &query.processing {
        if let Command::Where(filter) = command {
            push(&filter.condition, &mut out);
        }
    }
    out
}

/// `example` with the text `span` covers replaced by `null`.
fn nulled(example: &str, span: Span) -> String {
    format!("{}null{}", &example[..span.start], &example[span.end..])
}

/// Whether an `ANY_EXPRESSION` refusal at `line`:`column` of `example` is
/// the refusal of the one expression the entry named `owner` is about, and
/// of no other expression starting where it starts. The error carries only
/// where the refused expression starts, so its end is pinned by two
/// replacements: replacing the expression with `null` must remove the
/// refusal, which rules out an expression enclosing it, and replacing its
/// operand that starts there with `null` must keep it, which rules out that
/// operand and anything inside it.
fn refuses_own_expression(
    owner: &str,
    example: &str,
    line: usize,
    column: usize,
) -> Result<(), String> {
    let is_own: fn(&Expr) -> bool = match owner {
        "IN subquery" => |e| matches!(e, Expr::InSubquery(_)),
        "AND" => |e| matches!(e, Expr::Binary(b) if b.op == BinaryOp::And),
        _ => {
            return Err(format!(
                "{owner:?} names no expression kind a {ANY_EXPRESSION:?} refusal is placed against"
            ));
        }
    };
    let query = ravel_esql::parse(example, &Limits::default())
        .map_err(|error| format!("the example does not parse: {error:?}"))?;
    let own: Vec<&Expr> = where_expressions(&query)
        .into_iter()
        .filter(|e| is_own(e))
        .collect();
    let [expr] = own.as_slice() else {
        return Err(format!(
            "the example's WHERE conditions hold {} {owner} expressions, not one",
            own.len()
        ));
    };
    let span = expr.span();
    let start = span.line_col(example);
    if (line, column) != (start.line, start.column) {
        return Err(format!(
            "the refusal is at {line}:{column}, not at {}:{} where the {owner} expression starts",
            start.line, start.column
        ));
    }
    let refused_here = |source: &str| {
        matches!(
            prepare(source),
            Err(EsqlError::Unsupported { name, line: l, column: c })
                if name == ANY_EXPRESSION && (l, c) == (line, column)
        )
    };
    if refused_here(&nulled(example, span)) {
        return Err(format!(
            "the refusal at {line}:{column} stays with the {owner} expression replaced by null, so it refuses an expression enclosing it"
        ));
    }
    let operand = match expr {
        Expr::InSubquery(in_subquery) => Some(&*in_subquery.expr),
        Expr::Binary(binary) => Some(&*binary.left),
        _ => None,
    };
    if let Some(operand) = operand.filter(|o| o.span().start == span.start)
        && !refused_here(&nulled(example, operand.span()))
    {
        return Err(format!(
            "the refusal at {line}:{column} goes with the {owner} expression's left operand replaced by null, so it refuses that operand"
        ));
    }
    Ok(())
}

/// Whether `prepared` is the binder's refusal of `entry`: an `Unsupported`
/// error naming the entry's construct as a whole token, or, for an entry
/// refused through an earlier construct, naming that construct. A refused
/// source command is named "this source command", so for it the text at the
/// refusal's position must start with the construct as a whole token. A
/// refusal named `ANY_EXPRESSION` must be of exactly the expression the
/// construct is about (`refuses_own_expression`), since an expression starts
/// where an infix form whose left operand it is starts. No other refusal is
/// matched by position.
fn binder_refusal(
    manifest: &Manifest,
    entry: &Entry,
    prepared: &Result<PreparedEsql, EsqlError>,
) -> Result<(), String> {
    let (owner, marker) = match &entry.refused_by {
        Some(by) => manifest
            .released()
            .find(|e| &e.name == by)
            .map(|e| (e.name.clone(), e.refusal_marker()))
            .ok_or_else(|| format!("refused_by {by:?} names no released entry"))?,
        None => (entry.name.clone(), entry.refusal_marker()),
    };
    match prepared {
        Err(EsqlError::Unsupported { name, line, column }) if marker == ANY_EXPRESSION => {
            if name == ANY_EXPRESSION {
                refuses_own_expression(&owner, &entry.example, *line, *column)
            } else {
                Err(format!(
                    "the refusal names {name:?} at {line}:{column}, not {marker:?}"
                ))
            }
        }
        Err(EsqlError::Unsupported { name, line, column }) => {
            let at = text_at(&entry.example, *line, *column);
            if names_token(name, &marker)
                || (name == "this source command" && starts_with_token(&at, &marker))
            {
                Ok(())
            } else {
                Err(format!(
                    "the refusal names {name:?} at {line}:{column}, not {marker:?}"
                ))
            }
        }
        other => Err(format!(
            "prepare gives {:?}, not an Unsupported error naming {marker:?}",
            other.as_ref().map(|_| "a statement")
        )),
    }
}

/// Whether the run of a statement the binder accepts holds for an entry the
/// executor is marked `executor` for: it fails, with a typed refusal where
/// the executor refuses the entry and with an untyped error where it is
/// absent.
fn run_verdict<T>(executor: State, run: &Result<T, EsqlError>) -> Result<(), String> {
    match run {
        Err(error) if is_typed_refusal(error) == (executor == State::Refused) => Ok(()),
        Err(error) => Err(format!("the run fails with {error:?}")),
        Ok(_) => Err("the statement prepares and runs".to_string()),
    }
}

/// Check every entry not marked executor-supported against the layer its
/// states say refuses it, returning how many hold and what does not.
async fn check_refusals(manifest: &Manifest, fixture: &Fixture) -> (usize, Vec<String>) {
    let mut wrong = Vec::new();
    let mut refused = 0;
    for entry in &manifest.entries {
        if entry.executor == State::Supported {
            continue;
        }
        let prepared = prepare(&entry.example);
        let shown = || {
            format!(
                "prepare gives {:?}",
                prepared.as_ref().map(|_| "a statement")
            )
        };
        let verdict = match (entry.parser, entry.binder, &prepared) {
            (
                State::Refused,
                _,
                Err(EsqlError::Parse(ravel_esql::Error::UnsupportedSyntax { .. })),
            ) => Ok(()),
            (State::Absent, _, Err(EsqlError::Parse(error)))
                if !matches!(error, ravel_esql::Error::UnsupportedSyntax { .. }) =>
            {
                Ok(())
            }
            (State::Supported, State::Refused, _) => binder_refusal(manifest, entry, &prepared),
            (State::Supported, State::Supported, Ok(prepared)) => {
                run_verdict(entry.executor, &run_prepared(fixture, prepared).await)
            }
            _ => Err(shown()),
        };
        match verdict {
            Ok(()) => refused += 1,
            Err(found) => wrong.push(format!(
                "{} is marked parser = {}, binder = {}, executor = {} but {found}",
                describe(entry),
                entry.parser,
                entry.binder,
                entry.executor,
            )),
        }
    }
    (refused, wrong)
}

/// Every entry marked refused at a layer is refused there with a typed
/// error: by the parser with `UnsupportedSyntax`, by the binder with an
/// `Unsupported` error naming the entry's own construct (or the construct
/// its refused_by field names). A verification error never counts.
#[tokio::test]
async fn every_refused_entry_is_refused_with_a_typed_error() {
    let manifest = load();
    let (refused, wrong) = check_refusals(&manifest, &fixture().await).await;
    assert!(wrong.is_empty(), "{wrong:#?}");
    assert_eq!(
        refused,
        manifest
            .entries
            .iter()
            .filter(|e| e.executor != State::Supported)
            .count()
    );
    assert!(manifest.entries.iter().any(|e| e.refused_by.is_some()));
}

/// A manifest of one released entry with the given states and example.
fn one_entry(example: &str, parser: &str, binder: &str, executor: &str) -> Manifest {
    entry_with(example, parser, binder, executor, "")
}

/// A manifest of one binder-refused entry whose refusal must name `refusal`.
fn binder_refused(example: &str, refusal: &str) -> Manifest {
    entry_with(
        example,
        "supported",
        "refused",
        "refused",
        &format!("refusal = \"{refusal}\"\n"),
    )
}

/// `binder_refused` for an entry named `name`.
fn named_binder_refused(name: &str, example: &str, refusal: &str) -> Manifest {
    named_entry(
        name,
        example,
        "supported",
        "refused",
        "refused",
        &format!("refusal = \"{refusal}\"\n"),
    )
}

/// `one_entry` with `extra` appended to the entry's table.
fn entry_with(example: &str, parser: &str, binder: &str, executor: &str, extra: &str) -> Manifest {
    named_entry("X", example, parser, binder, executor, extra)
}

/// `entry_with` for an entry named `name`.
fn named_entry(
    name: &str,
    example: &str,
    parser: &str,
    binder: &str,
    executor: &str,
    extra: &str,
) -> Manifest {
    manifest::load(&format!(
        r#"
[release]
line = "9.5"
binary = "9.5.5"
reconciled = false

[[entry]]
kind = "syntax"
grammar = "own"
name = "{name}"
preview = false
example = '{example}'
parser = "{parser}"
binder = "{binder}"
executor = "{executor}"
evidence = "a.rs::t"
{extra}"#
    ))
    .expect("the test manifest loads")
}

/// The arms the real manifest does not reach: a parser-absent entry holds
/// only when its example fails to parse without a typed refusal, and an
/// entry the executor is said to refuse fails the check when it runs.
#[tokio::test]
async fn the_refusal_check_drives_the_arms_the_manifest_does_not_reach() {
    let fixture = fixture().await;
    let check = |manifest: Manifest| {
        let fixture = &fixture;
        async move { check_refusals(&manifest, fixture).await }
    };

    let (refused, wrong) = check(one_entry("FROM logs |", "absent", "absent", "absent")).await;
    assert_eq!((refused, wrong.len()), (1, 0), "{wrong:#?}");

    let (refused, wrong) = check(one_entry("ROW a = 1", "absent", "absent", "absent")).await;
    assert_eq!(refused, 0);
    assert!(
        wrong[0].contains("prepare gives Ok(\"a statement\")"),
        "{wrong:#?}"
    );

    let (refused, wrong) = check(one_entry("ROW a = 1", "supported", "supported", "refused")).await;
    assert_eq!(refused, 0);
    assert!(
        wrong[0].ends_with("the statement prepares and runs"),
        "{wrong:#?}"
    );

    let (refused, wrong) = check(one_entry("ROW a = 1", "supported", "supported", "absent")).await;
    assert_eq!(refused, 0);
    assert!(
        wrong[0].ends_with("the statement prepares and runs"),
        "{wrong:#?}"
    );
}

/// The run arms no entry reaches today, driven with constructed errors: a
/// typed refusal holds for an entry the executor refuses and an untyped
/// error for one it lacks, and neither holds for the other.
#[test]
fn the_run_verdict_holds_for_the_error_the_executor_state_names() {
    let typed: Result<(), EsqlError> = Err(EsqlError::Unsupported {
        name: "[X]".to_string(),
        line: 1,
        column: 1,
    });
    let untyped: Result<(), EsqlError> = Err(EsqlError::Plan("no plan".to_string()));
    assert_eq!(run_verdict(State::Refused, &typed), Ok(()));
    assert_eq!(run_verdict(State::Absent, &untyped), Ok(()));
    let wrong = run_verdict(State::Refused, &untyped).expect_err("untyped");
    assert!(wrong.starts_with("the run fails with Plan("), "{wrong}");
    let wrong = run_verdict(State::Absent, &typed).expect_err("typed");
    assert!(
        wrong.starts_with("the run fails with Unsupported"),
        "{wrong}"
    );
    assert_eq!(
        run_verdict(State::Refused, &Ok::<(), EsqlError>(())),
        Err("the statement prepares and runs".to_string())
    );
}

/// A binder refusal must be an `Unsupported` error naming the construct: a
/// verification error, or an `Unsupported` error naming another construct,
/// does not count.
#[tokio::test]
async fn a_binder_refusal_must_name_the_construct() {
    let fixture = fixture().await;
    let (refused, wrong) = check_refusals(
        &one_entry(
            "FROM logs | WHERE nope(1) > 0",
            "supported",
            "refused",
            "refused",
        ),
        &fixture,
    )
    .await;
    assert_eq!(refused, 0);
    assert!(wrong[0].contains("not \"[X]\""), "{wrong:#?}");

    let (refused, wrong) = check_refusals(
        &one_entry("ROW a = 1 hour", "supported", "refused", "refused"),
        &fixture,
    )
    .await;
    assert_eq!(refused, 0);
    assert!(wrong[0].contains("Verification"), "{wrong:#?}");
}

/// A refusal marker matches the refusal's construct as a whole token: an
/// RLIKE refusal does not hold for an entry whose refusal names LIKE.
#[tokio::test]
async fn a_refusal_marker_matches_a_whole_token() {
    let fixture = fixture().await;
    let like = r#"FROM logs | WHERE message LIKE "a*""#;
    let rlike = r#"FROM logs | WHERE message RLIKE "a.*""#;
    let (refused, wrong) = check_refusals(&binder_refused(like, "LIKE"), &fixture).await;
    assert_eq!((refused, wrong.len()), (1, 0), "{wrong:#?}");
    let (refused, wrong) = check_refusals(&binder_refused(rlike, "RLIKE"), &fixture).await;
    assert_eq!((refused, wrong.len()), (1, 0), "{wrong:#?}");
    let (refused, wrong) = check_refusals(&binder_refused(rlike, "LIKE"), &fixture).await;
    assert_eq!(refused, 0);
    assert!(
        wrong[0].contains("the refusal names \"RLIKE\""),
        "{wrong:#?}"
    );
}

/// A refusal of "this expression" reports where the refused expression
/// starts, which is also where an infix form whose left operand it is
/// starts, so it is never matched by the text there, and it satisfies only
/// the entry whose own expression is exactly the one refused: the IN
/// subquery entry, not the AND entry whose left operand that IN subquery
/// is, and no entry with no expression kind to place it against.
#[tokio::test]
async fn a_refused_left_operand_does_not_name_the_infix_form() {
    let fixture = fixture().await;
    let example = "FROM logs | WHERE dur IN (FROM logs | KEEP dur) AND dur > 1";
    let (refused, wrong) = check_refusals(
        &binder_refused(example, "dur IN (FROM logs | KEEP dur) AND"),
        &fixture,
    )
    .await;
    assert_eq!(refused, 0);
    assert!(
        wrong[0].contains("the refusal names \"this expression\" at 1:19"),
        "{wrong:#?}"
    );
    let (refused, wrong) = check_refusals(
        &named_binder_refused("IN subquery", example, "this expression"),
        &fixture,
    )
    .await;
    assert_eq!((refused, wrong.len()), (1, 0), "{wrong:#?}");
    let (refused, wrong) = check_refusals(
        &named_binder_refused("AND", example, "this expression"),
        &fixture,
    )
    .await;
    assert_eq!(refused, 0);
    assert!(
        wrong[0].contains(
            "the refusal at 1:19 goes with the AND expression's left operand replaced by null, so it refuses that operand"
        ),
        "{wrong:#?}"
    );
    let (refused, wrong) =
        check_refusals(&binder_refused(example, "this expression"), &fixture).await;
    assert_eq!(refused, 0);
    assert!(
        wrong[0].contains("\"X\" names no expression kind"),
        "{wrong:#?}"
    );
}

/// The IN subquery entry's own example is refused as "this expression" at
/// the IN expression's start, and an IN subquery whose refusal falls
/// elsewhere, or whose example holds two, does not satisfy it. Nor does the
/// refusal of an IN subquery satisfy the parenthesised AND that is its
/// operand and starts where it starts.
#[tokio::test]
async fn an_in_subquery_refusal_is_placed_at_the_in_expression() {
    let fixture = fixture().await;
    let manifest = load();
    let entry = manifest
        .entries
        .iter()
        .find(|e| e.name == "IN subquery")
        .expect("the manifest has an IN subquery entry");
    assert_eq!(entry.refusal.as_deref(), Some(ANY_EXPRESSION));
    let own = entry.example.as_str();
    assert_eq!(
        own,
        "FROM logs | WHERE service IN (FROM logs | WHERE dur > 5 | KEEP service)"
    );
    assert!(matches!(
        prepare(own),
        Err(EsqlError::Unsupported { ref name, line: 1, column: 19 }) if name == ANY_EXPRESSION
    ));
    assert_eq!(refuses_own_expression("IN subquery", own, 1, 19), Ok(()));
    assert_eq!(
        refuses_own_expression("IN subquery", own, 1, 20),
        Err(
            "the refusal is at 1:20, not at 1:19 where the IN subquery expression starts"
                .to_string()
        )
    );
    assert_eq!(
        refuses_own_expression(
            "AND",
            "FROM logs | WHERE (dur > 1 AND dur < 5) IN (FROM logs | KEEP dur)",
            1,
            19
        ),
        Err(
            "the refusal at 1:19 stays with the AND expression replaced by null, so it refuses an expression enclosing it"
                .to_string()
        )
    );
    let two = "FROM logs | WHERE dur IN (FROM logs | KEEP dur) OR dur IN (FROM logs | KEEP dur)";
    let (refused, wrong) = check_refusals(
        &named_binder_refused("IN subquery", two, "this expression"),
        &fixture,
    )
    .await;
    assert_eq!(refused, 0);
    assert!(
        wrong[0].contains("hold 2 IN subquery expressions, not one"),
        "{wrong:#?}"
    );
}

#[test]
fn a_timestamp_renders_with_nine_fraction_digits() {
    assert_eq!(iso_nanos(BASE), "2023-11-14T22:13:00.000000000Z");
    assert_eq!(iso_nanos(NOW + 7), "2023-11-14T22:23:00.000000007Z");
    assert_eq!(iso_nanos(0), "1970-01-01T00:00:00.000000000Z");
    assert_eq!(iso_nanos(-1), "1969-12-31T23:59:59.999999999Z");
    assert_eq!(
        iso_nanos(951_782_400 * SECOND),
        "2000-02-29T00:00:00.000000000Z"
    );
}

/// FROM without KEEP lists the native columns in a fixed order, not sorted
/// by name as the release lists mapped fields. That order is
/// `manifest::FROM_COLUMNS`, the only order from-columns undoes.
#[tokio::test]
async fn from_lists_the_native_columns_in_a_fixed_order() {
    let outcome = run(&fixture().await, "FROM logs").await.expect("runs");
    let names: Vec<String> = columns(&outcome)
        .into_iter()
        .map(|(name, _)| name)
        .collect();
    assert_eq!(names, manifest::FROM_COLUMNS);
    let mut sorted = names.clone();
    sorted.sort();
    assert_ne!(names, sorted);
}

/// NOW() is a date_nanos at the query clock, where the release gives a date.
#[tokio::test]
async fn now_is_typed_date_nanos() {
    let outcome = run(&fixture().await, "ROW t = NOW()").await.expect("runs");
    assert_eq!(
        columns(&outcome),
        vec![("t".to_string(), "date_nanos".to_string())]
    );
    assert_eq!(
        answer(&outcome).rows,
        vec![vec![Cell::Text(iso_nanos(NOW))]]
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
    assert_eq!(rows[0][1], "2023-11-14T22:13:00");
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

/// An overflow, a division by zero and a remainder by zero read null, and
/// the outcome has no field that could carry a warning about them.
#[tokio::test]
async fn an_overflow_gives_null_without_a_warning() {
    let outcome = run(
        &fixture().await,
        "ROW a = 9223372036854775807 + 1, b = 5 / 0, c = 5 % 0",
    )
    .await
    .expect("runs");
    assert_eq!(rows(&outcome), vec![vec!["null", "null", "null"]]);
    let EsqlOutcome {
        columns: _,
        run: _,
        clamped_limit,
    } = outcome;
    assert_eq!(clamped_limit, None);
}

/// The expected answers compare cell by cell, types included: a row value or
/// a column type that differs from Ravel's answer is a difference.
#[tokio::test]
async fn the_comparison_reports_a_changed_value_and_a_changed_type() {
    let manifest = load();
    let entry = manifest.released().find(|e| e.name == "SUM").expect("SUM");
    let outcome = run(&fixture().await, &entry.example).await.expect("runs");
    let actual = answer(&outcome);
    let Some(Expected {
        answer: release,
        order,
    }) = entry.expected.clone()
    else {
        panic!("SUM records an answer");
    };
    let with = |answer: Answer| Expected {
        answer,
        order: order.clone(),
    };
    assert_eq!(with(release.clone()).compare(&actual), Ok(()));
    let mut value = release.clone();
    value.rows[0][0] = Cell::Long(44);
    assert!(with(value).compare(&actual).is_err());
    let mut ty = release;
    ty.columns[0].ty = "double".to_string();
    assert!(with(ty).compare(&actual).is_err());
}

/// A divergent entry's rows are compared after the undo: a changed row value
/// in the release's answer fails the WHERE entry (from-columns: the first
/// row's dur, 20, as 21) and the `ROW a = 7 / 2` entry (integer-literal: 3
/// as 4), each naming the rows.
#[tokio::test]
async fn a_divergent_entry_fails_on_a_changed_row_value() {
    let manifest = load();
    let fixture = fixture().await;
    for (kind, name, id, column, value) in [
        ("processing_command", "WHERE", "from-columns", 1, 21),
        ("operator", "/", "integer-literal", 0, 4),
    ] {
        let entry = manifest
            .released()
            .find(|e| e.kind.as_str() == kind && e.name == name)
            .unwrap_or_else(|| panic!("{kind} {name}"));
        assert_eq!(entry.divergence.as_deref(), Some(id), "{name}");
        let expected = entry.expected.clone().expect("an answer");
        let outcome = run(&fixture, &entry.example).await.expect("runs");
        let actual = answer(&outcome);
        assert_eq!(expected.compare_divergent(id, &actual), Ok(()), "{name}");
        let mut changed = expected.clone();
        let first = changed.answer.rows.first_mut().expect("a row");
        assert_eq!(first[column], Cell::Long(value - 1), "{name}");
        first[column] = Cell::Long(value);
        let error = changed
            .compare_divergent(id, &actual)
            .expect_err("a changed value");
        assert!(
            error.starts_with(&format!("after undoing {id}: rows")),
            "{name}: {error}"
        );
    }
}
