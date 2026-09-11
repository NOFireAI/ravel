//! Executed page walks over `plan_page` (ADR-1374 D5).
//!
//! The rule this file exists to establish: **a total-order claim is proven by
//! an executed walk, never by a plan assertion.**
//!
//! `plan_page` reports `not_total: None` to say that its effective `ORDER BY`
//! uniquely determines the row sequence, which is what lets a caller page with
//! a strict keyset predicate. Every assertion about that claim in
//! `crate::page_plan`'s own unit tests reads the returned plan: the tiebreak
//! list, the `not_total` field, the rendered statement text. None of them runs
//! the statement. Three rounds of fixes to the same defect class each passed
//! their own plan assertions and each still lost rows, because a plan
//! assertion cannot see that the name the tiebreak claimed row identity on
//! belongs to a different value than the one the schema described.
//!
//! # What the walk asserts
//!
//! For every statement the planner reports a total order for, this suite pages
//! the statement with a row cap of 1 and asserts that **page k holds rows k of
//! the unpaged ordering, as a sequence**. Not the set of rows the walk
//! returned: the sequence, page by page, in page order.
//!
//! A multiset comparison of the walked rows against the unpaged result is the
//! weaker statement this suite used to make, and it is order-blind by
//! construction. It cannot see a `DESC` term the planner read as `ASC`, and it
//! cannot see a page statement rendered with no `ORDER BY` on the wrapper at
//! all: both still deliver every row exactly once, in an order no caller
//! asked for. The whole point of a total-order claim is the sequence, so the
//! sequence is what gets asserted.
//!
//! The ordering compared against is [`reference_statement`]: the caller's own
//! text, plus whatever columns the plan says it appended as a tiebreak, run
//! unpaged. It is deliberately NOT built from `plan.order_by`, because a plan
//! that misread the caller's `ORDER BY ts DESC` as ascending would then be
//! compared against its own misreading and agree with itself. Building the
//! reference from the caller's text hands the reading of it to DataFusion.
//! The reference is proved to be a reordering of the unpaged result, and
//! nothing else, by a multiset comparison against it.
//!
//! Two more properties fall out of asserting per page rather than over the
//! concatenation: a page that returns fewer rows than the cap while rows
//! remain is a finding (it breaks the indexing the claim rests on), and a walk
//! that ends early is reported against the ordering's length rather than
//! against a row set that happens to match.
//!
//! # The fixture has to be able to fail
//!
//! [`FIXTURE`] is six rows in deliberately unsorted insertion order, each
//! property present because a mutation of the planner survives without it:
//!
//! - two rows share `(ts, value)` under distinct `series_id`, so a keyset
//!   predicate over the wrong pair of columns cannot walk the tie;
//! - one `value` is `0.0`, so a projected `nullif(value, 0)` is NULL for it
//!   and a projection that shadows a NOT NULL name with a nullable expression
//!   loses rows;
//! - one `value` is `-0.0`, which DataFusion holds to be a DIFFERENT value
//!   from `0.0` (it compares floats by a total order, so `-0.0 = 0` is FALSE
//!   and `-0.0` sorts first);
//! - one `value` is NaN, which no cursor can carry, so a page ending on it
//!   mints no resume position;
//! - `series_id` is anti-correlated with `ts` at the head of the ordering (the
//!   smallest `ts` carries the largest `series_id`), so a tiebreak that orders
//!   on the wrong one of the two produces a different sequence rather than the
//!   same one;
//! - one `ts` is not a multiple of 1000 ns, so a truncating or rescaling
//!   round trip through a cursor value is visible;
//! - the same walk runs over a one-row table (an empty second page) and a
//!   zero-row table (an empty first page).
//!
//! [`the_fixture_can_expose_a_broken_keyset_predicate`] pins every one of
//! those as an exact count, so a later edit to the fixture that removes one
//! fails there rather than quietly turning the walks into a statement about
//! nothing.
//!
//! `samples` is registered as a plain in-memory table over
//! `ravel_sql::public_schema()`. The planner's row-identity claim is about what
//! the `samples` scan emits (at most one row per `(series_id, ts)`, which the
//! fixture respects), and everything a page walk depends on above that -- name
//! resolution, wildcard expansion, positional column aliases, keyset
//! comparison, NULL semantics -- is DataFusion's, which a `MemTable` exercises
//! exactly as the real provider would.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, BinaryArray, BooleanArray, DictionaryArray, FixedSizeBinaryArray,
    Float64Array, Int32Array, Int64Array, MapArray, MapBuilder, RecordBatch, StringArray,
    StringBuilder, TimestampNanosecondArray, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Int32Type, TimeUnit};
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::SessionConfig;
use ravel_sql::{
    PagePlan, PagePlanError, ResumePosition, ResumeValue, SAMPLES_TABLE, plan_page, public_schema,
};

/// The `ts` the two tied rows share.
const TIED_TS: i64 = 1_000;

/// The `value` the two tied rows share. Non-zero, so `nullif(value, 0)` is
/// this same number rather than NULL for both.
const TIED_VALUE: f64 = 1.0;

/// The `value` that a projected `nullif(value, 0)` turns into NULL.
const NULLING_VALUE: f64 = 0.0;

/// A `value` distinct from [`NULLING_VALUE`] under DataFusion's total float
/// order: `-0.0 = 0` is FALSE and `-0.0` sorts before `0.0`, so the two zero
/// rows are two values rather than one repeated.
const NEGATIVE_ZERO: f64 = -0.0;

/// A `ts` distinct from [`TIED_TS`], on the row carrying [`NULLING_VALUE`].
const LONE_TS: i64 = 2_000;

/// The smallest `ts` in the fixture, on the row carrying the largest
/// `series_id`. That anti-correlation is what makes ordering on `ts` and
/// ordering on `series_id` produce different sequences over this fixture.
const ANTI_TS: i64 = 500;

/// A `ts` that is not a multiple of 1000 ns, so a cursor round trip that
/// truncates or rescales the value lands the walk on the wrong row rather than
/// on the same one.
const ODD_TS: i64 = 1_234_567;

/// A page's row cap. One row per page is the smallest cap and the one that
/// makes a lost row visible at the first tie rather than only when a tie
/// straddles a page boundary of some larger size.
const PAGE_CAP: usize = 1;

/// How many pages a walk may take before it is treated as non-terminating.
/// Six rows at a cap of one needs seven pages including the empty last one;
/// anything near this bound is a planner that is not advancing.
const MAX_PAGES: usize = 32;

/// One fixture row. `labels` is not a parameter: it is NOT NULL and its
/// content is irrelevant to paging, so every row carries an empty label set.
#[derive(Clone, Copy)]
struct FixtureRow {
    ts: i64,
    value: f64,
    series_id: [u8; 16],
}

/// The six-row fixture, in insertion order, which is deliberately not the
/// order any statement below asks for.
const FIXTURE: &[FixtureRow] = &[
    FixtureRow {
        ts: TIED_TS,
        value: TIED_VALUE,
        series_id: [1u8; 16],
    },
    FixtureRow {
        ts: TIED_TS,
        value: TIED_VALUE,
        series_id: [2u8; 16],
    },
    FixtureRow {
        ts: LONE_TS,
        value: NULLING_VALUE,
        series_id: [3u8; 16],
    },
    // Anti-correlated: the smallest `ts` under the largest `series_id`.
    FixtureRow {
        ts: ANTI_TS,
        value: 3.0,
        series_id: [9u8; 16],
    },
    FixtureRow {
        ts: ODD_TS,
        value: f64::NAN,
        series_id: [4u8; 16],
    },
    FixtureRow {
        ts: 3_000,
        value: NEGATIVE_ZERO,
        series_id: [5u8; 16],
    },
];

/// One row, so a walk's second page is the empty one.
const ONE_ROW: &[FixtureRow] = &[FixtureRow {
    ts: TIED_TS,
    value: TIED_VALUE,
    series_id: [1u8; 16],
}];

/// No rows, so a walk's first page is the empty one.
const NO_ROWS: &[FixtureRow] = &[];

/// `rows` as a `samples` batch.
fn samples_batch(rows: &[FixtureRow]) -> RecordBatch {
    let ts = TimestampNanosecondArray::from(rows.iter().map(|row| row.ts).collect::<Vec<i64>>());
    let value = Float64Array::from(rows.iter().map(|row| row.value).collect::<Vec<f64>>());
    let series_id = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
        rows.iter().map(|row| Some(row.series_id)),
        16,
    )
    .expect("series id array");

    // One empty label set per row. The label content is irrelevant to paging
    // and the column is NOT NULL, so it has to be present and well-formed.
    let mut maps = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    for _ in rows {
        maps.append(true).expect("label map append");
    }
    let maps: MapArray = maps.finish();
    let labels = DictionaryArray::<Int32Type>::try_new(
        Int32Array::from((0..rows.len() as i32).collect::<Vec<i32>>()),
        Arc::new(maps) as ArrayRef,
    )
    .expect("labels dictionary");

    RecordBatch::try_new(
        public_schema(),
        vec![
            Arc::new(ts),
            Arc::new(value),
            Arc::new(series_id),
            Arc::new(labels),
        ],
    )
    .expect("fixture batch")
}

/// A context with `rows` registered as `samples`.
///
/// One target partition, so `collect` returns the sorted rows in the order the
/// `ORDER BY` produced them and "the first row of the result" is the row a
/// page cap of one would keep.
fn context_of(rows: &[FixtureRow]) -> SessionContext {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    let table =
        MemTable::try_new(public_schema(), vec![vec![samples_batch(rows)]]).expect("mem table");
    ctx.register_table(SAMPLES_TABLE, Arc::new(table))
        .expect("registered samples");
    ctx
}

/// A context over the full [`FIXTURE`].
fn context() -> SessionContext {
    context_of(FIXTURE)
}

async fn execute(ctx: &SessionContext, sql: &str) -> Vec<RecordBatch> {
    ctx.sql(sql)
        .await
        .unwrap_or_else(|e| panic!("planning {sql:?}: {e}"))
        .collect()
        .await
        .unwrap_or_else(|e| panic!("executing {sql:?}: {e}"))
}

/// Every row of `batches`, each rendered one string per column.
///
/// Rendered rather than compared as arrays because the statements below
/// project different column sets and types, `labels` among them, and the only
/// property under test is which rows came back in which order. NULL renders as
/// the literal `NULL`, distinct from an empty string.
fn rows_of(batches: &[RecordBatch]) -> Vec<Vec<String>> {
    let options = FormatOptions::default().with_null("NULL");
    let mut rows = Vec::new();
    for batch in batches {
        let formatters: Vec<ArrayFormatter<'_>> = batch
            .columns()
            .iter()
            .map(|column| {
                ArrayFormatter::try_new(column.as_ref(), &options).expect("array formatter")
            })
            .collect();
        for index in 0..batch.num_rows() {
            rows.push(
                formatters
                    .iter()
                    .map(|formatter| formatter.value(index).to_string())
                    .collect(),
            );
        }
    }
    rows
}

/// `rows` as a multiset: the row-to-count map two results are compared by when
/// only their contents are under test.
fn multiset(rows: &[Vec<String>]) -> BTreeMap<Vec<String>, usize> {
    let mut counts: BTreeMap<Vec<String>, usize> = BTreeMap::new();
    for row in rows {
        *counts.entry(row.clone()).or_default() += 1;
    }
    counts
}

/// The statement whose unpaged result is the ordering a walk of `sql` has to
/// reproduce page by page: the caller's own text, plus the tiebreak columns
/// the plan says it appended, which the planner always appends ascending.
///
/// Built from the caller's text and not from `plan.order_by` on purpose. A
/// planner that reads `ORDER BY ts DESC` as ascending renders an ascending
/// `order_by`, and a reference built from that would be ascending too and
/// agree with the walk. Handing the caller's text to DataFusion instead means
/// the direction under test is never the planner's own reading of it.
fn reference_statement(sql: &str, plan: &PagePlan) -> String {
    if plan.tiebreak_appended.is_empty() {
        return sql.to_string();
    }
    let appended = plan
        .tiebreak_appended
        .iter()
        .map(|column| format!("\"{column}\" ASC"))
        .collect::<Vec<String>>()
        .join(", ");
    if sql.to_ascii_uppercase().contains(" ORDER BY ") {
        format!("{sql}, {appended}")
    } else {
        format!("{sql} ORDER BY {appended}")
    }
}

/// The resume value for one order-term column of one row, or `None` when that
/// value is NULL.
///
/// A NULL is not a resume value ([`ResumeValue`] has no NULL variant, because
/// every comparison against NULL is NULL), so a walk that reaches one cannot
/// continue. That is itself a finding, and [`walk`] reports it rather than
/// treating the walk as finished.
fn resume_value(array: &ArrayRef, index: usize) -> Option<ResumeValue> {
    if array.is_null(index) {
        return None;
    }
    let value = match array.data_type() {
        DataType::Timestamp(TimeUnit::Nanosecond, None) => ResumeValue::TimestampNanos(
            array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .expect("timestamp array")
                .value(index),
        ),
        DataType::Float64 => ResumeValue::Float(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("float array")
                .value(index),
        ),
        DataType::Int64 => ResumeValue::Int(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 array")
                .value(index),
        ),
        DataType::Int32 => ResumeValue::Int(i64::from(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .expect("int32 array")
                .value(index),
        )),
        DataType::UInt64 => ResumeValue::UInt(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("uint64 array")
                .value(index),
        ),
        DataType::Boolean => ResumeValue::Bool(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("boolean array")
                .value(index),
        ),
        DataType::Utf8 => ResumeValue::Str(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("string array")
                .value(index)
                .to_string(),
        ),
        DataType::FixedSizeBinary(_) => ResumeValue::FixedSizeBinary(
            array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .expect("fixed size binary array")
                .value(index)
                .to_vec(),
        ),
        DataType::Binary => ResumeValue::Binary(
            array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("binary array")
                .value(index)
                .to_vec(),
        ),
        // Reachable only from a plan that claimed a total order over a term no
        // cursor can carry, which `plan_page` now refuses outright
        // (`OrderTermNotRepresentable`). It stays as the harness's own last
        // check on that refusal.
        other => panic!("no resume value for an order term of type {other}"),
    };
    Some(value)
}

/// What an executed walk found.
struct Walk {
    /// The rows each non-empty page returned, in page order. The terminating
    /// empty page contributes no entry; [`Self::pages`] counts it.
    page_rows: Vec<Vec<Vec<String>>>,
    /// The pages taken, the terminating empty one included.
    pages: usize,
    /// Set when the walk stopped because the last row's order term was NULL,
    /// so no resume position could be built from it.
    stopped_at_null: Option<String>,
}

impl Walk {
    /// Every row the walk collected, page order preserved.
    fn rows(&self) -> Vec<Vec<String>> {
        self.page_rows.iter().flatten().cloned().collect()
    }
}

/// Page `sql` with a cap of [`PAGE_CAP`] rows, from the first page to the
/// empty one, exactly as a paging caller would: each page's statement comes
/// from `plan_page` resumed at the previous page's last row.
async fn walk(ctx: &SessionContext, sql: &str) -> Walk {
    let mut page_rows: Vec<Vec<Vec<String>>> = Vec::new();
    let mut resume: Option<ResumePosition> = None;
    let mut pages = 0usize;
    loop {
        pages += 1;
        assert!(
            pages <= MAX_PAGES,
            "walk of {sql:?} did not terminate within {MAX_PAGES} pages",
        );
        let plan = plan_page(sql, resume.as_ref())
            .unwrap_or_else(|e| panic!("page {pages} of {sql:?} would not plan: {e}"));
        let batches = execute(ctx, &plan.statement).await;
        let page: Vec<Vec<String>> = rows_of(&batches).into_iter().take(PAGE_CAP).collect();
        if page.is_empty() {
            return Walk {
                page_rows,
                pages,
                stopped_at_null: None,
            };
        }

        // The cursor position is read off the page's own last row, which is
        // the only thing a redeeming caller has.
        let kept = page.len();
        let last = kept - 1;
        let mut tuple = Vec::with_capacity(plan.order_by.len());
        for term in &plan.order_by {
            let (batch, offset) = row_at(&batches, last);
            let column = batch
                .column_by_name(&term.column)
                .unwrap_or_else(|| panic!("page of {sql:?} does not carry {:?}", term.column));
            match resume_value(column, offset) {
                Some(value) => tuple.push(value),
                None => {
                    page_rows.push(page);
                    return Walk {
                        page_rows,
                        pages,
                        stopped_at_null: Some(term.column.clone()),
                    };
                }
            }
        }
        page_rows.push(page);
        resume = Some(ResumePosition::new(tuple));
    }
}

/// The batch holding global row `index` of `batches`, and that row's offset
/// inside it.
fn row_at(batches: &[RecordBatch], index: usize) -> (&RecordBatch, usize) {
    let mut remaining = index;
    for batch in batches {
        if remaining < batch.num_rows() {
            return (batch, remaining);
        }
        remaining -= batch.num_rows();
    }
    panic!("row {index} is past the end of the result");
}

/// Where the walk's page sequence departs from `reference`, or `None` when
/// page k held rows k of it from the first page to the last.
fn sequence_finding(reference: &[Vec<String>], found: &Walk) -> Option<String> {
    let mut cursor = 0usize;
    for (index, page) in found.page_rows.iter().enumerate() {
        let page_number = index + 1;
        let end = cursor + page.len();
        if end > reference.len() {
            return Some(format!(
                "page {page_number} runs past the end of the ordering: it starts at ordering \
                 row {cursor} and holds {} rows, and the ordering has {}",
                page.len(),
                reference.len(),
            ));
        }
        if page.as_slice() != &reference[cursor..end] {
            return Some(format!(
                "page {page_number} is not rows {cursor}..{end} of the ordering\n      page     \
                 {page:?}\n      ordering {:?}",
                &reference[cursor..end],
            ));
        }
        if page.len() != PAGE_CAP && end != reference.len() {
            return Some(format!(
                "page {page_number} returned {} of the {PAGE_CAP} rows a page holds while {} \
                 rows were still unwalked, so page k stopped meaning rows k of the ordering",
                page.len(),
                reference.len() - end,
            ));
        }
        cursor = end;
    }
    let stopped = match &found.stopped_at_null {
        Some(column) => format!(", stopped at a NULL {column}"),
        None => String::new(),
    };
    if cursor != reference.len() {
        return Some(format!(
            "the walk delivered {cursor} of the ordering's {} rows in {} pages{stopped}",
            reference.len(),
            found.pages,
        ));
    }
    found
        .stopped_at_null
        .as_ref()
        .map(|column| format!("the walk delivered every row but stopped at a NULL {column}"))
}

/// The four statements this round exists for, plus the shapes that must keep
/// walking so a fix cannot be a blanket refusal.
///
/// Each is a caller-plausible statement, not a contrived one: an alias that
/// happens to take a row-identity column's name, a positional column-rename
/// list, a wildcard with a shadowing item after it, and a constant projection.
const STATEMENTS: &[&str] = &[
    // F1: `series_id` as the OUTPUT name of `value`. The tiebreak's row
    // identity is claimed by matching that name.
    "SELECT ts, value AS series_id FROM samples ORDER BY ts",
    // F2: a positional column-rename list on the target table. The output
    // `series_id` is `value` and the output `ts` is `ts`.
    "SELECT ts, series_id FROM samples AS x (ts, series_id, a, b) ORDER BY ts, series_id",
    // F3: a wildcard, then an item that shadows the name it excluded.
    "SELECT * EXCEPT (ts), nullif(value, 0) AS ts FROM samples ORDER BY ts",
    "SELECT * EXCLUDE (ts), nullif(value, 0) AS ts FROM samples ORDER BY ts",
    "SELECT * REPLACE (nullif(value, 0) AS ts) FROM samples ORDER BY ts",
    // A constant projection under the identity column names.
    "SELECT 1 AS ts, 2 AS series_id FROM samples",
    // The shapes a fix must not break: these walk every row today and have to
    // keep doing so.
    "SELECT * FROM samples ORDER BY ts",
    "SELECT * FROM samples",
    "SELECT ts, series_id FROM samples ORDER BY ts",
    "SELECT ts, series_id, value FROM samples ORDER BY ts DESC, series_id",
    "SELECT ts, series_id FROM samples ORDER BY series_id",
    "SELECT * FROM samples WHERE value > 0.5 ORDER BY ts",
    // Refused now rather than walked, both for a term whose values no cursor
    // carries. `value` is a float, so it admits the NaN that
    // `ResumeValue::Float` refuses; `labels` is a `Dictionary` over a `Map`,
    // which no variant carries at all. Kept in the table so both refusals are
    // exercised on the same path the walks take: a planner that admits either
    // one reaches the cursor mint and dies there.
    "SELECT ts, series_id, value FROM samples ORDER BY value DESC, ts",
    "SELECT * FROM samples ORDER BY labels, ts, series_id",
];

/// Walk every total-order statement over one fixture, returning how many were
/// walked and what each departure from the ordering was.
async fn walk_findings(rows: &[FixtureRow]) -> (usize, Vec<String>) {
    let ctx = context_of(rows);
    let mut walked = 0usize;
    // Collected rather than asserted per statement: the first failing walk is
    // not the only one, and a suite that stops at it hides how wide the defect
    // is.
    let mut findings: Vec<String> = Vec::new();
    for sql in STATEMENTS {
        let plan = match plan_page(sql, None) {
            Ok(plan) => plan,
            Err(_) => continue,
        };
        if plan.not_total.is_some() {
            continue;
        }
        walked += 1;

        let unpaged = rows_of(&execute(&ctx, sql).await);
        let reference_sql = reference_statement(sql, &plan);
        let reference = rows_of(&execute(&ctx, &reference_sql).await);
        // The reference has to be the same rows in some order, or the sequence
        // assertion below is about the wrong result rather than about the walk.
        if multiset(&reference) != multiset(&unpaged) {
            findings.push(format!(
                "{sql:?}\n    the tiebreak reference {reference_sql:?} returned {} rows against \
                 the statement's own {}",
                reference.len(),
                unpaged.len(),
            ));
            continue;
        }

        let found = walk(&ctx, sql).await;
        if let Some(finding) = sequence_finding(&reference, &found) {
            findings.push(format!(
                "{sql:?}\n    order_by {:?}, tiebreak {:?}, ordering {reference_sql:?}\n    \
                 {finding}",
                plan.order_by
                    .iter()
                    .map(|term| term.render())
                    .collect::<Vec<String>>(),
                plan.tiebreak_appended,
            ));
        }
    }
    (walked, findings)
}

/// The acceptance gate: every statement the planner claims a total order for
/// pages to the unpaged ordering, page k holding rows k of it.
///
/// Statements the planner refuses, and statements it plans with a
/// `not_total` reason, are outside the claim: D5 pages the second kind under
/// the equal-group rule, which is the caller's half and not this planner's. So
/// a refusal and a not-total plan both satisfy this test, and the pinned
/// classification in [`the_four_defect_statements_are_classified_exactly`] is
/// what stops a fix from satisfying it by refusing everything.
#[tokio::test]
async fn a_total_order_claim_survives_an_executed_page_walk() {
    let fixtures: [(&str, &[FixtureRow]); 3] = [
        ("the six-row fixture", FIXTURE),
        ("a one-row table", ONE_ROW),
        ("an empty table", NO_ROWS),
    ];
    let mut walked = 0usize;
    let mut findings: Vec<String> = Vec::new();
    for (label, rows) in fixtures {
        let (count, found) = walk_findings(rows).await;
        walked += count;
        findings.extend(
            found
                .into_iter()
                .map(|finding| format!("{label}: {finding}")),
        );
    }
    assert!(
        findings.is_empty(),
        "{} of {walked} total-order claims did not page to their own ordering:\n  {}",
        findings.len(),
        findings.join("\n  "),
    );
    assert!(
        walked > 0,
        "no statement in the table claimed a total order, so the gate asserted \
         nothing",
    );
}

/// A one-row table ends the walk on an empty SECOND page, and a zero-row table
/// on an empty FIRST one.
///
/// Both are page counts the six-row fixture cannot produce, and both are the
/// boundary a resume position is never built at: the first has exactly one
/// cursor mint and the second has none.
#[tokio::test]
async fn a_walk_terminates_on_an_empty_page_at_both_table_boundaries() {
    let sql = "SELECT * FROM samples ORDER BY ts";

    let one = walk(&context_of(ONE_ROW), sql).await;
    assert_eq!(one.pages, 2, "pages over a one-row table");
    assert_eq!(
        one.page_rows.len(),
        1,
        "non-empty pages over a one-row table"
    );
    assert_eq!(one.rows().len(), 1, "rows walked from a one-row table");
    assert_eq!(one.stopped_at_null, None);

    let none = walk(&context_of(NO_ROWS), sql).await;
    assert_eq!(none.pages, 1, "pages over an empty table");
    assert!(
        none.page_rows.is_empty(),
        "non-empty pages over an empty table"
    );
    assert_eq!(none.stopped_at_null, None);
}

/// The fixture can actually expose the defects it is here to expose.
///
/// Pinned as exact counts. A fixture edit that drops one of these leaves every
/// walk above passing over rows that no mutation of the planner could disturb,
/// which is how a suite of this shape goes vacuous.
#[tokio::test]
async fn the_fixture_can_expose_a_broken_keyset_predicate() {
    let ctx = context();

    let rows = rows_of(&execute(&ctx, "SELECT ts, value, series_id FROM samples").await);
    assert_eq!(rows.len(), 6, "fixture row count");

    let tied = rows_of(
        &execute(
            &ctx,
            "SELECT count(*) AS n FROM (SELECT ts, value FROM samples GROUP BY ts, value \
             HAVING count(*) > 1)",
        )
        .await,
    );
    assert_eq!(tied, vec![vec!["1".to_string()]], "groups of tied rows");

    let tied_rows = rows_of(
        &execute(
            &ctx,
            "SELECT count(*) AS n FROM samples WHERE ts = arrow_cast(1000, \
             'Timestamp(Nanosecond, None)') AND value = 1.0",
        )
        .await,
    );
    assert_eq!(
        tied_rows,
        vec![vec!["2".to_string()]],
        "rows sharing one (ts, value)",
    );

    // One, not two: DataFusion compares floats by a total order, so `-0.0 = 0`
    // is FALSE and only the positive zero is NULLed. Measured, not assumed:
    // `SELECT value, value = 0 FROM samples` returns false for the `-0.0` row.
    let nulled = rows_of(
        &execute(
            &ctx,
            "SELECT count(*) AS n FROM samples WHERE nullif(value, 0) IS NULL",
        )
        .await,
    );
    assert_eq!(
        nulled,
        vec![vec!["1".to_string()]],
        "rows a projected nullif(value, 0) nulls",
    );
    let zeroes = rows_of(
        &execute(
            &ctx,
            "SELECT count(*) AS n FROM samples WHERE value = 0 OR value = -0.0",
        )
        .await,
    );
    assert_eq!(
        zeroes,
        vec![vec!["2".to_string()]],
        "the two zero rows are distinct values, not one repeated",
    );

    let distinct_identity = rows_of(
        &execute(
            &ctx,
            "SELECT count(*) AS n FROM (SELECT DISTINCT ts, series_id FROM samples)",
        )
        .await,
    );
    assert_eq!(
        distinct_identity,
        vec![vec!["6".to_string()]],
        "the fixture respects the samples row identity",
    );

    let rendered = rows_of(&execute(&ctx, "SELECT value FROM samples").await);
    let negative_zeroes = rendered.iter().filter(|row| row[0] == "-0.0").count();
    assert_eq!(negative_zeroes, 1, "rows rendering as a negative zero");
    let nans = rendered.iter().filter(|row| row[0] == "NaN").count();
    assert_eq!(nans, 1, "rows rendering as NaN");

    let odd_ts = rows_of(
        &execute(
            &ctx,
            "SELECT count(*) AS n FROM samples WHERE arrow_cast(ts, 'Int64') % 1000 != 0",
        )
        .await,
    );
    assert_eq!(
        odd_ts,
        vec![vec!["2".to_string()]],
        "rows whose ts is not a whole microsecond",
    );

    // Anti-correlation: ordering on `ts` and ordering on `series_id` do not
    // agree at the head, so a tiebreak that appends the wrong one of the two
    // produces a different sequence rather than the same one.
    let by_ts = rows_of(&execute(&ctx, "SELECT series_id FROM samples ORDER BY ts LIMIT 1").await);
    let by_series = rows_of(
        &execute(
            &ctx,
            "SELECT series_id FROM samples ORDER BY series_id DESC LIMIT 1",
        )
        .await,
    );
    assert_eq!(
        by_ts, by_series,
        "the smallest ts does not carry the largest series_id, so the fixture is \
         not anti-correlated",
    );
}

/// The exact classification of the six statements the four defects are
/// demonstrated by, so a fix cannot pass
/// [`a_total_order_claim_survives_an_executed_page_walk`] by refusing every
/// statement in the table.
///
/// `Refused` and `NotTotal` are both acceptable outcomes for these six: the
/// planner's job is not to page them, it is to not claim they page under a
/// keyset predicate. What is pinned is that none of them is `Total`, and that
/// the statements below them in [`STATEMENTS`] still are.
#[test]
fn the_four_defect_statements_are_classified_exactly() {
    let must_not_be_total = [
        "SELECT ts, value AS series_id FROM samples ORDER BY ts",
        "SELECT ts, series_id FROM samples AS x (ts, series_id, a, b) ORDER BY ts, series_id",
        "SELECT * EXCEPT (ts), nullif(value, 0) AS ts FROM samples ORDER BY ts",
        "SELECT * EXCLUDE (ts), nullif(value, 0) AS ts FROM samples ORDER BY ts",
        "SELECT * REPLACE (nullif(value, 0) AS ts) FROM samples ORDER BY ts",
        "SELECT 1 AS ts, 2 AS series_id FROM samples",
    ];
    let mut claimed: Vec<String> = Vec::new();
    for sql in must_not_be_total {
        match plan_page(sql, None) {
            Ok(plan) if plan.not_total.is_none() => claimed.push(format!(
                "{sql:?} over {:?}",
                plan.order_by
                    .iter()
                    .map(|term| term.render())
                    .collect::<Vec<String>>(),
            )),
            Ok(_) => {}
            Err(PagePlanError::Invalid(e)) => panic!("{sql:?} failed validation: {e}"),
            Err(_) => {}
        }
    }
    assert!(
        claimed.is_empty(),
        "{} statements still claim a total order:\n  {}",
        claimed.len(),
        claimed.join("\n  "),
    );

    let must_stay_total = [
        "SELECT * FROM samples ORDER BY ts",
        "SELECT * FROM samples",
        "SELECT ts, series_id FROM samples ORDER BY ts",
        // A DESC term, so a fix that reads every term as ASC is not a fix that
        // passes here. `value DESC` used to be this case and is now refused
        // outright: a float term admits NaN, which no keyset disjunct places.
        "SELECT ts, series_id, value FROM samples ORDER BY ts DESC, series_id",
        "SELECT ts, series_id FROM samples ORDER BY series_id",
        "SELECT * FROM samples WHERE value > 0.5 ORDER BY ts",
    ];
    for sql in must_stay_total {
        let plan = plan_page(sql, None).unwrap_or_else(|e| panic!("{sql:?} refused: {e}"));
        assert_eq!(
            plan.not_total, None,
            "{sql:?} lost its total order, so the fix is a blanket refusal",
        );
    }
}
