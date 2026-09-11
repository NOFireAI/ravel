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
//! So this suite executes. For every statement the planner reports a total
//! order for, it pages the statement with a row cap of 1, collects the rows
//! every page returned, and asserts MULTISET EQUALITY against the same
//! statement run unpaged. A row count is not enough: a count still passes when
//! one row is dropped and another duplicated, and both happen under a keyset
//! predicate built over the wrong column.
//!
//! # The fixture has to be able to fail
//!
//! [`samples_fixture`] carries deliberate ties and a deliberate NULL source:
//! two rows share `(ts, value)` under distinct `series_id`, and one row's
//! `value` is `0.0`, so a projected `nullif(value, 0)` is NULL for it. Without
//! the ties, a keyset predicate over the wrong pair of columns still walks
//! every row and the suite is a tautology; without the zero, a projection that
//! shadows a NOT NULL name with a nullable expression loses nothing.
//! [`the_fixture_can_expose_a_broken_keyset_predicate`] pins both properties as
//! exact counts, so a later edit to the fixture that removes them fails here
//! rather than quietly turning every walk below into a statement about nothing.
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
    PagePlanError, ResumePosition, ResumeValue, SAMPLES_TABLE, plan_page, public_schema,
};

/// The `ts` the two tied rows share.
const TIED_TS: i64 = 1_000;

/// The `value` the two tied rows share. Non-zero, so `nullif(value, 0)` is
/// this same number rather than NULL for both.
const TIED_VALUE: f64 = 1.0;

/// The third row's `value`. Zero, which is what makes a projected
/// `nullif(value, 0)` NULL for exactly one row of the fixture.
const NULLING_VALUE: f64 = 0.0;

/// The third row's `ts`, distinct from [`TIED_TS`].
const LONE_TS: i64 = 2_000;

/// A page's row cap. One row per page is the smallest cap and the one that
/// makes a lost row visible at the first tie rather than only when a tie
/// straddles a page boundary of some larger size.
const PAGE_CAP: usize = 1;

/// How many pages a walk may take before it is treated as non-terminating.
/// Three rows at a cap of one needs four pages including the empty last one;
/// anything near this bound is a planner that is not advancing.
const MAX_PAGES: usize = 32;

/// Three rows, two of them tied on `(ts, value)`, one of them carrying the
/// `value` that a projected `nullif(value, 0)` turns into NULL.
///
/// The `series_id`s are distinct, so the fixture respects the `samples` row
/// identity the planner's total-order claim rests on: `(series_id, ts)` is
/// unique across the three rows.
fn samples_fixture() -> RecordBatch {
    let ts = TimestampNanosecondArray::from(vec![TIED_TS, TIED_TS, LONE_TS]);
    let value = Float64Array::from(vec![TIED_VALUE, TIED_VALUE, NULLING_VALUE]);
    let series_id = FixedSizeBinaryArray::try_from_iter([[1u8; 16], [2u8; 16], [3u8; 16]].iter())
        .expect("series id array");

    // One empty label set per row. The label content is irrelevant to paging
    // and the column is NOT NULL, so it has to be present and well-formed.
    let mut maps = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    for _ in 0..3 {
        maps.append(true).expect("label map append");
    }
    let maps: MapArray = maps.finish();
    let labels = DictionaryArray::<Int32Type>::try_new(
        Int32Array::from(vec![0, 1, 2]),
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

/// A context with the fixture registered as `samples`.
///
/// One target partition, so `collect` returns the sorted rows in the order the
/// `ORDER BY` produced them and "the first row of the result" is the row a
/// page cap of one would keep.
fn context() -> SessionContext {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    let table =
        MemTable::try_new(public_schema(), vec![vec![samples_fixture()]]).expect("mem table");
    ctx.register_table(SAMPLES_TABLE, Arc::new(table))
        .expect("registered samples");
    ctx
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
/// property under test is which rows came back. NULL renders as the literal
/// `NULL`, distinct from an empty string.
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

/// `rows` as a multiset: the row-to-count map two walks are compared by.
///
/// A count alone passes when one row is dropped and another duplicated, which
/// is what a keyset predicate over a mis-resolved column actually does.
fn multiset(rows: &[Vec<String>]) -> BTreeMap<Vec<String>, usize> {
    let mut counts: BTreeMap<Vec<String>, usize> = BTreeMap::new();
    for row in rows {
        *counts.entry(row.clone()).or_default() += 1;
    }
    counts
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
        other => panic!("no resume value for an order term of type {other}"),
    };
    Some(value)
}

/// What an executed walk found.
struct Walk {
    /// The rows the walk collected, page by page, in page order.
    rows: Vec<Vec<String>>,
    /// The pages taken, the terminating empty one included.
    pages: usize,
    /// Set when the walk stopped because the last row's order term was NULL,
    /// so no resume position could be built from it.
    stopped_at_null: Option<String>,
}

/// Page `sql` with a cap of [`PAGE_CAP`] rows, from the first page to the
/// empty one, exactly as a paging caller would: each page's statement comes
/// from `plan_page` resumed at the previous page's last row.
async fn walk(ctx: &SessionContext, sql: &str) -> Walk {
    let mut rows: Vec<Vec<String>> = Vec::new();
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
                rows,
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
                    rows.extend(page);
                    return Walk {
                        rows,
                        pages,
                        stopped_at_null: Some(term.column.clone()),
                    };
                }
            }
        }
        rows.extend(page);
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
    "SELECT ts, series_id, value FROM samples ORDER BY value DESC, ts",
    "SELECT * FROM samples WHERE value > 0.5 ORDER BY ts",
];

/// The acceptance gate: every statement the planner claims a total order for
/// pages to exactly the rows it returns unpaged.
///
/// Statements the planner refuses, and statements it plans with a
/// `not_total` reason, are outside the claim: D5 pages the second kind under
/// the equal-group rule, which is the caller's half and not this planner's. So
/// a refusal and a not-total plan both satisfy this test, and the pinned
/// classification in [`the_four_defect_statements_are_classified_exactly`] is
/// what stops a fix from satisfying it by refusing everything.
#[tokio::test]
async fn a_total_order_claim_survives_an_executed_page_walk() {
    let ctx = context();
    let mut walked = 0usize;
    // Collected rather than asserted per statement: the first failing walk is
    // not the only one, and a suite that stops at it hides how wide the defect
    // is. Every finding below names the rows that went missing.
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

        let unpaged = multiset(&rows_of(&execute(&ctx, sql).await));
        let found = walk(&ctx, sql).await;
        let walked_rows = multiset(&found.rows);
        if walked_rows == unpaged && found.stopped_at_null.is_none() {
            continue;
        }

        let mut missing: Vec<String> = Vec::new();
        for (row, count) in &unpaged {
            let seen = walked_rows.get(row).copied().unwrap_or(0);
            if seen != *count {
                missing.push(format!("{row:?} unpaged {count} times, walked {seen}"));
            }
        }
        for (row, count) in &walked_rows {
            if !unpaged.contains_key(row) {
                missing.push(format!("{row:?} walked {count} times, never unpaged"));
            }
        }
        findings.push(format!(
            "{sql:?}\n    order_by {:?}, tiebreak {:?}\n    {} pages at a cap of \
             {PAGE_CAP} returned {} of {} rows{}\n    {}",
            plan.order_by
                .iter()
                .map(|term| term.render())
                .collect::<Vec<String>>(),
            plan.tiebreak_appended,
            found.pages,
            found.rows.len(),
            unpaged.values().sum::<usize>(),
            match &found.stopped_at_null {
                Some(column) => format!(", stopped at a NULL {column}"),
                None => String::new(),
            },
            missing.join("\n    "),
        ));
    }
    assert!(
        findings.is_empty(),
        "{} of {walked} total-order claims lost rows under an executed walk:\n  {}",
        findings.len(),
        findings.join("\n  "),
    );
    assert!(
        walked > 0,
        "no statement in the table claimed a total order, so the gate asserted \
         nothing",
    );
}

/// The fixture can actually expose the defects it is here to expose.
///
/// Pinned as exact counts. A fixture edit that drops the tie or the zero
/// leaves every walk above passing over rows that no keyset predicate could
/// lose, which is how a suite of this shape goes vacuous.
#[tokio::test]
async fn the_fixture_can_expose_a_broken_keyset_predicate() {
    let ctx = context();

    let rows = rows_of(&execute(&ctx, "SELECT ts, value, series_id FROM samples").await);
    assert_eq!(rows.len(), 3, "fixture row count");

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

    let distinct_identity = rows_of(
        &execute(
            &ctx,
            "SELECT count(*) AS n FROM (SELECT DISTINCT ts, series_id FROM samples)",
        )
        .await,
    );
    assert_eq!(
        distinct_identity,
        vec![vec!["3".to_string()]],
        "the fixture respects the samples row identity",
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
        "SELECT ts, series_id, value FROM samples ORDER BY value DESC, ts",
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
