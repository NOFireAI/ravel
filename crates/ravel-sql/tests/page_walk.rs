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
//! That reference is built by PARSING the caller's statement and appending the
//! tiebreak terms to its `ORDER BY` in the AST, not by concatenating text onto
//! it. Independence from the plan is the only reason the reference is worth
//! having, and string surgery is not independent: `", ts ASC"` appended to a
//! statement whose text does not end where the appender assumed lands inside
//! something else, and the reference is then wrong in the same direction as
//! the plan it is checking. A parse that fails, or a statement whose ordering
//! has no term list to append to, is a panic rather than a fallback spelling.
//!
//! [`COMMENT_TERMINATED_STATEMENT`] is what makes that a claim rather than a
//! preference. It is in the walked set precisely because its text does not
//! survive concatenation: everything appended after it is inside a `--`
//! comment, so a concatenating builder produces a reference with no ordering
//! at all. Every other walked statement agrees under both spellings, which is
//! why the parse could be reverted with nothing going red before it was added.
//!
//! One more property falls out of asserting per page rather than over the
//! concatenation: a walk that ends early is reported against the ordering's
//! length rather than against a row set that happens to match.
//!
//! # The fixture has to be able to fail
//!
//! [`FIXTURE`] is seven rows in deliberately unsorted insertion order, each
//! property present because a mutation of the planner survives without it:
//!
//! - two rows share `(ts, value)` under distinct `series_id`, so a keyset
//!   predicate over the wrong pair of columns cannot walk the tie;
//! - that tied pair is stored in DESCENDING `series_id` order, so its
//!   insertion order is not its tiebreak order: a wrapper `ORDER BY` that
//!   drops a term the keyset predicate still carries returns the pair the
//!   other way round rather than the same way round by luck;
//! - two rows share one `series_id` under distinct `ts`, so a row identity
//!   declared as `series_id` alone is not a key over this fixture;
//! - one `value` is `0.0`, so a projected `nullif(value, 0)` is NULL for it
//!   and a projection that shadows a NOT NULL name with a nullable expression
//!   loses rows;
//! - one `value` is `-0.0`, which DataFusion sorts before `0.0` (its sort is
//!   a total order) but compares EQUAL to it (`-0.0 = 0` is TRUE, since its
//!   comparison kernels normalise a zero's sign first), while `nullif` does
//!   not normalise, so `nullif(value, 0)` keeps the `-0.0` row;
//! - one `value` is NaN, which no cursor can carry, so a page ending on it
//!   mints no resume position;
//! - `series_id` is anti-correlated with `ts` at the head of the ordering (the
//!   smallest `ts` carries the largest `series_id`), so a tiebreak that orders
//!   on the wrong one of the two produces a different sequence rather than the
//!   same one;
//! - one `ts` is not a multiple of 1000 ns, so a truncating or rescaling
//!   round trip through a cursor value is visible.
//!
//! [`the_fixture_can_expose_a_broken_keyset_predicate`] pins every one of
//! those as an exact count, so a later edit to the fixture that removes one
//! fails there rather than quietly turning the walks into a statement about
//! nothing.
//!
//! # What the three fixtures each cover, and what the walked count is not
//!
//! Every statement is walked over three `samples` fixtures, so the pinned
//! count in [`a_total_order_claim_survives_an_executed_page_walk`] is three
//! times the walked statement count. That number is a real pin and it does
//! move, but it is not a breadth figure, and reading it as one overstates what
//! the suite covers.
//!
//! Only [`FIXTURE`] can say anything about an ORDERING. It is the seven-row
//! table, it is the one that carries every property listed above, and every
//! mutation this suite has caught was caught by it.
//!
//! [`ONE_ROW`] and [`NO_ROWS`] cover the walk's PAGE BOUNDARIES and nothing
//! else: a one-row table has exactly one cursor mint and ends on an empty
//! SECOND page, and an empty table has no mint at all and ends on an empty
//! FIRST page. Neither can distinguish two orderings, because one row admits
//! one sequence and no rows admit none. They are worth their third of the
//! count for the boundaries alone --
//! [`a_walk_terminates_on_an_empty_page_at_both_table_boundaries`] pins those
//! two page counts directly -- but a mutation that reorders rows is invisible
//! to both. Widening the ordering coverage means more STATEMENTS or more
//! fixture properties, never more rows in these two.
//!
//! [`LOG_FIXTURE`] does not vary across that axis at all: `logs` is registered
//! identically under all three, because what it is here for is the absence of
//! a row identity rather than a row count that tracks `samples`. It is still
//! registered under all three rather than only under [`FIXTURE`], so that
//! every statement in [`STATEMENTS`] can execute under every fixture: a
//! context missing the table would turn the planner regression this table
//! exists to catch into a "table not found" panic under two of the three,
//! which is a worse report of the same thing. What it does mean is that a
//! `logs` walk under the second and third fixtures would repeat the first
//! exactly, so the reverse direction is covered once, not three times.
//!
//! # Two tables, because one table hides a whole class of mutation
//!
//! `samples` is registered as a plain in-memory table over
//! `ravel_sql::public_schema()`. The planner's row-identity claim is about what
//! the `samples` scan emits (at most one row per `(series_id, ts)`, which the
//! fixture respects), and everything a page walk depends on above that -- name
//! resolution, wildcard expansion, positional column aliases, keyset
//! comparison, NULL semantics -- is DataFusion's, which a `MemTable` exercises
//! exactly as the real provider would.
//!
//! `logs` is registered beside it, over `ravel_sql::logs_schema()`, and its
//! fixture carries the duplicate row that makes `logs` have no row identity in
//! the first place: ingest is at-least-once, so the same record can arrive
//! twice and two rows can tie on every orderable column. Nothing in
//! [`STATEMENTS`] over `logs` is walked today, because the planner reports
//! [`ravel_sql::NotTotalOrder::NoRowIdentity`] for all four RLOG- and
//! RSPAN-backed tables and the walk is a statement about total-order claims
//! only. Registering it is what makes the reverse direction fail: a planner
//! that starts claiming a total order over one of those tables walks these
//! statements, and the duplicate row then leaves a row on no page. Without a
//! second table the harness cannot execute such a statement at all, so that
//! whole class of mutation passes every test here by construction.
//!
//! The walked set is not row-wise selects alone either. Two window statements
//! are in it, because a window statement DOES plan with a total-order claim in
//! production and the wrap is what makes it page correctly: the window runs
//! inside the derived table over all the statement's rows while the keyset
//! predicate filters outside it, so `count(*) OVER ()` reports the same total
//! on every page rather than counting down as the walk advances. A statement
//! whose result depends on rows a keyset filter would remove and which the
//! planner cannot page is asserted as a refusal rather than omitted.
//!
//! Walking them is not what makes them carry their weight, and for one round
//! they did not carry it: the walk compares a page against the same statement
//! run unpaged, so a window value that changed per page changes on both sides
//! and the sequence still matches. What pins them is
//! [`a_count_window_reports_the_whole_row_count_on_every_page`] and
//! [`a_rank_window_ranks_against_the_whole_row_set`], which assert the window
//! column BY VALUE over the walk. Those are what fail when the keyset
//! predicate moves inside the derived table, and they fail on the window's own
//! numbers -- 7, 6, 5, 4, 3, 2, 1 for the count, and 1 seven times over for the
//! rank -- rather than through the parse error that same mutation happens to
//! raise first on the `DISTINCT ON` statement.
//!
//! Each of those two also asserts that its statement is still IN
//! [`STATEMENTS`]. Without that the entries pinned nothing on their own:
//! deleting both and adjusting the walked count left every test here green,
//! because the two that read the window column reach their statement through
//! a constant rather than through the table.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::{
    Array, ArrayRef, DictionaryArray, FixedSizeBinaryArray, Float64Array, Int8Array, Int16Array,
    Int32Array, Int64Array, MapArray, MapBuilder, RecordBatch, StringArray, StringBuilder,
    TimestampNanosecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use datafusion::arrow::datatypes::{DataType, Field, Int32Type, Schema};
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::datasource::MemTable;
use datafusion::execution::context::SessionContext;
use datafusion::prelude::SessionConfig;
use datafusion::sql::parser::{DFParser, Statement as DFStatement};
use datafusion::sql::sqlparser::ast::{
    Expr as SqlExpr, Ident, OrderBy, OrderByExpr, OrderByKind, OrderByOptions, Statement,
};
use ravel_catalog::Snapshot;
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_query::LogSegmentFetcher;
use ravel_sql::{
    ALERTS_TABLE, AlertsTableProvider, CeilingBreach, LOGS_TABLE, NotTotalOrder, PagePlan,
    PagePlanError, ResumePosition, ResumeValue, SAMPLES_TABLE, SPANS_TABLE, SessionPrincipal,
    SessionTable, SpillDecision, SqlConfig, SqlError, TenantDelegatingPool, TenantMemoryAccountant,
    alerts_schema, build_session, logs_schema, plan_page, public_schema, resume_value,
    spans_schema,
};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;

/// The `ts` the two tied rows share.
const TIED_TS: i64 = 1_000;

/// The `value` the two tied rows share. Non-zero, so `nullif(value, 0)` is
/// this same number rather than NULL for both.
const TIED_VALUE: f64 = 1.0;

/// The `series_id` two rows carry under distinct `ts`, so `series_id` alone is
/// not a key over this fixture. It is also the lower half of the tied pair,
/// which is stored second.
const REPEATED_SERIES: [u8; 16] = [1u8; 16];

/// Its hex rendering, spelled out rather than recomputed, so an assertion pins
/// the literal text a walk returns.
const REPEATED_SERIES_HEX: &str = "01010101010101010101010101010101";

/// The upper half of the tied pair, stored FIRST, so the pair's insertion
/// order is the reverse of its tiebreak order.
const TIED_HIGH_SERIES: [u8; 16] = [2u8; 16];

/// Its hex rendering. See [`REPEATED_SERIES_HEX`].
const TIED_HIGH_SERIES_HEX: &str = "02020202020202020202020202020202";

/// The `value` that a projected `nullif(value, 0)` turns into NULL.
const NULLING_VALUE: f64 = 0.0;

/// A `value` that sorts before [`NULLING_VALUE`] under DataFusion's total
/// float sort order and that `nullif(value, 0)` leaves alone, though
/// `value = 0` is TRUE for it.
const NEGATIVE_ZERO: f64 = -0.0;

/// A `ts` distinct from [`TIED_TS`], on the row carrying [`NULLING_VALUE`].
const LONE_TS: i64 = 2_000;

/// The `ts` of the second row carrying [`REPEATED_SERIES`]. Larger than
/// [`TIED_TS`], so under `ORDER BY series_id` the two rows of that series are
/// adjacent and a keyset predicate over `series_id` alone skips the second.
const REPEATED_SERIES_TS: i64 = 4_000;

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
/// Seven rows at a cap of one needs eight pages including the empty last one;
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

/// The seven-row fixture, in insertion order, which is deliberately not the
/// order any statement below asks for.
const FIXTURE: &[FixtureRow] = &[
    // The tied pair, stored in DESCENDING `series_id` order: a wrapper
    // ordering that drops `series_id` returns these two the other way round
    // from the reference, rather than agreeing with it by luck.
    FixtureRow {
        ts: TIED_TS,
        value: TIED_VALUE,
        series_id: TIED_HIGH_SERIES,
    },
    FixtureRow {
        ts: TIED_TS,
        value: TIED_VALUE,
        series_id: REPEATED_SERIES,
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
    // Shares `REPEATED_SERIES` with the second row under a different `ts`, so
    // a row identity of `series_id` alone ties here and loses a row.
    FixtureRow {
        ts: REPEATED_SERIES_TS,
        value: 2.0,
        series_id: REPEATED_SERIES,
    },
];

/// One row, so a walk's second page is the empty one.
const ONE_ROW: &[FixtureRow] = &[FixtureRow {
    ts: TIED_TS,
    value: TIED_VALUE,
    series_id: REPEATED_SERIES,
}];

/// No rows, so a walk's first page is the empty one.
const NO_ROWS: &[FixtureRow] = &[];

/// One `logs` fixture row. Every other column of the public `logs` schema is
/// constant or NULL across the fixture: what this table is here for is the
/// absence of a row identity, not the breadth of its schema.
#[derive(Clone, Copy)]
struct LogRow {
    ts: i64,
    severity_num: u8,
    body: &'static str,
}

/// The `ts` the duplicate `logs` rows share.
const LOG_DUPLICATE_TS: i64 = 1_000;

/// The body the duplicate `logs` rows share.
const LOG_DUPLICATE_BODY: &str = "the record that arrived twice";

/// The `logs` fixture: three rows, two of which are equal in EVERY column.
///
/// That duplicate is the reason `logs` has no row identity (ingest is
/// at-least-once and nothing above the RLOG scan dedups), so it is the thing a
/// planner that wrongly claimed one would lose: no ordering over these columns
/// separates the pair, and a strict keyset predicate positioned at either one
/// excludes both.
const LOG_FIXTURE: &[LogRow] = &[
    LogRow {
        ts: LOG_DUPLICATE_TS,
        severity_num: 9,
        body: LOG_DUPLICATE_BODY,
    },
    LogRow {
        ts: LOG_DUPLICATE_TS,
        severity_num: 9,
        body: LOG_DUPLICATE_BODY,
    },
    LogRow {
        ts: 2_000,
        severity_num: 17,
        body: "a later record",
    },
];

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

/// `rows` as a `logs` batch, over the public `logs` schema in its field order.
fn logs_batch(rows: &[LogRow]) -> RecordBatch {
    let ts = TimestampNanosecondArray::from(rows.iter().map(|row| row.ts).collect::<Vec<i64>>());
    let observed_ts =
        TimestampNanosecondArray::from(rows.iter().map(|row| row.ts).collect::<Vec<i64>>());
    let severity_num =
        UInt8Array::from(rows.iter().map(|row| row.severity_num).collect::<Vec<u8>>());
    let severity_text = StringArray::from(rows.iter().map(|_| "INFO").collect::<Vec<&str>>());
    let body = StringArray::from(rows.iter().map(|row| row.body).collect::<Vec<&str>>());
    // Both id columns are nullable on the public schema, and an absent id is a
    // NULL cell: the fixture carries no trace context at all.
    let trace_id = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
        rows.iter().map(|_| None::<[u8; 16]>),
        16,
    )
    .expect("trace id array");
    let span_id = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
        rows.iter().map(|_| None::<[u8; 8]>),
        8,
    )
    .expect("span id array");
    let flags = UInt32Array::from(rows.iter().map(|_| 0u32).collect::<Vec<u32>>());

    let mut maps = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    for _ in rows {
        maps.append(true).expect("attrs map append");
    }
    let attrs: MapArray = maps.finish();

    RecordBatch::try_new(
        logs_schema(),
        vec![
            Arc::new(ts),
            Arc::new(observed_ts),
            Arc::new(severity_num),
            Arc::new(severity_text),
            Arc::new(body),
            Arc::new(trace_id),
            Arc::new(span_id),
            Arc::new(flags),
            Arc::new(attrs),
        ],
    )
    .expect("logs fixture batch")
}

/// A context with `rows` registered as `samples` and [`LOG_FIXTURE`]
/// registered as `logs`.
///
/// The `logs` table does not vary with `rows`: it is the no-row-identity
/// target, and what it contributes is the duplicate row, not a row count that
/// tracks the `samples` fixture.
///
/// One target partition, so `collect` returns the sorted rows in the order the
/// `ORDER BY` produced them and "the first row of the result" is the row a
/// page cap of one would keep. Multi-partition execution is deliberately out
/// of scope here: with more than one partition a statement with no total order
/// has no single answer to compare a walk against, so the pinned partition is
/// what makes "page k holds rows k of the ordering" a statement at all. A
/// harness for the multi-partition case is separate work, not a widening of
/// this one.
fn context_of(rows: &[FixtureRow]) -> SessionContext {
    let ctx = SessionContext::new_with_config(SessionConfig::new().with_target_partitions(1));
    let samples =
        MemTable::try_new(public_schema(), vec![vec![samples_batch(rows)]]).expect("mem table");
    ctx.register_table(SAMPLES_TABLE, Arc::new(samples))
        .expect("registered samples");
    let logs = MemTable::try_new(logs_schema(), vec![vec![logs_batch(LOG_FIXTURE)]])
        .expect("logs mem table");
    ctx.register_table(LOGS_TABLE, Arc::new(logs))
        .expect("registered logs");
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
///
/// The appending is done in the AST and rendered back, not spliced into the
/// text. A reference assembled by string concatenation is independent of the
/// plan only for statements whose text happens to end where the concatenation
/// assumed: a term list appended after a trailing comment, inside a quoted
/// alias, or onto an `ORDER BY` the scan for one did not find lands somewhere
/// other than the ordering, and the resulting reference is wrong in the same
/// direction as a plan that misread the same text. Parsing is what makes the
/// two readings independent.
fn reference_statement(sql: &str, plan: &PagePlan) -> String {
    if plan.tiebreak_appended.is_empty() {
        return sql.to_string();
    }
    let mut statements = DFParser::parse_sql(sql)
        .unwrap_or_else(|e| panic!("the reference for {sql:?} does not parse: {e}"));
    let Some(DFStatement::Statement(statement)) = statements.pop_front() else {
        panic!("the reference for {sql:?} is not one plain SQL statement");
    };
    let Statement::Query(mut query) = *statement else {
        panic!("the reference for {sql:?} is not a query");
    };

    let appended: Vec<OrderByExpr> = plan
        .tiebreak_appended
        .iter()
        .map(|column| OrderByExpr {
            expr: SqlExpr::Identifier(Ident::with_quote('"', column.as_str())),
            options: OrderByOptions {
                asc: Some(true),
                nulls_first: None,
            },
            with_fill: None,
        })
        .collect();
    match query.order_by.as_mut() {
        Some(OrderBy {
            kind: OrderByKind::Expressions(exprs),
            ..
        }) => exprs.extend(appended),
        // `ORDER BY ALL` has no term list to append to, and `plan_page` refuses
        // it outright, so a plan that reached here carrying a tiebreak over one
        // is a finding rather than a case to spell.
        Some(OrderBy { kind, .. }) => {
            panic!("the reference for {sql:?} orders by {kind:?}, which has no term list")
        }
        None => {
            query.order_by = Some(OrderBy {
                kind: OrderByKind::Expressions(appended),
                interpolate: None,
            });
        }
    }
    query.to_string()
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
            // A NULL is not a resume value, so a walk that reaches one cannot
            // continue. That is itself a finding, reported rather than treated
            // as the walk finishing.
            if column.is_null(offset) {
                page_rows.push(page);
                return Walk {
                    page_rows,
                    pages,
                    stopped_at_null: Some(term.column.clone()),
                };
            }
            // `None` on a non-NULL cell is reachable only from a plan that
            // claimed a total order over a term no cursor can carry, which
            // `plan_page` refuses (`OrderTermNotRepresentable`).
            let value = resume_value(column.as_ref(), offset).unwrap_or_else(|| {
                panic!(
                    "no resume value for order term {:?} of type {}",
                    term.column,
                    column.data_type(),
                )
            });
            tuple.push(value);
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
///
/// There is no short-page check here, because a short page cannot occur: the
/// page statement carries no row cap of its own, so [`walk`] imposes
/// [`PAGE_CAP`] with its own `take`, and a page that would have been short is
/// the empty one that ends the walk. A page that returns too FEW rows shows up
/// in the final length comparison below instead, as a walk that delivered
/// fewer rows than the ordering holds.
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

/// The `DISTINCT ON` statement, which asks for the LAST sample of each series.
///
/// Its own `ORDER BY` is what chooses the surviving row of each group, so it
/// is the one statement here whose answer changes when the wrap drops that
/// ordering. Over [`FIXTURE`] the choice is observable on exactly one series:
/// [`REPEATED_SERIES`] carries `ts` [`TIED_TS`] and [`REPEATED_SERIES_TS`],
/// and this statement asks for the second.
const DISTINCT_ON_STATEMENT: &str =
    "SELECT DISTINCT ON (series_id) ts, series_id FROM samples ORDER BY series_id, ts DESC";

/// The walked statement whose TEXT does not survive naive concatenation, so it
/// is what makes [`reference_statement`]'s parse load-bearing.
///
/// A `--` comment runs to the end of the line, so everything appended after
/// this statement's text is inside it. The statement carries no `ORDER BY`, so
/// the reference has a whole `ORDER BY "ts" ASC, "series_id" ASC` to append,
/// and a concatenating builder puts all of it in the comment: the reference is
/// then the statement itself, unordered, which is [`FIXTURE`]'s deliberately
/// unsorted insertion order rather than any ordering the walk produces.
///
/// Chosen over a trailing comment on a statement that DOES carry an `ORDER BY`
/// because that form would leave the reference ordered by the caller's own
/// terms and differ from the walk only on the tied pair, which puts the
/// assertion at the mercy of whether a sort happens to be stable. Here the
/// reference and the walk disagree on the first row.
const COMMENT_TERMINATED_STATEMENT: &str =
    "SELECT ts, series_id FROM samples -- the ordering is appended after this, not into it";

/// A window whose value is a property of the WHOLE statement's row set: the
/// count of every row it returns, which is [`FIXTURE`]'s seven on every page.
const WINDOW_COUNT_STATEMENT: &str =
    "SELECT ts, series_id, count(*) OVER () AS n FROM samples ORDER BY ts, series_id";

/// A window whose value is each row's position in the whole statement's row
/// set. Its `ORDER BY ts` ties the two rows at [`TIED_TS`], so the ranks it
/// produces are not the row numbers and a walk cannot reproduce them by
/// counting pages.
const WINDOW_RANK_STATEMENT: &str =
    "SELECT ts, series_id, rank() OVER (ORDER BY ts) AS rk FROM samples ORDER BY ts, series_id";

/// What [`WINDOW_COUNT_STATEMENT`] reports on every one of its rows: the
/// fixture's row count, spelled out rather than derived from `FIXTURE.len()`,
/// so a page that reports the count of its own filtered input cannot agree
/// with it by following the same fixture edit.
const WINDOW_COUNTS: [&str; 7] = ["7", "7", "7", "7", "7", "7", "7"];

/// What [`WINDOW_RANK_STATEMENT`] reports, in the order its own `ORDER BY ts,
/// series_id` returns the rows: `ts` ascending is 500, 1000, 1000, 2000, 3000,
/// 4000, 1234567, and the pair tied at 1000 takes rank 2 twice and leaves rank
/// 3 unused.
const WINDOW_RANKS: [&str; 7] = ["1", "2", "2", "4", "5", "6", "7"];

/// A `DESC` term with no tiebreak of its own: the planner appends
/// `series_id ASC`, so the effective ordering runs its two terms in opposite
/// directions and the keyset predicate has to compare each term in its own.
/// Over [`FIXTURE`] the pair tied at [`TIED_TS`] is where the two directions
/// meet: the walk crosses it from [`REPEATED_SERIES`] to [`TIED_HIGH_SERIES`].
const DESC_TIEBREAK_STATEMENT: &str = "SELECT ts, series_id FROM samples ORDER BY ts DESC";

/// [`DESC_TIEBREAK_STATEMENT`]'s `series_id` column in its ordering over
/// [`FIXTURE`]: `ts` descending is 1234567, 4000, 3000, 2000, 1000, 1000,
/// 500, and the pair at 1000 runs in ascending `series_id`. Spelled out
/// rather than read off the reference, so the walk is checked against the
/// ordering by value and not only against another run of the same statement.
const DESC_TIEBREAK_SERIES: [&str; 7] = [
    "04040404040404040404040404040404",
    REPEATED_SERIES_HEX,
    "05050505050505050505050505050505",
    "03030303030303030303030303030303",
    REPEATED_SERIES_HEX,
    TIED_HIGH_SERIES_HEX,
    "09090909090909090909090909090909",
];

/// The statements this suite walks, plus the shapes that must keep walking so
/// a fix cannot be a blanket refusal.
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
    // The statement whose text does not survive concatenation. See
    // [`COMMENT_TERMINATED_STATEMENT`].
    COMMENT_TERMINATED_STATEMENT,
    "SELECT ts, series_id FROM samples ORDER BY ts",
    "SELECT ts, series_id, value FROM samples ORDER BY ts DESC, series_id",
    // A DESC term the planner appends an ASC tiebreak to. See
    // [`DESC_TIEBREAK_STATEMENT`].
    DESC_TIEBREAK_STATEMENT,
    "SELECT ts, series_id FROM samples ORDER BY series_id",
    "SELECT * FROM samples WHERE value > 0.5 ORDER BY ts",
    // An order term whose output name is a reserved word under a quoted alias
    // that is not all lowercase. The rewrite quotes every name it emits, and
    // this is what asserts that behaviourally rather than by reading the
    // rendered text: emitted bare, `ORDER BY Select ASC` re-parses as syntax
    // and the page does not plan at all.
    "SELECT ts, series_id, ts AS \"Select\" FROM samples ORDER BY \"Select\", ts, series_id",
    // Two window statements. Both plan with a total-order claim, and both are
    // correct only because the keyset predicate sits OUTSIDE the derived
    // table: the window sees all the statement's rows on every page. What each
    // one reports per page is asserted by value in
    // [`a_count_window_reports_the_whole_row_count_on_every_page`] and
    // [`a_rank_window_ranks_against_the_whole_row_set`], and each of those
    // asserts its own membership here; this is where they are walked for
    // their ordering like any other statement.
    WINDOW_COUNT_STATEMENT,
    WINDOW_RANK_STATEMENT,
    // Refused now rather than walked, both for a term whose values no cursor
    // carries. `value` is a float, so it admits the NaN that
    // `ResumeValue::Float` refuses; `labels` is a `Dictionary` over a `Map`,
    // which no variant carries at all. Kept in the table so both refusals are
    // exercised on the same path the walks take: a planner that admits either
    // one reaches the cursor mint and dies there.
    "SELECT ts, series_id, value FROM samples ORDER BY value DESC, ts",
    "SELECT * FROM samples ORDER BY labels, ts, series_id",
    // The no-row-identity target. Not walked today: the planner reports
    // `NoRowIdentity` for `logs`, so these are outside the total-order claim.
    // They are here for the reverse direction, which no plan assertion can
    // make: a planner that starts claiming a total order over `logs` walks
    // these, and `LOG_FIXTURE`'s duplicate row then appears on no page.
    "SELECT ts, body FROM logs ORDER BY ts",
    "SELECT ts, severity_num, body FROM logs ORDER BY ts DESC, severity_num",
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
        ("the seven-row fixture", FIXTURE),
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
    // Eleven per fixture, three fixtures. Pinned exactly rather than as
    // `walked > 0`: a change that drops a statement out of the total-order
    // class leaves this gate passing over a smaller set, which is the way a
    // suite of this shape goes quiet without going red.
    //
    // What the 33 is and is not: see the module docs. Eleven of the walks are
    // over seven rows and are what every mutation caught so far was caught by;
    // the other twenty-two are over a one-row and a zero-row table, and what
    // they exercise is the walk's page boundaries, not any ordering.
    assert_eq!(
        walked, 33,
        "the walked set changed size, so this gate is asserting about a different \
         set of statements than the one it was sized for",
    );
}

/// A one-row table ends the walk on an empty SECOND page, and a zero-row table
/// on an empty FIRST one.
///
/// Both are page counts the seven-row fixture cannot produce, and both are the
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

/// `count(*) OVER ()` reports the whole statement's row count on every page.
///
/// The property the walk itself cannot assert, and the reason the window
/// statements are in [`STATEMENTS`] at all. The walk compares each page
/// against the same statement run unpaged, so a window value that changed per
/// page would have to disturb the ORDERING as well to be visible there. It
/// does not: the rows come back in the same sequence carrying a different
/// number, and the number is what the caller reads.
///
/// So the values are asserted here, by value, as the sequence the pages
/// deliver them in. That is what makes the keyset predicate's POSITION
/// load-bearing rather than incidental: the rewrite puts it outside the
/// derived table, so the window is computed over all seven rows on every page.
/// Moved inside, it counts the rows that survived the cursor filter and
/// reports 7, 6, 5, 4, 3, 2, 1 down the walk.
///
/// Asserted directly rather than through [`STATEMENTS`] because that loop
/// reaches a window statement only after the `DISTINCT ON` one, whose
/// preserved inner `ORDER BY` makes the same mutation a PARSE error first: a
/// suite that only caught it there would be reporting a syntax failure on an
/// unrelated statement, which says nothing about where a window is computed.
///
/// Membership in [`STATEMENTS`] is asserted here rather than left to the
/// count in [`a_total_order_claim_survives_an_executed_page_walk`]: that count
/// moves with any edit to the table, so deleting the entry and adjusting the
/// count left the whole suite green while nothing walked a window statement.
#[tokio::test]
async fn a_count_window_reports_the_whole_row_count_on_every_page() {
    assert!(
        STATEMENTS.contains(&WINDOW_COUNT_STATEMENT),
        "the count window statement left STATEMENTS, so no walk covers its ordering",
    );
    let found = walk(&context(), WINDOW_COUNT_STATEMENT).await;
    assert_eq!(found.pages, 8, "pages of the count window walk");
    assert_eq!(
        window_column(&found),
        WINDOW_COUNTS,
        "count(*) OVER () did not report the statement's whole row count on \
         every page, so the keyset predicate reached the rows the window was \
         computed over",
    );
}

/// `rank() OVER (ORDER BY ts)` ranks each row against the whole statement's
/// row set.
///
/// The same property as
/// [`a_count_window_reports_the_whole_row_count_on_every_page`] read through a
/// window whose value is per row rather than per result, so a page that
/// recomputed it over its own rows alone would report a plausible-looking 1
/// rather than an obviously shrinking total. [`WINDOW_RANKS`] is the sequence
/// only the whole row set produces: it skips 3, because the pair tied at
/// [`TIED_TS`] takes rank 2 twice.
///
/// Its membership in [`STATEMENTS`] is asserted for the same reason as the
/// count window's.
#[tokio::test]
async fn a_rank_window_ranks_against_the_whole_row_set() {
    assert!(
        STATEMENTS.contains(&WINDOW_RANK_STATEMENT),
        "the rank window statement left STATEMENTS, so no walk covers its ordering",
    );
    let found = walk(&context(), WINDOW_RANK_STATEMENT).await;
    assert_eq!(found.pages, 8, "pages of the rank window walk");
    assert_eq!(
        window_column(&found),
        WINDOW_RANKS,
        "rank() OVER (ORDER BY ts) did not rank each row against the \
         statement's whole row set, so the keyset predicate reached the rows \
         the window was computed over",
    );
}

/// A `DESC` term under an appended `ASC` tiebreak walks to the end over the
/// fixture's tied pair with every row on exactly one page, in the ordering's
/// sequence.
///
/// FLIP: with `keyset_predicate`'s `let operator = if term.descending { "<" }
/// else { ">" };` reading `terms[0].descending` in place of
/// `term.descending`, every term is compared in the first term's direction:
/// the page after [`REPEATED_SERIES`] at [`TIED_TS`] asks for a smaller
/// `series_id` at that `ts`, so [`TIED_HIGH_SERIES`] appears on no page and
/// the walk delivers six rows.
#[tokio::test]
async fn a_desc_term_under_an_appended_tiebreak_walks_every_row_once_in_order() {
    assert!(
        STATEMENTS.contains(&DESC_TIEBREAK_STATEMENT),
        "the DESC tiebreak statement left STATEMENTS, so the gate walk no longer covers it",
    );
    let plan = plan_page(DESC_TIEBREAK_STATEMENT, None).expect("planned");
    assert_eq!(
        plan.order_by
            .iter()
            .map(|term| term.render())
            .collect::<Vec<String>>(),
        ["\"ts\" DESC", "\"series_id\" ASC"],
        "the effective ordering",
    );
    assert_eq!(plan.tiebreak_appended, ["series_id"]);
    assert_eq!(plan.not_total, None);

    let ctx = context();
    let found = walk(&ctx, DESC_TIEBREAK_STATEMENT).await;
    let rows = found.rows();
    let unpaged = rows_of(&execute(&ctx, DESC_TIEBREAK_STATEMENT).await);
    assert_eq!(
        unpaged.len(),
        FIXTURE.len(),
        "the statement's own row count"
    );
    assert!(
        multiset(&unpaged).values().all(|&count| count == 1),
        "the fixture has no duplicate (ts, series_id) row",
    );
    assert_eq!(
        multiset(&rows),
        multiset(&unpaged),
        "the walk did not deliver every row exactly once",
    );
    assert_eq!(
        found.pages,
        FIXTURE.len() + 1,
        "pages, the empty last one included"
    );
    assert_eq!(
        rows.iter()
            .map(|row| row[1].as_str())
            .collect::<Vec<&str>>(),
        DESC_TIEBREAK_SERIES,
        "the walk's series_id sequence",
    );
    let reference =
        rows_of(&execute(&ctx, &reference_statement(DESC_TIEBREAK_STATEMENT, &plan)).await);
    assert_eq!(sequence_finding(&reference, &found), None);
}

/// A statement ordered first by `-9223372036854775808`, which DataFusion types
/// `Int64` by its signed value.
const NEGATED_MIN_STATEMENT: &str =
    "SELECT ts, series_id, -9223372036854775808 AS neg FROM samples ORDER BY neg, ts, series_id";

/// A negated literal term takes the kind of the value DataFusion computes for
/// it, so the cursor minted from page one's last row the way `ravel_query_sql`
/// mints it (`resume_value` over the page's own output) plans page two, and
/// the walk delivers every row once in order.
///
/// FLIP: with `expression_cursor_support` stripping the minus before it
/// classifies the literal by its text, the term is proven `UInt` from the text
/// `9223372036854775808`, the page's `Int64` column mints `Int`, and page two
/// is refused with `ResumeValueTypeMismatch { position: 1, column: "neg",
/// expected: "UInt", found: "Int" }`.
#[tokio::test]
async fn a_negated_i64_min_term_pages_past_its_first_page() {
    let ctx = context();
    let first = plan_page(NEGATED_MIN_STATEMENT, None).expect("planned");
    assert_eq!(first.not_total, None);
    let batches = execute(&ctx, &first.statement).await;
    let (batch, offset) = row_at(&batches, 0);
    let column = batch.column_by_name("neg").expect("neg column");
    assert_eq!(
        resume_value(column.as_ref(), offset),
        Some(ResumeValue::Int(i64::MIN)),
    );
    let tuple = first
        .order_by
        .iter()
        .map(|term| {
            let column = batch.column_by_name(&term.column).expect("order column");
            resume_value(column.as_ref(), offset).expect("a resume value")
        })
        .collect::<Vec<_>>();
    assert_eq!(tuple.len(), 3, "{:?}", first.order_by);
    let second = plan_page(NEGATED_MIN_STATEMENT, Some(&ResumePosition::new(tuple)));
    assert!(second.is_ok(), "page two should plan, got {second:?}");

    let found = walk(&ctx, NEGATED_MIN_STATEMENT).await;
    assert_eq!(found.stopped_at_null, None);
    assert_eq!(
        found.pages,
        FIXTURE.len() + 1,
        "pages, the empty last one included"
    );
    let reference =
        rows_of(&execute(&ctx, &reference_statement(NEGATED_MIN_STATEMENT, &first)).await);
    assert_eq!(reference.len(), FIXTURE.len());
    assert_eq!(sequence_finding(&reference, &found), None);
}

/// The window column of a walk over [`WINDOW_COUNT_STATEMENT`] or
/// [`WINDOW_RANK_STATEMENT`]: the third projected column, in page order.
fn window_column(found: &Walk) -> Vec<String> {
    found
        .rows()
        .iter()
        .map(|row| row[2].clone())
        .collect::<Vec<String>>()
}

/// A `DISTINCT ON` page delivers the rows the caller's own statement delivers.
///
/// The statement's `ORDER BY` chooses WHICH row of each `ON` group survives,
/// so it is not a sort the wrap can drop and re-impose from outside: dropped,
/// the group keeps a different row and the page is a wrong answer rather than
/// a differently ordered one. The assertion is behavioural on purpose --
/// nothing here reads the rendered statement -- because the defect was a
/// delivered row, not a rendering.
///
/// The walk is asserted as well as the first page: with one row per `ON`
/// group, `series_id` is unique over the result, so a strict keyset predicate
/// over the effective ordering is sound and every page has to keep choosing
/// the same row of the group it lands in.
#[tokio::test]
async fn a_distinct_on_page_delivers_the_rows_the_statement_itself_returns() {
    let ctx = context();
    let plan = plan_page(DISTINCT_ON_STATEMENT, None).expect("planned");
    // `DISTINCT` does not preserve the scan's rows one-for-one, so the plan
    // says so. The fix is to page it correctly, not to claim a total order.
    assert_eq!(
        plan.not_total,
        Some(NotTotalOrder::ShapeNotIdentityPreserving { shape: "DISTINCT" }),
    );

    let direct = rows_of(&execute(&ctx, DISTINCT_ON_STATEMENT).await);
    // The series the choice is observable on: the one carrying two rows.
    assert_eq!(
        direct.first().map(Vec::as_slice),
        Some(
            [
                "1970-01-01T00:00:00.000004".to_string(),
                REPEATED_SERIES_HEX.to_string(),
            ]
            .as_slice()
        ),
        "the statement itself does not return the LAST sample of the repeated \
         series, so this statement cannot show the ordering being dropped",
    );

    let paged = rows_of(&execute(&ctx, &plan.statement).await);
    assert_eq!(
        paged, direct,
        "the page statement delivered different rows than the caller's own statement",
    );

    let found = walk(&ctx, DISTINCT_ON_STATEMENT).await;
    assert_eq!(sequence_finding(&direct, &found), None);
    assert_eq!(found.rows(), direct, "the walked sequence");
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
    assert_eq!(rows.len(), 7, "fixture row count");

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

    // The tied pair's stored order is the reverse of its tiebreak order. A
    // wrapper `ORDER BY` that drops `series_id` sorts on `ts` alone, which
    // leaves these two in the order the scan produced them, and that has to be
    // the WRONG order for the walk to see it.
    let scan_order = rows_of(&execute(&ctx, "SELECT series_id FROM samples").await);
    assert_eq!(
        &scan_order[..2],
        &[
            vec![TIED_HIGH_SERIES_HEX.to_string()],
            vec![REPEATED_SERIES_HEX.to_string()],
        ],
        "the tied pair is not stored in descending series_id order, so dropping \
         the series_id term from the wrapper ordering returns the same sequence \
         anyway",
    );

    // One series carries two rows, so `series_id` alone is not a key here.
    let repeated_series = rows_of(
        &execute(
            &ctx,
            "SELECT count(*) AS n FROM (SELECT series_id FROM samples GROUP BY series_id \
             HAVING count(*) > 1)",
        )
        .await,
    );
    assert_eq!(
        repeated_series,
        vec![vec!["1".to_string()]],
        "series carrying more than one row",
    );
    let repeated_series_rows = rows_of(
        &execute(
            &ctx,
            "SELECT count(*) AS n FROM samples WHERE series_id = \
             arrow_cast(decode('01010101010101010101010101010101', 'hex'), \
             'FixedSizeBinary(16)')",
        )
        .await,
    );
    assert_eq!(
        repeated_series_rows,
        vec![vec!["2".to_string()]],
        "rows sharing one series_id",
    );

    // One, not two: `nullif` compares without normalising a zero's sign, so
    // only the positive zero is NULLed, although `value = 0` is TRUE for both
    // zero rows (the comparison below).
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
    let zeroes = rows_of(&execute(&ctx, "SELECT count(*) AS n FROM samples WHERE value = 0").await);
    assert_eq!(
        zeroes,
        vec![vec!["2".to_string()]],
        "rows `value = 0` holds for: both zero signs",
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
        vec![vec!["7".to_string()]],
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

    // The `logs` fixture's own property: two rows equal in every column, which
    // is what no ordering over the table can separate.
    let log_rows = rows_of(&execute(&ctx, "SELECT ts, severity_num, body FROM logs").await);
    assert_eq!(log_rows.len(), 3, "logs fixture row count");
    let log_duplicates = rows_of(
        &execute(
            &ctx,
            "SELECT count(*) AS n FROM (SELECT ts, observed_ts, severity_num, severity_text, \
             body, trace_id, span_id, flags FROM logs GROUP BY ts, observed_ts, severity_num, \
             severity_text, body, trace_id, span_id, flags HAVING count(*) > 1)",
        )
        .await,
    );
    assert_eq!(
        log_duplicates,
        vec![vec!["1".to_string()]],
        "groups of logs rows equal on every orderable column",
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
        "SELECT ts, series_id, ts AS \"Select\" FROM samples ORDER BY \"Select\", ts, series_id",
        COMMENT_TERMINATED_STATEMENT,
        WINDOW_COUNT_STATEMENT,
        WINDOW_RANK_STATEMENT,
    ];
    for sql in must_stay_total {
        let plan = plan_page(sql, None).unwrap_or_else(|e| panic!("{sql:?} refused: {e}"));
        assert_eq!(
            plan.not_total, None,
            "{sql:?} lost its total order, so the fix is a blanket refusal",
        );
    }
}

/// What the harness's second table is registered for: the statements over it
/// must never be claimed as a total order, and the ones whose result depends
/// on rows a keyset filter would remove must be refused or reported not-total
/// rather than paged.
///
/// This is the direction the walk itself cannot assert. The walk skips a
/// not-total plan by construction, so `logs` contributes nothing to it until a
/// planner starts claiming otherwise -- at which point these statements are
/// walked, over a fixture whose duplicate row no ordering separates.
#[test]
fn the_no_row_identity_target_is_never_claimed_as_a_total_order() {
    for sql in [
        "SELECT ts, body FROM logs ORDER BY ts",
        "SELECT ts, severity_num, body FROM logs ORDER BY ts DESC, severity_num",
    ] {
        let plan = plan_page(sql, None).unwrap_or_else(|e| panic!("{sql:?} refused: {e}"));
        assert_eq!(
            plan.not_total,
            Some(NotTotalOrder::NoRowIdentity { table: LOGS_TABLE }),
            "{sql:?}",
        );
        assert!(plan.tiebreak_appended.is_empty(), "{sql:?}");
    }

    // A result the keyset predicate would change if it reached the scanned
    // rows: every row of the input contributes to the one row out. There is
    // nothing to order by, so it is refused, and a refusal is a pass.
    assert!(
        matches!(
            plan_page("SELECT count(*) AS n FROM samples", None),
            Err(PagePlanError::NoOrdering {
                target: SAMPLES_TABLE
            })
        ),
        "an aggregate over every row was not refused",
    );
    // The grouped form has something to order by and is reported not-total,
    // which is the other acceptable answer: D5 pages it under the equal-group
    // rule, never under a strict keyset predicate.
    let grouped = plan_page(
        "SELECT ts, count(*) AS n FROM samples GROUP BY ts ORDER BY ts",
        None,
    )
    .expect("planned");
    assert_eq!(
        grouped.not_total,
        Some(NotTotalOrder::ShapeNotIdentityPreserving { shape: "GROUP BY" }),
    );
}

/// The `samples` fixture the `max` order walks: six series whose latest `ts`
/// values are 300, 300, 300, 250, 400 and 150, so three groups tie on the
/// aggregate and the tie crosses the first page boundary at a cap of two. Each
/// series carries its own `value`, so grouping by `value` groups by series too.
const MAX_FIXTURE: &[FixtureRow] = &[
    FixtureRow {
        ts: 100,
        value: 1.0,
        series_id: [1u8; 16],
    },
    FixtureRow {
        ts: 300,
        value: 1.0,
        series_id: [1u8; 16],
    },
    FixtureRow {
        ts: 300,
        value: 2.0,
        series_id: [2u8; 16],
    },
    FixtureRow {
        ts: 200,
        value: 3.0,
        series_id: [3u8; 16],
    },
    FixtureRow {
        ts: 300,
        value: 3.0,
        series_id: [3u8; 16],
    },
    FixtureRow {
        ts: 250,
        value: 4.0,
        series_id: [4u8; 16],
    },
    FixtureRow {
        ts: 400,
        value: 5.0,
        series_id: [5u8; 16],
    },
    FixtureRow {
        ts: 50,
        value: 6.0,
        series_id: [6u8; 16],
    },
    FixtureRow {
        ts: 150,
        value: 6.0,
        series_id: [6u8; 16],
    },
];

/// The walked `max` order. `series_id` follows the aggregate so the three
/// groups tied at 300 have a fixed order inside the tie.
const MAX_ORDER_STATEMENT: &str = "SELECT series_id, max(ts) AS m FROM samples \
     GROUP BY series_id ORDER BY m DESC, series_id";

/// The row cap of a grouped walk, as the tool's `max_rows`.
const GROUPED_PAGE_CAP: usize = 2;

/// Page `sql` the way `ravel_query_sql` pages an order that is not total: run
/// the page statement, keep `cap + 1` rows, hold back the trailing group of
/// rows equal under the order when the extra row continues it, and resume
/// after the last kept row. Each resume tuple is checked against the plan the
/// page ran under, as the tool's cursor mint checks it, so a tuple the next
/// page would refuse fails on the page that produced it.
async fn grouped_walk(ctx: &SessionContext, sql: &str, cap: usize) -> Vec<Vec<Vec<String>>> {
    let mut pages: Vec<Vec<Vec<String>>> = Vec::new();
    let mut resume: Option<ResumePosition> = None;
    loop {
        assert!(
            pages.len() < MAX_PAGES,
            "grouped walk of {sql:?} did not terminate within {MAX_PAGES} pages",
        );
        let plan = plan_page(sql, resume.as_ref())
            .unwrap_or_else(|e| panic!("page {} of {sql:?} would not plan: {e}", pages.len() + 1));
        let batches = execute(ctx, &plan.statement).await;
        let rendered = rows_of(&batches);
        if rendered.is_empty() {
            return pages;
        }
        let tuple_at = |index: usize| -> Vec<ResumeValue> {
            let (batch, offset) = row_at(&batches, index);
            plan.order_by
                .iter()
                .map(|term| {
                    let column = batch.column_by_name(&term.column).unwrap_or_else(|| {
                        panic!("page of {sql:?} does not carry {:?}", term.column)
                    });
                    assert!(
                        !column.is_null(offset),
                        "a NULL {:?} in {sql:?}",
                        term.column
                    );
                    resume_value(column.as_ref(), offset).unwrap_or_else(|| {
                        panic!(
                            "no resume value for {:?} of type {}",
                            term.column,
                            column.data_type()
                        )
                    })
                })
                .collect()
        };
        let mut kept = rendered.len().min(cap);
        if rendered.len() > cap {
            let boundary = tuple_at(cap);
            while kept > 0 && tuple_at(kept - 1) == boundary {
                kept -= 1;
            }
            assert!(
                kept > 0,
                "no complete group of {sql:?} fits a page of {cap}"
            );
        }
        let tuple = tuple_at(kept - 1);
        assert!(
            plan.admits_resume(&tuple),
            "the last row of page {} of {sql:?} is {tuple:?}, which the plan it ran under \
             would refuse as a resume position",
            pages.len() + 1,
        );
        pages.push(rendered.into_iter().take(kept).collect());
        resume = Some(ResumePosition::new(tuple));
    }
}

/// A grouped `max` order walks three full pages and an empty one, with the
/// three groups tied at 300 split across the first boundary, and delivers
/// exactly the unpaged result in its order, each group once.
#[tokio::test]
async fn max_order_walks_three_pages_exactly() {
    let ctx = context_of(MAX_FIXTURE);
    let plan = plan_page(MAX_ORDER_STATEMENT, None).expect("planned");
    assert_eq!(
        plan.not_total,
        Some(NotTotalOrder::ShapeNotIdentityPreserving { shape: "GROUP BY" }),
    );
    assert!(plan.order_from_other_rows, "a max order reads other rows");

    let series = |n: u8| format!("{n:02x}").repeat(16);
    let row = |n: u8, m: &str| vec![series(n), format!("1970-01-01T00:00:00.{m}")];
    let expected = vec![
        vec![row(5, "000000400"), row(1, "000000300")],
        vec![row(2, "000000300"), row(3, "000000300")],
        vec![row(4, "000000250"), row(6, "000000150")],
    ];

    let unpaged = rows_of(&execute(&ctx, MAX_ORDER_STATEMENT).await);
    assert_eq!(unpaged, expected.concat(), "the unpaged ordering");

    let pages = grouped_walk(&ctx, MAX_ORDER_STATEMENT, GROUPED_PAGE_CAP).await;
    assert_eq!(pages, expected, "the walked pages");
    let walked: Vec<Vec<String>> = pages.concat();
    assert_eq!(
        walked, unpaged,
        "the walk's union against the unpaged result"
    );
    assert!(
        multiset(&walked).values().all(|&count| count == 1),
        "a group was delivered twice: {walked:?}",
    );
}

/// An aggregate argument that shares its name with an output alias reads the
/// FROM column, not the alias: `max(ts)` beside `value AS ts` is the latest
/// sample time per value, a timestamp, never a float. The planner proves the
/// argument off the schema on that reading, so the executed answer has to
/// agree with it.
#[tokio::test]
async fn an_aggregate_argument_named_like_an_alias_reads_the_from_column() {
    let ctx = context_of(MAX_FIXTURE);
    let sql = "SELECT value AS ts, max(ts) AS m FROM samples GROUP BY value ORDER BY m DESC";
    let batches = execute(&ctx, sql).await;
    let schema = batches.first().expect("one batch").schema();
    assert_eq!(
        schema.field_with_name("m").expect("m").data_type(),
        &datafusion::arrow::datatypes::DataType::Timestamp(
            datafusion::arrow::datatypes::TimeUnit::Nanosecond,
            None,
        ),
    );
    let m: Vec<String> = rows_of(&batches)
        .into_iter()
        .map(|row| row[1].clone())
        .collect();
    assert_eq!(
        m,
        [
            "1970-01-01T00:00:00.000000400",
            "1970-01-01T00:00:00.000000300",
            "1970-01-01T00:00:00.000000300",
            "1970-01-01T00:00:00.000000300",
            "1970-01-01T00:00:00.000000250",
            "1970-01-01T00:00:00.000000150",
        ],
    );
}

/// A context with table `t`: a Utf8 key `k` beside the integer `columns`,
/// every row in its own partition, executed on `target_partitions`
/// partitions. One row per partition makes a grouped sum over more than one
/// row merge partial sums whenever the engine runs more than one partition.
///
/// The `sum` this context registers is DataFusion's built-in, which wraps.
/// Ravel's SQL session replaces it with the checked UDAF (issue #3838), so a
/// test of what a session pages builds its context with
/// [`checked_sum_context`] instead.
fn integer_table_context(
    keys: &[&str],
    columns: Vec<(&str, ArrayRef)>,
    target_partitions: usize,
) -> SessionContext {
    let mut fields = vec![Field::new("k", DataType::Utf8, false)];
    let mut arrays: Vec<ArrayRef> = vec![Arc::new(StringArray::from(keys.to_vec()))];
    for (name, array) in columns {
        fields.push(Field::new(name, array.data_type().clone(), false));
        arrays.push(array);
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), arrays).expect("integer batch");
    let partitions = (0..batch.num_rows())
        .map(|row| vec![batch.slice(row, 1)])
        .collect();
    let ctx = SessionContext::new_with_config(
        SessionConfig::new().with_target_partitions(target_partitions),
    );
    let table = MemTable::try_new(schema, partitions).expect("integer mem table");
    ctx.register_table("t", Arc::new(table))
        .expect("registered t");
    ctx
}

/// DataFusion's `sum` over a signed integer argument of any width is
/// `Int64`, and over an unsigned one `UInt64`: each column holds its type's
/// maximum and 1 in one group, and every narrower sum is exact past its
/// argument's own range.
#[tokio::test]
async fn an_integer_sum_is_int64_for_a_signed_and_uint64_for_an_unsigned_argument() {
    let ctx = integer_table_context(
        &["a", "a"],
        vec![
            ("i8", Arc::new(Int8Array::from(vec![i8::MAX, 1]))),
            ("i16", Arc::new(Int16Array::from(vec![i16::MAX, 1]))),
            ("i32", Arc::new(Int32Array::from(vec![i32::MAX, 1]))),
            ("i64", Arc::new(Int64Array::from(vec![i64::MAX - 1, 1]))),
            ("u8", Arc::new(UInt8Array::from(vec![u8::MAX, 1]))),
            ("u16", Arc::new(UInt16Array::from(vec![u16::MAX, 1]))),
            ("u32", Arc::new(UInt32Array::from(vec![u32::MAX, 1]))),
            ("u64", Arc::new(UInt64Array::from(vec![u64::MAX - 1, 1]))),
        ],
        1,
    );
    let sql = "SELECT sum(i8) AS i8, sum(i16) AS i16, sum(i32) AS i32, sum(i64) AS i64, \
               sum(u8) AS u8, sum(u16) AS u16, sum(u32) AS u32, sum(u64) AS u64 \
               FROM t GROUP BY k";
    let batches = execute(&ctx, sql).await;
    let schema = batches.first().expect("one batch").schema();
    let types: Vec<(&str, &DataType)> = schema
        .fields()
        .iter()
        .map(|field| (field.name().as_str(), field.data_type()))
        .collect();
    assert_eq!(
        types,
        [
            ("i8", &DataType::Int64),
            ("i16", &DataType::Int64),
            ("i32", &DataType::Int64),
            ("i64", &DataType::Int64),
            ("u8", &DataType::UInt64),
            ("u16", &DataType::UInt64),
            ("u32", &DataType::UInt64),
            ("u64", &DataType::UInt64),
        ],
    );
    assert_eq!(
        rows_of(&batches),
        [[
            "128",
            "32768",
            "2147483648",
            "9223372036854775807",
            "256",
            "65536",
            "4294967296",
            "18446744073709551615",
        ]],
    );
}

/// DataFusion's built-in integer `sum`, which the session's checked UDAF
/// replaces, wraps when it overflows its 64-bit result rather than
/// failing, at one partition and at eight alike: `i64::MAX + 1` is
/// `i64::MIN` and `u64::MAX + 1` is 0. Ordered by the sum, the group whose
/// true total is the largest sorts below a group summing to 5, so the order
/// is not the order of the totals.
#[tokio::test]
async fn an_integer_sum_wraps_on_overflow_at_one_and_eight_partitions() {
    let sql = "SELECT k, sum(s) AS s, sum(u) AS u FROM t GROUP BY k ORDER BY s DESC";
    for partitions in [1, 8] {
        let ctx = integer_table_context(
            &["a", "b", "a"],
            vec![
                ("s", Arc::new(Int64Array::from(vec![i64::MAX, 5, 1]))),
                ("u", Arc::new(UInt64Array::from(vec![u64::MAX, 5, 1]))),
            ],
            partitions,
        );
        assert_eq!(
            rows_of(&execute(&ctx, sql).await),
            [["b", "5", "5"], ["a", "-9223372036854775808", "0"]],
            "at {partitions} partitions",
        );
    }
}

/// A session built by `build_session` as the executor builds one, so `sum`
/// is the checked UDAF it registers, with `table` replaced by an in-memory
/// table of `columns`, each typed as `table`'s public schema types it, and
/// every row in its own partition. The partition count is the config's
/// `engine.sql_partition_count`, which `session_config` sets as the
/// session's `target_partitions`, and the session repartitions its final
/// aggregation, as the executor does for an exact-typed statement.
fn checked_sum_context(
    table: &str,
    columns: Vec<(&str, ArrayRef)>,
    target_partitions: usize,
) -> SessionContext {
    let mut config = SqlConfig::default();
    config.engine.sql_partition_count = Some(target_partitions);
    let pool = Arc::new(TenantDelegatingPool::new(
        1 << 30,
        TenantMemoryAccountant::new(1 << 30),
        CeilingBreach::new(),
        QueryAccounting::new(),
    ));
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let alerts = AlertsTableProvider::new(
        Snapshot {
            segments: Vec::new(),
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        },
        TenantHash([7u8; 16]),
        LogSegmentFetcher::new(store),
        QueryAccounting::new(),
    );
    let ctx = build_session(
        &config,
        pool,
        SessionTable::Alerts(Arc::new(alerts)),
        true,
        SpillDecision::Disabled,
        &SessionPrincipal::without_capabilities(TenantHash([0u8; 16])),
    )
    .expect("session builds");
    assert_eq!(
        ctx.state().config().target_partitions(),
        target_partitions,
        "the session's partition count",
    );

    let public = match table {
        SPANS_TABLE => spans_schema(),
        ALERTS_TABLE => alerts_schema(),
        other => panic!("no public schema for {other}"),
    };
    let mut fields = Vec::with_capacity(columns.len());
    let mut arrays = Vec::with_capacity(columns.len());
    for (name, array) in columns {
        let field = public
            .field_with_name(name)
            .expect("a public column")
            .clone();
        assert_eq!(field.data_type(), array.data_type(), "type of {name}");
        fields.push(field);
        arrays.push(array);
    }
    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema.clone(), arrays).expect("checked sum batch");
    let partitions = (0..batch.num_rows())
        .map(|row| vec![batch.slice(row, 1)])
        .collect();
    let mem = MemTable::try_new(schema, partitions).expect("checked sum mem table");
    ctx.deregister_table(table).expect("deregistered");
    ctx.register_table(table, Arc::new(mem))
        .expect("registered");
    ctx
}

/// `sql` run to completion, or the error its planning or execution returned,
/// rendered.
async fn try_execute(ctx: &SessionContext, sql: &str) -> Result<Vec<RecordBatch>, String> {
    ctx.sql(sql)
        .await
        .map_err(|e| e.to_string())?
        .collect()
        .await
        .map_err(|e| e.to_string())
}

/// An `ORDER BY` over an integer `sum` pages over the checked sum: six
/// groups walk as three pages of two and an empty fourth, with the two
/// groups tied at 70 split across the first boundary, and the walk at one
/// partition and at eight is the same sequence, equal to the unpaged result,
/// each group once. Group `d` passes through `i64::MAX` on its way to 50.
///
/// FLIP: the planner's `Sum` arm mapping a signed argument to `UInt` fails
/// the walk's resume check on page one's last row, `[Int(70), Str("b")]`.
#[tokio::test]
async fn integer_sum_walk_is_identical_at_one_and_eight_partitions() {
    let sql = "SELECT name, sum(duration_ns) AS s FROM spans GROUP BY name \
               ORDER BY s DESC, name";
    let plan = plan_page(sql, None).expect("planned");
    assert!(plan.order_from_other_rows, "a sum order reads other rows");
    assert_eq!(
        plan.order_by
            .iter()
            .map(|term| term.column.as_str())
            .collect::<Vec<_>>(),
        ["s", "name"],
    );

    let row = |name: &str, s: &str| vec![name.to_string(), s.to_string()];
    let expected = vec![
        vec![row("a", "100"), row("b", "70")],
        vec![row("c", "70"), row("d", "50")],
        vec![row("e", "-5"), row("f", "-20")],
    ];

    let mut walks = Vec::new();
    for partitions in [1, 8] {
        let ctx = checked_sum_context(
            SPANS_TABLE,
            vec![
                (
                    "name",
                    Arc::new(StringArray::from(vec![
                        "d", "b", "f", "a", "c", "d", "e", "b", "f", "a", "d", "b",
                    ])),
                ),
                (
                    "duration_ns",
                    Arc::new(Int64Array::from(vec![
                        i64::MAX,
                        30,
                        -30,
                        60,
                        70,
                        -i64::MAX,
                        -5,
                        25,
                        10,
                        40,
                        50,
                        15,
                    ])),
                ),
            ],
            partitions,
        );
        let unpaged = rows_of(&execute(&ctx, sql).await);
        assert_eq!(
            unpaged,
            expected.concat(),
            "the unpaged ordering at {partitions} partitions"
        );
        let pages = grouped_walk(&ctx, sql, GROUPED_PAGE_CAP).await;
        assert_eq!(
            pages, expected,
            "the walked pages at {partitions} partitions"
        );
        let walked: Vec<Vec<String>> = pages.concat();
        assert_eq!(walked, unpaged, "the walk against the unpaged result");
        assert!(
            multiset(&walked).values().all(|&count| count == 1),
            "a group was delivered twice at {partitions} partitions: {walked:?}",
        );
        walks.push(pages);
    }
    assert_eq!(walks[0], walks[1], "the walks at one and eight partitions");
}

/// The walked integer `avg` order and its fixture: eight groups whose means
/// are -3.5, -7/3, -0.5, 0, 1, 1.5 twice and 10/3. Group `d` sums to zero
/// through -3 and 3, and group `a` reaches 10 through `i64::MAX` and
/// `-i64::MAX`, which an `f64` running sum would round to a mean of 0.
const AVG_ORDER_STATEMENT: &str = "SELECT name, avg(duration_ns) AS a FROM spans \
     GROUP BY name ORDER BY a, name";

fn avg_fixture() -> Vec<(&'static str, ArrayRef)> {
    vec![
        (
            "name",
            Arc::new(StringArray::from(vec![
                "a", "f", "e", "g", "d", "b", "a", "c", "e", "h", "f", "d", "g", "b", "a", "e", "c",
            ])),
        ),
        (
            "duration_ns",
            Arc::new(Int64Array::from(vec![
                i64::MAX,
                -5,
                -1,
                -1,
                -3,
                1,
                10,
                3,
                -2,
                1,
                -2,
                3,
                0,
                2,
                -i64::MAX,
                -4,
                0,
            ])),
        ),
    ]
}

/// An `ORDER BY` over an integer `avg` pages over the exact mean: eight
/// groups walk as four pages of two and an empty fifth. Page one ends at
/// the negative non-integer mean -7/3, page two at the zero mean, and page
/// three at `b`, tied at 1.5 with `c` on page four. The walk at one
/// partition and at eight is the same sequence, equal to the unpaged result,
/// each group once, and every resume tuple is one the plan admits.
///
/// FLIP: re-registering DataFusion's built-in `avg` in the session (an
/// `f64` running sum) moves group `a` to a mean of 0, tied with `d`, and
/// fails the unpaged ordering.
#[tokio::test]
async fn integer_avg_walk_is_identical_at_one_and_eight_partitions() {
    let plan = plan_page(AVG_ORDER_STATEMENT, None).expect("planned");
    assert!(plan.order_from_other_rows, "an avg order reads other rows");
    assert_eq!(
        plan.order_by
            .iter()
            .map(|term| term.column.as_str())
            .collect::<Vec<_>>(),
        ["a", "name"],
    );

    let row = |name: &str, a: &str| vec![name.to_string(), a.to_string()];
    let expected = vec![
        vec![row("f", "-3.5"), row("e", "-2.3333333333333335")],
        vec![row("g", "-0.5"), row("d", "0.0")],
        vec![row("h", "1.0"), row("b", "1.5")],
        vec![row("c", "1.5"), row("a", "3.3333333333333335")],
    ];

    let mut walks = Vec::new();
    for partitions in [1, 8] {
        let ctx = checked_sum_context(SPANS_TABLE, avg_fixture(), partitions);
        let unpaged = rows_of(&execute(&ctx, AVG_ORDER_STATEMENT).await);
        assert_eq!(
            unpaged,
            expected.concat(),
            "the unpaged ordering at {partitions} partitions"
        );
        let pages = grouped_walk(&ctx, AVG_ORDER_STATEMENT, GROUPED_PAGE_CAP).await;
        assert_eq!(
            pages, expected,
            "the walked pages at {partitions} partitions"
        );
        let walked: Vec<Vec<String>> = pages.concat();
        assert_eq!(walked, unpaged, "the walk against the unpaged result");
        assert!(
            multiset(&walked).values().all(|&count| count == 1),
            "a group was delivered twice at {partitions} partitions: {walked:?}",
        );
        walks.push(pages);
    }
    assert_eq!(walks[0], walks[1], "the walks at one and eight partitions");
}

/// An integer `avg` whose sum is zero is `+0.0`, never `-0.0`, at one
/// partition and at eight, whatever the signs of the values that cancel:
/// `d` is -3 and 3, and `a` is `i64::MAX` and `-i64::MAX`, each value in
/// its own partition. So a cursor minted at a zero mean carries `+0.0`, and
/// the page resumed from it starts strictly above zero.
///
/// FLIP: resuming at `Float(-0.0)` instead serves `a` and `d` again, since
/// `+0.0` sorts after `-0.0`, and fails the empty-page assertion.
#[tokio::test]
async fn an_integer_avg_of_zero_is_positive_zero() {
    let sql = "SELECT name, avg(duration_ns) AS a FROM spans GROUP BY name ORDER BY a, name";
    for partitions in [1, 8] {
        let ctx = checked_sum_context(
            SPANS_TABLE,
            vec![
                (
                    "name",
                    Arc::new(StringArray::from(vec!["d", "a", "d", "a"])),
                ),
                (
                    "duration_ns",
                    Arc::new(Int64Array::from(vec![-3, i64::MAX, 3, -i64::MAX])),
                ),
            ],
            partitions,
        );
        let batches = execute(&ctx, sql).await;
        let mut bits = Vec::new();
        for batch in &batches {
            let column = batch.column_by_name("a").expect("the avg column");
            for offset in 0..batch.num_rows() {
                match resume_value(column.as_ref(), offset) {
                    Some(ResumeValue::Float(v)) => bits.push(v.to_bits()),
                    other => panic!("an avg resumed as {other:?}"),
                }
            }
        }
        assert_eq!(
            bits,
            [0.0_f64.to_bits(), 0.0_f64.to_bits()],
            "the zero means at {partitions} partitions"
        );

        let resumed = plan_page(
            sql,
            Some(&ResumePosition::new(vec![
                ResumeValue::Float(0.0),
                ResumeValue::Str("d".to_string()),
            ])),
        )
        .expect("resumed plan");
        assert_eq!(
            rows_of(&execute(&ctx, &resumed.statement).await),
            Vec::<Vec<String>>::new(),
            "the page after the last zero mean at {partitions} partitions"
        );
    }
}

/// A table, a statement over it, its columns, the sum's result type and a
/// resume value of the order's kind.
type OverflowCase<'a> = (
    &'a str,
    &'a str,
    Vec<(&'a str, ArrayRef)>,
    &'static str,
    ResumeValue,
);

/// A group whose integer sum overflows fails the paged statement with the
/// checked sum's overflow error, signed and unsigned, at one partition and at
/// eight: the first page computes every group's sum and fails, a page resumed
/// from a cursor past the other groups fails the same way, and no page
/// carries a wrapped total.
///
/// FLIP: re-registering DataFusion's built-in `sum` in
/// [`checked_sum_context`] makes page one return rows, a wrapped total
/// among them, instead of failing.
#[tokio::test]
async fn an_overflowing_integer_sum_order_fails_rather_than_pages() {
    let cases: Vec<OverflowCase<'_>> = vec![
        (
            SPANS_TABLE,
            "SELECT name, sum(duration_ns) AS s FROM spans GROUP BY name ORDER BY s DESC, name",
            vec![
                (
                    "name",
                    Arc::new(StringArray::from(vec!["a", "b", "a", "c"])),
                ),
                (
                    "duration_ns",
                    Arc::new(Int64Array::from(vec![i64::MAX, 5, 1, 3])),
                ),
            ],
            "Int64",
            ResumeValue::Int(3),
        ),
        (
            ALERTS_TABLE,
            "SELECT writer_id, sum(writer_seq) AS s FROM alerts GROUP BY writer_id \
             ORDER BY s, writer_id",
            vec![
                (
                    "writer_id",
                    Arc::new(StringArray::from(vec!["a", "b", "a", "c"])),
                ),
                (
                    "writer_seq",
                    Arc::new(UInt64Array::from(vec![u64::MAX, 5, 1, 3])),
                ),
            ],
            "UInt64",
            ResumeValue::UInt(5),
        ),
    ];
    for (table, sql, columns, result_type, resume) in cases {
        let overflow = SqlError::IntegerSumOverflow { result_type }.to_string();
        for partitions in [1, 8] {
            let ctx = checked_sum_context(table, columns.clone(), partitions);
            let first = plan_page(sql, None).expect("planned");
            let failed = try_execute(&ctx, &first.statement)
                .await
                .expect_err("the first page computes the overflowing sum");
            assert!(
                failed.contains(&overflow),
                "page one of {sql:?} at {partitions} partitions failed with {failed:?}",
            );
            let resumed = plan_page(
                sql,
                Some(&ResumePosition::new(vec![
                    resume.clone(),
                    ResumeValue::Str("c".to_string()),
                ])),
            )
            .expect("resumed plan");
            let failed = try_execute(&ctx, &resumed.statement)
                .await
                .expect_err("a resumed page computes the overflowing sum too");
            assert!(
                failed.contains(&overflow),
                "a resumed page of {sql:?} at {partitions} partitions failed with {failed:?}",
            );
        }
    }
}
