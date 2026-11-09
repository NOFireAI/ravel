//! ADR-0093, exact not-equal amendment: the logs scan evaluates a pushed
//! declared-`Str` `<>` conjunct on each batch it builds (pull request 1), and
//! the provider reports the conjunct `Exact`, so DataFusion keeps no
//! `FilterExec` for it (pull request 2).
//!
//! Every statement runs through the production session (`build_session`) over
//! real RLOG objects in a `MemoryStore`. The reference for an answer is the
//! same statement on a provider with the row filter off
//! (`LogsTableProvider::with_row_filter_pushdown(false)`), which reports the
//! conjunct `Inexact`, so DataFusion's `FilterExec` is the only evaluator.
//!
//! - `answers_match_the_residual_on_every_value_shape` (T2, T7): the fixture
//!   spreads every value shape the amendment lists over a plain-page block, a
//!   dictionary-page block, a block with an `attrs_raw` page, and a block with
//!   a non-UTF-8 cell. Each statement returns the reference answer, the scan
//!   drops exactly the 12 `''` and NULL rows (`rows_dropped_by_row_filter`),
//!   and no `FilterExec` is left above it.
//! - `exact_not_equal_conjunct_is_exact_and_leaves_no_filter` (T1): the
//!   optimized plans of the accepted shapes hold no `FilterExec`, the rejected
//!   ones keep one, and the scan's EXPLAIN line names the filter's column.
//! - `a_filter_column_outside_the_projection_is_built_and_dropped`,
//!   `the_q31_and_count_shapes_filter_a_column_they_do_not_select` and
//!   `the_filter_reads_its_own_column_when_the_projection_is_widened` (T3 a,
//!   c): a scan whose projection leaves the filter column out.
//! - `the_late_materialized_phase_one_scan_filters_and_keeps_its_row_refs`
//!   (T3 b, d) and `the_attrs_per_key_scan_keeps_the_row_filter` (T3 b2): the
//!   two rebuilt scans.
//! - `a_pushed_fetch_counts_rows_after_the_row_filter` (T5).
//! - `clickbench_shapes_plan_without_a_filter_over_the_scan` (T9).

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::BTreeMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, AsArray, DictionaryArray, StringArray};
use datafusion::arrow::datatypes::{Int32Type, TimestampNanosecondType};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::catalog::TableProvider;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::TreeNode;
use datafusion::logical_expr::{Expr, LogicalPlan};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::metrics::MetricValue;
use datafusion::physical_plan::{
    ExecutionPlan, StatisticsArgs, StatisticsContext, collect, displayable,
};
use datafusion::prelude::SessionContext;
use ravel_catalog::{SegmentLevel, SegmentRef, Snapshot};
use ravel_codec::encoding::Enc;
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_query::{LogSegmentFetcher, PhaseAccounting};
use ravel_sql::{
    CeilingBreach, DeclaredColumn, DeclaredType, LogsTableProvider, SessionTable, SpillDecision,
    SqlConfig, TenantDelegatingPool, TenantMemoryAccountant, build_session,
    logs_schema_with_declared,
};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use ravel_types::logstream::log_stream_id;
use uuid::Uuid;

use crate::logs_columnar::{column_page_encs, rewrite_str_column_cells, str_column_id};

const TENANT: TenantHash = TenantHash([0x93; 16]);

fn identity() -> ObjectIdentity {
    ObjectIdentity {
        tenant_hash: TENANT.0,
        shard: 0,
        writer_id: [4u8; 16],
        writer_epoch: 1,
        writer_seq: 1,
    }
}

pub(crate) fn s(v: &str) -> AttrValue {
    AttrValue::Str(v.to_string())
}

/// The stream a record is on, by what its resource says about `s`.
#[derive(Clone, Copy)]
pub(crate) enum Stream {
    /// No `s` at resource level: a row with no record value reads NULL.
    Bare,
    /// Resource `s = 'r'`.
    R,
    /// Resource `s = ''`.
    Empty,
    /// Resource `s = 'f'`.
    F,
}

fn resource(stream: Stream) -> Vec<(String, AttrValue)> {
    let mut out = vec![("service.name".to_string(), s("api"))];
    match stream {
        Stream::Bare => {}
        Stream::R => out.push(("s".to_string(), s("r"))),
        Stream::Empty => out.push(("s".to_string(), s(""))),
        Stream::F => out.push(("s".to_string(), s("f"))),
    }
    out
}

pub(crate) fn record(ts: i64, stream: Stream, attrs: &[(&str, AttrValue)]) -> LogRecord {
    let resource = resource(stream);
    LogRecord {
        stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".into(),
        body: format!("row {ts}"),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: attrs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect(),
    }
}

pub(crate) fn encode(records: &[LogRecord], cfg: RlogConfig) -> Vec<u8> {
    let mut w = RlogWriter::new(cfg, identity());
    for r in records {
        w.push(r.clone()).expect("push");
    }
    w.finish().expect("finish")
}

pub(crate) async fn put(
    store: &MemoryStore,
    key: &str,
    bytes: Vec<u8>,
    records: &[LogRecord],
) -> SegmentRef {
    let size = bytes.len() as u64;
    store
        .put(key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put");
    SegmentRef {
        data_object_key: key.to_string(),
        object_size: size,
        min_event_ts_ns: records.iter().map(|r| r.ts_ns).min().expect("rows"),
        max_event_ts_ns: records.iter().map(|r| r.ts_ns).max().expect("rows"),
        ingest_hour_bucket: 0,
        sample_count: records.len() as u64,
        series_count: 0,
        shard: 0,
        content_hash: [0u8; 32],
        writer_id: Uuid::from_u128(93),
        writer_epoch: 1,
        writer_seq: 1,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        declared_column_stats: Default::default(),
    }
}

pub(crate) fn provider(
    store: &Arc<MemoryStore>,
    segments: Vec<SegmentRef>,
    declared: &[DeclaredColumn],
) -> LogsTableProvider {
    let backend: Arc<dyn ObjectStoreBackend> = Arc::clone(store) as Arc<dyn ObjectStoreBackend>;
    LogsTableProvider::new(
        Snapshot {
            segments,
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        },
        TENANT,
        LogSegmentFetcher::new(backend),
        PhaseAccounting::new(),
    )
    .with_declared_columns(declared.to_vec())
}

fn session(provider: LogsTableProvider) -> SessionContext {
    let pool = Arc::new(TenantDelegatingPool::new(
        1 << 30,
        TenantMemoryAccountant::new(1 << 30),
        CeilingBreach::new(),
        QueryAccounting::new(),
    ));
    build_session(
        &SqlConfig::default(),
        pool,
        SessionTable::Logs(Arc::new(provider)),
        false,
        SpillDecision::Disabled,
        &ravel_sql::SessionPrincipal::without_capabilities(TenantHash([0u8; 16])),
    )
    .expect("session")
}

/// The physical plan of `sql`, executed, with its batches.
pub(crate) async fn run(
    ctx: &SessionContext,
    sql: &str,
) -> (Arc<dyn ExecutionPlan>, Vec<RecordBatch>) {
    let plan = ctx
        .sql(sql)
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"))
        .create_physical_plan()
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    let batches = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .unwrap_or_else(|e| panic!("{sql}: {e}"));
    (plan, batches)
}

fn find<'a>(
    plan: &'a Arc<dyn ExecutionPlan>,
    name: &str,
    out: &mut Vec<&'a Arc<dyn ExecutionPlan>>,
) {
    if plan.name() == name {
        out.push(plan);
    }
    for child in plan.children() {
        find(child, name, out);
    }
}

pub(crate) fn nodes<'a>(
    plan: &'a Arc<dyn ExecutionPlan>,
    name: &str,
) -> Vec<&'a Arc<dyn ExecutionPlan>> {
    let mut out = Vec::new();
    find(plan, name, &mut out);
    out
}

/// `name` summed over every `LogsScanExec` in `plan`.
fn scan_metric(plan: &Arc<dyn ExecutionPlan>, name: &str) -> usize {
    nodes(plan, "LogsScanExec")
        .iter()
        .map(|scan| {
            scan.metrics()
                .and_then(|m| m.sum_by_name(name))
                .map_or(0, |v| v.as_usize())
        })
        .sum()
}

/// The input and output rows of the one `FilterExec` in `plan`, from its
/// `selectivity` ratio (output over input).
fn filter_rows(plan: &Arc<dyn ExecutionPlan>) -> (usize, usize) {
    let filters = nodes(plan, "FilterExec");
    assert_eq!(
        filters.len(),
        1,
        "{}",
        displayable(plan.as_ref()).indent(true)
    );
    assert!(filters[0].downcast_ref::<FilterExec>().is_some());
    let metrics = filters[0].metrics().expect("metrics").aggregate_by_name();
    metrics
        .iter()
        .find_map(|m| match m.value() {
            MetricValue::Ratio {
                name,
                ratio_metrics,
            } if name.as_ref() == "selectivity" => {
                Some((ratio_metrics.total(), ratio_metrics.part()))
            }
            _ => None,
        })
        .expect("a selectivity metric")
}

/// `plan` holds no `FilterExec`: every conjunct of `sql` was reported
/// `Exact`.
fn assert_no_filter(plan: &Arc<dyn ExecutionPlan>, sql: &str) {
    assert!(
        nodes(plan, "FilterExec").is_empty(),
        "{sql}: no FilterExec: {}",
        displayable(plan.as_ref()).indent(true)
    );
}

pub(crate) fn pretty(batches: &[RecordBatch]) -> String {
    pretty_format_batches(batches).expect("pretty").to_string()
}

/// The text of a `Utf8` or `Dictionary(Int32, Utf8)` column, row by row.
fn texts(column: &dyn Array) -> Vec<Option<String>> {
    if let Some(a) = column.as_any().downcast_ref::<StringArray>() {
        return (0..a.len())
            .map(|i| a.is_valid(i).then(|| a.value(i).to_string()))
            .collect();
    }
    let d = column
        .as_any()
        .downcast_ref::<DictionaryArray<Int32Type>>()
        .expect("a Utf8 or dictionary column");
    let values = d.values().as_string::<i32>();
    (0..d.len())
        .map(|i| {
            d.is_valid(i)
                .then(|| values.value(d.keys().value(i) as usize).to_string())
        })
        .collect()
}

pub(crate) fn column_texts(batches: &[RecordBatch], name: &str) -> Vec<Option<String>> {
    batches
        .iter()
        .flat_map(|b| texts(b.column(b.schema().index_of(name).expect(name)).as_ref()))
        .collect()
}

fn timestamps(batches: &[RecordBatch]) -> Vec<i64> {
    batches
        .iter()
        .flat_map(|b| {
            let ts = b.column(b.schema().index_of("ts").expect("ts"));
            ts.as_primitive::<TimestampNanosecondType>()
                .values()
                .to_vec()
        })
        .collect()
}

/// The filters the optimizer hands `TableProvider::scan` for `sql`.
async fn pushed_filters(ctx: &SessionContext, sql: &str) -> Vec<Expr> {
    let plan = ctx
        .sql(sql)
        .await
        .expect("plan")
        .into_optimized_plan()
        .expect("optimize");
    let mut found = None;
    plan.apply(|node| {
        if let LogicalPlan::TableScan(scan) = node {
            found = Some(scan.filters.clone());
        }
        Ok(datafusion::common::tree_node::TreeNodeRecursion::Continue)
    })
    .expect("walk");
    found.expect("a table scan")
}

/// `scan` executed alone, every partition.
async fn execute_alone(ctx: &SessionContext, scan: &Arc<dyn ExecutionPlan>) -> Vec<RecordBatch> {
    collect(Arc::clone(scan), ctx.task_ctx())
        .await
        .expect("scan runs")
}

// --- T2 and T7: every value shape -------------------------------------------

fn shapes_declared() -> Vec<DeclaredColumn> {
    vec![
        DeclaredColumn::new("s", DeclaredType::Str),
        DeclaredColumn::new("t", DeclaredType::Str),
        DeclaredColumn::new("n", DeclaredType::I64),
    ]
}

/// Rows in the fixture: 8 + 9 + 5 + 4.
const SHAPE_ROWS: usize = 26;
/// The fixture's `''` and NULL rows, which `s <> ''` drops.
const SHAPE_DROPPED: usize = 12;
/// The rows `s <> ''` keeps, by `ts`.
const SHAPE_KEPT: [i64; 14] = [
    100, 103, 106, 107, 200, 202, 204, 205, 207, 301, 302, 401, 402, 403,
];

/// Four objects, each one block:
///
/// - plain page (every present value distinct): `'p1'` first, so key 0 of a
///   NULL row names a value `<> ''` holds for; then `''`; no value and no
///   fallback (NULL); no value with resource `'r'`; no value with resource
///   `''`; an I64 cell for `s` (NULL); `'p2'`; two occurrences of `s`.
/// - dictionary page: `'d'` and `''` repeated, `'e'`, NULL, `'r'` and `''`
///   by fallback.
/// - `attrs_raw`: one dynamic column, taken by `a`, so `s` spills and the
///   block takes the row path: `''`, `'q'`, `'r'` by fallback, an I64 cell,
///   `''` by fallback.
/// - a rewritten block on the resource-`'f'` stream: `''`, `'g'`, a non-UTF-8
///   cell (reads `'f'`), no cell (reads `'f'`).
async fn shapes_fixture() -> (Arc<MemoryStore>, Vec<SegmentRef>) {
    let store = Arc::new(MemoryStore::new());
    let n = |v: i64| ("n", AttrValue::I64(v));
    let plain = vec![
        record(100, Stream::Bare, &[("s", s("p1")), n(1)]),
        record(101, Stream::Bare, &[("s", s("")), n(2)]),
        record(102, Stream::Bare, &[n(3)]),
        record(103, Stream::R, &[n(4)]),
        record(104, Stream::Empty, &[n(5)]),
        record(105, Stream::Bare, &[("s", AttrValue::I64(7)), n(6)]),
        record(106, Stream::Bare, &[("s", s("p2")), ("t", s("x")), n(7)]),
        record(107, Stream::Bare, &[("s", s("p3")), ("s", s("p4")), n(8)]),
    ];
    let cfg = RlogConfig::default();
    let bytes = encode(&plain, cfg);
    let encs = column_page_encs(&bytes, cfg, str_column_id(&bytes, cfg, "s"));
    assert!(!encs.contains(&Enc::Dict), "plain block: {encs:?}");
    let mut segments = vec![put(&store, "logs/plain.rlog", bytes, &plain).await];

    let dict = vec![
        record(200, Stream::Bare, &[("s", s("d")), n(1)]),
        record(201, Stream::Bare, &[("s", s("")), n(2)]),
        record(202, Stream::Bare, &[("s", s("d")), n(3)]),
        record(203, Stream::Bare, &[("s", s("")), n(4)]),
        record(204, Stream::Bare, &[("s", s("d")), n(5)]),
        record(205, Stream::Bare, &[("s", s("e")), n(6)]),
        record(206, Stream::Bare, &[n(7)]),
        record(207, Stream::R, &[n(8)]),
        record(208, Stream::Empty, &[n(9)]),
    ];
    let bytes = encode(&dict, cfg);
    let encs = column_page_encs(&bytes, cfg, str_column_id(&bytes, cfg, "s"));
    assert!(encs.contains(&Enc::Dict), "dictionary block: {encs:?}");
    segments.push(put(&store, "logs/dict.rlog", bytes, &dict).await);

    let a = ("a", s("x"));
    let raw = vec![
        record(300, Stream::Bare, &[a.clone(), ("s", s("")), n(1)]),
        record(301, Stream::Bare, &[a.clone(), ("s", s("q")), n(2)]),
        record(302, Stream::R, &[a.clone(), n(3)]),
        record(
            303,
            Stream::Bare,
            &[a.clone(), ("s", AttrValue::I64(3)), n(4)],
        ),
        record(304, Stream::Empty, &[a, n(5)]),
    ];
    let raw_cfg = RlogConfig {
        max_dynamic_columns: 1,
        ..RlogConfig::default()
    };
    let bytes = encode(&raw, raw_cfg);
    segments.push(put(&store, "logs/raw.rlog", bytes, &raw).await);

    let seeded: Vec<LogRecord> = (400..404)
        .map(|ts| record(ts, Stream::F, &[("s", s("seed"))]))
        .collect();
    let clean = encode(&seeded, cfg);
    let column = str_column_id(&clean, cfg, "s");
    let cells = vec![
        Some(b"".to_vec()),
        Some(b"g".to_vec()),
        Some(vec![b'b', 0xff]),
        None,
    ];
    let bytes = rewrite_str_column_cells(&clean, cfg, &seeded, column, &cells);
    segments.push(put(&store, "logs/nonutf8.rlog", bytes, &seeded).await);
    (store, segments)
}

/// T2 and T7. Each statement, with `attrs` projected and without, returns the
/// answer the `FilterExec` alone gives, which is pinned to [`SHAPE_KEPT`]; the
/// scan drops exactly [`SHAPE_DROPPED`] rows and emits the other
/// `SHAPE_ROWS - SHAPE_DROPPED`, and no `FilterExec` is left above it.
///
/// Flipped: a filter on the columnar path only leaks the `attrs_raw` block's
/// `''` rows, which no `FilterExec` removes, and fails "the reference
/// answer". The amendment's other wrong implementations for this test (a
/// kernel reading raw dictionary keys, a record-cell-only reader test, a
/// keep-when-undecided rule) change the rows the scan emits in the same way.
#[tokio::test]
async fn answers_match_the_residual_on_every_value_shape() {
    let (store, segments) = shapes_fixture().await;
    let declared = shapes_declared();
    let on = session(provider(&store, segments.clone(), &declared));
    let off = session(provider(&store, segments, &declared).with_row_filter_pushdown(false));

    let statements = [
        "SELECT ts, s FROM logs WHERE s <> '' ORDER BY ts",
        "SELECT ts, s, attrs FROM logs WHERE s <> '' ORDER BY ts",
        "SELECT ts, s FROM logs WHERE '' <> s ORDER BY ts",
        // The q13 shape (T7).
        "SELECT s, COUNT(DISTINCT n) AS u FROM logs WHERE s <> '' GROUP BY s \
         ORDER BY u DESC, s LIMIT 10",
    ];
    for sql in statements {
        let (reference_plan, reference) = run(&off, sql).await;
        let (plan, got) = run(&on, sql).await;
        assert_eq!(
            pretty(&got),
            pretty(&reference),
            "{sql}: the reference answer"
        );
        assert_eq!(
            scan_metric(&reference_plan, "rows_dropped_by_row_filter"),
            0,
            "{sql}: the reference scan filters nothing"
        );
        assert_no_filter(&plan, sql);
        let scans = nodes(&plan, "LogsScanExec");
        assert_eq!(scans.len(), 1, "{sql}");
        assert_eq!(
            scan_metric(&plan, "rows_dropped_by_row_filter"),
            SHAPE_DROPPED,
            "{sql}: the scan drops every '' and NULL row"
        );
        // Run again alone, after the counter is read: the second run adds to
        // the same node's metrics.
        let emitted = execute_alone(&on, scans[0]).await;
        assert_eq!(
            emitted.iter().map(RecordBatch::num_rows).sum::<usize>(),
            SHAPE_ROWS - SHAPE_DROPPED,
            "{sql}: the scan emits the kept rows"
        );
        assert_eq!(
            filter_rows(&reference_plan),
            (SHAPE_ROWS, SHAPE_ROWS - SHAPE_DROPPED),
            "{sql}: the reference FilterExec"
        );
        assert!(
            (scan_metric(&plan, "rowpath_batches") > 0
                && scan_metric(&plan, "columnar_batches") > 0)
                || sql.contains("attrs"),
            "{sql}: both emission paths ran"
        );
        let shown = format!("{}", displayable(plan.as_ref()).indent(true));
        assert!(
            shown.contains("row_filters=[s != ]"),
            "{sql}: EXPLAIN shows the conjunct by column name: {shown}"
        );
    }

    let (_, got) = run(&on, statements[0]).await;
    assert_eq!(timestamps(&got), SHAPE_KEPT, "the rows s <> '' keeps");
    let values = column_texts(&got, "s");
    assert_eq!(
        values[1],
        Some("r".to_string()),
        "the resource fallback row"
    );
    assert_eq!(values[11], Some("g".to_string()));
    assert_eq!(
        values[12],
        Some("f".to_string()),
        "a non-UTF-8 cell reads 'f'"
    );
    assert_eq!(values[13], Some("f".to_string()), "no cell reads 'f'");
}

// --- T3: the filter column and the rebuilt scans ------------------------------

/// Two declared `Str` columns and enough declared columns that a `SELECT *`
/// TopK is wide enough for late materialization (more than eight columns the
/// sort and the filter do not read).
fn wide_declared() -> Vec<DeclaredColumn> {
    let mut out = vec![
        DeclaredColumn::new("s", DeclaredType::Str),
        DeclaredColumn::new("t", DeclaredType::Str),
        DeclaredColumn::new("n", DeclaredType::I64),
    ];
    out.extend((0..8).map(|i| DeclaredColumn::new(format!("w{i}"), DeclaredType::Str)));
    out
}

/// `s` for row `ts` of the T3 fixture: `''` on 0, 1, 3, 5, 8 and 9, else
/// `v<ts>`. The earliest rows are `''`, and `''` rows sit between the first
/// three kept rows (2, 4, 6) of the one block.
fn t3_s(ts: i64) -> String {
    if [0, 1, 3, 5, 8, 9].contains(&ts) {
        String::new()
    } else {
        format!("v{ts}")
    }
}

/// `t` for row `ts`: `''` on the odd rows, else `u<ts>`, so `t <> ''` keeps
/// another row set than `s <> ''`.
fn t3_t(ts: i64) -> String {
    if ts % 2 == 1 {
        String::new()
    } else {
        format!("u{ts}")
    }
}

const T3_ROWS: i64 = 12;

async fn t3_fixture() -> (Arc<MemoryStore>, Vec<SegmentRef>) {
    let store = Arc::new(MemoryStore::new());
    let records: Vec<LogRecord> = (0..T3_ROWS)
        .map(|ts| {
            let mut r = record(
                ts,
                Stream::Bare,
                &[
                    ("s", s(&t3_s(ts))),
                    ("t", s(&t3_t(ts))),
                    ("n", AttrValue::I64(ts % 3)),
                    ("k", s(&format!("k{ts}"))),
                ],
            );
            r.attrs
                .extend((0..8).map(|i| (format!("w{i}"), s(&format!("w{i}-{ts}")))));
            r
        })
        .collect();
    let bytes = encode(&records, RlogConfig::default());
    let segments = vec![put(&store, "logs/t3.rlog", bytes, &records).await];
    (store, segments)
}

fn kept_by_s() -> Vec<i64> {
    (0..T3_ROWS).filter(|&ts| !t3_s(ts).is_empty()).collect()
}

/// T3 (a): a scan whose projection does not name `s` still builds it, runs
/// `s <> ''` on it, and emits only the projected columns. The q31 and
/// `COUNT(*)` shapes hand the provider this projection once the conjunct is
/// `Exact`; here the provider is called with it directly, with the filters
/// the optimizer pushes for those statements.
///
/// Flipped: evaluating only when the column is in the output projection emits
/// all 12 rows and fails "only the kept rows".
#[tokio::test]
async fn a_filter_column_outside_the_projection_is_built_and_dropped() {
    let (store, segments) = t3_fixture().await;
    let declared = wide_declared();
    let p = Arc::new(provider(&store, segments, &declared));
    let ctx = session(provider(&store, Vec::new(), &declared));
    let schema = logs_schema_with_declared(&declared);
    let ts = schema.index_of("ts").expect("ts");
    let n = schema.index_of("n").expect("n");
    for sql in [
        "SELECT n, COUNT(*) FROM logs WHERE s <> '' GROUP BY 1",
        "SELECT COUNT(*) FROM logs WHERE s <> ''",
    ] {
        let filters = pushed_filters(&ctx, sql).await;
        assert_eq!(filters.len(), 1, "{sql}: {filters:?}");
        let scan = p
            .scan(&ctx.state(), Some(&vec![ts, n]), &filters, None)
            .await
            .expect("scan");
        let batches = execute_alone(&ctx, &scan).await;
        let fields: Vec<String> = scan
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        assert_eq!(fields, ["ts", "n"], "{sql}: only the projected columns");
        let mut got = timestamps(&batches);
        got.sort_unstable();
        assert_eq!(got, kept_by_s(), "{sql}: only the kept rows");
        assert_eq!(scan_metric(&scan, "rows_dropped_by_row_filter"), 6, "{sql}");
    }

    // No projection at all: the empty one `COUNT(*)` can reach.
    let filters = pushed_filters(&ctx, "SELECT COUNT(*) FROM logs WHERE s <> ''").await;
    let scan = p
        .scan(&ctx.state(), Some(&Vec::new()), &filters, None)
        .await
        .expect("scan");
    let batches = execute_alone(&ctx, &scan).await;
    assert_eq!(scan.schema().fields().len(), 0);
    let rows: usize = batches.iter().map(RecordBatch::num_rows).sum();
    assert_eq!(
        rows,
        kept_by_s().len(),
        "the zero-column batches keep their row counts"
    );
}

/// T3 (c): with `t` projected and the filter on `s`, the build appends `s`
/// after `t`, and the filter reads `s`. Both are declared `Str` columns, so a
/// filter bound to the wrong one still evaluates.
///
/// Flipped: binding the filter's column as if the build schema were `ts`
/// followed by the declared columns in order reads `t` and fails "the rows
/// s <> '' keeps".
#[tokio::test]
async fn the_filter_reads_its_own_column_when_the_projection_is_widened() {
    let (store, segments) = t3_fixture().await;
    let declared = wide_declared();
    let p = Arc::new(provider(&store, segments, &declared));
    let ctx = session(provider(&store, Vec::new(), &declared));
    let schema = logs_schema_with_declared(&declared);
    let ts = schema.index_of("ts").expect("ts");
    let t = schema.index_of("t").expect("t");
    let filters = pushed_filters(&ctx, "SELECT ts, t FROM logs WHERE s <> ''").await;
    let scan = p
        .scan(&ctx.state(), Some(&vec![ts, t]), &filters, None)
        .await
        .expect("scan");
    let batches = execute_alone(&ctx, &scan).await;
    let mut rows: Vec<(i64, Option<String>)> = timestamps(&batches)
        .into_iter()
        .zip(column_texts(&batches, "t"))
        .collect();
    rows.sort();
    let want: Vec<(i64, Option<String>)> = kept_by_s()
        .into_iter()
        .map(|ts| (ts, Some(t3_t(ts))))
        .collect();
    assert_eq!(rows, want, "the rows s <> '' keeps, with their own t");
}

/// T3 (b, d): `SELECT * ... WHERE s <> '' ORDER BY ts LIMIT 3` is late
/// materialized. The phase-1 scan, rebuilt by `reproject`, filters too, and
/// each row it keeps carries its own row ref, so phase 2 returns the `s` of
/// that row: rows 2, 4 and 6, with the `''` rows 3 and 5 between them in the
/// same block.
///
/// The conjunct is `Exact`, so no `FilterExec` reads `s` and phase 1
/// projects only `ts` and the row ref: it builds `s` for the filter and drops
/// it.
///
/// Flipped: a `reproject` that drops `row_filters` leaves phase 1 emitting
/// the `''` rows, so the statement returns rows 0, 1 and 2 and fails "the
/// three earliest kept rows"; row refs recomputed after the filter as
/// consecutive positions from the batch's first row point phase 2 at rows 0,
/// 1 and 2, and fail the same assertion.
#[tokio::test]
async fn the_late_materialized_phase_one_scan_filters_and_keeps_its_row_refs() {
    let (store, segments) = t3_fixture().await;
    let declared = wide_declared();
    let on = session(provider(&store, segments.clone(), &declared));
    let off = session(provider(&store, segments, &declared).with_row_filter_pushdown(false));
    let sql = "SELECT * FROM logs WHERE s <> '' ORDER BY ts LIMIT 3";

    let (plan, got) = run(&on, sql).await;
    let shown = format!("{}", displayable(plan.as_ref()).indent(true));
    assert!(
        shown.contains("LogsRowFetchExec"),
        "late materialized: {shown}"
    );
    assert_no_filter(&plan, sql);
    let (_, reference) = run(&off, sql).await;
    assert_eq!(timestamps(&got), [2, 4, 6], "the three earliest kept rows");
    let want: Vec<Option<String>> = [2, 4, 6].iter().map(|&ts| Some(t3_s(ts))).collect();
    assert_eq!(column_texts(&got, "s"), want, "each s is its own row's");
    assert_eq!(
        column_texts(&got, "w0"),
        [2, 4, 6].map(|ts| Some(format!("w0-{ts}"))),
        "each wide column is its own row's"
    );
    assert_eq!(pretty(&got), pretty(&reference), "the reference answer");

    let scans = nodes(&plan, "LogsScanExec");
    assert_eq!(scans.len(), 1, "{shown}");
    let phase_one = scans[0];
    let fields: Vec<String> = phase_one
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect();
    assert_eq!(
        fields,
        ["ts", "__ravel_row_ref"],
        "the phase-1 scan: {shown}"
    );
    let emitted = execute_alone(&on, phase_one).await;
    let mut emitted = timestamps(&emitted);
    emitted.sort_unstable();
    assert_eq!(emitted, kept_by_s(), "phase 1 emits only the kept rows");
}

/// T3 (b2): `SELECT attrs['k'] FROM logs WHERE s <> ''` plans through
/// `AttrsPerKeyProjection`, which rebuilds the scan with
/// `reproject_attr_keys`. That scan filters too.
///
/// Flipped: a `reproject_attr_keys` that drops `row_filters` emits the `''`
/// rows, which no `FilterExec` removes, and fails "the reference answer".
#[tokio::test]
async fn the_attrs_per_key_scan_keeps_the_row_filter() {
    let (store, segments) = t3_fixture().await;
    let declared = wide_declared();
    let on = session(provider(&store, segments.clone(), &declared));
    let off = session(provider(&store, segments, &declared).with_row_filter_pushdown(false));
    let sql = "SELECT ts, attrs['k'] AS k FROM logs WHERE s <> '' ORDER BY ts";

    let (plan, got) = run(&on, sql).await;
    assert_no_filter(&plan, sql);
    let (_, reference) = run(&off, sql).await;
    assert_eq!(pretty(&got), pretty(&reference), "the reference answer");
    assert_eq!(timestamps(&got), kept_by_s());

    assert_eq!(
        scan_metric(&plan, "rows_dropped_by_row_filter"),
        6,
        "the per-key scan drops the '' rows"
    );
    let scans = nodes(&plan, "LogsScanExec");
    assert_eq!(scans.len(), 1);
    let shown = format!("{}", displayable(scans[0].as_ref()).one_line());
    assert!(shown.contains("attrs[k]") || shown.contains("k"), "{shown}");
    let emitted = execute_alone(&on, scans[0]).await;
    let mut emitted = timestamps(&emitted);
    emitted.sort_unstable();
    assert_eq!(
        emitted,
        kept_by_s(),
        "the per-key scan emits only the kept rows"
    );
}

// --- T5: fetch ----------------------------------------------------------------

/// T5: one partition whose block starts with ten `''` rows, followed by ten
/// non-empty ones. With a pushed fetch of 5, the scan alone returns exactly
/// five rows, all non-empty, and its statistics claim no exact row count.
///
/// The conjunct is `Exact`, so `LimitPushdown` puts the fetch on the scan
/// itself: the statement's plan holds no `FilterExec` and its scan carries
/// `fetch = 5`.
///
/// Flipped: counting the fetch on built rows returns no row at all and fails
/// "exactly five rows". Leaving `apply_fetch_to_statistics` on its exact
/// branch is caught only beside a statistics gate that also ignores the row
/// filter: `stats_are_exact` already refuses this scan, so no exact count
/// reaches that branch. With both gates removed the scan reports `Exact(5)`
/// and fails "no exact row count".
#[tokio::test]
async fn a_pushed_fetch_counts_rows_after_the_row_filter() {
    let store = Arc::new(MemoryStore::new());
    let records: Vec<LogRecord> = (0..20)
        .map(|ts| {
            let v = if ts < 10 {
                String::new()
            } else {
                format!("x{ts}")
            };
            record(ts, Stream::Bare, &[("s", s(&v))])
        })
        .collect();
    let bytes = encode(&records, RlogConfig::default());
    let segments = vec![put(&store, "logs/fetch.rlog", bytes, &records).await];
    let declared = vec![DeclaredColumn::new("s", DeclaredType::Str)];
    let ctx = session(provider(&store, segments, &declared));

    let sql = "SELECT s FROM logs WHERE s <> '' LIMIT 5";
    let (plan, got) = run(&ctx, sql).await;
    assert_no_filter(&plan, sql);
    let want: Vec<Option<String>> = (10..15).map(|ts| Some(format!("x{ts}"))).collect();
    assert_eq!(column_texts(&got, "s"), want, "exactly five rows");
    let scans = nodes(&plan, "LogsScanExec");
    assert_eq!(scans.len(), 1);
    assert_eq!(scans[0].properties().partitioning.partition_count(), 1);
    assert_eq!(scans[0].fetch(), Some(5), "the fetch is on the scan");

    let fetched = scans[0]
        .with_fetch(Some(5))
        .expect("the scan takes a fetch");
    let batches = execute_alone(&ctx, &fetched).await;
    let values = column_texts(&batches, "s");
    assert_eq!(values.len(), 5, "exactly five rows");
    assert_eq!(values, want);

    for plan in [&fetched, scans[0]] {
        let stats = StatisticsContext::new()
            .compute(plan.as_ref(), &StatisticsArgs::new())
            .expect("statistics");
        assert!(
            !matches!(stats.num_rows, Precision::Exact(_)),
            "no exact row count: {:?}",
            stats.num_rows
        );
    }
}

/// Every name `rows_dropped_by_row_filter` and the scan's other row counters
/// are published under, so the counter renders in `EXPLAIN ANALYZE` beside
/// them.
#[tokio::test]
async fn the_counter_is_published_beside_the_scans_row_counters() {
    let (store, segments) = t3_fixture().await;
    let declared = wide_declared();
    let ctx = session(provider(&store, segments, &declared));
    let (plan, _) = run(&ctx, "SELECT ts, s FROM logs WHERE s <> ''").await;
    let scans = nodes(&plan, "LogsScanExec");
    let metrics = scans[0].metrics().expect("metrics");
    let names: BTreeMap<String, usize> = metrics
        .iter()
        .map(|m| (m.value().name().to_string(), m.value().as_usize()))
        .collect();
    assert!(names.contains_key("rows_predicate_evaluated"));
    assert_eq!(names.get("rows_dropped_by_row_filter"), Some(&6));
    let analyzed = ctx
        .sql("EXPLAIN ANALYZE SELECT ts, s FROM logs WHERE s <> ''")
        .await
        .expect("explain")
        .collect()
        .await
        .expect("explain runs");
    let text = pretty(&analyzed);
    assert!(text.contains("rows_dropped_by_row_filter=6"), "{text}");
}

// --- T1 and T3 through SQL -----------------------------------------------------

/// The text of the one `LogsScanExec` line in `plan`.
fn scan_line(plan: &Arc<dyn ExecutionPlan>) -> String {
    let scans = nodes(plan, "LogsScanExec");
    assert_eq!(
        scans.len(),
        1,
        "{}",
        displayable(plan.as_ref()).indent(true)
    );
    displayable(scans[0].as_ref()).one_line().to_string()
}

/// T1 through SQL: the planned and optimized plans of `s <> ''` and
/// `'' <> s`, alone and beside a `ts` bound, hold no `FilterExec`, and the
/// scan's EXPLAIN line names the conjunct's column (`row_filters=[s != ]`)
/// rather than printing a full-schema index that does not match its
/// `projection=[...]`. The shapes near it keep a `FilterExec`, and the
/// conjunct beside a `LIKE` leaves a `FilterExec` holding the `LIKE` only.
///
/// Flipped: a `leaf_is_exact` that does not recognize the shape leaves a
/// `FilterExec` and fails "no FilterExec"; printing the bound expression
/// (`s@4 != `) fails "by column name".
#[tokio::test]
async fn exact_not_equal_conjunct_is_exact_and_leaves_no_filter() {
    let (store, segments) = t3_fixture().await;
    let declared = wide_declared();
    let ctx = session(provider(&store, segments, &declared));
    for sql in [
        "SELECT ts FROM logs WHERE s <> ''",
        "SELECT ts FROM logs WHERE '' <> s",
        "SELECT ts FROM logs WHERE s <> '' AND ts >= TIMESTAMP '1970-01-01 00:00:00'",
    ] {
        let (plan, got) = run(&ctx, sql).await;
        assert_no_filter(&plan, sql);
        let line = scan_line(&plan);
        assert!(
            line.contains("projection=[ts], row_filters=[s != ]"),
            "{sql}: by column name: {line}"
        );
        assert!(!line.contains('@'), "{sql}: no column index: {line}");
        let mut got = timestamps(&got);
        got.sort_unstable();
        assert_eq!(got, kept_by_s(), "{sql}");
    }

    for sql in [
        "SELECT ts FROM logs WHERE body <> 'x'",
        "SELECT ts FROM logs WHERE attrs['k'] <> 'k1'",
        "SELECT ts FROM logs WHERE n <> 1",
        "SELECT ts FROM logs WHERE lower(s) <> 'v2'",
        "SELECT ts FROM logs WHERE s <> t",
        "SELECT ts FROM logs WHERE s <> 'v2' OR t <> 'u2'",
    ] {
        let (plan, _) = run(&ctx, sql).await;
        assert_eq!(
            nodes(&plan, "FilterExec").len(),
            1,
            "{sql}: a FilterExec stays: {}",
            displayable(plan.as_ref()).indent(true)
        );
    }

    let sql = "SELECT ts FROM logs WHERE s <> '' AND s LIKE '%v1%'";
    let (plan, got) = run(&ctx, sql).await;
    let filters = nodes(&plan, "FilterExec");
    assert_eq!(filters.len(), 1, "{sql}");
    let predicate = filters[0]
        .downcast_ref::<FilterExec>()
        .expect("FilterExec")
        .predicate()
        .to_string();
    assert!(
        predicate.to_lowercase().contains("like") && !predicate.contains("!="),
        "{sql}: the FilterExec holds the LIKE only: {predicate}"
    );
    assert!(scan_line(&plan).contains("row_filters=[s != ]"), "{sql}");
    let mut got = timestamps(&got);
    got.sort_unstable();
    assert_eq!(got, [10, 11], "{sql}");
}

/// T3 (a, c) through SQL: the q31 shape and `COUNT(*)` do not select `s`,
/// so once the conjunct is `Exact` the scan's projection does not name it.
/// The scan still builds `s`, filters on it and drops it, and each statement
/// returns the reference answer. `t` is a second declared `Str` column the
/// grouped statement selects.
///
/// Flipped: evaluating only when the column is in the output projection
/// counts every row and fails "the reference answer"; binding the filter to
/// the wrong build column reads `t` and fails the same.
#[tokio::test]
async fn the_q31_and_count_shapes_filter_a_column_they_do_not_select() {
    let (store, segments) = t3_fixture().await;
    let declared = wide_declared();
    let on = session(provider(&store, segments.clone(), &declared));
    let off = session(provider(&store, segments, &declared).with_row_filter_pushdown(false));
    for (sql, rows) in [
        (
            "SELECT n, COUNT(*) AS c FROM logs WHERE s <> '' GROUP BY n ORDER BY n",
            "+---+---+\n| n | c |\n+---+---+\n| 0 | 1 |\n| 1 | 3 |\n| 2 | 2 |\n+---+---+",
        ),
        (
            "SELECT COUNT(*) AS c FROM logs WHERE s <> ''",
            "+---+\n| c |\n+---+\n| 6 |\n+---+",
        ),
        (
            "SELECT t, COUNT(*) AS c FROM logs WHERE s <> '' GROUP BY t ORDER BY t",
            "+-----+---+\n| t   | c |\n+-----+---+\n|     | 2 |\n| u10 | 1 |\n| u2  | 1 |\n\
             | u4  | 1 |\n| u6  | 1 |\n+-----+---+",
        ),
    ] {
        let (plan, got) = run(&on, sql).await;
        let (_, reference) = run(&off, sql).await;
        assert_eq!(
            pretty(&got),
            pretty(&reference),
            "{sql}: the reference answer"
        );
        assert_eq!(pretty(&got), rows, "{sql}");
        assert_no_filter(&plan, sql);
        let line = scan_line(&plan);
        assert!(
            !line.contains("projection=[s") && !line.contains(", s,"),
            "{sql}: {line}"
        );
        assert_eq!(scan_metric(&plan, "rows_dropped_by_row_filter"), 6, "{sql}");
    }
}

// --- T9: plan shape over ClickBench's declared types ---------------------------

/// The ClickBench columns the q13, q22 and q31 shapes read, with the types
/// `hits.mapping.toml` declares for them.
fn clickbench_declared() -> Vec<DeclaredColumn> {
    vec![
        DeclaredColumn::new("SearchPhrase", DeclaredType::Str),
        DeclaredColumn::new("URL", DeclaredType::Str),
        DeclaredColumn::new("SearchEngineID", DeclaredType::I64),
        DeclaredColumn::new("ClientIP", DeclaredType::I64),
        DeclaredColumn::new("IsRefresh", DeclaredType::I64),
        DeclaredColumn::new("ResolutionWidth", DeclaredType::I64),
        DeclaredColumn::new("UserID", DeclaredType::I64),
    ]
}

/// The T9 session's `target_partitions`, and the fixture's object count: an
/// uncached scan plans one partition per object up to `target_partitions`.
const T9_PARTITIONS: usize = 4;

/// [`T9_PARTITIONS`] objects of 16 rows; `SearchPhrase` is `''` on every
/// third row and `URL` holds `google` on every other row.
async fn clickbench_fixture() -> (Arc<MemoryStore>, Vec<SegmentRef>) {
    let store = Arc::new(MemoryStore::new());
    let mut segments = Vec::new();
    for object in 0..T9_PARTITIONS as i64 {
        let records: Vec<LogRecord> = (0..16)
            .map(|i| {
                let ts = object * 100 + i;
                let phrase = if i % 3 == 0 {
                    String::new()
                } else {
                    format!("phrase {}", i % 4)
                };
                let url = if i % 2 == 0 {
                    format!("https://google.com/{i}")
                } else {
                    format!("https://example.com/{i}")
                };
                record(
                    ts,
                    Stream::Bare,
                    &[
                        ("SearchPhrase", s(&phrase)),
                        ("URL", s(&url)),
                        ("SearchEngineID", AttrValue::I64(i % 2)),
                        ("ClientIP", AttrValue::I64(i % 5)),
                        ("IsRefresh", AttrValue::I64(i % 2)),
                        ("ResolutionWidth", AttrValue::I64(1000 + i)),
                        ("UserID", AttrValue::I64(i % 7)),
                    ],
                )
            })
            .collect();
        let bytes = encode(&records, RlogConfig::default());
        let key = format!("logs/clickbench-{object}.rlog");
        segments.push(put(&store, &key, bytes, &records).await);
    }
    (store, segments)
}

pub(crate) fn partitioned_session(
    provider: LogsTableProvider,
    partitions: usize,
) -> SessionContext {
    let mut config = SqlConfig::default();
    config.engine.sql_partition_count = Some(partitions);
    config.first_attempt_min_partitions = 0;
    let pool = Arc::new(TenantDelegatingPool::new(
        1 << 30,
        TenantMemoryAccountant::new(1 << 30),
        CeilingBreach::new(),
        QueryAccounting::new(),
    ));
    build_session(
        &config,
        pool,
        SessionTable::Logs(Arc::new(provider)),
        false,
        SpillDecision::Disabled,
        &ravel_sql::SessionPrincipal::without_capabilities(TenantHash([0u8; 16])),
    )
    .expect("session")
}

/// The name of the node directly above each `LogsScanExec` in `plan`.
fn scan_parents(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<String>) {
    for child in plan.children() {
        if child.name() == "LogsScanExec" {
            out.push(plan.name().to_string());
        }
        scan_parents(child, out);
    }
}

/// T9: over a tenant with ClickBench's declared types, the q13 and q31
/// shapes hold no `FilterExec`, their scans report `target_partitions`
/// partitions, and no `RepartitionExec` sits directly above the scan. The
/// q22 shape keeps a `FilterExec` holding only its `LIKE`; its `<>` moves
/// into the scan. Each answer equals the reference answer.
///
/// Flipped: a scan reporting fewer partitions than `target_partitions`, the
/// condition under which DataFusion puts a round-robin `RepartitionExec` over
/// it, fails "the scan reports target_partitions partitions"; partial credit
/// for a compound
/// conjunct is not reachable through SQL, because the optimizer offers the
/// provider each top-level conjunct on its own, so that wrong implementation
/// is caught by `str_not_equal_is_exact_and_nothing_near_it_is` and
/// `str_not_equal_filters_are_exact_only_when_the_scan_filters` instead.
#[tokio::test]
async fn clickbench_shapes_plan_without_a_filter_over_the_scan() {
    let (store, segments) = clickbench_fixture().await;
    let declared = clickbench_declared();
    let on = partitioned_session(provider(&store, segments.clone(), &declared), T9_PARTITIONS);
    let off = partitioned_session(
        provider(&store, segments, &declared).with_row_filter_pushdown(false),
        T9_PARTITIONS,
    );
    let q13 = "SELECT \"SearchPhrase\", COUNT(*) AS c FROM logs WHERE \"SearchPhrase\" <> '' \
               GROUP BY \"SearchPhrase\" ORDER BY c DESC, \"SearchPhrase\" LIMIT 10";
    let q31 = "SELECT \"SearchEngineID\", \"ClientIP\", COUNT(*) AS c, SUM(\"IsRefresh\"), \
               AVG(\"ResolutionWidth\") FROM logs WHERE \"SearchPhrase\" <> '' \
               GROUP BY \"SearchEngineID\", \"ClientIP\" \
               ORDER BY c DESC, \"SearchEngineID\", \"ClientIP\" LIMIT 10";
    let q22 = "SELECT \"SearchPhrase\", MIN(\"URL\"), COUNT(*) AS c FROM logs \
               WHERE \"URL\" LIKE '%google%' AND \"SearchPhrase\" <> '' \
               GROUP BY \"SearchPhrase\" ORDER BY c DESC, \"SearchPhrase\" LIMIT 10";
    for sql in [q13, q31, q22] {
        let (plan, got) = run(&on, sql).await;
        let (_, reference) = run(&off, sql).await;
        assert_eq!(
            pretty(&got),
            pretty(&reference),
            "{sql}: the reference answer"
        );
        let shown = displayable(plan.as_ref()).indent(true).to_string();
        let scans = nodes(&plan, "LogsScanExec");
        assert_eq!(scans.len(), 1, "{shown}");
        assert_eq!(
            scans[0].properties().partitioning.partition_count(),
            T9_PARTITIONS,
            "{sql}: the scan reports target_partitions partitions: {shown}"
        );
        assert!(
            scan_line(&plan).contains("row_filters=[SearchPhrase != ]"),
            "{sql}: the <> is in the scan: {shown}"
        );
        let filters = nodes(&plan, "FilterExec");
        let mut parents = Vec::new();
        scan_parents(&plan, &mut parents);
        if sql == q22 {
            assert_eq!(filters.len(), 1, "q22 keeps a FilterExec: {shown}");
            let predicate = filters[0]
                .downcast_ref::<FilterExec>()
                .expect("FilterExec")
                .predicate()
                .to_string();
            assert!(
                predicate.contains("URL") && !predicate.contains("SearchPhrase"),
                "q22's FilterExec holds only the LIKE: {predicate}"
            );
            assert_eq!(parents, ["FilterExec"], "{shown}");
        } else {
            assert!(filters.is_empty(), "{sql}: no FilterExec: {shown}");
            assert!(
                parents.iter().all(|p| p != "RepartitionExec"),
                "{sql}: no RepartitionExec directly above the scan: {shown}"
            );
        }
    }
}
