//! ADR-0873 wave 4: `MIN`/`MAX` over a declared column answered from the
//! `SegmentRef` stamps that snapshot resolution already read, with zero data
//! GETs (issue #873's ClickBench q07 shape, `SELECT MIN(EventDate),
//! MAX(EventDate) FROM hits`).
//!
//! `COUNT(<declared column>)` rides the same statistics object, since the exact
//! NULL count is reported beside the extrema and DataFusion rewrites the
//! aggregate into `num_rows - null_count`; both statement shapes are covered
//! here, over both stamp-eligible types (`I64` and `BOOL`).
//!
//! Every positive test pins the same four facts, the way
//! `logs_count_from_stats.rs` pins them for `COUNT(*)`: the answer equals what
//! a scan of the same objects returns, the physical plan carries no
//! `LogsScanExec` (the scan was elided by DataFusion's stock
//! `AggregateStatistics` rule, not merely pruned), the store served exactly
//! zero GETs, and `data_objects_touched` is zero -- a statement answered from
//! statistics touches no object at all, so neither
//! `record_open_shape` (the fast path) nor `plan_segment` (the planned route)
//! can have run.
//!
//! Every negative test pins the #849 safety lemma extended to the new carrier:
//! whenever exactness cannot be proven for a column (one segment unstamped, a
//! stamp of an ineligible type, a duplicated stamp name, or the two carriers
//! disagreeing) the column's statistics stay `Precision::Absent`, the rule
//! does not fire, and the query scans to the correct answer.
//!
//! The objects here are real RLOG objects and every stamp is derived from the
//! same rows the object was written from, so a rewrite that answers a
//! different question than the one asked shows up as a wrong number rather
//! than as an agreement between two fabrications.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, BooleanArray, Int64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::stats::Precision;
use datafusion::physical_plan::{Statistics, collect, displayable};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use ravel_catalog::{
    DeclaredColumnStats, EntryIdentity, LoadedColumnStats, SegmentLevel, SegmentRef, Snapshot,
};
use ravel_commit::declared_stats::{
    encode as encode_stamp, read_commit_record, read_compaction_part,
};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_proto::catalog::v1::{ColumnStat, ColumnStatsSegment, ColumnValue};
use ravel_proto::commit::v1::{
    CommitRecord, CompactionPart, DeclaredColumnMinMax, DeclaredColumnStatValue,
    declared_column_stat_value::Kind as StampKind,
};
use ravel_query::LogSegmentFetcher;
use ravel_sql::{
    DeclaredColumn, DeclaredType, LogsTableProvider, SessionTable, SpillDecision, SqlConfig,
    TenantMemoryAccountant, build_session,
};
use ravel_types::TenantHash;
use ravel_types::accounting::{AccountedOp, QueryAccounting};
use ravel_types::declared_stats::{
    DeclaredColumnStat, DeclaredStatType, DeclaredStatValue, TYPED_ATTR_COLUMN_TYPE_F64,
};
use ravel_types::logstream::log_stream_id;
use uuid::Uuid;

mod util;
use util::CountingStore;

const TENANT: TenantHash = TenantHash([7u8; 16]);

/// The declared column under test, an I64 attribute, named the way issue
/// #873's ClickBench shape names it (so the SQL needs double quotes).
const COL: &str = "EventDate";

/// Segment A's rows: `(ts, EventDate)`, one NULL.
const SEG_A_ROWS: &[(i64, Option<i64>)] = &[(100, Some(19_000)), (101, None), (102, Some(18_500))];
/// Segment B's rows, holding both the corpus minimum and its maximum.
const SEG_B_ROWS: &[(i64, Option<i64>)] = &[(200, Some(17_100)), (201, Some(19_400))];
/// A segment whose declared column reads NULL in every row: the stamp carries
/// no extrema and `null_count == sample_count`, which is the exact statement
/// "zero non-null values" rather than a gap (ADR-0873 clause 5).
const ALL_NULL_ROWS: &[(i64, Option<i64>)] = &[(300, None), (301, None), (302, None)];

/// The BOOL declared column under test, the other half of ADR-0873 decision
/// 2's stamp allowlist. ClickBench's `IsRefresh` is the shape.
const BOOL_COL: &str = "IsRefresh";
/// The BOOL corpus, split over two objects: every value in the first is
/// `false`, so the corpus maximum lives in the second and an implementation
/// that answered from one segment alone would report `false` for `MAX`.
const BOOL_SEG_A_ROWS: &[(i64, Option<bool>)] =
    &[(100, Some(false)), (101, None), (102, Some(false))];
const BOOL_SEG_B_ROWS: &[(i64, Option<bool>)] = &[(200, Some(true)), (201, Some(false))];

/// The true extrema and NULL count over both segments, computed from the same
/// constants the objects are written from.
fn true_answer() -> (Option<i64>, Option<i64>) {
    let vals: Vec<i64> = SEG_A_ROWS
        .iter()
        .chain(SEG_B_ROWS.iter())
        .filter_map(|(_, v)| *v)
        .collect();
    (vals.iter().min().copied(), vals.iter().max().copied())
}

/// One record carrying `value` under `attr_key`, or carrying no attribute at
/// all when `value` is `None` (which is how a NULL declared column reads).
fn record(ts: i64, attr_key: &str, value: Option<AttrValue>) -> LogRecord {
    let resource: Vec<(String, AttrValue)> = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    let attrs = match value {
        Some(v) => vec![(attr_key.to_string(), v)],
        None => Vec::new(),
    };
    LogRecord {
        stream_id: log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: format!("row at {ts}"),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs,
    }
}

/// Write `rows` (`(ts, attribute value)`, a `None` value meaning the record
/// carries no such attribute) as one real RLOG object under `attr_key`, and
/// return its unstamped [`SegmentRef`].
async fn write_attr_segment(
    store: &Arc<CountingStore>,
    key: &str,
    seq: u64,
    attr_key: &str,
    rows: &[(i64, Option<AttrValue>)],
) -> SegmentRef {
    let mut w = RlogWriter::new(
        RlogConfig::default(),
        ObjectIdentity {
            tenant_hash: TENANT.0,
            shard: 0,
            writer_id: [2u8; 16],
            writer_epoch: 1,
            writer_seq: seq,
        },
    );
    for (ts, v) in rows {
        w.push(record(*ts, attr_key, v.clone())).expect("push");
    }
    let bytes = w.finish().expect("finish");
    let object_size = bytes.len() as u64;
    store
        .put(key, bytes::Bytes::from(bytes), PutOptions::default())
        .await
        .expect("put");
    SegmentRef {
        data_object_key: key.to_string(),
        object_size,
        min_event_ts_ns: rows.iter().map(|(ts, _)| *ts).min().expect("nonempty"),
        max_event_ts_ns: rows.iter().map(|(ts, _)| *ts).max().expect("nonempty"),
        ingest_hour_bucket: 0,
        sample_count: rows.len() as u64,
        series_count: 0,
        shard: 0,
        content_hash: [u8::try_from(seq).expect("small seq"); 32],
        writer_id: Uuid::from_u128(1),
        writer_epoch: 1,
        writer_seq: seq,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        declared_column_stats: DeclaredColumnStats::default(),
    }
}

/// The I64 corpus writer: `rows` as one real RLOG object under [`COL`].
async fn write_segment(
    store: &Arc<CountingStore>,
    key: &str,
    seq: u64,
    rows: &[(i64, Option<i64>)],
) -> SegmentRef {
    let rows: Vec<(i64, Option<AttrValue>)> = rows
        .iter()
        .map(|(ts, v)| (*ts, v.map(AttrValue::I64)))
        .collect();
    write_attr_segment(store, key, seq, COL, &rows).await
}

/// The same for the BOOL corpus, under [`BOOL_COL`].
async fn write_bool_segment(
    store: &Arc<CountingStore>,
    key: &str,
    seq: u64,
    rows: &[(i64, Option<bool>)],
) -> SegmentRef {
    let rows: Vec<(i64, Option<AttrValue>)> = rows
        .iter()
        .map(|(ts, v)| (*ts, v.map(AttrValue::Bool)))
        .collect();
    write_attr_segment(store, key, seq, BOOL_COL, &rows).await
}

/// The exact stamp a correct writer produces for `rows`: extrema over the
/// non-null values, absent when there are none, and the exact NULL count.
fn stamp_for(rows: &[(i64, Option<i64>)]) -> DeclaredColumnStat {
    let vals: Vec<i64> = rows.iter().filter_map(|(_, v)| *v).collect();
    let null_count = u64::try_from(rows.len() - vals.len()).expect("fits");
    DeclaredColumnStat::new(
        COL,
        DeclaredStatType::I64,
        vals.iter().min().map(|v| DeclaredStatValue::I64(*v)),
        vals.iter().max().map(|v| DeclaredStatValue::I64(*v)),
        null_count,
    )
    .expect("valid stamp")
}

/// The same for the BOOL corpus: `false` sorts below `true`, so the extrema of
/// a mixed segment are exactly `(false, true)`.
fn bool_stamp_for(rows: &[(i64, Option<bool>)]) -> DeclaredColumnStat {
    let vals: Vec<bool> = rows.iter().filter_map(|(_, v)| *v).collect();
    let null_count = u64::try_from(rows.len() - vals.len()).expect("fits");
    DeclaredColumnStat::new(
        BOOL_COL,
        DeclaredStatType::Bool,
        vals.iter().min().map(|v| DeclaredStatValue::Bool(*v)),
        vals.iter().max().map(|v| DeclaredStatValue::Bool(*v)),
        null_count,
    )
    .expect("valid bool stamp")
}

fn i64_stamp_entry(name: &str, min: i64, max: i64, null_count: u64) -> DeclaredColumnMinMax {
    encode_stamp(
        &DeclaredColumnStat::new(
            name,
            DeclaredStatType::I64,
            Some(DeclaredStatValue::I64(min)),
            Some(DeclaredStatValue::I64(max)),
            null_count,
        )
        .expect("valid stamp"),
    )
}

/// A stamp entry carrying ADR-0101's `F64` declared type: representable on the
/// wire (a broken or future writer can emit one) and never eligible, since
/// ADR-0873 decision 2's allowlist refuses `F64` by name pending a decided
/// comparator, NaN rule, and `-0.0` rule.
fn f64_stamp_entry(name: &str) -> DeclaredColumnMinMax {
    DeclaredColumnMinMax {
        name: name.to_string(),
        declared_type: TYPED_ATTR_COLUMN_TYPE_F64,
        min: Some(DeclaredColumnStatValue {
            kind: Some(StampKind::I64(17_100)),
        }),
        max: Some(DeclaredColumnStatValue {
            kind: Some(StampKind::I64(19_400)),
        }),
        null_count: 0,
    }
}

/// Put `entries` on `seg` the way resolution does: through the carrier read
/// whose row-count clauses bind against the record's own `sample_count`, so
/// the stamps a `SegmentRef` carries are exactly the ones the predicate
/// admitted. There is no other way to build a non-empty
/// [`DeclaredColumnStats`], which is the point of ADR-0873 decision 2.
fn stamped(seg: &SegmentRef, entries: Vec<DeclaredColumnMinMax>) -> SegmentRef {
    let record = CommitRecord {
        sample_count: seg.sample_count,
        declared_column_stats: entries,
        ..CommitRecord::default()
    };
    let mut out = seg.clone();
    out.declared_column_stats = DeclaredColumnStats::from_validated(&read_commit_record(&record));
    out
}

/// The same carriage through the compaction/rewrite part route (`CompactionPart`
/// field 12), for the tests that must hold on every read route.
fn stamped_via_part(seg: &SegmentRef, entries: Vec<DeclaredColumnMinMax>) -> SegmentRef {
    let part = CompactionPart {
        sample_count: seg.sample_count,
        declared_column_stats: entries,
        ..CompactionPart::default()
    };
    let mut out = seg.clone();
    out.declared_column_stats = DeclaredColumnStats::from_validated(&read_compaction_part(&part));
    out
}

fn identity_of(seg: &SegmentRef) -> EntryIdentity {
    (
        seg.ingest_hour_bucket,
        seg.shard,
        *seg.writer_id.as_bytes(),
        seg.writer_epoch,
        seg.writer_seq,
    )
}

fn i64_value(v: i64) -> ColumnValue {
    ColumnValue {
        kind: Some(ravel_proto::catalog::v1::column_value::Kind::I64(v)),
    }
}

/// A `.cstat` entry for `rows`, with no value dictionary: this path reads only
/// the extrema and the row accounting.
fn cstat_for(rows: &[(i64, Option<i64>)], min: Option<i64>, max: Option<i64>) -> ColumnStat {
    let non_null_count =
        u64::try_from(rows.iter().filter(|(_, v)| v.is_some()).count()).expect("fits");
    ColumnStat {
        name: COL.to_string(),
        declared_type: 2, // ravel.sys.v1.TypedAttrColumnType::I64
        non_null_count,
        null_count: u64::try_from(rows.len()).expect("fits") - non_null_count,
        min: min.map(i64_value),
        max: max.map(i64_value),
        dictionary_present: false,
        dictionary: Vec::new(),
        sum: None,
    }
}

fn loaded_stats(entries: Vec<(&SegmentRef, Vec<ColumnStat>)>) -> Arc<LoadedColumnStats> {
    let mut segments = HashMap::new();
    for (seg, columns) in entries {
        segments.insert(
            identity_of(seg),
            ColumnStatsSegment {
                ingest_hour_bucket: seg.ingest_hour_bucket,
                shard: seg.shard,
                writer_id: seg.writer_id.as_bytes().to_vec(),
                writer_epoch: seg.writer_epoch,
                writer_seq: seg.writer_seq,
                columns,
            },
        );
    }
    Arc::new(LoadedColumnStats {
        segments,
        part_blake3: Vec::new(),
    })
}

fn snapshot_of(segments: Vec<SegmentRef>) -> Snapshot {
    Snapshot {
        segments,
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    }
}

fn declared_cols() -> Vec<DeclaredColumn> {
    vec![DeclaredColumn::new(COL, DeclaredType::I64)]
}

/// The tenant's declaration for the BOOL tests: one BOOL column, so it sits at
/// [`ravel_sql::FIRST_DECLARED_COL`] exactly as the I64 column does in the
/// others.
fn bool_declared_cols() -> Vec<DeclaredColumn> {
    vec![DeclaredColumn::new(BOOL_COL, DeclaredType::Bool)]
}

fn provider_with(
    store: &Arc<CountingStore>,
    snapshot: Snapshot,
    accounting: &QueryAccounting,
    stats: Option<Arc<LoadedColumnStats>>,
    declared: Vec<DeclaredColumn>,
) -> LogsTableProvider {
    let backend: Arc<dyn ObjectStoreBackend> = Arc::clone(store) as Arc<dyn ObjectStoreBackend>;
    LogsTableProvider::new(
        snapshot,
        TENANT,
        LogSegmentFetcher::new(backend),
        accounting.clone(),
    )
    .with_declared_columns(declared)
    .with_column_stats(stats)
}

fn logs_session(provider: LogsTableProvider) -> datafusion::error::Result<SessionContext> {
    let config = SqlConfig::default();
    let tenant = TenantMemoryAccountant::new(1 << 30);
    let (pool, _breach) = config.query_pool(tenant, QueryAccounting::new());
    build_session(
        &config,
        pool,
        SessionTable::Logs(Arc::new(provider)),
        false,
        SpillDecision::Disabled,
    )
}

/// The single `COUNT(...)` value from a one-row, one-column result, or `None`
/// when the aggregate is SQL NULL (which `COUNT` never is, so a `None` here is
/// a failure the caller should assert on).
fn count_value(batches: &[RecordBatch]) -> Option<i64> {
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 count");
        return if arr.is_null(0) {
            None
        } else {
            Some(arr.value(0))
        };
    }
    None
}

/// `(min, max)` from `SELECT MIN(bool_col), MAX(bool_col)`; a component is
/// `None` when the aggregate is SQL NULL.
fn min_max_bool(batches: &[RecordBatch]) -> (Option<bool>, Option<bool>) {
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let col = |i: usize| {
            let arr = batch
                .column(i)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("boolean aggregate");
            if arr.is_null(0) {
                None
            } else {
                Some(arr.value(0))
            }
        };
        return (col(0), col(1));
    }
    (None, None)
}

/// `(min, max)` from `SELECT MIN(col), MAX(col)`; a component is `None` when
/// the aggregate is SQL NULL.
fn min_max_i64(batches: &[RecordBatch]) -> (Option<i64>, Option<i64>) {
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let col = |i: usize| {
            let arr = batch
                .column(i)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("int64 aggregate");
            if arr.is_null(0) {
                None
            } else {
                Some(arr.value(0))
            }
        };
        return (col(0), col(1));
    }
    (None, None)
}

/// One statement's outcome: the plan text, the answer, the store's GET count,
/// the accounted data GETs, and `data_objects_touched`.
struct Outcome {
    plan: String,
    answer: (Option<i64>, Option<i64>),
    gets: u64,
    accounted_gets: u64,
    objects_touched: u64,
}

/// One statement's raw outcome, for the statements whose answer the caller
/// reads out of the batches itself.
struct Run {
    plan: String,
    batches: Vec<RecordBatch>,
    gets: u64,
    accounted_gets: u64,
    objects_touched: u64,
}

/// Run `sql` over `snapshot` against the objects already written into `store`,
/// with `declared` as the tenant's declared columns.
async fn run_over_store(
    store: &Arc<CountingStore>,
    snapshot: Snapshot,
    stats: Option<Arc<LoadedColumnStats>>,
    declared: Vec<DeclaredColumn>,
    sql: &str,
) -> Run {
    let accounting = QueryAccounting::new();
    let before_gets = store.gets();
    let ctx = logs_session(provider_with(store, snapshot, &accounting, stats, declared))
        .expect("session");
    let plan = ctx
        .sql(sql)
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let text = displayable(plan.as_ref()).indent(true).to_string();
    let batches = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("collect");
    let snap = accounting.snapshot();
    Run {
        plan: text,
        batches,
        gets: store.gets() - before_gets,
        accounted_gets: snap.s3_requests[AccountedOp::Get.index()],
        objects_touched: snap.data_objects_touched,
    }
}

/// Run `SELECT MIN("EventDate"), MAX("EventDate") FROM logs` over `snapshot`,
/// against the objects already written into `store`.
async fn min_max_over_store(
    store: &Arc<CountingStore>,
    snapshot: Snapshot,
    stats: Option<Arc<LoadedColumnStats>>,
) -> Outcome {
    let run = run_over_store(
        store,
        snapshot,
        stats,
        declared_cols(),
        &format!("SELECT MIN(\"{COL}\"), MAX(\"{COL}\") FROM logs"),
    )
    .await;
    Outcome {
        answer: min_max_i64(&run.batches),
        plan: run.plan,
        gets: run.gets,
        accounted_gets: run.accounted_gets,
        objects_touched: run.objects_touched,
    }
}

/// The whole-plan statistics of the bare scan over `snapshot`, which is the
/// entry point `AggregateStatistics` consults. No object-store I/O.
fn scan_stats_for(
    snapshot: Snapshot,
    stats: Option<Arc<LoadedColumnStats>>,
    declared: Vec<DeclaredColumn>,
) -> Arc<Statistics> {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let accounting = QueryAccounting::new();
    let provider = provider_with(&store, snapshot, &accounting, stats, declared);
    let plan = provider.plan_filters(4, &[]).expect("plan_filters");
    plan.partition_statistics(None)
        .expect("partition_statistics")
}

fn scan_stats(snapshot: Snapshot, stats: Option<Arc<LoadedColumnStats>>) -> Arc<Statistics> {
    scan_stats_for(snapshot, stats, declared_cols())
}

/// The declared column's statistics in `stats`. The scan projects every schema
/// column when `plan_filters` is handed no projection, so the declared column
/// sits at its schema index.
fn declared_col_stats(stats: &Statistics) -> &datafusion::common::ColumnStatistics {
    &stats.column_statistics[ravel_sql::FIRST_DECLARED_COL]
}

/// Two real objects, both stamped by a correct writer.
async fn stamped_corpus(store: &Arc<CountingStore>) -> Snapshot {
    let a = write_segment(store, "logs/a.rlog", 1, SEG_A_ROWS).await;
    let b = write_segment(store, "logs/b.rlog", 2, SEG_B_ROWS).await;
    let a = stamped(&a, vec![encode_stamp(&stamp_for(SEG_A_ROWS))]);
    let b = stamped(&b, vec![encode_stamp(&stamp_for(SEG_B_ROWS))]);
    snapshot_of(vec![a, b])
}

// ---------------------------------------------------------------------------
// Test 1: the issue's acceptance criterion.
// ---------------------------------------------------------------------------

/// `SELECT MIN(EventDate), MAX(EventDate)` over a fully stamped tenant is a
/// plan-time literal: no `LogsScanExec`, exactly zero GETs (store-level and
/// accounted), and `data_objects_touched == 0`, because a statement answered
/// from statistics never opens a segment on either route.
///
/// Prove-the-test: comment out the `min_value`/`max_value` assignment in
/// `LogsScanExec::partition_statistics`
/// (crates/ravel-sql/src/logs_scan.rs, the `declared_min_max` loop) and the
/// plan keeps its `LogsScanExec` with 2 GETs and 2 objects touched; every
/// assertion below fails but the answer, which the scan then computes.
/// Narrower flip: return `None` from `stamp_coverage` and the same happens,
/// since no `.cstat` is loaded here.
#[tokio::test]
async fn stamped_min_max_is_answered_with_zero_gets_and_no_touches() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let snapshot = stamped_corpus(&store).await;
    let out = min_max_over_store(&store, snapshot, None).await;

    assert!(
        !out.plan.contains("LogsScanExec"),
        "a stamped MIN/MAX must be a plan-time literal; plan was:\n{}",
        out.plan
    );
    assert_eq!(out.answer, true_answer(), "17_100 .. 19_400");
    assert_eq!(out.gets, 0, "no data object may be read");
    assert_eq!(out.accounted_gets, 0, "and none may be accounted either");
    assert_eq!(
        out.objects_touched, 0,
        "a stats-answered statement touches no object on either route"
    );
}

/// The same corpus, read by a scan (stamps removed), answers identically. This
/// is what makes the test above a statistics test rather than an agreement
/// between two fabrications: the stamps are derived from the same rows the
/// objects were written from, and the scan is the reference implementation.
#[tokio::test]
async fn the_stamped_answer_equals_the_scanned_answer() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let a = write_segment(&store, "logs/a.rlog", 1, SEG_A_ROWS).await;
    let b = write_segment(&store, "logs/b.rlog", 2, SEG_B_ROWS).await;

    let scanned = min_max_over_store(&store, snapshot_of(vec![a.clone(), b.clone()]), None).await;
    assert!(
        scanned.plan.contains("LogsScanExec"),
        "an unstamped snapshot has nothing to answer from; plan was:\n{}",
        scanned.plan
    );
    assert!(scanned.gets > 0, "the reference answer comes from the data");

    let stamped_snapshot = snapshot_of(vec![
        stamped(&a, vec![encode_stamp(&stamp_for(SEG_A_ROWS))]),
        stamped(&b, vec![encode_stamp(&stamp_for(SEG_B_ROWS))]),
    ]);
    let from_stats = min_max_over_store(&store, stamped_snapshot, None).await;
    assert_eq!(from_stats.answer, scanned.answer);
    assert_eq!(from_stats.gets, 0);
}

/// The exact statistics the stamps prove, asserted at the seam
/// `AggregateStatistics` reads: `Exact` extrema, and an `Exact` NULL count
/// summed over the segments (one NULL row in segment A, none in B). The
/// row count stays exact too, so `COUNT(*)` and `COUNT(col)` are both
/// answerable from this one statistics object.
#[tokio::test]
async fn stamped_statistics_are_exact_per_column() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let snapshot = stamped_corpus(&store).await;
    let stats = scan_stats(snapshot, None);
    let col = declared_col_stats(&stats);
    assert_eq!(stats.num_rows, Precision::Exact(5), "3 + 2 rows");
    assert_eq!(
        col.min_value,
        Precision::Exact(ScalarValue::Int64(Some(17_100)))
    );
    assert_eq!(
        col.max_value,
        Precision::Exact(ScalarValue::Int64(Some(19_400)))
    );
    assert_eq!(col.null_count, Precision::Exact(1), "one NULL row, in A");
}

// ---------------------------------------------------------------------------
// Test 2: one unstamped segment leaves the column Absent and the query scans.
// ---------------------------------------------------------------------------

/// Coverage is per segment and per column: one touched segment carrying no
/// stamp leaves the whole column `Absent`, so the rule cannot fire and the
/// statement scans (#849's safety lemma, extended to the stamp carrier). The
/// unstamped segment holds the corpus maximum, so an implementation that
/// answered from the stamped segment alone would report 19_000 instead of
/// 19_400.
///
/// Prove-the-test: replace the `(None, None) => { a.declined = true; continue }`
/// arm in `declared_min_max_all` with `continue` (skip the segment instead of
/// declining) and the plan loses its `LogsScanExec` while the answer becomes
/// `(18_500, 19_000)`: both assertions fail.
#[tokio::test]
async fn one_unstamped_segment_leaves_the_column_absent_and_scans() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let a = write_segment(&store, "logs/a.rlog", 1, SEG_A_ROWS).await;
    let b = write_segment(&store, "logs/b.rlog", 2, SEG_B_ROWS).await;
    let snapshot = snapshot_of(vec![
        stamped(&a, vec![encode_stamp(&stamp_for(SEG_A_ROWS))]),
        b,
    ]);

    let stats = scan_stats(snapshot.clone(), None);
    let col = declared_col_stats(&stats);
    assert_eq!(col.min_value, Precision::Absent);
    assert_eq!(col.max_value, Precision::Absent);
    assert_eq!(col.null_count, Precision::Absent);

    let out = min_max_over_store(&store, snapshot, None).await;
    assert!(
        out.plan.contains("LogsScanExec"),
        "an uncovered segment must fail closed to a scan; plan was:\n{}",
        out.plan
    );
    assert_eq!(
        out.answer,
        true_answer(),
        "the scan answers over both objects"
    );
    assert!(out.gets > 0, "a scanning statement reads the data");
    assert_eq!(out.objects_touched, 2, "both objects, once each");
}

/// A stamp for a DIFFERENT column covers nothing for this one: the column is
/// uncovered even though the segment carries stamps.
#[tokio::test]
async fn a_stamp_for_another_column_leaves_this_one_absent() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let a = write_segment(&store, "logs/a.rlog", 1, SEG_A_ROWS).await;
    let b = write_segment(&store, "logs/b.rlog", 2, SEG_B_ROWS).await;
    let snapshot = snapshot_of(vec![
        stamped(&a, vec![i64_stamp_entry("SomethingElse", 1, 2, 0)]),
        stamped(&b, vec![encode_stamp(&stamp_for(SEG_B_ROWS))]),
    ]);
    let stats = scan_stats(snapshot, None);
    assert_eq!(declared_col_stats(&stats).min_value, Precision::Absent);
}

// ---------------------------------------------------------------------------
// Test 3: the float gate.
// ---------------------------------------------------------------------------

/// A float-typed stamp never takes the shortcut, and the gate is explicit
/// rather than a consequence of today's vocabulary: ADR-0101 (Accepted) adds
/// `TYPED_ATTR_COLUMN_TYPE_F64 = 5`, so the tag is representable now and only
/// the allowlist refuses it. The entry is dropped at the carrier read, so the
/// segment is uncovered, the column is `Absent`, and the statement scans --
/// exactly as it would with no stamp at all.
///
/// Prove-the-test: add `TYPED_ATTR_COLUMN_TYPE_F64 => Ok(DeclaredStatType::I64)`
/// to `DeclaredStatType::from_tag`
/// (crates/ravel-types/src/declared_stats.rs) and the plan loses its
/// `LogsScanExec` while the answer comes from the fabricated float stamp.
#[tokio::test]
async fn a_float_typed_stamp_never_takes_the_shortcut() {
    // The allowlist refuses the tag by name, which is what makes the stamp
    // unrepresentable in the validated form rather than merely unused.
    assert!(
        DeclaredStatType::from_tag(TYPED_ATTR_COLUMN_TYPE_F64).is_err(),
        "F64 is not stamp-eligible (ADR-0873 decision 2)"
    );

    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let a = write_segment(&store, "logs/a.rlog", 1, SEG_A_ROWS).await;
    let b = write_segment(&store, "logs/b.rlog", 2, SEG_B_ROWS).await;
    let a_float = stamped(&a, vec![f64_stamp_entry(COL)]);
    assert!(
        a_float.declared_column_stats.is_empty(),
        "an F64 stamp grants no coverage at all"
    );
    let snapshot = snapshot_of(vec![
        a_float,
        stamped(&b, vec![encode_stamp(&stamp_for(SEG_B_ROWS))]),
    ]);

    let stats = scan_stats(snapshot.clone(), None);
    assert_eq!(declared_col_stats(&stats).min_value, Precision::Absent);

    let out = min_max_over_store(&store, snapshot, None).await;
    assert!(
        out.plan.contains("LogsScanExec"),
        "an ineligible stamp must fail closed to a scan; plan was:\n{}",
        out.plan
    );
    assert_eq!(out.answer, true_answer());
}

/// A stamp whose eligible type disagrees with the type the tenant declares the
/// column as is refused too: the reader will not reinterpret a BOOL extremum
/// as an Int64 one.
#[tokio::test]
async fn a_stamp_of_the_wrong_eligible_type_is_refused() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let a = write_segment(&store, "logs/a.rlog", 1, SEG_A_ROWS).await;
    let b = write_segment(&store, "logs/b.rlog", 2, SEG_B_ROWS).await;
    let bool_stamp = encode_stamp(
        &DeclaredColumnStat::new(
            COL,
            DeclaredStatType::Bool,
            Some(DeclaredStatValue::Bool(false)),
            Some(DeclaredStatValue::Bool(true)),
            1,
        )
        .expect("valid bool stamp"),
    );
    let a_bool = stamped(&a, vec![bool_stamp]);
    // The stamp itself is valid -- it is the join against the I64 declaration
    // that refuses it -- so it does reach the query as coverage-shaped input.
    assert_eq!(a_bool.declared_column_stats.len(), 1);
    let snapshot = snapshot_of(vec![
        a_bool,
        stamped(&b, vec![encode_stamp(&stamp_for(SEG_B_ROWS))]),
    ]);
    let stats = scan_stats(snapshot, None);
    assert_eq!(declared_col_stats(&stats).min_value, Precision::Absent);
}

// ---------------------------------------------------------------------------
// Test 4: duplicate stamp names, on every read route and in both orderings.
// ---------------------------------------------------------------------------

/// A record stamping one name twice yields NO coverage for that name, from
/// every read route and under either ordering of the pair (ADR-0873 clause 6).
/// Both orderings are what make this a test: any pick-one resolution satisfies
/// whichever ordering places its pick where the assertion looks.
///
/// Prove-the-test: restore the first-wins arm in `decode_all_against_rows`
/// (crates/ravel-commit/src/declared_stats.rs: reserve the name in a `HashSet`
/// before validating and drop only later occurrences) and every
/// `is_empty`/`Absent` assertion here fails, with the extremum coming from
/// whichever duplicate was written first (17_000 in one ordering, 1 in the
/// other).
#[tokio::test]
async fn duplicate_stamp_names_grant_no_coverage_on_any_read_route() {
    for (first, second) in [(1i64, 17_000i64), (17_000, 1)] {
        let pair = vec![
            i64_stamp_entry(COL, first, first + 1, 0),
            i64_stamp_entry(COL, second, second + 1, 0),
        ];
        let store = CountingStore::new(Arc::new(MemoryStore::new()));
        let a = write_segment(&store, "logs/a.rlog", 1, SEG_A_ROWS).await;
        let b = write_segment(&store, "logs/b.rlog", 2, SEG_B_ROWS).await;

        // Route 1: the commit record (a listed or token-resolved L0 segment).
        let via_record = stamped(&a, pair.clone());
        // Route 2: the compaction/rewrite part (an L1 segment).
        let via_part = stamped_via_part(&a, pair.clone());
        for (route, seg) in [
            ("commit record", &via_record),
            ("compaction part", &via_part),
        ] {
            assert!(
                seg.declared_column_stats.is_empty(),
                "{route}: a duplicated name grants no coverage (order {first},{second})"
            );
            assert_eq!(seg.declared_column_stats.column(COL), None);
        }

        let snapshot = snapshot_of(vec![
            via_record,
            stamped(&b, vec![encode_stamp(&stamp_for(SEG_B_ROWS))]),
        ]);
        let stats = scan_stats(snapshot.clone(), None);
        assert_eq!(
            declared_col_stats(&stats).min_value,
            Precision::Absent,
            "order {first},{second}"
        );

        let out = min_max_over_store(&store, snapshot, None).await;
        assert!(
            out.plan.contains("LogsScanExec"),
            "a duplicated stamp name must fail closed to a scan (order {first},{second}); \
             plan was:\n{}",
            out.plan
        );
        assert_eq!(
            out.answer,
            true_answer(),
            "the scan answers over the real rows, not from either duplicate \
             (order {first},{second})"
        );
    }
}

// ---------------------------------------------------------------------------
// The union of the two carriers (ADR-0873 decision 4).
// ---------------------------------------------------------------------------

/// A snapshot split between carriers -- one segment stamped only, the other
/// covered only by `.cstat` -- is still fully covered: that split is the
/// normal state of every tenant after this ADR ships (a live tail with stamps
/// above a pre-stamp sealed history), so a reader consulting one carrier alone
/// would answer nothing.
///
/// Prove-the-test: drop the `.cstat` half of the union (`(None, Some(only))`
/// arm) and this plan regains its `LogsScanExec`.
#[tokio::test]
async fn one_carrier_each_still_covers_the_whole_snapshot() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let a = write_segment(&store, "logs/a.rlog", 1, SEG_A_ROWS).await;
    let b = write_segment(&store, "logs/b.rlog", 2, SEG_B_ROWS).await;
    let stats = loaded_stats(vec![(
        &b,
        vec![cstat_for(SEG_B_ROWS, Some(17_100), Some(19_400))],
    )]);
    let snapshot = snapshot_of(vec![
        stamped(&a, vec![encode_stamp(&stamp_for(SEG_A_ROWS))]),
        b.clone(),
    ]);

    let out = min_max_over_store(&store, snapshot.clone(), Some(Arc::clone(&stats))).await;
    assert!(
        !out.plan.contains("LogsScanExec"),
        "stamp for A plus .cstat for B covers everything; plan was:\n{}",
        out.plan
    );
    assert_eq!(out.answer, true_answer());
    assert_eq!(out.gets, 0);

    // The NULL count, by contrast, stays `Absent`: segment B's figure comes
    // from a `.cstat` entry, which is reconciled against the joined
    // `sample_count` before any grant but whose NULL count is deliberately
    // not promoted to proven. The extrema are exact regardless.
    let col_stats = scan_stats(snapshot, Some(stats));
    let col = declared_col_stats(&col_stats);
    assert_eq!(
        col.min_value,
        Precision::Exact(ScalarValue::Int64(Some(17_100)))
    );
    assert_eq!(col.null_count, Precision::Absent);
}

/// Both carriers covering one segment and agreeing: the answer is that value,
/// counted once. A union that summed the two `null_count`s would report 2 for
/// segment A's single NULL row, which is what the NULL-count assertion here
/// fails on.
#[tokio::test]
async fn both_carriers_agreeing_answer_once() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let a = write_segment(&store, "logs/a.rlog", 1, SEG_A_ROWS).await;
    let a_stamped = stamped(&a, vec![encode_stamp(&stamp_for(SEG_A_ROWS))]);
    let stats = loaded_stats(vec![(
        &a,
        vec![cstat_for(SEG_A_ROWS, Some(18_500), Some(19_000))],
    )]);
    let snapshot = snapshot_of(vec![a_stamped]);

    let col_stats = scan_stats(snapshot.clone(), Some(Arc::clone(&stats)));
    let col = declared_col_stats(&col_stats);
    assert_eq!(
        col.min_value,
        Precision::Exact(ScalarValue::Int64(Some(18_500)))
    );
    assert_eq!(
        col.max_value,
        Precision::Exact(ScalarValue::Int64(Some(19_000)))
    );
    assert_eq!(col.null_count, Precision::Exact(1), "one NULL row, once");

    let out = min_max_over_store(&store, snapshot, Some(stats)).await;
    assert!(
        !out.plan.contains("LogsScanExec"),
        "plan was:\n{}",
        out.plan
    );
    assert_eq!(out.answer, (Some(18_500), Some(19_000)));
    assert_eq!(out.gets, 0);
}

/// Both carriers covering one segment and disagreeing: the column is `Absent`
/// for the whole query and the statement scans. One conflicted segment poisons
/// the column exactly as one uncovered segment does -- the segment is
/// immutable and both carriers claim exactness, so a disagreement means one of
/// them is wrong and no answer is safe.
///
/// Prove-the-test: replace the `agrees_with` check in `declared_min_max_all`
/// with `true` and the plan loses its `LogsScanExec` while the answer becomes
/// whichever carrier the union happens to keep.
#[tokio::test]
async fn carriers_disagreeing_about_one_segment_decline() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let a = write_segment(&store, "logs/a.rlog", 1, SEG_A_ROWS).await;
    let a_stamped = stamped(&a, vec![encode_stamp(&stamp_for(SEG_A_ROWS))]);
    // Same segment, same column, a min the stamp does not agree with.
    let stats = loaded_stats(vec![(
        &a,
        vec![cstat_for(SEG_A_ROWS, Some(-1), Some(19_000))],
    )]);
    let snapshot = snapshot_of(vec![a_stamped]);

    let col_stats = scan_stats(snapshot.clone(), Some(Arc::clone(&stats)));
    assert_eq!(declared_col_stats(&col_stats).min_value, Precision::Absent);

    let out = min_max_over_store(&store, snapshot, Some(stats)).await;
    assert!(
        out.plan.contains("LogsScanExec"),
        "conflicting carriers must fail closed to a scan; plan was:\n{}",
        out.plan
    );
    assert_eq!(
        out.answer,
        (Some(18_500), Some(19_000)),
        "the scan answers segment A's real extrema, not the fabricated -1"
    );
}

// ---------------------------------------------------------------------------
// `COUNT(<declared column>)`, the second statement shape this statistics
// object answers (ADR-0873: the exact NULL count is reported beside the
// extrema, so DataFusion's `Count::value_from_stats` rewrites the aggregate
// into `num_rows - null_count`).
// ---------------------------------------------------------------------------

/// `COUNT(<declared column>)` over a stamped tenant is a plan-time literal,
/// equal to `sample_count - null_count` with the NULL count taken ONCE.
///
/// Both segments here are covered by BOTH carriers, in agreement, which is what
/// makes the exact value the assertion: a union that summed the carriers'
/// `null_count`s instead of taking one would answer 3, and one that dropped the
/// NULL count entirely would leave the column `Absent` and scan. The corpus has
/// 5 rows and exactly one NULL, so the only correct answer is 4.
///
/// Prove-the-test: in `LogsScanExec::declared_min_max_all`
/// (crates/ravel-sql/src/logs_scan.rs) replace the union's
/// `null_count_proven: stamp.null_count_proven || cstat.null_count_proven,
/// ..stamp` arm with an arm that sums (`null_count: stamp.null_count +
/// cstat.null_count`) and this reads 3; delete the `col.null_count =
/// Precision::Exact(nulls)` assignment in `partition_statistics` and the plan
/// keeps its `LogsScanExec` with 2 GETs while the count still answers 4.
#[tokio::test]
async fn count_of_a_stamped_column_subtracts_the_null_count_exactly_once() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let a = write_segment(&store, "logs/a.rlog", 1, SEG_A_ROWS).await;
    let b = write_segment(&store, "logs/b.rlog", 2, SEG_B_ROWS).await;
    let stats = loaded_stats(vec![
        (&a, vec![cstat_for(SEG_A_ROWS, Some(18_500), Some(19_000))]),
        (&b, vec![cstat_for(SEG_B_ROWS, Some(17_100), Some(19_400))]),
    ]);
    let snapshot = snapshot_of(vec![
        stamped(&a, vec![encode_stamp(&stamp_for(SEG_A_ROWS))]),
        stamped(&b, vec![encode_stamp(&stamp_for(SEG_B_ROWS))]),
    ]);

    // The seam `Count::value_from_stats` reads: both figures exact, and the
    // NULL count is 1 rather than the 2 a carrier-summing union would report.
    let col_stats = scan_stats(snapshot.clone(), Some(Arc::clone(&stats)));
    assert_eq!(col_stats.num_rows, Precision::Exact(5), "3 + 2 rows");
    assert_eq!(
        declared_col_stats(&col_stats).null_count,
        Precision::Exact(1),
        "one NULL row, counted once across two agreeing carriers"
    );

    let run = run_over_store(
        &store,
        snapshot,
        Some(stats),
        declared_cols(),
        &format!("SELECT COUNT(\"{COL}\") FROM logs"),
    )
    .await;
    assert!(
        !run.plan.contains("LogsScanExec"),
        "a stamped COUNT(col) must be a plan-time literal; plan was:\n{}",
        run.plan
    );
    assert_eq!(
        count_value(&run.batches),
        Some(4),
        "5 rows minus the one NULL row, taken once (3 if the carriers were summed)"
    );
    assert_eq!(run.gets, 0, "no data object may be read");
    assert_eq!(run.accounted_gets, 0, "and none may be accounted either");
    assert_eq!(run.objects_touched, 0);
}

/// The same count is what a scan of the same objects returns, which is what
/// makes the test above a statistics test rather than an agreement between two
/// fabrications.
#[tokio::test]
async fn the_stamped_count_equals_the_scanned_count() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let a = write_segment(&store, "logs/a.rlog", 1, SEG_A_ROWS).await;
    let b = write_segment(&store, "logs/b.rlog", 2, SEG_B_ROWS).await;
    let sql = format!("SELECT COUNT(\"{COL}\") FROM logs");

    let scanned = run_over_store(
        &store,
        snapshot_of(vec![a.clone(), b.clone()]),
        None,
        declared_cols(),
        &sql,
    )
    .await;
    assert!(
        scanned.plan.contains("LogsScanExec"),
        "an unstamped snapshot has nothing to answer from; plan was:\n{}",
        scanned.plan
    );
    assert!(scanned.gets > 0, "the reference answer comes from the data");

    let from_stats = run_over_store(
        &store,
        snapshot_of(vec![
            stamped(&a, vec![encode_stamp(&stamp_for(SEG_A_ROWS))]),
            stamped(&b, vec![encode_stamp(&stamp_for(SEG_B_ROWS))]),
        ]),
        None,
        declared_cols(),
        &sql,
    )
    .await;
    assert_eq!(
        count_value(&from_stats.batches),
        count_value(&scanned.batches)
    );
    assert_eq!(count_value(&from_stats.batches), Some(4));
    assert_eq!(from_stats.gets, 0);
}

/// An all-NULL declared column: the stamp carries no extrema and
/// `null_count == sample_count`, which is an exact statement about the segment
/// and not a gap. `COUNT(col)` is then exactly 0, answered at plan time with
/// zero GETs, and `MIN`/`MAX` are SQL NULL.
///
/// The `MIN`/`MAX` half of that answer does NOT come from statistics, and the
/// assertions say so: this scan reports `Precision::Exact(Int64(None))`, but
/// DataFusion's `FromColumnStatistics for Min`/`for Max`
/// (datafusion-functions-aggregate, `min_max.rs`) refuse an exact extremum that
/// is null (`!val.is_null()`), so the rule does not fire and the statement
/// scans to the same NULL. Only the count is a literal here.
///
/// Prove-the-test: replace `row_count_defect`'s `(Some(_), Some(_)) if
/// null_count == sample_count => "present"` arm with `_ => return None`
/// (crates/ravel-commit/src/declared_stats.rs) and a fabricated-extrema stamp
/// over an all-NULL column becomes coverage; delete the
/// `col.null_count = Precision::Exact(nulls)` assignment in
/// `partition_statistics` (crates/ravel-sql/src/logs_scan.rs) and the count
/// assertion fails with a `LogsScanExec` in the plan and 1 GET.
#[tokio::test]
async fn an_all_null_stamped_column_counts_zero_and_answers_null() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let seg = write_segment(&store, "logs/all-null.rlog", 3, ALL_NULL_ROWS).await;
    let stamp = stamp_for(ALL_NULL_ROWS);
    assert_eq!(
        (stamp.min(), stamp.max(), stamp.null_count()),
        (None, None, 3),
        "zero non-null values, stated exactly"
    );
    let snapshot = snapshot_of(vec![stamped(&seg, vec![encode_stamp(&stamp)])]);

    let col_stats = scan_stats(snapshot.clone(), None);
    let col = declared_col_stats(&col_stats);
    assert_eq!(col_stats.num_rows, Precision::Exact(3));
    assert_eq!(
        col.min_value,
        Precision::Exact(ScalarValue::Int64(None)),
        "the exact extremum of zero non-null values is NULL"
    );
    assert_eq!(col.max_value, Precision::Exact(ScalarValue::Int64(None)));
    assert_eq!(col.null_count, Precision::Exact(3), "every row is NULL");

    let counted = run_over_store(
        &store,
        snapshot.clone(),
        None,
        declared_cols(),
        &format!("SELECT COUNT(\"{COL}\") FROM logs"),
    )
    .await;
    assert!(
        !counted.plan.contains("LogsScanExec"),
        "COUNT over an all-NULL stamped column is a plan-time literal; plan was:\n{}",
        counted.plan
    );
    assert_eq!(
        count_value(&counted.batches),
        Some(0),
        "3 rows minus 3 NULLs, exactly"
    );
    assert_eq!(counted.gets, 0);
    assert_eq!(counted.accounted_gets, 0);
    assert_eq!(counted.objects_touched, 0);

    let out = min_max_over_store(&store, snapshot, None).await;
    assert_eq!(
        out.answer,
        (None, None),
        "SQL MIN/MAX over an all-NULL column is NULL"
    );
    assert!(
        out.plan.contains("LogsScanExec"),
        "DataFusion refuses a null exact extremum, so MIN/MAX scans here even \
         though the statistics are exact; plan was:\n{}",
        out.plan
    );
}

// ---------------------------------------------------------------------------
// The BOOL half of the stamp allowlist.
// ---------------------------------------------------------------------------

/// `MIN`/`MAX` over a stamped BOOL declared column is answered at plan time
/// with zero GETs, under `false < true`: the corpus holds `false` in both
/// objects and `true` only in the second, so the exact answer is
/// `(false, true)` and an implementation that reversed the comparator or read
/// one segment alone would answer `(false, false)` or `(true, true)`.
///
/// Prove-the-test: return `None` from `stamp_type_of`'s
/// `DeclaredType::Bool` arm (crates/ravel-sql/src/logs_scan.rs) and the plan
/// regains its `LogsScanExec` with 2 GETs; swap the `Ordering::Less`/
/// `Ordering::Greater` arguments in the `keep_extreme` calls in
/// `declared_min_max_all` and the answer becomes `(true, false)`.
#[tokio::test]
async fn stamped_bool_min_max_orders_false_below_true_with_zero_gets() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let a = write_bool_segment(&store, "logs/bool-a.rlog", 1, BOOL_SEG_A_ROWS).await;
    let b = write_bool_segment(&store, "logs/bool-b.rlog", 2, BOOL_SEG_B_ROWS).await;
    let a_stamp = bool_stamp_for(BOOL_SEG_A_ROWS);
    assert_eq!(
        (a_stamp.min(), a_stamp.max()),
        (
            Some(DeclaredStatValue::Bool(false)),
            Some(DeclaredStatValue::Bool(false))
        ),
        "the all-false segment stamps false for both extrema"
    );
    let snapshot = snapshot_of(vec![
        stamped(&a, vec![encode_stamp(&a_stamp)]),
        stamped(&b, vec![encode_stamp(&bool_stamp_for(BOOL_SEG_B_ROWS))]),
    ]);

    let col_stats = scan_stats_for(snapshot.clone(), None, bool_declared_cols());
    let col = declared_col_stats(&col_stats);
    assert_eq!(
        col.min_value,
        Precision::Exact(ScalarValue::Boolean(Some(false)))
    );
    assert_eq!(
        col.max_value,
        Precision::Exact(ScalarValue::Boolean(Some(true)))
    );
    assert_eq!(col.null_count, Precision::Exact(1), "one NULL row, in A");

    let run = run_over_store(
        &store,
        snapshot,
        None,
        bool_declared_cols(),
        &format!("SELECT MIN(\"{BOOL_COL}\"), MAX(\"{BOOL_COL}\") FROM logs"),
    )
    .await;
    assert!(
        !run.plan.contains("LogsScanExec"),
        "a stamped BOOL MIN/MAX must be a plan-time literal; plan was:\n{}",
        run.plan
    );
    assert_eq!(
        min_max_bool(&run.batches),
        (Some(false), Some(true)),
        "false < true, over both objects"
    );
    assert_eq!(run.gets, 0, "no data object may be read");
    assert_eq!(run.accounted_gets, 0);
    assert_eq!(run.objects_touched, 0);
}

/// And the stamped BOOL answer is the scanned BOOL answer: the stamps are
/// derived from the same rows the objects were written from, so the scan is the
/// reference implementation for the comparator too.
#[tokio::test]
async fn the_stamped_bool_answer_equals_the_scanned_answer() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let a = write_bool_segment(&store, "logs/bool-a.rlog", 1, BOOL_SEG_A_ROWS).await;
    let b = write_bool_segment(&store, "logs/bool-b.rlog", 2, BOOL_SEG_B_ROWS).await;
    let sql = format!("SELECT MIN(\"{BOOL_COL}\"), MAX(\"{BOOL_COL}\") FROM logs");

    let scanned = run_over_store(
        &store,
        snapshot_of(vec![a.clone(), b.clone()]),
        None,
        bool_declared_cols(),
        &sql,
    )
    .await;
    assert!(
        scanned.plan.contains("LogsScanExec"),
        "an unstamped snapshot has nothing to answer from; plan was:\n{}",
        scanned.plan
    );
    assert!(scanned.gets > 0);
    assert_eq!(min_max_bool(&scanned.batches), (Some(false), Some(true)));

    let from_stats = run_over_store(
        &store,
        snapshot_of(vec![
            stamped(&a, vec![encode_stamp(&bool_stamp_for(BOOL_SEG_A_ROWS))]),
            stamped(&b, vec![encode_stamp(&bool_stamp_for(BOOL_SEG_B_ROWS))]),
        ]),
        None,
        bool_declared_cols(),
        &sql,
    )
    .await;
    assert_eq!(
        min_max_bool(&from_stats.batches),
        min_max_bool(&scanned.batches)
    );
    assert_eq!(from_stats.gets, 0);
}
