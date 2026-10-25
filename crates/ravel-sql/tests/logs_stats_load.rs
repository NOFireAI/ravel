//! Integration tests for the ADR-0850 column-statistics LOAD gate in
//! `SqlExecutor` (issue #888).
//!
//! `plan_pinned` used to issue the two-GET `Catalog::load_column_stats` on
//! every logs plan with declared typed columns, even ones no metadata-only
//! path could ever consume. These tests pin the hoisted eligibility decision
//! by EXACT object-store GET counts through a real [`SqlExecutor`]:
//!
//! - a plan that cannot use statistics (no declared column referenced, or a
//!   content predicate present) issues ZERO GETs: the load is skipped;
//! - a plan that CAN use statistics still resolves them (exactly the HEAD GET
//!   and the one column-stats GET) and still answers from catalog metadata
//!   with no scan. This is the regression the hoist must not cause: skipping
//!   an eligible plan would silently turn ADR-0850 off.
//!
//! The HEAD and `.cstat` objects are built directly with the public
//! `encode_head`/`encode_column_stats_v3` codecs (no fold runs), exactly as
//! `ravel-catalog`'s own `load_column_stats` test builds them. The metadata
//! path never fetches a snapshot part or a data object, so neither needs to
//! exist: the only reads a metadata-answered eligible query makes are the two
//! stats GETs.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use datafusion::arrow::array::{Array, Float64Array, Int64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::displayable;
use futures::StreamExt;
use ravel_catalog::{
    Catalog, CatalogConfig, DEFAULT_MAX_COLUMN_STATS_BYTES, HEAD_FORMAT_VERSION, SegmentLevel,
    SegmentRef, Snapshot, encode_column_stats_v3, encode_head,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_proto::catalog::v1::{
    ColumnStat, ColumnStatsSegment, ColumnValue, DictEntry, SnapshotColumnStatsPartRef,
    SnapshotHead, SnapshotPartRef,
};
use ravel_query::{LogSegmentFetcher, SegmentFetcher};
use ravel_sql::{DeclaredColumn, DeclaredType, SpanSegmentFetcher, SqlConfig, SqlExecutor};
use ravel_types::accounting::QueryAccounting;
use ravel_types::{Signal, TenantHash};
use uuid::Uuid;

use crate::util;
use util::{CountingStore, stamped_like};

const TENANT: TenantHash = TenantHash([7u8; 16]);

/// The `TypedAttrColumnType::I64` discriminant (`ravel.sys.v1`), carried in
/// `ColumnStat.declared_type` as `i32`, matching the fold-side convention.
const DECLARED_TYPE_I64: u32 = 2;

/// A fabricated L0 [`SegmentRef`]; the metadata path never fetches the object,
/// so `data_object_key` need not name a real object. Only `content_hash` joins
/// it to the injected `ColumnStatsSegment` (v3 keying, ADR-1413). It carries
/// the stamp matching [`status_stat`], without which the entry answers nothing.
fn seg_ref(seq: u64, sample_count: u64) -> SegmentRef {
    let seg = SegmentRef {
        data_object_key: format!("logs/seg-{seq}.rlog"),
        object_size: 1,
        min_event_ts_ns: 0,
        max_event_ts_ns: 1_000,
        ingest_hour_bucket: 0,
        sample_count,
        series_count: 0,
        shard: 0,
        content_hash: [0u8; 32],
        writer_id: Uuid::from_u128(1),
        writer_epoch: 1,
        writer_seq: seq,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
        // The metadata path under test never opens the object, so nothing here
        // routes on this. Declared as the current logs version anyway, matching
        // the writer a real ref of this shape would have come from and the
        // other ravel-sql fixtures, rather than a literal that would be a lie
        // if anything later did read it.
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        declared_column_stats: Default::default(),
    };
    stamped_like(seg, &status_stat())
}

/// The `status` entry every eligible test here installs: {200:3, 404:2}, no
/// nulls, over a five-row segment.
fn status_stat() -> ColumnStat {
    i64_stat("status", &[(200, 3), (404, 2)], 0)
}

fn i64_value(v: i64) -> ColumnValue {
    ColumnValue {
        kind: Some(ravel_proto::catalog::v1::column_value::Kind::I64(v)),
    }
}

/// An exact I64 `ColumnStat` from a value->count dictionary, mirroring the
/// fold's output: `min`/`max` the dictionary extremes, `non_null_count` the
/// summed counts, dictionary present.
fn i64_stat(name: &str, dict: &[(i64, u64)], null_count: u64) -> ColumnStat {
    let non_null_count: u64 = dict.iter().map(|(_, c)| c).sum();
    let min = dict.iter().map(|(v, _)| *v).min();
    let max = dict.iter().map(|(v, _)| *v).max();
    // Accumulated in `i128` and narrowed once, mirroring what the fold does
    // with `checked_mul`/`checked_add`. Computing the expected value in raw
    // `i64` would panic on overflow in a debug build and wrap silently in a
    // release one, so a fixture with large values would either abort the test
    // or assert against a wrong expectation. A fixture that cannot fit `i64`
    // is a bug in the fixture, and this says which.
    let sum_wide: i128 = dict
        .iter()
        .map(|(v, c)| i128::from(*v) * i128::from(*c))
        .sum();
    let sum =
        i64::try_from(sum_wide).expect("fixture dictionary sums beyond i64; choose smaller values");
    ColumnStat {
        name: name.to_string(),
        declared_type: DECLARED_TYPE_I64,
        non_null_count,
        null_count,
        min: min.map(i64_value),
        max: max.map(i64_value),
        dictionary_present: true,
        dictionary: dict
            .iter()
            .map(|(v, c)| DictEntry {
                value: Some(i64_value(*v)),
                count: *c,
            })
            .collect(),
        sum: Some(sum),
    }
}

fn stats_segment(seg: &SegmentRef, columns: Vec<ColumnStat>) -> ColumnStatsSegment {
    ColumnStatsSegment {
        ingest_hour_bucket: seg.ingest_hour_bucket,
        shard: seg.shard,
        writer_id: seg.content_hash.to_vec(),
        writer_epoch: seg.writer_epoch,
        writer_seq: seg.writer_seq,
        columns,
    }
}

fn snapshot_of(segments: Vec<SegmentRef>) -> Snapshot {
    Snapshot {
        segments,
        segments_pruned: 0,
        pending_erasure: Vec::new(),
    }
}

fn head_key() -> String {
    format!(
        "t/{}/catalog/{}/HEAD",
        TENANT.to_hex(),
        Signal::Logs.key_prefix()
    )
}

/// Write a folded HEAD plus its `.cstat` object into `store`, binding the two
/// by a shared (arbitrary) part hash, so a subsequent `load_column_stats`
/// resolves `segments`' statistics. Returns nothing: the objects live in the
/// store at their canonical keys. No snapshot part or data object is written;
/// the metadata path fetches neither.
async fn install_head_and_stats(store: &dyn ObjectStoreBackend, segments: &[ColumnStatsSegment]) {
    let signal_num = ravel_commit::signal::to_proto(Signal::Logs) as u32;
    // An arbitrary part hash the HEAD and the stats object agree on. The part
    // object itself is never fetched by the load, so it is not written.
    let part_hash = *blake3::hash(b"part-0").as_bytes();

    let stats_bytes = encode_column_stats_v3(
        TENANT.0,
        signal_num,
        part_hash,
        segments,
        DEFAULT_MAX_COLUMN_STATS_BYTES,
    )
    .expect("encode column stats");
    let stats_hash = *blake3::hash(&stats_bytes).as_bytes();
    let stats_key = format!("t/{}/catalog/l/cstat/one.cstat", TENANT.to_hex());
    store
        .put(
            &stats_key,
            bytes::Bytes::from(stats_bytes.clone()),
            PutOptions::default(),
        )
        .await
        .expect("put stats");

    let head = SnapshotHead {
        format_version: HEAD_FORMAT_VERSION,
        tenant_hash: TENANT.0.to_vec(),
        signal: signal_num,
        shard_count: 1,
        watermark_hour: 10,
        parts: vec![SnapshotPartRef {
            key: format!("t/{}/catalog/l/snap/part.csnap", TENANT.to_hex()),
            blake3: part_hash.to_vec(),
            size: 1,
            entry_count: 0,
            watermark_hour: 10,
            min_hour: 0,
            column_stats: Some(SnapshotColumnStatsPartRef {
                key: stats_key,
                blake3: stats_hash.to_vec(),
                size: stats_bytes.len() as u64,
                segment_count: segments.len() as u32,
                part_blake3: vec![part_hash.to_vec()],
            }),
            key_index: Vec::new(),
        }],
        folder_id: Uuid::new_v4().into_bytes().to_vec(),
        created_unix_ns: 0,
        postings: None,
        shard_generation_count: 1,
    };
    let head_bytes = encode_head(&head).expect("encode head");
    store
        .put(
            &head_key(),
            bytes::Bytes::from(head_bytes),
            PutOptions::default(),
        )
        .await
        .expect("put head");
}

fn executor(store: &Arc<CountingStore>) -> SqlExecutor {
    let backend: Arc<dyn ObjectStoreBackend> = Arc::clone(store) as Arc<dyn ObjectStoreBackend>;
    let catalog =
        Arc::new(Catalog::new(Arc::clone(&backend), CatalogConfig::default()).expect("catalog"));
    SqlExecutor::new(
        catalog,
        SegmentFetcher::new(Arc::clone(&backend)),
        LogSegmentFetcher::new(Arc::clone(&backend)),
        SpanSegmentFetcher::new(Arc::clone(&backend)),
        SqlConfig::default(),
        1 << 30,
    )
}

fn status_col() -> Vec<DeclaredColumn> {
    vec![DeclaredColumn::new("status", DeclaredType::I64)]
}

/// The single scalar of a one-column, one-row result batch.
fn scalar(batches: &[RecordBatch], column: usize) -> Option<i64> {
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let arr = batch
            .column(column)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 column");
        return if arr.is_null(0) {
            None
        } else {
            Some(arr.value(0))
        };
    }
    None
}

async fn run(
    executor: &SqlExecutor,
    snapshot: Snapshot,
    sql: &str,
    declared: &[DeclaredColumn],
) -> (String, Vec<RecordBatch>) {
    let accounting = QueryAccounting::new();
    let pinned = executor
        .plan_pinned(TENANT, snapshot, sql, &accounting, declared)
        .await
        .expect("plan");
    let plan = pinned.create_physical_plan().await.expect("physical plan");
    let plan_str = displayable(plan.as_ref()).indent(true).to_string();
    let mut stream = pinned.execute().await.expect("execute");
    let mut batches = Vec::new();
    while let Some(next) = stream.next().await {
        batches.push(next.expect("batch"));
    }
    (plan_str, batches)
}

/// Deliverable 1, exact ZERO. A predicate-free `COUNT(*)` names no declared
/// column, so no ADR-0850 path can consume statistics. With `status` declared,
/// the pre-#888 executor still issued the HEAD GET `load_column_stats` begins
/// with; the hoist skips the load entirely, so the store sees zero GETs.
///
/// Pre-fix demonstration: making `logs_column_stats_eligible` return `true`
/// unconditionally (equivalently, deleting its
/// `plan_has_stats_answerable_aggregate` check) fires the load and this
/// becomes 1.
#[tokio::test]
async fn ineligible_no_declared_reference_issues_zero_gets() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let executor = executor(&store);

    let (_plan, batches) = run(
        &executor,
        snapshot_of(Vec::new()),
        "SELECT COUNT(*) FROM logs",
        &status_col(),
    )
    .await;

    assert_eq!(
        scalar(&batches, 0),
        Some(0),
        "empty snapshot counts zero rows"
    );
    assert_eq!(
        store.gets(),
        0,
        "no metadata-only path can consume stats, so the load must not fire"
    );
}

/// Deliverable 1, exact ZERO. A content predicate (`has_word`) makes every
/// ADR-0850 path decline at `stats_are_exact`, so the hoist skips the load even
/// though `SUM(status)` is a shape the metadata-only rewrite answers.
///
/// Pre-fix demonstration: deleting the `!pushdown.content.is_empty()` check
/// from the gate fires the load and this becomes 1.
#[tokio::test]
async fn ineligible_content_predicate_issues_zero_gets() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let executor = executor(&store);

    let (_plan, _batches) = run(
        &executor,
        snapshot_of(Vec::new()),
        "SELECT SUM(status) FROM logs WHERE has_word(body, 'timeout')",
        &status_col(),
    )
    .await;

    assert_eq!(
        store.gets(),
        0,
        "a content predicate declines every stats path, so the load must not fire"
    );
}

/// Issue #3244, exact ZERO. `MIN`/`MAX(status)` (q07) over a stamped segment
/// answers from the segment's stamp through DataFusion's
/// `AggregateStatistics`, which reads no `.cstat` entry, so the load does not
/// fire: no GET at all, no `LogsScanExec`, and the extrema of the stamp.
///
/// Pre-fix demonstration: a gate that still admits a lone `MIN`/`MAX` (the
/// `StatsAggregate::Min`/`Max` arm) fires the load and the GET count reads 2.
#[tokio::test]
async fn min_max_answers_from_the_stamp_with_zero_gets() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let seg = seg_ref(1, 5);
    install_head_and_stats(
        &*store,
        &[stats_segment(
            &seg,
            vec![i64_stat("status", &[(200, 3), (404, 2)], 0)],
        )],
    )
    .await;
    let executor = executor(&store);

    let (plan, batches) = run(
        &executor,
        snapshot_of(vec![seg]),
        "SELECT min(status), max(status) FROM logs",
        &status_col(),
    )
    .await;

    assert!(
        !plan.contains("LogsScanExec"),
        "min/max must answer from stats, not a scan; plan was:\n{plan}"
    );
    assert_eq!(scalar(&batches, 0), Some(200), "min(status)");
    assert_eq!(scalar(&batches, 1), Some(404), "max(status)");
    assert_eq!(
        store.gets(),
        0,
        "the stamp answers min/max, so no HEAD, `.cstat` or data GET"
    );
}

/// Issue #3244, exact ZERO. A `MIN` inside a scalar subquery answers from the
/// stamp like a top-level one, and loads nothing either.
///
/// Pre-fix demonstration: a gate that still admits a lone `MIN`/`MAX` (the
/// `StatsAggregate::Min`/`Max` arm) fires the load and the GET count reads 2.
#[tokio::test]
async fn min_inside_a_scalar_subquery_answers_from_the_stamp_with_zero_gets() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let seg = seg_ref(1, 5);
    install_head_and_stats(
        &*store,
        &[stats_segment(
            &seg,
            vec![i64_stat("status", &[(200, 3), (404, 2)], 0)],
        )],
    )
    .await;
    let executor = executor(&store);

    let (plan, batches) = run(
        &executor,
        snapshot_of(vec![seg]),
        "SELECT (SELECT min(status) FROM logs)",
        &status_col(),
    )
    .await;

    assert!(
        !plan.contains("LogsScanExec"),
        "a min() inside a scalar subquery must still answer from stats, not a \
         scan; plan was:\n{plan}"
    );
    assert_eq!(scalar(&batches, 0), Some(200), "min(status) via subquery");
    assert_eq!(
        store.gets(),
        0,
        "the stamp answers the min, so no HEAD, `.cstat` or data GET"
    );
}

/// Deliverable 1 regression, exact TWO. q02 (`COUNT(*) WHERE status <> 200`)
/// over a segment whose stamp matches its entry
/// keeps its `status <> 200` residual filter and pushes only a prune-only
/// `NumNotEq` arm, which the hoist counts as no prune predicate, so it must
/// not skip the load. The load fires (two GETs) and the answer comes from the
/// dictionary with no scan.
///
/// Answer: `non_null_count(5) - count(200)(3) = 2`.
///
/// Pre-fix demonstration: making `logs_column_stats_eligible` return `false`
/// leaves `MetadataOnlyExec` out of the plan and re-adds `LogsScanExec`.
#[tokio::test]
async fn eligible_not_equal_count_answers_from_metadata_with_two_gets() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let seg = seg_ref(1, 5);
    install_head_and_stats(
        &*store,
        &[stats_segment(
            &seg,
            vec![i64_stat("status", &[(200, 3), (404, 2)], 0)],
        )],
    )
    .await;
    let executor = executor(&store);

    let (plan, batches) = run(
        &executor,
        snapshot_of(vec![seg]),
        "SELECT COUNT(*) FROM logs WHERE status <> 200",
        &status_col(),
    )
    .await;

    assert!(
        plan.contains("MetadataOnlyExec"),
        "q02 must answer from the dictionary; plan was:\n{plan}"
    );
    assert!(
        !plan.contains("LogsScanExec"),
        "q02 must not scan; plan was:\n{plan}"
    );
    assert_eq!(
        scalar(&batches, 0),
        Some(2),
        "5 non-null minus 3 with value 200"
    );
    assert_eq!(
        store.gets(),
        2,
        "exactly the HEAD GET and the one column-stats GET; no scan"
    );
}

/// BLOCKING regression (issue #1483 review, ADR-1413 T2): `plan_pinned`'s
/// `(range, now_ns)` pair for `Catalog::load_column_stats` used to come from
/// segments' EVENT timestamps (`min_event_ts_ns`/`max_event_ts_ns`), while
/// every downstream consumer -- `Catalog::window_hour_bounds` and
/// `crate::snapshot_resolve::parts_intersecting` -- compares the pair against
/// INGEST-hour buckets (`SnapshotPartRef::min_hour`/`watermark_hour`). A
/// tenant backfilling data whose events happened long before they were
/// ingested (the ClickBench `hits` shape this ADR exists to rescue: event
/// timestamps in 2013-2014, ingested in 2026) then resolves a window that
/// intersects no part at all: `load_column_stats` returns `Ok(None)` and the
/// query gets zero column statistics, silently, on every pinned/Flight SQL
/// query over that tenant.
///
/// This fixture's one segment has event time at hour 10 and
/// `ingest_hour_bucket` 500_000 -- a gap far past `max_ingest_lag_ns`
/// (the catalog default, 2h) in either direction. Its HEAD part covers
/// ingest hours `[499_990, 500_010]`, bracketing the segment's ingest hour
/// but nowhere near its event hour.
///
/// Pre-fix demonstration: reverting `snapshot_covering_window`
/// (crates/ravel-sql/src/executor.rs) to derive its pair from
/// `seg.min_event_ts_ns`/`seg.max_event_ts_ns` instead of
/// `seg.ingest_hour_bucket` makes this fail: the window built from event
/// hour 10 never intersects the part's ingest-hour range `[499_990,
/// 500_010]`, so `load_column_stats` returns `Ok(None)`, the plan falls back
/// to a `LogsScanExec`, and the test panics trying to fetch the (never
/// written) segment data object the scan then needs -- proof no stats were
/// loaded and no exact answer was possible.
#[tokio::test]
async fn plan_pinned_loads_stats_when_ingest_hour_diverges_from_event_time() {
    const NS_PER_HOUR: i64 = 3_600_000_000_000;
    let store = CountingStore::new(Arc::new(MemoryStore::new()));

    let seg = SegmentRef {
        min_event_ts_ns: 10 * NS_PER_HOUR,
        max_event_ts_ns: 10 * NS_PER_HOUR + 1_000,
        ingest_hour_bucket: 500_000,
        ..seg_ref(1, 5)
    };
    let seg_stats = stats_segment(&seg, vec![i64_stat("status", &[(200, 3), (404, 2)], 0)]);

    let signal_num = ravel_commit::signal::to_proto(Signal::Logs) as u32;
    let part_hash = *blake3::hash(b"part-backfilled").as_bytes();
    let stats_bytes = encode_column_stats_v3(
        TENANT.0,
        signal_num,
        part_hash,
        &[seg_stats],
        DEFAULT_MAX_COLUMN_STATS_BYTES,
    )
    .expect("encode column stats");
    let stats_hash = *blake3::hash(&stats_bytes).as_bytes();
    let stats_key = format!("t/{}/catalog/l/cstat/backfilled.cstat", TENANT.to_hex());
    store
        .put(
            &stats_key,
            bytes::Bytes::from(stats_bytes.clone()),
            PutOptions::default(),
        )
        .await
        .expect("put stats");

    // Same shape as `install_head_and_stats`, but with the one part's ingest
    // hour coverage placed at the segment's ingest hour bucket (500_000)
    // rather than at hour 0: every other fixture in this file has its
    // segment's event hour and ingest hour both at 0, which is exactly why
    // it cannot tell the two clocks apart.
    let head = SnapshotHead {
        format_version: HEAD_FORMAT_VERSION,
        tenant_hash: TENANT.0.to_vec(),
        signal: signal_num,
        shard_count: 1,
        watermark_hour: 500_010,
        parts: vec![SnapshotPartRef {
            key: format!("t/{}/catalog/l/snap/backfilled.csnap", TENANT.to_hex()),
            blake3: part_hash.to_vec(),
            size: 1,
            entry_count: 0,
            watermark_hour: 500_010,
            min_hour: 499_990,
            column_stats: Some(SnapshotColumnStatsPartRef {
                key: stats_key,
                blake3: stats_hash.to_vec(),
                size: stats_bytes.len() as u64,
                segment_count: 1,
                part_blake3: vec![part_hash.to_vec()],
            }),
            key_index: Vec::new(),
        }],
        folder_id: Uuid::new_v4().into_bytes().to_vec(),
        created_unix_ns: 0,
        postings: None,
        shard_generation_count: 1,
    };
    let head_bytes = encode_head(&head).expect("encode head");
    store
        .put(
            &head_key(),
            bytes::Bytes::from(head_bytes),
            PutOptions::default(),
        )
        .await
        .expect("put head");

    let executor = executor(&store);

    // q02 rather than MIN/MAX: the stamp alone answers MIN/MAX, while this
    // statement needs the loaded `.cstat` dictionary.
    let (plan, batches) = run(
        &executor,
        snapshot_of(vec![seg]),
        "SELECT COUNT(*) FROM logs WHERE status <> 200",
        &status_col(),
    )
    .await;

    assert!(
        !plan.contains("LogsScanExec"),
        "q02 must answer from stats, not a scan; plan was:\n{plan}"
    );
    assert_eq!(
        scalar(&batches, 0),
        Some(2),
        "5 non-null minus 3 with value 200"
    );
    assert_eq!(
        store.gets(),
        2,
        "exactly the HEAD GET and the one column-stats GET; no scan"
    );
}

/// `status` and `bytes`, both declared I64: the second column gives a
/// `NumRange` arm a column of its own to sit on beside the `NumNotEq` arm.
fn status_and_bytes_cols() -> Vec<DeclaredColumn> {
    vec![
        DeclaredColumn::new("status", DeclaredType::I64),
        DeclaredColumn::new("bytes", DeclaredType::I64),
    ]
}

/// Exact ZERO. Only a prune list holding nothing but `NumNotEq` arms counts
/// as empty for the load decision. `status <> 404 AND bytes > 0` pushes a
/// `NumNotEq` arm on `status` and a `NumRange` arm on `bytes`; the range arm
/// keeps every metadata-only path declining (a range prune changes which rows
/// the scan emits, and q02 admits only a bare `<>` filter), so the load stays
/// skipped exactly as it was before the `NumNotEq` arm existed.
///
/// Pre-fix demonstration: making `prune_is_rewrite_neutral` answer `true` for
/// every prune list (ignoring every arm, not only `NumNotEq`) fires the load
/// and this becomes 1.
#[tokio::test]
async fn not_equal_beside_a_range_arm_still_skips_the_load() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let executor = executor(&store);

    let (plan, batches) = run(
        &executor,
        snapshot_of(Vec::new()),
        "SELECT COUNT(*) FROM logs WHERE status <> 404 AND bytes > 0",
        &status_and_bytes_cols(),
    )
    .await;

    assert!(
        !plan.contains("MetadataOnlyExec"),
        "a NumRange arm beside the NumNotEq arm refuses the rewrite; plan was:\n{plan}"
    );
    assert_eq!(scalar(&batches, 0), Some(0), "empty snapshot counts zero");
    assert_eq!(
        store.gets(),
        0,
        "a NumRange arm declines every stats path, so the load must not fire"
    );
}

/// One per-record-`status` record at `ts`; `None` writes no `status` at all.
fn status_record(ts: i64, status: Option<i64>) -> ravel_logseg::LogRecord {
    use ravel_logseg::{AttrValue, stream_attrs_bytes};
    let resource = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    ravel_logseg::LogRecord {
        stream_id: ravel_types::logstream::log_stream_id(&resource, "scope", "1.0", &[]),
        stream_attrs: stream_attrs_bytes(&resource, "scope", "1.0", &[]),
        ts_ns: ts,
        observed_ts_ns: ts,
        severity_num: 9,
        severity_text: "INFO".to_string(),
        body: format!("row at {ts}"),
        trace_id: None,
        span_id: None,
        flags: 0,
        attrs: status
            .map(|v| vec![("status".to_string(), AttrValue::I64(v))])
            .unwrap_or_default(),
    }
}

/// Write `statuses` as one real RLOG object, one record per block, ts 1..=n.
async fn write_one_record_blocks(
    store: &Arc<CountingStore>,
    key: &str,
    statuses: &[Option<i64>],
) -> SegmentRef {
    let mut w = ravel_logseg::RlogWriter::new(
        ravel_logseg::RlogConfig {
            block_target_records: 1,
            ..ravel_logseg::RlogConfig::default()
        },
        ravel_logseg::writer::ObjectIdentity {
            tenant_hash: TENANT.0,
            shard: 0,
            writer_id: [2u8; 16],
            writer_epoch: 1,
            writer_seq: 1,
        },
    );
    for (i, status) in statuses.iter().enumerate() {
        w.push(status_record(i as i64 + 1, *status)).expect("push");
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
        min_event_ts_ns: 1,
        max_event_ts_ns: statuses.len() as i64,
        sample_count: statuses.len() as u64,
        ..seg_ref(1, statuses.len() as u64)
    }
}

/// The value of `name=N` on `line`, when the line carries one.
fn metric(line: &str, name: &str) -> Option<usize> {
    let at = line.find(&format!("{name}="))? + name.len() + 1;
    let digits: String = line[at..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    digits.parse().ok()
}

/// `(blocks_total, blocks_scanned)` of the executed `LogsScanExec`, plus the
/// statement's `COUNT(*)`, for `sql` over `snapshot`.
async fn scanned_count(
    executor: &SqlExecutor,
    snapshot: Snapshot,
    sql: &str,
) -> (i64, usize, usize) {
    let (plan, batches) = run(executor, snapshot.clone(), sql, &status_col()).await;
    assert!(
        plan.contains("LogsScanExec") && !plan.contains("MetadataOnlyExec"),
        "with no .cstat object the statement must scan; plan was:\n{plan}"
    );
    let count = scalar(&batches, 0).expect("COUNT(*) is never NULL");
    let analyzed = executor
        .plan_pinned(
            TENANT,
            snapshot,
            sql,
            &QueryAccounting::new(),
            &status_col(),
        )
        .await
        .expect("plan")
        .execute_analyzed()
        .await
        .expect("execute analyzed");
    let shown = &analyzed.plan_with_metrics;
    let scan = shown
        .lines()
        .find(|l| l.trim_start().starts_with("LogsScanExec"))
        .unwrap_or_else(|| panic!("no LogsScanExec:\n{shown}"));
    let total = metric(scan, "blocks_total").unwrap_or_else(|| panic!("{shown}"));
    let scanned = metric(scan, "blocks_scanned").unwrap_or_else(|| panic!("{shown}"));
    (count, total, scanned)
}

/// A tenant with no `.cstat` object: the load now fires for q02 (the HEAD GET
/// finds nothing), the rewrite has no statistics to answer from, and the
/// statement scans. The `NumNotEq` arm then does its own job: of eight
/// one-record blocks, the four whose `status` is `0` are pruned before decode,
/// the block with no `status` is kept, and the count is the three nonzero
/// rows. The same statement OR-ed with a body equality no row satisfies
/// pushes no arm, decodes all eight blocks, and counts the same three.
///
/// Pre-fix demonstration: dropping the `NumNotEq` arm in
/// `declared_comparison_predicate` makes the pruned run scan all eight blocks.
#[tokio::test]
async fn not_equal_without_cstat_scans_and_prunes_the_all_equal_blocks() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let statuses = [
        Some(0),
        Some(0),
        Some(200),
        Some(0),
        Some(404),
        None,
        Some(0),
        Some(500),
    ];
    let seg = write_one_record_blocks(&store, "logs/ne-no-cstat.rlog", &statuses).await;
    let executor = executor(&store);

    let (count, total, scanned) = scanned_count(
        &executor,
        snapshot_of(vec![seg.clone()]),
        "SELECT COUNT(*) FROM logs WHERE status <> 0",
    )
    .await;
    let (plain_count, plain_total, plain_scanned) = scanned_count(
        &executor,
        snapshot_of(vec![seg]),
        "SELECT COUNT(*) FROM logs WHERE status <> 0 OR body = 'zzz'",
    )
    .await;

    assert_eq!(count, 3, "200, 404 and 500");
    assert_eq!(plain_count, count, "pruning never changes the count");
    assert_eq!(total, 8);
    assert_eq!(
        scanned, 4,
        "the four all-zero blocks are pruned; the no-stat block is kept"
    );
    assert_eq!((plain_total, plain_scanned), (8, 8), "no arm, no pruning");
}

/// The two declared columns the two-key `GROUP BY` test needs; `code` has no
/// statistics installed anywhere, and needs none, since the load never fires.
fn status_and_code_cols() -> Vec<DeclaredColumn> {
    vec![
        DeclaredColumn::new("status", DeclaredType::I64),
        DeclaredColumn::new("code", DeclaredType::I64),
    ]
}

/// Run `sql` against an empty snapshot and return the GET count. An empty
/// snapshot still issues the HEAD GET when the gate admits the statement (see
/// [`ineligible_no_declared_reference_issues_zero_gets`]), so a refused shape
/// reads 0 and an admitted one reads 1.
async fn gets_for_empty_snapshot(sql: &str, declared: &[DeclaredColumn]) -> u64 {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let executor = executor(&store);
    let (_plan, _batches) = run(&executor, snapshot_of(Vec::new()), sql, declared).await;
    store.gets()
}

/// Issue #3244, exact ZERO. ClickBench q03's shape, `SUM` beside `COUNT(*)`
/// and `AVG`, names the declared column, so the old "aggregate plus declared
/// reference" gate loaded the statistics. Neither consumer can answer it:
/// `MetadataOnlyAggregate` takes one aggregate, and DataFusion's
/// `AggregateStatistics` replaces all or none and has no `SUM`/`AVG`.
#[tokio::test]
async fn q03_shape_sum_count_avg_issues_zero_gets() {
    assert_eq!(
        gets_for_empty_snapshot(
            "SELECT SUM(status), COUNT(*), AVG(status) FROM logs",
            &status_col()
        )
        .await,
        0,
        "no statistics consumer answers SUM beside AVG, so the load must not fire"
    );
}

/// Issue #3244, exact ZERO. `COUNT(DISTINCT status)` needs an exact distinct
/// count, which no statistics path supplies.
#[tokio::test]
async fn count_distinct_declared_issues_zero_gets() {
    assert_eq!(
        gets_for_empty_snapshot("SELECT COUNT(DISTINCT status) FROM logs", &status_col()).await,
        0,
        "no statistics consumer answers COUNT(DISTINCT), so the load must not fire"
    );
}

/// Issue #3244, exact ZERO. A `<>` filter goes with an ungrouped `COUNT(*)`
/// only; `MetadataOnlyAggregate`'s group-by path takes no filter at all.
#[tokio::test]
async fn not_equal_filter_with_group_by_issues_zero_gets() {
    assert_eq!(
        gets_for_empty_snapshot(
            "SELECT status, COUNT(*) FROM logs WHERE status <> 200 GROUP BY status",
            &status_col()
        )
        .await,
        0,
        "the group-by path takes no filter, so the load must not fire"
    );
}

/// Issue #3244, exact ZERO. Three `SUM`s are three aggregates;
/// `MetadataOnlyAggregate` answers one.
#[tokio::test]
async fn three_sums_issue_zero_gets() {
    assert_eq!(
        gets_for_empty_snapshot(
            "SELECT SUM(status), SUM(status + 1), SUM(status + 2) FROM logs",
            &status_col()
        )
        .await,
        0,
        "no statistics consumer answers several SUMs, so the load must not fire"
    );
}

/// Issue #3244, exact ZERO. The group-by path answers a single declared key.
#[tokio::test]
async fn group_by_two_keys_issues_zero_gets() {
    assert_eq!(
        gets_for_empty_snapshot(
            "SELECT status, code, COUNT(*) FROM logs GROUP BY status, code",
            &status_and_code_cols()
        )
        .await,
        0,
        "the group-by path takes one key, so the load must not fire"
    );
}

/// One segment with `status` = {200:3, 404:2} installed with its `.cstat`
/// entry; returns the store and the snapshot holding the segment.
async fn installed_status_segment() -> (Arc<CountingStore>, Snapshot) {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let seg = seg_ref(1, 5);
    install_head_and_stats(&*store, &[stats_segment(&seg, vec![status_stat()])]).await;
    (store, snapshot_of(vec![seg]))
}

/// Run `sql` over [`installed_status_segment`] and assert the metadata answer:
/// exactly the HEAD GET and the one `.cstat` GET once planned,
/// `MetadataOnlyExec` in the plan and no `LogsScanExec`. Both are asserted
/// before execution, since the fixture's data object does not exist and a
/// scan fails on it. Returns the result batches.
async fn assert_metadata_answer_with_two_gets(sql: &str) -> Vec<RecordBatch> {
    let (store, snapshot) = installed_status_segment().await;
    let executor = executor(&store);
    let accounting = QueryAccounting::new();
    let pinned = executor
        .plan_pinned(TENANT, snapshot, sql, &accounting, &status_col())
        .await
        .expect("plan");
    let plan = pinned.create_physical_plan().await.expect("physical plan");
    let plan = displayable(plan.as_ref()).indent(true).to_string();
    assert_eq!(
        store.gets(),
        2,
        "{sql}: exactly the HEAD GET and the one column-stats GET"
    );
    assert!(
        plan.contains("MetadataOnlyExec"),
        "{sql} must answer from the column statistics; plan was:\n{plan}"
    );
    assert!(
        !plan.contains("LogsScanExec"),
        "{sql} must not scan; plan was:\n{plan}"
    );
    let mut stream = pinned.execute().await.expect("execute");
    let mut batches = Vec::new();
    while let Some(next) = stream.next().await {
        batches.push(next.expect("batch"));
    }
    assert_eq!(store.gets(), 2, "{sql}: no GET while executing");
    batches
}

/// Issue #3244, exact TWO. q08's shape: `COUNT(*)` grouped by one declared
/// column with no filter answers from the dictionary.
#[tokio::test]
async fn group_by_declared_count_answers_from_metadata_with_two_gets() {
    let batches =
        assert_metadata_answer_with_two_gets("SELECT status, COUNT(*) FROM logs GROUP BY status")
            .await;
    assert_eq!(
        status_groups(&batches),
        vec![(200, 3), (404, 2)],
        "the dictionary's groups"
    );
}

/// The `(status, COUNT(*))` rows of a grouped result, sorted.
fn status_groups(batches: &[RecordBatch]) -> Vec<(i64, i64)> {
    let mut groups = Vec::new();
    for batch in batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 key");
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 count");
        for row in 0..batch.num_rows() {
            groups.push((keys.value(row), counts.value(row)));
        }
    }
    groups.sort_unstable();
    groups
}

/// Issue #3244, exact TWO. `SUM(status)` answers from the stored sum.
#[tokio::test]
async fn sum_declared_answers_from_metadata_with_two_gets() {
    let batches = assert_metadata_answer_with_two_gets("SELECT SUM(status) FROM logs").await;
    assert_eq!(scalar(&batches, 0), Some(1408), "200*3 + 404*2");
}

/// Issue #3244, exact TWO. `SUM(status + 1)` answers from the stored sum plus
/// the addend times the non-null count.
#[tokio::test]
async fn sum_declared_plus_literal_answers_from_metadata_with_two_gets() {
    let batches = assert_metadata_answer_with_two_gets("SELECT SUM(status + 1) FROM logs").await;
    assert_eq!(scalar(&batches, 0), Some(1413), "1408 + 1*5");
}

/// Issue #3244, exact TWO. `AVG(status)` answers from the stored sum over the
/// non-null count.
#[tokio::test]
async fn avg_declared_answers_from_metadata_with_two_gets() {
    let batches = assert_metadata_answer_with_two_gets("SELECT AVG(status) FROM logs").await;
    let avg = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("float64 avg")
                .value(0)
        })
        .expect("one avg row");
    assert_eq!(avg.to_bits(), (1408.0_f64 / 5.0).to_bits(), "1408 / 5");
}

/// A `ts` bound written with `TIMESTAMP` literals, containing the fixture
/// segment's whole `0..=1000` ns range.
const CONTAINING_TS_BOUND: &str = "ts BETWEEN TIMESTAMP '1970-01-01 00:00:00.000000000' \
     AND TIMESTAMP '1970-01-01 00:00:00.000001000'";

/// Issue #3244, exact TWO. q02 beside a `ts` bound written with `TIMESTAMP`
/// literals. The gate reads the analyzed plan, where each literal is still a
/// cast of a string, so it cannot show the conjunct is a pure `ts` bound; an
/// unclassified conjunct inside an admitted shape keeps the load, and the
/// rewrite answers on the optimized plan.
///
/// Pre-fix demonstration: on the first round's gate, which refused any
/// residual conjunct it could not classify, the GET count reads 0.
#[tokio::test]
async fn not_equal_count_beside_a_timestamp_bound_loads_and_answers() {
    let sql = format!("SELECT COUNT(*) FROM logs WHERE status <> 200 AND {CONTAINING_TS_BOUND}");
    let batches = assert_metadata_answer_with_two_gets(&sql).await;
    assert_eq!(scalar(&batches, 0), Some(2), "5 non-null minus 3 with 200");
}

/// Issue #3244, exact TWO. q08 beside the same `TIMESTAMP` bound loads and
/// answers the dictionary's groups.
///
/// Pre-fix demonstration: on the first round's gate the GET count reads 0.
#[tokio::test]
async fn group_by_declared_beside_a_timestamp_bound_loads_and_answers() {
    let sql =
        format!("SELECT status, COUNT(*) FROM logs WHERE {CONTAINING_TS_BOUND} GROUP BY status");
    let batches = assert_metadata_answer_with_two_gets(&sql).await;
    assert_eq!(
        status_groups(&batches),
        vec![(200, 3), (404, 2)],
        "the dictionary's groups"
    );
}

/// Issue #3244, exact TWO. `NOT (status = 200)` is not a `<>` conjunct in
/// the analyzed plan; the optimizer rewrites it to `status <> 200`, and the
/// rewrite answers that. The gate loads because the conjunct is unknown, not
/// because it reads through the negation.
///
/// Pre-fix demonstration: a gate that refuses an unclassified conjunct (with
/// or without stripping casts from literals) reads 0 GETs here.
#[tokio::test]
async fn negated_equality_count_loads_and_answers() {
    let batches =
        assert_metadata_answer_with_two_gets("SELECT COUNT(*) FROM logs WHERE NOT (status = 200)")
            .await;
    assert_eq!(scalar(&batches, 0), Some(2), "5 non-null minus 3 with 200");
}

/// Issue #3244, exact TWO. `status <> 100 + 100` holds an unfolded constant
/// expression in the analyzed plan; the optimizer folds it to `<> 200`.
///
/// Pre-fix demonstration: on the first round's gate the GET count reads 0.
#[tokio::test]
async fn not_equal_to_a_constant_expression_loads_and_answers() {
    let batches =
        assert_metadata_answer_with_two_gets("SELECT COUNT(*) FROM logs WHERE status <> 100 + 100")
            .await;
    assert_eq!(scalar(&batches, 0), Some(2), "5 non-null minus 3 with 200");
}

/// Issue #3244, exact ZERO. A `COUNT(*)` whose only filter is a `ts` bound
/// names no declared column, so the rewrite has nothing to read from the
/// statistics and the load does not fire, `TIMESTAMP` literals or not.
#[tokio::test]
async fn count_star_beside_only_a_timestamp_bound_issues_zero_gets() {
    let sql = format!("SELECT COUNT(*) FROM logs WHERE {CONTAINING_TS_BOUND}");
    assert_eq!(
        gets_for_empty_snapshot(&sql, &status_col()).await,
        0,
        "no declared column is filtered, so the load must not fire"
    );
}

/// `status` beside a `Str` typed attribute column `url`. `url` has no
/// `.cstat` entry or stamp: no statistics consumer answers a `Str` column, so
/// a statement filtered on it alone must never reach the load.
fn status_and_url_cols() -> Vec<DeclaredColumn> {
    vec![
        DeclaredColumn::new("status", DeclaredType::I64),
        DeclaredColumn::new("url", DeclaredType::Str),
    ]
}

/// Plan `sql` over [`installed_status_segment`] with [`status_and_url_cols`]
/// declared and return the GET count once planned. Not executed: the fixture's
/// data object does not exist, and a statement the gate refuses scans.
async fn gets_to_plan_with_url(sql: &str) -> u64 {
    let (store, snapshot) = installed_status_segment().await;
    let executor = executor(&store);
    let accounting = QueryAccounting::new();
    let pinned = executor
        .plan_pinned(TENANT, snapshot, sql, &accounting, &status_and_url_cols())
        .await
        .expect("plan");
    let plan = pinned.create_physical_plan().await.expect("physical plan");
    let plan = displayable(plan.as_ref()).indent(true).to_string();
    assert!(
        !plan.contains("MetadataOnlyExec"),
        "{sql} has no metadata answer; plan was:\n{plan}"
    );
    store.gets()
}

/// Issue #3244, exact ZERO. ClickBench Q21's shape: a `COUNT(*)` filtered by
/// `LIKE` on a `Str` column. The conjunct is unknown to the gate, but it names
/// only a `Str` column, which can never become a `<>` the rewrite answers, so
/// the load must not fire.
///
/// Pre-fix demonstration: on a gate that counts any declared column in an
/// unknown conjunct, the GET count reads 2.
#[tokio::test]
async fn like_on_a_string_column_count_issues_zero_gets() {
    assert_eq!(
        gets_to_plan_with_url("SELECT COUNT(*) FROM logs WHERE url LIKE '%google%'").await,
        0,
        "no statistics consumer answers a LIKE on a Str column"
    );
}

/// Issue #3244, exact ZERO. A `<>` on a `Str` column is refused by the gate
/// and by `declared_not_equal_count`, so the load must not fire. This pins
/// the gate's `not_str` refusal of a classified `<>`; the dispatched gate
/// already read 0 here.
#[tokio::test]
async fn not_equal_on_a_string_column_count_issues_zero_gets() {
    assert_eq!(
        gets_to_plan_with_url("SELECT COUNT(*) FROM logs WHERE url <> 'x'").await,
        0,
        "no statistics consumer answers a <> on a Str column"
    );
}

/// Issue #3244, exact TWO. A `Str` column declared beside `status` does not
/// change q02 beside a `TIMESTAMP` bound: the unknown `ts` conjunct sits
/// beside a non-`Str` `<>`, so the load still fires.
#[tokio::test]
async fn not_equal_count_beside_a_timestamp_bound_loads_with_a_string_column_declared() {
    let sql = format!("SELECT COUNT(*) FROM logs WHERE status <> 200 AND {CONTAINING_TS_BOUND}");
    let (store, snapshot) = installed_status_segment().await;
    let executor = executor(&store);
    let accounting = QueryAccounting::new();
    let pinned = executor
        .plan_pinned(TENANT, snapshot, &sql, &accounting, &status_and_url_cols())
        .await
        .expect("plan");
    let _plan = pinned.create_physical_plan().await.expect("physical plan");
    assert_eq!(store.gets(), 2, "the HEAD GET and the column-stats GET");
}

/// Issue #3244, exact TWO. `SUM(status)` beside the `TIMESTAMP` bound loads
/// and answers from the stored sum.
///
/// Pre-fix demonstration: on a gate whose non-q02 arms refuse an unknown
/// conjunct, the GET count reads 0.
#[tokio::test]
async fn sum_declared_beside_a_timestamp_bound_loads_and_answers() {
    let sql = format!("SELECT SUM(status) FROM logs WHERE {CONTAINING_TS_BOUND}");
    let batches = assert_metadata_answer_with_two_gets(&sql).await;
    assert_eq!(scalar(&batches, 0), Some(1408), "200*3 + 404*2");
}

/// Issue #3244, exact TWO. `AVG(status)` beside the `TIMESTAMP` bound loads
/// and answers from the stored sum over the non-null count.
///
/// Pre-fix demonstration: on a gate whose non-q02 arms refuse an unknown
/// conjunct, the GET count reads 0.
#[tokio::test]
async fn avg_declared_beside_a_timestamp_bound_loads_and_answers() {
    let sql = format!("SELECT AVG(status) FROM logs WHERE {CONTAINING_TS_BOUND}");
    let batches = assert_metadata_answer_with_two_gets(&sql).await;
    let avg = batches
        .iter()
        .find(|b| b.num_rows() > 0)
        .map(|b| {
            b.column(0)
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("float64 avg")
                .value(0)
        })
        .expect("one avg row");
    assert_eq!(avg.to_bits(), (1408.0_f64 / 5.0).to_bits(), "1408 / 5");
}
