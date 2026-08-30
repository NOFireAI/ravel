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
//! `encode_head`/`encode_column_stats` codecs (no fold runs), exactly as
//! `ravel-catalog`'s own `load_column_stats` test builds them. The metadata
//! path never fetches a snapshot part or a data object, so neither needs to
//! exist: the only reads a metadata-answered eligible query makes are the two
//! stats GETs.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::displayable;
use futures::StreamExt;
use ravel_catalog::{
    Catalog, CatalogConfig, HEAD_FORMAT_VERSION, SegmentLevel, SegmentRef, Snapshot,
    encode_column_stats, encode_head,
};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_proto::catalog::v1::{
    ColumnStat, ColumnStatsSegment, ColumnValue, DictEntry, SnapshotColumnStatsRef, SnapshotHead,
    SnapshotPartRef,
};
use ravel_query::{LogSegmentFetcher, SegmentFetcher};
use ravel_sql::{DeclaredColumn, DeclaredType, SpanSegmentFetcher, SqlConfig, SqlExecutor};
use ravel_types::accounting::QueryAccounting;
use ravel_types::{Signal, TenantHash};
use uuid::Uuid;

mod util;
use util::CountingStore;

const TENANT: TenantHash = TenantHash([7u8; 16]);

/// The `TypedAttrColumnType::I64` discriminant (`ravel.sys.v1`), carried in
/// `ColumnStat.declared_type` as `i32`, matching the fold-side convention.
const DECLARED_TYPE_I64: u32 = 2;

/// A fabricated L0 [`SegmentRef`]; the metadata path never fetches the object,
/// so `data_object_key` need not name a real object. Only the identity fields
/// join it to the injected `ColumnStatsSegment`.
fn seg_ref(seq: u64, sample_count: u64) -> SegmentRef {
    SegmentRef {
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
    }
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
        writer_id: seg.writer_id.as_bytes().to_vec(),
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

    let stats_bytes = encode_column_stats(TENANT.0, signal_num, vec![part_hash.to_vec()], segments)
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
        }],
        folder_id: Uuid::new_v4().into_bytes().to_vec(),
        created_unix_ns: 0,
        postings: None,
        shard_generation_count: 1,
        column_stats: Some(SnapshotColumnStatsRef {
            key: stats_key,
            blake3: stats_hash.to_vec(),
            size: stats_bytes.len() as u64,
            segment_count: segments.len() as u32,
            part_blake3: vec![part_hash.to_vec()],
        }),
        column_stats_part: None,
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
/// unconditionally (equivalently, deleting the `|| !plan_references_declared(
/// &plan, declared)` clause) fires the load and this becomes 1.
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
/// though the aggregate names the declared `status` column.
///
/// Pre-fix demonstration: deleting the `if !pushdown.content.is_empty() ||
/// !pushdown.prune.is_empty()` guard fires the load and this becomes 1.
#[tokio::test]
async fn ineligible_content_predicate_issues_zero_gets() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let executor = executor(&store);

    let (_plan, _batches) = run(
        &executor,
        snapshot_of(Vec::new()),
        "SELECT min(status) FROM logs WHERE has_word(body, 'timeout')",
        &status_col(),
    )
    .await;

    assert_eq!(
        store.gets(),
        0,
        "a content predicate declines every stats path, so the load must not fire"
    );
}

/// Deliverable 1 regression, exact TWO. `MIN`/`MAX(status)` (q07) over a
/// segment with exact statistics must still resolve the stats (the HEAD GET and
/// the one `.cstat` GET, exactly two) and still answer from catalog metadata:
/// the plan carries no `LogsScanExec` and no data object is read.
///
/// Pre-fix demonstration: making `logs_column_stats_eligible` return `false`
/// skips the load, the provider gets no statistics, `partition_statistics`
/// cannot report exact min/max, and `LogsScanExec` stays in the plan (and the
/// GET count is not 2).
#[tokio::test]
async fn eligible_min_max_answers_from_metadata_with_two_gets() {
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
        2,
        "exactly the HEAD GET and the one column-stats GET; no scan"
    );
}

/// The declared reference can sit inside a subquery expression rather than the
/// outer plan. A subquery's plan hangs off the expression, not off
/// `plan.inputs()`, so an eligibility walk that only recurses inputs misses it
/// and skips the stats load -- which fails open (the nested aggregate falls
/// back to scanning) but throws away exactly the case ADR-0850 exists for.
#[tokio::test]
async fn eligible_min_max_inside_a_scalar_subquery_still_answers_from_metadata() {
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
        2,
        "exactly the HEAD GET and the one column-stats GET; no scan"
    );
}

/// Deliverable 1 regression, exact TWO. q02 (`COUNT(*) WHERE status <> 200`)
/// keeps its `status <> 200` residual filter -- `<>` is NEVER pushed to the
/// prune channel (`int_range_bounds` declines `NotEq`), so the hoist must not
/// mistake it for a prune predicate and skip the load. The load fires (two
/// GETs) and the answer comes from the dictionary with no scan.
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
