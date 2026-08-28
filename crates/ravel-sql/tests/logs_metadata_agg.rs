//! Integration tests for the ADR-0850 metadata-only aggregate rewrite (issue
//! #850): the two ClickBench shapes `MetadataOnlyAggregate` answers straight
//! from exact per-object column statistics with zero data-block GETs.
//!
//! - q02: `COUNT(*) FROM logs WHERE <declared column> <> <literal>`
//! - q08: `SELECT <declared column>, COUNT(*) FROM logs GROUP BY <declared
//!   column>`
//!
//! Every positive test pins BOTH halves as exact facts: the physical plan
//! carries no `LogsScanExec` (the scan was elided, not merely pruned) and the
//! executor issues exactly zero object-store GETs. The decline tests pin the
//! correctness constraint from #849's safety lemma: whenever exactness cannot
//! be proven (a pending erasure, a `Str`-typed column, a non-declared column,
//! or an omitted dictionary) the rule declines and the plan keeps its
//! `LogsScanExec`.
//!
//! These build the loaded statistics by hand and inject them with
//! `LogsTableProvider::with_column_stats`, so no fold runs: the object under
//! test is the query-side rewrite, and a hand-built `LoadedColumnStats` is the
//! exact input the resolver would have threaded down.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Int64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::SessionContext;
use ravel_catalog::{EntryIdentity, LoadedColumnStats, SegmentLevel, SegmentRef, Snapshot};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_proto::catalog::v1::{ColumnStat, ColumnStatsSegment, ColumnValue, DictEntry};
use ravel_query::LogSegmentFetcher;
use ravel_sql::{
    DeclaredColumn, DeclaredType, LogsTableProvider, SessionTable, SqlConfig,
    TenantMemoryAccountant, build_session,
};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use uuid::Uuid;

mod util;
use util::CountingStore;

const TENANT: TenantHash = TenantHash([7u8; 16]);

/// The one writer identity every fabricated segment shares; only `writer_seq`
/// varies between segments, so `segment_identity` stays deterministic.
fn writer() -> Uuid {
    Uuid::from_u128(1)
}

/// A fabricated L0 [`SegmentRef`]. The metadata path never fetches the object,
/// so `data_object_key` need not name a real object; the identity fields are
/// what join it to the injected statistics.
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
        writer_id: writer(),
        writer_epoch: 1,
        writer_seq: seq,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
    }
}

/// The identity `LogsScanExec::segment_identity` derives for `seg`.
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

/// An exact I64 `ColumnStat` from a value->count dictionary. `min`/`max` are
/// the dictionary's extremes; `non_null_count` is the summed dictionary
/// counts. `dictionary_present` mirrors the fold's decision.
fn i64_stat(
    name: &str,
    dict: &[(i64, u64)],
    null_count: u64,
    dictionary_present: bool,
) -> ColumnStat {
    let non_null_count: u64 = dict.iter().map(|(_, c)| c).sum();
    let min = dict.iter().map(|(v, _)| *v).min();
    let max = dict.iter().map(|(v, _)| *v).max();
    let dictionary = if dictionary_present {
        dict.iter()
            .map(|(v, c)| DictEntry {
                value: Some(i64_value(*v)),
                count: *c,
            })
            .collect()
    } else {
        Vec::new()
    };
    ColumnStat {
        name: name.to_string(),
        declared_type: 2, // ravel.sys.v1.TypedAttrColumnType::I64
        non_null_count,
        null_count,
        min: min.map(i64_value),
        max: max.map(i64_value),
        dictionary_present,
        dictionary,
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

fn loaded_stats(entries: Vec<(&SegmentRef, ColumnStatsSegment)>) -> Arc<LoadedColumnStats> {
    let mut segments = HashMap::new();
    for (seg, stat) in entries {
        segments.insert(identity_of(seg), stat);
    }
    Arc::new(LoadedColumnStats {
        segments,
        part_blake3: Vec::new(),
    })
}

fn snapshot_of(
    segments: Vec<SegmentRef>,
    erasure: Vec<ravel_proto::commit::v1::ErasureRequest>,
) -> Snapshot {
    Snapshot {
        segments,
        segments_pruned: 0,
        pending_erasure: erasure,
    }
}

/// A logs session over `provider`, built exactly as `SqlExecutor` builds one
/// (`build_session`, so the default physical optimizer chain, including
/// `MetadataOnlyAggregate`, is in force).
fn logs_session(provider: LogsTableProvider) -> datafusion::error::Result<SessionContext> {
    let config = SqlConfig::default();
    let tenant = TenantMemoryAccountant::new(1 << 30);
    let (pool, _breach) = config.query_pool(tenant, QueryAccounting::new());
    build_session(&config, pool, SessionTable::Logs(Arc::new(provider)), false)
}

fn provider(
    store: &Arc<CountingStore>,
    snapshot: Snapshot,
    declared: Vec<DeclaredColumn>,
    stats: Option<Arc<LoadedColumnStats>>,
) -> LogsTableProvider {
    let backend: Arc<dyn ObjectStoreBackend> = Arc::clone(store) as Arc<dyn ObjectStoreBackend>;
    LogsTableProvider::new(
        snapshot,
        TENANT,
        LogSegmentFetcher::new(backend),
        QueryAccounting::new(),
    )
    .with_declared_columns(declared)
    .with_column_stats(stats)
}

fn status_col() -> Vec<DeclaredColumn> {
    vec![DeclaredColumn::new("status", DeclaredType::I64)]
}

/// The two segments both q02 and q08 read: `status` dictionaries
/// {200:3, 404:2} (no nulls) and {200:4, 500:1} (one null row).
fn two_status_segments() -> (SegmentRef, SegmentRef) {
    (seg_ref(1, 5), seg_ref(2, 6))
}

fn status_stats(a: &SegmentRef, b: &SegmentRef) -> Arc<LoadedColumnStats> {
    loaded_stats(vec![
        (
            a,
            stats_segment(a, vec![i64_stat("status", &[(200, 3), (404, 2)], 0, true)]),
        ),
        (
            b,
            stats_segment(b, vec![i64_stat("status", &[(200, 4), (500, 1)], 1, true)]),
        ),
    ])
}

fn plan_str(plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>) -> String {
    displayable(plan.as_ref()).indent(true).to_string()
}

/// The single scalar of a `COUNT(*)` result.
fn count_scalar(batches: &[RecordBatch]) -> i64 {
    let mut out = 0;
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 count");
        out = arr.value(0);
    }
    out
}

/// `(status value or NULL) -> count` from a q08 result: column 0 is the group
/// key (nullable Int64), column 1 the count.
fn group_counts(batches: &[RecordBatch]) -> HashMap<Option<i64>, i64> {
    let mut out = HashMap::new();
    for batch in batches {
        let keys = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 group key");
        let counts = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 count");
        for row in 0..batch.num_rows() {
            let key = if keys.is_null(row) {
                None
            } else {
                Some(keys.value(row))
            };
            out.insert(key, counts.value(row));
        }
    }
    out
}

// ---------------------------------------------------------------------------
// q02: COUNT(*) WHERE <declared column> <> <literal>
// ---------------------------------------------------------------------------

/// Deliverable: the q02 shape is answered from statistics with zero GETs and
/// no scan. `status <> 404` counts every non-null row that is not 404:
/// segment A contributes 5 - 2 = 3, segment B contributes 5 - 0 = 5 (its one
/// null row is excluded by SQL three-valued logic), for exactly 8.
///
/// Pre-change proof: reverting `declared_not_equal_count` to `return None`
/// (line noted in the report) makes the rule decline, restoring a
/// `FilterExec`-over-`LogsScanExec` plan; both assertions below then fail.
#[tokio::test]
async fn q02_not_equal_count_answered_from_stats_with_zero_gets() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let (a, b) = two_status_segments();
    let stats = status_stats(&a, &b);
    let snapshot = snapshot_of(vec![a, b], Vec::new());
    let ctx = logs_session(provider(&store, snapshot, status_col(), Some(stats))).expect("session");

    let plan = ctx
        .sql("SELECT COUNT(*) FROM logs WHERE status <> 404")
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let shown = plan_str(&plan);
    assert!(
        !shown.contains("LogsScanExec"),
        "q02 must be answered from stats, not scanned; plan was:\n{shown}"
    );

    let batches = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("collect");
    assert_eq!(count_scalar(&batches), 8, "(5-2) + (5-0), nulls excluded");
    assert_eq!(store.gets(), 0, "the q02 answer must read no objects");
}

// ---------------------------------------------------------------------------
// q08: SELECT <declared column>, COUNT(*) GROUP BY <declared column>
// ---------------------------------------------------------------------------

/// Deliverable: the q08 shape is answered from statistics with zero GETs and
/// no scan. The merged dictionary is {200: 3+4, 404: 2, 500: 1} plus a NULL
/// group of 1 (segment B's null row), for four groups totalling 11 rows.
///
/// Pre-change proof: reverting `declared_group_counts` to `return None` (line
/// noted in the report) makes the rule decline; the plan then contains
/// `LogsScanExec` and issues GETs, so both assertions fail.
#[tokio::test]
async fn q08_group_counts_answered_from_stats_with_zero_gets() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let (a, b) = two_status_segments();
    let stats = status_stats(&a, &b);
    let snapshot = snapshot_of(vec![a, b], Vec::new());
    let ctx = logs_session(provider(&store, snapshot, status_col(), Some(stats))).expect("session");

    let plan = ctx
        .sql("SELECT status, COUNT(*) FROM logs GROUP BY status")
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let shown = plan_str(&plan);
    assert!(
        !shown.contains("LogsScanExec"),
        "q08 must be answered from stats, not scanned; plan was:\n{shown}"
    );

    let batches = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("collect");
    let counts = group_counts(&batches);
    let expected: HashMap<Option<i64>, i64> =
        HashMap::from([(Some(200), 7), (Some(404), 2), (Some(500), 1), (None, 1)]);
    assert_eq!(counts, expected, "merged dictionary plus the NULL group");
    assert_eq!(store.gets(), 0, "the q08 answer must read no objects");
}

// ---------------------------------------------------------------------------
// Observable marker (deliverable 3)
// ---------------------------------------------------------------------------

/// The rewrite installs a purpose-named `MetadataOnlyExec` leaf whose EXPLAIN
/// line reads `MetadataOnlyExec: metadata_only=true, rows=<n>`, distinct from
/// the generic nodes DataFusion's own statistics rule produces. q02 yields a
/// single count row (rows=1); q08 yields one row per group plus the NULL group
/// (rows=4).
#[tokio::test]
async fn metadata_only_exec_marker_reports_row_count() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let (a, b) = two_status_segments();

    let q02_ctx = logs_session(provider(
        &store,
        snapshot_of(vec![a.clone(), b.clone()], Vec::new()),
        status_col(),
        Some(status_stats(&a, &b)),
    ))
    .expect("session");
    let q02_plan = q02_ctx
        .sql("SELECT COUNT(*) FROM logs WHERE status <> 404")
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    assert!(
        plan_str(&q02_plan).contains("MetadataOnlyExec: metadata_only=true, rows=1"),
        "q02 marker must report one count row; plan was:\n{}",
        plan_str(&q02_plan)
    );

    let q08_ctx = logs_session(provider(
        &store,
        snapshot_of(vec![a.clone(), b.clone()], Vec::new()),
        status_col(),
        Some(status_stats(&a, &b)),
    ))
    .expect("session");
    let q08_plan = q08_ctx
        .sql("SELECT status, COUNT(*) FROM logs GROUP BY status")
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    assert!(
        plan_str(&q08_plan).contains("MetadataOnlyExec: metadata_only=true, rows=4"),
        "q08 marker must report four group rows; plan was:\n{}",
        plan_str(&q08_plan)
    );
}

// ---------------------------------------------------------------------------
// Decline paths (safety lemma): each keeps the LogsScanExec and never rewrites.
// ---------------------------------------------------------------------------

/// A pending selective erasure means the committed dictionary counts still
/// include rows the erasure removes, so the metadata path must decline. The
/// same q02 statement over the same stats, but with a pending erasure in the
/// snapshot, keeps its scan.
#[tokio::test]
async fn pending_erasure_declines_and_keeps_the_scan() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let (a, b) = two_status_segments();
    let stats = status_stats(&a, &b);
    let erasure = ravel_proto::commit::v1::ErasureRequest {
        predicate: vec![ravel_proto::commit::v1::ErasurePredicateMatcher {
            key: "status".to_string(),
            value: "404".to_string(),
        }],
        ..Default::default()
    };
    let snapshot = snapshot_of(vec![a, b], vec![erasure]);
    let ctx = logs_session(provider(&store, snapshot, status_col(), Some(stats))).expect("session");

    let plan = ctx
        .sql("SELECT COUNT(*) FROM logs WHERE status <> 404")
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let shown = plan_str(&plan);
    assert!(
        !shown.contains("MetadataOnlyExec"),
        "a pending erasure must decline the rewrite; plan was:\n{shown}"
    );
    assert!(
        shown.contains("LogsScanExec"),
        "a pending erasure must fall back to a scan; plan was:\n{shown}"
    );
}

/// A `Str`-typed declared column is not answerable from the dictionary here
/// (its Arrow projection is a dictionary array the reader does not decode into
/// a `ScalarValue` for this path), so q02 over a `Str` column declines. The
/// literal is a string so the plan is well-typed; the rule still refuses.
#[tokio::test]
async fn str_typed_column_declines_and_keeps_the_scan() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let (a, b) = two_status_segments();
    // The stats carry a `region` entry, but its declared type is Str, which
    // `declared_not_equal_count` refuses before reading the dictionary.
    let stats = loaded_stats(vec![
        (&a, stats_segment(&a, Vec::new())),
        (&b, stats_segment(&b, Vec::new())),
    ]);
    let snapshot = snapshot_of(vec![a, b], Vec::new());
    let declared = vec![DeclaredColumn::new("region", DeclaredType::Str)];
    let ctx = logs_session(provider(&store, snapshot, declared, Some(stats))).expect("session");

    let plan = ctx
        .sql("SELECT COUNT(*) FROM logs WHERE region <> 'us'")
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let shown = plan_str(&plan);
    assert!(
        !shown.contains("MetadataOnlyExec"),
        "a Str-typed column must decline the rewrite; plan was:\n{shown}"
    );
    assert!(
        shown.contains("LogsScanExec"),
        "a Str-typed column must fall back to a scan; plan was:\n{shown}"
    );
}

/// A predicate over a non-declared column (`body`, a fixed column below
/// `FIRST_DECLARED_COL`) has no per-value statistics at all, so the rule
/// declines before consulting any dictionary.
#[tokio::test]
async fn non_declared_column_declines_and_keeps_the_scan() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let (a, b) = two_status_segments();
    let stats = status_stats(&a, &b);
    let snapshot = snapshot_of(vec![a, b], Vec::new());
    let ctx = logs_session(provider(&store, snapshot, status_col(), Some(stats))).expect("session");

    let plan = ctx
        .sql("SELECT COUNT(*) FROM logs WHERE body <> 'x'")
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let shown = plan_str(&plan);
    assert!(
        !shown.contains("MetadataOnlyExec"),
        "a non-declared column must decline the rewrite; plan was:\n{shown}"
    );
    assert!(
        shown.contains("LogsScanExec"),
        "a non-declared column must fall back to a scan; plan was:\n{shown}"
    );
}

/// A segment whose dictionary was omitted (distinct-value count exceeded the
/// fold's cardinality ceiling, `dictionary_present = false`) carries no exact
/// per-value counts, so a q02/q08 answer derived from it could be wrong
/// outright, not merely unavailable. The rule must decline. Segment A has a
/// present dictionary; segment B's is omitted, which alone forces fallback.
#[tokio::test]
async fn omitted_dictionary_declines_and_keeps_the_scan() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let (a, b) = two_status_segments();
    let stats = loaded_stats(vec![
        (
            &a,
            stats_segment(&a, vec![i64_stat("status", &[(200, 3), (404, 2)], 0, true)]),
        ),
        (
            &b,
            // dictionary_present = false: the fold dropped this column's
            // dictionary for exceeding the cardinality ceiling.
            stats_segment(&b, vec![i64_stat("status", &[], 5, false)]),
        ),
    ]);
    let snapshot = snapshot_of(vec![a, b], Vec::new());
    let ctx = logs_session(provider(&store, snapshot, status_col(), Some(stats))).expect("session");

    let plan = ctx
        .sql("SELECT status, COUNT(*) FROM logs GROUP BY status")
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let shown = plan_str(&plan);
    assert!(
        !shown.contains("MetadataOnlyExec"),
        "an omitted dictionary must decline the rewrite; plan was:\n{shown}"
    );
    assert!(
        shown.contains("LogsScanExec"),
        "an omitted dictionary must fall back to a scan; plan was:\n{shown}"
    );
}
