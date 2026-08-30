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
//! Most of these build the loaded statistics by hand and inject them with
//! `LogsTableProvider::with_column_stats`, so no fold runs: the object under
//! test is the query-side rewrite, and a hand-built `LoadedColumnStats` is the
//! exact input the resolver would have threaded down.
//!
//! The equivalence tests at the end of the file are the exception, and they
//! carry the property the whole optimization rests on: over one real RLOG
//! corpus, the REWRITTEN answer equals the SCANNED answer for the same query.
//! They write real objects, derive the statistics from those same records, and
//! run each statement twice -- once with statistics loaded (rewrite eligible)
//! and once with none (rewrite ineligible, so the scan runs) -- asserting the
//! two agree. An assertion over fabricated statistics alone cannot catch a
//! rewrite that answers a different question than the one asked.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{Array, Float64Array, Int64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::SessionContext;
use ravel_catalog::{EntryIdentity, LoadedColumnStats, SegmentLevel, SegmentRef, Snapshot};
use ravel_logseg::writer::ObjectIdentity;
use ravel_logseg::{AttrValue, LogRecord, RlogConfig, RlogWriter, stream_attrs_bytes};
use ravel_object_store::memory::MemoryStore;
use ravel_object_store::{ObjectStoreBackend, PutOptions};
use ravel_proto::catalog::v1::{ColumnStat, ColumnStatsSegment, ColumnValue, DictEntry};
use ravel_query::LogSegmentFetcher;
use ravel_sql::{
    DeclaredColumn, DeclaredType, LogsTableProvider, SessionTable, SqlConfig,
    TenantMemoryAccountant, build_session,
};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use ravel_types::logstream::log_stream_id;
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
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
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
        sum: Some(sum),
    }
}

fn str_value(v: &str) -> ColumnValue {
    ColumnValue {
        kind: Some(ravel_proto::catalog::v1::column_value::Kind::StrUtf8(
            v.to_string(),
        )),
    }
}

/// An exact `Str` `ColumnStat` from a value->count dictionary, mirroring
/// [`i64_stat`]. Used to prove a `Str`-declared column declines even when its
/// statistics ARE present.
fn str_stat(name: &str, dict: &[(&str, u64)]) -> ColumnStat {
    let non_null_count: u64 = dict.iter().map(|(_, c)| c).sum();
    let min = dict.iter().map(|(v, _)| v.to_string()).min();
    let max = dict.iter().map(|(v, _)| v.to_string()).max();
    ColumnStat {
        name: name.to_string(),
        declared_type: 1, // ravel.sys.v1.TypedAttrColumnType::Str
        non_null_count,
        null_count: 0,
        min: min.as_deref().map(str_value),
        max: max.as_deref().map(str_value),
        dictionary_present: true,
        dictionary: dict
            .iter()
            .map(|(v, c)| DictEntry {
                value: Some(str_value(v)),
                count: *c,
            })
            .collect(),
        sum: None,
    }
}

/// An I64 `ColumnStat` whose dictionary was OMITTED (`dictionary_present =
/// false`) for exceeding the fold's cardinality ceiling, while
/// `non_null_count` still reflects the real non-null rows. This is the state
/// a q02/q08 answer cannot be derived from, so the rule must decline.
fn i64_omitted_stat(name: &str, non_null_count: u64) -> ColumnStat {
    ColumnStat {
        name: name.to_string(),
        declared_type: 2,
        non_null_count,
        null_count: 0,
        min: None,
        max: None,
        dictionary_present: false,
        dictionary: Vec::new(),
        sum: None,
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
/// `MetadataOnlyAggregate`, is in force). Single-stage aggregate:
/// `exact_typed_aggregates = false`.
fn logs_session(provider: LogsTableProvider) -> datafusion::error::Result<SessionContext> {
    logs_session_with(provider, false)
}

/// A logs session with an explicit `exact_typed_aggregates` flag. With `true`
/// (the production default for a query the classifier admits, ADR-0094), a
/// `GROUP BY` becomes the two-stage `Partial -> RepartitionExec(Hash) ->
/// FinalPartitioned` plan, which the rewrite must descend through
/// (`shuffle_child` admits the hash repartition) just as it does the
/// single-stage plan.
fn logs_session_with(
    provider: LogsTableProvider,
    exact_typed_aggregates: bool,
) -> datafusion::error::Result<SessionContext> {
    let config = SqlConfig::default();
    let tenant = TenantMemoryAccountant::new(1 << 30);
    let (pool, _breach) = config.query_pool(tenant, QueryAccounting::new());
    build_session(
        &config,
        pool,
        SessionTable::Logs(Arc::new(provider)),
        exact_typed_aggregates,
        false,
    )
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

/// `COUNT(NULL)` is 0 by SQL semantics, NOT `COUNT(*)`. The rule must not
/// treat a literal-NULL count argument as `COUNT(*)` and answer it from
/// `non_null_count`: with the pre-fix `is_count_star` this statement rewrote
/// to the metadata count of non-404 rows and returned it; it must return 0.
///
/// Run over the real corpus so the declined statement can execute its scan
/// (a fabricated segment has no object to read). With statistics loaded the
/// rule is eligible but declines on the NULL literal, so the scan runs and
/// returns 0.
#[tokio::test]
async fn count_null_is_zero_not_rewritten() {
    let corpus = RealCorpus::build().await;
    let (shown, batches) = corpus
        .run("SELECT COUNT(NULL) FROM logs WHERE status <> 404", true)
        .await;
    assert!(
        !shown.contains("MetadataOnlyExec"),
        "COUNT(NULL) must not be rewritten as COUNT(*); plan was:\n{shown}"
    );
    assert_eq!(
        count_scalar(&batches),
        0,
        "COUNT(NULL) is 0, not the row count"
    );
}

// ---------------------------------------------------------------------------
// Parallel final aggregation (ADR-0094, the shipped default): the rewrite must
// fire over the two-stage Partial -> RepartitionExec(Hash) -> FinalPartitioned
// plan, not only the single-stage aggregate.
// ---------------------------------------------------------------------------

/// q08 with `exact_typed_aggregates = true` (production's default for a query
/// the classifier admits). The scan path proves the plan shape is the
/// two-stage parallel one; with statistics loaded the rewrite descends through
/// the hash repartition and answers from metadata with zero GETs.
#[tokio::test]
async fn q08_parallel_final_answered_from_stats() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let (a, b) = two_status_segments();

    // No stats: the rule declines, so the ordinary exact-typed plan survives
    // and we can assert it really is Partial -> RepartitionExec(Hash) ->
    // FinalPartitioned.
    let scan_ctx = logs_session_with(
        provider(
            &store,
            snapshot_of(vec![a.clone(), b.clone()], Vec::new()),
            status_col(),
            None,
        ),
        true,
    )
    .expect("session");
    let scan_plan = scan_ctx
        .sql("SELECT status, COUNT(*) FROM logs GROUP BY status")
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let scan_shown = plan_str(&scan_plan);
    assert!(
        scan_shown.contains("RepartitionExec")
            && scan_shown.contains("mode=FinalPartitioned")
            && scan_shown.contains("mode=Partial"),
        "exact_typed_aggregates must produce the two-stage parallel plan; plan was:\n{scan_shown}"
    );

    // Stats loaded: the rewrite fires over that same shape.
    let stats = status_stats(&a, &b);
    let ctx = logs_session_with(
        provider(
            &store,
            snapshot_of(vec![a, b], Vec::new()),
            status_col(),
            Some(stats),
        ),
        true,
    )
    .expect("session");
    let plan = ctx
        .sql("SELECT status, COUNT(*) FROM logs GROUP BY status")
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let shown = plan_str(&plan);
    assert!(
        !shown.contains("LogsScanExec") && shown.contains("MetadataOnlyExec"),
        "the rewrite must fire over the parallel-final plan; plan was:\n{shown}"
    );
    let batches = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("collect");
    let expected: HashMap<Option<i64>, i64> =
        HashMap::from([(Some(200), 7), (Some(404), 2), (Some(500), 1), (None, 1)]);
    assert_eq!(
        group_counts(&batches),
        expected,
        "merged dictionary plus NULL"
    );
    assert_eq!(store.gets(), 0, "answered from stats, no objects read");
}

/// q02 with `exact_typed_aggregates = true`: the count aggregate is still
/// order/partition-independent, so the rewrite must fire and read no objects.
#[tokio::test]
async fn q02_parallel_final_answered_from_stats() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let (a, b) = two_status_segments();
    let stats = status_stats(&a, &b);
    let ctx = logs_session_with(
        provider(
            &store,
            snapshot_of(vec![a, b], Vec::new()),
            status_col(),
            Some(stats),
        ),
        true,
    )
    .expect("session");
    let plan = ctx
        .sql("SELECT COUNT(*) FROM logs WHERE status <> 404")
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let shown = plan_str(&plan);
    assert!(
        !shown.contains("LogsScanExec") && shown.contains("MetadataOnlyExec"),
        "the rewrite must fire under exact_typed_aggregates; plan was:\n{shown}"
    );
    let batches = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("collect");
    assert_eq!(count_scalar(&batches), 8, "(5-2) + (5-0), nulls excluded");
    assert_eq!(store.gets(), 0, "answered from stats, no objects read");
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

    // The same erasure must force the sum-backed shapes (q03/q04/q30) to
    // decline too: a pending erasure removes rows the folded sum and count
    // still include (`declared_column_sum` gates on the same `stats_are_exact`,
    // logs_scan.rs). Rebuilt rather than reused: the first `snapshot_of` call
    // consumed the segment refs and the stats, so this constructs an
    // equivalent snapshot carrying the same pending erasure.
    let ctx = logs_session(provider(
        &store,
        snapshot_of(
            vec![seg_ref(1, 5), seg_ref(2, 6)],
            vec![ravel_proto::commit::v1::ErasureRequest {
                predicate: vec![ravel_proto::commit::v1::ErasurePredicateMatcher {
                    key: "status".to_string(),
                    value: "404".to_string(),
                }],
                ..Default::default()
            }],
        ),
        status_col(),
        Some(status_stats(&seg_ref(1, 5), &seg_ref(2, 6))),
    ))
    .expect("session");
    for sql in [
        "SELECT SUM(status + 1) FROM logs",
        "SELECT AVG(status) FROM logs",
    ] {
        let plan = ctx
            .sql(sql)
            .await
            .expect("plan")
            .create_physical_plan()
            .await
            .expect("physical plan");
        let shown = plan_str(&plan);
        assert!(
            !shown.contains("MetadataOnlyExec") && shown.contains("LogsScanExec"),
            "a pending erasure must force {sql} to scan; plan was:\n{shown}"
        );
    }
}

/// A `Str`-typed declared column is not answerable from the dictionary here
/// (its Arrow projection is a dictionary array the reader does not decode into
/// a `ScalarValue` for this path), so q02 over a `Str` column declines. The
/// literal is a string so the plan is well-typed; the rule still refuses.
#[tokio::test]
async fn str_typed_column_declines_and_keeps_the_scan() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let (a, b) = two_status_segments();
    // The stats DO carry a `region` entry with a real present dictionary, so a
    // missing-column decline cannot mask the reason under test: the sole cause
    // is that `region` is declared `Str`, which `declared_not_equal_count`
    // refuses (logs_scan.rs, the `matches!(declared.ty, DeclaredType::Str)`
    // gate, reinforced by `declared_scalar` having no `Str` arm).
    let stats = loaded_stats(vec![
        (
            &a,
            stats_segment(&a, vec![str_stat("region", &[("us", 3), ("eu", 2)])]),
        ),
        (
            &b,
            stats_segment(&b, vec![str_stat("region", &[("us", 4), ("eu", 2)])]),
        ),
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

/// A `ts` bound that CLIPS a touched segment invalidates every folded
/// whole-segment figure, so q02 must decline.
///
/// This is the non-obvious reachable case (#850 wave-0 checkpoint): segments
/// are resolved on OVERLAP, and `LogsTableProvider::supports_filters_pushdown`
/// reports a pure `ts` bound `Exact`, so DataFusion deletes the ts
/// `FilterExec` outright and the bound survives only inside the scan's
/// `ts_min`/`ts_max`. The surviving `status <> 404` filter is then the single
/// `FilterExec` the rule admits, and the rewrite fires over segments whose
/// dictionaries describe rows the query excludes.
///
/// Both fixture segments span ts 0..=1_000, so `ts < 500` clips both. Before
/// the fix `declared_not_equal_count` consulted only `erasure` and returned 8,
/// the whole-segment total: byte-for-byte the answer to the query WITHOUT the
/// ts bound. A per-segment dictionary carries no intra-segment time
/// distribution, so the clipped count cannot be derived from it and declining
/// is the only exact option.
#[tokio::test]
async fn q02_clipping_ts_bound_declines_and_keeps_the_scan() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let (a, b) = two_status_segments();
    let stats = status_stats(&a, &b);
    let snapshot = snapshot_of(vec![a, b], Vec::new());
    let ctx = logs_session(provider(&store, snapshot, status_col(), Some(stats))).expect("session");

    let plan = ctx
        .sql(
            "SELECT COUNT(*) FROM logs WHERE status <> 404 \
             AND ts < TIMESTAMP '1970-01-01 00:00:00.000000500'",
        )
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let shown = plan_str(&plan);
    assert!(
        !shown.contains("MetadataOnlyExec"),
        "a clipping ts bound must decline the rewrite; plan was:\n{shown}"
    );
    assert!(
        shown.contains("LogsScanExec"),
        "a clipping ts bound must fall back to a scan; plan was:\n{shown}"
    );
}

/// The q08 half of the same hole. This shape carries no `FilterExec` at all,
/// so nothing but `declared_group_counts` itself can refuse for a clipping ts
/// bound. Before the fix it returned the same map as the unbounded query
/// (`{200: 7, 404: 2, 500: 1, NULL: 1}`), ignoring `ts < 500` entirely.
#[tokio::test]
async fn q08_clipping_ts_bound_declines_and_keeps_the_scan() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let (a, b) = two_status_segments();
    let stats = status_stats(&a, &b);
    let snapshot = snapshot_of(vec![a, b], Vec::new());
    let ctx = logs_session(provider(&store, snapshot, status_col(), Some(stats))).expect("session");

    let plan = ctx
        .sql(
            "SELECT status, COUNT(*) FROM logs \
             WHERE ts < TIMESTAMP '1970-01-01 00:00:00.000000500' GROUP BY status",
        )
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let shown = plan_str(&plan);
    assert!(
        !shown.contains("MetadataOnlyExec"),
        "a clipping ts bound must decline the rewrite; plan was:\n{shown}"
    );
    assert!(
        shown.contains("LogsScanExec"),
        "a clipping ts bound must fall back to a scan; plan was:\n{shown}"
    );
}

/// A `has_word` content predicate is the second hole of the same shape, and
/// it reaches the rule the same way a clipping ts bound does: `has_word` is
/// also reported `Exact` by `LogsTableProvider::supports_filters_pushdown`
/// (crates/ravel-sql/src/logs_provider.rs:288-304), so its `FilterExec` is
/// deleted and the predicate survives only as `LogsScanExec::content`, where
/// the reader evaluates it per row. Nothing in `classify_scan_chain`
/// (crates/ravel-sql/src/metadata_agg.rs:129-151) looks at `content`, so the
/// `status <> 404` filter is again the single admitted `FilterExec` and the
/// rewrite fires over dictionaries that count rows the content predicate
/// removes.
#[tokio::test]
async fn content_predicate_declines_and_keeps_the_scan() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let (a, b) = two_status_segments();
    let stats = status_stats(&a, &b);
    let snapshot = snapshot_of(vec![a, b], Vec::new());
    let ctx = logs_session(provider(&store, snapshot, status_col(), Some(stats))).expect("session");

    let plan = ctx
        .sql("SELECT COUNT(*) FROM logs WHERE status <> 404 AND has_word(body, 'needle')")
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let shown = plan_str(&plan);
    assert!(
        !shown.contains("MetadataOnlyExec"),
        "a content predicate must decline the rewrite; plan was:\n{shown}"
    );
    assert!(
        shown.contains("LogsScanExec"),
        "a content predicate must fall back to a scan; plan was:\n{shown}"
    );
}

/// A `ts` bound that CONTAINS every touched segment removes no row, so the
/// folded figures stay exact and the rewrite must still fire. Without this the
/// two tests above would be satisfied by a guard that refuses any ts bound at
/// all, which would silently retire the optimization for every windowed query.
#[tokio::test]
async fn q02_containing_ts_bound_still_answers_from_stats() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let (a, b) = two_status_segments();
    let stats = status_stats(&a, &b);
    let snapshot = snapshot_of(vec![a, b], Vec::new());
    let ctx = logs_session(provider(&store, snapshot, status_col(), Some(stats))).expect("session");

    // Both segments span 0..=1_000, so this bound contains them exactly.
    let plan = ctx
        .sql(
            "SELECT COUNT(*) FROM logs WHERE status <> 404 \
             AND ts BETWEEN TIMESTAMP '1970-01-01 00:00:00.000000000' \
             AND TIMESTAMP '1970-01-01 00:00:00.000001000'",
        )
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let shown = plan_str(&plan);
    assert!(
        !shown.contains("LogsScanExec"),
        "a containing ts bound must still answer from stats; plan was:\n{shown}"
    );

    let batches = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("collect");
    assert_eq!(count_scalar(&batches), 8, "(5-2) + (5-0), nulls excluded");
    assert_eq!(store.gets(), 0, "a contained window must read no objects");
}

/// A segment whose dictionary was omitted (distinct-value count exceeded the
/// fold's cardinality ceiling, `dictionary_present = false`) carries no exact
/// per-value counts, so a q02/q08 answer derived from it could be wrong
/// outright, not merely unavailable. The rule must decline. Segment A has a
/// present dictionary; segment B's is omitted while still declaring 5 non-null
/// rows, so dropping the `!dictionary_present` gate (logs_scan.rs) would
/// undercount by exactly those 5 rows -- the gate is the only thing that can
/// cause the decline here.
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
            // dictionary_present = false with 5 real non-null rows: the fold
            // dropped this column's dictionary for exceeding the cardinality
            // ceiling.
            stats_segment(&b, vec![i64_omitted_stat("status", 5)]),
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

// ---------------------------------------------------------------------------
// Rewrite-equals-scan equivalence over a real corpus
// ---------------------------------------------------------------------------

/// `(ts_ns, status)` for the first real segment. `None` means the record
/// carries no `status` attribute at all, so the declared column reads NULL.
const SEG_A_ROWS: &[(i64, Option<i64>)] = &[
    (0, Some(200)),
    (100, Some(200)),
    (200, Some(404)),
    (300, Some(200)),
    (400, Some(404)),
    (500, Some(500)),
];

/// `(ts_ns, status)` for the second real segment.
const SEG_B_ROWS: &[(i64, Option<i64>)] = &[
    (50, Some(200)),
    (150, Some(500)),
    (250, Some(200)),
    (350, None),
    (450, Some(200)),
    (550, Some(200)),
];

/// The clipping bound both equivalence tests use. Segment A spans 0..=500 and
/// segment B spans 50..=550, so `ts < 500` clips both: it drops A's last row
/// and B's last row while leaving each segment overlapping the window, which
/// is exactly the shape segment resolution keeps and whole-segment statistics
/// cannot describe.
const CLIP_SQL: &str = "TIMESTAMP '1970-01-01 00:00:00.000000500'";

fn eq_record(ts: i64, status: Option<i64>) -> LogRecord {
    let resource: Vec<(String, AttrValue)> = vec![(
        "service.name".to_string(),
        AttrValue::Str("api".to_string()),
    )];
    let attrs = match status {
        Some(v) => vec![("status".to_string(), AttrValue::I64(v))],
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

/// Write `rows` as one real RLOG object under `key` and return the
/// `SegmentRef` describing it, with the ts span and sample count taken from
/// the records actually written.
async fn write_real_segment(
    store: &Arc<CountingStore>,
    key: &str,
    seq: u64,
    rows: &[(i64, Option<i64>)],
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
    for (ts, status) in rows {
        w.push(eq_record(*ts, *status)).expect("push");
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
        content_hash: [0u8; 32],
        writer_id: writer(),
        writer_epoch: 1,
        writer_seq: seq,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
    }
}

/// The exact `status` statistics for `rows`: what a correct fold over that
/// object produces. Derived from the same constants the object is written
/// from, so the statistics and the data cannot drift apart.
fn stat_from_rows(rows: &[(i64, Option<i64>)]) -> ColumnStat {
    let mut dict: Vec<(i64, u64)> = Vec::new();
    let mut null_count = 0u64;
    for (_, status) in rows {
        match status {
            Some(v) => match dict.iter_mut().find(|(dv, _)| dv == v) {
                Some(entry) => entry.1 += 1,
                None => dict.push((*v, 1)),
            },
            None => null_count += 1,
        }
    }
    dict.sort_unstable();
    i64_stat("status", &dict, null_count, true)
}

/// One real corpus plus its exact statistics, reusable across both eligibility
/// settings so the two runs read byte-identical data.
struct RealCorpus {
    store: Arc<CountingStore>,
    a: SegmentRef,
    b: SegmentRef,
    stats: Arc<LoadedColumnStats>,
}

impl RealCorpus {
    async fn build() -> RealCorpus {
        let store = CountingStore::new(Arc::new(MemoryStore::new()));
        let a = write_real_segment(&store, "logs/eq-a.rlog", 1, SEG_A_ROWS).await;
        let b = write_real_segment(&store, "logs/eq-b.rlog", 2, SEG_B_ROWS).await;
        let stats = loaded_stats(vec![
            (&a, stats_segment(&a, vec![stat_from_rows(SEG_A_ROWS)])),
            (&b, stats_segment(&b, vec![stat_from_rows(SEG_B_ROWS)])),
        ]);
        RealCorpus { store, a, b, stats }
    }

    /// Run `sql` over this corpus. `with_stats` decides whether the rewrite is
    /// eligible at all: with no loaded statistics the rule always declines, so
    /// the same statement is forced down the ordinary scan path.
    async fn run(&self, sql: &str, with_stats: bool) -> (String, Vec<RecordBatch>) {
        let stats = with_stats.then(|| Arc::clone(&self.stats));
        let snapshot = snapshot_of(vec![self.a.clone(), self.b.clone()], Vec::new());
        let ctx =
            logs_session(provider(&self.store, snapshot, status_col(), stats)).expect("session");
        let plan = ctx
            .sql(sql)
            .await
            .expect("plan")
            .create_physical_plan()
            .await
            .expect("physical plan");
        let shown = plan_str(&plan);
        let batches = collect(plan, ctx.task_ctx()).await.expect("collect");
        (shown, batches)
    }
}

/// The q02 equivalence property, and the regression the wave-0 checkpoint
/// found. Over one real corpus:
///
/// - unbounded, the rewrite fires (no `LogsScanExec` in the plan) and its
///   count equals the scanned count, 9;
/// - under `ts < 500`, which clips both segments, the true count is 7. The
///   rewrite must decline, and the fallback answer must still be 7.
///
/// The clipped and unbounded answers differ (7 vs 9), so the ts bound provably
/// removes rows: a rewrite that ignores it returns 9 for both. That is exactly
/// what the pre-fix tree did, because `declared_not_equal_count` consulted
/// only `erasure` and summed whole-segment dictionary totals.
#[tokio::test]
async fn q02_rewritten_answer_equals_scanned_answer() {
    let corpus = RealCorpus::build().await;

    let unbounded = "SELECT COUNT(*) FROM logs WHERE status <> 404";
    let (rewritten_plan, rewritten) = corpus.run(unbounded, true).await;
    let (_, scanned) = corpus.run(unbounded, false).await;
    assert!(
        !rewritten_plan.contains("LogsScanExec"),
        "the unbounded q02 must be answered from stats, or this test compares \
         the scan against itself; plan was:\n{rewritten_plan}"
    );
    assert_eq!(
        count_scalar(&rewritten),
        count_scalar(&scanned),
        "the rewritten q02 answer must equal the scanned answer"
    );
    assert_eq!(
        count_scalar(&scanned),
        9,
        "4 from segment A, 5 from segment B"
    );

    let clipped = &format!("SELECT COUNT(*) FROM logs WHERE status <> 404 AND ts < {CLIP_SQL}");
    let (clipped_plan, clipped_rewrite) = corpus.run(clipped, true).await;
    let (_, clipped_scan) = corpus.run(clipped, false).await;
    assert!(
        clipped_plan.contains("LogsScanExec"),
        "a clipping ts bound must fall back to a scan; plan was:\n{clipped_plan}"
    );
    assert_eq!(
        count_scalar(&clipped_rewrite),
        count_scalar(&clipped_scan),
        "the clipped q02 answer must not depend on whether statistics loaded"
    );
    assert_eq!(
        count_scalar(&clipped_scan),
        7,
        "3 from segment A, 4 from segment B once ts < 500 drops one row each"
    );
    assert_ne!(
        count_scalar(&clipped_scan),
        count_scalar(&scanned),
        "the ts bound must actually remove rows, or the clipped case proves nothing"
    );
}

/// The q08 half of the same property. Unbounded, the merged dictionary is
/// `{200: 7, 404: 2, 500: 2, NULL: 1}`; under `ts < 500` the true grouping is
/// `{200: 6, 404: 2, 500: 1, NULL: 1}`. This shape carries no `FilterExec` at
/// all, so `declared_group_counts` is the only thing that can refuse for the
/// clipping bound.
#[tokio::test]
async fn q08_rewritten_answer_equals_scanned_answer() {
    let corpus = RealCorpus::build().await;

    let unbounded = "SELECT status, COUNT(*) FROM logs GROUP BY status";
    let (rewritten_plan, rewritten) = corpus.run(unbounded, true).await;
    let (_, scanned) = corpus.run(unbounded, false).await;
    assert!(
        !rewritten_plan.contains("LogsScanExec"),
        "the unbounded q08 must be answered from stats, or this test compares \
         the scan against itself; plan was:\n{rewritten_plan}"
    );
    assert_eq!(
        group_counts(&rewritten),
        group_counts(&scanned),
        "the rewritten q08 groups must equal the scanned groups"
    );
    assert_eq!(
        group_counts(&scanned),
        HashMap::from([(Some(200), 7), (Some(404), 2), (Some(500), 2), (None, 1)]),
        "the whole corpus, 12 rows over four groups"
    );

    let clipped =
        &format!("SELECT status, COUNT(*) FROM logs WHERE ts < {CLIP_SQL} GROUP BY status");
    let (clipped_plan, clipped_rewrite) = corpus.run(clipped, true).await;
    let (_, clipped_scan) = corpus.run(clipped, false).await;
    assert!(
        clipped_plan.contains("LogsScanExec"),
        "a clipping ts bound must fall back to a scan; plan was:\n{clipped_plan}"
    );
    assert_eq!(
        group_counts(&clipped_rewrite),
        group_counts(&clipped_scan),
        "the clipped q08 groups must not depend on whether statistics loaded"
    );
    assert_eq!(
        group_counts(&clipped_scan),
        HashMap::from([(Some(200), 6), (Some(404), 2), (Some(500), 1), (None, 1)]),
        "ts < 500 drops one 200 row and one 500 row"
    );
    assert_ne!(
        group_counts(&clipped_scan),
        group_counts(&scanned),
        "the ts bound must actually remove rows, or the clipped case proves nothing"
    );
}

// ---------------------------------------------------------------------------
// Fail-closed on an internally-inconsistent record (Finding 4, read side)
// ---------------------------------------------------------------------------

/// An internally-inconsistent `ColumnStat` (dictionary counts that do not sum
/// to `non_null_count`) is rejected at LOAD by `decode_column_stats`, so the
/// real path can never hand one to the query. Injected by hand here (bypassing
/// decode), the READ side must still fail closed: `declared_not_equal_count`
/// declines rather than subtracting from a dictionary that cannot account for
/// the claimed rows, and the query returns the correct SCANNED answer.
#[tokio::test]
async fn inconsistent_record_declines_at_use_and_scans_correct_answer() {
    let corpus = RealCorpus::build().await;

    // Corrupt segment A's stat: claim 100 more non-null rows than its
    // dictionary accounts for.
    let mut bad_a = stat_from_rows(SEG_A_ROWS);
    bad_a.non_null_count += 100;
    let bad_stats = loaded_stats(vec![
        (&corpus.a, stats_segment(&corpus.a, vec![bad_a])),
        (
            &corpus.b,
            stats_segment(&corpus.b, vec![stat_from_rows(SEG_B_ROWS)]),
        ),
    ]);

    let snapshot = snapshot_of(vec![corpus.a.clone(), corpus.b.clone()], Vec::new());
    let ctx = logs_session(provider(
        &corpus.store,
        snapshot,
        status_col(),
        Some(bad_stats),
    ))
    .expect("session");
    let plan = ctx
        .sql("SELECT COUNT(*) FROM logs WHERE status <> 404")
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let shown = plan_str(&plan);
    assert!(
        shown.contains("LogsScanExec") && !shown.contains("MetadataOnlyExec"),
        "an inconsistent record must decline at use and fall back to a scan; plan was:\n{shown}"
    );

    let batches = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("collect");
    assert_eq!(
        count_scalar(&batches),
        9,
        "the scan returns the true count, unaffected by the corrupt stat"
    );
}

/// A column with non-null rows but no recorded extremum cannot answer MIN/MAX.
/// Before the fix the accumulator stayed `None` and the caller substituted a
/// NULL scalar reported as `Precision::Exact`, so the plan claimed the minimum
/// of non-null data was exactly NULL. That is a wrong answer, not a missing
/// optimisation, and it is the third defect of this shape on this path: the
/// scan's ts window was ignored, a dictionary could be empty while rows were
/// counted, and now an extremum can be absent while rows exist.
///
/// Asserts the SCANNED answer, not merely that a decline happened: a test that
/// only checked for the decline would pass against an implementation that
/// declines on everything and silently deletes the optimisation.
///
/// Prove-the-test: removing the `stat.non_null_count > 0 && (min.is_none() ||
/// max.is_none())` decline in `declared_min_max_all` makes the plan report
/// MetadataOnlyExec and the assertion below fails on the plan shape.
#[tokio::test]
async fn a_missing_extremum_with_non_null_rows_declines_and_scans() {
    let corpus = RealCorpus::build().await;

    // Segment A keeps its row count but loses both extrema.
    let mut bad_a = stat_from_rows(SEG_A_ROWS);
    bad_a.min = None;
    bad_a.max = None;
    let bad_stats = loaded_stats(vec![
        (&corpus.a, stats_segment(&corpus.a, vec![bad_a])),
        (
            &corpus.b,
            stats_segment(&corpus.b, vec![stat_from_rows(SEG_B_ROWS)]),
        ),
    ]);

    let snapshot = snapshot_of(vec![corpus.a.clone(), corpus.b.clone()], Vec::new());
    let ctx = logs_session(provider(
        &corpus.store,
        snapshot,
        status_col(),
        Some(bad_stats),
    ))
    .expect("session");
    let plan = ctx
        .sql("SELECT MIN(status), MAX(status) FROM logs")
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let shown = plan_str(&plan);
    assert!(
        shown.contains("LogsScanExec") && !shown.contains("MetadataOnlyExec"),
        "non-null rows with no extremum must decline and fall back to a scan; plan was:\n{shown}"
    );
}

// ---------------------------------------------------------------------------
// q03/q04: SUM(<declared integer column> + k), and q30: AVG(<declared integer
// column>) -- answered from the exact per-object integer sum (#861). Every
// positive test pins the same two facts as the q02/q08 tests: no `LogsScanExec`
// in the plan, and exactly zero object-store GETs.
// ---------------------------------------------------------------------------

/// The single scalar of a one-column `SUM` result, or `None` when it is SQL
/// NULL (sum over zero non-null rows).
fn single_i64(batches: &[RecordBatch]) -> Option<i64> {
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("int64 sum");
        return if arr.is_null(0) {
            None
        } else {
            Some(arr.value(0))
        };
    }
    None
}

/// The single scalar of a one-column `AVG` result, or `None` when it is SQL
/// NULL. Returned as raw bits so callers compare with the repo's
/// bit-pattern float rule rather than `==`.
fn single_f64_bits(batches: &[RecordBatch]) -> Option<u64> {
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let arr = batch
            .column(0)
            .as_any()
            .downcast_ref::<Float64Array>()
            .expect("float64 avg");
        return if arr.is_null(0) {
            None
        } else {
            Some(arr.value(0).to_bits())
        };
    }
    None
}

/// q03/q04: `SUM(status + 1)` is answered from the exact per-object sums with
/// zero GETs and no scan. Segment A sums 1408 over 5 non-null rows, segment B
/// sums 1300 over 5; the `+ 1` applies once per non-null row, so the answer is
/// `2708 + 10 = 2718`.
///
/// Prove-the-test: reverting `LogsScanExec::declared_column_sum` to `return
/// None` makes the rule decline, restoring a `LogsScanExec` plan; both
/// assertions fail.
#[tokio::test]
async fn q03_sum_plus_k_answered_from_stats_with_zero_gets() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let (a, b) = two_status_segments();
    let stats = status_stats(&a, &b);
    let snapshot = snapshot_of(vec![a, b], Vec::new());
    let ctx = logs_session(provider(&store, snapshot, status_col(), Some(stats))).expect("session");

    let plan = ctx
        .sql("SELECT SUM(status + 1) FROM logs")
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let shown = plan_str(&plan);
    assert!(
        !shown.contains("LogsScanExec"),
        "q03 SUM(col + k) must be answered from stats, not scanned; plan was:\n{shown}"
    );
    assert!(
        shown.contains("MetadataOnlyExec: metadata_only=true, rows=1"),
        "q03 must report one sum row via the metadata marker; plan was:\n{shown}"
    );

    let batches = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("collect");
    assert_eq!(
        single_i64(&batches),
        Some(2718),
        "2708 + 1 * 10 non-null rows"
    );
    assert_eq!(store.gets(), 0, "the q03 answer must read no objects");
}

/// The plain `SUM(status)` (addend 0) is answered from stats with zero GETs:
/// `1408 + 1300 = 2708`.
#[tokio::test]
async fn plain_sum_answered_from_stats_with_zero_gets() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let (a, b) = two_status_segments();
    let stats = status_stats(&a, &b);
    let snapshot = snapshot_of(vec![a, b], Vec::new());
    let ctx = logs_session(provider(&store, snapshot, status_col(), Some(stats))).expect("session");

    let plan = ctx
        .sql("SELECT SUM(status) FROM logs")
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let shown = plan_str(&plan);
    assert!(
        !shown.contains("LogsScanExec"),
        "plain SUM(col) must be answered from stats; plan was:\n{shown}"
    );

    let batches = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("collect");
    assert_eq!(single_i64(&batches), Some(2708), "1408 + 1300");
    assert_eq!(store.gets(), 0, "the sum answer must read no objects");
}

/// q30: `AVG(status)` is answered from stats with zero GETs and no scan:
/// `2708 / 10 = 270.8`. Compared by bit pattern, since the answer is a float.
///
/// Prove-the-test: reverting `declared_column_sum` to `return None` restores a
/// `LogsScanExec` plan and both assertions fail.
#[tokio::test]
async fn q30_avg_answered_from_stats_with_zero_gets() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let (a, b) = two_status_segments();
    let stats = status_stats(&a, &b);
    let snapshot = snapshot_of(vec![a, b], Vec::new());
    let ctx = logs_session(provider(&store, snapshot, status_col(), Some(stats))).expect("session");

    let plan = ctx
        .sql("SELECT AVG(status) FROM logs")
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let shown = plan_str(&plan);
    assert!(
        !shown.contains("LogsScanExec"),
        "q30 AVG must be answered from stats, not scanned; plan was:\n{shown}"
    );
    assert!(
        shown.contains("MetadataOnlyExec: metadata_only=true, rows=1"),
        "q30 must report one avg row via the metadata marker; plan was:\n{shown}"
    );

    let batches = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("collect");
    assert_eq!(
        single_f64_bits(&batches),
        Some((2708f64 / 10f64).to_bits()),
        "2708 / 10"
    );
    assert_eq!(store.gets(), 0, "the q30 answer must read no objects");
}

/// A column whose stat carries NO sum -- what a float column would produce, and
/// what an i64-overflowing per-object sum produces at fold time -- must fall
/// back to scanning rather than answer approximately (#861). There is no float
/// declared type in this system, so a `sum = None` I64 stat is the exact state
/// a float column reduces to on this path.
///
/// Over one real corpus: with the sum stripped from both segments' stats,
/// `SUM(status + 1)` keeps its `LogsScanExec` and its answer equals the scanned
/// answer, 3219.
///
/// Prove-the-test: removing the `let seg_sum = stat.sum?;` decline in
/// `declared_column_sum` lets the rule read a phantom sum and the plan-shape
/// assertion fails.
#[tokio::test]
async fn sum_without_a_stored_sum_declines_and_scans() {
    let corpus = RealCorpus::build().await;

    // Strip the sum from both segments' stats.
    let strip = |rows: &[(i64, Option<i64>)]| {
        let mut stat = stat_from_rows(rows);
        stat.sum = None;
        stat
    };
    let no_sum_stats = loaded_stats(vec![
        (&corpus.a, stats_segment(&corpus.a, vec![strip(SEG_A_ROWS)])),
        (&corpus.b, stats_segment(&corpus.b, vec![strip(SEG_B_ROWS)])),
    ]);

    let snapshot = snapshot_of(vec![corpus.a.clone(), corpus.b.clone()], Vec::new());
    let ctx = logs_session(provider(
        &corpus.store,
        snapshot,
        status_col(),
        Some(no_sum_stats),
    ))
    .expect("session");
    let plan = ctx
        .sql("SELECT SUM(status + 1) FROM logs")
        .await
        .expect("plan")
        .create_physical_plan()
        .await
        .expect("physical plan");
    let shown = plan_str(&plan);
    assert!(
        shown.contains("LogsScanExec") && !shown.contains("MetadataOnlyExec"),
        "a missing sum must decline and fall back to a scan; plan was:\n{shown}"
    );
    let rewritten = collect(Arc::clone(&plan), ctx.task_ctx())
        .await
        .expect("collect");

    // The same statement with no loaded stats at all: the ordinary scan path.
    let (_, scanned) = corpus.run("SELECT SUM(status + 1) FROM logs", false).await;
    assert_eq!(
        single_i64(&rewritten),
        single_i64(&scanned),
        "the fallback answer must equal the scanned answer"
    );
    assert_eq!(single_i64(&scanned), Some(3219), "3208 + 11 non-null rows");
}

/// The q03 exactness property over one real corpus: the REWRITTEN `SUM(col + k)`
/// equals the SCANNED one, and a clipping ts bound falls back to a scan whose
/// answer still holds. This is what proves the stored sum is exact against a
/// scan of the same data, and that SQL null handling (segment B has a null row,
/// skipped by `col + k`) matches.
#[tokio::test]
async fn q03_rewritten_answer_equals_scanned_answer() {
    let corpus = RealCorpus::build().await;

    let unbounded = "SELECT SUM(status + 1) FROM logs";
    let (rewritten_plan, rewritten) = corpus.run(unbounded, true).await;
    let (_, scanned) = corpus.run(unbounded, false).await;
    assert!(
        !rewritten_plan.contains("LogsScanExec"),
        "the unbounded q03 must be answered from stats, or this test compares \
         the scan against itself; plan was:\n{rewritten_plan}"
    );
    assert_eq!(
        single_i64(&rewritten),
        single_i64(&scanned),
        "the rewritten q03 answer must equal the scanned answer"
    );
    assert_eq!(single_i64(&scanned), Some(3219), "3208 + 11 non-null rows");

    let clipped = &format!("SELECT SUM(status + 1) FROM logs WHERE ts < {CLIP_SQL}");
    let (clipped_plan, clipped_rewrite) = corpus.run(clipped, true).await;
    let (_, clipped_scan) = corpus.run(clipped, false).await;
    assert!(
        clipped_plan.contains("LogsScanExec"),
        "a clipping ts bound must fall back to a scan; plan was:\n{clipped_plan}"
    );
    assert_eq!(
        single_i64(&clipped_rewrite),
        single_i64(&clipped_scan),
        "the clipped q03 answer must not depend on whether statistics loaded"
    );
    assert_eq!(
        single_i64(&clipped_scan),
        Some(2517),
        "2508 + 9 non-null rows"
    );
    assert_ne!(
        single_i64(&clipped_scan),
        single_i64(&scanned),
        "the ts bound must actually remove rows, or the clipped case proves nothing"
    );
}

/// The q30 half of the same property, for `AVG`.
#[tokio::test]
async fn q30_rewritten_answer_equals_scanned_answer() {
    let corpus = RealCorpus::build().await;

    let unbounded = "SELECT AVG(status) FROM logs";
    let (rewritten_plan, rewritten) = corpus.run(unbounded, true).await;
    let (_, scanned) = corpus.run(unbounded, false).await;
    assert!(
        !rewritten_plan.contains("LogsScanExec"),
        "the unbounded q30 must be answered from stats; plan was:\n{rewritten_plan}"
    );
    assert_eq!(
        single_f64_bits(&rewritten),
        single_f64_bits(&scanned),
        "the rewritten q30 answer must be bit-identical to the scanned answer"
    );
    assert_eq!(
        single_f64_bits(&scanned),
        Some((3208f64 / 11f64).to_bits()),
        "3208 / 11"
    );

    let clipped = &format!("SELECT AVG(status) FROM logs WHERE ts < {CLIP_SQL}");
    let (clipped_plan, clipped_rewrite) = corpus.run(clipped, true).await;
    let (_, clipped_scan) = corpus.run(clipped, false).await;
    assert!(
        clipped_plan.contains("LogsScanExec"),
        "a clipping ts bound must fall back to a scan; plan was:\n{clipped_plan}"
    );
    assert_eq!(
        single_f64_bits(&clipped_rewrite),
        single_f64_bits(&clipped_scan),
        "the clipped q30 answer must not depend on whether statistics loaded"
    );
    assert_ne!(
        single_f64_bits(&clipped_scan),
        single_f64_bits(&scanned),
        "the ts bound must actually remove rows"
    );
}

/// SUM and AVG over an all-null column are SQL NULL, byte-identical whether
/// answered from stats or by scanning. The metadata path answers NULL from
/// `non_null_count == 0` without reading an object; the scan produces the same
/// NULL. Empty-input semantics ride the same path (zero non-null rows).
#[tokio::test]
async fn sum_and_avg_over_all_null_is_null_byte_identical() {
    let store = CountingStore::new(Arc::new(MemoryStore::new()));
    let all_null: &[(i64, Option<i64>)] = &[(0, None), (100, None), (200, None)];
    let seg = write_real_segment(&store, "logs/allnull.rlog", 1, all_null).await;
    let stats = loaded_stats(vec![(
        &seg,
        stats_segment(&seg, vec![stat_from_rows(all_null)]),
    )]);

    for (sql, is_avg) in [
        ("SELECT SUM(status + 1) FROM logs", false),
        ("SELECT AVG(status) FROM logs", true),
    ] {
        // From stats: the rewrite fires and reads no object.
        let ctx = logs_session(provider(
            &store,
            snapshot_of(vec![seg.clone()], Vec::new()),
            status_col(),
            Some(Arc::clone(&stats)),
        ))
        .expect("session");
        let plan = ctx
            .sql(sql)
            .await
            .expect("plan")
            .create_physical_plan()
            .await
            .expect("physical plan");
        let shown = plan_str(&plan);
        assert!(
            !shown.contains("LogsScanExec"),
            "{sql} over all-null must be answered from stats; plan was:\n{shown}"
        );
        let before = store.gets();
        let rewritten = collect(Arc::clone(&plan), ctx.task_ctx())
            .await
            .expect("collect");
        assert_eq!(store.gets(), before, "the all-null answer reads no objects");

        // By scanning: no stats loaded.
        let scan_ctx = logs_session(provider(
            &store,
            snapshot_of(vec![seg.clone()], Vec::new()),
            status_col(),
            None,
        ))
        .expect("session");
        let scanned = collect(
            scan_ctx
                .sql(sql)
                .await
                .expect("plan")
                .create_physical_plan()
                .await
                .expect("physical plan"),
            scan_ctx.task_ctx(),
        )
        .await
        .expect("collect");

        if is_avg {
            assert_eq!(single_f64_bits(&rewritten), None, "AVG of all-null is NULL");
            assert_eq!(single_f64_bits(&rewritten), single_f64_bits(&scanned));
        } else {
            assert_eq!(single_i64(&rewritten), None, "SUM of all-null is NULL");
            assert_eq!(single_i64(&rewritten), single_i64(&scanned));
        }
    }
}
