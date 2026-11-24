//! The `.cstat` half of the ADR-0873 decision 2 defect metric: the `.cstat`
//! entry is read in `ravel_sql` (by `statistics_from_inputs` too, although there
//! it never answers, since it describes record-level cells rather than the
//! merged value SQL returns), so its refusals are reported to
//! `ravel_commit::declared_stats::declared_stat_drops_observed` under the
//! `cstat` label. One metric, four carrier labels; a defect in the `.cstat`
//! build would otherwise be the one carrier whose refusals nothing counts.
//!
//! The metric has observation semantics (one increment per READ of a defective
//! entry, no per-entry dedup), which is why every assertion here is a delta
//! across exactly one `statistics_from_inputs` call.
//!
//! Own integration binary, all tests synchronous, all tests holding one lock:
//! the tally is process-wide and monotonic, so an exact-delta assertion means
//! something only when the test knows every other reader in the process.
//! No test here reads a data object -- the statistics path is pure plan-time
//! work over the resolved snapshot -- so the segments are fabricated. The
//! `SqlExecutor` tests at the end read a catalog HEAD and a `.cstat` object
//! from a `MemoryStore`, since whether that load runs is what they measure.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use datafusion::physical_plan::{StatisticsArgs, StatisticsContext};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use datafusion::common::stats::Precision;
use ravel_catalog::{
    DeclaredColumnStats, EntryIdentity, LoadedColumnStats, SegmentLevel, SegmentRef, Snapshot,
};
use ravel_commit::declared_stats::{StatCarrier, declared_stat_drops_observed};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_proto::catalog::v1::{ColumnStat, ColumnStatsSegment, ColumnValue};
use ravel_query::{LogSegmentFetcher, PhaseAccounting};
use ravel_sql::{DeclaredColumn, DeclaredType, LogsTableProvider};
use ravel_types::TenantHash;
use uuid::Uuid;

const TENANT: TenantHash = TenantHash([7u8; 16]);
const COL: &str = "EventDate";
/// Rows per fabricated segment.
const SAMPLE_COUNT: u64 = 4;

/// Serialises every test here so a delta on the process-wide tally is
/// attributable to exactly the statistics resolution this test ran.
static DROPS: Mutex<()> = Mutex::new(());

fn drops_lock() -> MutexGuard<'static, ()> {
    match DROPS.lock() {
        Ok(guard) => guard,
        // A poisoned lock means a sibling test panicked; the tally is still
        // sound to read, and re-reporting the sibling's failure here would only
        // obscure it.
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// An unstamped segment: the `.cstat` object is then the only carrier, so every
/// refusal under test is this reader's own.
fn seg_ref(seq: u64) -> SegmentRef {
    SegmentRef {
        data_object_key: format!("logs/seg-{seq}.rlog"),
        object_size: 1,
        min_event_ts_ns: 0,
        max_event_ts_ns: 1_000,
        ingest_hour_bucket: 0,
        sample_count: SAMPLE_COUNT,
        series_count: 0,
        shard: 0,
        content_hash: [0u8; 32],
        writer_id: Uuid::from_u128(1),
        writer_epoch: 1,
        writer_seq: seq,
        created_unix_ns: 0,
        level: SegmentLevel::L0,
        segment_format_version: u32::from(ravel_logseg::footer::VERSION),
        declared_column_stats: DeclaredColumnStats::default(),
    }
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

/// A well-formed `.cstat` entry for `name`.
fn cstat(name: &str, min: i64, max: i64, null_count: u64) -> ColumnStat {
    ColumnStat {
        name: name.to_string(),
        declared_type: 2, // ravel.sys.v1.TypedAttrColumnType::I64
        non_null_count: SAMPLE_COUNT - null_count,
        null_count,
        min: Some(i64_value(min)),
        max: Some(i64_value(max)),
        dictionary_present: false,
        dictionary: Vec::new(),
        sum: None,
    }
}

fn loaded(seg: &SegmentRef, columns: Vec<ColumnStat>) -> Arc<LoadedColumnStats> {
    let mut segments = HashMap::new();
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
    Arc::new(LoadedColumnStats::from_maps(segments, HashMap::new()))
}

/// Every label's tally, in [`StatCarrier::ALL`] order.
fn observed_all() -> Vec<u64> {
    StatCarrier::ALL
        .iter()
        .map(|carrier| declared_stat_drops_observed(*carrier))
        .collect()
}

/// The declared column's plan-time statistics over one segment plus its
/// `.cstat` entries, the `cstat`-labelled drop delta across resolving them, and
/// the summed delta of every other label (which must stay zero: a `.cstat`
/// refusal is not a stamp-carrier drop).
fn resolve(
    seg: SegmentRef,
    columns: Vec<ColumnStat>,
) -> (datafusion::common::ColumnStatistics, u64, u64) {
    let guard = drops_lock();
    let stats = loaded(&seg, columns);
    let backend: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let provider = LogsTableProvider::new(
        Snapshot {
            segments: vec![seg],
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        },
        TENANT,
        LogSegmentFetcher::new(backend),
        PhaseAccounting::new(),
    )
    .with_declared_columns(vec![DeclaredColumn::new(COL, DeclaredType::I64)])
    .with_column_stats(Some(stats));
    let plan = provider.plan_filters(4, &[]).expect("plan_filters");
    let before = observed_all();
    let resolved = StatisticsContext::new()
        .compute(plan.as_ref(), &StatisticsArgs::new())
        .expect("statistics");
    let after = observed_all();
    drop(guard);

    let mut mine = 0;
    let mut others = 0;
    for ((label, b), a) in StatCarrier::ALL.iter().zip(before.iter()).zip(after.iter()) {
        let delta = a - b;
        if *label == StatCarrier::Cstat {
            mine = delta;
        } else {
            others += delta;
        }
    }
    (
        resolved.column_statistics[ravel_sql::FIRST_DECLARED_COL].clone(),
        mine,
        others,
    )
}

/// Two `.cstat` entries under one column name: no rule can pick between two
/// claims about one immutable object, so the column is uncovered AND the
/// refusal is counted once under `cstat`.
///
/// Prove-the-test: delete the `observe_declared_stat_drops(StatCarrier::Cstat,
/// 1)` call in the `unique_column_stat` `None` arm of `reconciled_column_stat`
/// (crates/ravel-sql/src/logs_scan.rs) and the delta reads 0 while the
/// `Absent` assertion still passes, which is exactly the state this finding
/// was filed about.
#[test]
fn a_duplicated_cstat_column_name_is_counted_once() {
    let seg = seg_ref(1);
    let (col, mine, others) = resolve(seg, vec![cstat(COL, 200, 500, 1), cstat(COL, 1, 2, 0)]);
    assert_eq!(col.min_value, Precision::Absent, "no coverage from either");
    assert_eq!(mine, 1, "one refused entry set, counted once");
    assert_eq!(others, 0, "and under the cstat label only");
}

/// An entry claiming extrema over a column with zero non-null rows, and its
/// mirror image (non-null rows with no recorded extremum): both are #970's
/// defect, both leave the column uncovered, and both are counted.
///
/// Prove-the-test: replace the `if validate_min_max_presence(stat).is_err()`
/// block in `entry_coverage` with `let _ = validate_min_max_presence(stat);`
/// and both deltas read 0.
#[test]
fn a_presence_contradiction_is_counted_once_in_either_direction() {
    // Extrema present, zero non-null rows.
    let mut fabricated = cstat(COL, 200, 500, SAMPLE_COUNT);
    fabricated.non_null_count = 0;
    let (col, mine, others) = resolve(seg_ref(2), vec![fabricated]);
    assert_eq!(col.min_value, Precision::Absent);
    assert_eq!((mine, others), (1, 0));

    // Non-null rows, no recorded extremum.
    let mut missing = cstat(COL, 200, 500, 0);
    missing.min = None;
    missing.max = None;
    let (col, mine, others) = resolve(seg_ref(3), vec![missing]);
    assert_eq!(col.min_value, Precision::Absent);
    assert_eq!((mine, others), (1, 0));
}

/// Absence is not a defect, in any of its three shapes: a valid entry, an entry
/// for a different column, and a segment the `.cstat` object does not cover all
/// leave the metric where it was. A counter that fired on absence would report
/// every tenant whose fold never built a column as a writer bug.
#[test]
fn absence_and_validity_leave_the_cstat_label_untouched() {
    let (col, mine, others) = resolve(seg_ref(4), vec![cstat(COL, 200, 500, 1)]);
    assert_eq!(
        col.min_value,
        Precision::Absent,
        "an entry with no stamp beside it answers nothing"
    );
    assert_eq!((mine, others), (0, 0), "a valid entry is not a defect");

    let (col, mine, others) = resolve(seg_ref(5), vec![cstat("SomethingElse", 1, 2, 0)]);
    assert_eq!(col.min_value, Precision::Absent);
    assert_eq!(
        (mine, others),
        (0, 0),
        "an uncovered column is the ordinary state, not a defect"
    );

    let (col, mine, others) = resolve(seg_ref(6), Vec::new());
    assert_eq!(col.min_value, Precision::Absent);
    assert_eq!((mine, others), (0, 0), "nor is an entryless segment");
}

/// A raw `ColumnStat`: unlike [`cstat`], it does not force
/// `non_null_count + null_count == SAMPLE_COUNT`, so the reconciliation half of
/// #1023 can be exercised with a deliberately unbalanced entry.
fn raw_stat(
    non_null_count: u64,
    null_count: u64,
    min: Option<i64>,
    max: Option<i64>,
) -> ColumnStat {
    ColumnStat {
        name: COL.to_string(),
        declared_type: 2, // ravel.sys.v1.TypedAttrColumnType::I64
        non_null_count,
        null_count,
        min: min.map(i64_value),
        max: max.map(i64_value),
        dictionary_present: false,
        dictionary: Vec::new(),
        sum: None,
    }
}

/// #1023 reconciliation: an entry whose row accounting disagrees with the
/// joined `SegmentRef::sample_count` grants NOTHING for the column -- the
/// extrema included, not only the unproven null count -- and is counted once
/// under `cstat`. Here `non_null_count(2) + null_count(1) = 3`, but the segment
/// carries `SAMPLE_COUNT = 4` rows, so the entry describes an object that does
/// not exist. Its extrema are internally consistent (non-null rows, both
/// present), so `validate_min_max_presence` passes and only the sample_count
/// reconciliation refuses it.
///
/// Prove-the-test: delete the `accounted != Some(seg.sample_count)` refusal
/// block in `reconciled_column_stat` (crates/ravel-sql/src/logs_scan.rs) and
/// the delta reads 0, failing `mine == 1`. The `Absent` assertions hold either
/// way, since a `.cstat` entry no longer answers MIN/MAX at all; the dictionary
/// and sum paths' reconciliation is pinned in `logs_scan`'s
/// `cstat_reconcile_tests`.
#[test]
fn a_row_accounting_mismatch_grants_nothing_and_is_counted() {
    let (col, mine, others) = resolve(seg_ref(7), vec![raw_stat(2, 1, Some(200), Some(500))]);
    assert_eq!(
        col.min_value,
        Precision::Absent,
        "an unreconciled entry grants no MIN"
    );
    assert_eq!(
        col.max_value,
        Precision::Absent,
        "nor a MAX: the whole entry is refused, not only its null count"
    );
    assert_eq!(mine, 1, "one unreconciled entry, counted once");
    assert_eq!(others, 0, "and under the cstat label only");
}

/// #1023 reconciliation, the checkpoint's stale-all-NULL case: an entry claiming
/// all-NULL (both extrema absent, `non_null_count == 0`) whose row accounting
/// cannot be all-null, because `0 + null_count(0)` does not reach the segment's
/// `SAMPLE_COUNT = 4` rows. Such an entry is internally consistent by the
/// presence rule (`validate_min_max_presence` accepts absent extrema with zero
/// non-null rows), so before reconciliation it was accepted as covering the
/// segment while contributing nothing: the column's MIN/MAX then read as an
/// exact NULL over a segment that actually holds four rows.
///
/// Prove-the-test: delete the same `accounted != Some(seg.sample_count)` block.
/// The entry is then accepted with the delta at 0, and the `mine == 1`
/// assertion fails. As above, the `Absent` assertions hold either way.
#[test]
fn a_stale_all_null_entry_grants_nothing_and_is_counted() {
    let (col, mine, others) = resolve(seg_ref(8), vec![raw_stat(0, 0, None, None)]);
    assert_eq!(
        col.min_value,
        Precision::Absent,
        "a stale all-NULL entry that cannot be all-null is refused, not accepted \
         as an exact NULL"
    );
    assert_eq!(col.max_value, Precision::Absent);
    assert_eq!(mine, 1, "one stale entry, counted once");
    assert_eq!(others, 0, "and under the cstat label only");
}

/// The `cstat`-labelled drop delta across planning and running `sql` over one
/// segment stamped with `stamp` plus its `.cstat` entries, the summed delta of
/// every other label, the physical plan, and the one `Int64` value answered.
fn statement(
    sql: &str,
    stamp: ravel_types::declared_stats::DeclaredColumnStat,
    columns: Vec<ColumnStat>,
) -> (u64, u64, String, i64) {
    let guard = drops_lock();
    let mut record = ravel_proto::commit::v1::CommitRecord {
        sample_count: SAMPLE_COUNT,
        ..Default::default()
    };
    ravel_commit::declared_stats::stamp_commit_record(&mut record, std::slice::from_ref(&stamp));
    let read = ravel_commit::declared_stats::read_commit_record(&record);
    assert_eq!(read.covered().len(), 1, "a valid stamp");
    let seg = SegmentRef {
        declared_column_stats: DeclaredColumnStats::from_validated(&read),
        ..seg_ref(10)
    };
    let stats = loaded(&seg, columns);
    let backend: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    let provider = LogsTableProvider::new(
        Snapshot {
            segments: vec![seg],
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        },
        TENANT,
        LogSegmentFetcher::new(backend),
        PhaseAccounting::new(),
    )
    .with_declared_columns(vec![DeclaredColumn::new(COL, DeclaredType::I64)])
    .with_column_stats(Some(stats));
    let config = ravel_sql::SqlConfig::default();
    let (pool, _breach) = config.query_pool(
        ravel_sql::TenantMemoryAccountant::new(1 << 30),
        ravel_types::accounting::QueryAccounting::new(),
    );
    let ctx = ravel_sql::build_session(
        &config,
        pool,
        ravel_sql::SessionTable::Logs(Arc::new(provider)),
        false,
        ravel_sql::SpillDecision::Disabled,
        &ravel_sql::SessionPrincipal::without_capabilities(TenantHash([0u8; 16])),
    )
    .expect("session");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let before = observed_all();
    let (plan, batches) = runtime.block_on(async {
        let plan = ctx
            .sql(sql)
            .await
            .expect("plan")
            .create_physical_plan()
            .await
            .expect("physical plan");
        let shown = datafusion::physical_plan::displayable(plan.as_ref())
            .indent(true)
            .to_string();
        let batches = datafusion::physical_plan::collect(plan, ctx.task_ctx())
            .await
            .expect("answer");
        (shown, batches)
    });
    let after = observed_all();
    drop(guard);
    let mut mine = 0;
    let mut others = 0;
    for ((label, b), a) in StatCarrier::ALL.iter().zip(before.iter()).zip(after.iter()) {
        if *label == StatCarrier::Cstat {
            mine = a - b;
        } else {
            others += a - b;
        }
    }
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 1);
    let value = batches
        .iter()
        .find(|b| b.num_rows() == 1)
        .expect("one row")
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .expect("Int64")
        .value(0);
    (mine, others, plan, value)
}

/// A stamp answer still reads the segment's loaded `.cstat` entry for the
/// drop tally. The stamp `[200, 500]`, NULL count 1, sum 900, zero count 0
/// answers `<> 0` (0 is outside the range) and `SUM` (field 6) with its own
/// figures; the entry beside it accounts for three of the segment's four rows.
/// The rewrite's read of the entry adds exactly one observation, the count
/// one read of one defective entry carries in every test above. The `SUM`
/// statement's planning also resolves the scan's statistics four times, each
/// reading the entry through `cstat_coverage`; the q02 statement's does not
/// reach them.
///
/// Prove-the-test: remove the `merged_view_entry` call on the stamp branch of
/// `resolve_segment_figure` and the deltas read 0 and 4.
#[test]
fn a_stamp_answer_still_counts_a_defective_entry() {
    let stamp = ravel_types::declared_stats::DeclaredColumnStat::new(
        COL,
        ravel_types::declared_stats::DeclaredStatType::I64,
        Some(ravel_types::declared_stats::DeclaredStatValue::I64(200)),
        Some(ravel_types::declared_stats::DeclaredStatValue::I64(500)),
        1,
    )
    .expect("valid stamp")
    .with_sum(900)
    .expect("sum")
    .with_zero_count(0)
    .expect("zero count");
    for (sql, expected, statistics_reads) in [
        (
            format!("SELECT COUNT(*) FROM logs WHERE \"{COL}\" <> 0"),
            3,
            0,
        ),
        (format!("SELECT SUM(\"{COL}\") FROM logs"), 900, 4),
    ] {
        let (mine, others, plan, value) = statement(
            &sql,
            stamp.clone(),
            vec![raw_stat(2, 1, Some(200), Some(500))],
        );
        assert!(
            plan.contains("MetadataOnlyExec") && !plan.contains("LogsScanExec"),
            "{sql}: answered from the stamp; plan was:\n{plan}"
        );
        assert_eq!(value, expected, "{sql}: the stamp's figure");
        assert_eq!(
            mine,
            statistics_reads + 1,
            "{sql}: the rewrite's one read of the defective entry is counted"
        );
        assert_eq!(others, 0, "{sql}: under the cstat label only");
    }
}

/// A reconciled, consistent entry is not counted: the [`cstat`] helper balances
/// `non_null_count + null_count` to `SAMPLE_COUNT`, so the reconciliation is a
/// no-op. The column stays `Absent` because the entry describes record-level
/// cells and never answers without a stamp. Pinned separately so a
/// reconciliation that rejected a VALID entry (an off-by-one, a wrong
/// comparand) would fail here on the drop count rather than pass silently.
#[test]
fn a_reconciled_entry_is_not_counted() {
    let (col, mine, others) = resolve(seg_ref(9), vec![cstat(COL, 200, 500, 1)]);
    assert_eq!(col.min_value, Precision::Absent, "no stamp, no answer");
    assert_eq!(
        (mine, others),
        (0, 0),
        "3 non-null + 1 null == 4 == sample_count, so the entry is not a defect"
    );
}

// ---------------------------------------------------------------------------
// Through `SqlExecutor`: a skipped load reads no `.cstat` entry (ADR-4628
// decision 5, issue #4685)
// ---------------------------------------------------------------------------

/// Segment `seq` carrying `stamp`; `seq` also keys its content hash, which
/// the v3 `.cstat` joins on.
fn stamped_seg(seq: u64, stamp: ravel_types::declared_stats::DeclaredColumnStat) -> SegmentRef {
    let mut record = ravel_proto::commit::v1::CommitRecord {
        sample_count: SAMPLE_COUNT,
        ..Default::default()
    };
    ravel_commit::declared_stats::stamp_commit_record(&mut record, std::slice::from_ref(&stamp));
    let read = ravel_commit::declared_stats::read_commit_record(&record);
    assert_eq!(read.covered().len(), 1, "a valid stamp");
    SegmentRef {
        content_hash: [u8::try_from(seq).expect("small"); 32],
        declared_column_stats: DeclaredColumnStats::from_validated(&read),
        ..seg_ref(seq)
    }
}

/// A `.cstat` object holding `segments` and a folded HEAD naming it, written
/// with the public codecs as `logs_stats_load.rs` writes them; no snapshot
/// part or data object is needed for a statement answered from statistics.
async fn install_head_and_stats(store: &dyn ObjectStoreBackend, segments: &[ColumnStatsSegment]) {
    use ravel_catalog::{
        DEFAULT_MAX_COLUMN_STATS_BYTES, HEAD_FORMAT_VERSION_1, encode_column_stats_v3,
    };
    use ravel_object_store::PutOptions;
    use ravel_proto::catalog::v1::{SnapshotColumnStatsPartRef, SnapshotHead, SnapshotPartRef};

    let signal_num = ravel_commit::signal::to_proto(ravel_types::Signal::Logs) as u32;
    let part_hash = *blake3::hash(b"part-0").as_bytes();
    let stats_bytes = encode_column_stats_v3(
        TENANT.0,
        signal_num,
        part_hash,
        segments,
        DEFAULT_MAX_COLUMN_STATS_BYTES,
    )
    .expect("encode column stats");
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
        format_version: HEAD_FORMAT_VERSION_1,
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
                blake3: blake3::hash(&stats_bytes).as_bytes().to_vec(),
                size: stats_bytes.len() as u64,
                segment_count: segments.len() as u32,
                part_blake3: vec![part_hash.to_vec()],
            }),
            key_index: Vec::new(),
            retired_hours: Vec::new(),
        }],
        folder_id: Uuid::new_v4().into_bytes().to_vec(),
        created_unix_ns: 0,
        postings: None,
        shard_generation_count: 1,
    };
    store
        .put(
            &format!("t/{}/catalog/l/HEAD", TENANT.to_hex()),
            bytes::Bytes::from(ravel_catalog::encode_head(&head).expect("encode head")),
            PutOptions::default(),
        )
        .await
        .expect("put head");
}

/// The `cstat`-labelled drop delta, the summed delta of every other label,
/// the plan and the one `Int64` answered, across planning and running `sql`
/// through `SqlExecutor::plan_pinned` over `segments` on a folded tenant
/// whose `.cstat` carries `entries`.
fn executor_statement(
    sql: &str,
    segments: Vec<SegmentRef>,
    entries: Vec<ColumnStatsSegment>,
) -> (u64, u64, String, i64) {
    use futures::StreamExt;

    let guard = drops_lock();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
    runtime.block_on(install_head_and_stats(store.as_ref(), &entries));
    let catalog = Arc::new(
        ravel_catalog::Catalog::new(Arc::clone(&store), ravel_catalog::CatalogConfig::default())
            .expect("catalog"),
    );
    let executor = ravel_sql::SqlExecutor::new(
        catalog,
        ravel_query::SegmentFetcher::new(Arc::clone(&store)),
        LogSegmentFetcher::new(Arc::clone(&store)),
        ravel_sql::SpanSegmentFetcher::new(Arc::clone(&store)),
        ravel_sql::SqlConfig::default(),
        1 << 30,
    );
    let declared = vec![DeclaredColumn::new(COL, DeclaredType::I64)];
    // The load runs in `plan_pinned` and `execute` optimizes the plan once, so
    // the delta spans one rewrite. The plan shown is a second planning of the
    // same statement, outside the delta.
    let (before, after, plan, batches) = runtime.block_on(async {
        let accounting = ravel_types::accounting::QueryAccounting::new();
        let snapshot = || Snapshot {
            segments: segments.clone(),
            segments_pruned: 0,
            pending_erasure: Vec::new(),
        };
        let before = observed_all();
        let pinned = executor
            .plan_pinned(TENANT, snapshot(), sql, &accounting, &declared)
            .await
            .expect("plan");
        let mut stream = pinned.execute().await.expect("execute");
        let mut batches = Vec::new();
        while let Some(next) = stream.next().await {
            batches.push(next.expect("batch"));
        }
        let after = observed_all();
        let shown = executor
            .plan_pinned(TENANT, snapshot(), sql, &accounting, &declared)
            .await
            .expect("plan")
            .create_physical_plan()
            .await
            .expect("physical plan");
        let shown = datafusion::physical_plan::displayable(shown.as_ref())
            .indent(true)
            .to_string();
        (before, after, shown, batches)
    });
    drop(guard);
    let mut mine = 0;
    let mut others = 0;
    for ((label, b), a) in StatCarrier::ALL.iter().zip(before.iter()).zip(after.iter()) {
        if *label == StatCarrier::Cstat {
            mine = a - b;
        } else {
            others += a - b;
        }
    }
    let value = batches
        .iter()
        .find(|b| b.num_rows() == 1)
        .expect("one row")
        .column(0)
        .as_any()
        .downcast_ref::<datafusion::arrow::array::Int64Array>()
        .expect("Int64")
        .value(0);
    (mine, others, plan, value)
}

/// A dictionary-carrying `.cstat` entry for values `{0: 2, 3: 1, 7: 1}`, the
/// four rows of segment 2, which reconciles with its stamp.
fn segment_2_entry() -> ColumnStat {
    ColumnStat {
        dictionary_present: true,
        dictionary: [(0, 2), (3, 1), (7, 1)]
            .into_iter()
            .map(|(v, count)| ravel_proto::catalog::v1::DictEntry {
                value: Some(i64_value(v)),
                count,
            })
            .collect(),
        sum: Some(10),
        ..cstat(COL, 0, 7, 0)
    }
}

/// A skipped load reads no `.cstat` entry, so a defective one beside a
/// stamp-covered segment adds nothing to the `cstat` drop tally; once one
/// segment lacks the figure the load runs and the same statement adds
/// exactly the one observation a read of one defective entry carries
/// (`a_stamp_answer_still_counts_a_defective_entry`).
///
/// Segment 1's stamp is `[200, 500]` with sum 900 and zero count 0, beside
/// the defective entry (2 + 1 rows of 4). Segment 2 holds `{0, 0, 3, 7}`; its
/// full stamp's zero count answers `<> 0`, its stripped stamp cannot (0 lies
/// inside `[0, 7]`), and then its reconciled entry answers. The answer is
/// 3 + 2 either way.
///
/// Prove-the-test: drop the skip (`(!stamps_answer).then_some(request)` made
/// `Some(request)`) and the first delta reads 1.
#[test]
fn a_skipped_load_adds_nothing_to_the_cstat_drop_tally() {
    use ravel_types::declared_stats::{DeclaredColumnStat, DeclaredStatType, DeclaredStatValue};
    let sql = format!("SELECT COUNT(*) FROM logs WHERE \"{COL}\" <> 0");
    let stamp = |min: i64, max: i64, null_count: u64| {
        DeclaredColumnStat::new(
            COL,
            DeclaredStatType::I64,
            Some(DeclaredStatValue::I64(min)),
            Some(DeclaredStatValue::I64(max)),
            null_count,
        )
        .expect("valid stamp")
    };
    let seg_1 = stamped_seg(
        21,
        stamp(200, 500, 1)
            .with_sum(900)
            .expect("sum")
            .with_zero_count(0)
            .expect("zero count"),
    );
    let seg_2_full = stamped_seg(
        22,
        stamp(0, 7, 0)
            .with_sum(10)
            .expect("sum")
            .with_zero_count(2)
            .expect("zero count"),
    );
    let seg_2_stripped = stamped_seg(22, stamp(0, 7, 0));
    let entries = |seg_2: &SegmentRef| {
        let entry = |seg: &SegmentRef, columns| ColumnStatsSegment {
            ingest_hour_bucket: seg.ingest_hour_bucket,
            shard: seg.shard,
            writer_id: seg.content_hash.to_vec(),
            writer_epoch: seg.writer_epoch,
            writer_seq: seg.writer_seq,
            columns,
        };
        vec![
            entry(&seg_1, vec![raw_stat(2, 1, Some(200), Some(500))]),
            entry(seg_2, vec![segment_2_entry()]),
        ]
    };

    let (mine, others, plan, value) = executor_statement(
        &sql,
        vec![seg_1.clone(), seg_2_full.clone()],
        entries(&seg_2_full),
    );
    assert!(
        plan.contains("stamp_segments=2, cstat_segments=0") && !plan.contains("LogsScanExec"),
        "every segment answers from its stamp; plan was:\n{plan}"
    );
    assert_eq!(value, 5, "3 + 2 from the stamps");
    assert_eq!(
        (mine, others),
        (0, 0),
        "a skipped load reads no entry, so the defective one is not observed"
    );

    let (mine, others, plan, value) = executor_statement(
        &sql,
        vec![seg_1.clone(), seg_2_stripped.clone()],
        entries(&seg_2_stripped),
    );
    assert!(
        plan.contains("stamp_segments=1, cstat_segments=1") && !plan.contains("LogsScanExec"),
        "segment 2 answers from its entry; plan was:\n{plan}"
    );
    assert_eq!(value, 5, "3 from the stamp, 2 from the entry");
    assert_eq!(
        (mine, others),
        (1, 0),
        "the kept load reads the defective entry once, under cstat only"
    );
}
