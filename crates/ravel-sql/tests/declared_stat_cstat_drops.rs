//! The `.cstat` half of the ADR-0873 decision 2 defect metric: the second
//! carrier of the declared-statistics union is read in `ravel_sql`, so its
//! refusals are reported to
//! `ravel_commit::declared_stats::declared_stat_drops_observed` under the
//! `cstat` label. One metric, four carrier labels; a defect in the `.cstat`
//! build would otherwise be the one carrier whose refusals nothing counts.
//!
//! The metric has observation semantics (one increment per READ of a defective
//! entry, no per-entry dedup), which is why every assertion here is a delta
//! across exactly one `partition_statistics` call.
//!
//! Own integration binary, all tests synchronous, all tests holding one lock:
//! the tally is process-wide and monotonic, so an exact-delta assertion means
//! something only when the test knows every other reader in the process.
//! Nothing here reads an object -- the statistics path is pure plan-time work
//! over the resolved snapshot -- so the segments are fabricated and no store is
//! needed.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use datafusion::common::stats::Precision;
use datafusion::scalar::ScalarValue;
use ravel_catalog::{
    DeclaredColumnStats, EntryIdentity, LoadedColumnStats, SegmentLevel, SegmentRef, Snapshot,
};
use ravel_commit::declared_stats::{StatCarrier, declared_stat_drops_observed};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_proto::catalog::v1::{ColumnStat, ColumnStatsSegment, ColumnValue};
use ravel_query::LogSegmentFetcher;
use ravel_sql::{DeclaredColumn, DeclaredType, LogsTableProvider};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
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
    Arc::new(LoadedColumnStats {
        segments,
        part_blake3: Vec::new(),
    })
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
        QueryAccounting::new(),
    )
    .with_declared_columns(vec![DeclaredColumn::new(COL, DeclaredType::I64)])
    .with_column_stats(Some(stats));
    let plan = provider.plan_filters(4, &[]).expect("plan_filters");
    let before = observed_all();
    let resolved = plan
        .partition_statistics(None)
        .expect("partition_statistics");
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
/// 1)` call in the `unique_column_stat` `None` arm of `cstat_coverage`
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
/// block in `cstat_coverage` with `let _ = validate_min_max_presence(stat);`
/// and both deltas read 0 while the extrema come from a record that describes
/// no live row.
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
        Precision::Exact(ScalarValue::Int64(Some(200))),
        "a valid entry still answers"
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
