//! The ADR-0873 decision 4 conflict metric: when the `SegmentRef` stamp and
//! the ADR-0850 `.cstat` entry disagree about one segment and one column, the
//! column degrades to a scan AND the disagreement is counted and logged, so an
//! operator sees a ticket-shaped signal instead of a mysteriously slow query.
//!
//! The counter is observations, not distinct defective segments: one increment
//! per (column, conflicting segment, `partition_statistics` call), with no
//! dedup across calls. Each delta asserted here is therefore scoped to exactly
//! one `partition_statistics` call. The log line that carries the detail a
//! report is filed from is asserted in tests/declared_stat_conflict_log.rs,
//! which needs a process to itself.
//!
//! Own integration binary, all tests synchronous, all tests holding one lock:
//! `ravel_sql::declared_stat_carrier_conflicts` is a process-wide monotonic
//! tally, so an exact-delta assertion means something only when the test knows
//! every other reader in the process. Nothing here reads an object -- the
//! statistics path is pure plan-time work over the resolved snapshot -- so the
//! segments are fabricated and no store is needed.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use datafusion::common::stats::Precision;
use datafusion::scalar::ScalarValue;
use ravel_catalog::{
    DeclaredColumnStats, EntryIdentity, LoadedColumnStats, SegmentLevel, SegmentRef, Snapshot,
};
use ravel_commit::declared_stats::{encode as encode_stamp, read_commit_record};
use ravel_object_store::ObjectStoreBackend;
use ravel_object_store::memory::MemoryStore;
use ravel_proto::catalog::v1::{ColumnStat, ColumnStatsSegment, ColumnValue};
use ravel_proto::commit::v1::CommitRecord;
use ravel_query::LogSegmentFetcher;
use ravel_sql::{DeclaredColumn, DeclaredType, LogsTableProvider, declared_stat_carrier_conflicts};
use ravel_types::TenantHash;
use ravel_types::accounting::QueryAccounting;
use ravel_types::declared_stats::{DeclaredColumnStat, DeclaredStatType, DeclaredStatValue};
use uuid::Uuid;

const TENANT: TenantHash = TenantHash([7u8; 16]);
const COL: &str = "EventDate";
/// Rows per fabricated segment, and the figure the stamps' NULL counts are
/// reconciled against.
const SAMPLE_COUNT: u64 = 4;

/// Serialises every test here so a delta on the process-wide tally is
/// attributable to exactly the statistics resolution this test ran.
static CONFLICTS: Mutex<()> = Mutex::new(());

fn conflict_lock() -> MutexGuard<'static, ()> {
    match CONFLICTS.lock() {
        Ok(guard) => guard,
        // A poisoned lock means a sibling test panicked; the tally is still
        // sound to read, and re-reporting the sibling's failure here would
        // only obscure it.
        Err(poisoned) => poisoned.into_inner(),
    }
}

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

/// Stamp `seg` with one I64 triple, through the carrier read that binds the
/// row-count clauses (the only route to a non-empty [`DeclaredColumnStats`]).
fn stamped(seg: &SegmentRef, min: i64, max: i64, null_count: u64) -> SegmentRef {
    let stat = DeclaredColumnStat::new(
        COL,
        DeclaredStatType::I64,
        Some(DeclaredStatValue::I64(min)),
        Some(DeclaredStatValue::I64(max)),
        null_count,
    )
    .expect("valid stamp");
    let record = CommitRecord {
        sample_count: seg.sample_count,
        declared_column_stats: vec![encode_stamp(&stat)],
        ..CommitRecord::default()
    };
    let mut out = seg.clone();
    out.declared_column_stats = DeclaredColumnStats::from_validated(&read_commit_record(&record));
    assert_eq!(out.declared_column_stats.len(), 1, "fixture stamp is valid");
    out
}

fn i64_value(v: i64) -> ColumnValue {
    ColumnValue {
        kind: Some(ravel_proto::catalog::v1::column_value::Kind::I64(v)),
    }
}

/// The `.cstat` entry for the same column: `min`/`max` plus the row
/// accounting, with no value dictionary (this path reads neither).
fn cstat(min: i64, max: i64, null_count: u64) -> ColumnStat {
    ColumnStat {
        name: COL.to_string(),
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

fn loaded(seg: &SegmentRef, stat: ColumnStat) -> Arc<LoadedColumnStats> {
    let mut segments = HashMap::new();
    segments.insert(
        identity_of(seg),
        ColumnStatsSegment {
            ingest_hour_bucket: seg.ingest_hour_bucket,
            shard: seg.shard,
            writer_id: seg.writer_id.as_bytes().to_vec(),
            writer_epoch: seg.writer_epoch,
            writer_seq: seg.writer_seq,
            columns: vec![stat],
        },
    );
    Arc::new(LoadedColumnStats {
        segments,
        part_blake3: Vec::new(),
    })
}

/// The declared column's plan-time statistics over one stamped segment plus
/// one `.cstat` entry, and the exact movement of the conflict tally across
/// resolving them.
fn resolve(
    seg: SegmentRef,
    stats: Arc<LoadedColumnStats>,
) -> (datafusion::common::ColumnStatistics, u64) {
    let guard = conflict_lock();
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
    let before = declared_stat_carrier_conflicts();
    let resolved = plan
        .partition_statistics(None)
        .expect("partition_statistics");
    let delta = declared_stat_carrier_conflicts() - before;
    drop(guard);
    (
        resolved.column_statistics[ravel_sql::FIRST_DECLARED_COL].clone(),
        delta,
    )
}

/// A disagreement on `min` leaves the column `Absent` and moves the tally by
/// exactly one.
///
/// Prove-the-test: delete the `record_carrier_conflict(...)` call in
/// `declared_min_max_all` (crates/ravel-sql/src/logs_scan.rs) and the delta
/// reads 0; replace the `agrees_with` check with `true` and the precision
/// assertion fails as well.
#[test]
fn a_min_disagreement_declines_and_counts_once() {
    let seg = stamped(&seg_ref(1), 200, 500, 1);
    let (col, delta) = resolve(seg.clone(), loaded(&seg, cstat(-1, 500, 1)));
    assert_eq!(col.min_value, Precision::Absent);
    assert_eq!(col.max_value, Precision::Absent);
    assert_eq!(delta, 1, "one conflicted segment, counted once");
}

/// The same for a disagreement on `null_count` alone, with identical extrema:
/// the union compares all three fields, because a `COUNT(col)` answer rides on
/// the NULL count exactly as `MIN`/`MAX` ride on the extrema.
///
/// Prove-the-test: drop `self.null_count == other.null_count` from
/// `SegmentCoverage::agrees_with` and this reads `(Exact(200), 0)`.
#[test]
fn a_null_count_disagreement_declines_and_counts_once() {
    let seg = stamped(&seg_ref(2), 200, 500, 1);
    let (col, delta) = resolve(seg.clone(), loaded(&seg, cstat(200, 500, 2)));
    assert_eq!(col.min_value, Precision::Absent);
    assert_eq!(delta, 1);
}

/// Agreement costs nothing: the tally stays put and the column is exact, with
/// the NULL count reported once (never the sum of the two carriers).
#[test]
fn agreeing_carriers_move_nothing() {
    let seg = stamped(&seg_ref(3), 200, 500, 1);
    let (col, delta) = resolve(seg.clone(), loaded(&seg, cstat(200, 500, 1)));
    assert_eq!(
        col.min_value,
        Precision::Exact(ScalarValue::Int64(Some(200)))
    );
    assert_eq!(
        col.max_value,
        Precision::Exact(ScalarValue::Int64(Some(500)))
    );
    assert_eq!(
        col.null_count,
        Precision::Exact(1),
        "counted once, not twice"
    );
    assert_eq!(delta, 0, "no conflict, no increment");
}

/// A segment only one carrier covers is not a conflict: the union degenerating
/// to one carrier is the normal state (a live tail has stamps only, pre-stamp
/// sealed history `.cstat` only), so it must not touch the defect tally.
#[test]
fn one_carrier_alone_is_not_a_conflict() {
    // Stamp only: the `.cstat` object describes a different segment.
    let seg = stamped(&seg_ref(4), 200, 500, 1);
    let other = seg_ref(99);
    let (col, delta) = resolve(seg, loaded(&other, cstat(1, 2, 0)));
    assert_eq!(
        col.min_value,
        Precision::Exact(ScalarValue::Int64(Some(200)))
    );
    assert_eq!(delta, 0);

    // `.cstat` only: the segment carries no stamp at all.
    let bare = seg_ref(5);
    let (col, delta) = resolve(bare.clone(), loaded(&bare, cstat(200, 500, 1)));
    assert_eq!(
        col.min_value,
        Precision::Exact(ScalarValue::Int64(Some(200)))
    );
    assert_eq!(
        col.null_count,
        Precision::Absent,
        "a .cstat NULL count is not reconciled against the joined sample_count \
         on this path, so it is never reported as exact"
    );
    assert_eq!(delta, 0);
}
