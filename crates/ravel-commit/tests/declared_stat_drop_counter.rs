//! The ADR-0873 decision 2 defect metric: every stamp entry a reader drops is
//! counted, under the label of the carrier that was read, so a writer emitting
//! stamps no reader will spend is detectable exactly where coverage is spent
//! rather than showing up as a mysteriously slow query.
//!
//! The metric has OBSERVATION semantics, which the tests here pin as intended
//! behaviour rather than as an accident: each read of a defective carrier
//! increments it again, because a per-entry dedup would need unbounded state
//! keyed by (object, column) on a path walked once per segment per query.
//!
//! This lives in its own integration binary on purpose.
//! `ravel_commit::declared_stats::declared_stat_drops_observed` is a
//! process-wide monotonic tally, so an exact-delta assertion is only meaningful
//! when the test knows every other reader running in the same process. Here
//! that set is this file, and the lock below serialises it; in the crate's
//! `--lib` binary it would be every predicate test at once, racing.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use std::sync::{Mutex, MutexGuard};

use prost::Message;
use ravel_commit::declared_stats::{
    DeclaredStatDefect, StatCarrier, ValidatedDeclaredStats, declared_stat_drops_observed,
    declared_stat_drops_observed_total, encode, observe_declared_stat_drops, read_commit_record,
    read_compaction_part, read_snapshot_entry_twin, stamp_commit_record,
};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_proto::commit::v1::{
    CommitRecord, CompactionPart, DeclaredColumnMinMax, DeclaredColumnStatValue,
    declared_column_stat_value::Kind,
};
use ravel_types::declared_stats::{
    DeclaredColumnStat, DeclaredStatError, DeclaredStatType, DeclaredStatValue,
    TYPED_ATTR_COLUMN_TYPE_F64,
};
use ravel_types::{Signal, TenantHash};
use uuid::Uuid;

/// Serialises every test in this file, so a delta on the process-wide counter
/// is attributable to exactly the read this test performed.
static COUNTER: Mutex<()> = Mutex::new(());

fn counter_lock() -> MutexGuard<'static, ()> {
    match COUNTER.lock() {
        Ok(guard) => guard,
        // A poisoned lock means a sibling test panicked; the counter is still
        // sound to read, and reporting the sibling's failure twice would only
        // obscure it.
        Err(poisoned) => poisoned.into_inner(),
    }
}

/// Row count of every carrier here, and the figure clauses 4 and 5 bind
/// against.
const SAMPLE_COUNT: u64 = 100;

fn base_record() -> CommitRecord {
    record::build(NewCommitRecord {
        tenant_hash: TenantHash([0x11; 16]),
        signal: Signal::Logs,
        shard: 3,
        writer_id: Uuid::from_u128(7),
        writer_epoch: 100,
        writer_seq: 9,
        object_size: 4096,
        content_hash: [0x22; 32],
        sample_count: SAMPLE_COUNT,
        series_count: 4,
        min_event_ts_ns: 1_000,
        max_event_ts_ns: 2_000,
        min_ingest_ts_ns: 1_500,
        max_ingest_ts_ns: 2_500,
        segment_format_version: 4,
        created_unix_ns: 495_734 * 3_600_000_000_000,
        ingest_hour_bucket: 495_734,
    })
    .expect("valid record")
}

fn base_part() -> CompactionPart {
    CompactionPart {
        part_index: 0,
        first_series_id: vec![0x01; 16],
        last_series_id: vec![0x02; 16],
        content_hash: vec![0x03; 32],
        object_size: 8192,
        sample_count: SAMPLE_COUNT,
        series_count: 8,
        run_count: 1,
        min_event_ts_ns: 10,
        max_event_ts_ns: 20,
        segment_format_version: 3,
        declared_column_stats: vec![],
    }
}

fn i64_stat(name: &str, min: i64, max: i64, null_count: u64) -> DeclaredColumnStat {
    DeclaredColumnStat::new(
        name,
        DeclaredStatType::I64,
        Some(DeclaredStatValue::I64(min)),
        Some(DeclaredStatValue::I64(max)),
        null_count,
    )
    .expect("valid i64 stat")
}

/// A record carrying `entries`, round-tripped through the wire so every read
/// under test reads a decoded record.
fn decoded_record(entries: Vec<DeclaredColumnMinMax>) -> CommitRecord {
    let mut r = base_record();
    r.declared_column_stats = entries;
    record::decode(&record::encode(&r)).expect("record decodes")
}

fn decoded_part(entries: Vec<DeclaredColumnMinMax>) -> CompactionPart {
    let mut p = base_part();
    p.declared_column_stats = entries;
    CompactionPart::decode(p.encode_to_vec().as_slice()).expect("part decodes")
}

/// Every label's tally, in [`StatCarrier::ALL`] order.
fn observed_all() -> Vec<u64> {
    StatCarrier::ALL
        .iter()
        .map(|carrier| declared_stat_drops_observed(*carrier))
        .collect()
}

/// Run `read` and return its result, `carrier`'s observation delta across it,
/// and the summed delta of every OTHER label.
///
/// The second figure is the label assertion: a metric that counts the right
/// number of drops under the wrong carrier is as unusable as one that does not
/// count them, since the label is what names the writer to look at.
fn with_delta<T>(carrier: StatCarrier, read: impl FnOnce() -> T) -> (T, u64, u64) {
    let _guard = counter_lock();
    let before = observed_all();
    let out = read();
    let after = observed_all();
    let mut mine = 0;
    let mut others = 0;
    for ((label, b), a) in StatCarrier::ALL.iter().zip(before.iter()).zip(after.iter()) {
        let delta = a - b;
        if *label == carrier {
            mine = delta;
        } else {
            others += delta;
        }
    }
    (out, mine, others)
}

/// A stamp entry whose `declared_type` is ADR-0101's `F64` tag: representable
/// on the wire (a broken or future writer can emit one), never eligible
/// (ADR-0873 decision 2's allowlist refuses it by name).
fn f64_tagged_entry(name: &str) -> DeclaredColumnMinMax {
    DeclaredColumnMinMax {
        name: name.to_string(),
        declared_type: TYPED_ATTR_COLUMN_TYPE_F64,
        min: Some(DeclaredColumnStatValue {
            kind: Some(Kind::I64(1)),
        }),
        max: Some(DeclaredColumnStatValue {
            kind: Some(Kind::I64(9)),
        }),
        null_count: 0,
        sum: None,
        sum_wide: Vec::new(),
        zero_count: None,
    }
}

/// Test 5: a validation-failed stamp moves the metric exactly once per dropped
/// entry, on both commit-side read routes and under each route's OWN label, and
/// the valid entries beside it keep their coverage (the metric counts drops, not
/// records).
///
/// Prove-the-test: delete the `observe_declared_stat_drops(carrier,
/// dropped.len())` call in `decode_all_against_rows`
/// (crates/ravel-commit/src/declared_stats.rs) and both delta assertions read 0
/// instead of 2; pass `StatCarrier::CommitRecord` from `read_compaction_part`
/// and the part route's `others` assertion reads 2 with `mine` at 0.
#[test]
fn validation_failures_move_the_metric_once_per_dropped_entry() {
    let entries = vec![
        // Valid.
        encode(&i64_stat("EventDate", -5, 5, 1)),
        // Clause 5, both-present over an all-NULL column.
        encode(&i64_stat("Status", 200, 500, SAMPLE_COUNT)),
        // Clause 1, an ineligible declared type.
        f64_tagged_entry("Ratio"),
        // Valid.
        encode(&i64_stat("Latency", 1, 2, 0)),
    ];

    let (read, delta, others) = with_delta(StatCarrier::CommitRecord, || {
        read_commit_record(&decoded_record(entries.clone()))
    });
    assert_eq!(delta, 2, "exactly the two defective entries");
    assert_eq!(others, 0, "under the commit-record label only");
    assert_eq!(read.dropped().len(), 2);
    assert_eq!(read.covered().len(), 2);
    assert!(matches!(
        read.dropped()[0].reason,
        DeclaredStatDefect::PresenceDisagreesWithNullCount { .. }
    ));
    assert!(matches!(
        read.dropped()[1].reason,
        DeclaredStatDefect::Invalid(_)
    ));

    let (read, delta, others) = with_delta(StatCarrier::CompactionPart, || {
        read_compaction_part(&decoded_part(entries))
    });
    assert_eq!(delta, 2, "the part route counts the same two drops");
    assert_eq!(
        others, 0,
        "and counts them under compaction-part, not under the record label"
    );
    assert_eq!(read.covered().len(), 2);
}

/// The third stamp carrier's label: a snapshot entry read through
/// [`read_snapshot_entry_twin`] observes its drops under `snapshot-entry`, so a
/// defective fold copy is distinguishable from a defective record.
///
/// Prove-the-test: change `read_snapshot_entry_twin` to pass
/// `StatCarrier::CommitRecord` and `mine` reads 0 with `others` at 1.
#[test]
fn a_snapshot_entry_twin_observes_under_its_own_label() {
    let twin = decoded_record(vec![f64_tagged_entry("Ratio")]);
    let (read, delta, others) = with_delta(StatCarrier::SnapshotEntry, || {
        read_snapshot_entry_twin(&twin)
    });
    assert_eq!(delta, 1, "one ineligible entry on the fold's copy");
    assert_eq!(others, 0);
    assert!(read.covered().is_empty());
}

/// The `.cstat` label, which no reader in this crate produces: `ravel_sql`'s
/// `.cstat` reader reports its refusals through
/// [`observe_declared_stat_drops`], and they land under `cstat` and nowhere
/// else. The total is the sum over labels, so it moves with them.
#[test]
fn the_cstat_label_is_reportable_from_outside_this_crate() {
    // Hold the serialising lock across the WHOLE assertion: with_delta's
    // internal guard alone leaves windows before and after it in which a
    // parallel test in this binary can move the process-wide total.
    let _guard = counter_lock();
    let before_total = declared_stat_drops_observed_total();
    let before = observed_all();
    observe_declared_stat_drops(StatCarrier::Cstat, 3);
    let after = observed_all();
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
    assert_eq!(mine, 3);
    assert_eq!(others, 0, "a .cstat refusal is not a stamp-carrier drop");
    assert_eq!(
        declared_stat_drops_observed_total() - before_total,
        3,
        "the total sums the labels"
    );

    // Zero is not an event: a reader that refuses nothing must not move the
    // metric, or every clean read would look like a defect. Still under the
    // held guard above, so no parallel test can interleave (and calling
    // with_delta here would deadlock on the same lock).
    let zero_before = observed_all();
    observe_declared_stat_drops(StatCarrier::Cstat, 0);
    let zero_after = observed_all();
    assert_eq!(zero_before, zero_after, "a zero observation moves nothing");
}

/// The label set is complete and its strings are the ones ADR-0873 decision 2
/// names, since a dashboard query is written against the string and a renamed
/// label is a silently empty panel.
#[test]
fn every_carrier_has_its_adr_label() {
    let labels: Vec<&str> = StatCarrier::ALL.iter().map(|c| c.label()).collect();
    assert_eq!(
        labels,
        vec![
            "commit-record",
            "compaction-part",
            "snapshot-entry",
            "cstat"
        ]
    );
}

/// Test 4, metric half: a record stamping one name twice drops BOTH entries
/// (ADR-0873 clause 6), so the metric moves by exactly 2 -- once per dropped
/// entry, not once per name and not once for the loser only.
///
/// Prove-the-test: restore the first-wins arm (reserve the name with a
/// `HashSet` and drop only later occurrences) in `decode_all_against_rows` and
/// this reads 1 with `EventDate` covered.
#[test]
fn a_duplicated_name_moves_the_metric_by_both_entries() {
    for (a, b) in [(1i64, 2i64), (2, 1)] {
        let entries = vec![
            encode(&i64_stat("EventDate", a, a + 10, 0)),
            encode(&i64_stat("EventDate", b, b + 10, 0)),
            encode(&i64_stat("Latency", 7, 8, 0)),
        ];
        let (read, delta, others) = with_delta(StatCarrier::CommitRecord, || {
            read_commit_record(&decoded_record(entries))
        });
        assert_eq!(delta, 2, "both occurrences of the duplicated name");
        assert_eq!(others, 0);
        assert_eq!(
            read.column("EventDate"),
            None,
            "no coverage for a duplicate"
        );
        assert_eq!(read.covered().len(), 1, "only Latency survives");
    }
}

/// The metric is silent on the permanent, legal state: a record with no stamps
/// at all, and a record whose every stamp is valid, move it by zero. Absence
/// is not a defect (ADR-0873 decision 4), so a counter that fired on it would
/// report every pre-ADR-0873 record in the tenant as a writer bug.
#[test]
fn absence_and_full_validity_leave_the_metric_untouched() {
    let (read, delta, others) = with_delta(StatCarrier::CommitRecord, || {
        read_commit_record(&base_record())
    });
    assert_eq!(delta, 0, "an unstamped record is not a defect");
    assert_eq!(others, 0, "and moves no other label either");
    assert!(read.covered().is_empty());
    assert!(read.dropped().is_empty());

    let mut stamped = base_record();
    stamp_commit_record(&mut stamped, &[i64_stat("EventDate", 0, 19_000, 3)]);
    let stamped = record::decode(&record::encode(&stamped)).expect("decodes");
    let (read, delta, others) =
        with_delta(StatCarrier::CommitRecord, || read_commit_record(&stamped));
    assert_eq!(delta, 0, "a valid stamp is not a defect");
    assert_eq!(others, 0);
    assert_eq!(read.covered().len(), 1);
}

/// Observation semantics, asserted as the intended contract and not as an
/// incidental consequence: the metric counts one observation per READ of a
/// defective carrier, so reading the same defective record twice moves it twice.
///
/// This is what the name `declared_stat_drops_observed` states, and it is a
/// deliberate trade rather than a missing dedup. A "distinct defective entries"
/// figure would need unbounded state keyed by (object, column) held for the life
/// of the process, on a path walked once per segment per query; the figure it
/// would buy is only ever read as "nonzero, go look". The consequence a reader
/// of the metric must know is exactly what this test pins: the magnitude scales
/// with read rate, so it is comparable across equal windows and meaningless as
/// an absolute count of defects.
///
/// Prove-the-test: memoise the pass's drop count per record (or count only the
/// first read of a given `content_hash`) and the second delta reads 0.
#[test]
fn each_read_of_a_defective_record_is_its_own_observation() {
    let record = decoded_record(vec![f64_tagged_entry("Ratio")]);
    let (_, first, first_others) =
        with_delta(StatCarrier::CommitRecord, || read_commit_record(&record));
    let (_, second, second_others) =
        with_delta(StatCarrier::CommitRecord, || read_commit_record(&record));
    assert_eq!(
        (first, second),
        (1, 1),
        "one observation per read, by design: the second read spent and refused \
         the same entry again"
    );
    assert_eq!((first_others, second_others), (0, 0));
}

/// A `ValidatedDeclaredStats` is the coverage grant, and this file reads one
/// through both public producers; naming the type here keeps the import
/// honest about what the metric is attached to.
#[test]
fn both_read_routes_yield_the_validated_form() {
    let entries = vec![encode(&i64_stat("EventDate", 1, 2, 0))];
    let from_record: ValidatedDeclaredStats = read_commit_record(&decoded_record(entries.clone()));
    let from_part: ValidatedDeclaredStats = read_compaction_part(&decoded_part(entries));
    assert_eq!(from_record.covered(), from_part.covered());
}

// ADR-4628 decision 2: one corrupt-input case per validity clause of the sum
// (fields 6 and 7) and the zero count (field 8). Each asserts the entry is
// dropped whole with the clause's reason, the counter moves by exactly one
// under the label of the carrier read, and no other label moves.

/// Read `entry` alone through `carrier`'s read function on a carrier of
/// [`SAMPLE_COUNT`] rows, and assert it is dropped for `reason` and counted
/// once under that carrier.
fn assert_counted_drop(
    carrier: StatCarrier,
    entry: DeclaredColumnMinMax,
    reason: DeclaredStatDefect,
) {
    let name = entry.name.clone();
    let (read, delta, others) = with_delta(carrier, || match carrier {
        StatCarrier::CommitRecord => read_commit_record(&decoded_record(vec![entry])),
        StatCarrier::CompactionPart => read_compaction_part(&decoded_part(vec![entry])),
        StatCarrier::SnapshotEntry => read_snapshot_entry_twin(&decoded_record(vec![entry])),
        StatCarrier::Cstat => unreachable!("no stamp reader reads under cstat"),
    });
    assert!(read.covered().is_empty(), "covered: {:?}", read.covered());
    assert_eq!(read.dropped().len(), 1);
    assert_eq!(read.dropped()[0].name, name);
    assert_eq!(read.dropped()[0].reason, reason);
    assert_eq!(delta, 1, "one drop under {}", carrier.label());
    assert_eq!(others, 0, "no drop under any other label");
}

/// An I64 entry over [`SAMPLE_COUNT`] rows with `null_count` NULLs.
fn i64_entry(min: i64, max: i64, null_count: u64) -> DeclaredColumnMinMax {
    encode(&i64_stat("UserID", min, max, null_count))
}

fn bool_entry(min: bool, max: bool) -> DeclaredColumnMinMax {
    encode(
        &DeclaredColumnStat::new(
            "IsRefresh",
            DeclaredStatType::Bool,
            Some(DeclaredStatValue::Bool(min)),
            Some(DeclaredStatValue::Bool(max)),
            0,
        )
        .expect("valid bool stat"),
    )
}

fn wide(sum: i128) -> Vec<u8> {
    sum.to_le_bytes().to_vec()
}

/// Each field alone would be valid here: the narrow sum and the wide one
/// both lie inside `[100 * 1, 100 * i64::MAX]`. So a decoder that silently
/// preferred either field would cover the entry.
#[test]
fn sum_clause_both_sum_fields_present_is_a_counted_drop() {
    let mut entry = i64_entry(1, i64::MAX, 0);
    entry.sum = Some(1_000);
    entry.sum_wide = wide(i128::from(i64::MAX) + 1);
    let alone_narrow = DeclaredColumnMinMax {
        sum_wide: Vec::new(),
        ..entry.clone()
    };
    let alone_wide = DeclaredColumnMinMax {
        sum: None,
        ..entry.clone()
    };
    for alone in [alone_narrow, alone_wide] {
        let read = read_commit_record(&decoded_record(vec![alone]));
        assert_eq!(read.covered().len(), 1, "each field alone is valid");
    }
    assert_counted_drop(
        StatCarrier::CommitRecord,
        entry,
        DeclaredStatDefect::BothSumFields {
            name: "UserID".to_string(),
        },
    );
}

#[test]
fn sum_clause_sum_wide_length_other_than_16_is_a_counted_drop() {
    for len in [1usize, 8, 15, 17, 32] {
        let mut entry = i64_entry(1, i64::MAX, 0);
        entry.sum_wide = vec![0x7f; len];
        assert_counted_drop(
            StatCarrier::CompactionPart,
            entry,
            DeclaredStatDefect::SumWideLength {
                name: "UserID".to_string(),
                len,
            },
        );
    }
}

#[test]
fn sum_clause_sum_wide_holding_a_value_that_fits_i64_is_a_counted_drop() {
    for sum in [0, 100, i128::from(i64::MAX), i128::from(i64::MIN)] {
        let mut entry = i64_entry(i64::MIN, i64::MAX, 0);
        entry.sum_wide = wide(sum);
        assert_counted_drop(
            StatCarrier::SnapshotEntry,
            entry,
            DeclaredStatDefect::SumWideFitsI64 {
                name: "UserID".to_string(),
                sum,
            },
        );
    }
}

#[test]
fn sum_clause_sum_on_a_column_that_is_not_i64_is_a_counted_drop() {
    let mut narrow = bool_entry(false, true);
    narrow.sum = Some(1);
    let mut wide_sum = bool_entry(false, true);
    wide_sum.sum_wide = wide(i128::from(i64::MAX) + 1);
    for entry in [narrow, wide_sum] {
        assert_counted_drop(
            StatCarrier::CommitRecord,
            entry,
            DeclaredStatDefect::Invalid(DeclaredStatError::SumOnNonI64 {
                name: "IsRefresh".to_string(),
                declared: DeclaredStatType::Bool,
            }),
        );
    }
}

/// 90 non-null values in `[-3, 7]` sum to at least -270 and at most 630.
#[test]
fn sum_clause_sum_below_non_null_times_min_is_a_counted_drop() {
    let mut entry = i64_entry(-3, 7, 10);
    entry.sum = Some(-271);
    assert_counted_drop(
        StatCarrier::CompactionPart,
        entry,
        DeclaredStatDefect::SumOutsideBounds {
            name: "UserID".to_string(),
            sum: -271,
            non_null: 90,
            low: -270,
            high: 630,
        },
    );
    let mut edge = i64_entry(-3, 7, 10);
    edge.sum = Some(-270);
    assert_eq!(
        read_compaction_part(&decoded_part(vec![edge]))
            .covered()
            .len(),
        1,
        "the bound itself is valid"
    );
}

#[test]
fn sum_clause_sum_above_non_null_times_max_is_a_counted_drop() {
    let mut entry = i64_entry(-3, 7, 10);
    entry.sum = Some(631);
    assert_counted_drop(
        StatCarrier::SnapshotEntry,
        entry,
        DeclaredStatDefect::SumOutsideBounds {
            name: "UserID".to_string(),
            sum: 631,
            non_null: 90,
            low: -270,
            high: 630,
        },
    );
    // A wide sum past the bound of a column that spans the i64 range.
    let mut entry = i64_entry(i64::MIN, i64::MAX, 0);
    let high = 100 * i128::from(i64::MAX);
    entry.sum_wide = wide(high + 1);
    assert_counted_drop(
        StatCarrier::CommitRecord,
        entry,
        DeclaredStatDefect::SumOutsideBounds {
            name: "UserID".to_string(),
            sum: high + 1,
            non_null: 100,
            low: 100 * i128::from(i64::MIN),
            high,
        },
    );
}

#[test]
fn sum_clause_nonzero_sum_with_zero_non_null_rows_is_a_counted_drop() {
    let mut entry = encode(
        &DeclaredColumnStat::new("UserID", DeclaredStatType::I64, None, None, SAMPLE_COUNT)
            .expect("all-null stat"),
    );
    entry.sum = Some(5);
    assert_counted_drop(
        StatCarrier::CommitRecord,
        entry,
        DeclaredStatDefect::Invalid(DeclaredStatError::NonzeroSumWithoutValues {
            name: "UserID".to_string(),
            sum: 5,
        }),
    );
}

#[test]
fn zero_count_clause_zero_count_above_the_non_null_count_is_a_counted_drop() {
    let mut entry = i64_entry(-3, 7, 10);
    entry.zero_count = Some(91);
    assert_counted_drop(
        StatCarrier::CompactionPart,
        entry,
        DeclaredStatDefect::ZeroCountAboveNonNull {
            name: "UserID".to_string(),
            zero_count: 91,
            non_null: 90,
        },
    );
}

#[test]
fn zero_count_clause_nonzero_zero_count_with_zero_outside_min_max_is_a_counted_drop() {
    let mut above = i64_entry(1, 7, 0);
    above.zero_count = Some(1);
    let mut below = i64_entry(-7, -1, 0);
    below.zero_count = Some(1);
    for entry in [above, below] {
        assert_counted_drop(
            StatCarrier::SnapshotEntry,
            entry,
            DeclaredStatDefect::Invalid(DeclaredStatError::ZeroCountOutsideRange {
                name: "UserID".to_string(),
                zero_count: 1,
            }),
        );
    }
    let mut all_true = bool_entry(true, true);
    all_true.zero_count = Some(1);
    assert_counted_drop(
        StatCarrier::CommitRecord,
        all_true,
        DeclaredStatDefect::Invalid(DeclaredStatError::ZeroCountOutsideRange {
            name: "IsRefresh".to_string(),
            zero_count: 1,
        }),
    );
}

#[test]
fn zero_count_clause_zero_count_below_non_null_when_min_max_are_zero_is_a_counted_drop() {
    let mut entry = i64_entry(0, 0, 10);
    entry.zero_count = Some(89);
    assert_counted_drop(
        StatCarrier::CommitRecord,
        entry,
        DeclaredStatDefect::ZeroCountBelowNonNullAllZero {
            name: "UserID".to_string(),
            zero_count: 89,
            non_null: 90,
        },
    );
    let mut all_false = bool_entry(false, false);
    all_false.zero_count = Some(99);
    assert_counted_drop(
        StatCarrier::CompactionPart,
        all_false,
        DeclaredStatDefect::ZeroCountBelowNonNullAllZero {
            name: "IsRefresh".to_string(),
            zero_count: 99,
            non_null: 100,
        },
    );
}

/// Read `entry` alone through `carrier`'s read function on a carrier of
/// [`SAMPLE_COUNT`] rows, and assert it is kept with its zero count and
/// nothing is counted under any label.
fn assert_kept(carrier: StatCarrier, entry: DeclaredColumnMinMax) {
    let zero_count = entry.zero_count;
    let (read, delta, others) = with_delta(carrier, || match carrier {
        StatCarrier::CommitRecord => read_commit_record(&decoded_record(vec![entry])),
        StatCarrier::CompactionPart => read_compaction_part(&decoded_part(vec![entry])),
        StatCarrier::SnapshotEntry => read_snapshot_entry_twin(&decoded_record(vec![entry])),
        StatCarrier::Cstat => unreachable!("no stamp reader reads under cstat"),
    });
    assert!(read.dropped().is_empty(), "dropped: {:?}", read.dropped());
    assert_eq!(read.covered().len(), 1);
    assert_eq!(read.covered()[0].zero_count(), zero_count);
    assert_eq!((delta, others), (0, 0), "nothing counted");
}

/// Issue #4672 clause A: an extremum is a value some row holds, so a zero
/// count of 0 beside a min or max of 0 (or false) over non-null rows cannot
/// describe its object.
#[test]
fn zero_count_clause_zero_count_of_zero_beside_a_zero_extremum_is_a_counted_drop() {
    for (min, max, carrier) in [
        (0, 5, StatCarrier::CommitRecord),
        (-5, 0, StatCarrier::CompactionPart),
        (0, 5, StatCarrier::SnapshotEntry),
    ] {
        let mut entry = i64_entry(min, max, 10);
        entry.zero_count = Some(0);
        assert_counted_drop(
            carrier,
            entry,
            DeclaredStatDefect::ZeroCountZeroBesideZeroExtremum {
                name: "UserID".to_string(),
                non_null: 90,
            },
        );
    }
    for carrier in [StatCarrier::CompactionPart, StatCarrier::SnapshotEntry] {
        let mut entry = bool_entry(false, true);
        entry.zero_count = Some(0);
        assert_counted_drop(
            carrier,
            entry,
            DeclaredStatDefect::ZeroCountZeroBesideZeroExtremum {
                name: "IsRefresh".to_string(),
                non_null: 100,
            },
        );
    }
}

/// Issue #4672 clause B: a zero count equal to the non-null count says
/// every non-null row is the zero, so a min or max that is not the zero
/// contradicts it.
#[test]
fn zero_count_clause_zero_count_equal_to_non_null_beside_a_nonzero_extremum_is_a_counted_drop() {
    for (min, max, carrier) in [
        (0, 5, StatCarrier::CommitRecord),
        (-3, 4, StatCarrier::CompactionPart),
        (-3, 4, StatCarrier::SnapshotEntry),
    ] {
        let mut entry = i64_entry(min, max, 10);
        entry.zero_count = Some(90);
        assert_counted_drop(
            carrier,
            entry,
            DeclaredStatDefect::ZeroCountAllBesideNonZeroExtremum {
                name: "UserID".to_string(),
                zero_count: 90,
                non_null: 90,
            },
        );
    }
    for carrier in [StatCarrier::CommitRecord, StatCarrier::SnapshotEntry] {
        let mut entry = bool_entry(false, true);
        entry.zero_count = Some(100);
        assert_counted_drop(
            carrier,
            entry,
            DeclaredStatDefect::ZeroCountAllBesideNonZeroExtremum {
                name: "IsRefresh".to_string(),
                zero_count: 100,
                non_null: 100,
            },
        );
    }
}

/// With the zero outside `[1, 5]`, a zero count equal to the non-null count
/// is refused by the entry-local check in `with_zero_count`, which decode
/// runs before the clauses that compare with the non-null count.
#[test]
fn zero_count_clause_zero_count_equal_to_non_null_with_zero_outside_the_range_is_out_of_range() {
    let mut entry = i64_entry(1, 5, 10);
    entry.zero_count = Some(90);
    assert_counted_drop(
        StatCarrier::CommitRecord,
        entry,
        DeclaredStatDefect::Invalid(DeclaredStatError::ZeroCountOutsideRange {
            name: "UserID".to_string(),
            zero_count: 90,
        }),
    );
}

/// The boundaries of clauses A and B: each of these stamps can describe its
/// object, so every carrier keeps it with its zero count and counts nothing.
#[test]
fn zero_count_clause_boundaries_keep_the_stamp() {
    // Zero inside the range, neither extremum the zero.
    let mut inside = i64_entry(-5, 5, 10);
    inside.zero_count = Some(0);
    // Every non-null row the zero.
    let mut all_zero = i64_entry(0, 0, 10);
    all_zero.zero_count = Some(90);
    // One zero row among 90 non-null, with the minimum at 0.
    let mut one_zero = i64_entry(0, 5, 10);
    one_zero.zero_count = Some(1);
    // No non-null row, zero count 0.
    let mut all_null = encode(
        &DeclaredColumnStat::new("UserID", DeclaredStatType::I64, None, None, SAMPLE_COUNT)
            .expect("all-null stat"),
    );
    all_null.zero_count = Some(0);
    // No zero count beside a zero minimum.
    let absent = i64_entry(0, 5, 10);
    // BOOL: one false row among 100 non-null rows of both values.
    let mut bool_one = bool_entry(false, true);
    bool_one.zero_count = Some(1);
    for entry in [inside, all_zero, one_zero, all_null, absent, bool_one] {
        for carrier in [
            StatCarrier::CommitRecord,
            StatCarrier::CompactionPart,
            StatCarrier::SnapshotEntry,
        ] {
            assert_kept(carrier, entry.clone());
        }
    }
}
