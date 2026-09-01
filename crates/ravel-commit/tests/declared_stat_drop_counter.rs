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
    DeclaredColumnStat, DeclaredStatType, DeclaredStatValue, TYPED_ATTR_COLUMN_TYPE_F64,
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
