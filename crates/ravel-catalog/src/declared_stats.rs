//! Declared-column min/max stamps on the catalog side (ADR-0873 decision 4):
//! `SnapshotEntry.declared_column_stats` (field 15) and the shared list a
//! resolved [`SegmentRef`](crate::SegmentRef) carries.
//!
//! Three rules govern this layer, all from the ADR:
//!
//! - **The fold copies validated stamps only.** A commit record's or
//!   compaction part's entries reach a `SnapshotEntry` through
//!   [`ravel_commit::declared_stats::read_commit_record`] /
//!   [`ravel_commit::declared_stats::read_compaction_part`], the wave-2 gated
//!   read whose row-count clauses bind against the source record's own
//!   `sample_count`. A defective entry is dropped at the fold and never
//!   reaches a sealed part, where it would outlive the record that carried it.
//! - **Absence is permanent and legal.** An empty list means the segment is
//!   uncovered for every declared column: every entry a pre-ADR-0873 fold
//!   wrote, every metrics/spans entry, and every entry folded from an
//!   unstamped record is in that state forever. Nothing here treats absence as
//!   an error.
//! - **Coverage is unconstructable without the predicate.** The only way to
//!   put a non-empty [`DeclaredColumnStats`] on a `SegmentRef` is
//!   [`DeclaredColumnStats::from_validated`], which takes a
//!   [`ValidatedDeclaredStats`] -- a type whose sole producers are the three
//!   carrier reads (the two in `ravel-commit`, plus [`read_snapshot_entry`]
//!   here, which supplies the entry's own row count). A caller holding raw
//!   decoded entries has no route to coverage.

use std::sync::Arc;

use ravel_commit::declared_stats::{self as commit_stats, ValidatedDeclaredStats};
use ravel_proto::catalog::v1::{
    DeclaredColumnMinMax, DeclaredColumnStatValue, SnapshotEntry, declared_column_stat_value::Kind,
};
use ravel_proto::commit::v1 as commit_pb;
use ravel_types::declared_stats::DeclaredColumnStat;

/// The declared-column stamps of one resolved segment (ADR-0873 decision 1),
/// shared rather than owned per ref.
///
/// A `SegmentRef` is cloned per query, per plan partition, and per fetch, so
/// the payload sits behind an `Arc` and a clone is a refcount bump. The
/// overwhelmingly common state is "no stamps at all" -- every pre-ADR-0873
/// record, every metrics or spans segment -- and that state holds no
/// allocation at all, which is why the empty case is `None` rather than an
/// empty `Arc<[_]>`: it is on the resolve path for every segment of every
/// query. Building one normalises an empty list to that same `None`, so two
/// uncovered refs always compare equal.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DeclaredColumnStats(Option<Arc<[DeclaredColumnStat]>>);

impl DeclaredColumnStats {
    /// The stamps a carrier read validated, shared for the refs built from it.
    ///
    /// Taking the validated form rather than a slice of entries is the point:
    /// possessing a [`ValidatedDeclaredStats`] is proof the full statistics
    /// validity predicate ran, row-count clauses included, and this is the only
    /// public constructor that yields a non-empty list (ADR-0873 decision 2,
    /// "where the predicate binds").
    pub fn from_validated(validated: &ValidatedDeclaredStats) -> Self {
        let covered = validated.covered();
        if covered.is_empty() {
            return DeclaredColumnStats(None);
        }
        DeclaredColumnStats(Some(Arc::from(covered)))
    }

    /// The covered columns, in the carrier's entry order. Empty when the
    /// segment is uncovered, which is a legal permanent state.
    pub fn as_slice(&self) -> &[DeclaredColumnStat] {
        match &self.0 {
            Some(stats) => stats,
            None => &[],
        }
    }

    /// Whether this segment is uncovered for every declared column.
    pub fn is_empty(&self) -> bool {
        self.as_slice().is_empty()
    }

    /// Number of covered columns.
    pub fn len(&self) -> usize {
        self.as_slice().len()
    }

    /// One covered column's statistics by name, or `None` when this segment is
    /// uncovered for it.
    pub fn column(&self, name: &str) -> Option<&DeclaredColumnStat> {
        self.as_slice().iter().find(|stat| stat.name() == name)
    }
}

fn value_to_proto(value: &commit_pb::DeclaredColumnStatValue) -> DeclaredColumnStatValue {
    let kind = value.kind.as_ref().map(|kind| match kind {
        commit_pb::declared_column_stat_value::Kind::I64(v) => Kind::I64(*v),
        commit_pb::declared_column_stat_value::Kind::B(b) => Kind::B(*b),
    });
    DeclaredColumnStatValue { kind }
}

fn value_to_commit(value: &DeclaredColumnStatValue) -> commit_pb::DeclaredColumnStatValue {
    let kind = value.kind.as_ref().map(|kind| match kind {
        Kind::I64(v) => commit_pb::declared_column_stat_value::Kind::I64(*v),
        Kind::B(b) => commit_pb::declared_column_stat_value::Kind::B(*b),
    });
    commit_pb::DeclaredColumnStatValue { kind }
}

/// One commit-side entry as its catalog-side mirror. Total and lossless: the
/// two messages are field-for-field identical by construction (ADR-0873
/// decision 1), including the value kinds and a present-but-empty value
/// message, which is a defect the reader must still see rather than one this
/// conversion may quietly normalise away.
fn entry_to_proto(entry: &commit_pb::DeclaredColumnMinMax) -> DeclaredColumnMinMax {
    DeclaredColumnMinMax {
        name: entry.name.clone(),
        declared_type: entry.declared_type,
        min: entry.min.as_ref().map(value_to_proto),
        max: entry.max.as_ref().map(value_to_proto),
        null_count: entry.null_count,
    }
}

/// The same conversion in the read direction.
fn entry_to_commit(entry: &DeclaredColumnMinMax) -> commit_pb::DeclaredColumnMinMax {
    commit_pb::DeclaredColumnMinMax {
        name: entry.name.clone(),
        declared_type: entry.declared_type,
        min: entry.min.as_ref().map(value_to_commit),
        max: entry.max.as_ref().map(value_to_commit),
        null_count: entry.null_count,
    }
}

/// Encode validated stamps for the catalog wire.
///
/// Takes the validated form, not a slice of [`DeclaredColumnStat`]: what the
/// fold writes into a sealed part is exactly what the source record's own
/// predicate pass admitted, and nothing else can be written by accident.
fn encode_validated(validated: &ValidatedDeclaredStats) -> Vec<DeclaredColumnMinMax> {
    validated
        .covered()
        .iter()
        .map(|stat| entry_to_proto(&commit_stats::encode(stat)))
        .collect()
}

/// The stamps a fold carries from one L0 commit record onto its
/// [`SnapshotEntry`] (ADR-0873 decision 4).
///
/// The record is read through the wave-2 gated path, so only entries that
/// passed the full predicate against the record's own `sample_count` (field
/// 11) are carried; the dropped ones simply leave their column uncovered for
/// this segment. Returns an empty list for an unstamped record, which is the
/// permanent state of every record written before ADR-0873.
pub(crate) fn carry_commit_record(record: &commit_pb::CommitRecord) -> Vec<DeclaredColumnMinMax> {
    encode_validated(&commit_stats::read_commit_record(record))
}

/// The same carriage for one compaction or erasure-rewrite output part, whose
/// row count is `CompactionPart.sample_count` (field 6).
pub(crate) fn carry_compaction_part(part: &commit_pb::CompactionPart) -> Vec<DeclaredColumnMinMax> {
    encode_validated(&commit_stats::read_compaction_part(part))
}

/// Read a snapshot entry's stamps, reconciled against the entry's own row
/// count (`sample_count`, field 11).
///
/// This is the third producer of a [`ValidatedDeclaredStats`], and it binds
/// the predicate exactly where the other two do: at a carrier that knows how
/// many rows the segment holds. A snapshot entry is a copy of a record's
/// stamps, so its entries get the same treatment as the originals rather than
/// being trusted because a fold once wrote them -- a part sealed by a fold
/// whose copy was wrong is still an untrusted object.
///
/// The predicate itself is not reimplemented here. The catalog-side messages
/// are a field-for-field mirror of the commit-side pair (ADR-0873 decision 1),
/// so the entries are converted to their twins and read through
/// [`ravel_commit::declared_stats::read_commit_record`] against the entry's row
/// count, which is the same quantity clauses 4 and 5 are stated over. A second
/// implementation of the predicate is a second thing to keep in agreement, and
/// the ADR's reader-agreement rule exists because that divergence has already
/// shipped once.
pub fn read_snapshot_entry(entry: &SnapshotEntry) -> ValidatedDeclaredStats {
    let twin = commit_pb::CommitRecord {
        sample_count: entry.sample_count,
        declared_column_stats: entry
            .declared_column_stats
            .iter()
            .map(entry_to_commit)
            .collect(),
        ..Default::default()
    };
    commit_stats::read_commit_record(&twin)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use ravel_commit::declared_stats::{DeclaredStatDefect, stamp_commit_record};
    use ravel_types::declared_stats::{
        DeclaredStatError, DeclaredStatType, DeclaredStatValue, TYPED_ATTR_COLUMN_TYPE_F64,
        TYPED_ATTR_COLUMN_TYPE_STR,
    };

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

    fn bool_stat(name: &str, min: bool, max: bool, null_count: u64) -> DeclaredColumnStat {
        DeclaredColumnStat::new(
            name,
            DeclaredStatType::Bool,
            Some(DeclaredStatValue::Bool(min)),
            Some(DeclaredStatValue::Bool(max)),
            null_count,
        )
        .expect("valid bool stat")
    }

    fn entry(sample_count: u64, stats: Vec<DeclaredColumnMinMax>) -> SnapshotEntry {
        SnapshotEntry {
            level: 0,
            shard: 0,
            ingest_hour_bucket: 500_000,
            writer_id: vec![0xAA; 16],
            writer_epoch: 1,
            writer_seq: 1,
            content_hash: vec![0xBB; 32],
            object_size: 4_096,
            min_event_ts_ns: 0,
            max_event_ts_ns: 100,
            sample_count,
            series_count: 1,
            segment_format_version: 4,
            created_unix_ns: 1_000,
            declared_column_stats: stats,
        }
    }

    fn commit_record(sample_count: u64, stats: &[DeclaredColumnStat]) -> commit_pb::CommitRecord {
        let mut record = commit_pb::CommitRecord {
            sample_count,
            ..Default::default()
        };
        stamp_commit_record(&mut record, stats);
        record
    }

    #[test]
    fn a_commit_records_validated_stamps_are_carried_verbatim() {
        let stats = vec![
            i64_stat("EventDate", -5, 19_000, 12),
            bool_stat("IsRefresh", false, true, 0),
        ];
        let carried = carry_commit_record(&commit_record(1_000, &stats));
        let read = read_snapshot_entry(&entry(1_000, carried));
        assert!(read.dropped().is_empty());
        assert_eq!(read.covered().to_vec(), stats);
    }

    #[test]
    fn a_compaction_parts_validated_stamps_are_carried_verbatim() {
        let stats = vec![i64_stat("EventDate", 3, 4, 1)];
        let mut part = commit_pb::CompactionPart {
            sample_count: 10,
            ..Default::default()
        };
        ravel_commit::declared_stats::stamp_compaction_part(&mut part, &stats);
        let read = read_snapshot_entry(&entry(10, carry_compaction_part(&part)));
        assert!(read.dropped().is_empty());
        assert_eq!(read.covered().to_vec(), stats);
    }

    #[test]
    fn an_unstamped_record_carries_an_empty_list_not_a_placeholder() {
        let carried = carry_commit_record(&commit_record(1_000, &[]));
        assert_eq!(carried.len(), 0);
        let read = read_snapshot_entry(&entry(1_000, carried));
        assert_eq!(read.covered().len(), 0);
        assert_eq!(read.dropped().len(), 0);
        assert_eq!(read.column("EventDate"), None);
        assert!(DeclaredColumnStats::from_validated(&read).is_empty());
    }

    #[test]
    fn a_stamp_that_fails_a_row_count_clause_is_not_carried() {
        // Eight NULLs in a seven-row object (clause 4) beside a valid entry.
        // The invalid one must not reach the entry at all: it would otherwise
        // outlive the record whose row count convicts it.
        let valid = i64_stat("EventDate", 1, 2, 6);
        let mut record = commit_record(7, std::slice::from_ref(&valid));
        record
            .declared_column_stats
            .push(commit_stats::encode(&i64_stat("Status", 200, 500, 8)));
        let carried = carry_commit_record(&record);
        assert_eq!(carried.len(), 1);
        assert_eq!(carried[0].name, "EventDate");
        let read = read_snapshot_entry(&entry(7, carried));
        assert_eq!(read.covered().to_vec(), vec![valid]);
        assert_eq!(read.column("Status"), None);
    }

    #[test]
    fn the_entrys_own_row_count_binds_clauses_four_and_five() {
        // The same entries, read against two row counts. The predicate is not
        // carried over from the fold: it re-binds against the entry's own
        // sample_count (field 11), so a copy whose row count disagrees with
        // its stamps is dropped here even though some earlier reader accepted
        // it.
        let stamps = vec![entry_to_proto(&commit_stats::encode(&i64_stat(
            "EventDate",
            1,
            2,
            6,
        )))];
        let honest = read_snapshot_entry(&entry(7, stamps.clone()));
        assert_eq!(honest.covered().len(), 1);
        assert!(honest.dropped().is_empty());

        let lying = read_snapshot_entry(&entry(6, stamps));
        assert_eq!(lying.covered().len(), 0);
        assert_eq!(lying.dropped().len(), 1);
        assert_eq!(
            lying.dropped()[0].reason,
            DeclaredStatDefect::PresenceDisagreesWithNullCount {
                name: "EventDate".to_string(),
                extrema: "present",
                null_count: 6,
                sample_count: 6,
            }
        );
        assert!(DeclaredColumnStats::from_validated(&lying).is_empty());
    }

    #[test]
    fn ineligible_and_malformed_entries_on_an_entry_are_dropped_per_entry() {
        let stats = vec![
            DeclaredColumnMinMax {
                name: "URL".to_string(),
                declared_type: TYPED_ATTR_COLUMN_TYPE_STR,
                min: None,
                max: None,
                null_count: 7,
            },
            DeclaredColumnMinMax {
                name: "Ratio".to_string(),
                declared_type: TYPED_ATTR_COLUMN_TYPE_F64,
                min: None,
                max: None,
                null_count: 7,
            },
            // Present value message with no kind set.
            DeclaredColumnMinMax {
                name: "Empty".to_string(),
                declared_type: DeclaredStatType::I64.tag(),
                min: Some(DeclaredColumnStatValue { kind: None }),
                max: Some(DeclaredColumnStatValue { kind: None }),
                null_count: 0,
            },
            entry_to_proto(&commit_stats::encode(&i64_stat("EventDate", 1, 2, 0))),
        ];
        let read = read_snapshot_entry(&entry(7, stats));
        assert_eq!(
            read.covered().to_vec(),
            vec![i64_stat("EventDate", 1, 2, 0)]
        );
        assert_eq!(read.dropped().len(), 3);
        assert_eq!(
            read.dropped()[0].reason,
            DeclaredStatDefect::Invalid(DeclaredStatError::IneligibleType {
                tag: TYPED_ATTR_COLUMN_TYPE_STR
            })
        );
        assert_eq!(
            read.dropped()[1].reason,
            DeclaredStatDefect::Invalid(DeclaredStatError::IneligibleType {
                tag: TYPED_ATTR_COLUMN_TYPE_F64
            })
        );
        assert_eq!(
            read.dropped()[2].reason,
            DeclaredStatDefect::EmptyValueKind {
                name: "Empty".to_string(),
                which: "min",
            }
        );
        let shared = DeclaredColumnStats::from_validated(&read);
        assert_eq!(shared.len(), 1);
        assert_eq!(shared.column("URL"), None);
        assert_eq!(shared.column("Ratio"), None);
        assert_eq!(
            shared.column("EventDate").map(|s| s.max()),
            Some(Some(DeclaredStatValue::I64(2)))
        );
    }

    #[test]
    fn an_empty_shared_list_holds_no_allocation_and_compares_equal() {
        let empty = DeclaredColumnStats::default();
        assert!(empty.is_empty());
        assert_eq!(empty.len(), 0);
        assert_eq!(empty.as_slice(), &[]);
        // Built from a read that covered nothing: the same value, so two
        // uncovered refs are equal whichever route produced them.
        let read = read_snapshot_entry(&entry(1_000, Vec::new()));
        assert_eq!(DeclaredColumnStats::from_validated(&read), empty);
    }

    #[test]
    fn cloning_the_shared_list_shares_one_allocation() {
        let stats = vec![i64_stat("EventDate", 1, 2, 0)];
        let read = read_snapshot_entry(&entry(7, carry_commit_record(&commit_record(7, &stats))));
        let shared = DeclaredColumnStats::from_validated(&read);
        let cloned = shared.clone();
        assert_eq!(shared, cloned);
        // A clone is a refcount bump, not a copy of the entries: the two lists
        // are the same allocation. `SegmentRef` is cloned per query, which is
        // why the field is shared at all.
        assert!(std::ptr::eq(shared.as_slice(), cloned.as_slice()));
    }

    fn arb_value() -> impl Strategy<Value = Option<DeclaredColumnStatValue>> {
        prop_oneof![
            Just(None),
            Just(Some(DeclaredColumnStatValue { kind: None })),
            any::<i64>().prop_map(|v| Some(DeclaredColumnStatValue {
                kind: Some(Kind::I64(v))
            })),
            any::<bool>().prop_map(|b| Some(DeclaredColumnStatValue {
                kind: Some(Kind::B(b))
            })),
        ]
    }

    fn arb_entry_message() -> impl Strategy<Value = DeclaredColumnMinMax> {
        (".{0,8}", 0u32..8, arb_value(), arb_value(), any::<u64>()).prop_map(
            |(name, declared_type, min, max, null_count)| DeclaredColumnMinMax {
                name,
                declared_type,
                min,
                max,
                null_count,
            },
        )
    }

    proptest! {
        /// The mirror conversion is lossless in both directions, for every
        /// shape the wire can hold including the defective ones. It has to be:
        /// the read path converts a catalog entry to its commit twin to run
        /// one shared predicate, so a conversion that normalised anything
        /// would make the two carriers disagree about the same bytes.
        #[test]
        fn mirror_conversion_round_trips(entry in arb_entry_message()) {
            let there_and_back = entry_to_proto(&entry_to_commit(&entry));
            prop_assert_eq!(there_and_back, entry);
        }

        /// Whatever the entries, the read is total: it never panics, every
        /// covered entry is self-consistent, and covered plus dropped accounts
        /// for every entry exactly once.
        #[test]
        fn reading_arbitrary_entries_never_panics(
            stats in prop::collection::vec(arb_entry_message(), 0..6),
            sample_count in any::<u64>(),
        ) {
            let count = stats.len();
            let read = read_snapshot_entry(&entry(sample_count, stats));
            prop_assert_eq!(read.covered().len() + read.dropped().len(), count);
            for stat in read.covered() {
                prop_assert!(!stat.name().is_empty());
                prop_assert!(stat.null_count() <= sample_count);
                match (stat.min(), stat.max()) {
                    (Some(min), Some(max)) => {
                        prop_assert_eq!(min.stat_type(), stat.declared_type());
                        prop_assert_eq!(max.stat_type(), stat.declared_type());
                        prop_assert!(stat.null_count() < sample_count);
                    }
                    (None, None) => prop_assert_eq!(stat.null_count(), sample_count),
                    _ => prop_assert!(false, "a one-sided pair is never covered"),
                }
            }
        }
    }
}
