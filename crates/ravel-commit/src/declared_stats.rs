//! Per-declared-column exact statistics on the commit-family wire records
//! (ADR-0873 decision 1): `CommitRecord.declared_column_stats` (field 20) and
//! `CompactionPart.declared_column_stats` (field 12).
//!
//! The typed vocabulary and the eligibility allowlist live in
//! [`ravel_types::declared_stats`]; this module is only the wire layer over
//! it: a [`DeclaredColumnStat`] in, a `DeclaredColumnMinMax` out, and back.
//!
//! Three rules govern the read direction, and all three come straight from
//! the ADR:
//!
//! - **Absence is permanent and legal.** An empty list means the record's
//!   object is uncovered for every declared column. Every record written
//!   before ADR-0873 is in that state forever (commit records are immutable
//!   and are never rewritten, ADR-0873 decision 5, migration class C), as is
//!   every metrics/spans record and any column declared after its flush
//!   opened. Nothing here treats absence as an error, a fallback, or an event
//!   worth logging.
//! - **A defective entry is dropped, never trusted, and never fatal.** An
//!   ineligible `declared_type`, a value kind that disagrees with it, a
//!   half-present min/max pair, a min above its max, an empty name, or a
//!   duplicate column all leave that one column uncovered for that segment
//!   and are counted for the caller's defect metric. The record itself still
//!   decodes: a bad statistic must not make a durable commit record
//!   unreadable.
//! - **The stamp side cannot express an ineligible type at all.** Encoding
//!   takes only [`DeclaredColumnStat`]s, whose constructor is the typed
//!   boundary that refuses ineligible types (ADR-0873 decision 2).

use std::collections::HashSet;

use ravel_proto::commit::v1::{
    CommitRecord, CompactionPart, DeclaredColumnMinMax, DeclaredColumnStatValue,
    declared_column_stat_value::Kind,
};
use ravel_types::declared_stats::{
    DeclaredColumnStat, DeclaredStatError, DeclaredStatType, DeclaredStatValue,
};

/// Why one `DeclaredColumnMinMax` entry was dropped on decode.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum DeclaredStatDefect {
    /// The entry violates the typed contract in [`ravel_types::declared_stats`].
    #[error(transparent)]
    Invalid(#[from] DeclaredStatError),
    /// A `DeclaredColumnStatValue` message is present but sets no oneof arm.
    /// Distinct from an absent message, which is the legal "zero non-null
    /// values" statement: a present-but-empty value names no extremum at all.
    #[error("declared column {name:?}: {which} value message is present but sets no kind")]
    EmptyValueKind { name: String, which: &'static str },
    /// Two entries name the same column. Which one wins is undefined, so
    /// neither is trusted beyond the first.
    #[error("duplicate entry for declared column {0:?}")]
    DuplicateName(String),
}

/// One dropped entry, with its position in the record's list so a defect
/// counter can be attributed without re-deriving it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DroppedStatEntry {
    /// Index of the offending entry in the record's `declared_column_stats`.
    pub index: usize,
    /// The entry's `name` as stored, which may be empty or a duplicate.
    pub name: String,
    /// Why it was dropped.
    pub reason: DeclaredStatDefect,
}

/// The result of reading a record's declared-column statistics: the entries
/// that are trustworthy, and the ones that were dropped.
///
/// `covered` empty with `dropped` empty is the normal permanent state of an
/// unstamped record, not a failure.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct DecodedDeclaredStats {
    /// Entries that passed validation, in record order.
    pub covered: Vec<DeclaredColumnStat>,
    /// Entries that were dropped; each leaves its column uncovered for this
    /// segment. Feed the length to the defect metric (ADR-0873 decision 2).
    pub dropped: Vec<DroppedStatEntry>,
}

impl DecodedDeclaredStats {
    /// Look up one declared column's statistics by name.
    pub fn column(&self, name: &str) -> Option<&DeclaredColumnStat> {
        self.covered.iter().find(|stat| stat.name() == name)
    }
}

fn value_to_proto(value: DeclaredStatValue) -> DeclaredColumnStatValue {
    let kind = match value {
        DeclaredStatValue::I64(v) => Kind::I64(v),
        DeclaredStatValue::Bool(b) => Kind::B(b),
    };
    DeclaredColumnStatValue { kind: Some(kind) }
}

fn value_from_proto(value: &DeclaredColumnStatValue) -> Option<DeclaredStatValue> {
    match value.kind {
        Some(Kind::I64(v)) => Some(DeclaredStatValue::I64(v)),
        Some(Kind::B(b)) => Some(DeclaredStatValue::Bool(b)),
        None => None,
    }
}

/// Encode one typed stat for the wire. Infallible: a [`DeclaredColumnStat`]
/// cannot hold an ineligible type or an inconsistent pair.
pub fn encode(stat: &DeclaredColumnStat) -> DeclaredColumnMinMax {
    DeclaredColumnMinMax {
        name: stat.name().to_string(),
        declared_type: stat.declared_type().tag(),
        min: stat.min().map(value_to_proto),
        max: stat.max().map(value_to_proto),
        null_count: stat.null_count(),
    }
}

/// Encode a whole stamp list for the wire.
pub fn encode_all(stats: &[DeclaredColumnStat]) -> Vec<DeclaredColumnMinMax> {
    stats.iter().map(encode).collect()
}

/// Decode one entry, or say why it cannot be trusted.
pub fn decode(entry: &DeclaredColumnMinMax) -> Result<DeclaredColumnStat, DeclaredStatDefect> {
    let declared_type = DeclaredStatType::from_tag(entry.declared_type)?;
    let extremum = |value: &Option<DeclaredColumnStatValue>,
                    which: &'static str|
     -> Result<Option<DeclaredStatValue>, DeclaredStatDefect> {
        match value {
            None => Ok(None),
            Some(value) => value_from_proto(value).map(Some).ok_or_else(|| {
                DeclaredStatDefect::EmptyValueKind {
                    name: entry.name.clone(),
                    which,
                }
            }),
        }
    };
    let min = extremum(&entry.min, "min")?;
    let max = extremum(&entry.max, "max")?;
    DeclaredColumnStat::new(
        entry.name.clone(),
        declared_type,
        min,
        max,
        entry.null_count,
    )
    .map_err(DeclaredStatDefect::from)
}

/// Decode a record's stamp list, dropping (and reporting) every entry that
/// cannot be trusted. Never fails: a defective statistic leaves its column
/// uncovered, it does not make the record unreadable.
///
/// Never fails on entry count either: the list is bounded by the bytes prost
/// already decoded and allocated, so the pass below is linear in work that has
/// already been paid for. A count cap would have to refuse a durable,
/// immutable record whose declared-column count nothing in the tenant-config
/// path bounds, which is the one outcome this module exists to avoid.
pub fn decode_all(entries: &[DeclaredColumnMinMax]) -> DecodedDeclaredStats {
    let mut out = DecodedDeclaredStats::default();
    // Names, not decoded stats: the name is stored verbatim, and borrowing
    // from `entries` keeps the set independent of the pushes into `out`.
    let mut seen: HashSet<&str> = HashSet::with_capacity(entries.len());
    for (index, entry) in entries.iter().enumerate() {
        match decode(entry) {
            Ok(stat) => {
                if !seen.insert(entry.name.as_str()) {
                    out.dropped.push(DroppedStatEntry {
                        index,
                        name: entry.name.clone(),
                        reason: DeclaredStatDefect::DuplicateName(entry.name.clone()),
                    });
                } else {
                    out.covered.push(stat);
                }
            }
            Err(reason) => out.dropped.push(DroppedStatEntry {
                index,
                name: entry.name.clone(),
                reason,
            }),
        }
    }
    out
}

/// Stamp a commit record's declared-column statistics (field 20), replacing
/// whatever the in-memory record carried. Only ever applied to a record being
/// built: a published commit record is immutable.
pub fn stamp_commit_record(record: &mut CommitRecord, stats: &[DeclaredColumnStat]) {
    record.declared_column_stats = encode_all(stats);
}

/// Read a commit record's declared-column statistics.
pub fn read_commit_record(record: &CommitRecord) -> DecodedDeclaredStats {
    decode_all(&record.declared_column_stats)
}

/// Stamp one compaction/rewrite output part's declared-column statistics
/// (field 12). The caller recomputes these over the rows the part holds and
/// never copies an input's stamp (ADR-0873 decision 3).
pub fn stamp_compaction_part(part: &mut CompactionPart, stats: &[DeclaredColumnStat]) {
    part.declared_column_stats = encode_all(stats);
}

/// Read one compaction/rewrite output part's declared-column statistics.
pub fn read_compaction_part(part: &CompactionPart) -> DecodedDeclaredStats {
    decode_all(&part.declared_column_stats)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use prost::Message;
    use ravel_types::declared_stats::{
        TYPED_ATTR_COLUMN_TYPE_BYTES, TYPED_ATTR_COLUMN_TYPE_F64, TYPED_ATTR_COLUMN_TYPE_STR,
        TYPED_ATTR_COLUMN_TYPE_UNSPECIFIED,
    };
    use ravel_types::{Signal, TenantHash};
    use uuid::Uuid;

    use crate::record::{self, NewCommitRecord};

    /// A `CommitRecord` as a writer that predates ADR-0873 encodes it: the
    /// same field numbers 1-19 and no field 20 in the schema at all, so its
    /// encoded bytes cannot contain the new field even accidentally.
    #[derive(Clone, PartialEq, prost::Message)]
    struct LegacyCommitRecord {
        #[prost(uint32, tag = "1")]
        format_version: u32,
        #[prost(bytes = "vec", tag = "2")]
        tenant_hash: Vec<u8>,
        #[prost(int32, tag = "3")]
        signal: i32,
        #[prost(uint32, tag = "4")]
        shard: u32,
        #[prost(string, tag = "5")]
        writer_id: String,
        #[prost(uint64, tag = "6")]
        writer_epoch: u64,
        #[prost(uint64, tag = "7")]
        writer_seq: u64,
        #[prost(string, tag = "8")]
        object_key: String,
        #[prost(uint64, tag = "9")]
        object_size: u64,
        #[prost(bytes = "vec", tag = "10")]
        content_hash: Vec<u8>,
        #[prost(uint64, tag = "11")]
        sample_count: u64,
        #[prost(uint64, tag = "12")]
        series_count: u64,
        #[prost(sfixed64, tag = "13")]
        min_event_ts_ns: i64,
        #[prost(sfixed64, tag = "14")]
        max_event_ts_ns: i64,
        #[prost(sfixed64, tag = "15")]
        min_ingest_ts_ns: i64,
        #[prost(sfixed64, tag = "16")]
        max_ingest_ts_ns: i64,
        #[prost(uint32, tag = "17")]
        segment_format_version: u32,
        #[prost(sfixed64, tag = "18")]
        created_unix_ns: i64,
        #[prost(uint32, tag = "19")]
        ingest_hour_bucket: u32,
    }

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
            sample_count: 1_000,
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

    fn as_legacy(record: &CommitRecord) -> LegacyCommitRecord {
        LegacyCommitRecord {
            format_version: record.format_version,
            tenant_hash: record.tenant_hash.clone(),
            signal: record.signal,
            shard: record.shard,
            writer_id: record.writer_id.clone(),
            writer_epoch: record.writer_epoch,
            writer_seq: record.writer_seq,
            object_key: record.object_key.clone(),
            object_size: record.object_size,
            content_hash: record.content_hash.clone(),
            sample_count: record.sample_count,
            series_count: record.series_count,
            min_event_ts_ns: record.min_event_ts_ns,
            max_event_ts_ns: record.max_event_ts_ns,
            min_ingest_ts_ns: record.min_ingest_ts_ns,
            max_ingest_ts_ns: record.max_ingest_ts_ns,
            segment_format_version: record.segment_format_version,
            created_unix_ns: record.created_unix_ns,
            ingest_hour_bucket: record.ingest_hour_bucket,
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

    #[test]
    fn commit_record_round_trips_stamped_stats() {
        let mut record = base_record();
        let stats = vec![
            i64_stat("EventDate", -5, 19_000, 12),
            DeclaredColumnStat::new(
                "IsRefresh",
                DeclaredStatType::Bool,
                Some(DeclaredStatValue::Bool(false)),
                Some(DeclaredStatValue::Bool(true)),
                0,
            )
            .expect("valid bool stat"),
        ];
        stamp_commit_record(&mut record, &stats);
        let decoded = record::decode(&record::encode(&record)).expect("decode");
        let read = read_commit_record(&decoded);
        assert_eq!(read.dropped, vec![]);
        assert_eq!(read.covered, stats);
        let event_date = read.column("EventDate").expect("EventDate covered");
        assert_eq!(event_date.min(), Some(DeclaredStatValue::I64(-5)));
        assert_eq!(event_date.max(), Some(DeclaredStatValue::I64(19_000)));
        assert_eq!(event_date.null_count(), 12);
        assert_eq!(decoded.format_version, 1);
    }

    #[test]
    fn compaction_part_round_trips_stamped_stats() {
        let mut part = CompactionPart {
            part_index: 0,
            first_series_id: vec![0x01; 16],
            last_series_id: vec![0x02; 16],
            content_hash: vec![0x03; 32],
            object_size: 8192,
            sample_count: 2_000,
            series_count: 8,
            run_count: 1,
            min_event_ts_ns: 10,
            max_event_ts_ns: 20,
            segment_format_version: 3,
            declared_column_stats: vec![],
        };
        let stats = vec![i64_stat("EventDate", 3, 4, 1)];
        stamp_compaction_part(&mut part, &stats);
        let bytes = part.encode_to_vec();
        let decoded = CompactionPart::decode(bytes.as_slice()).expect("decode part");
        let read = read_compaction_part(&decoded);
        assert_eq!(read.dropped, vec![]);
        assert_eq!(read.covered, stats);
        assert_eq!(read.covered.len(), 1);
    }

    #[test]
    fn record_written_without_the_field_decodes_with_it_absent() {
        let record = base_record();
        let legacy_bytes = as_legacy(&record).encode_to_vec();
        // The old schema has no field 20, so the field's key byte pair
        // (tag 20, wire type 2 -> varint 0xa2 0x01) is not on the wire.
        assert!(
            !legacy_bytes.windows(2).any(|w| w == [0xa2, 0x01]),
            "legacy encoding must carry no field-20 key"
        );
        let decoded = record::decode(&legacy_bytes).expect("legacy record decodes");
        // Absent, asserted as absent: no entries at all, and therefore no
        // column and no defect. Not "one entry whose values are zero".
        assert_eq!(decoded.declared_column_stats.len(), 0);
        let read = read_commit_record(&decoded);
        assert_eq!(read.covered, vec![]);
        assert_eq!(read.dropped, vec![]);
        assert_eq!(read.column("EventDate"), None);
        // Absence of the field is a different state from an entry carrying
        // present-but-zero values, which decodes to a real covered column.
        let mut stamped = record;
        stamp_commit_record(&mut stamped, &[i64_stat("EventDate", 0, 0, 0)]);
        let stamped = record::decode(&record::encode(&stamped)).expect("decode stamped");
        let stamped_read = read_commit_record(&stamped);
        assert_eq!(stamped_read.covered.len(), 1);
        assert_eq!(
            stamped_read.column("EventDate").map(|s| s.min()),
            Some(Some(DeclaredStatValue::I64(0)))
        );
    }

    #[test]
    fn absent_extrema_are_distinct_from_zero_extrema_on_the_wire() {
        let all_null = DeclaredColumnStat::new("EventDate", DeclaredStatType::I64, None, None, 42)
            .expect("all-null stat");
        let zero = i64_stat("EventDate", 0, 0, 42);
        let all_null_wire = encode(&all_null);
        let zero_wire = encode(&zero);
        assert_eq!(all_null_wire.min, None);
        assert_eq!(
            zero_wire.min,
            Some(DeclaredColumnStatValue {
                kind: Some(Kind::I64(0))
            })
        );
        // A zero-valued extremum still occupies bytes on the wire; an absent
        // one occupies none. The two encodings differ by exactly eight bytes:
        // four per sub-message (field key, length, oneof arm key, value).
        assert_eq!(
            zero_wire.encode_to_vec().len() - all_null_wire.encode_to_vec().len(),
            8
        );
        let round_tripped = decode(&all_null_wire).expect("decode all-null");
        assert_eq!(round_tripped.min(), None);
        assert_eq!(round_tripped.max(), None);
        assert_eq!(round_tripped.null_count(), 42);
    }

    #[test]
    fn ineligible_declared_types_are_dropped_on_decode() {
        let mut record = base_record();
        record.declared_column_stats = vec![
            DeclaredColumnMinMax {
                name: "URL".to_string(),
                declared_type: TYPED_ATTR_COLUMN_TYPE_STR,
                min: None,
                max: None,
                null_count: 0,
            },
            DeclaredColumnMinMax {
                name: "Payload".to_string(),
                declared_type: TYPED_ATTR_COLUMN_TYPE_BYTES,
                min: None,
                max: None,
                null_count: 0,
            },
            DeclaredColumnMinMax {
                name: "Ratio".to_string(),
                declared_type: TYPED_ATTR_COLUMN_TYPE_F64,
                min: None,
                max: None,
                null_count: 0,
            },
            DeclaredColumnMinMax {
                name: "Unset".to_string(),
                declared_type: TYPED_ATTR_COLUMN_TYPE_UNSPECIFIED,
                min: None,
                max: None,
                null_count: 0,
            },
            encode(&i64_stat("EventDate", 1, 2, 0)),
        ];
        // Decoding the record itself still succeeds: a bad statistic never
        // makes a durable commit record unreadable.
        let decoded = record::decode(&record::encode(&record)).expect("record still decodes");
        let read = read_commit_record(&decoded);
        assert_eq!(read.covered, vec![i64_stat("EventDate", 1, 2, 0)]);
        assert_eq!(read.dropped.len(), 4);
        assert_eq!(
            read.dropped[0],
            DroppedStatEntry {
                index: 0,
                name: "URL".to_string(),
                reason: DeclaredStatDefect::Invalid(DeclaredStatError::IneligibleType {
                    tag: TYPED_ATTR_COLUMN_TYPE_STR
                }),
            }
        );
        assert_eq!(
            read.dropped[2].reason,
            DeclaredStatDefect::Invalid(DeclaredStatError::IneligibleType {
                tag: TYPED_ATTR_COLUMN_TYPE_F64
            })
        );
        assert_eq!(read.dropped[3].index, 3);
        assert_eq!(read.column("Ratio"), None);
    }

    #[test]
    fn ineligible_type_is_refused_at_the_typed_boundary() {
        // The stamp side cannot express an ineligible type: the only way to
        // reach a declared type is through the allowlist.
        for tag in [
            TYPED_ATTR_COLUMN_TYPE_STR,
            TYPED_ATTR_COLUMN_TYPE_BYTES,
            TYPED_ATTR_COLUMN_TYPE_F64,
            TYPED_ATTR_COLUMN_TYPE_UNSPECIFIED,
        ] {
            assert_eq!(
                DeclaredStatType::from_tag(tag),
                Err(DeclaredStatError::IneligibleType { tag })
            );
        }
        // ...and every value a stamp can hold maps back to an allowlisted tag.
        let stamped = encode_all(&[i64_stat("EventDate", 1, 2, 0)]);
        assert_eq!(stamped.len(), 1);
        assert_eq!(stamped[0].declared_type, 2);
    }

    #[test]
    fn type_mismatched_and_half_present_entries_are_dropped() {
        let entries = vec![
            // BOOL column carrying an I64 min.
            DeclaredColumnMinMax {
                name: "IsRefresh".to_string(),
                declared_type: DeclaredStatType::Bool.tag(),
                min: Some(DeclaredColumnStatValue {
                    kind: Some(Kind::I64(0)),
                }),
                max: Some(DeclaredColumnStatValue {
                    kind: Some(Kind::B(true)),
                }),
                null_count: 0,
            },
            // min present, max absent.
            DeclaredColumnMinMax {
                name: "EventDate".to_string(),
                declared_type: DeclaredStatType::I64.tag(),
                min: Some(DeclaredColumnStatValue {
                    kind: Some(Kind::I64(1)),
                }),
                max: None,
                null_count: 0,
            },
            // min above max.
            DeclaredColumnMinMax {
                name: "Backwards".to_string(),
                declared_type: DeclaredStatType::I64.tag(),
                min: Some(DeclaredColumnStatValue {
                    kind: Some(Kind::I64(9)),
                }),
                max: Some(DeclaredColumnStatValue {
                    kind: Some(Kind::I64(8)),
                }),
                null_count: 0,
            },
            // Present value message with no kind set.
            DeclaredColumnMinMax {
                name: "Empty".to_string(),
                declared_type: DeclaredStatType::I64.tag(),
                min: Some(DeclaredColumnStatValue { kind: None }),
                max: Some(DeclaredColumnStatValue { kind: None }),
                null_count: 0,
            },
            // Empty column name.
            DeclaredColumnMinMax {
                name: String::new(),
                declared_type: DeclaredStatType::I64.tag(),
                min: None,
                max: None,
                null_count: 0,
            },
        ];
        let read = decode_all(&entries);
        assert_eq!(read.covered, vec![]);
        assert_eq!(read.dropped.len(), 5);
        assert_eq!(
            read.dropped[0].reason,
            DeclaredStatDefect::Invalid(DeclaredStatError::ValueTypeMismatch {
                name: "IsRefresh".to_string(),
                declared: DeclaredStatType::Bool,
                actual: DeclaredStatType::I64,
                which: "min",
            })
        );
        assert_eq!(
            read.dropped[1].reason,
            DeclaredStatDefect::Invalid(DeclaredStatError::PresenceMismatch {
                name: "EventDate".to_string(),
                min_state: "present",
                max_state: "absent",
            })
        );
        assert_eq!(
            read.dropped[2].reason,
            DeclaredStatDefect::Invalid(DeclaredStatError::MinAboveMax {
                name: "Backwards".to_string(),
                min: DeclaredStatValue::I64(9),
                max: DeclaredStatValue::I64(8),
            })
        );
        assert_eq!(
            read.dropped[3].reason,
            DeclaredStatDefect::EmptyValueKind {
                name: "Empty".to_string(),
                which: "min",
            }
        );
        assert_eq!(
            read.dropped[4].reason,
            DeclaredStatDefect::Invalid(DeclaredStatError::EmptyName)
        );
    }

    #[test]
    fn duplicate_column_entries_keep_only_the_first() {
        // Both orderings of the same duplicate pair. A resolution that keeps
        // whichever entry it prefers (widest range, highest null count, last
        // seen) satisfies one ordering and fails the other; only first-wins
        // satisfies both.
        let narrow = i64_stat("EventDate", 1, 2, 0);
        let wide = i64_stat("EventDate", 100, 200, 5);
        for (first, second) in [(&narrow, &wide), (&wide, &narrow)] {
            let entries = vec![encode(first), encode(second)];
            let read = decode_all(&entries);
            // The exact surviving entry, not just its name or its count.
            assert_eq!(read.covered, vec![first.clone()]);
            assert_eq!(read.dropped.len(), 1);
            // The exact dropped index, which the defect counter attributes by.
            assert_eq!(
                read.dropped[0],
                DroppedStatEntry {
                    index: 1,
                    name: "EventDate".to_string(),
                    reason: DeclaredStatDefect::DuplicateName("EventDate".to_string()),
                }
            );
        }
    }

    #[test]
    fn duplicates_do_not_disturb_the_order_of_the_entries_around_them() {
        // Order is record order for both lists, and a duplicate drops out of
        // the middle without reordering what follows it.
        let entries = vec![
            encode(&i64_stat("Aaa", 1, 2, 0)),
            encode(&i64_stat("Bbb", 3, 4, 0)),
            encode(&i64_stat("Aaa", 5, 6, 0)),
            encode(&i64_stat("Ccc", 7, 8, 0)),
            encode(&i64_stat("Bbb", 9, 10, 0)),
        ];
        let read = decode_all(&entries);
        assert_eq!(
            read.covered,
            vec![
                i64_stat("Aaa", 1, 2, 0),
                i64_stat("Bbb", 3, 4, 0),
                i64_stat("Ccc", 7, 8, 0),
            ]
        );
        assert_eq!(
            read.dropped
                .iter()
                .map(|d| (d.index, d.name.as_str()))
                .collect::<Vec<_>>(),
            vec![(2, "Aaa"), (4, "Bbb")]
        );
    }

    #[test]
    fn an_entry_count_above_any_declared_column_list_is_read_in_full() {
        // ADR-0873 decision 2 has no entry-count cap, and nothing in the
        // tenant-config declared-column path bounds the count either, so a
        // record carrying far more entries than any real declaration decodes
        // whole: no truncation, no error, no dropped entry. A cap added later
        // would fail here, which is where its derivation belongs.
        let count = 4_096;
        let entries: Vec<_> = (0..count)
            .map(|i| encode(&i64_stat(&format!("Column{i}"), 0, i, 0)))
            .collect();
        let read = decode_all(&entries);
        assert_eq!(read.covered.len(), usize::try_from(count).expect("fits"));
        assert_eq!(read.dropped, vec![]);
        assert_eq!(
            read.column("Column4095").map(|s| s.max()),
            Some(Some(DeclaredStatValue::I64(4_095)))
        );
    }

    #[test]
    fn truncated_stamped_record_bytes_produce_a_typed_error() {
        let mut record = base_record();
        stamp_commit_record(&mut record, &[i64_stat("EventDate", -1, 1, 3)]);
        let bytes = record::encode(&record);
        // Cutting inside the field-20 submessage must be a typed decode
        // error, never a panic and never a partially trusted stat.
        let err = record::decode(&bytes[..bytes.len() - 1]).expect_err("truncated bytes refused");
        assert!(matches!(err, record::RecordError::Decode(_)), "got {err:?}");
        // Every prefix either fails with a typed error or decodes to a record
        // whose stats are a subset of what was written: prost decodes whole
        // length-delimited fields only, so no half-read stat is ever trusted.
        let written = read_commit_record(&record).covered;
        assert_eq!(written.len(), 1);
        for cut in 0..bytes.len() {
            if let Ok(decoded) = record::decode(&bytes[..cut]) {
                let read = read_commit_record(&decoded);
                assert!(
                    read.dropped.is_empty(),
                    "cut {cut} produced a defective stat"
                );
                assert!(
                    read.covered.iter().all(|stat| written.contains(stat)),
                    "cut {cut} invented a stat"
                );
            }
        }
    }

    fn stat_strategy() -> impl Strategy<Value = DeclaredColumnStat> {
        let name = "[a-zA-Z][a-zA-Z0-9_]{0,12}";
        let i64_pair = (any::<i64>(), any::<i64>()).prop_map(|(a, b)| {
            let (min, max) = if a <= b { (a, b) } else { (b, a) };
            (
                Some(DeclaredStatValue::I64(min)),
                Some(DeclaredStatValue::I64(max)),
            )
        });
        let bool_pair = (any::<bool>(), any::<bool>()).prop_map(|(a, b)| {
            let (min, max) = if a <= b { (a, b) } else { (b, a) };
            (
                Some(DeclaredStatValue::Bool(min)),
                Some(DeclaredStatValue::Bool(max)),
            )
        });
        let extrema = prop_oneof![
            i64_pair.prop_map(|p| (DeclaredStatType::I64, p)),
            bool_pair.prop_map(|p| (DeclaredStatType::Bool, p)),
            // The all-null case: both extrema absent, still exact.
            prop::sample::select(vec![DeclaredStatType::I64, DeclaredStatType::Bool])
                .prop_map(|ty| (ty, (None, None))),
        ];
        (name, extrema, any::<u64>()).prop_map(|(name, (ty, (min, max)), null_count)| {
            DeclaredColumnStat::new(name, ty, min, max, null_count).expect("strategy builds valid")
        })
    }

    /// Distinct names, so the duplicate rule does not shrink the list.
    fn stats_strategy() -> impl Strategy<Value = Vec<DeclaredColumnStat>> {
        prop::collection::vec(stat_strategy(), 0..6).prop_map(|stats| {
            let mut seen = Vec::new();
            let mut out = Vec::new();
            for stat in stats {
                if !seen.contains(&stat.name().to_string()) {
                    seen.push(stat.name().to_string());
                    out.push(stat);
                }
            }
            out
        })
    }

    proptest! {
        #[test]
        fn stamped_stats_round_trip_through_a_commit_record(stats in stats_strategy()) {
            let mut record = base_record();
            stamp_commit_record(&mut record, &stats);
            let decoded = record::decode(&record::encode(&record)).expect("decode");
            let read = read_commit_record(&decoded);
            prop_assert_eq!(&read.dropped, &vec![]);
            prop_assert_eq!(&read.covered, &stats);
        }

        #[test]
        fn stamped_stats_round_trip_through_a_compaction_part(stats in stats_strategy()) {
            let mut part = CompactionPart {
                part_index: 1,
                first_series_id: vec![0x01; 16],
                last_series_id: vec![0x02; 16],
                content_hash: vec![0x03; 32],
                object_size: 1,
                sample_count: 1,
                series_count: 1,
                run_count: 1,
                min_event_ts_ns: 0,
                max_event_ts_ns: 0,
                segment_format_version: 3,
                declared_column_stats: vec![],
            };
            stamp_compaction_part(&mut part, &stats);
            let bytes = part.encode_to_vec();
            let decoded = CompactionPart::decode(bytes.as_slice()).expect("decode");
            let read = read_compaction_part(&decoded);
            prop_assert_eq!(&read.dropped, &vec![]);
            prop_assert_eq!(&read.covered, &stats);
        }

        // Truncating a stamped record at any point is a typed error or a
        // clean shorter record, never a panic and never a trusted stat that
        // was not written.
        #[test]
        fn truncated_stamped_bytes_never_panic(stats in stats_strategy(), cut in 0usize..512) {
            let mut record = base_record();
            stamp_commit_record(&mut record, &stats);
            let bytes = record::encode(&record);
            let n = cut.min(bytes.len());
            if let Ok(decoded) = record::decode(&bytes[..n]) {
                let read = read_commit_record(&decoded);
                prop_assert!(read.dropped.is_empty());
                prop_assert!(read.covered.iter().all(|s| stats.contains(s)));
            }
        }

        // Arbitrary bytes in the field-20 position: a decoded record must
        // either drop the entry or produce a self-consistent stat, never
        // panic and never a min above its max.
        #[test]
        fn arbitrary_bytes_never_panic(raw in prop::collection::vec(any::<u8>(), 0..192)) {
            if let Ok(record) = CommitRecord::decode(raw.as_slice()) {
                let read = read_commit_record(&record);
                for stat in &read.covered {
                    prop_assert!(!stat.name().is_empty());
                    if let (Some(min), Some(max)) = (stat.min(), stat.max()) {
                        prop_assert_eq!(min.stat_type(), stat.declared_type());
                        prop_assert_eq!(max.stat_type(), stat.declared_type());
                    } else {
                        prop_assert_eq!(stat.min(), None);
                        prop_assert_eq!(stat.max(), None);
                    }
                }
            }
        }

        #[test]
        fn arbitrary_entry_fields_decode_or_drop(
            name in ".{0,8}",
            declared_type in 0u32..8,
            min in proptest::option::of(any::<i64>()),
            max in proptest::option::of(any::<i64>()),
            null_count in any::<u64>(),
        ) {
            let entry = DeclaredColumnMinMax {
                name,
                declared_type,
                min: min.map(|v| DeclaredColumnStatValue { kind: Some(Kind::I64(v)) }),
                max: max.map(|v| DeclaredColumnStatValue { kind: Some(Kind::I64(v)) }),
                null_count,
            };
            match decode(&entry) {
                Ok(stat) => {
                    // An accepted entry's type is exactly what the allowlist
                    // resolved the stored tag to, and every extremum it kept
                    // is an I64 one, because that is all this strategy
                    // stores: a BOOL entry is only ever accepted with both
                    // extrema absent.
                    prop_assert_eq!(
                        Ok(stat.declared_type()),
                        DeclaredStatType::from_tag(declared_type)
                    );
                    if stat.min().is_some() || stat.max().is_some() {
                        prop_assert_eq!(stat.declared_type(), DeclaredStatType::I64);
                        prop_assert_eq!(stat.min(), min.map(DeclaredStatValue::I64));
                        prop_assert_eq!(stat.max(), max.map(DeclaredStatValue::I64));
                    }
                    prop_assert_eq!(stat.null_count(), null_count);
                    prop_assert!(!stat.name().is_empty());
                }
                Err(_) => {
                    // Dropped, and the record-level read reports exactly one
                    // defect for it.
                    let read = decode_all(&[entry]);
                    prop_assert_eq!(read.covered.len(), 0);
                    prop_assert_eq!(read.dropped.len(), 1);
                }
            }
        }
    }
}
