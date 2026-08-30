//! Column-statistics envelope: whole-object read, no per-section access
//! protocol. ADR-0850.
//!
//! ```text
//! magic           "RCST" (4 bytes)
//! version         u8 = 1
//! reserved        u8[3] = 0
//! header_len      u32 LE
//! header          protobuf ravel.catalog.v1.ColumnStatsHeader
//! body_len        u64 LE
//! body            zstd(segments)  segments = length-delimited protobuf
//!                                 ravel.catalog.v1.ColumnStatsSegment,
//!                                 sorted by (ingest_hour_bucket, shard,
//!                                 writer_id, writer_epoch, writer_seq)
//! body_crc32c     u32 LE          over the compressed body bytes
//! header_crc32c   u32 LE          over magic..header inclusive
//! ```
//!
//! Deliberately reuses `part.rs`'s plain length-delimited-protobuf body
//! convention rather than `postings.rs`'s hand-rolled varint dictionary:
//! that dictionary earns its complexity for a huge flat cross-segment name
//! space, which per-segment column statistics don't have.

use std::collections::HashSet;

use prost::Message;
use ravel_proto::catalog::v1::column_value::Kind;
use ravel_proto::catalog::v1::{ColumnStat, ColumnStatsHeader, ColumnStatsSegment, ColumnValue};

use super::error::SnapshotFormatError;
use super::{
    COLUMN_STATS_MAGIC, COLUMN_STATS_RESERVED, COLUMN_STATS_VERSION, MIN_COLUMN_STATS_ENVELOPE_LEN,
    ZSTD_LEVEL,
};
use crate::snapshot_format::ColumnStatsLimits;

/// A decoded, fully validated column-statistics object.
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedColumnStats {
    pub header: ColumnStatsHeader,
    pub segments: Vec<ColumnStatsSegment>,
}

/// Encodes a column-statistics object. Validates `segments` against the same
/// rules `decode_column_stats` enforces (sort order, no duplicate identity),
/// mirroring `encode_part`'s defensive-validation precedent: an object this
/// function writes can never fail its own decode.
pub fn encode_column_stats(
    tenant_hash: [u8; 16],
    signal: u32,
    part_blake3: Vec<Vec<u8>>,
    segments: &[ColumnStatsSegment],
) -> Result<Vec<u8>, SnapshotFormatError> {
    validate_segments(segments)?;

    let mut segments_raw = Vec::new();
    for segment in segments {
        segments_raw.extend_from_slice(&segment.encode_length_delimited_to_vec());
    }
    let body_uncompressed_len = segments_raw.len() as u64;

    let body = zstd::bulk::compress(&segments_raw, ZSTD_LEVEL)
        .map_err(|e| SnapshotFormatError::Compress(e.to_string()))?;

    let header = ColumnStatsHeader {
        format_version: u32::from(COLUMN_STATS_VERSION),
        tenant_hash: tenant_hash.to_vec(),
        signal,
        part_blake3,
        segment_count: segments.len() as u64,
        body_uncompressed_len,
    };
    let header_bytes = header.encode_to_vec();
    let header_len =
        u32::try_from(header_bytes.len()).map_err(|_| SnapshotFormatError::HeaderTooLarge)?;

    let mut out =
        Vec::with_capacity(MIN_COLUMN_STATS_ENVELOPE_LEN + header_bytes.len() + body.len());
    out.extend_from_slice(&COLUMN_STATS_MAGIC);
    out.push(COLUMN_STATS_VERSION);
    out.extend_from_slice(&COLUMN_STATS_RESERVED);
    out.extend_from_slice(&header_len.to_le_bytes());
    out.extend_from_slice(&header_bytes);

    let header_crc = crc32c::crc32c(&out);

    let body_len = body.len() as u64;
    out.extend_from_slice(&body_len.to_le_bytes());
    out.extend_from_slice(&body);

    let body_crc = crc32c::crc32c(&body);

    out.extend_from_slice(&body_crc.to_le_bytes());
    out.extend_from_slice(&header_crc.to_le_bytes());

    Ok(out)
}

/// Decodes and fully validates a column-statistics object. Every byte is
/// untrusted; every failure is a typed error, never a panic.
pub fn decode_column_stats(
    bytes: &[u8],
    limits: &ColumnStatsLimits,
) -> Result<DecodedColumnStats, SnapshotFormatError> {
    if bytes.len() < MIN_COLUMN_STATS_ENVELOPE_LEN {
        return Err(SnapshotFormatError::ColumnStatsTooSmall { size: bytes.len() });
    }

    let mut pos = 0usize;
    let magic = take_array::<4>(bytes, &mut pos)?;
    if magic != COLUMN_STATS_MAGIC {
        return Err(SnapshotFormatError::ColumnStatsBadMagic);
    }
    let version = take_bytes(bytes, &mut pos, 1)?[0];
    if version != COLUMN_STATS_VERSION {
        return Err(SnapshotFormatError::ColumnStatsUnsupportedVersion(version));
    }
    let reserved = take_array::<3>(bytes, &mut pos)?;
    if reserved != COLUMN_STATS_RESERVED {
        return Err(SnapshotFormatError::ColumnStatsReservedNonZero);
    }
    let header_len = take_u32_le(bytes, &mut pos)?;
    let header_bytes = take_bytes(bytes, &mut pos, to_usize(header_len)?)?;
    let header_end = pos;
    let header_crc_expected = crc32c::crc32c(&bytes[..header_end]);

    let body_len = take_u64_le(bytes, &mut pos)?;
    let body = take_bytes(bytes, &mut pos, to_usize(body_len)?)?;
    let body_crc_stored = take_u32_le(bytes, &mut pos)?;
    let header_crc_stored = take_u32_le(bytes, &mut pos)?;

    if pos != bytes.len() {
        return Err(SnapshotFormatError::ColumnStatsTrailingBytes);
    }
    if header_crc_stored != header_crc_expected {
        return Err(SnapshotFormatError::ColumnStatsHeaderCrcMismatch);
    }
    if body_crc_stored != crc32c::crc32c(body) {
        return Err(SnapshotFormatError::ColumnStatsBodyCrcMismatch);
    }

    let header = ColumnStatsHeader::decode(header_bytes)
        .map_err(|e| SnapshotFormatError::ColumnStatsHeaderDecode(e.to_string()))?;
    if header.format_version != u32::from(COLUMN_STATS_VERSION) {
        return Err(SnapshotFormatError::ColumnStatsHeaderVersionMismatch {
            header: header.format_version,
            envelope: COLUMN_STATS_VERSION,
        });
    }
    if header.tenant_hash.len() != 16 {
        return Err(SnapshotFormatError::ColumnStatsBadTenantHashLen(
            header.tenant_hash.len(),
        ));
    }
    if header.body_uncompressed_len > limits.max_column_stats_bytes {
        return Err(SnapshotFormatError::ColumnStatsDecompressedTooLarge {
            declared: header.body_uncompressed_len,
            cap: limits.max_column_stats_bytes,
        });
    }
    let capacity = to_usize(header.body_uncompressed_len)?;
    let decompressed = zstd::bulk::decompress(body, capacity)
        .map_err(|e| SnapshotFormatError::Decompress(e.to_string()))?;
    if decompressed.len() as u64 != header.body_uncompressed_len {
        return Err(SnapshotFormatError::ColumnStatsDecompressedLenMismatch {
            expected: header.body_uncompressed_len,
            actual: decompressed.len() as u64,
        });
    }

    let mut segments = Vec::new();
    let mut cursor: &[u8] = &decompressed[..];
    while !cursor.is_empty() {
        let segment = ColumnStatsSegment::decode_length_delimited(&mut cursor)
            .map_err(|e| SnapshotFormatError::ColumnStatsSegmentDecode(e.to_string()))?;
        segments.push(segment);
    }
    if segments.len() as u64 != header.segment_count {
        return Err(SnapshotFormatError::ColumnStatsSegmentCountMismatch {
            expected: header.segment_count,
            actual: segments.len() as u64,
        });
    }
    validate_segments(&segments)?;

    Ok(DecodedColumnStats { header, segments })
}

/// Sort/uniqueness/field validation shared by `encode_column_stats`
/// (defensive check of caller input) and `decode_column_stats` (untrusted-
/// bytes check). Beyond segment identity this also validates every
/// `ColumnStat`'s internal semantics (ADR-0850): a record the metadata-only
/// query path could read (`declared_not_equal_count`/`declared_group_counts`)
/// is rejected here before it can ever be loaded, so those paths can never
/// derive a wrong answer from an internally-inconsistent record. Fail closed.
fn validate_segments(segments: &[ColumnStatsSegment]) -> Result<(), SnapshotFormatError> {
    for (i, segment) in segments.iter().enumerate() {
        if segment.writer_id.len() != 16 {
            return Err(SnapshotFormatError::ColumnStatsBadFieldLen {
                field: "writer_id",
                expected: 16,
                actual: segment.writer_id.len(),
            });
        }
        if i > 0 {
            match segment_key(&segments[i - 1]).cmp(&segment_key(segment)) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Equal => {
                    return Err(SnapshotFormatError::ColumnStatsDuplicateSegment);
                }
                std::cmp::Ordering::Greater => {
                    return Err(SnapshotFormatError::ColumnStatsSegmentsUnsorted);
                }
            }
        }
        validate_columns(&segment.columns)?;
    }
    Ok(())
}

/// Per-segment column-list validation: no duplicate column name, and every
/// column internally consistent.
fn validate_columns(columns: &[ColumnStat]) -> Result<(), SnapshotFormatError> {
    let mut names: HashSet<&str> = HashSet::with_capacity(columns.len());
    for column in columns {
        if !names.insert(column.name.as_str()) {
            return Err(SnapshotFormatError::ColumnStatsDuplicateColumnName {
                name: column.name.clone(),
            });
        }
        validate_column(column)?;
    }
    Ok(())
}

/// One column's internal-consistency rules (ADR-0850). A record failing any
/// of these could make the metadata-only path answer a query wrong, so it is
/// a typed rejection, never silently trusted:
///
/// - `declared_type` names a known typed-attribute type;
/// - every `min`/`max`/dictionary value's kind matches `declared_type`;
/// - `min`/`max` are absent when `non_null_count == 0` and `min <= max`;
/// - a present dictionary has no duplicate value and its counts sum to
///   exactly `non_null_count`; an absent dictionary carries no entries.
fn validate_column(column: &ColumnStat) -> Result<(), SnapshotFormatError> {
    if !(1..=4).contains(&column.declared_type) {
        return Err(SnapshotFormatError::ColumnStatsUnknownDeclaredType {
            name: column.name.clone(),
            declared_type: column.declared_type,
        });
    }

    if column.non_null_count == 0 && (column.min.is_some() || column.max.is_some()) {
        return Err(SnapshotFormatError::ColumnStatsUnexpectedMinMax {
            name: column.name.clone(),
        });
    }
    // The symmetric case: non-null rows must carry both extrema. A record with
    // rows but no min/max cannot support a MIN/MAX answer, and a reader that
    // trusted it would report the extremum of non-null data as exactly NULL.
    if column.non_null_count > 0 && (column.min.is_none() || column.max.is_none()) {
        return Err(SnapshotFormatError::ColumnStatsMissingMinMax {
            name: column.name.clone(),
        });
    }
    if let Some(min) = &column.min {
        check_value_kind(column, min, "min")?;
    }
    if let Some(max) = &column.max {
        check_value_kind(column, max, "max")?;
    }
    if let (Some(min), Some(max)) = (&column.min, &column.max)
        && compare_values(min, max) == Some(std::cmp::Ordering::Greater)
    {
        return Err(SnapshotFormatError::ColumnStatsMinMaxInverted {
            name: column.name.clone(),
        });
    }

    // #861: a `sum` is stored for I64 columns only. Any other declared type
    // carrying one is internally inconsistent; reject before a reader can trust
    // it.
    if column.sum.is_some() && column.declared_type != 2 {
        return Err(SnapshotFormatError::ColumnStatsSumOnNonInteger {
            name: column.name.clone(),
            declared_type: column.declared_type,
        });
    }

    if column.dictionary_present {
        let mut seen: HashSet<Vec<u8>> = HashSet::with_capacity(column.dictionary.len());
        let mut total: u64 = 0;
        // Exact `Σ value * count` for an I64 column, in `i128` so it never
        // overflows a valid record; `None` marks an accumulation that exceeded
        // `i128`, which cannot equal the stored `i64` sum and so fails the
        // cross-check below.
        let mut dict_sum: Option<i128> = if column.declared_type == 2 {
            Some(0)
        } else {
            None
        };
        for entry in &column.dictionary {
            let value = entry.value.as_ref().ok_or_else(|| {
                SnapshotFormatError::ColumnStatsDictEntryMissingValue {
                    name: column.name.clone(),
                }
            })?;
            check_value_kind(column, value, "dictionary")?;
            if !seen.insert(value.encode_to_vec()) {
                return Err(SnapshotFormatError::ColumnStatsDuplicateDictValue {
                    name: column.name.clone(),
                });
            }
            total = total.saturating_add(entry.count);
            if let (Some(acc), Some(Kind::I64(v))) = (dict_sum, value.kind.as_ref()) {
                dict_sum = i128::from(*v)
                    .checked_mul(i128::from(entry.count))
                    .and_then(|term| acc.checked_add(term));
            }
        }
        if total != column.non_null_count {
            return Err(SnapshotFormatError::ColumnStatsDictCountMismatch {
                name: column.name.clone(),
                dict_total: total,
                non_null_count: column.non_null_count,
            });
        }
        // A present dictionary AND a present sum must agree exactly: the
        // dictionary is the ground truth the fold summed. `dict_sum == None`
        // (an i128 overflow) can never equal an i64 sum, so it is reported as a
        // mismatch, not silently accepted.
        if let Some(sum) = column.sum {
            let dict_total = dict_sum.unwrap_or(i128::MAX);
            if dict_total != i128::from(sum) {
                return Err(SnapshotFormatError::ColumnStatsSumMismatch {
                    name: column.name.clone(),
                    sum,
                    dict_sum: dict_total.clamp(i128::from(i64::MIN), i128::from(i64::MAX)) as i64,
                });
            }
        }
    } else if !column.dictionary.is_empty() {
        return Err(SnapshotFormatError::ColumnStatsDictPresentMismatch {
            name: column.name.clone(),
            entries: column.dictionary.len(),
        });
    }

    Ok(())
}

/// Whether `value`'s wire kind matches the `ColumnStat.declared_type` tag
/// (1=Str, 2=I64, 3=Bool, 4=Bytes; the `declared_type_to_stats_tag` mapping
/// the fold writes with).
fn value_kind_matches(declared_type: u32, value: &ColumnValue) -> bool {
    matches!(
        (declared_type, value.kind.as_ref()),
        (1, Some(Kind::StrUtf8(_)))
            | (2, Some(Kind::I64(_)))
            | (3, Some(Kind::B(_)))
            | (4, Some(Kind::BytesVal(_)))
    )
}

fn check_value_kind(
    column: &ColumnStat,
    value: &ColumnValue,
    field: &'static str,
) -> Result<(), SnapshotFormatError> {
    if value_kind_matches(column.declared_type, value) {
        Ok(())
    } else {
        Err(SnapshotFormatError::ColumnStatsValueTypeMismatch {
            name: column.name.clone(),
            field,
            declared_type: column.declared_type,
        })
    }
}

/// Total order over two same-kind values. `None` when the kinds differ (the
/// caller has already kind-checked both against `declared_type`, so this
/// never happens for a valid record); a `None` never triggers the inverted
/// check, keeping the failure attributable to the kind rule instead.
fn compare_values(a: &ColumnValue, b: &ColumnValue) -> Option<std::cmp::Ordering> {
    match (a.kind.as_ref(), b.kind.as_ref()) {
        (Some(Kind::I64(x)), Some(Kind::I64(y))) => Some(x.cmp(y)),
        (Some(Kind::B(x)), Some(Kind::B(y))) => Some(x.cmp(y)),
        (Some(Kind::StrUtf8(x)), Some(Kind::StrUtf8(y))) => Some(x.cmp(y)),
        (Some(Kind::BytesVal(x)), Some(Kind::BytesVal(y))) => Some(x.cmp(y)),
        _ => None,
    }
}

fn segment_key(segment: &ColumnStatsSegment) -> (u32, u32, &[u8], u64, u64) {
    (
        segment.ingest_hour_bucket,
        segment.shard,
        segment.writer_id.as_slice(),
        segment.writer_epoch,
        segment.writer_seq,
    )
}

fn to_usize<T: TryInto<usize>>(v: T) -> Result<usize, SnapshotFormatError> {
    v.try_into().map_err(|_| SnapshotFormatError::Truncated)
}

fn take_bytes<'a>(
    bytes: &'a [u8],
    pos: &mut usize,
    n: usize,
) -> Result<&'a [u8], SnapshotFormatError> {
    let end = pos.checked_add(n).ok_or(SnapshotFormatError::Truncated)?;
    let slice = bytes.get(*pos..end).ok_or(SnapshotFormatError::Truncated)?;
    *pos = end;
    Ok(slice)
}

fn take_array<const N: usize>(
    bytes: &[u8],
    pos: &mut usize,
) -> Result<[u8; N], SnapshotFormatError> {
    let slice = take_bytes(bytes, pos, N)?;
    slice.try_into().map_err(|_| SnapshotFormatError::Truncated)
}

fn take_u32_le(bytes: &[u8], pos: &mut usize) -> Result<u32, SnapshotFormatError> {
    Ok(u32::from_le_bytes(take_array::<4>(bytes, pos)?))
}

fn take_u64_le(bytes: &[u8], pos: &mut usize) -> Result<u64, SnapshotFormatError> {
    Ok(u64::from_le_bytes(take_array::<8>(bytes, pos)?))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use ravel_proto::catalog::v1::{ColumnStat, ColumnStatsSegment, ColumnValue, DictEntry};

    use super::*;

    fn i64_value(v: i64) -> ColumnValue {
        ColumnValue {
            kind: Some(ravel_proto::catalog::v1::column_value::Kind::I64(v)),
        }
    }

    fn segment(hour: u32, shard: u32, seq: u64) -> ColumnStatsSegment {
        // A consistent I64 column: values 0..=9, each seen once, so the
        // dictionary counts sum to non_null_count and min/max bracket it.
        let dictionary: Vec<DictEntry> = (0..10)
            .map(|v| DictEntry {
                value: Some(i64_value(v)),
                count: 1,
            })
            .collect();
        ColumnStatsSegment {
            ingest_hour_bucket: hour,
            shard,
            writer_id: vec![0xAA; 16],
            writer_epoch: 1,
            writer_seq: seq,
            columns: vec![ColumnStat {
                name: "AdvEngineID".to_string(),
                declared_type: 2,
                non_null_count: 10,
                null_count: 0,
                min: Some(i64_value(0)),
                max: Some(i64_value(9)),
                dictionary_present: true,
                dictionary,
                sum: Some(45), // 0 + 1 + ... + 9
            }],
        }
    }

    #[test]
    fn round_trips() {
        let segments = vec![segment(1, 0, 1), segment(1, 0, 2), segment(2, 0, 1)];
        let bytes =
            encode_column_stats([0x11; 16], 3, vec![vec![0x22; 32]], &segments).expect("encodes");
        let decoded = decode_column_stats(&bytes, &ColumnStatsLimits::default()).expect("decodes");
        assert_eq!(decoded.segments, segments);
        assert_eq!(decoded.header.segment_count, 3);
    }

    #[test]
    fn unsorted_segments_rejected() {
        let segments = vec![segment(2, 0, 1), segment(1, 0, 1)];
        let err =
            encode_column_stats([0x11; 16], 3, vec![], &segments).expect_err("unsorted rejected");
        assert_eq!(err, SnapshotFormatError::ColumnStatsSegmentsUnsorted);
    }

    #[test]
    fn duplicate_identity_rejected() {
        let segments = vec![segment(1, 0, 1), segment(1, 0, 1)];
        let err =
            encode_column_stats([0x11; 16], 3, vec![], &segments).expect_err("duplicate rejected");
        assert_eq!(err, SnapshotFormatError::ColumnStatsDuplicateSegment);
    }

    /// Finding 4's exact example: a column claiming `non_null_count = 10`
    /// with `dictionary_present = true` but an empty dictionary is internally
    /// inconsistent (the metadata-only path would treat all 10 rows as
    /// not-equal to any literal). It must be rejected at encode/decode, not
    /// trusted.
    #[test]
    fn empty_dictionary_with_nonzero_non_null_rejected() {
        let mut seg = segment(1, 0, 1);
        seg.columns[0].dictionary = vec![];
        let err = encode_column_stats([0x11; 16], 3, vec![], &[seg]).expect_err("rejected");
        assert_eq!(
            err,
            SnapshotFormatError::ColumnStatsDictCountMismatch {
                name: "AdvEngineID".to_string(),
                dict_total: 0,
                non_null_count: 10,
            }
        );
    }

    #[test]
    fn dictionary_counts_not_summing_to_non_null_rejected() {
        let mut seg = segment(1, 0, 1);
        // Drop one entry so the counts total 9 but non_null_count stays 10.
        seg.columns[0].dictionary.pop();
        let err = encode_column_stats([0x11; 16], 3, vec![], &[seg]).expect_err("rejected");
        assert_eq!(
            err,
            SnapshotFormatError::ColumnStatsDictCountMismatch {
                name: "AdvEngineID".to_string(),
                dict_total: 9,
                non_null_count: 10,
            }
        );
    }

    #[test]
    fn wrong_value_kind_rejected() {
        let mut seg = segment(1, 0, 1);
        seg.columns[0].dictionary[0].value = Some(ColumnValue {
            kind: Some(ravel_proto::catalog::v1::column_value::Kind::B(true)),
        });
        let err = encode_column_stats([0x11; 16], 3, vec![], &[seg]).expect_err("rejected");
        assert_eq!(
            err,
            SnapshotFormatError::ColumnStatsValueTypeMismatch {
                name: "AdvEngineID".to_string(),
                field: "dictionary",
                declared_type: 2,
            }
        );
    }

    #[test]
    fn duplicate_dictionary_value_rejected() {
        let mut seg = segment(1, 0, 1);
        // Two entries with the same value; total still 10.
        seg.columns[0].dictionary = vec![
            DictEntry {
                value: Some(i64_value(0)),
                count: 5,
            },
            DictEntry {
                value: Some(i64_value(0)),
                count: 5,
            },
        ];
        let err = encode_column_stats([0x11; 16], 3, vec![], &[seg]).expect_err("rejected");
        assert_eq!(
            err,
            SnapshotFormatError::ColumnStatsDuplicateDictValue {
                name: "AdvEngineID".to_string(),
            }
        );
    }

    #[test]
    fn duplicate_column_name_rejected() {
        let mut seg = segment(1, 0, 1);
        let dup = seg.columns[0].clone();
        seg.columns.push(dup);
        let err = encode_column_stats([0x11; 16], 3, vec![], &[seg]).expect_err("rejected");
        assert_eq!(
            err,
            SnapshotFormatError::ColumnStatsDuplicateColumnName {
                name: "AdvEngineID".to_string(),
            }
        );
    }

    #[test]
    fn min_greater_than_max_rejected() {
        let mut seg = segment(1, 0, 1);
        seg.columns[0].min = Some(i64_value(9));
        seg.columns[0].max = Some(i64_value(0));
        let err = encode_column_stats([0x11; 16], 3, vec![], &[seg]).expect_err("rejected");
        assert_eq!(
            err,
            SnapshotFormatError::ColumnStatsMinMaxInverted {
                name: "AdvEngineID".to_string(),
            }
        );
    }

    #[test]
    fn min_max_present_with_zero_non_null_rejected() {
        let mut seg = segment(1, 0, 1);
        seg.columns[0].non_null_count = 0;
        seg.columns[0].dictionary = vec![];
        // min/max still populated: inconsistent with an all-null column.
        let err = encode_column_stats([0x11; 16], 3, vec![], &[seg]).expect_err("rejected");
        assert_eq!(
            err,
            SnapshotFormatError::ColumnStatsUnexpectedMinMax {
                name: "AdvEngineID".to_string(),
            }
        );
    }

    #[test]
    fn dictionary_entries_without_present_flag_rejected() {
        let mut seg = segment(1, 0, 1);
        seg.columns[0].dictionary_present = false;
        let err = encode_column_stats([0x11; 16], 3, vec![], &[seg]).expect_err("rejected");
        assert_eq!(
            err,
            SnapshotFormatError::ColumnStatsDictPresentMismatch {
                name: "AdvEngineID".to_string(),
                entries: 10,
            }
        );
    }

    /// #861: a stored `sum` that disagrees with the dictionary the fold summed
    /// is internally inconsistent, so a reader could derive a wrong SUM/AVG.
    /// Reject it at encode/decode.
    ///
    /// Prove-the-test: dropping the `ColumnStatsSumMismatch` check in
    /// `validate_column` lets this encode succeed and the assertion fails.
    #[test]
    fn sum_disagreeing_with_dictionary_rejected() {
        let mut seg = segment(1, 0, 1);
        // True dictionary sum is 45; claim 44.
        seg.columns[0].sum = Some(44);
        let err = encode_column_stats([0x11; 16], 3, vec![], &[seg]).expect_err("rejected");
        assert_eq!(
            err,
            SnapshotFormatError::ColumnStatsSumMismatch {
                name: "AdvEngineID".to_string(),
                sum: 44,
                dict_sum: 45,
            }
        );
    }

    /// #861: a sum is I64-only. A non-integer column carrying one is rejected.
    ///
    /// Prove-the-test: dropping the `ColumnStatsSumOnNonInteger` check lets a
    /// Bool column keep a sum and the assertion fails.
    #[test]
    fn sum_on_non_integer_column_rejected() {
        let mut seg = segment(1, 0, 1);
        // Reshape the column into a consistent Bool column, then attach a sum.
        seg.columns[0].declared_type = 3; // Bool
        seg.columns[0].min = Some(ColumnValue {
            kind: Some(Kind::B(false)),
        });
        seg.columns[0].max = Some(ColumnValue {
            kind: Some(Kind::B(true)),
        });
        seg.columns[0].dictionary = vec![
            DictEntry {
                value: Some(ColumnValue {
                    kind: Some(Kind::B(false)),
                }),
                count: 4,
            },
            DictEntry {
                value: Some(ColumnValue {
                    kind: Some(Kind::B(true)),
                }),
                count: 6,
            },
        ];
        seg.columns[0].sum = Some(6);
        let err = encode_column_stats([0x11; 16], 3, vec![], &[seg]).expect_err("rejected");
        assert_eq!(
            err,
            SnapshotFormatError::ColumnStatsSumOnNonInteger {
                name: "AdvEngineID".to_string(),
                declared_type: 3,
            }
        );
    }

    /// A high-cardinality I64 column with its dictionary omitted still carries
    /// an exact sum (the sum is stored independently of the dictionary), and
    /// the codec round-trips it without a dictionary to cross-check against.
    #[test]
    fn sum_without_dictionary_round_trips() {
        let mut seg = segment(1, 0, 1);
        seg.columns[0].dictionary_present = false;
        seg.columns[0].dictionary = vec![];
        seg.columns[0].sum = Some(45);
        let bytes = encode_column_stats([0x11; 16], 3, vec![vec![0x22; 32]], &[seg.clone()])
            .expect("encodes");
        let decoded = decode_column_stats(&bytes, &ColumnStatsLimits::default()).expect("decodes");
        assert_eq!(decoded.segments[0].columns[0].sum, Some(45));
    }

    #[test]
    fn oversized_declared_length_rejected_before_decompress() {
        let segments = vec![segment(1, 0, 1)];
        let bytes = encode_column_stats([0x11; 16], 3, vec![], &segments).expect("encodes");
        let tiny_limit = ColumnStatsLimits {
            max_column_stats_bytes: 1,
        };
        let err = decode_column_stats(&bytes, &tiny_limit).expect_err("rejected");
        assert!(matches!(
            err,
            SnapshotFormatError::ColumnStatsDecompressedTooLarge { .. }
        ));
    }
}
