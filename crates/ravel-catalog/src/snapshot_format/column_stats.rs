//! Column-statistics envelope: whole-object read, no per-section access
//! protocol. ADR-0850.
//!
//! ```text
//! magic           "RCST" (4 bytes)
//! version         u8 = 1 (ADR-0850, L0-tuple keyed) or 2 (ADR-0942, part-keyed)
//! reserved        u8[3] = 0
//! header_len      u32 LE
//! header          protobuf ravel.catalog.v1.ColumnStatsHeader
//! body_len        u64 LE
//! body            zstd(segments)  segments = length-delimited protobuf
//!                                 ravel.catalog.v1.ColumnStatsSegment,
//!                                 sorted by (ingest_hour_bucket, shard,
//!                                 writer_id, writer_epoch, writer_seq) in v1,
//!                                 by writer_id (the part content hash) in v2
//! body_crc32c     u32 LE          over the compressed body bytes
//! header_crc32c   u32 LE          over magic..header inclusive
//! ```
//!
//! Two envelope versions coexist during the ADR-0942 dual-publish window. The
//! `ColumnStatsSegment` record shape is frozen and shared; the key model is the
//! version's. v1 keys each record by the five-field identity tuple (writer_id is
//! the 16-byte flush-writer uuid) and covers L0 only. v2 keys by the covered
//! part's content hash, which the writer carries in the `writer_id` slot as 32
//! bytes (the same slot an L1 `SnapshotEntry` already repurposes for a 32-byte
//! hash), and covers L0 and L1 uniformly. The keying is self-describing in the
//! version byte, so an object read outside its head ref declares which key
//! model it carries.
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
    COLUMN_STATS_MAGIC, COLUMN_STATS_RESERVED, COLUMN_STATS_WRITE_VERSION,
    MIN_COLUMN_STATS_ENVELOPE_LEN, ZSTD_LEVEL, column_stats_version_accepted,
};
use crate::snapshot_format::ColumnStatsLimits;

/// A decoded, fully validated column-statistics object.
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedColumnStats {
    pub header: ColumnStatsHeader,
    pub segments: Vec<ColumnStatsSegment>,
}

/// Encodes a **v1** (ADR-0850, L0-tuple-keyed) column-statistics object, the
/// `SnapshotHead.column_stats` (field 11) artifact. Validates `segments`
/// against the same rules `decode_column_stats` enforces for v1 (writer_id
/// width, tuple sort order, no duplicate identity), mirroring `encode_part`'s
/// defensive-validation precedent: an object this function writes can never
/// fail its own decode.
///
/// Stamps envelope version 1 explicitly, NOT [`COLUMN_STATS_WRITE_VERSION`]:
/// the ADR-0942 dual-publish window keeps writing the v1 object byte-for-byte
/// as before even after the write version moves to 2. The v2 (part-keyed)
/// artifact is written by [`encode_column_stats_v2`].
pub fn encode_column_stats(
    tenant_hash: [u8; 16],
    signal: u32,
    part_blake3: Vec<Vec<u8>>,
    segments: &[ColumnStatsSegment],
) -> Result<Vec<u8>, SnapshotFormatError> {
    encode_column_stats_versioned(1, tenant_hash, signal, part_blake3, segments)
}

/// Encodes a **v2** (ADR-0942, part-hash-keyed) column-statistics object, the
/// `SnapshotHead.column_stats_part` (field 13) artifact. Each segment record
/// must carry its covered part's content hash (blake3) in its `writer_id` slot
/// as 32 bytes; the records' key is that hash, not the five-field identity
/// tuple, so L0 and L1 parts are named uniformly and two L1 parts of one bucket
/// never collide. Stamps [`COLUMN_STATS_WRITE_VERSION`].
///
/// Like [`encode_column_stats`] this VALIDATES the caller's ordering, it does
/// not impose it: `segments` must already be sorted by that hash and free of
/// duplicates, or the call fails. Sorting here instead would silently repair a
/// caller that built two records for one part, and a duplicate is a build bug
/// worth surfacing (the caller decides which record wins); the callee also
/// takes a borrowed slice and could not reorder it in place.
pub fn encode_column_stats_v2(
    tenant_hash: [u8; 16],
    signal: u32,
    part_blake3: Vec<Vec<u8>>,
    segments: &[ColumnStatsSegment],
) -> Result<Vec<u8>, SnapshotFormatError> {
    encode_column_stats_versioned(
        COLUMN_STATS_WRITE_VERSION,
        tenant_hash,
        signal,
        part_blake3,
        segments,
    )
}

/// Envelope framing shared by the public writers and the tests. Parameterised
/// on `version`, which selects both the stamped version byte and the segment
/// key model `validate_segments` enforces (v1: five-field tuple; v2:
/// `content_hash`). Stamps `version` into both the envelope version byte and
/// the header `format_version`, so the two always agree by construction.
fn encode_column_stats_versioned(
    version: u8,
    tenant_hash: [u8; 16],
    signal: u32,
    part_blake3: Vec<Vec<u8>>,
    segments: &[ColumnStatsSegment],
) -> Result<Vec<u8>, SnapshotFormatError> {
    validate_segments(segments, version)?;

    let mut segments_raw = Vec::new();
    for segment in segments {
        segments_raw.extend_from_slice(&segment.encode_length_delimited_to_vec());
    }
    let body_uncompressed_len = segments_raw.len() as u64;

    let body = zstd::bulk::compress(&segments_raw, ZSTD_LEVEL)
        .map_err(|e| SnapshotFormatError::Compress(e.to_string()))?;

    let header = ColumnStatsHeader {
        format_version: u32::from(version),
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
    out.push(version);
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
    // Membership in the accepted read set, not equality against the write
    // version (ADR-0942): a v1 object must keep decoding after A2 bumps the
    // write version to 2, or the L0 reader path loses coverage.
    if !column_stats_version_accepted(version) {
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
    // The header's self-declared version must agree with the accepted envelope
    // version byte. The byte already passed the membership gate above, so this
    // accepts any object in the read set while still rejecting a header that
    // disagrees with its own envelope (the ADR-0942 self-describing-state rule:
    // a v1 header under a v2 envelope, or vice versa, subtracts coverage).
    if header.format_version != u32::from(version) {
        return Err(SnapshotFormatError::ColumnStatsHeaderVersionMismatch {
            header: header.format_version,
            envelope: version,
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
    validate_segments(&segments, version)?;

    Ok(DecodedColumnStats { header, segments })
}

/// Sort/uniqueness/field validation shared by the encoders (defensive check of
/// caller input) and `decode_column_stats` (untrusted-bytes check). Beyond
/// segment identity this also validates every `ColumnStat`'s internal
/// semantics (ADR-0850): a record the metadata-only query path could read
/// (`declared_not_equal_count`/`declared_group_counts`) is rejected here before
/// it can ever be loaded, so those paths can never derive a wrong answer from
/// an internally-inconsistent record. Fail closed.
///
/// `version` selects the segment key model (ADR-0942). Both models key on
/// `writer_id` (field 3), differing in width and meaning:
/// - v1 (`version < 2`): the 16-byte flush-writer uuid, one component of the
///   five-field identity tuple the records must be sorted by and unique under.
/// - v2 (`version >= 2`): the covered part's 32-byte content hash (blake3),
///   which the writer carries in the same slot an L1 `SnapshotEntry` already
///   repurposes for a 32-byte hash. Records must be sorted by that hash alone
///   and unique under it; the remaining tuple fields are informational.
///
/// Requiring the exact width per version is what makes a v1 object encountered
/// under field 13, or a v2 object under field 11, self-evidently wrong to a
/// reader that has already established which version it expects.
fn validate_segments(
    segments: &[ColumnStatsSegment],
    version: u8,
) -> Result<(), SnapshotFormatError> {
    let part_keyed = version >= 2;
    let expected_writer_id_len = if part_keyed { 32 } else { 16 };
    for (i, segment) in segments.iter().enumerate() {
        if segment.writer_id.len() != expected_writer_id_len {
            return Err(SnapshotFormatError::ColumnStatsBadFieldLen {
                field: "writer_id",
                expected: expected_writer_id_len,
                actual: segment.writer_id.len(),
            });
        }
        if i > 0 {
            let ordering = if part_keyed {
                // v2: the whole key is the content hash carried in writer_id.
                segments[i - 1]
                    .writer_id
                    .as_slice()
                    .cmp(segment.writer_id.as_slice())
            } else {
                segment_key(&segments[i - 1]).cmp(&segment_key(segment))
            };
            match ordering {
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

    // A column with no non-null values has nothing to sum, so the only exact
    // sum is zero. Checked here rather than inside the dictionary branch
    // below, because a record with `dictionary_present = false` never reaches
    // that branch: without this, `non_null_count = 0` with `sum = Some(k)`
    // validates, and `LogsScanExec::declared_column_sum` then folds `k` into
    // the cross-segment total while adding nothing to the count. The SUM would
    // be wrong by `k` and the AVG wrong in both terms — a wrong answer where
    // the contract is an exact answer or a decline.
    if column.non_null_count == 0 && column.sum.is_some_and(|s| s != 0) {
        return Err(SnapshotFormatError::ColumnStatsSumWithoutValues {
            name: column.name.clone(),
            sum: column.sum.unwrap_or(0),
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

    /// An all-null column carrying a non-zero sum is rejected at encode.
    ///
    /// This shape reaches neither the dictionary cross-check (no dictionary)
    /// nor the non-integer check (it is I64), so before this guard it
    /// validated. `LogsScanExec::declared_column_sum` folds `sum` and
    /// `non_null_count` from each segment independently, so such a record adds
    /// to the running total while adding nothing to the count: a multi-segment
    /// SUM comes back wrong by that amount, and AVG wrong in both terms. The
    /// contract is an exact answer or a decline, never a wrong one.
    #[test]
    fn sum_without_non_null_values_rejected() {
        let mut seg = segment(1, 0, 1);
        seg.columns[0].dictionary_present = false;
        seg.columns[0].dictionary = vec![];
        seg.columns[0].non_null_count = 0;
        // An all-null column carries no extrema either (checked above this
        // guard), so clear them: the record must be invalid for exactly one
        // reason, or the test would pass on the wrong error.
        seg.columns[0].min = None;
        seg.columns[0].max = None;
        seg.columns[0].sum = Some(5);
        let err = encode_column_stats([0x11; 16], 3, vec![vec![0x22; 32]], &[seg.clone()])
            .expect_err("an all-null column with a non-zero sum must be rejected");
        assert!(
            matches!(
                &err,
                SnapshotFormatError::ColumnStatsSumWithoutValues { sum, .. } if *sum == 5
            ),
            "{err:?}"
        );

        // Zero is the exact sum of no values, so it stays valid: the guard
        // rejects an impossible total, not the absence of one.
        seg.columns[0].sum = Some(0);
        encode_column_stats([0x11; 16], 3, vec![vec![0x22; 32]], &[seg.clone()])
            .expect("an all-null column may carry sum 0");

        // And an absent sum is likewise fine.
        seg.columns[0].sum = None;
        encode_column_stats([0x11; 16], 3, vec![vec![0x22; 32]], &[seg])
            .expect("an all-null column may carry no sum");
    }

    /// The regression that matters for ADR-0942 A1: the existing L0 path must
    /// stay byte-for-byte readable after the version split. A v1 object decodes
    /// with EXACT expected field values, not merely `is_ok()`.
    #[test]
    fn v1_object_decodes_with_exact_field_values() {
        let seg = segment(1, 0, 1);
        let bytes = encode_column_stats(
            [0x11; 16],
            3,
            vec![vec![0x22; 32]],
            std::slice::from_ref(&seg),
        )
        .expect("v1 encodes");
        // The public writer stamps the write version, which is 1 in A1.
        assert_eq!(bytes[4], 1, "envelope version byte is the v1 write version");
        let decoded =
            decode_column_stats(&bytes, &ColumnStatsLimits::default()).expect("v1 decodes");
        assert_eq!(decoded.header.format_version, 1);
        assert_eq!(decoded.header.tenant_hash, vec![0x11; 16]);
        assert_eq!(decoded.header.signal, 3);
        assert_eq!(decoded.header.part_blake3, vec![vec![0x22; 32]]);
        assert_eq!(decoded.header.segment_count, 1);
        assert_eq!(decoded.segments.len(), 1);
        let out = &decoded.segments[0];
        assert_eq!(out.ingest_hour_bucket, 1);
        assert_eq!(out.shard, 0);
        assert_eq!(out.writer_id, vec![0xAA; 16]);
        assert_eq!(out.writer_epoch, 1);
        assert_eq!(out.writer_seq, 1);
        assert_eq!(out.columns.len(), 1);
        let col = &out.columns[0];
        assert_eq!(col.name, "AdvEngineID");
        assert_eq!(col.declared_type, 2);
        assert_eq!(col.non_null_count, 10);
        assert_eq!(col.null_count, 0);
        assert_eq!(col.min, Some(i64_value(0)));
        assert_eq!(col.max, Some(i64_value(9)));
        assert!(col.dictionary_present);
        assert_eq!(col.dictionary.len(), 10);
        assert_eq!(col.sum, Some(45));
    }

    /// ADR-0942: a v2-stamped object must decode today, even though nothing
    /// writes one, so A2's writer and this decoder cannot disagree the moment
    /// v2 first appears. Constructed directly via the versioned framing helper
    /// (no v2 writer needed). This is also the test the "prove-the-test"
    /// demonstration flips the decoder to break: change the `:115`
    /// `column_stats_version_accepted(version)` membership check back to
    /// `version != COLUMN_STATS_WRITE_VERSION` and this fails with
    /// `ColumnStatsUnsupportedVersion(2)`.
    #[test]
    fn v2_stamped_object_decodes() {
        // A v2 record is keyed by its covered part's content hash, carried in
        // writer_id as 32 bytes.
        let mut seg = segment(1, 0, 1);
        seg.writer_id = vec![0x77; 32];
        let bytes = encode_column_stats_versioned(
            2,
            [0x11; 16],
            3,
            vec![vec![0x22; 32]],
            std::slice::from_ref(&seg),
        )
        .expect("v2 framing encodes");
        assert_eq!(bytes[4], 2, "envelope version byte is v2");
        let decoded =
            decode_column_stats(&bytes, &ColumnStatsLimits::default()).expect("v2 decodes");
        assert_eq!(decoded.header.format_version, 2);
        assert_eq!(decoded.segments, vec![seg]);
    }

    /// v2 keys by the content hash carried in writer_id, not the rest of the
    /// tuple: two records sharing every other tuple field but carrying distinct
    /// content hashes are a valid v2 object with two distinct records (the
    /// collision the v1 tuple key could not represent for L1, where the reader
    /// side has no distinguishing writer identity). Codec-level analogue of the
    /// fold test's "two L1 parts of one bucket produce two distinct records".
    #[test]
    fn v2_distinct_content_hash_round_trips() {
        let mut a = segment(1, 0, 1);
        a.writer_id = vec![0xA0; 32];
        let mut b = segment(1, 0, 1); // identical remaining tuple to `a`
        b.writer_id = vec![0xB0; 32]; // sorts after `a`
        let bytes =
            encode_column_stats_v2([0x11; 16], 3, vec![vec![0x22; 32]], &[a.clone(), b.clone()])
                .expect("two distinct-part records encode under v2");
        let decoded = decode_column_stats(&bytes, &ColumnStatsLimits::default()).expect("decodes");
        assert_eq!(decoded.segments, vec![a, b]);
        assert_eq!(decoded.header.format_version, 2);
    }

    /// A v2 record whose writer_id is not the 32-byte content hash (here the
    /// 16-byte v1 uuid width) is fail-closed rejected: a reader could not bind
    /// it, and a v1 object mislabeled v2 must not pass.
    #[test]
    fn v2_record_with_v1_width_writer_id_rejected() {
        let seg = segment(1, 0, 1); // writer_id is the 16-byte v1 uuid
        let err = encode_column_stats_v2([0x11; 16], 3, vec![vec![0x22; 32]], &[seg])
            .expect_err("a v2 record with a 16-byte writer_id is rejected");
        assert_eq!(
            err,
            SnapshotFormatError::ColumnStatsBadFieldLen {
                field: "writer_id",
                expected: 32,
                actual: 16,
            }
        );
    }

    /// Two v2 records with the identical content hash are a duplicate part
    /// binding, rejected the same way a v1 duplicate tuple is.
    #[test]
    fn v2_duplicate_content_hash_rejected() {
        let mut a = segment(1, 0, 1);
        a.writer_id = vec![0xC0; 32];
        let mut b = segment(2, 0, 9); // different remaining tuple, same hash
        b.writer_id = vec![0xC0; 32];
        let err = encode_column_stats_v2([0x11; 16], 3, vec![vec![0x22; 32]], &[a, b])
            .expect_err("duplicate content hash rejected");
        assert_eq!(err, SnapshotFormatError::ColumnStatsDuplicateSegment);
    }

    /// v2 records not sorted by their content hash are rejected: both the
    /// encoder (defensively, over caller input) and the decoder (over
    /// untrusted bytes) enforce the order; neither imposes it.
    #[test]
    fn v2_unsorted_by_content_hash_rejected() {
        let mut a = segment(1, 0, 1);
        a.writer_id = vec![0xF0; 32];
        let mut b = segment(1, 0, 2);
        b.writer_id = vec![0x10; 32]; // sorts before `a`
        let err = encode_column_stats_v2([0x11; 16], 3, vec![vec![0x22; 32]], &[a, b])
            .expect_err("unsorted-by-content-hash rejected");
        assert_eq!(err, SnapshotFormatError::ColumnStatsSegmentsUnsorted);
    }

    /// A version outside the accepted set is refused with the specific typed
    /// error carrying the offending version, not merely "an error occurred".
    #[test]
    fn version_outside_accepted_set_rejected() {
        for bad in [0u8, 3, 255] {
            // A bad version selects the v2 (part) key model in the framing
            // helper (version >= 2 for 3/255) or the v1 model (0), so use a
            // 32-byte writer_id: it satisfies v2's width check for 3/255. 0 is
            // v1 mode (needs 16), handled by its own case below.
            let writer_id_len = if bad >= 2 { 32 } else { 16 };
            let mut seg = segment(1, 0, 1);
            seg.writer_id = vec![0x44; writer_id_len];
            let bytes =
                encode_column_stats_versioned(bad, [0x11; 16], 3, vec![vec![0x22; 32]], &[seg])
                    .expect("framing encodes any version byte");
            let err = decode_column_stats(&bytes, &ColumnStatsLimits::default())
                .expect_err("version outside the accepted set is rejected");
            assert_eq!(
                err,
                SnapshotFormatError::ColumnStatsUnsupportedVersion(bad),
                "version {bad} must be refused with its own version"
            );
        }
    }

    /// The accepted read set is exactly {1, 2} and nothing else across the whole
    /// u8 domain. The expectation is a hardcoded `1 | 2`, deliberately NOT
    /// `COLUMN_STATS_ACCEPTED_READ_VERSIONS.contains(..)`: adding a version to
    /// the constant later cannot silently widen what is accepted without this
    /// literal changing too.
    #[test]
    fn accepted_read_set_is_exactly_v1_and_v2() {
        for version in 0u8..=255 {
            // The framing helper validates in the version's key model, so give
            // writer_id the width that model requires (v1: 16, v2+: 32).
            // Acceptance is then decided by the decode version gate, which this
            // test pins.
            let writer_id_len = if version >= 2 { 32 } else { 16 };
            let mut seg = segment(1, 0, 1);
            seg.writer_id = vec![0x44; writer_id_len];
            let bytes =
                encode_column_stats_versioned(version, [0x11; 16], 3, vec![vec![0x22; 32]], &[seg])
                    .expect("framing encodes any version byte");
            let decoded = decode_column_stats(&bytes, &ColumnStatsLimits::default());
            let expected_accept = matches!(version, 1 | 2);
            assert_eq!(
                decoded.is_ok(),
                expected_accept,
                "version {version} acceptance must match the hardcoded {{1, 2}} set"
            );
            if !expected_accept {
                assert_eq!(
                    decoded.expect_err("rejected"),
                    SnapshotFormatError::ColumnStatsUnsupportedVersion(version)
                );
            }
        }
    }

    /// A header whose self-declared `format_version` disagrees with its accepted
    /// envelope version byte is rejected: a v2 envelope must not carry a v1
    /// header. Built by decoding a v2 object, rewriting only the header's
    /// `format_version` to 1, and re-stamping both CRCs so the object is
    /// otherwise well-formed and the version disagreement is the sole defect.
    #[test]
    fn header_envelope_version_disagreement_rejected() {
        let seg = segment(1, 0, 1);
        // Encode a v2 object, then rebuild the envelope with the header claiming
        // v1 while the envelope byte stays v2.
        let header = ColumnStatsHeader {
            format_version: 1, // disagrees with the v2 envelope byte below
            tenant_hash: vec![0x11; 16],
            signal: 3,
            part_blake3: vec![vec![0x22; 32]],
            segment_count: 1,
            body_uncompressed_len: seg.encode_length_delimited_to_vec().len() as u64,
        };
        let header_bytes = header.encode_to_vec();
        let segments_raw = seg.encode_length_delimited_to_vec();
        let body = zstd::bulk::compress(&segments_raw, ZSTD_LEVEL).expect("compress");
        let mut out = Vec::new();
        out.extend_from_slice(&COLUMN_STATS_MAGIC);
        out.push(2); // v2 envelope
        out.extend_from_slice(&COLUMN_STATS_RESERVED);
        out.extend_from_slice(&(header_bytes.len() as u32).to_le_bytes());
        out.extend_from_slice(&header_bytes);
        let header_crc = crc32c::crc32c(&out);
        out.extend_from_slice(&(body.len() as u64).to_le_bytes());
        out.extend_from_slice(&body);
        out.extend_from_slice(&crc32c::crc32c(&body).to_le_bytes());
        out.extend_from_slice(&header_crc.to_le_bytes());

        let err = decode_column_stats(&out, &ColumnStatsLimits::default())
            .expect_err("header/envelope version disagreement is rejected");
        assert_eq!(
            err,
            SnapshotFormatError::ColumnStatsHeaderVersionMismatch {
                header: 1,
                envelope: 2,
            }
        );
    }

    /// A truncated v2 envelope produces a typed error, never a panic and never
    /// wrong data: every prefix of a well-formed v2 object is rejected.
    #[test]
    fn truncated_v2_envelope_is_typed_error() {
        let mut seg = segment(1, 0, 1);
        seg.writer_id = vec![0x77; 32];
        let bytes = encode_column_stats_versioned(2, [0x11; 16], 3, vec![vec![0x22; 32]], &[seg])
            .expect("v2 framing encodes");
        for len in 0..bytes.len() {
            // Must return a typed error rather than panic; the whole object at
            // full length is covered by `v2_stamped_object_decodes`.
            let _: SnapshotFormatError =
                decode_column_stats(&bytes[..len], &ColumnStatsLimits::default()).expect_err(
                    "a truncated envelope must be a typed error, not Ok and not a panic",
                );
        }
    }

    /// A corrupted body under a v2 envelope is caught by the body CRC, a typed
    /// error rather than a decode of wrong data.
    #[test]
    fn corrupt_v2_body_is_typed_error() {
        let mut seg = segment(1, 0, 1);
        seg.writer_id = vec![0x77; 32];
        let mut bytes =
            encode_column_stats_versioned(2, [0x11; 16], 3, vec![vec![0x22; 32]], &[seg])
                .expect("v2 framing encodes");
        // Flip a byte inside the compressed body region (past the header,
        // before the trailing CRCs).
        let mid = bytes.len() / 2;
        bytes[mid] ^= 0xFF;
        let err = decode_column_stats(&bytes, &ColumnStatsLimits::default())
            .expect_err("a corrupted body is rejected");
        assert!(
            matches!(
                err,
                SnapshotFormatError::ColumnStatsBodyCrcMismatch
                    | SnapshotFormatError::ColumnStatsHeaderCrcMismatch
                    | SnapshotFormatError::Decompress(_)
                    | SnapshotFormatError::ColumnStatsDecompressedLenMismatch { .. }
                    | SnapshotFormatError::ColumnStatsSegmentDecode(_)
            ),
            "unexpected error variant: {err:?}"
        );
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
