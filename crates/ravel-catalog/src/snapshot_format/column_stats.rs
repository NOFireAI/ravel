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

use prost::Message;
use ravel_proto::catalog::v1::{ColumnStatsHeader, ColumnStatsSegment};

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
/// bytes check).
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
    }
    Ok(())
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
    use ravel_proto::catalog::v1::{ColumnStat, ColumnStatsSegment, ColumnValue};

    use super::*;

    fn segment(hour: u32, shard: u32, seq: u64) -> ColumnStatsSegment {
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
                min: Some(ColumnValue {
                    kind: Some(ravel_proto::catalog::v1::column_value::Kind::I64(0)),
                }),
                max: Some(ColumnValue {
                    kind: Some(ravel_proto::catalog::v1::column_value::Kind::I64(9)),
                }),
                dictionary_present: true,
                dictionary: vec![],
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
