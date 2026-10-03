//! Snapshot part envelope: whole-object read, no per-section access
//! protocol.
//!
//! ```text
//! magic           "RCS1" (4 bytes)
//! version         u8 = 1
//! reserved        u8[3] = 0
//! header_len      u32 LE
//! header          protobuf ravel.catalog.v1.SnapshotPartHeader
//! body_len        u64 LE
//! body            zstd(entries)   entries = length-delimited protobuf
//!                                 ravel.catalog.v1.SnapshotEntry, sorted
//! body_crc32c     u32 LE          over the compressed body bytes
//! header_crc32c   u32 LE          over magic..header inclusive
//! ```

use prost::Message;
use ravel_proto::catalog::v1::{SnapshotEntry, SnapshotPartHeader};

use super::error::SnapshotFormatError;
use super::{MAGIC, MIN_ENVELOPE_LEN, RESERVED, VERSION, ZSTD_LEVEL};
use crate::snapshot_format::PartLimits;

/// A decoded, fully validated snapshot part.
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedPart {
    pub header: SnapshotPartHeader,
    pub entries: Vec<SnapshotEntry>,
}

/// Encodes a snapshot part. Validates `entries` against the same rules
/// `decode_part` enforces (sort order, no duplicates, field lengths,
/// watermark bound, level), mirroring RSEG writer's defensive-validation
/// precedent: a part this function writes can never fail its own decode.
pub fn encode_part(
    tenant_hash: [u8; 16],
    signal: u32,
    shard_count: u32,
    watermark_hour: u32,
    entries: &[SnapshotEntry],
) -> Result<Vec<u8>, SnapshotFormatError> {
    // A single-part v1 part is the special case of an hour-partitioned part
    // whose min_hour is the epoch floor 0. Byte-for-byte identical to what
    // this function has always written, so every existing single-part object
    // and its content address are unchanged.
    encode_part_ranged(tenant_hash, signal, shard_count, 0, watermark_hour, entries)
}

/// Encodes an hour-partitioned snapshot part covering `[min_hour,
/// watermark_hour]` (ADR-0063 section 1, T2). Identical envelope to
/// [`encode_part`] but stamps a real `min_hour` into the header and validates
/// every entry against the full `[min_hour, watermark_hour]` range, so a part
/// this function writes can never fail its own `decode_part`
/// (`entry.ingest_hour_bucket >= min_hour` and `<= watermark_hour`). The fold
/// uses `min_hour = 0` for the single-part case (via [`encode_part`]) and the
/// covering part's own first hour for each part of a multi-part head.
pub fn encode_part_ranged(
    tenant_hash: [u8; 16],
    signal: u32,
    shard_count: u32,
    min_hour: u32,
    watermark_hour: u32,
    entries: &[SnapshotEntry],
) -> Result<Vec<u8>, SnapshotFormatError> {
    validate_entries(entries, min_hour, watermark_hour)?;

    let mut entries_raw = Vec::new();
    for entry in entries {
        entries_raw.extend_from_slice(&entry.encode_length_delimited_to_vec());
    }
    let entries_uncompressed_len = entries_raw.len() as u64;

    let body = zstd::bulk::compress(&entries_raw, ZSTD_LEVEL)
        .map_err(|e| SnapshotFormatError::Compress(e.to_string()))?;

    let header = SnapshotPartHeader {
        format_version: u32::from(VERSION),
        tenant_hash: tenant_hash.to_vec(),
        signal,
        shard_count,
        watermark_hour,
        entry_count: entries.len() as u64,
        entries_uncompressed_len,
        min_hour,
    };
    let header_bytes = header.encode_to_vec();
    let header_len =
        u32::try_from(header_bytes.len()).map_err(|_| SnapshotFormatError::HeaderTooLarge)?;

    let mut out = Vec::with_capacity(MIN_ENVELOPE_LEN + header_bytes.len() + body.len());
    out.extend_from_slice(&MAGIC);
    out.push(VERSION);
    out.extend_from_slice(&RESERVED);
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

/// Reads only a part's header, without checking either crc or touching the
/// body, so a caller can reserve `entries_uncompressed_len` before
/// [`decode_part`] decompresses it. The caller must already have verified
/// `bytes` against the part ref's blake3; every failure is still a typed
/// error. `decode_part` re-reads and fully validates the same header, so this
/// never widens what is accepted.
pub fn decode_part_header(bytes: &[u8]) -> Result<SnapshotPartHeader, SnapshotFormatError> {
    if bytes.len() < MIN_ENVELOPE_LEN {
        return Err(SnapshotFormatError::TooSmall { size: bytes.len() });
    }
    let mut pos = 0usize;
    let magic = take_array::<4>(bytes, &mut pos)?;
    if magic != MAGIC {
        return Err(SnapshotFormatError::BadMagic);
    }
    let version = take_bytes(bytes, &mut pos, 1)?[0];
    if version != VERSION {
        return Err(SnapshotFormatError::UnsupportedVersion(version));
    }
    let reserved = take_array::<3>(bytes, &mut pos)?;
    if reserved != RESERVED {
        return Err(SnapshotFormatError::ReservedNonZero);
    }
    let header_len = take_u32_le(bytes, &mut pos)?;
    let header_bytes = take_bytes(bytes, &mut pos, to_usize(header_len)?)?;
    SnapshotPartHeader::decode(header_bytes)
        .map_err(|e| SnapshotFormatError::HeaderDecode(e.to_string()))
}

/// Decodes and fully validates a snapshot part. Every byte is untrusted;
/// every failure is a typed error, never a panic.
pub fn decode_part(bytes: &[u8], limits: &PartLimits) -> Result<DecodedPart, SnapshotFormatError> {
    if bytes.len() < MIN_ENVELOPE_LEN {
        return Err(SnapshotFormatError::TooSmall { size: bytes.len() });
    }

    let mut pos = 0usize;
    let magic = take_array::<4>(bytes, &mut pos)?;
    if magic != MAGIC {
        return Err(SnapshotFormatError::BadMagic);
    }
    let version = take_bytes(bytes, &mut pos, 1)?[0];
    if version != VERSION {
        return Err(SnapshotFormatError::UnsupportedVersion(version));
    }
    let reserved = take_array::<3>(bytes, &mut pos)?;
    if reserved != RESERVED {
        return Err(SnapshotFormatError::ReservedNonZero);
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
        return Err(SnapshotFormatError::TrailingBytes);
    }
    if header_crc_stored != header_crc_expected {
        return Err(SnapshotFormatError::HeaderCrcMismatch);
    }
    if body_crc_stored != crc32c::crc32c(body) {
        return Err(SnapshotFormatError::BodyCrcMismatch);
    }

    let header = SnapshotPartHeader::decode(header_bytes)
        .map_err(|e| SnapshotFormatError::HeaderDecode(e.to_string()))?;
    if header.format_version != u32::from(VERSION) {
        return Err(SnapshotFormatError::HeaderVersionMismatch {
            header: header.format_version,
            envelope: VERSION,
        });
    }
    if header.tenant_hash.len() != 16 {
        return Err(SnapshotFormatError::BadTenantHashLen(
            header.tenant_hash.len(),
        ));
    }
    if header.entries_uncompressed_len > limits.max_snapshot_part_bytes {
        return Err(SnapshotFormatError::DecompressedTooLarge {
            declared: header.entries_uncompressed_len,
            cap: limits.max_snapshot_part_bytes,
        });
    }
    let capacity = to_usize(header.entries_uncompressed_len)?;
    let decompressed = zstd::bulk::decompress(body, capacity)
        .map_err(|e| SnapshotFormatError::Decompress(e.to_string()))?;
    if decompressed.len() as u64 != header.entries_uncompressed_len {
        return Err(SnapshotFormatError::DecompressedLenMismatch {
            expected: header.entries_uncompressed_len,
            actual: decompressed.len() as u64,
        });
    }

    let mut entries = Vec::new();
    let mut cursor: &[u8] = &decompressed[..];
    while !cursor.is_empty() {
        let entry = SnapshotEntry::decode_length_delimited(&mut cursor)
            .map_err(|e| SnapshotFormatError::EntryDecode(e.to_string()))?;
        entries.push(entry);
    }
    if entries.len() as u64 != header.entry_count {
        return Err(SnapshotFormatError::EntryCountMismatch {
            expected: header.entry_count,
            actual: entries.len() as u64,
        });
    }
    // A header claiming an empty or inverted hour range is malformed
    // (ADR-0063). Checked before the per-entry bounds so the range itself is
    // known sane first.
    if header.min_hour > header.watermark_hour {
        return Err(SnapshotFormatError::MinHourExceedsWatermark {
            min_hour: header.min_hour,
            watermark: header.watermark_hour,
        });
    }
    validate_entries(&entries, header.min_hour, header.watermark_hour)?;

    Ok(DecodedPart { header, entries })
}

/// Sort/uniqueness/field validation shared by `encode_part` (defensive
/// check of caller input) and `decode_part` (untrusted-bytes check): the
/// "Sort order, mandatory and validated" rule.
fn validate_entries(
    entries: &[SnapshotEntry],
    min_hour: u32,
    watermark_hour: u32,
) -> Result<(), SnapshotFormatError> {
    for (i, entry) in entries.iter().enumerate() {
        // Level 0 is an L0 commit; level 1 is a compaction (L1) part.
        // An L0 entry's writer_id is
        // the 16-byte flush writer id; an L1 entry has no writer identity, so
        // its writer_id slot instead carries the parent compaction record's
        // 32-byte input_set_hash (fold.rs `build_l1_snapshot_entry`). A level
        // beyond these is reserved: a reader rejects a level it does not
        // understand rather than guess its layout.
        let writer_id_expected_len = match entry.level {
            0 => 16,
            1 => 32,
            other => return Err(SnapshotFormatError::UnsupportedLevel(other)),
        };
        if entry.writer_id.len() != writer_id_expected_len {
            return Err(SnapshotFormatError::BadFieldLen {
                field: "writer_id",
                expected: writer_id_expected_len,
                actual: entry.writer_id.len(),
            });
        }
        if entry.content_hash.len() != 32 {
            return Err(SnapshotFormatError::BadFieldLen {
                field: "content_hash",
                expected: 32,
                actual: entry.content_hash.len(),
            });
        }
        if entry.ingest_hour_bucket > watermark_hour {
            return Err(SnapshotFormatError::WatermarkExceeded {
                hour: entry.ingest_hour_bucket,
                watermark: watermark_hour,
            });
        }
        if entry.ingest_hour_bucket < min_hour {
            return Err(SnapshotFormatError::BelowMinHour {
                hour: entry.ingest_hour_bucket,
                min_hour,
            });
        }
        if i > 0 {
            match entry_key(&entries[i - 1]).cmp(&entry_key(entry)) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Equal => return Err(SnapshotFormatError::DuplicateEntry),
                std::cmp::Ordering::Greater => return Err(SnapshotFormatError::EntriesUnsorted),
            }
        }
    }
    Ok(())
}

fn entry_key(entry: &SnapshotEntry) -> (u32, u32, &[u8], u64, u64) {
    (
        entry.ingest_hour_bucket,
        entry.shard,
        entry.writer_id.as_slice(),
        entry.writer_epoch,
        entry.writer_seq,
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
    use proptest::prelude::*;

    use super::*;

    fn entry(seq: u64) -> SnapshotEntry {
        SnapshotEntry {
            level: 0,
            ingest_hour_bucket: 5,
            writer_id: vec![0xAA; 16],
            writer_epoch: 1,
            writer_seq: seq,
            content_hash: vec![seq as u8; 32],
            object_size: 100,
            ..SnapshotEntry::default()
        }
    }

    /// The header peek reads exactly the header `decode_part` validates, so
    /// the length the resolve reserves is the length the decode allocates.
    #[test]
    fn header_peek_matches_the_full_decode() {
        let bytes = encode_part([7; 16], 1, 1, 10, &[entry(1), entry(2)]).expect("encode");
        let peeked = decode_part_header(&bytes).expect("peek");
        let decoded = decode_part(&bytes, &PartLimits::default()).expect("decode");
        assert_eq!(peeked, decoded.header);
        assert!(peeked.entries_uncompressed_len > 0);
    }

    /// The level match in `validate_entries` accepts every level up to
    /// `MAX_ENTRY_LEVEL`, the bound `SnapshotFormatError::is_newer_format_version`
    /// reads, and refuses the next one.
    #[test]
    fn entry_levels_accepted_end_at_max_entry_level() {
        use super::super::error::MAX_ENTRY_LEVEL;

        let validate = |level: u32| {
            let mut e = entry(1);
            e.level = level;
            validate_entries(&[e], 0, 10)
        };
        for level in 0..=MAX_ENTRY_LEVEL {
            assert!(
                !matches!(
                    validate(level),
                    Err(SnapshotFormatError::UnsupportedLevel(_))
                ),
                "level {level} is refused as unsupported"
            );
        }
        assert_eq!(
            validate(MAX_ENTRY_LEVEL + 1),
            Err(SnapshotFormatError::UnsupportedLevel(MAX_ENTRY_LEVEL + 1))
        );
    }

    /// The header peek returns on every truncation of a valid part, never
    /// panics.
    #[test]
    fn header_peek_of_any_truncation_never_panics() {
        let bytes = encode_part([7; 16], 1, 1, 10, &[entry(1)]).expect("encode");
        for len in 0..bytes.len() {
            let _ = decode_part_header(&bytes[..len]);
        }
    }

    proptest! {
        #[test]
        fn header_peek_of_arbitrary_bytes_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..256)) {
            let _ = decode_part_header(&bytes);
        }
    }
}
