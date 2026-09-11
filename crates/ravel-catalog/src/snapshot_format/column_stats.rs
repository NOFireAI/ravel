//! Column-statistics envelope: whole-object read, no per-section access
//! protocol. ADR-0850.
//!
//! ```text
//! magic           "RCST" (4 bytes)
//! version         u8 = 1 (ADR-0850, L0-tuple keyed), 2 (ADR-0942, part-keyed)
//!                      or 3 (ADR-1413, per-part, content-hash keyed)
//! reserved        u8[3] = 0
//! header_len      u32 LE
//! header          protobuf ravel.catalog.v1.ColumnStatsHeader
//! body_len        u64 LE
//! body            zstd(segments)  segments = length-delimited protobuf
//!                                 ravel.catalog.v1.ColumnStatsSegment,
//!                                 sorted by (ingest_hour_bucket, shard,
//!                                 writer_id, writer_epoch, writer_seq) in v1,
//!                                 by writer_id (the part content hash) in v2
//!                                 and v3
//! body_crc32c     u32 LE          over the compressed body bytes
//! header_crc32c   u32 LE          over magic..header inclusive
//! ```
//!
//! Three envelope versions coexist during the ADR-1413 dual-publish window
//! (which itself extends the ADR-0942 dual-publish window). The
//! `ColumnStatsSegment` record shape is frozen and shared; the key model is the
//! version's. v1 keys each record by the five-field identity tuple (writer_id is
//! the 16-byte flush-writer uuid) and covers L0 only. v2 keys by the covered
//! part's content hash, which the writer carries in the `writer_id` slot as 32
//! bytes (the same slot an L1 `SnapshotEntry` already repurposes for a 32-byte
//! hash), and covers L0 and L1 uniformly, over the WHOLE tenant/signal. v3
//! reuses v2's content-hash keying exactly, but the header's `part_blake3`
//! names exactly one part: the object covers only that part's segments, so its
//! size scales with one part rather than the whole tenant. The keying is
//! self-describing in the version byte, so an object read outside its head ref
//! declares which key model it carries.
//!
//! Deliberately reuses `part.rs`'s plain length-delimited-protobuf body
//! convention rather than `postings.rs`'s hand-rolled varint dictionary:
//! that dictionary earns its complexity for a huge flat cross-segment name
//! space, which per-segment column statistics don't have.

use std::collections::HashSet;

use prost::Message;
use ravel_proto::catalog::v1::column_value::Kind;
use ravel_proto::catalog::v1::{ColumnStat, ColumnStatsHeader, ColumnStatsSegment, ColumnValue};

#[cfg(test)]
use super::COLUMN_STATS_WRITE_VERSION;
use super::error::SnapshotFormatError;
use super::{
    COLUMN_STATS_MAGIC, COLUMN_STATS_RESERVED, MIN_COLUMN_STATS_ENVELOPE_LEN, ZSTD_LEVEL,
    column_stats_version_accepted,
};
use crate::snapshot_format::ColumnStatsLimits;

/// A decoded, fully validated column-statistics object.
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedColumnStats {
    pub header: ColumnStatsHeader,
    pub segments: Vec<ColumnStatsSegment>,
}

/// Encodes a **v1** (ADR-0850, L0-tuple-keyed) column-statistics object, the
/// former `SnapshotHead.column_stats` (field 11) artifact. Validates `segments`
/// against the same rules `decode_column_stats` enforces for v1 (writer_id
/// width, tuple sort order, no duplicate identity), mirroring `encode_part`'s
/// defensive-validation precedent: an object this function writes can never
/// fail its own decode.
///
/// Retired format. ADR-1413 decision 6 removed the whole-object publish, so no
/// production path calls this: it survives to build v1 bytes for the tests that
/// prove the decoder now rejects them, the read set being exactly `{3}`. Stamps
/// envelope version 1 explicitly, never [`COLUMN_STATS_WRITE_VERSION`].
pub fn encode_column_stats(
    tenant_hash: [u8; 16],
    signal: u32,
    part_blake3: Vec<Vec<u8>>,
    segments: &[ColumnStatsSegment],
) -> Result<Vec<u8>, SnapshotFormatError> {
    encode_column_stats_versioned(1, tenant_hash, signal, part_blake3, segments)
}

/// Encodes a **v2** (ADR-0942, part-hash-keyed) column-statistics object, the
/// former `SnapshotHead.column_stats_part` (field 13) artifact. Each segment
/// record must carry its covered part's content hash (blake3) in its
/// `writer_id` slot as 32 bytes; records are sorted and deduplicated by that
/// hash, not the five-field identity tuple, so L0 and L1 parts are named
/// uniformly and two L1 parts of one bucket never collide.
///
/// Retired format, on the same ADR-1413 decision 6 as [`encode_column_stats`],
/// and `#[cfg(test)]` because nothing outside the tests that prove the decoder
/// rejects v2 constructs one.
#[cfg(test)]
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

/// The uncompressed, length-delimited protobuf concatenation of `segments`:
/// exactly the bytes [`frame_column_stats`] compresses, and exactly what a
/// pre-encode ceiling check must measure to agree with the encoder. Shared by
/// [`encode_column_stats_v3`]'s ceiling check and the fold's degrade loop
/// (`ravel_catalog::fold`) so the two can never drift apart on what "over
/// ceiling" means.
pub fn column_stats_segments_concat(segments: &[ColumnStatsSegment]) -> Vec<u8> {
    let mut segments_raw = Vec::new();
    for segment in segments {
        segments_raw.extend_from_slice(&segment.encode_length_delimited_to_vec());
    }
    segments_raw
}

/// Encodes a **v3** (ADR-1413, per-part, content-hash-keyed) column-statistics
/// object, referenced by `SnapshotPartRef.column_stats` (field 7). Unlike
/// [`encode_column_stats_v2`], the header's `part_blake3` names exactly the
/// one part this object covers, and `segments` must be exactly that part's
/// segments (each still carrying its covered part's content hash in
/// `writer_id`, v2 semantics).
///
/// Refuses to encode, returning
/// [`SnapshotFormatError::ColumnStatsPartOverBound`], when the segments'
/// concatenated uncompressed length would exceed `ceiling_bytes` (ADR-1413
/// decision 3: `DEFAULT_MAX_COLUMN_STATS_BYTES`, the same fixed ceiling the
/// v3 reader enforces), checked before compression. The fold degrades before
/// ever calling this with a part still over ceiling (ADR-1413 decision 4): by
/// the time this refuses, no dictionary is left to drop, so the caller must
/// treat this as a hard failure for the part, never publish a truncated or
/// over-ceiling object, and never silently skip it.
pub fn encode_column_stats_v3(
    tenant_hash: [u8; 16],
    signal: u32,
    part_blake3: [u8; 32],
    segments: &[ColumnStatsSegment],
    ceiling_bytes: u64,
) -> Result<Vec<u8>, SnapshotFormatError> {
    validate_segments(segments, 3)?;
    // Concatenated once: measured for the ceiling, then handed to the
    // framing step as the body it compresses.
    let segments_raw = column_stats_segments_concat(segments);
    let declared = segments_raw.len() as u64;
    if declared > ceiling_bytes {
        return Err(SnapshotFormatError::ColumnStatsPartOverBound {
            declared,
            ceiling: ceiling_bytes,
        });
    }
    frame_column_stats_body(
        3,
        tenant_hash,
        signal,
        vec![part_blake3.to_vec()],
        segments.len() as u64,
        &segments_raw,
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
    frame_column_stats(version, tenant_hash, signal, part_blake3, segments)
}

/// Envelope framing with NO validation: the byte layout only. Split out of
/// [`encode_column_stats_versioned`] so a test can build a well-framed object
/// carrying a record that `validate_segments` would refuse, which is the only
/// way to reach `decode_column_stats`'s own validation call with hostile input
/// (every public writer validates first, so an encoder-side test proves
/// nothing about the decoder).
fn frame_column_stats(
    version: u8,
    tenant_hash: [u8; 16],
    signal: u32,
    part_blake3: Vec<Vec<u8>>,
    segments: &[ColumnStatsSegment],
) -> Result<Vec<u8>, SnapshotFormatError> {
    let segments_raw = column_stats_segments_concat(segments);
    frame_column_stats_body(
        version,
        tenant_hash,
        signal,
        part_blake3,
        segments.len() as u64,
        &segments_raw,
    )
}

/// The framing step proper, over an already-concatenated body (the bytes
/// [`column_stats_segments_concat`] produces for `segment_count` segments),
/// so a caller that measured the body for a ceiling check hands those same
/// bytes over instead of encoding the segments a second time.
fn frame_column_stats_body(
    version: u8,
    tenant_hash: [u8; 16],
    signal: u32,
    part_blake3: Vec<Vec<u8>>,
    segment_count: u64,
    segments_raw: &[u8],
) -> Result<Vec<u8>, SnapshotFormatError> {
    let body_uncompressed_len = segments_raw.len() as u64;

    let body = zstd::bulk::compress(segments_raw, ZSTD_LEVEL)
        .map_err(|e| SnapshotFormatError::Compress(e.to_string()))?;

    let header = ColumnStatsHeader {
        format_version: u32::from(version),
        tenant_hash: tenant_hash.to_vec(),
        signal,
        part_blake3,
        segment_count,
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

/// A header-only envelope peek: everything [`decode_column_stats`] validates
/// up to and including the header decode and its self-consistency checks
/// (format-version/envelope agreement, tenant-hash width, the v3
/// exactly-one-part rule), but stopping before the `body_uncompressed_len`
/// ceiling check and never decompressing the body. `body_uncompressed_len`
/// is on `header`, so a caller can compare it against any ceiling it likes
/// (or none) without this function needing to know one.
///
/// Exists for `ravel-cli inspect cstat`: an object whose declared
/// `body_uncompressed_len` exceeds the decode ceiling cannot be decoded by
/// [`decode_column_stats`] at all (that is the point of the ceiling), so a
/// reader wanting to report "this object is over ceiling" needs a path that
/// answers the question without decompressing.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnStatsHeaderPeek {
    pub envelope_version: u8,
    pub header_len: u32,
    pub header: ColumnStatsHeader,
}

/// The envelope-parsing and header-validation prefix shared by
/// [`decode_column_stats_header`] and [`decode_column_stats`], returning the
/// validated peek alongside the still-compressed body slice so the full
/// decoder does not re-parse the envelope. Every check performed here never
/// requires decompressing `body`.
fn decode_column_stats_prefix(
    bytes: &[u8],
) -> Result<(ColumnStatsHeaderPeek, &[u8]), SnapshotFormatError> {
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
    // ADR-1413: a v3 object is scoped to exactly one part, so its header must
    // name exactly one part_blake3 entry. v1/v2 carry no such constraint (v1
    // ignores part_blake3 entirely, v2 names every part in the tenant).
    if version == 3 && header.part_blake3.len() != 1 {
        return Err(SnapshotFormatError::ColumnStatsV3PartBlake3CountMismatch(
            header.part_blake3.len(),
        ));
    }

    Ok((
        ColumnStatsHeaderPeek {
            envelope_version: version,
            header_len,
            header,
        },
        body,
    ))
}

/// Parses and validates a column-statistics object's envelope and header
/// WITHOUT touching any ceiling and WITHOUT decompressing the body, so an
/// object whose declared `body_uncompressed_len` is over any ceiling a
/// caller might check still yields its header rather than an error.
pub fn decode_column_stats_header(
    bytes: &[u8],
) -> Result<ColumnStatsHeaderPeek, SnapshotFormatError> {
    decode_column_stats_prefix(bytes).map(|(peek, _body)| peek)
}

/// Decodes and fully validates a column-statistics object. Every byte is
/// untrusted; every failure is a typed error, never a panic.
pub fn decode_column_stats(
    bytes: &[u8],
    limits: &ColumnStatsLimits,
) -> Result<DecodedColumnStats, SnapshotFormatError> {
    let (peek, body) = decode_column_stats_prefix(bytes)?;
    let header = peek.header;
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
    validate_segments(&segments, peek.envelope_version)?;

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
///   five-field identity tuple the records are sorted and deduplicated by.
/// - v2 (`version >= 2`): the covered part's 32-byte content hash (blake3),
///   which the writer carries in the same slot an L1 `SnapshotEntry` already
///   repurposes for a 32-byte hash. Records are sorted and deduplicated by that
///   hash alone; the remaining tuple fields are informational.
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

    validate_min_max_presence(column)?;
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

/// The min/max presence clause of the column-statistics validity predicate:
/// `min` and `max` are BOTH present when `non_null_count > 0` and BOTH absent
/// when it is zero. Both directions are wrong answers on a path that reports
/// `Precision::Exact`, in mirror-image ways:
///
/// - non-null rows with no recorded extremum makes the extremum of non-null
///   data read as exactly NULL;
/// - `non_null_count == 0` with extrema present describes no live row at all,
///   so an all-NULL column contributes a value to a MIN/MAX the scan would
///   answer NULL for.
///
/// Public because [`crate::LoadedColumnStats`] has public fields, so a reader
/// can receive `ColumnStat` records from a carrier that never passed through
/// [`decode_column_stats`] (an in-process construction, a cache, a test
/// fixture). Such a reader enforces this clause by calling this function
/// rather than restating it, so the decode boundary and the read site cannot
/// drift into disagreeing about what a usable entry is.
pub fn validate_min_max_presence(column: &ColumnStat) -> Result<(), SnapshotFormatError> {
    if column.non_null_count == 0 && (column.min.is_some() || column.max.is_some()) {
        return Err(SnapshotFormatError::ColumnStatsUnexpectedMinMax {
            name: column.name.clone(),
        });
    }
    if column.non_null_count > 0 && (column.min.is_none() || column.max.is_none()) {
        return Err(SnapshotFormatError::ColumnStatsMissingMinMax {
            name: column.name.clone(),
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
    use proptest::prelude::*;
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
        let mut a = segment(1, 0, 1);
        a.writer_id = vec![0x10; 32];
        let mut b = segment(1, 0, 2);
        b.writer_id = vec![0x20; 32];
        let mut c = segment(2, 0, 1);
        c.writer_id = vec![0x30; 32];
        let segments = vec![a, b, c];
        let bytes = encode_column_stats_v3(
            [0x11; 16],
            3,
            [0x22; 32],
            &segments,
            crate::snapshot_format::DEFAULT_MAX_COLUMN_STATS_BYTES,
        )
        .expect("encodes");
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

    /// #970: the DECODER refuses a duplicate column name, not merely the
    /// encoder. Every test above reaches `validate_columns` through
    /// `encode_column_stats`, so none of them would fail if
    /// `decode_column_stats` stopped calling `validate_segments` -- and it is
    /// the decode path that faces bytes a reader did not write. Framed here
    /// without validation so the malformed record is what decode actually
    /// receives.
    ///
    /// Prove-the-test: delete the `validate_segments(&segments, version)?;`
    /// call in `decode_column_stats` and this decode succeeds, so
    /// `expect_err` panics.
    #[test]
    fn decode_refuses_duplicate_column_name_in_unvalidated_bytes() {
        let mut seg = segment(1, 0, 1);
        seg.writer_id = vec![0xAA; 32];
        let dup = seg.columns[0].clone();
        seg.columns.push(dup);
        let bytes =
            frame_column_stats(3, [0x11; 16], 3, vec![vec![0x22; 32]], &[seg]).expect("frames");
        let err =
            decode_column_stats(&bytes, &ColumnStatsLimits::default()).expect_err("decode refuses");
        assert_eq!(
            err,
            SnapshotFormatError::ColumnStatsDuplicateColumnName {
                name: "AdvEngineID".to_string(),
            }
        );
    }

    /// #970, the mirror of the presence clause at the decode boundary: an
    /// all-NULL column (`non_null_count == 0`) carrying extrema describes no
    /// live row, and a reader that trusted it would report a value for a
    /// MIN/MAX whose true answer is NULL.
    ///
    /// Prove-the-test: delete the `validate_segments(&segments, version)?;`
    /// call in `decode_column_stats` and this decode succeeds.
    #[test]
    fn decode_refuses_min_max_with_zero_non_null_in_unvalidated_bytes() {
        let mut seg = segment(1, 0, 1);
        seg.writer_id = vec![0xAA; 32];
        seg.columns[0].non_null_count = 0;
        seg.columns[0].null_count = 10;
        seg.columns[0].dictionary = vec![];
        seg.columns[0].dictionary_present = false;
        seg.columns[0].sum = None;
        // min/max still populated: inconsistent with an all-null column.
        let bytes =
            frame_column_stats(3, [0x11; 16], 3, vec![vec![0x22; 32]], &[seg]).expect("frames");
        let err =
            decode_column_stats(&bytes, &ColumnStatsLimits::default()).expect_err("decode refuses");
        assert_eq!(
            err,
            SnapshotFormatError::ColumnStatsUnexpectedMinMax {
                name: "AdvEngineID".to_string(),
            }
        );
    }

    /// The other direction of the same clause, also through decode: non-null
    /// rows with no recorded extremum.
    ///
    /// Prove-the-test: delete the `validate_segments(&segments, version)?;`
    /// call in `decode_column_stats` and this decode succeeds.
    #[test]
    fn decode_refuses_missing_min_max_with_non_null_rows_in_unvalidated_bytes() {
        let mut seg = segment(1, 0, 1);
        seg.writer_id = vec![0xAA; 32];
        seg.columns[0].min = None;
        seg.columns[0].max = None;
        let bytes =
            frame_column_stats(3, [0x11; 16], 3, vec![vec![0x22; 32]], &[seg]).expect("frames");
        let err =
            decode_column_stats(&bytes, &ColumnStatsLimits::default()).expect_err("decode refuses");
        assert_eq!(
            err,
            SnapshotFormatError::ColumnStatsMissingMinMax {
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
        seg.writer_id = vec![0x77; 32];
        seg.columns[0].dictionary_present = false;
        seg.columns[0].dictionary = vec![];
        seg.columns[0].sum = Some(45);
        let bytes = encode_column_stats_v3(
            [0x11; 16],
            3,
            [0x22; 32],
            &[seg.clone()],
            crate::snapshot_format::DEFAULT_MAX_COLUMN_STATS_BYTES,
        )
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

    /// v2 records not sorted by their content hash are rejected (the encoder
    /// sorts; the decoder enforces).
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
        for bad in [0u8, 4, 255] {
            // A bad version selects the v2/v3 (part) key model in the framing
            // helper (version >= 2 for 4/255) or the v1 model (0), so use a
            // 32-byte writer_id: it satisfies v2's width check for 4/255. 0 is
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

    /// The accepted read set is exactly {3} and nothing else across the
    /// whole u8 domain. The expectation is a hardcoded `3`,
    /// deliberately NOT `COLUMN_STATS_ACCEPTED_READ_VERSIONS.contains(..)`:
    /// adding a version to the constant later cannot silently widen what is
    /// accepted without this literal changing too.
    #[test]
    fn accepted_read_set_is_exactly_v3() {
        for version in 0u8..=255 {
            // The framing helper validates in the version's key model, so give
            // writer_id the width that model requires (v1: 16, v2+: 32).
            // Acceptance is then decided by the decode version gate, which this
            // test pins. part_blake3 has length 1, satisfying v3's own
            // additional structural check so version alone decides acceptance.
            let writer_id_len = if version >= 2 { 32 } else { 16 };
            let mut seg = segment(1, 0, 1);
            seg.writer_id = vec![0x44; writer_id_len];
            let bytes =
                encode_column_stats_versioned(version, [0x11; 16], 3, vec![vec![0x22; 32]], &[seg])
                    .expect("framing encodes any version byte");
            let decoded = decode_column_stats(&bytes, &ColumnStatsLimits::default());
            let expected_accept = version == 3;
            assert_eq!(
                decoded.is_ok(),
                expected_accept,
                "version {version} acceptance must match the hardcoded {{3}} set"
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
    /// envelope version byte is rejected: a v3 envelope must not carry a header
    /// claiming a different version. Built by hand-framing a v3 envelope whose
    /// header's `format_version` is stamped 1, so the version disagreement is
    /// the sole defect.
    #[test]
    fn header_envelope_version_disagreement_rejected() {
        let seg = segment(1, 0, 1);
        // Hand-frame a v3 envelope, but with the header claiming v1 while the
        // envelope byte stays v3.
        let header = ColumnStatsHeader {
            format_version: 1, // disagrees with the v3 envelope byte below
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
        out.push(3); // v3 envelope
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
                envelope: 3,
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

    /// A corrupted body under a v3 envelope is caught by the body CRC, a typed
    /// error rather than a decode of wrong data.
    #[test]
    fn corrupt_v3_body_is_typed_error() {
        let mut seg = segment(1, 0, 1);
        seg.writer_id = vec![0x77; 32];
        let mut bytes =
            encode_column_stats_versioned(3, [0x11; 16], 3, vec![vec![0x22; 32]], &[seg])
                .expect("v3 framing encodes");
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
        let mut seg = segment(1, 0, 1);
        seg.writer_id = vec![0x77; 32];
        let segments = vec![seg];
        let bytes = encode_column_stats_v3(
            [0x11; 16],
            3,
            [0x22; 32],
            &segments,
            crate::snapshot_format::DEFAULT_MAX_COLUMN_STATS_BYTES,
        )
        .expect("encodes");
        let tiny_limit = ColumnStatsLimits {
            max_column_stats_bytes: 1,
        };
        let err = decode_column_stats(&bytes, &tiny_limit).expect_err("rejected");
        assert!(matches!(
            err,
            SnapshotFormatError::ColumnStatsDecompressedTooLarge { .. }
        ));
    }

    /// ADR-1413: a v3 object round-trips like v2 (content-hash keying), and
    /// its header names exactly the one part it covers.
    #[test]
    fn v3_stamped_object_round_trips() {
        let mut seg = segment(1, 0, 1);
        seg.writer_id = vec![0x77; 32];
        let part_blake3 = [0x22u8; 32];
        let bytes = encode_column_stats_v3(
            [0x11; 16],
            3,
            part_blake3,
            std::slice::from_ref(&seg),
            crate::snapshot_format::DEFAULT_MAX_COLUMN_STATS_BYTES,
        )
        .expect("fits the ceiling");
        assert_eq!(bytes[4], 3, "envelope version byte is v3");
        let decoded =
            decode_column_stats(&bytes, &ColumnStatsLimits::default()).expect("v3 decodes");
        assert_eq!(decoded.header.format_version, 3);
        assert_eq!(decoded.header.part_blake3, vec![part_blake3.to_vec()]);
        assert_eq!(decoded.segments, vec![seg]);
    }

    /// The write-time enforcement ADR-1413 decision 4 requires: a segment set
    /// whose concatenated uncompressed length exceeds the caller's ceiling is
    /// refused with the declared size and the ceiling, never truncated or
    /// silently written over budget.
    #[test]
    fn encode_v3_refuses_a_segment_over_the_ceiling() {
        let mut seg = segment(1, 0, 1);
        seg.writer_id = vec![0x88; 32];
        let declared = seg.encode_length_delimited_to_vec().len() as u64;
        let ceiling = declared - 1;
        let err = encode_column_stats_v3([0x11; 16], 3, [0x66; 32], &[seg], ceiling)
            .expect_err("a segment set over the ceiling is refused");
        assert_eq!(
            err,
            SnapshotFormatError::ColumnStatsPartOverBound { declared, ceiling }
        );
    }

    /// A v3 header naming anything other than exactly one part is
    /// structurally invalid: v3 is scoped to a single part by construction,
    /// so a reader encountering more (or fewer) than one bound part is a
    /// typed rejection, not a value it could partially trust.
    #[test]
    fn v3_decode_refuses_part_blake3_count_other_than_one() {
        let mut seg = segment(1, 0, 1);
        seg.writer_id = vec![0x33; 32];
        let bytes = encode_column_stats_versioned(
            3,
            [0x11; 16],
            3,
            vec![vec![0x22; 32], vec![0x33; 32]],
            &[seg],
        )
        .expect("framing encodes any part_blake3 length");
        let err = decode_column_stats(&bytes, &ColumnStatsLimits::default())
            .expect_err("v3 with part_blake3 length != 1 is rejected");
        assert_eq!(
            err,
            SnapshotFormatError::ColumnStatsV3PartBlake3CountMismatch(2)
        );
    }

    /// ADR-1413 (amended): the per-part ceiling IS the reader's fixed
    /// whole-object ceiling, so a v3 object at exactly the ceiling decodes,
    /// and a reader sized one byte under the object's declared uncompressed
    /// body rejects it. A small injected ceiling keeps this fast: no
    /// hundreds-of-MB fixture is needed to exercise the boundary, since the
    /// decoder's check is against `header.body_uncompressed_len`, not the
    /// constant's real-world value.
    #[test]
    fn v3_object_at_the_ceiling_decodes_and_one_byte_over_is_rejected() {
        let mut seg = segment(1, 0, 1);
        seg.writer_id = vec![0x99; 32];
        let part_blake3 = [0x55u8; 32];
        let declared = seg.encode_length_delimited_to_vec().len() as u64;
        let bytes = encode_column_stats_v3(
            [0x11; 16],
            3,
            part_blake3,
            std::slice::from_ref(&seg),
            declared,
        )
        .expect("fixture is sized to fit its own declared length exactly");

        let at_ceiling = ColumnStatsLimits {
            max_column_stats_bytes: declared,
        };
        let decoded =
            decode_column_stats(&bytes, &at_ceiling).expect("decodes at exactly the ceiling");
        assert_eq!(decoded.segments, vec![seg]);
        assert_eq!(decoded.header.part_blake3, vec![part_blake3.to_vec()]);

        let one_under = ColumnStatsLimits {
            max_column_stats_bytes: declared - 1,
        };
        let err = decode_column_stats(&bytes, &one_under)
            .expect_err("one byte over a reader's ceiling is rejected");
        assert!(matches!(
            err,
            SnapshotFormatError::ColumnStatsDecompressedTooLarge { .. }
        ));
    }

    /// A v3 segment set of arbitrary size (0..6 distinct part-hash-keyed
    /// segments), tenant hash, signal, and part hash, encoded with no
    /// per-part bound (`u64::MAX`, since the bound itself is covered by the
    /// dedicated unit tests above): `decode_column_stats(encode_column_stats_v3(x))
    /// == x`.
    fn arb_v3_segments(n: usize) -> Vec<ColumnStatsSegment> {
        (0..n)
            .map(|i| {
                let mut seg = segment(i as u32, 0, i as u64);
                seg.writer_id = vec![i as u8; 32];
                seg
            })
            .collect()
    }

    proptest! {
        #[test]
        fn v3_round_trips_for_arbitrary_valid_segment_sets(
            tenant_hash in proptest::array::uniform16(any::<u8>()),
            signal in any::<u32>(),
            part_blake3 in proptest::array::uniform32(any::<u8>()),
            n in 0usize..6,
        ) {
            let segments = arb_v3_segments(n);
            let bytes = encode_column_stats_v3(tenant_hash, signal, part_blake3, &segments, u64::MAX)
                .expect("a sorted, distinct segment set always fits an unbounded per-part bound");
            let decoded = decode_column_stats(&bytes, &ColumnStatsLimits::default())
                .expect("a freshly encoded v3 object always decodes");
            prop_assert_eq!(decoded.header.format_version, 3);
            prop_assert_eq!(decoded.header.tenant_hash, tenant_hash.to_vec());
            prop_assert_eq!(decoded.header.signal, signal);
            prop_assert_eq!(decoded.header.part_blake3, vec![part_blake3.to_vec()]);
            prop_assert_eq!(decoded.segments, segments);
        }

        /// ADR-1413 / repo testing convention: a corrupt (here, truncated)
        /// input must produce a typed error, never a panic and never a
        /// successful decode of the wrong data. Any strict prefix of a
        /// well-formed v3 object is missing bytes some length/CRC check in
        /// the envelope depends on, so it must always be rejected.
        #[test]
        fn v3_truncated_input_never_panics_and_is_rejected(
            n in 1usize..6,
            cut_seed in 0usize..10_000,
        ) {
            let segments = arb_v3_segments(n);
            let bytes = encode_column_stats_v3([0x11; 16], 3, [0x22; 32], &segments, u64::MAX)
                .expect("a sorted, distinct segment set always fits an unbounded per-part bound");
            prop_assume!(!bytes.is_empty());
            let truncate_to = cut_seed % bytes.len();
            let truncated = &bytes[..truncate_to];
            let result = decode_column_stats(truncated, &ColumnStatsLimits::default());
            prop_assert!(
                result.is_err(),
                "a strictly truncated v3 object must never decode successfully"
            );
        }

        /// Every byte of a v3 object is protected against a single-bit flip
        /// by one of two independently sufficient guards: `header_crc`
        /// covers magic/version/reserved/header_len/header_bytes, and
        /// `body_crc` covers the compressed body -- crc32c is guaranteed to
        /// detect any single-bit error within the range it covers, and both
        /// checks run before the header is decoded or the body is
        /// decompressed. The bytes neither CRC covers -- the `body_len`
        /// length prefix and the two stored CRC words themselves -- are not
        /// silently trusted either: a flipped `body_len` desyncs the
        /// remaining reads into a `ColumnStatsTrailingBytes` or CRC mismatch,
        /// and a flipped stored CRC word simply fails the comparison it feeds.
        /// So a single-bit flip anywhere always surfaces a typed error, never
        /// a panic and never a different-but-plausible decode.
        #[test]
        fn v3_single_bit_flip_never_panics_and_is_rejected(
            n in 1usize..6,
            byte_seed in 0usize..10_000,
            bit in 0u8..8,
        ) {
            let segments = arb_v3_segments(n);
            let mut bytes = encode_column_stats_v3([0x11; 16], 3, [0x22; 32], &segments, u64::MAX)
                .expect("a sorted, distinct segment set always fits an unbounded per-part bound");
            prop_assume!(!bytes.is_empty());
            let flip_at = byte_seed % bytes.len();
            bytes[flip_at] ^= 1 << bit;
            let result = decode_column_stats(&bytes, &ColumnStatsLimits::default());
            prop_assert!(
                result.is_err(),
                "a single-bit-flipped v3 object must never decode successfully"
            );
        }
    }
}
