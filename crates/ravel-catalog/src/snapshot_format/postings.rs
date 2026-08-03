//! Name-postings envelope: sorted `__name__` dictionary with delta-varint
//! entry-ordinal postings lists (docs/metric-index-plan.md 3.3, P5a). Bound
//! to an exact covered-parts set via `part_blake3`, in `SnapshotHead.parts`
//! order; postings decoded against a different part set are meaningless and
//! must be rejected (`decode_postings`'s `expected_part_blake3` check).
//!
//! ```text
//! magic           "RNP1" (4 bytes)
//! version         u8 = 1
//! reserved        u8[3] = 0
//! header_len      u32 LE
//! header          protobuf ravel.catalog.v1.SnapshotPostingsHeader
//! body_len        u64 LE
//! body            zstd(name blocks), sorted ascending by name, each:
//!                   name_len         uvarint
//!                   name             utf-8 bytes
//!                   posting_count    uvarint
//!                   ordinals         posting_count delta-varints, ascending
//! body_crc32c     u32 LE          over the compressed body bytes
//! header_crc32c   u32 LE          over magic..header inclusive
//! ```

use prost::Message;
use ravel_proto::catalog::v1::SnapshotPostingsHeader;

use super::error::SnapshotFormatError;
use super::{
    MIN_POSTINGS_ENVELOPE_LEN, POSTINGS_MAGIC, POSTINGS_RESERVED, POSTINGS_VERSION, PostingsLimits,
};

/// One name's sorted, deduplicated entry-ordinal postings list. Ordinals
/// index into the covered parts' entries in concatenated part order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamePostings {
    pub name: String,
    pub ordinals: Vec<u64>,
}

/// A decoded, fully validated postings object.
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedPostings {
    pub header: SnapshotPostingsHeader,
    pub names: Vec<NamePostings>,
}

/// Encodes a name-postings object. Validates `names` against the same rules
/// `decode_postings` enforces (sort order, no duplicate names, strictly
/// increasing in-bounds ordinals per name), mirroring the part envelope's
/// defensive-validation precedent: a postings object this function writes
/// can never fail its own decode.
pub fn encode_postings(
    tenant_hash: [u8; 16],
    signal: u32,
    part_blake3: &[[u8; 32]],
    entry_count: u64,
    names: &[NamePostings],
) -> Result<Vec<u8>, SnapshotFormatError> {
    validate_names(names, entry_count)?;
    let name_count = u32::try_from(names.len())
        .map_err(|_| SnapshotFormatError::PostingsTooManyNames(names.len()))?;

    let mut body_raw = Vec::new();
    for np in names {
        write_uvarint(&mut body_raw, np.name.len() as u64);
        body_raw.extend_from_slice(np.name.as_bytes());
        write_uvarint(&mut body_raw, np.ordinals.len() as u64);
        let mut prev = 0u64;
        for (i, &ordinal) in np.ordinals.iter().enumerate() {
            let delta = if i == 0 { ordinal } else { ordinal - prev };
            write_uvarint(&mut body_raw, delta);
            prev = ordinal;
        }
    }
    let body_uncompressed_len = body_raw.len() as u64;

    let body = zstd::bulk::compress(&body_raw, super::ZSTD_LEVEL)
        .map_err(|e| SnapshotFormatError::Compress(e.to_string()))?;

    let header = SnapshotPostingsHeader {
        format_version: u32::from(POSTINGS_VERSION),
        tenant_hash: tenant_hash.to_vec(),
        signal,
        part_blake3: part_blake3.iter().map(|h| h.to_vec()).collect(),
        entry_count,
        name_count,
        body_uncompressed_len,
    };
    let header_bytes = header.encode_to_vec();
    let header_len = u32::try_from(header_bytes.len())
        .map_err(|_| SnapshotFormatError::PostingsHeaderTooLarge)?;

    let mut out = Vec::with_capacity(MIN_POSTINGS_ENVELOPE_LEN + header_bytes.len() + body.len());
    out.extend_from_slice(&POSTINGS_MAGIC);
    out.push(POSTINGS_VERSION);
    out.extend_from_slice(&POSTINGS_RESERVED);
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

/// Decodes and fully validates a name-postings object. Every byte is
/// untrusted; every failure is a typed error, never a panic.
///
/// `expected_part_blake3` is the caller's covered-parts blake3 list, in
/// `SnapshotHead.parts` order; a postings object whose header names a
/// different set is rejected with
/// [`SnapshotFormatError::PostingsPartBindingMismatch`] rather than trusted
/// against the wrong parts.
pub fn decode_postings(
    bytes: &[u8],
    limits: &PostingsLimits,
    expected_part_blake3: &[[u8; 32]],
) -> Result<DecodedPostings, SnapshotFormatError> {
    if bytes.len() < MIN_POSTINGS_ENVELOPE_LEN {
        return Err(SnapshotFormatError::PostingsTooSmall { size: bytes.len() });
    }

    let mut pos = 0usize;
    let magic = take_array::<4>(bytes, &mut pos)?;
    if magic != POSTINGS_MAGIC {
        return Err(SnapshotFormatError::BadMagic);
    }
    let version = take_bytes(bytes, &mut pos, 1)?[0];
    if version != POSTINGS_VERSION {
        return Err(SnapshotFormatError::PostingsUnsupportedVersion(version));
    }
    let reserved = take_array::<3>(bytes, &mut pos)?;
    if reserved != POSTINGS_RESERVED {
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

    let header = SnapshotPostingsHeader::decode(header_bytes)
        .map_err(|e| SnapshotFormatError::PostingsHeaderDecode(e.to_string()))?;
    if header.format_version != u32::from(POSTINGS_VERSION) {
        return Err(SnapshotFormatError::HeaderVersionMismatch {
            header: header.format_version,
            envelope: POSTINGS_VERSION,
        });
    }
    if header.tenant_hash.len() != 16 {
        return Err(SnapshotFormatError::BadTenantHashLen(
            header.tenant_hash.len(),
        ));
    }
    for (index, part_hash) in header.part_blake3.iter().enumerate() {
        if part_hash.len() != 32 {
            return Err(SnapshotFormatError::PostingsPartBlake3Len {
                index,
                actual: part_hash.len(),
            });
        }
    }
    if header.part_blake3.len() != expected_part_blake3.len()
        || header
            .part_blake3
            .iter()
            .zip(expected_part_blake3.iter())
            .any(|(actual, expected)| actual.as_slice() != expected.as_slice())
    {
        return Err(SnapshotFormatError::PostingsPartBindingMismatch);
    }

    if header.body_uncompressed_len > limits.max_postings_bytes {
        return Err(SnapshotFormatError::DecompressedTooLarge {
            declared: header.body_uncompressed_len,
            cap: limits.max_postings_bytes,
        });
    }
    let capacity = to_usize(header.body_uncompressed_len)?;
    let decompressed = zstd::bulk::decompress(body, capacity)
        .map_err(|e| SnapshotFormatError::Decompress(e.to_string()))?;
    if decompressed.len() as u64 != header.body_uncompressed_len {
        return Err(SnapshotFormatError::DecompressedLenMismatch {
            expected: header.body_uncompressed_len,
            actual: decompressed.len() as u64,
        });
    }

    let mut names = Vec::new();
    let mut body_pos = 0usize;
    while body_pos < decompressed.len() {
        let name_len = read_uvarint(&decompressed, &mut body_pos)?;
        let name_bytes = take_bytes(&decompressed, &mut body_pos, to_usize(name_len)?)?;
        let name = std::str::from_utf8(name_bytes)
            .map_err(|_| SnapshotFormatError::PostingsNameNotUtf8)?
            .to_string();
        let posting_count = read_uvarint(&decompressed, &mut body_pos)?;
        let mut ordinals = Vec::with_capacity(to_usize(posting_count)?);
        let mut prev = 0u64;
        for i in 0..posting_count {
            let delta = read_uvarint(&decompressed, &mut body_pos)?;
            let ordinal = if i == 0 { delta } else { prev + delta };
            ordinals.push(ordinal);
            prev = ordinal;
        }
        names.push(NamePostings { name, ordinals });
    }
    if body_pos != decompressed.len() {
        return Err(SnapshotFormatError::TrailingBytes);
    }

    if names.len() as u32 != header.name_count {
        return Err(SnapshotFormatError::PostingsNameCountMismatch {
            expected: header.name_count,
            actual: names.len(),
        });
    }
    validate_names(&names, header.entry_count)?;

    Ok(DecodedPostings { header, names })
}

/// Reads only the declared `tenant_hash` from a postings object's header,
/// without decoding or validating its body or checking its part binding.
///
/// This exists so the ADR-0050 §2 tenant-hash isolation check can run
/// *before* [`decode_postings`]'s part-binding check: the binding check
/// degrades to a listing fallback (a postings object bound to a different
/// part set is stale, not a breach), and if it ran first it would mask a
/// foreign `tenant_hash` on an object that also happens not to bind, letting
/// an isolation breach degrade silently instead of hard-failing (#528).
///
/// The caller must have already verified `bytes` against the postings ref's
/// blake3 (postings objects are content-addressed), so the framing is
/// trusted enough to locate and decode the header; every failure is still a
/// typed error, never a panic. `decode_postings` re-reads and fully
/// validates the same header, so this never widens what is accepted.
pub fn postings_declared_tenant_hash(bytes: &[u8]) -> Result<[u8; 16], SnapshotFormatError> {
    if bytes.len() < MIN_POSTINGS_ENVELOPE_LEN {
        return Err(SnapshotFormatError::PostingsTooSmall { size: bytes.len() });
    }
    let mut pos = 0usize;
    let magic = take_array::<4>(bytes, &mut pos)?;
    if magic != POSTINGS_MAGIC {
        return Err(SnapshotFormatError::BadMagic);
    }
    let version = take_bytes(bytes, &mut pos, 1)?[0];
    if version != POSTINGS_VERSION {
        return Err(SnapshotFormatError::PostingsUnsupportedVersion(version));
    }
    let reserved = take_array::<3>(bytes, &mut pos)?;
    if reserved != POSTINGS_RESERVED {
        return Err(SnapshotFormatError::ReservedNonZero);
    }
    let header_len = take_u32_le(bytes, &mut pos)?;
    let header_bytes = take_bytes(bytes, &mut pos, to_usize(header_len)?)?;
    let header = SnapshotPostingsHeader::decode(header_bytes)
        .map_err(|e| SnapshotFormatError::PostingsHeaderDecode(e.to_string()))?;
    <[u8; 16]>::try_from(header.tenant_hash.as_slice())
        .map_err(|_| SnapshotFormatError::BadTenantHashLen(header.tenant_hash.len()))
}

/// Sort/uniqueness/bound validation shared by `encode_postings` (defensive
/// check of caller input) and `decode_postings` (untrusted-bytes check).
fn validate_names(names: &[NamePostings], entry_count: u64) -> Result<(), SnapshotFormatError> {
    for (i, np) in names.iter().enumerate() {
        let mut prev = 0u64;
        for (j, &ordinal) in np.ordinals.iter().enumerate() {
            if ordinal >= entry_count {
                return Err(SnapshotFormatError::PostingsOrdinalOutOfBounds {
                    name: np.name.clone(),
                    ordinal,
                    entry_count,
                });
            }
            if j > 0 && ordinal <= prev {
                return Err(SnapshotFormatError::PostingsOrdinalsNotStrictlyIncreasing {
                    name: np.name.clone(),
                });
            }
            prev = ordinal;
        }
        if i > 0 {
            match names[i - 1].name.cmp(&np.name) {
                std::cmp::Ordering::Less => {}
                std::cmp::Ordering::Equal => {
                    return Err(SnapshotFormatError::PostingsDuplicateName);
                }
                std::cmp::Ordering::Greater => {
                    return Err(SnapshotFormatError::PostingsNamesUnsorted);
                }
            }
        }
    }
    Ok(())
}

/// Maximum bytes a u64 LEB128 varint can occupy (ceil(64/7)).
const MAX_VARINT_BYTES: usize = 10;

fn write_uvarint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            break;
        }
        out.push(byte | 0x80);
    }
}

fn read_uvarint(bytes: &[u8], pos: &mut usize) -> Result<u64, SnapshotFormatError> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    for i in 0..MAX_VARINT_BYTES {
        let byte = *bytes.get(*pos).ok_or(SnapshotFormatError::Truncated)?;
        *pos += 1;
        let low7 = u64::from(byte & 0x7f);
        if i == MAX_VARINT_BYTES - 1 && low7 > 1 {
            return Err(SnapshotFormatError::PostingsBadVarint);
        }
        let shifted = low7
            .checked_shl(shift)
            .ok_or(SnapshotFormatError::PostingsBadVarint)?;
        result |= shifted;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
    }
    Err(SnapshotFormatError::PostingsBadVarint)
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
