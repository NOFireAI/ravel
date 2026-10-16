//! KEY_IDX section grammar (docs/log-segment-format.md "KEY_IDX", ADR-2707
//! decision 1): an exact `(key8, block ordinal)` index per indexed field,
//! split into buckets a probe reads one at a time by range.
//!
//! The writer is [`encode_key_idx`]. The reader is ranged and runs in three
//! steps so a caller issues one GET per step that needs bytes:
//!
//! 1. [`key_idx_prefix_range`] bounds the first read from the footer entry's
//!    `prefix_len` before it is issued, and [`KeyIdxPrefix::parse`] verifies
//!    the header and its bounds over `[0, prefix_len)`.
//! 2. [`KeyIdxPrefix::locate`] declines when the header names the field
//!    under a key type other than the caller's, else verifies the field's
//!    directory and names the byte range of the one bucket the key mixes to.
//! 3. [`KeyIdxBucket::blocks`] verifies and decodes that bucket and returns
//!    the blocks holding the key.
//!
//! [`probe`] and [`probe_ranged`] run exactly those steps. [`verify_key_idx`]
//! walks every frame of a whole section, for inspection. Every grammar
//! violation is [`CodecError::Corrupted`].

use std::ops::Range;

use crate::error::CodecError;
use crate::varint::{get_uvarint, put_uvarint};

/// The section's own grammar version, separate from any trailer version.
pub const KEY_IDX_VERSION: u8 = 1;
/// The `bucket_bits` every format writer uses.
pub const KEY_IDX_WRITER_BUCKET_BITS: u8 = 8;
/// Smallest `bucket_bits` a reader accepts.
pub const KEY_IDX_MIN_BUCKET_BITS: u8 = 1;
/// Largest `bucket_bits` a reader accepts.
pub const KEY_IDX_MAX_BUCKET_BITS: u8 = 16;
/// Reader-side ceiling on `prefix_len`, the size of a probe's first read.
pub const KEY_IDX_PREFIX_MAX: u32 = 4 * 1024 * 1024;
/// Ceiling on one bucket frame's `entry_count` (12 MiB decompressed).
pub const KEY_IDX_BUCKET_MAX_ENTRIES: u32 = 1 << 20;
/// Most fields one section may name.
pub const KEY_IDX_MAX_FIELDS: usize = 64;
/// Longest field name, in bytes.
pub const KEY_IDX_MAX_NAME_LEN: usize = 128;
/// Bytes per decompressed entry: `key8` then the block ordinal as u32 LE.
pub const KEY_IDX_ENTRY_LEN: usize = 12;

const BUCKET_MIX: u64 = 0x9E37_79B9_7F4A_7C15;
const FRAME_HEADER_LEN: usize = 8;
const ZSTD_LEVEL: i32 = 3;
/// The header length is a fixed point of its own uvarint offsets; it only
/// grows between rounds, so it settles within a handful.
const LAYOUT_ROUNDS: usize = 16;

/// The 8-byte key an entry is indexed and probed by.
pub type Key8 = [u8; 8];

fn corrupted(msg: impl std::fmt::Display) -> CodecError {
    CodecError::Corrupted(format!("key_idx: {msg}"))
}

/// The section's key type byte. Not `TypedAttrColumnType`: a format writer
/// maps its declared column types onto this enumeration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KeyType {
    /// A 16-byte id (`trace_id`); `key8` is its first 8 bytes.
    Id16 = 1,
    /// An i64; `key8` is the big-endian sign-flipped bit pattern.
    I64 = 2,
    /// UTF-8 bytes; `key8` is the first 8 bytes of their unkeyed blake3.
    Str = 3,
    /// Raw bytes; `key8` is the first 8 bytes of their unkeyed blake3.
    Bytes = 4,
}

impl KeyType {
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    /// Parses a stored `key_type` byte; anything outside 1..=4 is `Corrupted`.
    pub fn from_u8(byte: u8) -> Result<Self, CodecError> {
        match byte {
            1 => Ok(KeyType::Id16),
            2 => Ok(KeyType::I64),
            3 => Ok(KeyType::Str),
            4 => Ok(KeyType::Bytes),
            other => Err(corrupted(format_args!("unknown key_type {other}"))),
        }
    }
}

/// `key8` of an `Id16` value: its first 8 bytes.
pub fn key8_id16(id: &[u8; 16]) -> Key8 {
    let mut key = [0u8; 8];
    key.copy_from_slice(&id[..8]);
    key
}

/// `key8` of an `I64` value: big-endian bytes of `(v as u64) ^ (1 << 63)`.
pub fn key8_i64(value: i64) -> Key8 {
    ((value as u64) ^ (1u64 << 63)).to_be_bytes()
}

/// `key8` of a `Str` value: the first 8 bytes of the unkeyed blake3.
pub fn key8_str(value: &str) -> Key8 {
    key8_bytes(value.as_bytes())
}

/// `key8` of a `Bytes` value: the first 8 bytes of the unkeyed blake3.
pub fn key8_bytes(value: &[u8]) -> Key8 {
    let hash = blake3::hash(value);
    let mut key = [0u8; 8];
    key.copy_from_slice(&hash.as_bytes()[..8]);
    key
}

fn check_bucket_bits(bucket_bits: u8) -> Result<(), CodecError> {
    if (KEY_IDX_MIN_BUCKET_BITS..=KEY_IDX_MAX_BUCKET_BITS).contains(&bucket_bits) {
        Ok(())
    } else {
        Err(corrupted(format_args!(
            "bucket_bits {bucket_bits} outside {KEY_IDX_MIN_BUCKET_BITS}..={KEY_IDX_MAX_BUCKET_BITS}"
        )))
    }
}

/// The mix for a `bucket_bits` already checked to be in range.
fn mix(key8: &Key8, bucket_bits: u8) -> u32 {
    let mixed = u64::from_be_bytes(*key8).wrapping_mul(BUCKET_MIX);
    (mixed >> (64 - u32::from(bucket_bits))) as u32
}

/// The bucket `key8` belongs to:
/// `(u64::from_be_bytes(key8).wrapping_mul(0x9E3779B97F4A7C15)) >> (64 - bucket_bits)`.
/// A `bucket_bits` outside 1..=16 is `Corrupted`.
pub fn bucket_index(key8: &Key8, bucket_bits: u8) -> Result<u32, CodecError> {
    check_bucket_bits(bucket_bits)?;
    Ok(mix(key8, bucket_bits))
}

fn bucket_count(bucket_bits: u8) -> usize {
    1usize << bucket_bits
}

/// Bytes of one field's directory: the `ends` array plus its crc.
fn dir_len(bucket_bits: u8) -> usize {
    4 * bucket_count(bucket_bits) + 4
}

fn read_u32(bytes: &[u8], at: usize) -> Option<u32> {
    let b = bytes.get(at..at.checked_add(4)?)?;
    Some(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
}

fn to_usize(value: u64, what: &str) -> Result<usize, CodecError> {
    usize::try_from(value).map_err(|_| corrupted(format_args!("{what} {value} out of range")))
}

// ---------------------------------------------------------------------------
// Header
// ---------------------------------------------------------------------------

/// One field's header record exactly as stored, before semantic checks.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RawField {
    name: Vec<u8>,
    key_type: u8,
    entry_count: u64,
    dir_offset: u64,
    buckets_offset: u64,
    buckets_len: u64,
}

/// The header exactly as stored, before semantic checks.
#[derive(Debug, Clone, PartialEq, Eq)]
struct RawHeader {
    version: u8,
    bucket_bits: u8,
    reserved: u16,
    prefix_len: u32,
    fields: Vec<RawField>,
}

/// Serializes the header followed by its `header_crc32c`.
fn encode_header(header: &RawHeader) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(header.version);
    out.push(header.bucket_bits);
    out.extend_from_slice(&header.reserved.to_le_bytes());
    out.extend_from_slice(&header.prefix_len.to_le_bytes());
    put_uvarint(&mut out, header.fields.len() as u64);
    for f in &header.fields {
        put_uvarint(&mut out, f.name.len() as u64);
        out.extend_from_slice(&f.name);
        out.push(f.key_type);
        put_uvarint(&mut out, f.entry_count);
        put_uvarint(&mut out, f.dir_offset);
        put_uvarint(&mut out, f.buckets_offset);
        put_uvarint(&mut out, f.buckets_len);
    }
    let crc = crc32c::crc32c(&out);
    out.extend_from_slice(&crc.to_le_bytes());
    out
}

/// Reads the header's structure without verifying its crc or any bound
/// beyond what is needed to walk it. Returns the header and the offset of
/// `header_crc32c`.
fn parse_raw_header(bytes: &[u8]) -> Result<(RawHeader, usize), CodecError> {
    let fixed = bytes
        .get(0..8)
        .ok_or_else(|| corrupted("header truncated"))?;
    let version = fixed[0];
    let bucket_bits = fixed[1];
    let reserved = u16::from_le_bytes([fixed[2], fixed[3]]);
    let prefix_len = u32::from_le_bytes([fixed[4], fixed[5], fixed[6], fixed[7]]);
    let mut pos = 8usize;
    let field_count = get_uvarint(bytes, &mut pos)?;
    if field_count == 0 || field_count > KEY_IDX_MAX_FIELDS as u64 {
        return Err(corrupted(format_args!(
            "field_count {field_count} outside 1..={KEY_IDX_MAX_FIELDS}"
        )));
    }
    let mut fields = Vec::with_capacity(field_count as usize);
    for _ in 0..field_count {
        let name_len = get_uvarint(bytes, &mut pos)?;
        if name_len == 0 || name_len > KEY_IDX_MAX_NAME_LEN as u64 {
            return Err(corrupted(format_args!(
                "name_len {name_len} outside 1..={KEY_IDX_MAX_NAME_LEN}"
            )));
        }
        let end = pos + name_len as usize;
        let name = bytes
            .get(pos..end)
            .ok_or_else(|| corrupted("header truncated in a field name"))?
            .to_vec();
        pos = end;
        let key_type = *bytes
            .get(pos)
            .ok_or_else(|| corrupted("header truncated at key_type"))?;
        pos += 1;
        let entry_count = get_uvarint(bytes, &mut pos)?;
        let dir_offset = get_uvarint(bytes, &mut pos)?;
        let buckets_offset = get_uvarint(bytes, &mut pos)?;
        let buckets_len = get_uvarint(bytes, &mut pos)?;
        fields.push(RawField {
            name,
            key_type,
            entry_count,
            dir_offset,
            buckets_offset,
            buckets_len,
        });
    }
    if bytes.len() < pos + 4 {
        return Err(corrupted("header truncated at header_crc32c"));
    }
    Ok((
        RawHeader {
            version,
            bucket_bits,
            reserved,
            prefix_len,
            fields,
        },
        pos,
    ))
}

// ---------------------------------------------------------------------------
// Writer
// ---------------------------------------------------------------------------

/// One indexed field handed to the writer: its name, key type, and every
/// `(key8, block)` pair in any order, duplicates allowed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyIdxFieldInput {
    pub name: String,
    pub key_type: KeyType,
    pub entries: Vec<(Key8, u32)>,
}

/// A written section and the `prefix_len` its footer entry must carry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedKeyIdx {
    pub bytes: Vec<u8>,
    pub prefix_len: u32,
}

/// One field ready for layout: per bucket its encoded frame (empty for an
/// empty bucket). No ordering or count check is applied at this level.
#[derive(Debug, Clone)]
struct FieldParts {
    name: Vec<u8>,
    key_type: u8,
    entry_count: u64,
    frames: Vec<Vec<u8>>,
}

/// Writes a section at [`KEY_IDX_WRITER_BUCKET_BITS`].
///
/// Fields must be 1..=64, ascending by name bytes and unique, with names of
/// 1..=128 bytes; anything else is a typed error. Entries are deduplicated
/// and sorted by `(key8, block)` within each bucket.
pub fn encode_key_idx(fields: &[KeyIdxFieldInput]) -> Result<EncodedKeyIdx, CodecError> {
    encode_key_idx_with_bucket_bits(fields, KEY_IDX_WRITER_BUCKET_BITS)
}

/// [`encode_key_idx`] at an explicit `bucket_bits` (1..=16, the range a reader
/// accepts). Format writers use [`encode_key_idx`]; this exists so tests and
/// tools can produce every `bucket_bits` the reader must handle.
pub fn encode_key_idx_with_bucket_bits(
    fields: &[KeyIdxFieldInput],
    bucket_bits: u8,
) -> Result<EncodedKeyIdx, CodecError> {
    check_bucket_bits(bucket_bits)?;
    if fields.is_empty() || fields.len() > KEY_IDX_MAX_FIELDS {
        return Err(corrupted(format_args!(
            "writer given {} fields, outside 1..={KEY_IDX_MAX_FIELDS}",
            fields.len()
        )));
    }
    let mut parts = Vec::with_capacity(fields.len());
    let mut previous: Option<&[u8]> = None;
    for field in fields {
        let name = field.name.as_bytes();
        if name.is_empty() || name.len() > KEY_IDX_MAX_NAME_LEN {
            return Err(corrupted(format_args!(
                "writer field name of {} bytes, outside 1..={KEY_IDX_MAX_NAME_LEN}",
                name.len()
            )));
        }
        if let Some(prev) = previous
            && prev >= name
        {
            return Err(corrupted(format_args!(
                "writer field names not ascending and unique at {:?}",
                field.name
            )));
        }
        previous = Some(name);
        parts.push(bucket_field(field, bucket_bits)?);
    }
    assemble(bucket_bits, &parts)
}

/// Buckets, sorts and deduplicates one field's entries and frames each
/// non-empty bucket.
fn bucket_field(field: &KeyIdxFieldInput, bucket_bits: u8) -> Result<FieldParts, CodecError> {
    let mut keyed: Vec<(u32, Key8, u32)> = field
        .entries
        .iter()
        .map(|&(key8, block)| (mix(&key8, bucket_bits), key8, block))
        .collect();
    keyed.sort_unstable();
    keyed.dedup();
    let mut frames = vec![Vec::new(); bucket_count(bucket_bits)];
    let mut start = 0usize;
    while start < keyed.len() {
        let bucket = keyed[start].0;
        let end = keyed[start..]
            .iter()
            .position(|e| e.0 != bucket)
            .map_or(keyed.len(), |n| start + n);
        let entries: Vec<(Key8, u32)> = keyed[start..end].iter().map(|e| (e.1, e.2)).collect();
        let slot = frames
            .get_mut(bucket as usize)
            .ok_or_else(|| corrupted("writer bucket out of range"))?;
        *slot = encode_frame(&entries)?;
        start = end;
    }
    Ok(FieldParts {
        name: field.name.as_bytes().to_vec(),
        key_type: field.key_type.as_u8(),
        entry_count: keyed.len() as u64,
        frames,
    })
}

/// One bucket frame: `entry_count` u32 LE, `frame_crc32c` over the zstd
/// bytes, then the zstd bytes of the entries in the order given.
fn encode_frame(entries: &[(Key8, u32)]) -> Result<Vec<u8>, CodecError> {
    let count = u32::try_from(entries.len())
        .ok()
        .filter(|&n| n <= KEY_IDX_BUCKET_MAX_ENTRIES)
        .ok_or_else(|| {
            corrupted(format_args!(
                "writer bucket of {} entries over KEY_IDX_BUCKET_MAX_ENTRIES {KEY_IDX_BUCKET_MAX_ENTRIES}",
                entries.len()
            ))
        })?;
    let mut raw = Vec::with_capacity(entries.len() * KEY_IDX_ENTRY_LEN);
    for (key8, block) in entries {
        raw.extend_from_slice(key8);
        raw.extend_from_slice(&block.to_le_bytes());
    }
    let compressed = zstd::bulk::compress(&raw, ZSTD_LEVEL)
        .map_err(|e| corrupted(format_args!("writer zstd compress failed: {e}")))?;
    let mut out = Vec::with_capacity(FRAME_HEADER_LEN + compressed.len());
    out.extend_from_slice(&count.to_le_bytes());
    out.extend_from_slice(&crc32c::crc32c(&compressed).to_le_bytes());
    out.extend_from_slice(&compressed);
    Ok(out)
}

/// Lays out the header, then every directory in field order, then every
/// bucket area, and serializes them.
fn assemble(bucket_bits: u8, parts: &[FieldParts]) -> Result<EncodedKeyIdx, CodecError> {
    let mut dirs = Vec::with_capacity(parts.len());
    for part in parts {
        let mut ends = Vec::with_capacity(part.frames.len());
        let mut end = 0u64;
        for frame in &part.frames {
            end += frame.len() as u64;
            ends.push(
                u32::try_from(end).map_err(|_| {
                    corrupted("writer bucket area over u32::MAX bytes for one field")
                })?,
            );
        }
        dirs.push((ends, end));
    }
    let dir_bytes = dir_len(bucket_bits) as u64;
    let mut header = RawHeader {
        version: KEY_IDX_VERSION,
        bucket_bits,
        reserved: 0,
        prefix_len: 0,
        fields: parts
            .iter()
            .zip(&dirs)
            .map(|(p, (_, buckets_len))| RawField {
                name: p.name.clone(),
                key_type: p.key_type,
                entry_count: p.entry_count,
                dir_offset: 0,
                buckets_offset: 0,
                buckets_len: *buckets_len,
            })
            .collect(),
    };
    let mut header_len = encode_header(&header).len() as u64;
    let mut encoded = None;
    for _ in 0..LAYOUT_ROUNDS {
        let prefix_len = header_len + dir_bytes * parts.len() as u64;
        header.prefix_len = u32::try_from(prefix_len)
            .ok()
            .filter(|&p| p <= KEY_IDX_PREFIX_MAX)
            .ok_or_else(|| {
                corrupted(format_args!(
                    "writer prefix_len {prefix_len} over KEY_IDX_PREFIX_MAX {KEY_IDX_PREFIX_MAX}"
                ))
            })?;
        let mut buckets_offset = prefix_len;
        for (i, field) in header.fields.iter_mut().enumerate() {
            field.dir_offset = header_len + dir_bytes * i as u64;
            field.buckets_offset = buckets_offset;
            buckets_offset += field.buckets_len;
        }
        let bytes = encode_header(&header);
        if bytes.len() as u64 == header_len {
            encoded = Some(bytes);
            break;
        }
        header_len = bytes.len() as u64;
    }
    let mut out = encoded.ok_or_else(|| corrupted("writer header layout did not converge"))?;
    for (ends, _) in &dirs {
        let start = out.len();
        for end in ends {
            out.extend_from_slice(&end.to_le_bytes());
        }
        let crc = crc32c::crc32c(&out[start..]);
        out.extend_from_slice(&crc.to_le_bytes());
    }
    for part in parts {
        for frame in &part.frames {
            out.extend_from_slice(frame);
        }
    }
    Ok(EncodedKeyIdx {
        bytes: out,
        prefix_len: header.prefix_len,
    })
}

// ---------------------------------------------------------------------------
// Reader
// ---------------------------------------------------------------------------

/// Step (a), before any section byte is read: checks the footer entry's
/// `prefix_len` against the section length and [`KEY_IDX_PREFIX_MAX`], and
/// returns the range of the first read, `[0, prefix_len)`.
pub fn key_idx_prefix_range(
    footer_prefix_len: u32,
    section_len: u64,
) -> Result<Range<u64>, CodecError> {
    if footer_prefix_len == 0 {
        return Err(corrupted("footer prefix_len is zero"));
    }
    if footer_prefix_len > KEY_IDX_PREFIX_MAX {
        return Err(corrupted(format_args!(
            "footer prefix_len {footer_prefix_len} over KEY_IDX_PREFIX_MAX {KEY_IDX_PREFIX_MAX}"
        )));
    }
    if u64::from(footer_prefix_len) > section_len {
        return Err(corrupted(format_args!(
            "footer prefix_len {footer_prefix_len} past the section length {section_len}"
        )));
    }
    Ok(0..u64::from(footer_prefix_len))
}

/// One field as the verified header names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyIdxFieldHeader {
    name: String,
    key_type: KeyType,
    entry_count: u64,
    dir_offset: u64,
    buckets_offset: u64,
    buckets_len: u64,
    dir_len: u64,
}

impl KeyIdxFieldHeader {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn key_type(&self) -> KeyType {
        self.key_type
    }

    /// The header's `entry_count` over every bucket. A probe reads one bucket
    /// and cannot check the sum, so for a probe this figure is advisory;
    /// [`verify_key_idx`] checks it.
    pub fn entry_count(&self) -> u64 {
        self.entry_count
    }

    /// The directory's byte range in the section, inside `[0, prefix_len)`.
    pub fn dir_range(&self) -> Range<u64> {
        self.dir_offset..self.dir_offset + self.dir_len
    }

    /// The bucket area's byte range in the section, at or past `prefix_len`.
    pub fn buckets_range(&self) -> Range<u64> {
        self.buckets_offset..self.buckets_offset + self.buckets_len
    }
}

/// The verified header and directories of a section: the result of the
/// first read, `[0, prefix_len)`.
#[derive(Debug)]
pub struct KeyIdxPrefix<'a> {
    bytes: &'a [u8],
    bucket_bits: u8,
    prefix_len: u32,
    fields: Vec<KeyIdxFieldHeader>,
}

impl<'a> KeyIdxPrefix<'a> {
    /// Parses the first read. `prefix` must be exactly `[0, prefix_len)` of a
    /// section `section_len` bytes long, `footer_prefix_len` the footer
    /// entry's value. Verifies `header_crc32c`, the version, `bucket_bits`,
    /// the reserved field, the header's `prefix_len` against the footer's,
    /// field count, names, order and key types, and that every directory
    /// lies inside the prefix past the header's `header_crc32c` and every
    /// bucket area past the prefix and inside the section. Directories are verified when a field is located.
    pub fn parse(
        prefix: &'a [u8],
        footer_prefix_len: u32,
        section_len: u64,
    ) -> Result<Self, CodecError> {
        let expected = key_idx_prefix_range(footer_prefix_len, section_len)?;
        if prefix.len() as u64 != expected.end {
            return Err(corrupted(format_args!(
                "prefix read is {} bytes, footer prefix_len is {footer_prefix_len}",
                prefix.len()
            )));
        }
        let version = *prefix
            .first()
            .ok_or_else(|| corrupted("header truncated at version"))?;
        if version != KEY_IDX_VERSION {
            return Err(corrupted(format_args!(
                "version {version} is not the known version {KEY_IDX_VERSION}"
            )));
        }
        let (raw, crc_at) = parse_raw_header(prefix)?;
        let stored_crc = read_u32(prefix, crc_at)
            .ok_or_else(|| corrupted("header truncated at header_crc32c"))?;
        if crc32c::crc32c(&prefix[..crc_at]) != stored_crc {
            return Err(corrupted("header_crc32c mismatch"));
        }
        let header_len = crc_at as u64 + 4;
        check_bucket_bits(raw.bucket_bits)?;
        if raw.reserved != 0 {
            return Err(corrupted(format_args!(
                "reserved field is {} (must be 0)",
                raw.reserved
            )));
        }
        if raw.prefix_len != footer_prefix_len {
            return Err(corrupted(format_args!(
                "header prefix_len {} differs from footer prefix_len {footer_prefix_len}",
                raw.prefix_len
            )));
        }
        let prefix_len = u64::from(footer_prefix_len);
        let dir_bytes = dir_len(raw.bucket_bits) as u64;
        let max_entries =
            bucket_count(raw.bucket_bits) as u64 * u64::from(KEY_IDX_BUCKET_MAX_ENTRIES);
        let mut fields: Vec<KeyIdxFieldHeader> = Vec::with_capacity(raw.fields.len());
        for f in raw.fields {
            let name =
                String::from_utf8(f.name).map_err(|_| corrupted("field name is not UTF-8"))?;
            if let Some(prev) = fields.last()
                && prev.name.as_bytes() >= name.as_bytes()
            {
                return Err(corrupted(format_args!(
                    "fields not ascending and unique at {name:?}"
                )));
            }
            let key_type = KeyType::from_u8(f.key_type)?;
            let dir_end = f.dir_offset.checked_add(dir_bytes);
            if f.dir_offset < header_len
                || f.dir_offset >= prefix_len
                || dir_end.is_none_or(|e| e > prefix_len)
            {
                return Err(corrupted(format_args!(
                    "field {name:?} dir_offset {} outside [{header_len}, {prefix_len}) or its directory runs past prefix_len",
                    f.dir_offset
                )));
            }
            if f.buckets_offset < prefix_len {
                return Err(corrupted(format_args!(
                    "field {name:?} buckets_offset {} below prefix_len {prefix_len}",
                    f.buckets_offset
                )));
            }
            if f.buckets_len > u64::from(u32::MAX)
                || f.buckets_offset
                    .checked_add(f.buckets_len)
                    .is_none_or(|e| e > section_len)
            {
                return Err(corrupted(format_args!(
                    "field {name:?} bucket area {}+{} past the section length {section_len}",
                    f.buckets_offset, f.buckets_len
                )));
            }
            if (f.entry_count == 0) != (f.buckets_len == 0) || f.entry_count > max_entries {
                return Err(corrupted(format_args!(
                    "field {name:?} entry_count {} inconsistent with buckets_len {}",
                    f.entry_count, f.buckets_len
                )));
            }
            fields.push(KeyIdxFieldHeader {
                name,
                key_type,
                entry_count: f.entry_count,
                dir_offset: f.dir_offset,
                buckets_offset: f.buckets_offset,
                buckets_len: f.buckets_len,
                dir_len: dir_bytes,
            });
        }
        Ok(KeyIdxPrefix {
            bytes: prefix,
            bucket_bits: raw.bucket_bits,
            prefix_len: footer_prefix_len,
            fields,
        })
    }

    pub fn bucket_bits(&self) -> u8 {
        self.bucket_bits
    }

    pub fn prefix_len(&self) -> u32 {
        self.prefix_len
    }

    /// Every field the header names, ascending by name bytes.
    pub fn fields(&self) -> &[KeyIdxFieldHeader] {
        &self.fields
    }

    /// The named field, or `None` when this section does not cover it.
    pub fn field(&self, name: &str) -> Option<&KeyIdxFieldHeader> {
        self.field_index(name).map(|i| &self.fields[i])
    }

    fn field_index(&self, name: &str) -> Option<usize> {
        self.fields
            .binary_search_by(|f| f.name.as_bytes().cmp(name.as_bytes()))
            .ok()
    }

    /// Verifies a field's `dir_crc32c` and its `ends`: non-decreasing, the
    /// last equal to `buckets_len`.
    fn directory(&self, field: &KeyIdxFieldHeader) -> Result<Vec<u32>, CodecError> {
        let start = to_usize(field.dir_offset, "dir_offset")?;
        let n = bucket_count(self.bucket_bits);
        let dir = self
            .bytes
            .get(start..start + dir_len(self.bucket_bits))
            .ok_or_else(|| corrupted("directory past prefix_len"))?;
        let stored_crc = read_u32(dir, 4 * n).ok_or_else(|| corrupted("directory truncated"))?;
        if crc32c::crc32c(&dir[..4 * n]) != stored_crc {
            return Err(corrupted(format_args!(
                "field {:?} dir_crc32c mismatch",
                field.name
            )));
        }
        let mut ends = Vec::with_capacity(n);
        let mut previous = 0u32;
        for chunk in dir[..4 * n].chunks_exact(4) {
            let end = u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]);
            if end < previous {
                return Err(corrupted(format_args!(
                    "field {:?} directory ends decrease ({end} after {previous})",
                    field.name
                )));
            }
            previous = end;
            ends.push(end);
        }
        if u64::from(previous) != field.buckets_len {
            return Err(corrupted(format_args!(
                "field {:?} last directory end {previous} is not buckets_len {}",
                field.name, field.buckets_len
            )));
        }
        Ok(ends)
    }

    /// Step (b): verifies the field's directory and returns the bucket
    /// `key8` mixes to, with its byte range in the section. `key_type` is the
    /// type the caller derived `key8` under. `None` (decline) when the header
    /// does not name the field or names it under another `key_type`.
    pub fn locate(
        &self,
        field: &str,
        key_type: KeyType,
        key8: &Key8,
    ) -> Result<Option<KeyIdxBucket>, CodecError> {
        let Some(index) = self.field_index(field) else {
            return Ok(None);
        };
        let header = &self.fields[index];
        if header.key_type != key_type {
            return Ok(None);
        }
        let ends = self.directory(header)?;
        let bucket = mix(key8, self.bucket_bits);
        let (start, end) = bucket_bounds(&ends, bucket)?;
        Ok(Some(KeyIdxBucket {
            key8: *key8,
            bucket,
            bucket_bits: self.bucket_bits,
            field_entry_count: header.entry_count,
            range: header.buckets_offset + u64::from(start)..header.buckets_offset + u64::from(end),
        }))
    }
}

fn bucket_bounds(ends: &[u32], bucket: u32) -> Result<(u32, u32), CodecError> {
    let i = bucket as usize;
    let end = *ends
        .get(i)
        .ok_or_else(|| corrupted(format_args!("bucket {bucket} outside the directory")))?;
    let start = if i == 0 { 0 } else { ends[i - 1] };
    Ok((start, end))
}

/// The one bucket a probe reads: step (b)'s result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyIdxBucket {
    key8: Key8,
    bucket: u32,
    bucket_bits: u8,
    field_entry_count: u64,
    range: Range<u64>,
}

impl KeyIdxBucket {
    /// The bucket number the key mixes to.
    pub fn bucket(&self) -> u32 {
        self.bucket
    }

    /// The bucket's byte range in the section; the second read.
    pub fn range(&self) -> Range<u64> {
        self.range.clone()
    }

    /// An empty bucket has no frame: the key is absent with no second read.
    pub fn is_empty(&self) -> bool {
        self.range.is_empty()
    }

    /// Step (c): verifies and decodes the bucket's bytes (exactly
    /// [`KeyIdxBucket::range`]) and returns the blocks whose `key8` equals
    /// the probed one, ascending.
    pub fn blocks(&self, bucket_bytes: &[u8]) -> Result<Vec<u32>, CodecError> {
        Ok(self.matching(&self.entries(bucket_bytes)?))
    }

    /// The blocks of `entries` (as [`KeyIdxBucket::entries`] returns them)
    /// whose `key8` equals the probed one, ascending.
    pub fn matching(&self, entries: &[(Key8, u32)]) -> Vec<u32> {
        entries
            .iter()
            .filter(|(k, _)| *k == self.key8)
            .map(|&(_, b)| b)
            .collect()
    }

    /// Verifies and decodes the bucket's bytes (exactly
    /// [`KeyIdxBucket::range`]) and returns every `(key8, block)` entry of
    /// the bucket, whichever key it belongs to, in stored order. The frame's
    /// crc, entry count, decompressed size, bucket membership and sort order
    /// are checked first. Empty bytes (an empty bucket) are no entries.
    pub fn entries(&self, bucket_bytes: &[u8]) -> Result<Vec<(Key8, u32)>, CodecError> {
        if bucket_bytes.len() as u64 != self.range.end - self.range.start {
            return Err(corrupted(format_args!(
                "bucket read is {} bytes, directory says {}",
                bucket_bytes.len(),
                self.range.end - self.range.start
            )));
        }
        if bucket_bytes.is_empty() {
            return Ok(Vec::new());
        }
        decode_frame(
            bucket_bytes,
            self.bucket,
            self.bucket_bits,
            self.field_entry_count,
        )
    }
}

/// Decodes one bucket frame occupying all of `frame`. The `entry_count`
/// bounds are checked before the decompression buffer is allocated.
fn decode_frame(
    frame: &[u8],
    bucket: u32,
    bucket_bits: u8,
    field_entry_count: u64,
) -> Result<Vec<(Key8, u32)>, CodecError> {
    let (count, stored_crc) = match (read_u32(frame, 0), read_u32(frame, 4)) {
        (Some(c), Some(crc)) => (c, crc),
        _ => return Err(corrupted(format_args!("bucket {bucket} frame truncated"))),
    };
    let payload = &frame[FRAME_HEADER_LEN..];
    if crc32c::crc32c(payload) != stored_crc {
        return Err(corrupted(format_args!(
            "bucket {bucket} frame_crc32c mismatch"
        )));
    }
    if count == 0 {
        return Err(corrupted(format_args!(
            "bucket {bucket} frame entry_count is 0 (an empty bucket has no frame)"
        )));
    }
    if count > KEY_IDX_BUCKET_MAX_ENTRIES {
        return Err(corrupted(format_args!(
            "bucket {bucket} frame entry_count {count} over KEY_IDX_BUCKET_MAX_ENTRIES {KEY_IDX_BUCKET_MAX_ENTRIES}, refused before allocation"
        )));
    }
    if u64::from(count) > field_entry_count {
        return Err(corrupted(format_args!(
            "bucket {bucket} frame entry_count {count} over the field's entry_count {field_entry_count}, refused before allocation"
        )));
    }
    let expected = count as usize * KEY_IDX_ENTRY_LEN;
    let raw = zstd::bulk::decompress(payload, expected).map_err(|e| {
        corrupted(format_args!(
            "bucket {bucket} frame does not decompress within entry_count x 12 = {expected} bytes: {e}"
        ))
    })?;
    if raw.len() != expected {
        return Err(corrupted(format_args!(
            "bucket {bucket} frame decompressed to {} bytes, not entry_count x 12 = {expected}",
            raw.len()
        )));
    }
    let mut entries: Vec<(Key8, u32)> = Vec::with_capacity(count as usize);
    for chunk in raw.chunks_exact(KEY_IDX_ENTRY_LEN) {
        let mut key8 = [0u8; 8];
        key8.copy_from_slice(&chunk[..8]);
        let block = u32::from_le_bytes([chunk[8], chunk[9], chunk[10], chunk[11]]);
        if mix(&key8, bucket_bits) != bucket {
            return Err(corrupted(format_args!(
                "bucket {bucket} holds a key that mixes to bucket {}",
                mix(&key8, bucket_bits)
            )));
        }
        if let Some(prev) = entries.last() {
            let cur = (key8, block);
            if *prev == cur {
                return Err(corrupted(format_args!("bucket {bucket} duplicate entry")));
            }
            if *prev > cur {
                return Err(corrupted(format_args!(
                    "bucket {bucket} entries not sorted by (key8, block)"
                )));
            }
        }
        entries.push((key8, block));
    }
    Ok(entries)
}

/// Probes a section held whole in memory: steps (a) to (c) over slices of
/// `section`. `key_type` is the type the caller derived `key8` under.
/// `Ok(None)` when the header does not name `field` or names it under
/// another `key_type` (the probe declines), `Ok(Some(empty))` when the key
/// is absent, otherwise the ascending blocks whose `key8` matches.
pub fn probe(
    section: &[u8],
    footer_prefix_len: u32,
    field: &str,
    key_type: KeyType,
    key8: &Key8,
) -> Result<Option<Vec<u32>>, CodecError> {
    probe_ranged(
        section.len() as u64,
        footer_prefix_len,
        field,
        key_type,
        key8,
        |range| {
            let start = to_usize(range.start, "range start")?;
            let end = to_usize(range.end, "range end")?;
            section
                .get(start..end)
                .ok_or_else(|| corrupted("read past the section"))
        },
    )
}

/// Steps (a) to (c) through a caller's ranged byte source: one read of
/// `[0, prefix_len)`, then one read of the bucket the key mixes to, skipped
/// when that bucket is empty or the probe declines. Same results as [`probe`].
pub fn probe_ranged<B, F>(
    section_len: u64,
    footer_prefix_len: u32,
    field: &str,
    key_type: KeyType,
    key8: &Key8,
    mut read: F,
) -> Result<Option<Vec<u32>>, CodecError>
where
    B: AsRef<[u8]>,
    F: FnMut(Range<u64>) -> Result<B, CodecError>,
{
    let prefix_range = key_idx_prefix_range(footer_prefix_len, section_len)?;
    let prefix_bytes = read(prefix_range)?;
    let prefix = KeyIdxPrefix::parse(prefix_bytes.as_ref(), footer_prefix_len, section_len)?;
    let Some(bucket) = prefix.locate(field, key_type, key8)? else {
        return Ok(None);
    };
    if bucket.is_empty() {
        return Ok(Some(Vec::new()));
    }
    let bucket_bytes = read(bucket.range())?;
    bucket.blocks(bucket_bytes.as_ref()).map(Some)
}

/// A whole section's shape, for inspection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyIdxSummary {
    pub version: u8,
    pub bucket_bits: u8,
    pub prefix_len: u32,
    pub fields: Vec<KeyIdxFieldSummary>,
}

/// One field of a verified section.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyIdxFieldSummary {
    pub name: String,
    pub key_type: KeyType,
    pub entry_count: u64,
    /// Entries per bucket, `2^bucket_bits` of them.
    pub bucket_entry_counts: Vec<u32>,
}

/// One field of a whole section and every `(key8, block)` entry it holds, in
/// bucket order and sorted by `(key8, block)` within each bucket.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyIdxFieldEntries {
    pub name: String,
    pub key_type: KeyType,
    pub entries: Vec<(Key8, u32)>,
}

/// One field as [`walk_section`] verified it.
struct WalkedField {
    summary: KeyIdxFieldSummary,
    entries: Vec<(Key8, u32)>,
}

/// Verifies every field and frame of a whole section, keeping the decoded
/// entries when `keep_entries` is set.
fn walk_section(
    section: &[u8],
    footer_prefix_len: u32,
    keep_entries: bool,
) -> Result<(u8, u32, Vec<WalkedField>), CodecError> {
    let section_len = section.len() as u64;
    let prefix_range = key_idx_prefix_range(footer_prefix_len, section_len)?;
    let prefix_end = to_usize(prefix_range.end, "prefix_len")?;
    let prefix = KeyIdxPrefix::parse(&section[..prefix_end], footer_prefix_len, section_len)?;
    let bits = prefix.bucket_bits;
    let mut fields = Vec::with_capacity(prefix.fields.len());
    for header in &prefix.fields {
        let ends = prefix.directory(header)?;
        let area_start = to_usize(header.buckets_offset, "buckets_offset")?;
        let mut counts = Vec::with_capacity(ends.len());
        let mut kept = Vec::new();
        let mut total = 0u64;
        let mut start = 0u32;
        for (bucket, &end) in ends.iter().enumerate() {
            if end == start {
                counts.push(0);
                continue;
            }
            let frame = section
                .get(area_start + start as usize..area_start + end as usize)
                .ok_or_else(|| corrupted("bucket frame past the section"))?;
            let entries = decode_frame(frame, bucket as u32, bits, header.entry_count)?;
            let n = entries.len() as u32;
            counts.push(n);
            total += u64::from(n);
            if keep_entries {
                kept.extend(entries);
            }
            start = end;
        }
        if total != header.entry_count {
            return Err(corrupted(format_args!(
                "field {:?} bucket entry counts sum to {total}, header entry_count is {}",
                header.name, header.entry_count
            )));
        }
        fields.push(WalkedField {
            summary: KeyIdxFieldSummary {
                name: header.name.clone(),
                key_type: header.key_type,
                entry_count: header.entry_count,
                bucket_entry_counts: counts,
            },
            entries: kept,
        });
    }
    Ok((bits, prefix.prefix_len, fields))
}

/// Verifies a whole section: everything a probe checks, for every field and
/// every frame, plus that each field's frames tile `[0, buckets_len)` exactly
/// and their entry counts sum to the field's `entry_count`.
pub fn verify_key_idx(section: &[u8], footer_prefix_len: u32) -> Result<KeyIdxSummary, CodecError> {
    let (bucket_bits, prefix_len, fields) = walk_section(section, footer_prefix_len, false)?;
    Ok(KeyIdxSummary {
        version: KEY_IDX_VERSION,
        bucket_bits,
        prefix_len,
        fields: fields.into_iter().map(|f| f.summary).collect(),
    })
}

/// Every field of a whole section with its entries, after the checks
/// [`verify_key_idx`] makes.
pub fn decode_key_idx(
    section: &[u8],
    footer_prefix_len: u32,
) -> Result<Vec<KeyIdxFieldEntries>, CodecError> {
    let (_, _, fields) = walk_section(section, footer_prefix_len, true)?;
    Ok(fields
        .into_iter()
        .map(|f| KeyIdxFieldEntries {
            name: f.summary.name,
            key_type: f.summary.key_type,
            entries: f.entries,
        })
        .collect())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::collections::{BTreeMap, BTreeSet};

    /// The mix in u128 arithmetic, written independently of `mix`.
    fn reference_bucket(key8: &Key8, bucket_bits: u8) -> u32 {
        let key = u128::from(u64::from_be_bytes(*key8));
        let product = (key * 0x9E37_79B9_7F4A_7C15u128) % (1u128 << 64);
        (product >> (64 - u32::from(bucket_bits))) as u32
    }

    fn corrupted_message<T: std::fmt::Debug>(result: Result<T, CodecError>) -> String {
        match result {
            Err(CodecError::Corrupted(message)) => message,
            Ok(value) => panic!("expected Corrupted, got Ok({value:?})"),
        }
    }

    fn assert_corrupted<T: std::fmt::Debug>(result: Result<T, CodecError>, needle: &str) {
        let message = corrupted_message(result);
        assert!(
            message.contains(needle),
            "expected a Corrupted naming {needle:?}, got {message:?}"
        );
    }

    const USER_ID: i64 = 435_090_932_899_640_449;

    /// i64 values whose key8 mixes to `bucket` at 8 bits: `below` of them
    /// sorting under `key`'s key8 and `above` sorting over it.
    fn same_bucket_neighbours(key: &Key8, below: usize, above: usize) -> (Vec<i64>, Vec<i64>) {
        let bucket = reference_bucket(key, 8);
        let lower: Vec<i64> = (0i64..)
            .filter(|v| reference_bucket(&key8_i64(*v), 8) == bucket && key8_i64(*v) < *key)
            .take(below)
            .collect();
        let upper: Vec<i64> = (0i64..)
            .map(|n| i64::MAX - n)
            .filter(|v| reference_bucket(&key8_i64(*v), 8) == bucket && key8_i64(*v) > *key)
            .take(above)
            .collect();
        (lower, upper)
    }

    struct Acceptance {
        section: EncodedKeyIdx,
        key: Key8,
        below: Key8,
        above: Key8,
        absent_same_bucket: Key8,
    }

    fn acceptance_section() -> Acceptance {
        let key = key8_i64(USER_ID);
        let (lower, upper) = same_bucket_neighbours(&key, 2, 1);
        let below = key8_i64(lower[0]);
        let absent_same_bucket = key8_i64(lower[1]);
        let above = key8_i64(upper[0]);
        // The key: a run 10..=14 and isolated blocks 3, 40, 97, given out of
        // order and with a duplicate. Its neighbours share its bucket and
        // touch the blocks on either side of the run.
        let mut user_entries: Vec<(Key8, u32)> = vec![
            (key, 97),
            (key, 14),
            (below, 9),
            (key, 12),
            (key, 3),
            (above, 15),
            (key, 10),
            (key, 12),
            (key, 11),
            (below, 40),
            (key, 13),
            (above, 2),
            (key, 40),
            (above, 98),
        ];
        user_entries.extend((0..50i64).map(|v| (key8_i64(v * 7_919), (v % 13) as u32)));
        let fields = vec![
            KeyIdxFieldInput {
                name: "a_str".into(),
                key_type: KeyType::Str,
                entries: vec![(key8_str("GET"), 0), (key8_str("PUT"), 1)],
            },
            KeyIdxFieldInput {
                name: "empty".into(),
                key_type: KeyType::Bytes,
                entries: Vec::new(),
            },
            KeyIdxFieldInput {
                name: "user_id".into(),
                key_type: KeyType::I64,
                entries: user_entries,
            },
        ];
        Acceptance {
            section: encode_key_idx(&fields).expect("encode"),
            key,
            below,
            above,
            absent_same_bucket,
        }
    }

    fn probe_of(
        section: &EncodedKeyIdx,
        field: &str,
        key_type: KeyType,
        key: &Key8,
    ) -> Option<Vec<u32>> {
        probe(&section.bytes, section.prefix_len, field, key_type, key).expect("probe")
    }

    #[test]
    fn probe_names_exactly_the_blocks_holding_the_key() {
        let a = acceptance_section();
        let s = &a.section;
        assert_eq!(
            probe_of(s, "user_id", KeyType::I64, &a.key),
            Some(vec![3, 10, 11, 12, 13, 14, 40, 97])
        );
        assert_eq!(
            probe_of(s, "user_id", KeyType::I64, &a.below),
            Some(vec![9, 40])
        );
        assert_eq!(
            probe_of(s, "user_id", KeyType::I64, &a.above),
            Some(vec![2, 15, 98])
        );
        assert_eq!(
            probe_of(s, "user_id", KeyType::I64, &a.absent_same_bucket),
            Some(vec![])
        );
        assert_eq!(
            probe_of(s, "a_str", KeyType::Str, &key8_str("GET")),
            Some(vec![0])
        );
        assert_eq!(probe_of(s, "empty", KeyType::Bytes, &a.key), Some(vec![]));
        assert_eq!(probe_of(s, "missing", KeyType::I64, &a.key), None);

        // The mix, pinned by hand (python: ((k * 0x9E3779B97F4A7C15) % 2**64) >> 56)
        // and by `reference_bucket`, against the bucket the reader reads.
        let prefix_bytes = &s.bytes[..s.prefix_len as usize];
        let prefix =
            KeyIdxPrefix::parse(prefix_bytes, s.prefix_len, s.bytes.len() as u64).expect("prefix");
        let pinned: [(u64, u32); 4] = [
            (0x0000_0000_0000_0001, 158),
            (0x0123_4567_89AB_CDEF, 12),
            (0xFFFF_FFFF_FFFF_FFFF, 97),
            (0x8609_C0E5_F479_F481, 5),
        ];
        assert_eq!(a.key, 0x8609_C0E5_F479_F481u64.to_be_bytes());
        for (key, bucket) in pinned {
            let key8 = key.to_be_bytes();
            assert_eq!(reference_bucket(&key8, 8), bucket, "reference mix {key:#x}");
            let located = prefix
                .locate("user_id", KeyType::I64, &key8)
                .expect("locate")
                .expect("field named");
            assert_eq!(
                located.bucket(),
                bucket,
                "bucket the reader reads for {key:#x}"
            );
        }
    }

    #[test]
    fn probe_issues_exactly_two_reads_prefix_then_one_bucket() {
        let a = acceptance_section();
        let s = &a.section;
        let section_len = s.bytes.len() as u64;

        // The bucket's range from the directory bytes, read here by hand.
        let prefix =
            KeyIdxPrefix::parse(&s.bytes[..s.prefix_len as usize], s.prefix_len, section_len)
                .expect("prefix");
        let field = prefix.field("user_id").expect("named").clone();
        let dir = field.dir_range();
        let bucket = reference_bucket(&a.key, 8) as usize;
        let end_at = |i: usize| {
            let at = dir.start as usize + 4 * i;
            u64::from(u32::from_le_bytes(
                s.bytes[at..at + 4].try_into().expect("4 bytes"),
            ))
        };
        let start = if bucket == 0 { 0 } else { end_at(bucket - 1) };
        let expected_bucket =
            field.buckets_range().start + start..field.buckets_range().start + end_at(bucket);
        assert!(!expected_bucket.is_empty());

        let mut reads: Vec<Range<u64>> = Vec::new();
        let blocks = probe_ranged(
            section_len,
            s.prefix_len,
            "user_id",
            KeyType::I64,
            &a.key,
            |range| {
                reads.push(range.clone());
                Ok(s.bytes[range.start as usize..range.end as usize].to_vec())
            },
        )
        .expect("probe");
        assert_eq!(blocks, Some(vec![3, 10, 11, 12, 13, 14, 40, 97]));
        assert_eq!(
            reads,
            vec![0..u64::from(s.prefix_len), expected_bucket.clone()]
        );

        // The same three steps driven by hand read the same two ranges.
        let first = key_idx_prefix_range(s.prefix_len, section_len).expect("range");
        assert_eq!(first, 0..u64::from(s.prefix_len));
        let prefix = KeyIdxPrefix::parse(&s.bytes[..first.end as usize], s.prefix_len, section_len)
            .expect("prefix");
        let located = prefix
            .locate("user_id", KeyType::I64, &a.key)
            .expect("locate")
            .expect("named");
        assert_eq!(located.range(), expected_bucket);
        let bytes = &s.bytes[located.range().start as usize..located.range().end as usize];
        assert_eq!(
            located.blocks(bytes).expect("blocks"),
            vec![3, 10, 11, 12, 13, 14, 40, 97]
        );
    }

    #[test]
    fn entries_return_the_whole_bucket_and_matching_filters_it_to_the_key() {
        let a = acceptance_section();
        let s = &a.section;
        let section_len = s.bytes.len() as u64;
        let prefix =
            KeyIdxPrefix::parse(&s.bytes[..s.prefix_len as usize], s.prefix_len, section_len)
                .expect("prefix");
        let located = prefix
            .locate("user_id", KeyType::I64, &a.key)
            .expect("locate")
            .expect("named");
        let bytes = &s.bytes[located.range().start as usize..located.range().end as usize];
        let entries = located.entries(bytes).expect("entries");
        assert!(
            entries.iter().any(|(k, _)| *k != a.key),
            "the bucket holds other keys too: {entries:?}"
        );
        let mut sorted = entries.clone();
        sorted.sort_unstable();
        assert_eq!(entries, sorted, "entries come back in stored order");
        assert_eq!(
            located.matching(&entries),
            vec![3, 10, 11, 12, 13, 14, 40, 97]
        );
        assert_eq!(
            located.matching(&entries),
            located.blocks(bytes).expect("blocks")
        );
        assert!(
            located.entries(&bytes[1..]).is_err(),
            "a short read is refused"
        );
    }

    #[test]
    fn probe_of_an_empty_bucket_reads_the_prefix_only() {
        let a = acceptance_section();
        let s = &a.section;
        let summary = verify_key_idx(&s.bytes, s.prefix_len).expect("verify");
        let user = summary
            .fields
            .iter()
            .find(|f| f.name == "user_id")
            .expect("user_id");
        let empty_bucket = user
            .bucket_entry_counts
            .iter()
            .position(|&n| n == 0)
            .expect("an empty bucket") as u32;
        let absent = (0u64..)
            .map(u64::to_be_bytes)
            .find(|k| reference_bucket(k, 8) == empty_bucket)
            .expect("a key in the empty bucket");
        let mut reads = 0;
        let blocks = probe_ranged(
            s.bytes.len() as u64,
            s.prefix_len,
            "user_id",
            KeyType::I64,
            &absent,
            |r| {
                reads += 1;
                Ok(s.bytes[r.start as usize..r.end as usize].to_vec())
            },
        )
        .expect("probe");
        assert_eq!(blocks, Some(vec![]));
        assert_eq!(reads, 1);
    }

    #[test]
    fn a_field_probed_under_another_key_type_declines() {
        let a = acceptance_section();
        let s = &a.section;
        // user_id was written as I64. A caller whose column is now STR derives
        // its key8 from the string form; the header's key_type decides.
        let as_str = key8_str(&USER_ID.to_string());
        assert_eq!(probe_of(s, "user_id", KeyType::Str, &as_str), None);
        // Even the I64 key8 itself, asked under another type, declines.
        for other in [KeyType::Id16, KeyType::Str, KeyType::Bytes] {
            assert_eq!(probe_of(s, "user_id", other, &a.key), None, "{other:?}");
        }
        assert_eq!(
            probe_of(s, "user_id", KeyType::I64, &a.key),
            Some(vec![3, 10, 11, 12, 13, 14, 40, 97])
        );
        assert_eq!(
            probe_of(s, "user_id", KeyType::I64, &a.absent_same_bucket),
            Some(vec![])
        );
        // Str and Bytes derive the same key8; the stored key_type still decides.
        assert_eq!(key8_bytes(b"GET"), key8_str("GET"));
        assert_eq!(
            probe_of(s, "a_str", KeyType::Bytes, &key8_bytes(b"GET")),
            None
        );
        assert_eq!(
            probe_of(s, "a_str", KeyType::Str, &key8_str("GET")),
            Some(vec![0])
        );
        // A field with no entries proves absence only under its own type.
        assert_eq!(probe_of(s, "empty", KeyType::I64, &a.key), None);
        assert_eq!(probe_of(s, "empty", KeyType::Bytes, &a.key), Some(vec![]));

        // Ranged: the decline costs the prefix read only.
        let section_len = s.bytes.len() as u64;
        let mut reads: Vec<Range<u64>> = Vec::new();
        let declined = probe_ranged(
            section_len,
            s.prefix_len,
            "user_id",
            KeyType::Str,
            &a.key,
            |r| {
                reads.push(r.clone());
                Ok(s.bytes[r.start as usize..r.end as usize].to_vec())
            },
        )
        .expect("probe");
        assert_eq!(declined, None);
        assert_eq!(reads, vec![0..u64::from(s.prefix_len)]);
        reads.clear();
        let unnamed = probe_ranged(
            section_len,
            s.prefix_len,
            "missing",
            KeyType::I64,
            &a.key,
            |r| {
                reads.push(r.clone());
                Ok(s.bytes[r.start as usize..r.end as usize].to_vec())
            },
        )
        .expect("probe");
        assert_eq!(unnamed, None);
        assert_eq!(reads, vec![0..u64::from(s.prefix_len)]);

        // The three-step API applies the same check in `locate`.
        let prefix =
            KeyIdxPrefix::parse(&s.bytes[..s.prefix_len as usize], s.prefix_len, section_len)
                .expect("prefix");
        assert_eq!(
            prefix.field("user_id").expect("named").key_type(),
            KeyType::I64
        );
        assert_eq!(
            prefix
                .locate("user_id", KeyType::Str, &a.key)
                .expect("locate"),
            None
        );
        let located = prefix
            .locate("user_id", KeyType::I64, &a.key)
            .expect("locate")
            .expect("named under I64");
        assert_eq!(located.bucket(), reference_bucket(&a.key, 8));
    }

    #[test]
    fn key8_derivation_follows_the_key_table() {
        let mut id = [0u8; 16];
        for (i, b) in id.iter_mut().enumerate() {
            *b = i as u8 + 1;
        }
        assert_eq!(key8_id16(&id), [1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(key8_i64(0), [0x80, 0, 0, 0, 0, 0, 0, 0]);
        assert_eq!(
            key8_i64(-1),
            [0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]
        );
        assert_eq!(key8_i64(i64::MIN), [0; 8]);
        assert_eq!(key8_i64(i64::MAX), [0xff; 8]);
        assert!(key8_i64(-5) < key8_i64(3));
        // blake3("") = af1349b9f5f9a1a6...
        assert_eq!(
            key8_bytes(b""),
            [0xaf, 0x13, 0x49, 0xb9, 0xf5, 0xf9, 0xa1, 0xa6]
        );
        assert_eq!(key8_str("abc"), key8_bytes(b"abc"));
        assert_eq!(KeyType::from_u8(1).expect("1"), KeyType::Id16);
        assert_eq!(KeyType::from_u8(4).expect("4"), KeyType::Bytes);
        assert_corrupted(KeyType::from_u8(0), "key_type 0");
        assert_corrupted(KeyType::from_u8(5), "key_type 5");
    }

    #[test]
    fn writer_lays_out_header_directories_then_buckets() {
        let a = acceptance_section();
        let s = &a.section;
        let (raw, crc_at) = parse_raw_header(&s.bytes).expect("raw");
        let header_len = crc_at as u64 + 4;
        let dir = dir_len(8) as u64;
        assert_eq!(raw.version, 1);
        assert_eq!(raw.bucket_bits, KEY_IDX_WRITER_BUCKET_BITS);
        assert_eq!(raw.reserved, 0);
        assert_eq!(u64::from(s.prefix_len), header_len + 3 * dir);
        assert_eq!(raw.prefix_len, s.prefix_len);
        let mut next_area = u64::from(s.prefix_len);
        for (i, f) in raw.fields.iter().enumerate() {
            assert_eq!(f.dir_offset, header_len + i as u64 * dir);
            assert_eq!(f.buckets_offset, next_area);
            next_area += f.buckets_len;
        }
        assert_eq!(next_area, s.bytes.len() as u64);
        // The field with no entries: named, entry_count 0, all-zero
        // directory, buckets_len 0.
        let empty = &raw.fields[1];
        assert_eq!(empty.name, b"empty");
        assert_eq!((empty.entry_count, empty.buckets_len), (0, 0));
        let start = empty.dir_offset as usize;
        assert!(s.bytes[start..start + 4 * 256].iter().all(|&b| b == 0));
        // Duplicates are folded: (key, 12) was given twice.
        let summary = verify_key_idx(&s.bytes, s.prefix_len).expect("verify");
        let user = &summary.fields[2];
        assert_eq!(user.entry_count, raw.fields[2].entry_count);
        assert_eq!(
            user.bucket_entry_counts
                .iter()
                .map(|&n| u64::from(n))
                .sum::<u64>(),
            user.entry_count
        );
        assert_eq!(user.bucket_entry_counts[5], 8 + 2 + 3);
    }

    #[test]
    fn writer_layout_converges_when_offsets_need_longer_varints() {
        let fields: Vec<KeyIdxFieldInput> = (0..KEY_IDX_MAX_FIELDS)
            .map(|i| KeyIdxFieldInput {
                name: format!("f{i:03}"),
                key_type: KeyType::I64,
                entries: (0..20i64)
                    .map(|v| (key8_i64(v * i as i64), v as u32))
                    .collect(),
            })
            .collect();
        let s = encode_key_idx(&fields).expect("encode");
        let summary = verify_key_idx(&s.bytes, s.prefix_len).expect("verify");
        assert_eq!(summary.fields.len(), KEY_IDX_MAX_FIELDS);
        let (raw, crc_at) = parse_raw_header(&s.bytes).expect("raw");
        assert_eq!(raw.fields[0].dir_offset, crc_at as u64 + 4);
        assert!(raw.fields[63].dir_offset > 1 << 14);
        assert_eq!(
            probe_of(&s, "f007", KeyType::I64, &key8_i64(21)),
            Some(vec![3])
        );
    }

    #[test]
    fn writer_refuses_bad_field_sets_with_typed_errors() {
        let field = |name: &str| KeyIdxFieldInput {
            name: name.into(),
            key_type: KeyType::Str,
            entries: vec![(key8_str("x"), 1)],
        };
        assert_corrupted(encode_key_idx(&[]), "0 fields");
        let too_many: Vec<_> = (0..65).map(|i| field(&format!("f{i:03}"))).collect();
        assert_corrupted(encode_key_idx(&too_many), "65 fields");
        assert_corrupted(encode_key_idx(&[field("")]), "name of 0 bytes");
        assert_corrupted(
            encode_key_idx(&[field(&"n".repeat(129))]),
            "name of 129 bytes",
        );
        assert!(encode_key_idx(&[field(&"n".repeat(128))]).is_ok());
        assert_corrupted(encode_key_idx(&[field("b"), field("a")]), "not ascending");
        assert_corrupted(encode_key_idx(&[field("a"), field("a")]), "not ascending");
        assert_corrupted(
            encode_key_idx_with_bucket_bits(&[field("a")], 0),
            "bucket_bits 0",
        );
        assert_corrupted(
            encode_key_idx_with_bucket_bits(&[field("a")], 17),
            "bucket_bits 17",
        );
        assert_corrupted(bucket_index(&[0; 8], 0), "bucket_bits 0");
        assert_corrupted(bucket_index(&[0; 8], 17), "bucket_bits 17");
    }

    // -- corrupt and truncated inputs ----------------------------------------

    /// Re-encodes the header with `mutate` applied and a fresh header crc,
    /// keeping every byte after it.
    fn reseal(section: &[u8], mutate: impl FnOnce(&mut RawHeader)) -> Vec<u8> {
        let (mut raw, crc_at) = parse_raw_header(section).expect("raw header");
        mutate(&mut raw);
        let mut out = encode_header(&raw);
        out.extend_from_slice(&section[crc_at + 4..]);
        out
    }

    /// Rewrites one field's `ends` with a fresh `dir_crc32c`.
    fn rewrite_dir(s: &EncodedKeyIdx, field: &str, mutate: impl FnOnce(&mut Vec<u32>)) -> Vec<u8> {
        let mut bytes = s.bytes.clone();
        let prefix = KeyIdxPrefix::parse(
            &s.bytes[..s.prefix_len as usize],
            s.prefix_len,
            s.bytes.len() as u64,
        )
        .expect("prefix");
        let range = prefix.field(field).expect("named").dir_range();
        let start = range.start as usize;
        let n = (range.end - range.start) as usize / 4 - 1;
        let mut ends: Vec<u32> = (0..n)
            .map(|i| read_u32(&s.bytes, start + 4 * i).expect("end"))
            .collect();
        mutate(&mut ends);
        for (i, e) in ends.iter().enumerate() {
            bytes[start + 4 * i..start + 4 * i + 4].copy_from_slice(&e.to_le_bytes());
        }
        let crc = crc32c::crc32c(&bytes[start..start + 4 * n]);
        bytes[start + 4 * n..start + 4 * n + 4].copy_from_slice(&crc.to_le_bytes());
        bytes
    }

    fn probe_user(
        bytes: &[u8],
        prefix_len: u32,
        key: &Key8,
    ) -> Result<Option<Vec<u32>>, CodecError> {
        probe(bytes, prefix_len, "user_id", KeyType::I64, key)
    }

    #[test]
    fn a_flipped_byte_in_the_header_is_corrupted() {
        let a = acceptance_section();
        let s = &a.section;
        let (_, crc_at) = parse_raw_header(&s.bytes).expect("raw");
        for i in 0..crc_at + 4 {
            let mut bytes = s.bytes.clone();
            bytes[i] ^= 0x01;
            corrupted_message(probe_user(&bytes, s.prefix_len, &a.key));
            corrupted_message(verify_key_idx(&bytes, s.prefix_len));
        }
    }

    #[test]
    fn a_flipped_byte_in_a_directory_is_corrupted() {
        let a = acceptance_section();
        let s = &a.section;
        let prefix = KeyIdxPrefix::parse(
            &s.bytes[..s.prefix_len as usize],
            s.prefix_len,
            s.bytes.len() as u64,
        )
        .expect("prefix");
        let dir = prefix.field("user_id").expect("named").dir_range();
        for i in dir.start as usize..dir.end as usize {
            let mut bytes = s.bytes.clone();
            bytes[i] ^= 0x80;
            assert_corrupted(
                probe_user(&bytes, s.prefix_len, &a.key),
                "dir_crc32c mismatch",
            );
            // A probe on any key of the field checks the whole directory.
            assert_corrupted(
                probe_user(&bytes, s.prefix_len, &[0; 8]),
                "dir_crc32c mismatch",
            );
        }
    }

    #[test]
    fn a_flipped_byte_in_a_frame_is_corrupted() {
        let a = acceptance_section();
        let s = &a.section;
        let prefix = KeyIdxPrefix::parse(
            &s.bytes[..s.prefix_len as usize],
            s.prefix_len,
            s.bytes.len() as u64,
        )
        .expect("prefix");
        let range = prefix
            .locate("user_id", KeyType::I64, &a.key)
            .expect("locate")
            .expect("named")
            .range();
        for i in range.start as usize..range.end as usize {
            for flip in [0x01u8, 0x80] {
                let mut bytes = s.bytes.clone();
                bytes[i] ^= flip;
                corrupted_message(probe_user(&bytes, s.prefix_len, &a.key));
                corrupted_message(verify_key_idx(&bytes, s.prefix_len));
            }
        }
    }

    #[test]
    fn a_truncated_section_is_corrupted() {
        let a = acceptance_section();
        let s = &a.section;
        for n in 0..s.bytes.len() {
            corrupted_message(probe_user(&s.bytes[..n], s.prefix_len, &a.key));
            corrupted_message(verify_key_idx(&s.bytes[..n], s.prefix_len));
        }
        let prefix_len = s.prefix_len as usize;
        assert_corrupted(
            KeyIdxPrefix::parse(
                &s.bytes[..prefix_len - 1],
                s.prefix_len,
                s.bytes.len() as u64,
            ),
            "prefix read is",
        );
    }

    #[test]
    fn header_field_violations_are_corrupted() {
        let a = acceptance_section();
        let s = &a.section;
        let p = s.prefix_len;
        let check = |bytes: Vec<u8>, needle: &str| {
            assert_corrupted(probe_user(&bytes, p, &a.key), needle);
            assert_corrupted(verify_key_idx(&bytes, p), needle);
        };
        check(reseal(&s.bytes, |h| h.reserved = 1), "reserved field is 1");
        check(reseal(&s.bytes, |h| h.reserved = 0x8000), "reserved field");
        check(reseal(&s.bytes, |h| h.version = 2), "version 2");
        check(reseal(&s.bytes, |h| h.version = 0), "version 0");
        check(reseal(&s.bytes, |h| h.bucket_bits = 0), "bucket_bits 0");
        check(reseal(&s.bytes, |h| h.bucket_bits = 17), "bucket_bits 17");
        check(
            reseal(&s.bytes, |h| h.prefix_len += 1),
            "differs from footer",
        );
        check(reseal(&s.bytes, |h| h.fields[2].key_type = 0), "key_type 0");
        check(reseal(&s.bytes, |h| h.fields[2].key_type = 5), "key_type 5");
        check(reseal(&s.bytes, |h| h.fields.swap(0, 2)), "not ascending");
        check(
            reseal(&s.bytes, |h| h.fields[1].name = h.fields[0].name.clone()),
            "not ascending",
        );
        check(
            reseal(&s.bytes, |h| {
                h.fields[2].dir_offset = u64::from(h.prefix_len)
            }),
            "dir_offset",
        );
        check(
            reseal(&s.bytes, |h| {
                h.fields[2].dir_offset = u64::from(h.prefix_len) - 4
            }),
            "dir_offset",
        );
        check(
            reseal(&s.bytes, |h| h.fields[2].dir_offset = 0),
            "dir_offset",
        );
        check(
            reseal(&s.bytes, |h| {
                h.fields[2].buckets_offset = u64::from(h.prefix_len) - 1
            }),
            "buckets_offset",
        );
        check(
            reseal(&s.bytes, |h| h.fields[2].buckets_len += 1),
            "past the section length",
        );
        check(
            reseal(&s.bytes, |h| h.fields[1].entry_count = 1),
            "inconsistent",
        );
        check(
            reseal(&s.bytes, |h| h.fields[0].name = vec![0xff]),
            "not UTF-8",
        );
    }

    #[test]
    fn a_dir_offset_inside_the_header_is_corrupted() {
        let a = acceptance_section();
        let s = &a.section;
        let p = s.prefix_len;
        let mut boundary_checked = false;
        for d in 0..128u64 {
            let bytes = reseal(&s.bytes, |h| h.fields[2].dir_offset = d);
            let (_, crc_at) = parse_raw_header(&bytes).expect("raw");
            let header_len = crc_at as u64 + 4;
            if d >= header_len {
                break;
            }
            boundary_checked |= d == header_len - 1;
            let needle = format!("dir_offset {d} outside [{header_len}, {p})");
            assert_corrupted(probe_user(&bytes, p, &a.key), &needle);
            assert_corrupted(verify_key_idx(&bytes, p), &needle);
        }
        assert!(
            boundary_checked,
            "the last header_crc32c byte was not probed"
        );
    }

    #[test]
    fn footer_prefix_len_violations_are_corrupted() {
        let a = acceptance_section();
        let s = &a.section;
        let len = s.bytes.len() as u64;
        assert_corrupted(
            probe_user(&s.bytes, s.prefix_len + 1, &a.key),
            "differs from footer",
        );
        assert_corrupted(
            probe_user(&s.bytes, s.prefix_len - 1, &a.key),
            "differs from footer",
        );
        assert_corrupted(probe_user(&s.bytes, 0, &a.key), "prefix_len is zero");
        assert_corrupted(
            probe_user(&s.bytes, len as u32 + 1, &a.key),
            "past the section length",
        );
        assert_corrupted(key_idx_prefix_range(0, len), "zero");
        assert_corrupted(
            key_idx_prefix_range(KEY_IDX_PREFIX_MAX + 1, u64::MAX),
            "over KEY_IDX_PREFIX_MAX",
        );
        assert!(key_idx_prefix_range(KEY_IDX_PREFIX_MAX, u64::from(KEY_IDX_PREFIX_MAX)).is_ok());
        // Over the ceiling on a section long enough to hold it: refused before
        // the read is issued.
        let mut padded = s.bytes.clone();
        padded.resize(KEY_IDX_PREFIX_MAX as usize + 16, 0);
        let mut reads = 0;
        let result = probe_ranged(
            padded.len() as u64,
            KEY_IDX_PREFIX_MAX + 1,
            "user_id",
            KeyType::I64,
            &a.key,
            |r| {
                reads += 1;
                Ok(padded[r.start as usize..r.end as usize].to_vec())
            },
        );
        assert_corrupted(result, "over KEY_IDX_PREFIX_MAX");
        assert_eq!(reads, 0);
    }

    #[test]
    fn directory_violations_are_corrupted() {
        let a = acceptance_section();
        let s = &a.section;
        let p = s.prefix_len;
        let decreasing = rewrite_dir(s, "user_id", |ends| {
            let last = *ends.last().expect("ends");
            ends[0] = last;
        });
        assert_corrupted(probe_user(&decreasing, p, &a.key), "ends decrease");
        assert_corrupted(verify_key_idx(&decreasing, p), "ends decrease");
        let short = rewrite_dir(s, "user_id", |ends| {
            // Every end equal to the last moves down one: still monotonic.
            let last = *ends.last().expect("ends");
            for end in ends.iter_mut().filter(|e| **e == last) {
                *end -= 1;
            }
        });
        assert_corrupted(probe_user(&short, p, &a.key), "is not buckets_len");
        assert_corrupted(verify_key_idx(&short, p), "is not buckets_len");
    }

    fn part(name: &str, key_type: u8, entry_count: u64, frames: Vec<Vec<u8>>) -> FieldParts {
        FieldParts {
            name: name.as_bytes().to_vec(),
            key_type,
            entry_count,
            frames,
        }
    }

    /// One field at `bucket_bits` 1 whose bucket `bucket` holds `frame`.
    fn one_frame_section(bucket: u32, frame: Vec<u8>, entry_count: u64) -> EncodedKeyIdx {
        let mut frames = vec![Vec::new(), Vec::new()];
        frames[bucket as usize] = frame;
        assemble(1, &[part("k", 2, entry_count, frames)]).expect("assemble")
    }

    fn raw_frame(count: u32, raw: &[u8]) -> Vec<u8> {
        let compressed = zstd::bulk::compress(raw, 3).expect("compress");
        let mut out = count.to_le_bytes().to_vec();
        out.extend_from_slice(&crc32c::crc32c(&compressed).to_le_bytes());
        out.extend_from_slice(&compressed);
        out
    }

    fn entry_bytes(entries: &[(Key8, u32)]) -> Vec<u8> {
        let mut raw = Vec::new();
        for (k, b) in entries {
            raw.extend_from_slice(k);
            raw.extend_from_slice(&b.to_le_bytes());
        }
        raw
    }

    /// Two i64 key8 values in bucket 0 at one bit and one in bucket 1, the
    /// first two ascending.
    fn keys_by_bucket() -> (Key8, Key8, Key8) {
        let mut zero = (0i64..)
            .map(key8_i64)
            .filter(|k| reference_bucket(k, 1) == 0);
        let lo = zero.next().expect("lo");
        let hi = zero.next().expect("hi");
        let one = (0i64..)
            .map(key8_i64)
            .find(|k| reference_bucket(k, 1) == 1)
            .expect("one");
        assert!(lo < hi);
        (lo, hi, one)
    }

    fn frame_check(s: &EncodedKeyIdx, key: &Key8, needle: &str) {
        assert_corrupted(
            probe(&s.bytes, s.prefix_len, "k", KeyType::I64, key),
            needle,
        );
        assert_corrupted(verify_key_idx(&s.bytes, s.prefix_len), needle);
    }

    #[test]
    fn a_frame_valid_as_written_round_trips() {
        let (lo, hi, _) = keys_by_bucket();
        let s = one_frame_section(
            0,
            encode_frame(&[(lo, 1), (lo, 4), (hi, 2)]).expect("frame"),
            3,
        );
        assert_eq!(
            probe(&s.bytes, s.prefix_len, "k", KeyType::I64, &lo).expect("probe"),
            Some(vec![1, 4])
        );
        verify_key_idx(&s.bytes, s.prefix_len).expect("verify");
    }

    #[test]
    fn a_bucket_holding_a_key_that_mixes_elsewhere_is_corrupted() {
        let (lo, _, one) = keys_by_bucket();
        let s = one_frame_section(0, encode_frame(&[(lo, 1), (one, 2)]).expect("frame"), 2);
        frame_check(&s, &lo, "mixes to bucket 1");
    }

    #[test]
    fn unsorted_and_duplicate_entries_are_corrupted() {
        let (lo, hi, _) = keys_by_bucket();
        let unsorted = one_frame_section(0, encode_frame(&[(hi, 1), (lo, 2)]).expect("f"), 2);
        frame_check(&unsorted, &lo, "not sorted");
        let unsorted_blocks =
            one_frame_section(0, encode_frame(&[(lo, 2), (lo, 1)]).expect("f"), 2);
        frame_check(&unsorted_blocks, &lo, "not sorted");
        let duplicate = one_frame_section(0, encode_frame(&[(lo, 1), (lo, 1)]).expect("f"), 2);
        frame_check(&duplicate, &lo, "duplicate entry");
    }

    #[test]
    fn an_oversized_frame_entry_count_is_refused_before_allocation() {
        // The payload is not zstd: had the count been accepted, decompression
        // (the only allocation of the entry buffer) would have run and failed
        // with a "does not decompress" message instead of the bound's.
        let garbage = [0xde, 0xad, 0xbe, 0xef];
        let frame = |count: u32| {
            let mut out = count.to_le_bytes().to_vec();
            out.extend_from_slice(&crc32c::crc32c(&garbage).to_le_bytes());
            out.extend_from_slice(&garbage);
            out
        };
        let (lo, _, _) = keys_by_bucket();
        let over_max = one_frame_section(
            0,
            frame(KEY_IDX_BUCKET_MAX_ENTRIES + 1),
            u64::from(KEY_IDX_BUCKET_MAX_ENTRIES) + 1,
        );
        frame_check(
            &over_max,
            &lo,
            "over KEY_IDX_BUCKET_MAX_ENTRIES 1048576, refused before allocation",
        );
        // A field total at the header's own ceiling (2 buckets x the bucket
        // bound) still lets the frame bound fire for u32::MAX.
        let over_max = one_frame_section(
            0,
            frame(u32::MAX),
            2 * u64::from(KEY_IDX_BUCKET_MAX_ENTRIES),
        );
        frame_check(&over_max, &lo, "over KEY_IDX_BUCKET_MAX_ENTRIES");
        let over_total = one_frame_section(0, frame(5), 4);
        frame_check(
            &over_total,
            &lo,
            "over the field's entry_count 4, refused before allocation",
        );
        // At the bound the count is accepted and decompression is reached.
        let at_total = one_frame_section(0, frame(4), 4);
        frame_check(&at_total, &lo, "does not decompress");
        let zero = one_frame_section(0, frame(0), 1);
        frame_check(&zero, &lo, "entry_count is 0");
    }

    #[test]
    fn a_decompressed_length_other_than_entry_count_x_12_is_corrupted() {
        let (lo, hi, _) = keys_by_bucket();
        let one_entry = entry_bytes(&[(lo, 1)]);
        frame_check(
            &one_frame_section(0, raw_frame(2, &one_entry), 2),
            &lo,
            "decompressed to 12 bytes",
        );
        let mut thirteen = one_entry.clone();
        thirteen.push(0);
        frame_check(
            &one_frame_section(0, raw_frame(1, &thirteen), 1),
            &lo,
            "does not decompress",
        );
        let two_entries = entry_bytes(&[(lo, 1), (hi, 1)]);
        frame_check(
            &one_frame_section(0, raw_frame(1, &two_entries), 2),
            &lo,
            "does not decompress",
        );
        let mut short = one_entry.clone();
        short.pop();
        frame_check(
            &one_frame_section(0, raw_frame(1, &short), 1),
            &lo,
            "decompressed to 11 bytes",
        );
        let truncated_frame = one_frame_section(0, vec![1, 0, 0, 0, 0], 1);
        frame_check(&truncated_frame, &lo, "frame truncated");
    }

    #[test]
    fn field_set_violations_are_corrupted() {
        let (lo, _, _) = keys_by_bucket();
        let empty = || vec![Vec::new(), Vec::new()];
        let check = |parts: Vec<FieldParts>, needle: &str| {
            let s = assemble(1, &parts).expect("assemble");
            assert_corrupted(
                probe(&s.bytes, s.prefix_len, "a", KeyType::I64, &lo),
                needle,
            );
            assert_corrupted(verify_key_idx(&s.bytes, s.prefix_len), needle);
        };
        check(
            vec![part("b", 2, 0, empty()), part("a", 2, 0, empty())],
            "not ascending",
        );
        check(
            vec![part("a", 2, 0, empty()), part("a", 2, 0, empty())],
            "not ascending",
        );
        check(vec![part("a", 0, 0, empty())], "key_type 0");
        check(vec![part("a", 5, 0, empty())], "key_type 5");
        check(vec![part("", 2, 0, empty())], "name_len 0");
        check(vec![part(&"a".repeat(129), 2, 0, empty())], "name_len 129");
        check(Vec::new(), "field_count 0");
        let many: Vec<FieldParts> = (0..65)
            .map(|i| part(&format!("f{i:03}"), 2, 0, empty()))
            .collect();
        check(many, "field_count 65");
        let ok: Vec<FieldParts> = (0..64)
            .map(|i| part(&format!("f{i:03}"), 2, 0, empty()))
            .collect();
        let s = assemble(1, &ok).expect("assemble");
        assert_eq!(
            probe(&s.bytes, s.prefix_len, "f063", KeyType::I64, &lo).expect("probe"),
            Some(vec![])
        );
    }

    #[test]
    fn verify_checks_the_bucket_counts_sum_that_a_probe_cannot() {
        let (lo, hi, _) = keys_by_bucket();
        // Header says 3 entries; the one frame holds 2.
        let s = one_frame_section(0, encode_frame(&[(lo, 1), (hi, 2)]).expect("frame"), 3);
        assert_eq!(
            probe(&s.bytes, s.prefix_len, "k", KeyType::I64, &lo).expect("probe"),
            Some(vec![1])
        );
        assert_corrupted(verify_key_idx(&s.bytes, s.prefix_len), "sum to 2");
    }

    // -- properties -----------------------------------------------------------

    fn block() -> impl Strategy<Value = u32> {
        prop_oneof![0u32..48, any::<u32>()]
    }

    /// A field's key type and its entries, drawn from small value domains so
    /// duplicates and shared key8 prefixes are common.
    fn typed_entries() -> impl Strategy<Value = (KeyType, Vec<(Key8, u32)>)> {
        let n = 0usize..60;
        prop_oneof![
            prop::collection::vec((0u8..6, any::<u8>(), block()), n.clone()).prop_map(|v| {
                let entries = v
                    .into_iter()
                    .map(|(head, tail, b)| {
                        let mut id = [0u8; 16];
                        id[0] = head;
                        id[15] = tail;
                        (key8_id16(&id), b)
                    })
                    .collect();
                (KeyType::Id16, entries)
            }),
            prop::collection::vec((prop_oneof![-4i64..4, any::<i64>()], block()), n.clone())
                .prop_map(|v| (
                    KeyType::I64,
                    v.into_iter().map(|(x, b)| (key8_i64(x), b)).collect()
                )),
            prop::collection::vec(("[a-c]{0,3}", block()), n.clone()).prop_map(|v| (
                KeyType::Str,
                v.into_iter().map(|(x, b)| (key8_str(&x), b)).collect()
            )),
            prop::collection::vec((prop::collection::vec(any::<u8>(), 0..3), block()), n).prop_map(
                |v| (
                    KeyType::Bytes,
                    v.into_iter().map(|(x, b)| (key8_bytes(&x), b)).collect()
                )
            ),
        ]
    }

    proptest! {
        #[test]
        fn round_trip_names_exactly_the_written_blocks(
            fields in prop::collection::btree_map("[a-z_]{1,10}", typed_entries(), 1..4),
            bucket_bits in prop_oneof![Just(1u8), Just(8u8), Just(16u8)],
            absent in any::<[u8; 8]>(),
        ) {
            let inputs: Vec<KeyIdxFieldInput> = fields
                .iter()
                .map(|(name, (key_type, entries))| KeyIdxFieldInput {
                    name: name.clone(),
                    key_type: *key_type,
                    entries: entries.clone(),
                })
                .collect();
            let s = encode_key_idx_with_bucket_bits(&inputs, bucket_bits).expect("encode");
            let summary = verify_key_idx(&s.bytes, s.prefix_len).expect("verify");
            prop_assert_eq!(summary.bucket_bits, bucket_bits);
            prop_assert_eq!(summary.fields.len(), inputs.len());
            let decoded = decode_key_idx(&s.bytes, s.prefix_len).expect("decode");
            prop_assert_eq!(decoded.len(), inputs.len());
            for (input, field) in inputs.iter().zip(&decoded) {
                let model: BTreeSet<(Key8, u32)> = input.entries.iter().copied().collect();
                let got: BTreeSet<(Key8, u32)> = field.entries.iter().copied().collect();
                prop_assert_eq!(&field.name, &input.name);
                prop_assert_eq!(field.key_type, input.key_type);
                prop_assert_eq!(field.entries.len(), model.len());
                prop_assert_eq!(got, model);
            }
            let prefix = KeyIdxPrefix::parse(
                &s.bytes[..s.prefix_len as usize],
                s.prefix_len,
                s.bytes.len() as u64,
            )
            .expect("prefix");
            for (input, field) in inputs.iter().zip(&summary.fields) {
                let mut model: BTreeMap<Key8, BTreeSet<u32>> = BTreeMap::new();
                for (k, b) in &input.entries {
                    model.entry(*k).or_default().insert(*b);
                }
                let distinct: usize = model.values().map(BTreeSet::len).sum();
                prop_assert_eq!(&field.name, &input.name);
                prop_assert_eq!(field.key_type, input.key_type);
                prop_assert_eq!(field.entry_count, distinct as u64);
                prop_assert_eq!(field.bucket_entry_counts.len(), 1usize << bucket_bits);
                for (k, blocks) in &model {
                    let got = probe(&s.bytes, s.prefix_len, &input.name, input.key_type, k).expect("probe");
                    prop_assert_eq!(got, Some(blocks.iter().copied().collect::<Vec<_>>()));
                    let located = prefix.locate(&input.name, input.key_type, k).expect("locate").expect("named");
                    prop_assert_eq!(located.bucket(), reference_bucket(k, bucket_bits));
                }
                if !model.contains_key(&absent) {
                    let got = probe(&s.bytes, s.prefix_len, &input.name, input.key_type, &absent).expect("probe");
                    prop_assert_eq!(got, Some(vec![]));
                }
            }
            let got = probe(&s.bytes, s.prefix_len, "~unnamed", KeyType::I64, &absent).expect("probe");
            prop_assert_eq!(got, None);
        }

        #[test]
        fn bucket_index_is_the_multiplicative_mix(key8 in any::<[u8; 8]>(), bits in 1u8..=16) {
            prop_assert_eq!(bucket_index(&key8, bits).expect("in range"), reference_bucket(&key8, bits));
        }
    }
}
