//! Codec and two-GET probe for the per-part key-index leaf (`.kidx`,
//! ADR-2707 decision 4): the second tier of the exact-key index, one object
//! per (part, field, key slice). docs/catalog-and-mvcc.md "Per-part key-index
//! leaves" is the normative layout and reader procedure.
//!
//! Encode, split, decode and probe. The fold (`fold.rs`) writes the leaves;
//! no query reads one yet. The object key is the caller's to fill in on the
//! returned [`SnapshotKeyIndexLeafRef`].

use std::future::Future;
use std::ops::Range;

use bytes::Bytes;
use prost::Message;
use ravel_codec::key_idx::{KEY_IDX_MAX_NAME_LEN, Key8, KeyType, bucket_index};
use ravel_object_store::GetRange;
use ravel_proto::catalog::v1::{KeyIndexLeafHeader, SnapshotKeyIndexLeafRef};
use ravel_types::{Signal, TenantHash};

use super::{DEFAULT_MAX_COLUMN_STATS_BYTES, ZSTD_LEVEL};
use crate::{Catalog, CatalogError};

/// First 4 bytes of every key-index leaf.
pub const KEY_INDEX_LEAF_MAGIC: [u8; 4] = *b"RKI1";

/// The header `format_version` this module writes and reads. Any other
/// value rejects the leaf, and the part reads as uncovered.
pub const KEY_INDEX_LEAF_FORMAT_VERSION: u32 = 1;

/// Smallest `bucket_bits` either side accepts.
pub const KEY_INDEX_LEAF_MIN_BUCKET_BITS: u8 = 1;

/// Largest `bucket_bits` the writer chooses: the balance value at the body
/// ceiling.
pub const KEY_INDEX_LEAF_WRITER_MAX_BUCKET_BITS: u8 = 13;

/// Largest `bucket_bits` the reader accepts, checked before the directory
/// is read.
pub const KEY_INDEX_LEAF_READER_MAX_BUCKET_BITS: u8 = 16;

/// Default cap on decompressed body bytes: the column-statistics ceiling,
/// in that constant's own meaning.
pub const DEFAULT_MAX_KEY_INDEX_LEAF_BODY_BYTES: u64 = DEFAULT_MAX_COLUMN_STATS_BYTES;

/// magic, header_len, prefix_len, header_crc32c and dir_crc32c.
const FIXED_PREFIX_LEN: u64 = 4 + 4 + 4 + 4 + 4;
/// entry_count, uncompressed_len and frame_crc32c.
const FRAME_HEADER_LEN: usize = 12;
const MAX_VARINT_BYTES: usize = 10;

/// The body ceiling. The writer applies it to the whole leaf, a reader to
/// each frame it decompresses.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyIndexLeafLimits {
    pub max_body_bytes: u64,
}

impl Default for KeyIndexLeafLimits {
    fn default() -> Self {
        KeyIndexLeafLimits {
            max_body_bytes: DEFAULT_MAX_KEY_INDEX_LEAF_BODY_BYTES,
        }
    }
}

/// One index entry: a `key8` present in one part entry, and the ascending
/// block ordinals of that entry's segment holding it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyIndexLeafEntry {
    pub key8: Key8,
    pub entry_ordinal: u64,
    pub blocks: Vec<u32>,
}

/// Everything a leaf records. `entries` may be in any order;
/// `uncovered_entry_ordinals` may be unsorted and may repeat.
#[derive(Debug, Clone, Copy)]
pub struct KeyIndexLeafInput<'a> {
    pub tenant_hash: TenantHash,
    pub signal: Signal,
    pub part_blake3: [u8; 32],
    pub part_entry_count: u64,
    pub field: &'a str,
    pub key_type: KeyType,
    pub key8_lo: u64,
    pub key8_hi: u64,
    pub uncovered_entry_ordinals: &'a [u64],
    pub entries: &'a [KeyIndexLeafEntry],
}

/// A leaf's bytes and the ref HEAD carries for it. `leaf_ref.key` is empty:
/// the object key is the caller's.
#[derive(Debug, Clone, PartialEq)]
pub struct EncodedKeyIndexLeaf {
    pub bytes: Vec<u8>,
    pub leaf_ref: SnapshotKeyIndexLeafRef,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KeyIndexLeafEncodeError {
    #[error("invalid key-index leaf input: {0}")]
    InvalidInput(String),
    /// The decompressed body would exceed the ceiling; split the entries
    /// with [`split_key_index_entries`].
    #[error("key-index leaf body of {body} decompressed bytes exceeds the {ceiling}-byte ceiling")]
    BodyOverCeiling { body: u64, ceiling: u64 },
    #[error("bucket_bits {bucket_bits} outside {min}..={max}")]
    BucketBitsOutOfRange { bucket_bits: u8, min: u8, max: u8 },
    #[error("zstd compression failed: {0}")]
    Compress(String),
}

fn invalid(msg: impl std::fmt::Display) -> KeyIndexLeafEncodeError {
    KeyIndexLeafEncodeError::InvalidInput(msg.to_string())
}

fn signal_code(signal: Signal) -> u32 {
    u32::try_from(ravel_commit::signal::to_proto(signal) as i32).unwrap_or(0)
}

fn uvarint_len(value: u64) -> u64 {
    let bits = 64 - u64::from(value.leading_zeros());
    bits.div_ceil(7).max(1)
}

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

fn read_uvarint(bytes: &[u8], pos: &mut usize) -> Result<u64, KeyIndexLeafRejection> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    for i in 0..MAX_VARINT_BYTES {
        let byte = *bytes
            .get(*pos)
            .ok_or_else(|| corrupted("entry truncated inside a uvarint"))?;
        *pos += 1;
        let low7 = u64::from(byte & 0x7f);
        if i == MAX_VARINT_BYTES - 1 && low7 > 1 {
            return Err(corrupted("uvarint overflows u64"));
        }
        result |= low7 << shift;
        if byte & 0x80 == 0 {
            return Ok(result);
        }
        shift += 7;
    }
    Err(corrupted("uvarint longer than 10 bytes"))
}

/// Delta from the previous block ordinal, the first taken from -1.
fn block_deltas(blocks: &[u32]) -> impl Iterator<Item = u64> + '_ {
    let mut prev = u64::MAX;
    blocks.iter().map(move |&block| {
        let delta = u64::from(block).wrapping_sub(prev);
        prev = u64::from(block);
        delta
    })
}

/// Bytes `entry` occupies in a decompressed bucket.
pub fn encoded_entry_len(entry: &KeyIndexLeafEntry) -> u64 {
    8 + uvarint_len(entry.entry_ordinal)
        + uvarint_len(entry.blocks.len() as u64)
        + block_deltas(&entry.blocks).map(uvarint_len).sum::<u64>()
}

fn write_entry(out: &mut Vec<u8>, entry: &KeyIndexLeafEntry) {
    out.extend_from_slice(&entry.key8);
    write_uvarint(out, entry.entry_ordinal);
    write_uvarint(out, entry.blocks.len() as u64);
    for delta in block_deltas(&entry.blocks) {
        write_uvarint(out, delta);
    }
}

/// The `bucket_bits` in 1..=13 whose balance body `4 x 2^(2b)` is nearest
/// `body` decompressed bytes (ties go to the smaller value).
pub fn choose_key_index_bucket_bits(body: u64) -> u8 {
    let mut best = KEY_INDEX_LEAF_MIN_BUCKET_BITS;
    let mut best_diff = u64::MAX;
    for bits in KEY_INDEX_LEAF_MIN_BUCKET_BITS..=KEY_INDEX_LEAF_WRITER_MAX_BUCKET_BITS {
        let diff = (4u64 << (2 * u32::from(bits))).abs_diff(body);
        if diff < best_diff {
            best = bits;
            best_diff = diff;
        }
    }
    best
}

struct Prepared<'a> {
    entries: Vec<&'a KeyIndexLeafEntry>,
    uncovered: Vec<u64>,
    body: u64,
}

fn prepare<'a>(
    input: &KeyIndexLeafInput<'a>,
    limits: &KeyIndexLeafLimits,
) -> Result<Prepared<'a>, KeyIndexLeafEncodeError> {
    if input.field.is_empty() || input.field.len() > KEY_IDX_MAX_NAME_LEN {
        return Err(invalid(format_args!(
            "field name must be 1..={KEY_IDX_MAX_NAME_LEN} bytes, got {}",
            input.field.len()
        )));
    }
    if input.key8_lo > input.key8_hi {
        return Err(invalid(format_args!(
            "key8_lo {:#018x} above key8_hi {:#018x}",
            input.key8_lo, input.key8_hi
        )));
    }
    let mut uncovered = input.uncovered_entry_ordinals.to_vec();
    uncovered.sort_unstable();
    uncovered.dedup();
    if let Some(&last) = uncovered.last()
        && last >= input.part_entry_count
    {
        return Err(invalid(format_args!(
            "uncovered entry ordinal {last} at or past part_entry_count {}",
            input.part_entry_count
        )));
    }
    let mut entries: Vec<&KeyIndexLeafEntry> = input.entries.iter().collect();
    entries.sort_by_key(|e| (e.key8, e.entry_ordinal));
    let mut body: u64 = 0;
    let mut prev: Option<(Key8, u64)> = None;
    for entry in &entries {
        let pair = (entry.key8, entry.entry_ordinal);
        if prev == Some(pair) {
            return Err(invalid(format_args!(
                "duplicate entry (key8 {}, entry ordinal {})",
                hex::encode(entry.key8),
                entry.entry_ordinal
            )));
        }
        prev = Some(pair);
        let key = u64::from_be_bytes(entry.key8);
        if key < input.key8_lo || key > input.key8_hi {
            return Err(invalid(format_args!(
                "key8 {key:#018x} outside the slice [{:#018x}, {:#018x}]",
                input.key8_lo, input.key8_hi
            )));
        }
        if entry.entry_ordinal >= input.part_entry_count {
            return Err(invalid(format_args!(
                "entry ordinal {} at or past part_entry_count {}",
                entry.entry_ordinal, input.part_entry_count
            )));
        }
        if uncovered.binary_search(&entry.entry_ordinal).is_ok() {
            return Err(invalid(format_args!(
                "entry ordinal {} is both indexed and uncovered",
                entry.entry_ordinal
            )));
        }
        if entry.blocks.is_empty() {
            return Err(invalid(format_args!(
                "entry ordinal {} has an empty block set",
                entry.entry_ordinal
            )));
        }
        if entry.blocks.windows(2).any(|w| w[0] >= w[1]) {
            return Err(invalid(format_args!(
                "entry ordinal {} has a block set that is not strictly ascending",
                entry.entry_ordinal
            )));
        }
        body = body.saturating_add(encoded_entry_len(entry));
    }
    if body > limits.max_body_bytes {
        return Err(KeyIndexLeafEncodeError::BodyOverCeiling {
            body,
            ceiling: limits.max_body_bytes,
        });
    }
    Ok(Prepared {
        entries,
        uncovered,
        body,
    })
}

/// Encodes one leaf, choosing `bucket_bits` with
/// [`choose_key_index_bucket_bits`]. A body over `limits.max_body_bytes` is
/// [`KeyIndexLeafEncodeError::BodyOverCeiling`].
pub fn encode_key_index_leaf(
    input: &KeyIndexLeafInput<'_>,
    limits: &KeyIndexLeafLimits,
) -> Result<EncodedKeyIndexLeaf, KeyIndexLeafEncodeError> {
    let prepared = prepare(input, limits)?;
    let bucket_bits = choose_key_index_bucket_bits(prepared.body);
    encode_prepared(input, prepared, bucket_bits)
}

/// [`encode_key_index_leaf`] at a fixed `bucket_bits` within the writer's
/// bound, 1..=[`KEY_INDEX_LEAF_WRITER_MAX_BUCKET_BITS`]. Any other value is
/// [`KeyIndexLeafEncodeError::BucketBitsOutOfRange`].
pub fn encode_key_index_leaf_with_bucket_bits(
    input: &KeyIndexLeafInput<'_>,
    bucket_bits: u8,
    limits: &KeyIndexLeafLimits,
) -> Result<EncodedKeyIndexLeaf, KeyIndexLeafEncodeError> {
    encode_with_bucket_bits_up_to(
        input,
        bucket_bits,
        KEY_INDEX_LEAF_WRITER_MAX_BUCKET_BITS,
        limits,
    )
}

/// Builds a leaf at any `bucket_bits` the reader accepts, 1..=16, so tests
/// reach 14..=16, which neither public encoder produces.
#[cfg(test)]
pub(crate) fn encode_key_index_leaf_with_reader_bucket_bits(
    input: &KeyIndexLeafInput<'_>,
    bucket_bits: u8,
    limits: &KeyIndexLeafLimits,
) -> Result<EncodedKeyIndexLeaf, KeyIndexLeafEncodeError> {
    encode_with_bucket_bits_up_to(
        input,
        bucket_bits,
        KEY_INDEX_LEAF_READER_MAX_BUCKET_BITS,
        limits,
    )
}

fn encode_with_bucket_bits_up_to(
    input: &KeyIndexLeafInput<'_>,
    bucket_bits: u8,
    max: u8,
    limits: &KeyIndexLeafLimits,
) -> Result<EncodedKeyIndexLeaf, KeyIndexLeafEncodeError> {
    if !(KEY_INDEX_LEAF_MIN_BUCKET_BITS..=max).contains(&bucket_bits) {
        return Err(KeyIndexLeafEncodeError::BucketBitsOutOfRange {
            bucket_bits,
            min: KEY_INDEX_LEAF_MIN_BUCKET_BITS,
            max,
        });
    }
    let prepared = prepare(input, limits)?;
    encode_prepared(input, prepared, bucket_bits)
}

fn to_u32(value: usize, what: &str) -> Result<u32, KeyIndexLeafEncodeError> {
    u32::try_from(value).map_err(|_| invalid(format_args!("{what} {value} does not fit u32")))
}

fn encode_prepared(
    input: &KeyIndexLeafInput<'_>,
    prepared: Prepared<'_>,
    bucket_bits: u8,
) -> Result<EncodedKeyIndexLeaf, KeyIndexLeafEncodeError> {
    let bucket_count = 1usize << bucket_bits;
    let mut raw: Vec<Vec<u8>> = vec![Vec::new(); bucket_count];
    let mut counts = vec![0u32; bucket_count];
    for entry in &prepared.entries {
        let bucket = bucket_index(&entry.key8, bucket_bits).map_err(invalid)? as usize;
        write_entry(&mut raw[bucket], entry);
        counts[bucket] = counts[bucket]
            .checked_add(1)
            .ok_or_else(|| invalid("bucket entry count overflows u32"))?;
    }
    let mut body = Vec::new();
    let mut ends = Vec::with_capacity(bucket_count * 4);
    for (bucket, raw_bucket) in raw.iter().enumerate() {
        if !raw_bucket.is_empty() {
            let compressed = zstd::bulk::compress(raw_bucket, ZSTD_LEVEL)
                .map_err(|e| KeyIndexLeafEncodeError::Compress(e.to_string()))?;
            body.extend_from_slice(&counts[bucket].to_le_bytes());
            body.extend_from_slice(&to_u32(raw_bucket.len(), "bucket length")?.to_le_bytes());
            body.extend_from_slice(&crc32c::crc32c(&compressed).to_le_bytes());
            body.extend_from_slice(&compressed);
        }
        ends.extend_from_slice(&to_u32(body.len(), "body length")?.to_le_bytes());
    }
    let header = KeyIndexLeafHeader {
        format_version: KEY_INDEX_LEAF_FORMAT_VERSION,
        tenant_hash: input.tenant_hash.0.to_vec(),
        signal: signal_code(input.signal),
        part_blake3: input.part_blake3.to_vec(),
        field: input.field.to_string(),
        key_type: u32::from(input.key_type.as_u8()),
        key8_lo: input.key8_lo,
        key8_hi: input.key8_hi,
        bucket_bits: u32::from(bucket_bits),
        entry_count: prepared.entries.len() as u64,
        part_entry_count: input.part_entry_count,
        uncovered_entry_ordinals: prepared.uncovered,
        body_len: body.len() as u64,
    };
    let header_bytes = header.encode_to_vec();
    let header_len = to_u32(header_bytes.len(), "header length")?;
    let prefix_len = to_u32(
        FIXED_PREFIX_LEN as usize + header_bytes.len() + ends.len(),
        "prefix length",
    )?;
    let mut bytes = Vec::with_capacity(prefix_len as usize + body.len());
    bytes.extend_from_slice(&KEY_INDEX_LEAF_MAGIC);
    bytes.extend_from_slice(&header_len.to_le_bytes());
    bytes.extend_from_slice(&prefix_len.to_le_bytes());
    bytes.extend_from_slice(&header_bytes);
    let header_crc = crc32c::crc32c(&bytes);
    bytes.extend_from_slice(&header_crc.to_le_bytes());
    bytes.extend_from_slice(&ends);
    bytes.extend_from_slice(&crc32c::crc32c(&ends).to_le_bytes());
    bytes.extend_from_slice(&body);
    let leaf_ref = SnapshotKeyIndexLeafRef {
        key: String::new(),
        blake3: blake3::hash(&bytes).as_bytes().to_vec(),
        size: bytes.len() as u64,
        field: header.field.clone(),
        key_type: header.key_type,
        key8_lo: header.key8_lo,
        key8_hi: header.key8_hi,
        prefix_len,
        bucket_bits: header.bucket_bits,
        entry_count: header.entry_count,
        uncovered_count: header.uncovered_entry_ordinals.len() as u64,
        part_blake3: header.part_blake3.clone(),
    };
    Ok(EncodedKeyIndexLeaf { bytes, leaf_ref })
}

/// One leaf's share of a (part, field): its inclusive key slice and the
/// range of the sorted entry list it holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyIndexSlice {
    pub key8_lo: u64,
    pub key8_hi: u64,
    pub entries: Range<usize>,
}

/// Splits `entries`, sorted ascending by `(key8, entry_ordinal)`, into slices
/// whose decompressed bodies each stay at or below `max_body_bytes`. Cuts
/// fall only between distinct `key8` values, and the slices' bounds tile
/// `[0, u64::MAX]`: the first starts at 0, the last ends at `u64::MAX`, and
/// each starts one past its predecessor's end. An empty list is one slice.
/// One `key8` whose entries alone exceed the ceiling is
/// [`KeyIndexLeafEncodeError::BodyOverCeiling`]; an unsorted list is
/// [`KeyIndexLeafEncodeError::InvalidInput`].
pub fn split_key_index_entries(
    entries: &[KeyIndexLeafEntry],
    max_body_bytes: u64,
) -> Result<Vec<KeyIndexSlice>, KeyIndexLeafEncodeError> {
    if entries
        .windows(2)
        .any(|w| (w[0].key8, w[0].entry_ordinal) >= (w[1].key8, w[1].entry_ordinal))
    {
        return Err(invalid(
            "entries are not sorted and unique by (key8, entry_ordinal)",
        ));
    }
    let mut slices = Vec::new();
    let mut slice_start = 0usize;
    let mut slice_lo = 0u64;
    let mut slice_body = 0u64;
    let mut run_start = 0usize;
    while run_start < entries.len() {
        let key8 = entries[run_start].key8;
        let mut run_end = run_start;
        let mut run_body = 0u64;
        while run_end < entries.len() && entries[run_end].key8 == key8 {
            run_body = run_body.saturating_add(encoded_entry_len(&entries[run_end]));
            run_end += 1;
        }
        if run_body > max_body_bytes {
            return Err(KeyIndexLeafEncodeError::BodyOverCeiling {
                body: run_body,
                ceiling: max_body_bytes,
            });
        }
        if run_start > slice_start && slice_body.saturating_add(run_body) > max_body_bytes {
            let cut = u64::from_be_bytes(key8);
            slices.push(KeyIndexSlice {
                key8_lo: slice_lo,
                key8_hi: cut - 1,
                entries: slice_start..run_start,
            });
            slice_start = run_start;
            slice_lo = cut;
            slice_body = 0;
        }
        slice_body += run_body;
        run_start = run_end;
    }
    slices.push(KeyIndexSlice {
        key8_lo: slice_lo,
        key8_hi: u64::MAX,
        entries: slice_start..entries.len(),
    });
    Ok(slices)
}

/// Why a leaf does not answer a lookup. Every variant leaves the part
/// uncovered for the field: the reader scans it.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KeyIndexLeafRejection {
    #[error("corrupted key-index leaf: {0}")]
    Corrupted(String),
    #[error("unsupported key-index leaf format_version {0}")]
    UnsupportedVersion(u32),
    /// The leaf indexes another part than the one HEAD resolved.
    #[error("leaf binds part {actual}, HEAD names part {expected}")]
    PartBindingMismatch { expected: String, actual: String },
    /// The leaf is valid but indexes another field or key type, or another
    /// key slice, than the lookup asked about.
    #[error("leaf does not answer this lookup: {0}")]
    RequestMismatch(String),
    #[error("key-index leaf read failed: {0}")]
    Read(String),
}

fn corrupted(msg: impl std::fmt::Display) -> KeyIndexLeafRejection {
    KeyIndexLeafRejection::Corrupted(msg.to_string())
}

/// A leaf whose header `tenant_hash` or `signal` differs from the request:
/// a misplaced object, never an uncovered part.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("key-index leaf header {field} is {actual}, request expects {expected}")]
pub struct KeyIndexLeafBreach {
    pub field: &'static str,
    pub expected: String,
    pub actual: String,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KeyIndexLeafError {
    #[error(transparent)]
    Breach(KeyIndexLeafBreach),
    #[error(transparent)]
    Rejected(KeyIndexLeafRejection),
}

impl From<KeyIndexLeafRejection> for KeyIndexLeafError {
    fn from(rejection: KeyIndexLeafRejection) -> Self {
        KeyIndexLeafError::Rejected(rejection)
    }
}

/// What a lookup expects of the leaf, from the tenant, the signal prefix it
/// reads under, and the part ref in the HEAD it resolved.
#[derive(Debug, Clone, Copy)]
pub struct KeyIndexLeafRequest<'a> {
    pub tenant_hash: TenantHash,
    pub signal: Signal,
    /// The covered part's `blake3` in the resolved HEAD.
    pub part_blake3: &'a [u8],
    /// The covered part's `entry_count` in the resolved HEAD.
    pub part_entry_count: u64,
    pub field: &'a str,
    pub key_type: KeyType,
}

/// GET 1's range, `[0, prefix_len)` from the ref. A `prefix_len` of zero or
/// greater than the ref's `size` is `Corrupted` before anything is read.
pub fn key_index_leaf_prefix_range(
    leaf_ref: &SnapshotKeyIndexLeafRef,
) -> Result<Range<u64>, KeyIndexLeafRejection> {
    let prefix_len = u64::from(leaf_ref.prefix_len);
    if prefix_len == 0 || prefix_len > leaf_ref.size {
        return Err(corrupted(format_args!(
            "ref prefix_len {prefix_len} is zero or greater than the ref size {}",
            leaf_ref.size
        )));
    }
    Ok(0..prefix_len)
}

fn le_u32(bytes: &[u8], at: usize) -> Result<u32, KeyIndexLeafRejection> {
    bytes
        .get(at..at + 4)
        .and_then(|s| <[u8; 4]>::try_from(s).ok())
        .map(u32::from_le_bytes)
        .ok_or_else(|| corrupted(format_args!("truncated before byte {}", at + 4)))
}

/// The CRC-verified header and the length fields around it, before any
/// semantic check.
struct Envelope {
    header: KeyIndexLeafHeader,
    header_len: u64,
    prefix_len: u64,
}

fn decode_envelope(bytes: &[u8]) -> Result<Envelope, KeyIndexLeafRejection> {
    if bytes.get(..4) != Some(&KEY_INDEX_LEAF_MAGIC[..]) {
        return Err(corrupted("magic is not RKI1"));
    }
    let header_len = le_u32(bytes, 4)? as usize;
    let prefix_len = le_u32(bytes, 8)?;
    let header_end = 12usize
        .checked_add(header_len)
        .ok_or_else(|| corrupted("header_len overflows"))?;
    let (Some(covered), Ok(stored_crc)) = (bytes.get(..header_end), le_u32(bytes, header_end))
    else {
        return Err(corrupted(format_args!(
            "header_len {header_len} runs past the {} bytes read",
            bytes.len()
        )));
    };
    if crc32c::crc32c(covered) != stored_crc {
        return Err(corrupted("header crc32c mismatch"));
    }
    let header = KeyIndexLeafHeader::decode(&bytes[12..header_end])
        .map_err(|e| corrupted(format_args!("header protobuf: {e}")))?;
    Ok(Envelope {
        header,
        header_len: header_len as u64,
        prefix_len: u64::from(prefix_len),
    })
}

/// A leaf's verified header and bucket directory: GET 1's result.
#[derive(Debug, Clone)]
pub struct KeyIndexLeafPrefix {
    header: KeyIndexLeafHeader,
    key_type: KeyType,
    bucket_bits: u8,
    prefix_len: u64,
    ends: Vec<u32>,
}

/// The one bucket a `key8` selects, and the absolute byte range of its
/// frame: GET 2's range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyIndexBucketRange {
    pub bucket: u32,
    pub range: Range<u64>,
}

/// One part entry holding the probed key, and the blocks within it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyIndexMatch {
    pub entry_ordinal: u64,
    pub blocks: Vec<u32>,
}

fn validate_prefix(
    envelope: Envelope,
    bytes: &[u8],
) -> Result<KeyIndexLeafPrefix, KeyIndexLeafRejection> {
    let Envelope {
        header,
        header_len,
        prefix_len,
    } = envelope;
    if header.format_version != KEY_INDEX_LEAF_FORMAT_VERSION {
        return Err(KeyIndexLeafRejection::UnsupportedVersion(
            header.format_version,
        ));
    }
    let bucket_bits = u8::try_from(header.bucket_bits)
        .ok()
        .filter(|b| {
            (KEY_INDEX_LEAF_MIN_BUCKET_BITS..=KEY_INDEX_LEAF_READER_MAX_BUCKET_BITS).contains(b)
        })
        .ok_or_else(|| {
            corrupted(format_args!(
                "bucket_bits {} outside {KEY_INDEX_LEAF_MIN_BUCKET_BITS}..={KEY_INDEX_LEAF_READER_MAX_BUCKET_BITS}",
                header.bucket_bits
            ))
        })?;
    let bucket_count = 1u64 << bucket_bits;
    let expected_prefix = FIXED_PREFIX_LEN + header_len + 4 * bucket_count;
    if prefix_len != expected_prefix {
        return Err(corrupted(format_args!(
            "header prefix_len {prefix_len} differs from the layout's {expected_prefix}"
        )));
    }
    let Some(prefix) = bytes.get(..prefix_len as usize) else {
        return Err(corrupted(format_args!(
            "read {} bytes, shorter than the header prefix_len {prefix_len}",
            bytes.len()
        )));
    };
    let dir_start = (12 + header_len + 4) as usize;
    let dir_end = prefix.len() - 4;
    let dir = &prefix[dir_start..dir_end];
    if crc32c::crc32c(dir) != le_u32(prefix, dir_end)? {
        return Err(corrupted("directory crc32c mismatch"));
    }
    let ends: Vec<u32> = dir
        .chunks_exact(4)
        .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect();
    if ends.windows(2).any(|w| w[0] > w[1]) {
        return Err(corrupted("directory ends decrease"));
    }
    if ends.last().map(|&e| u64::from(e)) != Some(header.body_len) {
        return Err(corrupted(format_args!(
            "last directory end differs from body_len {}",
            header.body_len
        )));
    }
    let key_type = u8::try_from(header.key_type)
        .ok()
        .and_then(|b| KeyType::from_u8(b).ok())
        .ok_or_else(|| corrupted(format_args!("unknown key_type {}", header.key_type)))?;
    if header.tenant_hash.len() != 16 || header.part_blake3.len() != 32 {
        return Err(corrupted("tenant_hash or part_blake3 has the wrong length"));
    }
    if header.field.is_empty() || header.field.len() > KEY_IDX_MAX_NAME_LEN {
        return Err(corrupted("field name empty or too long"));
    }
    if header.key8_lo > header.key8_hi {
        return Err(corrupted("key8_lo above key8_hi"));
    }
    let uncovered = &header.uncovered_entry_ordinals;
    if uncovered.windows(2).any(|w| w[0] >= w[1]) {
        return Err(corrupted("uncovered_entry_ordinals not strictly ascending"));
    }
    if uncovered
        .last()
        .is_some_and(|&last| last >= header.part_entry_count)
    {
        return Err(corrupted(
            "uncovered entry ordinal at or past part_entry_count",
        ));
    }
    Ok(KeyIndexLeafPrefix {
        header,
        key_type,
        bucket_bits,
        prefix_len,
        ends,
    })
}

fn check_ref_copy<T: PartialEq + std::fmt::Debug>(
    name: &str,
    in_ref: T,
    in_header: T,
) -> Result<(), KeyIndexLeafRejection> {
    if in_ref == in_header {
        Ok(())
    } else {
        Err(corrupted(format_args!(
            "ref {name} {in_ref:?} disagrees with the header's {in_header:?}"
        )))
    }
}

impl KeyIndexLeafPrefix {
    /// Verifies GET 1's bytes against the ref and the request, in the order
    /// the reader procedure gives: magic and header crc; then the tenant
    /// and signal binding ([`KeyIndexLeafError::Breach`]); then version,
    /// `bucket_bits`, the `prefix_len` arithmetic, the directory crc and
    /// ends; then every ref copy, the part binding, and the request's
    /// field, key type and `part_entry_count`.
    pub fn parse(
        bytes: &[u8],
        leaf_ref: &SnapshotKeyIndexLeafRef,
        request: &KeyIndexLeafRequest<'_>,
    ) -> Result<Self, KeyIndexLeafError> {
        let envelope = decode_envelope(bytes)?;
        let header = &envelope.header;
        if header.tenant_hash.as_slice() != request.tenant_hash.0.as_slice() {
            return Err(KeyIndexLeafError::Breach(KeyIndexLeafBreach {
                field: "tenant_hash",
                expected: request.tenant_hash.to_hex(),
                actual: hex::encode(&header.tenant_hash),
            }));
        }
        let signal = signal_code(request.signal);
        if header.signal != signal {
            return Err(KeyIndexLeafError::Breach(KeyIndexLeafBreach {
                field: "signal",
                expected: signal.to_string(),
                actual: header.signal.to_string(),
            }));
        }
        let prefix = validate_prefix(envelope, bytes)?;
        let header = &prefix.header;
        check_ref_copy(
            "prefix_len",
            u64::from(leaf_ref.prefix_len),
            prefix.prefix_len,
        )?;
        check_ref_copy(
            "size",
            leaf_ref.size,
            prefix.prefix_len.saturating_add(header.body_len),
        )?;
        check_ref_copy("field", &leaf_ref.field, &header.field)?;
        check_ref_copy("key_type", leaf_ref.key_type, header.key_type)?;
        check_ref_copy("key8_lo", leaf_ref.key8_lo, header.key8_lo)?;
        check_ref_copy("key8_hi", leaf_ref.key8_hi, header.key8_hi)?;
        check_ref_copy("bucket_bits", leaf_ref.bucket_bits, header.bucket_bits)?;
        check_ref_copy("entry_count", leaf_ref.entry_count, header.entry_count)?;
        check_ref_copy(
            "uncovered_count",
            leaf_ref.uncovered_count,
            header.uncovered_entry_ordinals.len() as u64,
        )?;
        check_ref_copy("part_blake3", &leaf_ref.part_blake3, &header.part_blake3)?;
        if header.part_blake3.as_slice() != request.part_blake3 {
            return Err(KeyIndexLeafRejection::PartBindingMismatch {
                expected: hex::encode(request.part_blake3),
                actual: hex::encode(&header.part_blake3),
            }
            .into());
        }
        if header.part_entry_count != request.part_entry_count {
            return Err(corrupted(format_args!(
                "header part_entry_count {} differs from the part ref's entry_count {}",
                header.part_entry_count, request.part_entry_count
            ))
            .into());
        }
        if header.field != request.field || prefix.key_type != request.key_type {
            return Err(KeyIndexLeafRejection::RequestMismatch(format!(
                "leaf indexes field {:?} as {:?}, lookup asks for {:?} as {:?}",
                header.field, prefix.key_type, request.field, request.key_type
            ))
            .into());
        }
        Ok(prefix)
    }

    pub fn header(&self) -> &KeyIndexLeafHeader {
        &self.header
    }

    pub fn key_type(&self) -> KeyType {
        self.key_type
    }

    /// The header's `bucket_bits`, the only value a bucket is selected by.
    pub fn bucket_bits(&self) -> u8 {
        self.bucket_bits
    }

    pub fn prefix_len(&self) -> u64 {
        self.prefix_len
    }

    pub fn uncovered_entry_ordinals(&self) -> &[u64] {
        &self.header.uncovered_entry_ordinals
    }

    fn bucket_body_range(&self, bucket: usize) -> Range<u64> {
        let start = if bucket == 0 {
            0
        } else {
            self.ends[bucket - 1]
        };
        u64::from(start)..u64::from(self.ends[bucket])
    }

    /// The bucket `key8` mixes to under the header's `bucket_bits`, and its
    /// frame's absolute range; `None` when the bucket is empty, so the key
    /// is absent without GET 2. A `key8` outside the leaf's slice is
    /// [`KeyIndexLeafRejection::RequestMismatch`].
    pub fn locate(
        &self,
        key8: &Key8,
    ) -> Result<Option<KeyIndexBucketRange>, KeyIndexLeafRejection> {
        let key = u64::from_be_bytes(*key8);
        if key < self.header.key8_lo || key > self.header.key8_hi {
            return Err(KeyIndexLeafRejection::RequestMismatch(format!(
                "key8 {key:#018x} outside the leaf's slice [{:#018x}, {:#018x}]",
                self.header.key8_lo, self.header.key8_hi
            )));
        }
        let bucket = bucket_index(key8, self.bucket_bits).map_err(corrupted)?;
        let body = self.bucket_body_range(bucket as usize);
        if body.is_empty() {
            return Ok(None);
        }
        Ok(Some(KeyIndexBucketRange {
            bucket,
            range: self.prefix_len + body.start..self.prefix_len + body.end,
        }))
    }

    /// Verifies GET 2's frame in full and returns the entries holding
    /// `key8`, ascending by entry ordinal.
    pub fn probe_bucket(
        &self,
        located: &KeyIndexBucketRange,
        frame: &[u8],
        key8: &Key8,
        limits: &KeyIndexLeafLimits,
    ) -> Result<Vec<KeyIndexMatch>, KeyIndexLeafRejection> {
        let expected = located.range.end - located.range.start;
        if frame.len() as u64 != expected {
            return Err(corrupted(format_args!(
                "bucket {} read returned {} bytes, the directory gives {expected}",
                located.bucket,
                frame.len()
            )));
        }
        let decoded = self.decode_frame(located.bucket, frame, limits)?;
        let start = decoded.entries.partition_point(|e| e.key8 < *key8);
        Ok(decoded.entries[start..]
            .iter()
            .take_while(|e| e.key8 == *key8)
            .map(|e| KeyIndexMatch {
                entry_ordinal: e.entry_ordinal,
                blocks: e.blocks.clone(),
            })
            .collect())
    }

    fn decode_frame(
        &self,
        bucket: u32,
        frame: &[u8],
        limits: &KeyIndexLeafLimits,
    ) -> Result<DecodedFrame, KeyIndexLeafRejection> {
        let entry_count = le_u32(frame, 0)?;
        let uncompressed_len = le_u32(frame, 4)?;
        let stored_crc = le_u32(frame, 8)?;
        if entry_count == 0 {
            return Err(corrupted(format_args!(
                "bucket {bucket} frame declares zero entries"
            )));
        }
        if u64::from(uncompressed_len) > limits.max_body_bytes {
            return Err(corrupted(format_args!(
                "bucket {bucket} declares {uncompressed_len} decompressed bytes, over the {}-byte ceiling",
                limits.max_body_bytes
            )));
        }
        let compressed = &frame[FRAME_HEADER_LEN..];
        if crc32c::crc32c(compressed) != stored_crc {
            return Err(corrupted(format_args!(
                "bucket {bucket} frame crc32c mismatch"
            )));
        }
        let raw = zstd::bulk::decompress(compressed, uncompressed_len as usize)
            .map_err(|e| corrupted(format_args!("bucket {bucket} zstd: {e}")))?;
        if raw.len() != uncompressed_len as usize {
            return Err(corrupted(format_args!(
                "bucket {bucket} decompressed to {} bytes, frame declares {uncompressed_len}",
                raw.len()
            )));
        }
        let mut entries = Vec::new();
        let mut pos = 0usize;
        let mut prev: Option<(Key8, u64)> = None;
        for _ in 0..entry_count {
            let key8: Key8 = raw
                .get(pos..pos + 8)
                .and_then(|s| s.try_into().ok())
                .ok_or_else(|| corrupted(format_args!("bucket {bucket} entry truncated")))?;
            pos += 8;
            let entry_ordinal = read_uvarint(&raw, &mut pos)?;
            let block_count = read_uvarint(&raw, &mut pos)?;
            if block_count == 0 {
                return Err(corrupted(format_args!(
                    "bucket {bucket} entry has an empty block set"
                )));
            }
            if block_count > (raw.len() - pos) as u64 {
                return Err(corrupted(format_args!(
                    "bucket {bucket} block_count {block_count} exceeds the bytes left"
                )));
            }
            let mut blocks = Vec::with_capacity(block_count as usize);
            let mut last: Option<u32> = None;
            for _ in 0..block_count {
                let delta = read_uvarint(&raw, &mut pos)?;
                let block = match (delta, last) {
                    (0, _) => None,
                    (d, None) => Some(d - 1),
                    (d, Some(prev_block)) => u64::from(prev_block).checked_add(d),
                }
                .and_then(|b| u32::try_from(b).ok())
                .ok_or_else(|| {
                    corrupted(format_args!(
                        "bucket {bucket} block delta {delta} is zero or overflows u32"
                    ))
                })?;
                blocks.push(block);
                last = Some(block);
            }
            let key = u64::from_be_bytes(key8);
            if key < self.header.key8_lo || key > self.header.key8_hi {
                return Err(corrupted(format_args!(
                    "bucket {bucket} key8 {key:#018x} outside the leaf's slice"
                )));
            }
            if bucket_index(&key8, self.bucket_bits).map_err(corrupted)? != bucket {
                return Err(corrupted(format_args!(
                    "key8 {key:#018x} does not mix to bucket {bucket}"
                )));
            }
            if entry_ordinal >= self.header.part_entry_count {
                return Err(corrupted(format_args!(
                    "entry ordinal {entry_ordinal} at or past part_entry_count {}",
                    self.header.part_entry_count
                )));
            }
            let pair = (key8, entry_ordinal);
            if prev.is_some_and(|p| p >= pair) {
                return Err(corrupted(format_args!(
                    "bucket {bucket} entries unsorted or duplicated"
                )));
            }
            prev = Some(pair);
            entries.push(KeyIndexLeafEntry {
                key8,
                entry_ordinal,
                blocks,
            });
        }
        if pos != raw.len() {
            return Err(corrupted(format_args!(
                "bucket {bucket} holds {} bytes past its {entry_count} entries",
                raw.len() - pos
            )));
        }
        Ok(DecodedFrame {
            entries,
            uncompressed_len,
        })
    }
}

struct DecodedFrame {
    entries: Vec<KeyIndexLeafEntry>,
    uncompressed_len: u32,
}

/// The answer one leaf gives for one key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyIndexLeafProbe {
    /// The key is in these covered entries (ascending by ordinal) and in no
    /// other covered entry. The uncovered entries are still candidates.
    Matches {
        matches: Vec<KeyIndexMatch>,
        uncovered_entry_ordinals: Vec<u64>,
    },
    /// The key is in no covered entry. The uncovered entries are still
    /// candidates.
    Absent { uncovered_entry_ordinals: Vec<u64> },
    /// The leaf does not answer: every entry of the part is a candidate.
    Rejected(KeyIndexLeafRejection),
}

impl KeyIndexLeafProbe {
    /// The entries the probe did not cover; `None` for a rejected leaf, which
    /// covers no entry of the part.
    pub fn uncovered_entry_ordinals(&self) -> Option<&[u64]> {
        match self {
            KeyIndexLeafProbe::Matches {
                uncovered_entry_ordinals,
                ..
            }
            | KeyIndexLeafProbe::Absent {
                uncovered_entry_ordinals,
            } => Some(uncovered_entry_ordinals),
            KeyIndexLeafProbe::Rejected(_) => None,
        }
    }
}

/// The two-GET probe of one leaf for one `key8`, over a caller-supplied
/// ranged byte source (`read(range)` returns those bytes of the leaf).
/// A ref `prefix_len` of zero or past `size` rejects before any read; GET 1
/// is `[0, prefix_len)`; GET 2 is the one bucket the header's
/// `bucket_bits` selects, skipped when that bucket is empty. A tenant or
/// signal mismatch is the only `Err`; every other failure is
/// [`KeyIndexLeafProbe::Rejected`].
pub async fn probe_key_index_leaf_ranged<F, Fut>(
    leaf_ref: &SnapshotKeyIndexLeafRef,
    request: &KeyIndexLeafRequest<'_>,
    key8: &Key8,
    limits: &KeyIndexLeafLimits,
    mut read: F,
) -> Result<KeyIndexLeafProbe, KeyIndexLeafBreach>
where
    F: FnMut(Range<u64>) -> Fut,
    Fut: Future<Output = Result<Bytes, String>>,
{
    let get1 = match key_index_leaf_prefix_range(leaf_ref) {
        Ok(range) => range,
        Err(rejection) => return Ok(KeyIndexLeafProbe::Rejected(rejection)),
    };
    let prefix_bytes = match read(get1).await {
        Ok(bytes) => bytes,
        Err(e) => return Ok(KeyIndexLeafProbe::Rejected(KeyIndexLeafRejection::Read(e))),
    };
    let prefix = match KeyIndexLeafPrefix::parse(&prefix_bytes, leaf_ref, request) {
        Ok(prefix) => prefix,
        Err(KeyIndexLeafError::Breach(breach)) => return Err(breach),
        Err(KeyIndexLeafError::Rejected(rejection)) => {
            return Ok(KeyIndexLeafProbe::Rejected(rejection));
        }
    };
    let uncovered_entry_ordinals = prefix.uncovered_entry_ordinals().to_vec();
    let located = match prefix.locate(key8) {
        Ok(Some(located)) => located,
        Ok(None) => {
            return Ok(KeyIndexLeafProbe::Absent {
                uncovered_entry_ordinals,
            });
        }
        Err(rejection) => return Ok(KeyIndexLeafProbe::Rejected(rejection)),
    };
    let frame = match read(located.range.clone()).await {
        Ok(bytes) => bytes,
        Err(e) => return Ok(KeyIndexLeafProbe::Rejected(KeyIndexLeafRejection::Read(e))),
    };
    Ok(match prefix.probe_bucket(&located, &frame, key8, limits) {
        Ok(matches) if matches.is_empty() => KeyIndexLeafProbe::Absent {
            uncovered_entry_ordinals,
        },
        Ok(matches) => KeyIndexLeafProbe::Matches {
            matches,
            uncovered_entry_ordinals,
        },
        Err(rejection) => KeyIndexLeafProbe::Rejected(rejection),
    })
}

impl Catalog {
    /// [`probe_key_index_leaf_ranged`] over this catalog's store, reading
    /// `leaf_ref.key`. A tenant or signal mismatch counts in
    /// [`Catalog::isolation_breaches`] and is
    /// [`CatalogError::FieldMismatch`], with no fallback.
    pub async fn probe_key_index_leaf(
        &self,
        leaf_ref: &SnapshotKeyIndexLeafRef,
        request: &KeyIndexLeafRequest<'_>,
        key8: &Key8,
        limits: &KeyIndexLeafLimits,
    ) -> Result<KeyIndexLeafProbe, CatalogError> {
        let store = self.store();
        let key = leaf_ref.key.as_str();
        let probe =
            probe_key_index_leaf_ranged(leaf_ref, request, key8, limits, |range| async move {
                store
                    .get(key, GetRange::Range(range.start, range.end))
                    .await
                    .map(|got| got.data)
                    .map_err(|e| e.to_string())
            })
            .await;
        probe.map_err(|breach| {
            self.record_isolation_breach();
            CatalogError::FieldMismatch {
                key: leaf_ref.key.clone(),
                field: breach.field,
                expected: breach.expected,
                actual: breach.actual,
            }
        })
    }
}

fn check_slice_tiling(
    mut slices: Vec<(u64, u64)>,
    field: &str,
) -> Result<(), KeyIndexLeafRejection> {
    slices.sort_unstable();
    let mut next_lo = Some(0u64);
    for (lo, hi) in slices {
        if Some(lo) != next_lo || lo > hi {
            return Err(corrupted(format_args!(
                "field {field:?} slices do not tile the key8 space at [{lo:#018x}, {hi:#018x}]"
            )));
        }
        next_lo = hi.checked_add(1);
    }
    if next_lo.is_some() {
        return Err(corrupted(format_args!(
            "field {field:?} slices stop short of u64::MAX"
        )));
    }
    Ok(())
}

/// Checks one part's refs for `field` (refs for other fields are ignored):
/// their slices must tile `[0, u64::MAX]` with no gap or overlap, and agree
/// on `key_type`, `uncovered_count` and `part_blake3`. Returns the ref whose
/// slice holds `key8`, or `None` when the part has no leaf for the field
/// (uncovered, not an error).
pub fn select_key_index_slice<'a>(
    refs: &'a [SnapshotKeyIndexLeafRef],
    field: &str,
    key8: &Key8,
) -> Result<Option<&'a SnapshotKeyIndexLeafRef>, KeyIndexLeafRejection> {
    let for_field: Vec<&SnapshotKeyIndexLeafRef> =
        refs.iter().filter(|r| r.field == field).collect();
    let Some(first) = for_field.first() else {
        return Ok(None);
    };
    for r in &for_field {
        if r.key_type != first.key_type
            || r.uncovered_count != first.uncovered_count
            || r.part_blake3 != first.part_blake3
        {
            return Err(corrupted(format_args!(
                "field {field:?} slices disagree on key_type, uncovered_count or part_blake3"
            )));
        }
    }
    check_slice_tiling(
        for_field.iter().map(|r| (r.key8_lo, r.key8_hi)).collect(),
        field,
    )?;
    let key = u64::from_be_bytes(*key8);
    Ok(for_field
        .into_iter()
        .find(|r| r.key8_lo <= key && key <= r.key8_hi))
}

/// A whole leaf, every frame verified: what `inspect kidx` reports.
#[derive(Debug, Clone, PartialEq)]
pub struct KeyIndexLeafSummary {
    pub header: KeyIndexLeafHeader,
    pub key_type: KeyType,
    pub prefix_len: u32,
    pub size: u64,
    pub blake3: [u8; 32],
    /// Entries per bucket, empty buckets included.
    pub bucket_entry_counts: Vec<u32>,
    /// Sum of the frames' declared `uncompressed_len`.
    pub body_uncompressed_len: u64,
    /// Every entry, in bucket order and sorted within each bucket.
    pub entries: Vec<KeyIndexLeafEntry>,
}

impl KeyIndexLeafSummary {
    /// The ref this leaf's own header implies, with an empty `key`.
    pub fn leaf_ref(&self) -> SnapshotKeyIndexLeafRef {
        SnapshotKeyIndexLeafRef {
            key: String::new(),
            blake3: self.blake3.to_vec(),
            size: self.size,
            field: self.header.field.clone(),
            key_type: self.header.key_type,
            key8_lo: self.header.key8_lo,
            key8_hi: self.header.key8_hi,
            prefix_len: self.prefix_len,
            bucket_bits: self.header.bucket_bits,
            entry_count: self.header.entry_count,
            uncovered_count: self.header.uncovered_entry_ordinals.len() as u64,
            part_blake3: self.header.part_blake3.clone(),
        }
    }
}

/// Verifies a whole leaf without a request: every check the probe makes on
/// its prefix and on a frame, applied to every frame, plus the checks only a
/// whole-object reader can make: the object is exactly `prefix_len +
/// body_len` bytes, the frames' entry counts sum to the header's
/// `entry_count`, and their `uncompressed_len`s sum to at most the ceiling.
pub fn verify_key_index_leaf(
    bytes: &[u8],
    limits: &KeyIndexLeafLimits,
) -> Result<KeyIndexLeafSummary, KeyIndexLeafRejection> {
    let prefix = validate_prefix(decode_envelope(bytes)?, bytes)?;
    let expected_size = prefix.prefix_len.saturating_add(prefix.header.body_len);
    if bytes.len() as u64 != expected_size {
        return Err(corrupted(format_args!(
            "object is {} bytes, prefix_len + body_len is {expected_size}",
            bytes.len()
        )));
    }
    let mut bucket_entry_counts = Vec::with_capacity(prefix.ends.len());
    let mut entries = Vec::new();
    let mut body_uncompressed_len = 0u64;
    for bucket in 0..prefix.ends.len() {
        let body = prefix.bucket_body_range(bucket);
        if body.is_empty() {
            bucket_entry_counts.push(0);
            continue;
        }
        let frame = &bytes
            [(prefix.prefix_len + body.start) as usize..(prefix.prefix_len + body.end) as usize];
        let decoded = prefix.decode_frame(bucket as u32, frame, limits)?;
        bucket_entry_counts.push(decoded.entries.len() as u32);
        body_uncompressed_len += u64::from(decoded.uncompressed_len);
        entries.extend(decoded.entries);
    }
    if entries.len() as u64 != prefix.header.entry_count {
        return Err(corrupted(format_args!(
            "frames hold {} entries, header entry_count is {}",
            entries.len(),
            prefix.header.entry_count
        )));
    }
    if body_uncompressed_len > limits.max_body_bytes {
        return Err(corrupted(format_args!(
            "frames decompress to {body_uncompressed_len} bytes, over the {}-byte ceiling",
            limits.max_body_bytes
        )));
    }
    Ok(KeyIndexLeafSummary {
        key_type: prefix.key_type,
        prefix_len: prefix.prefix_len as u32,
        size: bytes.len() as u64,
        blake3: *blake3::hash(bytes).as_bytes(),
        bucket_entry_counts,
        body_uncompressed_len,
        entries,
        header: prefix.header,
    })
}

/// Checks verified leaves that claim one (part, field) against each other:
/// the same tenant, signal, part, field, key type and `part_entry_count`,
/// the identical `uncovered_entry_ordinals` list, and slices that tile
/// `[0, u64::MAX]`.
pub fn check_key_index_leaf_set(
    leaves: &[KeyIndexLeafSummary],
) -> Result<(), KeyIndexLeafRejection> {
    let Some(first) = leaves.first() else {
        return Ok(());
    };
    let identity = |h: &KeyIndexLeafHeader| {
        (
            h.tenant_hash.clone(),
            h.signal,
            h.part_blake3.clone(),
            h.field.clone(),
            h.key_type,
            h.part_entry_count,
        )
    };
    for leaf in leaves {
        if identity(&leaf.header) != identity(&first.header) {
            return Err(corrupted(
                "leaves name different tenants, signals, parts, fields, key types or part_entry_counts",
            ));
        }
        if leaf.header.uncovered_entry_ordinals != first.header.uncovered_entry_ordinals {
            return Err(corrupted(format_args!(
                "field {:?} slices carry different uncovered_entry_ordinals",
                first.header.field
            )));
        }
    }
    check_slice_tiling(
        leaves
            .iter()
            .map(|l| (l.header.key8_lo, l.header.key8_hi))
            .collect(),
        &first.header.field,
    )
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests;
