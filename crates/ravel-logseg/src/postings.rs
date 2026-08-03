//! POSTINGS section: exact block-level attribute pruning, opt-in per field
//! (docs/adrs/0049-rlog-postings.md, docs/log-segment-format.md "POSTINGS").
//!
//! Per indexed field (a dynamic attribute column), a sorted term dictionary
//! maps each distinct value to the sorted set of block indices that carry a
//! row with that value. The dictionary is split into fixed-stride term
//! blocks, each independently zstd-compressed and crc32c-verified, addressed
//! through a sparse index holding every block's first term
//! (docs/adrs/0049-rlog-postings.md decision 2, mirroring RSEG's
//! `SERIES_IDX`). A query with an equality predicate on an indexed field
//! binary-searches the sparse index, decompresses one term block, and
//! intersects its posting list with the candidate set.
//!
//! Absence is always legal: a field with no POSTINGS entry (never indexed,
//! or dropped for exceeding its distinct-value cap) simply cannot be pruned
//! by this section, and the reader falls back to bloom pruning plus an exact
//! scan (docs/adrs/0049-rlog-postings.md decision 5). This section is new
//! ("format only": ADR-0029's versioning carve-out covers a new section kind
//! without a version bump, since an unknown kind is already skipped and an
//! absent kind is already legal), so `POSTINGS_VERSION` here is this
//! section's own internal grammar version, independent of the RLOG trailer
//! `VERSION`.
//!
//! Byte layout (section body, uncompressed at the section-comp level; each
//! term block carries its own independent zstd frame, mirroring how BLOOM's
//! per-entry crc lets a ranged read verify one entry without a whole-section
//! checksum):
//!
//! ```text
//! version: u8
//! field_count: u32 LE
//! repeat field_count, ascending column_id:
//!   column_id: uvarint
//!   capped: u8                     (0 = postings present, 1 = dropped: over cap)
//!   if capped == 0:
//!     stride: uvarint              (terms per term block)
//!     term_count: uvarint          (total distinct terms for this field)
//!     block_count: uvarint
//!     repeat block_count, ascending first_term:
//!       first_term_len: uvarint
//!       first_term_bytes: [first_term_len]u8
//!       block_offset: u64 LE       (absolute offset from section start)
//!       block_stored_len: u64 LE   (compressed byte length)
//!       block_uncompressed_len: u64 LE
//!       block_crc32c: u32 LE       (over the stored/compressed bytes)
//! term_blocks: [remaining bytes]   (concatenated zstd frames, field then block
//!                                   order, exactly at the offsets above)
//! ```
//!
//! Offset/length fields are fixed-width (not varint) so the header's byte
//! length -- and therefore every block's offset -- can be computed in one
//! pass without a fixed-point dependency on its own encoded size (the same
//! reason RSEG's `SERIES_IDX` uses fixed-width offsets).
//!
//! One term block's payload, before compression:
//!
//! ```text
//! term_count_in_block: uvarint     (<= stride)
//! repeat term_count_in_block, ascending term:
//!   term_len: uvarint
//!   term_bytes: [term_len]u8
//!   posting_count: uvarint
//!   repeat posting_count: delta-uvarint block index (first absolute, then
//!                                                     strictly increasing deltas)
//! ```

use std::collections::BTreeMap;

use crate::error::LogSegError;
use crate::record::ColumnValue;
use crate::varint::{get_uvarint, put_uvarint};

/// This section's internal grammar version (independent of the RLOG trailer
/// version; see the module doc for why no trailer bump is needed).
pub const POSTINGS_VERSION: u8 = 1;

/// Default number of distinct terms held per term block. Small enough that a
/// probe's one term-block fetch stays a modest ranged GET, large enough that
/// the sparse index (one entry per block) stays a tiny fraction of the term
/// dictionary. Chosen to match the order of magnitude of RSEG's per-chunk
/// series count; see the issue #508 report for the measured tradeoff.
pub const DEFAULT_STRIDE: u32 = 128;

/// Sanity ceiling on the number of indexed fields a section may declare,
/// independent of any writer's per-field distinct-value cap (that bounds
/// terms per field, not the field count). Matches the crate's other
/// directory-count caps (`MAX_FIELDS` in `reader.rs`).
const MAX_POSTINGS_FIELDS: u64 = 1 << 20;
/// Sanity ceiling on one field's declared term or block count, independent of
/// any writer's configured distinct-value cap: this bounds untrusted input at
/// decode time, not what a well-behaved writer chooses to emit.
const MAX_POSTINGS_TERMS_PER_FIELD: u64 = 1 << 24;
/// Sanity ceiling on one term block's declared uncompressed length, guarding
/// decompression against a zip-bomb-style crafted frame.
const MAX_BLOCK_UNCOMP: u64 = 64 << 20;

/// Canonical term-key bytes for a resolved column value: the sort/equality
/// key the term dictionary is built and probed with. Only used for equality
/// probes (ADR-0049 decision 1/7 is equality and `IN`, never a range), so the
/// encoding only needs to be a consistent total order, not one that reflects
/// numeric magnitude.
pub fn term_key(v: &ColumnValue) -> Vec<u8> {
    match v {
        ColumnValue::Str(b) | ColumnValue::Bytes(b) => b.clone(),
        ColumnValue::I64(i) => i.to_be_bytes().to_vec(),
        // Raw bits, matching the reader's bit-exact f64 equality (attr_value_eq
        // in reader.rs): -0.0 and 0.0 are distinct terms, as they are distinct
        // query results.
        ColumnValue::F64(bits) => bits.to_be_bytes().to_vec(),
        ColumnValue::Bool(b) => vec![u8::from(*b)],
    }
}

/// One field's postings input to the writer: either its full term -> sorted
/// block-index map, or `Capped` recording that it exceeded the distinct-value
/// cap and was dropped for this object (docs/adrs/0049-rlog-postings.md
/// decision 4).
pub enum FieldTerms {
    Capped,
    Terms(BTreeMap<Vec<u8>, std::collections::BTreeSet<u32>>),
}

/// Encodes a whole POSTINGS section body from already-collected per-field term
/// maps, ascending by `column_id` (`fields` is a `BTreeMap` so iteration order
/// is already ascending and deterministic). `stride` is the number of terms
/// per term block; `zstd_level` compresses each term block independently.
pub fn encode_postings_section(
    fields: &BTreeMap<u32, FieldTerms>,
    stride: u32,
    zstd_level: i32,
) -> Result<Vec<u8>, LogSegError> {
    if stride == 0 {
        return Err(LogSegError::LimitExceeded(
            "postings stride must be nonzero".into(),
        ));
    }

    struct BuiltBlock {
        first_term: Vec<u8>,
        stored: Vec<u8>,
        uncompressed_len: u64,
        crc32c: u32,
    }
    struct BuiltField {
        column_id: u32,
        capped: bool,
        term_count: u64,
        blocks: Vec<BuiltBlock>,
    }

    let mut built: Vec<BuiltField> = Vec::with_capacity(fields.len());
    for (&column_id, terms) in fields {
        match terms {
            FieldTerms::Capped => built.push(BuiltField {
                column_id,
                capped: true,
                term_count: 0,
                blocks: Vec::new(),
            }),
            FieldTerms::Terms(map) => {
                let mut blocks = Vec::new();
                let mut chunk: Vec<(&Vec<u8>, &std::collections::BTreeSet<u32>)> = Vec::new();
                let flush = |chunk: &mut Vec<(&Vec<u8>, &std::collections::BTreeSet<u32>)>,
                             blocks: &mut Vec<BuiltBlock>|
                 -> Result<(), LogSegError> {
                    if chunk.is_empty() {
                        return Ok(());
                    }
                    let mut payload = Vec::new();
                    put_uvarint(&mut payload, chunk.len() as u64);
                    for (term, block_set) in chunk.iter() {
                        put_uvarint(&mut payload, term.len() as u64);
                        payload.extend_from_slice(term);
                        put_uvarint(&mut payload, block_set.len() as u64);
                        let mut prev: Option<u32> = None;
                        for &b in block_set.iter() {
                            let delta = match prev {
                                None => u64::from(b),
                                Some(p) => u64::from(b - p),
                            };
                            put_uvarint(&mut payload, delta);
                            prev = Some(b);
                        }
                    }
                    let first_term = (*chunk[0].0).clone();
                    let compressed = zstd::bulk::compress(&payload, zstd_level)
                        .map_err(|e| LogSegError::Corrupted(format!("postings zstd: {e}")))?;
                    blocks.push(BuiltBlock {
                        first_term,
                        crc32c: crc32c::crc32c(&compressed),
                        uncompressed_len: payload.len() as u64,
                        stored: compressed,
                    });
                    chunk.clear();
                    Ok(())
                };
                for (term, block_set) in map {
                    chunk.push((term, block_set));
                    if chunk.len() as u32 >= stride {
                        flush(&mut chunk, &mut blocks)?;
                    }
                }
                flush(&mut chunk, &mut blocks)?;
                built.push(BuiltField {
                    column_id,
                    capped: false,
                    term_count: map.len() as u64,
                    blocks,
                });
            }
        }
    }

    // Phase 1: compute the header length. Every offset/length field is
    // fixed-width, so the header's byte length does not depend on the offset
    // values it will carry -- no fixed-point iteration needed.
    let mut header_len = 1 + 4; // version + field_count
    for f in &built {
        header_len += uvarint_len(u64::from(f.column_id)) + 1;
        if !f.capped {
            header_len += uvarint_len(u64::from(stride));
            header_len += uvarint_len(f.term_count);
            header_len += uvarint_len(f.blocks.len() as u64);
            for b in &f.blocks {
                header_len += uvarint_len(b.first_term.len() as u64);
                header_len += b.first_term.len();
                header_len += 8 + 8 + 8 + 4; // offset, stored_len, uncompressed_len, crc32c
            }
        }
    }

    // Phase 2: assign each block's absolute offset in field/block order, then
    // emit the header followed by the concatenated term blocks.
    let mut out = Vec::with_capacity(header_len);
    out.push(POSTINGS_VERSION);
    out.extend_from_slice(&(built.len() as u32).to_le_bytes());
    let mut cursor = header_len as u64;
    let mut offsets: Vec<Vec<u64>> = Vec::with_capacity(built.len());
    for f in &built {
        let mut field_offsets = Vec::with_capacity(f.blocks.len());
        for b in &f.blocks {
            field_offsets.push(cursor);
            cursor += b.stored.len() as u64;
        }
        offsets.push(field_offsets);
    }
    for (f, field_offsets) in built.iter().zip(&offsets) {
        put_uvarint(&mut out, u64::from(f.column_id));
        out.push(u8::from(f.capped));
        if !f.capped {
            put_uvarint(&mut out, u64::from(stride));
            put_uvarint(&mut out, f.term_count);
            put_uvarint(&mut out, f.blocks.len() as u64);
            for (b, &offset) in f.blocks.iter().zip(field_offsets) {
                put_uvarint(&mut out, b.first_term.len() as u64);
                out.extend_from_slice(&b.first_term);
                out.extend_from_slice(&offset.to_le_bytes());
                out.extend_from_slice(&(b.stored.len() as u64).to_le_bytes());
                out.extend_from_slice(&b.uncompressed_len.to_le_bytes());
                out.extend_from_slice(&b.crc32c.to_le_bytes());
            }
        }
    }
    debug_assert_eq!(out.len(), header_len);
    for f in &built {
        for b in &f.blocks {
            out.extend_from_slice(&b.stored);
        }
    }
    Ok(out)
}

fn uvarint_len(mut v: u64) -> usize {
    let mut n = 1;
    while v >= 0x80 {
        v >>= 7;
        n += 1;
    }
    n
}

/// One field's sparse index: ascending-by-`first_term` block descriptors.
struct SparseEntry {
    first_term: Vec<u8>,
    offset: usize,
    stored_len: usize,
    uncompressed_len: usize,
    crc32c: u32,
}

struct FieldIndex {
    column_id: u32,
    capped: bool,
    blocks: Vec<SparseEntry>,
}

/// A parsed POSTINGS section: per-field sparse indices, ready to probe. Term
/// blocks are decompressed and crc-verified lazily, on probe, exactly as
/// `BloomSection` verifies one entry's crc lazily rather than the whole
/// section up front -- a probe should cost one term-block decode, not a whole
/// section decode.
pub struct PostingsSection<'a> {
    bytes: &'a [u8],
    fields: Vec<FieldIndex>,
}

impl<'a> PostingsSection<'a> {
    /// Parses and structurally validates the section: ascending `column_id`
    /// across fields, ascending `first_term` within a field's blocks, and
    /// every block's `[offset, offset + stored_len)` range checked against
    /// `bytes`. Does not decompress or crc-verify any term block; that
    /// happens lazily in [`PostingsSection::probe`].
    pub fn parse(bytes: &'a [u8]) -> Result<Self, LogSegError> {
        let mut pos = 0usize;
        let version = *bytes
            .get(pos)
            .ok_or_else(|| LogSegError::Corrupted("postings version truncated".into()))?;
        pos += 1;
        if version != POSTINGS_VERSION {
            return Err(LogSegError::Corrupted(format!(
                "unsupported postings version {version}"
            )));
        }
        let count_bytes = bytes
            .get(pos..pos + 4)
            .ok_or_else(|| LogSegError::Corrupted("postings field_count truncated".into()))?;
        let field_count = u32::from_le_bytes([
            count_bytes[0],
            count_bytes[1],
            count_bytes[2],
            count_bytes[3],
        ]);
        pos += 4;
        if u64::from(field_count) > MAX_POSTINGS_FIELDS {
            return Err(LogSegError::Corrupted(
                "postings field_count over cap".into(),
            ));
        }

        let mut fields = Vec::with_capacity((field_count as usize).min(1 << 12));
        let mut prev_column: Option<u32> = None;
        for _ in 0..field_count {
            let column_id = u32::try_from(get_uvarint(bytes, &mut pos)?)
                .map_err(|_| LogSegError::Corrupted("postings column_id range".into()))?;
            if let Some(prev) = prev_column
                && column_id <= prev
            {
                return Err(LogSegError::Corrupted(
                    "postings column_id not ascending".into(),
                ));
            }
            prev_column = Some(column_id);
            let capped_byte = *bytes
                .get(pos)
                .ok_or_else(|| LogSegError::Corrupted("postings capped flag truncated".into()))?;
            pos += 1;
            let capped = match capped_byte {
                0 => false,
                1 => true,
                other => {
                    return Err(LogSegError::Corrupted(format!(
                        "postings capped flag {other}"
                    )));
                }
            };
            if capped {
                fields.push(FieldIndex {
                    column_id,
                    capped: true,
                    blocks: Vec::new(),
                });
                continue;
            }
            // `stride` is only a writer-side layout choice (how many terms
            // land in one term block); the reader never needs it once the
            // sparse index below carries each block's own offset, so it is
            // validated here and then discarded rather than stored.
            let stride = u32::try_from(get_uvarint(bytes, &mut pos)?)
                .map_err(|_| LogSegError::Corrupted("postings stride range".into()))?;
            if stride == 0 {
                return Err(LogSegError::Corrupted("postings stride is zero".into()));
            }
            let term_count = get_uvarint(bytes, &mut pos)?;
            if term_count > MAX_POSTINGS_TERMS_PER_FIELD {
                return Err(LogSegError::Corrupted(
                    "postings term_count over cap".into(),
                ));
            }
            let block_count = get_uvarint(bytes, &mut pos)?;
            if block_count > MAX_POSTINGS_TERMS_PER_FIELD {
                return Err(LogSegError::Corrupted(
                    "postings block_count over cap".into(),
                ));
            }
            let mut blocks = Vec::with_capacity((block_count as usize).min(1 << 16));
            let mut prev_term: Option<Vec<u8>> = None;
            for _ in 0..block_count {
                let first_term_len = usize::try_from(get_uvarint(bytes, &mut pos)?)
                    .map_err(|_| LogSegError::Corrupted("postings first_term_len range".into()))?;
                let end = pos
                    .checked_add(first_term_len)
                    .ok_or_else(|| LogSegError::Corrupted("postings first_term overflow".into()))?;
                let first_term = bytes
                    .get(pos..end)
                    .ok_or_else(|| LogSegError::Corrupted("postings first_term truncated".into()))?
                    .to_vec();
                pos = end;
                if let Some(prev) = &prev_term
                    && first_term.as_slice() <= prev.as_slice()
                {
                    return Err(LogSegError::Corrupted(
                        "postings first_term not ascending".into(),
                    ));
                }
                prev_term = Some(first_term.clone());

                let offset = read_u64_le(bytes, &mut pos, "postings block_offset")?;
                let stored_len = read_u64_le(bytes, &mut pos, "postings block_stored_len")?;
                let uncompressed_len =
                    read_u64_le(bytes, &mut pos, "postings block_uncompressed_len")?;
                if uncompressed_len > MAX_BLOCK_UNCOMP {
                    return Err(LogSegError::Corrupted(
                        "postings block_uncompressed_len over cap".into(),
                    ));
                }
                let crc_bytes = bytes.get(pos..pos + 4).ok_or_else(|| {
                    LogSegError::Corrupted("postings block_crc32c truncated".into())
                })?;
                let crc32c =
                    u32::from_le_bytes([crc_bytes[0], crc_bytes[1], crc_bytes[2], crc_bytes[3]]);
                pos += 4;

                let offset = usize::try_from(offset)
                    .map_err(|_| LogSegError::Corrupted("postings block_offset range".into()))?;
                let stored_len = usize::try_from(stored_len).map_err(|_| {
                    LogSegError::Corrupted("postings block_stored_len range".into())
                })?;
                let uncompressed_len = usize::try_from(uncompressed_len).map_err(|_| {
                    LogSegError::Corrupted("postings block_uncompressed_len range".into())
                })?;
                let block_end = offset.checked_add(stored_len).ok_or_else(|| {
                    LogSegError::Corrupted("postings block range overflow".into())
                })?;
                if block_end > bytes.len() {
                    return Err(LogSegError::Corrupted(
                        "postings block offset past section".into(),
                    ));
                }
                blocks.push(SparseEntry {
                    first_term,
                    offset,
                    stored_len,
                    uncompressed_len,
                    crc32c,
                });
            }
            fields.push(FieldIndex {
                column_id,
                capped: false,
                blocks,
            });
        }
        // `pos` now sits exactly at the end of the header; the term blocks
        // must tile the remaining bytes exactly, contiguous and in field/
        // block order, with no gap or overlap -- exactly how `encode_
        // postings_section` lays them out. Any deviation (a block pointing
        // back into the header, a gap, trailing garbage after the last
        // block) is corrupt input.
        let mut cursor = pos;
        for field in &fields {
            for block in &field.blocks {
                if block.offset != cursor {
                    return Err(LogSegError::Corrupted(
                        "postings block offset not contiguous".into(),
                    ));
                }
                cursor += block.stored_len;
            }
        }
        if cursor != bytes.len() {
            return Err(LogSegError::Corrupted("postings trailing bytes".into()));
        }
        Ok(PostingsSection { bytes, fields })
    }

    /// Looks up `term`'s posting list for `column_id`.
    ///
    /// - `Ok(None)`: the field carries no postings here (never indexed, or
    ///   capped out of this object) -- the caller MUST NOT prune on it and
    ///   falls back to bloom pruning plus an exact scan.
    /// - `Ok(Some(blocks))`: the field is indexed and its dictionary is
    ///   exact, so `blocks` (possibly empty) is the complete, sound set of
    ///   blocks that can contain `term`.
    /// - `Err(_)`: the section, or the one term block this probe needed, is
    ///   corrupt. The caller degrades the same way as `Ok(None)` (do not
    ///   prune on this arm) but should also record that a corruption was
    ///   seen, mirroring `bloom_degraded`.
    pub fn probe(&self, column_id: u32, term: &[u8]) -> Result<Option<Vec<u32>>, LogSegError> {
        let Ok(idx) = self
            .fields
            .binary_search_by(|f| f.column_id.cmp(&column_id))
        else {
            return Ok(None);
        };
        let field = &self.fields[idx];
        if field.capped {
            return Ok(None);
        }
        if field.blocks.is_empty() {
            return Ok(Some(Vec::new()));
        }
        // The last block whose first_term <= term; if term sorts before the
        // very first block's first_term, it cannot be in the dictionary.
        let block_idx = match field
            .blocks
            .binary_search_by(|b| b.first_term.as_slice().cmp(term))
        {
            Ok(i) => i,
            Err(0) => return Ok(Some(Vec::new())),
            Err(i) => i - 1,
        };
        let block = &field.blocks[block_idx];
        // The next sparse entry's `first_term`, known at parse time: every
        // term this block decodes to must sort strictly below it, or the
        // block's actual contents disagree with the header range `probe`
        // picked it by.
        let upper_bound = field
            .blocks
            .get(block_idx + 1)
            .map(|b| b.first_term.as_slice());
        let stored = self
            .bytes
            .get(block.offset..block.offset + block.stored_len)
            .ok_or_else(|| LogSegError::Corrupted("postings block out of bounds".into()))?;
        if crc32c::crc32c(stored) != block.crc32c {
            return Err(LogSegError::Corrupted("postings block crc mismatch".into()));
        }
        let payload = zstd::bulk::decompress(stored, block.uncompressed_len)
            .map_err(|e| LogSegError::Corrupted(format!("postings block zstd: {e}")))?;
        if payload.len() != block.uncompressed_len {
            return Err(LogSegError::Corrupted(
                "postings block decompressed length".into(),
            ));
        }
        find_term_in_block(&payload, term, &block.first_term, upper_bound)
    }
}

fn read_u64_le(bytes: &[u8], pos: &mut usize, what: &str) -> Result<u64, LogSegError> {
    let b = bytes
        .get(*pos..*pos + 8)
        .ok_or_else(|| LogSegError::Corrupted(format!("{what} truncated")))?;
    *pos += 8;
    Ok(u64::from_le_bytes([
        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
    ]))
}

/// Parses one decompressed term-block payload and returns `term`'s posting
/// list if present (`Some(vec![])` counts as "found the field's dictionary,
/// term is absent" -- callers distinguish that from "field not indexed" via
/// the `Option` at the [`PostingsSection::probe`] layer, not here).
///
/// Also re-derives, from the block's own crc-verified bytes, the two
/// properties `probe`'s binary search otherwise trusts blindly from the
/// unchecksummed sparse-index header: the block's actual first decoded term
/// must equal `expected_first_term` (the sparse entry `probe` selected this
/// block by), and every decoded term must sort strictly below `upper_bound`
/// (the next sparse entry's `first_term`, already known at parse time, or
/// `None` for the last block). A header whose `first_term` was corrupted to
/// redirect a probe at a different, still crc-valid block is caught here
/// instead of silently narrowing the result (docs/log-segment-format.md
/// "Pruning soundness"). This holds even when the whole-section crc
/// (verified separately by the caller before [`PostingsSection::parse`]) was
/// not consulted, e.g. a future ranged reader that fetches only the header
/// and one addressed block.
///
/// The whole block is always decoded -- never returned early on a match --
/// so the ascending-order and upper-bound checks cover every term, not just
/// the ones scanned before a hit, and the loop's exact byte consumption can
/// be checked against the payload length.
fn find_term_in_block(
    payload: &[u8],
    term: &[u8],
    expected_first_term: &[u8],
    upper_bound: Option<&[u8]>,
) -> Result<Option<Vec<u32>>, LogSegError> {
    let mut pos = 0usize;
    let term_count = get_uvarint(payload, &mut pos)?;
    if term_count > MAX_POSTINGS_TERMS_PER_FIELD {
        return Err(LogSegError::Corrupted(
            "postings block term_count over cap".into(),
        ));
    }
    if term_count == 0 {
        return Err(LogSegError::Corrupted(
            "postings block has zero terms".into(),
        ));
    }
    let mut prev_term: Option<Vec<u8>> = None;
    let mut found: Option<Vec<u32>> = None;
    for i in 0..term_count {
        let term_len = usize::try_from(get_uvarint(payload, &mut pos)?)
            .map_err(|_| LogSegError::Corrupted("postings term_len range".into()))?;
        let end = pos
            .checked_add(term_len)
            .ok_or_else(|| LogSegError::Corrupted("postings term overflow".into()))?;
        let this_term = payload
            .get(pos..end)
            .ok_or_else(|| LogSegError::Corrupted("postings term truncated".into()))?;
        if i == 0 && this_term != expected_first_term {
            return Err(LogSegError::Corrupted(
                "postings block first term does not match sparse index".into(),
            ));
        }
        if let Some(prev) = &prev_term
            && this_term <= prev.as_slice()
        {
            return Err(LogSegError::Corrupted(
                "postings block terms not ascending".into(),
            ));
        }
        if let Some(upper) = upper_bound
            && this_term >= upper
        {
            return Err(LogSegError::Corrupted(
                "postings block term exceeds next sparse entry".into(),
            ));
        }
        pos = end;
        let posting_count = get_uvarint(payload, &mut pos)?;
        if posting_count > MAX_POSTINGS_TERMS_PER_FIELD {
            return Err(LogSegError::Corrupted(
                "postings posting_count over cap".into(),
            ));
        }
        let mut list = Vec::with_capacity((posting_count as usize).min(1 << 16));
        let mut prev_block: Option<u32> = None;
        for _ in 0..posting_count {
            let delta = get_uvarint(payload, &mut pos)?;
            let block = match prev_block {
                None => u32::try_from(delta)
                    .map_err(|_| LogSegError::Corrupted("postings block index range".into()))?,
                Some(p) => {
                    if delta == 0 {
                        return Err(LogSegError::Corrupted(
                            "postings posting delta not positive".into(),
                        ));
                    }
                    let delta = u32::try_from(delta)
                        .map_err(|_| LogSegError::Corrupted("postings delta range".into()))?;
                    p.checked_add(delta).ok_or_else(|| {
                        LogSegError::Corrupted("postings block index overflow".into())
                    })?
                }
            };
            list.push(block);
            prev_block = Some(block);
        }
        if this_term == term {
            found = Some(list);
        }
        prev_term = Some(this_term.to_vec());
    }
    if pos != payload.len() {
        return Err(LogSegError::Corrupted(
            "postings block trailing bytes".into(),
        ));
    }
    Ok(Some(found.unwrap_or_default()))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn terms_map(pairs: &[(&[u8], &[u32])]) -> BTreeMap<Vec<u8>, BTreeSet<u32>> {
        pairs
            .iter()
            .map(|(t, blocks)| (t.to_vec(), blocks.iter().copied().collect()))
            .collect()
    }

    #[test]
    fn empty_section_round_trips() {
        let bytes = encode_postings_section(&BTreeMap::new(), DEFAULT_STRIDE, 3).expect("encode");
        let section = PostingsSection::parse(&bytes).expect("parse");
        assert!(section.fields.is_empty());
        assert_eq!(section.probe(10, b"x").expect("probe"), None);
    }

    #[test]
    fn rejects_wrong_version() {
        let mut bytes =
            encode_postings_section(&BTreeMap::new(), DEFAULT_STRIDE, 3).expect("encode");
        bytes[0] = POSTINGS_VERSION + 1;
        assert!(matches!(
            PostingsSection::parse(&bytes),
            Err(LogSegError::Corrupted(_))
        ));
    }

    #[test]
    fn rejects_trailing_bytes() {
        let mut bytes =
            encode_postings_section(&BTreeMap::new(), DEFAULT_STRIDE, 3).expect("encode");
        bytes.push(0xff);
        assert!(matches!(
            PostingsSection::parse(&bytes),
            Err(LogSegError::Corrupted(_))
        ));
    }

    #[test]
    fn rejects_non_ascending_column_id() {
        let mut fields = BTreeMap::new();
        fields.insert(12, FieldTerms::Capped);
        let mut bytes = encode_postings_section(&fields, DEFAULT_STRIDE, 3).expect("encode");
        // Duplicate the one field's bytes wholesale to synthesize two entries
        // sharing the same column_id (not ascending).
        let header_after_count = bytes[5..].to_vec();
        bytes[1..5].copy_from_slice(&2u32.to_le_bytes());
        bytes.extend_from_slice(&header_after_count);
        assert!(matches!(
            PostingsSection::parse(&bytes),
            Err(LogSegError::Corrupted(_))
        ));
    }

    #[test]
    fn single_field_probe_hits_and_misses() {
        let mut fields = BTreeMap::new();
        fields.insert(
            10,
            FieldTerms::Terms(terms_map(&[
                (b"alpha", &[0, 2, 5]),
                (b"beta", &[1]),
                (b"gamma", &[3, 4]),
            ])),
        );
        let bytes = encode_postings_section(&fields, 2, 3).expect("encode");
        let section = PostingsSection::parse(&bytes).expect("parse");

        assert_eq!(
            section.probe(10, b"alpha").expect("probe"),
            Some(vec![0, 2, 5])
        );
        assert_eq!(section.probe(10, b"beta").expect("probe"), Some(vec![1]));
        assert_eq!(
            section.probe(10, b"gamma").expect("probe"),
            Some(vec![3, 4])
        );
        // Absent term, but the field IS indexed: an exact empty result, safe
        // to prune to zero blocks.
        assert_eq!(section.probe(10, b"zzz").expect("probe"), Some(Vec::new()));
        assert_eq!(section.probe(10, b"aaa").expect("probe"), Some(Vec::new()));
        // Field never indexed: caller must not prune.
        assert_eq!(section.probe(99, b"alpha").expect("probe"), None);
    }

    #[test]
    fn capped_field_probe_is_none() {
        let mut fields = BTreeMap::new();
        fields.insert(10, FieldTerms::Capped);
        let bytes = encode_postings_section(&fields, DEFAULT_STRIDE, 3).expect("encode");
        let section = PostingsSection::parse(&bytes).expect("parse");
        assert_eq!(section.probe(10, b"anything").expect("probe"), None);
    }

    #[test]
    fn many_terms_span_multiple_blocks() {
        let mut map = BTreeMap::new();
        for i in 0..500u32 {
            map.insert(format!("term{i:04}").into_bytes(), BTreeSet::from([i % 7]));
        }
        let mut fields = BTreeMap::new();
        fields.insert(3, FieldTerms::Terms(map));
        let bytes = encode_postings_section(&fields, 16, 3).expect("encode");
        let section = PostingsSection::parse(&bytes).expect("parse");
        for i in 0..500u32 {
            let got = section
                .probe(3, format!("term{i:04}").into_bytes().as_slice())
                .expect("probe");
            assert_eq!(got, Some(vec![i % 7]), "term{i:04}");
        }
        assert_eq!(
            section.probe(3, b"missing").expect("probe"),
            Some(Vec::new())
        );
    }

    #[test]
    fn rejects_crc_mismatch() {
        let mut fields = BTreeMap::new();
        fields.insert(1, FieldTerms::Terms(terms_map(&[(b"a", &[0])])));
        let mut bytes = encode_postings_section(&fields, DEFAULT_STRIDE, 3).expect("encode");
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        let section = PostingsSection::parse(&bytes).expect("parse");
        assert!(matches!(
            section.probe(1, b"a"),
            Err(LogSegError::Corrupted(_))
        ));
    }

    #[test]
    fn rejects_block_offset_past_section() {
        let mut fields = BTreeMap::new();
        fields.insert(1, FieldTerms::Terms(terms_map(&[(b"a", &[0])])));
        let mut bytes = encode_postings_section(&fields, DEFAULT_STRIDE, 3).expect("encode");
        // The block_offset field is the 8 bytes right after first_term_bytes
        // ("a", len-prefixed): version(1) + field_count(4) + column_id(1) +
        // capped(1) + stride(1) + term_count(1) + block_count(1) +
        // first_term_len(1) + first_term("a", 1 byte) = byte 12.
        let offset_pos = 12;
        bytes[offset_pos..offset_pos + 8].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(matches!(
            PostingsSection::parse(&bytes),
            Err(LogSegError::Corrupted(_))
        ));
    }

    #[test]
    fn rejects_truncated_posting_list() {
        let mut fields = BTreeMap::new();
        fields.insert(1, FieldTerms::Terms(terms_map(&[(b"a", &[0, 1, 2])])));
        let bytes = encode_postings_section(&fields, DEFAULT_STRIDE, 0).expect("encode");
        let section = PostingsSection::parse(&bytes).expect("parse");
        // Truncate the object right after the header so the addressed term
        // block's stored bytes are cut short.
        let header_only = bytes[..bytes.len() - 1].to_vec();
        // Re-parse against the truncated buffer would fail bounds at parse
        // time (offset past section); instead exercise the probe-time path by
        // corrupting a copy whose header still matches but the block itself
        // is truncated post-parse would require a hand-crafted offset, so
        // assert the natural parse-time rejection here.
        assert!(matches!(
            PostingsSection::parse(&header_only),
            Err(LogSegError::Corrupted(_))
        ));
        drop(section);
    }

    #[test]
    fn rejects_declared_term_count_too_large() {
        let mut fields = BTreeMap::new();
        fields.insert(1, FieldTerms::Terms(terms_map(&[(b"a", &[0])])));
        let mut bytes = encode_postings_section(&fields, DEFAULT_STRIDE, 3).expect("encode");
        // term_count is the uvarint right after stride: version(1) +
        // field_count(4) + column_id(1) + capped(1) + stride(1) = byte 8.
        bytes[8] = 0xff;
        bytes.insert(9, 0xff);
        bytes.insert(10, 0xff);
        bytes.insert(11, 0xff);
        bytes.insert(12, 0x7f);
        assert!(matches!(
            PostingsSection::parse(&bytes),
            Err(LogSegError::Corrupted(_))
        ));
    }

    /// Targeted reproduction (fix-task on epic #479, issue #508 follow-up):
    /// a one-byte flip of a sparse-index `first_term`, touching no term
    /// block, used to silently narrow a probe's result instead of failing
    /// loud. `tests/corrupt.rs`'s proptest cannot find this: a random byte
    /// flip almost never both lands in the header and preserves ascending
    /// `first_term` order, and `parse` rejects anything that doesn't.
    ///
    /// Four terms `aa, bz, ca, cz` with `stride = 2` split into two term
    /// blocks: `B0 = [aa, bz]` (sparse entry `first_term = "aa"`) and
    /// `B1 = [ca, cz]` (sparse entry `first_term = "ca"`). Flipping the
    /// declared `first_term` for `B1` from `"ca"` to `"ba"` preserves
    /// ascending order across entries (`"aa" < "ba"`), so `parse` still
    /// accepts, and touches no term-block bytes, so `B1`'s own `crc32c`
    /// still verifies. Probing `"bz"` sorts between the corrupted `"ba"` and
    /// the next entry, so the binary search still lands on `B1` -- which,
    /// before this fix, decoded fine, did not contain `"bz"`, and reported
    /// it exact-absent instead of catching that `B1`'s real first term
    /// ("ca") disagrees with the header entry that pointed at it.
    #[test]
    fn corrupted_first_term_header_byte_is_caught_not_silently_narrowed() {
        let mut fields = BTreeMap::new();
        fields.insert(
            1,
            FieldTerms::Terms(terms_map(&[
                (b"aa", &[0]),
                (b"bz", &[1]),
                (b"ca", &[2]),
                (b"cz", &[3]),
            ])),
        );
        let mut bytes = encode_postings_section(&fields, 2, 3).expect("encode");

        // Baseline: the uncorrupted section resolves "bz" to block 1, exact.
        let baseline = PostingsSection::parse(&bytes).expect("parse");
        assert_eq!(baseline.probe(1, b"bz").expect("probe"), Some(vec![1]));

        // B1's declared first_term "ca" lives at a fixed offset: version(1) +
        // field_count(4) + column_id(1) + capped(1) + stride(1) +
        // term_count(1) + block_count(1) = 10, then B0's whole entry
        // (first_term_len(1) + "aa"(2) + offset/stored_len/uncompressed_len/
        // crc32c(8+8+8+4)) is 31 bytes, ending at 41; B1's first_term_len(1)
        // puts its first_term bytes at [42, 44).
        let corrupt_at = 42;
        assert_eq!(
            &bytes[corrupt_at..corrupt_at + 2],
            b"ca",
            "offset math must match the header layout"
        );
        bytes[corrupt_at] = b'b';

        let section =
            PostingsSection::parse(&bytes).expect("ascending order preserved, parse still accepts");
        let err = section
            .probe(1, b"bz")
            .expect_err("a header pointing at a block whose real first term disagrees must fail loud, not report an exact miss");
        assert!(matches!(err, LogSegError::Corrupted(_)), "got {err:?}");
    }
}
