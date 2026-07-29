//! RSEG v5 sparse catalog: SERIES_IDX (kind 8) and chunked SERIES_META
//! (kind 9), the compacted-tier selective-read structures of ADR-0026.
//!
//! v5 is the v4 grammar (ADR-0018) plus, when the output object carries at
//! least [`crate::format::V5_SPARSE_THRESHOLD`] series, two sparse sections:
//!
//! - SERIES_IDX (kind 8): every Kth series id (K = [`crate::format::V5_STRIDE`])
//!   with the SERIES_IDS byte window it heads (offset, length, and a crc32c
//!   over exactly that window), plus the meta-chunk directory (each chunk's
//!   stored byte range, first series index, series count, and a crc32c over
//!   the stored frame). A point lookup binary-searches the sparse ids, then
//!   range-GETs one id window and one meta chunk. SERIES_IDX itself is small
//!   and always fetched whole, covered by its ordinary `Section.crc32c`.
//!
//! - SERIES_META_CHUNKS (kind 9): the v4 SERIES_META re-laid as a small schema
//!   header followed by per-chunk zstd frames, replacing the kind 6
//!   whole-section form. Each frame is the run-major column set for one chunk
//!   of K series, with per-frame page-offset bases so a single frame
//!   reconstructs its series' absolute page ranges without the rest of the
//!   catalog.
//!
//! Partial fetches verify what they touch (ADR-0026 point 6): the id-window
//! and meta-chunk crc32c carried in SERIES_IDX cover exactly the ranges a
//! range-GET returns, which a whole-section crc cannot. Below the threshold a
//! v5 object carries the v4-shaped whole-section catalog and no sparse
//! sections; the reader uses the legacy path, signalled by the sections'
//! presence.

use bytes::Bytes;
use prost::Message;
use ravel_proto::segment::v1::{Footer, Section};

use crate::crc::footer_crc;
use crate::error::{SegmentError, WriteError};
use crate::format::{
    MAGIC, RESERVED, ReaderLimits, SIGNAL_METRICS, V5_STRIDE, VERSION_V5, ZSTD_LEVEL, compression,
    section_kind,
};
use crate::reader::{
    DictResolver, RUN_LIVE_BYTES_CHUNK, RunEntry, SeriesEntry, SeriesEntryV4, ValueKind,
    check_run_total_live_budget, check_series_counts_v2, decode_section_bytes, find_section,
    index_label_dict, parse_schema_list_v2, parse_schema_ref_block_v2, parse_series_ids_v2,
    parse_value_ord_block_all_v2, take_block, take_u32_le,
};
use crate::varint::{read_uvarint, write_uvarint};
use crate::writer::WrittenSegment;
use ravel_types::{Label, LabelSet, SeriesId};

/// Byte-layout version of the SERIES_IDX section body. Bumping this is a
/// format change (it is inside a frozen section), so the reader rejects any
/// other value with [`SegmentError::UnsupportedSparseIndexVersion`].
const SPARSE_INDEX_VERSION: u8 = 1;

// ===========================================================================
// SERIES_IDX in-memory model
// ===========================================================================

/// One sparse-id entry: an indexed series id, the SERIES_IDS byte window it
/// heads, and that window's crc32c (so a range-GET of just the window is
/// verifiable).
#[derive(Debug, Clone, Copy)]
struct SparseEntry {
    id: [u8; 16],
    /// Byte offset within the SERIES_IDS section payload.
    ids_offset: u64,
    /// Byte length of the window this entry heads (a multiple of 16).
    window_len: u64,
    /// crc32c over `SERIES_IDS[ids_offset .. ids_offset + window_len]`.
    window_crc32c: u32,
}

/// One meta-chunk directory entry: the stored (zstd) frame's byte range
/// within SERIES_META_CHUNKS, its uncompressed length, the absolute series
/// index of its first row, its row count, and a crc32c over the stored frame.
#[derive(Debug, Clone, Copy)]
struct ChunkDirEntry {
    frame_offset: u64,
    frame_stored_len: u64,
    frame_uncompressed_len: u64,
    first_index: u32,
    n: u32,
    frame_crc32c: u32,
}

/// Parsed SERIES_IDX section: the sparse id index plus the meta-chunk
/// directory. Always fetched whole and covered by its `Section.crc32c`, so
/// this parse trusts the section bytes were already crc-verified by the
/// caller's whole-section read.
#[derive(Debug, Clone)]
pub struct SparseIdIndex {
    stride: u32,
    series_count: u32,
    entries: Vec<SparseEntry>,
    chunk_stride: u32,
    chunks: Vec<ChunkDirEntry>,
}

/// A SERIES_IDS byte window to range-GET, plus the absolute series index of
/// its first id and the crc32c the fetched bytes must match.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdWindow {
    /// Byte offset within the SERIES_IDS section payload.
    pub section_offset: u64,
    /// Byte length (a multiple of 16).
    pub len: u64,
    /// Absolute series index of the window's first id.
    pub first_index: u64,
    /// crc32c the fetched window bytes must match ([`verify_id_window`]).
    pub crc32c: u32,
}

/// Where one series' meta chunk lives, which row inside it to read, and the
/// crc32c the fetched stored frame must match.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkLocation {
    /// Stored byte offset of the frame within the SERIES_META_CHUNKS section.
    pub frame_offset: u64,
    pub frame_stored_len: u64,
    pub frame_uncompressed_len: u64,
    pub row_in_chunk: u64,
    /// crc32c the fetched stored frame must match
    /// ([`verify_and_decompress_chunk_frame`]).
    pub crc32c: u32,
}

// ===========================================================================
// SERIES_IDX decode
// ===========================================================================

fn take_u32(bytes: &[u8], pos: &mut usize) -> Result<u32, SegmentError> {
    let end = pos.checked_add(4).ok_or(SegmentError::Truncated)?;
    let s = bytes.get(*pos..end).ok_or(SegmentError::Truncated)?;
    *pos = end;
    Ok(u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
}

fn take_u64(bytes: &[u8], pos: &mut usize) -> Result<u64, SegmentError> {
    let end = pos.checked_add(8).ok_or(SegmentError::Truncated)?;
    let s = bytes.get(*pos..end).ok_or(SegmentError::Truncated)?;
    *pos = end;
    let arr: [u8; 8] = s.try_into().map_err(|_| SegmentError::Truncated)?;
    Ok(u64::from_le_bytes(arr))
}

fn take_id(bytes: &[u8], pos: &mut usize) -> Result<[u8; 16], SegmentError> {
    let end = pos.checked_add(16).ok_or(SegmentError::Truncated)?;
    let s = bytes.get(*pos..end).ok_or(SegmentError::Truncated)?;
    *pos = end;
    s.try_into().map_err(|_| SegmentError::Truncated)
}

/// Decodes a SERIES_IDX section (kind 8). The section bytes are assumed
/// already crc-verified as a whole (SERIES_IDX is never range-fetched); the
/// per-window and per-chunk crc32c it carries verify the *other* sections'
/// range-GETs, not itself.
///
/// Beyond field-level truncation/sortedness checks, this also validates the
/// structural invariants `locate`/`chunk_for` rely on: `sparse_count` and
/// `chunk_count` against `series_count`/the strides, each entry's
/// `ids_offset`/`window_len` against its formula, and the chunk directory's
/// dense `first_index`/`n` chain. A corrupt-but-crc-consistent index (a
/// tampered count or offset that still passes the section crc32c) fails to
/// parse here instead of `chunk_for`/`locate` silently treating a present id
/// as absent.
pub fn parse_series_idx(bytes: &[u8]) -> Result<SparseIdIndex, SegmentError> {
    let mut pos = 0usize;
    let version = *bytes.get(pos).ok_or(SegmentError::Truncated)?;
    pos += 1;
    if version != SPARSE_INDEX_VERSION {
        return Err(SegmentError::UnsupportedSparseIndexVersion(version));
    }
    // flags(1) + reserved(2): reserved for a future additive field, must be
    // walked but carry no meaning today.
    let _flags = *bytes.get(pos).ok_or(SegmentError::Truncated)?;
    pos = pos.checked_add(3).ok_or(SegmentError::Truncated)?;

    let stride = take_u32(bytes, &mut pos)?;
    if stride == 0 {
        return Err(SegmentError::ZeroStride);
    }
    let series_count = take_u32(bytes, &mut pos)?;
    let sparse_count = take_u32(bytes, &mut pos)?;
    // Entry `p` indexes series `p*stride`, so the entry count is pinned by
    // series_count/stride (docs/segment-format.md "SERIES_IDX"): one entry
    // per full stride, plus one more for a nonempty remainder.
    let expected_sparse_count = series_count.div_ceil(stride);
    if sparse_count != expected_sparse_count {
        return Err(SegmentError::BadSparseIndex(
            "sparse_count does not match series_count/stride",
        ));
    }

    let mut entries = Vec::with_capacity((sparse_count as usize).min(bytes.len() / 36 + 1));
    let mut prev: Option<[u8; 16]> = None;
    for p in 0..sparse_count {
        let id = take_id(bytes, &mut pos)?;
        if let Some(prev_id) = prev
            && id <= prev_id
        {
            return Err(SegmentError::SeriesIdsUnsorted);
        }
        prev = Some(id);
        let ids_offset = take_u64(bytes, &mut pos)?;
        let window_len = take_u64(bytes, &mut pos)?;
        let window_crc32c = take_u32(bytes, &mut pos)?;

        // Bounded by the sparse_count check above (entry_first < series_count
        // <= u32::MAX), so this arithmetic cannot overflow u64.
        let entry_first = u64::from(p) * u64::from(stride);
        let expected_offset = 4u64 + entry_first * 16;
        if ids_offset != expected_offset {
            return Err(SegmentError::BadSparseIndex(
                "ids_offset does not match its expected formula",
            ));
        }
        let is_last_entry = p + 1 == sparse_count;
        let expected_id_count = if is_last_entry {
            u64::from(series_count) - entry_first
        } else {
            u64::from(stride)
        };
        if window_len != expected_id_count * 16 {
            return Err(SegmentError::BadSparseIndex(
                "window_len does not match the id count it should cover",
            ));
        }

        entries.push(SparseEntry {
            id,
            ids_offset,
            window_len,
            window_crc32c,
        });
    }

    let chunk_stride = take_u32(bytes, &mut pos)?;
    if chunk_stride == 0 {
        return Err(SegmentError::ZeroStride);
    }
    let chunk_count = take_u32(bytes, &mut pos)?;
    let expected_chunk_count = series_count.div_ceil(chunk_stride);
    if chunk_count != expected_chunk_count {
        return Err(SegmentError::BadSparseIndex(
            "chunk_count does not match series_count/chunk_stride",
        ));
    }
    let mut chunks = Vec::with_capacity((chunk_count as usize).min(bytes.len() / 36 + 1));
    for k in 0..chunk_count {
        let frame_offset = take_u64(bytes, &mut pos)?;
        let frame_stored_len = take_u64(bytes, &mut pos)?;
        let frame_uncompressed_len = take_u64(bytes, &mut pos)?;
        let first_index = take_u32(bytes, &mut pos)?;
        let n = take_u32(bytes, &mut pos)?;
        let frame_crc32c = take_u32(bytes, &mut pos)?;

        // Dense chain (docs/segment-format.md): chunk k covers
        // [k*chunk_stride, k*chunk_stride + n), n == chunk_stride except the
        // last chunk, which covers the remainder. Validating this here (not
        // just in decode_catalog_v5) protects the point-lookup path
        // (`chunk_for`) too: a corrupt-but-crc-consistent directory now
        // fails to parse instead of `chunk_for` silently answering "absent".
        let expected_first_index = u64::from(k) * u64::from(chunk_stride);
        if u64::from(first_index) != expected_first_index {
            return Err(SegmentError::BadSparseIndex(
                "chunk first_index does not match the dense stride chain",
            ));
        }
        let is_last_chunk = k + 1 == chunk_count;
        let expected_n = if is_last_chunk {
            u64::from(series_count) - expected_first_index
        } else {
            u64::from(chunk_stride)
        };
        if u64::from(n) != expected_n {
            return Err(SegmentError::BadSparseIndex(
                "chunk n does not match chunk_stride (or the final remainder)",
            ));
        }

        chunks.push(ChunkDirEntry {
            frame_offset,
            frame_stored_len,
            frame_uncompressed_len,
            first_index,
            n,
            frame_crc32c,
        });
    }

    if pos != bytes.len() {
        return Err(SegmentError::TrailingBytes);
    }

    Ok(SparseIdIndex {
        stride,
        series_count,
        entries,
        chunk_stride,
        chunks,
    })
}

impl SparseIdIndex {
    pub fn series_count(&self) -> u32 {
        self.series_count
    }

    /// Binary-searches the sparse ids for the SERIES_IDS window that must
    /// contain `target` if the object contains it at all. Returns `None` only
    /// when `target` is below the smallest indexed id (so it cannot be
    /// present).
    pub fn locate(&self, target: &[u8; 16]) -> Option<IdWindow> {
        if self.entries.is_empty() || target < &self.entries[0].id {
            return None;
        }
        // Largest p with entries[p].id <= target.
        let mut lo = 0usize;
        let mut hi = self.entries.len();
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if &self.entries[mid].id <= target {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        let p = lo - 1;
        let e = &self.entries[p];
        Some(IdWindow {
            section_offset: e.ids_offset,
            len: e.window_len,
            first_index: u64::from(p as u32) * u64::from(self.stride),
            crc32c: e.window_crc32c,
        })
    }

    /// Which chunk covers absolute series `index` (frame byte range, row
    /// offset, and crc32c). `None` when `index` is out of range.
    pub fn chunk_for(&self, index: u64) -> Option<ChunkLocation> {
        let k = usize::try_from(index / u64::from(self.chunk_stride)).ok()?;
        let c = self.chunks.get(k)?;
        if index < u64::from(c.first_index) || index >= u64::from(c.first_index) + u64::from(c.n) {
            return None;
        }
        Some(ChunkLocation {
            frame_offset: c.frame_offset,
            frame_stored_len: c.frame_stored_len,
            frame_uncompressed_len: c.frame_uncompressed_len,
            row_in_chunk: index - u64::from(c.first_index),
            crc32c: c.frame_crc32c,
        })
    }
}

/// Verifies a fetched SERIES_IDS window against the crc32c
/// [`SparseIdIndex::locate`] returned. Every byte the sparse path interprets
/// stays checksum-verified (ADR-0010 §4, ADR-0026 point 6).
pub fn verify_id_window(window: &[u8], window_meta: &IdWindow) -> Result<(), SegmentError> {
    if window.len() as u64 != window_meta.len {
        return Err(SegmentError::SectionOutOfBounds);
    }
    if crc32c::crc32c(window) != window_meta.crc32c {
        return Err(SegmentError::IdWindowCrcMismatch);
    }
    Ok(())
}

/// Binary-searches a fetched SERIES_IDS window (`window` is `n*16` raw id
/// bytes, `first_index` the absolute index of its first id) for `target`,
/// returning its absolute index or `None`. Verify the window's crc32c with
/// [`verify_id_window`] before calling.
pub fn find_index_in_window(
    window: &[u8],
    first_index: u64,
    target: &[u8; 16],
) -> Result<Option<u64>, SegmentError> {
    if !window.len().is_multiple_of(16) {
        return Err(SegmentError::SectionOutOfBounds);
    }
    let n = window.len() / 16;
    let mut lo = 0usize;
    let mut hi = n;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        let start = mid * 16;
        let id: &[u8] = &window[start..start + 16];
        match id.cmp(target.as_slice()) {
            std::cmp::Ordering::Less => lo = mid + 1,
            std::cmp::Ordering::Greater => hi = mid,
            std::cmp::Ordering::Equal => return Ok(Some(first_index + mid as u64)),
        }
    }
    Ok(None)
}

/// Verifies a fetched stored meta-chunk frame against the crc32c
/// [`SparseIdIndex::chunk_for`] returned, then decompresses it.
pub fn verify_and_decompress_chunk_frame(
    stored: &[u8],
    chunk: &ChunkLocation,
    limits: ReaderLimits,
) -> Result<Vec<u8>, SegmentError> {
    if stored.len() as u64 != chunk.frame_stored_len {
        return Err(SegmentError::SectionOutOfBounds);
    }
    if crc32c::crc32c(stored) != chunk.crc32c {
        return Err(SegmentError::ChunkCrcMismatch);
    }
    if chunk.frame_uncompressed_len > limits.max_section_uncompressed_bytes {
        return Err(SegmentError::SectionTooLarge {
            len: chunk.frame_uncompressed_len,
            cap: limits.max_section_uncompressed_bytes,
        });
    }
    let cap = usize::try_from(chunk.frame_uncompressed_len)
        .map_err(|_| SegmentError::SectionOutOfBounds)?;
    let out =
        zstd::bulk::decompress(stored, cap).map_err(|e| SegmentError::Decompress(e.to_string()))?;
    if out.len() as u64 != chunk.frame_uncompressed_len {
        return Err(SegmentError::DecompressedLenMismatch {
            expected: chunk.frame_uncompressed_len,
            actual: out.len() as u64,
        });
    }
    Ok(out)
}

// ===========================================================================
// Meta-chunk frame decode
// ===========================================================================

/// Reads a length-prefixed block of exactly `expected` uvarints.
fn read_uvarint_block(
    bytes: &[u8],
    pos: &mut usize,
    expected: usize,
) -> Result<Vec<u64>, SegmentError> {
    let block = take_block(bytes, pos)?;
    let mut bpos = 0usize;
    let mut out = Vec::with_capacity(expected.min(block.len()));
    for _ in 0..expected {
        out.push(read_uvarint(block, &mut bpos)?);
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }
    Ok(out)
}

/// Reconstructs `(offset, len)` ranges from a gap/len column pair starting
/// from a per-frame `base` end, the chunk counterpart of
/// [`crate::reader::reconstruct_ranges_v2`] (which starts from 0). `base` is
/// the absolute `end` accumulated over every run before this frame's first
/// run, so the reconstructed offsets equal the whole-section reconstruction.
fn reconstruct_from_base(
    gaps: &[u64],
    lens: &[u64],
    base: u64,
    section_len: u64,
) -> Result<Vec<(u64, u64)>, SegmentError> {
    let mut out = Vec::with_capacity(gaps.len());
    let mut end = base;
    for (&gap, &len) in gaps.iter().zip(lens) {
        let offset = end
            .checked_add(gap)
            .ok_or(SegmentError::SectionOutOfBounds)?;
        let new_end = offset
            .checked_add(len)
            .ok_or(SegmentError::SectionOutOfBounds)?;
        if new_end > section_len {
            return Err(SegmentError::SectionOutOfBounds);
        }
        out.push((offset, len));
        end = new_end;
    }
    Ok(out)
}

/// The decoded run-major columns of one meta-chunk frame, shared by the
/// single-row point path ([`decode_chunk_runs`]) and the whole-frame
/// reassembly path ([`decode_catalog_v5`]).
struct ChunkFrame {
    n: usize,
    /// Per-series value_kind (`n` entries).
    value_kind: Vec<ValueKind>,
    /// Per-series run count (`n` entries).
    run_count: Vec<u32>,
    /// Per-series schema_ref (`n` entries); needed only for label materialization.
    schema_ref: Vec<u32>,
    /// Series-major value ordinals; needed only for label materialization.
    value_ord: Vec<u64>,
    /// Per-run reconstructed entries (`frame_run_total` entries, in series
    /// then run order), with absolute page offsets and provenance.
    runs: Vec<RunEntry>,
}

/// Decodes one meta-chunk frame's full run-major column set, reconstructing
/// every run's absolute page ranges and provenance. `ts_section_len`,
/// `val_section_len`, `hist_section_len` bound the reconstructed ranges.
fn decode_chunk_frame(
    frame_raw: &[u8],
    footer: &Footer,
    ts_section_len: u64,
    val_section_len: u64,
    hist_section_len: u64,
    limits: ReaderLimits,
) -> Result<ChunkFrame, SegmentError> {
    let mut pos = 0usize;
    let n = take_u32(frame_raw, &mut pos)? as usize;
    let frame_run_total = take_u32(frame_raw, &mut pos)? as usize;
    check_run_total_live_budget(frame_run_total as u64, RUN_LIVE_BYTES_CHUNK, limits)?;
    let ts_base = read_uvarint(frame_raw, &mut pos)?;
    let val_base = read_uvarint(frame_raw, &mut pos)?;
    let hist_base = read_uvarint(frame_raw, &mut pos)?;

    // schema_ref: n uvarints.
    let schema_ref: Vec<u32> = read_uvarint_block(frame_raw, &mut pos, n)?
        .into_iter()
        .map(|v| u32::try_from(v).map_err(|_| SegmentError::FieldOverflow))
        .collect::<Result<_, _>>()?;

    // value_ord: a length-prefixed block; the count is implied by the
    // schemas, which the point path does not have, so it is taken whole and
    // re-split by the reassembly caller (which does have the schemas).
    let value_ord_block = take_block(frame_raw, &mut pos)?.to_vec();

    // value_kind: n bytes.
    let vk_block = take_block(frame_raw, &mut pos)?;
    if vk_block.len() != n {
        return Err(SegmentError::BadBlockLen);
    }
    let mut value_kind = Vec::with_capacity(n);
    for &b in vk_block {
        match b {
            0 => value_kind.push(ValueKind::Scalar),
            1 => value_kind.push(ValueKind::Histogram),
            other => return Err(SegmentError::InvalidValueKind(other)),
        }
    }

    // run_count: n uvarints, summing to frame_run_total.
    let run_count: Vec<u32> = read_uvarint_block(frame_raw, &mut pos, n)?
        .into_iter()
        .map(|v| u32::try_from(v).map_err(|_| SegmentError::FieldOverflow))
        .collect::<Result<_, _>>()?;
    let mut rc_sum: u64 = 0;
    for &rc in &run_count {
        if rc == 0 {
            return Err(SegmentError::ZeroRunCount);
        }
        rc_sum = rc_sum
            .checked_add(u64::from(rc))
            .ok_or(SegmentError::FieldOverflow)?;
    }
    if rc_sum != frame_run_total as u64 {
        return Err(SegmentError::RunCountSumMismatch {
            run_count_sum: rc_sum,
            run_total: frame_run_total as u64,
        });
    }

    let created_delta = read_uvarint_block(frame_raw, &mut pos, frame_run_total)?;
    let epoch = read_uvarint_block(frame_raw, &mut pos, frame_run_total)?;
    let seq = read_uvarint_block(frame_raw, &mut pos, frame_run_total)?;
    let sample_count = read_uvarint_block(frame_raw, &mut pos, frame_run_total)?;
    let min_ts_delta = read_uvarint_block(frame_raw, &mut pos, frame_run_total)?;
    let ts_span = read_uvarint_block(frame_raw, &mut pos, frame_run_total)?;
    let ts_gap = read_uvarint_block(frame_raw, &mut pos, frame_run_total)?;
    let ts_len = read_uvarint_block(frame_raw, &mut pos, frame_run_total)?;
    let val_gap = read_uvarint_block(frame_raw, &mut pos, frame_run_total)?;
    let val_len = read_uvarint_block(frame_raw, &mut pos, frame_run_total)?;
    let hist_gap = read_uvarint_block(frame_raw, &mut pos, frame_run_total)?;
    let hist_len = read_uvarint_block(frame_raw, &mut pos, frame_run_total)?;
    if pos != frame_raw.len() {
        return Err(SegmentError::TrailingBytes);
    }

    let ts_page = reconstruct_from_base(&ts_gap, &ts_len, ts_base, ts_section_len)?;
    let val_page = reconstruct_from_base(&val_gap, &val_len, val_base, val_section_len)?;
    let hist_page = reconstruct_from_base(&hist_gap, &hist_len, hist_base, hist_section_len)?;

    let mut runs = Vec::with_capacity(frame_run_total);
    for i in 0..frame_run_total {
        let created = i128::from(footer.base_created_unix_ns) + i128::from(created_delta[i]);
        let created_unix_ns =
            i64::try_from(created).map_err(|_| SegmentError::ProvenanceBoundsOverflow)?;
        let min = i128::from(footer.min_event_ts_ns) + i128::from(min_ts_delta[i]);
        let min_ts_ns = i64::try_from(min).map_err(|_| SegmentError::ProvenanceBoundsOverflow)?;
        let max = i128::from(min_ts_ns) + i128::from(ts_span[i]);
        let max_ts_ns = i64::try_from(max).map_err(|_| SegmentError::ProvenanceBoundsOverflow)?;
        let sc = u32::try_from(sample_count[i]).map_err(|_| SegmentError::FieldOverflow)?;
        if sc == 0 {
            return Err(SegmentError::ZeroSampleCount);
        }
        runs.push(RunEntry {
            created_unix_ns,
            writer_epoch: epoch[i],
            writer_seq: seq[i],
            sample_count: sc,
            min_ts_ns,
            max_ts_ns,
            ts_page: ts_page[i],
            val_page: val_page[i],
            hist_page: hist_page[i],
        });
    }

    // Per-run value_kind-vs-page cross-check, the frame-local counterpart of
    // the reader's whole-section check.
    let mut run_idx = 0usize;
    for i in 0..n {
        for _ in 0..run_count[i] {
            match value_kind[i] {
                ValueKind::Scalar => {
                    if runs[run_idx].hist_page.1 != 0 {
                        return Err(SegmentError::ScalarSeriesHasHistPage);
                    }
                }
                ValueKind::Histogram => {
                    if runs[run_idx].val_page.1 != 0 {
                        return Err(SegmentError::HistogramSeriesHasValPage);
                    }
                    if runs[run_idx].hist_page.1 == 0 {
                        return Err(SegmentError::ZeroHistPageLen);
                    }
                }
            }
            run_idx += 1;
        }
    }

    Ok(ChunkFrame {
        n,
        value_kind,
        run_count,
        schema_ref,
        value_ord: value_ord_block_to_flat(&value_ord_block)?,
        runs,
    })
}

/// The value_ord block is series-major but its per-series lengths need the
/// schema list, which the frame decode does not hold. It is decoded to a flat
/// `Vec<u64>` of every ordinal in order; the reassembly caller re-splits it by
/// schema. A malformed varint here is a typed error, never a panic.
fn value_ord_block_to_flat(block: &[u8]) -> Result<Vec<u64>, SegmentError> {
    let mut pos = 0usize;
    let mut out = Vec::with_capacity(block.len());
    while pos < block.len() {
        out.push(read_uvarint(block, &mut pos)?);
    }
    Ok(out)
}

/// Decodes just the runs of the series at `row_in_chunk` from a decompressed
/// meta-chunk frame (the selective point path). Skips label materialization
/// entirely: a by-id fetch already knows the series it wants. `limits` bounds
/// `frame_run_total`'s live-decode footprint exactly as it does in
/// [`decode_catalog_v5`]'s whole-frame path; pass the same limits used to
/// decompress `frame_raw` (`verify_and_decompress_chunk_frame`).
pub fn decode_chunk_runs(
    frame_raw: &[u8],
    row_in_chunk: u64,
    footer: &Footer,
    limits: ReaderLimits,
) -> Result<Vec<RunEntry>, SegmentError> {
    let ts_len = find_section(footer, section_kind::TS_PAGES)
        .map(|s| s.len)
        .unwrap_or(0);
    let val_len = find_section(footer, section_kind::VAL_PAGES)
        .map(|s| s.len)
        .unwrap_or(0);
    let hist_len = find_section(footer, section_kind::HIST_PAGES)
        .map(|s| s.len)
        .unwrap_or(0);
    let frame = decode_chunk_frame(frame_raw, footer, ts_len, val_len, hist_len, limits)?;
    let row = usize::try_from(row_in_chunk).map_err(|_| SegmentError::SectionOutOfBounds)?;
    if row >= frame.n {
        return Err(SegmentError::SectionOutOfBounds);
    }
    let run_offset: usize = frame.run_count[..row].iter().map(|&c| c as usize).sum();
    let rc = frame.run_count[row] as usize;
    Ok(frame.runs[run_offset..run_offset + rc].to_vec())
}

// ===========================================================================
// Whole-catalog decode (v5)
// ===========================================================================

/// Decodes the full v5 series catalog into [`SeriesEntryV4`] values. Below the
/// sparse threshold a v5 object carries the v4-shaped whole-section
/// SERIES_META, so this delegates to [`crate::reader::decode_catalog_v4`]. At
/// or above the threshold it reassembles the chunked SERIES_META
/// (SERIES_META_CHUNKS) frame by frame, using SERIES_IDX's chunk directory to
/// locate each stored frame. Unlike the range-fetched decoders, this takes the
/// whole object bytes, since a v5 chunked catalog inherently spans several
/// sections.
pub fn decode_catalog_v5(
    footer: &Footer,
    object_bytes: &[u8],
    limits: ReaderLimits,
) -> Result<Vec<SeriesEntryV4>, SegmentError> {
    let label_dict_section = find_section(footer, section_kind::LABEL_DICT)
        .ok_or(SegmentError::MissingSection("LABEL_DICT"))?;
    let series_ids_section = find_section(footer, section_kind::SERIES_IDS)
        .ok_or(SegmentError::MissingSection("SERIES_IDS"))?;
    let dict_bytes = decode_section_bytes(
        label_dict_section,
        slice(object_bytes, label_dict_section)?,
        limits,
    )?;
    let ids_bytes = decode_section_bytes(
        series_ids_section,
        slice(object_bytes, series_ids_section)?,
        limits,
    )?;
    let dict_index = index_label_dict(&dict_bytes)?;
    let series_ids = parse_series_ids_v2(&ids_bytes)?;

    // Whole-section (below-threshold) path: delegate to v4.
    if let Some(meta_section) = find_section(footer, section_kind::SERIES_META) {
        return crate::reader::decode_catalog_v4(
            footer,
            slice(object_bytes, label_dict_section)?,
            slice(object_bytes, series_ids_section)?,
            slice(object_bytes, meta_section)?,
            limits,
        );
    }

    // Chunked path: SERIES_IDX + SERIES_META_CHUNKS.
    let idx_section = find_section(footer, section_kind::SERIES_IDX)
        .ok_or(SegmentError::SparseSectionsIncomplete)?;
    let chunks_section = find_section(footer, section_kind::SERIES_META_CHUNKS)
        .ok_or(SegmentError::SparseSectionsIncomplete)?;
    let idx_bytes = decode_section_bytes(idx_section, slice(object_bytes, idx_section)?, limits)?;
    let chunk_bytes =
        decode_section_bytes(chunks_section, slice(object_bytes, chunks_section)?, limits)?;
    let index = parse_series_idx(&idx_bytes)?;

    // Section header: count, schema_count, schemas, run_total. The schemas
    // list is shared across frames (label names); the frames carry schema_ref
    // + value_ord (label values). run_total is cross-checked against the sum
    // over chunks.
    let mut hpos = 0usize;
    let (count, schemas) = parse_schema_list_v2(&chunk_bytes, &mut hpos, &dict_bytes, &dict_index)?;
    check_series_counts_v2(
        series_ids.len() as u64,
        u64::from(count),
        footer.series_count,
    )?;
    let run_total = take_u32_le(&chunk_bytes, &mut hpos)?;

    let ts_len = find_section(footer, section_kind::TS_PAGES)
        .map(|s| s.len)
        .unwrap_or(0);
    let val_len = find_section(footer, section_kind::VAL_PAGES)
        .map(|s| s.len)
        .unwrap_or(0);
    let hist_len = find_section(footer, section_kind::HIST_PAGES)
        .map(|s| s.len)
        .unwrap_or(0);

    let mut resolver = DictResolver::new(&dict_bytes, &dict_index);
    let mut entries: Vec<SeriesEntryV4> = Vec::with_capacity(series_ids.len());
    // The chunk directory's first_index/n dense-chain (0, K, 2K, ...) is
    // already validated structurally by `parse_series_idx`, so this loop
    // does not re-check it.
    let mut run_total_sum: u64 = 0;
    for chunk in index.chunk_directory() {
        let start =
            usize::try_from(chunk.frame_offset).map_err(|_| SegmentError::SectionOutOfBounds)?;
        let stored_len = usize::try_from(chunk.frame_stored_len)
            .map_err(|_| SegmentError::SectionOutOfBounds)?;
        let end = start
            .checked_add(stored_len)
            .ok_or(SegmentError::SectionOutOfBounds)?;
        let stored = chunk_bytes
            .get(start..end)
            .ok_or(SegmentError::SectionOutOfBounds)?;
        if crc32c::crc32c(stored) != chunk.frame_crc32c {
            return Err(SegmentError::ChunkCrcMismatch);
        }
        let cap = usize::try_from(chunk.frame_uncompressed_len)
            .map_err(|_| SegmentError::SectionOutOfBounds)?;
        if chunk.frame_uncompressed_len > limits.max_section_uncompressed_bytes {
            return Err(SegmentError::SectionTooLarge {
                len: chunk.frame_uncompressed_len,
                cap: limits.max_section_uncompressed_bytes,
            });
        }
        let frame_raw = zstd::bulk::decompress(stored, cap)
            .map_err(|e| SegmentError::Decompress(e.to_string()))?;
        let frame = decode_chunk_frame(&frame_raw, footer, ts_len, val_len, hist_len, limits)?;
        if frame.n != chunk.n as usize {
            return Err(SegmentError::BadChunkFrame);
        }
        run_total_sum = run_total_sum
            .checked_add(frame.run_count.iter().map(|&c| u64::from(c)).sum::<u64>())
            .ok_or(SegmentError::FieldOverflow)?;

        // Materialize each series in the frame.
        let mut voff = 0usize;
        let mut roff = 0usize;
        for i in 0..frame.n {
            let abs = entries.len();
            let schema = schemas.get(frame.schema_ref[i] as usize).ok_or(
                SegmentError::SchemaRefOutOfRange(u64::from(frame.schema_ref[i])),
            )?;
            let name_count = schema.name_ords.len();
            let vals = frame
                .value_ord
                .get(voff..voff + name_count)
                .ok_or(SegmentError::BadBlockLen)?;
            voff += name_count;
            let mut label_pairs = Vec::with_capacity(name_count);
            for (&name_ord, &val_ord) in schema.name_ords.iter().zip(vals) {
                label_pairs.push(Label {
                    name: resolver.get(name_ord)?.to_string(),
                    value: resolver.get(val_ord)?.to_string(),
                });
            }
            let label_set = LabelSet::new(label_pairs)?;

            let rc = frame.run_count[i] as usize;
            let runs: Vec<RunEntry> = frame.runs[roff..roff + rc].to_vec();
            roff += rc;

            let mut sample_count: u32 = 0;
            let mut min_ts_ns = i64::MAX;
            let mut max_ts_ns = i64::MIN;
            for r in &runs {
                sample_count = sample_count
                    .checked_add(r.sample_count)
                    .ok_or(SegmentError::FieldOverflow)?;
                min_ts_ns = min_ts_ns.min(r.min_ts_ns);
                max_ts_ns = max_ts_ns.max(r.max_ts_ns);
            }
            let series_id = *series_ids
                .get(abs)
                .ok_or(SegmentError::SectionOutOfBounds)?;
            entries.push(SeriesEntryV4 {
                entry: SeriesEntry {
                    series_id: SeriesId(series_id),
                    labels: label_set,
                    sample_count,
                    min_ts_ns,
                    max_ts_ns,
                    ts_page: (0, 0),
                    val_page: (0, 0),
                    value_kind: frame.value_kind[i],
                    hist_page: (0, 0),
                },
                runs,
            });
        }
        if voff != frame.value_ord.len() {
            return Err(SegmentError::BadBlockLen);
        }
    }

    if run_total_sum != u64::from(run_total) {
        return Err(SegmentError::RunCountSumMismatch {
            run_count_sum: run_total_sum,
            run_total: u64::from(run_total),
        });
    }
    if entries.len() != series_ids.len() {
        return Err(SegmentError::BadChunkFrame);
    }
    Ok(entries)
}

impl SparseIdIndex {
    /// The chunk directory in first-index order, for whole-catalog
    /// reassembly. Internal to the crate's decode path.
    fn chunk_directory(&self) -> impl Iterator<Item = &ChunkDirEntry> {
        self.chunks.iter()
    }
}

fn slice<'a>(object_bytes: &'a [u8], section: &Section) -> Result<&'a [u8], SegmentError> {
    let start = usize::try_from(section.offset).map_err(|_| SegmentError::SectionOutOfBounds)?;
    let len = usize::try_from(section.len).map_err(|_| SegmentError::SectionOutOfBounds)?;
    let end = start
        .checked_add(len)
        .ok_or(SegmentError::SectionOutOfBounds)?;
    object_bytes
        .get(start..end)
        .ok_or(SegmentError::SectionOutOfBounds)
}

// ===========================================================================
// Encode: layer the sparse sections onto a freshly built v4 base object
// ===========================================================================

fn write_err(e: SegmentError) -> WriteError {
    WriteError::SparseAssembly(e.to_string())
}

/// A located section's stored bytes plus the compression metadata to carry
/// forward verbatim.
struct Located<'a> {
    kind: u32,
    comp: i32,
    uncompressed_len: u64,
    stored: &'a [u8],
}

fn locate<'a>(obj: &'a [u8], footer: &Footer, kind: u32) -> Result<Located<'a>, WriteError> {
    let s = find_section(footer, kind).ok_or(WriteError::SparseAssembly(format!(
        "base v4 object missing section kind {kind}"
    )))?;
    let start =
        usize::try_from(s.offset).map_err(|_| write_err(SegmentError::SectionOutOfBounds))?;
    let len = usize::try_from(s.len).map_err(|_| write_err(SegmentError::SectionOutOfBounds))?;
    let stored = obj
        .get(start..start + len)
        .ok_or(write_err(SegmentError::SectionOutOfBounds))?;
    Ok(Located {
        kind,
        comp: s.comp,
        uncompressed_len: s.uncompressed_len,
        stored,
    })
}

/// The v4 SERIES_META split into raw columns, ready to re-chunk without
/// reconstruction (so the chunked form decodes bit-identically to the v4
/// whole form).
struct MetaColumns {
    /// Verbatim section header bytes (count, schema_count, schemas,
    /// run_total), copied into the chunk section header.
    header: Vec<u8>,
    /// Per-series count.
    count: usize,
    /// Per-series schema_ref.
    schema_ref: Vec<u32>,
    /// Series-major value ordinals (flat).
    value_ord: Vec<u64>,
    /// Prefix sums into `value_ord`, length `count + 1`.
    value_ord_offsets: Vec<usize>,
    /// Per-series value_kind byte.
    value_kind: Vec<u8>,
    /// Per-series run count.
    run_count: Vec<u32>,
    /// Prefix sums of `run_count`, length `count + 1`.
    run_offsets: Vec<usize>,
    /// Run-major columns (each `run_total` long).
    created_delta: Vec<u64>,
    epoch: Vec<u64>,
    seq: Vec<u64>,
    sample_count: Vec<u64>,
    min_ts_delta: Vec<u64>,
    ts_span: Vec<u64>,
    ts_gap: Vec<u64>,
    ts_len: Vec<u64>,
    val_gap: Vec<u64>,
    val_len: Vec<u64>,
    hist_gap: Vec<u64>,
    hist_len: Vec<u64>,
    /// Prefix `end` per run for ts/val/hist (length `run_total + 1`).
    ts_end: Vec<u64>,
    val_end: Vec<u64>,
    hist_end: Vec<u64>,
}

fn read_raw_uvarint_block(
    meta: &[u8],
    pos: &mut usize,
    expected: usize,
) -> Result<Vec<u64>, SegmentError> {
    let block = take_block(meta, pos)?;
    let mut bpos = 0usize;
    let mut out = Vec::with_capacity(expected.min(block.len()));
    for _ in 0..expected {
        out.push(read_uvarint(block, &mut bpos)?);
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }
    Ok(out)
}

fn parse_v4_meta_columns(
    meta: &[u8],
    dict_bytes: &[u8],
    dict_index: &[(usize, usize)],
) -> Result<MetaColumns, SegmentError> {
    let mut pos = 0usize;
    let (count_u32, schemas) = parse_schema_list_v2(meta, &mut pos, dict_bytes, dict_index)?;
    let count = count_u32 as usize;
    let schema_count = schemas.len() as u32;
    let run_total = take_u32_le(meta, &mut pos)? as usize;
    let header = meta[..pos].to_vec();

    let schema_ref = parse_schema_ref_block_v2(meta, &mut pos, count_u32, schema_count)?;
    let value_ord = parse_value_ord_block_all_v2(meta, &mut pos, count_u32, &schema_ref, &schemas)?;

    // Per-series value_ord boundaries.
    let mut value_ord_offsets = Vec::with_capacity(count + 1);
    value_ord_offsets.push(0usize);
    let mut acc = 0usize;
    for &sref in &schema_ref {
        acc += schemas[sref as usize].name_ords.len();
        value_ord_offsets.push(acc);
    }

    // value_kind: n bytes.
    let vk_block = take_block(meta, &mut pos)?;
    if vk_block.len() != count {
        return Err(SegmentError::BadBlockLen);
    }
    let value_kind = vk_block.to_vec();

    // run_count: n uvarints.
    let run_count: Vec<u32> = read_raw_uvarint_block(meta, &mut pos, count)?
        .into_iter()
        .map(|v| u32::try_from(v).map_err(|_| SegmentError::FieldOverflow))
        .collect::<Result<_, _>>()?;
    let mut run_offsets = Vec::with_capacity(count + 1);
    run_offsets.push(0usize);
    let mut racc = 0usize;
    for &rc in &run_count {
        racc += rc as usize;
        run_offsets.push(racc);
    }
    if racc != run_total {
        return Err(SegmentError::RunCountSumMismatch {
            run_count_sum: racc as u64,
            run_total: run_total as u64,
        });
    }

    let created_delta = read_raw_uvarint_block(meta, &mut pos, run_total)?;
    let epoch = read_raw_uvarint_block(meta, &mut pos, run_total)?;
    let seq = read_raw_uvarint_block(meta, &mut pos, run_total)?;
    let sample_count = read_raw_uvarint_block(meta, &mut pos, run_total)?;
    let min_ts_delta = read_raw_uvarint_block(meta, &mut pos, run_total)?;
    let ts_span = read_raw_uvarint_block(meta, &mut pos, run_total)?;
    let ts_gap = read_raw_uvarint_block(meta, &mut pos, run_total)?;
    let ts_len = read_raw_uvarint_block(meta, &mut pos, run_total)?;
    let val_gap = read_raw_uvarint_block(meta, &mut pos, run_total)?;
    let val_len = read_raw_uvarint_block(meta, &mut pos, run_total)?;
    let hist_gap = read_raw_uvarint_block(meta, &mut pos, run_total)?;
    let hist_len = read_raw_uvarint_block(meta, &mut pos, run_total)?;
    if pos != meta.len() {
        return Err(SegmentError::TrailingBytes);
    }

    let ts_end = prefix_end(&ts_gap, &ts_len);
    let val_end = prefix_end(&val_gap, &val_len);
    let hist_end = prefix_end(&hist_gap, &hist_len);

    Ok(MetaColumns {
        header,
        count,
        schema_ref,
        value_ord,
        value_ord_offsets,
        value_kind,
        run_count,
        run_offsets,
        created_delta,
        epoch,
        seq,
        sample_count,
        min_ts_delta,
        ts_span,
        ts_gap,
        ts_len,
        val_gap,
        val_len,
        hist_gap,
        hist_len,
        ts_end,
        val_end,
        hist_end,
    })
}

/// Running `end` before each run: `out[r] = Σ_{j<r} (gap[j] + len[j])`,
/// length `gaps.len() + 1`. `out[r0]` is the per-frame base for a chunk whose
/// first run is global run `r0`.
fn prefix_end(gaps: &[u64], lens: &[u64]) -> Vec<u64> {
    let mut out = Vec::with_capacity(gaps.len() + 1);
    out.push(0u64);
    let mut end = 0u64;
    for (&g, &l) in gaps.iter().zip(lens) {
        end = end.saturating_add(g).saturating_add(l);
        out.push(end);
    }
    out
}

fn push_uvarint_block(buf: &mut Vec<u8>, values: &[u64]) {
    let mut block = Vec::new();
    for &v in values {
        write_uvarint(&mut block, v);
    }
    write_uvarint(buf, block.len() as u64);
    buf.extend_from_slice(&block);
}

fn push_u32_uvarint_block(buf: &mut Vec<u8>, values: &[u32]) {
    let mut block = Vec::new();
    for &v in values {
        write_uvarint(&mut block, u64::from(v));
    }
    write_uvarint(buf, block.len() as u64);
    buf.extend_from_slice(&block);
}

fn push_bytes_block(buf: &mut Vec<u8>, bytes: &[u8]) {
    write_uvarint(buf, bytes.len() as u64);
    buf.extend_from_slice(bytes);
}

/// Builds the SERIES_META_CHUNKS section (header + per-chunk zstd frames) and
/// the chunk directory, at stride K = [`V5_STRIDE`].
fn build_meta_chunks(cols: &MetaColumns) -> Result<(Vec<u8>, Vec<ChunkDirEntry>), WriteError> {
    let mut section = cols.header.clone();
    let mut dir = Vec::new();
    let stride = V5_STRIDE as usize;

    let mut s0 = 0usize;
    while s0 < cols.count {
        let s1 = (s0 + stride).min(cols.count);
        let r0 = cols.run_offsets[s0];
        let r1 = cols.run_offsets[s1];
        let n = (s1 - s0) as u32;
        let frame_run_total = (r1 - r0) as u32;

        let mut frame = Vec::new();
        frame.extend_from_slice(&n.to_le_bytes());
        frame.extend_from_slice(&frame_run_total.to_le_bytes());
        write_uvarint(&mut frame, cols.ts_end[r0]);
        write_uvarint(&mut frame, cols.val_end[r0]);
        write_uvarint(&mut frame, cols.hist_end[r0]);

        push_u32_uvarint_block(&mut frame, &cols.schema_ref[s0..s1]);
        let vo0 = cols.value_ord_offsets[s0];
        let vo1 = cols.value_ord_offsets[s1];
        push_uvarint_block(&mut frame, &cols.value_ord[vo0..vo1]);
        push_bytes_block(&mut frame, &cols.value_kind[s0..s1]);
        push_u32_uvarint_block(&mut frame, &cols.run_count[s0..s1]);

        push_uvarint_block(&mut frame, &cols.created_delta[r0..r1]);
        push_uvarint_block(&mut frame, &cols.epoch[r0..r1]);
        push_uvarint_block(&mut frame, &cols.seq[r0..r1]);
        push_uvarint_block(&mut frame, &cols.sample_count[r0..r1]);
        push_uvarint_block(&mut frame, &cols.min_ts_delta[r0..r1]);
        push_uvarint_block(&mut frame, &cols.ts_span[r0..r1]);
        push_uvarint_block(&mut frame, &cols.ts_gap[r0..r1]);
        push_uvarint_block(&mut frame, &cols.ts_len[r0..r1]);
        push_uvarint_block(&mut frame, &cols.val_gap[r0..r1]);
        push_uvarint_block(&mut frame, &cols.val_len[r0..r1]);
        push_uvarint_block(&mut frame, &cols.hist_gap[r0..r1]);
        push_uvarint_block(&mut frame, &cols.hist_len[r0..r1]);

        let stored = zstd::bulk::compress(&frame, ZSTD_LEVEL)
            .map_err(|e| WriteError::Zstd(e.to_string()))?;
        dir.push(ChunkDirEntry {
            frame_offset: section.len() as u64,
            frame_stored_len: stored.len() as u64,
            frame_uncompressed_len: frame.len() as u64,
            first_index: s0 as u32,
            n,
            frame_crc32c: crc32c::crc32c(&stored),
        });
        section.extend_from_slice(&stored);
        s0 = s1;
    }

    Ok((section, dir))
}

/// Builds the SERIES_IDX section body from SERIES_IDS stored bytes (`count`
/// 16-byte ids after a 4-byte count) and the chunk directory. Every Kth id is
/// indexed with the byte window it heads and that window's crc32c.
fn build_series_idx(
    series_ids_stored: &[u8],
    count: u32,
    chunk_dir: &[ChunkDirEntry],
) -> Result<Vec<u8>, WriteError> {
    let stride = V5_STRIDE;
    let count_usize = count as usize;
    let ids_end = 4u64 + u64::from(count) * 16;
    if series_ids_stored.len() as u64 != ids_end {
        return Err(write_err(SegmentError::SectionOutOfBounds));
    }

    let mut entries: Vec<SparseEntry> = Vec::new();
    let mut i: usize = 0;
    while i < count_usize {
        let off = 4 + i * 16;
        let id: [u8; 16] = series_ids_stored
            .get(off..off + 16)
            .ok_or(write_err(SegmentError::SectionOutOfBounds))?
            .try_into()
            .map_err(|_| write_err(SegmentError::SectionOutOfBounds))?;
        let next = (i + stride as usize).min(count_usize);
        let end = if next < count_usize {
            4 + next * 16
        } else {
            ids_end as usize
        };
        let window = series_ids_stored
            .get(off..end)
            .ok_or(write_err(SegmentError::SectionOutOfBounds))?;
        entries.push(SparseEntry {
            id,
            ids_offset: off as u64,
            window_len: (end - off) as u64,
            window_crc32c: crc32c::crc32c(window),
        });
        i = next;
    }

    let mut buf = Vec::with_capacity(16 + entries.len() * 36 + chunk_dir.len() * 36);
    buf.push(SPARSE_INDEX_VERSION);
    buf.push(0); // flags
    buf.extend_from_slice(&0u16.to_le_bytes()); // reserved
    buf.extend_from_slice(&stride.to_le_bytes());
    buf.extend_from_slice(&count.to_le_bytes());
    buf.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for e in &entries {
        buf.extend_from_slice(&e.id);
        buf.extend_from_slice(&e.ids_offset.to_le_bytes());
        buf.extend_from_slice(&e.window_len.to_le_bytes());
        buf.extend_from_slice(&e.window_crc32c.to_le_bytes());
    }
    buf.extend_from_slice(&stride.to_le_bytes());
    buf.extend_from_slice(&(chunk_dir.len() as u32).to_le_bytes());
    for c in chunk_dir {
        buf.extend_from_slice(&c.frame_offset.to_le_bytes());
        buf.extend_from_slice(&c.frame_stored_len.to_le_bytes());
        buf.extend_from_slice(&c.frame_uncompressed_len.to_le_bytes());
        buf.extend_from_slice(&c.first_index.to_le_bytes());
        buf.extend_from_slice(&c.n.to_le_bytes());
        buf.extend_from_slice(&c.frame_crc32c.to_le_bytes());
    }
    Ok(buf)
}

/// Layers the SERIES_IDX + chunked SERIES_META sections onto a freshly built
/// v4 base object, emitting the v5 trailer. Called by
/// `SegmentWriter::write_v5` only when the base carries at least
/// [`crate::format::V5_SPARSE_THRESHOLD`] series. Re-reads the base object
/// (which this same process just wrote and which is therefore well-formed);
/// any decode failure is an internal invariant violation surfaced as
/// [`WriteError::SparseAssembly`].
/// Decodes the Footer protobuf from a trusted, freshly built v4-grammar base
/// object's trailer, bypassing the reader's version gate (the base carries
/// the retired v4 trailer version that `parse_footer` rejects; this is
/// trusted in-memory input from the same process).
fn decode_base_footer(obj: &[u8]) -> Result<Footer, WriteError> {
    let total = obj.len();
    let trailer_len = crate::format::TRAILER_LEN as usize;
    if total < trailer_len {
        return Err(WriteError::SparseAssembly(
            "base object smaller than trailer".into(),
        ));
    }
    let footer_len = u32::from_le_bytes([
        obj[total - 16],
        obj[total - 15],
        obj[total - 14],
        obj[total - 13],
    ]) as usize;
    let footer_end = total - trailer_len;
    let footer_start = footer_end
        .checked_sub(footer_len)
        .ok_or_else(|| WriteError::SparseAssembly("base footer_len out of range".into()))?;
    Footer::decode(&obj[footer_start..footer_end])
        .map_err(|e| WriteError::SparseAssembly(e.to_string()))
}

pub(crate) fn build_sparse_object(base: &WrittenSegment) -> Result<WrittenSegment, WriteError> {
    let obj = base.bytes.as_ref();
    let limits = ReaderLimits::default();
    // The base is this process's own freshly built v4-grammar object, so it
    // carries the retired v4 trailer version that the public reader now
    // rejects (ADR-0027). Decode its footer directly -- trusted in-memory
    // input, not an untrusted stored object.
    let footer = decode_base_footer(obj)?;
    let footer = &footer;

    let label_dict = locate(obj, footer, section_kind::LABEL_DICT)?;
    let series_ids = locate(obj, footer, section_kind::SERIES_IDS)?;
    let series_meta = locate(obj, footer, section_kind::SERIES_META)?;
    let ts_pages = locate(obj, footer, section_kind::TS_PAGES)?;
    let val_pages = find_section(footer, section_kind::VAL_PAGES)
        .map(|_| locate(obj, footer, section_kind::VAL_PAGES))
        .transpose()?;
    let hist_pages = find_section(footer, section_kind::HIST_PAGES)
        .map(|_| locate(obj, footer, section_kind::HIST_PAGES))
        .transpose()?;

    let count = u32::try_from(footer.series_count).map_err(|_| WriteError::TooManySeries)?;

    // Split the v4 SERIES_META into raw columns and re-chunk.
    let dict_raw = decode_section_bytes(
        find_section(footer, section_kind::LABEL_DICT)
            .ok_or(write_err(SegmentError::MissingSection("LABEL_DICT")))?,
        label_dict.stored,
        limits,
    )
    .map_err(write_err)?;
    let dict_index = index_label_dict(&dict_raw).map_err(write_err)?;
    let meta_raw = decode_section_bytes(
        find_section(footer, section_kind::SERIES_META)
            .ok_or(write_err(SegmentError::MissingSection("SERIES_META")))?,
        series_meta.stored,
        limits,
    )
    .map_err(write_err)?;
    let cols = parse_v4_meta_columns(&meta_raw, &dict_raw, &dict_index).map_err(write_err)?;

    let (chunk_section, chunk_dir) = build_meta_chunks(&cols)?;
    let series_idx_bytes = build_series_idx(series_ids.stored, count, &chunk_dir)?;

    // Reassemble: LABEL_DICT, SERIES_IDS, SERIES_META_CHUNKS, SERIES_IDX,
    // TS_PAGES, <pad> VAL_PAGES, HIST_PAGES. VAL_PAGES stays 8-byte aligned so
    // its VAL_RAW_F64 payload alignment (recorded relative to the section
    // start by write_v4) is preserved verbatim.
    let mut object = Vec::with_capacity(obj.len() + series_idx_bytes.len() + 512);
    let mut sections: Vec<Section> = Vec::with_capacity(7);

    push_located(&mut object, &mut sections, &label_dict);
    push_located(&mut object, &mut sections, &series_ids);
    push_raw(
        &mut object,
        &mut sections,
        section_kind::SERIES_META_CHUNKS,
        &chunk_section,
    );
    push_raw(
        &mut object,
        &mut sections,
        section_kind::SERIES_IDX,
        &series_idx_bytes,
    );
    push_located(&mut object, &mut sections, &ts_pages);
    if let Some(vp) = &val_pages {
        align_to_8(&mut object);
        push_located(&mut object, &mut sections, vp);
    }
    if let Some(hp) = &hist_pages {
        push_located(&mut object, &mut sections, hp);
    }

    let mut new_footer: Footer = footer.clone();
    new_footer.sections = sections;

    let footer_bytes = new_footer.encode_to_vec();
    let footer_len = u32::try_from(footer_bytes.len()).map_err(|_| WriteError::FooterTooLarge)?;
    object.extend_from_slice(&footer_bytes);
    let crc = footer_crc(
        &footer_bytes,
        footer_len,
        VERSION_V5,
        SIGNAL_METRICS,
        RESERVED,
    );
    object.extend_from_slice(&footer_len.to_le_bytes());
    object.extend_from_slice(&crc.to_le_bytes());
    object.extend_from_slice(&VERSION_V5.to_le_bytes());
    object.push(SIGNAL_METRICS);
    object.push(RESERVED);
    object.extend_from_slice(&MAGIC);

    let blake3 = *blake3::hash(&object).as_bytes();
    let mut summary = base.summary.clone();
    summary.blake3 = blake3;

    Ok(WrittenSegment {
        bytes: Bytes::from(object),
        summary,
    })
}

fn push_located(object: &mut Vec<u8>, sections: &mut Vec<Section>, s: &Located<'_>) {
    let offset = object.len() as u64;
    object.extend_from_slice(s.stored);
    sections.push(Section {
        kind: s.kind,
        offset,
        len: s.stored.len() as u64,
        crc32c: crc32c::crc32c(s.stored),
        comp: s.comp,
        uncompressed_len: s.uncompressed_len,
    });
}

fn push_raw(object: &mut Vec<u8>, sections: &mut Vec<Section>, kind: u32, bytes: &[u8]) {
    let offset = object.len() as u64;
    object.extend_from_slice(bytes);
    sections.push(Section {
        kind,
        offset,
        len: bytes.len() as u64,
        crc32c: crc32c::crc32c(bytes),
        comp: compression::NONE,
        uncompressed_len: bytes.len() as u64,
    });
}

fn align_to_8(object: &mut Vec<u8>) {
    let pad = (8 - (object.len() % 8)) % 8;
    object.extend(std::iter::repeat_n(0u8, pad));
}
