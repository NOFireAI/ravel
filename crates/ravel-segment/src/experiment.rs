//! Within-segment selective-read experiment (GitHub issue #167). PROTOTYPE.
//!
//! This module is a measured experiment behind a writer flag, never a format
//! rollout. Nothing here changes default writer output or any frozen contract
//! (docs/segment-format.md): the default writer/reader paths are untouched,
//! and `write_v2_experimental` with an all-false [`ExperimentFlags`] returns
//! the byte-for-byte output of `SegmentWriter::write_v2`
//! (`experiment_flag_off_is_byte_identical_to_write_v2` proves it).
//!
//! Two prototype pieces, measurable separately:
//!
//! 1. Additive piece -- SERIES_IDX (section kind 8): a sparse index holding
//!    every Kth series id plus its byte offset into SERIES_IDS. SERIES_IDS is
//!    raw and uncompressed, so a range-GET binary search works today; this
//!    turns the point-lookup id probe from "fetch the whole 1.6 MB SERIES_IDS
//!    at 100k series" into "fetch a few KB". Unknown section kinds are skipped
//!    by existing readers (docs/segment-format.md forward-compatibility rule),
//!    so an object carrying SERIES_IDX alongside an unchanged v2 catalog is
//!    still a valid v2 object (`series_idx_only_is_readable_by_v2_reader`).
//!
//! 2. Chunked-meta piece -- SERIES_META_CHUNKS (section kind 9): SERIES_META
//!    is whole-section zstd, so any selective read pays all of it. This piece
//!    re-lays SERIES_META's per-series records into per-chunk zstd frames
//!    (frame directory carried in SERIES_IDX), so a lookup decompresses only
//!    the one frame covering its series. This piece is NOT additive (it
//!    removes the whole SERIES_META section), so it stays flag-gated prototype
//!    code; shipping it would require an ADR and a version bump, out of scope
//!    for this experiment.
//!
//! Both kind numbers (8, 9) are defined here, not in the frozen
//! `format::section_kind` module, precisely because they are prototype-only.
//! The trailer `version` stays 2: a SERIES_IDX-only object is a real v2
//! object; a chunked-meta object reuses the v2 trailer but drops SERIES_META,
//! so a stock v2 reader fails closed (`MissingSection`) rather than
//! misreading it -- the correct behavior for a non-additive prototype.

use std::collections::HashMap;

use bytes::Bytes;
use prost::Message;
use ravel_proto::segment::v1::{Footer, Section};

use crate::crc::footer_crc;
use crate::error::{SegmentError, WriteError};
use crate::format::{
    MAGIC, RESERVED, ReaderLimits, SIGNAL_METRICS, VERSION_V2, ZSTD_LEVEL, compression,
    section_kind,
};
use crate::reader::{FooterOutcome, SeriesEntry, decode_catalog_v2, open_from_full, parse_footer};
use crate::varint::{read_uvarint, write_uvarint};
use crate::writer::{IngestBounds, SegmentIdentity, SegmentWriter, SeriesInput, WrittenSegment};

/// Prototype section kind: the sparse series-id index (piece 1). Unknown to a
/// stock v2 reader, which skips it (docs/segment-format.md).
pub const SECTION_KIND_SERIES_IDX: u32 = 8;

/// Prototype section kind: the per-chunk zstd meta frames (piece 2). Present
/// only when [`ExperimentFlags::chunked_meta`] is set, and only alongside the
/// removal of the whole SERIES_META section.
pub const SECTION_KIND_SERIES_META_CHUNKS: u32 = 9;

/// Byte layout version of the prototype structures. Not a persistent contract;
/// bumped freely while iterating.
const IDX_PROTO_VERSION: u8 = 1;

/// Every Kth series id is indexed (piece 1) and every K series form one meta
/// chunk (piece 2). K = 512 balances the two costs: at 100k series a
/// point-lookup id probe fetches the sparse index (196 entries * 24 B ~ 4.7 KB)
/// plus one SERIES_IDS window (K * 16 = 8 KB) and one meta chunk (~a few KB) --
/// all "a few KB" rather than the whole 1.6 MB SERIES_IDS + ~2 MB SERIES_META,
/// while the sparse index adds ~0.15% to the object. Smaller K shrinks the
/// per-probe fetch but grows the index and (for chunks) the frame count, which
/// erodes zstd's cross-record context; larger K does the reverse. 512 keeps
/// every per-probe fetch in the low-KB range at 100k while keeping frame count
/// (196) low enough that chunk compression stays close to the whole-section
/// baseline. See the bench report for the measured object-size delta.
pub const DEFAULT_STRIDE: u32 = 512;

/// Selects which prototype pieces the experimental writer emits. All-false is
/// the default and produces byte-identical output to `write_v2`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ExperimentFlags {
    /// Emit the additive SERIES_IDX section (piece 1).
    pub series_idx: bool,
    /// Replace whole-section SERIES_META with per-chunk zstd frames (piece 2).
    /// Implies `series_idx` (the chunk directory lives in SERIES_IDX).
    pub chunked_meta: bool,
}

impl ExperimentFlags {
    /// Neither piece: the writer delegates straight to `write_v2`.
    pub fn none() -> Self {
        ExperimentFlags {
            series_idx: false,
            chunked_meta: false,
        }
    }

    /// Piece 1 only (additive SERIES_IDX; SERIES_META unchanged).
    pub fn series_idx_only() -> Self {
        ExperimentFlags {
            series_idx: true,
            chunked_meta: false,
        }
    }

    /// Both pieces (SERIES_IDX + chunked SERIES_META).
    pub fn series_idx_and_chunked_meta() -> Self {
        ExperimentFlags {
            series_idx: true,
            chunked_meta: true,
        }
    }
}

/// Errors from the experimental writer. Wraps the normal write error and any
/// reader error hit while re-parsing the base v2 object this writer builds on.
#[derive(Debug, thiserror::Error)]
pub enum ExperimentError {
    #[error(transparent)]
    Write(#[from] WriteError),
    #[error(transparent)]
    Segment(#[from] SegmentError),
    #[error("experiment prototype invariant violated: {0}")]
    Invariant(&'static str),
}

/// Builds a v2 object and, per `flags`, layers the SERIES_IDX and/or
/// chunked-meta prototype pieces on top by re-assembling the sections. With
/// `flags == ExperimentFlags::none()` this is exactly `SegmentWriter::write_v2`
/// (returned directly), so default output is provably unchanged.
///
/// The writer works by post-processing `write_v2`'s output rather than sharing
/// its internals: it never touches the frozen v2 encode path, and the flag-off
/// case cannot diverge because it is a literal delegation.
pub fn write_v2_experimental(
    series: Vec<SeriesInput>,
    identity: SegmentIdentity,
    ingest_bounds: IngestBounds,
    flags: ExperimentFlags,
) -> Result<WrittenSegment, ExperimentError> {
    let base = SegmentWriter::write_v2(series, identity, ingest_bounds)?;
    if !flags.series_idx && !flags.chunked_meta {
        return Ok(base);
    }

    let limits = ReaderLimits::default();
    let loc = open_from_full(&base.bytes, limits)?;
    let footer = loc.footer;
    let obj = base.bytes.as_ref();

    let label_dict = stored_section(obj, &footer, section_kind::LABEL_DICT)?;
    let series_ids = stored_section(obj, &footer, section_kind::SERIES_IDS)?;
    let series_meta = stored_section(obj, &footer, section_kind::SERIES_META)?;
    let ts_pages = stored_section(obj, &footer, section_kind::TS_PAGES)?;
    let val_pages = stored_section(obj, &footer, section_kind::VAL_PAGES)?;

    let series_count = u32::try_from(footer.series_count)
        .map_err(|_| ExperimentError::Invariant("series_count exceeds u32"))?;

    // The chunked-meta path needs each series' schema, value ordinals and page
    // location, so it decodes the base catalog once. The SERIES_IDX-only path
    // needs neither, so it skips the decode entirely.
    let (meta_chunks, chunk_dir) = if flags.chunked_meta {
        let entries = decode_catalog_v2(
            &footer,
            label_dict.stored,
            series_ids.stored,
            series_meta.stored,
            limits,
        )?;
        let dict = parse_dict_reverse(label_dict.decoded()?.as_slice())?;
        build_meta_chunks(&entries, &dict, footer.min_event_ts_ns, DEFAULT_STRIDE)?
    } else {
        (Vec::new(), Vec::new())
    };

    let sparse = build_sparse_index(series_ids.stored, series_count, DEFAULT_STRIDE)?;
    let series_idx_bytes = encode_series_idx(
        &sparse,
        series_count,
        DEFAULT_STRIDE,
        if flags.chunked_meta {
            Some((DEFAULT_STRIDE, &chunk_dir))
        } else {
            None
        },
    );

    // Re-assemble: LABEL_DICT, SERIES_IDS, (SERIES_META | SERIES_META_CHUNKS),
    // SERIES_IDX, TS_PAGES, <pad> VAL_PAGES. VAL_PAGES stays 8-byte aligned so
    // its VAL_RAW_F64 payload alignment (recorded relative to the section start
    // by write_v2) is preserved verbatim.
    let mut object = Vec::with_capacity(obj.len() + series_idx_bytes.len() + 512);
    let mut sections: Vec<Section> = Vec::with_capacity(6);

    push_stored(&mut object, &mut sections, &label_dict);
    push_stored(&mut object, &mut sections, &series_ids);
    if flags.chunked_meta {
        push_raw(
            &mut object,
            &mut sections,
            SECTION_KIND_SERIES_META_CHUNKS,
            &meta_chunks,
        );
    } else {
        push_stored(&mut object, &mut sections, &series_meta);
    }
    push_raw(
        &mut object,
        &mut sections,
        SECTION_KIND_SERIES_IDX,
        &series_idx_bytes,
    );
    push_stored(&mut object, &mut sections, &ts_pages);
    align_to_8(&mut object);
    push_stored(&mut object, &mut sections, &val_pages);

    let mut new_footer = footer;
    new_footer.sections = sections;

    let footer_bytes = new_footer.encode_to_vec();
    let footer_len = u32::try_from(footer_bytes.len())
        .map_err(|_| ExperimentError::Invariant("footer exceeds u32 bytes"))?;
    object.extend_from_slice(&footer_bytes);
    let crc = footer_crc(
        &footer_bytes,
        footer_len,
        VERSION_V2,
        SIGNAL_METRICS,
        RESERVED,
    );
    object.extend_from_slice(&footer_len.to_le_bytes());
    object.extend_from_slice(&crc.to_le_bytes());
    object.extend_from_slice(&VERSION_V2.to_le_bytes());
    object.push(SIGNAL_METRICS);
    object.push(RESERVED);
    object.extend_from_slice(&MAGIC);

    let blake3 = *blake3::hash(&object).as_bytes();
    let mut summary = base.summary;
    summary.blake3 = blake3;

    Ok(WrittenSegment {
        bytes: Bytes::from(object),
        summary,
    })
}

// ----------------------------------------------------------- write helpers

/// A located section: its footer descriptor plus its stored (possibly
/// compressed) bytes sliced out of the object.
struct LocatedSection<'a> {
    kind: u32,
    comp: i32,
    uncompressed_len: u64,
    stored: &'a [u8],
}

impl LocatedSection<'_> {
    /// Decompresses the section's stored bytes (used only for LABEL_DICT in the
    /// chunked path). Small and infrequent, so a fresh allocation is fine.
    fn decoded(&self) -> Result<Vec<u8>, SegmentError> {
        if self.comp == compression::ZSTD {
            let cap = usize::try_from(self.uncompressed_len)
                .map_err(|_| SegmentError::SectionOutOfBounds)?;
            zstd::bulk::decompress(self.stored, cap)
                .map_err(|e| SegmentError::Decompress(e.to_string()))
        } else {
            Ok(self.stored.to_vec())
        }
    }
}

fn stored_section<'a>(
    obj: &'a [u8],
    footer: &Footer,
    kind: u32,
) -> Result<LocatedSection<'a>, ExperimentError> {
    let s = footer
        .sections
        .iter()
        .find(|s| s.kind == kind)
        .ok_or(ExperimentError::Invariant("base section missing"))?;
    let start = usize::try_from(s.offset).map_err(|_| ExperimentError::Invariant("offset"))?;
    let len = usize::try_from(s.len).map_err(|_| ExperimentError::Invariant("len"))?;
    let stored = obj
        .get(start..start + len)
        .ok_or(ExperimentError::Invariant("section out of bounds"))?;
    Ok(LocatedSection {
        kind,
        comp: s.comp,
        uncompressed_len: s.uncompressed_len,
        stored,
    })
}

fn push_stored(object: &mut Vec<u8>, sections: &mut Vec<Section>, s: &LocatedSection<'_>) {
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

/// One sparse index entry: an indexed series id and the byte offset of that id
/// within SERIES_IDS's (uncompressed, comp=NONE) payload.
#[derive(Debug, Clone, Copy)]
struct SparseEntry {
    id: [u8; 16],
    ids_offset: u64,
}

/// Reads every `stride`th id out of the SERIES_IDS stored bytes (`count: u32`
/// then `count` 16-byte ids). Index 0 is always included (0 is a multiple of
/// stride), so the smallest id is always indexed.
fn build_sparse_index(
    series_ids_stored: &[u8],
    series_count: u32,
    stride: u32,
) -> Result<Vec<SparseEntry>, ExperimentError> {
    if stride == 0 {
        return Err(ExperimentError::Invariant("stride is zero"));
    }
    let mut out = Vec::new();
    let mut i: u32 = 0;
    while i < series_count {
        let byte = 4usize + (i as usize) * 16;
        let id: [u8; 16] = series_ids_stored
            .get(byte..byte + 16)
            .ok_or(ExperimentError::Invariant("series id out of bounds"))?
            .try_into()
            .map_err(|_| ExperimentError::Invariant("series id slice"))?;
        out.push(SparseEntry {
            id,
            ids_offset: byte as u64,
        });
        i = i.saturating_add(stride);
    }
    if out.is_empty() && series_count > 0 {
        return Err(ExperimentError::Invariant("empty sparse index"));
    }
    Ok(out)
}

fn encode_series_idx(
    sparse: &[SparseEntry],
    series_count: u32,
    stride: u32,
    chunks: Option<(u32, &[ChunkDirEntry])>,
) -> Vec<u8> {
    let mut buf = Vec::with_capacity(16 + sparse.len() * 24 + 64);
    buf.push(IDX_PROTO_VERSION);
    buf.push(if chunks.is_some() { 1 } else { 0 });
    buf.extend_from_slice(&0u16.to_le_bytes()); // reserved
    buf.extend_from_slice(&stride.to_le_bytes());
    buf.extend_from_slice(&series_count.to_le_bytes());
    buf.extend_from_slice(&(sparse.len() as u32).to_le_bytes());
    for e in sparse {
        buf.extend_from_slice(&e.id);
        buf.extend_from_slice(&e.ids_offset.to_le_bytes());
    }
    if let Some((chunk_stride, dir)) = chunks {
        buf.extend_from_slice(&chunk_stride.to_le_bytes());
        buf.extend_from_slice(&(dir.len() as u32).to_le_bytes());
        for c in dir {
            buf.extend_from_slice(&c.frame_offset.to_le_bytes());
            buf.extend_from_slice(&c.frame_stored_len.to_le_bytes());
            buf.extend_from_slice(&c.frame_uncompressed_len.to_le_bytes());
            buf.extend_from_slice(&c.first_index.to_le_bytes());
            buf.extend_from_slice(&c.n.to_le_bytes());
        }
    }
    buf
}

/// Reverse LABEL_DICT: maps a dictionary string's bytes to its ordinal, for
/// re-deriving the schema/value ordinals the chunk rows carry.
fn parse_dict_reverse(dict_bytes: &[u8]) -> Result<HashMap<Vec<u8>, u64>, SegmentError> {
    let mut pos = 0usize;
    let count = take_u32(dict_bytes, &mut pos)?;
    let mut map = HashMap::with_capacity(count as usize);
    for ord in 0..count {
        let len = read_uvarint(dict_bytes, &mut pos)?;
        let len = usize::try_from(len).map_err(|_| SegmentError::SectionOutOfBounds)?;
        let end = pos.checked_add(len).ok_or(SegmentError::Truncated)?;
        let s = dict_bytes.get(pos..end).ok_or(SegmentError::Truncated)?;
        map.insert(s.to_vec(), u64::from(ord));
        pos = end;
    }
    Ok(map)
}

/// Chunk-directory entry, stored in SERIES_IDX (frame locations are not
/// derivable after zstd, so they are recorded explicitly).
#[derive(Debug, Clone, Copy)]
struct ChunkDirEntry {
    frame_offset: u64,
    frame_stored_len: u64,
    frame_uncompressed_len: u64,
    first_index: u32,
    n: u32,
}

/// Builds the SERIES_META_CHUNKS section (a small schema header followed by
/// concatenated per-chunk zstd frames) and the chunk directory.
///
/// Each frame is itself columnar (same block layout v2's SERIES_META uses), so
/// zstd keeps most of the per-column locality; the only losses versus the
/// whole-section baseline are one zstd frame per chunk instead of one, and two
/// small per-frame base offsets. Page offsets stay delta-encoded (cumulative
/// lengths from a per-frame base), not absolute, so the offset columns cost the
/// same as v2's. Frame grammar (uncompressed):
///
/// ```text
/// n: u32
/// ts_first_off: uvarint    (absolute offset in TS_PAGES of row 0's ts page)
/// val_first_off: uvarint   (absolute offset in VAL_PAGES of row 0's val page)
/// block schema_ref   (block_len, n varints)
/// block value_ord    (block_len, series-major varints)
/// block sample_count (block_len, n varints)
/// block min_ts_delta (block_len, n varints)   (min_ts_ns - footer.min_event_ts_ns)
/// block ts_span      (block_len, n varints)   (max_ts_ns - min_ts_ns)
/// block ts_len       (block_len, n varints)   (ts pages are contiguous; gap is 0)
/// block val_gap      (block_len, n varints)   (alignment pad before each val page)
/// block val_len      (block_len, n varints)
/// ```
///
/// A page-location lookup skips the schema_ref and value_ord blocks by their
/// `block_len` (no schema needed), then reconstructs its row's ts/val offsets:
/// `ts_off_i = ts_first + sum(ts_len[0..i])`, and
/// `val_off_i = val_first + sum_{j=1..=i}(val_len[j-1] + val_gap[j])`.
fn build_meta_chunks(
    entries: &[SeriesEntry],
    dict_reverse: &HashMap<Vec<u8>, u64>,
    min_event_ts_ns: i64,
    chunk_stride: u32,
) -> Result<(Vec<u8>, Vec<ChunkDirEntry>), ExperimentError> {
    // Derive the schema list (distinct name-ordinal sequences) and per-series
    // (schema_ref, value_ords). Labels are already sorted by name bytes.
    let mut schema_index: HashMap<Vec<u64>, u32> = HashMap::new();
    let mut schemas: Vec<Vec<u64>> = Vec::new();
    let mut per_series: Vec<(u32, Vec<u64>)> = Vec::with_capacity(entries.len());
    for e in entries {
        let mut name_ords = Vec::with_capacity(e.labels.len());
        let mut value_ords = Vec::with_capacity(e.labels.len());
        for label in e.labels.iter() {
            let n = *dict_reverse
                .get(label.name.as_bytes())
                .ok_or(ExperimentError::Invariant("label name not in dict"))?;
            let v = *dict_reverse
                .get(label.value.as_bytes())
                .ok_or(ExperimentError::Invariant("label value not in dict"))?;
            name_ords.push(n);
            value_ords.push(v);
        }
        let sref = match schema_index.get(&name_ords) {
            Some(&idx) => idx,
            None => {
                let idx = u32::try_from(schemas.len())
                    .map_err(|_| ExperimentError::Invariant("too many schemas"))?;
                schema_index.insert(name_ords.clone(), idx);
                schemas.push(name_ords);
                idx
            }
        };
        per_series.push((sref, value_ords));
    }

    // Header: count, schema_count, schemas.
    let mut section = Vec::new();
    section.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    section.extend_from_slice(&(schemas.len() as u32).to_le_bytes());
    for schema in &schemas {
        write_uvarint(&mut section, schema.len() as u64);
        for &ord in schema {
            write_uvarint(&mut section, ord);
        }
    }

    let mut dir = Vec::new();
    let mut idx = 0usize;
    let mut first_index: u32 = 0;
    let stride = chunk_stride as usize;
    while idx < entries.len() {
        let end = (idx + stride).min(entries.len());
        let rows = &entries[idx..end];
        let ts_first = rows.first().map_or(0, |e| e.ts_page.0);
        let val_first = rows.first().map_or(0, |e| e.val_page.0);

        // Reconstruct the per-row val gaps: gap_i = off_i - (off_{i-1} +
        // len_{i-1}); gap_0 folds into val_first.
        let mut frame = Vec::new();
        frame.extend_from_slice(&((end - idx) as u32).to_le_bytes());
        write_uvarint(&mut frame, ts_first);
        write_uvarint(&mut frame, val_first);

        let ps = &per_series[idx..end];
        write_block(&mut frame, ps.iter(), |b, (sref, _)| {
            write_uvarint(b, u64::from(*sref));
        });
        write_block(&mut frame, ps.iter(), |b, (_, vals)| {
            for &v in vals {
                write_uvarint(b, v);
            }
        });
        write_block(&mut frame, rows.iter(), |b, e| {
            write_uvarint(b, u64::from(e.sample_count));
        });
        write_block(&mut frame, rows.iter(), |b, e| {
            let d = (i128::from(e.min_ts_ns) - i128::from(min_event_ts_ns)) as u64;
            write_uvarint(b, d);
        });
        write_block(&mut frame, rows.iter(), |b, e| {
            let span = (i128::from(e.max_ts_ns) - i128::from(e.min_ts_ns)) as u64;
            write_uvarint(b, span);
        });
        write_block(&mut frame, rows.iter(), |b, e| {
            write_uvarint(b, e.ts_page.1);
        });
        write_block(&mut frame, rows.iter().enumerate(), |b, (i, e)| {
            let gap = if i == 0 {
                0
            } else {
                let prev = &rows[i - 1];
                e.val_page
                    .0
                    .saturating_sub(prev.val_page.0 + prev.val_page.1)
            };
            write_uvarint(b, gap);
        });
        write_block(&mut frame, rows.iter(), |b, e| {
            write_uvarint(b, e.val_page.1);
        });

        let frame_stored = zstd::bulk::compress(&frame, ZSTD_LEVEL)
            .map_err(|e| ExperimentError::Segment(SegmentError::Decompress(e.to_string())))?;
        dir.push(ChunkDirEntry {
            frame_offset: section.len() as u64,
            frame_stored_len: frame_stored.len() as u64,
            frame_uncompressed_len: frame.len() as u64,
            first_index,
            n: (end - idx) as u32,
        });
        section.extend_from_slice(&frame_stored);
        first_index = first_index.saturating_add((end - idx) as u32);
        idx = end;
    }
    Ok((section, dir))
}

/// Writes one `block_len`-prefixed column block by running `emit` over `items`
/// into a scratch buffer.
fn write_block<T>(
    frame: &mut Vec<u8>,
    items: impl Iterator<Item = T>,
    mut emit: impl FnMut(&mut Vec<u8>, T),
) {
    let mut block = Vec::new();
    for it in items {
        emit(&mut block, it);
    }
    write_uvarint(frame, block.len() as u64);
    frame.extend_from_slice(&block);
}

// ------------------------------------------------------------ read side

/// Parsed SERIES_IDX section: the sparse id index plus, when present, the chunk
/// directory for the chunked-meta variant.
#[derive(Debug, Clone)]
pub struct SparseIdIndex {
    stride: u32,
    series_count: u32,
    entries: Vec<SparseEntry>,
    chunks: Option<ChunkTable>,
}

#[derive(Debug, Clone)]
struct ChunkTable {
    stride: u32,
    dir: Vec<ChunkDirEntry>,
}

/// A byte window of SERIES_IDS to range-GET, plus the absolute series index of
/// its first id, produced by the sparse-index binary search.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IdsWindow {
    /// Byte offset within the SERIES_IDS section's payload.
    pub section_offset: u64,
    /// Byte length (a multiple of 16).
    pub len: u64,
    /// Absolute series index of the first id in the window.
    pub first_index: u64,
}

/// One series' page location and bounds, decoded from either the whole
/// SERIES_META (status quo / SERIES_IDX-only) or a single meta chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PageLoc {
    pub sample_count: u32,
    pub min_ts_ns: i64,
    pub max_ts_ns: i64,
    /// (offset, len) relative to the TS_PAGES section start.
    pub ts_page: (u64, u64),
    /// (offset, len) relative to the VAL_PAGES section start.
    pub val_page: (u64, u64),
}

/// Where one series' meta chunk lives and which row inside it to read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkLocation {
    /// Stored byte range of the frame within the SERIES_META_CHUNKS section.
    pub frame_offset: u64,
    pub frame_stored_len: u64,
    pub frame_uncompressed_len: u64,
    pub row_in_chunk: u64,
}

/// Locates a section in a footer parsed by [`parse_footer_unvalidated`].
pub fn find_experimental_section(footer: &Footer, kind: u32) -> Option<&Section> {
    footer.sections.iter().find(|s| s.kind == kind)
}

/// Parses a footer from full object bytes WITHOUT section validation (the
/// chunked-meta variant has no SERIES_META, so `open_from_full` rejects it).
pub fn parse_footer_unvalidated(bytes: &[u8]) -> Result<Footer, SegmentError> {
    match parse_footer(bytes.len() as u64, bytes)? {
        FooterOutcome::Ready(loc) => Ok(loc.footer),
        FooterOutcome::NeedRange { .. } => Err(SegmentError::Truncated),
    }
}

/// Parses a footer from a suffix of the object WITHOUT section validation.
/// Returns the located footer or, if the suffix does not cover it, the range to
/// re-fetch (mirrors [`crate::open_from_suffix`] but skips validation).
pub fn parse_footer_from_suffix_unvalidated(
    suffix: &[u8],
    total_size: u64,
) -> Result<FooterOutcome, SegmentError> {
    parse_footer(total_size, suffix)
}

/// Decodes a SERIES_IDX section (kind 8).
pub fn parse_series_idx(bytes: &[u8]) -> Result<SparseIdIndex, SegmentError> {
    let mut pos = 0usize;
    let version = *bytes.get(pos).ok_or(SegmentError::Truncated)?;
    pos += 1;
    if version != IDX_PROTO_VERSION {
        return Err(SegmentError::UnsupportedVersion(u16::from(version)));
    }
    let flags = *bytes.get(pos).ok_or(SegmentError::Truncated)?;
    pos += 1;
    pos = pos.checked_add(2).ok_or(SegmentError::Truncated)?; // reserved
    let stride = take_u32(bytes, &mut pos)?;
    let series_count = take_u32(bytes, &mut pos)?;
    let sparse_count = take_u32(bytes, &mut pos)?;
    if stride == 0 {
        return Err(SegmentError::SectionOutOfBounds);
    }
    let mut entries = Vec::with_capacity((sparse_count as usize).min(bytes.len() / 24 + 1));
    let mut prev: Option<[u8; 16]> = None;
    for _ in 0..sparse_count {
        let id = take_id(bytes, &mut pos)?;
        if let Some(p) = prev
            && id <= p
        {
            return Err(SegmentError::SeriesIdsUnsorted);
        }
        prev = Some(id);
        let ids_offset = take_u64(bytes, &mut pos)?;
        entries.push(SparseEntry { id, ids_offset });
    }

    let chunks = if flags & 1 != 0 {
        let chunk_stride = take_u32(bytes, &mut pos)?;
        let chunk_count = take_u32(bytes, &mut pos)?;
        if chunk_stride == 0 {
            return Err(SegmentError::SectionOutOfBounds);
        }
        let mut dir = Vec::with_capacity((chunk_count as usize).min(bytes.len() / 32 + 1));
        for _ in 0..chunk_count {
            let frame_offset = take_u64(bytes, &mut pos)?;
            let frame_stored_len = take_u64(bytes, &mut pos)?;
            let frame_uncompressed_len = take_u64(bytes, &mut pos)?;
            let first_index = take_u32(bytes, &mut pos)?;
            let n = take_u32(bytes, &mut pos)?;
            dir.push(ChunkDirEntry {
                frame_offset,
                frame_stored_len,
                frame_uncompressed_len,
                first_index,
                n,
            });
        }
        Some(ChunkTable {
            stride: chunk_stride,
            dir,
        })
    } else {
        None
    };

    if pos != bytes.len() {
        return Err(SegmentError::TrailingBytes);
    }

    Ok(SparseIdIndex {
        stride,
        series_count,
        entries,
        chunks,
    })
}

impl SparseIdIndex {
    pub fn series_count(&self) -> u32 {
        self.series_count
    }

    pub fn has_chunks(&self) -> bool {
        self.chunks.is_some()
    }

    /// Binary-searches the sparse ids for the window of SERIES_IDS that must
    /// contain `target` if the object contains it at all. Returns `None` only
    /// when `target` is below the smallest id (so it cannot be present).
    pub fn locate(&self, target: &[u8; 16]) -> Option<IdsWindow> {
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
        let start = self.entries[p].ids_offset;
        let end = if p + 1 < self.entries.len() {
            self.entries[p + 1].ids_offset
        } else {
            4 + u64::from(self.series_count) * 16
        };
        Some(IdsWindow {
            section_offset: start,
            len: end - start,
            first_index: u64::from(p as u32) * u64::from(self.stride),
        })
    }

    /// Which chunk covers absolute series `index` (frame byte range + row
    /// offset). `None` when this index carries no chunk table.
    pub fn chunk_for(&self, index: u64) -> Option<ChunkLocation> {
        let chunks = self.chunks.as_ref()?;
        let k = usize::try_from(index / u64::from(chunks.stride)).ok()?;
        let c = chunks.dir.get(k)?;
        if index < u64::from(c.first_index) || index >= u64::from(c.first_index) + u64::from(c.n) {
            return None;
        }
        Some(ChunkLocation {
            frame_offset: c.frame_offset,
            frame_stored_len: c.frame_stored_len,
            frame_uncompressed_len: c.frame_uncompressed_len,
            row_in_chunk: index - u64::from(c.first_index),
        })
    }
}

/// Binary-searches a fetched SERIES_IDS window (`window` is `n*16` raw id
/// bytes, `first_index` the absolute index of its first id) for `target`,
/// returning its absolute index or `None`.
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

/// Decodes the page-location + bounds columns (v2 SERIES_META blocks 3-9) for
/// every series out of the whole SERIES_META section, WITHOUT materializing
/// labels or touching LABEL_DICT. This is exactly what a page-fetching point
/// lookup needs; the status-quo and SERIES_IDX-only measured paths use it so
/// the id lookup is not charged the LABEL_DICT bytes it does not need.
///
/// `series_meta_stored` is the section's stored (zstd) bytes; `ts_section_len`
/// and `val_section_len` bound the reconstructed page ranges.
pub fn decode_page_locations_v2(
    footer: &Footer,
    series_meta_stored: &[u8],
    ts_section_len: u64,
    val_section_len: u64,
    limits: ReaderLimits,
) -> Result<Vec<PageLoc>, SegmentError> {
    let meta_section = footer
        .sections
        .iter()
        .find(|s| s.kind == section_kind::SERIES_META)
        .ok_or(SegmentError::MissingSection("SERIES_META"))?;
    if meta_section.uncompressed_len > limits.max_section_uncompressed_bytes {
        return Err(SegmentError::SectionTooLarge {
            len: meta_section.uncompressed_len,
            cap: limits.max_section_uncompressed_bytes,
        });
    }
    let cap = usize::try_from(meta_section.uncompressed_len)
        .map_err(|_| SegmentError::SectionOutOfBounds)?;
    let meta = if meta_section.comp == compression::ZSTD {
        zstd::bulk::decompress(series_meta_stored, cap)
            .map_err(|e| SegmentError::Decompress(e.to_string()))?
    } else {
        series_meta_stored.to_vec()
    };

    let mut pos = 0usize;
    let count = take_u32(&meta, &mut pos)?;
    let schema_count = take_u32(&meta, &mut pos)?;
    let mut schema_name_counts = Vec::with_capacity((schema_count as usize).min(meta.len()));
    for _ in 0..schema_count {
        let name_count = read_uvarint(&meta, &mut pos)?;
        let name_count =
            usize::try_from(name_count).map_err(|_| SegmentError::SectionOutOfBounds)?;
        for _ in 0..name_count {
            read_uvarint(&meta, &mut pos)?;
        }
        schema_name_counts.push(name_count);
    }

    // Block 1: schema_ref (sizes the value_ord skip in block 2).
    let block = take_meta_block(&meta, &mut pos)?;
    let mut bpos = 0usize;
    let mut schema_ref = Vec::with_capacity((count as usize).min(block.len()));
    for _ in 0..count {
        let v = read_uvarint(block, &mut bpos)?;
        let idx = usize::try_from(v).map_err(|_| SegmentError::SchemaRefOutOfRange(v))?;
        if idx >= schema_name_counts.len() {
            return Err(SegmentError::SchemaRefOutOfRange(v));
        }
        schema_ref.push(idx);
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }

    // Block 2: value_ord (skip; walk exactly name_count varints per series).
    let block = take_meta_block(&meta, &mut pos)?;
    let mut bpos = 0usize;
    for &sref in &schema_ref {
        for _ in 0..schema_name_counts[sref] {
            read_uvarint(block, &mut bpos)?;
        }
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }

    // Blocks 3-9.
    let sample_count = read_varint_block(&meta, &mut pos, count)?;
    let min_delta = read_varint_block(&meta, &mut pos, count)?;
    let ts_span = read_varint_block(&meta, &mut pos, count)?;
    let ts_gap = read_varint_block(&meta, &mut pos, count)?;
    let ts_len = read_varint_block(&meta, &mut pos, count)?;
    let val_gap = read_varint_block(&meta, &mut pos, count)?;
    let val_len = read_varint_block(&meta, &mut pos, count)?;
    if pos != meta.len() {
        return Err(SegmentError::TrailingBytes);
    }

    let ts_page = reconstruct(&ts_gap, &ts_len, ts_section_len)?;
    let val_page = reconstruct(&val_gap, &val_len, val_section_len)?;

    let mut out = Vec::with_capacity(count as usize);
    for i in 0..count as usize {
        out.push(assemble_loc(
            footer.min_event_ts_ns,
            sample_count[i],
            min_delta[i],
            ts_span[i],
            ts_page[i],
            val_page[i],
        )?);
    }
    Ok(out)
}

/// Decodes one series' [`PageLoc`] out of a single decompressed meta chunk
/// frame (chunked variant), reconstructing its page offsets from the frame's
/// per-column blocks. Skips the schema_ref and value_ord blocks by their
/// `block_len`, so no schema is needed.
pub fn decode_chunk_page_location(
    frame_raw: &[u8],
    row_in_chunk: u64,
    footer_min_event_ts_ns: i64,
) -> Result<PageLoc, SegmentError> {
    let mut pos = 0usize;
    let n = take_u32(frame_raw, &mut pos)?;
    let row = usize::try_from(row_in_chunk).map_err(|_| SegmentError::SectionOutOfBounds)?;
    if row >= n as usize {
        return Err(SegmentError::SectionOutOfBounds);
    }
    let ts_first = read_uvarint(frame_raw, &mut pos)?;
    let val_first = read_uvarint(frame_raw, &mut pos)?;

    // Skip schema_ref and value_ord blocks.
    let _schema_ref = take_meta_block(frame_raw, &mut pos)?;
    let _value_ord = take_meta_block(frame_raw, &mut pos)?;

    let sample_count = read_varint_block(frame_raw, &mut pos, n)?;
    let min_delta = read_varint_block(frame_raw, &mut pos, n)?;
    let ts_span = read_varint_block(frame_raw, &mut pos, n)?;
    let ts_len = read_varint_block(frame_raw, &mut pos, n)?;
    let val_gap = read_varint_block(frame_raw, &mut pos, n)?;
    let val_len = read_varint_block(frame_raw, &mut pos, n)?;

    // Reconstruct the target row's absolute page offsets from the per-frame
    // bases and the cumulative lengths.
    let mut ts_off = ts_first;
    for &len in ts_len.iter().take(row) {
        ts_off = ts_off
            .checked_add(len)
            .ok_or(SegmentError::SectionOutOfBounds)?;
    }
    let mut val_off = val_first;
    for j in 1..=row {
        val_off = val_off
            .checked_add(val_len[j - 1])
            .and_then(|v| v.checked_add(val_gap[j]))
            .ok_or(SegmentError::SectionOutOfBounds)?;
    }

    assemble_loc(
        footer_min_event_ts_ns,
        sample_count[row],
        min_delta[row],
        ts_span[row],
        (ts_off, ts_len[row]),
        (val_off, val_len[row]),
    )
}

fn assemble_loc(
    footer_min_event_ts_ns: i64,
    sample_count: u64,
    min_delta: u64,
    ts_span: u64,
    ts_page: (u64, u64),
    val_page: (u64, u64),
) -> Result<PageLoc, SegmentError> {
    let sc = u32::try_from(sample_count).map_err(|_| SegmentError::FieldOverflow)?;
    let min_ts = i64::try_from(i128::from(footer_min_event_ts_ns) + i128::from(min_delta))
        .map_err(|_| SegmentError::TimestampBoundsOverflow)?;
    let max_ts = i64::try_from(i128::from(min_ts) + i128::from(ts_span))
        .map_err(|_| SegmentError::TimestampBoundsOverflow)?;
    Ok(PageLoc {
        sample_count: sc,
        min_ts_ns: min_ts,
        max_ts_ns: max_ts,
        ts_page,
        val_page,
    })
}

/// Decompresses one meta chunk frame (`stored` is `frame_stored_len` bytes).
pub fn decompress_chunk_frame(
    stored: &[u8],
    uncompressed_len: u64,
    limits: ReaderLimits,
) -> Result<Vec<u8>, SegmentError> {
    if uncompressed_len > limits.max_section_uncompressed_bytes {
        return Err(SegmentError::SectionTooLarge {
            len: uncompressed_len,
            cap: limits.max_section_uncompressed_bytes,
        });
    }
    let cap = usize::try_from(uncompressed_len).map_err(|_| SegmentError::SectionOutOfBounds)?;
    zstd::bulk::decompress(stored, cap).map_err(|e| SegmentError::Decompress(e.to_string()))
}

fn take_meta_block<'a>(bytes: &'a [u8], pos: &mut usize) -> Result<&'a [u8], SegmentError> {
    let block_len = read_uvarint(bytes, pos)?;
    let block_len = usize::try_from(block_len).map_err(|_| SegmentError::SectionOutOfBounds)?;
    let end = pos.checked_add(block_len).ok_or(SegmentError::Truncated)?;
    let slice = bytes.get(*pos..end).ok_or(SegmentError::Truncated)?;
    *pos = end;
    Ok(slice)
}

fn read_varint_block(bytes: &[u8], pos: &mut usize, count: u32) -> Result<Vec<u64>, SegmentError> {
    let block = take_meta_block(bytes, pos)?;
    let mut bpos = 0usize;
    let mut out = Vec::with_capacity((count as usize).min(block.len()));
    for _ in 0..count {
        out.push(read_uvarint(block, &mut bpos)?);
    }
    if bpos != block.len() {
        return Err(SegmentError::BadBlockLen);
    }
    Ok(out)
}

fn reconstruct(
    gaps: &[u64],
    lens: &[u64],
    section_len: u64,
) -> Result<Vec<(u64, u64)>, SegmentError> {
    let mut out = Vec::with_capacity(gaps.len());
    let mut end = 0u64;
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

#[cfg(test)]
#[path = "experiment_tests.rs"]
mod experiment_tests;
