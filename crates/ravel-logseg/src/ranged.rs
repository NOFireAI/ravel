//! Ranged RLOG reader: read the footer and section directory from
//! a suffix probe, then serve one stream's records from only the blocks that
//! stream occupies, fetched by range.
//!
//! [`crate::reader::RlogReader`] holds the whole object in memory and scans it.
//! [`RlogRangeReader`] instead keeps only the decoded whole-read directories
//! (STREAM_DIR, FIELD_DIR, SKIP_IDX) that the caller fetched by range, plus the
//! absolute offset of the BLOCKS section. It never holds BLOCKS or BLOOM bytes.
//! To decode a stream the caller asks for its [`StreamBlockSpan`] (the absolute
//! byte range covering exactly the blocks that stream can appear in), fetches
//! that one range, and hands the bytes back to [`RlogRangeReader::decode_stream`].
//! Peak resident raw bytes are then the directories plus one stream's blocks,
//! not the whole object -- the RLOG analogue of RSEG's `open_from_suffix` +
//! ranged page fetch, and what lets the compactor bound its raw footprint to
//! one part plus one stream.
//!
//! The format is unchanged: this reader parses exactly the bytes
//! docs/log-segment-format.md already defines. Record decode reuses the same
//! [`crate::reader::rebuild_record`] the whole-object reader uses, so a record
//! decoded through a selective fetch is byte-for-byte the record a whole-object
//! scan would produce (the differential proptests in `ravel-maintain` gate this).

use ravel_types::logstream::LogStreamId;

use crate::block::{DecodedBlock, read_block};
use crate::error::LogSegError;
use crate::field_dir::FieldDir;
use crate::footer::{LogFooter, kind};
use crate::page::DEFAULT_MAX_UNCOMP;
use crate::page_dir::PageDir;
use crate::reader::{
    MAX_BLOCKS, MAX_FIELDS, MAX_STREAMS, column_plans, decode_v4_block, i64_at, rebuild_record,
};
use crate::record::{COL_STREAM_REF, LogRecord};
use crate::skip_index::SkipIndex;
use crate::stream_dir::StreamDir;

/// The absolute byte range covering every block one stream can appear in, and
/// the candidate block indices inside it. The caller fetches exactly
/// `[start, end)` with a ranged GET and passes the bytes to
/// [`RlogRangeReader::decode_stream`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamBlockSpan {
    stream_ref: u32,
    start: u64,
    end: u64,
    /// Candidate level-0 block indices, ascending, all within `[start, end)`.
    blocks: Vec<usize>,
}

impl StreamBlockSpan {
    /// Absolute start offset of the range to fetch.
    pub fn start(&self) -> u64 {
        self.start
    }

    /// Absolute end offset (exclusive) of the range to fetch.
    pub fn end(&self) -> u64 {
        self.end
    }

    /// Number of bytes to fetch (`end - start`).
    pub fn byte_len(&self) -> u64 {
        self.end - self.start
    }

    /// Number of candidate blocks in the span.
    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }
}

/// The absolute byte range of exactly one candidate block for a stream, for the
/// block-at-a-time streaming decode a memory-bounded merge needs.
///
/// [`StreamBlockSpan`] names the whole run of blocks a stream occupies in one
/// GET; a caller that decodes that whole run at once holds decoded records
/// proportional to the stream's size in that input. A k-way streaming merge
/// instead fetches one block at a time via [`RlogRangeReader::stream_blocks`]
/// (ascending, ts-order for the stream), decodes it with
/// [`RlogRangeReader::decode_block`], drains it, and only then fetches the next,
/// so at most one block's raw bytes and one decoded block are resident per
/// input regardless of how many blocks the stream spans.
///
/// Under version 4 a block is not a contiguous byte range: its pages live in
/// its row group's column chunks (ADR-0699 decision 1). A loc then names the
/// whole row group -- one contiguous range, because a merge reads every column
/// -- and covers every candidate block inside it, so the unit a memory-bounded
/// merge holds is one row group rather than one block. That is the read-side
/// mirror of the writer's stated one-row-group working set; `group_target_blocks`
/// bounds both.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamBlockLoc {
    /// The stream's dense ref in this object (the decode filter for the block).
    stream_ref: u32,
    /// The level-0 block indices this loc covers: exactly one under version 3,
    /// the row group's candidate blocks under version 4.
    blocks: Vec<usize>,
    /// Absolute start offset of the block (version 3) or row group (version 4).
    start: u64,
    /// Absolute end offset (exclusive).
    end: u64,
}

impl StreamBlockLoc {
    /// Absolute start offset of the block to fetch.
    pub fn start(&self) -> u64 {
        self.start
    }

    /// Absolute end offset (exclusive) of the block to fetch.
    pub fn end(&self) -> u64 {
        self.end
    }

    /// Number of bytes to fetch (`end - start`).
    pub fn byte_len(&self) -> u64 {
        self.end - self.start
    }

    /// The first level-0 block index this loc covers.
    pub fn block_index(&self) -> usize {
        self.blocks.first().copied().unwrap_or(0)
    }

    /// Every level-0 block index this loc covers: one under version 3, a row
    /// group's candidate blocks under version 4.
    pub fn block_indices(&self) -> &[usize] {
        &self.blocks
    }
}

/// A ranged RLOG reader over one object's directories. Built from the three
/// whole-read sections (fetched by range and decoded via
/// [`crate::decode_section`]); serves per-stream block spans and decode without
/// ever holding the BLOCKS/BLOOM bytes.
#[derive(Clone, Debug)]
pub struct RlogRangeReader {
    stream_dir: StreamDir,
    field_dir: FieldDir,
    skip: SkipIndex,
    blocks_offset: u64,
    /// The decoded PAGE_DIR of a version-4 object, absent for a version-3 one
    /// (ADR-0699 decision 2). Its presence selects the layout, exactly as it
    /// does in [`crate::RlogReader`].
    page_dir: Option<PageDir>,
}

impl RlogRangeReader {
    /// Build from a decoded [`LogFooter`] and the *decompressed* bytes of the
    /// STREAM_DIR, FIELD_DIR, and SKIP_IDX sections. The caller obtains those by
    /// fetching each section's `[offset, offset + len)` by range and running the
    /// stored bytes through [`crate::decode_section`]. FIELD_DIR is decoded (and
    /// validated) even though a compaction merge rebuilds it, so a corrupt input
    /// fails loud here rather than mid-merge.
    pub fn from_sections(
        footer: &LogFooter,
        stream_dir_raw: &[u8],
        field_dir_raw: &[u8],
        skip_idx_raw: &[u8],
    ) -> Result<Self, LogSegError> {
        Self::from_sections_with_page_dir(footer, stream_dir_raw, field_dir_raw, skip_idx_raw, None)
    }

    /// [`Self::from_sections`] plus the version-4 PAGE_DIR section's
    /// decompressed bytes (ADR-0699 decision 2).
    ///
    /// `page_dir_raw` is `Some` exactly when the footer carries a
    /// `kind::PAGE_DIR` entry, which is exactly when the object is version 4.
    /// Passing `None` for a version-4 object is refused rather than tolerated:
    /// without the directory its pages cannot be located at all, and a reader
    /// that fell back to the version-3 block ranges would read another block's
    /// bytes.
    pub fn from_sections_with_page_dir(
        footer: &LogFooter,
        stream_dir_raw: &[u8],
        field_dir_raw: &[u8],
        skip_idx_raw: &[u8],
        page_dir_raw: Option<&[u8]>,
    ) -> Result<Self, LogSegError> {
        let stream_dir = StreamDir::decode(stream_dir_raw, MAX_STREAMS)?;
        let field_dir = FieldDir::decode(field_dir_raw, MAX_FIELDS)?;
        let skip = SkipIndex::decode(skip_idx_raw, MAX_BLOCKS)?;
        let blocks = footer
            .section(kind::BLOCKS)
            .ok_or_else(|| LogSegError::Corrupted("missing BLOCKS section".into()))?;
        let has_page_dir = footer.section(kind::PAGE_DIR).is_some();
        let page_dir = match (has_page_dir, page_dir_raw) {
            (true, Some(raw)) => {
                let dir = PageDir::decode(raw)?;
                dir.validate_extents(blocks.len)?;
                if dir.block_count() != skip.l0.len() as u64 {
                    return Err(LogSegError::Corrupted(format!(
                        "page_dir covers {} blocks but skip index has {}",
                        dir.block_count(),
                        skip.l0.len()
                    )));
                }
                Some(dir)
            }
            (true, None) => {
                return Err(LogSegError::Corrupted(
                    "version-4 object opened without its PAGE_DIR section".into(),
                ));
            }
            (false, _) => None,
        };
        Ok(RlogRangeReader {
            stream_dir,
            field_dir,
            skip,
            blocks_offset: blocks.offset,
            page_dir,
        })
    }

    /// The absolute byte extent of the run of blocks `blocks` occupies.
    ///
    /// Under version 3 that is the union of their own extents. Under version 4
    /// a block's pages are spread across its row group's column chunks, so the
    /// union is taken over the whole row groups the blocks fall in: fetching
    /// that range brings every page of every block in it, which is what a
    /// merge (an all-columns read) wants anyway.
    fn extent_of(&self, blocks: &[usize]) -> Result<(u64, u64), LogSegError> {
        let mut start = u64::MAX;
        let mut end = 0u64;
        for &b in blocks {
            let (rel_start, rel_len) = match &self.page_dir {
                Some(dir) => {
                    let index = u32::try_from(b)
                        .map_err(|_| LogSegError::Corrupted("block index range".into()))?;
                    let (group, _) = dir
                        .locate_block(index)
                        .ok_or_else(|| LogSegError::Corrupted("block not in page_dir".into()))?;
                    group_extent(group)?
                }
                None => {
                    let entry = self.skip.l0.get(b).ok_or_else(|| {
                        LogSegError::Corrupted("skip block index out of range".into())
                    })?;
                    (entry.block_offset, entry.block_len)
                }
            };
            let abs = self
                .blocks_offset
                .checked_add(rel_start)
                .ok_or_else(|| LogSegError::Corrupted("block offset overflow".into()))?;
            let abs_end = abs
                .checked_add(rel_len)
                .ok_or_else(|| LogSegError::Corrupted("block range overflow".into()))?;
            start = start.min(abs);
            end = end.max(abs_end);
        }
        Ok((start, end))
    }

    /// Decodes one block out of a fetched byte range whose first byte sits at
    /// absolute offset `base`, dispatching on the object's layout.
    fn decode_one_block(
        &self,
        block: usize,
        base: u64,
        bytes: &[u8],
        plans: &[crate::block::ColumnPlan],
    ) -> Result<DecodedBlock, LogSegError> {
        let entry = self
            .skip
            .l0
            .get(block)
            .ok_or_else(|| LogSegError::Corrupted("skip block index out of range".into()))?;
        match &self.page_dir {
            Some(dir) => {
                let index = u32::try_from(block)
                    .map_err(|_| LogSegError::Corrupted("block index range".into()))?;
                let mut pages = dir
                    .block_pages(index)
                    .ok_or_else(|| LogSegError::Corrupted("block not in page_dir".into()))?;
                for p in &mut pages {
                    p.offset = self
                        .blocks_offset
                        .checked_add(p.offset)
                        .ok_or_else(|| LogSegError::Corrupted("page offset overflow".into()))?;
                }
                decode_v4_block(
                    bytes,
                    base,
                    entry.record_count as usize,
                    entry.block_crc32c,
                    &pages,
                    plans,
                    None,
                )
            }
            None => {
                let abs = self
                    .blocks_offset
                    .checked_add(entry.block_offset)
                    .ok_or_else(|| LogSegError::Corrupted("block offset overflow".into()))?;
                let rel = abs
                    .checked_sub(base)
                    .ok_or_else(|| LogSegError::Corrupted("block before span start".into()))?;
                let rel = usize::try_from(rel)
                    .map_err(|_| LogSegError::Corrupted("block offset range".into()))?;
                let len = usize::try_from(entry.block_len)
                    .map_err(|_| LogSegError::Corrupted("block len range".into()))?;
                let end = rel
                    .checked_add(len)
                    .ok_or_else(|| LogSegError::Corrupted("block range overflow".into()))?;
                let block_bytes = bytes
                    .get(rel..end)
                    .ok_or_else(|| LogSegError::Corrupted("block outside fetched span".into()))?;
                read_block(block_bytes, entry.block_crc32c, plans, DEFAULT_MAX_UNCOMP)
            }
        }
    }

    /// The object's STREAM_DIR, for the compaction merge's global stream remap.
    pub fn stream_dir(&self) -> &StreamDir {
        &self.stream_dir
    }

    /// The absolute byte range and candidate blocks covering `stream_id`, or
    /// `None` if the object does not carry the stream. A stream's records are
    /// stored in a contiguous run of blocks (records sorted by
    /// `(stream_ref, ts)`), so the span is that run plus at most the boundary
    /// blocks it shares with its neighbours; fetching it stays proportional to
    /// the one stream, never the whole object.
    pub fn stream_block_span(
        &self,
        stream_id: &LogStreamId,
    ) -> Result<Option<StreamBlockSpan>, LogSegError> {
        let Some(stream_ref) = self.stream_dir.stream_ref(stream_id) else {
            return Ok(None);
        };
        let blocks = self
            .skip
            .candidate_blocks(i64::MIN, i64::MAX, Some(&[stream_ref]), &[]);
        if blocks.is_empty() {
            return Ok(None);
        }
        let (start, end) = self.extent_of(&blocks)?;
        Ok(Some(StreamBlockSpan {
            stream_ref,
            start,
            end,
            blocks,
        }))
    }

    /// Decode every record of `span`'s stream from the fetched span bytes.
    /// `span_bytes` MUST be exactly the object's `[span.start, span.end)` range.
    /// Each candidate block is crc-verified and decoded, and only rows whose
    /// `stream_ref` matches the span's stream are rebuilt (a boundary block can
    /// hold rows of the neighbouring stream too). Records come back in stored
    /// `(stream_ref, ts)` order across the span's blocks.
    pub fn decode_stream(
        &self,
        span: &StreamBlockSpan,
        span_bytes: &[u8],
    ) -> Result<Vec<LogRecord>, LogSegError> {
        if span_bytes.len() as u64 != span.byte_len() {
            return Err(LogSegError::Corrupted(
                "span bytes length != span range".into(),
            ));
        }
        let plans = column_plans(&self.field_dir);
        let mut out = Vec::new();
        for &b in &span.blocks {
            let decoded = self.decode_one_block(b, span.start, span_bytes, &plans)?;
            self.push_stream_rows(&decoded, span.stream_ref, &mut out)?;
        }
        Ok(out)
    }

    /// The candidate blocks covering `stream_id`, each as its own absolute byte
    /// range, in ascending block order, or `None` if the object does not carry
    /// the stream.
    ///
    /// This is the block-granular counterpart to [`Self::stream_block_span`]:
    /// where that returns one fused range for a whole-run fetch, this returns
    /// each block separately so a memory-bounded merge can fetch and decode one
    /// block at a time, holding at most one block per input.
    /// Because a stream's records are stored in `(stream_ref, ts)` order, the
    /// ascending block order is ts-ascending for the stream, so decoding the
    /// blocks in this order yields the stream's records already sorted.
    pub fn stream_blocks(
        &self,
        stream_id: &LogStreamId,
    ) -> Result<Option<Vec<StreamBlockLoc>>, LogSegError> {
        let Some(stream_ref) = self.stream_dir.stream_ref(stream_id) else {
            return Ok(None);
        };
        let blocks = self
            .skip
            .candidate_blocks(i64::MIN, i64::MAX, Some(&[stream_ref]), &[]);
        if blocks.is_empty() {
            return Ok(None);
        }
        // Version 3: one loc per block. Version 4: one loc per row group, since
        // a block's pages are spread across its group's column chunks and a
        // merge reads every column (ADR-0699 decision 1).
        let runs: Vec<Vec<usize>> = match &self.page_dir {
            Some(dir) => {
                let mut runs: Vec<Vec<usize>> = Vec::new();
                let mut current: Option<u32> = None;
                for b in blocks {
                    let index = u32::try_from(b)
                        .map_err(|_| LogSegError::Corrupted("block index range".into()))?;
                    let (group, _) = dir
                        .locate_block(index)
                        .ok_or_else(|| LogSegError::Corrupted("block not in page_dir".into()))?;
                    if current == Some(group.first_block) {
                        if let Some(run) = runs.last_mut() {
                            run.push(b);
                        }
                    } else {
                        current = Some(group.first_block);
                        runs.push(vec![b]);
                    }
                }
                runs
            }
            None => blocks.into_iter().map(|b| vec![b]).collect(),
        };
        let mut locs = Vec::with_capacity(runs.len());
        for run in runs {
            let (start, end) = self.extent_of(&run)?;
            locs.push(StreamBlockLoc {
                stream_ref,
                blocks: run,
                start,
                end,
            });
        }
        Ok(Some(locs))
    }

    /// Decode exactly one block's records for its stream, given that one block's
    /// bytes (the object's `[loc.start, loc.end)` range). Only rows whose
    /// `stream_ref` matches `loc`'s stream are rebuilt (a boundary block can
    /// hold rows of a neighbouring stream too), returned in stored (ts) order.
    ///
    /// This is the per-block seam [`Self::decode_stream`] is built on, exposed
    /// so a streaming merge holds one decoded block per input rather than a
    /// whole stream's worth. It reuses the same
    /// [`crate::reader::rebuild_record`] path, so a record decoded one block at
    /// a time is byte-for-byte the record a whole-span or whole-object decode
    /// would produce.
    pub fn decode_block(
        &self,
        loc: &StreamBlockLoc,
        block_bytes: &[u8],
    ) -> Result<Vec<LogRecord>, LogSegError> {
        if block_bytes.len() as u64 != loc.byte_len() {
            return Err(LogSegError::Corrupted(
                "block bytes length != block range".into(),
            ));
        }
        let plans = column_plans(&self.field_dir);
        let mut out = Vec::new();
        for &b in &loc.blocks {
            let decoded = self.decode_one_block(b, loc.start, block_bytes, &plans)?;
            self.push_stream_rows(&decoded, loc.stream_ref, &mut out)?;
        }
        Ok(out)
    }

    /// Appends the rows of `decoded` that belong to `stream_ref`, rebuilt into
    /// records, in stored order.
    fn push_stream_rows(
        &self,
        decoded: &DecodedBlock,
        stream_ref: u32,
        out: &mut Vec<LogRecord>,
    ) -> Result<(), LogSegError> {
        for row in 0..decoded.record_count() {
            let sref = u32::try_from(i64_at(decoded, COL_STREAM_REF, row)?)
                .map_err(|_| LogSegError::Corrupted("stream_ref range".into()))?;
            if sref == stream_ref {
                out.push(rebuild_record(
                    &self.stream_dir,
                    &self.field_dir,
                    decoded,
                    row,
                )?);
            }
        }
        Ok(())
    }
}

/// A row group's byte extent in BLOCKS: from its first column chunk's offset to
/// the end of its last. The chunks of one group are contiguous and in
/// `column_id` order, so this covers every page of every block in it.
fn group_extent(group: &crate::page_dir::GroupEntry) -> Result<(u64, u64), LogSegError> {
    let mut start = u64::MAX;
    let mut end = 0u64;
    for chunk in &group.chunks {
        let (offset, len) = chunk
            .extent()
            .ok_or_else(|| LogSegError::Corrupted("page_dir chunk length overflow".into()))?;
        let chunk_end = offset
            .checked_add(len)
            .ok_or_else(|| LogSegError::Corrupted("page_dir chunk extent overflow".into()))?;
        start = start.min(offset);
        end = end.max(chunk_end);
    }
    if start == u64::MAX {
        return Err(LogSegError::Corrupted("row group has no chunks".into()));
    }
    Ok((start, end - start))
}
