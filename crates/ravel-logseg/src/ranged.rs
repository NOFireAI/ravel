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

use crate::block::read_block;
use crate::error::LogSegError;
use crate::field_dir::FieldDir;
use crate::footer::{LogFooter, kind};
use crate::page::DEFAULT_MAX_UNCOMP;
use crate::reader::{MAX_BLOCKS, MAX_FIELDS, MAX_STREAMS, column_plans, i64_at, rebuild_record};
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
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamBlockLoc {
    /// The stream's dense ref in this object (the decode filter for the block).
    stream_ref: u32,
    /// The level-0 block index this loc names.
    block_index: usize,
    /// Absolute start offset of the block.
    start: u64,
    /// Absolute end offset (exclusive) of the block.
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

    /// The level-0 block index this loc names.
    pub fn block_index(&self) -> usize {
        self.block_index
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
        let stream_dir = StreamDir::decode(stream_dir_raw, MAX_STREAMS)?;
        let field_dir = FieldDir::decode(field_dir_raw, MAX_FIELDS)?;
        let skip = SkipIndex::decode(skip_idx_raw, MAX_BLOCKS)?;
        let blocks = footer
            .section(kind::BLOCKS)
            .ok_or_else(|| LogSegError::Corrupted("missing BLOCKS section".into()))?;
        Ok(RlogRangeReader {
            stream_dir,
            field_dir,
            skip,
            blocks_offset: blocks.offset,
        })
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
            .candidate_blocks(i64::MIN, i64::MAX, Some(&[stream_ref]));
        if blocks.is_empty() {
            return Ok(None);
        }
        let mut start = u64::MAX;
        let mut end = 0u64;
        for &b in &blocks {
            let entry =
                self.skip.l0.get(b).ok_or_else(|| {
                    LogSegError::Corrupted("skip block index out of range".into())
                })?;
            let abs = self
                .blocks_offset
                .checked_add(entry.block_offset)
                .ok_or_else(|| LogSegError::Corrupted("block offset overflow".into()))?;
            let abs_end = abs
                .checked_add(entry.block_len)
                .ok_or_else(|| LogSegError::Corrupted("block range overflow".into()))?;
            start = start.min(abs);
            end = end.max(abs_end);
        }
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
            let entry =
                self.skip.l0.get(b).ok_or_else(|| {
                    LogSegError::Corrupted("skip block index out of range".into())
                })?;
            let abs = self
                .blocks_offset
                .checked_add(entry.block_offset)
                .ok_or_else(|| LogSegError::Corrupted("block offset overflow".into()))?;
            let rel = abs
                .checked_sub(span.start)
                .ok_or_else(|| LogSegError::Corrupted("block before span start".into()))?;
            let rel = usize::try_from(rel)
                .map_err(|_| LogSegError::Corrupted("block offset range".into()))?;
            let len = usize::try_from(entry.block_len)
                .map_err(|_| LogSegError::Corrupted("block len range".into()))?;
            let end = rel
                .checked_add(len)
                .ok_or_else(|| LogSegError::Corrupted("block range overflow".into()))?;
            let block_bytes = span_bytes
                .get(rel..end)
                .ok_or_else(|| LogSegError::Corrupted("block outside fetched span".into()))?;
            let decoded = read_block(block_bytes, entry.block_crc32c, &plans, DEFAULT_MAX_UNCOMP)?;
            for row in 0..decoded.record_count() {
                let sref = u32::try_from(i64_at(&decoded, COL_STREAM_REF, row)?)
                    .map_err(|_| LogSegError::Corrupted("stream_ref range".into()))?;
                if sref == span.stream_ref {
                    out.push(rebuild_record(
                        &self.stream_dir,
                        &self.field_dir,
                        &decoded,
                        row,
                    )?);
                }
            }
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
            .candidate_blocks(i64::MIN, i64::MAX, Some(&[stream_ref]));
        if blocks.is_empty() {
            return Ok(None);
        }
        let mut locs = Vec::with_capacity(blocks.len());
        for b in blocks {
            let entry =
                self.skip.l0.get(b).ok_or_else(|| {
                    LogSegError::Corrupted("skip block index out of range".into())
                })?;
            let start = self
                .blocks_offset
                .checked_add(entry.block_offset)
                .ok_or_else(|| LogSegError::Corrupted("block offset overflow".into()))?;
            let end = start
                .checked_add(entry.block_len)
                .ok_or_else(|| LogSegError::Corrupted("block range overflow".into()))?;
            locs.push(StreamBlockLoc {
                stream_ref,
                block_index: b,
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
        let entry = self
            .skip
            .l0
            .get(loc.block_index)
            .ok_or_else(|| LogSegError::Corrupted("skip block index out of range".into()))?;
        if entry.block_len != loc.byte_len() {
            return Err(LogSegError::Corrupted("block len != skip entry len".into()));
        }
        let plans = column_plans(&self.field_dir);
        let decoded = read_block(block_bytes, entry.block_crc32c, &plans, DEFAULT_MAX_UNCOMP)?;
        let mut out = Vec::new();
        for row in 0..decoded.record_count() {
            let sref = u32::try_from(i64_at(&decoded, COL_STREAM_REF, row)?)
                .map_err(|_| LogSegError::Corrupted("stream_ref range".into()))?;
            if sref == loc.stream_ref {
                out.push(rebuild_record(
                    &self.stream_dir,
                    &self.field_dir,
                    &decoded,
                    row,
                )?);
            }
        }
        Ok(out)
    }
}
