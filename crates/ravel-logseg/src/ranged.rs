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

use crate::block::DecodedBlock;
use crate::error::LogSegError;
use crate::field_dir::FieldDir;
use crate::footer::{LogFooter, kind};
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
/// A block is not a contiguous byte range: its pages live in its row group's
/// column chunks (ADR-0699 decision 1). A loc therefore names the whole row
/// group -- one contiguous range, because a merge reads every column -- and
/// covers every candidate block inside it, so the raw bytes a memory-bounded
/// merge holds are one row group rather than one block. That is the read-side
/// mirror of the writer's stated one-row-group working set;
/// `group_target_blocks` bounds both. The *decoded* residency stays one block:
/// the caller decodes the group's blocks one at a time out of those raw bytes
/// with [`RlogRangeReader::decode_block_in_group`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamBlockLoc {
    /// The stream's dense ref in this object (the decode filter for the block).
    stream_ref: u32,
    /// The level-0 block indices this loc covers: its row group's candidate
    /// blocks.
    blocks: Vec<usize>,
    /// Absolute start offset of the row group.
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

    /// Every level-0 block index this loc covers: its row group's candidate
    /// blocks.
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
    /// The object's decoded PAGE_DIR (ADR-0699 decision 2), through which every
    /// block's pages are located, exactly as in [`crate::RlogReader`].
    page_dir: PageDir,
}

impl RlogRangeReader {
    /// Build from a decoded [`LogFooter`] and the *decompressed* bytes of the
    /// STREAM_DIR, FIELD_DIR, SKIP_IDX, and PAGE_DIR sections. The caller
    /// obtains those by fetching each section's `[offset, offset + len)` by
    /// range and running the stored bytes through [`crate::decode_section`].
    /// FIELD_DIR is decoded (and validated) even though a compaction merge
    /// rebuilds it, so a corrupt input fails loud here rather than mid-merge.
    ///
    /// PAGE_DIR is mandatory (ADR-0699 decision 2): without the directory a
    /// block's pages cannot be located at all, so an object whose footer omits
    /// it, or a caller that omits its bytes, is refused rather than read under
    /// a guessed layout.
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
        if footer.section(kind::PAGE_DIR).is_none() {
            return Err(LogSegError::Corrupted("missing PAGE_DIR section".into()));
        }
        let page_dir = {
            let raw = page_dir_raw.ok_or_else(|| {
                LogSegError::Corrupted("object opened without its PAGE_DIR section".into())
            })?;
            PageDir::decode_validated(raw, blocks.len, skip.l0.len())?
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
    /// A block's pages are spread across its row group's column chunks, so the
    /// union is taken over the whole row groups the blocks fall in: fetching
    /// that range brings every page of every block in it, which is what a
    /// merge (an all-columns read) wants anyway.
    fn extent_of(&self, blocks: &[usize]) -> Result<(u64, u64), LogSegError> {
        let mut start = u64::MAX;
        let mut end = 0u64;
        for &b in blocks {
            let (rel_start, rel_len) = {
                let index = u32::try_from(b)
                    .map_err(|_| LogSegError::Corrupted("block index range".into()))?;
                let (group, _) = self
                    .page_dir
                    .locate_block(index)
                    .ok_or_else(|| LogSegError::Corrupted("block not in page_dir".into()))?;
                group_extent(group)?
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
    /// absolute offset `base`.
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
        let index =
            u32::try_from(block).map_err(|_| LogSegError::Corrupted("block index range".into()))?;
        let mut pages = self
            .page_dir
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
        // One loc per row group, since a block's pages are spread across its
        // group's column chunks and a merge reads every column (ADR-0699
        // decision 1).
        let runs: Vec<Vec<usize>> = {
            let mut runs: Vec<Vec<usize>> = Vec::new();
            let mut current: Option<u32> = None;
            for b in blocks {
                let index = u32::try_from(b)
                    .map_err(|_| LogSegError::Corrupted("block index range".into()))?;
                let (group, _) = self
                    .page_dir
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
    ///
    /// A loc names a whole row group, so this returns every candidate block of
    /// that group at once. A caller that wants the decoded residency of one
    /// block rather than one group decodes the group's blocks individually with
    /// [`Self::decode_block_in_group`].
    pub fn decode_block(
        &self,
        loc: &StreamBlockLoc,
        block_bytes: &[u8],
    ) -> Result<Vec<LogRecord>, LogSegError> {
        let plans = self.check_loc_bytes(loc, block_bytes)?;
        let mut out = Vec::new();
        for &b in &loc.blocks {
            let decoded = self.decode_one_block(b, loc.start, block_bytes, &plans)?;
            self.push_stream_rows(&decoded, loc.stream_ref, &mut out)?;
        }
        Ok(out)
    }

    /// Decode exactly one of `loc`'s blocks out of `loc`'s fetched bytes.
    ///
    /// `block` is a level-0 block index and must be one of
    /// [`StreamBlockLoc::block_indices`]; `group_bytes` must be the object's
    /// `[loc.start, loc.end)` range, the same buffer [`Self::decode_block`]
    /// takes. A loc holds a whole row group, and this is what lets a caller
    /// keep the group's raw bytes resident (they are the smallest contiguous
    /// range that holds any one of its blocks) while decoding one block at a
    /// time and releasing each before the next: the decoded residency is then
    /// one block, not one group.
    ///
    /// Rows are filtered and rebuilt exactly as [`Self::decode_block`] does, so
    /// decoding a loc's blocks one at a time and concatenating the results in
    /// `block_indices` order yields byte-for-byte what [`Self::decode_block`]
    /// returns for the whole loc.
    pub fn decode_block_in_group(
        &self,
        loc: &StreamBlockLoc,
        block: usize,
        group_bytes: &[u8],
    ) -> Result<Vec<LogRecord>, LogSegError> {
        let plans = self.check_loc_bytes(loc, group_bytes)?;
        if !loc.blocks.contains(&block) {
            return Err(LogSegError::Corrupted(
                "block index is not in this loc".into(),
            ));
        }
        let decoded = self.decode_one_block(block, loc.start, group_bytes, &plans)?;
        let mut out = Vec::new();
        self.push_stream_rows(&decoded, loc.stream_ref, &mut out)?;
        Ok(out)
    }

    /// Check that `bytes` is exactly `loc`'s fetched range and return the column
    /// plans a decode of it needs.
    fn check_loc_bytes(
        &self,
        loc: &StreamBlockLoc,
        bytes: &[u8],
    ) -> Result<Vec<crate::block::ColumnPlan>, LogSegError> {
        if bytes.len() as u64 != loc.byte_len() {
            return Err(LogSegError::Corrupted(
                "block bytes length != block range".into(),
            ));
        }
        Ok(column_plans(&self.field_dir))
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

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::footer::open;
    use crate::reader::read_section;
    use crate::record::stream_attrs_bytes;
    use crate::writer::{ObjectIdentity, RlogConfig, RlogWriter};
    use ravel_types::logstream::AttrValue;

    /// 4 records to a block, 2 blocks to a row group: 2 streams x 40 records is
    /// 20 blocks over 10 row groups, so every stream's blocks span several
    /// groups and a group holds more than one block.
    const BLOCK_RECORDS: usize = 4;
    const GROUP_BLOCKS: usize = 2;
    const PER_STREAM: i64 = 40;

    fn sid(n: u8) -> LogStreamId {
        let mut a = [0u8; 16];
        a[0] = n;
        LogStreamId(a)
    }

    fn rec(stream: u8, i: i64) -> LogRecord {
        LogRecord {
            stream_id: sid(stream),
            stream_attrs: stream_attrs_bytes(
                &[(
                    "service.name".to_string(),
                    AttrValue::Str(format!("s{stream}")),
                )],
                "scope",
                "1",
                &[],
            ),
            ts_ns: i,
            observed_ts_ns: i + 1,
            severity_num: (i % 24) as u8,
            severity_text: if i % 2 == 0 { "INFO" } else { "WARN" }.to_string(),
            body: format!("stream {stream} record {i}"),
            trace_id: Some([(i % 251) as u8; 16]),
            span_id: Some([(i % 251) as u8; 8]),
            flags: (i as u32) & 7,
            attrs: vec![
                ("k".to_string(), AttrValue::I64(i % 11)),
                ("t".to_string(), AttrValue::Str(format!("v{}", i % 5))),
            ],
        }
    }

    fn corpus() -> Vec<LogRecord> {
        let mut out = Vec::new();
        for s in 0..2u8 {
            for i in 0..PER_STREAM {
                out.push(rec(s, i));
            }
        }
        out
    }

    fn write_object() -> Vec<u8> {
        let cfg = RlogConfig {
            block_target_records: BLOCK_RECORDS,
            group_target_blocks: GROUP_BLOCKS,
            ..RlogConfig::default()
        };
        let identity = ObjectIdentity {
            tenant_hash: [3u8; 16],
            shard: 0,
            writer_id: [4u8; 16],
            writer_epoch: 1,
            writer_seq: 2,
        };
        let mut w = RlogWriter::new(cfg, identity);
        for r in corpus() {
            w.push(r).expect("push");
        }
        w.finish().expect("finish")
    }

    fn reader_of(object: &[u8]) -> RlogRangeReader {
        let cfg = RlogConfig::default();
        let ftr = open(object).expect("open footer");
        let section = |k: u32| {
            read_section(object, ftr.section(k).expect("section present"), &cfg).expect("section")
        };
        let page_dir = ftr
            .section(kind::PAGE_DIR)
            .map(|d| read_section(object, d, &cfg).expect("PAGE_DIR"));
        RlogRangeReader::from_sections_with_page_dir(
            &ftr,
            &section(kind::STREAM_DIR),
            &section(kind::FIELD_DIR),
            &section(kind::SKIP_IDX),
            page_dir.as_deref(),
        )
        .expect("range reader")
    }

    fn slice(object: &[u8], start: u64, end: u64) -> Vec<u8> {
        object[start as usize..end as usize].to_vec()
    }

    /// Decoding a version-4 object one block at a time out of its row group's
    /// bytes yields exactly the records `decode_stream` yields for the whole
    /// stream: same count, same values, same order. This is the property the
    /// compactor's per-block decode transient rests on.
    #[test]
    fn decode_block_in_group_matches_decode_stream_block_by_block() {
        let object = write_object();
        let reader = reader_of(&object);
        let stream = sid(1);

        let locs = reader
            .stream_blocks(&stream)
            .expect("stream_blocks")
            .expect("stream present");
        // The fixture must really exercise multi-block groups, or decoding one
        // block at a time would be indistinguishable from decoding the loc.
        assert!(
            locs.len() > 1,
            "the stream must span several row groups, got {}",
            locs.len()
        );
        assert!(
            locs.iter().any(|l| l.block_indices().len() > 1),
            "at least one row group must hold more than one of the stream's blocks"
        );

        let mut per_block = Vec::new();
        for loc in &locs {
            let bytes = slice(&object, loc.start(), loc.end());
            for &b in loc.block_indices() {
                let recs = reader
                    .decode_block_in_group(loc, b, &bytes)
                    .expect("decode one block");
                // One block holds at most the block target, so a single call
                // never materializes a whole group's records.
                assert!(
                    recs.len() <= BLOCK_RECORDS,
                    "block {b} decoded {} records, over the {BLOCK_RECORDS} block target",
                    recs.len()
                );
                per_block.extend(recs);
            }
        }

        let span = reader
            .stream_block_span(&stream)
            .expect("span")
            .expect("stream present");
        let whole = reader
            .decode_stream(&span, &slice(&object, span.start(), span.end()))
            .expect("decode_stream");

        let expected: Vec<LogRecord> = (0..PER_STREAM).map(|i| rec(1, i)).collect();
        assert_eq!(per_block.len() as i64, PER_STREAM, "exact record count");
        assert_eq!(whole.len() as i64, PER_STREAM, "exact record count");
        assert_eq!(per_block, whole, "block-at-a-time == whole-stream decode");
        assert_eq!(per_block, expected, "and both equal the written records");
    }

    /// Corrupt input is a typed error, never a panic or a wrong record: a
    /// flipped byte inside the fetched row group fails a checksum, a block index
    /// from another group is refused, and a short buffer is refused.
    #[test]
    fn decode_block_in_group_rejects_corrupt_input() {
        let object = write_object();
        let reader = reader_of(&object);
        let stream = sid(1);
        let locs = reader
            .stream_blocks(&stream)
            .expect("stream_blocks")
            .expect("stream present");
        let loc = &locs[0];
        let block = loc.block_indices()[0];
        let bytes = slice(&object, loc.start(), loc.end());

        let mut flipped = bytes.clone();
        flipped[0] ^= 0xff;
        let err = reader
            .decode_block_in_group(loc, block, &flipped)
            .expect_err("a flipped page byte must not decode");
        assert!(
            matches!(err, LogSegError::Corrupted(_)),
            "expected Corrupted, got {err:?}"
        );

        // A block index that belongs to a different row group.
        let other = locs
            .iter()
            .flat_map(|l| l.block_indices())
            .find(|b| !loc.block_indices().contains(b))
            .copied()
            .expect("another group's block");
        let err = reader
            .decode_block_in_group(loc, other, &bytes)
            .expect_err("a block outside the loc must be refused");
        assert!(
            matches!(err, LogSegError::Corrupted(_)),
            "expected Corrupted, got {err:?}"
        );

        let err = reader
            .decode_block_in_group(loc, block, &bytes[..bytes.len() - 1])
            .expect_err("a short buffer must be refused");
        assert!(
            matches!(err, LogSegError::Corrupted(_)),
            "expected Corrupted, got {err:?}"
        );
    }
}
