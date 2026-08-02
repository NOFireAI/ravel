//! Ranged RSPAN reader (issue #433): read the footer and SKIP_IDX bytes from a
//! suffix/ranged probe, then serve one trace's records from only the blocks
//! that trace occupies, fetched by range.
//!
//! [`crate::reader::RspanReader`] holds the whole object in memory and scans
//! it. [`RspanRangeReader`] instead keeps only the decoded SKIP_IDX (which the
//! caller fetched by range) plus the absolute offset and length of the BLOCKS
//! section; it never holds BLOCKS bytes until the caller hands back the one
//! range it asked for. To decode a trace the caller asks for its
//! [`TraceBlockSpan`] (the absolute byte range covering exactly the blocks
//! that trace can appear in), fetches that one range, and hands the bytes to
//! [`RspanRangeReader::decode_trace`]. Peak resident raw bytes are then the
//! SKIP_IDX plus one trace's blocks, not the whole object -- the RSPAN
//! analogue of RLOG's post-#275 `RlogRangeReader`
//! (`ravel-logseg/src/ranged.rs`), the model this follows (ADR-0045 decision
//! 6). RSPAN needs no STREAM_DIR/FIELD_DIR equivalent: every field of a
//! `SpanRecord` is stored inline per row with no per-object directory to
//! remap (docs/span-segment-format.md), so this reader is simpler than
//! RLOG's: one section to decode, and no column-plan indirection to rebuild
//! a record.
//!
//! The format is unchanged: this reader parses exactly the bytes
//! docs/span-segment-format.md already defines. Record decode reuses
//! [`crate::block::read_block`] and `DecodedBlock::record`, the same path the
//! whole-object reader uses, so a record decoded through a selective fetch is
//! byte-for-byte the record a whole-object scan would produce.

use crate::block::{DEFAULT_MAX_UNCOMP, read_block};
use crate::error::SpanSegError;
use crate::footer::{SpanFooter, kind};
use crate::reader::MAX_BLOCKS;
use crate::record::SpanRecord;
use crate::skip_index::{BlockEntry, SkipIndex};

/// The absolute byte range covering every block one trace can appear in, and
/// the candidate block indices inside it. The caller fetches exactly
/// `[start, end)` with a ranged GET and passes the bytes to
/// [`RspanRangeReader::decode_trace`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceBlockSpan {
    trace_id: [u8; 16],
    start: u64,
    end: u64,
    /// Candidate block indices, ascending and contiguous, all within
    /// `[start, end)`.
    blocks: Vec<usize>,
}

impl TraceBlockSpan {
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

/// A ranged RSPAN reader over one object's SKIP_IDX. Built from the footer and
/// the *decompressed* SKIP_IDX section bytes (fetched by range and run through
/// [`crate::footer::read_section`]); serves per-trace block spans and decode
/// without ever holding the BLOCKS bytes until the caller fetches exactly the
/// range it named.
#[derive(Clone, Debug)]
pub struct RspanRangeReader {
    skip: SkipIndex,
    blocks_offset: u64,
    blocks_len: u64,
}

impl RspanRangeReader {
    /// Builds from a decoded [`SpanFooter`] and the *decompressed* bytes of
    /// the SKIP_IDX section. The caller obtains `skip_idx_raw` by fetching the
    /// section's `[offset, offset + len)` by range and running the stored
    /// bytes through [`crate::footer::read_section`]. Never constructed from a
    /// whole-object fetch.
    pub fn from_sections(footer: &SpanFooter, skip_idx_raw: &[u8]) -> Result<Self, SpanSegError> {
        let skip = SkipIndex::decode(skip_idx_raw, MAX_BLOCKS)?;
        let blocks = footer
            .section(kind::BLOCKS)
            .ok_or_else(|| SpanSegError::Corrupted("missing BLOCKS section".into()))?;
        Ok(RspanRangeReader {
            skip,
            blocks_offset: blocks.offset,
            blocks_len: blocks.len,
        })
    }

    /// The absolute byte range and candidate blocks covering `trace_id`, or
    /// `None` if the object does not carry the trace. A trace's spans are
    /// stored in a contiguous run of blocks (records sorted by `(trace_id,
    /// start_ts)`), so the span is exactly that run; fetching it stays
    /// proportional to the one trace, never the whole object.
    ///
    /// The skip index's per-block `[min_trace_id, max_trace_id]` range check
    /// is an interval test, not exact membership: it is sound (a block
    /// outside the range cannot hold the trace) but a crafted or corrupt
    /// SKIP_IDX could in principle claim a non-contiguous candidate set,
    /// which would violate the sort invariant that makes a byte-range fetch
    /// safe. That case is impossible from a writer this crate produced, but
    /// is checked explicitly and rejected as `Corrupted` rather than silently
    /// returning a wrong or too-narrow range.
    pub fn trace_block_span(
        &self,
        trace_id: &[u8; 16],
    ) -> Result<Option<TraceBlockSpan>, SpanSegError> {
        let blocks = self
            .skip
            .candidate_blocks(Some(trace_id), i64::MIN, i64::MAX, None, None);
        if blocks.is_empty() {
            return Ok(None);
        }
        for w in blocks.windows(2) {
            if w[1] != w[0] + 1 {
                return Err(SpanSegError::Corrupted(
                    "trace blocks not contiguous".into(),
                ));
            }
        }
        let mut start = u64::MAX;
        let mut end = 0u64;
        for &b in &blocks {
            let entry = self
                .skip
                .blocks
                .get(b)
                .ok_or_else(|| SpanSegError::Corrupted("skip block index out of range".into()))?;
            let (abs, abs_end) = self.block_abs_range(entry)?;
            start = start.min(abs);
            end = end.max(abs_end);
        }
        Ok(Some(TraceBlockSpan {
            trace_id: *trace_id,
            start,
            end,
            blocks,
        }))
    }

    /// Decode every record of `span`'s trace from the fetched span bytes.
    /// `span_bytes` MUST be exactly the object's `[span.start, span.end)`
    /// range. Each candidate block is crc-verified against its skip-index
    /// entry, in block order, before any of its bytes are decoded -- the
    /// per-block crc32c is the only integrity check a partial fetch can rely
    /// on, since a whole-section checksum cannot verify a range shorter than
    /// the section. Only rows whose `trace_id` matches the span's trace are
    /// kept (a boundary block can hold rows of a neighbouring trace too).
    /// Records come back in stored `(trace_id, start_ts)` order across the
    /// span's blocks.
    pub fn decode_trace(
        &self,
        span: &TraceBlockSpan,
        span_bytes: &[u8],
    ) -> Result<Vec<SpanRecord>, SpanSegError> {
        if span_bytes.len() as u64 != span.byte_len() {
            return Err(SpanSegError::Corrupted(
                "span bytes length != span range".into(),
            ));
        }
        let mut out = Vec::new();
        for &b in &span.blocks {
            let entry = self
                .skip
                .blocks
                .get(b)
                .ok_or_else(|| SpanSegError::Corrupted("skip block index out of range".into()))?;
            let (abs, _abs_end) = self.block_abs_range(entry)?;
            let rel = abs
                .checked_sub(span.start)
                .ok_or_else(|| SpanSegError::Corrupted("block before span start".into()))?;
            let rel = usize::try_from(rel)
                .map_err(|_| SpanSegError::Corrupted("block offset range".into()))?;
            let len = usize::try_from(entry.block_len)
                .map_err(|_| SpanSegError::Corrupted("block len range".into()))?;
            let end = rel
                .checked_add(len)
                .ok_or_else(|| SpanSegError::Corrupted("block range overflow".into()))?;
            let block_bytes = span_bytes
                .get(rel..end)
                .ok_or_else(|| SpanSegError::Corrupted("block outside fetched span".into()))?;
            let decoded = read_block(block_bytes, entry.block_crc32c, DEFAULT_MAX_UNCOMP)?;
            for row in 0..decoded.record_count() {
                let rec = decoded.record(row)?;
                if rec.trace_id == span.trace_id {
                    out.push(rec);
                }
            }
        }
        Ok(out)
    }

    /// The absolute `[start, end)` byte range one block occupies, checked
    /// against the BLOCKS section's own bounds captured at construction time
    /// (not merely the whole object): a corrupt `(block_offset, block_len)`
    /// decoded from SKIP_IDX could otherwise land past the BLOCKS section's
    /// own end while still inside the object, and return foreign bytes
    /// (the same class of bug issue #349 fixed in the whole-object reader).
    fn block_abs_range(&self, entry: &BlockEntry) -> Result<(u64, u64), SpanSegError> {
        let rel_end = entry
            .block_offset
            .checked_add(entry.block_len)
            .ok_or_else(|| SpanSegError::Corrupted("block range overflow".into()))?;
        if rel_end > self.blocks_len {
            return Err(SpanSegError::Corrupted(
                "block range exceeds BLOCKS section".into(),
            ));
        }
        let abs = self
            .blocks_offset
            .checked_add(entry.block_offset)
            .ok_or_else(|| SpanSegError::Corrupted("block offset overflow".into()))?;
        let abs_end = abs
            .checked_add(entry.block_len)
            .ok_or_else(|| SpanSegError::Corrupted("block range overflow".into()))?;
        Ok((abs, abs_end))
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::footer::{DEFAULT_MAX_SECTION_UNCOMP, open, read_section};
    use crate::reader::{RspanReader, SpanQuery};
    use crate::record::StatusCode;
    use crate::writer::{ObjectIdentity, RspanConfig, RspanWriter};

    fn identity() -> ObjectIdentity {
        ObjectIdentity {
            tenant_hash: [3u8; 16],
            shard: 2,
            writer_id: [4u8; 16],
            writer_epoch: 1,
            writer_seq: 5,
        }
    }

    fn span(trace: u8, span: u8, start: i64, end: i64) -> SpanRecord {
        SpanRecord {
            trace_id: [trace; 16],
            span_id: [span; 8],
            parent_span_id: if span == 0 { None } else { Some([span - 1; 8]) },
            name: format!("op-{span}"),
            start_ts_ns: start,
            end_ts_ns: end,
            status_code: if span % 3 == 0 {
                StatusCode::Error
            } else {
                StatusCode::Ok
            },
            status_message: if span % 3 == 0 {
                Some(format!("err {span}"))
            } else {
                None
            },
            attrs: vec![("svc".into(), format!("s{trace}"))],
        }
    }

    fn build(cfg: RspanConfig, recs: Vec<SpanRecord>) -> Vec<u8> {
        let mut w = RspanWriter::new(cfg, identity());
        for r in recs {
            w.push(r);
        }
        w.finish().expect("finish")
    }

    /// Opens `obj`'s footer and SKIP_IDX exactly as a caller would: a ranged
    /// fetch of the section bytes, decompressed and crc-checked by
    /// `read_section`, never the whole object handed to the range reader.
    fn open_range_reader(obj: &[u8]) -> (SpanFooter, RspanRangeReader) {
        let footer = open(obj).expect("open");
        let skip_desc = footer.section(kind::SKIP_IDX).expect("skip section");
        let skip_raw =
            read_section(obj, skip_desc, DEFAULT_MAX_SECTION_UNCOMP).expect("read skip section");
        let reader = RspanRangeReader::from_sections(&footer, &skip_raw).expect("range reader");
        (footer, reader)
    }

    /// ACCEPTANCE TEST (issue #433): for several traces in a multi-block
    /// object, the ranged path returns the identical records, in the same
    /// order, as a whole-object scan for the same trace.
    #[test]
    fn ranged_trace_read_matches_whole_object_scan() {
        let cfg = RspanConfig {
            block_target_records: 4,
            ..RspanConfig::default()
        };
        let n_traces = 10u8;
        let spans_per_trace = 7u8;
        let mut recs = Vec::new();
        for t in 0..n_traces {
            for s in 0..spans_per_trace {
                recs.push(span(
                    t,
                    s,
                    i64::from(t) * 1000 + i64::from(s),
                    i64::from(t) * 1000 + i64::from(s) + 3,
                ));
            }
        }
        let obj = build(cfg, recs);
        let whole = RspanReader::new(&obj, &cfg).expect("open whole");
        let (footer, ranged) = open_range_reader(&obj);
        assert!(
            footer.block_count > 1,
            "test needs a multi-block object to be meaningful"
        );

        let mut min_span_bytes = u64::MAX;
        for t in 0..n_traces {
            let trace_id = [t; 16];
            let span_desc = ranged
                .trace_block_span(&trace_id)
                .expect("trace_block_span")
                .unwrap_or_else(|| panic!("trace {t} must be present"));
            min_span_bytes = min_span_bytes.min(span_desc.byte_len());

            let range = span_desc.start() as usize..span_desc.end() as usize;
            let got = ranged
                .decode_trace(&span_desc, &obj[range])
                .expect("decode_trace");

            let (want, _stats) = whole
                .scan(&SpanQuery::trace(trace_id, i64::MIN, i64::MAX))
                .expect("whole scan");
            assert_eq!(got, want, "trace {t} ranged read must match whole scan");
            assert_eq!(got.len(), spans_per_trace as usize);
        }

        // Off-by-one check: the first trace's span must reach block 0 and the
        // last trace's span must reach the final block.
        let first = ranged
            .trace_block_span(&[0u8; 16])
            .expect("trace_block_span")
            .expect("trace 0 present");
        assert_eq!(*first.blocks.first().expect("blocks"), 0);
        let last_trace = n_traces - 1;
        let last = ranged
            .trace_block_span(&[last_trace; 16])
            .expect("trace_block_span")
            .expect("last trace present");
        assert_eq!(
            *last.blocks.last().expect("blocks"),
            footer.block_count as usize - 1
        );

        // Byte reduction: one trace's span must be a small fraction of the
        // whole object, since the object holds ten traces.
        assert!(
            (min_span_bytes as f64) < (obj.len() as f64) * 0.5,
            "a single trace's span ({min_span_bytes} bytes) should be well under \
             half the {}-byte object across {n_traces} traces",
            obj.len()
        );
    }

    #[test]
    fn absent_trace_returns_none() {
        let cfg = RspanConfig {
            block_target_records: 3,
            ..RspanConfig::default()
        };
        let recs: Vec<SpanRecord> = (0..5u8).map(|t| span(t, 0, 0, 1)).collect();
        let obj = build(cfg, recs);
        let (_footer, ranged) = open_range_reader(&obj);
        assert_eq!(
            ranged
                .trace_block_span(&[0xffu8; 16])
                .expect("trace_block_span"),
            None
        );
    }

    /// Degenerate case: every record belongs to one trace, so its span must
    /// cover the entire BLOCKS section.
    #[test]
    fn single_trace_object_spans_whole_blocks_section() {
        let cfg = RspanConfig {
            block_target_records: 5,
            ..RspanConfig::default()
        };
        let recs: Vec<SpanRecord> = (0..37u8).map(|s| span(9, s, i64::from(s), i64::from(s) + 1)).collect();
        let obj = build(cfg, recs.clone());
        let footer = open(&obj).expect("open");
        let blocks_section = footer.section(kind::BLOCKS).expect("blocks section");
        assert!(footer.block_count > 1, "need multiple blocks");

        let (_footer, ranged) = open_range_reader(&obj);
        let span_desc = ranged
            .trace_block_span(&[9u8; 16])
            .expect("trace_block_span")
            .expect("trace present");
        assert_eq!(span_desc.start(), blocks_section.offset);
        assert_eq!(span_desc.end(), blocks_section.offset + blocks_section.len);
        assert_eq!(span_desc.block_count(), footer.block_count as usize);

        let range = span_desc.start() as usize..span_desc.end() as usize;
        let got = ranged
            .decode_trace(&span_desc, &obj[range])
            .expect("decode_trace");
        assert_eq!(got.len(), recs.len());
    }

    #[test]
    fn corrupt_block_crc_is_typed_error() {
        let cfg = RspanConfig {
            block_target_records: 4,
            ..RspanConfig::default()
        };
        let recs: Vec<SpanRecord> = (0..6u8)
            .flat_map(|t| (0..4u8).map(move |s| span(t, s, i64::from(s), i64::from(s) + 1)))
            .collect();
        let mut obj = build(cfg, recs);
        let (_footer, ranged) = open_range_reader(&obj);
        let trace_id = [2u8; 16];
        let span_desc = ranged
            .trace_block_span(&trace_id)
            .expect("trace_block_span")
            .expect("trace present");

        // Flip a byte inside the fetched span's range.
        let flip = span_desc.start() as usize;
        obj[flip] ^= 0xff;
        let range = span_desc.start() as usize..span_desc.end() as usize;
        let err = ranged
            .decode_trace(&span_desc, &obj[range])
            .expect_err("corrupt block must be rejected");
        assert!(matches!(err, SpanSegError::Corrupted(_)));
    }

    #[test]
    fn short_span_bytes_is_typed_error() {
        let cfg = RspanConfig {
            block_target_records: 4,
            ..RspanConfig::default()
        };
        let recs: Vec<SpanRecord> = (0..6u8)
            .flat_map(|t| (0..4u8).map(move |s| span(t, s, i64::from(s), i64::from(s) + 1)))
            .collect();
        let obj = build(cfg, recs);
        let (_footer, ranged) = open_range_reader(&obj);
        let trace_id = [2u8; 16];
        let span_desc = ranged
            .trace_block_span(&trace_id)
            .expect("trace_block_span")
            .expect("trace present");

        let range = span_desc.start() as usize..span_desc.end() as usize;
        let short = &obj[range][..span_desc.byte_len() as usize - 1];
        let err = ranged
            .decode_trace(&span_desc, short)
            .expect_err("short span bytes must be rejected");
        assert!(matches!(err, SpanSegError::Corrupted(_)));
    }
}
