//! Object writer: span records in, a whole RSPAN object out
//! (docs/span-segment-format.md).
//!
//! [`RspanWriter::finish`] runs the full pipeline: sort records by
//! `(trace_id, start_ts)`, chunk into blocks, build each block and its
//! interval-aware SKIP_IDX entry, then emit the BLOCKS and SKIP_IDX sections,
//! the footer, and the trailer. Identical input yields byte-identical output.

use crate::block::{BlockWriteOut, write_block};
use crate::error::SpanSegError;
use crate::footer::{
    COMP_NONE, COMP_ZSTD, SectionDesc, SpanFooter, kind, write_footer_and_trailer,
};
use crate::record::SpanRecord;
use crate::skip_index::{BlockEntry, SkipIndex};

/// Writer configuration and format constants (docs/span-segment-format.md).
#[derive(Clone, Copy, Debug)]
pub struct RspanConfig {
    pub block_target_records: usize,
    pub block_max_bytes: usize,
    pub zstd_level: i32,
    pub max_uncomp_section: u64,
}

impl Default for RspanConfig {
    fn default() -> Self {
        RspanConfig {
            block_target_records: 8192,
            block_max_bytes: 8 << 20,
            zstd_level: 3,
            max_uncomp_section: 1 << 30,
        }
    }
}

/// The object identity written into the footer.
#[derive(Clone, Copy, Debug)]
pub struct ObjectIdentity {
    pub tenant_hash: [u8; 16],
    pub shard: u32,
    pub writer_id: [u8; 16],
    pub writer_epoch: u64,
    pub writer_seq: u64,
}

/// Buffers span records and emits one RSPAN object.
pub struct RspanWriter {
    cfg: RspanConfig,
    identity: ObjectIdentity,
    records: Vec<SpanRecord>,
}

impl RspanWriter {
    pub fn new(cfg: RspanConfig, identity: ObjectIdentity) -> Self {
        RspanWriter {
            cfg,
            identity,
            records: Vec::new(),
        }
    }

    /// Buffers one span record.
    pub fn push(&mut self, rec: SpanRecord) {
        self.records.push(rec);
    }

    /// Produces the whole object as an L0 flush object: the compaction-identity
    /// fields are stamped at their L0 sentinels (`level = 0`, empty
    /// `input_set_hash`, `part_index = 0`). Empty input is rejected: the flush
    /// layer never writes empty objects.
    pub fn finish(self) -> Result<Vec<u8>, SpanSegError> {
        self.build_object(0, Vec::new(), 0)
    }

    /// Produces the whole object as an L1 compacted part, stamping the caller's
    /// compaction identity into the footer instead of the L0 sentinels. Every
    /// other byte is produced by the same pipeline as [`RspanWriter::finish`], so
    /// identical records plus identical identity yield byte-identical output
    /// regardless of entry point.
    pub fn finish_compacted(
        self,
        level: u32,
        input_set_hash: Vec<u8>,
        part_index: u32,
    ) -> Result<Vec<u8>, SpanSegError> {
        self.build_object(level, input_set_hash, part_index)
    }

    fn build_object(
        mut self,
        level: u32,
        input_set_hash: Vec<u8>,
        part_index: u32,
    ) -> Result<Vec<u8>, SpanSegError> {
        if self.records.is_empty() {
            return Err(SpanSegError::LimitExceeded("empty object".into()));
        }

        // Sort by (trace_id, start_ts). A stable sort keeps input order for
        // records that tie on both keys, so output stays deterministic.
        self.records.sort_by(|a, b| {
            a.trace_id
                .cmp(&b.trace_id)
                .then_with(|| a.start_ts_ns.cmp(&b.start_ts_ns))
        });

        let spans = chunk_blocks(&self.records, &self.cfg);

        let mut blocks_bytes: Vec<u8> = Vec::new();
        let mut entries: Vec<BlockEntry> = Vec::with_capacity(spans.len());
        let mut min_start = i64::MAX;
        let mut max_end = i64::MIN;

        for span in &spans {
            let rows = &self.records[span.clone()];
            let out: BlockWriteOut = write_block(rows, self.cfg.zstd_level)?;
            min_start = min_start.min(out.min_start_ts);
            max_end = max_end.max(out.max_end_ts);
            let block_offset = blocks_bytes.len() as u64;
            blocks_bytes.extend_from_slice(&out.bytes);
            entries.push(BlockEntry {
                block_offset,
                block_len: out.bytes.len() as u64,
                block_crc32c: out.crc32c,
                record_count: out.record_count,
                min_trace_id: out.min_trace_id,
                max_trace_id: out.max_trace_id,
                min_start_ts: out.min_start_ts,
                max_end_ts: out.max_end_ts,
            });
        }

        let skip = SkipIndex::new(entries);

        // Assemble sections in kind order: BLOCKS (raw), SKIP_IDX (zstd).
        let mut object = Vec::new();
        let mut sections: Vec<SectionDesc> = Vec::new();
        push_section(
            &mut object,
            &mut sections,
            kind::BLOCKS,
            Stored::raw(blocks_bytes),
        );
        let skip_stored = compress(&skip.encode(), self.cfg.zstd_level)?;
        push_section(&mut object, &mut sections, kind::SKIP_IDX, skip_stored);

        let record_count = self.records.len() as u64;
        let min_trace_id = self.records[0].trace_id;
        let max_trace_id = self.records[self.records.len() - 1].trace_id;

        let footer = SpanFooter {
            tenant_hash: self.identity.tenant_hash,
            shard: self.identity.shard,
            writer_id: self.identity.writer_id,
            writer_epoch: self.identity.writer_epoch,
            writer_seq: self.identity.writer_seq,
            min_start_ts_ns: min_start,
            max_end_ts_ns: max_end,
            record_count,
            block_count: spans.len() as u64,
            min_trace_id,
            max_trace_id,
            sections,
            level,
            input_set_hash,
            part_index,
        };
        write_footer_and_trailer(&mut object, &footer);
        Ok(object)
    }
}

/// Splits row indices into block spans by record target and an estimated
/// uncompressed byte cap.
fn chunk_blocks(rows: &[SpanRecord], cfg: &RspanConfig) -> Vec<std::ops::Range<usize>> {
    let mut spans = Vec::new();
    let mut start = 0usize;
    let mut bytes = 0usize;
    for (i, row) in rows.iter().enumerate() {
        let est = row_estimate(row);
        let count = i - start;
        if count > 0 && (count >= cfg.block_target_records || bytes + est > cfg.block_max_bytes) {
            spans.push(start..i);
            start = i;
            bytes = 0;
        }
        bytes += est;
    }
    if start < rows.len() {
        spans.push(start..rows.len());
    }
    spans
}

/// A rough uncompressed byte estimate for one span, for the block byte cap.
fn row_estimate(row: &SpanRecord) -> usize {
    let mut est = 48; // fixed-field overhead (ids, timestamps, status)
    est += row.name.len();
    if let Some(m) = &row.status_message {
        est += m.len();
    }
    for (k, v) in &row.attrs {
        est += k.len() + v.len() + 4;
    }
    est
}

/// A section's stored bytes plus its compression descriptor fields.
struct Stored {
    bytes: Vec<u8>,
    comp: u8,
    uncomp_len: u64,
}

impl Stored {
    fn raw(bytes: Vec<u8>) -> Self {
        let uncomp_len = bytes.len() as u64;
        Stored {
            bytes,
            comp: COMP_NONE,
            uncomp_len,
        }
    }
}

/// Whole-section zstd compression (SKIP_IDX).
fn compress(raw: &[u8], level: i32) -> Result<Stored, SpanSegError> {
    let compressed = zstd::bulk::compress(raw, level)
        .map_err(|e| SpanSegError::Corrupted(format!("zstd compress: {e}")))?;
    Ok(Stored {
        bytes: compressed,
        comp: COMP_ZSTD,
        uncomp_len: raw.len() as u64,
    })
}

/// Appends a section's stored bytes and records its descriptor.
fn push_section(object: &mut Vec<u8>, sections: &mut Vec<SectionDesc>, kind: u32, stored: Stored) {
    let offset = object.len() as u64;
    object.extend_from_slice(&stored.bytes);
    sections.push(SectionDesc {
        kind,
        offset,
        len: stored.bytes.len() as u64,
        crc32c: crc32c::crc32c(&stored.bytes),
        comp: stored.comp,
        uncomp_len: stored.uncomp_len,
    });
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::footer::open;
    use crate::record::StatusCode;

    fn identity() -> ObjectIdentity {
        ObjectIdentity {
            tenant_hash: [1u8; 16],
            shard: 0,
            writer_id: [2u8; 16],
            writer_epoch: 1,
            writer_seq: 1,
        }
    }

    fn span(trace: u8, span: u8, start: i64, end: i64) -> SpanRecord {
        SpanRecord {
            trace_id: [trace; 16],
            span_id: [span; 8],
            parent_span_id: None,
            name: format!("op-{span}"),
            start_ts_ns: start,
            end_ts_ns: end,
            status_code: StatusCode::Unset,
            status_message: None,
            attrs: Vec::new(),
        }
    }

    #[test]
    fn empty_input_rejected() {
        let w = RspanWriter::new(RspanConfig::default(), identity());
        assert!(matches!(w.finish(), Err(SpanSegError::LimitExceeded(_))));
    }

    #[test]
    fn end_to_end_and_deterministic() {
        let cfg = RspanConfig {
            block_target_records: 100,
            ..RspanConfig::default()
        };
        let build = || {
            let mut w = RspanWriter::new(cfg, identity());
            for i in 0..1000i64 {
                let trace = (i % 4) as u8;
                let mut r = span(trace, i as u8, i, i + 10);
                r.attrs = vec![("svc".into(), format!("s{trace}"))];
                w.push(r);
            }
            w.finish().expect("finish")
        };
        let a = build();
        let b = build();
        assert_eq!(a, b, "identical input must be byte-identical");

        let footer = open(&a).expect("open");
        assert_eq!(footer.record_count, 1000);
        assert_eq!(footer.block_count, 10);
        // Whole-object bounds.
        assert_eq!(footer.min_start_ts_ns, 0);
        assert_eq!(footer.max_end_ts_ns, 1009);
        assert_eq!(footer.min_trace_id, [0u8; 16]);
        assert_eq!(footer.max_trace_id, [3u8; 16]);
    }

    #[test]
    fn finish_compacted_stamps_identity_and_shares_body_with_finish() {
        let build = |records: &[SpanRecord]| {
            let mut w = RspanWriter::new(RspanConfig::default(), identity());
            for r in records {
                w.push(r.clone());
            }
            w
        };
        let records: Vec<SpanRecord> = (0..50i64)
            .map(|i| span((i % 3) as u8, i as u8, i, i + 5))
            .collect();

        let l0 = build(&records).finish().expect("finish");
        let hash = vec![0xaa, 0xbb, 0xcc];
        let l1 = build(&records)
            .finish_compacted(1, hash.clone(), 2)
            .expect("finish_compacted");

        let f0 = open(&l0).expect("open l0");
        let f1 = open(&l1).expect("open l1");
        assert_eq!(f0.level, 0);
        assert!(f0.input_set_hash.is_empty());
        assert_eq!(f0.part_index, 0);
        assert_eq!(f1.level, 1);
        assert_eq!(f1.input_set_hash, hash);
        assert_eq!(f1.part_index, 2);
        // Sections are byte-identical; only footer identity differs.
        assert_eq!(f0.sections, f1.sections);
        for s in &f0.sections {
            let range = |o: u64, l: u64| (o as usize)..((o + l) as usize);
            assert_eq!(&l0[range(s.offset, s.len)], &l1[range(s.offset, s.len)]);
        }
    }
}
