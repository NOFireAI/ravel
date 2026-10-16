//! Pruned scan reader (docs/span-segment-format.md "Pruning soundness").
//!
//! [`RspanReader::scan`] prunes blocks through the interval-aware skip index,
//! then reads, verifies, and re-evaluates the survivors exactly. Pruning is
//! proof-based: a block is dropped only when the skip index proves its
//! trace_id range or time interval disjoint from the query. A corrupt SKIP_IDX
//! is a loud `Corrupted` error rather than a degrade, because its bytes carry
//! the block framing and per-block checksums: without it no block can be
//! located or verified.

use ravel_codec::bloom_section::BloomSection;

use crate::block::{DEFAULT_MAX_UNCOMP, read_block};
use crate::error::SpanSegError;
use crate::footer::{SpanFooter, kind, open, read_section};
use crate::key_index::{self, KeyIdxSummary, TraceKeyProbe};
use crate::record::SpanRecord;
use crate::skip_index::SkipIndex;
use crate::writer::RspanConfig;

/// Upper bound on the number of blocks a decoded skip index may claim.
pub const MAX_BLOCKS: u64 = 1 << 24;

/// A scan predicate over one RSPAN object. A window `[ts_min, ts_max]` is always
/// present (a bare time-range query); `trace_id`, when set, additionally
/// restricts to spans of that trace (ADR-0041: trace_id is the primary key).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SpanQuery {
    pub trace_id: Option<[u8; 16]>,
    pub ts_min: i64,
    pub ts_max: i64,
}

impl SpanQuery {
    /// A bare time-range query over `[min_ns, max_ns]`, matching every trace.
    pub fn ts_range(min_ns: i64, max_ns: i64) -> Self {
        SpanQuery {
            trace_id: None,
            ts_min: min_ns,
            ts_max: max_ns,
        }
    }

    /// A query for one trace within a time window.
    pub fn trace(trace_id: [u8; 16], min_ns: i64, max_ns: i64) -> Self {
        SpanQuery {
            trace_id: Some(trace_id),
            ts_min: min_ns,
            ts_max: max_ns,
        }
    }
}

/// Counters describing how much a scan pruned (docs/span-segment-format.md).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ScanStats {
    pub blocks_total: u32,
    pub blocks_after_skip: u32,
    pub blocks_scanned: u32,
}

/// An opened RSPAN object ready to scan.
pub struct RspanReader<'a> {
    bytes: &'a [u8],
    skip: SkipIndex,
    blocks_offset: u64,
    blocks_len: u64,
    /// Decompressed BLOOM section bytes (v3, ADR-0054). Held so callers can
    /// parse a [`BloomSection`] for bloom-backed block pruning without
    /// re-reading the object. The whole-section crc is verified at open time
    /// by `read_section`; per-entry crcs are verified on probe.
    bloom_raw: Vec<u8>,
    /// The KEY_IDX section's stored bytes (v5, ADR-2707), crc-verified at open,
    /// and its footer `prefix_len`.
    key_idx: &'a [u8],
    key_idx_prefix_len: u32,
    max_uncomp: u64,
}

impl<'a> RspanReader<'a> {
    /// Opens and validates the object, decoding the skip index, reading the
    /// mandatory BLOOM section, and checking the mandatory KEY_IDX section's
    /// crc and RSPAN parameters (one field, `trace_id`, key type `Id16`).
    pub fn new(bytes: &'a [u8], cfg: &RspanConfig) -> Result<Self, SpanSegError> {
        let footer = open(bytes)?;
        let key_idx_desc = section(&footer, kind::KEY_IDX)?;
        let key_idx = section_bytes(bytes, key_idx_desc)?;
        if crc32c::crc32c(key_idx) != key_idx_desc.crc32c {
            return Err(SpanSegError::Corrupted(
                "KEY_IDX section crc mismatch".into(),
            ));
        }
        key_index::check_section(key_idx, key_idx_desc.prefix_len)?;
        let key_idx_prefix_len = key_idx_desc.prefix_len;
        let skip_desc = section(&footer, kind::SKIP_IDX)?;
        let skip_raw = read_section(bytes, skip_desc, cfg.max_uncomp_section)?;
        let skip = SkipIndex::decode(&skip_raw, MAX_BLOCKS)?;
        let bloom_desc = section(&footer, kind::BLOOM)?;
        let bloom_raw = read_section(bytes, bloom_desc, cfg.max_uncomp_section)?;
        // Structural validation at open time, not deferred to the first
        // `bloom()` call: BLOOM is mandatory (docs/span-segment-format.md
        // "Pruning soundness"), so a truncated container or an entry count
        // that disagrees with the block count must fail here, the same as a
        // corrupt SKIP_IDX does above, rather than opening clean and only
        // surfacing on a query that happens to probe the bloom.
        let bloom_entry_count = BloomSection::parse(&bloom_raw)?.len();
        if bloom_entry_count != skip.blocks.len() {
            return Err(SpanSegError::Corrupted(format!(
                "bloom entry count {bloom_entry_count} disagrees with block count {}",
                skip.blocks.len()
            )));
        }
        let blocks = *section(&footer, kind::BLOCKS)?;
        Ok(RspanReader {
            bytes,
            skip,
            blocks_offset: blocks.offset,
            blocks_len: blocks.len,
            bloom_raw,
            key_idx,
            key_idx_prefix_len,
            max_uncomp: DEFAULT_MAX_UNCOMP,
        })
    }

    /// The decoded skip index.
    pub fn skip_index(&self) -> &SkipIndex {
        &self.skip
    }

    /// The SKIP_IDX block ordinals the KEY_IDX section names for `trace_id`,
    /// ascending: every block holding a trace whose first 8 bytes equal
    /// `trace_id`'s. Reads `[0, prefix_len)` of the section and then the one
    /// bucket the trace mixes to. An ordinal at or past the block count
    /// anywhere in that bucket is `Corrupted`.
    pub fn probe_trace(&self, trace_id: &[u8; 16]) -> Result<TraceKeyProbe, SpanSegError> {
        let section = self.key_idx;
        key_index::probe(
            section.len() as u64,
            self.key_idx_prefix_len,
            self.skip.blocks.len(),
            trace_id,
            |range| {
                usize::try_from(range.start)
                    .ok()
                    .zip(usize::try_from(range.end).ok())
                    .and_then(|(start, end)| section.get(start..end))
                    .ok_or_else(|| SpanSegError::Corrupted("KEY_IDX read past the section".into()))
            },
        )
    }

    /// Verifies every KEY_IDX bucket frame and summarizes the section (field,
    /// key type, entry count, entries per bucket), for inspection.
    pub fn key_idx_summary(&self) -> Result<KeyIdxSummary, SpanSegError> {
        key_index::summarize(self.key_idx, self.key_idx_prefix_len)
    }

    /// Parses the object's BLOOM section (v3, ADR-0054). Entry `i` covers block
    /// `i`. Pass the returned view, with `service_name`/`name` predicates, to
    /// [`SkipIndex::candidate_blocks_with_bloom`]. A malformed container is a
    /// typed `Corrupted` error.
    pub fn bloom(&self) -> Result<BloomSection<'_>, SpanSegError> {
        Ok(BloomSection::parse(&self.bloom_raw)?)
    }

    /// Scans the object for spans matching `query`.
    pub fn scan(&self, query: &SpanQuery) -> Result<(Vec<SpanRecord>, ScanStats), SpanSegError> {
        let mut stats = ScanStats {
            blocks_total: self.skip.blocks.len() as u32,
            ..ScanStats::default()
        };
        if query.ts_min > query.ts_max {
            return Ok((Vec::new(), stats));
        }

        let candidates = self.skip.candidate_blocks(
            query.trace_id.as_ref(),
            query.ts_min,
            query.ts_max,
            None,
            None,
        );
        stats.blocks_after_skip = candidates.len() as u32;

        let mut out = Vec::new();
        for &b in &candidates {
            let entry = &self.skip.blocks[b];
            let block_bytes = self.block_bytes(entry.block_offset, entry.block_len)?;
            let decoded = read_block(block_bytes, entry.block_crc32c, self.max_uncomp)?;
            for row in 0..decoded.record_count() {
                let rec = decoded.record(row)?;
                if matches(&rec, query) {
                    out.push(rec);
                }
            }
        }
        stats.blocks_scanned = candidates.len() as u32;
        Ok((out, stats))
    }

    /// Slice of one block's stored bytes (offset is relative to BLOCKS).
    fn block_bytes(&self, block_offset: u64, block_len: u64) -> Result<&'a [u8], SpanSegError> {
        // The read must stay within the BLOCKS section itself, not merely within
        // the whole object. A corrupt (block_offset, block_len) decoded from
        // SKIP_IDX could otherwise land `end` past the BLOCKS section's own
        // boundary while still inside the file (into SKIP_IDX or the footer),
        // and return foreign bytes. Checked against the section
        // length captured at open time: `block_offset + block_len <=
        // blocks_len`.
        let rel_end = block_offset
            .checked_add(block_len)
            .ok_or_else(|| SpanSegError::Corrupted("block range overflow".into()))?;
        if rel_end > self.blocks_len {
            return Err(SpanSegError::Corrupted(
                "block range exceeds BLOCKS section".into(),
            ));
        }
        let abs = self
            .blocks_offset
            .checked_add(block_offset)
            .ok_or_else(|| SpanSegError::Corrupted("block offset overflow".into()))?;
        let start = usize::try_from(abs)
            .map_err(|_| SpanSegError::Corrupted("block offset range".into()))?;
        let len = usize::try_from(block_len)
            .map_err(|_| SpanSegError::Corrupted("block len range".into()))?;
        let end = start
            .checked_add(len)
            .ok_or_else(|| SpanSegError::Corrupted("block range overflow".into()))?;
        self.bytes
            .get(start..end)
            .ok_or_else(|| SpanSegError::Corrupted("block out of bounds".into()))
    }
}

/// Exact predicate evaluation on a rebuilt record: the span's `[start, end]`
/// interval must overlap the query window, and (when set) its trace_id must
/// match. Overlap is `start <= ts_max && end >= ts_min`.
fn matches(rec: &SpanRecord, query: &SpanQuery) -> bool {
    if let Some(tid) = &query.trace_id
        && rec.trace_id != *tid
    {
        return false;
    }
    rec.start_ts_ns <= query.ts_max && rec.end_ts_ns >= query.ts_min
}

fn section(footer: &SpanFooter, k: u32) -> Result<&crate::footer::SectionDesc, SpanSegError> {
    footer
        .section(k)
        .ok_or_else(|| SpanSegError::Corrupted(format!("missing section {k}")))
}

/// The stored bytes of one section, borrowed from the whole object.
fn section_bytes<'a>(
    bytes: &'a [u8],
    desc: &crate::footer::SectionDesc,
) -> Result<&'a [u8], SpanSegError> {
    usize::try_from(desc.offset)
        .ok()
        .zip(usize::try_from(desc.len).ok())
        .and_then(|(start, len)| Some(start..start.checked_add(len)?))
        .and_then(|range| bytes.get(range))
        .ok_or_else(|| SpanSegError::Corrupted("section out of bounds".into()))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::record::StatusCode;
    use crate::writer::{ObjectIdentity, RspanWriter};

    fn identity() -> ObjectIdentity {
        ObjectIdentity {
            tenant_hash: [0u8; 16],
            shard: 0,
            writer_id: [0u8; 16],
            writer_epoch: 0,
            writer_seq: 0,
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
            status_code: StatusCode::Ok,
            status_message: Some(format!("msg {span}")),
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

    /// Appends a valid KEY_IDX section for a hand-built object, naming each
    /// skip entry's `min_trace_id` in that entry's block, and returns its
    /// footer entry.
    fn append_key_idx(obj: &mut Vec<u8>, skip_raw: &[u8]) -> crate::footer::SectionDesc {
        append_key_idx_with(obj, skip_raw, &[])
    }

    /// [`append_key_idx`] plus `extra` `(trace_id, block ordinal)` pairs,
    /// which the encoder takes as given, ordinals past the block count
    /// included.
    fn append_key_idx_with(
        obj: &mut Vec<u8>,
        skip_raw: &[u8],
        extra: &[([u8; 16], u32)],
    ) -> crate::footer::SectionDesc {
        let skip = SkipIndex::decode(skip_raw, MAX_BLOCKS).expect("skip index decodes");
        let encoded = crate::key_index::encode_trace_key_idx(
            skip.blocks
                .iter()
                .enumerate()
                .map(|(block, entry)| (entry.min_trace_id, block as u32))
                .chain(extra.iter().copied()),
        )
        .expect("encode KEY_IDX");
        let offset = obj.len() as u64;
        obj.extend_from_slice(&encoded.bytes);
        crate::footer::SectionDesc {
            kind: kind::KEY_IDX,
            offset,
            len: encoded.bytes.len() as u64,
            crc32c: crc32c::crc32c(&encoded.bytes),
            comp: crate::footer::COMP_NONE,
            uncomp_len: encoded.bytes.len() as u64,
            prefix_len: encoded.prefix_len,
        }
    }

    #[test]
    fn ts_range_prunes_blocks() {
        let cfg = RspanConfig {
            block_target_records: 1,
            ..RspanConfig::default()
        };
        // 100 single-span blocks; span i covers [i*10, i*10+5].
        let recs: Vec<SpanRecord> = (0..100)
            .map(|i| span(0, i as u8, i * 10, i * 10 + 5))
            .collect();
        let obj = build(cfg, recs);
        let reader = RspanReader::new(&obj, &cfg).expect("open");
        let (rows, stats) = reader.scan(&SpanQuery::ts_range(400, 425)).expect("scan");
        assert_eq!(stats.blocks_total, 100);
        // Spans at 400,410,420 overlap [400,425].
        assert_eq!(rows.len(), 3);
        assert!(stats.blocks_after_skip <= 4);
    }

    #[test]
    fn trace_id_lookup_returns_one_trace() {
        let cfg = RspanConfig {
            block_target_records: 2,
            ..RspanConfig::default()
        };
        let mut recs = Vec::new();
        for t in 0u8..5 {
            for s in 0u8..4 {
                recs.push(span(
                    t,
                    s,
                    i64::from(t) * 1000 + i64::from(s),
                    i64::from(t) * 1000 + i64::from(s) + 1,
                ));
            }
        }
        let obj = build(cfg, recs);
        let reader = RspanReader::new(&obj, &cfg).expect("open");
        let (rows, _) = reader
            .scan(&SpanQuery::trace([3u8; 16], i64::MIN, i64::MAX))
            .expect("scan");
        assert_eq!(rows.len(), 4);
        assert!(rows.iter().all(|r| r.trace_id == [3u8; 16]));
    }

    #[test]
    fn full_roundtrip_records_match() {
        let cfg = RspanConfig {
            block_target_records: 3,
            ..RspanConfig::default()
        };
        let mut recs = Vec::new();
        for t in 0u8..3 {
            for s in 0u8..5 {
                recs.push(span(t, s, i64::from(s), i64::from(s) + 2));
            }
        }
        let obj = build(cfg, recs.clone());
        let reader = RspanReader::new(&obj, &cfg).expect("open");
        let (mut rows, _) = reader
            .scan(&SpanQuery::ts_range(i64::MIN, i64::MAX))
            .expect("scan");
        assert_eq!(rows.len(), recs.len());
        // Every input record comes back byte-identical (order differs by sort).
        rows.sort_by(|a, b| a.trace_id.cmp(&b.trace_id).then(a.span_id.cmp(&b.span_id)));
        let mut want = recs;
        want.sort_by(|a, b| a.trace_id.cmp(&b.trace_id).then(a.span_id.cmp(&b.span_id)));
        assert_eq!(rows, want);
    }

    #[test]
    fn corrupt_block_is_loud_error() {
        let cfg = RspanConfig::default();
        let recs: Vec<SpanRecord> = (0..10).map(|i| span(0, i as u8, i, i + 1)).collect();
        let mut obj = build(cfg, recs);
        // Flip a byte inside the BLOCKS section (offset 0).
        obj[4] ^= 0xff;
        let reader = RspanReader::new(&obj, &cfg).expect("open");
        assert!(matches!(
            reader.scan(&SpanQuery::ts_range(i64::MIN, i64::MAX)),
            Err(SpanSegError::Corrupted(_))
        ));
    }

    #[test]
    fn block_range_past_blocks_section_is_error() {
        use crate::footer::{COMP_NONE, SectionDesc, write_footer_and_trailer};
        use crate::skip_index::BlockEntry;

        let cfg = RspanConfig::default();
        // Arbitrary BLOCKS content; the crafted entry errors out in block_bytes
        // before this is ever decoded, so its bytes are irrelevant.
        let blocks_bytes = vec![0u8; 8];

        // One skip entry whose (offset, len) stays inside the whole file but
        // runs past the BLOCKS section's own end (8): rel_end = 0 + 12 > 8. Its
        // trace_id range and time interval are wide open so pruning keeps it and
        // the scan actually reaches block_bytes.
        let entry = BlockEntry {
            block_offset: 0,
            block_len: 12,
            block_crc32c: 0,
            record_count: 1,
            min_trace_id: [0u8; 16],
            max_trace_id: [0xffu8; 16],
            min_start_ts: i64::MIN,
            max_end_ts: i64::MAX,
            min_duration_ns: i64::MIN,
            max_duration_ns: i64::MAX,
            status_mask: 0,
        };
        let skip_raw = SkipIndex::new(vec![entry]).encode();

        // A minimal valid BLOOM section with one entry, matching the one skip
        // entry above: scan never parses it, but `RspanReader::new` checks the
        // entry count against the block count at open time (BLOOM is
        // mandatory as of v3, ADR-0054), so an empty, zero-entry section here
        // would fail to open before this test ever reaches the block-range
        // bug it exists to pin.
        let bloom_raw: Vec<u8> = {
            use ravel_codec::bloom::BloomBuilder;
            use ravel_codec::bloom_section::encode_bloom_section;
            encode_bloom_section(&[BloomBuilder::new(0).finish()])
        };

        let mut obj = Vec::new();
        obj.extend_from_slice(&blocks_bytes);
        let skip_off = obj.len() as u64;
        obj.extend_from_slice(&skip_raw);
        let bloom_off = obj.len() as u64;
        obj.extend_from_slice(&bloom_raw);
        let key_idx_desc = append_key_idx(&mut obj, &skip_raw);

        let sections = vec![
            SectionDesc {
                kind: kind::BLOCKS,
                offset: 0,
                len: blocks_bytes.len() as u64,
                crc32c: 0,
                comp: COMP_NONE,
                uncomp_len: blocks_bytes.len() as u64,
                prefix_len: 0,
            },
            SectionDesc {
                kind: kind::SKIP_IDX,
                offset: skip_off,
                len: skip_raw.len() as u64,
                crc32c: crc32c::crc32c(&skip_raw),
                comp: COMP_NONE,
                uncomp_len: skip_raw.len() as u64,
                prefix_len: 0,
            },
            SectionDesc {
                kind: kind::BLOOM,
                offset: bloom_off,
                len: bloom_raw.len() as u64,
                crc32c: crc32c::crc32c(&bloom_raw),
                comp: COMP_NONE,
                uncomp_len: bloom_raw.len() as u64,
                prefix_len: 0,
            },
            key_idx_desc,
        ];
        let footer = SpanFooter {
            tenant_hash: [0u8; 16],
            shard: 0,
            writer_id: [0u8; 16],
            writer_epoch: 0,
            writer_seq: 0,
            min_start_ts_ns: i64::MIN,
            max_end_ts_ns: i64::MAX,
            record_count: 1,
            block_count: 1,
            min_trace_id: [0u8; 16],
            max_trace_id: [0xffu8; 16],
            sections,
            level: 0,
            input_set_hash: Vec::new(),
            part_index: 0,
        };
        write_footer_and_trailer(&mut obj, &footer);

        // The crafted end (12) lands well inside the whole file, so the old
        // whole-object bound would have returned foreign SKIP_IDX/footer bytes.
        assert!(obj.len() as u64 > 12);

        let reader = RspanReader::new(&obj, &cfg).expect("open");
        let err = reader
            .scan(&SpanQuery::ts_range(i64::MIN, i64::MAX))
            .expect_err("out-of-section block range must be rejected");
        match err {
            SpanSegError::Corrupted(msg) => {
                assert!(msg.contains("BLOCKS section"), "unexpected message: {msg}");
            }
            other => panic!("expected Corrupted, got {other:?}"),
        }
    }

    #[test]
    fn corrupt_skip_index_fails_open() {
        let cfg = RspanConfig::default();
        let recs: Vec<SpanRecord> = (0..10).map(|i| span(0, i as u8, i, i + 1)).collect();
        let obj = build(cfg, recs);
        let footer = crate::footer::open(&obj).expect("open");
        let skip = footer.section(kind::SKIP_IDX).expect("skip");
        let flip = skip.offset as usize + (skip.len as usize / 2);
        let mut corrupt = obj.clone();
        corrupt[flip] ^= 0xff;
        assert!(matches!(
            RspanReader::new(&corrupt, &cfg),
            Err(SpanSegError::Corrupted(_))
        ));
    }

    /// A structurally valid, whole-section-crc-correct BLOOM container whose
    /// entry count disagrees with the block count must still fail at open,
    /// not open clean and silently under-prune or panic the first time
    /// something indexes past its entries (an entry-count mismatch checked
    /// only lazily at the first bloom probe, never at open, would let a short
    /// BLOOM body open and scan clean). Built manually
    /// (like `block_range_past_blocks_section_is_error` above) rather than
    /// patching a real writer object, so the section crc is computed over
    /// the actually-short content and only the entry-count check can catch
    /// the mismatch.
    #[test]
    fn bloom_entry_count_mismatch_fails_open() {
        use crate::footer::{COMP_NONE, SectionDesc, write_footer_and_trailer};
        use crate::skip_index::BlockEntry;
        use ravel_codec::bloom::BloomBuilder;
        use ravel_codec::bloom_section::encode_bloom_section;

        let cfg = RspanConfig::default();
        let blocks_bytes = vec![0u8; 8];
        let entries = vec![
            BlockEntry {
                block_offset: 0,
                block_len: 4,
                block_crc32c: 0,
                record_count: 1,
                min_trace_id: [0u8; 16],
                max_trace_id: [0u8; 16],
                min_start_ts: 0,
                max_end_ts: 1,
                min_duration_ns: 0,
                max_duration_ns: 1,
                status_mask: 0,
            },
            BlockEntry {
                block_offset: 4,
                block_len: 4,
                block_crc32c: 0,
                record_count: 1,
                min_trace_id: [1u8; 16],
                max_trace_id: [1u8; 16],
                min_start_ts: 1,
                max_end_ts: 2,
                min_duration_ns: 0,
                max_duration_ns: 1,
                status_mask: 0,
            },
        ];
        let skip_raw = SkipIndex::new(entries).encode();

        // Two blocks, but only one bloom entry: a genuine entry-count
        // mismatch, correctly checksummed so only the count check fires.
        let bloom_raw = encode_bloom_section(&[BloomBuilder::new(0).finish()]);

        let mut obj = Vec::new();
        obj.extend_from_slice(&blocks_bytes);
        let skip_off = obj.len() as u64;
        obj.extend_from_slice(&skip_raw);
        let bloom_off = obj.len() as u64;
        obj.extend_from_slice(&bloom_raw);
        let key_idx_desc = append_key_idx(&mut obj, &skip_raw);

        let sections = vec![
            SectionDesc {
                kind: kind::BLOCKS,
                offset: 0,
                len: blocks_bytes.len() as u64,
                crc32c: 0,
                comp: COMP_NONE,
                uncomp_len: blocks_bytes.len() as u64,
                prefix_len: 0,
            },
            SectionDesc {
                kind: kind::SKIP_IDX,
                offset: skip_off,
                len: skip_raw.len() as u64,
                crc32c: crc32c::crc32c(&skip_raw),
                comp: COMP_NONE,
                uncomp_len: skip_raw.len() as u64,
                prefix_len: 0,
            },
            SectionDesc {
                kind: kind::BLOOM,
                offset: bloom_off,
                len: bloom_raw.len() as u64,
                crc32c: crc32c::crc32c(&bloom_raw),
                comp: COMP_NONE,
                uncomp_len: bloom_raw.len() as u64,
                prefix_len: 0,
            },
            key_idx_desc,
        ];
        let footer = SpanFooter {
            tenant_hash: [0u8; 16],
            shard: 0,
            writer_id: [0u8; 16],
            writer_epoch: 0,
            writer_seq: 0,
            min_start_ts_ns: 0,
            max_end_ts_ns: 2,
            record_count: 2,
            block_count: 2,
            min_trace_id: [0u8; 16],
            max_trace_id: [1u8; 16],
            sections,
            level: 0,
            input_set_hash: Vec::new(),
            part_index: 0,
        };
        write_footer_and_trailer(&mut obj, &footer);

        match RspanReader::new(&obj, &cfg) {
            Err(SpanSegError::Corrupted(msg)) => {
                assert!(
                    msg.contains("bloom entry count"),
                    "unexpected message: {msg}"
                );
            }
            Ok(_) => panic!("expected Corrupted(bloom entry count ...), opened successfully"),
            Err(other) => panic!("expected Corrupted(bloom entry count ...), got {other:?}"),
        }
    }

    /// A two-block object whose KEY_IDX carries each block's `min_trace_id`
    /// plus `extra` pairs, every bucket frame well-formed and crc-correct, so
    /// only the probe's block-count check can refuse an ordinal in `extra`.
    fn two_block_object(extra: &[([u8; 16], u32)]) -> Vec<u8> {
        use crate::footer::{COMP_NONE, SectionDesc, write_footer_and_trailer};
        use crate::skip_index::BlockEntry;
        use ravel_codec::bloom::BloomBuilder;
        use ravel_codec::bloom_section::encode_bloom_section;

        let blocks_bytes = vec![0u8; 8];
        let entry = |block: u8| BlockEntry {
            block_offset: 4 * u64::from(block),
            block_len: 4,
            block_crc32c: 0,
            record_count: 1,
            min_trace_id: [block; 16],
            max_trace_id: [block; 16],
            min_start_ts: 0,
            max_end_ts: 1,
            min_duration_ns: 0,
            max_duration_ns: 1,
            status_mask: 0,
        };
        let skip_raw = SkipIndex::new(vec![entry(0), entry(1)]).encode();
        let bloom_raw =
            encode_bloom_section(&[BloomBuilder::new(0).finish(), BloomBuilder::new(0).finish()]);

        let mut obj = Vec::new();
        obj.extend_from_slice(&blocks_bytes);
        let skip_off = obj.len() as u64;
        obj.extend_from_slice(&skip_raw);
        let bloom_off = obj.len() as u64;
        obj.extend_from_slice(&bloom_raw);
        let key_idx_desc = append_key_idx_with(&mut obj, &skip_raw, extra);

        let sections = vec![
            SectionDesc {
                kind: kind::BLOCKS,
                offset: 0,
                len: blocks_bytes.len() as u64,
                crc32c: 0,
                comp: COMP_NONE,
                uncomp_len: blocks_bytes.len() as u64,
                prefix_len: 0,
            },
            SectionDesc {
                kind: kind::SKIP_IDX,
                offset: skip_off,
                len: skip_raw.len() as u64,
                crc32c: crc32c::crc32c(&skip_raw),
                comp: COMP_NONE,
                uncomp_len: skip_raw.len() as u64,
                prefix_len: 0,
            },
            SectionDesc {
                kind: kind::BLOOM,
                offset: bloom_off,
                len: bloom_raw.len() as u64,
                crc32c: crc32c::crc32c(&bloom_raw),
                comp: COMP_NONE,
                uncomp_len: bloom_raw.len() as u64,
                prefix_len: 0,
            },
            key_idx_desc,
        ];
        let footer = SpanFooter {
            tenant_hash: [0u8; 16],
            shard: 0,
            writer_id: [0u8; 16],
            writer_epoch: 0,
            writer_seq: 0,
            min_start_ts_ns: 0,
            max_end_ts_ns: 1,
            record_count: 2,
            block_count: 2,
            min_trace_id: [0u8; 16],
            max_trace_id: [1u8; 16],
            sections,
            level: 0,
            input_set_hash: Vec::new(),
            part_index: 0,
        };
        write_footer_and_trailer(&mut obj, &footer);
        obj
    }

    /// Probes `trace` through both readers of `obj`.
    fn probe_both(obj: &[u8], trace: &[u8; 16]) -> [Result<TraceKeyProbe, SpanSegError>; 2] {
        let whole = RspanReader::new(obj, &RspanConfig::default())
            .expect("a KEY_IDX naming any ordinal still opens")
            .probe_trace(trace);
        let footer = crate::footer::open(obj).expect("footer");
        let skip_desc = footer.section(kind::SKIP_IDX).expect("skip section");
        let skip_raw = crate::footer::read_section(obj, skip_desc, DEFAULT_MAX_UNCOMP)
            .expect("skip section bytes");
        let ranged = crate::ranged::RspanRangeReader::from_sections(&footer, &skip_raw)
            .expect("range reader")
            .probe_trace(trace, |r| {
                obj.get(r.start as usize..r.end as usize)
                    .ok_or_else(|| SpanSegError::Corrupted("test read past the object".into()))
            });
        [whole, ranged]
    }

    /// An ordinal at or past the SKIP_IDX block count in the probed bucket is
    /// `Corrupted` from both readers, whether it belongs to the probed trace or
    /// to another trace sharing the bucket. The last ordinal below the count is
    /// answered, so the refusal is the bound and not the construction.
    #[test]
    fn key_idx_ordinal_at_or_past_block_count_is_corrupted() {
        use ravel_codec::key_idx::{KEY_IDX_WRITER_BUCKET_BITS, bucket_index, key8_id16};

        const BLOCK_COUNT: u32 = 2;
        let probed = [0xabu8; 16];
        let bucket_of =
            |t: &[u8; 16]| bucket_index(&key8_id16(t), KEY_IDX_WRITER_BUCKET_BITS).expect("bucket");
        let neighbour = (0u8..=255)
            .map(|b| {
                let mut t = [0x5au8; 16];
                t[0] = b;
                t
            })
            .find(|t| key8_id16(t) != key8_id16(&probed) && bucket_of(t) == bucket_of(&probed))
            .expect("a second key8 in the probed bucket");

        for bad in [BLOCK_COUNT, BLOCK_COUNT + 5] {
            for (owner, extra) in [
                ("probed trace", vec![(probed, bad)]),
                ("bucket neighbour", vec![(probed, 0), (neighbour, bad)]),
            ] {
                let obj = two_block_object(&extra);
                for (reader, got) in ["RspanReader", "RspanRangeReader"]
                    .into_iter()
                    .zip(probe_both(&obj, &probed))
                {
                    match got {
                        Err(SpanSegError::Corrupted(msg)) => assert!(
                            msg.contains(&format!("names block {bad}, object has 2 blocks")),
                            "{reader}, ordinal {bad} on the {owner}: unexpected message {msg}"
                        ),
                        other => panic!(
                            "{reader}, ordinal {bad} on the {owner}: expected Corrupted, got {other:?}"
                        ),
                    }
                }
            }
        }

        let obj = two_block_object(&[(probed, BLOCK_COUNT - 1), (neighbour, 0)]);
        for got in probe_both(&obj, &probed) {
            assert_eq!(
                got.expect("in-range ordinal"),
                TraceKeyProbe::Present(vec![BLOCK_COUNT - 1])
            );
        }
    }

    /// A key whose bucket is empty costs the ranged probe one read, the prefix.
    #[test]
    fn ranged_probe_of_an_empty_bucket_reads_only_the_prefix() {
        use ravel_codec::key_idx::{KEY_IDX_WRITER_BUCKET_BITS, bucket_index, key8_id16};

        let obj = two_block_object(&[]);
        let bucket_of =
            |t: &[u8; 16]| bucket_index(&key8_id16(t), KEY_IDX_WRITER_BUCKET_BITS).expect("bucket");
        let occupied = [bucket_of(&[0u8; 16]), bucket_of(&[1u8; 16])];
        let absent = (2u8..=255)
            .map(|b| [b; 16])
            .find(|t| !occupied.contains(&bucket_of(t)))
            .expect("a key in an empty bucket");

        let footer = crate::footer::open(&obj).expect("footer");
        let key_idx = *footer.section(kind::KEY_IDX).expect("KEY_IDX section");
        let skip_desc = footer.section(kind::SKIP_IDX).expect("skip section");
        let skip_raw = crate::footer::read_section(&obj, skip_desc, DEFAULT_MAX_UNCOMP)
            .expect("skip section bytes");
        let ranged = crate::ranged::RspanRangeReader::from_sections(&footer, &skip_raw)
            .expect("range reader");
        let mut reads = Vec::new();
        let got = ranged
            .probe_trace(&absent, |r| {
                reads.push(r.clone());
                obj.get(r.start as usize..r.end as usize)
                    .ok_or_else(|| SpanSegError::Corrupted("test read past the object".into()))
            })
            .expect("probe");
        assert_eq!(got, TraceKeyProbe::Absent);
        assert_eq!(
            reads,
            vec![key_idx.offset..key_idx.offset + u64::from(key_idx.prefix_len)]
        );
    }
}

/// Writer/reader round trip over arbitrary span batches: encode N spans, decode
/// them all, and assert every field value comes back byte-identical (ADR-0041
/// deliverable). The comparison is order-insensitive because the writer sorts by
/// (trace_id, start_ts); records are keyed for comparison by (trace_id, span_id,
/// start_ts).
#[cfg(test)]
#[allow(clippy::expect_used)]
mod proptests {
    use proptest::prelude::*;

    use super::*;
    use crate::record::StatusCode;
    use crate::writer::{ObjectIdentity, RspanWriter};

    fn arb_status() -> impl Strategy<Value = StatusCode> {
        prop_oneof![
            Just(StatusCode::Unset),
            Just(StatusCode::Ok),
            Just(StatusCode::Error),
        ]
    }

    prop_compose! {
        fn arb_span()(
            trace in 0u8..6,
            span in any::<u8>(),
            // parent_span_id ranges over its whole domain: None (a root span)
            // and Some over arbitrary 8-byte ids (ADR-0041's span-id width),
            // generated independently of span_id rather than derived from it, so
            // the round trip exercises the parent column's full value space.
            parent_span_id in proptest::option::of(any::<[u8; 8]>()),
            name in "[a-zA-Z0-9/._-]{0,24}",
            start in any::<i64>(),
            dur in 0i64..1_000_000,
            // status_code ranges over every StatusCode variant this crate
            // defines (Unset/Ok/Error), not one fixed value.
            code in arb_status(),
            has_msg in any::<bool>(),
            msg in "[a-z ]{0,20}",
            attrs in proptest::collection::vec(("[a-z.]{1,8}", "[a-zA-Z0-9]{0,12}"), 0..8),
            // service.name is lifted into its own dictionary column (v3,
            // ADR-0054); generate it independently and sometimes present so the
            // round trip exercises the column's extract/re-insert and the block
            // bloom over its tokens. The key is out of the generated-attrs key
            // space ("[a-z.]{1,8}" caps at 8 chars, "service.name" is 12), so it
            // never collides with a random attr.
            service in proptest::option::of("[a-z][a-z0-9 ./_-]{0,20}"),
            // Zero or more span events, each an arbitrary opaque payload. Framed
            // as the ingest path frames `_events_raw` (length-delimited, hex),
            // so the round trip exercises the nested event columns: the writer
            // promotes the blob into columns and the reader reconstructs the
            // identical value.
            event_payloads in proptest::collection::vec(
                proptest::collection::vec(any::<u8>(), 0..8), 0..3),
        ) -> SpanRecord {
            let mut attrs: Vec<(String, String)> = attrs.into_iter().collect();
            if let Some(s) = service {
                attrs.push(("service.name".to_string(), s));
            }
            if !event_payloads.is_empty() {
                let mut raw = Vec::new();
                for p in &event_payloads {
                    crate::varint::put_uvarint(&mut raw, p.len() as u64);
                    raw.extend_from_slice(p);
                }
                let hex: String = raw.iter().map(|b| format!("{b:02x}")).collect();
                attrs.push(("_events_raw".to_string(), hex));
            }
            SpanRecord {
                trace_id: [trace; 16],
                span_id: [span; 8],
                parent_span_id,
                name,
                start_ts_ns: start,
                end_ts_ns: start.saturating_add(dur),
                status_code: code,
                status_message: if has_msg { Some(msg) } else { None },
                attrs,
            }
        }
    }

    /// The canonical form the writer stores an input record as: attrs collapsed
    /// to a sorted unique-key map (encode_attrs's behavior), everything else
    /// verbatim. Two logically-equal inputs map to the same canonical record.
    fn canonicalize(mut r: SpanRecord) -> SpanRecord {
        let map: std::collections::BTreeMap<String, String> = r.attrs.into_iter().collect();
        r.attrs = map.into_iter().collect();
        r
    }

    fn key8(trace_id: &[u8; 16]) -> [u8; 8] {
        let mut k = [0u8; 8];
        k.copy_from_slice(&trace_id[..8]);
        k
    }

    fn key(r: &SpanRecord) -> ([u8; 16], [u8; 8], i64) {
        (r.trace_id, r.span_id, r.start_ts_ns)
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 128, ..ProptestConfig::default() })]

        #[test]
        fn writer_reader_roundtrip_byte_identical_fields(
            spans in proptest::collection::vec(arb_span(), 1..80),
            block_target in 1usize..8,
        ) {
            let cfg = RspanConfig { block_target_records: block_target, ..RspanConfig::default() };
            let identity = ObjectIdentity {
                tenant_hash: [7u8; 16],
                shard: 1,
                writer_id: [9u8; 16],
                writer_epoch: 2,
                writer_seq: 3,
            };
            let mut w = RspanWriter::new(cfg, identity);
            for s in &spans {
                w.push(s.clone());
            }
            let obj = w.finish().expect("finish");
            let reader = RspanReader::new(&obj, &cfg).expect("open");
            let (got, stats) = reader
                .scan(&SpanQuery::ts_range(i64::MIN, i64::MAX))
                .expect("scan");
            prop_assert_eq!(got.len(), spans.len());
            prop_assert_eq!(stats.blocks_scanned, stats.blocks_after_skip);

            // KEY_IDX oracle: the probe for each trace names exactly the
            // blocks holding a record whose trace_id shares its first 8 bytes.
            let footer = crate::footer::open(&obj).expect("footer");
            let skip_desc = footer.section(kind::SKIP_IDX).expect("skip");
            let skip_raw = crate::footer::read_section(
                &obj,
                skip_desc,
                crate::footer::DEFAULT_MAX_SECTION_UNCOMP,
            )
            .expect("skip raw");
            let ranged =
                crate::ranged::RspanRangeReader::from_sections(&footer, &skip_raw).expect("ranged");
            let mut block_keys: Vec<std::collections::BTreeSet<[u8; 8]>> = Vec::new();
            for i in 0..ranged.block_count() {
                let loc = ranged.block_loc(i).expect("loc");
                let rows = ranged
                    .decode_block(&loc, &obj[loc.start() as usize..loc.end() as usize])
                    .expect("decode block");
                block_keys.push(rows.iter().map(|r| key8(&r.trace_id)).collect());
            }
            for s in &spans {
                let want: Vec<u32> = (0..block_keys.len() as u32)
                    .filter(|&b| block_keys[b as usize].contains(&key8(&s.trace_id)))
                    .collect();
                prop_assert_eq!(reader.probe_trace(&s.trace_id).expect("probe"), TraceKeyProbe::Present(want));
            }

            let mut want: Vec<SpanRecord> = spans.into_iter().map(canonicalize).collect();
            let mut got = got;
            want.sort_by_key(key);
            got.sort_by_key(key);
            prop_assert_eq!(got, want);
        }
    }
}
