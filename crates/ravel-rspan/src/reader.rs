//! Pruned scan reader (docs/span-segment-format.md "Pruning soundness").
//!
//! [`RspanReader::scan`] prunes blocks through the interval-aware skip index,
//! then reads, verifies, and re-evaluates the survivors exactly. Pruning is
//! proof-based: a block is dropped only when the skip index proves its
//! trace_id range or time interval disjoint from the query. A corrupt SKIP_IDX
//! is a loud `Corrupted` error rather than a degrade, because its bytes carry
//! the block framing and per-block checksums: without it no block can be
//! located or verified.

use crate::block::{DEFAULT_MAX_UNCOMP, read_block};
use crate::error::SpanSegError;
use crate::footer::{SpanFooter, kind, open, read_section};
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
    max_uncomp: u64,
}

impl<'a> RspanReader<'a> {
    /// Opens and validates the object, decoding the skip index.
    pub fn new(bytes: &'a [u8], cfg: &RspanConfig) -> Result<Self, SpanSegError> {
        let footer = open(bytes)?;
        let skip_desc = section(&footer, kind::SKIP_IDX)?;
        let skip_raw = read_section(bytes, skip_desc, cfg.max_uncomp_section)?;
        let skip = SkipIndex::decode(&skip_raw, MAX_BLOCKS)?;
        let blocks = *section(&footer, kind::BLOCKS)?;
        Ok(RspanReader {
            bytes,
            skip,
            blocks_offset: blocks.offset,
            max_uncomp: DEFAULT_MAX_UNCOMP,
        })
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

        let candidates =
            self.skip
                .candidate_blocks(query.trace_id.as_ref(), query.ts_min, query.ts_max);
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
            has_parent in any::<bool>(),
            name in "[a-zA-Z0-9/._-]{0,24}",
            start in any::<i64>(),
            dur in 0i64..1_000_000,
            code in arb_status(),
            has_msg in any::<bool>(),
            msg in "[a-z ]{0,20}",
            attrs in proptest::collection::vec(("[a-z.]{1,8}", "[a-zA-Z0-9]{0,12}"), 0..8),
        ) -> SpanRecord {
            SpanRecord {
                trace_id: [trace; 16],
                span_id: [span; 8],
                parent_span_id: if has_parent { Some([span.wrapping_add(1); 8]) } else { None },
                name,
                start_ts_ns: start,
                end_ts_ns: start.saturating_add(dur),
                status_code: code,
                status_message: if has_msg { Some(msg) } else { None },
                attrs: attrs.into_iter().collect(),
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

            let mut want: Vec<SpanRecord> = spans.into_iter().map(canonicalize).collect();
            let mut got = got;
            want.sort_by_key(key);
            got.sort_by_key(key);
            prop_assert_eq!(got, want);
        }
    }
}
