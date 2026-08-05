//! SKIP_IDX section (docs/span-segment-format.md "SKIP_IDX").
//!
//! One entry per block, carrying its byte range and crc, record count, trace_id
//! bounds, time-interval bounds, duration bounds, and a status mask. Two
//! departures from RLOG's SKIP_IDX (ADR-0041): the time bound is an interval
//! `(min_start_ts, max_end_ts)` pruned by overlap, not a single point range;
//! and the identity bound is the block's `(min_trace_id, max_trace_id)`, since
//! records sort by `(trace_id, start_ts)` and trace_id is the primary lookup
//! key rather than a derived stream ref. v2 (ADR-0045 decision 2) adds
//! `(min_duration_ns, max_duration_ns)`, derived at write time from
//! `end_ts - start_ts` per row, and `status_mask`, a 3-bit summary of which
//! OTLP status codes appear in the block, so a trace-investigation query on
//! duration or status can prune. The index is a single level: RSPAN keeps this
//! leaner than RLOG's two-level index because a span object is a single sorted
//! run and there is no separate stream-ref dimension to summarize. Pruning is
//! sound: a block is dropped only when its bounds prove no record in it can
//! match.

use ravel_codec::bloom_section::BloomSection;
use ravel_codec::tokenizer::tokens;

use crate::error::SpanSegError;
use crate::record::TRACE_ID_WIDTH;
use crate::varint::{get_ivarint, get_uvarint, put_ivarint, put_uvarint};

/// A bloom-backed equality predicate for [`SkipIndex::candidate_blocks_with_bloom`].
///
/// `field_id` is the bloom field the literal is scoped to: `COL_NAME` (3) for a
/// `name = <literal>` predicate, `COL_SERVICE_NAME` (9) for a
/// `service_name = <literal>` predicate (docs/span-segment-format.md "BLOOM",
/// ADR-0054). A caller in another crate (for example ravel-sql, issue #650)
/// builds this from a plain `u32` and `&str` and needs no RSPAN-internal type.
#[derive(Clone, Copy, Debug)]
pub struct BloomPredicate<'a> {
    /// The bloom field id (`COL_NAME` or `COL_SERVICE_NAME`).
    pub field_id: u32,
    /// The equality literal, tokenized with the shared tokenizer.
    pub literal: &'a str,
}

/// `status_mask` bit: the block has at least one record with `StatusCode::Unset`.
pub const STATUS_BIT_UNSET: u8 = 0b0000_0001;
/// `status_mask` bit: the block has at least one record with `StatusCode::Ok`.
pub const STATUS_BIT_OK: u8 = 0b0000_0010;
/// `status_mask` bit: the block has at least one record with `StatusCode::Error`.
pub const STATUS_BIT_ERROR: u8 = 0b0000_0100;
/// The only bits a `status_mask` byte may set. Bits 3-7 are reserved: the
/// writer emits 0 for them and the reader rejects a nonzero value (ADR-0045).
const STATUS_MASK_VALID: u8 = STATUS_BIT_UNSET | STATUS_BIT_OK | STATUS_BIT_ERROR;

/// One per-block skip entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockEntry {
    pub block_offset: u64,
    pub block_len: u64,
    pub block_crc32c: u32,
    pub record_count: u32,
    pub min_trace_id: [u8; 16],
    pub max_trace_id: [u8; 16],
    pub min_start_ts: i64,
    pub max_end_ts: i64,
    /// `min(end_ts_ns - start_ts_ns)` over the block's records.
    pub min_duration_ns: i64,
    /// `max(end_ts_ns - start_ts_ns)` over the block's records.
    pub max_duration_ns: i64,
    /// Bits 0/1/2 set when the block has any Unset/Ok/Error record,
    /// respectively; bits 3-7 always 0.
    pub status_mask: u8,
}

/// The decoded skip index: one entry per block, in block order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SkipIndex {
    pub blocks: Vec<BlockEntry>,
}

impl SkipIndex {
    pub fn new(blocks: Vec<BlockEntry>) -> Self {
        SkipIndex { blocks }
    }

    /// Block indices whose bounds survive the predicate. Sound: a block is
    /// included unless its bounds prove no record matches. `trace_id`, when
    /// given, restricts to blocks whose `[min, max]` trace_id range contains it;
    /// the time window `[ts_min, ts_max]` restricts to blocks whose
    /// `[min_start_ts, max_end_ts]` interval overlaps it. `duration_ns`, when
    /// given, is an inclusive `[min, max]` duration window restricting to
    /// blocks whose `[min_duration_ns, max_duration_ns]` bound overlaps it.
    /// `status_mask`, when given, restricts to blocks whose `status_mask` has
    /// at least one bit in common with it.
    pub fn candidate_blocks(
        &self,
        trace_id: Option<&[u8; 16]>,
        ts_min: i64,
        ts_max: i64,
        duration_ns: Option<(i64, i64)>,
        status_mask: Option<u8>,
    ) -> Vec<usize> {
        let mut out = Vec::new();
        for (i, e) in self.blocks.iter().enumerate() {
            if block_pruned(e, trace_id, ts_min, ts_max, duration_ns, status_mask) {
                continue;
            }
            out.push(i);
        }
        out
    }

    /// [`SkipIndex::candidate_blocks`] with an additional bloom-backed prune
    /// over `service_name`/`name` equality predicates (ADR-0054). `bloom` is
    /// the object's parsed BLOOM section (entry `i` covers block `i`); each
    /// `predicate` names a bloom field id and an equality literal.
    ///
    /// A bloom is false-positive-only (ADR-0013 widen-only rule): a negative
    /// probe proves the token absent and excludes the block; a positive probe
    /// changes nothing and the block still survives to exact re-evaluation. The
    /// literal is tokenized with the shared tokenizer and the block is dropped
    /// only when the bloom proves some token of some predicate absent (the
    /// conjunctive AND-of-proofs, matching [`block_pruned`]). A literal that
    /// tokenizes to nothing cannot prune. A per-entry crc mismatch inside the
    /// bloom, or a bloom whose entry count does not match the block count, is a
    /// typed `Corrupted` error, never a panic.
    ///
    /// This is the surface ravel-sql's span pushdown (issue #650) calls: it
    /// passes a parsed [`BloomSection`] (obtained through this crate's public
    /// `open`/`read_section` on the BLOOM descriptor) and a slice of
    /// [`BloomPredicate`]s built from plain `u32`/`&str`, constructing no
    /// RSPAN-internal value.
    // The signature is `candidate_blocks`'s five pruning axes plus the two
    // bloom inputs; keeping them positionally parallel to `candidate_blocks`
    // (rather than bundling into a struct) is the clearer surface for a caller
    // that already builds the axes for the non-bloom path.
    #[allow(clippy::too_many_arguments)]
    pub fn candidate_blocks_with_bloom(
        &self,
        trace_id: Option<&[u8; 16]>,
        ts_min: i64,
        ts_max: i64,
        duration_ns: Option<(i64, i64)>,
        status_mask: Option<u8>,
        bloom: &BloomSection<'_>,
        predicates: &[BloomPredicate<'_>],
    ) -> Result<Vec<usize>, SpanSegError> {
        if bloom.len() != self.blocks.len() {
            return Err(SpanSegError::Corrupted(format!(
                "bloom entry count {} != block count {}",
                bloom.len(),
                self.blocks.len()
            )));
        }
        let base = self.candidate_blocks(trace_id, ts_min, ts_max, duration_ns, status_mask);
        if predicates.is_empty() {
            return Ok(base);
        }
        // Tokenize each literal once up front (the token set is reused across
        // every candidate block).
        let tokenized: Vec<(u32, Vec<Vec<u8>>)> = predicates
            .iter()
            .map(|p| (p.field_id, tokens(p.literal)))
            .collect();

        let mut out = Vec::with_capacity(base.len());
        for i in base {
            let view = bloom.entry(i)?;
            // Survives unless some predicate's tokens are all provably present
            // is false for at least one token: a single absent token proves the
            // literal cannot be in the block (no false negatives), so prune.
            let pruned = tokenized.iter().any(|(field_id, toks)| {
                !toks.is_empty() && !toks.iter().all(|t| view.may_contain(*field_id, t))
            });
            if !pruned {
                out.push(i);
            }
        }
        Ok(out)
    }

    /// Serializes the section in its uncompressed form.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(self.blocks.len() as u32).to_le_bytes());
        for e in &self.blocks {
            put_uvarint(&mut out, e.block_offset);
            put_uvarint(&mut out, e.block_len);
            out.extend_from_slice(&e.block_crc32c.to_le_bytes());
            put_uvarint(&mut out, u64::from(e.record_count));
            out.extend_from_slice(&e.min_trace_id);
            out.extend_from_slice(&e.max_trace_id);
            put_ivarint(&mut out, e.min_start_ts);
            put_ivarint(&mut out, e.max_end_ts);
            put_ivarint(&mut out, e.min_duration_ns);
            put_ivarint(&mut out, e.max_duration_ns);
            out.push(e.status_mask);
        }
        out
    }

    /// Decodes the uncompressed section form. Rejects a block count over
    /// `max_blocks`, truncation, trailing bytes, and a `status_mask` with any
    /// reserved bit (3-7) set.
    pub fn decode(bytes: &[u8], max_blocks: u64) -> Result<Self, SpanSegError> {
        let mut pos = 0usize;
        let count = read_u32(bytes, &mut pos)?;
        if u64::from(count) > max_blocks {
            return Err(SpanSegError::Corrupted(format!(
                "skip block count {count} over cap {max_blocks}"
            )));
        }
        let mut blocks = Vec::with_capacity((count as usize).min(1 << 16));
        for _ in 0..count {
            let block_offset = get_uvarint(bytes, &mut pos)?;
            let block_len = get_uvarint(bytes, &mut pos)?;
            let block_crc32c = read_u32(bytes, &mut pos)?;
            let record_count = u32::try_from(get_uvarint(bytes, &mut pos)?)
                .map_err(|_| SpanSegError::Corrupted("skip record_count range".into()))?;
            let min_trace_id = read_trace_id(bytes, &mut pos)?;
            let max_trace_id = read_trace_id(bytes, &mut pos)?;
            let min_start_ts = get_ivarint(bytes, &mut pos)?;
            let max_end_ts = get_ivarint(bytes, &mut pos)?;
            let min_duration_ns = get_ivarint(bytes, &mut pos)?;
            let max_duration_ns = get_ivarint(bytes, &mut pos)?;
            let status_mask = *bytes
                .get(pos)
                .ok_or_else(|| SpanSegError::Corrupted("skip truncated status_mask".into()))?;
            pos += 1;
            if status_mask & !STATUS_MASK_VALID != 0 {
                return Err(SpanSegError::Corrupted(format!(
                    "skip status_mask {status_mask:#04x} sets a reserved bit"
                )));
            }
            blocks.push(BlockEntry {
                block_offset,
                block_len,
                block_crc32c,
                record_count,
                min_trace_id,
                max_trace_id,
                min_start_ts,
                max_end_ts,
                min_duration_ns,
                max_duration_ns,
                status_mask,
            });
        }
        if pos != bytes.len() {
            return Err(SpanSegError::Corrupted("skip index trailing bytes".into()));
        }
        Ok(SkipIndex { blocks })
    }
}

/// True when `entry`'s bounds prove no record in the block can match the
/// predicate (docs/span-segment-format.md "Pruning soundness"). A block is
/// pruned when its time interval is disjoint from the query window, when (a
/// trace_id is given and) the trace_id falls outside the block's trace_id
/// range, when (a duration window is given and) the block's duration bound is
/// disjoint from it, or when (a status mask is given and) the block's
/// `status_mask` shares no bit with it.
pub fn block_pruned(
    entry: &BlockEntry,
    trace_id: Option<&[u8; 16]>,
    ts_min: i64,
    ts_max: i64,
    duration_ns: Option<(i64, i64)>,
    status_mask: Option<u8>,
) -> bool {
    // Interval-overlap test: prune when the block's [min_start, max_end] does
    // not overlap the query [ts_min, ts_max].
    if entry.max_end_ts < ts_min || entry.min_start_ts > ts_max {
        return true;
    }
    if let Some(tid) = trace_id
        && (*tid < entry.min_trace_id || *tid > entry.max_trace_id)
    {
        return true;
    }
    if let Some((dmin, dmax)) = duration_ns
        && (entry.max_duration_ns < dmin || entry.min_duration_ns > dmax)
    {
        return true;
    }
    if let Some(mask) = status_mask
        && (entry.status_mask & mask) == 0
    {
        return true;
    }
    false
}

fn read_u32(bytes: &[u8], pos: &mut usize) -> Result<u32, SpanSegError> {
    let s = bytes
        .get(*pos..*pos + 4)
        .ok_or_else(|| SpanSegError::Corrupted("skip truncated u32".into()))?;
    *pos += 4;
    Ok(u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
}

fn read_trace_id(bytes: &[u8], pos: &mut usize) -> Result<[u8; 16], SpanSegError> {
    let end = pos
        .checked_add(TRACE_ID_WIDTH)
        .ok_or_else(|| SpanSegError::Corrupted("skip trace_id overflow".into()))?;
    let s = bytes
        .get(*pos..end)
        .ok_or_else(|| SpanSegError::Corrupted("skip trace_id truncated".into()))?;
    let mut a = [0u8; 16];
    a.copy_from_slice(s);
    *pos = end;
    Ok(a)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn entry(trace: u8, min_start: i64, max_end: i64) -> BlockEntry {
        BlockEntry {
            block_offset: 0,
            block_len: 10,
            block_crc32c: 1,
            record_count: 4,
            min_trace_id: [trace; 16],
            max_trace_id: [trace; 16],
            min_start_ts: min_start,
            max_end_ts: max_end,
            min_duration_ns: 0,
            max_duration_ns: 0,
            status_mask: STATUS_MASK_VALID,
        }
    }

    #[test]
    fn roundtrip() {
        let idx = SkipIndex::new(vec![entry(1, 0, 100), entry(2, 50, 300)]);
        let got = SkipIndex::decode(&idx.encode(), 1000).expect("decode");
        assert_eq!(got, idx);
    }

    #[test]
    fn roundtrip_with_duration_and_status() {
        let e = BlockEntry {
            min_duration_ns: -5,
            max_duration_ns: 1_000_000,
            status_mask: STATUS_BIT_OK | STATUS_BIT_ERROR,
            ..entry(1, 0, 100)
        };
        let idx = SkipIndex::new(vec![e]);
        let got = SkipIndex::decode(&idx.encode(), 1000).expect("decode");
        assert_eq!(got, idx);
    }

    #[test]
    fn interval_overlap_boundary_cases() {
        // Block interval [100, 200].
        let e = entry(1, 100, 200);
        // Window fully before the block: [0, 99] -> pruned.
        assert!(block_pruned(&e, None, 0, 99, None, None));
        // Window fully after the block: [201, 300] -> pruned.
        assert!(block_pruned(&e, None, 201, 300, None, None));
        // Touch at the left edge: [0, 100] -> kept (max_end 200 >= 0, min_start
        // 100 <= 100).
        assert!(!block_pruned(&e, None, 0, 100, None, None));
        // Touch at the right edge: [200, 300] -> kept.
        assert!(!block_pruned(&e, None, 200, 300, None, None));
        // Partial overlap at each edge.
        assert!(!block_pruned(&e, None, 50, 150, None, None));
        assert!(!block_pruned(&e, None, 150, 250, None, None));
        // Window fully containing the block.
        assert!(!block_pruned(&e, None, 0, 1000, None, None));
        // Block fully containing the window.
        assert!(!block_pruned(&e, None, 120, 130, None, None));
    }

    #[test]
    fn trace_id_pruning() {
        // Block covers trace ids [2..=4] and interval [0, 1000].
        let e = BlockEntry {
            min_trace_id: [2u8; 16],
            max_trace_id: [4u8; 16],
            ..entry(0, 0, 1000)
        };
        assert!(block_pruned(&e, Some(&[1u8; 16]), 0, 1000, None, None));
        assert!(!block_pruned(&e, Some(&[2u8; 16]), 0, 1000, None, None));
        assert!(!block_pruned(&e, Some(&[3u8; 16]), 0, 1000, None, None));
        assert!(!block_pruned(&e, Some(&[4u8; 16]), 0, 1000, None, None));
        assert!(block_pruned(&e, Some(&[5u8; 16]), 0, 1000, None, None));
        // A matching trace_id in a disjoint time window is still pruned.
        assert!(block_pruned(&e, Some(&[3u8; 16]), 2000, 3000, None, None));
    }

    #[test]
    fn candidate_blocks_selects_survivors() {
        let idx = SkipIndex::new(vec![
            entry(1, 0, 100),
            entry(2, 100, 200),
            entry(3, 200, 300),
        ]);
        assert_eq!(idx.candidate_blocks(None, 120, 130, None, None), vec![1]);
        assert_eq!(
            idx.candidate_blocks(Some(&[3u8; 16]), 0, 1000, None, None),
            vec![2]
        );
        // No overlap at all.
        assert!(idx.candidate_blocks(None, 400, 500, None, None).is_empty());
    }

    /// The acceptance test for ADR-0045 decision 2's duration/status half:
    /// proves BOTH halves of pruning soundness. A duration-and-status
    /// predicate must actually reduce the candidate set (pruning happens), and
    /// every block whose own bounds cannot rule out a match must still survive
    /// (pruning is sound).
    #[test]
    fn v2_prunes_on_duration_and_status() {
        // Four blocks, all with a wide-open, identical time interval so only
        // the duration/status predicate can distinguish them.
        let wide_open = |duration: (i64, i64), status_mask: u8| BlockEntry {
            min_duration_ns: duration.0,
            max_duration_ns: duration.1,
            status_mask,
            ..entry(0, 0, 1000)
        };
        let blocks = vec![
            wide_open((10, 20), STATUS_BIT_OK),      // A: short, no error
            wide_open((100, 200), STATUS_BIT_ERROR), // B: long, error
            wide_open((10, 20), STATUS_BIT_ERROR),   // C: short, error -- must survive
            wide_open((500, 600), STATUS_BIT_OK | STATUS_BIT_ERROR), // D: long, error
        ];
        let idx = SkipIndex::new(blocks.clone());

        // Query: short-duration (<= 50ns) error spans.
        let duration_query = Some((0i64, 50i64));
        let status_query = Some(STATUS_BIT_ERROR);
        let cands = idx.candidate_blocks(None, 0, 1000, duration_query, status_query);

        // Pruning happens: candidates are a strict subset of all blocks.
        assert!(
            cands.len() < blocks.len(),
            "duration+status predicate must drop at least one block"
        );
        // Pruning is sound: block C (index 2) contains a possible match (its
        // duration bound overlaps the query window and its status_mask shares
        // the Error bit) and must survive.
        assert!(cands.contains(&2), "block C must survive: it can match");
        // Blocks A, B, D each fail on duration or status alone and must be
        // dropped.
        assert_eq!(cands, vec![2]);

        // Soundness holds generally, not just for this hand-picked case: any
        // block whose bounds do not disjoint-prove absence on EITHER axis
        // survives even when only one predicate is given.
        let duration_only = idx.candidate_blocks(None, 0, 1000, duration_query, None);
        for (i, e) in blocks.iter().enumerate() {
            let overlaps = e.max_duration_ns >= 0 && e.min_duration_ns <= 50;
            if overlaps {
                assert!(duration_only.contains(&i), "block {i} must survive");
            }
        }
        let status_only = idx.candidate_blocks(None, 0, 1000, None, status_query);
        for (i, e) in blocks.iter().enumerate() {
            if e.status_mask & STATUS_BIT_ERROR != 0 {
                assert!(status_only.contains(&i), "block {i} must survive");
            }
        }
    }

    /// Builds a BLOOM container over per-block `(field_id, token)` corpora,
    /// one entry per block. Mirrors what the writer emits.
    fn bloom_bytes(per_block: &[Vec<(u32, &str)>]) -> Vec<u8> {
        use ravel_codec::bloom::BloomBuilder;
        use ravel_codec::bloom_section::encode_bloom_section;
        use ravel_codec::tokenizer::tokens;
        let entries: Vec<Vec<u8>> = per_block
            .iter()
            .map(|corpus| {
                let mut b = BloomBuilder::new(0);
                for (field, text) in corpus {
                    for tok in tokens(text) {
                        b.insert(*field, &tok);
                    }
                }
                b.finish()
            })
            .collect();
        encode_bloom_section(&entries)
    }

    const FIELD_SERVICE: u32 = crate::record::COL_SERVICE_NAME;
    const FIELD_NAME: u32 = crate::record::COL_NAME;

    #[test]
    fn bloom_prunes_and_is_sound() {
        // Three blocks, wide-open time/trace so only the bloom distinguishes.
        let idx = SkipIndex::new(vec![
            entry(0, 0, 1000),
            entry(0, 0, 1000),
            entry(0, 0, 1000),
        ]);
        let raw = bloom_bytes(&[
            vec![(FIELD_SERVICE, "checkout"), (FIELD_NAME, "GET /cart")],
            vec![(FIELD_SERVICE, "payments"), (FIELD_NAME, "charge card")],
            vec![(FIELD_SERVICE, "checkout"), (FIELD_NAME, "POST /order")],
        ]);
        let bloom = BloomSection::parse(&raw).expect("parse");

        // service_name = "payments": only block 1 can match. No false negative.
        let pred = [BloomPredicate {
            field_id: FIELD_SERVICE,
            literal: "payments",
        }];
        let cands = idx
            .candidate_blocks_with_bloom(None, i64::MIN, i64::MAX, None, None, &bloom, &pred)
            .expect("prune");
        assert_eq!(cands, vec![1], "only the payments block survives");

        // service_name = "checkout": blocks 0 and 2 survive.
        let pred = [BloomPredicate {
            field_id: FIELD_SERVICE,
            literal: "checkout",
        }];
        let cands = idx
            .candidate_blocks_with_bloom(None, i64::MIN, i64::MAX, None, None, &bloom, &pred)
            .expect("prune");
        assert_eq!(cands, vec![0, 2]);

        // An absent service prunes every block (a bloom negative is proof).
        let pred = [BloomPredicate {
            field_id: FIELD_SERVICE,
            literal: "nonexistent-service-xyz",
        }];
        let cands = idx
            .candidate_blocks_with_bloom(None, i64::MIN, i64::MAX, None, None, &bloom, &pred)
            .expect("prune");
        assert!(cands.is_empty());
    }

    #[test]
    fn bloom_field_scoping_and_conjunction() {
        let idx = SkipIndex::new(vec![entry(0, 0, 1000), entry(0, 0, 1000)]);
        let raw = bloom_bytes(&[
            vec![(FIELD_SERVICE, "checkout"), (FIELD_NAME, "charge")],
            vec![(FIELD_SERVICE, "payments"), (FIELD_NAME, "charge")],
        ]);
        let bloom = BloomSection::parse(&raw).expect("parse");

        // A span-name token probed under the service field must not match: the
        // two blooms are field-scoped by distinct field ids.
        let pred = [BloomPredicate {
            field_id: FIELD_SERVICE,
            literal: "charge",
        }];
        let cands = idx
            .candidate_blocks_with_bloom(None, i64::MIN, i64::MAX, None, None, &bloom, &pred)
            .expect("prune");
        assert!(cands.is_empty(), "'charge' is a name token, not a service");

        // service_name = "checkout" AND name = "charge": only block 0 has the
        // service, both blocks have the name -> conjunction keeps block 0 only.
        let preds = [
            BloomPredicate {
                field_id: FIELD_SERVICE,
                literal: "checkout",
            },
            BloomPredicate {
                field_id: FIELD_NAME,
                literal: "charge",
            },
        ];
        let cands = idx
            .candidate_blocks_with_bloom(None, i64::MIN, i64::MAX, None, None, &bloom, &preds)
            .expect("prune");
        assert_eq!(cands, vec![0]);
    }

    #[test]
    fn bloom_empty_literal_cannot_prune() {
        let idx = SkipIndex::new(vec![entry(0, 0, 1000)]);
        let raw = bloom_bytes(&[vec![(FIELD_SERVICE, "checkout")]]);
        let bloom = BloomSection::parse(&raw).expect("parse");
        // A literal with no tokens (all separators) proves nothing: keep all.
        let pred = [BloomPredicate {
            field_id: FIELD_SERVICE,
            literal: "  --  ",
        }];
        let cands = idx
            .candidate_blocks_with_bloom(None, i64::MIN, i64::MAX, None, None, &bloom, &pred)
            .expect("prune");
        assert_eq!(cands, vec![0]);
    }

    #[test]
    fn bloom_count_mismatch_is_error() {
        let idx = SkipIndex::new(vec![entry(0, 0, 1000), entry(0, 0, 1000)]);
        // One bloom entry for a two-block index: a mandatory-invariant violation.
        let raw = bloom_bytes(&[vec![(FIELD_SERVICE, "checkout")]]);
        let bloom = BloomSection::parse(&raw).expect("parse");
        let pred = [BloomPredicate {
            field_id: FIELD_SERVICE,
            literal: "checkout",
        }];
        assert!(matches!(
            idx.candidate_blocks_with_bloom(None, i64::MIN, i64::MAX, None, None, &bloom, &pred),
            Err(SpanSegError::Corrupted(_))
        ));
    }

    #[test]
    fn bloom_corrupt_entry_is_error() {
        let idx = SkipIndex::new(vec![entry(0, 0, 1000)]);
        let mut raw = bloom_bytes(&[vec![(FIELD_SERVICE, "checkout")]]);
        // Flip a byte in the last entry payload; its per-entry crc must catch it.
        let last = raw.len() - 1;
        raw[last] ^= 0xff;
        let bloom = BloomSection::parse(&raw).expect("parse container");
        let pred = [BloomPredicate {
            field_id: FIELD_SERVICE,
            literal: "checkout",
        }];
        assert!(matches!(
            idx.candidate_blocks_with_bloom(None, i64::MIN, i64::MAX, None, None, &bloom, &pred),
            Err(SpanSegError::Corrupted(_))
        ));
    }

    #[test]
    fn decode_rejects_cap_and_trailing() {
        let idx = SkipIndex::new(vec![entry(1, 0, 9)]);
        let bytes = idx.encode();
        assert!(matches!(
            SkipIndex::decode(&bytes, 0),
            Err(SpanSegError::Corrupted(_))
        ));
        let mut extra = bytes.clone();
        extra.push(0);
        assert!(matches!(
            SkipIndex::decode(&extra, 100),
            Err(SpanSegError::Corrupted(_))
        ));
    }

    #[test]
    fn decode_rejects_reserved_status_bit() {
        let idx = SkipIndex::new(vec![entry(1, 0, 9)]);
        let mut bytes = idx.encode();
        // The status_mask byte is the last byte of the single entry.
        let last = bytes.len() - 1;
        assert_eq!(bytes[last], STATUS_MASK_VALID);
        bytes[last] = 0b1000_0000; // set reserved bit 7
        assert!(matches!(
            SkipIndex::decode(&bytes, 100),
            Err(SpanSegError::Corrupted(_))
        ));
    }

    #[test]
    fn decode_rejects_truncated_entry() {
        // Truncate mid-way through the new duration/status fields: keep the
        // header plus everything up to (but not including) status_mask.
        let idx = SkipIndex::new(vec![entry(1, 0, 9)]);
        let bytes = idx.encode();
        let truncated = &bytes[..bytes.len() - 1];
        assert!(matches!(
            SkipIndex::decode(truncated, 100),
            Err(SpanSegError::Corrupted(_))
        ));
    }

    #[test]
    fn decode_never_panics_on_garbage() {
        use proptest::prelude::*;
        proptest!(|(bytes in proptest::collection::vec(any::<u8>(), 0..256))| {
            let _ = SkipIndex::decode(&bytes, 1 << 16);
        });
    }

    /// Pruning soundness: any block that actually contains a record matching the
    /// (trace_id, ts window) predicate must survive pruning.
    #[test]
    fn pruning_is_sound_over_random() {
        use proptest::prelude::*;
        proptest!(|(entries in proptest::collection::vec(
            (0u8..8, any::<i64>(), 0i64..1000), 1..200),
            qtrace in 0u8..8, qmin in any::<i64>(), qspan in 0i64..2000)| {
            let blocks: Vec<BlockEntry> = entries
                .iter()
                .map(|(t, base, span)| {
                    let min_start = *base;
                    let max_end = base.saturating_add(*span);
                    BlockEntry {
                        min_trace_id: [*t; 16],
                        max_trace_id: [*t; 16],
                        min_start_ts: min_start,
                        max_end_ts: max_end,
                        ..entry(*t, min_start, max_end)
                    }
                })
                .collect();
            let idx = SkipIndex::new(blocks.clone());
            let qmax = qmin.saturating_add(qspan);
            let tid = [qtrace; 16];
            let cands = idx.candidate_blocks(Some(&tid), qmin, qmax, None, None);
            for (i, e) in blocks.iter().enumerate() {
                let time_overlaps = e.max_end_ts >= qmin && e.min_start_ts <= qmax;
                let trace_hit = tid >= e.min_trace_id && tid <= e.max_trace_id;
                if time_overlaps && trace_hit {
                    prop_assert!(cands.contains(&i));
                }
            }
        });
    }

    /// Pruning soundness over random duration/status predicates: any block
    /// whose duration bound overlaps the query window AND whose status_mask
    /// shares a bit with the query mask must survive `candidate_blocks`,
    /// regardless of what random values the other blocks take.
    #[test]
    fn pruning_is_sound_over_random_duration_and_status() {
        use proptest::prelude::*;
        proptest!(|(entries in proptest::collection::vec(
            (any::<i64>(), 0i64..2000, 0u8..8u8), 1..200),
            qmin in any::<i64>(), qspan in 0i64..2000, qmask in 0u8..8u8)| {
            let blocks: Vec<BlockEntry> = entries
                .iter()
                .map(|(base, span, mask)| {
                    let min_duration_ns = *base;
                    let max_duration_ns = base.saturating_add(*span);
                    BlockEntry {
                        min_duration_ns,
                        max_duration_ns,
                        status_mask: *mask,
                        ..entry(0, i64::MIN, i64::MAX)
                    }
                })
                .collect();
            let idx = SkipIndex::new(blocks.clone());
            let qmax = qmin.saturating_add(qspan);
            let cands = idx.candidate_blocks(
                None, i64::MIN, i64::MAX, Some((qmin, qmax)), Some(qmask),
            );
            for (i, e) in blocks.iter().enumerate() {
                let duration_overlaps = e.max_duration_ns >= qmin && e.min_duration_ns <= qmax;
                let status_hit = (e.status_mask & qmask) != 0;
                if duration_overlaps && status_hit {
                    prop_assert!(cands.contains(&i));
                }
            }
        });
    }
}
