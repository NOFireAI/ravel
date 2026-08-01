//! SKIP_IDX section (docs/span-segment-format.md "SKIP_IDX").
//!
//! One entry per block, carrying its byte range and crc, record count, trace_id
//! bounds, and time-interval bounds. Two departures from RLOG's SKIP_IDX
//! (ADR-0041): the time bound is an interval `(min_start_ts, max_end_ts)` pruned
//! by overlap, not a single point range; and the identity bound is the block's
//! `(min_trace_id, max_trace_id)`, since records sort by `(trace_id, start_ts)`
//! and trace_id is the primary lookup key rather than a derived stream ref. The
//! index is a single level: RSPAN keeps this leaner than RLOG's two-level index
//! because a span object is a single sorted run and there is no separate
//! stream-ref dimension to summarize. Pruning is sound: a block is dropped only
//! when its bounds prove no record in it can match.

use crate::error::SpanSegError;
use crate::record::TRACE_ID_WIDTH;
use crate::varint::{get_ivarint, get_uvarint, put_ivarint, put_uvarint};

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
    /// `[min_start_ts, max_end_ts]` interval overlaps it.
    pub fn candidate_blocks(
        &self,
        trace_id: Option<&[u8; 16]>,
        ts_min: i64,
        ts_max: i64,
    ) -> Vec<usize> {
        let mut out = Vec::new();
        for (i, e) in self.blocks.iter().enumerate() {
            if block_pruned(e, trace_id, ts_min, ts_max) {
                continue;
            }
            out.push(i);
        }
        out
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
        }
        out
    }

    /// Decodes the uncompressed section form. Rejects a block count over
    /// `max_blocks`, truncation, and trailing bytes.
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
            blocks.push(BlockEntry {
                block_offset,
                block_len,
                block_crc32c,
                record_count,
                min_trace_id,
                max_trace_id,
                min_start_ts,
                max_end_ts,
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
/// pruned when its time interval is disjoint from the query window, or (when a
/// trace_id is given) the trace_id falls outside the block's trace_id range.
pub fn block_pruned(
    entry: &BlockEntry,
    trace_id: Option<&[u8; 16]>,
    ts_min: i64,
    ts_max: i64,
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
        }
    }

    #[test]
    fn roundtrip() {
        let idx = SkipIndex::new(vec![entry(1, 0, 100), entry(2, 50, 300)]);
        let got = SkipIndex::decode(&idx.encode(), 1000).expect("decode");
        assert_eq!(got, idx);
    }

    #[test]
    fn interval_overlap_boundary_cases() {
        // Block interval [100, 200].
        let e = entry(1, 100, 200);
        // Window fully before the block: [0, 99] -> pruned.
        assert!(block_pruned(&e, None, 0, 99));
        // Window fully after the block: [201, 300] -> pruned.
        assert!(block_pruned(&e, None, 201, 300));
        // Touch at the left edge: [0, 100] -> kept (max_end 200 >= 0, min_start
        // 100 <= 100).
        assert!(!block_pruned(&e, None, 0, 100));
        // Touch at the right edge: [200, 300] -> kept.
        assert!(!block_pruned(&e, None, 200, 300));
        // Partial overlap at each edge.
        assert!(!block_pruned(&e, None, 50, 150));
        assert!(!block_pruned(&e, None, 150, 250));
        // Window fully containing the block.
        assert!(!block_pruned(&e, None, 0, 1000));
        // Block fully containing the window.
        assert!(!block_pruned(&e, None, 120, 130));
    }

    #[test]
    fn trace_id_pruning() {
        // Block covers trace ids [2..=4] and interval [0, 1000].
        let e = BlockEntry {
            min_trace_id: [2u8; 16],
            max_trace_id: [4u8; 16],
            ..entry(0, 0, 1000)
        };
        assert!(block_pruned(&e, Some(&[1u8; 16]), 0, 1000));
        assert!(!block_pruned(&e, Some(&[2u8; 16]), 0, 1000));
        assert!(!block_pruned(&e, Some(&[3u8; 16]), 0, 1000));
        assert!(!block_pruned(&e, Some(&[4u8; 16]), 0, 1000));
        assert!(block_pruned(&e, Some(&[5u8; 16]), 0, 1000));
        // A matching trace_id in a disjoint time window is still pruned.
        assert!(block_pruned(&e, Some(&[3u8; 16]), 2000, 3000));
    }

    #[test]
    fn candidate_blocks_selects_survivors() {
        let idx = SkipIndex::new(vec![
            entry(1, 0, 100),
            entry(2, 100, 200),
            entry(3, 200, 300),
        ]);
        assert_eq!(idx.candidate_blocks(None, 120, 130), vec![1]);
        assert_eq!(idx.candidate_blocks(Some(&[3u8; 16]), 0, 1000), vec![2]);
        // No overlap at all.
        assert!(idx.candidate_blocks(None, 400, 500).is_empty());
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
            let cands = idx.candidate_blocks(Some(&tid), qmin, qmax);
            for (i, e) in blocks.iter().enumerate() {
                let time_overlaps = e.max_end_ts >= qmin && e.min_start_ts <= qmax;
                let trace_hit = tid >= e.min_trace_id && tid <= e.max_trace_id;
                if time_overlaps && trace_hit {
                    prop_assert!(cands.contains(&i));
                }
            }
        });
    }
}
