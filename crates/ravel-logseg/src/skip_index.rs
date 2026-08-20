//! SKIP_IDX section (docs/log-segment-format.md "SKIP_IDX").
//!
//! A two-level min/max index. Level 0 holds one entry per block (its byte
//! range and crc, record count, ts and stream_ref bounds, and per-numeric
//! column stats). Level 1 holds one entry per 64 blocks, merging its
//! children's bounds. [`SkipIndex::candidate_blocks`] probes level 1 first and
//! descends only into surviving groups, so pruning cost scales with surviving
//! data. Pruning is sound (ADR-0013): a block is dropped only when its bounds
//! prove no record in it can match.
//!
//! Since RLOG v3 (ADR-0095) a numeric stat bounds the value each row *resolves*
//! for its column's attribute name -- the row's resource and scope layers
//! overridden by its own attributes, cross-type duplicates resolved -- not the
//! row's raw columnar occurrence (see [`crate::block::NumStat`]). A row that
//! carries no occurrence of the name is bounded too, by its stream-level value,
//! so a level-0 entry carries a stat for every column its rows resolve, not just
//! the ones its block has pages for. The byte grammar is unchanged, which is
//! exactly why the trailer version had to move: nothing in these bytes tells a
//! v2 stat from a v3 one.
//!
//! [`merge_stats`] relies on that completeness: it folds only the children that
//! carry a stat for a column, so a level-0 entry that omitted one would be read
//! as "no information about this column" and its rows would silently drop out of
//! the level-1 bounds a query prunes on.

use crate::block::NumStat;
use crate::error::LogSegError;
use crate::record::FieldType;
use crate::varint::{get_ivarint, get_uvarint, put_ivarint, put_uvarint};

/// Level-1 fanout: one merged entry per this many level-0 blocks.
pub const FANOUT: usize = 64;

/// One level-0 (per-block) skip entry.
#[derive(Clone, Debug, PartialEq)]
pub struct Level0Entry {
    pub block_offset: u64,
    pub block_len: u64,
    pub block_crc32c: u32,
    pub record_count: u32,
    pub min_ts: i64,
    pub max_ts: i64,
    pub min_stream_ref: u32,
    pub max_stream_ref: u32,
    pub stats: Vec<NumStat>,
}

/// One level-1 (per-64-block) skip entry: a level-0 entry with the byte range
/// and crc omitted and its children's bounds merged.
#[derive(Clone, Debug, PartialEq)]
pub struct Level1Entry {
    pub record_count: u32,
    pub min_ts: i64,
    pub max_ts: i64,
    pub min_stream_ref: u32,
    pub max_stream_ref: u32,
    pub stats: Vec<NumStat>,
}

/// The decoded skip index: level 0 and its level-1 summary.
#[derive(Clone, Debug, PartialEq)]
pub struct SkipIndex {
    pub l0: Vec<Level0Entry>,
    pub l1: Vec<Level1Entry>,
}

/// Merges the stats of `children` by column id (type-aware min/max, summed
/// null counts, OR-ed NaN flags).
fn merge_stats(children: &[Level0Entry]) -> Vec<NumStat> {
    let mut merged: Vec<NumStat> = Vec::new();
    for child in children {
        for s in &child.stats {
            if let Some(m) = merged.iter_mut().find(|m| m.column_id == s.column_id) {
                m.min_bits = min_bits(m.ty, m.min_bits, s.min_bits);
                m.max_bits = max_bits(m.ty, m.max_bits, s.max_bits);
                m.null_count = m.null_count.saturating_add(s.null_count);
                m.has_nan |= s.has_nan;
            } else {
                merged.push(*s);
            }
        }
    }
    merged
}

fn min_bits(ty: FieldType, a: u64, b: u64) -> u64 {
    match ty {
        FieldType::F64 => {
            if f64::from_bits(a).total_cmp(&f64::from_bits(b)).is_le() {
                a
            } else {
                b
            }
        }
        FieldType::I64 => {
            if (a as i64) <= (b as i64) {
                a
            } else {
                b
            }
        }
        _ => a.min(b),
    }
}

fn max_bits(ty: FieldType, a: u64, b: u64) -> u64 {
    match ty {
        FieldType::F64 => {
            if f64::from_bits(a).total_cmp(&f64::from_bits(b)).is_ge() {
                a
            } else {
                b
            }
        }
        FieldType::I64 => {
            if (a as i64) >= (b as i64) {
                a
            } else {
                b
            }
        }
        _ => a.max(b),
    }
}

impl SkipIndex {
    /// Builds the index from level-0 entries, deriving level 1 at fanout 64.
    pub fn build(l0: Vec<Level0Entry>) -> Self {
        let mut l1 = Vec::with_capacity(l0.len().div_ceil(FANOUT));
        for chunk in l0.chunks(FANOUT) {
            let mut record_count: u32 = 0;
            let mut min_ts = i64::MAX;
            let mut max_ts = i64::MIN;
            let mut min_stream_ref = u32::MAX;
            let mut max_stream_ref = 0u32;
            for e in chunk {
                record_count = record_count.saturating_add(e.record_count);
                min_ts = min_ts.min(e.min_ts);
                max_ts = max_ts.max(e.max_ts);
                min_stream_ref = min_stream_ref.min(e.min_stream_ref);
                max_stream_ref = max_stream_ref.max(e.max_stream_ref);
            }
            l1.push(Level1Entry {
                record_count,
                min_ts,
                max_ts,
                min_stream_ref,
                max_stream_ref,
                stats: merge_stats(chunk),
            });
        }
        SkipIndex { l0, l1 }
    }

    /// Block indices whose level-0 entries survive the coarse ts/stream
    /// predicate. Sound: a block is included unless its bounds prove no record
    /// matches. `stream_refs`, when given, is the set of stream refs of
    /// interest; a block survives only if its `[min, max]` ref range contains
    /// at least one.
    pub fn candidate_blocks(
        &self,
        ts_min: i64,
        ts_max: i64,
        stream_refs: Option<&[u32]>,
    ) -> Vec<usize> {
        let mut out = Vec::new();
        for (g, group) in self.l1.iter().enumerate() {
            // Coarse skip: whole 64-block group proven disjoint.
            if group.max_ts < ts_min || group.min_ts > ts_max {
                continue;
            }
            if stream_refs.is_some_and(|refs| {
                !refs_intersect(refs, group.min_stream_ref, group.max_stream_ref)
            }) {
                continue;
            }
            let start = g * FANOUT;
            let end = (start + FANOUT).min(self.l0.len());
            for (j, e) in self.l0[start..end].iter().enumerate() {
                if e.max_ts < ts_min || e.min_ts > ts_max {
                    continue;
                }
                if stream_refs
                    .is_some_and(|refs| !refs_intersect(refs, e.min_stream_ref, e.max_stream_ref))
                {
                    continue;
                }
                out.push(start + j);
            }
        }
        out
    }

    /// Serializes the section in its uncompressed form.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(self.l0.len() as u32).to_le_bytes());
        for e in &self.l0 {
            put_uvarint(&mut out, e.block_offset);
            put_uvarint(&mut out, e.block_len);
            out.extend_from_slice(&e.block_crc32c.to_le_bytes());
            put_uvarint(&mut out, u64::from(e.record_count));
            put_ivarint(&mut out, e.min_ts);
            put_ivarint(&mut out, e.max_ts);
            put_uvarint(&mut out, u64::from(e.min_stream_ref));
            put_uvarint(&mut out, u64::from(e.max_stream_ref));
            encode_stats(&mut out, &e.stats);
        }
        out.extend_from_slice(&(self.l1.len() as u32).to_le_bytes());
        for e in &self.l1 {
            put_uvarint(&mut out, u64::from(e.record_count));
            put_ivarint(&mut out, e.min_ts);
            put_ivarint(&mut out, e.max_ts);
            put_uvarint(&mut out, u64::from(e.min_stream_ref));
            put_uvarint(&mut out, u64::from(e.max_stream_ref));
            encode_stats(&mut out, &e.stats);
        }
        out
    }

    /// Decodes the uncompressed section form. Rejects a block count over
    /// `max_blocks`, an unknown stat type byte, truncation, and trailing bytes.
    pub fn decode(bytes: &[u8], max_blocks: u64) -> Result<Self, LogSegError> {
        let mut pos = 0usize;
        let count0 = read_u32(bytes, &mut pos)?;
        if u64::from(count0) > max_blocks {
            return Err(LogSegError::Corrupted(format!(
                "skip l0 count {count0} over cap {max_blocks}"
            )));
        }
        let mut l0 = Vec::with_capacity((count0 as usize).min(1 << 16));
        for _ in 0..count0 {
            let block_offset = get_uvarint(bytes, &mut pos)?;
            let block_len = get_uvarint(bytes, &mut pos)?;
            let block_crc32c = read_u32(bytes, &mut pos)?;
            let record_count = read_u32_varint(bytes, &mut pos)?;
            let min_ts = get_ivarint(bytes, &mut pos)?;
            let max_ts = get_ivarint(bytes, &mut pos)?;
            let min_stream_ref = read_u32_varint(bytes, &mut pos)?;
            let max_stream_ref = read_u32_varint(bytes, &mut pos)?;
            let stats = decode_stats(bytes, &mut pos)?;
            l0.push(Level0Entry {
                block_offset,
                block_len,
                block_crc32c,
                record_count,
                min_ts,
                max_ts,
                min_stream_ref,
                max_stream_ref,
                stats,
            });
        }
        let count1 = read_u32(bytes, &mut pos)?;
        if u64::from(count1) > max_blocks {
            return Err(LogSegError::Corrupted(format!(
                "skip l1 count {count1} over cap {max_blocks}"
            )));
        }
        let mut l1 = Vec::with_capacity((count1 as usize).min(1 << 16));
        for _ in 0..count1 {
            let record_count = read_u32_varint(bytes, &mut pos)?;
            let min_ts = get_ivarint(bytes, &mut pos)?;
            let max_ts = get_ivarint(bytes, &mut pos)?;
            let min_stream_ref = read_u32_varint(bytes, &mut pos)?;
            let max_stream_ref = read_u32_varint(bytes, &mut pos)?;
            let stats = decode_stats(bytes, &mut pos)?;
            l1.push(Level1Entry {
                record_count,
                min_ts,
                max_ts,
                min_stream_ref,
                max_stream_ref,
                stats,
            });
        }
        if pos != bytes.len() {
            return Err(LogSegError::Corrupted("skip index trailing bytes".into()));
        }
        Ok(SkipIndex { l0, l1 })
    }
}

fn refs_intersect(refs: &[u32], min: u32, max: u32) -> bool {
    refs.iter().any(|r| *r >= min && *r <= max)
}

fn encode_stats(out: &mut Vec<u8>, stats: &[NumStat]) {
    put_uvarint(out, stats.len() as u64);
    for s in stats {
        put_uvarint(out, u64::from(s.column_id));
        out.push(s.ty.to_u8());
        out.extend_from_slice(&s.min_bits.to_le_bytes());
        out.extend_from_slice(&s.max_bits.to_le_bytes());
        put_uvarint(out, u64::from(s.null_count));
        out.push(u8::from(s.has_nan));
    }
}

fn decode_stats(bytes: &[u8], pos: &mut usize) -> Result<Vec<NumStat>, LogSegError> {
    let count = get_uvarint(bytes, pos)?;
    if count > 4096 {
        return Err(LogSegError::Corrupted(format!(
            "stat count {count} over cap"
        )));
    }
    let mut stats = Vec::with_capacity(count.min(4096) as usize);
    for _ in 0..count {
        let column_id = read_u32_varint(bytes, pos)?;
        let ty_byte = *bytes
            .get(*pos)
            .ok_or_else(|| LogSegError::Corrupted("stat truncated at type".into()))?;
        *pos += 1;
        let ty = FieldType::from_u8(ty_byte)
            .ok_or_else(|| LogSegError::Corrupted(format!("stat bad type {ty_byte}")))?;
        let min_bits = read_u64(bytes, pos)?;
        let max_bits = read_u64(bytes, pos)?;
        let null_count = read_u32_varint(bytes, pos)?;
        let has_nan_byte = *bytes
            .get(*pos)
            .ok_or_else(|| LogSegError::Corrupted("stat truncated at has_nan".into()))?;
        *pos += 1;
        stats.push(NumStat {
            column_id,
            ty,
            min_bits,
            max_bits,
            null_count,
            has_nan: has_nan_byte != 0,
        });
    }
    Ok(stats)
}

fn read_u32(bytes: &[u8], pos: &mut usize) -> Result<u32, LogSegError> {
    let s = bytes
        .get(*pos..*pos + 4)
        .ok_or_else(|| LogSegError::Corrupted("skip truncated u32".into()))?;
    *pos += 4;
    Ok(u32::from_le_bytes([s[0], s[1], s[2], s[3]]))
}

fn read_u64(bytes: &[u8], pos: &mut usize) -> Result<u64, LogSegError> {
    let s = bytes
        .get(*pos..*pos + 8)
        .ok_or_else(|| LogSegError::Corrupted("skip truncated u64".into()))?;
    let mut a = [0u8; 8];
    a.copy_from_slice(s);
    *pos += 8;
    Ok(u64::from_le_bytes(a))
}

fn read_u32_varint(bytes: &[u8], pos: &mut usize) -> Result<u32, LogSegError> {
    u32::try_from(get_uvarint(bytes, pos)?)
        .map_err(|_| LogSegError::Corrupted("skip u32 varint range".into()))
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn entry(idx: u64, min_ts: i64, max_ts: i64, min_ref: u32, max_ref: u32) -> Level0Entry {
        Level0Entry {
            block_offset: idx * 100,
            block_len: 100,
            block_crc32c: idx as u32,
            record_count: 8,
            min_ts,
            max_ts,
            min_stream_ref: min_ref,
            max_stream_ref: max_ref,
            stats: Vec::new(),
        }
    }

    #[test]
    fn roundtrip_and_level1_bounds() {
        let mut l0 = Vec::new();
        for i in 0..200u64 {
            let t = i as i64 * 10;
            l0.push(entry(i, t, t + 9, (i % 5) as u32, (i % 5) as u32));
        }
        let idx = SkipIndex::build(l0);
        // 200 blocks -> ceil(200/64) = 4 level-1 groups.
        assert_eq!(idx.l1.len(), 4);
        // Group 0 covers blocks 0..64: ts 0..=639.
        assert_eq!(idx.l1[0].min_ts, 0);
        assert_eq!(idx.l1[0].max_ts, 639);
        // Round-trip.
        let bytes = idx.encode();
        let got = SkipIndex::decode(&bytes, 100_000).expect("decode");
        assert_eq!(got, idx);
    }

    #[test]
    fn candidate_blocks_ts_range() {
        // 100 blocks, block i covers ts [i*10, i*10+9]. Query [30, 55] hits
        // blocks 3,4,5 (block 5 covers 50..59, overlaps 55).
        let mut l0 = Vec::new();
        for i in 0..100u64 {
            let t = i as i64 * 10;
            l0.push(entry(i, t, t + 9, 0, 0));
        }
        let idx = SkipIndex::build(l0);
        let got = idx.candidate_blocks(30, 55, None);
        assert_eq!(got, vec![3, 4, 5]);
    }

    #[test]
    fn candidate_blocks_stream_filter() {
        let mut l0 = Vec::new();
        for i in 0..100u64 {
            // Every block spans all ts; stream ref = i.
            l0.push(entry(i, 0, 1000, i as u32, i as u32));
        }
        let idx = SkipIndex::build(l0);
        let refs = [7u32, 42u32];
        let got = idx.candidate_blocks(0, 1000, Some(&refs));
        assert_eq!(got, vec![7, 42]);
    }

    #[test]
    fn pruning_is_sound_over_random() {
        use proptest::prelude::*;
        proptest!(|(entries in proptest::collection::vec(
            (any::<i64>(), 0i64..1000, 0u32..10, 0u32..10), 1..300),
            qmin in any::<i64>(), qspan in 0i64..2000, qref in 0u32..10)| {
            let mut l0 = Vec::new();
            for (i, (base, span, a, b)) in entries.iter().enumerate() {
                let min_ts = *base;
                let max_ts = base.saturating_add(*span);
                let (min_ref, max_ref) = (a.min(b), a.max(b));
                let mut e = entry(i as u64, min_ts, max_ts, *min_ref, *max_ref);
                e.record_count = 1;
                l0.push(e);
            }
            let idx = SkipIndex::build(l0.clone());
            let qmax = qmin.saturating_add(qspan);
            let refs = [qref];
            let cands = idx.candidate_blocks(qmin, qmax, Some(&refs));
            // Soundness: any block that actually contains a matching (ts,ref)
            // pair must be a candidate.
            for (i, e) in l0.iter().enumerate() {
                let ts_overlaps = e.max_ts >= qmin && e.min_ts <= qmax;
                let ref_hit = qref >= e.min_stream_ref && qref <= e.max_stream_ref;
                if ts_overlaps && ref_hit {
                    prop_assert!(cands.contains(&i));
                }
            }
        });
    }

    /// A level-0 entry carrying one stat of each numeric type, for the
    /// stat-grammar corruption tests below.
    fn entry_with_stats(idx: u64) -> Level0Entry {
        let mut e = entry(idx, 0, 9, 0, 0);
        e.stats = vec![
            NumStat {
                column_id: 10,
                ty: FieldType::I64,
                min_bits: (-7i64) as u64,
                max_bits: 900i64 as u64,
                null_count: 3,
                has_nan: false,
            },
            NumStat {
                column_id: 11,
                ty: FieldType::F64,
                min_bits: (-1.5f64).to_bits(),
                max_bits: 2.5f64.to_bits(),
                null_count: 0,
                has_nan: true,
            },
            NumStat {
                column_id: 12,
                ty: FieldType::Bool,
                min_bits: 0,
                max_bits: 1,
                null_count: 1,
                has_nan: false,
            },
        ];
        e
    }

    /// An index carrying v3 numeric stats round-trips exactly, including the
    /// merged level-1 stats: the semantics changed in v3, the encoding did not,
    /// so a decode must reproduce every stat field bit-for-bit.
    #[test]
    fn stats_round_trip_including_level1_merge() {
        let l0: Vec<Level0Entry> = (0..3u64).map(entry_with_stats).collect();
        let idx = SkipIndex::build(l0);
        let got = SkipIndex::decode(&idx.encode(), 100).expect("decode");
        assert_eq!(got, idx);
        // Level 1 merged the three children's identical stats: same bounds,
        // summed null counts, OR-ed NaN flag.
        let l1 = &got.l1[0].stats;
        let i = l1.iter().find(|s| s.column_id == 10).expect("i64 stat");
        assert_eq!(i.min_bits as i64, -7);
        assert_eq!(i.max_bits as i64, 900);
        assert_eq!(i.null_count, 9);
        assert!(l1.iter().any(|s| s.column_id == 11 && s.has_nan));
    }

    /// Truncating a stat-bearing SKIP_IDX at every prefix length is always a
    /// typed `Corrupted` error, never a panic and never a short read that
    /// silently drops entries. Every field of the stat grammar (varint column
    /// id, type byte, two 8-byte bit patterns, varint null count, has_nan byte)
    /// falls inside some prefix, so this covers each of their truncation paths.
    #[test]
    fn truncated_stats_are_typed_errors() {
        let bytes = SkipIndex::build((0..2u64).map(entry_with_stats).collect()).encode();
        for cut in 0..bytes.len() {
            match SkipIndex::decode(&bytes[..cut], 100) {
                Err(LogSegError::Corrupted(_)) => {}
                other => panic!("prefix of {cut} byte(s) must be Corrupted, got {other:?}"),
            }
        }
        assert!(
            SkipIndex::decode(&bytes, 100).is_ok(),
            "the whole thing decodes"
        );
    }

    /// Flipping any single byte of a stat-bearing SKIP_IDX either fails with a
    /// typed `Corrupted` error or decodes to some index that is itself
    /// canonical (re-encoding and decoding again reproduces it). It never
    /// panics and never yields a value the encoder could not have written: a
    /// flip inside a `min_bits` field really is indistinguishable from a
    /// different legitimate bound, which is why the section carries a crc
    /// (verified before this decoder ever runs on a real object).
    #[test]
    fn flipped_stat_bytes_never_panic_and_stay_canonical() {
        use proptest::prelude::*;
        let bytes = SkipIndex::build((0..2u64).map(entry_with_stats).collect()).encode();
        proptest!(|(at in any::<usize>(), xor in any::<u8>())| {
            let mut m = bytes.clone();
            let i = at % m.len();
            m[i] ^= xor | 1;
            match SkipIndex::decode(&m, 100) {
                Ok(idx) => {
                    let again = SkipIndex::decode(&idx.encode(), 100).expect("re-decode");
                    prop_assert_eq!(again, idx);
                }
                Err(LogSegError::Corrupted(_)) => {}
                Err(other) => prop_assert!(false, "expected Corrupted, got {:?}", other),
            }
        });
    }

    #[test]
    fn decode_rejects_cap_and_trailing() {
        let idx = SkipIndex::build(vec![entry(0, 0, 9, 0, 0)]);
        let bytes = idx.encode();
        assert!(matches!(
            SkipIndex::decode(&bytes, 0),
            Err(LogSegError::Corrupted(_))
        ));
        let mut extra = bytes.clone();
        extra.push(0);
        assert!(matches!(
            SkipIndex::decode(&extra, 100),
            Err(LogSegError::Corrupted(_))
        ));
    }
}
