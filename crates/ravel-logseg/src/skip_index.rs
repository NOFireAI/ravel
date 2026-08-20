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

/// A resolved numeric-range prune arm for [`SkipIndex::candidate_blocks`]: the
/// dynamic column to probe and the inclusive bounds a query wants to keep.
///
/// `min_bits`/`max_bits` are in the same bit-pattern encoding
/// [`NumStat::min_bits`]/[`NumStat::max_bits`] store -- an `i64` as its
/// two's-complement `u64`, an `f64` as `to_bits`, a `bool` as `0`/`1` -- so the
/// overlap test reuses the exact type-aware order [`merge_stats`] folds under
/// (nothing re-implements per-type comparison). `None` is an open end. The
/// bounds are treated as inclusive, which is always the widen-only choice: a
/// strict SQL bound resolves to an inclusive arm here, so pruning can only ever
/// keep a block the exact residual would drop, never the reverse (ADR-0013).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NumRangeArm {
    pub column_id: u32,
    pub ty: FieldType,
    pub min_bits: Option<u64>,
    pub max_bits: Option<u64>,
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

/// Strict less-than over two bit patterns in the same per-type order
/// [`min_bits`]/[`max_bits`] fold under, so a range-overlap test and the stat
/// merge can never disagree on ordering. `a < b` iff `a` is the pair's minimum
/// and the two are not equal.
fn bits_lt(ty: FieldType, a: u64, b: u64) -> bool {
    a != b && min_bits(ty, a, b) == a
}

/// True when `arm`'s inclusive bounds cannot overlap `stat`'s recorded
/// `[min_bits, max_bits]` range -- the only case in which the arm proves the
/// entry holds no row it can match.
///
/// Null rows and NaN rows never satisfy a numeric range, so a stat's min/max
/// (which bound exactly the non-NaN resolved values, ADR-0095) is the whole
/// test; `null_count`/`has_nan` are irrelevant to it. A stat whose type does
/// not match the arm's proves nothing (the reader resolves an arm to one exact
/// `(name, type)` column, so this is defensive, not expected).
fn stat_disjoint(stat: &NumStat, arm: &NumRangeArm) -> bool {
    if stat.ty != arm.ty {
        return false;
    }
    // Query entirely below the block: its top is strictly under the stat's min.
    if let Some(qmax) = arm.max_bits
        && bits_lt(stat.ty, qmax, stat.min_bits)
    {
        return true;
    }
    // Query entirely above the block: its bottom is strictly over the stat's max.
    if let Some(qmin) = arm.min_bits
        && bits_lt(stat.ty, stat.max_bits, qmin)
    {
        return true;
    }
    false
}

/// True if some numeric arm proves `stats` holds no matching row. An arm whose
/// column has no stat in `stats` proves nothing and is skipped: absence is "no
/// information", never "no match" (ADR-0013, and the completeness contract in
/// this module's header). Pruning on an absent stat would silently drop correct
/// results, so this degrade-safe fallthrough is unconditional.
fn numeric_prunes(stats: &[NumStat], numeric: &[NumRangeArm]) -> bool {
    numeric.iter().any(|arm| {
        stats
            .iter()
            .find(|s| s.column_id == arm.column_id)
            .is_some_and(|stat| stat_disjoint(stat, arm))
    })
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

    /// Block indices whose level-0 entries survive the coarse ts/stream and
    /// numeric-range predicates. Sound: a block is included unless its bounds
    /// prove no record matches. `stream_refs`, when given, is the set of stream
    /// refs of interest; a block survives only if its `[min, max]` ref range
    /// contains at least one.
    ///
    /// `numeric` carries the prune-only numeric-range arms (ADR-0095 decision
    /// 6). Each probes one NumStat-eligible column at both tiers this method
    /// already prunes at: a whole level-1 group is skipped when its merged stat
    /// for the column proves no overlap, then a surviving group's individual
    /// level-0 blocks are skipped the same way. An arm whose column has no stat
    /// in the entry being tested prunes nothing there -- absence is "no
    /// information" (ADR-0013), which is why a level-1 group carrying no stat
    /// for the column is descended into rather than dropped, and a level-0 block
    /// with none survives. A group *does* carry a merged stat whenever any of
    /// its children resolve the column; a child that resolves none holds only
    /// nulls for it (write-path completeness, this module's header), and nulls
    /// never satisfy a range, so pruning such a group on its merged stat still
    /// drops no matching row.
    pub fn candidate_blocks(
        &self,
        ts_min: i64,
        ts_max: i64,
        stream_refs: Option<&[u32]>,
        numeric: &[NumRangeArm],
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
            if numeric_prunes(&group.stats, numeric) {
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
                if numeric_prunes(&e.stats, numeric) {
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
        let got = idx.candidate_blocks(30, 55, None, &[]);
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
        let got = idx.candidate_blocks(0, 1000, Some(&refs), &[]);
        assert_eq!(got, vec![7, 42]);
    }

    /// A level-0 entry spanning all ts/streams, carrying one i64 stat for
    /// `column_id` over `[min, max]`.
    fn i64_entry(idx: u64, column_id: u32, min: i64, max: i64) -> Level0Entry {
        let mut e = entry(idx, 0, 1000, 0, 0);
        e.record_count = 1;
        e.stats = vec![NumStat {
            column_id,
            ty: FieldType::I64,
            min_bits: min as u64,
            max_bits: max as u64,
            null_count: 0,
            has_nan: false,
        }];
        e
    }

    fn i64_arm(column_id: u32, min: Option<i64>, max: Option<i64>) -> NumRangeArm {
        NumRangeArm {
            column_id,
            ty: FieldType::I64,
            min_bits: min.map(|v| v as u64),
            max_bits: max.map(|v| v as u64),
        }
    }

    /// A numeric arm drops exactly the level-0 blocks whose stat range is
    /// disjoint from the query, at both an inclusive lower and upper bound, and
    /// keeps a block whose range overlaps.
    #[test]
    fn candidate_blocks_numeric_range_prunes_disjoint_blocks() {
        // Block 0: [0,10], block 1: [50,60], block 2: [100,110].
        let l0 = vec![
            i64_entry(0, 10, 0, 10),
            i64_entry(1, 10, 50, 60),
            i64_entry(2, 10, 100, 110),
        ];
        let idx = SkipIndex::build(l0);

        // Range [45, 65] overlaps only block 1.
        let arm = [i64_arm(10, Some(45), Some(65))];
        assert_eq!(
            idx.candidate_blocks(i64::MIN, i64::MAX, None, &arm),
            vec![1]
        );

        // Open upper bound: >= 55 keeps blocks 1 and 2, drops block 0.
        let arm = [i64_arm(10, Some(55), None)];
        assert_eq!(
            idx.candidate_blocks(i64::MIN, i64::MAX, None, &arm),
            vec![1, 2]
        );

        // Open lower bound: <= 5 keeps only block 0.
        let arm = [i64_arm(10, None, Some(5))];
        assert_eq!(
            idx.candidate_blocks(i64::MIN, i64::MAX, None, &arm),
            vec![0]
        );
    }

    /// The soundness rule of ADR-0095 decision 6: a block with NO stat for the
    /// arm's column is never pruned by that arm, even though a naive "the block
    /// holds no value in range" reading would drop it. Block 1 carries no stat
    /// for column 10; a range that excludes block 0's value must still keep
    /// block 1.
    #[test]
    fn candidate_blocks_numeric_range_never_prunes_a_block_missing_the_stat() {
        // Block 0 has a stat [0,10]; block 1 has NO stat for column 10. They are
        // in different level-1 groups (block 1 is the 65th block) so the group
        // summary cannot prune block 1 either: block 1's own group carries no
        // stat for the column and is descended into.
        let mut l0 = vec![i64_entry(0, 10, 0, 10)];
        for i in 1..FANOUT as u64 {
            // Filler blocks in group 0, also without the stat, ts-disjoint from
            // the query below so they never appear in the candidate set.
            l0.push(entry(i, 5000, 5000, 0, 0));
        }
        // Block 64: the no-stat block under test, in level-1 group 1.
        let mut no_stat = entry(FANOUT as u64, 0, 1000, 0, 0);
        no_stat.record_count = 1;
        no_stat.stats = Vec::new();
        l0.push(no_stat);
        let idx = SkipIndex::build(l0);
        assert_eq!(idx.l1.len(), 2, "block 64 forces a second level-1 group");

        // A range far above block 0's [0,10]. Block 0 is pruned; the no-stat
        // block (index 64) survives -- absence is no information.
        let arm = [i64_arm(10, Some(1_000_000), None)];
        let got = idx.candidate_blocks(0, 1000, None, &arm);
        assert!(
            !got.contains(&0),
            "block 0's stat is disjoint, so it prunes"
        );
        assert!(
            got.contains(&(FANOUT)),
            "a block with no stat for the column must never be pruned by the arm"
        );
    }

    /// The level-1 coarse tier prunes too: a whole group whose merged stat is
    /// disjoint from the query is skipped without descending. 128 blocks (two
    /// groups): group 0's values are all low, group 1's all high, so a
    /// high range drops every block of group 0 at the group tier.
    #[test]
    fn candidate_blocks_numeric_range_prunes_at_level1_group() {
        let mut l0 = Vec::new();
        for i in 0..FANOUT as u64 {
            l0.push(i64_entry(i, 10, 0, 100));
        }
        for i in FANOUT as u64..(2 * FANOUT) as u64 {
            l0.push(i64_entry(i, 10, 1000, 1100));
        }
        let idx = SkipIndex::build(l0);
        assert_eq!(idx.l1.len(), 2);

        // Range [1050, 1200] overlaps only group 1's blocks.
        let arm = [i64_arm(10, Some(1050), Some(1200))];
        let got = idx.candidate_blocks(i64::MIN, i64::MAX, None, &arm);
        assert_eq!(got.len(), FANOUT, "only the 64 blocks of group 1 survive");
        assert!(got.iter().all(|&b| b >= FANOUT));
    }

    /// f64 arms compare by `total_cmp` order (via the shared `min_bits` helper),
    /// and bool arms by their 0/1 bit, so both prune the same way i64 does.
    #[test]
    fn candidate_blocks_numeric_range_f64_and_bool() {
        let f = |bits_min: f64, bits_max: f64| {
            let mut e = entry(0, 0, 1000, 0, 0);
            e.record_count = 1;
            e.stats = vec![NumStat {
                column_id: 11,
                ty: FieldType::F64,
                min_bits: bits_min.to_bits(),
                max_bits: bits_max.to_bits(),
                null_count: 0,
                has_nan: false,
            }];
            e
        };
        let idx = SkipIndex::build(vec![f(1.0, 2.0)]);
        let arm = [NumRangeArm {
            column_id: 11,
            ty: FieldType::F64,
            min_bits: Some(3.0f64.to_bits()),
            max_bits: Some(4.0f64.to_bits()),
        }];
        assert!(
            idx.candidate_blocks(i64::MIN, i64::MAX, None, &arm)
                .is_empty(),
            "f64 [1,2] is disjoint from [3,4]"
        );
        let arm = [NumRangeArm {
            column_id: 11,
            ty: FieldType::F64,
            min_bits: Some(1.5f64.to_bits()),
            max_bits: None,
        }];
        assert_eq!(
            idx.candidate_blocks(i64::MIN, i64::MAX, None, &arm),
            vec![0]
        );

        // Bool block carrying only `false` (0), queried for `true` (1): pruned.
        let mut b = entry(0, 0, 1000, 0, 0);
        b.record_count = 1;
        b.stats = vec![NumStat {
            column_id: 12,
            ty: FieldType::Bool,
            min_bits: 0,
            max_bits: 0,
            null_count: 0,
            has_nan: false,
        }];
        let idx = SkipIndex::build(vec![b]);
        let arm = [NumRangeArm {
            column_id: 12,
            ty: FieldType::Bool,
            min_bits: Some(1),
            max_bits: Some(1),
        }];
        assert!(
            idx.candidate_blocks(i64::MIN, i64::MAX, None, &arm)
                .is_empty()
        );
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
            let cands = idx.candidate_blocks(qmin, qmax, Some(&refs), &[]);
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
