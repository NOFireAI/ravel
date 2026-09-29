//! The RLOG BLOOM section (docs/log-segment-format.md "BLOOM"): the sorted
//! list of column ids the filters cover under its own crc32c, then one blocked
//! bloom filter per row block, each framed with its own crc32c so a single
//! entry is readable and verifiable alone.
//!
//! RSPAN's BLOOM keeps the uncovered container and power-of-two filters of
//! `ravel_codec::bloom_section`; this module is RLOG's own form (ADR-2135).

use std::ops::Range;

use crate::bloom::BloomView;
use crate::error::LogSegError;
use crate::field_dir::FieldDir;
use crate::record::FIRST_DYNAMIC_COL;
use crate::varint::{get_uvarint, put_uvarint};

/// Serializes the section: `covered_count` u32, the covered column ids as
/// varints in ascending order, `covered_crc32c` u32 over those list bytes
/// (count and ids), `count` u32, then per entry `entry_len` varint, `crc32c`
/// u32 over the entry bytes, and the entry itself.
pub fn encode_rlog_bloom_section(covered: &[u32], entries: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(covered.len() as u32).to_le_bytes());
    for &cid in covered {
        put_uvarint(&mut out, u64::from(cid));
    }
    let list_crc = crc32c::crc32c(&out);
    out.extend_from_slice(&list_crc.to_le_bytes());
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for e in entries {
        put_uvarint(&mut out, e.len() as u64);
        out.extend_from_slice(&crc32c::crc32c(e).to_le_bytes());
        out.extend_from_slice(e);
    }
    out
}

/// A parsed BLOOM section: the covered columns and each entry's byte range, so
/// entries are addressable in O(1) after one linear pass.
pub struct RlogBloomSection<'a> {
    covered: Vec<u32>,
    bytes: &'a [u8],
    ranges: Vec<(u32, Range<usize>)>,
}

fn read_u32(bytes: &[u8], pos: &mut usize, what: &str) -> Result<u32, LogSegError> {
    let b = bytes
        .get(*pos..*pos + 4)
        .ok_or_else(|| LogSegError::Corrupted(format!("bloom section truncated at {what}")))?;
    *pos += 4;
    Ok(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
}

impl<'a> RlogBloomSection<'a> {
    /// Parses the section. Rejects truncation, a covered list whose crc does
    /// not match, an entry length that runs past the section, trailing bytes,
    /// and a covered list that is not strictly ascending or names a column that
    /// is neither a fixed column nor a `field_dir` entry. The list crc is
    /// checked before any id is validated or used; the per-entry crc is checked
    /// lazily in [`RlogBloomSection::entry`].
    pub fn parse(bytes: &'a [u8], field_dir: &FieldDir) -> Result<Self, LogSegError> {
        let mut pos = 0usize;
        let covered_count = read_u32(bytes, &mut pos, "covered count")?;
        // Every covered id is a distinct fixed or FIELD_DIR column, so the
        // count cannot exceed their total; checked before any allocation.
        let known = u64::from(FIRST_DYNAMIC_COL) + field_dir.len() as u64;
        if u64::from(covered_count) > known {
            return Err(LogSegError::Corrupted(format!(
                "bloom covers {covered_count} columns, more than the {known} the object has"
            )));
        }
        let mut covered = Vec::with_capacity(covered_count as usize);
        for _ in 0..covered_count {
            let cid = u32::try_from(get_uvarint(bytes, &mut pos)?)
                .map_err(|_| LogSegError::Corrupted("bloom covered column id range".into()))?;
            covered.push(cid);
        }
        let list_end = pos;
        let list_crc = read_u32(bytes, &mut pos, "covered crc")?;
        if crc32c::crc32c(&bytes[..list_end]) != list_crc {
            return Err(LogSegError::Corrupted(
                "bloom covered column list crc mismatch".into(),
            ));
        }
        let mut dynamic_ids: Vec<u32> = field_dir.entries().iter().map(|e| e.column_id).collect();
        dynamic_ids.sort_unstable();
        for pair in covered.windows(2) {
            if pair[1] <= pair[0] {
                return Err(LogSegError::Corrupted(format!(
                    "bloom covered columns not strictly ascending: {} after {}",
                    pair[1], pair[0]
                )));
            }
        }
        for &cid in &covered {
            if cid >= FIRST_DYNAMIC_COL && dynamic_ids.binary_search(&cid).is_err() {
                return Err(LogSegError::Corrupted(format!(
                    "bloom covers column {cid}, which FIELD_DIR does not name"
                )));
            }
        }
        let count = read_u32(bytes, &mut pos, "count")?;
        let mut ranges = Vec::with_capacity((count as usize).min(1 << 16));
        for _ in 0..count {
            let entry_len = usize::try_from(get_uvarint(bytes, &mut pos)?)
                .map_err(|_| LogSegError::Corrupted("bloom entry len range".into()))?;
            let crc = read_u32(bytes, &mut pos, "entry crc")?;
            let end = pos
                .checked_add(entry_len)
                .ok_or_else(|| LogSegError::Corrupted("bloom entry overflow".into()))?;
            if end > bytes.len() {
                return Err(LogSegError::Corrupted("bloom entry past section".into()));
            }
            ranges.push((crc, pos..end));
            pos = end;
        }
        if pos != bytes.len() {
            return Err(LogSegError::Corrupted(
                "bloom section trailing bytes".into(),
            ));
        }
        Ok(RlogBloomSection {
            covered,
            bytes,
            ranges,
        })
    }

    /// The covered column ids, ascending.
    pub fn covered(&self) -> &[u32] {
        &self.covered
    }

    /// Whether the filters hold keys for `column_id`. A filter probe for an
    /// uncovered column proves nothing.
    pub fn covers(&self, column_id: u32) -> bool {
        self.covered.binary_search(&column_id).is_ok()
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.ranges.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    /// The bloom view for entry `index`, verifying its crc first. An index out
    /// of range, a crc mismatch, or an `m_bits` that is not a nonzero multiple
    /// of 512 is `Corrupted`.
    pub fn entry(&self, index: usize) -> Result<BloomView<'a>, LogSegError> {
        let (crc, range) = self
            .ranges
            .get(index)
            .ok_or_else(|| LogSegError::Corrupted(format!("bloom index {index} out of range")))?;
        let entry = &self.bytes[range.clone()];
        if crc32c::crc32c(entry) != *crc {
            return Err(LogSegError::Corrupted(format!(
                "bloom entry {index} crc mismatch"
            )));
        }
        Ok(BloomView::parse_exact(entry)?)
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::bloom::BloomBuilder;
    use crate::field_dir::FieldEntry;
    use crate::record::{COL_BODY, COL_SEVERITY_TEXT, FieldType};

    fn field_dir() -> FieldDir {
        FieldDir::new(vec![FieldEntry {
            name: "k".into(),
            ty: FieldType::Str,
            column_id: FIRST_DYNAMIC_COL,
            present_blocks: 1,
            null_count: 0,
        }])
    }

    fn entries() -> Vec<Vec<u8>> {
        let mut a = BloomBuilder::new(1);
        a.insert(COL_BODY, b"timeout");
        let mut b = BloomBuilder::new(1);
        b.insert(COL_BODY, b"connection");
        vec![a.finish_exact(), b.finish_exact()]
    }

    #[test]
    fn round_trips_coverage_and_entries() {
        let covered = [COL_SEVERITY_TEXT, COL_BODY, FIRST_DYNAMIC_COL];
        let bytes = encode_rlog_bloom_section(&covered, &entries());
        let s = RlogBloomSection::parse(&bytes, &field_dir()).expect("parse");
        assert_eq!(s.covered(), covered);
        assert!(s.covers(COL_BODY));
        assert!(!s.covers(COL_BODY + 1));
        assert_eq!(s.len(), 2);
        assert!(s.entry(0).expect("e0").may_contain(COL_BODY, b"timeout"));
        assert!(!s.entry(0).expect("e0").may_contain(COL_BODY, b"connection"));
        assert!(s.entry(1).expect("e1").may_contain(COL_BODY, b"connection"));
        assert!(matches!(s.entry(2), Err(LogSegError::Corrupted(_))));
    }

    #[test]
    fn rejects_truncation_and_trailing_bytes() {
        let bytes = encode_rlog_bloom_section(&[COL_BODY], &entries());
        for cut in 0..bytes.len() {
            assert!(
                RlogBloomSection::parse(&bytes[..cut], &field_dir()).is_err(),
                "cut at {cut}"
            );
        }
        let mut long = bytes.clone();
        long.push(0);
        assert!(RlogBloomSection::parse(&long, &field_dir()).is_err());
    }

    #[test]
    fn rejects_an_entry_with_a_crc_flip() {
        let mut bytes = encode_rlog_bloom_section(&[COL_BODY], &entries());
        let last = bytes.len() - 1;
        bytes[last] ^= 1;
        let s = RlogBloomSection::parse(&bytes, &field_dir()).expect("framing");
        assert!(matches!(s.entry(1), Err(LogSegError::Corrupted(_))));
        s.entry(0).expect("untouched entry");
    }

    /// Every single-bit flip in the covered list or its crc is refused. A flip
    /// that keeps the framing (bits 0 to 6 of an id byte, any crc bit) is
    /// refused by the list crc itself, which is the check the structural rules
    /// alone lack: most of those flips leave an ascending list of known ids.
    #[test]
    fn rejects_any_bit_flip_in_the_covered_list_or_its_crc() {
        let covered = [COL_SEVERITY_TEXT, COL_BODY, FIRST_DYNAMIC_COL];
        let bytes = encode_rlog_bloom_section(&covered, &entries());
        // 4 count bytes, then one varint byte per id (each below 128), then the
        // 4 crc bytes.
        let ids = 4..4 + covered.len();
        let crc = ids.end..ids.end + 4;
        let mut crc_refusals = 0;
        for i in 0..crc.end {
            for bit in 0..8 {
                let mut flipped = bytes.clone();
                flipped[i] ^= 1 << bit;
                let framing_kept = crc.contains(&i) || (ids.contains(&i) && bit < 7);
                match RlogBloomSection::parse(&flipped, &field_dir()) {
                    Err(LogSegError::Corrupted(m)) if framing_kept => {
                        assert_eq!(m, "bloom covered column list crc mismatch", "byte {i}");
                        crc_refusals += 1;
                    }
                    Err(LogSegError::Corrupted(_)) => {}
                    Err(other) => panic!("byte {i} bit {bit}: {other:?}"),
                    Ok(s) => panic!("byte {i} bit {bit} parsed as {:?}", s.covered()),
                }
            }
        }
        assert_eq!(crc_refusals, covered.len() * 7 + 4 * 8);
    }

    #[test]
    fn rejects_an_entry_length_not_a_multiple_of_512() {
        // 1000 bits is neither a power of two nor a multiple of 512.
        let mut entry = Vec::new();
        put_uvarint(&mut entry, 1000);
        entry.push(7);
        entry.extend_from_slice(&0u64.to_le_bytes());
        entry.extend_from_slice(&[0u8; 125]);
        let bytes = encode_rlog_bloom_section(&[COL_BODY], &[entry]);
        let s = RlogBloomSection::parse(&bytes, &field_dir()).expect("framing");
        assert!(matches!(s.entry(0), Err(LogSegError::Corrupted(_))));
    }

    #[test]
    fn rejects_a_covered_count_past_the_known_columns() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&u32::MAX.to_le_bytes());
        assert!(matches!(
            RlogBloomSection::parse(&bytes, &field_dir()),
            Err(LogSegError::Corrupted(msg)) if msg.contains("more than")
        ));
    }
}
