//! BLOOM section container (docs/log-segment-format.md "BLOOM").
//!
//! One blocked bloom filter per row block, framed so a single entry is
//! readable and verifiable alone. Entry `i` corresponds to block `i`. Each
//! entry carries its own crc32c, verified before the entry is probed.

use crate::bloom::BloomView;
use crate::error::LogSegError;
use crate::varint::{get_uvarint, put_uvarint};

/// Serializes the BLOOM container: `count` u32, then per entry
/// `entry_len` varint, `crc32c` u32 over the entry bytes, then the entry.
pub fn encode_bloom_section(entries: &[Vec<u8>]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for e in entries {
        put_uvarint(&mut out, e.len() as u64);
        out.extend_from_slice(&crc32c::crc32c(e).to_le_bytes());
        out.extend_from_slice(e);
    }
    out
}

/// A parsed BLOOM container: the count and each entry's byte range, so entries
/// are addressable in O(1) after one linear pass.
pub struct BloomSection<'a> {
    bytes: &'a [u8],
    ranges: Vec<(u32, std::ops::Range<usize>)>,
}

impl<'a> BloomSection<'a> {
    /// Parses the container framing. Rejects truncation, an entry length that
    /// runs past the section, and trailing bytes. The per-entry crc is checked
    /// lazily in [`BloomSection::entry`].
    pub fn parse(bytes: &'a [u8]) -> Result<Self, LogSegError> {
        let count_bytes = bytes
            .get(0..4)
            .ok_or_else(|| LogSegError::Corrupted("bloom section truncated at count".into()))?;
        let count = u32::from_le_bytes([
            count_bytes[0],
            count_bytes[1],
            count_bytes[2],
            count_bytes[3],
        ]);
        let mut pos = 4usize;
        let mut ranges = Vec::with_capacity((count as usize).min(1 << 16));
        for _ in 0..count {
            let entry_len = get_uvarint(bytes, &mut pos)?;
            let entry_len = usize::try_from(entry_len)
                .map_err(|_| LogSegError::Corrupted("bloom entry len range".into()))?;
            let crc = bytes
                .get(pos..pos + 4)
                .ok_or_else(|| LogSegError::Corrupted("bloom entry truncated at crc".into()))?;
            let crc = u32::from_le_bytes([crc[0], crc[1], crc[2], crc[3]]);
            pos += 4;
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
        Ok(BloomSection { bytes, ranges })
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.ranges.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    /// The bloom view for entry `index`, verifying its crc first. An index out
    /// of range or a crc mismatch is `Corrupted`.
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
        BloomView::parse(entry)
    }
}

/// Convenience: parse the container and return entry `index`'s view.
pub fn bloom_entry(bytes: &[u8], index: usize) -> Result<BloomView<'_>, LogSegError> {
    BloomSection::parse(bytes)?.entry(index)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::bloom::BloomBuilder;

    fn build_entries() -> Vec<Vec<u8>> {
        let mut a = BloomBuilder::new(1);
        a.insert(5, b"timeout");
        let mut b = BloomBuilder::new(1);
        b.insert(5, b"connection");
        vec![a.finish(), b.finish()]
    }

    #[test]
    fn roundtrip_and_probe() {
        let entries = build_entries();
        let section = encode_bloom_section(&entries);
        let parsed = BloomSection::parse(&section).expect("parse");
        assert_eq!(parsed.len(), 2);
        assert!(parsed.entry(0).expect("e0").may_contain(5, b"timeout"));
        assert!(!parsed.entry(0).expect("e0").may_contain(5, b"connection"));
        assert!(parsed.entry(1).expect("e1").may_contain(5, b"connection"));
        assert!(
            bloom_entry(&section, 1)
                .expect("entry")
                .may_contain(5, b"connection")
        );
    }

    #[test]
    fn rejects_out_of_range_and_crc_flip() {
        let entries = build_entries();
        let mut section = encode_bloom_section(&entries);
        let parsed = BloomSection::parse(&section).expect("parse");
        assert!(matches!(parsed.entry(2), Err(LogSegError::Corrupted(_))));
        drop(parsed);
        // Flip a byte inside the first entry payload; its crc must catch it.
        // Layout: count(4) len(varint) crc(4) entry... The entry byte sits well
        // past the first 9 bytes.
        let flip = section.len() - 1;
        section[flip] ^= 0xff;
        let parsed = BloomSection::parse(&section).expect("parse");
        assert!(matches!(parsed.entry(1), Err(LogSegError::Corrupted(_))));
    }

    #[test]
    fn rejects_truncation() {
        let entries = build_entries();
        let section = encode_bloom_section(&entries);
        assert!(matches!(
            BloomSection::parse(&section[..section.len() - 2]),
            Err(LogSegError::Corrupted(_))
        ));
        assert!(matches!(
            BloomSection::parse(&[]),
            Err(LogSegError::Corrupted(_))
        ));
    }
}
