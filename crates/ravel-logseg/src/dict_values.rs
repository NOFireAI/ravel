//! A string dictionary decoded once and shared by every block that reads it
//! (ADR-2773 decision D3).
//!
//! A row group dictionary page (tag 12) is decoded into one [`DictValues`] per
//! row group, held behind an `Arc` beside its `DictCache` entry, and every
//! block of the group references it instead of cloning the entries it uses. A
//! per-block dictionary page (tag 7) decodes into the same shape once per
//! block.

use crate::LogSegError;

/// One bit per position: a dictionary entry's UTF-8 validity, or a block row's
/// presence.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Validity {
    words: Vec<u64>,
    len: usize,
}

impl Validity {
    /// `len` positions, every one unset.
    pub(crate) fn unset(len: usize) -> Self {
        Validity {
            words: vec![0; len.div_ceil(64)],
            len,
        }
    }

    pub(crate) fn set(&mut self, i: usize) {
        if i < self.len {
            self.words[i / 64] |= 1 << (i % 64);
        }
    }

    /// Whether position `i` is set; `false` out of range.
    pub fn get(&self, i: usize) -> bool {
        i < self.len && self.words[i / 64] & (1 << (i % 64)) != 0
    }

    /// The number of positions.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether there are no positions.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub(crate) fn heap_bytes(&self) -> usize {
        self.words.capacity() * std::mem::size_of::<u64>()
    }
}

/// The entries' bytes, concatenated. Held as a `String` when every entry is
/// UTF-8, so [`DictValues::text`] slices it with no second validation.
#[derive(Debug)]
enum Region {
    Text(String),
    Raw(Vec<u8>),
}

impl Region {
    fn bytes(&self) -> &[u8] {
        match self {
            Region::Text(s) => s.as_bytes(),
            Region::Raw(b) => b,
        }
    }
}

/// A decoded string dictionary: one contiguous byte region, `len + 1` `i32`
/// offsets into it, and a per-entry validity bit that is set when the entry's
/// bytes are UTF-8. Validity is decided once, when the dictionary is built.
#[derive(Debug)]
pub struct DictValues {
    region: Region,
    offsets: Vec<i32>,
    valid: Validity,
}

impl DictValues {
    /// Builds the dictionary from its entries in dictionary order, checking
    /// each entry's UTF-8 once. Fails when the entries' total length does not
    /// fit an `i32` offset.
    pub(crate) fn from_entries(entries: &[Vec<u8>]) -> Result<Self, LogSegError> {
        let total: usize = entries.iter().map(Vec::len).sum();
        if i32::try_from(total).is_err() || i32::try_from(entries.len()).is_err() {
            return Err(LogSegError::Corrupted(format!(
                "dictionary of {} entries and {total} bytes exceeds the i32 offset range",
                entries.len()
            )));
        }
        let mut offsets = Vec::with_capacity(entries.len() + 1);
        offsets.push(0i32);
        let mut valid = Validity::unset(entries.len());
        let mut region = Region::Text(String::with_capacity(total));
        for (k, entry) in entries.iter().enumerate() {
            let text = std::str::from_utf8(entry).ok();
            if text.is_some() {
                valid.set(k);
            }
            region = match (region, text) {
                (Region::Text(mut s), Some(t)) => {
                    s.push_str(t);
                    Region::Text(s)
                }
                (Region::Text(s), None) => {
                    let mut b = s.into_bytes();
                    b.extend_from_slice(entry);
                    Region::Raw(b)
                }
                (Region::Raw(mut b), _) => {
                    b.extend_from_slice(entry);
                    Region::Raw(b)
                }
            };
            // `total` fits an i32, so every running length does too.
            offsets.push(region.bytes().len() as i32);
        }
        Ok(DictValues {
            region,
            offsets,
            valid,
        })
    }

    /// The number of entries.
    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    /// Whether the dictionary has no entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether every entry is UTF-8.
    pub fn all_valid(&self) -> bool {
        matches!(self.region, Region::Text(_))
    }

    /// Entry `k`'s bytes, or `None` when `k` is out of range.
    pub fn entry(&self, k: usize) -> Option<&[u8]> {
        let (a, b) = self.span(k)?;
        self.region.bytes().get(a..b)
    }

    /// Whether entry `k` is UTF-8; `false` out of range.
    pub fn is_valid(&self, k: usize) -> bool {
        self.valid.get(k)
    }

    /// Entry `k` as text, or `None` when it is not UTF-8 or out of range. A
    /// dictionary whose entries are all UTF-8 slices its region; one holding a
    /// non-UTF-8 entry checks the requested entry again.
    pub fn text(&self, k: usize) -> Option<&str> {
        if !self.valid.get(k) {
            return None;
        }
        let (a, b) = self.span(k)?;
        match &self.region {
            Region::Text(s) => s.get(a..b),
            Region::Raw(r) => std::str::from_utf8(r.get(a..b)?).ok(),
        }
    }

    /// The entries in dictionary order as [`text`](Self::text) reads them.
    pub fn texts(&self) -> impl Iterator<Item = Option<&str>> + '_ {
        (0..self.len()).map(|k| self.text(k))
    }

    fn span(&self, k: usize) -> Option<(usize, usize)> {
        let a = usize::try_from(*self.offsets.get(k)?).ok()?;
        let b = usize::try_from(*self.offsets.get(k + 1)?).ok()?;
        Some((a, b))
    }

    /// Bytes this dictionary holds on the heap.
    pub(crate) fn heap_bytes(&self) -> usize {
        let region = match &self.region {
            Region::Text(s) => s.capacity(),
            Region::Raw(b) => b.capacity(),
        };
        region + self.offsets.capacity() * std::mem::size_of::<i32>() + self.valid.heap_bytes()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn entries_round_trip_with_validity_decided_once() {
        let entries = vec![b"a".to_vec(), Vec::new(), b"bc".to_vec()];
        let d = DictValues::from_entries(&entries).expect("dict");
        assert!(d.all_valid());
        assert_eq!(d.len(), 3);
        for (k, e) in entries.iter().enumerate() {
            assert_eq!(d.entry(k), Some(e.as_slice()));
            assert!(d.is_valid(k));
        }
        assert_eq!(d.text(2), Some("bc"));
        assert_eq!(d.entry(3), None);
        assert!(!d.is_valid(3));
    }

    #[test]
    fn a_non_utf8_entry_is_invalid_and_keeps_its_bytes() {
        let entries = vec![b"a".to_vec(), vec![b'b', 0xff], b"c".to_vec()];
        let d = DictValues::from_entries(&entries).expect("dict");
        assert!(!d.all_valid());
        assert_eq!(d.texts().collect::<Vec<_>>(), [Some("a"), None, Some("c")]);
        assert_eq!(d.entry(1), Some(&[b'b', 0xff][..]));
        assert!(!d.is_valid(1));
        assert!(d.is_valid(2));
    }

    #[test]
    fn validity_bits_cross_word_boundaries() {
        let mut v = Validity::unset(130);
        for i in [0, 63, 64, 129] {
            v.set(i);
        }
        v.set(130);
        let set: Vec<usize> = (0..131).filter(|&i| v.get(i)).collect();
        assert_eq!(set, [0, 63, 64, 129]);
    }
}
