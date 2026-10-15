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

/// The entries' bytes, concatenated. Held as one `String` when every entry is
/// UTF-8; otherwise the raw bytes beside a `String` of the UTF-8 entries alone,
/// with its own offsets. Either way [`DictValues::text`] is a slice and a
/// boundary check, never a second validation.
#[derive(Debug)]
enum Region {
    Text(String),
    Mixed {
        raw: Vec<u8>,
        /// The UTF-8 entries concatenated in dictionary order; a non-UTF-8
        /// entry contributes nothing.
        text: String,
        /// `len + 1` offsets into `text`; a non-UTF-8 entry's span is empty.
        text_offsets: Vec<i32>,
    },
    /// The raw bytes of a dictionary whose entries were not validated
    /// ([`DictValues::unchecked`]); each validity or text read validates its
    /// entry.
    Unchecked(Vec<u8>),
}

impl Region {
    fn bytes(&self) -> &[u8] {
        match self {
            Region::Text(s) => s.as_bytes(),
            Region::Mixed { raw, .. } | Region::Unchecked(raw) => raw,
        }
    }
}

/// Fails when a dictionary of `entries` entries and `total` bytes cannot be
/// addressed by `i32` offsets, the range Arrow string offsets and the
/// dictionary's own keys use.
fn check_offset_range(entries: usize, total: usize) -> Result<(), LogSegError> {
    if i32::try_from(total).is_err() || i32::try_from(entries).is_err() {
        return Err(LogSegError::Corrupted(format!(
            "dictionary of {entries} entries and {total} bytes exceeds the i32 offset range"
        )));
    }
    Ok(())
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
    /// each entry's UTF-8 once. A dictionary whose entries total more than
    /// `i32::MAX` bytes (or number more than `i32::MAX`) is refused as
    /// [`LogSegError::Corrupted`].
    pub(crate) fn from_entries(entries: &[Vec<u8>]) -> Result<Self, LogSegError> {
        let checked: Vec<(&[u8], Option<&str>)> = entries
            .iter()
            .map(|e| (e.as_slice(), validate_entry(e)))
            .collect();
        Self::from_checked(&checked)
    }

    /// Builds the dictionary from entries already validated: each entry's
    /// bytes beside its text when they are UTF-8. Validates nothing.
    pub(crate) fn from_checked(entries: &[(&[u8], Option<&str>)]) -> Result<Self, LogSegError> {
        let total: usize = entries.iter().map(|(e, _)| e.len()).sum();
        check_offset_range(entries.len(), total)?;
        let texts: Vec<Option<&str>> = entries.iter().map(|&(_, t)| t).collect();
        let mut valid = Validity::unset(entries.len());
        for (k, t) in texts.iter().enumerate() {
            if t.is_some() {
                valid.set(k);
            }
        }
        // `total` fits an i32, so every running length below does too.
        let mut offsets = Vec::with_capacity(entries.len() + 1);
        offsets.push(0i32);
        let mut at = 0usize;
        for (entry, _) in entries {
            at += entry.len();
            offsets.push(at as i32);
        }
        let region = if texts.iter().all(Option::is_some) {
            let mut s = String::with_capacity(total);
            s.extend(texts.iter().flatten().copied());
            Region::Text(s)
        } else {
            let mut raw = Vec::with_capacity(total);
            for (entry, _) in entries {
                raw.extend_from_slice(entry);
            }
            let text_total: usize = texts.iter().flatten().map(|t| t.len()).sum();
            let mut text = String::with_capacity(text_total);
            let mut text_offsets = Vec::with_capacity(entries.len() + 1);
            text_offsets.push(0i32);
            for t in &texts {
                text.push_str(t.unwrap_or(""));
                text_offsets.push(text.len() as i32);
            }
            Region::Mixed {
                raw,
                text,
                text_offsets,
            }
        };
        Ok(DictValues {
            region,
            offsets,
            valid,
        })
    }

    /// Builds the dictionary from its entries without validating any of them,
    /// for a scan that tests the entries' raw bytes and validates only the
    /// ones its surviving rows use. Each [`is_valid`](Self::is_valid) or
    /// [`text`](Self::text) read then validates the entry it reads. The same
    /// offset-range refusal as [`from_entries`](Self::from_entries).
    pub(crate) fn unchecked(entries: &[Vec<u8>]) -> Result<Self, LogSegError> {
        let total: usize = entries.iter().map(Vec::len).sum();
        check_offset_range(entries.len(), total)?;
        let mut offsets = Vec::with_capacity(entries.len() + 1);
        offsets.push(0i32);
        let mut raw = Vec::with_capacity(total);
        for entry in entries {
            raw.extend_from_slice(entry);
            offsets.push(raw.len() as i32);
        }
        Ok(DictValues {
            region: Region::Unchecked(raw),
            offsets,
            valid: Validity::default(),
        })
    }

    /// Whether this dictionary was built by [`unchecked`](Self::unchecked).
    pub(crate) fn is_unchecked(&self) -> bool {
        matches!(self.region, Region::Unchecked(_))
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
        match &self.region {
            Region::Text(_) => true,
            Region::Mixed { .. } => false,
            Region::Unchecked(_) => (0..self.len()).all(|k| self.is_valid(k)),
        }
    }

    /// Entry `k`'s bytes, or `None` when `k` is out of range.
    pub fn entry(&self, k: usize) -> Option<&[u8]> {
        let (a, b) = span(&self.offsets, k)?;
        self.region.bytes().get(a..b)
    }

    /// Whether entry `k` is UTF-8; `false` out of range.
    pub fn is_valid(&self, k: usize) -> bool {
        match self.region {
            Region::Unchecked(_) => self.text(k).is_some(),
            _ => self.valid.get(k),
        }
    }

    /// Entry `k` as text, or `None` when it is not UTF-8 or out of range. O(1)
    /// for a dictionary built by [`from_entries`](Self::from_entries) or
    /// [`from_checked`](Self::from_checked), whose UTF-8 was checked once; a
    /// validation per read for one built by [`unchecked`](Self::unchecked).
    pub fn text(&self, k: usize) -> Option<&str> {
        if let Region::Unchecked(_) = self.region {
            return validate_entry(self.entry(k)?);
        }
        if !self.valid.get(k) {
            return None;
        }
        match &self.region {
            Region::Unchecked(_) => None,
            Region::Text(s) => {
                let (a, b) = span(&self.offsets, k)?;
                s.get(a..b)
            }
            Region::Mixed {
                text, text_offsets, ..
            } => {
                let (a, b) = span(text_offsets, k)?;
                text.get(a..b)
            }
        }
    }

    /// The entries in dictionary order as [`text`](Self::text) reads them.
    pub fn texts(&self) -> impl Iterator<Item = Option<&str>> + '_ {
        (0..self.len()).map(|k| self.text(k))
    }

    /// Bytes this dictionary holds on the heap.
    pub(crate) fn heap_bytes(&self) -> usize {
        let region = match &self.region {
            Region::Text(s) => s.capacity(),
            Region::Mixed {
                raw,
                text,
                text_offsets,
            } => raw.capacity() + text.capacity() + text_offsets.capacity() * 4,
            Region::Unchecked(raw) => raw.capacity(),
        };
        region + self.offsets.capacity() * std::mem::size_of::<i32>() + self.valid.heap_bytes()
    }

    /// Bytes this dictionary narrowed to the entries set in `used` would hold:
    /// those entries' [`entry_heap_bytes`](Self::entry_heap_bytes), the leading
    /// offset of each offset array, and one validity bit per used entry. What a
    /// block referencing only those entries is charged for a shared dictionary.
    pub(crate) fn share_heap_bytes(&self, used: &Validity) -> usize {
        let mut count = 0usize;
        let mut bytes = 0usize;
        for k in (0..self.len()).filter(|&k| used.get(k)) {
            count += 1;
            bytes += self.entry_heap_bytes(k);
        }
        let leading = match self.region {
            Region::Text(_) | Region::Unchecked(_) => 1,
            Region::Mixed { .. } => 2,
        };
        bytes
            + leading * std::mem::size_of::<i32>()
            + count.div_ceil(64) * std::mem::size_of::<u64>()
    }

    /// Bytes entry `k` holds in this dictionary: its bytes (twice for a UTF-8
    /// entry of a dictionary holding a non-UTF-8 one, which keeps a text copy)
    /// and its offsets. Zero out of range.
    pub(crate) fn entry_heap_bytes(&self, k: usize) -> usize {
        let Some((a, b)) = span(&self.offsets, k) else {
            return 0;
        };
        let offsets = std::mem::size_of::<i32>();
        match &self.region {
            Region::Text(_) | Region::Unchecked(_) => (b - a) + offsets,
            Region::Mixed { text_offsets, .. } => {
                let text = span(text_offsets, k).map_or(0, |(c, d)| d - c);
                (b - a) + offsets + text + offsets
            }
        }
    }
}

fn span(offsets: &[i32], k: usize) -> Option<(usize, usize)> {
    let a = usize::try_from(*offsets.get(k)?).ok()?;
    let b = usize::try_from(*offsets.get(k + 1)?).ok()?;
    Some((a, b))
}

/// An entry's text when it is UTF-8. The one place a dictionary entry is
/// validated.
fn validate_entry(entry: &[u8]) -> Option<&str> {
    #[cfg(test)]
    ENTRY_VALIDATIONS.with(|n| n.set(n.get() + 1));
    std::str::from_utf8(entry).ok()
}

#[cfg(test)]
thread_local! {
    /// [`validate_entry`] calls on this thread, for the validated-once tests.
    pub(crate) static ENTRY_VALIDATIONS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
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
    fn a_mixed_dictionary_validates_each_entry_once_however_often_it_is_read() {
        let entries = vec![
            Vec::new(),
            vec![0xff],
            "é".as_bytes().to_vec(),
            vec![b'x', 0xc3],
            b"tail".to_vec(),
        ];
        let before = ENTRY_VALIDATIONS.with(|n| n.get());
        let d = DictValues::from_entries(&entries).expect("dict");
        for _ in 0..3 {
            assert_eq!(
                d.texts().collect::<Vec<_>>(),
                [Some(""), None, Some("é"), None, Some("tail")]
            );
        }
        assert_eq!(ENTRY_VALIDATIONS.with(|n| n.get()) - before, 5);
        for (k, e) in entries.iter().enumerate() {
            assert_eq!(d.entry(k), Some(e.as_slice()));
        }
        assert_eq!(d.text(5), None);
    }

    #[test]
    fn the_offset_range_check_refuses_past_i32_with_a_typed_error() {
        let max = i32::MAX as usize;
        assert!(check_offset_range(3, max).is_ok());
        assert!(check_offset_range(max, 0).is_ok());
        for (entries, total) in [(3, max + 1), (max + 1, 0)] {
            match check_offset_range(entries, total) {
                Err(LogSegError::Corrupted(m)) => {
                    assert!(m.contains("exceeds the i32 offset range"), "{m}")
                }
                other => panic!("expected Corrupted, got {other:?}"),
            }
        }
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
