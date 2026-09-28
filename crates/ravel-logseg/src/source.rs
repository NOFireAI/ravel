//! Where a reader's object bytes come from.
//!
//! [`crate::RlogReader`] and [`crate::BlockScan`] address an RLOG object at the
//! absolute offsets its footer and directories name. [`ByteSource`] is the one
//! operation they need from the bytes behind those offsets: the object's total
//! length, and the stored bytes of one absolute range. A whole object in memory
//! (`[u8]`, `Vec<u8>`, [`Bytes`]) is a source that holds every range. A
//! [`SparseObject`] holds only the regions a ranged read placed, and refuses
//! every other range with [`LogSegError::Unplaced`], so a read the fetch did not
//! anticipate fails typed instead of decoding bytes that were never fetched.
//!
//! The reader is generic over the source rather than taking a trait object:
//! every page and section read goes through [`ByteSource::read`], so a
//! whole-object caller pays a bounds-checked slice per read with no dynamic
//! dispatch, and a sparse one a binary search over its regions.

use std::borrow::Cow;

use bytes::Bytes;

use crate::error::LogSegError;

/// An RLOG object's bytes, addressed by absolute offset.
///
/// `read` returns the stored bytes of `[start, start + len)`, borrowed when the
/// source holds them contiguously. A source never returns bytes it does not
/// hold: a range outside `[0, object_len())` or not held is an error, never a
/// panic, zero fill, or another object's bytes.
pub trait ByteSource {
    /// The object's total length in bytes.
    fn object_len(&self) -> u64;

    /// The stored bytes of `[start, start + len)`.
    fn read(&self, start: u64, len: u64) -> Result<Cow<'_, [u8]>, LogSegError>;
}

impl ByteSource for [u8] {
    fn object_len(&self) -> u64 {
        self.len() as u64
    }

    fn read(&self, start: u64, len: u64) -> Result<Cow<'_, [u8]>, LogSegError> {
        let range = usize::try_from(start)
            .ok()
            .zip(usize::try_from(len).ok())
            .and_then(|(s, l)| Some(s..s.checked_add(l)?));
        range
            .and_then(|r| self.get(r))
            .map(Cow::Borrowed)
            .ok_or_else(|| LogSegError::Corrupted("read out of bounds".into()))
    }
}

impl ByteSource for Vec<u8> {
    fn object_len(&self) -> u64 {
        self.as_slice().object_len()
    }

    fn read(&self, start: u64, len: u64) -> Result<Cow<'_, [u8]>, LogSegError> {
        self.as_slice().read(start, len)
    }
}

impl ByteSource for Bytes {
    fn object_len(&self) -> u64 {
        self.as_ref().object_len()
    }

    fn read(&self, start: u64, len: u64) -> Result<Cow<'_, [u8]>, LogSegError> {
        self.as_ref().read(start, len)
    }
}

impl<T: ByteSource + ?Sized> ByteSource for &T {
    fn object_len(&self) -> u64 {
        (**self).object_len()
    }

    fn read(&self, start: u64, len: u64) -> Result<Cow<'_, [u8]>, LogSegError> {
        (**self).read(start, len)
    }
}

/// An object of known length of which only some regions are held: each region
/// is an absolute start plus the stored bytes placed there, kept as the
/// [`Bytes`] they arrived in (no copy, no object-sized buffer).
///
/// A read inside one region borrows it. A read that spans two or more regions
/// that together hold every byte of the range (adjacent or overlapping
/// placements) is stitched into an owned copy; placement never merges regions,
/// so no placement copies. Any byte of the range held by no region makes the
/// read fail with [`LogSegError::Unplaced`].
#[derive(Clone, Debug, Default)]
pub struct SparseObject {
    object_len: u64,
    /// Sorted by start; regions may be adjacent or overlap.
    regions: Vec<(u64, Bytes)>,
    placed_len: u64,
}

impl SparseObject {
    /// An object of `object_len` bytes with nothing placed yet.
    pub fn new(object_len: u64) -> Self {
        SparseObject {
            object_len,
            regions: Vec::new(),
            placed_len: 0,
        }
    }

    /// Holds `bytes` as the object's stored bytes at `[start, start +
    /// bytes.len())`. A region reaching past the object's end is refused. The
    /// caller vouches that the bytes are this object's: a source holds what it
    /// is given.
    pub fn place(&mut self, start: u64, bytes: Bytes) -> Result<(), LogSegError> {
        let len = bytes.len() as u64;
        let end = start
            .checked_add(len)
            .ok_or_else(|| LogSegError::Corrupted("placed region overflow".into()))?;
        if end > self.object_len {
            return Err(LogSegError::Corrupted(format!(
                "placed region [{start}, {end}) past object end {}",
                self.object_len
            )));
        }
        if len == 0 {
            return Ok(());
        }
        let at = self.regions.partition_point(|(s, _)| *s <= start);
        self.regions.insert(at, (start, bytes));
        self.placed_len = self.placed_len.saturating_add(len);
        Ok(())
    }

    /// Whether one placed region alone holds all of `[start, end)`.
    pub fn holds_in_one_region(&self, start: u64, end: u64) -> bool {
        self.region_holding(start, end).is_some()
    }

    /// Summed length of every placed region, overlaps counted once per
    /// placement: the bytes this source keeps alive.
    pub fn placed_len(&self) -> u64 {
        self.placed_len
    }

    /// The placed regions, `(absolute start, bytes)`, sorted by start.
    pub fn regions(&self) -> impl Iterator<Item = (u64, &Bytes)> {
        self.regions.iter().map(|(s, b)| (*s, b))
    }

    fn region_holding(&self, start: u64, end: u64) -> Option<(u64, &Bytes)> {
        // The region with the greatest start at or before `start` is the one
        // that holds the range whenever no region overlaps another, the common
        // case; with overlaps an earlier, longer region may hold it instead.
        let at = self.regions.partition_point(|(s, _)| *s <= start);
        let holds = |(s, b): &(u64, Bytes)| end <= s.saturating_add(b.len() as u64);
        let last = at.checked_sub(1).and_then(|i| self.regions.get(i));
        match last {
            Some(r) if holds(r) => Some((r.0, &r.1)),
            Some(_) => self
                .regions
                .get(..at)?
                .iter()
                .find(|r| holds(r))
                .map(|r| (r.0, &r.1)),
            None => None,
        }
    }

    /// Stitches `[start, end)` from several regions, or `None` when some byte
    /// of it is held by none.
    fn stitch(&self, start: u64, end: u64) -> Option<Vec<u8>> {
        let mut out = Vec::with_capacity(usize::try_from(end - start).ok()?);
        let mut at = start;
        while at < end {
            // Of the regions holding byte `at`, the one reaching furthest.
            let (s, b) = self
                .regions
                .iter()
                .filter(|(s, b)| *s <= at && at < s.saturating_add(b.len() as u64))
                .max_by_key(|(s, b)| s.saturating_add(b.len() as u64))?;
            let region_end = s.saturating_add(b.len() as u64).min(end);
            let from = usize::try_from(at - s).ok()?;
            let to = usize::try_from(region_end - s).ok()?;
            out.extend_from_slice(b.get(from..to)?);
            at = region_end;
        }
        Some(out)
    }
}

impl ByteSource for SparseObject {
    fn object_len(&self) -> u64 {
        self.object_len
    }

    fn read(&self, start: u64, len: u64) -> Result<Cow<'_, [u8]>, LogSegError> {
        let end = start
            .checked_add(len)
            .filter(|end| *end <= self.object_len)
            .ok_or_else(|| LogSegError::Corrupted("read out of bounds".into()))?;
        if len == 0 {
            return Ok(Cow::Borrowed(&[]));
        }
        if let Some((s, b)) = self.region_holding(start, end) {
            let from =
                usize::try_from(start - s).map_err(|_| LogSegError::Unplaced { start, end })?;
            let to = usize::try_from(end - s).map_err(|_| LogSegError::Unplaced { start, end })?;
            return b
                .get(from..to)
                .map(Cow::Borrowed)
                .ok_or(LogSegError::Unplaced { start, end });
        }
        self.stitch(start, end)
            .map(Cow::Owned)
            .ok_or(LogSegError::Unplaced { start, end })
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    fn object() -> Vec<u8> {
        (0..=255u8).cycle().take(1000).collect()
    }

    fn sparse(obj: &[u8], regions: &[(usize, usize)]) -> SparseObject {
        let mut s = SparseObject::new(obj.len() as u64);
        for &(start, end) in regions {
            s.place(start as u64, Bytes::copy_from_slice(&obj[start..end]))
                .expect("place");
        }
        s
    }

    /// A whole buffer holds every in-bounds range, and refuses the rest typed.
    #[test]
    fn whole_buffer_reads_in_bounds_and_refuses_past_the_end() {
        let obj = object();
        assert_eq!(obj.as_slice().read(10, 5).expect("in bounds"), &obj[10..15]);
        assert!(obj.as_slice().read(996, 5).is_err());
        assert!(obj.as_slice().read(u64::MAX, 2).is_err());
        assert_eq!(obj.object_len(), 1000);
    }

    /// A read inside one region borrows it, one outside every region is
    /// `Unplaced`, and one that starts inside a region and runs off its end
    /// into nothing is `Unplaced` too.
    #[test]
    fn sparse_reads_inside_a_region_and_refuses_outside() {
        let obj = object();
        let s = sparse(&obj, &[(100, 200), (500, 600)]);
        let got = s.read(120, 30).expect("held");
        assert!(matches!(got, Cow::Borrowed(_)), "one region is a borrow");
        assert_eq!(&*got, &obj[120..150]);
        assert!(matches!(
            s.read(300, 10),
            Err(LogSegError::Unplaced {
                start: 300,
                end: 310
            })
        ));
        assert!(matches!(
            s.read(190, 20),
            Err(LogSegError::Unplaced {
                start: 190,
                end: 210
            })
        ));
        assert!(matches!(s.read(995, 10), Err(LogSegError::Corrupted(_))));
        assert_eq!(s.placed_len(), 200);
    }

    /// The boundary between two separately placed, adjacent regions: a read
    /// ending exactly at the first region's end borrows it, one starting there
    /// borrows the second, and one straddling the seam is stitched from both
    /// into exactly the object's bytes. A gap of one byte between them makes
    /// the straddling read `Unplaced`.
    #[test]
    fn a_read_across_adjacent_regions_is_stitched_and_a_one_byte_gap_refuses() {
        let obj = object();
        let s = sparse(&obj, &[(300, 400), (100, 300)]);
        assert!(matches!(s.read(250, 50).expect("first"), Cow::Borrowed(_)));
        assert!(matches!(s.read(300, 50).expect("second"), Cow::Borrowed(_)));
        let seam = s.read(299, 2).expect("across the seam");
        assert!(matches!(seam, Cow::Owned(_)), "two regions are stitched");
        assert_eq!(&*seam, &obj[299..301]);
        assert_eq!(&*s.read(100, 300).expect("both whole"), &obj[100..400]);
        assert!(!s.holds_in_one_region(299, 301));
        assert!(s.holds_in_one_region(300, 400));

        let gapped = sparse(&obj, &[(100, 299), (300, 400)]);
        assert!(matches!(
            gapped.read(298, 3),
            Err(LogSegError::Unplaced {
                start: 298,
                end: 301
            })
        ));
    }

    /// Overlapping placements: a range held by an earlier, longer region is
    /// found even though a later-starting region sits between it and the range.
    #[test]
    fn an_overlapped_region_still_serves_the_range_it_holds() {
        let obj = object();
        let s = sparse(&obj, &[(0, 1000), (10, 20)]);
        assert!(matches!(s.read(50, 10).expect("held"), Cow::Borrowed(_)));
        assert_eq!(&*s.read(15, 10).expect("held"), &obj[15..25]);
        assert_eq!(s.placed_len(), 1010, "each placement counts what it keeps");
    }

    #[test]
    fn a_region_past_the_object_end_is_refused() {
        let mut s = SparseObject::new(10);
        assert!(s.place(8, Bytes::from_static(b"abc")).is_err());
        assert_eq!(s.placed_len(), 0);
    }
}
