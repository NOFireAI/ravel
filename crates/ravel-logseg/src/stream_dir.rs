//! STREAM_DIR section (docs/log-segment-format.md "STREAM_DIR").
//!
//! Maps each log stream id to its canonical resource+scope blob and the block
//! range holding its records. Entries are sorted ascending by `stream_id`, so
//! the dense `stream_ref` used everywhere else is the entry ordinal and lookup
//! is a binary search. Decode treats every field as untrusted: a non-ascending
//! sequence, an entry count over the cap, or any truncation is `Corrupted`.
//!
//! [`StreamDir::encode`] and [`StreamDir::encode_borrowed`] write the same
//! bytes for the same entries; both go through the private `entry_len` /
//! `write_entry` pair below so the layout exists in one place. The borrowed
//! form exists so a writer holding its blobs elsewhere (never owning a
//! `StreamEntry` per stream) can still size its output buffer exactly before
//! writing it.

use ravel_types::logstream::LogStreamId;

use crate::error::LogSegError;
use crate::varint::{get_uvarint, put_uvarint};

/// One STREAM_DIR entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamEntry {
    pub stream_id: LogStreamId,
    pub blob: Vec<u8>,
    pub first_blk: u32,
    pub last_blk: u32,
}

/// A STREAM_DIR entry whose blob is borrowed rather than owned, for
/// [`StreamDir::encode_borrowed`].
#[derive(Clone, Copy, Debug)]
pub struct BorrowedStreamEntry<'a> {
    pub stream_id: LogStreamId,
    pub blob: &'a [u8],
    pub first_blk: u32,
    pub last_blk: u32,
}

/// Bytes a LEB128 uvarint occupies for `value` (mirrors [`put_uvarint`]'s own
/// loop, without writing).
fn uvarint_len(mut value: u64) -> usize {
    let mut n = 1;
    loop {
        value >>= 7;
        if value == 0 {
            return n;
        }
        n += 1;
    }
}

/// Bytes one entry occupies once encoded: 16-byte id, the blob's
/// length-prefixed form, then the two block-range varints.
fn entry_len(blob_len: usize, first_blk: u32, last_blk: u32) -> usize {
    16 + uvarint_len(blob_len as u64)
        + blob_len
        + uvarint_len(u64::from(first_blk))
        + uvarint_len(u64::from(last_blk))
}

/// Writes one entry's bytes, the layout both `encode` and `encode_borrowed`
/// share.
fn write_entry(
    out: &mut Vec<u8>,
    stream_id: &LogStreamId,
    blob: &[u8],
    first_blk: u32,
    last_blk: u32,
) {
    out.extend_from_slice(&stream_id.0);
    put_uvarint(out, blob.len() as u64);
    out.extend_from_slice(blob);
    put_uvarint(out, u64::from(first_blk));
    put_uvarint(out, u64::from(last_blk));
}

/// The decoded STREAM_DIR: entries sorted ascending by `stream_id`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamDir {
    entries: Vec<StreamEntry>,
}

impl StreamDir {
    /// Builds a directory from entries already sorted ascending by stream id.
    pub fn new(entries: Vec<StreamEntry>) -> Self {
        StreamDir { entries }
    }

    pub fn entries(&self) -> &[StreamEntry] {
        &self.entries
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The stream id at `stream_ref`, if in range.
    pub fn stream_id(&self, stream_ref: u32) -> Option<&LogStreamId> {
        self.entries.get(stream_ref as usize).map(|e| &e.stream_id)
    }

    /// The dense `stream_ref` for `id` (binary search), if present.
    pub fn stream_ref(&self, id: &LogStreamId) -> Option<u32> {
        self.entries
            .binary_search_by(|e| e.stream_id.cmp(id))
            .ok()
            .map(|i| i as u32)
    }

    /// Serializes the section in its uncompressed form.
    pub fn encode(&self) -> Vec<u8> {
        let size = 4 + self
            .entries
            .iter()
            .map(|e| entry_len(e.blob.len(), e.first_blk, e.last_blk))
            .sum::<usize>();
        let mut out = Vec::with_capacity(size);
        out.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for e in &self.entries {
            write_entry(&mut out, &e.stream_id, &e.blob, e.first_blk, e.last_blk);
        }
        out
    }

    /// Serializes borrowed entries into exactly the bytes [`StreamDir::encode`]
    /// would write for the same entries (same id, blob, and block range, in
    /// the same order), without building an owned `StreamEntry` per stream.
    /// The buffer is sized exactly once before any byte is written.
    pub fn encode_borrowed(entries: &[BorrowedStreamEntry<'_>]) -> Vec<u8> {
        let size = 4 + entries
            .iter()
            .map(|e| entry_len(e.blob.len(), e.first_blk, e.last_blk))
            .sum::<usize>();
        let mut out = Vec::with_capacity(size);
        out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
        for e in entries {
            write_entry(&mut out, &e.stream_id, e.blob, e.first_blk, e.last_blk);
        }
        out
    }

    /// Decodes the uncompressed section form. Rejects a count over
    /// `max_entries`, a non-ascending stream-id sequence, truncation, and
    /// trailing bytes.
    pub fn decode(bytes: &[u8], max_entries: u64) -> Result<Self, LogSegError> {
        let count_bytes = bytes
            .get(0..4)
            .ok_or_else(|| LogSegError::Corrupted("stream_dir truncated at count".into()))?;
        let count = u32::from_le_bytes([
            count_bytes[0],
            count_bytes[1],
            count_bytes[2],
            count_bytes[3],
        ]) as u64;
        if count > max_entries {
            return Err(LogSegError::Corrupted(format!(
                "stream_dir count {count} over cap {max_entries}"
            )));
        }
        let mut pos = 4usize;
        let mut entries: Vec<StreamEntry> = Vec::with_capacity(count.min(1 << 16) as usize);
        let mut prev: Option<LogStreamId> = None;
        for _ in 0..count {
            let id_bytes = bytes
                .get(pos..pos + 16)
                .ok_or_else(|| LogSegError::Corrupted("stream_dir truncated at id".into()))?;
            let mut id = [0u8; 16];
            id.copy_from_slice(id_bytes);
            let stream_id = LogStreamId(id);
            pos += 16;
            if prev.is_some_and(|p| stream_id <= p) {
                return Err(LogSegError::Corrupted("stream_dir not ascending".into()));
            }
            prev = Some(stream_id);
            let blob_len = get_uvarint(bytes, &mut pos)?;
            let blob_len = usize::try_from(blob_len)
                .map_err(|_| LogSegError::Corrupted("stream_dir blob len".into()))?;
            let end = pos
                .checked_add(blob_len)
                .ok_or_else(|| LogSegError::Corrupted("stream_dir blob overflow".into()))?;
            let blob = bytes
                .get(pos..end)
                .ok_or_else(|| LogSegError::Corrupted("stream_dir blob truncated".into()))?
                .to_vec();
            pos = end;
            let first_blk = u32::try_from(get_uvarint(bytes, &mut pos)?)
                .map_err(|_| LogSegError::Corrupted("stream_dir first_blk range".into()))?;
            let last_blk = u32::try_from(get_uvarint(bytes, &mut pos)?)
                .map_err(|_| LogSegError::Corrupted("stream_dir last_blk range".into()))?;
            entries.push(StreamEntry {
                stream_id,
                blob,
                first_blk,
                last_blk,
            });
        }
        if pos != bytes.len() {
            return Err(LogSegError::Corrupted("stream_dir trailing bytes".into()));
        }
        Ok(StreamDir { entries })
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn id(n: u8) -> LogStreamId {
        let mut a = [0u8; 16];
        a[0] = n;
        LogStreamId(a)
    }

    fn dir() -> StreamDir {
        StreamDir::new(vec![
            StreamEntry {
                stream_id: id(1),
                blob: b"a".to_vec(),
                first_blk: 0,
                last_blk: 3,
            },
            StreamEntry {
                stream_id: id(5),
                blob: Vec::new(),
                first_blk: 2,
                last_blk: 9,
            },
            StreamEntry {
                stream_id: id(9),
                blob: b"zzz".to_vec(),
                first_blk: 4,
                last_blk: 4,
            },
        ])
    }

    #[test]
    fn roundtrip_and_lookup() {
        let d = dir();
        let bytes = d.encode();
        let got = StreamDir::decode(&bytes, 1000).expect("decode");
        assert_eq!(got, d);
        assert_eq!(got.stream_ref(&id(1)), Some(0));
        assert_eq!(got.stream_ref(&id(5)), Some(1));
        assert_eq!(got.stream_ref(&id(9)), Some(2));
        assert_eq!(got.stream_ref(&id(7)), None);
        assert_eq!(got.stream_id(2), Some(&id(9)));
        assert_eq!(got.stream_id(3), None);
    }

    #[test]
    fn rejects_unsorted() {
        let d = StreamDir::new(vec![
            StreamEntry {
                stream_id: id(5),
                blob: Vec::new(),
                first_blk: 0,
                last_blk: 0,
            },
            StreamEntry {
                stream_id: id(1),
                blob: Vec::new(),
                first_blk: 0,
                last_blk: 0,
            },
        ]);
        let bytes = d.encode();
        assert!(matches!(
            StreamDir::decode(&bytes, 1000),
            Err(LogSegError::Corrupted(_))
        ));
    }

    #[test]
    fn rejects_count_over_cap_and_truncation() {
        let bytes = dir().encode();
        assert!(matches!(
            StreamDir::decode(&bytes, 2),
            Err(LogSegError::Corrupted(_))
        ));
        assert!(matches!(
            StreamDir::decode(&bytes[..bytes.len() - 1], 1000),
            Err(LogSegError::Corrupted(_))
        ));
        assert!(matches!(
            StreamDir::decode(&[], 1000),
            Err(LogSegError::Corrupted(_))
        ));
    }

    fn id32(n: u32) -> LogStreamId {
        let mut a = [0u8; 16];
        a[0..4].copy_from_slice(&n.to_be_bytes());
        LogStreamId(a)
    }

    fn to_owned_entries(raw: &[(u32, Vec<u8>, u32, u32)]) -> Vec<StreamEntry> {
        raw.iter()
            .map(|(n, blob, first_blk, last_blk)| StreamEntry {
                stream_id: id32(*n),
                blob: blob.clone(),
                first_blk: *first_blk,
                last_blk: *last_blk,
            })
            .collect()
    }

    fn to_borrowed_entries(raw: &[(u32, Vec<u8>, u32, u32)]) -> Vec<BorrowedStreamEntry<'_>> {
        raw.iter()
            .map(|(n, blob, first_blk, last_blk)| BorrowedStreamEntry {
                stream_id: id32(*n),
                blob: blob.as_slice(),
                first_blk: *first_blk,
                last_blk: *last_blk,
            })
            .collect()
    }

    #[test]
    fn borrowed_encoder_matches_owned_encode_for_empty_and_single_entry() {
        let empty: Vec<(u32, Vec<u8>, u32, u32)> = Vec::new();
        assert_eq!(
            StreamDir::encode_borrowed(&to_borrowed_entries(&empty)),
            StreamDir::new(to_owned_entries(&empty)).encode(),
        );

        let one = vec![(7u32, b"resource-blob".to_vec(), 2u32, 9u32)];
        let owned_bytes = StreamDir::new(to_owned_entries(&one)).encode();
        let borrowed_bytes = StreamDir::encode_borrowed(&to_borrowed_entries(&one));
        assert_eq!(borrowed_bytes, owned_bytes);
        let got = StreamDir::decode(&borrowed_bytes, 1000).expect("decode");
        assert_eq!(got, StreamDir::new(to_owned_entries(&one)));
    }

    #[test]
    fn borrowed_encoder_sizes_its_buffer_exactly() {
        for n in [0usize, 1, 50] {
            let raw: Vec<(u32, Vec<u8>, u32, u32)> = (0..n as u32)
                .map(|i| (i, vec![b'x'; (i as usize) % 7], i, i + 1))
                .collect();
            let borrowed = to_borrowed_entries(&raw);
            let out = StreamDir::encode_borrowed(&borrowed);
            assert_eq!(
                out.capacity(),
                out.len(),
                "buffer should be sized exactly for {n} entries"
            );
        }
    }

    #[test]
    fn uvarint_len_matches_put_uvarint_at_every_width_boundary() {
        let mut values = vec![0u64, u64::from(u32::MAX), u64::MAX];
        for shift in (7..64).step_by(7) {
            values.push((1u64 << shift) - 1);
            values.push(1u64 << shift);
        }
        for v in values {
            let mut out = Vec::new();
            put_uvarint(&mut out, v);
            assert_eq!(uvarint_len(v), out.len(), "value {v}");
        }
    }

    #[test]
    fn borrowed_encoder_sizes_multi_byte_varints_exactly() {
        let raw = vec![
            (1u32, vec![b'x'; 127], 127u32, 128u32),
            (2u32, vec![b'y'; 128], 16_383u32, 16_384u32),
            (3u32, vec![b'z'; 16_384], u32::MAX, 0u32),
        ];
        let out = StreamDir::encode_borrowed(&to_borrowed_entries(&raw));
        assert_eq!(out.capacity(), out.len());
        assert_eq!(
            StreamDir::decode(&out, 1000).expect("decode"),
            StreamDir::new(to_owned_entries(&raw))
        );
    }

    mod proptests {
        use proptest::prelude::*;

        use super::*;

        fn arb_entries() -> impl Strategy<Value = Vec<(u32, Vec<u8>, u32, u32)>> {
            proptest::collection::btree_set(any::<u32>(), 0..=300usize).prop_flat_map(|ids| {
                let ids: Vec<u32> = ids.into_iter().collect();
                let n = ids.len();
                (
                    Just(ids),
                    proptest::collection::vec(proptest::collection::vec(any::<u8>(), 0..400), n),
                    proptest::collection::vec(any::<u32>(), n),
                    proptest::collection::vec(any::<u32>(), n),
                )
                    .prop_map(|(ids, blobs, firsts, lasts)| {
                        ids.into_iter()
                            .zip(blobs)
                            .zip(firsts)
                            .zip(lasts)
                            .map(|(((id, blob), first_blk), last_blk)| {
                                (id, blob, first_blk, last_blk)
                            })
                            .collect()
                    })
            })
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(256))]

            #[test]
            fn borrowed_encoder_matches_owned_encode_byte_for_byte(raw in arb_entries()) {
                let owned = StreamDir::new(to_owned_entries(&raw));
                let owned_bytes = owned.encode();
                let borrowed_bytes = StreamDir::encode_borrowed(&to_borrowed_entries(&raw));
                prop_assert_eq!(&borrowed_bytes, &owned_bytes);
                prop_assert_eq!(borrowed_bytes.capacity(), borrowed_bytes.len());

                let decoded = StreamDir::decode(&borrowed_bytes, raw.len() as u64 + 1).expect("decode");
                prop_assert_eq!(decoded, owned);
            }
        }
    }
}
