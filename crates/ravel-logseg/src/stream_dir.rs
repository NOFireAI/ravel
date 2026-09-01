//! STREAM_DIR section (docs/log-segment-format.md "STREAM_DIR").
//!
//! Maps each log stream id to its canonical resource+scope blob and the block
//! range holding its records. Entries are sorted ascending by `stream_id`, so
//! the dense `stream_ref` used everywhere else is the entry ordinal and lookup
//! is a binary search. Decode treats every field as untrusted: a non-ascending
//! sequence, an entry count over the cap, or any truncation is `Corrupted`.

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
        let mut out = Vec::new();
        out.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for e in &self.entries {
            out.extend_from_slice(&e.stream_id.0);
            put_uvarint(&mut out, e.blob.len() as u64);
            out.extend_from_slice(&e.blob);
            put_uvarint(&mut out, u64::from(e.first_blk));
            put_uvarint(&mut out, u64::from(e.last_blk));
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
}
