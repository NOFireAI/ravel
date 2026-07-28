//! FIELD_DIR section (docs/log-segment-format.md "FIELD_DIR").
//!
//! Maps each dynamic attribute `(name, type)` to its column id and object-wide
//! stats. Fixed columns (ids 0..=9) never appear here; dynamic columns start at
//! [`crate::record::FIRST_DYNAMIC_COL`]. A key observed with two value types
//! yields two entries (per-type splitting). Entries are sorted ascending by
//! `(name bytes, type)`. Decode is untrusted: a non-ascending sequence, a count
//! over the cap, an unknown type byte, or truncation is `Corrupted`.

use crate::error::LogSegError;
use crate::record::FieldType;
use crate::varint::{get_uvarint, put_uvarint};

/// One FIELD_DIR entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FieldEntry {
    pub name: String,
    pub ty: FieldType,
    pub column_id: u32,
    pub present_blocks: u32,
    pub null_count: u64,
}

/// The decoded FIELD_DIR: entries sorted ascending by `(name bytes, type)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FieldDir {
    entries: Vec<FieldEntry>,
}

impl FieldDir {
    /// Builds a directory from entries already sorted by `(name bytes, type)`.
    pub fn new(entries: Vec<FieldEntry>) -> Self {
        FieldDir { entries }
    }

    pub fn entries(&self) -> &[FieldEntry] {
        &self.entries
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The entry for `(name, ty)`, if present.
    pub fn column(&self, name: &str, ty: FieldType) -> Option<&FieldEntry> {
        self.entries
            .binary_search_by(|e| {
                e.name
                    .as_bytes()
                    .cmp(name.as_bytes())
                    .then_with(|| e.ty.to_u8().cmp(&ty.to_u8()))
            })
            .ok()
            .map(|i| &self.entries[i])
    }

    /// The entry with the given column id, if present.
    pub fn by_column_id(&self, column_id: u32) -> Option<&FieldEntry> {
        self.entries.iter().find(|e| e.column_id == column_id)
    }

    /// Serializes the section in its uncompressed form.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());
        for e in &self.entries {
            put_uvarint(&mut out, e.name.len() as u64);
            out.extend_from_slice(e.name.as_bytes());
            out.push(e.ty.to_u8());
            put_uvarint(&mut out, u64::from(e.column_id));
            put_uvarint(&mut out, u64::from(e.present_blocks));
            put_uvarint(&mut out, e.null_count);
        }
        out
    }

    /// Decodes the uncompressed section form. Rejects a count over
    /// `max_entries`, an unknown type byte, a non-ascending `(name, type)`
    /// sequence, truncation, and trailing bytes.
    pub fn decode(bytes: &[u8], max_entries: u64) -> Result<Self, LogSegError> {
        let count_bytes = bytes
            .get(0..4)
            .ok_or_else(|| LogSegError::Corrupted("field_dir truncated at count".into()))?;
        let count = u32::from_le_bytes([
            count_bytes[0],
            count_bytes[1],
            count_bytes[2],
            count_bytes[3],
        ]) as u64;
        if count > max_entries {
            return Err(LogSegError::Corrupted(format!(
                "field_dir count {count} over cap {max_entries}"
            )));
        }
        let mut pos = 4usize;
        let mut entries: Vec<FieldEntry> = Vec::with_capacity(count.min(1 << 16) as usize);
        let mut prev: Option<(Vec<u8>, u8)> = None;
        for _ in 0..count {
            let name_len = get_uvarint(bytes, &mut pos)?;
            let name_len = usize::try_from(name_len)
                .map_err(|_| LogSegError::Corrupted("field_dir name len".into()))?;
            let end = pos
                .checked_add(name_len)
                .ok_or_else(|| LogSegError::Corrupted("field_dir name overflow".into()))?;
            let name_bytes = bytes
                .get(pos..end)
                .ok_or_else(|| LogSegError::Corrupted("field_dir name truncated".into()))?;
            let name = std::str::from_utf8(name_bytes)
                .map_err(|_| LogSegError::Corrupted("field_dir name not utf-8".into()))?
                .to_string();
            pos = end;
            let ty_byte = *bytes
                .get(pos)
                .ok_or_else(|| LogSegError::Corrupted("field_dir truncated at type".into()))?;
            pos += 1;
            let ty = FieldType::from_u8(ty_byte)
                .ok_or_else(|| LogSegError::Corrupted(format!("field_dir bad type {ty_byte}")))?;
            let key = (name.clone().into_bytes(), ty_byte);
            if prev.as_ref().is_some_and(|p| key <= *p) {
                return Err(LogSegError::Corrupted("field_dir not ascending".into()));
            }
            prev = Some(key);
            let column_id = u32::try_from(get_uvarint(bytes, &mut pos)?)
                .map_err(|_| LogSegError::Corrupted("field_dir column_id range".into()))?;
            let present_blocks = u32::try_from(get_uvarint(bytes, &mut pos)?)
                .map_err(|_| LogSegError::Corrupted("field_dir present_blocks range".into()))?;
            let null_count = get_uvarint(bytes, &mut pos)?;
            entries.push(FieldEntry {
                name,
                ty,
                column_id,
                present_blocks,
                null_count,
            });
        }
        if pos != bytes.len() {
            return Err(LogSegError::Corrupted("field_dir trailing bytes".into()));
        }
        Ok(FieldDir { entries })
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    fn dir() -> FieldDir {
        // Sorted by (name bytes, type). "http" appears as both Str and I64.
        FieldDir::new(vec![
            FieldEntry {
                name: "http".into(),
                ty: FieldType::Str,
                column_id: 10,
                present_blocks: 3,
                null_count: 100,
            },
            FieldEntry {
                name: "http".into(),
                ty: FieldType::I64,
                column_id: 11,
                present_blocks: 2,
                null_count: 5,
            },
            FieldEntry {
                name: "level".into(),
                ty: FieldType::Str,
                column_id: 12,
                present_blocks: 8,
                null_count: 0,
            },
        ])
    }

    #[test]
    fn roundtrip_and_lookup() {
        let d = dir();
        let bytes = d.encode();
        let got = FieldDir::decode(&bytes, 1000).expect("decode");
        assert_eq!(got, d);
        assert_eq!(
            got.column("http", FieldType::Str).map(|e| e.column_id),
            Some(10)
        );
        assert_eq!(
            got.column("http", FieldType::I64).map(|e| e.column_id),
            Some(11)
        );
        assert_eq!(
            got.column("level", FieldType::Str).map(|e| e.column_id),
            Some(12)
        );
        assert!(got.column("level", FieldType::I64).is_none());
        assert!(got.column("missing", FieldType::Str).is_none());
        assert_eq!(got.by_column_id(11).map(|e| e.name.as_str()), Some("http"));
    }

    #[test]
    fn rejects_unsorted_and_bad_type() {
        let d = FieldDir::new(vec![
            FieldEntry {
                name: "b".into(),
                ty: FieldType::Str,
                column_id: 10,
                present_blocks: 0,
                null_count: 0,
            },
            FieldEntry {
                name: "a".into(),
                ty: FieldType::Str,
                column_id: 11,
                present_blocks: 0,
                null_count: 0,
            },
        ]);
        assert!(matches!(
            FieldDir::decode(&d.encode(), 1000),
            Err(LogSegError::Corrupted(_))
        ));
        // Corrupt the type byte of a single-entry directory to an unknown value.
        let single = FieldDir::new(vec![FieldEntry {
            name: "a".into(),
            ty: FieldType::Str,
            column_id: 10,
            present_blocks: 0,
            null_count: 0,
        }]);
        let mut bytes = single.encode();
        // layout: count(4) name_len(1) 'a'(1) type(1) ...
        bytes[6] = 0xff;
        assert!(matches!(
            FieldDir::decode(&bytes, 1000),
            Err(LogSegError::Corrupted(_))
        ));
    }

    #[test]
    fn rejects_count_over_cap_and_truncation() {
        let bytes = dir().encode();
        assert!(matches!(
            FieldDir::decode(&bytes, 1),
            Err(LogSegError::Corrupted(_))
        ));
        assert!(matches!(
            FieldDir::decode(&bytes[..bytes.len() - 1], 1000),
            Err(LogSegError::Corrupted(_))
        ));
    }
}
