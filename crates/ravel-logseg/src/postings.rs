//! POSTINGS section: exact block-level attribute pruning, opt-in per field
//! (docs/adrs/0049-rlog-postings.md, docs/log-segment-format.md "POSTINGS").
//!
//! Per indexed field (a dynamic attribute column), a sorted term dictionary
//! maps each distinct value to the sorted set of block indices that carry a
//! row with that value. The dictionary is split into fixed-stride term
//! blocks, each independently zstd-compressed and crc32c-verified, addressed
//! through a sparse index holding every block's first term
//! (docs/adrs/0049-rlog-postings.md decision 2, mirroring RSEG's
//! `SERIES_IDX`). A query with an equality predicate on an indexed field
//! binary-searches the sparse index, decompresses one term block, and
//! intersects its posting list with the candidate set.
//!
//! Absence is always legal: a field with no POSTINGS entry (never indexed,
//! or dropped for exceeding its distinct-value cap) simply cannot be pruned
//! by this section, and the reader falls back to bloom pruning plus an exact
//! scan (docs/adrs/0049-rlog-postings.md decision 5). This section is new
//! ("format only": ADR-0029's versioning carve-out covers a new section kind
//! without a version bump, since an unknown kind is already skipped and an
//! absent kind is already legal), so `POSTINGS_VERSION` here is this
//! section's own internal grammar version, independent of the RLOG trailer
//! `VERSION`.

use crate::error::LogSegError;

/// This section's internal grammar version (independent of the RLOG trailer
/// version; see the module doc for why no trailer bump is needed).
pub const POSTINGS_VERSION: u8 = 1;

/// A decoded POSTINGS section: zero or more indexed fields, ascending by
/// `column_id`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PostingsSection {
    pub fields: Vec<PostingsField>,
}

/// One indexed field's postings, or a record that it was capped out of this
/// object (docs/adrs/0049-rlog-postings.md decision 4).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostingsField {
    pub column_id: u32,
    pub capped: bool,
}

impl PostingsSection {
    /// Encodes the section body (docs/log-segment-format.md "POSTINGS"):
    /// `version: u8`, `field_count: u32 LE`, then per field (ascending
    /// `column_id`) a header. Term dictionaries and posting lists are added
    /// once field-building lands; an empty section (no indexed fields) is
    /// already a complete, legal, round-trippable encoding.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.push(POSTINGS_VERSION);
        out.extend_from_slice(&(self.fields.len() as u32).to_le_bytes());
        for f in &self.fields {
            crate::varint::put_uvarint(&mut out, u64::from(f.column_id));
            out.push(u8::from(f.capped));
        }
        out
    }

    /// Decodes and validates a POSTINGS section body. Every count is
    /// bounds-checked before use; `column_id` must be strictly ascending
    /// across fields (required for the reader's future binary-search probe);
    /// trailing bytes after the last field are rejected.
    pub fn decode(bytes: &[u8]) -> Result<Self, LogSegError> {
        let mut pos = 0usize;
        let version = *bytes
            .get(pos)
            .ok_or_else(|| LogSegError::Corrupted("postings version truncated".into()))?;
        pos += 1;
        if version != POSTINGS_VERSION {
            return Err(LogSegError::Corrupted(format!(
                "unsupported postings version {version}"
            )));
        }
        let count_bytes = bytes
            .get(pos..pos + 4)
            .ok_or_else(|| LogSegError::Corrupted("postings field_count truncated".into()))?;
        let field_count = u32::from_le_bytes(count_bytes.try_into().unwrap());
        pos += 4;
        if u64::from(field_count) > MAX_POSTINGS_FIELDS {
            return Err(LogSegError::Corrupted(
                "postings field_count over cap".into(),
            ));
        }

        let mut fields = Vec::with_capacity((field_count as usize).min(1 << 12));
        let mut prev_column: Option<u32> = None;
        for _ in 0..field_count {
            let column_id = u32::try_from(crate::varint::get_uvarint(bytes, &mut pos)?)
                .map_err(|_| LogSegError::Corrupted("postings column_id range".into()))?;
            if let Some(prev) = prev_column
                && column_id <= prev
            {
                return Err(LogSegError::Corrupted(
                    "postings column_id not ascending".into(),
                ));
            }
            prev_column = Some(column_id);
            let capped_byte = *bytes
                .get(pos)
                .ok_or_else(|| LogSegError::Corrupted("postings capped flag truncated".into()))?;
            pos += 1;
            let capped = match capped_byte {
                0 => false,
                1 => true,
                other => {
                    return Err(LogSegError::Corrupted(format!(
                        "postings capped flag {other}"
                    )));
                }
            };
            fields.push(PostingsField { column_id, capped });
        }
        if pos != bytes.len() {
            return Err(LogSegError::Corrupted("postings trailing bytes".into()));
        }
        Ok(PostingsSection { fields })
    }
}

/// Sanity ceiling on the number of indexed fields a section may declare,
/// independent of any writer's `postings_max_distinct` config (that caps
/// distinct values per field, not the field count). Matches the crate's other
/// directory-count caps (`MAX_FIELDS` in `reader.rs`).
const MAX_POSTINGS_FIELDS: u64 = 1 << 20;

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn empty_section_round_trips() {
        let section = PostingsSection::default();
        let bytes = section.encode();
        let decoded = PostingsSection::decode(&bytes).expect("decode");
        assert_eq!(decoded, section);
        assert!(decoded.fields.is_empty());
    }

    #[test]
    fn rejects_wrong_version() {
        let mut bytes = PostingsSection::default().encode();
        bytes[0] = POSTINGS_VERSION + 1;
        assert!(matches!(
            PostingsSection::decode(&bytes),
            Err(LogSegError::Corrupted(_))
        ));
    }

    #[test]
    fn rejects_trailing_bytes() {
        let mut bytes = PostingsSection::default().encode();
        bytes.push(0xff);
        assert!(matches!(
            PostingsSection::decode(&bytes),
            Err(LogSegError::Corrupted(_))
        ));
    }

    #[test]
    fn rejects_non_ascending_column_id() {
        let section = PostingsSection {
            fields: vec![
                PostingsField {
                    column_id: 12,
                    capped: false,
                },
                PostingsField {
                    column_id: 12,
                    capped: false,
                },
            ],
        };
        let bytes = section.encode();
        assert!(matches!(
            PostingsSection::decode(&bytes),
            Err(LogSegError::Corrupted(_))
        ));
    }
}
