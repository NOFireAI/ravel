//! HEAD record: small, mutable, CAS-updated; bare protobuf (like commit
//! records), no envelope.

use std::collections::HashSet;

use prost::Message;
use ravel_proto::catalog::v1::SnapshotHead;

use super::error::SnapshotFormatError;

/// HEAD's `format_version`. This is the v1 layout.
pub const HEAD_FORMAT_VERSION: u32 = 1;

/// Encodes a HEAD record. Validates the same fields `decode_head` enforces,
/// so a HEAD this function writes can never fail its own decode.
pub fn encode_head(head: &SnapshotHead) -> Result<Vec<u8>, SnapshotFormatError> {
    validate_head(head)?;
    Ok(head.encode_to_vec())
}

/// Decodes and validates a HEAD record. Every byte is untrusted; every
/// failure is a typed error, never a panic.
pub fn decode_head(bytes: &[u8]) -> Result<SnapshotHead, SnapshotFormatError> {
    let head =
        SnapshotHead::decode(bytes).map_err(|e| SnapshotFormatError::HeadDecode(e.to_string()))?;
    validate_head(&head)?;
    Ok(head)
}

/// Every object key `head` references: the set a garbage collector must treat
/// as live. Deleting a key in this set breaks the snapshot the HEAD publishes,
/// so anything that decides reachability from a HEAD derives it here rather
/// than listing the reference fields itself.
///
/// The exhaustive destructuring below carries the invariant: it has no `..`
/// rest pattern, so adding a field to `SnapshotHead` stops this function
/// compiling until the new field is classified as a reference (contributing
/// its key) or as not one (bound to `_`). A hand-maintained list in a caller
/// gives a silent wrong answer instead, which for a sweeper means deleting a
/// live object.
pub fn head_referenced_keys(head: &SnapshotHead) -> HashSet<String> {
    let SnapshotHead {
        format_version: _,
        tenant_hash: _,
        signal: _,
        shard_count: _,
        watermark_hour: _,
        parts,
        folder_id: _,
        created_unix_ns: _,
        postings,
        shard_generation_count: _,
        column_stats,
        column_stats_part,
    } = head;

    let mut keys = HashSet::with_capacity(parts.len() + 3);
    keys.extend(parts.iter().map(|part| part.key.clone()));
    keys.extend(postings.iter().map(|postings| postings.key.clone()));
    keys.extend(column_stats.iter().map(|stats| stats.key.clone()));
    keys.extend(column_stats_part.iter().map(|stats| stats.key.clone()));
    keys
}

fn validate_head(head: &SnapshotHead) -> Result<(), SnapshotFormatError> {
    if head.format_version != HEAD_FORMAT_VERSION {
        return Err(SnapshotFormatError::UnsupportedHeadVersion(
            head.format_version,
        ));
    }
    if head.tenant_hash.len() != 16 {
        return Err(SnapshotFormatError::BadHeadTenantHashLen(
            head.tenant_hash.len(),
        ));
    }
    if head.folder_id.len() != 16 {
        return Err(SnapshotFormatError::BadFolderIdLen(head.folder_id.len()));
    }
    if head.parts.is_empty() {
        return Err(SnapshotFormatError::HeadNoParts);
    }

    let mut max_watermark = 0u32;
    for (index, part) in head.parts.iter().enumerate() {
        if part.blake3.len() != 32 {
            return Err(SnapshotFormatError::BadPartRefFieldLen {
                index,
                field: "blake3",
                expected: 32,
                actual: part.blake3.len(),
            });
        }
        if part.key.is_empty() {
            return Err(SnapshotFormatError::EmptyPartKey { index });
        }
        max_watermark = max_watermark.max(part.watermark_hour);
    }

    // Multi-part ordering rules (ADR-0063). A single-part head keeps v1
    // semantics unchanged: its part's min_hour is unconstrained (a legacy
    // ref decodes min_hour to 0, but a non-zero value is equally valid), so
    // none of these apply when there is exactly one part.
    if head.parts.len() > 1 {
        for (index, part) in head.parts.iter().enumerate() {
            // Each part's own range must be sane; the head cannot open the
            // part object to verify this against its header, so it checks the
            // mirrored ref fields directly.
            if part.min_hour > part.watermark_hour {
                return Err(SnapshotFormatError::PartRefRangeInverted {
                    index,
                    min_hour: part.min_hour,
                    watermark: part.watermark_hour,
                });
            }
        }
        for index in 1..head.parts.len() {
            let prev = &head.parts[index - 1];
            let cur = &head.parts[index];
            // Ascending by min_hour.
            if cur.min_hour < prev.min_hour {
                return Err(SnapshotFormatError::PartsNotSortedByMinHour { index });
            }
            // Ranges strictly disjoint: an hour bucket belongs to exactly one
            // part, so the next part must start strictly after the previous
            // part's watermark.
            if prev.watermark_hour >= cur.min_hour {
                return Err(SnapshotFormatError::PartRangesOverlap {
                    index,
                    prev_watermark: prev.watermark_hour,
                    next_min_hour: cur.min_hour,
                });
            }
        }
    }
    if head.watermark_hour != max_watermark {
        return Err(SnapshotFormatError::HeadWatermarkMismatch {
            head: head.watermark_hour,
            max_part: max_watermark,
        });
    }

    if let Some(postings) = &head.postings {
        if postings.blake3.len() != 32 {
            return Err(SnapshotFormatError::BadPostingsRefBlake3Len(
                postings.blake3.len(),
            ));
        }
        if postings.key.is_empty() {
            return Err(SnapshotFormatError::EmptyPostingsKey);
        }
        if postings.part_blake3.len() != head.parts.len() {
            return Err(SnapshotFormatError::PostingsRefPartCountMismatch {
                postings_parts: postings.part_blake3.len(),
                head_parts: head.parts.len(),
            });
        }
        for (index, (postings_hash, part)) in postings
            .part_blake3
            .iter()
            .zip(head.parts.iter())
            .enumerate()
        {
            if postings_hash.len() != 32 {
                return Err(SnapshotFormatError::BadPostingsRefPartBlake3Len {
                    index,
                    actual: postings_hash.len(),
                });
            }
            if postings_hash != &part.blake3 {
                return Err(SnapshotFormatError::PostingsRefPartBlake3Mismatch { index });
            }
        }
    }

    if let Some(column_stats) = &head.column_stats {
        if column_stats.blake3.len() != 32 {
            return Err(SnapshotFormatError::BadColumnStatsRefBlake3Len(
                column_stats.blake3.len(),
            ));
        }
        if column_stats.key.is_empty() {
            return Err(SnapshotFormatError::EmptyColumnStatsKey);
        }
        if column_stats.part_blake3.len() != head.parts.len() {
            return Err(SnapshotFormatError::ColumnStatsRefPartCountMismatch {
                stats_parts: column_stats.part_blake3.len(),
                head_parts: head.parts.len(),
            });
        }
        for (index, (stats_hash, part)) in column_stats
            .part_blake3
            .iter()
            .zip(head.parts.iter())
            .enumerate()
        {
            if stats_hash.len() != 32 {
                return Err(SnapshotFormatError::BadColumnStatsRefPartBlake3Len {
                    index,
                    actual: stats_hash.len(),
                });
            }
            if stats_hash != &part.blake3 {
                return Err(SnapshotFormatError::ColumnStatsRefPartBlake3Mismatch { index });
            }
        }
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use ravel_proto::catalog::v1::{
        SnapshotColumnStatsPartRef, SnapshotColumnStatsRef, SnapshotPartRef, SnapshotPostingsRef,
    };

    use super::*;

    fn part(key: &str, blake3: u8, min_hour: u32, watermark_hour: u32) -> SnapshotPartRef {
        SnapshotPartRef {
            key: key.to_string(),
            blake3: vec![blake3; 32],
            size: 1,
            entry_count: 1,
            watermark_hour,
            min_hour,
        }
    }

    fn head(parts: Vec<SnapshotPartRef>) -> SnapshotHead {
        SnapshotHead {
            format_version: HEAD_FORMAT_VERSION,
            tenant_hash: vec![0x11; 16],
            signal: 0,
            shard_count: 1,
            watermark_hour: 10,
            parts,
            folder_id: vec![0x22; 16],
            created_unix_ns: 0,
            postings: None,
            shard_generation_count: 1,
            column_stats: None,
            column_stats_part: None,
        }
    }

    /// A head naming every kind of reference contributes EXACTLY one key per
    /// reference and nothing else. The equality against the full expected set
    /// is the point: a missing key means a garbage collector deletes a live
    /// object, an extra one means it never collects a real orphan.
    #[test]
    fn referenced_keys_are_every_reference_and_only_those() {
        let mut h = head(vec![part("snap/a", 1, 0, 5), part("snap/b", 2, 6, 10)]);
        h.postings = Some(SnapshotPostingsRef {
            key: "idx/p".to_string(),
            blake3: vec![3; 32],
            size: 1,
            name_count: 1,
            part_blake3: vec![vec![1; 32], vec![2; 32]],
        });
        h.column_stats = Some(SnapshotColumnStatsRef {
            key: "idx/v1.cstat".to_string(),
            blake3: vec![4; 32],
            size: 1,
            segment_count: 1,
            part_blake3: vec![vec![1; 32], vec![2; 32]],
        });
        h.column_stats_part = Some(SnapshotColumnStatsPartRef {
            key: "idx/v2.cstat".to_string(),
            blake3: vec![5; 32],
            size: 1,
            segment_count: 1,
            part_blake3: vec![vec![1; 32], vec![2; 32]],
        });
        encode_head(&h).expect("the fixture is a valid head");

        let expected: HashSet<String> =
            ["snap/a", "snap/b", "idx/p", "idx/v1.cstat", "idx/v2.cstat"]
                .into_iter()
                .map(str::to_string)
                .collect();
        assert_eq!(head_referenced_keys(&h), expected);
    }

    /// Absent optional references contribute nothing: the set is exactly the
    /// part keys.
    #[test]
    fn referenced_keys_of_a_parts_only_head_are_the_part_keys() {
        let h = head(vec![part("snap/a", 1, 0, 10)]);
        let expected: HashSet<String> = ["snap/a".to_string()].into_iter().collect();
        assert_eq!(head_referenced_keys(&h), expected);
    }
}
