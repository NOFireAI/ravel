//! HEAD record: small, mutable, CAS-updated; bare protobuf (like commit
//! records), no envelope (docs/metric-index-plan.md 3.2).

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
    Ok(())
}
