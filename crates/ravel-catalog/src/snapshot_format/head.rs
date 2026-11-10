//! HEAD record: small, mutable, CAS-updated; bare protobuf (like commit
//! records), no envelope.

use prost::Message;
use ravel_proto::catalog::v1::SnapshotHead;

use super::error::SnapshotFormatError;

/// HEAD `format_version` 1: no part ref carries `retired_hours`.
pub const HEAD_FORMAT_VERSION_1: u32 = 1;

/// HEAD `format_version` 2: at least one part ref carries a non-empty
/// `retired_hours` (ADR-3939 decision 6). A reader that knows only 1 rejects
/// it, so it never serves a retired hour's entries.
pub const HEAD_FORMAT_VERSION_2: u32 = 2;

/// Every HEAD `format_version` this build reads. Which one a HEAD carries is
/// a function of its content, [`head_format_version_for`]; a version above
/// all of these is newer ([`head_format_version_is_newer`]).
pub const HEAD_FORMAT_VERSIONS: [u32; 2] = [HEAD_FORMAT_VERSION_1, HEAD_FORMAT_VERSION_2];

/// The `format_version` `head`'s content requires: 2 exactly when some part
/// ref carries a retired hour, else 1. Every HEAD writer stamps this, and
/// `validate_head` rejects a HEAD stamped otherwise.
pub fn head_format_version_for(head: &SnapshotHead) -> u32 {
    if head.parts.iter().any(|part| !part.retired_hours.is_empty()) {
        HEAD_FORMAT_VERSION_2
    } else {
        HEAD_FORMAT_VERSION_1
    }
}

/// Whether a rejected HEAD `format_version` is above every version this build
/// reads, so the HEAD was written by a newer build (ADR-0066 decision 2).
pub fn head_format_version_is_newer(format_version: u32) -> bool {
    HEAD_FORMAT_VERSIONS
        .iter()
        .all(|known| format_version > *known)
}

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
    if !HEAD_FORMAT_VERSIONS.contains(&head.format_version) {
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
        if let Some(pair) = part
            .retired_hours
            .windows(2)
            .find(|pair| pair[0] >= pair[1])
        {
            return Err(SnapshotFormatError::RetiredHoursNotAscending {
                index,
                previous: pair[0],
                next: pair[1],
            });
        }
        if let Some(&hour) = part
            .retired_hours
            .iter()
            .find(|&&hour| hour < part.min_hour || hour > part.watermark_hour)
        {
            return Err(SnapshotFormatError::RetiredHourOutsidePart {
                index,
                hour,
                min_hour: part.min_hour,
                watermark: part.watermark_hour,
            });
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
    let required = head_format_version_for(head);
    if head.format_version != required {
        return Err(SnapshotFormatError::HeadVersionContentMismatch {
            format_version: head.format_version,
            required,
        });
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
