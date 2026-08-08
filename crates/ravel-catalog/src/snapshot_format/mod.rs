//! Codec for the metric index's snapshot part envelope and HEAD record
//! (docs/metric-index-plan.md 3.1, 3.2; ADR-0020). Pure encode/decode: this
//! module performs no store I/O and is not yet wired into `Catalog` (that is
//! phase 2/3's job). Kept in one place so writer and reader can't drift
//! apart, mirroring `ravel-segment`'s `format.rs` convention.

mod error;
mod head;
mod part;
mod postings;

pub use error::SnapshotFormatError;
pub use head::{HEAD_FORMAT_VERSION, decode_head, encode_head};
pub use part::{DecodedPart, decode_part, encode_part, encode_part_ranged};
pub use postings::{
    DecodedPostings, NamePostings, decode_postings, encode_postings, postings_declared_tenant_hash,
};

/// Envelope magic, first 4 bytes of every snapshot part object.
pub const MAGIC: [u8; 4] = *b"RCS1";

/// Envelope format version. This is the v1 layout.
pub const VERSION: u8 = 1;

/// Reserved envelope bytes; must always be zero in v1.
pub const RESERVED: [u8; 3] = [0, 0, 0];

/// zstd compression level for the entry body.
pub(crate) const ZSTD_LEVEL: i32 = 3;

/// Minimum possible envelope size: every fixed-width field present, with a
/// zero-length header and zero-length body.
pub(crate) const MIN_ENVELOPE_LEN: usize = 4 + 1 + 3 + 4 + 8 + 4 + 4;

/// Default resource cap for a part's declared decompressed entry-body size
/// (docs/metric-index-plan.md 3.1: `max_snapshot_part_bytes`, default
/// 256 MiB), applied before decompression allocates a buffer.
pub const DEFAULT_MAX_SNAPSHOT_PART_BYTES: u64 = 256 << 20;

/// Decode-time resource bound, checked before a part's entry body is
/// decompressed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartLimits {
    pub max_snapshot_part_bytes: u64,
}

impl Default for PartLimits {
    fn default() -> Self {
        PartLimits {
            max_snapshot_part_bytes: DEFAULT_MAX_SNAPSHOT_PART_BYTES,
        }
    }
}

/// Envelope magic, first 4 bytes of every name-postings object
/// (docs/metric-index-plan.md 3.3, P5a).
pub const POSTINGS_MAGIC: [u8; 4] = *b"RNP1";

/// Postings envelope format version. This is the v1 layout.
pub const POSTINGS_VERSION: u8 = 1;

/// Reserved postings envelope bytes; must always be zero in v1.
pub const POSTINGS_RESERVED: [u8; 3] = [0, 0, 0];

/// Minimum possible postings envelope size: every fixed-width field
/// present, with a zero-length header and zero-length body.
pub(crate) const MIN_POSTINGS_ENVELOPE_LEN: usize = 4 + 1 + 3 + 4 + 8 + 4 + 4;

/// Default resource cap for a postings object's declared decompressed body
/// size, applied before decompression allocates a buffer.
pub const DEFAULT_MAX_POSTINGS_BYTES: u64 = 256 << 20;

/// Decode-time resource bound, checked before a postings object's body is
/// decompressed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PostingsLimits {
    pub max_postings_bytes: u64,
}

impl Default for PostingsLimits {
    fn default() -> Self {
        PostingsLimits {
            max_postings_bytes: DEFAULT_MAX_POSTINGS_BYTES,
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use prost::Message;
    use ravel_proto::catalog::v1::{SnapshotEntry, SnapshotHead, SnapshotPartRef};

    use super::*;

    fn part_ref(key: &str, min_hour: u32, watermark_hour: u32) -> SnapshotPartRef {
        SnapshotPartRef {
            key: key.to_string(),
            blake3: vec![0x22; 32],
            size: 1,
            entry_count: 0,
            watermark_hour,
            min_hour,
        }
    }

    fn head_with_parts(parts: Vec<SnapshotPartRef>) -> SnapshotHead {
        let watermark_hour = parts.iter().map(|p| p.watermark_hour).max().unwrap_or(0);
        SnapshotHead {
            format_version: HEAD_FORMAT_VERSION,
            tenant_hash: vec![0x11; 16],
            signal: 1,
            shard_count: 8,
            watermark_hour,
            parts,
            postings: None,
            folder_id: vec![0x33; 16],
            created_unix_ns: 0,
            shard_generation_count: 1,
        }
    }

    /// Acceptance test for EH-T1 (#740): a multi-part head with correctly
    /// ordered, strictly-disjoint hour ranges round-trips; each ordering
    /// violation surfaces its own distinct typed error, never a panic.
    #[test]
    fn multi_part_head_ranges_validated() {
        // Ordered, disjoint ranges: [0,4], [5,9], [10,14]. Round-trips clean.
        let valid = head_with_parts(vec![
            part_ref("a", 0, 4),
            part_ref("b", 5, 9),
            part_ref("c", 10, 14),
        ]);
        let bytes = encode_head(&valid).expect("valid multi-part head encodes");
        assert_eq!(decode_head(&bytes).expect("decodes"), valid);

        // Out-of-order min_hour: part[1] starts before part[0]. Head watermark
        // is still the max over parts, so the ordering rule is what trips.
        let mut unsorted = head_with_parts(vec![
            part_ref("a", 5, 9),
            part_ref("b", 0, 4),
            part_ref("c", 10, 14),
        ]);
        unsorted.watermark_hour = 14;
        let err = decode_head(&unsorted.encode_to_vec()).expect_err("unsorted rejected");
        assert_eq!(
            err,
            SnapshotFormatError::PartsNotSortedByMinHour { index: 1 }
        );

        // Overlapping ranges: part[0] covers [0,10], part[1] starts at 5 (<=
        // 10), so the two ranges share hours 5..=10.
        let overlap = head_with_parts(vec![part_ref("a", 0, 10), part_ref("b", 5, 15)]);
        let err = decode_head(&overlap.encode_to_vec()).expect_err("overlap rejected");
        assert_eq!(
            err,
            SnapshotFormatError::PartRangesOverlap {
                index: 1,
                prev_watermark: 10,
                next_min_hour: 5,
            }
        );

        // Boundary case that discriminates the disjointness operator itself:
        // part[0]'s watermark_hour (4) equals part[1]'s min_hour (4), so hour
        // 4 would live in both parts under a too-loose `>` comparison and
        // only a strict `>=` correctly rejects it. The [0,10]/[5,15] case
        // above is rejected under either operator and the exact-adjacency
        // valid case above (4/5, 9/10) is accepted under either operator, so
        // neither pins this direction on its own -- this one does.
        let touching = head_with_parts(vec![part_ref("a", 0, 4), part_ref("b", 4, 9)]);
        let err = decode_head(&touching.encode_to_vec()).expect_err("touching ranges rejected");
        assert_eq!(
            err,
            SnapshotFormatError::PartRangesOverlap {
                index: 1,
                prev_watermark: 4,
                next_min_hour: 4,
            }
        );

        // Inverted range: part[1] claims min_hour 10 > watermark_hour 5.
        let mut inverted = head_with_parts(vec![
            part_ref("a", 0, 4),
            part_ref("b", 10, 5),
            part_ref("c", 11, 14),
        ]);
        inverted.watermark_hour = 14;
        let err = decode_head(&inverted.encode_to_vec()).expect_err("inverted rejected");
        assert_eq!(
            err,
            SnapshotFormatError::PartRefRangeInverted {
                index: 1,
                min_hour: 10,
                watermark: 5,
            }
        );
    }

    /// A legacy-shaped single-part head and part (no `min_hour` set, so it
    /// decodes to proto3 zero) still validate under the new multi-part
    /// checks: the additive-field backward-compatibility claim, proven. A
    /// single part is also exempt from the ordering rules, so even a
    /// non-zero or inverted single-part range stays valid (v1 semantics
    /// unchanged, no hidden new constraint).
    #[test]
    fn legacy_single_part_head_and_part_validate() {
        // Single part, min_hour unset (0): the common legacy HEAD shape.
        let legacy = head_with_parts(vec![part_ref("a", 0, 7)]);
        let bytes = encode_head(&legacy).expect("legacy single-part head encodes");
        assert_eq!(decode_head(&bytes).expect("decodes"), legacy);

        // Single part with a non-zero, even inverted, range: still valid,
        // because none of the multi-part rules apply to a lone part.
        let mut lone_inverted = head_with_parts(vec![part_ref("a", 100, 5)]);
        lone_inverted.watermark_hour = 5;
        encode_head(&lone_inverted).expect("single-part range is unconstrained");

        // A part object written by `encode_part` carries min_hour 0 (the
        // epoch floor) and decodes clean, entry hours accepted from 0 up.
        let entry = SnapshotEntry {
            level: 0,
            shard: 0,
            ingest_hour_bucket: 0,
            writer_id: vec![0xAA; 16],
            writer_epoch: 1,
            writer_seq: 1,
            content_hash: vec![0xBB; 32],
            object_size: 100,
            min_event_ts_ns: 0,
            max_event_ts_ns: 100,
            sample_count: 1,
            series_count: 1,
            segment_format_version: 1,
            created_unix_ns: 1_000,
        };
        let part =
            encode_part([0x11; 16], 1, 8, 5, std::slice::from_ref(&entry)).expect("encode part");
        let decoded = decode_part(&part, &PartLimits::default()).expect("decode legacy part");
        assert_eq!(decoded.header.min_hour, 0);
        assert_eq!(decoded.entries, vec![entry]);
    }

    /// Pins the persistent-format constants. A change here is a format
    /// change (docs/metric-index-plan.md 3.1, .claude/skills/format-change),
    /// never a refactor.
    #[test]
    fn format_constants_are_pinned() {
        assert_eq!(MAGIC, *b"RCS1");
        assert_eq!(VERSION, 1);
        assert_eq!(RESERVED, [0, 0, 0]);
        assert_eq!(HEAD_FORMAT_VERSION, 1);
        assert_eq!(DEFAULT_MAX_SNAPSHOT_PART_BYTES, 256 << 20);
        assert_eq!(POSTINGS_MAGIC, *b"RNP1");
        assert_eq!(POSTINGS_VERSION, 1);
        assert_eq!(POSTINGS_RESERVED, [0, 0, 0]);
        assert_eq!(DEFAULT_MAX_POSTINGS_BYTES, 256 << 20);
    }
}
