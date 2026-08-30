//! Codec for the metric index's snapshot part envelope and HEAD record
//! (ADR-0020). Pure encode/decode: this
//! module performs no store I/O and is not yet wired into `Catalog` (that is
//! phase 2/3's job). Kept in one place so writer and reader can't drift
//! apart, mirroring `ravel-segment`'s `format.rs` convention.

mod column_stats;
mod error;
mod head;
mod part;
mod postings;

pub use column_stats::{
    DecodedColumnStats, decode_column_stats, encode_column_stats, encode_column_stats_v2,
};
pub use error::SnapshotFormatError;
pub use head::{HEAD_FORMAT_VERSION, decode_head, encode_head, head_referenced_keys};
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
/// (`max_snapshot_part_bytes`, default
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

/// Envelope magic, first 4 bytes of every name-postings object.
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

/// Envelope magic, first 4 bytes of every column-statistics object
/// (ADR-0850).
pub const COLUMN_STATS_MAGIC: [u8; 4] = *b"RCST";

/// Column-statistics envelope WRITE version: the version the fold stamps into
/// every part-hash-keyed (v2) `.cstat` object it writes (ADR-0942 A2). Single
/// source for the stamped v2 version so a later bump edits one literal.
///
/// The field-11 v1 object is NOT stamped from this constant: during the
/// ADR-0942 dual-publish window the fold keeps writing a v1 (L0-tuple-keyed)
/// `.cstat` under `SnapshotHead.column_stats` byte-for-byte as before, so
/// [`column_stats::encode_column_stats`] stamps envelope version 1 explicitly.
/// This constant governs only the new field-13 v2 object.
pub const COLUMN_STATS_WRITE_VERSION: u8 = 2;

/// Column-statistics envelope ACCEPTED READ SET: the versions
/// `decode_column_stats` accepts. v1 is ADR-0850's L0-tuple keying; v2 is
/// ADR-0942's part-hash keying. The decoder checks MEMBERSHIP against this set,
/// never equality against [`COLUMN_STATS_WRITE_VERSION`]: bumping the write
/// version must not make the decoder reject the v1 objects the ADR-0942 L0
/// reader rule still depends on. Single source for the accepted set so a later
/// version is added in one place. v2 is accepted before anything writes one, so
/// A2's writer and this decoder cannot disagree the moment v2 first appears.
pub const COLUMN_STATS_ACCEPTED_READ_VERSIONS: [u8; 2] = [1, 2];

/// Whether `version` is an accepted `.cstat` envelope read version. Membership,
/// not equality against the write version (ADR-0942).
pub fn column_stats_version_accepted(version: u8) -> bool {
    COLUMN_STATS_ACCEPTED_READ_VERSIONS.contains(&version)
}

/// Reserved column-statistics envelope bytes; must always be zero in v1.
pub const COLUMN_STATS_RESERVED: [u8; 3] = [0, 0, 0];

/// Minimum possible column-statistics envelope size: every fixed-width
/// field present, with a zero-length header and zero-length body.
pub(crate) const MIN_COLUMN_STATS_ENVELOPE_LEN: usize = 4 + 1 + 3 + 4 + 8 + 4 + 4;

/// Default resource cap for a column-stats object's declared decompressed
/// body size, applied before decompression allocates a buffer.
pub const DEFAULT_MAX_COLUMN_STATS_BYTES: u64 = 256 << 20;

/// Fold-time cardinality ceiling: the most distinct values one segment's
/// dictionary for one column may hold before the dictionary is omitted
/// outright (ADR-0850 decision 3). Chosen to comfortably cover legitimate
/// per-segment categorical columns while bounding one segment's dictionary
/// size; never used to truncate, only to gate presence.
pub const DEFAULT_MAX_COLUMN_DICTIONARY_ENTRIES: usize = 10_000;

/// Decode-time resource bound, checked before a column-stats object's body
/// is decompressed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnStatsLimits {
    pub max_column_stats_bytes: u64,
}

impl Default for ColumnStatsLimits {
    fn default() -> Self {
        ColumnStatsLimits {
            max_column_stats_bytes: DEFAULT_MAX_COLUMN_STATS_BYTES,
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
            column_stats: None,
            column_stats_part: None,
        }
    }

    /// Acceptance test: a multi-part head with correctly
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

    /// The per-level writer_id width is the identity contract every producer
    /// of a snapshot entry must honour: a level-0 L0 entry carries the 16-byte
    /// flush writer uuid, a level-1 compaction or rewrite part carries the
    /// 32-byte `input_set_hash` in the same slot (fold.rs
    /// `build_l1_snapshot_entry`). A future producer emitting the wrong width
    /// fails here at encode time, not hours later at a `catalog verify` on a
    /// real tenant (issue #819).
    #[test]
    fn writer_id_width_is_pinned_per_level() {
        let base = SnapshotEntry {
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

        // Level 0 accepts exactly the 16-byte flush writer uuid.
        encode_part([0x11; 16], 1, 8, 5, std::slice::from_ref(&base))
            .expect("16-byte level-0 writer_id encodes");
        let l0_wide = SnapshotEntry {
            writer_id: vec![0xAA; 32],
            ..base.clone()
        };
        assert_eq!(
            encode_part([0x11; 16], 1, 8, 5, std::slice::from_ref(&l0_wide))
                .expect_err("32-byte level-0 writer_id is rejected"),
            SnapshotFormatError::BadFieldLen {
                field: "writer_id",
                expected: 16,
                actual: 32,
            }
        );

        // Level 1 accepts exactly the 32-byte input_set_hash.
        let l1 = SnapshotEntry {
            level: 1,
            writer_id: vec![0xCC; 32],
            writer_epoch: 0,
            writer_seq: 0,
            ..base.clone()
        };
        encode_part([0x11; 16], 1, 8, 5, std::slice::from_ref(&l1))
            .expect("32-byte level-1 writer_id encodes");
        let l1_narrow = SnapshotEntry {
            level: 1,
            writer_id: vec![0xCC; 16],
            writer_epoch: 0,
            writer_seq: 0,
            ..base
        };
        assert_eq!(
            encode_part([0x11; 16], 1, 8, 5, std::slice::from_ref(&l1_narrow))
                .expect_err("16-byte level-1 writer_id is rejected"),
            SnapshotFormatError::BadFieldLen {
                field: "writer_id",
                expected: 32,
                actual: 16,
            }
        );
    }

    /// Pins the persistent-format constants. A change here is a format
    /// change (.claude/skills/format-change),
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
        assert_eq!(COLUMN_STATS_MAGIC, *b"RCST");
        assert_eq!(COLUMN_STATS_WRITE_VERSION, 2);
        assert_eq!(COLUMN_STATS_ACCEPTED_READ_VERSIONS, [1, 2]);
        assert_eq!(COLUMN_STATS_RESERVED, [0, 0, 0]);
        assert_eq!(DEFAULT_MAX_COLUMN_STATS_BYTES, 256 << 20);
        assert_eq!(DEFAULT_MAX_COLUMN_DICTIONARY_ENTRIES, 10_000);
    }
}
