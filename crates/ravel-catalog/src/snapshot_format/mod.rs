//! Codec for the metric index's snapshot part envelope and HEAD record
//! (docs/metric-index-plan.md 3.1, 3.2; ADR-0020). Pure encode/decode: this
//! module performs no store I/O and is not yet wired into `Catalog` (that is
//! phase 2/3's job). Kept in one place so writer and reader can't drift
//! apart, mirroring `ravel-segment`'s `format.rs` convention.

mod error;
mod head;
mod part;

pub use error::SnapshotFormatError;
pub use head::{HEAD_FORMAT_VERSION, decode_head, encode_head};
pub use part::{DecodedPart, decode_part, encode_part};

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

#[cfg(test)]
mod tests {
    use super::*;

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
    }
}
