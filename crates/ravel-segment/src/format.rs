//! Constants and small binary-layout facts from docs/segment-format.md.
//! Kept in one place so writer and reader can't drift apart.

/// Trailer magic bytes, last 4 bytes of every RSEG v1 object.
pub const MAGIC: [u8; 4] = *b"RSG1";

/// Format version recorded in the trailer. This is the v1 layout; the
/// writer emits this value until the v2 writer path lands
/// (docs/rseg-v2-plan.md phase 2).
pub const VERSION: u16 = 1;

/// RSEG v2 trailer version (ADR-0014, docs/rseg-v2-plan.md). Same magic and
/// trailer layout as v1; only the catalog section set and LABEL_DICT
/// ordering rule change. Written by `SegmentWriter::write_v2` (issue #30);
/// not yet read by this crate's reader (phase 3, issue #31).
pub const VERSION_V2: u16 = 2;

/// RSEG v3 trailer version (ADR-0017, docs/rseg-v3-plan.md). Strict
/// superset of v2: adds the HIST_PAGES section, a HIST_SPANS page
/// encoding, and three SERIES_META column blocks for native-histogram
/// values. Same magic and trailer layout as v1/v2. Written by
/// `SegmentWriter::write_v3` (docs/rseg-v3-plan.md C3); not yet read by
/// this crate's reader (C4).
pub const VERSION_V3: u16 = 3;

/// Signal byte for metric segments.
pub const SIGNAL_METRICS: u8 = 1;

/// Reserved trailer byte; must always be zero in v1.
pub const RESERVED: u8 = 0;

/// Trailer size in bytes: footer_len(4) + footer_crc32c(4) + version(2) +
/// signal(1) + reserved(1) + magic(4).
pub const TRAILER_LEN: u64 = 16;

/// Known section kinds (docs/segment-format.md). Values are part of the
/// persistent format; unknown kinds (including any value not listed here)
/// MUST be skipped by readers.
pub mod section_kind {
    pub const LABEL_DICT: u32 = 1;
    pub const SERIES_TABLE: u32 = 2;
    pub const TS_PAGES: u32 = 3;
    pub const VAL_PAGES: u32 = 4;
    /// RSEG v2 only (ADR-0014); v1 objects never emit this kind. Emitted by
    /// `SegmentWriter::write_v2` (issue #30).
    pub const SERIES_IDS: u32 = 5;
    /// RSEG v2 only (ADR-0014); v1 objects never emit this kind. Emitted by
    /// `SegmentWriter::write_v2` (issue #30).
    pub const SERIES_META: u32 = 6;
    /// RSEG v3 only (ADR-0017); v1/v2 objects never emit this kind.
    /// Histogram-value pages, one per histogram series
    /// (docs/segment-format.md "RSEG v3 amendment"). Emitted by
    /// `SegmentWriter::write_v3` (docs/rseg-v3-plan.md C3).
    pub const HIST_PAGES: u32 = 7;
}

/// Section-level compression tags, matching `ravel.segment.v1.Compression`.
pub mod compression {
    pub const NONE: i32 = 0;
    pub const LZ4: i32 = 1;
    pub const ZSTD: i32 = 2;
}

/// Page encodings (docs/segment-format.md).
pub mod page_enc {
    pub const TS_DELTA_VARINT: u8 = 1;
    pub const VAL_GORILLA: u8 = 16;
    pub const VAL_RAW_F64: u8 = 17;
    /// RSEG v3 only (ADR-0017); native-histogram record grammar
    /// (docs/segment-format.md "RSEG v3 amendment", HIST_PAGES). Emitted by
    /// `SegmentWriter::write_v3` (docs/rseg-v3-plan.md C3).
    pub const HIST_SPANS: u8 = 32;
}

/// Page-level compression byte (independent numbering from the section
/// `Compression` enum, per the page header grammar: `0=none, 1=lz4`).
pub mod page_comp {
    pub const NONE: u8 = 0;
    pub const LZ4: u8 = 1;
}

/// zstd compression level for whole-section compression (LABEL_DICT,
/// SERIES_TABLE).
pub const ZSTD_LEVEL: i32 = 3;

/// Default resource caps for untrusted section/page uncompressed sizes
/// (docs/segment-format.md: "default 1 GiB per section, 64 MiB per page").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReaderLimits {
    pub max_section_uncompressed_bytes: u64,
    pub max_page_uncompressed_bytes: u64,
}

impl Default for ReaderLimits {
    fn default() -> Self {
        ReaderLimits {
            max_section_uncompressed_bytes: 1 << 30,
            max_page_uncompressed_bytes: 64 << 20,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Pins every persistent-format constant's wire value. A change here is
    /// a format change (docs/segment-format.md, ADR-0014), never a refactor.
    #[test]
    fn format_constants_are_pinned() {
        assert_eq!(VERSION, 1);
        assert_eq!(VERSION_V2, 2);
        assert_eq!(VERSION_V3, 3);
        assert_eq!(MAGIC, *b"RSG1");
        assert_eq!(section_kind::LABEL_DICT, 1);
        assert_eq!(section_kind::SERIES_TABLE, 2);
        assert_eq!(section_kind::TS_PAGES, 3);
        assert_eq!(section_kind::VAL_PAGES, 4);
        assert_eq!(section_kind::SERIES_IDS, 5);
        assert_eq!(section_kind::SERIES_META, 6);
        assert_eq!(section_kind::HIST_PAGES, 7);
        assert_eq!(page_enc::HIST_SPANS, 32);
    }
}
