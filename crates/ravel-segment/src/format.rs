//! Constants and small binary-layout facts from docs/segment-format.md.
//! Kept in one place so writer and reader can't drift apart.

/// Trailer magic bytes, last 4 bytes of every RSEG v1 object.
pub const MAGIC: [u8; 4] = *b"RSG1";

/// Format version recorded in the trailer.
pub const VERSION: u16 = 1;

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
