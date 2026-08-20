//! Constants and small binary-layout facts from docs/segment-format.md.
//! Kept in one place so writer and reader can't drift apart.

/// Trailer magic bytes, last 4 bytes of every RSEG object.
pub const MAGIC: [u8; 4] = *b"RSG1";

/// Retired RSEG v1 trailer version (ADR-0027). The reader rejects it with
/// `UnsupportedVersion`; the number is reserved forever and never reused, so
/// a stray v1 object stays detectably foreign. Kept only to pin the retired
/// value.
#[allow(dead_code)]
pub const VERSION: u16 = 1;

/// Retired RSEG v2 trailer version (retired by ADR-0027). Rejected
/// by the reader; reserved, never reused.
#[allow(dead_code)]
pub const VERSION_V2: u16 = 2;

/// Retired RSEG v3 trailer version (ADR-0017, retired by ADR-0027). Rejected
/// by the reader; reserved, never reused.
#[allow(dead_code)]
pub const VERSION_V3: u16 = 3;

/// Retired RSEG v4 trailer version (ADR-0018, retired by ADR-0027). Rejected
/// by the reader; reserved, never reused. The v4 grammar itself lives on as
/// the below-threshold v5 grammar, so this number is written only
/// transiently by the private v4 encode core before the trailer is rewritten
/// to `VERSION_V5`.
pub const VERSION_V4: u16 = 4;

/// Retired RSEG v5 trailer version (ADR-0026, retired by ADR-0047). Rejected
/// by the reader; reserved, never reused. The v5 grammar itself lives on
/// unchanged as the v6 grammar plus the optional EXEMPLARS section.
#[allow(dead_code)]
pub const VERSION_V5: u16 = 5;

/// RSEG v6 trailer version (docs/segment-format.md, ADR-0047). ADR-0027's
/// single-supported-version rule leaves this the only readable and writable
/// version: the v5 run-major grammar plus two optional sparse-catalog
/// sections, SERIES_IDX (kind 8) and chunked SERIES_META (kind 9, replacing
/// the kind 6 whole-section form when present), plus the optional EXEMPLARS
/// section (kind 10, present only when at least one sample in the object
/// carried an exemplar). The sparse sections are emitted only when the
/// output object carries [`V5_SPARSE_THRESHOLD`] or more series; below that
/// the object omits them and uses the whole SERIES_META. Written by every
/// writer.
pub const VERSION_V6: u16 = 6;

/// The set of RSEG trailer versions this build's reader accepts (ADR-0066
/// decision 1: "N/N-1 window, readers first"). Writers always emit the current
/// version [`VERSION_V6`]; readers accept the current version and, once a
/// version bump lands, the immediately preceding one. The window is at most two
/// versions wide by construction and never accepts anything below the
/// immediately preceding version, so a retired version (RSEG v1-v5) stays
/// rejected.
///
/// This is the single source the reader gate, `audit-versions`, and `migrate`
/// all read, so a future bump edits one constant instead of the sixteen
/// hand-mirrored version sites ADR-0049 measured for the RSEG bump alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SupportedVersions {
    newest: u16,
    oldest: u16,
}

impl SupportedVersions {
    /// A window accepting exactly one version. This is the shape today: only
    /// one RSEG version has existed since ADR-0027 deleted the older readers,
    /// so there is no N-1 to accept and the reader behaves identically to the
    /// old single-version gate.
    pub const fn single(version: u16) -> Self {
        Self {
            newest: version,
            oldest: version,
        }
    }

    /// The N/N-1 window: accept `newest` and the immediately preceding version.
    /// Used at the first format bump that ships a dual reader; no RSEG version
    /// uses it today.
    pub const fn n_and_prev(newest: u16) -> Self {
        // `newest` is always a real format version (>= 1), so the predecessor
        // never underflows.
        Self {
            newest,
            oldest: newest - 1,
        }
    }

    /// The current (newest, always-written) version.
    pub const fn newest(&self) -> u16 {
        self.newest
    }

    /// The oldest accepted version (the window floor).
    pub const fn oldest(&self) -> u16 {
        self.oldest
    }

    /// Whether `version` is inside the accepted window.
    pub const fn contains(&self, version: u16) -> bool {
        version >= self.oldest && version <= self.newest
    }
}

/// RSEG's supported-version window. Today it resolves to the single current
/// version [`VERSION_V6`] (ADR-0027's single-version state persists until the
/// first bump); the machinery carries ADR-0066's two-wide shape ready for it.
pub const SUPPORTED_VERSIONS: SupportedVersions = SupportedVersions::single(VERSION_V6);

/// Series-count threshold at or above which `SegmentWriter::write_v5` emits
/// the sparse SERIES_IDX + chunked SERIES_META sections (ADR-0026 decision
/// point 4). A writer-side constant, not a reader contract: presence is
/// signalled by the sections themselves, so changing this later changes no
/// reader behaviour. 4096 is the conservative power of two inside the
/// measured 500-loses / 10k-wins crossover bracket.
pub const V5_SPARSE_THRESHOLD: u64 = 4096;

/// Stride K for both the sparse-id index and the meta-chunk grouping
/// (ADR-0026 decision point 5): every Kth series id is indexed in SERIES_IDX,
/// and every K series form one SERIES_META chunk. 512 keeps the index under
/// 0.1% of object bytes while keeping the chunk frame count low enough that
/// per-frame zstd stays close to the whole-section baseline.
pub const V5_STRIDE: u32 = 512;

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
    /// Retired with RSEG v1 (ADR-0027): the old row-major catalog. The kind
    /// number is reserved forever and never reused, so a stray v1 object
    /// stays detectably foreign; no v5 object ever emits it. Kept to pin the
    /// retired value.
    #[allow(dead_code)]
    pub const SERIES_TABLE: u32 = 2;
    pub const TS_PAGES: u32 = 3;
    pub const VAL_PAGES: u32 = 4;
    /// The columnar series-id list. Emitted by every v5 object.
    pub const SERIES_IDS: u32 = 5;
    /// The whole-section columnar SERIES_META, emitted by a v5 object below
    /// the sparse threshold (replaced by SERIES_META_CHUNKS above it).
    pub const SERIES_META: u32 = 6;
    /// Histogram-value pages, one per histogram run
    /// (docs/segment-format.md). Present only when the object carries a
    /// histogram-kind series.
    pub const HIST_PAGES: u32 = 7;
    /// The sparse series-id index: every Kth series id plus its SERIES_IDS
    /// byte window (offset/len/crc32c) and the meta-chunk directory (each
    /// chunk's stored range plus crc32c). Present only in a v5 object that
    /// met the sparse-emission threshold (docs/segment-format.md).
    pub const SERIES_IDX: u32 = 8;
    /// The chunked SERIES_META form: a small schema header followed by
    /// per-chunk zstd frames, replacing the kind 6 whole-section SERIES_META
    /// when present (docs/segment-format.md). Present only alongside
    /// SERIES_IDX.
    pub const SERIES_META_CHUNKS: u32 = 9;
    /// Per-sample exemplars (ADR-0047), RSEG v6 only: run-major, sorted by
    /// `(series_index, ts_ns)` so a per-series lookup is a scan that can
    /// stop early on the sort invariant. Present only when at least one
    /// sample in the object carried an exemplar; absent is always legal.
    pub const EXEMPLARS: u32 = 10;
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
    /// TS page GCD stack (ADR-0092 decision 6, issue #312): every timestamp
    /// divided by the page GCD then `ravel_codec::encode_i64`, selected per
    /// page against [`TS_DELTA_VARINT`] and kept only when smaller
    /// (`crate::ts_gcd`, docs/segment-format.md). A second timestamp encoding,
    /// not a replacement.
    pub const TS_GCD_I64: u8 = 2;
    pub const VAL_GORILLA: u8 = 16;
    pub const VAL_RAW_F64: u8 = 17;
    /// ALP value page (ADR-0092 decision 6, issue #312): one decimal exponent,
    /// the fit digits through `ravel_codec::encode_i64`, non-fitting values as
    /// raw-`f64` exceptions (`crate::value_codecs`, docs/segment-format.md).
    pub const VAL_ALP: u8 = 18;
    /// GCD-of-deltas + frame-of-reference value page (ADR-0092 decision 6,
    /// issue #312): one decimal exponent for every value, deltas divided by
    /// their GCD, FOR bit-packed, with a whole-page raw fallback
    /// (`crate::value_codecs`, docs/segment-format.md).
    pub const VAL_GCD_DELTA_FOR: u8 = 19;
    /// RSEG v3 only (ADR-0017); native-histogram record grammar
    /// (docs/segment-format.md "RSEG v3 amendment", HIST_PAGES). Emitted by
    /// `SegmentWriter::write_v3`.
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
    /// a format change (docs/segment-format.md, ADR-0027), never a refactor.
    #[test]
    fn format_constants_are_pinned() {
        assert_eq!(VERSION, 1);
        assert_eq!(VERSION_V2, 2);
        assert_eq!(VERSION_V3, 3);
        assert_eq!(VERSION_V4, 4);
        assert_eq!(VERSION_V5, 5);
        assert_eq!(VERSION_V6, 6);
        assert_eq!(MAGIC, *b"RSG1");
        assert_eq!(section_kind::LABEL_DICT, 1);
        assert_eq!(section_kind::SERIES_TABLE, 2);
        assert_eq!(section_kind::TS_PAGES, 3);
        assert_eq!(section_kind::VAL_PAGES, 4);
        assert_eq!(section_kind::SERIES_IDS, 5);
        assert_eq!(section_kind::SERIES_META, 6);
        assert_eq!(section_kind::HIST_PAGES, 7);
        assert_eq!(section_kind::SERIES_IDX, 8);
        assert_eq!(section_kind::SERIES_META_CHUNKS, 9);
        assert_eq!(section_kind::EXEMPLARS, 10);
        assert_eq!(page_enc::TS_DELTA_VARINT, 1);
        assert_eq!(page_enc::TS_GCD_I64, 2);
        assert_eq!(page_enc::VAL_GORILLA, 16);
        assert_eq!(page_enc::VAL_RAW_F64, 17);
        assert_eq!(page_enc::VAL_ALP, 18);
        assert_eq!(page_enc::VAL_GCD_DELTA_FOR, 19);
        assert_eq!(page_enc::HIST_SPANS, 32);
        assert_eq!(V5_SPARSE_THRESHOLD, 4096);
        assert_eq!(V5_STRIDE, 512);
    }

    /// Today's RSEG window resolves to exactly the single current version, so
    /// the reader's accepted set is byte-for-byte the pre-ADR-0066 behaviour:
    /// v6 accepted, everything else (including v5 and a hypothetical v7)
    /// rejected. Only the window's shape is new machinery.
    #[test]
    fn todays_window_accepts_only_the_current_version() {
        assert_eq!(SUPPORTED_VERSIONS.newest(), VERSION_V6);
        assert_eq!(SUPPORTED_VERSIONS.oldest(), VERSION_V6);
        assert!(SUPPORTED_VERSIONS.contains(VERSION_V6));
        assert!(!SUPPORTED_VERSIONS.contains(VERSION_V5));
        assert!(!SUPPORTED_VERSIONS.contains(VERSION_V6 + 1));
        assert!(!SUPPORTED_VERSIONS.contains(0));
    }

    /// The N/N-1 window shape, proven on a synthetic version number rather than
    /// a real N-1 byte fixture (no RSEG version below v6 has ever existed
    /// post-ADR-0027). The window is exactly two versions wide: it accepts N
    /// and N-1 and has a hard floor at N-1, rejecting N-2 and older. This is
    /// the machinery a real bump will switch [`SUPPORTED_VERSIONS`] to; it must
    /// never silently widen past N-1.
    #[test]
    fn n_and_prev_window_is_exactly_two_wide_with_a_floor() {
        const N: u16 = 100;
        let window = SupportedVersions::n_and_prev(N);
        assert_eq!(window.newest(), N);
        assert_eq!(window.oldest(), N - 1);
        assert!(window.contains(N), "N is accepted");
        assert!(window.contains(N - 1), "N-1 is accepted");
        assert!(!window.contains(N - 2), "N-2 is below the floor, rejected");
        assert!(!window.contains(N + 1), "a newer version is rejected");
    }

    /// A single-version window has no predecessor: it is a one-wide window with
    /// its floor equal to its newest, so N-1 is rejected exactly like N-2.
    #[test]
    fn single_window_has_no_predecessor() {
        const N: u16 = 42;
        let window = SupportedVersions::single(N);
        assert_eq!(window.newest(), N);
        assert_eq!(window.oldest(), N);
        assert!(window.contains(N));
        assert!(!window.contains(N - 1));
        assert!(!window.contains(N + 1));
    }
}
