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

/// Retired RSEG v6 trailer version (ADR-0047, retired by ADR-0092). Rejected
/// by the reader; reserved, never reused. The v6 grammar itself lives on
/// unchanged as the v7 grammar: v7 is v6 plus the optional per-sample dedup
/// provenance extension in SERIES_META and three additional per-page value/
/// timestamp encodings, none of which change how a v6-shaped object is laid
/// out. Kept only to pin the retired value and to reject stray v6 objects.
#[allow(dead_code)]
pub const VERSION_V6: u16 = 6;

/// RSEG v7 trailer version (docs/segment-format.md, ADR-0092). ADR-0027's
/// single-supported-version rule leaves this the only readable and writable
/// version: the v6 run-major grammar (the v5 grammar plus two optional
/// sparse-catalog sections, SERIES_IDX (kind 8) and chunked SERIES_META
/// (kind 9, replacing the kind 6 whole-section form when present), plus the
/// optional EXEMPLARS section (kind 10)) with three additions from ADR-0092:
///
/// - an optional per-sample dedup provenance extension appended to the
///   whole-section SERIES_META (kind 6), present only when at least one run in
///   the object merged several writes' samples;
/// - two new value page encodings, `VAL_ALP` (18) and `VAL_GCD_DELTA_FOR`
///   (19), and one new timestamp page encoding, `TS_GCD_I64` (2), each
///   selected per page against the prior encoding and kept only when smaller;
/// - a run's first timestamp encoded as a delta from the run minimum, and
///   single-sample raw-`f64` value pages that drop the 8-byte alignment pad.
///
/// The sparse sections are emitted only when the output object carries
/// [`V5_SPARSE_THRESHOLD`] or more series; below that the object omits them and
/// uses the whole SERIES_META. Written by every writer.
pub const VERSION_V7: u16 = 7;

/// One RSEG trailer version this build admits: a token that exists only for a
/// version inside the reader window ([`SegmentVersion::WINDOW`]), and the type
/// the structural validator dispatches on.
///
/// This is what makes the trailer gate and the structural validator unable to
/// disagree. A raw `u16` read out of a trailer becomes a `SegmentVersion` at
/// exactly one place ([`SegmentVersion::from_number`]); a number outside the
/// window has no token at all, so it cannot reach a rule set, and a token can
/// only have come from the window. The per-version rule set is then selected by
/// an exhaustive `match` over this enum with no wildcard arm, so adding a
/// variant without giving it a validator is a compile error rather than a
/// silent acceptance of a version nothing knows how to check.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum SegmentVersion {
    /// RSEG v7 ([`VERSION_V7`]), the current version and the only one any
    /// writer emits.
    V7,
}

impl SegmentVersion {
    /// The reader window (ADR-0066 decision 1: "N/N-1 window, readers first"),
    /// newest first. THIS IS THE ONE PLACE the set of admitted versions is
    /// written down: the trailer gate, the structural validator's dispatch,
    /// [`SUPPORTED_VERSIONS`] (and through it `audit-versions` and `migrate`)
    /// are all functions of this slice, none of them carrying a version literal
    /// of its own.
    ///
    /// Adding N-1 at a bump is one line here. The const assertions below hold
    /// the window to ADR-0066's shape: non-empty, at most two wide, and
    /// contiguous, so it can never reach past N-1 or skip a version.
    pub const WINDOW: &'static [SegmentVersion] = &[SegmentVersion::V7];

    /// This version's trailer number. An exhaustive match, so a new variant
    /// must state its number here before anything else compiles.
    pub const fn number(self) -> u16 {
        match self {
            SegmentVersion::V7 => VERSION_V7,
        }
    }

    /// Resolve a raw trailer version into a token, or `None` when it is outside
    /// this build's window. The single resolution point: both the reader's
    /// trailer gate and [`crate::validate_sections`] go through it, so neither
    /// can admit a version the other rejects.
    pub const fn from_number(version: u16) -> Option<SegmentVersion> {
        let window = SegmentVersion::WINDOW;
        let mut i = 0;
        while i < window.len() {
            let candidate = window[i];
            if candidate.number() == version {
                return Some(candidate);
            }
            i += 1;
        }
        None
    }
}

/// Shape guards on the window itself (ADR-0066 decision 1). A window that is
/// empty, wider than N/N-1, or non-contiguous fails the build rather than
/// quietly widening what the reader admits. Non-emptiness is also what lets
/// [`SupportedVersions::newest`] and [`SupportedVersions::oldest`] index
/// element 0 without a fallback.
const _: () = {
    assert!(
        !SegmentVersion::WINDOW.is_empty(),
        "the RSEG reader window must admit at least the current version"
    );
    assert!(
        SegmentVersion::WINDOW.len() <= 2,
        "ADR-0066 decision 1 caps the reader window at N and N-1"
    );
    let mut i = 1;
    while i < SegmentVersion::WINDOW.len() {
        assert!(
            SegmentVersion::WINDOW[i - 1].number() == SegmentVersion::WINDOW[i].number() + 1,
            "the RSEG reader window must be contiguous and newest-first"
        );
        i += 1;
    }
};

/// The set of RSEG trailer versions this build's reader accepts (ADR-0066
/// decision 1: "N/N-1 window, readers first"). Writers always emit the current
/// version [`VERSION_V7`]; readers accept the current version and, once a
/// version bump lands, the immediately preceding one. The window is at most two
/// versions wide by construction and never accepts anything below the
/// immediately preceding version, so a retired version (RSEG v1-v6) stays
/// rejected.
///
/// This is a projection of [`SegmentVersion::WINDOW`], not a second declaration
/// of it: membership is membership in that slice, so this and the structural
/// validator cannot drift apart. It is the single source the reader gate,
/// `audit-versions`, and `migrate` all read, so a future bump edits one slice
/// instead of the sixteen hand-mirrored version sites ADR-0049 measured for the
/// RSEG bump alone.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SupportedVersions {
    window: &'static [SegmentVersion],
}

impl SupportedVersions {
    /// The window over a version slice. Private: the only window that exists is
    /// [`SUPPORTED_VERSIONS`], over [`SegmentVersion::WINDOW`], because a second
    /// window built somewhere else is exactly the drift this type removes.
    const fn over(window: &'static [SegmentVersion]) -> Self {
        Self { window }
    }

    /// The current (newest, always-written) version.
    pub const fn newest(&self) -> u16 {
        let mut newest = self.window[0].number();
        let mut i = 1;
        while i < self.window.len() {
            let v = self.window[i].number();
            if v > newest {
                newest = v;
            }
            i += 1;
        }
        newest
    }

    /// The oldest accepted version (the window floor).
    pub const fn oldest(&self) -> u16 {
        let mut oldest = self.window[0].number();
        let mut i = 1;
        while i < self.window.len() {
            let v = self.window[i].number();
            if v < oldest {
                oldest = v;
            }
            i += 1;
        }
        oldest
    }

    /// Whether `version` is inside the accepted window. Membership in the
    /// version slice, not `oldest..=newest`: a range would admit a number with
    /// no [`SegmentVersion`] token if the window were ever non-contiguous, and
    /// that number would then pass the trailer gate and be rejected by the
    /// validator.
    pub const fn contains(&self, version: u16) -> bool {
        let mut i = 0;
        while i < self.window.len() {
            if self.window[i].number() == version {
                return true;
            }
            i += 1;
        }
        false
    }

    /// How many versions the window admits (1 today, 2 across a bump).
    pub const fn len(&self) -> usize {
        self.window.len()
    }

    /// Always false: the window is asserted non-empty at compile time. Present
    /// because clippy requires it alongside [`Self::len`].
    pub const fn is_empty(&self) -> bool {
        self.window.is_empty()
    }

    /// The admitted versions, newest first.
    pub fn versions(&self) -> impl Iterator<Item = SegmentVersion> + '_ {
        self.window.iter().copied()
    }
}

/// RSEG's supported-version window. Today it resolves to the single current
/// version [`VERSION_V7`] (ADR-0027's single-version state persists after the
/// v7 bump, which deleted the v6 read and write paths in the same change,
/// ADR-0092 decision 7); the machinery carries ADR-0066's two-wide shape ready
/// for the first post-release bump.
pub const SUPPORTED_VERSIONS: SupportedVersions = SupportedVersions::over(SegmentVersion::WINDOW);

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
#[allow(clippy::expect_used)]
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
        assert_eq!(VERSION_V7, 7);
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
    /// v7 accepted, everything else (including the now-retired v6 and a
    /// hypothetical v8) rejected. Only the window's shape is new machinery.
    #[test]
    fn todays_window_accepts_only_the_current_version() {
        assert_eq!(SUPPORTED_VERSIONS.newest(), VERSION_V7);
        assert_eq!(SUPPORTED_VERSIONS.oldest(), VERSION_V7);
        assert!(SUPPORTED_VERSIONS.contains(VERSION_V7));
        assert!(!SUPPORTED_VERSIONS.contains(VERSION_V6));
        assert!(!SUPPORTED_VERSIONS.contains(VERSION_V7 + 1));
        assert!(!SUPPORTED_VERSIONS.contains(0));
    }

    /// The window keeps ADR-0066 decision 1's shape: non-empty, at most two
    /// versions wide, contiguous, newest first, and newest equal to the version
    /// every writer emits. The const assertions beside [`SegmentVersion::WINDOW`]
    /// already fail the build on the first three; this states them where a
    /// reader of the test list can see the policy, and adds the two the const
    /// block cannot check (newest-first ordering is checkable there, the tie to
    /// [`VERSION_V7`] is the one that changes at a bump).
    #[test]
    fn the_window_keeps_the_n_and_prev_shape() {
        let window = SUPPORTED_VERSIONS;
        assert!(!window.is_empty() && window.len() <= 2, "N/N-1 at most");
        assert!(!window.is_empty());
        assert_eq!(window.newest(), VERSION_V7, "writers emit the newest");

        let versions: Vec<u16> = window.versions().map(SegmentVersion::number).collect();
        assert_eq!(versions[0], window.newest(), "newest first");
        assert_eq!(
            *versions.last().expect("non-empty window"),
            window.oldest(),
            "oldest last"
        );
        for pair in versions.windows(2) {
            assert_eq!(pair[0], pair[1] + 1, "contiguous, descending");
        }
    }

    /// Every version the window admits has a token, and every version it does
    /// not admit has none. This is the property the structural validator relies
    /// on to be unable to disagree with the trailer gate: the two ask the same
    /// question through [`SegmentVersion::from_number`], and a number with no
    /// token cannot reach any rule set. Swept over the whole `u16` domain, so a
    /// window that ever admits a number without a variant fails here.
    #[test]
    fn token_resolution_agrees_with_the_window_over_every_u16() {
        for version in 0..=u16::MAX {
            let token = SegmentVersion::from_number(version);
            assert_eq!(
                token.is_some(),
                SUPPORTED_VERSIONS.contains(version),
                "version {version}: token presence must match window membership"
            );
            if let Some(token) = token {
                assert_eq!(token.number(), version, "a token round-trips its number");
            }
        }
    }
}
