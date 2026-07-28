//! Typed errors for RSEG v1 encode/decode. Every fallible path in this crate
//! returns one of these instead of panicking; every offset, length, count,
//! and ordinal read from stored bytes is untrusted (docs/segment-format.md).

/// Errors that can occur while writing a segment. Distinct from
/// [`crate::SegmentError`] because writer failures stem from caller input
/// (oversized fields, overflowing deltas), not from untrusted bytes.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum WriteError {
    #[error("duplicate series id in write batch")]
    DuplicateSeriesId,
    #[error("series has more than u16::MAX labels")]
    TooManyLabels,
    #[error("series has more than u32::MAX samples")]
    TooManySamples,
    #[error("label dictionary has more than u32::MAX distinct strings")]
    TooManyDictStrings,
    #[error("more than u32::MAX series in one segment")]
    TooManySeries,
    #[error("timestamp delta overflowed i64 accumulation")]
    TimestampDeltaOverflow,
    #[error("zstd compression failed: {0}")]
    Zstd(String),
    #[error("encoded footer exceeds u32::MAX bytes")]
    FooterTooLarge,
    #[error("internal invariant violated: label ordinal missing from dictionary")]
    DictionaryInvariant,
    #[error("more than u32::MAX distinct label-name schemas in one segment")]
    TooManySchemas,

    // --- RSEG v3 only (ADR-0017, docs/rseg-v3-plan.md section 3.5):
    // HIST_SPANS record encoding. v1/v2 objects never produce these. ---
    #[error("histogram scale {0} is below the -53 custom-boundaries sentinel")]
    HistogramScaleTooSmall(i32),
    #[error(
        "histogram custom_values presence and content must match scale == -53 with strictly ascending boundaries"
    )]
    HistogramCustomValuesMismatch,
    #[error("histogram span length is zero")]
    HistogramSpanLengthZero,
    #[error("histogram bucket count vector length does not match its spans' total length")]
    HistogramBucketCountLenMismatch,
}

/// Errors that can occur while parsing or decoding a segment. All violations
/// are "Corrupted" in the sense of docs/segment-format.md: never a panic,
/// always a typed, recoverable error.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SegmentError {
    #[error("object is smaller than the minimum trailer size: {size} bytes")]
    TooSmall { size: u64 },
    #[error("bad magic bytes")]
    BadMagic,
    #[error("unsupported format version {0}")]
    UnsupportedVersion(u16),
    #[error("unsupported signal {0}")]
    UnsupportedSignal(u8),
    #[error("reserved trailer byte is non-zero: {0}")]
    ReservedNonZero(u8),
    #[error("footer_len is zero or does not fit before the trailer")]
    InvalidFooterLen,
    #[error("footer_crc32c mismatch")]
    FooterCrcMismatch,
    #[error("footer protobuf failed to decode: {0}")]
    FooterDecode(String),
    #[error("duplicate section for known kind {0}")]
    DuplicateSection(u32),
    #[error("missing mandatory section: {0}")]
    MissingSection(&'static str),
    #[error("section range is out of bounds or overflows")]
    SectionOutOfBounds,
    #[error("section uncompressed_len {len} exceeds configured cap {cap}")]
    SectionTooLarge { len: u64, cap: u64 },
    #[error("page uncompressed_len {len} exceeds configured cap {cap}")]
    PageTooLarge { len: u64, cap: u64 },
    #[error("section crc32c mismatch")]
    SectionCrcMismatch,
    #[error("decompressed length {actual} does not match declared {expected}")]
    DecompressedLenMismatch { expected: u64, actual: u64 },
    #[error("decompression failed: {0}")]
    Decompress(String),
    #[error("label dictionary ordinal {0} out of range")]
    BadOrdinal(u64),
    #[error("label dictionary entry is not valid UTF-8")]
    BadUtf8,
    #[error("stored data is truncated")]
    Truncated,
    #[error("stored data has trailing bytes past the declared structure")]
    TrailingBytes,
    #[error("SERIES_TABLE entries are not strictly sorted by series_id")]
    SeriesTableUnsorted,
    #[error("value does not fit the on-disk field width")]
    FieldOverflow,
    #[error("page crc32c mismatch")]
    PageCrcMismatch,
    #[error("invalid page encoding byte {0}")]
    InvalidEncoding(u8),
    #[error("invalid page compression byte {0}")]
    InvalidCompression(u8),
    #[error("invalid section compression tag {0}")]
    InvalidSectionCompression(i32),
    #[error("timestamp delta accumulation overflowed")]
    TimestampOverflow,
    #[error("decoded timestamp {ts} outside series bounds [{min}, {max}]")]
    TimestampOutOfBounds { ts: i64, min: i64, max: i64 },
    #[error("corrupt gorilla bit stream")]
    CorruptGorilla,
    #[error("varint is malformed or too long")]
    BadVarint,
    #[error("label set invariant violated: {0}")]
    Labels(#[from] ravel_types::TypeError),
    #[error("identity mismatch on field {0}")]
    IdentityMismatch(&'static str),

    // --- RSEG v2 only (ADR-0014, docs/segment-format.md "RSEG v2
    // amendment"): SERIES_IDS / SERIES_META decode. v1 objects never
    // produce these. ---
    #[error("SERIES_IDS entries are not strictly ascending by series_id bytes")]
    SeriesIdsUnsorted,
    #[error(
        "series count mismatch: SERIES_IDS={series_ids} SERIES_META={series_meta} footer={footer}"
    )]
    SeriesCountMismatch {
        series_ids: u64,
        series_meta: u64,
        footer: u64,
    },
    #[error("SERIES_META column block does not consume exactly its declared block_len")]
    BadBlockLen,
    #[error("SERIES_META schema_ref {0} out of range")]
    SchemaRefOutOfRange(u64),
    #[error("SERIES_META schema name_count {0} exceeds 65535")]
    SchemaNameCountTooLarge(u64),
    #[error(
        "SERIES_META schema name_ord sequence is not strictly ascending by referenced name bytes"
    )]
    SchemaNamesUnsorted,
    #[error("SERIES_META sample_count is zero")]
    ZeroSampleCount,
    #[error("SERIES_META timestamp bounds arithmetic overflowed i64")]
    TimestampBoundsOverflow,
}
