//! Typed errors for the snapshot part envelope and HEAD record codecs. Decode paths treat every byte as
//! untrusted; encode paths defensively validate caller-supplied entries and
//! HEAD fields against the same rules decode enforces, so an object this
//! crate writes can never fail its own decode validation.

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum SnapshotFormatError {
    #[error("part object is smaller than the minimum envelope prefix: {size} bytes")]
    TooSmall { size: usize },
    #[error("bad magic bytes")]
    BadMagic,
    #[error("unsupported part format version {0}")]
    UnsupportedVersion(u8),
    #[error("reserved envelope bytes are non-zero")]
    ReservedNonZero,
    #[error("stored data is truncated")]
    Truncated,
    #[error("stored data has trailing bytes past the declared structure")]
    TrailingBytes,

    #[error("header_crc32c mismatch")]
    HeaderCrcMismatch,
    #[error("part header protobuf failed to decode: {0}")]
    HeaderDecode(String),
    #[error("encoded part header exceeds u32::MAX bytes")]
    HeaderTooLarge,
    #[error("header format_version {header} does not match envelope version {envelope}")]
    HeaderVersionMismatch { header: u32, envelope: u8 },
    #[error("header tenant_hash must be 16 bytes, got {0}")]
    BadTenantHashLen(usize),

    #[error("body_crc32c mismatch")]
    BodyCrcMismatch,
    #[error("zstd compression failed: {0}")]
    Compress(String),
    #[error("zstd decompression failed: {0}")]
    Decompress(String),
    #[error("declared decompressed body length {declared} exceeds configured cap {cap}")]
    DecompressedTooLarge { declared: u64, cap: u64 },
    #[error("decompressed body length {actual} does not match header's declared {expected}")]
    DecompressedLenMismatch { expected: u64, actual: u64 },

    #[error("entry protobuf failed to decode: {0}")]
    EntryDecode(String),
    #[error("entry count {actual} does not match header's declared {expected}")]
    EntryCountMismatch { expected: u64, actual: u64 },
    #[error(
        "entries are not strictly sorted by (ingest_hour_bucket, shard, writer_id, writer_epoch, writer_seq)"
    )]
    EntriesUnsorted,
    #[error("duplicate entry identity in entry set")]
    DuplicateEntry,
    #[error("entry ingest_hour_bucket {hour} exceeds header watermark_hour {watermark}")]
    WatermarkExceeded { hour: u32, watermark: u32 },
    #[error("entry ingest_hour_bucket {hour} is below header min_hour {min_hour}")]
    BelowMinHour { hour: u32, min_hour: u32 },
    #[error("header min_hour {min_hour} exceeds watermark_hour {watermark}")]
    MinHourExceedsWatermark { min_hour: u32, watermark: u32 },
    #[error("unsupported entry level {0}")]
    UnsupportedLevel(u32),
    #[error("entry {field} must be {expected} bytes, got {actual}")]
    BadFieldLen {
        field: &'static str,
        expected: usize,
        actual: usize,
    },

    #[error("head protobuf failed to decode: {0}")]
    HeadDecode(String),
    #[error("unsupported head format_version {0}")]
    UnsupportedHeadVersion(u32),
    #[error("head tenant_hash must be 16 bytes, got {0}")]
    BadHeadTenantHashLen(usize),
    #[error("head folder_id must be 16 bytes, got {0}")]
    BadFolderIdLen(usize),
    #[error("head has no parts")]
    HeadNoParts,
    #[error("head watermark_hour {head} does not equal the max part watermark_hour {max_part}")]
    HeadWatermarkMismatch { head: u32, max_part: u32 },
    #[error("head part[{index}] {field} must be {expected} bytes, got {actual}")]
    BadPartRefFieldLen {
        index: usize,
        field: &'static str,
        expected: usize,
        actual: usize,
    },
    #[error("head part[{index}] has an empty key")]
    EmptyPartKey { index: usize },
    #[error("head part[{index}] min_hour {min_hour} exceeds its watermark_hour {watermark}")]
    PartRefRangeInverted {
        index: usize,
        min_hour: u32,
        watermark: u32,
    },
    #[error("head parts are not sorted by min_hour ascending at part[{index}]")]
    PartsNotSortedByMinHour { index: usize },
    #[error(
        "head part[{index}] min_hour {next_min_hour} overlaps the previous part's watermark_hour {prev_watermark}"
    )]
    PartRangesOverlap {
        index: usize,
        prev_watermark: u32,
        next_min_hour: u32,
    },
    #[error("head postings ref blake3 must be 32 bytes, got {0}")]
    BadPostingsRefBlake3Len(usize),
    #[error("head postings ref has an empty key")]
    EmptyPostingsKey,
    #[error("head postings ref part_blake3[{index}] must be 32 bytes, got {actual}")]
    BadPostingsRefPartBlake3Len { index: usize, actual: usize },
    #[error("head postings ref names {postings_parts} parts but head has {head_parts} parts")]
    PostingsRefPartCountMismatch {
        postings_parts: usize,
        head_parts: usize,
    },
    #[error("head postings ref part_blake3[{index}] does not match parts[{index}].blake3")]
    PostingsRefPartBlake3Mismatch { index: usize },

    #[error("postings object is smaller than the minimum envelope prefix: {size} bytes")]
    PostingsTooSmall { size: usize },
    #[error("unsupported postings format version {0}")]
    PostingsUnsupportedVersion(u8),
    #[error("postings header protobuf failed to decode: {0}")]
    PostingsHeaderDecode(String),
    #[error("encoded postings header exceeds u32::MAX bytes")]
    PostingsHeaderTooLarge,
    #[error("postings has too many names to encode ({0})")]
    PostingsTooManyNames(usize),
    #[error("postings name bytes are not valid utf-8")]
    PostingsNameNotUtf8,
    #[error("postings names are not strictly sorted ascending")]
    PostingsNamesUnsorted,
    #[error("duplicate name in postings dictionary")]
    PostingsDuplicateName,
    #[error("postings name_count {actual} does not match header's declared {expected}")]
    PostingsNameCountMismatch { expected: u32, actual: usize },
    #[error("postings entry ordinal {ordinal} for name {name:?} exceeds entry_count {entry_count}")]
    PostingsOrdinalOutOfBounds {
        name: String,
        ordinal: u64,
        entry_count: u64,
    },
    #[error("postings entry ordinals for name {name:?} are not strictly increasing")]
    PostingsOrdinalsNotStrictlyIncreasing { name: String },
    #[error("postings header part_blake3[{index}] must be 32 bytes, got {actual}")]
    PostingsPartBlake3Len { index: usize, actual: usize },
    #[error("postings header part_blake3 does not match the expected covered parts")]
    PostingsPartBindingMismatch,
    #[error("malformed varint in postings body")]
    PostingsBadVarint,

    #[error("head column-stats ref blake3 must be 32 bytes, got {0}")]
    BadColumnStatsRefBlake3Len(usize),
    #[error("head column-stats ref has an empty key")]
    EmptyColumnStatsKey,
    #[error("head column-stats ref part_blake3[{index}] must be 32 bytes, got {actual}")]
    BadColumnStatsRefPartBlake3Len { index: usize, actual: usize },
    #[error("head column-stats ref names {stats_parts} parts but head has {head_parts} parts")]
    ColumnStatsRefPartCountMismatch {
        stats_parts: usize,
        head_parts: usize,
    },
    #[error("head column-stats ref part_blake3[{index}] does not match parts[{index}].blake3")]
    ColumnStatsRefPartBlake3Mismatch { index: usize },

    #[error("column-stats object is smaller than the minimum envelope prefix: {size} bytes")]
    ColumnStatsTooSmall { size: usize },
    #[error("bad column-stats magic bytes")]
    ColumnStatsBadMagic,
    #[error("unsupported column-stats format version {0}")]
    ColumnStatsUnsupportedVersion(u8),
    #[error("column-stats reserved envelope bytes are non-zero")]
    ColumnStatsReservedNonZero,
    #[error("column-stats data has trailing bytes past the declared structure")]
    ColumnStatsTrailingBytes,
    #[error("column-stats header_crc32c mismatch")]
    ColumnStatsHeaderCrcMismatch,
    #[error("column-stats header protobuf failed to decode: {0}")]
    ColumnStatsHeaderDecode(String),
    #[error(
        "column-stats header format_version {header} does not match envelope version {envelope}"
    )]
    ColumnStatsHeaderVersionMismatch { header: u32, envelope: u8 },
    #[error("column-stats header tenant_hash must be 16 bytes, got {0}")]
    ColumnStatsBadTenantHashLen(usize),
    #[error("column-stats body_crc32c mismatch")]
    ColumnStatsBodyCrcMismatch,
    #[error(
        "declared decompressed column-stats body length {declared} exceeds configured cap {cap}"
    )]
    ColumnStatsDecompressedTooLarge { declared: u64, cap: u64 },
    #[error(
        "decompressed column-stats body length {actual} does not match header's declared {expected}"
    )]
    ColumnStatsDecompressedLenMismatch { expected: u64, actual: u64 },
    #[error("column-stats segment protobuf failed to decode: {0}")]
    ColumnStatsSegmentDecode(String),
    #[error("column-stats segment_count {actual} does not match header's declared {expected}")]
    ColumnStatsSegmentCountMismatch { expected: u64, actual: u64 },
    #[error(
        "column-stats segments are not strictly sorted by (ingest_hour_bucket, shard, writer_id, writer_epoch, writer_seq)"
    )]
    ColumnStatsSegmentsUnsorted,
    #[error("duplicate segment identity in column-stats segment set")]
    ColumnStatsDuplicateSegment,
    #[error("column-stats segment {field} must be {expected} bytes, got {actual}")]
    ColumnStatsBadFieldLen {
        field: &'static str,
        expected: usize,
        actual: usize,
    },
    #[error("column-stats header part_blake3 does not match the expected covered parts")]
    ColumnStatsPartBindingMismatch,
    #[error("column-stats segment carries duplicate column name {name:?}")]
    ColumnStatsDuplicateColumnName { name: String },
    #[error("column-stats column {name:?} has an unknown declared_type {declared_type}")]
    ColumnStatsUnknownDeclaredType { name: String, declared_type: u32 },
    #[error(
        "column-stats column {name:?} carries a {field} value whose kind does not match its declared_type {declared_type}"
    )]
    ColumnStatsValueTypeMismatch {
        name: String,
        field: &'static str,
        declared_type: u32,
    },
    #[error("column-stats column {name:?} carries a dictionary entry with no value")]
    ColumnStatsDictEntryMissingValue { name: String },
    #[error("column-stats column {name:?} carries duplicate dictionary value")]
    ColumnStatsDuplicateDictValue { name: String },
    #[error(
        "column-stats column {name:?} dictionary counts total {dict_total} but non_null_count is {non_null_count}"
    )]
    ColumnStatsDictCountMismatch {
        name: String,
        dict_total: u64,
        non_null_count: u64,
    },
    #[error(
        "column-stats column {name:?} has dictionary_present=false but carries {entries} dictionary entries"
    )]
    ColumnStatsDictPresentMismatch { name: String, entries: usize },
    #[error(
        "column-stats column {name:?} carries min/max but non_null_count is zero (must be absent)"
    )]
    ColumnStatsUnexpectedMinMax { name: String },
    /// A column reports non-null rows but omits `min` or `max`, so it cannot
    /// support a MIN/MAX answer.
    #[error(
        "column-stats column {name:?} has non_null_count > 0 but omits min or max (both required)"
    )]
    ColumnStatsMissingMinMax { name: String },
    #[error("column-stats column {name:?} has min greater than max")]
    ColumnStatsMinMaxInverted { name: String },
    /// A non-integer column carries a `sum`. Sums are stored for I64 columns
    /// only (#861); any other declared type carrying one is internally
    /// inconsistent and the metadata-only SUM/AVG path must never read it.
    #[error(
        "column-stats column {name:?} declared_type {declared_type} carries a sum \
         (sums are I64-only)"
    )]
    ColumnStatsSumOnNonInteger { name: String, declared_type: u32 },
    /// A column's stored `sum` disagrees with the sum its own exact dictionary
    /// implies. A reader deriving SUM/AVG from this record would return a wrong
    /// total, so it is rejected at encode/decode rather than trusted.
    #[error(
        "column-stats column {name:?} sum {sum} disagrees with its dictionary total {dict_sum}"
    )]
    ColumnStatsSumMismatch {
        name: String,
        /// The column's stored `sum` (proto `i64`).
        sum: i64,
        /// The sum its dictionary implies, clamped into `i64` for the message
        /// only (an `i128` field would 16-byte-align this whole error enum and
        /// grow every error that embeds it): a real mismatch is what matters,
        /// not the exact overflowed magnitude.
        dict_sum: i64,
    },
    /// A column with no non-null values carries a non-zero `sum`. An all-null
    /// column has nothing to sum, so zero is the only exact answer. Rejected
    /// because the metadata-only path folds `sum` and `non_null_count` from
    /// each segment independently: a record like this adds to the total while
    /// adding nothing to the count, making a multi-segment SUM wrong by that
    /// amount and an AVG wrong in both terms.
    #[error("column-stats column {name:?} has no non-null values but carries sum {sum}")]
    ColumnStatsSumWithoutValues { name: String, sum: i64 },
}
