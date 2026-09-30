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
    /// ADR-1413: a v3 (per-part) column-stats header must name exactly one
    /// part. Any other count means the object was framed for the wrong
    /// version or is corrupt; never decoded as if it covered zero or several
    /// parts.
    #[error("v3 column-stats header part_blake3 must have exactly one entry, got {0}")]
    ColumnStatsV3PartBlake3CountMismatch(usize),
    /// ADR-1413 (amended): the fold degrades a per-part column-stats object
    /// that would exceed the ceiling by dropping its largest dictionaries
    /// first against a running total. This error fires only once no
    /// dictionary is left to drop and the dictionary-free body (fixed fields:
    /// min/max/count/sum, never truncated) is still over the fixed ceiling
    /// (`DEFAULT_MAX_COLUMN_STATS_BYTES`), checked before compression. Never
    /// a silent skip: the caller must fail the whole fold for this part
    /// rather than publish no v3 object for it.
    #[error(
        "column-stats part object body {declared} bytes exceeds the ceiling {ceiling} with no dictionary left to drop"
    )]
    ColumnStatsPartOverBound { declared: u64, ceiling: u64 },
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

    /// The read CPU gate produced no result for this decode (ADR-1702
    /// decision 2): the job panicked, or the runtime dropped it.
    #[error("decode job on the read CPU gate failed: {0}")]
    DecodeJob(ravel_cpu_gate::CpuGateError),
}

/// The highest `SnapshotEntry.level` `part.rs`'s entry validation accepts
/// (0 is an L0 commit, 1 a compaction part).
const MAX_ENTRY_LEVEL: u32 = 1;

impl SnapshotFormatError {
    /// True when the object carries a format version, or an entry level, above
    /// the highest this build reads: a peer on a newer build can read it during
    /// a rolling upgrade, so a query surface answers it as retryable. A version
    /// below the supported minimum is false, like every other variant: it is a
    /// fault in immutable stored bytes (a writer that left proto3's default 0),
    /// and no build reads it.
    ///
    /// `UnsupportedLevel` counts as a newer version because a new entry field
    /// has shipped without a part envelope or header version bump
    /// (docs/catalog-and-mvcc.md, `declared_column_stats`), so a new level
    /// cannot be assumed to bump the part version first. A header whose
    /// version disagrees with an envelope version this build accepts is a
    /// self-inconsistent object, not a newer one.
    ///
    /// Every variant is named, so a new one fails to compile until it is
    /// classified here.
    pub fn is_newer_format_version(&self) -> bool {
        use super::{
            COLUMN_STATS_ACCEPTED_READ_VERSIONS, HEAD_FORMAT_VERSION, POSTINGS_VERSION, VERSION,
        };
        match self {
            SnapshotFormatError::UnsupportedVersion(version) => *version > VERSION,
            SnapshotFormatError::UnsupportedLevel(level) => *level > MAX_ENTRY_LEVEL,
            SnapshotFormatError::UnsupportedHeadVersion(version) => *version > HEAD_FORMAT_VERSION,
            SnapshotFormatError::PostingsUnsupportedVersion(version) => *version > POSTINGS_VERSION,
            SnapshotFormatError::ColumnStatsUnsupportedVersion(version) => {
                COLUMN_STATS_ACCEPTED_READ_VERSIONS
                    .iter()
                    .all(|accepted| version > accepted)
            }

            SnapshotFormatError::TooSmall { .. }
            | SnapshotFormatError::BadMagic
            | SnapshotFormatError::ReservedNonZero
            | SnapshotFormatError::Truncated
            | SnapshotFormatError::TrailingBytes
            | SnapshotFormatError::HeaderCrcMismatch
            | SnapshotFormatError::HeaderDecode(_)
            | SnapshotFormatError::HeaderTooLarge
            | SnapshotFormatError::HeaderVersionMismatch { .. }
            | SnapshotFormatError::BadTenantHashLen(_)
            | SnapshotFormatError::BodyCrcMismatch
            | SnapshotFormatError::Compress(_)
            | SnapshotFormatError::Decompress(_)
            | SnapshotFormatError::DecompressedTooLarge { .. }
            | SnapshotFormatError::DecompressedLenMismatch { .. }
            | SnapshotFormatError::EntryDecode(_)
            | SnapshotFormatError::EntryCountMismatch { .. }
            | SnapshotFormatError::EntriesUnsorted
            | SnapshotFormatError::DuplicateEntry
            | SnapshotFormatError::WatermarkExceeded { .. }
            | SnapshotFormatError::BelowMinHour { .. }
            | SnapshotFormatError::MinHourExceedsWatermark { .. }
            | SnapshotFormatError::BadFieldLen { .. }
            | SnapshotFormatError::HeadDecode(_)
            | SnapshotFormatError::BadHeadTenantHashLen(_)
            | SnapshotFormatError::BadFolderIdLen(_)
            | SnapshotFormatError::HeadNoParts
            | SnapshotFormatError::HeadWatermarkMismatch { .. }
            | SnapshotFormatError::BadPartRefFieldLen { .. }
            | SnapshotFormatError::EmptyPartKey { .. }
            | SnapshotFormatError::PartRefRangeInverted { .. }
            | SnapshotFormatError::PartsNotSortedByMinHour { .. }
            | SnapshotFormatError::PartRangesOverlap { .. }
            | SnapshotFormatError::BadPostingsRefBlake3Len(_)
            | SnapshotFormatError::EmptyPostingsKey
            | SnapshotFormatError::BadPostingsRefPartBlake3Len { .. }
            | SnapshotFormatError::PostingsRefPartCountMismatch { .. }
            | SnapshotFormatError::PostingsRefPartBlake3Mismatch { .. }
            | SnapshotFormatError::PostingsTooSmall { .. }
            | SnapshotFormatError::PostingsHeaderDecode(_)
            | SnapshotFormatError::PostingsHeaderTooLarge
            | SnapshotFormatError::PostingsTooManyNames(_)
            | SnapshotFormatError::PostingsNameNotUtf8
            | SnapshotFormatError::PostingsNamesUnsorted
            | SnapshotFormatError::PostingsDuplicateName
            | SnapshotFormatError::PostingsNameCountMismatch { .. }
            | SnapshotFormatError::PostingsOrdinalOutOfBounds { .. }
            | SnapshotFormatError::PostingsOrdinalsNotStrictlyIncreasing { .. }
            | SnapshotFormatError::PostingsPartBlake3Len { .. }
            | SnapshotFormatError::PostingsPartBindingMismatch
            | SnapshotFormatError::PostingsBadVarint
            | SnapshotFormatError::ColumnStatsTooSmall { .. }
            | SnapshotFormatError::ColumnStatsBadMagic
            | SnapshotFormatError::ColumnStatsReservedNonZero
            | SnapshotFormatError::ColumnStatsTrailingBytes
            | SnapshotFormatError::ColumnStatsHeaderCrcMismatch
            | SnapshotFormatError::ColumnStatsHeaderDecode(_)
            | SnapshotFormatError::ColumnStatsHeaderVersionMismatch { .. }
            | SnapshotFormatError::ColumnStatsBadTenantHashLen(_)
            | SnapshotFormatError::ColumnStatsBodyCrcMismatch
            | SnapshotFormatError::ColumnStatsDecompressedTooLarge { .. }
            | SnapshotFormatError::ColumnStatsDecompressedLenMismatch { .. }
            | SnapshotFormatError::ColumnStatsSegmentDecode(_)
            | SnapshotFormatError::ColumnStatsSegmentCountMismatch { .. }
            | SnapshotFormatError::ColumnStatsSegmentsUnsorted
            | SnapshotFormatError::ColumnStatsDuplicateSegment
            | SnapshotFormatError::ColumnStatsBadFieldLen { .. }
            | SnapshotFormatError::ColumnStatsPartBindingMismatch
            | SnapshotFormatError::ColumnStatsV3PartBlake3CountMismatch(_)
            | SnapshotFormatError::ColumnStatsPartOverBound { .. }
            | SnapshotFormatError::ColumnStatsDuplicateColumnName { .. }
            | SnapshotFormatError::ColumnStatsUnknownDeclaredType { .. }
            | SnapshotFormatError::ColumnStatsValueTypeMismatch { .. }
            | SnapshotFormatError::ColumnStatsDictEntryMissingValue { .. }
            | SnapshotFormatError::ColumnStatsDuplicateDictValue { .. }
            | SnapshotFormatError::ColumnStatsDictCountMismatch { .. }
            | SnapshotFormatError::ColumnStatsDictPresentMismatch { .. }
            | SnapshotFormatError::ColumnStatsUnexpectedMinMax { .. }
            | SnapshotFormatError::ColumnStatsMissingMinMax { .. }
            | SnapshotFormatError::ColumnStatsMinMaxInverted { .. }
            | SnapshotFormatError::ColumnStatsSumOnNonInteger { .. }
            | SnapshotFormatError::ColumnStatsSumMismatch { .. }
            | SnapshotFormatError::ColumnStatsSumWithoutValues { .. }
            | SnapshotFormatError::DecodeJob(_) => false,
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::snapshot_format::{
        COLUMN_STATS_ACCEPTED_READ_VERSIONS, HEAD_FORMAT_VERSION, POSTINGS_VERSION, VERSION,
    };

    /// Each version kind is newer exactly above the highest version this build
    /// reads; one below the floor (a record stamped 0) is not.
    #[test]
    fn only_versions_above_the_supported_maximum_are_newer() {
        let column_stats_max = COLUMN_STATS_ACCEPTED_READ_VERSIONS
            .iter()
            .copied()
            .max()
            .expect("the accepted read set is non-empty");
        let newer = [
            SnapshotFormatError::UnsupportedVersion(VERSION + 1),
            SnapshotFormatError::UnsupportedLevel(MAX_ENTRY_LEVEL + 1),
            SnapshotFormatError::UnsupportedHeadVersion(HEAD_FORMAT_VERSION + 1),
            SnapshotFormatError::PostingsUnsupportedVersion(POSTINGS_VERSION + 1),
            SnapshotFormatError::ColumnStatsUnsupportedVersion(column_stats_max + 1),
        ];
        for err in &newer {
            assert!(err.is_newer_format_version(), "{err:?}");
        }

        let not_newer = [
            SnapshotFormatError::UnsupportedVersion(0),
            SnapshotFormatError::UnsupportedHeadVersion(0),
            SnapshotFormatError::PostingsUnsupportedVersion(0),
            SnapshotFormatError::ColumnStatsUnsupportedVersion(0),
            // A retired whole-object version below the only accepted one.
            SnapshotFormatError::ColumnStatsUnsupportedVersion(column_stats_max - 1),
            SnapshotFormatError::HeaderVersionMismatch {
                header: u32::from(VERSION) + 1,
                envelope: VERSION,
            },
            SnapshotFormatError::BadMagic,
            SnapshotFormatError::DecodeJob(ravel_cpu_gate::CpuGateError::Cancelled),
        ];
        for err in &not_newer {
            assert!(!err.is_newer_format_version(), "{err:?}");
        }
    }
}
