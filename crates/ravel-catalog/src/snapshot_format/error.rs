//! Typed errors for the snapshot part envelope (docs/metric-index-plan.md
//! 3.1) and HEAD record (3.2) codecs. Decode paths treat every byte as
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
}
