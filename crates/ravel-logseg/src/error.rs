//! Typed errors for RLOG v1 encode/decode (docs/log-segment-format.md).
//!
//! Every fallible decode path returns one of these instead of panicking.
//! Every offset, length, count, and tag read from stored bytes is untrusted:
//! bounds-checked, overflow-checked, and turned into a typed error, never a
//! panic and never wrong data.

/// Errors from writing or reading an RLOG segment.
///
/// `Corrupted` is the catch-all for every violation of the on-object
/// contract in docs/log-segment-format.md: a bad tag, an out-of-range
/// offset, an overflowing accumulation, a checksum mismatch, or trailing
/// bytes. `LimitExceeded` is a caller/config bound (too many columns, an
/// empty object). `InconsistentStreamAttrs` is writer-side input validation:
/// two records claiming one stream id but disagreeing on the resource+scope
/// bytes behind it. `InvalidSortDescriptor` is writer-side validation of a
/// caller's sort descriptor. `InvalidRowOrder` is a writer-side internal check
/// on its own row order. `Io` wraps compression backend failures.
#[derive(Debug, thiserror::Error)]
pub enum LogSegError {
    #[error("corrupted segment: {0}")]
    Corrupted(String),
    /// The trailer's format version is not the one this build supports
    /// (ADR-0066 decision 2: fail-closed-on-newer, everywhere, typed). This is
    /// distinct from `Corrupted`: the bytes are well-formed, they just carry a
    /// version this reader does not implement. Kept separate so a caller can
    /// tell a genuine corruption from a stray older/newer object. Mirrors
    /// `ravel_segment::SegmentError::UnsupportedVersion`.
    #[error("unsupported format version {0}")]
    UnsupportedVersion(u16),
    #[error("limit exceeded: {0}")]
    LimitExceeded(String),
    /// A read of `[start, end)` from a [`crate::source::SparseObject`] that
    /// holds none, or not all, of it: the range is inside the object but no
    /// placed region covers it. Distinct from `Corrupted` because the object
    /// may be sound; the read asked for bytes its fetch never placed.
    #[error("read of [{start}, {end}) outside the placed regions")]
    Unplaced { start: u64, end: u64 },
    /// Records handed to one writer share a `stream_id` but carry different
    /// `stream_attrs` bytes, so the object has no single truthful STREAM_DIR
    /// blob for that stream. Either a caller bug or a stream-id hash
    /// collision; the writer refuses the whole object instead of silently
    /// picking one blob. This is input validation, not object corruption:
    /// nothing has been decoded yet, so it is never `Corrupted`.
    #[error("inconsistent stream attrs: {0}")]
    InconsistentStreamAttrs(String),
    /// A [`crate::columnar_batch::ColumnarLogBatch`] whose fields contradict
    /// each other or its own `num_rows`, refused by
    /// [`crate::columnar_batch::ColumnarLogBatch::validate`] before the
    /// columnar build path indexes into it. Covers: `ts_ns`, `observed_ts_ns`,
    /// `severity_num`, `flags`, `severity_text`, `body`, `trace_id_validity`,
    /// `span_id_validity`, `stream_refs`, or `residual_attrs` whose length is
    /// not `num_rows`; a packed `trace_id` or `span_id` buffer whose length
    /// does not match its present rows; `stream_ids` and `stream_attrs` of
    /// different lengths; a repeated id within `stream_ids`; a `stream_refs`
    /// value at or past `stream_ids.len()`; a `dyn_columns` entry whose
    /// `validity` does not describe `num_rows` rows, whose `cells` count does
    /// not match `validity`'s present count, or one of whose cells has a type
    /// other than the column's `field_type`; a `dyn_col_dicts` that is
    /// non-empty but not one entry per dyn column; a present dictionary whose
    /// `ids` is not parallel to its column's present cells, holds an id at or
    /// past its own `distinct.len()`, or names a `distinct` entry that differs
    /// from its cell's bytes (for a `Str` cell or a `Bytes` cell; `List` and
    /// `Map` cells are not compared). Not covered, see `validate`: duplicate
    /// `(name, field_type)` columns, more than 4 GiB in one `VarBytes`, and
    /// dictionary contents for `List`/`Map` cells. A caller-side input error,
    /// not a stream id collision and not object corruption.
    #[error("malformed columnar batch: {0}")]
    MalformedColumnarBatch(String),
    /// The sort descriptor handed to the writer cannot be recorded for this
    /// object because its shape would not decode (no key or more than four, an
    /// empty or repeated name, generation 0). Writer-side input validation,
    /// never `Corrupted`.
    #[error("invalid sort descriptor: {0}")]
    InvalidSortDescriptor(String),
    /// The row order the writer computed is not a permutation of its rows: an
    /// index is repeated, out of range, or missing. The writer refuses the
    /// object rather than write one whose rows, footer and counters silently
    /// drop or repeat a record. Nothing was decoded, so it is never `Corrupted`.
    #[error("invalid row order: {0}")]
    InvalidRowOrder(String),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}

/// `ravel-codec`'s moved encoding/bloom/tokenizer code returns its own
/// `CodecError` (it must not depend on `ravel-logseg`, see the crate's
/// `lib.rs`). This conversion is the crate boundary: every existing `?`
/// call site in this crate that decodes a page or a bloom entry keeps
/// propagating the same `Corrupted` message unchanged.
impl From<ravel_codec::error::CodecError> for LogSegError {
    fn from(e: ravel_codec::error::CodecError) -> Self {
        match e {
            ravel_codec::error::CodecError::Corrupted(s) => LogSegError::Corrupted(s),
        }
    }
}
