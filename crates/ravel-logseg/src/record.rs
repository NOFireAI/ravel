//! Log record model, fixed column ids, and the resolved-row form the object
//! writer feeds to the block encoder (docs/log-segment-format.md).
//!
//! [`LogRecord`] is the caller-facing record. [`ResolvedRow`] is its
//! storage-facing shape: the stream id replaced by its dense `stream_ref`, and
//! every dynamic attribute already mapped to a `(column_id, value)` typed
//! slot, with overflow attributes canonicalized into `attrs_raw`. The writer
//! (task 12) produces [`ResolvedRow`]s; [`crate::block`] encodes them.

use ravel_types::logstream::{AttrValue, LogStreamId, canonical_attr_bytes};

use crate::varint::put_uvarint;

// Fixed column ids (docs/log-segment-format.md FIELD_DIR). These occupy the
// reserved ids 0..=9 and never appear in FIELD_DIR; dynamic attribute columns
// start at [`FIRST_DYNAMIC_COL`].
pub const COL_TS: u32 = 0;
pub const COL_OBSERVED_TS: u32 = 1;
pub const COL_STREAM_REF: u32 = 2;
pub const COL_SEVERITY_NUM: u32 = 3;
pub const COL_SEVERITY_TEXT: u32 = 4;
pub const COL_BODY: u32 = 5;
pub const COL_TRACE_ID: u32 = 6;
pub const COL_SPAN_ID: u32 = 7;
pub const COL_FLAGS: u32 = 8;
pub const COL_ATTRS_RAW: u32 = 9;
/// First column id available to dynamic attribute columns.
pub const FIRST_DYNAMIC_COL: u32 = 10;

/// Fixed byte width of a trace id.
pub const TRACE_ID_WIDTH: usize = 16;
/// Fixed byte width of a span id.
pub const SPAN_ID_WIDTH: usize = 8;

/// Builds the [`LogRecord::stream_attrs`] blob for a resource+scope: exactly
/// the bytes `log_stream_id` hashes after its domain string
/// (docs/log-segment-format.md "STREAM_DIR"), in this order:
///
/// ```text
/// canonical_attr_bytes(resource_attrs)
/// varint(len(scope_name))    scope_name    (UTF-8, no terminator)
/// varint(len(scope_version)) scope_version (UTF-8, no terminator)
/// canonical_attr_bytes(scope_attrs)
/// ```
///
/// So for any resource+scope,
/// `blake3("ravel-logstream-v1" || stream_attrs_bytes(..))` truncated to 16
/// bytes is the [`LogStreamId`] that
/// [`ravel_types::logstream::log_stream_id`] returns for the same input: the
/// blob stored in STREAM_DIR is the hash preimage, so stream identity is
/// verifiable from the object alone.
///
/// Both attribute sets are self-delimiting (each carries a leading entry
/// count) and both scope strings are length-prefixed, so the concatenation is
/// injective: no two distinct resource+scope inputs produce the same blob.
pub fn stream_attrs_bytes(
    resource_attrs: &[(String, AttrValue)],
    scope_name: &str,
    scope_version: &str,
    scope_attrs: &[(String, AttrValue)],
) -> Vec<u8> {
    let mut out = canonical_attr_bytes(resource_attrs);
    put_uvarint(&mut out, scope_name.len() as u64);
    out.extend_from_slice(scope_name.as_bytes());
    put_uvarint(&mut out, scope_version.len() as u64);
    out.extend_from_slice(scope_version.as_bytes());
    out.extend_from_slice(&canonical_attr_bytes(scope_attrs));
    out
}

/// A single log record as handed to the writer.
#[derive(Clone, Debug, PartialEq)]
pub struct LogRecord {
    pub stream_id: LogStreamId,
    /// The canonical resource+scope bytes `stream_id` was derived from, as
    /// built by [`stream_attrs_bytes`]. The writer stores these verbatim as the
    /// STREAM_DIR blob for this stream, which is what makes stream identity
    /// recoverable from the object; every record sharing a `stream_id` must
    /// carry identical bytes here or
    /// [`crate::error::LogSegError::InconsistentStreamAttrs`] rejects the
    /// whole object.
    pub stream_attrs: Vec<u8>,
    pub ts_ns: i64,
    pub observed_ts_ns: i64,
    pub severity_num: u8,
    pub severity_text: String,
    pub body: String,
    pub trace_id: Option<[u8; 16]>,
    pub span_id: Option<[u8; 8]>,
    pub flags: u32,
    pub attrs: Vec<(String, AttrValue)>,
}

/// The type a dynamic attribute column carries (docs/log-segment-format.md
/// FIELD_DIR). A key observed with two value types yields two columns
/// (per-type splitting).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
#[repr(u8)]
pub enum FieldType {
    Str = 1,
    I64 = 2,
    F64 = 3,
    Bool = 4,
    Bytes = 5,
}

impl FieldType {
    /// Maps a stored type byte to a [`FieldType`]; an unknown byte is the
    /// caller's to reject as `Corrupted`.
    pub fn from_u8(v: u8) -> Option<FieldType> {
        Some(match v {
            1 => FieldType::Str,
            2 => FieldType::I64,
            3 => FieldType::F64,
            4 => FieldType::Bool,
            5 => FieldType::Bytes,
            _ => return None,
        })
    }

    /// The stored type byte.
    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

/// One dynamic attribute value, already resolved to its storage type. `Str`
/// and `Bytes` both carry byte strings; `F64` carries `f64::to_bits`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ColumnValue {
    I64(i64),
    F64(u64),
    Bool(bool),
    Str(Vec<u8>),
    Bytes(Vec<u8>),
}

impl ColumnValue {
    /// The column type this value stores under.
    pub fn field_type(&self) -> FieldType {
        match self {
            ColumnValue::I64(_) => FieldType::I64,
            ColumnValue::F64(_) => FieldType::F64,
            ColumnValue::Bool(_) => FieldType::Bool,
            ColumnValue::Str(_) => FieldType::Str,
            ColumnValue::Bytes(_) => FieldType::Bytes,
        }
    }
}

/// Canonical byte encoding of a single attribute value, used to store `List`
/// and `Map` values in a `Bytes` column and to compare them for equality. It
/// wraps the value in a one-entry attribute set so the frozen
/// [`canonical_attr_bytes`] grammar (ravel-types) does the encoding; the
/// wrapper key is constant so the mapping stays injective over values.
pub fn canonical_value_bytes(value: &AttrValue) -> Vec<u8> {
    canonical_attr_bytes(std::slice::from_ref(&(String::new(), value.clone())))
}

/// Maps an [`AttrValue`] to the column type and resolved value it stores
/// under. `List` and `Map` canonicalize into a `Bytes` column
/// (docs/log-segment-format.md: nested values are canonically encoded and
/// typed `Bytes`).
pub fn resolve_value(value: &AttrValue) -> (FieldType, ColumnValue) {
    match value {
        AttrValue::Str(s) => (FieldType::Str, ColumnValue::Str(s.clone().into_bytes())),
        AttrValue::I64(v) => (FieldType::I64, ColumnValue::I64(*v)),
        AttrValue::F64(f) => (FieldType::F64, ColumnValue::F64(f.to_bits())),
        AttrValue::Bool(b) => (FieldType::Bool, ColumnValue::Bool(*b)),
        AttrValue::Bytes(b) => (FieldType::Bytes, ColumnValue::Bytes(b.clone())),
        other => (
            FieldType::Bytes,
            ColumnValue::Bytes(canonical_value_bytes(other)),
        ),
    }
}

/// A [`LogRecord`] resolved for storage: dense `stream_ref`, dynamic columns
/// mapped to `(column_id, value)`, and any overflow attributes canonicalized
/// into `attrs_raw`.
#[derive(Clone, Debug, PartialEq)]
pub struct ResolvedRow {
    pub stream_ref: u32,
    pub ts_ns: i64,
    pub observed_ts_ns: i64,
    pub severity_num: u8,
    pub severity_text: String,
    pub body: String,
    pub trace_id: Option<[u8; 16]>,
    pub span_id: Option<[u8; 8]>,
    pub flags: u32,
    /// Canonical bytes of the attributes that overflowed the dynamic-column
    /// budget, present only when this row had any. Stored in [`COL_ATTRS_RAW`].
    pub attrs_raw: Option<Vec<u8>>,
    /// Resolved dynamic columns, `(column_id, value)`. A column id appears at
    /// most once per row.
    pub columns: Vec<(u32, ColumnValue)>,
    /// The merged-view values this row contributes to POSTINGS, one
    /// `(column_id, value)` per indexed field the row carries after merging its
    /// resource, scope, and per-record attributes (the record winning on a key
    /// collision), keyed by the same dynamic column the value's type resolves
    /// to. Empty when the writer was given no indexed fields, or the row's
    /// merged value for an indexed field has no matching dynamic column. This
    /// is what makes a v2 POSTINGS list index the merged attribute view rather
    /// than the per-record layer alone (docs/adrs/0049-rlog-postings.md
    /// amendment 2026-08-03); it never affects block encoding, only postings
    /// accumulation.
    pub indexed_terms: Vec<(u32, ColumnValue)>,
}

/// Selects the field a predicate arm applies to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FieldSel {
    Body,
    SeverityText,
    Attr(String),
}

/// A scan predicate. `And` is the only combinator; every arm prunes
/// independently and the surviving blocks are re-evaluated exactly per row
/// (docs/log-segment-format.md "Pruning soundness").
#[derive(Clone, Debug, PartialEq)]
pub enum Predicate {
    And(Vec<Predicate>),
    /// Inclusive timestamp range on `ts_ns`.
    TsRange {
        min_ns: i64,
        max_ns: i64,
    },
    StreamIn(Vec<LogStreamId>),
    HasWord {
        field: FieldSel,
        word: String,
    },
    Equals {
        field: FieldSel,
        value: AttrValue,
    },
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use ravel_types::logstream::log_stream_id;

    fn resource() -> Vec<(String, AttrValue)> {
        vec![
            ("service.name".into(), AttrValue::Str("api".into())),
            ("host".into(), AttrValue::Str("h1".into())),
        ]
    }

    fn scope_attrs() -> Vec<(String, AttrValue)> {
        vec![("lib".into(), AttrValue::I64(7))]
    }

    #[test]
    fn stream_attrs_bytes_is_the_hash_preimage() {
        let blob = stream_attrs_bytes(&resource(), "scope.name", "1.2.3", &scope_attrs());
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"ravel-logstream-v1");
        hasher.update(&blob);
        let digest = hasher.finalize();
        let expected = log_stream_id(&resource(), "scope.name", "1.2.3", &scope_attrs());
        assert_eq!(&digest.as_bytes()[..16], &expected.0);
    }

    #[test]
    fn stream_attrs_bytes_matches_the_documented_layout() {
        let blob = stream_attrs_bytes(&resource(), "sc", "1", &scope_attrs());
        let mut want = canonical_attr_bytes(&resource());
        want.push(2); // len("sc")
        want.extend_from_slice(b"sc");
        want.push(1); // len("1")
        want.extend_from_slice(b"1");
        want.extend_from_slice(&canonical_attr_bytes(&scope_attrs()));
        assert_eq!(blob, want);
    }

    #[test]
    fn stream_attrs_bytes_is_attribute_order_insensitive() {
        let mut reversed = resource();
        reversed.reverse();
        assert_eq!(
            stream_attrs_bytes(&resource(), "sc", "1", &[]),
            stream_attrs_bytes(&reversed, "sc", "1", &[])
        );
    }

    #[test]
    fn stream_attrs_bytes_separates_scope_name_from_version() {
        // Length prefixes keep the concatenation injective: "ab"+"c" and
        // "a"+"bc" must not produce the same blob.
        assert_ne!(
            stream_attrs_bytes(&[], "ab", "c", &[]),
            stream_attrs_bytes(&[], "a", "bc", &[])
        );
    }
}
