//! Log record model, fixed column ids, and the resolved-row form the object
//! writer feeds to the block encoder (docs/log-segment-format.md).
//!
//! [`LogRecord`] is the caller-facing record. [`ResolvedRow`] is its
//! storage-facing shape: the stream id replaced by its dense `stream_ref`, and
//! every dynamic attribute already mapped to a `(column_id, value)` typed
//! slot, with overflow attributes canonicalized into `attrs_raw`. The writer
//! (task 12) produces [`ResolvedRow`]s; [`crate::block`] encodes them.

use ravel_types::logstream::{AttrValue, LogStreamId, canonical_attr_bytes};

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

/// A single log record as handed to the writer.
#[derive(Clone, Debug, PartialEq)]
pub struct LogRecord {
    pub stream_id: LogStreamId,
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
