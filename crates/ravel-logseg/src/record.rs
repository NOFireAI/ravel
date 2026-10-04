//! Log record model, fixed column ids, and the resolved-row form the object
//! writer feeds to the block encoder (docs/log-segment-format.md).
//!
//! [`LogRecord`] is the caller-facing record. [`ResolvedRow`] is its
//! storage-facing shape: the stream id replaced by its dense `stream_ref`, and
//! every dynamic attribute already mapped to a `(column_id, value)` typed
//! slot, with overflow attributes canonicalized into `attrs_raw`. The writer
//! (task 12) produces [`ResolvedRow`]s; [`crate::block`] encodes them.

use ravel_types::logstream::{AttrValue, LogStreamId, canonical_attr_bytes};

use crate::error::LogSegError;
use crate::varint::{get_ivarint, get_uvarint, put_uvarint};

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

/// [`stream_attrs_bytes`] with the scope name and version taken as raw bytes,
/// so a test can build the non-UTF-8 blob a malformed producer could hand the
/// writer.
#[cfg(test)]
pub(crate) fn stream_attrs_bytes_raw_scope(
    resource_attrs: &[(String, AttrValue)],
    scope_name: &[u8],
    scope_version: &[u8],
    scope_attrs: &[(String, AttrValue)],
) -> Vec<u8> {
    let mut out = canonical_attr_bytes(resource_attrs);
    put_uvarint(&mut out, scope_name.len() as u64);
    out.extend_from_slice(scope_name);
    put_uvarint(&mut out, scope_version.len() as u64);
    out.extend_from_slice(scope_version);
    out.extend_from_slice(&canonical_attr_bytes(scope_attrs));
    out
}

/// The deepest nesting level a writer may store, in the units
/// [`attr_value_fits_depth`] states. The `stream_attrs` decoder enforces it
/// too, so hostile nesting cannot exhaust the stack. The `attrs_raw` decoder
/// reads a superset under its own, looser bounds, so values written before
/// this rule stay readable.
pub const MAX_ATTR_DEPTH: u32 = 32;

/// Whether `value`, stored as one entry of a top-level attribute set, nests
/// shallowly enough for the segment format to hold it.
///
/// The accounting: a top-level attribute set sits at level 0 and each of its
/// values at level 1. A list's elements sit one level below the list. A map
/// costs two levels: its entry set sits one level below the map, and the
/// entry values one level below that. A value fits when no value and no map
/// entry set it contains sits past [`MAX_ATTR_DEPTH`]. So 15 nested maps
/// around a scalar fit and 16 do not; 31 nested lists around a scalar fit and
/// 32 do not.
///
/// This is the one definition of what may be admitted and written: the
/// `stream_attrs` decoder ([`decode_stream_attrs`]) accepts an encoded
/// attribute set exactly when every value in it satisfies this predicate, the
/// writer refuses a value that does not, in a STREAM_DIR blob or in
/// `attrs_raw`, and OTLP admission (`ravel-otlp`) rejects such an attribute
/// before it reaches the writer. The `attrs_raw` decoder accepts every value
/// that satisfies it and more besides: it keeps reading values written under
/// the older bound that predates this rule.
pub fn attr_value_fits_depth(value: &AttrValue) -> bool {
    value_fits_at(value, 1)
}

/// [`attr_value_fits_depth`] for a value at `depth`. Returns as soon as a level
/// passes the cap, so recursion never goes deeper than [`MAX_ATTR_DEPTH`].
fn value_fits_at(value: &AttrValue, depth: u32) -> bool {
    if depth > MAX_ATTR_DEPTH {
        return false;
    }
    match value {
        AttrValue::List(items) => items.iter().all(|v| value_fits_at(v, depth + 1)),
        AttrValue::Map(entries) => {
            depth < MAX_ATTR_DEPTH && entries.iter().all(|(_, v)| value_fits_at(v, depth + 2))
        }
        _ => true,
    }
}

/// Entry/element-count cap per attribute set or list when decoding, so a
/// corrupt count is rejected rather than allocated on.
const MAX_ATTR_ENTRIES: u64 = 1 << 20;

/// The decoded form of a [`LogRecord::stream_attrs`] blob: the resource
/// attribute set, scope name and version, and the scope attribute set, exactly
/// as [`stream_attrs_bytes`] encoded them (the inverse of that function).
#[derive(Clone, Debug, PartialEq)]
pub struct StreamAttrs {
    pub resource: Vec<(String, AttrValue)>,
    pub scope_name: String,
    pub scope_version: String,
    pub scope_attrs: Vec<(String, AttrValue)>,
}

/// Decodes a [`LogRecord::stream_attrs`] blob into its structured form
/// ([`stream_attrs_bytes`]'s inverse). Every [`AttrValue`] variant round-trips
/// exactly, including a nested `List`/`Map` and an `F64`'s exact bit pattern (a
/// NaN payload or -0.0 survives, since the encoding stores `to_bits` verbatim).
/// Corrupt input (a truncated blob, an over-long length prefix, an unknown
/// value tag, a key, string value, scope name or scope version that is not
/// UTF-8) is a typed [`LogSegError::Corrupted`], never a panic.
///
/// This is the one definition of a valid blob:
/// [`crate::reader::stream_attr_pairs`] is implemented on it and refuses
/// exactly what it refuses, with the same error, and the writer validates every
/// STREAM_DIR blob through that function.
pub fn decode_stream_attrs(blob: &[u8]) -> Result<StreamAttrs, LogSegError> {
    let mut pos = 0usize;
    let resource = decode_attr_set(blob, &mut pos, 0)?;
    let scope_name = decode_len_prefixed_string(blob, &mut pos)?;
    let scope_version = decode_len_prefixed_string(blob, &mut pos)?;
    let scope_attrs = decode_attr_set(blob, &mut pos, 0)?;
    Ok(StreamAttrs {
        resource,
        scope_name,
        scope_version,
        scope_attrs,
    })
}

/// v1 stringification of a dynamic attribute value, used to render `attrs`
/// map values to text. Scalar values render to their natural text; `Bytes`,
/// `List`, and `Map` render to the lowercase hex of their canonical encoding, a
/// deterministic, injective form pending a richer typed column.
pub fn attr_value_to_string(v: &AttrValue) -> String {
    match v {
        AttrValue::Str(s) => s.clone(),
        AttrValue::I64(i) => i.to_string(),
        AttrValue::F64(f) => f.to_string(),
        AttrValue::Bool(b) => b.to_string(),
        AttrValue::Bytes(b) => hex_lower(b),
        AttrValue::List(_) | AttrValue::Map(_) => hex_lower(&canonical_attr_bytes(
            std::slice::from_ref(&(String::new(), v.clone())),
        )),
    }
}

fn hex_lower(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        // Writing to a String never fails.
        let _ = write!(out, "{b:02x}");
    }
    out
}

fn decode_attr_set(
    buf: &[u8],
    pos: &mut usize,
    depth: u32,
) -> Result<Vec<(String, AttrValue)>, LogSegError> {
    if depth > MAX_ATTR_DEPTH {
        return Err(corrupt("stream_attrs nesting too deep"));
    }
    let count = get_uvarint(buf, pos)?;
    if count > MAX_ATTR_ENTRIES {
        return Err(corrupt("stream_attrs entry count over cap"));
    }
    let mut out = Vec::new();
    for _ in 0..count {
        let klen = usize_of(get_uvarint(buf, pos)?)?;
        let kstart = *pos;
        advance(buf, pos, klen)?;
        let key = std::str::from_utf8(&buf[kstart..*pos])
            .map_err(|_| corrupt("stream_attrs key not utf-8"))?
            .to_string();
        let value = decode_value(buf, pos, depth + 1)?;
        out.push((key, value));
    }
    Ok(out)
}

/// Decodes one encoded attribute value at `pos` (frozen grammar,
/// `ravel_types::logstream`: 1=Str 2=I64 3=F64 4=Bool 5=Bytes 6=List 7=Map),
/// advancing `pos` past it.
fn decode_value(buf: &[u8], pos: &mut usize, depth: u32) -> Result<AttrValue, LogSegError> {
    if depth > MAX_ATTR_DEPTH {
        return Err(corrupt("stream_attrs nesting too deep"));
    }
    let tag = read_u8(buf, pos)?;
    Ok(match tag {
        1 => {
            let len = usize_of(get_uvarint(buf, pos)?)?;
            let start = *pos;
            advance(buf, pos, len)?;
            let s = std::str::from_utf8(&buf[start..*pos])
                .map_err(|_| corrupt("stream_attrs str not utf-8"))?
                .to_string();
            AttrValue::Str(s)
        }
        2 => AttrValue::I64(get_ivarint(buf, pos)?),
        3 => {
            let start = *pos;
            advance(buf, pos, 8)?;
            let mut b = [0u8; 8];
            b.copy_from_slice(&buf[start..*pos]);
            AttrValue::F64(f64::from_bits(u64::from_le_bytes(b)))
        }
        4 => AttrValue::Bool(read_u8(buf, pos)? != 0),
        5 => {
            let len = usize_of(get_uvarint(buf, pos)?)?;
            let start = *pos;
            advance(buf, pos, len)?;
            AttrValue::Bytes(buf[start..*pos].to_vec())
        }
        6 => {
            let n = get_uvarint(buf, pos)?;
            if n > MAX_ATTR_ENTRIES {
                return Err(corrupt("stream_attrs list length over cap"));
            }
            let mut items = Vec::new();
            for _ in 0..n {
                items.push(decode_value(buf, pos, depth + 1)?);
            }
            AttrValue::List(items)
        }
        7 => AttrValue::Map(decode_attr_set(buf, pos, depth + 1)?),
        _ => return Err(corrupt("bad stream_attrs value tag")),
    })
}

fn decode_len_prefixed_string(buf: &[u8], pos: &mut usize) -> Result<String, LogSegError> {
    let len = usize_of(get_uvarint(buf, pos)?)?;
    let start = *pos;
    advance(buf, pos, len)?;
    std::str::from_utf8(&buf[start..*pos])
        .map(|s| s.to_string())
        .map_err(|_| corrupt("stream_attrs scope name/version not utf-8"))
}

fn read_u8(buf: &[u8], pos: &mut usize) -> Result<u8, LogSegError> {
    let b = *buf
        .get(*pos)
        .ok_or_else(|| corrupt("stream_attrs truncated"))?;
    *pos += 1;
    Ok(b)
}

fn advance(buf: &[u8], pos: &mut usize, n: usize) -> Result<(), LogSegError> {
    let end = pos
        .checked_add(n)
        .ok_or_else(|| corrupt("stream_attrs length overflow"))?;
    if end > buf.len() {
        return Err(corrupt("stream_attrs truncated"));
    }
    *pos = end;
    Ok(())
}

fn usize_of(v: u64) -> Result<usize, LogSegError> {
    usize::try_from(v).map_err(|_| corrupt("stream_attrs length exceeds usize"))
}

fn corrupt(what: &str) -> LogSegError {
    LogSegError::Corrupted(format!("stream_attrs: {what}"))
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
    /// The resolved merged-view value of each NumStat-eligible attribute name
    /// this row resolves, keyed by the dynamic column the *value's* type
    /// resolves to, `(column_id, value)`, at most one entry per column id.
    ///
    /// This is what [`crate::block::write_block`] folds into a block's
    /// `NumStat` min/max, and it is deliberately not the same thing as
    /// [`ResolvedRow::columns`] (ADR-0095). The value is the one the read side
    /// reports for the name: the record's resource and scope layer, then its
    /// own attributes overriding, and within its own attributes the two-tier
    /// winner `writer::StampScratch::finish` computes when it carries the name
    /// more than once (two types, or a same-type duplicate that spilled into
    /// `attrs_raw`). A declared typed column materializes a value only when
    /// that resolved value's type matches, so a row whose resolved value for a
    /// name is of some other type has no entry for that name's numeric column
    /// here and contributes to the stat's `null_count` exactly as an absent
    /// attribute does, rather than contributing a value the reader will never
    /// produce.
    ///
    /// A name the row carries only on its resource or scope still has an entry:
    /// a reader resolves that stream-level value for the row, so the stat has
    /// to bound it (that is the same shape `indexed_terms` indexes).
    ///
    /// Empty when the object has no I64/F64/Bool dynamic column, or the row
    /// resolves none of those names. Absence is always a null contribution,
    /// never a fallback to the raw columnar value: a fallback would silently
    /// restore the pre-v3 semantics for any producer that forgot to populate
    /// this.
    pub stat_winners: Vec<(u32, ColumnValue)>,
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
    /// Prune-only inclusive numeric range on a dynamic numeric column
    /// (I64/F64/Bool), selected by attribute name and exact column type.
    ///
    /// `min`/`max` are inclusive bounds in the same bit-pattern encoding
    /// [`crate::block::NumStat`] stores: an `i64` as its two's-complement `u64`,
    /// an `f64` as `to_bits`, a `bool` as `0`/`1`. `None` is an open end.
    ///
    /// This arm may drive block pruning ONLY, through
    /// [`crate::RlogReader::scan_blocks`]'s `prune` channel (exactly like the
    /// POSTINGS `Equals` prune channel). It is never an exact per-row filter:
    /// the caller (the SQL layer, not this crate) re-evaluates the real,
    /// exactly-typed and exactly-bounded range above the scan. Placing it in the
    /// `content` channel matches every row rather than filtering (ADR-0095
    /// decision 6, ADR-0013).
    ///
    /// An `f64` bound is ordered by [`crate::block::NumStat`]'s own
    /// `total_cmp`-based comparison, under which `-0.0 < +0.0` and NaN sorts to
    /// an extreme -- both disagree with SQL's float equality. A caller building
    /// a range that should include zero MUST widen it to cover both zero bit
    /// patterns explicitly, and MUST NOT construct a NaN bound (this pruning
    /// layer has no way to detect or reject one; a NaN bound silently prunes
    /// either everything or nothing depending on its sign bit). Neither case is
    /// reachable through any caller in this crate today; this is a contract
    /// note for the first caller that builds one (ADR-0095 decision 6's
    /// planner-side consumer, tracked separately).
    NumRange {
        field: FieldSel,
        ty: FieldType,
        min: Option<u64>,
        max: Option<u64>,
    },
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use proptest::prelude::*;
    use ravel_types::logstream::log_stream_id;

    use super::*;

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

    #[test]
    fn attr_value_to_string_renders_each_type() {
        assert_eq!(attr_value_to_string(&AttrValue::Str("api".into())), "api");
        assert_eq!(attr_value_to_string(&AttrValue::I64(-42)), "-42");
        assert_eq!(attr_value_to_string(&AttrValue::Bool(true)), "true");
        assert_eq!(
            attr_value_to_string(&AttrValue::Bytes(vec![0xab, 0x01])),
            "ab01"
        );
        // Bytes/List/Map render as lowercase hex of the canonical encoding,
        // never the natural text.
        let list = AttrValue::List(vec![AttrValue::Str("a".into())]);
        let want = {
            use std::fmt::Write as _;
            let mut s = String::new();
            for b in canonical_attr_bytes(std::slice::from_ref(&(String::new(), list.clone()))) {
                let _ = write!(s, "{b:02x}");
            }
            s
        };
        assert_eq!(attr_value_to_string(&list), want);
    }

    #[test]
    fn decode_stream_attrs_rejects_truncated_blob() {
        let blob = stream_attrs_bytes(&[("k".into(), AttrValue::Str("v".into()))], "s", "1", &[]);
        let err = decode_stream_attrs(&blob[..blob.len() - 1]).unwrap_err();
        assert!(matches!(err, LogSegError::Corrupted(_)), "got {err:?}");
    }

    #[test]
    fn decode_stream_attrs_rejects_bad_length_prefix() {
        // One resource entry whose key length varint claims a length far
        // beyond the buffer.
        let blob = vec![1u8, 0x80, 0x80, 0x80, 0x80, 0x01];
        let err = decode_stream_attrs(&blob).unwrap_err();
        assert!(matches!(err, LogSegError::Corrupted(_)), "got {err:?}");
    }

    fn corrupted_message(err: LogSegError) -> String {
        match err {
            LogSegError::Corrupted(msg) => msg,
            other => panic!("expected Corrupted, got {other:?}"),
        }
    }

    /// `stream_attr_pairs` refuses `blob` with exactly the error
    /// `decode_stream_attrs` returns for it.
    fn assert_both_refuse(blob: &[u8]) {
        let want = corrupted_message(decode_stream_attrs(blob).expect_err("decode_stream_attrs"));
        let got = corrupted_message(
            crate::reader::stream_attr_pairs(blob)
                .expect_err("stream_attr_pairs must refuse what decode_stream_attrs refuses"),
        );
        assert_eq!(got, want);
    }

    #[test]
    fn stream_attr_pairs_refuses_a_non_utf8_scope_name() {
        let blob = stream_attrs_bytes_raw_scope(&resource(), b"sc\xff", b"1", &scope_attrs());
        assert_both_refuse(&blob);
    }

    #[test]
    fn stream_attr_pairs_refuses_a_non_utf8_scope_version() {
        let blob = stream_attrs_bytes_raw_scope(&resource(), b"sc", b"1\xc3", &scope_attrs());
        assert_both_refuse(&blob);
    }

    /// The nesting cap counts every map and list level, the same in both
    /// functions: 20 nested maps is past `MAX_ATTR_DEPTH` for the decoder.
    #[test]
    fn stream_attr_pairs_refuses_nesting_past_the_depth_cap() {
        let mut nested = AttrValue::I64(1);
        for _ in 0..20 {
            nested = AttrValue::Map(vec![("m".into(), nested)]);
        }
        let blob = stream_attrs_bytes(&[("deep".into(), nested)], "sc", "1", &[]);
        assert_both_refuse(&blob);
    }

    /// `levels` maps (or lists) nested around `leaf`, built without recursion.
    fn nest(levels: usize, map: bool, leaf: AttrValue) -> AttrValue {
        let mut v = leaf;
        for _ in 0..levels {
            v = if map {
                AttrValue::Map(vec![("m".into(), v)])
            } else {
                AttrValue::List(vec![v])
            };
        }
        v
    }

    /// Whether `value`, as the one resource attribute of a `stream_attrs` blob,
    /// decodes, and whether it decodes as the one `attrs_raw` attribute; each
    /// decoded value must be `value` itself.
    fn decodes_under_each(value: &AttrValue) -> (bool, bool) {
        let pairs = vec![("k".to_string(), value.clone())];
        let stream = decode_stream_attrs(&stream_attrs_bytes(&pairs, "s", "1", &[]));
        let raw = crate::reader::decode_canonical_attrs(&canonical_attr_bytes(&pairs));
        if let Ok(d) = &stream {
            assert_eq!(
                canonical_attr_bytes(&d.resource),
                canonical_attr_bytes(&pairs)
            );
        }
        if let Ok(d) = &raw {
            assert_eq!(canonical_attr_bytes(d), canonical_attr_bytes(&pairs));
        }
        (stream.is_ok(), raw.is_ok())
    }

    /// The depth rule at its boundary: 15 nested maps around a scalar fit and
    /// decode under both decoders, 16 do not fit and the `stream_attrs`
    /// decoder refuses them; 31 nested lists fit and decode, 32 do not. The
    /// `attrs_raw` decoder still reads the value one level past the rule.
    #[test]
    fn depth_rule_agrees_with_the_stream_attrs_decoder_at_the_boundary() {
        for (fits, map) in [(15usize, true), (31, false)] {
            let at = nest(fits, map, AttrValue::I64(1));
            assert!(attr_value_fits_depth(&at), "{fits} levels (map={map})");
            assert_eq!(decodes_under_each(&at), (true, true), "{fits} levels");
            let past = nest(fits + 1, map, AttrValue::I64(1));
            assert!(!attr_value_fits_depth(&past), "{} levels", fits + 1);
            assert_eq!(
                decodes_under_each(&past),
                (false, true),
                "{} levels",
                fits + 1
            );
        }
    }

    /// An empty innermost map or list costs less than one holding a scalar,
    /// exactly as the decoders charge it: the predicate follows them.
    #[test]
    fn depth_rule_charges_an_empty_leaf_as_the_decoders_do() {
        for (levels, map, leaf) in [
            (16usize, true, AttrValue::Map(Vec::new())),
            (32, false, AttrValue::List(Vec::new())),
        ] {
            let at = nest(levels - 1, map, leaf);
            assert!(attr_value_fits_depth(&at), "{levels} levels (map={map})");
            assert_eq!(decodes_under_each(&at), (true, true), "{levels} levels");
        }
    }

    /// One top-level attribute named `k` whose value is `levels` single-element
    /// lists around an `I64`, as canonical attribute-set bytes, built
    /// iteratively so the test itself never recurses.
    fn deep_list_set_bytes(levels: usize) -> Vec<u8> {
        let mut out = vec![1u8, 1, b'k'];
        for _ in 0..levels {
            out.extend_from_slice(&[6, 1]);
        }
        out.extend_from_slice(&[2, 0]);
        out
    }

    /// A list nest thousands of levels deep is a typed error under both
    /// decoders, refused at the cap rather than recursed into.
    #[test]
    fn deep_list_nest_is_a_typed_error_under_both_decoders() {
        let set = deep_list_set_bytes(5_000);
        let raw = crate::reader::decode_canonical_attrs(&set).expect_err("attrs_raw refuses");
        assert_eq!(corrupted_message(raw), "attrs_raw too deep");

        let mut blob = set;
        blob.extend_from_slice(&[0, 0, 0]);
        let stream = decode_stream_attrs(&blob).expect_err("stream_attrs refuses");
        assert_eq!(
            corrupted_message(stream),
            "stream_attrs: stream_attrs nesting too deep"
        );
    }

    /// Decodes `value`, the one attribute of a canonical set, under the
    /// `attrs_raw` decoder and checks it reads back unchanged.
    fn attrs_raw_reads_back(value: AttrValue) -> Result<(), LogSegError> {
        let pairs = vec![("k".to_string(), value)];
        let bytes = canonical_attr_bytes(&pairs);
        let decoded = crate::reader::decode_canonical_attrs(&bytes)?;
        assert_eq!(canonical_attr_bytes(&decoded), bytes);
        Ok(())
    }

    /// Record attributes the write-side rule now refuses but OTLP admission
    /// used to accept: 16 nested maps, 32 nested maps (the old map cap), and
    /// 100 nested lists (the old admission limit). An object already in
    /// storage may hold any of them in `attrs_raw`, so the decoder reads them.
    #[test]
    fn attrs_raw_reads_values_written_under_the_older_bound() {
        for (levels, map) in [(16usize, true), (32, true), (100, false)] {
            let value = nest(levels, map, AttrValue::I64(1));
            assert!(!attr_value_fits_depth(&value), "{levels} (map={map})");
            attrs_raw_reads_back(value)
                .unwrap_or_else(|e| panic!("{levels} levels (map={map}) must decode: {e}"));
        }
    }

    /// The `attrs_raw` decoder's two bounds: a list nest decodes up to 128
    /// levels and is a typed error at 129 and at 5,000, without recursing past
    /// the cap; a map past the old map cap is refused as it always was, the
    /// lists enclosing it counted.
    #[test]
    fn attrs_raw_refuses_nesting_past_its_read_bounds() {
        attrs_raw_reads_back(nest(128, false, AttrValue::I64(1))).expect("128 nested lists");
        attrs_raw_reads_back(nest(127, false, AttrValue::List(Vec::new())))
            .expect("128 nested lists, the innermost empty");

        let pairs = vec![("k".to_string(), nest(129, false, AttrValue::I64(0)))];
        assert_eq!(canonical_attr_bytes(&pairs), deep_list_set_bytes(129));
        for levels in [129usize, 5_000] {
            let err = crate::reader::decode_canonical_attrs(&deep_list_set_bytes(levels))
                .expect_err("past the read cap");
            assert_eq!(corrupted_message(err), "attrs_raw too deep", "{levels}");
        }

        let maps = nest(33, true, AttrValue::I64(1));
        let map_under_lists = nest(32, false, AttrValue::Map(Vec::new()));
        for value in [maps, map_under_lists] {
            let err = attrs_raw_reads_back(value).expect_err("past the map cap");
            assert_eq!(corrupted_message(err), "attrs_raw too deep");
        }
    }

    /// What the `attrs_raw` decoder accepts, stated over a decoded value: the
    /// bound it has always applied (a map entry set inside more than 32 maps
    /// and lists, its own map included, is refused) plus the stack-safety cap
    /// (more than 128 maps and lists on one path is refused). `depth` is the
    /// number of maps and lists enclosing `value`.
    fn attrs_raw_read_bound_accepts(value: &AttrValue, depth: u32) -> bool {
        let inner = depth + 1;
        match value {
            AttrValue::List(items) => {
                inner <= 128 && items.iter().all(|v| attrs_raw_read_bound_accepts(v, inner))
            }
            AttrValue::Map(entries) => {
                inner <= 32
                    && entries
                        .iter()
                        .all(|(_, v)| attrs_raw_read_bound_accepts(v, inner))
            }
            _ => true,
        }
    }

    /// Values nested past the cap on purpose: a spine of up to 40 map or list
    /// levels, each optionally carrying a shallow sibling, around a leaf that
    /// may be a scalar, an empty map or list, or a shallow nested value.
    fn arb_deep_value() -> impl Strategy<Value = AttrValue> {
        let leaf = prop_oneof![
            arb_value(),
            Just(AttrValue::Map(Vec::new())),
            Just(AttrValue::List(Vec::new())),
        ];
        let layer = (any::<bool>(), proptest::option::weighted(0.3, arb_value()));
        (leaf, proptest::collection::vec(layer, 0..40)).prop_map(|(leaf, layers)| {
            let mut v = leaf;
            for (map, sibling) in layers {
                v = if map {
                    let mut entries = vec![("m".to_string(), v)];
                    entries.extend(sibling.map(|s| ("s".to_string(), s)));
                    AttrValue::Map(entries)
                } else {
                    let mut items = vec![v];
                    items.extend(sibling);
                    AttrValue::List(items)
                };
            }
            v
        })
    }

    fn arb_value() -> impl Strategy<Value = AttrValue> {
        let leaf = prop_oneof![
            ".*".prop_map(AttrValue::Str),
            any::<i64>().prop_map(AttrValue::I64),
            any::<u64>().prop_map(|b| AttrValue::F64(f64::from_bits(b))),
            any::<bool>().prop_map(AttrValue::Bool),
            proptest::collection::vec(any::<u8>(), 0..8).prop_map(AttrValue::Bytes),
        ];
        leaf.prop_recursive(3, 16, 4, |inner| {
            prop_oneof![
                proptest::collection::vec(inner.clone(), 0..4).prop_map(AttrValue::List),
                proptest::collection::vec(("[a-z]{1,4}", inner), 0..4).prop_map(AttrValue::Map),
            ]
        })
    }

    fn arb_attrs() -> impl Strategy<Value = Vec<(String, AttrValue)>> {
        proptest::collection::vec(("[a-z]{1,4}", arb_value()), 0..6)
    }

    proptest! {
        /// The predicate is true exactly when the encoded value decodes under
        /// the `stream_attrs` decoder; when it is true the value decodes under
        /// the `attrs_raw` decoder too, which reads a superset: exactly the
        /// values its older bound accepts.
        #[test]
        fn depth_rule_is_what_stream_attrs_reads_and_attrs_raw_reads_a_superset(
            value in arb_deep_value(),
        ) {
            let fits = attr_value_fits_depth(&value);
            let (stream, raw) = decodes_under_each(&value);
            prop_assert_eq!(stream, fits);
            if fits {
                prop_assert!(raw);
            }
            prop_assert_eq!(raw, attrs_raw_read_bound_accepts(&value, 0));
        }

        #[test]
        fn stream_attrs_round_trip(
            resource in arb_attrs(),
            scope_name in "[a-z]{0,8}",
            scope_version in "[a-z0-9.]{0,8}",
            scope_attrs in arb_attrs(),
        ) {
            let blob = stream_attrs_bytes(&resource, &scope_name, &scope_version, &scope_attrs);
            let decoded = decode_stream_attrs(&blob).expect("decode");
            // `canonical_attr_bytes` sorts entries (by key then encoded value),
            // so a decoded set need not match the input's insertion order; two
            // sets carry the same information iff their canonical bytes match.
            // The encoding stores an F64 as `to_bits()`, so this comparison is
            // bit-exact too (a NaN payload or -0.0 changes the bytes).
            prop_assert_eq!(
                canonical_attr_bytes(&decoded.resource),
                canonical_attr_bytes(&resource)
            );
            prop_assert_eq!(decoded.scope_name, scope_name);
            prop_assert_eq!(decoded.scope_version, scope_version);
            prop_assert_eq!(
                canonical_attr_bytes(&decoded.scope_attrs),
                canonical_attr_bytes(&scope_attrs)
            );
        }

        /// Every blob the encoder produces decodes under both functions, and
        /// `stream_attr_pairs` yields the resource entries then the scope
        /// attributes, in decoded order.
        #[test]
        fn encoded_stream_attrs_decode_under_both_functions(
            resource in arb_attrs(),
            scope_name in ".{0,8}",
            scope_version in ".{0,8}",
            scope_attrs in arb_attrs(),
        ) {
            let blob = stream_attrs_bytes(&resource, &scope_name, &scope_version, &scope_attrs);
            let decoded = decode_stream_attrs(&blob).expect("decode_stream_attrs");
            let pairs = crate::reader::stream_attr_pairs(&blob).expect("stream_attr_pairs");
            let mut want = decoded.resource;
            want.extend(decoded.scope_attrs);
            prop_assert_eq!(canonical_attr_bytes(&pairs), canonical_attr_bytes(&want));
            let keys: Vec<&str> = pairs.iter().map(|(k, _)| k.as_str()).collect();
            let want_keys: Vec<&str> = want.iter().map(|(k, _)| k.as_str()).collect();
            prop_assert_eq!(keys, want_keys);
        }

        /// Arbitrary bytes in the scope name and version: both functions accept
        /// exactly when both strings are UTF-8, and refuse with the same error
        /// otherwise.
        #[test]
        fn stream_attr_decoders_agree_on_arbitrary_scope_bytes(
            resource in arb_attrs(),
            scope_name in proptest::collection::vec(any::<u8>(), 0..12),
            scope_version in proptest::collection::vec(any::<u8>(), 0..12),
            scope_attrs in arb_attrs(),
        ) {
            let blob =
                stream_attrs_bytes_raw_scope(&resource, &scope_name, &scope_version, &scope_attrs);
            let valid = std::str::from_utf8(&scope_name).is_ok()
                && std::str::from_utf8(&scope_version).is_ok();
            prop_assert_eq!(decode_stream_attrs(&blob).is_ok(), valid);
            assert_decoders_agree(&blob)?;
        }

        /// Arbitrary byte sequences, and encoded blobs with one byte
        /// overwritten: the two functions agree on Ok versus Err, and on the
        /// error.
        #[test]
        fn stream_attr_decoders_agree_on_arbitrary_bytes(
            bytes in proptest::collection::vec(any::<u8>(), 0..64),
            resource in arb_attrs(),
            scope_attrs in arb_attrs(),
            at in any::<proptest::sample::Index>(),
            byte in any::<u8>(),
        ) {
            assert_decoders_agree(&bytes)?;
            let mut blob = stream_attrs_bytes(&resource, "scope", "1.0", &scope_attrs);
            let i = at.index(blob.len());
            blob[i] = byte;
            assert_decoders_agree(&blob)?;
        }
    }

    fn assert_decoders_agree(blob: &[u8]) -> Result<(), TestCaseError> {
        match (
            decode_stream_attrs(blob),
            crate::reader::stream_attr_pairs(blob),
        ) {
            (Ok(_), Ok(_)) => {}
            (Err(LogSegError::Corrupted(want)), Err(LogSegError::Corrupted(got))) => {
                prop_assert_eq!(got, want);
            }
            (want, got) => {
                return Err(TestCaseError::fail(format!(
                    "decoders disagree: decode_stream_attrs {:?}, stream_attr_pairs {:?}",
                    want.map(|_| ()),
                    got.map(|_| ())
                )));
            }
        }
        Ok(())
    }
}
