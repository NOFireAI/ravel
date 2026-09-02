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

/// Depth cap when decoding a stream_attrs blob, so hostile nesting cannot
/// exhaust the stack.
const MAX_ATTR_DEPTH: u32 = 32;

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
/// value tag) is a typed [`LogSegError::Corrupted`], never a panic.
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
    }
}
