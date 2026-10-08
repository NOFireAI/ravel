//! [`ColumnarLogBatch`]: a batch of log records in column-major form, the
//! interchange type the columnar object-build fast path consumes instead of
//! per-row [`crate::record::ResolvedRow`]s (ADR-0109).
//!
//! Every buffer is plain, owned, and `Send`: the batch is built by a producer
//! (the ingest router, #604) and moved across a channel to the writer, so it
//! borrows nothing. Optional per-row columns are stored densely -- a value
//! buffer holds only the present rows and a per-row [`Bitmap`] records which
//! rows are present -- so an all-absent column costs no per-row materialization.
//!
//! ## Dynamic attribute cells are typed and dense
//!
//! A dynamic column stores its present cells as one typed buffer
//! ([`DynCells`]): string bytes behind `u32` offsets, `Vec<i64>`, `Vec<f64>`,
//! `Vec<bool>`, or resolved bytes for a `Bytes` column. A `List`/`Map` cell
//! resolves to a `Bytes` column holding its `canonical_value_bytes`, but when it
//! folds into `attrs_raw` (past the `max_dynamic_columns` budget) or competes in
//! the merged view, the row path canonicalizes the *original* attribute, whose
//! canonical bytes differ from those of the `Bytes` value it resolves to. So a
//! `Bytes` column also keeps the original `List`/`Map` values in a sparse side
//! list ([`BytesCells::nested`]); every other cell type round-trips exactly from
//! its typed form.

use std::collections::HashMap;

use ravel_types::logstream::{AttrValue, LogStreamId};

use crate::error::LogSegError;
use crate::record::{ColumnValue, FieldType, LogRecord, canonical_value_bytes};

/// A packed presence bitmap, one bit per row, LSB-first within each byte.
///
/// `len` is the logical row count; `bits` is `ceil(len / 8)` bytes. A set bit
/// marks a present (non-null) row.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Bitmap {
    bits: Vec<u8>,
    len: usize,
}

impl Bitmap {
    /// An empty bitmap.
    pub fn new() -> Self {
        Bitmap {
            bits: Vec::new(),
            len: 0,
        }
    }

    /// An empty bitmap with room for `rows` rows.
    pub fn with_capacity(rows: usize) -> Self {
        Bitmap {
            bits: Vec::with_capacity(rows.div_ceil(8)),
            len: 0,
        }
    }

    /// Extends the bitmap to `len` rows with absent rows. A no-op when it
    /// already describes `len` or more rows.
    pub fn pad_to(&mut self, len: usize) {
        if len > self.len {
            self.bits.resize(len.div_ceil(8), 0);
            self.len = len;
        }
    }

    /// Appends one row's presence bit.
    pub fn push(&mut self, present: bool) {
        let byte = self.len / 8;
        if byte >= self.bits.len() {
            self.bits.push(0);
        }
        if present {
            self.bits[byte] |= 1 << (self.len % 8);
        }
        self.len += 1;
    }

    /// Whether row `row` is present. Rows at or past `len` read as absent.
    pub fn get(&self, row: usize) -> bool {
        if row >= self.len {
            return false;
        }
        (self.bits[row / 8] >> (row % 8)) & 1 == 1
    }

    /// Number of rows the bitmap describes.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Whether the bitmap describes zero rows.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Count of present (set) rows.
    pub fn count_present(&self) -> usize {
        self.bits.iter().map(|b| b.count_ones() as usize).sum()
    }

    /// The raw packed bytes.
    pub fn bytes(&self) -> &[u8] {
        &self.bits
    }

    /// Heap bytes allocated for the bits (capacity, not length).
    pub fn heap_bytes(&self) -> usize {
        self.bits.capacity()
    }
}

/// Heap bytes a `Vec<T>`'s buffer holds: its capacity, not its length.
fn vec_heap<T>(v: &Vec<T>) -> usize {
    v.capacity() * size_of::<T>()
}

/// Heap bytes owned by one [`AttrValue`], beyond its own inline size.
fn attr_value_heap(value: &AttrValue) -> usize {
    match value {
        AttrValue::Str(s) => s.capacity(),
        AttrValue::Bytes(b) => b.capacity(),
        AttrValue::I64(_) | AttrValue::F64(_) | AttrValue::Bool(_) => 0,
        AttrValue::List(items) => {
            vec_heap(items) + items.iter().map(attr_value_heap).sum::<usize>()
        }
        AttrValue::Map(entries) => vec_heap(entries) + attr_pairs_heap(entries),
    }
}

/// Heap bytes owned by the keys and values of `(name, value)` pairs, beyond the
/// vector holding the pairs.
fn attr_pairs_heap(pairs: &[(String, AttrValue)]) -> usize {
    pairs
        .iter()
        .map(|(k, v)| k.capacity() + attr_value_heap(v))
        .sum()
}

/// A variable-length byte column: contiguous `data` with `offsets` marking each
/// value's end. `offsets` has one more entry than there are values; value `i`
/// is `data[offsets[i]..offsets[i + 1]]`.
///
/// The `u32` offsets address at most [`VAR_BYTES_MAX`] bytes per column.
/// [`Self::try_push`] refuses a value that would pass that limit;
/// [`Self::push`] does not check, and a column it took past the limit fails
/// [`ColumnarLogBatch::validate`].
#[derive(Clone, Debug)]
pub struct VarBytes {
    offsets: Vec<u32>,
    data: Vec<u8>,
    /// The most value bytes [`Self::try_push`] admits, at most
    /// [`VAR_BYTES_MAX`]. Not part of the column's value: equality ignores it.
    limit: usize,
}

/// The most value bytes one [`VarBytes`] holds: the largest `u32` offset,
/// `u32::MAX` bytes, one byte short of 4 GiB.
pub const VAR_BYTES_MAX: usize = u32::MAX as usize;

impl Default for VarBytes {
    /// No offsets at all, not even the leading `0` [`Self::new`] puts in place.
    fn default() -> Self {
        VarBytes {
            offsets: Vec::new(),
            data: Vec::new(),
            limit: VAR_BYTES_MAX,
        }
    }
}

impl PartialEq for VarBytes {
    fn eq(&self, other: &Self) -> bool {
        self.offsets == other.offsets && self.data == other.data
    }
}

impl Eq for VarBytes {}

impl VarBytes {
    /// An empty column with the leading `0` offset in place.
    pub fn new() -> Self {
        Self::with_capacity(0, 0)
    }

    /// An empty column with room for `values` values totalling `bytes` bytes.
    pub fn with_capacity(values: usize, bytes: usize) -> Self {
        Self::with_byte_limit(values, bytes, VAR_BYTES_MAX)
    }

    /// [`Self::with_capacity`], with [`Self::try_push`] refusing a value that
    /// would take the column past `limit` bytes. A `limit` above
    /// [`VAR_BYTES_MAX`] is lowered to it. Lets a test reach the refusal
    /// without holding `u32::MAX` bytes, one byte short of 4 GiB.
    pub fn with_byte_limit(values: usize, bytes: usize, limit: usize) -> Self {
        let mut offsets = Vec::with_capacity(values.saturating_add(1));
        offsets.push(0);
        VarBytes {
            offsets,
            data: Vec::with_capacity(bytes),
            limit: limit.min(VAR_BYTES_MAX),
        }
    }

    /// Appends one value without checking the column's byte limit: past
    /// [`VAR_BYTES_MAX`] bytes the offset wraps, and
    /// [`ColumnarLogBatch::validate`] refuses the column. A producer that can
    /// reach that size uses [`Self::try_push`].
    pub fn push(&mut self, value: &[u8]) {
        self.data.extend_from_slice(value);
        self.offsets.push(self.data.len() as u32);
    }

    /// Appends one value, or refuses it with [`LogSegError::LimitExceeded`]
    /// when the column would then hold more bytes than its limit
    /// ([`VAR_BYTES_MAX`] unless set by [`Self::with_byte_limit`]). A refused
    /// value is not stored.
    pub fn try_push(&mut self, value: &[u8]) -> Result<(), LogSegError> {
        let end = self
            .data
            .len()
            .checked_add(value.len())
            .filter(|&end| end <= self.limit)
            .and_then(|end| u32::try_from(end).ok());
        let Some(end) = end else {
            return Err(LogSegError::LimitExceeded(format!(
                "a {}-byte value would take a column holding {} bytes past its limit of {} \
                 bytes, the most its u32 offsets address in one batch",
                value.len(),
                self.data.len(),
                self.limit,
            )));
        };
        self.data.extend_from_slice(value);
        self.offsets.push(end);
        Ok(())
    }

    /// Why the offsets do not describe `data`, if they do not: an offset
    /// smaller than the one before it, a first offset other than `0`, or a
    /// last offset other than `data.len()`. An empty `offsets` (the
    /// [`Default`] column) describes zero values and passes. [`Self::get`]
    /// cannot panic on a column this accepts.
    pub fn offsets_error(&self) -> Option<String> {
        let (Some(&first), Some(&last)) = (self.offsets.first(), self.offsets.last()) else {
            return None;
        };
        if first != 0 {
            return Some(format!("its first offset is {first}, not 0"));
        }
        if let Some(i) = self.offsets.windows(2).position(|w| w[1] < w[0]) {
            return Some(format!(
                "offset {} is {} but offset {i} before it is {}: the offsets are not \
                 monotonic, as when more than {VAR_BYTES_MAX} bytes wrap them",
                i + 1,
                self.offsets[i + 1],
                self.offsets[i],
            ));
        }
        if last as usize != self.data.len() {
            return Some(format!(
                "its last offset is {last} but it holds {} bytes",
                self.data.len()
            ));
        }
        None
    }

    /// Number of values stored. `Default::default()` produces an empty
    /// `offsets` (no leading `0`, unlike [`Self::new`]); `saturating_sub`
    /// keeps that case at `0` instead of underflowing, so a
    /// default-constructed column reads as empty rather than panicking (debug)
    /// or wrapping to `usize::MAX` (release) before [`ColumnarLogBatch::validate`]
    /// has a chance to refuse it.
    pub fn len(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    /// Whether the column stores zero values.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Borrows value `i`.
    pub fn get(&self, i: usize) -> &[u8] {
        let start = self.offsets[i] as usize;
        let end = self.offsets[i + 1] as usize;
        &self.data[start..end]
    }

    /// The raw offsets slice (`len + 1` entries).
    pub fn offsets(&self) -> &[u32] {
        &self.offsets
    }

    /// The raw contiguous value bytes.
    pub fn data(&self) -> &[u8] {
        &self.data
    }

    /// Heap bytes allocated for offsets and data (capacity, not length).
    pub fn heap_bytes(&self) -> usize {
        vec_heap(&self.offsets) + self.data.capacity()
    }
}

/// The present cells of a `Bytes` dynamic column.
#[derive(Clone, Debug)]
pub struct BytesCells {
    /// One resolved value per present cell: a `Bytes` value's own bytes, or a
    /// `List`/`Map` value's [`canonical_value_bytes`].
    pub values: VarBytes,
    /// `(slot, original value)` for every cell that is a `List` or `Map`,
    /// strictly ascending by slot, with `values[slot]` its canonical bytes.
    /// `attrs_raw` and the merged-view stamp canonicalize the original value,
    /// whose encoding differs from that of the `Bytes` value it resolves to.
    pub nested: Vec<(u32, AttrValue)>,
}

impl BytesCells {
    /// An empty set of cells.
    pub fn new() -> Self {
        BytesCells {
            values: VarBytes::new(),
            nested: Vec::new(),
        }
    }

    /// The original `List`/`Map` value of cell `slot`, when it is one.
    pub fn nested_at(&self, slot: usize) -> Option<&AttrValue> {
        let slot = u32::try_from(slot).ok()?;
        self.nested
            .binary_search_by_key(&slot, |(s, _)| *s)
            .ok()
            .map(|i| &self.nested[i].1)
    }
}

impl Default for BytesCells {
    fn default() -> Self {
        Self::new()
    }
}

/// The present cells of one dynamic column, dense in row order, one variant per
/// [`FieldType`]. Cell `slot` is the column's `slot`-th present row.
#[derive(Clone, Debug)]
pub enum DynCells {
    /// UTF-8 string bytes.
    Str(VarBytes),
    /// Resolved bytes, with the original of every `List`/`Map` cell.
    Bytes(BytesCells),
    I64(Vec<i64>),
    /// Compared by bit pattern: NaN payloads and `-0.0` are significant.
    F64(Vec<f64>),
    Bool(Vec<bool>),
}

/// Structural equality with floats compared by bit pattern.
fn attr_bits_eq(a: &AttrValue, b: &AttrValue) -> bool {
    match (a, b) {
        (AttrValue::F64(x), AttrValue::F64(y)) => x.to_bits() == y.to_bits(),
        (AttrValue::List(x), AttrValue::List(y)) => {
            x.len() == y.len() && x.iter().zip(y).all(|(p, q)| attr_bits_eq(p, q))
        }
        (AttrValue::Map(x), AttrValue::Map(y)) => {
            x.len() == y.len()
                && x.iter()
                    .zip(y)
                    .all(|((kp, p), (kq, q))| kp == kq && attr_bits_eq(p, q))
        }
        _ => a == b,
    }
}

impl PartialEq for DynCells {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (DynCells::Str(a), DynCells::Str(b)) => a == b,
            (DynCells::Bytes(a), DynCells::Bytes(b)) => {
                a.values == b.values
                    && a.nested.len() == b.nested.len()
                    && a.nested
                        .iter()
                        .zip(&b.nested)
                        .all(|((sa, va), (sb, vb))| sa == sb && attr_bits_eq(va, vb))
            }
            (DynCells::I64(a), DynCells::I64(b)) => a == b,
            (DynCells::F64(a), DynCells::F64(b)) => {
                a.len() == b.len() && a.iter().zip(b).all(|(x, y)| x.to_bits() == y.to_bits())
            }
            (DynCells::Bool(a), DynCells::Bool(b)) => a == b,
            _ => false,
        }
    }
}

impl DynCells {
    /// No cells, of type `field_type`.
    pub fn new(field_type: FieldType) -> Self {
        Self::with_capacity(field_type, 0, 0)
    }

    /// No cells, with room for `cells` cells and, for `Str`/`Bytes`, `bytes`
    /// value bytes.
    pub fn with_capacity(field_type: FieldType, cells: usize, bytes: usize) -> Self {
        match field_type {
            FieldType::Str => DynCells::Str(VarBytes::with_capacity(cells, bytes)),
            FieldType::Bytes => DynCells::Bytes(BytesCells {
                values: VarBytes::with_capacity(cells, bytes),
                nested: Vec::new(),
            }),
            FieldType::I64 => DynCells::I64(Vec::with_capacity(cells)),
            FieldType::F64 => DynCells::F64(Vec::with_capacity(cells)),
            FieldType::Bool => DynCells::Bool(Vec::with_capacity(cells)),
        }
    }

    /// The cells of `values`, in order, as a column of type `field_type`.
    pub fn from_values(field_type: FieldType, values: &[AttrValue]) -> Result<Self, LogSegError> {
        let mut cells = Self::with_capacity(field_type, values.len(), 0);
        for v in values {
            cells.push_value(v)?;
        }
        Ok(cells)
    }

    /// The column type this variant stores.
    pub fn field_type(&self) -> FieldType {
        match self {
            DynCells::Str(_) => FieldType::Str,
            DynCells::Bytes(_) => FieldType::Bytes,
            DynCells::I64(_) => FieldType::I64,
            DynCells::F64(_) => FieldType::F64,
            DynCells::Bool(_) => FieldType::Bool,
        }
    }

    /// Number of cells.
    pub fn len(&self) -> usize {
        match self {
            DynCells::Str(v) => v.len(),
            DynCells::Bytes(b) => b.values.len(),
            DynCells::I64(v) => v.len(),
            DynCells::F64(v) => v.len(),
            DynCells::Bool(v) => v.len(),
        }
    }

    /// Whether there are zero cells.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Heap bytes allocated for the cells (capacity, not length), including
    /// the original value of every nested `Bytes` cell.
    pub fn heap_bytes(&self) -> usize {
        match self {
            DynCells::Str(v) => v.heap_bytes(),
            DynCells::Bytes(b) => {
                b.values.heap_bytes()
                    + vec_heap(&b.nested)
                    + b.nested
                        .iter()
                        .map(|(_, v)| attr_value_heap(v))
                        .sum::<usize>()
            }
            DynCells::I64(v) => vec_heap(v),
            DynCells::F64(v) => vec_heap(v),
            DynCells::Bool(v) => vec_heap(v),
        }
    }

    /// Appends `value`, refusing one whose resolved type is not this column's
    /// ([`LogSegError::MalformedColumnarBatch`]) or one that would take a
    /// `Str`/`Bytes` column past its byte limit ([`VarBytes::try_push`]). A
    /// refused value is not stored.
    pub fn push_value(&mut self, value: &AttrValue) -> Result<(), LogSegError> {
        match (&mut *self, value) {
            (DynCells::Str(v), AttrValue::Str(s)) => v.try_push(s.as_bytes())?,
            (DynCells::Bytes(b), AttrValue::Bytes(x)) => b.values.try_push(x)?,
            (DynCells::Bytes(b), AttrValue::List(_) | AttrValue::Map(_)) => {
                let slot = u32::try_from(b.values.len()).map_err(|_| {
                    LogSegError::MalformedColumnarBatch("more than u32::MAX cells".into())
                })?;
                b.values.try_push(&canonical_value_bytes(value))?;
                b.nested.push((slot, value.clone()));
            }
            (DynCells::I64(v), AttrValue::I64(x)) => v.push(*x),
            (DynCells::F64(v), AttrValue::F64(x)) => v.push(*x),
            (DynCells::Bool(v), AttrValue::Bool(x)) => v.push(*x),
            (cells, _) => {
                return Err(LogSegError::MalformedColumnarBatch(format!(
                    "a {:?} value cannot be a cell of a {:?} column",
                    attr_field_type(value),
                    cells.field_type(),
                )));
            }
        }
        Ok(())
    }

    /// Appends cell `slot` of `src`, which must be of the same type, copying
    /// its bytes and cloning a nested original. Refuses a cell that would take
    /// a `Str`/`Bytes` column past its byte limit ([`VarBytes::try_push`]),
    /// storing nothing.
    pub fn push_from(&mut self, src: &DynCells, slot: usize) -> Result<(), LogSegError> {
        match (&mut *self, src) {
            (DynCells::Str(d), DynCells::Str(s)) => d.try_push(s.get(slot))?,
            (DynCells::Bytes(d), DynCells::Bytes(s)) => {
                let at = u32::try_from(d.values.len()).map_err(|_| {
                    LogSegError::MalformedColumnarBatch("more than u32::MAX cells".into())
                })?;
                d.values.try_push(s.values.get(slot))?;
                if let Some(original) = s.nested_at(slot) {
                    d.nested.push((at, original.clone()));
                }
            }
            (DynCells::I64(d), DynCells::I64(s)) => d.push(s[slot]),
            (DynCells::F64(d), DynCells::F64(s)) => d.push(s[slot]),
            (DynCells::Bool(d), DynCells::Bool(s)) => d.push(s[slot]),
            (d, s) => {
                return Err(LogSegError::MalformedColumnarBatch(format!(
                    "a {:?} cell cannot be appended to a {:?} column",
                    s.field_type(),
                    d.field_type(),
                )));
            }
        }
        Ok(())
    }

    /// The stored bytes of cell `slot` of a `Str` or `Bytes` column; `None` for
    /// any other type.
    pub fn bytes_at(&self, slot: usize) -> Option<&[u8]> {
        match self {
            DynCells::Str(v) => Some(v.get(slot)),
            DynCells::Bytes(b) => Some(b.values.get(slot)),
            _ => None,
        }
    }

    /// Cell `slot` as the attribute value the row path would have held: the
    /// original `List`/`Map` for a nested cell. Fails only on a `Str` cell
    /// that is not UTF-8, which [`ColumnarLogBatch::validate`] refuses.
    pub fn value(&self, slot: usize) -> Result<AttrValue, LogSegError> {
        Ok(match self {
            DynCells::Str(v) => {
                AttrValue::Str(String::from_utf8(v.get(slot).to_vec()).map_err(|_| {
                    LogSegError::MalformedColumnarBatch(format!("Str cell {slot} is not UTF-8"))
                })?)
            }
            DynCells::Bytes(b) => match b.nested_at(slot) {
                Some(original) => original.clone(),
                None => AttrValue::Bytes(b.values.get(slot).to_vec()),
            },
            DynCells::I64(v) => AttrValue::I64(v[slot]),
            DynCells::F64(v) => AttrValue::F64(v[slot]),
            DynCells::Bool(v) => AttrValue::Bool(v[slot]),
        })
    }

    /// Cell `slot` resolved for storage: what [`resolve_value`] returns for the
    /// value the cell holds.
    pub fn column_value(&self, slot: usize) -> ColumnValue {
        match self {
            DynCells::Str(v) => ColumnValue::Str(v.get(slot).to_vec()),
            DynCells::Bytes(b) => ColumnValue::Bytes(b.values.get(slot).to_vec()),
            DynCells::I64(v) => ColumnValue::I64(v[slot]),
            DynCells::F64(v) => ColumnValue::F64(v[slot].to_bits()),
            DynCells::Bool(v) => ColumnValue::Bool(v[slot]),
        }
    }

    /// Cell `slot`'s share of the writer's per-row block-size estimate: the
    /// stored length plus two for `Str`/`Bytes`, a flat eight otherwise.
    pub fn estimate(&self, slot: usize) -> usize {
        match self.bytes_at(slot) {
            Some(b) => b.len() + 2,
            None => 8,
        }
    }
}

/// One dynamic attribute column: an attribute `name` observed with one resolved
/// [`FieldType`], its dense per-present-row typed cells, and a per-row presence
/// [`Bitmap`]. A `(name, type)` pair is unique within a batch; a name seen with
/// two value types yields two columns (per-type splitting, exactly as the row
/// path splits). `cells.len()` equals `validity.count_present()`.
///
/// The cell that lands here is the *first* occurrence of the `(name, type)`
/// pair within a record, matching the row path's rule that the first occurrence
/// wins the column slot; later same-`(name, type)` occurrences within one
/// record go to [`ColumnarLogBatch::residual_attrs`].
#[derive(Clone, Debug, PartialEq)]
pub struct DynColumn {
    /// The attribute name.
    pub name: String,
    /// The resolved column type; `cells` is the variant of this type.
    pub field_type: FieldType,
    /// Dense present-row values, in row order.
    pub cells: DynCells,
    /// Presence over all `num_rows` rows.
    pub validity: Bitmap,
}

/// The dictionary shape of one Str/Bytes dynamic column (ADR-0109 decision 3,
/// mirroring a decoded RLOG dictionary string column, ADR-0099 decision 4):
/// a `distinct` value set and one `id` per PRESENT cell, parallel to the
/// column's [`DynColumn::cells`]. `ids[slot]` indexes `distinct`, and
/// `distinct[ids[slot]]` equals the cell's stored bytes
/// ([`DynCells::bytes_at`]), so the writer can map each distinct value to its
/// RLOG entry once per block instead of once per row. `distinct` may hold
/// entries no id references. The set order is the producer's; the writer sorts
/// it to match [`encode_strings`].
///
/// [`encode_strings`]: crate::encoding::encode_strings
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StrColumnDict {
    /// Distinct value bytes, in the producer's order.
    pub distinct: Vec<Vec<u8>>,
    /// One index into `distinct` per present cell, parallel to `DynColumn::cells`.
    pub ids: Vec<u32>,
}

/// A batch of log records in column-major form. Row `i` is assembled by reading
/// index `i` (or the `i`-th present slot, for optional columns) out of each
/// buffer. All per-row buffers describe the same `num_rows` rows.
#[derive(Clone, Debug, PartialEq)]
pub struct ColumnarLogBatch {
    /// Row count. Every mandatory per-row buffer has this length; every
    /// optional column's validity bitmap has this length.
    pub num_rows: usize,

    // Mandatory fixed columns, one entry per row.
    pub ts_ns: Vec<i64>,
    pub observed_ts_ns: Vec<i64>,
    pub severity_num: Vec<u8>,
    pub flags: Vec<u32>,

    // Variable-length text columns, one value per row (always present; an
    // absent severity_text or body is the empty string, matching the row path).
    pub severity_text: VarBytes,
    pub body: VarBytes,

    /// Packed 16-byte trace ids, dense over present rows, with a per-row
    /// presence bitmap.
    pub trace_id: Vec<u8>,
    pub trace_id_validity: Bitmap,
    /// Packed 8-byte span ids, dense over present rows, with a per-row presence
    /// bitmap.
    pub span_id: Vec<u8>,
    pub span_id_validity: Bitmap,

    /// Dense stream reference per row, indexing [`Self::stream_ids`] and
    /// [`Self::stream_attrs`].
    pub stream_refs: Vec<u32>,
    /// Distinct stream ids in `stream_ref` order.
    pub stream_ids: Vec<LogStreamId>,
    /// Distinct STREAM_DIR blobs, parallel to [`Self::stream_ids`]:
    /// `stream_attrs[r]` is the hash-preimage blob for `stream_ids[r]`.
    pub stream_attrs: Vec<Vec<u8>>,

    /// Dynamic attribute columns, one per distinct `(name, FieldType)`.
    pub dyn_columns: Vec<DynColumn>,

    /// Optional dictionary shape per dynamic column: when non-empty this has
    /// one entry per [`Self::dyn_columns`] entry, `Some` only for a Str/Bytes
    /// column whose distinct set and per-cell ids the producer supplies (a
    /// Parquet dictionary, #604). The writer then pays string encoding and
    /// token bloom per distinct value, not per row (ADR-0109 decision 3). An
    /// empty vector (the default) means no column carries a dictionary and the
    /// writer takes the plain per-cell path unchanged, so this field is purely
    /// additive: a producer built against the pre-#603 type leaves it empty.
    pub dyn_col_dicts: Vec<Option<StrColumnDict>>,

    /// Per-row duplicate-loser attributes: the second and later occurrences of
    /// a `(name, type)` pair within one record, which cannot occupy that row's
    /// single column cell. Empty for almost every row. The writer folds these
    /// into `attrs_raw` exactly as the row path folds a within-record duplicate.
    pub residual_attrs: Vec<Vec<(String, AttrValue)>>,
}

/// The [`FieldType`] a cell's column must have: the type half of the writer's
/// `resolve_value` (`record.rs`), where `List` and `Map` resolve to `Bytes`.
fn attr_field_type(value: &AttrValue) -> FieldType {
    match value {
        AttrValue::Str(_) => FieldType::Str,
        AttrValue::I64(_) => FieldType::I64,
        AttrValue::F64(_) => FieldType::F64,
        AttrValue::Bool(_) => FieldType::Bool,
        AttrValue::Bytes(_) | AttrValue::List(_) | AttrValue::Map(_) => FieldType::Bytes,
    }
}

impl ColumnarLogBatch {
    /// An empty batch describing zero rows.
    pub fn new() -> Self {
        ColumnarLogBatch {
            num_rows: 0,
            ts_ns: Vec::new(),
            observed_ts_ns: Vec::new(),
            severity_num: Vec::new(),
            flags: Vec::new(),
            severity_text: VarBytes::new(),
            body: VarBytes::new(),
            trace_id: Vec::new(),
            trace_id_validity: Bitmap::new(),
            span_id: Vec::new(),
            span_id_validity: Bitmap::new(),
            stream_refs: Vec::new(),
            stream_ids: Vec::new(),
            stream_attrs: Vec::new(),
            dyn_columns: Vec::new(),
            dyn_col_dicts: Vec::new(),
            residual_attrs: Vec::new(),
        }
    }

    /// Whether the batch has zero rows.
    pub fn is_empty(&self) -> bool {
        self.num_rows == 0
    }

    /// Checks the cross-field invariants the columnar build path
    /// (`build_object_columnar` and what it calls, `crates/ravel-logseg/src/writer.rs`)
    /// relies on when it indexes a batch's fields, so a malformed batch is
    /// refused here instead of making that path index out of range or write
    /// data under the wrong attribution. The conditions below, and only
    /// these, are enforced, each returning
    /// [`LogSegError::MalformedColumnarBatch`] naming the condition and the
    /// offending column name, row, or id.
    ///
    /// - `ts_ns`, `observed_ts_ns`, `severity_num`, `flags`, `severity_text`,
    ///   `body`, `trace_id_validity`, `span_id_validity`, `stream_refs`, and
    ///   `residual_attrs` each have exactly `num_rows` entries.
    /// - `trace_id`'s length is `trace_id_validity.count_present() * 16`, and
    ///   `span_id`'s is `span_id_validity.count_present() * 8`.
    /// - `stream_ids` and `stream_attrs` have the same length, and every id in
    ///   `stream_ids` is distinct (the field's own doc: "Distinct stream ids").
    /// - Every `stream_refs` entry is a valid index into `stream_ids`.
    /// - Every `dyn_columns` entry's `validity` describes `num_rows` rows, and
    ///   its `cells` has exactly as many entries as `validity` marks present.
    /// - Every dyn column's `cells` is the [`DynCells`] variant of its
    ///   `field_type`.
    /// - The offsets of `severity_text`, `body`, and every `Str`/`Bytes` dyn
    ///   column describe their bytes ([`VarBytes::offsets_error`]): a column
    ///   whose unchecked [`VarBytes::push`] wrapped them past
    ///   [`VAR_BYTES_MAX`] bytes is refused here, not indexed.
    /// - Every cell of a `Str` column is UTF-8.
    /// - A `Bytes` column's `nested` slots are strictly ascending and below its
    ///   cell count, each holds a `List` or `Map`, and that value's
    ///   `canonical_value_bytes` equals the cell's stored bytes.
    /// - `dyn_col_dicts`, when non-empty, has exactly one entry per
    ///   `dyn_columns` entry; a present (`Some`) entry's `ids` has exactly one
    ///   id per present cell in its column, and every id is a valid index into
    ///   that entry's own `distinct`.
    /// - For a present dictionary on a `Str` or `Bytes` column,
    ///   `distinct[ids[slot]]` equals every cell's stored bytes.
    ///
    /// Does not check:
    ///
    /// - Two `dyn_columns` entries with the same `(name, field_type)`: the
    ///   writer does not check this either, and nothing here compares column
    ///   identities.
    /// - The entries of a dictionary on an `I64`, `F64` or `Bool` column
    ///   against its cells. Its `ids` are checked for count and range like
    ///   any other dictionary's; the writer does not read such a dictionary.
    /// - `distinct` entries no id references, and the payload content of
    ///   `stream_attrs` and `residual_attrs`.
    ///
    /// The pass allocates only for a `nested` cell, whose canonical encoding
    /// it recomputes.
    pub fn validate(&self) -> Result<(), LogSegError> {
        let malformed = |message: String| Err(LogSegError::MalformedColumnarBatch(message));
        let n = self.num_rows;
        let per_row = [
            ("ts_ns", self.ts_ns.len()),
            ("observed_ts_ns", self.observed_ts_ns.len()),
            ("severity_num", self.severity_num.len()),
            ("flags", self.flags.len()),
            ("severity_text", self.severity_text.len()),
            ("body", self.body.len()),
            ("trace_id_validity", self.trace_id_validity.len()),
            ("span_id_validity", self.span_id_validity.len()),
            ("stream_refs", self.stream_refs.len()),
            ("residual_attrs", self.residual_attrs.len()),
        ];
        for (name, len) in per_row {
            if len != n {
                return malformed(format!("{name} has {len} entries but num_rows is {n}"));
            }
        }
        for (name, values) in [("severity_text", &self.severity_text), ("body", &self.body)] {
            if let Some(why) = values.offsets_error() {
                return malformed(format!("{name} offsets are malformed: {why}"));
            }
        }
        let trace_len = self.trace_id_validity.count_present() * 16;
        if self.trace_id.len() != trace_len {
            return malformed(format!(
                "trace_id holds {} bytes but {} present trace ids need {trace_len}",
                self.trace_id.len(),
                self.trace_id_validity.count_present(),
            ));
        }
        let span_len = self.span_id_validity.count_present() * 8;
        if self.span_id.len() != span_len {
            return malformed(format!(
                "span_id holds {} bytes but {} present span ids need {span_len}",
                self.span_id.len(),
                self.span_id_validity.count_present(),
            ));
        }
        let (ids, blobs) = (self.stream_ids.len(), self.stream_attrs.len());
        if blobs < ids {
            return malformed(format!(
                "stream {} (index {blobs}) has no stream_attrs blob: {ids} stream ids but {blobs} stream_attrs entries",
                self.stream_ids[blobs].to_hex(),
            ));
        }
        if blobs > ids {
            return malformed(format!(
                "stream_attrs entry at index {ids} has no stream id: {blobs} stream_attrs entries but {ids} stream ids"
            ));
        }
        // `stream_ids` must be distinct (field doc: "Distinct stream ids in
        // stream_ref order"): a repeated id, whether or not its blob also
        // repeats, breaks the local-ref -> id mapping every row's
        // `stream_refs` entry depends on. Left unchecked, a within-batch
        // repeat with a differing blob reaches the writer's cross-batch
        // directory merge and surfaces as `InconsistentStreamAttrs` (a stream
        // id collision), misclassifying malformed input as a hash collision.
        let mut seen_streams: std::collections::HashSet<LogStreamId> =
            std::collections::HashSet::with_capacity(ids);
        for (idx, sid) in self.stream_ids.iter().enumerate() {
            if !seen_streams.insert(*sid) {
                return malformed(format!(
                    "stream_ids[{idx}] repeats stream {}: stream_ids must be distinct",
                    sid.to_hex(),
                ));
            }
        }
        if let Some((row, r)) = self
            .stream_refs
            .iter()
            .enumerate()
            .find(|(_, r)| **r as usize >= ids)
        {
            return malformed(format!(
                "stream_refs[{row}] is {r} but the batch has {ids} stream ids"
            ));
        }

        let mut seen_dyn_columns: std::collections::HashSet<(&str, u8)> =
            std::collections::HashSet::with_capacity(self.dyn_columns.len());
        for (ci, c) in self.dyn_columns.iter().enumerate() {
            if !seen_dyn_columns.insert((c.name.as_str(), c.field_type.to_u8())) {
                return malformed(format!(
                    "dyn column {:?} (index {ci}) repeats an earlier column of the same name and \
                     type {:?}: two columns of one (name, type) would double-count presence and \
                     let the last one written win",
                    c.name, c.field_type,
                ));
            }
            if c.validity.len() != n {
                return malformed(format!(
                    "dyn column {:?} (index {ci}) validity describes {} rows but num_rows is {n}",
                    c.name,
                    c.validity.len(),
                ));
            }
            let present = c.validity.count_present();
            if c.cells.len() != present {
                return malformed(format!(
                    "dyn column {:?} (index {ci}) has {} cells but validity marks {present} rows present",
                    c.name,
                    c.cells.len(),
                ));
            }
            let found = c.cells.field_type();
            if found != c.field_type {
                return malformed(format!(
                    "dyn column {:?} (index {ci}) holds {found:?} cells but the column's field_type is {:?}",
                    c.name, c.field_type,
                ));
            }
            let var_bytes = match &c.cells {
                DynCells::Str(v) => Some(v),
                DynCells::Bytes(b) => Some(&b.values),
                DynCells::I64(_) | DynCells::F64(_) | DynCells::Bool(_) => None,
            };
            if let Some(why) = var_bytes.and_then(VarBytes::offsets_error) {
                return malformed(format!(
                    "dyn column {:?} (index {ci}) offsets are malformed: {why}",
                    c.name,
                ));
            }
            match &c.cells {
                DynCells::Str(v) => {
                    if let Some(cell) =
                        (0..v.len()).find(|&i| std::str::from_utf8(v.get(i)).is_err())
                    {
                        return malformed(format!(
                            "dyn column {:?} (index {ci}) cell {cell} is not UTF-8",
                            c.name,
                        ));
                    }
                }
                DynCells::Bytes(b) => {
                    let mut floor = 0usize;
                    for (slot, value) in &b.nested {
                        let slot = *slot as usize;
                        if slot < floor || slot >= present {
                            return malformed(format!(
                                "dyn column {:?} (index {ci}) nested slot {slot} is out of order or past its {present} cells",
                                c.name,
                            ));
                        }
                        floor = slot + 1;
                        if !matches!(value, AttrValue::List(_) | AttrValue::Map(_)) {
                            return malformed(format!(
                                "dyn column {:?} (index {ci}) nested slot {slot} holds a {:?} value, not a List or Map",
                                c.name,
                                attr_field_type(value),
                            ));
                        }
                        if canonical_value_bytes(value) != b.values.get(slot) {
                            return malformed(format!(
                                "dyn column {:?} (index {ci}) cell {slot} differs from its nested value's canonical bytes",
                                c.name,
                            ));
                        }
                    }
                }
                DynCells::I64(_) | DynCells::F64(_) | DynCells::Bool(_) => {}
            }
        }

        if !self.dyn_col_dicts.is_empty() {
            if self.dyn_col_dicts.len() != self.dyn_columns.len() {
                return malformed(format!(
                    "dyn_col_dicts has {} entries but dyn_columns has {}",
                    self.dyn_col_dicts.len(),
                    self.dyn_columns.len(),
                ));
            }
            for (ci, dict) in self.dyn_col_dicts.iter().enumerate() {
                let Some(dict) = dict else { continue };
                let c = &self.dyn_columns[ci];
                if dict.ids.len() != c.cells.len() {
                    return malformed(format!(
                        "dyn column {:?} (index {ci}) dictionary has {} ids but {} present cells",
                        c.name,
                        dict.ids.len(),
                        c.cells.len(),
                    ));
                }
                if let Some((slot, gid)) = dict
                    .ids
                    .iter()
                    .enumerate()
                    .find(|(_, id)| **id as usize >= dict.distinct.len())
                {
                    return malformed(format!(
                        "dyn column {:?} (index {ci}) dictionary id[{slot}] is {gid} but distinct has {} entries",
                        c.name,
                        dict.distinct.len(),
                    ));
                }
                for (slot, id) in dict.ids.iter().enumerate() {
                    let Some(cell_bytes) = c.cells.bytes_at(slot) else {
                        break;
                    };
                    if dict.distinct[*id as usize] != cell_bytes {
                        return malformed(format!(
                            "dyn column {:?} (index {ci}) dictionary id[{slot}] is {id} but distinct[{id}] differs from the cell's bytes",
                            c.name,
                        ));
                    }
                }
            }
        }

        Ok(())
    }

    /// The 16-byte trace id of the `slot`-th present trace-id row.
    pub fn trace_id_at(&self, slot: usize) -> &[u8] {
        &self.trace_id[slot * 16..slot * 16 + 16]
    }

    /// The 8-byte span id of the `slot`-th present span-id row.
    pub fn span_id_at(&self, slot: usize) -> &[u8] {
        &self.span_id[slot * 8..slot * 8 + 8]
    }

    /// The heap bytes this batch holds, measured: the summed capacity of every
    /// buffer it owns, nested ones included, each times its element size. The
    /// batch struct itself is not counted (it is inline wherever the batch
    /// lives). The bulk loader charges its memory budget this figure
    /// (ADR-2614 decision 5); unlike the shard's `est_columnar_bytes`, which
    /// models the row-major footprint the same records would have, it counts
    /// what the typed columns actually allocate.
    pub fn heap_bytes(&self) -> usize {
        let dyn_columns: usize = self
            .dyn_columns
            .iter()
            .map(|c| c.name.capacity() + c.cells.heap_bytes() + c.validity.heap_bytes())
            .sum();
        let dicts: usize = self
            .dyn_col_dicts
            .iter()
            .flatten()
            .map(|d| {
                vec_heap(&d.distinct)
                    + d.distinct.iter().map(Vec::capacity).sum::<usize>()
                    + vec_heap(&d.ids)
            })
            .sum();
        let residual: usize = self
            .residual_attrs
            .iter()
            .map(|row| vec_heap(row) + attr_pairs_heap(row))
            .sum();
        vec_heap(&self.ts_ns)
            + vec_heap(&self.observed_ts_ns)
            + vec_heap(&self.severity_num)
            + vec_heap(&self.flags)
            + self.severity_text.heap_bytes()
            + self.body.heap_bytes()
            + vec_heap(&self.trace_id)
            + self.trace_id_validity.heap_bytes()
            + vec_heap(&self.span_id)
            + self.span_id_validity.heap_bytes()
            + vec_heap(&self.stream_refs)
            + vec_heap(&self.stream_ids)
            + vec_heap(&self.stream_attrs)
            + self.stream_attrs.iter().map(Vec::capacity).sum::<usize>()
            + vec_heap(&self.dyn_columns)
            + dyn_columns
            + vec_heap(&self.dyn_col_dicts)
            + dicts
            + vec_heap(&self.residual_attrs)
            + residual
    }

    /// Builds a batch from a sequence of records, column by column. This is the
    /// bridge the writer-level differential test and the loader (#604) use to
    /// reach the columnar path with the same records the row path sees.
    ///
    /// It mirrors the row path's per-`(name, type)` first-occurrence column
    /// assignment (before the `max_dynamic_columns` budget, which is the
    /// writer's): the first occurrence of a `(name, type)` pair in a record
    /// takes that column's cell for the row; later same-pair occurrences within
    /// the same record become `residual_attrs`. It does not decide the budget,
    /// the stream directory ordering across batches, or the block layout -- all
    /// of which the writer owns.
    ///
    /// For callers whose values cannot reach a column's byte limit (tests and
    /// benches). A value that would take its column past [`VAR_BYTES_MAX`]
    /// bytes is not stored and its row stays marked present, so the batch
    /// fails [`Self::validate`]; [`Self::try_from_records`] returns that
    /// refusal instead.
    pub fn from_records(records: &[LogRecord]) -> Self {
        Self::build_from_records(records, VAR_BYTES_MAX).0
    }

    /// [`Self::from_records`], returning [`LogSegError::LimitExceeded`] naming
    /// the column when a value would take it past its byte limit.
    pub fn try_from_records(records: &[LogRecord]) -> Result<Self, LogSegError> {
        Self::try_from_records_with_limit(records, VAR_BYTES_MAX)
    }

    /// [`Self::try_from_records`] with each `Str`/`Bytes` column's byte limit
    /// lowered to `limit`, so a test reaches the refusal without holding
    /// `u32::MAX` bytes.
    fn try_from_records_with_limit(
        records: &[LogRecord],
        limit: usize,
    ) -> Result<Self, LogSegError> {
        match Self::build_from_records(records, limit) {
            (batch, None) => Ok(batch),
            (_, Some(refused)) => Err(refused),
        }
    }

    /// The batch [`Self::from_records`] builds, and the first push its byte
    /// limit refused, with the column named.
    fn build_from_records(records: &[LogRecord], limit: usize) -> (Self, Option<LogSegError>) {
        use std::collections::BTreeMap;

        let n = records.len();
        let mut batch = ColumnarLogBatch::new();
        batch.num_rows = n;

        // Distinct stream ids and their blobs. A BTreeMap, so it iterates in id
        // order; the binary search below depends on that order.
        let mut stream_blob: BTreeMap<LogStreamId, Vec<u8>> = BTreeMap::new();

        // Dynamic columns keyed by (name, type byte), each accumulating its
        // typed cells and its presence up to the last row it was seen in.
        let mut col_cells: BTreeMap<(String, u8), (DynCells, Bitmap)> = BTreeMap::new();

        batch.residual_attrs = vec![Vec::new(); n];
        let mut refused: Option<LogSegError> = None;

        for (row, r) in records.iter().enumerate() {
            batch.ts_ns.push(r.ts_ns);
            batch.observed_ts_ns.push(r.observed_ts_ns);
            batch.severity_num.push(r.severity_num);
            batch.flags.push(r.flags);
            batch.severity_text.push(r.severity_text.as_bytes());
            batch.body.push(r.body.as_bytes());

            match &r.trace_id {
                Some(t) => {
                    batch.trace_id.extend_from_slice(t);
                    batch.trace_id_validity.push(true);
                }
                None => batch.trace_id_validity.push(false),
            }
            match &r.span_id {
                Some(s) => {
                    batch.span_id.extend_from_slice(s);
                    batch.span_id_validity.push(true);
                }
                None => batch.span_id_validity.push(false),
            }

            stream_blob
                .entry(r.stream_id)
                .or_insert_with(|| r.stream_attrs.clone());

            // Placeholder stream ref filled after the id order is known.
            batch.stream_refs.push(0);

            // First occurrence of a (name, type) takes the column cell; a later
            // same-(name,type) occurrence in this record is a residual.
            let mut taken: std::collections::HashSet<(String, u8)> =
                std::collections::HashSet::new();
            for (k, v) in &r.attrs {
                let ty = attr_field_type(v);
                let key = (k.clone(), ty.to_u8());
                if taken.insert(key.clone()) {
                    let (cells, validity) = col_cells.entry(key).or_insert_with(|| {
                        let mut cells = DynCells::new(ty);
                        match &mut cells {
                            DynCells::Str(v) => *v = VarBytes::with_byte_limit(0, 0, limit),
                            DynCells::Bytes(b) => {
                                b.values = VarBytes::with_byte_limit(0, 0, limit);
                            }
                            DynCells::I64(_) | DynCells::F64(_) | DynCells::Bool(_) => {}
                        }
                        (cells, Bitmap::with_capacity(n))
                    });
                    // The column was created for this value's resolved type,
                    // so only the byte limit can refuse the push. The row is
                    // marked present either way: a refused cell leaves the
                    // column one cell short of its validity, which `validate`
                    // refuses, rather than silently dropping the value.
                    if let Err(e) = cells.push_value(v) {
                        let detail = match e {
                            LogSegError::LimitExceeded(detail) => detail,
                            other => other.to_string(),
                        };
                        refused.get_or_insert_with(|| {
                            LogSegError::LimitExceeded(format!("column {k:?}: {detail}"))
                        });
                    }
                    validity.pad_to(row);
                    validity.push(true);
                } else {
                    batch.residual_attrs[row].push((k.clone(), v.clone()));
                }
            }
        }

        // Stream directory: id-ascending (BTreeMap iteration order), dense ref.
        for (id, blob) in stream_blob {
            batch.stream_ids.push(id);
            batch.stream_attrs.push(blob);
        }
        // `batch.stream_ids` is ascending by construction, so each record's ref
        // is its stream id's position in it, found by binary search rather than
        // a hash lookup. A miss cannot happen: `stream_blob` above is built from
        // every record's own `r.stream_id` in this same `records` slice, so each
        // id resolved here was already inserted into it.
        for (row, r) in records.iter().enumerate() {
            batch.stream_refs[row] = batch
                .stream_ids
                .binary_search(&r.stream_id)
                .map(|i| i as u32)
                .unwrap_or(0);
        }

        // Materialize dynamic columns in (name, type) order.
        for ((name, _), (cells, mut validity)) in col_cells {
            validity.pad_to(n);
            batch.dyn_columns.push(DynColumn {
                name,
                field_type: cells.field_type(),
                cells,
                validity,
            });
        }

        (batch, refused)
    }

    /// Fills [`Self::dyn_col_dicts`] with the dictionary shape of every
    /// Str/Bytes dynamic column, derived from its stored cell bytes: the
    /// distinct value bytes in first-seen order plus one id per present cell.
    /// This is the bridge the writer-level differential test uses to drive the
    /// writer's dictionary fast path from records. Non-string columns get
    /// `None`. Idempotent in effect; overwrites any prior value.
    pub fn with_dictionaries(mut self) -> Self {
        let mut dicts: Vec<Option<StrColumnDict>> = Vec::with_capacity(self.dyn_columns.len());
        for c in &self.dyn_columns {
            if !matches!(c.cells, DynCells::Str(_) | DynCells::Bytes(_)) {
                dicts.push(None);
                continue;
            }
            let mut interner: HashMap<&[u8], u32> = HashMap::new();
            let mut distinct: Vec<Vec<u8>> = Vec::new();
            let mut ids: Vec<u32> = Vec::with_capacity(c.cells.len());
            for slot in 0..c.cells.len() {
                let bytes = c.cells.bytes_at(slot).unwrap_or_default();
                let next = distinct.len() as u32;
                let id = *interner.entry(bytes).or_insert_with(|| {
                    distinct.push(bytes.to_vec());
                    next
                });
                ids.push(id);
            }
            dicts.push(Some(StrColumnDict { distinct, ids }));
        }
        self.dyn_col_dicts = dicts;
        self
    }
}

impl Default for ColumnarLogBatch {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    use super::*;
    use crate::record::{resolve_value, stream_attrs_bytes};

    #[test]
    fn heap_bytes_sums_buffer_capacities_not_lengths() {
        let mut batch = ColumnarLogBatch::new();
        // `new` allocates only the two text columns' leading offset.
        assert_eq!(batch.heap_bytes(), 2 * size_of::<u32>());

        batch.ts_ns = Vec::with_capacity(10);
        batch.ts_ns.push(1);
        batch.body = VarBytes::with_capacity(4, 100);
        batch.stream_attrs = vec![Vec::with_capacity(7)];
        let cells = VarBytes::with_capacity(3, 33);
        let mut nested = BytesCells::new();
        nested.values = VarBytes::with_capacity(0, 0);
        nested.nested = vec![(0, AttrValue::Str(String::with_capacity(9)))];
        let nested_pairs = nested.nested.capacity() * size_of::<(u32, AttrValue)>();
        batch.dyn_columns = vec![
            DynColumn {
                name: String::with_capacity(5),
                field_type: FieldType::Str,
                cells: DynCells::Str(cells),
                validity: Bitmap::with_capacity(16),
            },
            DynColumn {
                name: String::new(),
                field_type: FieldType::Bytes,
                cells: DynCells::Bytes(nested),
                validity: Bitmap::new(),
            },
        ];
        let want = 2 * size_of::<u32>() // severity_text offsets, body replaced below
            - size_of::<u32>()
            + 10 * 8 // ts_ns: capacity 10, length 1
            + 5 * size_of::<u32>() + 100 // body: 5 offsets, 100 data
            + size_of::<Vec<u8>>() + 7 // stream_attrs
            + 2 * size_of::<DynColumn>()
            + 5 + 4 * size_of::<u32>() + 33 + 2 // Str column: name, offsets, data, bits
            + size_of::<u32>() // Bytes column offsets
            + nested_pairs
            + 9;
        assert_eq!(batch.heap_bytes(), want);
    }

    /// A distinct stream id per `n` (up to `u32::MAX` streams), with the
    /// ordering of `n` matching `LogStreamId`'s own `Ord` (big-endian bytes in
    /// the leading 4 of 16), so "sorted position" and "numeric order of `n`"
    /// are the same thing in these tests.
    fn wide_id(n: u32) -> LogStreamId {
        let mut a = [0u8; 16];
        a[0..4].copy_from_slice(&n.to_be_bytes());
        LogStreamId(a)
    }

    fn wide_attrs_blob(n: u32) -> Vec<u8> {
        stream_attrs_bytes(
            &[("service.name".into(), AttrValue::Str(format!("svc{n}")))],
            "scope",
            "1.0",
            &[("lib".into(), AttrValue::I64(i64::from(n)))],
        )
    }

    /// Asserts `result` is `Err(MalformedColumnarBatch)` and its message
    /// contains `needle`.
    fn assert_malformed(result: Result<(), LogSegError>, needle: &str) {
        match result {
            Err(LogSegError::MalformedColumnarBatch(msg)) => {
                assert!(
                    msg.contains(needle),
                    "message must contain {needle:?}: {msg}"
                );
            }
            other => panic!("expected Err(MalformedColumnarBatch), got {other:?}"),
        }
    }

    /// An otherwise-valid `n`-row batch: one stream, no dynamic columns, every
    /// per-row field at exactly length `n`, no trace/span ids present. A
    /// starting point for tests that break exactly one invariant at a time.
    fn minimal_batch(n: usize) -> ColumnarLogBatch {
        let mut batch = ColumnarLogBatch::new();
        batch.num_rows = n;
        batch.ts_ns = vec![0; n];
        batch.observed_ts_ns = vec![0; n];
        batch.severity_num = vec![0; n];
        batch.flags = vec![0; n];
        for _ in 0..n {
            batch.severity_text.push(b"");
            batch.body.push(b"");
            batch.trace_id_validity.push(false);
            batch.span_id_validity.push(false);
        }
        batch.stream_refs = vec![0; n];
        batch.stream_ids = vec![wide_id(0)];
        batch.stream_attrs = vec![wide_attrs_blob(0)];
        batch.residual_attrs = vec![Vec::new(); n];
        batch
    }

    fn wide_record(n: u32, ts_ns: i64) -> LogRecord {
        LogRecord {
            stream_id: wide_id(n),
            stream_attrs: wide_attrs_blob(n),
            ts_ns,
            observed_ts_ns: ts_ns,
            severity_num: 9,
            severity_text: "INFO".into(),
            body: "hello world".into(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: Vec::new(),
        }
    }

    /// `stream_refs` resolves each row's stream id to its position in the
    /// id-ascending stream directory, by binary search rather than the
    /// deleted `ref_of` hash map. Pushing 300 distinct streams in reverse
    /// (push order is the opposite of id order) and asserting each row's ref
    /// equals its stream id's numeric value (which is also its sorted
    /// position, by `wide_id`'s construction) would pass under a push-order
    /// ref just as easily as a sorted one if push order happened to match id
    /// order; reversing rules that out.
    #[test]
    fn stream_refs_equal_sorted_position_for_many_streams() {
        const STREAM_COUNT: u32 = 300;
        let records: Vec<LogRecord> = (0..STREAM_COUNT)
            .rev()
            .enumerate()
            .map(|(i, n)| wide_record(n, i as i64))
            .collect();

        let batch = ColumnarLogBatch::from_records(&records);
        assert_eq!(batch.stream_ids.len(), STREAM_COUNT as usize);
        assert!(
            batch.stream_ids.windows(2).all(|w| w[0] < w[1]),
            "stream directory must be id-ascending"
        );

        for (row, r) in records.iter().enumerate() {
            let local_ref = batch.stream_refs[row] as usize;
            assert_eq!(
                batch.stream_ids[local_ref], r.stream_id,
                "row {row} resolves to the wrong stream id"
            );
            // `wide_id` makes a stream's sorted position equal its own `n`.
            let n = u32::from_be_bytes([
                r.stream_id.0[0],
                r.stream_id.0[1],
                r.stream_id.0[2],
                r.stream_id.0[3],
            ]);
            assert_eq!(
                local_ref, n as usize,
                "row {row} (stream {n}) must resolve to its sorted position"
            );
        }
    }

    /// One row-length violation at a time: builds an otherwise-valid
    /// `N`-row batch, then makes exactly the named field one row short and,
    /// separately, one row long. Both directions are tried for each of the
    /// nine fields, so a check written as `len < n` or `len > n` passes
    /// neither half.
    #[test]
    fn each_per_row_field_length_is_checked_in_both_directions() {
        const N: usize = 3;
        fn bitmap(len: usize) -> Bitmap {
            let mut v = Bitmap::new();
            for _ in 0..len {
                v.push(false);
            }
            v
        }
        fn var_bytes(len: usize) -> VarBytes {
            let mut v = VarBytes::new();
            for _ in 0..len {
                v.push(b"");
            }
            v
        }
        let fields = [
            "ts_ns",
            "observed_ts_ns",
            "severity_num",
            "flags",
            "severity_text",
            "body",
            "trace_id_validity",
            "span_id_validity",
            "stream_refs",
        ];
        for field in fields {
            for len in [N - 1, N + 1] {
                let mut batch = minimal_batch(N);
                match field {
                    "ts_ns" => batch.ts_ns.resize(len, 0),
                    "observed_ts_ns" => batch.observed_ts_ns.resize(len, 0),
                    "severity_num" => batch.severity_num.resize(len, 0),
                    "flags" => batch.flags.resize(len, 0),
                    "severity_text" => batch.severity_text = var_bytes(len),
                    "body" => batch.body = var_bytes(len),
                    "trace_id_validity" => batch.trace_id_validity = bitmap(len),
                    "span_id_validity" => batch.span_id_validity = bitmap(len),
                    "stream_refs" => batch.stream_refs.resize(len, 0),
                    _ => unreachable!("every named field has a case above"),
                }
                assert_malformed(
                    batch.validate(),
                    &format!("{field} has {len} entries but num_rows is {N}"),
                );
            }
        }
    }

    #[test]
    fn residual_attrs_shorter_than_num_rows_is_rejected() {
        let mut batch = minimal_batch(2);
        batch.residual_attrs.truncate(1);
        assert_malformed(
            batch.validate(),
            "residual_attrs has 1 entries but num_rows is 2",
        );
    }

    #[test]
    fn residual_attrs_longer_than_num_rows_is_rejected() {
        let mut batch = minimal_batch(2);
        batch.residual_attrs.push(Vec::new());
        assert_malformed(
            batch.validate(),
            "residual_attrs has 3 entries but num_rows is 2",
        );
    }

    /// `trace_id` is packed 16 bytes per present row (field doc); a length
    /// that happens to equal `span_id`'s own stride (8) for a single present
    /// row must still be refused, not accepted by a validator that used the
    /// wrong field's multiplier.
    #[test]
    fn trace_id_length_matching_span_id_stride_is_rejected() {
        let mut batch = minimal_batch(1);
        batch.trace_id_validity = Bitmap::new();
        batch.trace_id_validity.push(true);
        batch.trace_id = vec![0u8; 8];
        assert_malformed(batch.validate(), "trace_id holds 8 bytes");
    }

    /// The mirror case: `span_id` is packed 8 bytes per present row; a
    /// length equal to `trace_id`'s 16-byte stride must still be refused.
    #[test]
    fn span_id_length_matching_trace_id_stride_is_rejected() {
        let mut batch = minimal_batch(1);
        batch.span_id_validity = Bitmap::new();
        batch.span_id_validity.push(true);
        batch.span_id = vec![0u8; 16];
        assert_malformed(batch.validate(), "span_id holds 16 bytes");
    }

    #[test]
    fn dyn_column_too_few_cells_for_validity_present_count_is_rejected() {
        let mut batch = minimal_batch(1);
        let mut validity = Bitmap::new();
        validity.push(true);
        batch.dyn_columns.push(DynColumn {
            name: "k".into(),
            field_type: FieldType::I64,
            cells: DynCells::I64(Vec::new()),
            validity,
        });
        assert_malformed(
            batch.validate(),
            "has 0 cells but validity marks 1 rows present",
        );
    }

    #[test]
    fn dyn_column_validity_len_not_num_rows_is_rejected() {
        let mut batch = minimal_batch(1);
        let mut validity = Bitmap::new();
        validity.push(true);
        validity.push(false);
        batch.dyn_columns.push(DynColumn {
            name: "k".into(),
            field_type: FieldType::I64,
            cells: DynCells::I64(vec![1]),
            validity,
        });
        assert_malformed(
            batch.validate(),
            "validity describes 2 rows but num_rows is 1",
        );
    }

    #[test]
    fn dyn_col_dicts_len_not_dyn_columns_len_is_rejected() {
        let mut batch = minimal_batch(1);
        let mut validity = Bitmap::new();
        validity.push(true);
        batch.dyn_columns.push(DynColumn {
            name: "k".into(),
            field_type: FieldType::Str,
            cells: str_cells(&["v"]),
            validity,
        });
        batch.dyn_col_dicts = vec![None, None];
        assert_malformed(
            batch.validate(),
            "dyn_col_dicts has 2 entries but dyn_columns has 1",
        );
    }

    #[test]
    fn dyn_col_dict_too_few_ids_for_present_cells_is_rejected() {
        let mut batch = minimal_batch(1);
        let mut validity = Bitmap::new();
        validity.push(true);
        batch.dyn_columns.push(DynColumn {
            name: "k".into(),
            field_type: FieldType::Str,
            cells: str_cells(&["v"]),
            validity,
        });
        batch.dyn_col_dicts = vec![Some(StrColumnDict {
            distinct: vec![b"v".to_vec()],
            ids: Vec::new(),
        })];
        assert_malformed(batch.validate(), "dictionary has 0 ids but 1 present cells");
    }

    /// The boundary case: `id == distinct.len()` is one past the end.
    /// Distinguishes the correct `>=` bound from a flawed `>` check that
    /// would let exactly this id through.
    #[test]
    fn dyn_col_dict_id_equal_to_distinct_len_is_rejected() {
        let mut batch = minimal_batch(1);
        let mut validity = Bitmap::new();
        validity.push(true);
        batch.dyn_columns.push(DynColumn {
            name: "k".into(),
            field_type: FieldType::Str,
            cells: str_cells(&["v"]),
            validity,
        });
        batch.dyn_col_dicts = vec![Some(StrColumnDict {
            distinct: vec![b"v".to_vec()],
            ids: vec![1],
        })];
        assert_malformed(
            batch.validate(),
            "id[0] is 1 but distinct has 1 entries",
        );
    }

    #[test]
    fn var_bytes_default_len_is_zero_not_panicking() {
        assert_eq!(VarBytes::default().len(), 0);
    }

    #[test]
    fn default_constructed_var_bytes_is_rejected_not_panicking() {
        let mut batch = minimal_batch(1);
        batch.severity_text = VarBytes::default();
        assert_malformed(
            batch.validate(),
            "severity_text has 0 entries but num_rows is 1",
        );
    }

    /// `Str` cells holding `values`, in order.
    fn str_cells(values: &[&str]) -> DynCells {
        let mut v = VarBytes::new();
        for s in values {
            v.push(s.as_bytes());
        }
        DynCells::Str(v)
    }

    /// A one-row batch with one dynamic column over a single present cell. The
    /// cell is stored in the variant of its own resolved type, so a
    /// `field_type` naming another type builds a mis-typed column.
    fn one_cell_batch(field_type: FieldType, cell: AttrValue) -> ColumnarLogBatch {
        let mut batch = minimal_batch(1);
        let mut validity = Bitmap::new();
        validity.push(true);
        let cells =
            DynCells::from_values(attr_field_type(&cell), &[cell]).expect("cell of its own type");
        batch.dyn_columns.push(DynColumn {
            name: "k".into(),
            field_type,
            cells,
            validity,
        });
        batch
    }

    #[test]
    fn well_formed_columns_of_every_type_pass() {
        for (ty, cell) in [
            (FieldType::Str, AttrValue::Str("v".into())),
            (FieldType::I64, AttrValue::I64(1)),
            (FieldType::F64, AttrValue::F64(1.5)),
            (FieldType::Bool, AttrValue::Bool(true)),
            (FieldType::Bytes, AttrValue::Bytes(vec![1])),
            (FieldType::Bytes, AttrValue::List(vec![AttrValue::I64(1)])),
            (
                FieldType::Bytes,
                AttrValue::Map(vec![("a".into(), AttrValue::I64(1))]),
            ),
        ] {
            one_cell_batch(ty, cell).validate().expect("well-formed");
        }
    }

    /// A Bool column whose cells are stored as `I64`: the writer would write
    /// the column under the wrong type.
    #[test]
    fn bool_column_with_i64_cells_is_rejected() {
        let mut batch = minimal_batch(2);
        let mut validity = Bitmap::new();
        validity.push(true);
        validity.push(true);
        batch.dyn_columns.push(DynColumn {
            name: "flag".into(),
            field_type: FieldType::Bool,
            cells: DynCells::I64(vec![1, 2]),
            validity,
        });
        assert_malformed(
            batch.validate(),
            "dyn column \"flag\" (index 0) holds I64 cells but the column's field_type is Bool",
        );
    }

    #[test]
    fn i64_column_with_str_cells_is_rejected() {
        let batch = one_cell_batch(FieldType::I64, AttrValue::Str("x".into()));
        assert_malformed(
            batch.validate(),
            "dyn column \"k\" (index 0) holds Str cells but the column's field_type is I64",
        );
    }

    #[test]
    fn str_column_with_bytes_cells_is_rejected() {
        let batch = one_cell_batch(FieldType::Str, AttrValue::List(Vec::new()));
        assert_malformed(
            batch.validate(),
            "holds Bytes cells but the column's field_type is Str",
        );
    }

    #[test]
    fn push_value_refuses_a_value_of_another_type() {
        let mut cells = DynCells::new(FieldType::Bool);
        match cells.push_value(&AttrValue::I64(1)) {
            Err(LogSegError::MalformedColumnarBatch(msg)) => assert_eq!(
                msg, "a I64 value cannot be a cell of a Bool column",
                "refusal names both types"
            ),
            other => panic!("expected a refusal, got {other:?}"),
        }
        assert_eq!(cells.len(), 0, "a refused value is not stored");
        let mut str_cells = DynCells::new(FieldType::Str);
        assert!(str_cells.push_value(&AttrValue::List(Vec::new())).is_err());
        assert!(
            DynCells::new(FieldType::Bytes)
                .push_from(&DynCells::I64(vec![1]), 0)
                .is_err(),
            "push_from refuses a source of another type"
        );
    }

    #[test]
    fn str_cell_that_is_not_utf8_is_rejected() {
        let mut batch = minimal_batch(1);
        let mut validity = Bitmap::new();
        validity.push(true);
        let mut v = VarBytes::new();
        v.push(&[0xff, 0xfe]);
        batch.dyn_columns.push(DynColumn {
            name: "k".into(),
            field_type: FieldType::Str,
            cells: DynCells::Str(v),
            validity,
        });
        assert_malformed(
            batch.validate(),
            "dyn column \"k\" (index 0) cell 0 is not UTF-8",
        );
    }

    /// Asserts `result` is `Err(LimitExceeded)` naming the column's limit.
    fn assert_limit_exceeded(result: Result<(), LogSegError>, limit: usize) {
        match result {
            Err(LogSegError::LimitExceeded(msg)) => assert!(
                msg.contains(&format!("past its limit of {limit} bytes")),
                "refusal names the limit: {msg}"
            ),
            other => panic!("expected Err(LimitExceeded), got {other:?}"),
        }
    }

    #[test]
    fn try_push_refuses_a_value_past_the_byte_limit_and_stores_nothing() {
        let mut v = VarBytes::with_byte_limit(0, 0, 8);
        v.try_push(b"abcd").expect("4 of 8 bytes");
        v.try_push(b"efgh").expect("exactly 8 of 8 bytes");
        assert_limit_exceeded(v.try_push(b"i"), 8);
        assert_eq!(v.len(), 2, "the refused value adds no offset");
        assert_eq!(v.data(), b"abcdefgh", "the refused value adds no bytes");
        assert_eq!(v.offsets(), &[0, 4, 8]);
        v.try_push(b"").expect("an empty value adds no bytes");
        assert_eq!(v.len(), 3);
        assert_eq!(
            VarBytes::with_byte_limit(0, 0, usize::MAX).limit,
            VAR_BYTES_MAX,
            "a limit above what u32 offsets address is lowered to it"
        );
    }

    /// `try_from_records` returns the byte-limit refusal of a dynamic cell,
    /// naming the column, where `from_records` leaves a batch `validate`
    /// refuses.
    #[test]
    fn try_from_records_names_the_column_a_value_overflows() {
        let mut first = wide_record(0, 1);
        first.attrs = vec![("url".into(), AttrValue::Str("abcd".into()))];
        let mut second = wide_record(0, 2);
        second.attrs = vec![("url".into(), AttrValue::Str("e".into()))];
        let records = [first, second];

        ColumnarLogBatch::try_from_records_with_limit(&records[..1], 4).expect("4 of 4 bytes");
        match ColumnarLogBatch::try_from_records_with_limit(&records, 4) {
            Err(LogSegError::LimitExceeded(msg)) => assert!(
                msg.starts_with("column \"url\": ") && msg.contains("past its limit of 4 bytes"),
                "the refusal names the column and the limit: {msg}"
            ),
            other => panic!("expected Err(LimitExceeded), got {other:?}"),
        }
        let unchecked = ColumnarLogBatch::build_from_records(&records, 4).0;
        assert!(
            unchecked.validate().is_err(),
            "the batch from_records would return fails validate"
        );
    }

    #[test]
    fn dyn_cell_pushes_refuse_a_cell_past_the_byte_limit() {
        let mut s = DynCells::Str(VarBytes::with_byte_limit(0, 0, 3));
        s.push_value(&AttrValue::Str("abc".into()))
            .expect("3 of 3 bytes");
        assert_limit_exceeded(s.push_value(&AttrValue::Str("d".into())), 3);
        assert_eq!(s.len(), 1, "a refused Str value is not stored");

        let src = str_cells(&["abc", "d"]);
        let mut d = DynCells::Str(VarBytes::with_byte_limit(0, 0, 3));
        d.push_from(&src, 0).expect("3 of 3 bytes");
        assert_limit_exceeded(d.push_from(&src, 1), 3);
        assert_eq!(d.len(), 1, "a refused Str cell is not copied");

        let list = AttrValue::List(vec![AttrValue::I64(1)]);
        let canon_len = canonical_value_bytes(&list).len();
        let small = || {
            DynCells::Bytes(BytesCells {
                values: VarBytes::with_byte_limit(0, 0, canon_len - 1),
                nested: Vec::new(),
            })
        };
        let mut b = small();
        assert_limit_exceeded(b.push_value(&list), canon_len - 1);
        let DynCells::Bytes(cells) = &b else {
            panic!("a Bytes column")
        };
        assert_eq!(cells.values.len(), 0, "a refused List value is not stored");
        assert!(cells.nested.is_empty(), "nor is its nested original");

        let src = DynCells::from_values(FieldType::Bytes, std::slice::from_ref(&list))
            .expect("a List resolves to Bytes");
        let mut b = small();
        assert_limit_exceeded(b.push_from(&src, 0), canon_len - 1);
        let DynCells::Bytes(cells) = &b else {
            panic!("a Bytes column")
        };
        assert_eq!(cells.values.len(), 0, "a refused nested cell is not copied");
        assert!(cells.nested.is_empty(), "nor is its nested original");
    }

    /// Two values whose offsets go back from 5 to 3, the shape a `u32` wrap
    /// leaves: value 1 would be `data[5..3]`.
    fn non_monotonic() -> VarBytes {
        VarBytes {
            offsets: vec![0, 5, 3],
            data: b"abc".to_vec(),
            limit: VAR_BYTES_MAX,
        }
    }

    #[test]
    fn validate_refuses_non_monotonic_offsets_in_every_var_bytes_column() {
        let want = "offsets are malformed: offset 2 is 3 but offset 1 before it is 5";

        let mut batch = minimal_batch(2);
        batch.body = non_monotonic();
        assert_malformed(batch.validate(), &format!("body {want}"));

        let mut batch = minimal_batch(2);
        batch.severity_text = non_monotonic();
        assert_malformed(batch.validate(), &format!("severity_text {want}"));

        for cells in [
            DynCells::Str(non_monotonic()),
            DynCells::Bytes(BytesCells {
                values: non_monotonic(),
                nested: Vec::new(),
            }),
        ] {
            let mut batch = minimal_batch(2);
            let mut validity = Bitmap::new();
            validity.push(true);
            validity.push(true);
            batch.dyn_columns.push(DynColumn {
                name: "k".into(),
                field_type: cells.field_type(),
                cells,
                validity,
            });
            assert_malformed(
                batch.validate(),
                &format!("dyn column \"k\" (index 0) {want}"),
            );
        }
    }

    #[test]
    fn validate_refuses_offsets_that_do_not_start_at_zero_or_end_at_the_data() {
        let mut batch = minimal_batch(1);
        batch.body = VarBytes {
            offsets: vec![1, 3],
            data: b"abc".to_vec(),
            limit: VAR_BYTES_MAX,
        };
        assert_malformed(
            batch.validate(),
            "body offsets are malformed: its first offset is 1, not 0",
        );
        let mut batch = minimal_batch(1);
        batch.body = VarBytes {
            offsets: vec![0, 2],
            data: b"abc".to_vec(),
            limit: VAR_BYTES_MAX,
        };
        assert_malformed(
            batch.validate(),
            "body offsets are malformed: its last offset is 2 but it holds 3 bytes",
        );
    }

    /// A one-row `Bytes` column whose single cell stores `stored` with
    /// `nested` as its side list.
    fn nested_batch(stored: &[u8], nested: Vec<(u32, AttrValue)>) -> ColumnarLogBatch {
        let mut batch = minimal_batch(1);
        let mut validity = Bitmap::new();
        validity.push(true);
        let mut values = VarBytes::new();
        values.push(stored);
        batch.dyn_columns.push(DynColumn {
            name: "k".into(),
            field_type: FieldType::Bytes,
            cells: DynCells::Bytes(BytesCells { values, nested }),
            validity,
        });
        batch
    }

    #[test]
    fn nested_cells_are_checked_against_their_stored_bytes() {
        let list = AttrValue::List(vec![AttrValue::I64(1)]);
        let canon = canonical_value_bytes(&list);
        nested_batch(&canon, vec![(0, list.clone())])
            .validate()
            .expect("canonical bytes match");
        assert_malformed(
            nested_batch(b"x", vec![(0, list.clone())]).validate(),
            "cell 0 differs from its nested value's canonical bytes",
        );
        assert_malformed(
            nested_batch(&canon, vec![(1, list.clone())]).validate(),
            "nested slot 1 is out of order or past its 1 cells",
        );
        assert_malformed(
            nested_batch(&canon, vec![(0, list.clone()), (0, list)]).validate(),
            "nested slot 0 is out of order",
        );
        assert_malformed(
            nested_batch(b"x", vec![(0, AttrValue::Bytes(b"x".to_vec()))]).validate(),
            "nested slot 0 holds a Bytes value, not a List or Map",
        );
    }

    /// Every cell round-trips to the value the row path holds: the original
    /// `List`/`Map` for a nested cell, the bit pattern for a float.
    #[test]
    fn cells_round_trip_to_their_original_values() {
        let values = [
            AttrValue::Bytes(vec![1, 2]),
            AttrValue::Map(vec![("a".into(), AttrValue::F64(-0.0))]),
            AttrValue::Bytes(Vec::new()),
            AttrValue::List(vec![AttrValue::Str(String::new())]),
        ];
        let cells = DynCells::from_values(FieldType::Bytes, &values).expect("bytes cells");
        for (slot, want) in values.iter().enumerate() {
            let got = cells.value(slot).expect("value");
            assert!(attr_bits_eq(&got, want), "slot {slot}: {got:?} vs {want:?}");
            assert_eq!(
                cells.column_value(slot),
                resolve_value(want).1,
                "slot {slot} resolves as the row path resolves it"
            );
        }
        let floats = [f64::NAN, -0.0, 0.0, f64::from_bits(0x7ff8_0000_0000_0001)];
        let fv: Vec<AttrValue> = floats.iter().map(|f| AttrValue::F64(*f)).collect();
        let cells = DynCells::from_values(FieldType::F64, &fv).expect("f64 cells");
        for (slot, f) in floats.iter().enumerate() {
            assert_eq!(cells.column_value(slot), ColumnValue::F64(f.to_bits()));
        }
    }

    /// Float cells compare by bit pattern: NaN equals itself and `-0.0` does
    /// not equal `0.0`.
    #[test]
    fn float_cells_compare_by_bits() {
        assert_eq!(DynCells::F64(vec![f64::NAN]), DynCells::F64(vec![f64::NAN]));
        assert_ne!(DynCells::F64(vec![-0.0]), DynCells::F64(vec![0.0]));
        let nan_list = |f: f64| {
            DynCells::from_values(
                FieldType::Bytes,
                &[AttrValue::List(vec![AttrValue::F64(f)])],
            )
            .expect("bytes cells")
        };
        assert_eq!(nan_list(f64::NAN), nan_list(f64::NAN));
        assert_ne!(nan_list(-0.0), nan_list(0.0));
    }

    fn with_dict(
        mut batch: ColumnarLogBatch,
        distinct: Vec<&[u8]>,
        ids: Vec<u32>,
    ) -> ColumnarLogBatch {
        batch.dyn_col_dicts = vec![Some(StrColumnDict {
            distinct: distinct.into_iter().map(<[u8]>::to_vec).collect(),
            ids,
        })];
        batch
    }

    /// The dictionary names a value other than its cell's: the value page
    /// would hold "other" while postings and stats are computed from "v".
    #[test]
    fn dictionary_entry_differing_from_its_cell_is_rejected() {
        let batch = with_dict(
            one_cell_batch(FieldType::Str, AttrValue::Str("v".into())),
            vec![b"other"],
            vec![0],
        );
        assert_malformed(
            batch.validate(),
            "dyn column \"k\" (index 0) dictionary id[0] is 0 but distinct[0] differs from the cell's bytes",
        );
        let batch = with_dict(
            one_cell_batch(FieldType::Bytes, AttrValue::Bytes(vec![1, 2])),
            vec![&[1, 3]],
            vec![0],
        );
        assert_malformed(batch.validate(), "differs from the cell's bytes");
    }

    #[test]
    fn matching_dictionary_passes_and_list_cells_are_compared() {
        with_dict(
            one_cell_batch(FieldType::Str, AttrValue::Str("v".into())),
            vec![b"v", b"unused"],
            vec![0],
        )
        .validate()
        .expect("matching dictionary");
        let list = AttrValue::List(Vec::new());
        let canon = canonical_value_bytes(&list);
        with_dict(
            one_cell_batch(FieldType::Bytes, list.clone()),
            vec![&canon],
            vec![0],
        )
        .validate()
        .expect("a List cell's dictionary entry is its canonical bytes");
        assert_malformed(
            with_dict(
                one_cell_batch(FieldType::Bytes, list),
                vec![b"whatever"],
                vec![0],
            )
            .validate(),
            "differs from the cell's bytes",
        );
    }

    #[test]
    fn dictionary_with_more_ids_than_cells_is_rejected() {
        let batch = with_dict(
            one_cell_batch(FieldType::Str, AttrValue::Str("v".into())),
            vec![b"v"],
            vec![0, 0],
        );
        assert_malformed(batch.validate(), "dictionary has 2 ids but 1 present cells");
    }

    #[test]
    fn fewer_dictionaries_than_columns_is_rejected() {
        let mut batch = one_cell_batch(FieldType::Str, AttrValue::Str("v".into()));
        batch.dyn_columns.push(batch.dyn_columns[0].clone());
        batch.dyn_col_dicts = vec![None];
        assert_malformed(
            batch.validate(),
            "dyn_col_dicts has 1 entries but dyn_columns has 2",
        );
    }

    #[test]
    fn dyn_column_with_more_cells_than_present_bits_is_rejected() {
        let mut batch = one_cell_batch(FieldType::I64, AttrValue::I64(1));
        batch.dyn_columns[0]
            .cells
            .push_value(&AttrValue::I64(2))
            .expect("an I64 cell");
        assert_malformed(
            batch.validate(),
            "has 2 cells but validity marks 1 rows present",
        );
    }

    #[test]
    fn dyn_column_validity_shorter_than_num_rows_is_rejected() {
        let mut batch = minimal_batch(2);
        let mut validity = Bitmap::new();
        validity.push(true);
        batch.dyn_columns.push(DynColumn {
            name: "k".into(),
            field_type: FieldType::I64,
            cells: DynCells::I64(vec![1]),
            validity,
        });
        assert_malformed(
            batch.validate(),
            "validity describes 1 rows but num_rows is 2",
        );
    }

    #[test]
    fn duplicate_stream_id_with_identical_blobs_is_rejected() {
        let mut batch = minimal_batch(1);
        batch.stream_ids = vec![wide_id(0), wide_id(0)];
        batch.stream_attrs = vec![wide_attrs_blob(0), wide_attrs_blob(0)];
        assert_malformed(batch.validate(), "stream_ids[1] repeats stream");
    }

    /// Trace ids are 16 bytes per present row: one byte too few and one too
    /// many must each be refused, so a comparison with the wrong direction
    /// fails one of the two.
    #[test]
    fn trace_id_one_byte_off_either_way_is_rejected() {
        for bytes in [15usize, 17] {
            let mut batch = minimal_batch(1);
            batch.trace_id_validity = Bitmap::new();
            batch.trace_id_validity.push(true);
            batch.trace_id = vec![0u8; bytes];
            assert_malformed(batch.validate(), &format!("trace_id holds {bytes} bytes"));
        }
    }

    #[test]
    fn span_id_one_byte_off_either_way_is_rejected() {
        for bytes in [7usize, 9] {
            let mut batch = minimal_batch(1);
            batch.span_id_validity = Bitmap::new();
            batch.span_id_validity.push(true);
            batch.span_id = vec![0u8; bytes];
            assert_malformed(batch.validate(), &format!("span_id holds {bytes} bytes"));
        }
    }

    /// Two dynamic columns sharing one (name, type) pair: unrefused, the
    /// writer's last-value-wins merge would double FIELD_DIR's present count
    /// against one stored value.
    #[test]
    fn two_dyn_columns_of_the_same_name_and_type_are_rejected() {
        let mut batch = minimal_batch(2);
        let mut first_validity = Bitmap::new();
        first_validity.push(true);
        first_validity.push(false);
        batch.dyn_columns.push(DynColumn {
            name: "k".into(),
            field_type: FieldType::I64,
            cells: DynCells::I64(vec![1]),
            validity: first_validity,
        });
        let mut second_validity = Bitmap::new();
        second_validity.push(false);
        second_validity.push(true);
        batch.dyn_columns.push(DynColumn {
            name: "k".into(),
            field_type: FieldType::I64,
            cells: DynCells::I64(vec![2]),
            validity: second_validity,
        });
        assert_malformed(
            batch.validate(),
            "dyn column \"k\" (index 1) repeats an earlier column of the same name and type I64",
        );
    }
}
