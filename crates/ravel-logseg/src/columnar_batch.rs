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
//! ## Dynamic attribute cells carry the original [`AttrValue`]
//!
//! A dynamic column stores one [`AttrValue`] per present cell rather than a
//! pre-resolved typed buffer. The reason is byte-identity: when a `List`/`Map`
//! value (which [`resolve_value`] canonicalizes into a `Bytes` column) or a
//! value past the `max_dynamic_columns` budget folds into `attrs_raw`, the row
//! path canonicalizes the *original* attribute, and the canonical bytes of
//! `List`/`Map` differ from the canonical bytes of the `Bytes` column value it
//! resolves to. Keeping the original attribute makes the writer's `attrs_raw`
//! reproduction exact. Pre-resolved typed value buffers (`Vec<i64>` and the
//! like) are a follow-up performance refinement (#603 covers the dictionary
//! shape); this task builds the plain-value, correctness-anchoring form.

use ravel_types::logstream::{AttrValue, LogStreamId};

use crate::record::{FieldType, LogRecord, resolve_value};

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
}

/// A variable-length byte column: contiguous `data` with `offsets` marking each
/// value's end. `offsets` has one more entry than there are values; value `i`
/// is `data[offsets[i]..offsets[i + 1]]`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VarBytes {
    offsets: Vec<u32>,
    data: Vec<u8>,
}

impl VarBytes {
    /// An empty column with the leading `0` offset in place.
    pub fn new() -> Self {
        VarBytes {
            offsets: vec![0],
            data: Vec::new(),
        }
    }

    /// Appends one value.
    pub fn push(&mut self, value: &[u8]) {
        self.data.extend_from_slice(value);
        self.offsets.push(self.data.len() as u32);
    }

    /// Number of values stored.
    pub fn len(&self) -> usize {
        self.offsets.len() - 1
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
}

/// One dynamic attribute column: an attribute `name` observed with one resolved
/// [`FieldType`], its dense per-present-row [`AttrValue`] cells, and a per-row
/// presence [`Bitmap`]. A `(name, type)` pair is unique within a batch; a name
/// seen with two value types yields two columns (per-type splitting, exactly as
/// the row path splits). `cells.len()` equals `validity.count_present()`.
///
/// The cell that lands here is the *first* occurrence of the `(name, type)`
/// pair within a record, matching the row path's rule that the first occurrence
/// wins the column slot; later same-`(name, type)` occurrences within one
/// record go to [`ColumnarLogBatch::residual_attrs`].
#[derive(Clone, Debug, PartialEq)]
pub struct DynColumn {
    /// The attribute name.
    pub name: String,
    /// The resolved column type (`resolve_value(cell).0` for every cell).
    pub field_type: FieldType,
    /// Dense present-row values, in row order.
    pub cells: Vec<AttrValue>,
    /// Presence over all `num_rows` rows.
    pub validity: Bitmap,
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

    /// Per-row duplicate-loser attributes: the second and later occurrences of
    /// a `(name, type)` pair within one record, which cannot occupy that row's
    /// single column cell. Empty for almost every row. The writer folds these
    /// into `attrs_raw` exactly as the row path folds a within-record duplicate.
    pub residual_attrs: Vec<Vec<(String, AttrValue)>>,
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
            residual_attrs: Vec::new(),
        }
    }

    /// Whether the batch has zero rows.
    pub fn is_empty(&self) -> bool {
        self.num_rows == 0
    }

    /// The 16-byte trace id of the `slot`-th present trace-id row.
    pub fn trace_id_at(&self, slot: usize) -> &[u8] {
        &self.trace_id[slot * 16..slot * 16 + 16]
    }

    /// The 8-byte span id of the `slot`-th present span-id row.
    pub fn span_id_at(&self, slot: usize) -> &[u8] {
        &self.span_id[slot * 8..slot * 8 + 8]
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
    pub fn from_records(records: &[LogRecord]) -> Self {
        use std::collections::BTreeMap;

        let n = records.len();
        let mut batch = ColumnarLogBatch::new();
        batch.num_rows = n;

        // Distinct stream ids in first-seen order mapped to a dense local ref;
        // re-sorted to id order at the end so `stream_ids` is ascending.
        let mut stream_blob: BTreeMap<LogStreamId, Vec<u8>> = BTreeMap::new();

        // Dynamic columns keyed by (name, type byte), each accumulating a value
        // per row (None when absent).
        let mut col_cells: BTreeMap<(String, u8), Vec<Option<AttrValue>>> = BTreeMap::new();

        batch.residual_attrs = vec![Vec::new(); n];

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
                let (ty, _) = resolve_value(v);
                let key = (k.clone(), ty.to_u8());
                if taken.insert(key.clone()) {
                    let col = col_cells.entry(key).or_insert_with(|| vec![None; n]);
                    col[row] = Some(v.clone());
                } else {
                    batch.residual_attrs[row].push((k.clone(), v.clone()));
                }
            }
        }

        // Stream directory: id-ascending (BTreeMap iteration order), dense ref.
        let mut ref_of: std::collections::HashMap<LogStreamId, u32> =
            std::collections::HashMap::with_capacity(stream_blob.len());
        for (i, (id, blob)) in stream_blob.into_iter().enumerate() {
            ref_of.insert(id, i as u32);
            batch.stream_ids.push(id);
            batch.stream_attrs.push(blob);
        }
        for (row, r) in records.iter().enumerate() {
            batch.stream_refs[row] = ref_of[&r.stream_id];
        }

        // Materialize dynamic columns in (name, type) order.
        for ((name, ty_byte), cells) in col_cells {
            let field_type = FieldType::from_u8(ty_byte).unwrap_or(FieldType::Bytes);
            let mut validity = Bitmap::new();
            let mut dense = Vec::new();
            for cell in cells {
                match cell {
                    Some(v) => {
                        validity.push(true);
                        dense.push(v);
                    }
                    None => validity.push(false),
                }
            }
            batch.dyn_columns.push(DynColumn {
                name,
                field_type,
                cells: dense,
                validity,
            });
        }

        batch
    }
}

impl Default for ColumnarLogBatch {
    fn default() -> Self {
        Self::new()
    }
}
