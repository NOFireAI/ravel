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

use std::collections::HashMap;

use ravel_types::logstream::{AttrValue, LogStreamId};

use crate::error::LogSegError;
use crate::record::{ColumnValue, FieldType, LogRecord, resolve_value};

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

/// The dictionary shape of one Str/Bytes dynamic column (ADR-0109 decision 3,
/// mirroring a decoded RLOG dictionary string column, ADR-0099 decision 4):
/// a `distinct` value set and one `id` per PRESENT cell, parallel to the
/// column's [`DynColumn::cells`]. `ids[slot]` indexes `distinct`, and
/// `distinct[ids[slot]]` equals `resolve_value(&cells[slot]).1`'s bytes, so the
/// writer can map each distinct value to its RLOG entry once per block instead
/// of once per row. `distinct` may hold entries no id references. The set order
/// is the producer's; the writer sorts it to match [`encode_strings`].
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

    /// Checks every cross-field invariant the columnar build path
    /// (`build_object_columnar` and what it calls, `crates/ravel-logseg/src/writer.rs`)
    /// relies on when it indexes a batch's fields, so a malformed batch is
    /// refused here instead of making that path index out of range or
    /// misattribute a row's data. Exactly the following are enforced, each
    /// returning [`LogSegError::MalformedColumnarBatch`] naming the condition
    /// and the offending column name, row index, or id (ids and slot indexes
    /// in hex):
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
    /// - `dyn_col_dicts`, when non-empty, has exactly one entry per
    ///   `dyn_columns` entry; a present (`Some`) entry's `ids` has exactly one
    ///   id per present cell in its column, and every id is a valid index into
    ///   that entry's own `distinct`.
    ///
    /// Does not check (the writer's own concern, not a precondition the index
    /// math above depends on): `dyn_columns` name/type uniqueness,
    /// `stream_attrs`/`residual_attrs`/`AttrValue` payload content, or
    /// `dyn_col_dicts` entries unreferenced by any id.
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
                    "stream_ids[{idx:#x}] repeats stream {}: stream_ids must be distinct",
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
                "stream_refs[{row}] is {r:#x} but the batch has {ids} stream ids"
            ));
        }

        for (ci, c) in self.dyn_columns.iter().enumerate() {
            if c.validity.len() != n {
                return malformed(format!(
                    "dyn column {:?} (index {ci:#x}) validity describes {} rows but num_rows is {n}",
                    c.name,
                    c.validity.len(),
                ));
            }
            let present = c.validity.count_present();
            if c.cells.len() != present {
                return malformed(format!(
                    "dyn column {:?} (index {ci:#x}) has {} cells but validity marks {present} rows present",
                    c.name,
                    c.cells.len(),
                ));
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
                        "dyn column {:?} (index {ci:#x}) dictionary has {} ids but {} present cells",
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
                        "dyn column {:?} (index {ci:#x}) dictionary id[{slot:#x}] is {gid:#x} but distinct has {} entries",
                        c.name,
                        dict.distinct.len(),
                    ));
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

        // Distinct stream ids and their blobs. A BTreeMap, so it iterates in id
        // order; the binary search below depends on that order.
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

    /// Fills [`Self::dyn_col_dicts`] with the dictionary shape of every
    /// Str/Bytes dynamic column, derived from its plain `cells`: the distinct
    /// value bytes in first-seen order plus one id per present cell. This is
    /// the bridge the writer-level differential test uses to drive the writer's
    /// dictionary fast path from records; a producer that already holds Parquet
    /// dictionaries (#604) supplies the shape directly instead. Non-string
    /// columns get `None`. Idempotent in effect; overwrites any prior value.
    pub fn with_dictionaries(mut self) -> Self {
        let mut dicts: Vec<Option<StrColumnDict>> = Vec::with_capacity(self.dyn_columns.len());
        for c in &self.dyn_columns {
            match c.field_type {
                FieldType::Str | FieldType::Bytes => {
                    let mut interner: HashMap<Vec<u8>, u32> = HashMap::new();
                    let mut distinct: Vec<Vec<u8>> = Vec::new();
                    let mut ids: Vec<u32> = Vec::with_capacity(c.cells.len());
                    for cell in &c.cells {
                        let bytes = match resolve_value(cell).1 {
                            ColumnValue::Str(b) | ColumnValue::Bytes(b) => b,
                            // A Str/Bytes column resolves only to Str/Bytes; any
                            // other value would be a mis-typed column, so fall
                            // back to the plain path rather than guess.
                            _ => {
                                distinct.clear();
                                ids.clear();
                                break;
                            }
                        };
                        let next = distinct.len() as u32;
                        let id = *interner.entry(bytes.clone()).or_insert_with(|| {
                            distinct.push(bytes);
                            next
                        });
                        ids.push(id);
                    }
                    if ids.len() == c.cells.len() {
                        dicts.push(Some(StrColumnDict { distinct, ids }));
                    } else {
                        dicts.push(None);
                    }
                }
                _ => dicts.push(None),
            }
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
mod tests {
    use super::*;
    use crate::record::stream_attrs_bytes;

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
    /// `N`-row batch, then makes exactly the named field one row short.
    /// Every field `validate` checks for row-count parity must be caught
    /// individually -- a check with one name quietly dropped from the list
    /// would pass a batch this test refuses.
    #[test]
    fn each_per_row_field_length_is_checked() {
        const N: usize = 2;
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
            let mut batch = minimal_batch(N);
            let short = N - 1;
            match field {
                "ts_ns" => batch.ts_ns.truncate(short),
                "observed_ts_ns" => batch.observed_ts_ns.truncate(short),
                "severity_num" => batch.severity_num.truncate(short),
                "flags" => batch.flags.truncate(short),
                "severity_text" => {
                    batch.severity_text = VarBytes::new();
                    for _ in 0..short {
                        batch.severity_text.push(b"");
                    }
                }
                "body" => {
                    batch.body = VarBytes::new();
                    for _ in 0..short {
                        batch.body.push(b"");
                    }
                }
                "trace_id_validity" => {
                    let mut v = Bitmap::new();
                    for _ in 0..short {
                        v.push(false);
                    }
                    batch.trace_id_validity = v;
                }
                "span_id_validity" => {
                    let mut v = Bitmap::new();
                    for _ in 0..short {
                        v.push(false);
                    }
                    batch.span_id_validity = v;
                }
                "stream_refs" => batch.stream_refs.truncate(short),
                _ => unreachable!("every named field has a case above"),
            }
            assert_malformed(
                batch.validate(),
                &format!("{field} has {short} entries but num_rows is {N}"),
            );
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
            cells: Vec::new(),
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
            cells: vec![AttrValue::I64(1)],
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
            cells: vec![AttrValue::Str("v".into())],
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
            cells: vec![AttrValue::Str("v".into())],
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
            cells: vec![AttrValue::Str("v".into())],
            validity,
        });
        batch.dyn_col_dicts = vec![Some(StrColumnDict {
            distinct: vec![b"v".to_vec()],
            ids: vec![1],
        })];
        assert_malformed(
            batch.validate(),
            "id[0x0] is 0x1 but distinct has 1 entries",
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
}
