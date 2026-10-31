//! [`LogRowRef`], by-reference access to one buffered log row, and
//! [`ColumnarLogBatch::try_from_rows`], which builds a batch from a slice of
//! such rows without first copying each row into a [`LogRecord`] (ADR-3492
//! decision 2).

use std::collections::HashMap;

use ravel_types::logstream::{AttrValue, LogStreamId};

use crate::columnar_batch::{
    Bitmap, ColumnarLogBatch, DynCells, DynColumn, VAR_BYTES_MAX, VarBytes, attr_field_type,
};
use crate::error::LogSegError;
use crate::record::{FieldType, LogRecord};

/// What [`ColumnarLogBatch::try_from_rows`] reads from one row, by reference.
/// [`LogRecord`] implements it; so can any buffered row type that holds the
/// same fields, such as one sharing its `stream_attrs` blob between records.
pub trait LogRowRef {
    fn stream_id(&self) -> LogStreamId;
    /// The STREAM_DIR blob for [`Self::stream_id`] ([`LogRecord::stream_attrs`]).
    fn stream_attrs(&self) -> &[u8];
    fn ts_ns(&self) -> i64;
    fn observed_ts_ns(&self) -> i64;
    fn severity_num(&self) -> u8;
    fn flags(&self) -> u32;
    fn severity_text(&self) -> &str;
    fn body(&self) -> &str;
    fn trace_id(&self) -> Option<&[u8; 16]>;
    fn span_id(&self) -> Option<&[u8; 8]>;
    fn attrs(&self) -> &[(String, AttrValue)];
}

impl LogRowRef for LogRecord {
    fn stream_id(&self) -> LogStreamId {
        self.stream_id
    }
    fn stream_attrs(&self) -> &[u8] {
        &self.stream_attrs
    }
    fn ts_ns(&self) -> i64 {
        self.ts_ns
    }
    fn observed_ts_ns(&self) -> i64 {
        self.observed_ts_ns
    }
    fn severity_num(&self) -> u8 {
        self.severity_num
    }
    fn flags(&self) -> u32 {
        self.flags
    }
    fn severity_text(&self) -> &str {
        &self.severity_text
    }
    fn body(&self) -> &str {
        &self.body
    }
    fn trace_id(&self) -> Option<&[u8; 16]> {
        self.trace_id.as_ref()
    }
    fn span_id(&self) -> Option<&[u8; 8]> {
        self.span_id.as_ref()
    }
    fn attrs(&self) -> &[(String, AttrValue)] {
        &self.attrs
    }
}

/// No column yet for this `(name, type)`.
const NONE: u32 = u32::MAX;

/// A dynamic column under construction. `name` borrows the first row that
/// carried it and is copied once, when the column is materialized.
struct Col<'a> {
    name: &'a str,
    field_type: FieldType,
    cells: DynCells,
    validity: Bitmap,
    /// The last row this column took a cell in, so a later same-`(name, type)`
    /// occurrence in that row goes to the residuals.
    last_row: usize,
}

/// The index of `ty` in a name's per-type column slots.
fn type_slot(ty: FieldType) -> usize {
    match ty {
        FieldType::Str => 0,
        FieldType::I64 => 1,
        FieldType::F64 => 2,
        FieldType::Bool => 3,
        FieldType::Bytes => 4,
    }
}

impl ColumnarLogBatch {
    /// [`Self::try_from_records`] over any rows that expose the same fields by
    /// reference: the same batch, or the same refusal when a value would take
    /// a column past its byte limit.
    pub fn try_from_rows<R: LogRowRef>(rows: &[R]) -> Result<Self, LogSegError> {
        Self::try_from_rows_with_limit(rows, VAR_BYTES_MAX)
    }

    /// [`Self::try_from_rows`] with each `Str`/`Bytes` column's byte limit
    /// lowered to `limit`, so a test reaches the refusal without holding
    /// `u32::MAX` bytes.
    pub(crate) fn try_from_rows_with_limit<R: LogRowRef>(
        rows: &[R],
        limit: usize,
    ) -> Result<Self, LogSegError> {
        match Self::build_from_rows(rows, limit) {
            (batch, None) => Ok(batch),
            (_, Some(refused)) => Err(refused),
        }
    }

    /// The batch [`Self::from_records`] builds, and the first push its byte
    /// limit refused, with the column named.
    ///
    /// Columns are found by a per-stream, per-attribute-position hint checked
    /// by name and type, falling back to a map keyed by name; a row's
    /// duplicate `(name, type)` is detected by the column's last row, and the
    /// stream directory is built in first-seen order and sorted by id once.
    pub(crate) fn build_from_rows<'a, R: LogRowRef>(
        rows: &'a [R],
        limit: usize,
    ) -> (Self, Option<LogSegError>) {
        let n = rows.len();
        let mut batch = ColumnarLogBatch::new();
        batch.num_rows = n;

        let (mut sev_bytes, mut body_bytes, mut traces, mut spans) = (0usize, 0usize, 0, 0);
        for r in rows {
            sev_bytes = sev_bytes.saturating_add(r.severity_text().len());
            body_bytes = body_bytes.saturating_add(r.body().len());
            traces += usize::from(r.trace_id().is_some());
            spans += usize::from(r.span_id().is_some());
        }
        batch.ts_ns = Vec::with_capacity(n);
        batch.observed_ts_ns = Vec::with_capacity(n);
        batch.severity_num = Vec::with_capacity(n);
        batch.flags = Vec::with_capacity(n);
        batch.severity_text = VarBytes::with_capacity(n, sev_bytes);
        batch.body = VarBytes::with_capacity(n, body_bytes);
        batch.trace_id = Vec::with_capacity(traces * 16);
        batch.trace_id_validity = Bitmap::with_capacity(n);
        batch.span_id = Vec::with_capacity(spans * 8);
        batch.span_id_validity = Bitmap::with_capacity(n);
        batch.stream_refs = Vec::with_capacity(n);
        batch.residual_attrs = vec![Vec::new(); n];

        // Streams in first-seen order; `stream_refs` holds these provisional
        // positions until the directory is sorted by id below.
        let mut streams: Vec<(LogStreamId, &'a [u8])> = Vec::new();
        let mut stream_pos: HashMap<LogStreamId, u32> = HashMap::new();
        let mut last_stream: Option<(LogStreamId, u32)> = None;
        // Per stream, the column each attribute position resolved to last.
        let mut hints: Vec<Vec<u32>> = Vec::new();

        let mut cols: Vec<Col<'a>> = Vec::new();
        let mut col_index: HashMap<&'a str, [u32; 5]> = HashMap::new();
        let mut refused: Option<LogSegError> = None;

        for (row, r) in rows.iter().enumerate() {
            batch.ts_ns.push(r.ts_ns());
            batch.observed_ts_ns.push(r.observed_ts_ns());
            batch.severity_num.push(r.severity_num());
            batch.flags.push(r.flags());
            batch.severity_text.push(r.severity_text().as_bytes());
            batch.body.push(r.body().as_bytes());

            match r.trace_id() {
                Some(t) => {
                    batch.trace_id.extend_from_slice(t);
                    batch.trace_id_validity.push(true);
                }
                None => batch.trace_id_validity.push(false),
            }
            match r.span_id() {
                Some(s) => {
                    batch.span_id.extend_from_slice(s);
                    batch.span_id_validity.push(true);
                }
                None => batch.span_id_validity.push(false),
            }

            let id = r.stream_id();
            let pos = match last_stream {
                Some((last, pos)) if last == id => pos,
                _ => {
                    let next = streams.len() as u32;
                    let pos = *stream_pos.entry(id).or_insert(next);
                    if pos == next {
                        streams.push((id, r.stream_attrs()));
                        hints.push(Vec::new());
                    }
                    last_stream = Some((id, pos));
                    pos
                }
            };
            batch.stream_refs.push(pos);

            let row_hints = &mut hints[pos as usize];
            for (j, (k, v)) in r.attrs().iter().enumerate() {
                let ty = attr_field_type(v);
                let hinted = row_hints.get(j).copied().filter(|&c| {
                    let col = &cols[c as usize];
                    col.field_type == ty && col.name == k.as_str()
                });
                let c = match hinted {
                    Some(c) => c,
                    None => {
                        let slots = col_index.entry(k.as_str()).or_insert([NONE; 5]);
                        let slot = &mut slots[type_slot(ty)];
                        if *slot == NONE {
                            *slot = cols.len() as u32;
                            cols.push(Col {
                                name: k.as_str(),
                                field_type: ty,
                                cells: new_cells(ty, limit),
                                validity: Bitmap::with_capacity(n),
                                last_row: usize::MAX,
                            });
                        }
                        let c = *slot;
                        match row_hints.get_mut(j) {
                            Some(h) => *h = c,
                            None => row_hints.push(c),
                        }
                        c
                    }
                };
                let col = &mut cols[c as usize];
                // First occurrence of a (name, type) takes the column cell; a
                // later same-(name, type) occurrence in this row is a residual.
                if col.last_row == row {
                    batch.residual_attrs[row].push((k.clone(), v.clone()));
                    continue;
                }
                col.last_row = row;
                // The column was created for this value's resolved type, so
                // only the byte limit can refuse the push. The row is marked
                // present either way: a refused cell leaves the column one
                // cell short of its validity, which `validate` refuses, rather
                // than silently dropping the value.
                if let Err(e) = col.cells.push_value(v) {
                    let detail = match e {
                        LogSegError::LimitExceeded(detail) => detail,
                        other => other.to_string(),
                    };
                    refused.get_or_insert_with(|| {
                        LogSegError::LimitExceeded(format!("column {k:?}: {detail}"))
                    });
                }
                col.validity.pad_to(row);
                col.validity.push(true);
            }
        }

        // Stream directory: id-ascending, dense ref; the first blob seen for
        // an id is the one kept.
        let mut order: Vec<u32> = (0..streams.len() as u32).collect();
        order.sort_unstable_by_key(|&p| streams[p as usize].0);
        let mut dense = vec![0u32; streams.len()];
        batch.stream_ids = Vec::with_capacity(streams.len());
        batch.stream_attrs = Vec::with_capacity(streams.len());
        for (r, &p) in order.iter().enumerate() {
            let (id, blob) = streams[p as usize];
            dense[p as usize] = r as u32;
            batch.stream_ids.push(id);
            batch.stream_attrs.push(blob.to_vec());
        }
        for sref in &mut batch.stream_refs {
            *sref = dense[*sref as usize];
        }

        // Materialize dynamic columns in (name, type) order.
        cols.sort_unstable_by(|a, b| {
            (a.name, a.field_type.to_u8()).cmp(&(b.name, b.field_type.to_u8()))
        });
        batch.dyn_columns = cols
            .into_iter()
            .map(|mut col| {
                col.validity.pad_to(n);
                DynColumn {
                    name: col.name.to_owned(),
                    field_type: col.field_type,
                    cells: col.cells,
                    validity: col.validity,
                }
            })
            .collect();

        (batch, refused)
    }
}

/// An empty cell buffer of type `ty`, a `Str`/`Bytes` one refusing values past
/// `limit` bytes.
fn new_cells(ty: FieldType, limit: usize) -> DynCells {
    let mut cells = DynCells::new(ty);
    match &mut cells {
        DynCells::Str(v) => *v = VarBytes::with_byte_limit(0, 0, limit),
        DynCells::Bytes(b) => b.values = VarBytes::with_byte_limit(0, 0, limit),
        DynCells::I64(_) | DynCells::F64(_) | DynCells::Bool(_) => {}
    }
    cells
}
