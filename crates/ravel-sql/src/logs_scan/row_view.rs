//! The row path's batch build straight from a block's columnar view (ADR-2773
//! decision D4).
//!
//! A row-path block (one carrying an `attrs_raw` overflow page, or any block of
//! a scan with a pending erasure or an `attrs` projection) is built with no
//! [`LogRecord`] and no merged `Vec<(String, AttrValue)>` per row. The merged
//! view of every row is a slice of one borrowed arena per block: FIELD_DIR
//! cells read through cursors, `attrs_raw` entries decoded into one buffer per
//! block, and each stream's resource and scope attributes decoded once per
//! stream in the block. The merge rule is
//! [`ravel_query::erasure::merged_log_attrs`]'s, and erased rows are dropped
//! before any column is built.

use super::*;
use ravel_logseg::record::{COL_STREAM_REF, COL_TS};
use ravel_query::erasure::log_row_erased;
use std::fmt::Write as _;
use std::ops::Range;

/// One attribute value, borrowed from a block view, a block's decoded
/// `attrs_raw` entries, or a stream's decoded resource and scope attributes.
#[derive(Clone, Copy, Debug)]
pub(super) enum Cell<'a> {
    Str(&'a str),
    Bytes(&'a [u8]),
    I64(i64),
    F64(f64),
    Bool(bool),
    /// A `List` or `Map` value. Only `attrs_raw` carries one into a merged
    /// view: a nested resource or scope value is omitted at decode.
    Nested(&'a AttrValue),
}

impl<'a> Cell<'a> {
    pub(super) fn of(value: &'a AttrValue) -> Self {
        match value {
            AttrValue::Str(s) => Cell::Str(s),
            AttrValue::Bytes(b) => Cell::Bytes(b),
            AttrValue::I64(v) => Cell::I64(*v),
            AttrValue::F64(v) => Cell::F64(*v),
            AttrValue::Bool(v) => Cell::Bool(*v),
            AttrValue::List(_) | AttrValue::Map(_) => Cell::Nested(value),
        }
    }

    /// The value as erasure matching sees it: text for a `Str`, nothing for
    /// any other variant.
    fn str_text(self) -> Option<&'a str> {
        match self {
            Cell::Str(s) => Some(s),
            _ => None,
        }
    }

    /// Appends the value's text to `out`, the text [`attr_value_to_string`]
    /// renders for the same value.
    pub(super) fn render_into(self, out: &mut String) {
        // Writing to a String never fails.
        match self {
            Cell::Str(s) => out.push_str(s),
            Cell::I64(v) => {
                let _ = write!(out, "{v}");
            }
            Cell::F64(v) => {
                let _ = write!(out, "{v}");
            }
            Cell::Bool(v) => {
                let _ = write!(out, "{v}");
            }
            Cell::Bytes(bytes) => {
                for b in bytes {
                    let _ = write!(out, "{b:02x}");
                }
            }
            Cell::Nested(v) => out.push_str(&attr_value_to_string(v)),
        }
    }
}

/// Appends `cell` rendered as text to `b`. A `Str` is appended as it is, with
/// no copy into `scratch`.
pub(super) fn append_rendered(b: &mut StringBuilder, cell: Cell<'_>, scratch: &mut String) {
    match cell {
        Cell::Str(s) => b.append_value(s),
        other => {
            scratch.clear();
            other.render_into(scratch);
            b.append_value(scratch.as_str());
        }
    }
}

/// A FIELD_DIR column resolved once for the block.
enum FieldCursor<'a> {
    Str(BytesCursor<'a>),
    Bytes(BytesCursor<'a>),
    I64(I64Cursor<'a>),
    F64(F64BitsCursor<'a>),
    Bool(BoolCursor<'a>),
}

impl<'a> FieldCursor<'a> {
    fn resolve(view: &ColumnarBlockView<'a>, col: AttrColumn) -> Self {
        match col.ty {
            FieldType::Str => FieldCursor::Str(view.bytes_cursor(col.column_id)),
            FieldType::Bytes => FieldCursor::Bytes(view.bytes_cursor(col.column_id)),
            FieldType::I64 => FieldCursor::I64(view.i64_cursor(col.column_id)),
            FieldType::F64 => FieldCursor::F64(view.f64_bits_cursor(col.column_id)),
            FieldType::Bool => FieldCursor::Bool(view.bool_cursor(col.column_id)),
        }
    }

    /// The record's value at surviving row `i`. A `Str` cell that is not UTF-8
    /// is no value, as in the record rebuild.
    fn cell_at(&self, i: usize) -> Option<Cell<'a>> {
        match self {
            FieldCursor::Str(c) => c.str_at(i).map(Cell::Str),
            FieldCursor::Bytes(c) => c.at(i).map(Cell::Bytes),
            FieldCursor::I64(c) => c.at(i).map(Cell::I64),
            FieldCursor::F64(c) => c.at(i).map(|bits| Cell::F64(f64::from_bits(bits))),
            FieldCursor::Bool(c) => c.at(i).map(Cell::Bool),
        }
    }
}

fn erasure_entry<'a>(&(key, cell): &(&'a str, Cell<'a>)) -> (&'a str, Option<&'a str>) {
    (key, cell.str_text())
}

/// The error the record rebuild reports for the same corrupt cell.
fn corrupt_segment(key: &str, what: &str) -> DataFusionError {
    SqlError::from(LogFetchError::Corrupt {
        key: key.to_string(),
        source: LogSegError::Corrupted(what.to_string()),
    })
    .into()
}

#[cfg(test)]
thread_local! {
    /// Merged views [`build_row_view_batches`] built on this thread, one per
    /// row it kept, so a test can show the row path ran.
    pub(super) static ROW_VIEWS_BUILT: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// A kept row: its surviving-row index, event timestamp, and its merged
/// entries' span in the block's entry arena.
struct KeptRow {
    row: usize,
    ts: i64,
    entries: Range<usize>,
}

/// Everything a row-path block build reads besides the view.
pub(super) struct RowViewBuild<'s> {
    /// The object key, for the corruption errors.
    pub(super) key: &'s str,
    /// The fetcher's erasure predicates, matched against each record's own
    /// attributes, as the record exit's per-block filter matched them.
    pub(super) fetch_erasure: &'s [ErasurePredicate],
    /// The scan's authoritative erasure predicates, matched against each row's
    /// merged resource, scope and record view.
    pub(super) scan_erasure: &'s [ErasurePredicate],
    pub(super) schema: &'s SchemaRef,
    pub(super) projection: &'s [usize],
    pub(super) declared: &'s [DeclaredColumn],
    pub(super) attr_keys: &'s [String],
    pub(super) full_len: usize,
    /// The block's row-ref address; `first_row` counts kept rows.
    pub(super) row_refs: Option<RowRefRange>,
}

/// A block's row-path batches and how many rows survived erasure.
pub(super) struct RowViewBatches {
    pub(super) batches: Vec<RecordBatch>,
    pub(super) kept: usize,
}

/// Builds one row-path block's output batches from its view, chunked at
/// [`BATCH_ROWS`], with exactly the columns [`build_batch`] builds from the
/// records the record exit rebuilds for the same block once both erasure
/// layers have run.
///
/// Each record's attributes are its FIELD_DIR cells in FIELD_DIR order, then
/// its `attrs_raw` entries. A row's merged view is its stream's resource
/// entries, then its scope entries, then each record attribute overwriting the
/// first entry with its key or appended. A row is dropped when a fetch-layer
/// predicate matches its record attributes or a scan-layer predicate matches
/// its merged view; a dropped row builds no cell.
///
/// `attrs_raw` and the stream attributes are decoded only when a projected
/// column or an erasure predicate reads attributes.
pub(super) fn build_row_view_batches(
    view: &ColumnarBlockView<'_>,
    build: &RowViewBuild<'_>,
) -> DFResult<RowViewBatches> {
    let n = view.surviving_count();
    let needs_merged = build
        .projection
        .iter()
        .any(|&i| i == LOG_COL_ATTRS || i >= FIRST_DECLARED_COL);
    let needs_stream = needs_merged || !build.scan_erasure.is_empty();
    let needs_record = needs_stream || !build.fetch_erasure.is_empty();

    let fields: Vec<(&str, FieldCursor<'_>)> = if needs_record {
        view.attr_columns()
            .map(|(name, col)| (name, FieldCursor::resolve(view, col)))
            .collect()
    } else {
        Vec::new()
    };
    let mut raw: Vec<(String, AttrValue)> = Vec::new();
    let mut raw_spans: Vec<Range<usize>> = Vec::new();
    let mut streams: HashMap<u32, Vec<(String, AttrValue)>> = HashMap::new();
    if needs_record {
        raw_spans.reserve(n);
        for i in 0..n {
            let start = raw.len();
            view.attrs_raw_into(i, &mut raw)
                .map_err(|source| -> DataFusionError {
                    SqlError::from(LogFetchError::Corrupt {
                        key: build.key.to_string(),
                        source,
                    })
                    .into()
                })?;
            raw_spans.push(start..raw.len());
            if needs_stream {
                let sref = stream_ref_at(view, i, build.key)?;
                if let std::collections::hash_map::Entry::Vacant(e) = streams.entry(sref) {
                    let blob = view
                        .stream_attrs_of(sref)
                        .ok_or_else(|| corrupt_segment(build.key, "stream_ref out of range"))?;
                    e.insert(decode_stream_attrs(blob)?);
                }
            }
        }
    }

    let mut record: Vec<(&str, Cell<'_>)> = Vec::new();
    let mut entries: Vec<(&str, Cell<'_>)> = Vec::new();
    let mut kept: Vec<KeptRow> = Vec::with_capacity(n);
    for i in 0..n {
        let ts = view
            .ts(i)
            .ok_or_else(|| corrupt_segment(build.key, &format!("missing i64 col {COL_TS}")))?;
        let start = entries.len();
        if needs_record {
            record.clear();
            for (name, cursor) in &fields {
                if let Some(cell) = cursor.cell_at(i) {
                    record.push((name, cell));
                }
            }
            if let Some(span) = raw_spans.get(i) {
                record.extend(
                    raw[span.clone()]
                        .iter()
                        .map(|(k, v)| (k.as_str(), Cell::of(v))),
                );
            }
            if log_row_erased(build.fetch_erasure, ts, || record.iter().map(erasure_entry)) {
                continue;
            }
        }
        if needs_stream {
            let sref = stream_ref_at(view, i, build.key)?;
            let stream = streams
                .get(&sref)
                .ok_or_else(|| corrupt_segment(build.key, "stream_ref out of range"))?;
            entries.extend(stream.iter().map(|(k, v)| (k.as_str(), Cell::of(v))));
            for &(k, cell) in &record {
                match entries[start..].iter_mut().find(|(ek, _)| *ek == k) {
                    Some(slot) => slot.1 = cell,
                    None => entries.push((k, cell)),
                }
            }
            if log_row_erased(build.scan_erasure, ts, || {
                entries[start..].iter().map(erasure_entry)
            }) {
                entries.truncate(start);
                continue;
            }
        }
        kept.push(KeptRow {
            row: i,
            ts,
            entries: start..entries.len(),
        });
    }
    #[cfg(test)]
    if needs_stream {
        ROW_VIEWS_BUILT.with(|c| c.set(c.get() + kept.len() as u64));
    }

    let mut batches = Vec::with_capacity(kept.len().div_ceil(BATCH_ROWS));
    let mut chunk_start = 0;
    while chunk_start < kept.len() {
        let chunk_end = (chunk_start + BATCH_ROWS).min(kept.len());
        let row_refs = build.row_refs.map(|r| RowRefRange {
            first_row: r.first_row + chunk_start,
            ..r
        });
        batches.push(build_chunk(
            view,
            build,
            &kept[chunk_start..chunk_end],
            &entries,
            row_refs,
        )?);
        chunk_start = chunk_end;
    }
    Ok(RowViewBatches {
        batches,
        kept: kept.len(),
    })
}

fn stream_ref_at(view: &ColumnarBlockView<'_>, i: usize, key: &str) -> DFResult<u32> {
    view.stream_ref(i)
        .ok_or_else(|| corrupt_segment(key, &format!("missing i64 col {COL_STREAM_REF}")))
}

/// A UTF-8 log field (`body`, `severity_text`), empty when absent.
fn text_field<'a>(bytes: Option<&'a [u8]>, key: &str) -> DFResult<&'a str> {
    match bytes {
        None => Ok(""),
        Some(b) => std::str::from_utf8(b).map_err(|_| corrupt_segment(key, "value not utf-8")),
    }
}

/// A `trace_id` or `span_id` column. A stored id of the wrong width is the
/// rebuild's `what` corruption error.
fn fixed_id_array<'v>(
    rows: &[KeptRow],
    cell_at: impl Fn(usize) -> Option<&'v [u8]>,
    width: i32,
    what: &str,
    key: &str,
) -> DFResult<ArrayRef> {
    let mut b = FixedSizeBinaryBuilder::with_capacity(rows.len(), width);
    for r in rows {
        match cell_at(r.row) {
            Some(id) if i32::try_from(id.len()) == Ok(width) => b
                .append_value(id)
                .map_err(|e| SqlError::Internal(format!("{what} array build: {e}")))?,
            Some(_) => return Err(corrupt_segment(key, what)),
            None => b.append_null(),
        }
    }
    Ok(Arc::new(b.finish()))
}

/// Each projected key column's value for every row of a chunk, from the rows'
/// merged entries: the first entry holding a key wins.
struct ChunkKeys<'a> {
    slot_of_column: HashMap<usize, usize>,
    /// `cells[slot][row]`.
    cells: Vec<Vec<Option<Cell<'a>>>>,
}

impl<'a> ChunkKeys<'a> {
    fn resolve(
        build: &RowViewBuild<'_>,
        rows: &[KeptRow],
        entries: &[(&'a str, Cell<'a>)],
    ) -> Self {
        let mut keys: Vec<&str> = Vec::new();
        let mut slot_of_column = HashMap::new();
        for &index in build.projection {
            let Some(key) = projected_key(index, build.declared, build.attr_keys, build.full_len)
            else {
                continue;
            };
            let slot = match keys.iter().position(|k| *k == key) {
                Some(slot) => slot,
                None => {
                    keys.push(key);
                    keys.len() - 1
                }
            };
            slot_of_column.insert(index, slot);
        }
        let mut cells = vec![vec![None; rows.len()]; keys.len()];
        if keys.len() < ONE_PASS_MIN_KEY_COLUMNS {
            for (slot, key) in keys.iter().enumerate() {
                for (row, r) in rows.iter().enumerate() {
                    cells[slot][row] = entries[r.entries.clone()]
                        .iter()
                        .find(|(k, _)| k == key)
                        .map(|&(_, cell)| cell);
                }
            }
        } else {
            let slot_of_key: HashMap<&str, usize> = keys
                .iter()
                .enumerate()
                .map(|(slot, k)| (*k, slot))
                .collect();
            for (row, r) in rows.iter().enumerate() {
                for &(key, cell) in &entries[r.entries.clone()] {
                    if let Some(&slot) = slot_of_key.get(key)
                        && let Some(slot_cell) = cells.get_mut(slot).and_then(|c| c.get_mut(row))
                        && slot_cell.is_none()
                    {
                        *slot_cell = Some(cell);
                    }
                }
            }
        }
        ChunkKeys {
            slot_of_column,
            cells,
        }
    }

    fn column(&self, index: usize) -> Option<&[Option<Cell<'a>>]> {
        let slot = *self.slot_of_column.get(&index)?;
        self.cells.get(slot).map(Vec::as_slice)
    }
}

/// A declared typed column from each row's resolved cell (ADR-0090 decisions
/// 5-7): a wrong variant or an absent key is NULL, never a cast, and a `List`
/// or `Map` under a `Bytes` declaration is its canonical encoding.
fn declared_cells_array(dc: &DeclaredColumn, cells: &[Option<Cell<'_>>]) -> ArrayRef {
    match dc.ty {
        DeclaredType::Str => {
            let mut b = StringDictionaryBuilder::<Int32Type>::new();
            for cell in cells {
                match cell {
                    Some(Cell::Str(s)) => b.append_value(s),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DeclaredType::I64 => {
            let mut b = Int64Builder::with_capacity(cells.len());
            for cell in cells {
                match cell {
                    Some(Cell::I64(v)) => b.append_value(*v),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DeclaredType::Bool => {
            let mut b = BooleanBuilder::with_capacity(cells.len());
            for cell in cells {
                match cell {
                    Some(Cell::Bool(v)) => b.append_value(*v),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DeclaredType::Bytes => {
            let mut b = BinaryBuilder::new();
            for cell in cells {
                match cell {
                    Some(Cell::Bytes(bytes)) => b.append_value(bytes),
                    Some(Cell::Nested(v)) => b.append_value(canonical_value_bytes(v)),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
    }
}

fn build_chunk(
    view: &ColumnarBlockView<'_>,
    build: &RowViewBuild<'_>,
    rows: &[KeptRow],
    entries: &[(&str, Cell<'_>)],
    row_refs: Option<RowRefRange>,
) -> DFResult<RecordBatch> {
    let keys = ChunkKeys::resolve(build, rows, entries);
    let mut scratch = String::new();
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(build.projection.len() + 1);
    for &i in build.projection {
        let array: ArrayRef = match i {
            LOG_COL_TS => Arc::new(TimestampNanosecondArray::from(
                rows.iter().map(|r| r.ts).collect::<Vec<_>>(),
            )),
            LOG_COL_OBSERVED_TS => Arc::new(TimestampNanosecondArray::from(
                rows.iter()
                    .map(|r| view.observed_ts(r.row).unwrap_or(0))
                    .collect::<Vec<_>>(),
            )),
            LOG_COL_SEVERITY_NUM => Arc::new(UInt8Array::from(
                rows.iter()
                    .map(|r| view.severity_num(r.row).unwrap_or(0) as u8)
                    .collect::<Vec<_>>(),
            )),
            LOG_COL_SEVERITY_TEXT => {
                let mut b = StringBuilder::new();
                for r in rows {
                    b.append_value(text_field(view.severity_text(r.row), build.key)?);
                }
                Arc::new(b.finish())
            }
            LOG_COL_BODY => {
                let mut b = StringBuilder::new();
                for r in rows {
                    b.append_value(text_field(view.body(r.row), build.key)?);
                }
                Arc::new(b.finish())
            }
            LOG_COL_TRACE_ID => fixed_id_array(
                rows,
                |row| view.trace_id(row),
                TRACE_ID_WIDTH,
                "trace_id width",
                build.key,
            )?,
            LOG_COL_SPAN_ID => fixed_id_array(
                rows,
                |row| view.span_id(row),
                SPAN_ID_WIDTH,
                "span_id width",
                build.key,
            )?,
            LOG_COL_FLAGS => Arc::new(UInt32Array::from(
                rows.iter()
                    .map(|r| view.flags(r.row).unwrap_or(0) as u32)
                    .collect::<Vec<_>>(),
            )),
            LOG_COL_ATTRS => {
                let mut attrs = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
                for r in rows {
                    for &(k, cell) in &entries[r.entries.clone()] {
                        attrs.keys().append_value(k);
                        append_rendered(attrs.values(), cell, &mut scratch);
                    }
                    attrs
                        .append(true)
                        .map_err(|e| SqlError::Internal(format!("attrs map build: {e}")))?;
                }
                Arc::new(attrs.finish())
            }
            other if other >= build.full_len => match keys.column(other) {
                Some(cells) => {
                    let mut b = StringBuilder::new();
                    for cell in cells {
                        match cell {
                            Some(cell) => append_rendered(&mut b, *cell, &mut scratch),
                            None => b.append_null(),
                        }
                    }
                    Arc::new(b.finish())
                }
                None => {
                    return Err(DataFusionError::Internal(format!(
                        "logs scan projection index {other} out of range"
                    )));
                }
            },
            other => match (
                other
                    .checked_sub(FIRST_DECLARED_COL)
                    .and_then(|k| build.declared.get(k)),
                keys.column(other),
            ) {
                (Some(dc), Some(cells)) => declared_cells_array(dc, cells),
                _ => {
                    return Err(DataFusionError::Internal(format!(
                        "logs scan projection index {other} out of range"
                    )));
                }
            },
        };
        columns.push(array);
    }
    if let Some(range) = row_refs {
        columns.push(row_ref_array(range, rows.len())?);
    }
    let options = RecordBatchOptions::new().with_row_count(Some(rows.len()));
    RecordBatch::try_new_with_options(Arc::clone(build.schema), columns, &options)
        .map_err(DataFusionError::from)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    //! ADR-2773 decision D4: the view build against the record-based build it
    //! replaces, which stays here as the oracle: rebuild the block's records
    //! ([`ravel_logseg::BlockScan::next_block`]), apply the fetcher's record
    //! filter and the scan's merged-view filter, then [`build_batch`].

    use super::*;
    use crate::logs_schema::logs_schema_with_declared;
    use crate::rlog_attrs::retain_unerased;
    use proptest::prelude::*;
    use proptest::strategy::ValueTree;
    use proptest::test_runner::TestRunner;
    use ravel_logseg::record::stream_attrs_bytes;
    use ravel_logseg::{ObjectIdentity, RlogConfig, RlogReader, RlogWriter};
    use ravel_query::erasure::retain_log_records;
    use ravel_types::logstream::log_stream_id;

    /// Attribute keys. Stream 0's resource and scope both carry `s`.
    const KEYS: [&str; 5] = ["s", "a", "b", "c", "d"];
    const STREAMS: usize = 3;
    /// Values an erasure matcher names: the two `Str` values [`value`] draws,
    /// and stream 0's resource value of `s`.
    const MATCH_VALUES: [&str; 3] = ["x", "y", "r"];
    /// The key-column projection: `attrs['zz']` is never set.
    const ATTR_KEYS: [&str; 3] = ["s", "c", "zz"];

    fn cfg() -> RlogConfig {
        // Two dynamic columns for five keys: most blocks overflow into
        // `attrs_raw`, and a `List` or `Map` value always does.
        RlogConfig {
            block_target_records: 256,
            max_dynamic_columns: 2,
            ..RlogConfig::default()
        }
    }

    fn declared() -> Vec<DeclaredColumn> {
        vec![
            DeclaredColumn::new("s", DeclaredType::Str),
            DeclaredColumn::new("a", DeclaredType::I64),
            DeclaredColumn::new("b", DeclaredType::Bool),
            DeclaredColumn::new("c", DeclaredType::Bytes),
            DeclaredColumn::new("d", DeclaredType::Str),
        ]
    }

    fn value() -> BoxedStrategy<AttrValue> {
        prop_oneof![
            prop::sample::select(vec!["x", "y"]).prop_map(|s| AttrValue::Str(s.into())),
            (-2i64..2).prop_map(AttrValue::I64),
            prop::sample::select(vec![1.5f64, -0.0, f64::NAN]).prop_map(AttrValue::F64),
            any::<bool>().prop_map(AttrValue::Bool),
            Just(AttrValue::Bytes(vec![1, 0xab])),
            Just(AttrValue::List(vec![
                AttrValue::I64(1),
                AttrValue::Str("x".into())
            ])),
            Just(AttrValue::Map(vec![("m".into(), AttrValue::Bool(true))])),
        ]
        .boxed()
    }

    fn attrs(max: usize) -> impl Strategy<Value = Vec<(String, AttrValue)>> {
        prop::collection::btree_map(prop::sample::select(KEYS.to_vec()), value(), 0..=max)
            .prop_map(|m| m.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
    }

    fn predicate() -> impl Strategy<Value = ErasurePredicate> {
        (
            prop::collection::vec(
                (
                    prop::sample::select(KEYS.to_vec()),
                    prop::sample::select(MATCH_VALUES.to_vec()),
                ),
                1..=2,
            ),
            prop::option::of((0i64..40, 0i64..40)),
        )
            .prop_map(|(matchers, window)| {
                let matchers = matchers
                    .into_iter()
                    .map(|(k, v)| (k.to_string(), v.to_string()))
                    .collect();
                match window {
                    // `0` is an unset bound, so the window starts at 1.
                    Some((a, b)) => ErasurePredicate::new(matchers, a.min(b) + 1, a.max(b) + 2),
                    None => ErasurePredicate::windowless(matchers),
                }
            })
    }

    type Attrs = Vec<(String, AttrValue)>;

    #[derive(Clone, Debug)]
    struct Case {
        /// Per stream: its resource and its scope attributes.
        streams: Vec<(Attrs, Attrs)>,
        /// Per row, `ts` its position: its stream and its record attributes.
        rows: Vec<(usize, Attrs)>,
        fetch_erasure: Vec<ErasurePredicate>,
        scan_erasure: Vec<ErasurePredicate>,
        /// Which of the full schema's columns and the [`ATTR_KEYS`] columns
        /// the batch projects.
        mask: Vec<bool>,
        row_refs: bool,
    }

    fn case() -> impl Strategy<Value = Case> {
        let columns = logs_schema_with_declared(&declared()).fields().len() + ATTR_KEYS.len();
        (
            prop::collection::vec((attrs(2), attrs(2)), STREAMS),
            prop::collection::vec((0..STREAMS, attrs(4)), STREAMS..40),
            prop::collection::vec(predicate(), 0..=2),
            prop::collection::vec(predicate(), 0..=2),
            prop::collection::vec(any::<bool>(), columns),
            any::<bool>(),
        )
            .prop_map(
                |(mut streams, mut rows, fetch_erasure, scan_erasure, mask, row_refs)| {
                    let (resource, scope) = &mut streams[0];
                    resource.retain(|(k, _)| k != "s");
                    resource.insert(0, ("s".into(), AttrValue::Str("r".into())));
                    scope.retain(|(k, _)| k != "s");
                    scope.insert(0, ("s".into(), AttrValue::Str("q".into())));
                    // Every stream has a row.
                    for (s, row) in rows.iter_mut().take(STREAMS).enumerate() {
                        row.0 = s;
                    }
                    Case {
                        streams,
                        rows,
                        fetch_erasure,
                        scan_erasure,
                        mask,
                        row_refs,
                    }
                },
            )
    }

    fn encode(case: &Case) -> Vec<u8> {
        let mut w = RlogWriter::new(
            cfg(),
            ObjectIdentity {
                tenant_hash: [0; 16],
                shard: 0,
                writer_id: [1; 16],
                writer_epoch: 1,
                writer_seq: 1,
            },
        );
        for (ts, (stream, attrs)) in case.rows.iter().enumerate() {
            let (resource, scope) = &case.streams[*stream];
            w.push(LogRecord {
                stream_id: log_stream_id(resource, "scope", "1", scope),
                stream_attrs: stream_attrs_bytes(resource, "scope", "1", scope),
                ts_ns: ts as i64,
                observed_ts_ns: ts as i64 + 1,
                severity_num: 9,
                severity_text: "INFO".into(),
                body: format!("row {ts}"),
                trace_id: (ts % 2 == 0).then_some([ts as u8; 16]),
                span_id: (ts % 3 == 0).then_some([ts as u8; 8]),
                flags: ts as u32,
                attrs: attrs.clone(),
            })
            .expect("push");
        }
        w.finish().expect("finish")
    }

    fn every_row() -> Predicate {
        Predicate::TsRange {
            min_ns: 0,
            max_ns: i64::MAX,
        }
    }

    /// The batch shape a case projects.
    struct Shape {
        declared: Vec<DeclaredColumn>,
        attr_keys: Vec<String>,
        full_len: usize,
        projection: Vec<usize>,
        schema: SchemaRef,
        row_refs: Option<RowRefRange>,
    }

    fn shape(case: &Case) -> Shape {
        let declared = declared();
        let full = logs_schema_with_declared(&declared);
        let full_len = full.fields().len();
        let attr_keys: Vec<String> = ATTR_KEYS.iter().map(|k| k.to_string()).collect();
        let projection: Vec<usize> = (0..full_len + attr_keys.len())
            .filter(|&i| case.mask[i])
            .collect();
        let mut fields: Vec<Field> = projection
            .iter()
            .map(|&i| match i.checked_sub(full_len) {
                None => full.field(i).clone(),
                Some(k) => Field::new(attr_key_field_name(&attr_keys[k]), DataType::Utf8, true),
            })
            .collect();
        let row_refs = case.row_refs.then_some(RowRefRange {
            segment: 3,
            block: 5,
            first_row: 0,
        });
        if row_refs.is_some() {
            fields.push(row_ref_field());
        }
        Shape {
            declared,
            attr_keys,
            full_len,
            projection,
            schema: Arc::new(Schema::new(fields)),
            row_refs,
        }
    }

    /// The record-based build: the block's records through both erasure
    /// layers, then [`build_batch`].
    fn record_build(obj: &[u8], case: &Case, shape: &Shape) -> (Vec<LogRecord>, Vec<RecordBatch>) {
        let reader = RlogReader::new(obj, &cfg()).expect("open");
        let mut scan = reader
            .scan_blocks(&every_row(), &[], &ColumnSelection::all())
            .expect("scan");
        let mut records = scan.next_block(obj).expect("decode").expect("one block");
        assert!(scan.next_block(obj).expect("decode").is_none(), "one block");
        retain_log_records(&mut records, &case.fetch_erasure);
        retain_unerased(&mut records, &case.scan_erasure).expect("erasure");
        let batches = if records.is_empty() {
            Vec::new()
        } else {
            vec![
                build_batch(
                    &records,
                    Arc::clone(&shape.schema),
                    &shape.projection,
                    &shape.declared,
                    &shape.attr_keys,
                    shape.full_len,
                    shape.row_refs,
                )
                .expect("record build"),
            ]
        };
        (records, batches)
    }

    fn view_build(obj: &[u8], case: &Case, shape: &Shape) -> RowViewBatches {
        let reader = RlogReader::new(obj, &cfg()).expect("open");
        let mut scan = reader
            .scan_blocks(&every_row(), &[], &ColumnSelection::all())
            .expect("scan");
        let view = scan
            .next_block_view(obj)
            .expect("decode")
            .expect("one block");
        build_row_view_batches(
            &view,
            &RowViewBuild {
                key: "obj",
                fetch_erasure: &case.fetch_erasure,
                scan_erasure: &case.scan_erasure,
                schema: &shape.schema,
                projection: &shape.projection,
                declared: &shape.declared,
                attr_keys: &shape.attr_keys,
                full_len: shape.full_len,
                row_refs: shape.row_refs,
            },
        )
        .expect("view build")
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(512))]

        /// Every column of a row-path batch built from the block view equals
        /// the record-based build's, over generated blocks carrying
        /// `attrs_raw` overflow cells, resource and scope overlays (stream 0's
        /// resource and scope both carry `s`, with different values), records
        /// overriding resource and scope values, values of every variant under
        /// each declared column (a `List` or `Map` under the `Bytes` column
        /// `c`), and rows the fetcher's record filter or the scan's merged
        /// filter erases. [`the_case_generator_reaches_every_case_the_property_names`]
        /// shows the generator reaches each.
        #[test]
        fn row_path_attrs_map_equals_the_record_based_build(case in case()) {
            let obj = encode(&case);
            let shape = shape(&case);
            let (records, want) = record_build(&obj, &case, &shape);
            let got = view_build(&obj, &case, &shape);
            prop_assert_eq!(got.kept, records.len());
            prop_assert_eq!(got.batches.len(), want.len());
            for (got, want) in got.batches.iter().zip(&want) {
                prop_assert_eq!(got.num_rows(), want.num_rows());
                for (c, field) in shape.schema.fields().iter().enumerate() {
                    prop_assert_eq!(
                        got.column(c).to_data(),
                        want.column(c).to_data(),
                        "column {}",
                        field.name()
                    );
                }
            }
        }
    }

    /// The generator reaches every case the property names, each in at least
    /// one of the first 512 cases of a fixed-seed run.
    #[test]
    fn the_case_generator_reaches_every_case_the_property_names() {
        let mut runner = TestRunner::deterministic();
        let strategy = case();
        let (mut overflow, mut overrides, mut wrong_variant, mut nested_bytes) =
            (false, false, false, false);
        let (mut fetch_erased, mut scan_erased, mut one_pass, mut per_column) =
            (false, false, false, false);
        for _ in 0..512 {
            let case = strategy.new_tree(&mut runner).expect("generate").current();
            let obj = encode(&case);
            let reader = RlogReader::new(&obj, &cfg()).expect("open");
            let mut scan = reader
                .scan_blocks(&every_row(), &[], &ColumnSelection::all())
                .expect("scan");
            let view = scan.next_block_view(&obj).expect("decode").expect("block");
            let mut raw = Vec::new();
            overflow |= (0..view.surviving_count())
                .any(|i| view.attrs_raw_into(i, &mut raw).expect("attrs_raw") > 0);
            for (stream, attrs) in &case.rows {
                let (resource, scope) = &case.streams[*stream];
                overrides |= attrs
                    .iter()
                    .any(|(k, _)| resource.iter().chain(scope).any(|(sk, _)| sk == k));
                wrong_variant |= attrs
                    .iter()
                    .any(|(k, v)| k == "a" && !matches!(v, AttrValue::I64(_)));
                nested_bytes |= attrs
                    .iter()
                    .any(|(k, v)| k == "c" && matches!(v, AttrValue::List(_) | AttrValue::Map(_)));
            }
            let mut records = {
                let mut rows = reader
                    .scan_blocks(&every_row(), &[], &ColumnSelection::all())
                    .expect("scan");
                rows.next_block(&obj).expect("decode").expect("block")
            };
            let all = records.len();
            retain_log_records(&mut records, &case.fetch_erasure);
            fetch_erased |= records.len() < all;
            let fetched = records.len();
            retain_unerased(&mut records, &case.scan_erasure).expect("erasure");
            scan_erased |= records.len() < fetched;
            let shape = shape(&case);
            let keys = shape
                .projection
                .iter()
                .filter(|&&i| {
                    projected_key(i, &shape.declared, &shape.attr_keys, shape.full_len).is_some()
                })
                .count();
            one_pass |= keys >= ONE_PASS_MIN_KEY_COLUMNS;
            per_column |= (1..ONE_PASS_MIN_KEY_COLUMNS).contains(&keys);
        }
        assert!(overflow, "a block with an attrs_raw cell");
        assert!(overrides, "a record setting a resource or scope key");
        assert!(wrong_variant, "a wrong variant under the I64 declaration");
        assert!(nested_bytes, "a List or Map under the Bytes declaration");
        assert!(fetch_erased, "a row the fetcher's record filter erases");
        assert!(scan_erased, "a row only the merged-view filter erases");
        assert!(one_pass, "a batch resolving key columns in one pass");
        assert!(per_column, "a batch resolving key columns per column");
    }
}
