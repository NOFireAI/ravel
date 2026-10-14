use super::*;

/// Decode a slice of records into one [`RecordBatch`] over `schema`, which is
/// `logs_schema_with_declared(&declared)` projected by `projection`.
///
/// Only the projected columns are built. A column the query did not ask for is
/// never materialized here, which is the second half of the projection story:
/// the reader does not decode its pages ([`resolve_columns`]) and this does not
/// allocate an Arrow array for it. `attrs` is the expensive one to skip -- at a
/// hundred attributes a row it dominated the batch's footprint even for
/// `COUNT(*)`, which projects nothing at all.
///
/// The merged attribute view ([`merged_attrs`]) is computed **once per record**
/// (ADR-0090 decision 5) into `merged`, shared by the `attrs` map arm and every
/// declared-column arm, rather than decoded again per declared column. A
/// declared column (schema index >= [`FIRST_DECLARED_COL`]) is built as a
/// native typed Arrow array from that precomputed view via [`find_attr`]:
/// NULL for an absent key or a variant that does not match the declared type,
/// never a cast (ADR-0090 decision 7), with the one `bytes` normalization of a
/// `List`/`Map` value through [`canonical_value_bytes`].
pub(super) fn build_batch(
    records: &[LogRecord],
    schema: SchemaRef,
    projection: &[usize],
    declared: &[DeclaredColumn],
    attr_keys: &[String],
    full_len: usize,
    row_refs: Option<RowRefRange>,
) -> DFResult<RecordBatch> {
    // Precompute the merged attribute view once per record when any projected
    // column needs it -- the `attrs` map or any declared typed column. Hoisted
    // out of the per-column loop so a query projecting `attrs` and several
    // declared columns decodes each record's stream_attrs blob exactly once.
    let needs_merged = projection
        .iter()
        .any(|&i| i == LOG_COL_ATTRS || i >= FIRST_DECLARED_COL);
    let merged: Vec<Vec<(String, AttrValue)>> = if needs_merged {
        let mut v = Vec::with_capacity(records.len());
        for r in records {
            v.push(merged_attrs(r)?);
        }
        v
    } else {
        Vec::new()
    };

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(projection.len());
    for &i in projection {
        let array: ArrayRef = match i {
            LOG_COL_TS => Arc::new(TimestampNanosecondArray::from(
                records.iter().map(|r| r.ts_ns).collect::<Vec<_>>(),
            )),
            LOG_COL_OBSERVED_TS => Arc::new(TimestampNanosecondArray::from(
                records.iter().map(|r| r.observed_ts_ns).collect::<Vec<_>>(),
            )),
            LOG_COL_SEVERITY_NUM => Arc::new(UInt8Array::from(
                records.iter().map(|r| r.severity_num).collect::<Vec<_>>(),
            )),
            LOG_COL_SEVERITY_TEXT => Arc::new(StringArray::from(
                records
                    .iter()
                    .map(|r| r.severity_text.as_str())
                    .collect::<Vec<_>>(),
            )),
            LOG_COL_BODY => Arc::new(StringArray::from(
                records.iter().map(|r| r.body.as_str()).collect::<Vec<_>>(),
            )),
            LOG_COL_TRACE_ID => {
                let mut trace =
                    FixedSizeBinaryBuilder::with_capacity(records.len(), TRACE_ID_WIDTH);
                for r in records {
                    match &r.trace_id {
                        Some(id) => trace.append_value(id).map_err(|e| {
                            SqlError::Internal(format!("trace_id array build: {e}"))
                        })?,
                        None => trace.append_null(),
                    }
                }
                Arc::new(trace.finish())
            }
            LOG_COL_SPAN_ID => {
                let mut span = FixedSizeBinaryBuilder::with_capacity(records.len(), SPAN_ID_WIDTH);
                for r in records {
                    match &r.span_id {
                        Some(id) => span
                            .append_value(id)
                            .map_err(|e| SqlError::Internal(format!("span_id array build: {e}")))?,
                        None => span.append_null(),
                    }
                }
                Arc::new(span.finish())
            }
            LOG_COL_FLAGS => Arc::new(UInt32Array::from(
                records.iter().map(|r| r.flags).collect::<Vec<_>>(),
            )),
            // `attrs` map: each record's stream-identity (resource + scope)
            // attributes merged with its dynamic per-record attributes, values
            // rendered to text. DataFusion's mandatory `Inexact` residual
            // re-applies `attrs['k'] = 'v'` against this column, and that
            // residual is the sole exactness mechanism, so the column must carry
            // the fully merged view. Populating it from `r.attrs` alone silently
            // dropped every record whose matched attribute was a genuine
            // resource attribute (ADR-0033 amendment). See `merged_attrs`.
            LOG_COL_ATTRS => {
                let mut attrs = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
                for row in &merged {
                    for (k, v) in row {
                        attrs.keys().append_value(k);
                        attrs.values().append_value(attr_value_to_string(v));
                    }
                    attrs
                        .append(true)
                        .map_err(|e| SqlError::Internal(format!("attrs map build: {e}")))?;
                }
                Arc::new(attrs.finish())
            }
            // A synthetic per-key attribute column (issue #1768): index
            // >= full_len selects `attr_keys[i - full_len]`, rendered as `Utf8`
            // straight from the merged view with the map's own rules. This is
            // the row-path (attrs_raw fallback) sibling of the columnar
            // [`build_attr_key_columnar_array`], and it renders each value with
            // the same [`attr_value_to_string`] the `attrs` map arm above uses,
            // so the divergence from a declared column (a non-`Str` value under
            // a `Str` declaration renders as text here, reads NULL there) is
            // reproduced exactly.
            other if other >= full_len => match attr_keys.get(other - full_len) {
                Some(key) => attr_key_column_array(key, &merged),
                None => {
                    return Err(DataFusionError::Internal(format!(
                        "logs scan projection index {other} out of range"
                    )));
                }
            },
            // A declared typed attribute column (ADR-0090 decisions 5-7):
            // index >= FIRST_DECLARED_COL selects `declared[i - FIRST_DECLARED_COL]`.
            // The declared key is still present in the `attrs` map arm above
            // (decision 6, keys stay in the map); this arm additionally
            // materializes it as a native typed column from the same merged
            // view.
            other => match other
                .checked_sub(FIRST_DECLARED_COL)
                .and_then(|k| declared.get(k))
            {
                Some(dc) => declared_column_array(dc, &merged),
                None => {
                    return Err(DataFusionError::Internal(format!(
                        "logs scan projection index {other} out of range"
                    )));
                }
            },
        };
        columns.push(array);
    }
    if let Some(range) = row_refs {
        columns.push(row_ref_array(range, records.len())?);
    }
    debug_assert_eq!(schema.fields().len(), columns.len());
    // The row count is carried explicitly: an empty projection (what a bare
    // `COUNT(*)` asks for, and the cheapest case this change exists to make
    // work) has no column to infer it from, and inferring zero rows there would
    // silently lose every row.
    let options = RecordBatchOptions::new().with_row_count(Some(records.len()));
    RecordBatch::try_new_with_options(schema, columns, &options).map_err(DataFusionError::from)
}

/// Build one declared typed attribute column as a native Arrow array from the
/// per-record precomputed merged views (ADR-0090 decisions 5-7).
///
/// For each record, the key is looked up via [`find_attr`] against that record's
/// merged view. A value whose [`AttrValue`] variant matches the declared type is
/// appended natively; every other case -- an absent key, or a present value of a
/// different variant -- appends NULL, never a cast and never an error. The one
/// exception is a `Bytes`-declared column: a `List`/`Map` value is first
/// normalized to its canonical encoding via [`canonical_value_bytes`] (the same
/// function the write path uses in `ravel_logseg::record::resolve_value`), so a
/// value that fit the object's dynamic-column budget and was stored as a `Bytes`
/// column reads identically to the same logical value that overflowed into
/// `attrs_raw` and decoded back as `List`/`Map`.
fn declared_column_array(dc: &DeclaredColumn, merged: &[Vec<(String, AttrValue)>]) -> ArrayRef {
    match dc.ty {
        // Dictionary-typed to match the fast path's schema (ADR-0099 decision
        // 5): every batch DataFusion validates carries one type per column, so
        // the row path must produce `Dictionary(Int32, Utf8)` too or every
        // fallback batch (erasure pending, `attrs` projected, an `attrs_raw`
        // page) would fail schema validation at runtime. The builder dedups; a
        // wrong variant or absent key is a NULL cell, never a cast (decision 7).
        DeclaredType::Str => {
            let mut b = StringDictionaryBuilder::<Int32Type>::new();
            for row in merged {
                match find_attr(row, &dc.key) {
                    Some(AttrValue::Str(s)) => b.append_value(s),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DeclaredType::I64 => {
            let mut b = Int64Builder::new();
            for row in merged {
                match find_attr(row, &dc.key) {
                    Some(AttrValue::I64(v)) => b.append_value(*v),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DeclaredType::Bool => {
            let mut b = BooleanBuilder::new();
            for row in merged {
                match find_attr(row, &dc.key) {
                    Some(AttrValue::Bool(v)) => b.append_value(*v),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        DeclaredType::Bytes => {
            let mut b = BinaryBuilder::new();
            for row in merged {
                match find_attr(row, &dc.key) {
                    Some(AttrValue::Bytes(bytes)) => b.append_value(bytes),
                    // A record-level `List`/`Map` value that fit the dynamic-
                    // column budget was canonicalized into a `Bytes` column at
                    // write time; one that overflowed decodes back as
                    // `List`/`Map`. Canonicalize the latter here so both storage
                    // locations produce the identical `bytes` value (decision 7).
                    Some(v @ (AttrValue::List(_) | AttrValue::Map(_))) => {
                        b.append_value(canonical_value_bytes(v))
                    }
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
    }
}

/// Build one synthetic per-key attribute column as a `Utf8` array from the
/// per-record precomputed merged views (issue #1768), the row-path sibling of
/// [`build_attr_key_columnar_array`].
///
/// For each record the key is looked up in that record's merged view via
/// [`find_attr`] and rendered to text with [`attr_value_to_string`], exactly as
/// the `attrs` map column renders it; an absent key is NULL. This is what makes
/// `attrs['k']` diverge from a declared `Str` column `"k"` by design: a record
/// holding a non-`Str` value under the key renders as text here (`7`) while the
/// declared column reads NULL. The row path runs only for the `attrs_raw`
/// overflow fallback of an otherwise-columnar query; the common case is
/// [`build_attr_key_columnar_array`].
fn attr_key_column_array(key: &str, merged: &[Vec<(String, AttrValue)>]) -> ArrayRef {
    let mut b = StringBuilder::new();
    for row in merged {
        match find_attr(row, key) {
            Some(v) => b.append_value(attr_value_to_string(v)),
            None => b.append_null(),
        }
    }
    Arc::new(b.finish())
}

// ---------------------------------------------------------------------------
// Columnar fast path (ADR-0099 decision 2)
// ---------------------------------------------------------------------------

/// A FIELD_DIR column of a declared key, resolved once for the whole block into
/// a per-column cursor (#875). The scan's row loop read one cell with one
/// `HashMap<u32, _>` lookup per cell; the cursor resolves the column's storage
/// once and then indexes the resolved slice per row with no lookup.
enum DeclaredCursor<'a> {
    Str(StrCursor<'a>),
    Bytes(BytesCursor<'a>),
    I64(I64Cursor<'a>),
    F64(F64BitsCursor<'a>),
    Bool(BoolCursor<'a>),
}

#[cfg(test)]
thread_local! {
    /// UTF-8 validations [`cell_text`] ran on this thread, so a test can pin how
    /// often a declared `Str` cell's bytes are validated.
    static CELL_TEXT_VALIDATIONS: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// A `Str` cell's bytes as text, or `None` when they are not UTF-8. The
/// columnar path validates every plain-page attribute `Str` cell through here;
/// a dictionary page's entries were validated when the reader decoded them.
fn cell_text(bytes: &[u8]) -> Option<&str> {
    #[cfg(test)]
    CELL_TEXT_VALIDATIONS.with(|n| n.set(n.get() + 1));
    std::str::from_utf8(bytes).ok()
}

/// A `Str` FIELD_DIR column resolved once per block, read so that a declared
/// `Str` build validates each cell's UTF-8 at most once per block (ADR-2121 D3).
///
/// On a dictionary page every entry's UTF-8 was decided once, when the reader
/// decoded the dictionary ([`DictValues`]), so a row's presence is its key plus
/// that entry's validity bit and costs no validation. A plain page validates
/// the cell on each [`text_at`](Self::text_at) call, so a caller that needs
/// both presence and text reads it once and keeps the result ([`winner_at`]).
struct StrCursor<'a> {
    cells: BytesCursor<'a>,
    /// The page's keys and shared dictionary, `Some` only for a dictionary
    /// page.
    dict: Option<StrDictColumn<'a>>,
}

impl<'a> StrCursor<'a> {
    fn resolve(view: &ColumnarBlockView<'a>, column_id: u32) -> Self {
        StrCursor {
            cells: view.bytes_cursor(column_id),
            dict: view.str_dict(column_id),
        }
    }

    /// The dictionary key of surviving row `i` when the row has a value and
    /// its entry is UTF-8; `None` on a plain page.
    fn dict_key(&self, i: usize) -> Option<i32> {
        let d = self.dict.as_ref()?;
        d.key_at(i)
            .filter(|&k| usize::try_from(k).is_ok_and(|k| d.values().is_valid(k)))
    }

    /// The cell at surviving row `i` as text, or `None` when absent or not
    /// UTF-8. Treating invalid UTF-8 as no value matches the row path
    /// (`String::from_utf8(..).ok()`), which lets a resource/scope fallback show
    /// through.
    fn text_at(&self, i: usize) -> Option<&'a str> {
        match &self.dict {
            Some(d) => d.values().text(usize::try_from(d.key_at(i)?).ok()?),
            None => cell_text(self.cells.at(i)?),
        }
    }

    /// Whether the record sets the key in this column at surviving row `i`. A
    /// plain page's answer carries the text it validated; a dictionary page's
    /// is a key and validity-bit lookup.
    fn presence_at(&self, i: usize) -> Presence<'a> {
        if self.dict.is_some() {
            return if self.dict_key(i).is_some() {
                Presence::Present
            } else {
                Presence::Absent
            };
        }
        self.text_at(i).map_or(Presence::Absent, Presence::Text)
    }
}

/// The Arrow values of each shared string dictionary one partition's scan has
/// read (ADR-2773 decision D3), found by the dictionary's `Arc` identity, so a
/// row group dictionary becomes one `StringArray` however many of its blocks
/// and chunks read it, and every chunk's `DictionaryArray` shares that array.
/// An entry is dropped once the cache holds the last reference to its
/// dictionary: the reader has moved past its row group. An extended array
/// (the base with fallback values appended) is dropped then too, or as soon
/// as the cache holds its last reference: no batch reads it any more.
///
/// So at each [`Self::live_bytes`] call the cache holds, per dictionary still
/// being read, its base values plus one extended array per distinct sorted
/// fallback value list that a live batch or a block being built still reads.
pub(super) struct StrValuesCache {
    pub(super) entries: Vec<SharedStrValues>,
    /// [`BlockMetrics::str_dict_values_built`].
    built: Count,
}

pub(super) struct SharedStrValues {
    dict: Arc<DictValues>,
    values: ArrayRef,
    /// The values with a block's resource/scope fallback values appended past
    /// them, keyed by those values sorted and deduplicated
    /// ([`canonical_fallbacks`]).
    extended: Vec<(Vec<String>, ArrayRef)>,
}

impl StrValuesCache {
    pub(super) fn new(built: Count) -> Self {
        StrValuesCache {
            entries: Vec::new(),
            built,
        }
    }

    /// `dict`'s entries as an Arrow `StringArray`, a non-UTF-8 entry as NULL,
    /// with `extra` appended past them. Built on the first request for that
    /// dictionary and `extra`; every later request returns the same array.
    fn values(&mut self, dict: &Arc<DictValues>, extra: Vec<String>) -> ArrayRef {
        let at = match self.entries.iter().position(|e| Arc::ptr_eq(&e.dict, dict)) {
            Some(at) => at,
            None => {
                self.retain_live();
                self.built.add(1);
                self.entries.push(SharedStrValues {
                    dict: Arc::clone(dict),
                    values: dict_values_array(dict, &[]),
                    extended: Vec::new(),
                });
                self.entries.len() - 1
            }
        };
        let entry = &mut self.entries[at];
        if extra.is_empty() {
            return Arc::clone(&entry.values);
        }
        if let Some((_, values)) = entry.extended.iter().find(|(e, _)| *e == extra) {
            return Arc::clone(values);
        }
        self.built.add(1);
        let values = dict_values_array(dict, &extra);
        entry.extended.push((extra, Arc::clone(&values)));
        values
    }

    fn retain_live(&mut self) {
        self.entries.retain(|e| Arc::strong_count(&e.dict) > 1);
        for entry in &mut self.entries {
            entry.extended.retain(|(_, v)| Arc::strong_count(v) > 1);
        }
    }

    /// The Arrow bytes of every array the live entries hold, after dropping
    /// the entries whose row group the reader has moved past.
    pub(super) fn live_bytes(&mut self) -> usize {
        self.retain_live();
        self.arrays().map(|a| a.get_array_memory_size()).sum()
    }

    /// Whether `values` is one of the arrays this cache holds.
    fn owns(&self, values: &ArrayRef) -> bool {
        self.arrays().any(|a| Arc::ptr_eq(a, values))
    }

    fn arrays(&self) -> impl Iterator<Item = &ArrayRef> {
        self.entries
            .iter()
            .flat_map(|e| std::iter::once(&e.values).chain(e.extended.iter().map(|(_, v)| v)))
    }
}

/// Each of one block's columnar batches paired with its pool charge, and the
/// charge for the dictionary values its batches share.
///
/// A batch is charged its Arrow footprint less the values of each dictionary
/// column. Those values are shared by every chunk of the block, and a row group
/// dictionary's by every block of its group, so `get_array_memory_size` would
/// count them once per batch. A values array the scan's [`StrValuesCache`]
/// holds is charged once per cache entry, by [`LogScanStream::hold_batches`];
/// any other is charged once for the block, in the returned total.
pub(super) fn columnar_charges(
    batches: Vec<RecordBatch>,
    cache: &StrValuesCache,
) -> (VecDeque<(RecordBatch, usize)>, usize) {
    use datafusion::arrow::array::{Array, AsArray};
    let mut seen: Vec<ArrayRef> = Vec::new();
    let mut shared = 0;
    let charged = batches
        .into_iter()
        .map(|batch| {
            let mut charge = 0;
            for column in batch.columns() {
                let bytes = column.get_array_memory_size();
                let Some(dict) = column.as_any_dictionary_opt() else {
                    charge += bytes;
                    continue;
                };
                let values = dict.values();
                let values_bytes = values.get_array_memory_size();
                charge += bytes.saturating_sub(values_bytes);
                if !cache.owns(values) && !seen.iter().any(|v| Arc::ptr_eq(v, values)) {
                    shared += values_bytes;
                    seen.push(Arc::clone(values));
                }
            }
            (batch, charge)
        })
        .collect();
    (charged, shared)
}

/// A partition scan's [`StrValuesCache`], shared by the block jobs it submits.
pub(super) type SharedStrValuesCache = Arc<std::sync::Mutex<StrValuesCache>>;

pub(super) fn lock_str_values(
    cache: &SharedStrValuesCache,
) -> std::sync::MutexGuard<'_, StrValuesCache> {
    cache
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// `dict`'s entries then `extra` as one `StringArray`, a non-UTF-8 entry as
/// NULL. The entries' text was validated once when the dictionary decoded.
fn dict_values_array(dict: &DictValues, extra: &[String]) -> ArrayRef {
    let extra = extra.iter().map(String::as_str);
    if dict.all_valid() {
        Arc::new(StringArray::from_iter_values(
            dict.texts().flatten().chain(extra),
        ))
    } else {
        Arc::new(StringArray::from_iter(dict.texts().chain(extra.map(Some))))
    }
}

/// Whether a cursor sets the key at a row, and for a `Str` cursor the text its
/// presence test already validated.
enum Presence<'a> {
    Absent,
    Present,
    Text(&'a str),
}

impl<'a> DeclaredCursor<'a> {
    fn resolve(view: &ColumnarBlockView<'a>, col: AttrColumn) -> Self {
        match col.ty {
            FieldType::Str => DeclaredCursor::Str(StrCursor::resolve(view, col.column_id)),
            FieldType::Bytes => DeclaredCursor::Bytes(view.bytes_cursor(col.column_id)),
            FieldType::I64 => DeclaredCursor::I64(view.i64_cursor(col.column_id)),
            FieldType::F64 => DeclaredCursor::F64(view.f64_bits_cursor(col.column_id)),
            FieldType::Bool => DeclaredCursor::Bool(view.bool_cursor(col.column_id)),
        }
    }

    /// Whether the record sets the key in this column at surviving row `i`, with
    /// the same "readable" rule the row path uses: an invalid-UTF-8 `Str` cell is
    /// not a value (matches [`read_str_cell`]), so it must not suppress the
    /// resource/scope fallback. A `Str` cell's validated text is returned with
    /// the answer so the caller does not validate it again.
    fn presence_at(&self, i: usize) -> Presence<'a> {
        let present = match self {
            DeclaredCursor::Str(c) => return c.presence_at(i),
            DeclaredCursor::Bytes(c) => c.at(i).is_some(),
            DeclaredCursor::I64(c) => c.at(i).is_some(),
            DeclaredCursor::F64(c) => c.at(i).is_some(),
            DeclaredCursor::Bool(c) => c.at(i).is_some(),
        };
        if present {
            Presence::Present
        } else {
            Presence::Absent
        }
    }

    /// The cell at surviving row `i` as an [`AttrValue`], or `None` when NULL (or,
    /// for `Str`, not UTF-8). Used for the multi-occurrence non-`Str` builders and
    /// the synthetic per-key columns; the `Str` builder reads `&str` directly so
    /// it never allocates a throwaway `String` (#875).
    fn value_at(&self, i: usize) -> Option<AttrValue> {
        match self {
            DeclaredCursor::Str(c) => c.text_at(i).map(|s| AttrValue::Str(s.to_string())),
            DeclaredCursor::Bytes(c) => c.at(i).map(|b| AttrValue::Bytes(b.to_vec())),
            DeclaredCursor::I64(c) => c.at(i).map(AttrValue::I64),
            DeclaredCursor::F64(c) => c.at(i).map(|bits| AttrValue::F64(f64::from_bits(bits))),
            DeclaredCursor::Bool(c) => c.at(i).map(AttrValue::Bool),
        }
    }
}

/// The index into `cols`/`cursors` of the record's WINNING occurrence of a key
/// at surviving row `i`, or `None` when the record does not set the key, plus
/// the winner's text when it is a `Str` cursor (validated once, by the presence
/// test).
///
/// The highest FIELD_DIR type byte wins, and among equal type bytes the last
/// one, the tie-break `max_by_key` gives; see [`DeclaredPlan::winning_idx`] for
/// the rule this implements.
fn winner_at<'a>(
    cols: &[AttrColumn],
    cursors: &[DeclaredCursor<'a>],
    i: usize,
) -> Option<(usize, Option<&'a str>)> {
    let mut best: Option<(usize, u8, Option<&'a str>)> = None;
    for (k, (col, cursor)) in cols.iter().zip(cursors).enumerate() {
        let text = match cursor.presence_at(i) {
            Presence::Absent => continue,
            Presence::Present => None,
            Presence::Text(s) => Some(s),
        };
        let ty = col.ty.to_u8();
        if best.is_none_or(|(_, b, _)| ty >= b) {
            best = Some((k, ty, text));
        }
    }
    best.map(|(k, _, text)| (k, text))
}

/// One declared column's FIELD_DIR resolution for a block, done once rather than
/// per row (ADR-0099 decision 2). Every FIELD_DIR column of the key is resolved
/// to a cursor once when the plan is built; the row loop then reads through the
/// cursors with no per-cell column lookup (#875).
struct DeclaredPlan<'d, 'a> {
    dc: &'d DeclaredColumn,
    /// The raw FIELD_DIR columns of this key, across all stored types. Kept for
    /// their type bytes, which order the occurrences ([`winner_at`]).
    cols: Vec<AttrColumn>,
    /// Cursors parallel to [`Self::cols`], resolved once for the block.
    cursors: Vec<DeclaredCursor<'a>>,
    /// Index into [`Self::cols`]/[`Self::cursors`] of the declared-type column,
    /// if the key has one. A record row whose value lives here reads that value;
    /// a row whose value lives in a different-typed column of the same key reads
    /// NULL (record wins, wrong variant), matching the row path exactly.
    matching_idx: Option<usize>,
}

impl<'d, 'a> DeclaredPlan<'d, 'a> {
    fn build(view: &ColumnarBlockView<'a>, dc: &'d DeclaredColumn) -> DeclaredPlan<'d, 'a> {
        let cols: Vec<AttrColumn> = view.attr_columns_for(&dc.key).collect();
        let declared_ty = declared_field_type(dc.ty);
        let matching_idx = cols.iter().position(|c| c.ty == declared_ty);
        let cursors = cols
            .iter()
            .map(|&c| DeclaredCursor::resolve(view, c))
            .collect();
        DeclaredPlan {
            dc,
            cols,
            cursors,
            matching_idx,
        }
    }

    /// The cursor over the declared-type column, if any.
    fn matching_cursor(&self) -> Option<&DeclaredCursor<'a>> {
        self.matching_idx.map(|k| &self.cursors[k])
    }

    /// True when the key's only FIELD_DIR column is the declared-type one, so a
    /// single cursor read is both the presence answer and the value: the
    /// double read (`attr_present` then `read_typed_cell`) collapses to one
    /// (deliverable 2, #875).
    fn single_matching(&self) -> bool {
        self.cursors.len() == 1 && self.matching_idx == Some(0)
    }

    /// The index into [`Self::cols`]/[`Self::cursors`] of the record's WINNING
    /// occurrence of the key at surviving row `i`, or `None` when the record does
    /// not set the key at all.
    ///
    /// A record that carries one name in several FIELD_DIR columns (a name
    /// written as both a string and an integer splits into two columns) resolves
    /// to one value, and which one is fixed by docs/log-segment-format.md
    /// ("Within the record layer"): `rebuild_record` lays a record's columnar
    /// occurrences out ascending by FIELD_DIR type byte and `merged_attrs` folds
    /// that list last-wins, so the occurrence with the highest type byte wins.
    /// The `attrs_raw` overflow tier, which beats every columnar occurrence,
    /// cannot appear here: a block carrying an `attrs_raw` page falls the whole
    /// segment back to the row path before the fast path builds a column.
    ///
    /// `max_by_key` returns the LAST maximum, which is the tie-break the same
    /// paragraph pins; the type byte is read from [`Self::cols`] rather than
    /// from FIELD_DIR position, so the answer does not depend on the directory's
    /// sort order.
    ///
    /// The fused `single_matching` reads get the same answer from the single
    /// matching read.
    fn winning_idx(&self, i: usize) -> Option<usize> {
        self.winner(i).map(|(k, _)| k)
    }

    /// [`winning_idx`](Self::winning_idx) plus the winner's text when it is the
    /// `Str` cursor, as its presence test validated it ([`winner_at`]).
    fn winner(&self, i: usize) -> Option<(usize, Option<&'a str>)> {
        winner_at(&self.cols, &self.cursors, i)
    }
}

/// The [`FieldType`] a declared column resolves its FIELD_DIR column at. A
/// `match` (not a two-arm `if`) so a future declared `f64` (ADR-0090, deferred)
/// slots in as one more arm rather than silently falling through.
fn declared_field_type(ty: DeclaredType) -> FieldType {
    match ty {
        DeclaredType::Str => FieldType::Str,
        DeclaredType::I64 => FieldType::I64,
        DeclaredType::Bool => FieldType::Bool,
        DeclaredType::Bytes => FieldType::Bytes,
    }
}

/// The block's per-`stream_ref` decoded resource/scope scalar attributes, cached
/// so a block's streams are each decoded once even though the fallback is a
/// per-row lookup. This is the fast path's only stream-blob decode, reached only
/// for a declared key a record row does not set in a FIELD_DIR column; a query
/// whose declared keys are all record attributes never enters it.
fn resource_attrs<'c>(
    view: &ColumnarBlockView<'_>,
    cache: &'c mut HashMap<u32, Arc<Vec<(String, AttrValue)>>>,
    stream_ref: u32,
) -> DFResult<&'c Arc<Vec<(String, AttrValue)>>> {
    match cache.entry(stream_ref) {
        std::collections::hash_map::Entry::Occupied(e) => Ok(e.into_mut()),
        std::collections::hash_map::Entry::Vacant(e) => {
            let blob = view.stream_attrs_of(stream_ref).ok_or_else(|| {
                DataFusionError::from(SqlError::CorruptStreamAttrs(
                    "columnar fast path: stream_ref has no STREAM_DIR entry".to_string(),
                ))
            })?;
            let decoded = Arc::new(decode_stream_attrs(blob)?);
            Ok(e.insert(decoded))
        }
    }
}

/// Per-column, per-block resolver for a declared key's merged value under the
/// same record-wins-over-resource precedence the row path's [`merged_attrs`] +
/// [`find_attr`] produce.
///
/// Three parts of the merge that do not vary by row are resolved once here rather
/// than in the per-row read. [`DeclaredPlan::single_matching`] and the
/// declared-type cursor ([`DeclaredPlan::matching_cursor`]) are plan-invariant
/// and become fields. The resource/scope fallback ([`find_attr`] over the decoded
/// stream attributes) returns the same answer for every row sharing a
/// `stream_ref`, so it is memoized per `stream_ref`: its scan and the one clone
/// of its result run once per distinct stream in the block, not once per row.
///
/// The precedence is ADR-0090 decision 7: the record's own value wins if it sets
/// the key in any FIELD_DIR column; a record whose winning occurrence
/// ([`DeclaredPlan::winning_idx`]) is at a different type yields NULL and does
/// NOT consult the fallback; only a record that does not set the key at all
/// reads the resource/scope value. The `single_matching` fast path still treats
/// a `Str` cell that is not valid UTF-8 as absent, falling through to the
/// fallback.
struct DeclaredResolver<'p, 'd, 'a> {
    plan: &'p DeclaredPlan<'d, 'a>,
    /// [`DeclaredPlan::single_matching`], resolved once for the block.
    single_matching: bool,
    /// [`DeclaredPlan::matching_cursor`], resolved once for the block.
    matching_cursor: Option<&'p DeclaredCursor<'a>>,
    /// Memo of the resource/scope fallback value per `stream_ref`, so the
    /// [`find_attr`] scan runs once per distinct stream, not once per row.
    fallback: HashMap<u32, Option<AttrValue>>,
    /// How many times the fallback scan actually ran: exactly the number of
    /// distinct stream refs that reached the fallback. Pinned by the
    /// memoization test.
    resolve_count: usize,
    /// The scan's shared-dictionary values, read by a declared `Str` build.
    str_values: SharedStrValuesCache,
    /// A declared `Str` column's keys and values for every surviving row of
    /// the block, built by the block's first chunk and sliced by every chunk.
    str_block: Option<StrBlock>,
}

/// One block's declared `Str` column ([`build_declared_str_columnar`]).
struct StrBlock {
    keys: Int32Array,
    values: ArrayRef,
}

impl<'p, 'd, 'a> DeclaredResolver<'p, 'd, 'a> {
    /// A resolver with a values cache of its own, which lives for this block.
    #[cfg(test)]
    fn new(plan: &'p DeclaredPlan<'d, 'a>) -> Self {
        Self::with_str_values(
            plan,
            Arc::new(std::sync::Mutex::new(StrValuesCache::new(Count::new()))),
        )
    }

    fn with_str_values(plan: &'p DeclaredPlan<'d, 'a>, str_values: SharedStrValuesCache) -> Self {
        DeclaredResolver {
            plan,
            single_matching: plan.single_matching(),
            matching_cursor: plan.matching_cursor(),
            fallback: HashMap::new(),
            resolve_count: 0,
            str_values,
            str_block: None,
        }
    }

    /// The resource/scope fallback value for `stream_ref`, memoized. The
    /// [`find_attr`] scan and the one clone of its result run once per distinct
    /// stream; every later row of the same stream reads the memo.
    fn fallback_value(
        &mut self,
        view: &ColumnarBlockView<'_>,
        cache: &mut HashMap<u32, Arc<Vec<(String, AttrValue)>>>,
        stream_ref: u32,
    ) -> DFResult<&Option<AttrValue>> {
        match self.fallback.entry(stream_ref) {
            std::collections::hash_map::Entry::Occupied(e) => Ok(e.into_mut()),
            std::collections::hash_map::Entry::Vacant(e) => {
                let resource = resource_attrs(view, cache, stream_ref)?;
                let value = find_attr(resource, &self.plan.dc.key).cloned();
                self.resolve_count += 1;
                Ok(e.insert(value))
            }
        }
    }

    /// The cursor over the key's one FIELD_DIR column when
    /// [`DeclaredPlan::single_matching`] holds for the block, else `None`.
    fn single_cursor(&self) -> Option<&'p DeclaredCursor<'a>> {
        self.matching_cursor.filter(|_| self.single_matching)
    }

    /// The resource/scope value for surviving row `i`'s stream, for a row the
    /// record does not set the key at; `None` when the stream has none.
    fn fallback_at(
        &mut self,
        view: &ColumnarBlockView<'_>,
        i: usize,
        cache: &mut HashMap<u32, Arc<Vec<(String, AttrValue)>>>,
    ) -> DFResult<Option<&AttrValue>> {
        let Some(stream_ref) = view.stream_ref(i) else {
            return Ok(None);
        };
        Ok(self.fallback_value(view, cache, stream_ref)?.as_ref())
    }

    /// The merged value of the declared key at surviving row `i`. Returns the
    /// record-column value when the record's WINNING occurrence of the key
    /// ([`DeclaredPlan::winning_idx`]) is at the declared type; `None` when that
    /// winner is at a different type (wrong variant, NULL by ADR-0090 decision
    /// 7), including the case where the record also has a declared-type cell at
    /// this row that the winner shadows. Only when the record does not set the
    /// key at all is the resource/scope fallback consulted, whose variant the
    /// caller checks against the declared type.
    fn merged_value(
        &mut self,
        view: &ColumnarBlockView<'_>,
        i: usize,
        cache: &mut HashMap<u32, Arc<Vec<(String, AttrValue)>>>,
    ) -> DFResult<Option<AttrValue>> {
        if self.single_matching {
            // Fused: the one matching-column read is both the presence test and
            // the value (deliverable 2, #875). `value_at` is `Some` exactly when
            // the record sets the key at the declared type; `None` (absent, or a
            // `Str` cell that is not UTF-8) falls through to the resource/scope
            // value, matching the row path.
            if let Some(v) = self.matching_cursor.and_then(|c| c.value_at(i)) {
                return Ok(Some(v));
            }
        } else if let Some(win) = self.plan.winning_idx(i) {
            // Record wins. Its value is the cell of the occurrence that wins the
            // record layer's last-occurrence-wins order
            // ([`DeclaredPlan::winning_idx`]), or NULL when that winner is of a
            // type other than the declared one (ADR-0090 decision 7); either way
            // the fallback is not consulted.
            return Ok(if Some(win) == self.plan.matching_idx {
                self.plan.cursors[win].value_at(i)
            } else {
                None
            });
        }
        Ok(self.fallback_at(view, i, cache)?.cloned())
    }
}

/// Append a merged `I64` value, or NULL for an absent value or another variant.
fn append_i64(b: &mut Int64Builder, v: Option<&AttrValue>) {
    match v {
        Some(AttrValue::I64(v)) => b.append_value(*v),
        _ => b.append_null(),
    }
}

/// Append a merged `Bool` value, or NULL for an absent value or another variant.
fn append_bool(b: &mut BooleanBuilder, v: Option<&AttrValue>) {
    match v {
        Some(AttrValue::Bool(v)) => b.append_value(*v),
        _ => b.append_null(),
    }
}

/// Append a merged `Bytes` value, or NULL for an absent value or another
/// variant.
fn append_bytes(b: &mut BinaryBuilder, v: Option<&AttrValue>) {
    match v {
        Some(AttrValue::Bytes(bytes)) => b.append_value(bytes),
        // Parity with the row path: a resource/scope `List`/`Map` value is
        // canonicalized. In the eligible (no `attrs_raw`) case a record's
        // `List`/`Map` is already stored as a canonicalized `Bytes` column, and
        // `decode_stream_attrs` omits nested resource values, so this arm is
        // effectively dead here; it is kept identical to `declared_column_array`.
        Some(v @ (AttrValue::List(_) | AttrValue::Map(_))) => {
            b.append_value(canonical_value_bytes(v))
        }
        _ => b.append_null(),
    }
}

/// Build one declared typed attribute column for surviving rows `start..end`
/// straight from the view (ADR-0099 decision 2). Byte-identical to
/// [`declared_column_array`] over the same input: a value whose variant matches
/// the declared type is appended natively, and every other case -- absent key,
/// or a value of a different variant -- appends NULL, never a cast (ADR-0090
/// decision 7). The `Bytes` arm applies the same `List`/`Map` canonicalization.
///
/// The `match` on the declared type mirrors [`declared_column_array`], so a
/// future declared `f64` slots in as one arm on both paths.
///
/// When [`DeclaredPlan::single_matching`] holds, the `I64`, `Bool` and `Bytes`
/// arms read each row's cell straight from the one cursor (ADR-2121 D2): a
/// present cell is appended with no [`AttrValue`], and only an absent cell
/// takes the resource/scope fallback, the order
/// [`DeclaredResolver::merged_value`] follows. Every other block goes through
/// `merged_value` per cell.
fn build_declared_columnar_array(
    view: &ColumnarBlockView<'_>,
    resolver: &mut DeclaredResolver<'_, '_, '_>,
    start: usize,
    end: usize,
    cache: &mut HashMap<u32, Arc<Vec<(String, AttrValue)>>>,
) -> DFResult<ArrayRef> {
    let plan = resolver.plan;
    let single = resolver.single_cursor();
    Ok(match plan.dc.ty {
        DeclaredType::Str => build_declared_str_columnar(view, resolver, start, end, cache)?,
        DeclaredType::I64 => {
            if let Some(DeclaredCursor::I64(cursor)) = single {
                return declared_i64_from_buffers(
                    view,
                    resolver,
                    &cursor.values(),
                    start,
                    end,
                    cache,
                );
            }
            let mut b = Int64Builder::with_capacity(end - start);
            for i in start..end {
                append_i64(&mut b, resolver.merged_value(view, i, cache)?.as_ref());
            }
            Arc::new(b.finish())
        }
        DeclaredType::Bool => {
            let mut b = BooleanBuilder::with_capacity(end - start);
            if let Some(DeclaredCursor::Bool(c)) = single {
                for i in start..end {
                    match c.at(i) {
                        Some(v) => b.append_value(v),
                        None => append_bool(&mut b, resolver.fallback_at(view, i, cache)?),
                    }
                }
            } else {
                for i in start..end {
                    append_bool(&mut b, resolver.merged_value(view, i, cache)?.as_ref());
                }
            }
            Arc::new(b.finish())
        }
        DeclaredType::Bytes => {
            let mut b = BinaryBuilder::new();
            if let Some(DeclaredCursor::Bytes(c)) = single {
                for i in start..end {
                    match c.at(i) {
                        Some(bytes) => b.append_value(bytes),
                        None => append_bytes(&mut b, resolver.fallback_at(view, i, cache)?),
                    }
                }
            } else {
                for i in start..end {
                    append_bytes(&mut b, resolver.merged_value(view, i, cache)?.as_ref());
                }
            }
            Arc::new(b.finish())
        }
    })
}

/// The declared `I64` arm of [`build_declared_columnar_array`] when the key's
/// only FIELD_DIR column is the `I64` one: the record's cells come from the
/// column's buffers through [`int_column_array`], and only a row the record
/// does not set reads the resource/scope fallback, the same order the
/// per-cell build follows. The array is rebuilt only when a fallback supplies
/// a value.
fn declared_i64_from_buffers(
    view: &ColumnarBlockView<'_>,
    resolver: &mut DeclaredResolver<'_, '_, '_>,
    values: &ravel_logseg::I64Values<'_>,
    start: usize,
    end: usize,
    cache: &mut HashMap<u32, Arc<Vec<(String, AttrValue)>>>,
) -> DFResult<ArrayRef> {
    use datafusion::arrow::array::{Array, Int64Array};
    let array: Int64Array = int_column_array(values, start, end, true)?;
    if array.null_count() == 0 {
        return Ok(Arc::new(array));
    }
    let mut filled: Vec<(usize, i64)> = Vec::new();
    for k in 0..array.len() {
        if array.is_null(k)
            && let Some(AttrValue::I64(v)) = resolver.fallback_at(view, start + k, cache)?
        {
            filled.push((k, *v));
        }
    }
    if filled.is_empty() {
        return Ok(Arc::new(array));
    }
    let mut b = Int64Builder::with_capacity(array.len());
    let mut fill = filled.into_iter().peekable();
    for k in 0..array.len() {
        match fill.next_if(|&(at, _)| at == k) {
            Some((_, v)) => b.append_value(v),
            None if array.is_null(k) => b.append_null(),
            None => b.append_value(array.value(k)),
        }
    }
    Ok(Arc::new(b.finish()))
}

/// Build a declared `Str` column as an Arrow `Dictionary(Int32, Utf8)` for
/// surviving rows `start..end` (ADR-0099 decision 5, ADR-2773 decision D3).
///
/// The block's first chunk builds the column's keys for every surviving row of
/// the block and its one values array ([`build_str_block`]); every chunk
/// copies its rows' keys ([`chunk_keys`]) over that values array, so no chunk
/// copies a string.
/// The logical values are the ones [`declared_column_array`]'s `Str` arm
/// produces on the row path, so a fast-path batch and a fallback batch
/// validate against the one schema DataFusion checks.
fn build_declared_str_columnar(
    view: &ColumnarBlockView<'_>,
    resolver: &mut DeclaredResolver<'_, '_, '_>,
    start: usize,
    end: usize,
    cache: &mut HashMap<u32, Arc<Vec<(String, AttrValue)>>>,
) -> DFResult<ArrayRef> {
    let block = match resolver.str_block.take() {
        Some(block) => block,
        None => build_str_block(view, resolver, cache)?,
    };
    let built = if start <= end && end <= block.keys.len() {
        DictionaryArray::<Int32Type>::try_new(
            chunk_keys(&block.keys, start, end),
            Arc::clone(&block.values),
        )
        .map(|dict| Arc::new(dict) as ArrayRef)
        .map_err(DataFusionError::from)
    } else {
        Err(DataFusionError::Internal(format!(
            "declared Str rows {start}..{end} outside the block's {} rows",
            block.keys.len()
        )))
    };
    resolver.str_block = Some(block);
    built
}

/// The keys of rows `start..end` (in bounds) in buffers of their own, so a
/// chunk's footprint counts its own keys and not the whole block's. A chunk
/// covering the block shares the block's buffers.
fn chunk_keys(keys: &Int32Array, start: usize, end: usize) -> Int32Array {
    use datafusion::arrow::array::Array;
    if start == 0 && end == keys.len() {
        return keys.clone();
    }
    let slice = keys.slice(start, end - start);
    let nulls = slice
        .nulls()
        .filter(|n| n.null_count() > 0)
        .map(|n| NullBuffer::new(n.iter().collect()));
    Int32Array::new(slice.values().iter().copied().collect(), nulls)
}

/// The declared `Str` column for every surviving row of the block.
///
/// - **Dictionary page** ([`ColumnarBlockView::str_dict`] returns `Some`): the
///   values are the page's shared dictionary, built into a `StringArray` once
///   per dictionary for the scan ([`StrValuesCache`]), a non-UTF-8 entry
///   NULL. A row the record sets in the `Str` column keeps its page key, and
///   no key points at a NULL entry: a non-UTF-8 entry is no value, so the row
///   reads the fallback below. A row whose winning occurrence
///   ([`DeclaredPlan::winning_idx`]) is of another type is NULL (ADR-0090
///   decision 7).
/// - **Plain page** (`str_dict` returns `None`, or the key has no `Str`
///   FIELD_DIR column at all): the values are the record's present cells in
///   row order, each validated once, so their keys are the identity.
///
/// A row the record does not set reads the resource/scope fallback, whose
/// value is appended past the record's values: on a plain page once per
/// distinct stream in the block, on a dictionary page once per distinct value,
/// sorted ([`canonical_fallbacks`]).
fn build_str_block(
    view: &ColumnarBlockView<'_>,
    resolver: &mut DeclaredResolver<'_, '_, '_>,
    cache: &mut HashMap<u32, Arc<Vec<(String, AttrValue)>>>,
) -> DFResult<StrBlock> {
    let n = view.surviving_count();
    let plan = resolver.plan;
    let single_matching = resolver.single_matching;
    // The declared-type (`Str`) column's cursor; `None` when the key has no
    // `Str` column at all (then only the resource/scope fallback yields a value).
    let matching_str = match plan.matching_cursor() {
        Some(DeclaredCursor::Str(c)) => Some(c),
        _ => None,
    };
    let mut keys: Vec<i32> = Vec::with_capacity(n);
    let mut nulls = NullBufferBuilder::new(n);
    let mut fallbacks = StrFallbacks::default();
    let values = match matching_str.and_then(|c| c.dict.as_ref().map(|d| (c, d))) {
        Some((cursor, dict)) => {
            let shared = dict.values();
            let base = i32::try_from(shared.len()).map_err(|_| {
                DataFusionError::Internal("declared Str dictionary exceeds i32 keys".into())
            })?;
            for i in 0..n {
                let record = if single_matching {
                    cursor.dict_key(i).map(Some)
                } else {
                    plan.winning_idx(i).map(|win| {
                        (Some(win) == plan.matching_idx)
                            .then(|| cursor.dict_key(i))
                            .flatten()
                    })
                };
                let key = match record {
                    Some(key) => key,
                    // A placeholder below zero until the fallback values are
                    // all known: fallback slot `s` is key `-1 - s`.
                    None => fallbacks.slot(view, resolver, cache, i)?.map(|s| -1 - s),
                };
                push_key(&mut keys, &mut nulls, key);
            }
            let (extra, slots) = canonical_fallbacks(fallbacks.values)?;
            for k in keys.iter_mut().filter(|k| **k < 0) {
                let slot = usize::try_from(-1 - *k)
                    .ok()
                    .and_then(|s| slots.get(s).copied())
                    .ok_or_else(|| {
                        DataFusionError::Internal("declared Str fallback slot out of range".into())
                    })?;
                *k = base + slot;
            }
            lock_str_values(&resolver.str_values).values(shared, extra)
        }
        None => {
            let mut texts: Vec<&str> = Vec::with_capacity(n);
            for i in 0..n {
                let record = if single_matching {
                    // A `None` here (absent, or not UTF-8) falls through to
                    // the fallback, matching the row path.
                    matching_str.and_then(|c| c.text_at(i)).map(Some)
                } else {
                    // The winning occurrence's text, as its presence test
                    // validated it, or NULL when that winner sits in a
                    // different-typed column of the same key.
                    plan.winner(i).map(|(win, text)| {
                        (Some(win) == plan.matching_idx).then_some(text).flatten()
                    })
                };
                let key = match record {
                    Some(Some(text)) => {
                        texts.push(text);
                        Some(str_key(texts.len() - 1)?)
                    }
                    Some(None) => None,
                    // A placeholder below zero until the record's values are
                    // all known: fallback slot `s` is key `-1 - s`.
                    None => fallbacks.slot(view, resolver, cache, i)?.map(|s| -1 - s),
                };
                push_key(&mut keys, &mut nulls, key);
            }
            let base = str_key(texts.len())?;
            for k in keys.iter_mut().filter(|k| **k < 0) {
                *k = base - 1 - *k;
            }
            lock_str_values(&resolver.str_values).built.add(1);
            Arc::new(StringArray::from_iter_values(
                texts
                    .into_iter()
                    .chain(fallbacks.values.iter().map(String::as_str)),
            ))
        }
    };
    Ok(StrBlock {
        keys: Int32Array::new(keys.into(), nulls.finish()),
        values,
    })
}

fn push_key(keys: &mut Vec<i32>, nulls: &mut NullBufferBuilder, key: Option<i32>) {
    keys.push(key.unwrap_or(0));
    nulls.append(key.is_some());
}

fn str_key(at: usize) -> DFResult<i32> {
    i32::try_from(at)
        .map_err(|_| DataFusionError::Internal("declared Str values exceed i32 keys".into()))
}

/// `values` sorted and deduplicated, and each input slot's index in that
/// list. Blocks of one dictionary that show the same fallback values, from
/// whichever streams in whichever order, then key one [`StrValuesCache`]
/// extended array.
fn canonical_fallbacks(values: Vec<String>) -> DFResult<(Vec<String>, Vec<i32>)> {
    let mut sorted = values.clone();
    sorted.sort_unstable();
    sorted.dedup();
    let slots = values
        .iter()
        .map(|v| str_key(sorted.partition_point(|s| s < v)))
        .collect::<DFResult<_>>()?;
    Ok((sorted, slots))
}

/// The resource/scope `Str` fallback values one block's declared `Str` column
/// appends, each stream's once, in the order the streams are first read.
#[derive(Default)]
struct StrFallbacks {
    values: Vec<String>,
    /// Each stream's slot in [`Self::values`], `None` for a stream with no
    /// `Str` value for the key.
    slots: HashMap<u32, Option<i32>>,
}

impl StrFallbacks {
    /// The slot of surviving row `i`'s stream's fallback value, or `None` when
    /// the stream has no `Str` value for the key.
    fn slot(
        &mut self,
        view: &ColumnarBlockView<'_>,
        resolver: &mut DeclaredResolver<'_, '_, '_>,
        cache: &mut HashMap<u32, Arc<Vec<(String, AttrValue)>>>,
        i: usize,
    ) -> DFResult<Option<i32>> {
        let Some(stream_ref) = view.stream_ref(i) else {
            return Ok(None);
        };
        if let Some(slot) = self.slots.get(&stream_ref) {
            return Ok(*slot);
        }
        let slot = match resolver.fallback_value(view, cache, stream_ref)? {
            Some(AttrValue::Str(s)) => {
                let slot = str_key(self.values.len())?;
                self.values.push(s.clone());
                Some(slot)
            }
            _ => None,
        };
        self.slots.insert(stream_ref, slot);
        Ok(slot)
    }
}

/// One synthetic per-key attribute column's FIELD_DIR resolution for a block
/// (issue #1768), the map-rendering sibling of [`DeclaredPlan`]. It resolves
/// every FIELD_DIR column of the key to a cursor once, so the row loop reads a
/// value with no per-cell column lookup.
struct AttrKeyPlan<'a> {
    key: String,
    /// The raw FIELD_DIR columns of this key, across all stored types.
    cols: Vec<AttrColumn>,
    /// Cursors parallel to [`Self::cols`], resolved once for the block.
    cursors: Vec<DeclaredCursor<'a>>,
}

impl<'a> AttrKeyPlan<'a> {
    fn build(view: &ColumnarBlockView<'a>, key: &str) -> AttrKeyPlan<'a> {
        let cols: Vec<AttrColumn> = view.attr_columns_for(key).collect();
        let cursors = cols
            .iter()
            .map(|&c| DeclaredCursor::resolve(view, c))
            .collect();
        AttrKeyPlan {
            key: key.to_string(),
            cols,
            cursors,
        }
    }

    /// The index of the record's WINNING occurrence of the key at surviving row
    /// `i`, or `None` when the record sets the key in no FIELD_DIR column.
    /// Identical rule to [`DeclaredPlan::winning_idx`]: the highest FIELD_DIR
    /// type byte wins (the record layer's last-occurrence-wins order,
    /// docs/log-segment-format.md), and a `Str` cell that is not valid UTF-8 is
    /// not "present", so it does not shadow the resource/scope fallback. The
    /// `attrs_raw` overflow tier cannot appear here: a block carrying an
    /// `attrs_raw` page falls the whole segment back to the row path before this
    /// builds a column.
    fn winning_idx(&self, i: usize) -> Option<usize> {
        winner_at(&self.cols, &self.cursors, i).map(|(k, _)| k)
    }
}

/// Per-key, per-block resolver for a synthetic attribute column's merged value
/// rendered as text (issue #1768). Unlike [`DeclaredResolver`], which yields
/// NULL for a record value whose variant does not match the declared type, this
/// renders the WINNING value of any variant to text with
/// [`attr_value_to_string`], reproducing the `attrs` map's own rendering and its
/// divergence from a declared column. The resource/scope fallback is memoized
/// per `stream_ref` exactly as `DeclaredResolver` does.
struct AttrKeyResolver<'p, 'a> {
    plan: &'p AttrKeyPlan<'a>,
    fallback: HashMap<u32, Option<String>>,
}

impl<'p, 'a> AttrKeyResolver<'p, 'a> {
    fn new(plan: &'p AttrKeyPlan<'a>) -> Self {
        AttrKeyResolver {
            plan,
            fallback: HashMap::new(),
        }
    }

    /// The merged value of the key at surviving row `i`, rendered to text, or
    /// `None` (NULL) when the key is absent. The record's own winning
    /// occurrence wins over the resource/scope value; only a record that sets
    /// the key in no FIELD_DIR column consults the fallback.
    fn text_at(
        &mut self,
        view: &ColumnarBlockView<'_>,
        i: usize,
        cache: &mut HashMap<u32, Arc<Vec<(String, AttrValue)>>>,
    ) -> DFResult<Option<String>> {
        if let Some(win) = self.plan.winning_idx(i) {
            // A winning occurrence is present by construction, so `value_at` is
            // `Some`; render it to text like the `attrs` map column does.
            return Ok(self.plan.cursors[win]
                .value_at(i)
                .as_ref()
                .map(attr_value_to_string));
        }
        let Some(stream_ref) = view.stream_ref(i) else {
            return Ok(None);
        };
        match self.fallback.entry(stream_ref) {
            std::collections::hash_map::Entry::Occupied(e) => Ok(e.into_mut().clone()),
            std::collections::hash_map::Entry::Vacant(e) => {
                let resource = resource_attrs(view, cache, stream_ref)?;
                let value = find_attr(resource, &self.plan.key).map(attr_value_to_string);
                Ok(e.insert(value).clone())
            }
        }
    }
}

/// Build one synthetic per-key attribute column as a `Utf8` array for surviving
/// rows `start..end` straight from the view (issue #1768). Byte-identical to
/// [`attr_key_column_array`] over the same input.
fn build_attr_key_columnar_array(
    view: &ColumnarBlockView<'_>,
    resolver: &mut AttrKeyResolver<'_, '_>,
    start: usize,
    end: usize,
    cache: &mut HashMap<u32, Arc<Vec<(String, AttrValue)>>>,
) -> DFResult<ArrayRef> {
    let mut b = StringBuilder::new();
    for i in start..end {
        match resolver.text_at(view, i, cache)? {
            Some(s) => b.append_value(s),
            None => b.append_null(),
        }
    }
    Ok(Arc::new(b.finish()))
}

/// A UTF-8 log field (`body`, `severity_text`) read from the view; a violation
/// is the same client-visible corruption class the row path's `string_from_bytes`
/// produces, never a panic or silently-wrong data.
fn view_str(bytes: &[u8]) -> DFResult<&str> {
    std::str::from_utf8(bytes)
        .map_err(|_| SqlError::CorruptStreamAttrs("log text field not utf-8".to_string()).into())
}

/// Build all of one block's output batches straight from its columnar view
/// (ADR-0099 decision 2), chunked at [`BATCH_ROWS`] exactly as the row path
/// chunks its records, so the two paths' batches are byte-identical. Returns an
/// empty vec for a block with no surviving row. The view borrows the scan, so
/// the whole block is built here and the batches handed back owned, letting the
/// caller drop the view before decoding the next block.
#[allow(clippy::too_many_arguments)]
pub(super) fn build_columnar_batches(
    view: &ColumnarBlockView<'_>,
    schema: &SchemaRef,
    projection: &[usize],
    declared: &[DeclaredColumn],
    attr_keys: &[String],
    full_len: usize,
    row_refs: Option<RowRefRange>,
    str_values: &SharedStrValuesCache,
) -> DFResult<Vec<RecordBatch>> {
    let n = view.surviving_count();
    // Resolve each projected declared column's FIELD_DIR columns to cursors once
    // for the whole block (ADR-0099 decision 2, #875), not per row and not per
    // chunk: the row loop then reads through the cursors with no column lookup.
    // A synthetic per-key column (index >= full_len) is out of `declared`'s
    // range, so this loop skips it; it gets its own plan below.
    let mut plans: HashMap<usize, DeclaredPlan<'_, '_>> = HashMap::new();
    for &idx in projection {
        if (FIRST_DECLARED_COL..full_len).contains(&idx)
            && let Some(dc) = declared.get(idx - FIRST_DECLARED_COL)
        {
            plans.insert(idx, DeclaredPlan::build(view, dc));
        }
    }
    // Synthetic per-key attribute columns (issue #1768): one plan per projected
    // per-key index, resolved once for the whole block like the declared plans.
    let mut attr_plans: HashMap<usize, AttrKeyPlan<'_>> = HashMap::new();
    for &idx in projection {
        if idx >= full_len
            && let Some(key) = attr_keys.get(idx - full_len)
        {
            attr_plans.insert(idx, AttrKeyPlan::build(view, key));
        }
    }
    // One resolver per declared column for the WHOLE block, not per chunk:
    // the fallback memo must survive across `BATCH_ROWS` chunk boundaries, or
    // a block larger than one chunk repeats the find_attr scan per chunk for
    // every stream that spans them.
    let mut resolvers: HashMap<usize, DeclaredResolver<'_, '_, '_>> = plans
        .iter()
        .map(|(idx, plan)| {
            (
                *idx,
                DeclaredResolver::with_str_values(plan, Arc::clone(str_values)),
            )
        })
        .collect();
    let mut attr_resolvers: HashMap<usize, AttrKeyResolver<'_, '_>> = attr_plans
        .iter()
        .map(|(idx, plan)| (*idx, AttrKeyResolver::new(plan)))
        .collect();
    let mut cache: HashMap<u32, Arc<Vec<(String, AttrValue)>>> = HashMap::new();
    let mut out = Vec::new();
    let mut start = 0;
    while start < n {
        let end = (start + BATCH_ROWS).min(n);
        out.push(build_columnar_batch(
            view,
            schema,
            projection,
            &mut resolvers,
            &mut attr_resolvers,
            &mut cache,
            start,
            end,
            row_refs.map(|r| RowRefRange {
                first_row: r.first_row + start,
                ..r
            }),
        )?);
        start = end;
    }
    Ok(out)
}

/// Surviving rows `start..end` of a block's integer column as an Arrow array,
/// built from the column's decoded value and validity buffers (ADR-2773
/// decision 1): one copy of the block-row span the rows cover, then a `take`
/// over their offsets in it unless every row of the span survives. With
/// `keep_nulls` an absent cell is NULL; without it, it reads 0, the slot value
/// the decoder writes there. A column the block does not carry reads all-NULL
/// or all-0 the same way.
fn int_column_array<T>(
    col: &ravel_logseg::I64Values<'_>,
    start: usize,
    end: usize,
    keep_nulls: bool,
) -> DFResult<datafusion::arrow::array::PrimitiveArray<T>>
where
    T: datafusion::arrow::datatypes::ArrowPrimitiveType<Native = i64>,
{
    use datafusion::arrow::array::{AsArray, PrimitiveArray};
    use datafusion::arrow::buffer::{BooleanBuffer, Buffer, NullBuffer, ScalarBuffer};
    let out_of_range =
        || DataFusionError::Internal("columnar integer column: row out of range".into());
    let rows = col.rows().get(start..end).ok_or_else(out_of_range)?;
    let n = rows.len();
    let Some(values) = col.values() else {
        return Ok(if keep_nulls {
            PrimitiveArray::new_null(n)
        } else {
            PrimitiveArray::try_new(ScalarBuffer::from(vec![0i64; n]), None)?
        });
    };
    let (Some(&first), Some(&last)) = (rows.first(), rows.last()) else {
        return Ok(PrimitiveArray::try_new(
            ScalarBuffer::from(Vec::new()),
            None,
        )?);
    };
    let span = values.get(first..=last).ok_or_else(out_of_range)?;
    let nulls = match col.validity() {
        Some(bits) if keep_nulls => {
            if bits.len().saturating_mul(8) <= last {
                return Err(out_of_range());
            }
            Some(NullBuffer::new(BooleanBuffer::new(
                Buffer::from_slice_ref(bits),
                first,
                span.len(),
            )))
        }
        _ => None,
    };
    let span_array = PrimitiveArray::<T>::try_new(ScalarBuffer::from(span.to_vec()), nulls)?;
    // `rows` is strictly ascending, so a span as long as the row count is
    // exactly the surviving rows.
    if span.len() == n {
        return Ok(span_array);
    }
    let offsets = rows
        .iter()
        .map(|&r| u32::try_from(r - first).map_err(|_| out_of_range()))
        .collect::<DFResult<Vec<u32>>>()?;
    let taken = datafusion::arrow::compute::take(
        &span_array,
        &datafusion::arrow::array::UInt32Array::from(offsets),
        None,
    )?;
    Ok(taken.as_primitive::<T>().clone())
}

/// Build one output batch for surviving rows `start..end` from the view, one
/// array per projected column. The column set is the same eligible set
/// [`columnar_static_eligible`] admits: fixed columns and declared typed
/// columns, never the `attrs` map.
#[allow(clippy::too_many_arguments)]
fn build_columnar_batch(
    view: &ColumnarBlockView<'_>,
    schema: &SchemaRef,
    projection: &[usize],
    resolvers: &mut HashMap<usize, DeclaredResolver<'_, '_, '_>>,
    attr_resolvers: &mut HashMap<usize, AttrKeyResolver<'_, '_>>,
    cache: &mut HashMap<u32, Arc<Vec<(String, AttrValue)>>>,
    start: usize,
    end: usize,
    row_refs: Option<RowRefRange>,
) -> DFResult<RecordBatch> {
    let mut columns: Vec<ArrayRef> = Vec::with_capacity(projection.len());
    for &idx in projection {
        let array: ArrayRef = match idx {
            LOG_COL_TS => Arc::new(int_column_array::<
                datafusion::arrow::datatypes::TimestampNanosecondType,
            >(&view.ts_values(), start, end, false)?),
            LOG_COL_OBSERVED_TS => Arc::new(int_column_array::<
                datafusion::arrow::datatypes::TimestampNanosecondType,
            >(
                &view.observed_ts_values(), start, end, false
            )?),
            LOG_COL_SEVERITY_NUM => {
                let raw = int_column_array::<datafusion::arrow::datatypes::Int64Type>(
                    &view.severity_num_values(),
                    start,
                    end,
                    false,
                )?;
                Arc::new(UInt8Array::from_iter_values(
                    raw.values().iter().map(|&v| v as u8),
                ))
            }
            LOG_COL_SEVERITY_TEXT => {
                let cur = view.severity_text_cursor();
                let mut b = StringBuilder::new();
                for i in start..end {
                    match cur.at(i) {
                        Some(bytes) => b.append_value(view_str(bytes)?),
                        None => b.append_value(""),
                    }
                }
                Arc::new(b.finish())
            }
            LOG_COL_BODY => {
                let cur = view.body_cursor();
                let mut b = StringBuilder::new();
                for i in start..end {
                    match cur.at(i) {
                        Some(bytes) => b.append_value(view_str(bytes)?),
                        None => b.append_value(""),
                    }
                }
                Arc::new(b.finish())
            }
            LOG_COL_TRACE_ID => {
                let cur = view.trace_id_cursor();
                let mut b = FixedSizeBinaryBuilder::with_capacity(end - start, TRACE_ID_WIDTH);
                for i in start..end {
                    match cur.at(i) {
                        Some(id) => b.append_value(id).map_err(|e| {
                            SqlError::Internal(format!("trace_id array build: {e}"))
                        })?,
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            LOG_COL_SPAN_ID => {
                let cur = view.span_id_cursor();
                let mut b = FixedSizeBinaryBuilder::with_capacity(end - start, SPAN_ID_WIDTH);
                for i in start..end {
                    match cur.at(i) {
                        Some(id) => b
                            .append_value(id)
                            .map_err(|e| SqlError::Internal(format!("span_id array build: {e}")))?,
                        None => b.append_null(),
                    }
                }
                Arc::new(b.finish())
            }
            LOG_COL_FLAGS => {
                let raw = int_column_array::<datafusion::arrow::datatypes::Int64Type>(
                    &view.flags_values(),
                    start,
                    end,
                    false,
                )?;
                Arc::new(UInt32Array::from_iter_values(
                    raw.values().iter().map(|&v| v as u32),
                ))
            }
            // Ruled out by `columnar_static_eligible`; a projection reaching the
            // fast path never carries the `attrs` map.
            LOG_COL_ATTRS => {
                return Err(DataFusionError::Internal(
                    "columnar fast path reached with an attrs map projection".into(),
                ));
            }
            other => {
                if let Some(resolver) = resolvers.get_mut(&other) {
                    build_declared_columnar_array(view, resolver, start, end, cache)?
                } else if let Some(resolver) = attr_resolvers.get_mut(&other) {
                    build_attr_key_columnar_array(view, resolver, start, end, cache)?
                } else {
                    return Err(DataFusionError::Internal(format!(
                        "logs columnar scan projection index {other} out of range"
                    )));
                }
            }
        };
        columns.push(array);
    }
    if let Some(range) = row_refs {
        columns.push(row_ref_array(range, end - start)?);
    }
    debug_assert_eq!(schema.fields().len(), columns.len());
    // Carry the row count explicitly so an empty projection (a bare `COUNT(*)`)
    // still reports its rows, exactly as the row path does.
    let options = RecordBatchOptions::new().with_row_count(Some(end - start));
    RecordBatch::try_new_with_options(Arc::clone(schema), columns, &options)
        .map_err(DataFusionError::from)
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod columnar_lookup_tests {
    use super::*;
    use datafusion::arrow::array::{Array, BinaryArray, BooleanArray, Int64Array};
    use ravel_logseg::record::stream_attrs_bytes;
    use ravel_logseg::{
        ColumnSelection, LogRecord, ObjectIdentity, Predicate, RlogConfig, RlogReader, RlogWriter,
    };
    use ravel_types::logstream::LogStreamId;

    pub(super) fn sid(n: u8) -> LogStreamId {
        let mut a = [0u8; 16];
        a[0] = n;
        LogStreamId(a)
    }

    fn rec_k(ts: i64, k: Option<i64>) -> LogRecord {
        LogRecord {
            stream_id: sid(0),
            stream_attrs: stream_attrs_bytes(
                &[("service.name".into(), AttrValue::Str("svc".into()))],
                "scope",
                "1",
                &[],
            ),
            ts_ns: ts,
            observed_ts_ns: ts,
            severity_num: 9,
            severity_text: "INFO".into(),
            body: "keep".into(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: k
                .map(|v| vec![("k".to_string(), AttrValue::I64(v))])
                .unwrap_or_default(),
        }
    }

    /// Deliverable 2 (#875): for a single-matching-column declared scan the
    /// presence test and the value read fuse to one cursor read, so the block's
    /// declared column is resolved exactly once. The pre-change path ran
    /// `attr_present` and then `read_typed_cell` -- two full cell reads, each
    /// resolving the column by id -- for `2 * rows` resolutions per block.
    /// Flipping the fused read back to that double read makes the count below
    /// `2 * rows` instead of `1`.
    #[test]
    fn single_matching_declared_scan_reads_each_cell_once() {
        let cfg = RlogConfig {
            block_target_records: 16,
            max_dynamic_columns: 8,
            ..RlogConfig::default()
        };
        let mut w = RlogWriter::new(
            cfg,
            ObjectIdentity {
                tenant_hash: [0; 16],
                shard: 0,
                writer_id: [0; 16],
                writer_epoch: 0,
                writer_seq: 0,
            },
        );
        // Every row sets `k`, so the record always wins and no resource/scope
        // fallback (which would resolve `stream_ref` per row) is consulted: the
        // count isolates the declared column's own resolution.
        for (ts, v) in [(100i64, 10i64), (101, 20), (102, 30), (103, 40)] {
            w.push(rec_k(ts, Some(v))).expect("push");
        }
        let obj = w.finish().expect("finish");
        let reader = RlogReader::new(&obj, &cfg).expect("open");
        let mut scan = reader
            .scan_blocks(&Predicate::And(Vec::new()), &[], &ColumnSelection::all())
            .expect("scan");
        let view = scan
            .next_block_columnar(&obj)
            .expect("columnar exit")
            .expect("one block");
        let rows = view.surviving_count();
        assert_eq!(rows, 4);

        let dc = DeclaredColumn::new("k", DeclaredType::I64);
        let base = view.column_lookups();
        let plan = DeclaredPlan::build(&view, &dc);
        assert!(
            plan.single_matching(),
            "the key has exactly one FIELD_DIR column, of the declared type"
        );
        let mut cache = HashMap::new();
        let arr = build_declared_columnar_array(
            &view,
            &mut DeclaredResolver::new(&plan),
            0,
            rows,
            &mut cache,
        )
        .expect("declared array");
        let lookups = view.column_lookups() - base;
        assert_eq!(
            lookups, 1,
            "the declared column is resolved once per block, not 2*rows (the pre-change double read)"
        );

        let ints = arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("declared I64 array");
        assert_eq!(ints.len(), 4);
        for (i, want) in [10i64, 20, 30, 40].into_iter().enumerate() {
            assert!(ints.is_valid(i), "row {i} is non-null");
            assert_eq!(ints.value(i), want, "row {i}");
        }
    }

    /// A record on stream `stream`, with the given resource scalars and dynamic
    /// per-record attributes.
    fn rec(
        stream: u8,
        ts: i64,
        resource: &[(&str, AttrValue)],
        attrs: &[(&str, AttrValue)],
    ) -> LogRecord {
        let res: Vec<(String, AttrValue)> = resource
            .iter()
            .map(|(k, v)| ((*k).to_string(), v.clone()))
            .collect();
        LogRecord {
            stream_id: sid(stream),
            stream_attrs: stream_attrs_bytes(&res, "scope", "1", &[]),
            ts_ns: ts,
            observed_ts_ns: ts,
            severity_num: 9,
            severity_text: "INFO".into(),
            body: "b".into(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: attrs
                .iter()
                .map(|(k, v)| ((*k).to_string(), v.clone()))
                .collect(),
        }
    }

    /// Write `records` as one RLOG object and open a fresh scan over the whole
    /// object with no predicate.
    fn write_and_scan<'o>(
        records: &[LogRecord],
        cfg: &RlogConfig,
        obj: &'o mut Vec<u8>,
    ) -> RlogReader<'o> {
        let mut w = RlogWriter::new(
            *cfg,
            ObjectIdentity {
                tenant_hash: [0; 16],
                shard: 0,
                writer_id: [0; 16],
                writer_epoch: 0,
                writer_seq: 0,
            },
        );
        for r in records {
            w.push(r.clone()).expect("push");
        }
        *obj = w.finish().expect("finish");
        RlogReader::new(obj, cfg).expect("open")
    }

    /// Byte-identity oracle at block granularity: `build_declared_columnar_array`
    /// must produce the exact column `declared_column_array` builds from the row
    /// path's merged view over the SAME block, across every branch of the merge in
    /// one input.
    ///
    /// The input exercises, for declared I64 key `k`: a record that sets `k` at
    /// the declared type (record wins), a record that sets `k` at a DIFFERENT type
    /// while its stream carries `k` in resource (must be NULL and must NOT fall
    /// through to the resource value: ADR-0090 decision 7), two rows of one stream
    /// whose records do not set `k` and whose stream has `k` in resource (fallback
    /// value, memoized once), and a row whose record does not set `k` and whose
    /// stream has no `k` (NULL). Both paths read the block in the same surviving
    /// order (`next_block` and `next_block_columnar` are documented to), so the
    /// two arrays compare element by element.
    ///
    /// Failing-against-pre-change was demonstrated by flipping the
    /// `record_sets_key` arm of `DeclaredResolver::merged_value` to fall through
    /// to the fallback when the declared-type cell is `None`: the different-typed
    /// row then reads its stream's resource `k=777` instead of NULL, and both the
    /// element-wise and the sorted-multiset assertions below fail.
    #[test]
    fn columnar_declared_column_is_byte_identical_to_row_path() {
        let cfg = RlogConfig {
            block_target_records: 16,
            max_dynamic_columns: 8,
            ..RlogConfig::default()
        };
        // stream 3: resource has k=I64(777). Its two records set k -- once at the
        // declared type (wins), once at a different type (NULL, decision 7).
        // stream 1: resource has k=I64(999); two records set nothing (fallback).
        // stream 2: no resource k; one record sets nothing (NULL fallback).
        let records = vec![
            rec(
                3,
                100,
                &[
                    ("svc", AttrValue::Str("c".into())),
                    ("k", AttrValue::I64(777)),
                ],
                &[("k", AttrValue::I64(10))],
            ),
            rec(
                3,
                101,
                &[
                    ("svc", AttrValue::Str("c".into())),
                    ("k", AttrValue::I64(777)),
                ],
                &[("k", AttrValue::Str("x".into()))],
            ),
            rec(
                1,
                102,
                &[
                    ("svc", AttrValue::Str("a".into())),
                    ("k", AttrValue::I64(999)),
                ],
                &[],
            ),
            rec(
                1,
                103,
                &[
                    ("svc", AttrValue::Str("a".into())),
                    ("k", AttrValue::I64(999)),
                ],
                &[],
            ),
            rec(2, 104, &[("svc", AttrValue::Str("b".into()))], &[]),
        ];
        let dc = DeclaredColumn::new("k", DeclaredType::I64);

        // Row path: decode the block's records in surviving order, merge, build.
        let mut obj = Vec::new();
        let reader = write_and_scan(&records, &cfg, &mut obj);
        let mut scan = reader
            .scan_blocks(&Predicate::And(Vec::new()), &[], &ColumnSelection::all())
            .expect("scan");
        let block = scan.next_block(&obj).expect("row exit").expect("one block");
        assert!(
            scan.next_block(&obj).expect("row exit").is_none(),
            "the input fits one block"
        );
        let merged: Vec<Vec<(String, AttrValue)>> = block
            .iter()
            .map(|r| merged_attrs(r).expect("merge"))
            .collect();
        let row_arr = declared_column_array(&dc, &merged);
        let row = row_arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("row I64 array");

        // Columnar path over the same object.
        let mut obj2 = Vec::new();
        let reader2 = write_and_scan(&records, &cfg, &mut obj2);
        let mut scan2 = reader2
            .scan_blocks(&Predicate::And(Vec::new()), &[], &ColumnSelection::all())
            .expect("scan");
        let view = scan2
            .next_block_columnar(&obj2)
            .expect("columnar exit")
            .expect("one block");
        let rows = view.surviving_count();
        let plan = DeclaredPlan::build(&view, &dc);
        assert!(
            !plan.single_matching(),
            "the key has an I64 and a Str FIELD_DIR column, so the multi-column \
             precedence path is what runs"
        );
        let mut cache = HashMap::new();
        let col_arr = build_declared_columnar_array(
            &view,
            &mut DeclaredResolver::new(&plan),
            0,
            rows,
            &mut cache,
        )
        .expect("columnar array");
        let col = col_arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("columnar I64 array");

        // Byte-identity: same length, same null bitmap, same values, in order.
        assert_eq!(col.len(), row.len(), "row counts match");
        assert_eq!(col.len(), 5, "all five records survive in one block");
        for i in 0..col.len() {
            assert_eq!(col.is_null(i), row.is_null(i), "null bit at row {i}");
            if !col.is_null(i) {
                assert_eq!(col.value(i), row.value(i), "value at row {i}");
            }
        }

        // Pin the semantic outcome independent of block ordering: the different-
        // typed record is NULL (not 777), both fallbacks are 999, the record-wins
        // row is 10, the no-resource row is NULL. If decision 7 were violated the
        // second NULL would become 777 and this multiset would not match.
        let mut got: Vec<Option<i64>> = (0..col.len())
            .map(|i| (!col.is_null(i)).then(|| col.value(i)))
            .collect();
        got.sort();
        assert_eq!(
            got,
            vec![None, None, Some(10), Some(999), Some(999)],
            "declared column merge outcome"
        );
    }

    /// The fallback resolve runs once per DISTINCT stream ref in the block, not
    /// once per row: `DeclaredResolver::resolve_count` equals the number of
    /// distinct streams, never the row count. Three streams (2 + 2 + 1 rows),
    /// none setting the key, so every row reaches the fallback; the memo makes the
    /// scan run exactly three times.
    ///
    /// Failing-against-pre-change was demonstrated by resolving per row (dropping
    /// the memo and incrementing on every call): the count then reads 5, the row
    /// count, and the `== 3` assertion fails.
    #[test]
    fn fallback_resolves_once_per_distinct_stream() {
        let cfg = RlogConfig {
            block_target_records: 16,
            max_dynamic_columns: 8,
            ..RlogConfig::default()
        };
        let records = vec![
            rec(
                1,
                100,
                &[
                    ("svc", AttrValue::Str("a".into())),
                    ("k", AttrValue::I64(1)),
                ],
                &[],
            ),
            rec(
                1,
                101,
                &[
                    ("svc", AttrValue::Str("a".into())),
                    ("k", AttrValue::I64(1)),
                ],
                &[],
            ),
            rec(
                2,
                102,
                &[
                    ("svc", AttrValue::Str("b".into())),
                    ("k", AttrValue::I64(2)),
                ],
                &[],
            ),
            rec(
                2,
                103,
                &[
                    ("svc", AttrValue::Str("b".into())),
                    ("k", AttrValue::I64(2)),
                ],
                &[],
            ),
            rec(
                3,
                104,
                &[
                    ("svc", AttrValue::Str("c".into())),
                    ("k", AttrValue::I64(3)),
                ],
                &[],
            ),
        ];
        let dc = DeclaredColumn::new("k", DeclaredType::I64);

        let mut obj = Vec::new();
        let reader = write_and_scan(&records, &cfg, &mut obj);
        let mut scan = reader
            .scan_blocks(&Predicate::And(Vec::new()), &[], &ColumnSelection::all())
            .expect("scan");
        let view = scan
            .next_block_columnar(&obj)
            .expect("columnar exit")
            .expect("one block");
        let rows = view.surviving_count();
        assert_eq!(rows, 5, "five surviving rows");

        let plan = DeclaredPlan::build(&view, &dc);
        // No record sets `k`, so the fallback is consulted for every row and the
        // resolve count is purely a function of the memoization.
        let mut streams = std::collections::HashSet::new();
        for i in 0..rows {
            streams.insert(view.stream_ref(i).expect("stream ref"));
        }
        assert_eq!(streams.len(), 3, "three distinct streams in the block");

        let mut resolver = DeclaredResolver::new(&plan);
        let mut cache = HashMap::new();
        for i in 0..rows {
            resolver
                .merged_value(&view, i, &mut cache)
                .expect("merged value");
        }
        assert_eq!(
            resolver.resolve_count, 3,
            "the fallback scan runs once per distinct stream ref, not once per row"
        );
    }

    /// The fallback memo survives across `BATCH_ROWS` chunk boundaries: the
    /// resolver set is built once per BLOCK in `build_columnar_batches`, so a
    /// block larger than one chunk does not repeat the `find_attr` scan for a
    /// stream that spans chunks. This drives `build_columnar_batch` twice over
    /// the same resolver map, exactly as the chunk loop does, and pins the
    /// TOTAL resolve count to the distinct-stream count, not
    /// distinct-streams-per-chunk summed.
    ///
    /// Failing-against-pre-change was demonstrated by rebuilding the resolver
    /// map between the two calls (the per-chunk construction this fixes): the
    /// count then reads 6 (3 streams x 2 chunks) and the `== 3` fails.
    #[test]
    fn fallback_memo_survives_chunk_boundaries() {
        let cfg = RlogConfig {
            block_target_records: 16,
            max_dynamic_columns: 8,
            ..RlogConfig::default()
        };
        // Six rows, three streams, every stream present in BOTH halves.
        let mut records = Vec::new();
        for half in 0..2u8 {
            for stream in 1..=3u8 {
                records.push(rec(
                    stream,
                    100 + i64::from(half) * 10 + i64::from(stream),
                    &[
                        ("svc", AttrValue::Str(format!("s{stream}"))),
                        ("k", AttrValue::I64(i64::from(stream))),
                    ],
                    &[],
                ));
            }
        }
        let dc = DeclaredColumn::new("k", DeclaredType::I64);

        let mut obj = Vec::new();
        let reader = write_and_scan(&records, &cfg, &mut obj);
        let mut scan = reader
            .scan_blocks(&Predicate::And(Vec::new()), &[], &ColumnSelection::all())
            .expect("scan");
        let view = scan
            .next_block_columnar(&obj)
            .expect("columnar exit")
            .expect("one block");
        let rows = view.surviving_count();
        assert_eq!(rows, 6, "six surviving rows");

        let plan = DeclaredPlan::build(&view, &dc);
        let mut resolver = DeclaredResolver::new(&plan);
        let mut cache = HashMap::new();
        // Two chunk-shaped passes over ONE resolver, as build_columnar_batches
        // now does; the memo carries from the first half into the second.
        for i in 0..3 {
            resolver
                .merged_value(&view, i, &mut cache)
                .expect("merged value");
        }
        for i in 3..rows {
            resolver
                .merged_value(&view, i, &mut cache)
                .expect("merged value");
        }
        assert_eq!(
            resolver.resolve_count, 3,
            "one scan per distinct stream across BOTH chunks; a per-chunk \
             resolver would read 6"
        );
    }

    /// One declared column's cells rendered to a form the two paths can be
    /// compared in: `None` for NULL, otherwise the value's text (lowercase hex
    /// for `bytes`). `Str` is a `Dictionary(Int32, Utf8)` on both paths, so its
    /// keys are resolved through the dictionary values here rather than
    /// compared as ids: the two paths build different dictionaries for the same
    /// logical column and only the logical values must match.
    pub(super) fn declared_cells(arr: &ArrayRef, ty: DeclaredType) -> Vec<Option<String>> {
        match ty {
            DeclaredType::Str => {
                let d = arr
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .expect("declared Str dictionary array");
                let values = d
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Utf8 dictionary values");
                (0..d.len())
                    .map(|i| {
                        if d.is_null(i) {
                            return None;
                        }
                        let k = usize::try_from(d.keys().value(i)).expect("non-negative key");
                        (!values.is_null(k)).then(|| values.value(k).to_string())
                    })
                    .collect()
            }
            DeclaredType::I64 => {
                let a = arr
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .expect("declared I64 array");
                (0..a.len())
                    .map(|i| (!a.is_null(i)).then(|| a.value(i).to_string()))
                    .collect()
            }
            DeclaredType::Bool => {
                let a = arr
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .expect("declared Bool array");
                (0..a.len())
                    .map(|i| (!a.is_null(i)).then(|| a.value(i).to_string()))
                    .collect()
            }
            DeclaredType::Bytes => {
                let a = arr
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .expect("declared Bytes array");
                (0..a.len())
                    .map(|i| (!a.is_null(i)).then(|| hex::encode(a.value(i))))
                    .collect()
            }
        }
    }

    /// Issue #1182, the reader half: a record carrying one attribute name more
    /// than once resolves that name to ONE value, and both reader paths must
    /// pick the same one.
    ///
    /// The rule is docs/log-segment-format.md, "Within the record layer": the
    /// record's columnar occurrences ascending by FIELD_DIR type byte
    /// (str=1 i64=2 f64=3 bool=4 bytes=5), then its `attrs_raw` overflow
    /// occurrences ascending by canonical encoded value bytes, last entry wins.
    /// It is NOT the record's write order, which the on-disk format does not
    /// preserve. So among the columnar occurrences the HIGHEST type byte wins,
    /// and the two records below that carry the same pair in opposite write
    /// orders resolve identically.
    ///
    /// Every row is asserted against a literal expected value for each of the
    /// four declarable types, so a drift names both the consumer and the type.
    /// The two consumers this can reach in-crate are the row path
    /// (`declared_column_array` over `merged_attrs` + `find_attr`, already
    /// correct before #1182) and the columnar fast path
    /// (`DeclaredResolver::merged_value` + `build_declared_str_columnar`, which
    /// was first-occurrence-per-(name, type)-wins). The other two consumers of
    /// the same rule live in crates this one does not depend on and pin the
    /// SAME table of records and expectations:
    /// `ravel_ingest::log_declared_stats::tests::duplicate_key_stamp_follows_the_readers_rule`
    /// and `ravel_maintain::rlog::tests::duplicate_key_stamp_follows_the_readers_rule`.
    ///
    /// Prove-the-test: restoring `merged_value`'s `record_sets_key` arm
    /// (`return Ok(self.matching_cursor.and_then(|c| c.value_at(i)))`) makes the
    /// declared I64 column read 9 at ts 102 and 1 at ts 103 where the row path
    /// reads NULL; restoring either `record_sets_key` arm of
    /// `build_declared_str_columnar` makes the declared Str column read "x" at
    /// ts 100 and 101 where the row path reads NULL.
    #[test]
    fn a_duplicate_key_resolves_last_occurrence_wins_on_both_reader_paths() {
        let cfg = RlogConfig {
            block_target_records: 16,
            max_dynamic_columns: 16,
            ..RlogConfig::default()
        };
        let res = [("svc", AttrValue::Str("a".into()))];
        let records = vec![
            // I64 written first, Str second: i64 (2) > str (1), so `k` is 5 and
            // a declared Str column reads NULL, not "x".
            rec(
                1,
                100,
                &res,
                &[("k", AttrValue::I64(5)), ("k", AttrValue::Str("x".into()))],
            ),
            // The same pair in the opposite write order: same winner.
            rec(
                1,
                101,
                &res,
                &[("k", AttrValue::Str("x".into())), ("k", AttrValue::I64(5))],
            ),
            // bool (4) > i64 (2): a declared I64 column reads NULL, not 9.
            rec(
                1,
                102,
                &res,
                &[("k", AttrValue::Bool(true)), ("k", AttrValue::I64(9))],
            ),
            // bytes (5) beats every other type.
            rec(
                1,
                103,
                &res,
                &[
                    ("k", AttrValue::I64(1)),
                    ("k", AttrValue::Bytes(vec![0xab])),
                ],
            ),
            // A single occurrence resolves to itself: the Str column is not
            // simply always NULL, and the I64 column is not simply always set.
            rec(1, 104, &res, &[("k", AttrValue::Str("y".into()))]),
        ];
        // The merged view every consumer must agree on, per record, keyed by ts.
        let want_merged: Vec<(i64, AttrValue)> = vec![
            (100, AttrValue::I64(5)),
            (101, AttrValue::I64(5)),
            (102, AttrValue::Bool(true)),
            (103, AttrValue::Bytes(vec![0xab])),
            (104, AttrValue::Str("y".into())),
        ];
        // The same view projected onto each declarable type: a value of another
        // variant is NULL, never a cast (ADR-0090 decision 7).
        let want_str = [None, None, None, None, Some("y".to_string())];
        let want_i64 = [
            Some("5".to_string()),
            Some("5".to_string()),
            None,
            None,
            None,
        ];
        let want_bool = [None, None, Some("true".to_string()), None, None];
        let want_bytes = [None, None, None, Some("ab".to_string()), None];

        // Row path (consumer 1, correct before #1182): decode the block's
        // records, merge, build.
        let mut obj = Vec::new();
        let reader = write_and_scan(&records, &cfg, &mut obj);
        let mut scan = reader
            .scan_blocks(&Predicate::And(Vec::new()), &[], &ColumnSelection::all())
            .expect("scan");
        let block = scan.next_block(&obj).expect("row exit").expect("one block");
        assert!(
            scan.next_block(&obj).expect("row exit").is_none(),
            "the input fits one block"
        );
        let merged: Vec<Vec<(String, AttrValue)>> = block
            .iter()
            .map(|r| merged_attrs(r).expect("merge"))
            .collect();
        // Consumer 1, field by field: `merged_attrs` + `find_attr` resolve the
        // winner the format doc pins, for the record with that ts.
        assert_eq!(block.len(), want_merged.len(), "every record survives");
        for (r, row) in block.iter().zip(merged.iter()) {
            let (_, want) = want_merged
                .iter()
                .find(|(ts, _)| *ts == r.ts_ns)
                .expect("a record per expected ts");
            assert_eq!(
                find_attr(row, "k"),
                Some(want),
                "row path merged view at ts {}",
                r.ts_ns
            );
        }

        // Columnar fast path (consumer 2) over the same object.
        let mut obj2 = Vec::new();
        let reader2 = write_and_scan(&records, &cfg, &mut obj2);
        let mut scan2 = reader2
            .scan_blocks(&Predicate::And(Vec::new()), &[], &ColumnSelection::all())
            .expect("scan");
        let view = scan2
            .next_block_columnar(&obj2)
            .expect("columnar exit")
            .expect("one block");
        assert!(
            !view.has_attrs_raw_page(),
            "no occurrence overflows, so the columnar path is what a scan takes \
             here rather than falling back to the row path"
        );
        let rows = view.surviving_count();
        assert_eq!(rows, records.len(), "all records survive in one block");

        for (ty, want) in [
            (DeclaredType::Str, &want_str),
            (DeclaredType::I64, &want_i64),
            (DeclaredType::Bool, &want_bool),
            (DeclaredType::Bytes, &want_bytes),
        ] {
            let dc = DeclaredColumn::new("k", ty);
            let plan = DeclaredPlan::build(&view, &dc);
            assert!(
                !plan.single_matching(),
                "{ty:?}: `k` has a Str, I64, Bool and Bytes FIELD_DIR column, so \
                 the multi-column precedence arm is what runs"
            );
            let mut cache = HashMap::new();
            let col = declared_cells(
                &build_declared_columnar_array(
                    &view,
                    &mut DeclaredResolver::new(&plan),
                    0,
                    rows,
                    &mut cache,
                )
                .expect("columnar array"),
                ty,
            );
            let row = declared_cells(&declared_column_array(&dc, &merged), ty);

            // Consumer 1 against its literal expectation, in block order.
            for (i, r) in block.iter().enumerate() {
                let k = want_merged
                    .iter()
                    .position(|(ts, _)| *ts == r.ts_ns)
                    .expect("a record per expected ts");
                assert_eq!(
                    row[i], want[k],
                    "{ty:?}: row path at ts {} (block position {i})",
                    r.ts_ns
                );
            }
            // Consumer 2 against consumer 1, cell by cell. Both paths read the
            // block's surviving rows in the same order, so this compares the
            // same record on both sides.
            assert_eq!(
                col.len(),
                row.len(),
                "{ty:?}: the two paths build the same row count"
            );
            for i in 0..row.len() {
                assert_eq!(
                    col[i], row[i],
                    "{ty:?}: columnar path disagrees with the row path at block \
                     position {i}"
                );
            }
        }
    }

    /// ADR-2773 D3: a row group dictionary's Arrow values are built once for
    /// the scan, however many blocks and `BATCH_ROWS` chunks read it, and every
    /// chunk of every block of the row group reads that one values array.
    ///
    /// Six blocks of `BATCH_ROWS + 1000` rows in row groups of three, so each
    /// block spans two chunks and each dictionary three blocks. The counter
    /// reads 2, one per dictionary; a values rebuild per chunk would read 12
    /// and a rebuild per block 6.
    ///
    /// Flipped: `StrValuesCache::values` looking up no cached entry fails the
    /// cross-block pointer check at the group's second block; together with
    /// `build_declared_str_columnar` not keeping `resolver.str_block`, the two
    /// chunks' values stop sharing.
    #[test]
    fn declared_str_dictionary_values_are_built_once_per_dictionary() {
        const POOL: [&str; 4] = ["alpha", "beta", "gamma", "delta"];
        let block_rows = BATCH_ROWS + 1000;
        let cfg = RlogConfig {
            block_target_records: block_rows,
            max_dynamic_columns: 8,
            group_target_blocks: 3,
            ..RlogConfig::default()
        };
        let records: Vec<LogRecord> = (0..6 * block_rows)
            .map(|n| {
                let ts = i64::try_from(n).expect("ts");
                rec(
                    1,
                    ts,
                    &[("svc", AttrValue::Str("s".into()))],
                    &[("k", AttrValue::Str(POOL[n % POOL.len()].into()))],
                )
            })
            .collect();
        let dc = DeclaredColumn::new("k", DeclaredType::Str);

        let mut obj = Vec::new();
        let reader = write_and_scan(&records, &cfg, &mut obj);
        let mut scan = reader
            .scan_blocks(&Predicate::And(Vec::new()), &[], &ColumnSelection::all())
            .expect("scan");
        let shared = Arc::new(std::sync::Mutex::new(StrValuesCache::new(Count::new())));
        let mut dicts: Vec<(Arc<DictValues>, ArrayRef)> = Vec::new();
        let mut blocks = 0;
        while let Some(view) = scan.next_block_columnar(&obj).expect("columnar exit") {
            blocks += 1;
            let rows = view.surviving_count();
            assert_eq!(rows, block_rows, "every block holds {block_rows} rows");
            let plan = DeclaredPlan::build(&view, &dc);
            let Some(DeclaredCursor::Str(StrCursor {
                dict: Some(page), ..
            })) = plan.matching_cursor()
            else {
                panic!("block {blocks}: `k` reads a dictionary page");
            };
            let dict = Arc::clone(page.values());
            let mut resolver = DeclaredResolver::with_str_values(&plan, Arc::clone(&shared));
            let mut cache = HashMap::new();
            let mut chunks = Vec::new();
            for start in (0..rows).step_by(BATCH_ROWS) {
                let end = (start + BATCH_ROWS).min(rows);
                chunks.push(
                    build_declared_columnar_array(&view, &mut resolver, start, end, &mut cache)
                        .expect("declared Str chunk"),
                );
            }
            assert_eq!(chunks.len(), 2, "a block spans two chunks");
            let values: Vec<ArrayRef> = chunks
                .iter()
                .map(|c| {
                    Arc::clone(
                        c.as_any()
                            .downcast_ref::<DictionaryArray<Int32Type>>()
                            .expect("declared Str dictionary array")
                            .values(),
                    )
                })
                .collect();
            assert!(
                Arc::ptr_eq(&values[0], &values[1]),
                "block {blocks}: both chunks share one values array"
            );
            match dicts.iter().find(|(d, _)| Arc::ptr_eq(d, &dict)) {
                Some((_, first)) => assert!(
                    Arc::ptr_eq(first, &values[0]),
                    "block {blocks}: shares its row group's values array with the \
                     group's earlier blocks"
                ),
                None => {
                    assert!(
                        dicts.iter().all(|(_, v)| !Arc::ptr_eq(v, &values[0])),
                        "block {blocks}: a new dictionary gets its own values array"
                    );
                    dicts.push((dict, Arc::clone(&values[0])));
                }
            }
            let cells: Vec<Option<String>> = chunks
                .iter()
                .flat_map(|c| declared_cells(c, DeclaredType::Str))
                .collect();
            for (i, cell) in cells.iter().enumerate() {
                let n = (blocks - 1) * block_rows + i;
                assert_eq!(cell.as_deref(), Some(POOL[n % POOL.len()]), "row {n}");
            }
        }
        assert_eq!(blocks, 6, "six blocks");
        assert_eq!(dicts.len(), 2, "two row groups, one dictionary each");
        assert_eq!(
            lock_str_values(&shared).built.value(),
            2,
            "values built once per dictionary, not per block or chunk"
        );
    }

    /// The resource `k` of each fallback stream of [`fallback_order_records`]:
    /// blocks 0 to 2 show `ra` and `rb` in the orders ab, ba, ab, block 3 shows
    /// `rc` then `ra`.
    const FALLBACK_K: [&str; 8] = ["ra", "rb", "rb", "ra", "ra", "rb", "rc", "ra"];
    const FALLBACK_BLOCK_ROWS: usize = 64;

    /// One row group of four 64-row blocks, two streams per block (the writer
    /// orders rows by stream). Every fourth row of a stream sets no `k` and
    /// reads its stream's resource `k`; the rest read the group dictionary.
    fn fallback_order_records() -> Vec<LogRecord> {
        let per_stream = FALLBACK_BLOCK_ROWS / 2;
        (0..FALLBACK_K.len())
            .flat_map(|s| {
                (0..per_stream).map(move |j| {
                    let stream = u8::try_from(s + 1).expect("stream");
                    let ts = i64::try_from(s * 1000 + j).expect("ts");
                    let resource = [("k", AttrValue::Str(FALLBACK_K[s].into()))];
                    let own = AttrValue::Str(format!("v{:02}", j % 12));
                    if j % 4 == 3 {
                        rec(stream, ts, &resource, &[])
                    } else {
                        rec(stream, ts, &resource, &[("k", own)])
                    }
                })
            })
            .collect()
    }

    /// Per block of [`fallback_order_records`], the cache's live extended
    /// array count and live bytes (what `hold_batches` charges) while the
    /// block's chunks are alive and, when `retain` is false, after they drop
    /// (what `release_block` charges). Also returns the base values bytes,
    /// each block's extended values bytes, and the values-built count.
    #[allow(clippy::type_complexity)]
    fn fallback_order_charges(
        retain: bool,
    ) -> (
        Vec<(usize, usize)>,
        Vec<(usize, usize)>,
        usize,
        Vec<usize>,
        usize,
    ) {
        let cfg = RlogConfig {
            block_target_records: FALLBACK_BLOCK_ROWS,
            max_dynamic_columns: 8,
            group_target_blocks: 4,
            ..RlogConfig::default()
        };
        let records = fallback_order_records();
        let dc = DeclaredColumn::new("k", DeclaredType::Str);
        let mut obj = Vec::new();
        let reader = write_and_scan(&records, &cfg, &mut obj);
        let mut scan = reader
            .scan_blocks(&Predicate::And(Vec::new()), &[], &ColumnSelection::all())
            .expect("scan");
        let shared = Arc::new(std::sync::Mutex::new(StrValuesCache::new(Count::new())));
        let live = |cache: &SharedStrValuesCache| {
            let mut cache = lock_str_values(cache);
            let bytes = cache.live_bytes();
            let extended = cache.entries.iter().map(|e| e.extended.len()).sum();
            (extended, bytes)
        };
        let mut group: Option<Arc<DictValues>> = None;
        let (mut held, mut released, mut extended_bytes) = (Vec::new(), Vec::new(), Vec::new());
        let mut retained: Vec<ArrayRef> = Vec::new();
        let mut row = 0;
        while let Some(view) = scan.next_block_columnar(&obj).expect("columnar exit") {
            let block = held.len();
            let rows = view.surviving_count();
            assert_eq!(rows, FALLBACK_BLOCK_ROWS, "block {block}: rows");
            let plan = DeclaredPlan::build(&view, &dc);
            let Some(DeclaredCursor::Str(StrCursor {
                dict: Some(page), ..
            })) = plan.matching_cursor()
            else {
                panic!("block {block}: `k` reads a dictionary page");
            };
            match &group {
                Some(d) => assert!(
                    Arc::ptr_eq(d, page.values()),
                    "block {block}: one row group dictionary"
                ),
                None => group = Some(Arc::clone(page.values())),
            }
            let mut resolver = DeclaredResolver::with_str_values(&plan, Arc::clone(&shared));
            let mut cache = HashMap::new();
            let chunks: Vec<ArrayRef> = [(0, rows / 2), (rows / 2, rows)]
                .into_iter()
                .map(|(start, end)| {
                    build_declared_columnar_array(&view, &mut resolver, start, end, &mut cache)
                        .expect("declared Str chunk")
                })
                .collect();
            // The block job ends here: its resolver, which keeps the block's
            // values for its chunks, drops before `hold_batches` charges them.
            drop(resolver);
            for chunk in &chunks {
                for cell in declared_cells(chunk, DeclaredType::Str) {
                    let r = &records[row];
                    let want = match r.attrs.first() {
                        Some((_, AttrValue::Str(s))) => s.clone(),
                        _ => FALLBACK_K[row / (FALLBACK_BLOCK_ROWS / 2)].to_string(),
                    };
                    assert_eq!(cell.as_deref(), Some(want.as_str()), "row {row}");
                    row += 1;
                }
            }
            let values = Arc::clone(
                datafusion::arrow::array::AsArray::as_any_dictionary(&chunks[0]).values(),
            );
            assert_eq!(
                values.len(),
                page.values().len() + 2,
                "block {block}: the group dictionary plus two distinct fallback values"
            );
            extended_bytes.push(values.get_array_memory_size());
            drop(values);
            held.push(live(&shared));
            if retain {
                retained.extend(chunks);
            } else {
                drop(chunks);
                released.push(live(&shared));
            }
        }
        assert_eq!(held.len(), 4, "four blocks");
        let base = {
            let cache = lock_str_values(&shared);
            assert_eq!(cache.entries.len(), 1, "one dictionary");
            cache.entries[0].values.get_array_memory_size()
        };
        let built = lock_str_values(&shared).built.value();
        (held, released, base, extended_bytes, built)
    }

    /// ADR-2773 D3's live bound for extended values: a dictionary's cache
    /// holds its base values plus one extended array per distinct sorted
    /// fallback value list that a live batch still reads.
    ///
    /// Blocks 0 to 2 show the same fallback values in the orders ab, ba, ab and
    /// block 3 a different pair. With every block's chunks retained (a
    /// consumer that buffers them), blocks 0 to 2 share one extended array and
    /// block 3 adds the second: 1, 1, 1, 2 live. With each block's chunks
    /// dropped before the next block, one is live while a block is held and
    /// none once it drops, and the charge falls back to the base values.
    ///
    /// Flipped: keying the extended arrays by first-appearance order holds 1,
    /// 2, 2, 3 in both runs; without dropping unreferenced extended arrays the
    /// dropped run releases nothing until the row group ends.
    #[test]
    fn extended_str_values_live_while_a_batch_reads_them() {
        let (held, released, base, ext, built) = fallback_order_charges(true);
        assert!(released.is_empty());
        assert_eq!(ext[0], ext[1], "the two value pairs are the same width");
        assert_eq!(ext[0], ext[3], "the two value pairs are the same width");
        assert_eq!(
            held,
            vec![
                (1, base + ext[0]),
                (1, base + ext[0]),
                (1, base + ext[0]),
                (2, base + ext[0] + ext[3]),
            ],
            "retained: (live extended arrays, live bytes) per block"
        );
        assert_eq!(built, 3, "the base and one extended array per value set");
        for (block, (live, bytes)) in held.iter().enumerate() {
            let distinct = if block < 3 { 1 } else { 2 };
            assert!(
                *live <= distinct && *bytes <= base + distinct * ext[0],
                "block {block}: within one base plus one extended array per \
                 distinct value set still read"
            );
        }

        let (held, released, base, ext, built) = fallback_order_charges(false);
        assert_eq!(
            held,
            ext.iter().map(|e| (1, base + e)).collect::<Vec<_>>(),
            "dropped: (live extended arrays, live bytes) while each block is held"
        );
        assert_eq!(
            released,
            vec![(0, base); 4],
            "dropped: (live extended arrays, live bytes) once each block drops"
        );
        assert_eq!(built, 5, "the base, and the extended array again per block");
    }
}

#[cfg(test)]
#[allow(clippy::expect_used)]
mod tests {
    //! ADR-2121 D2 and D3: the declared-column builds read a single-column
    //! block's cells with no per-cell `AttrValue` and validate a `Str` cell's
    //! UTF-8 at most once per block, and still build the arrays the per-cell
    //! path built.
    //!
    //! `RlogWriter` only writes valid UTF-8 `Str` cells, so every fixture here
    //! is a writer-produced object whose one block is rewritten with the cells
    //! a test names, through the writer's own `write_block` and block layout.

    use super::columnar_lookup_tests::{declared_cells, sid};
    use super::*;
    use datafusion::arrow::array::{Array, BinaryArray, BooleanArray, Int64Array};
    use proptest::prelude::*;
    use proptest::strategy::ValueTree;
    use proptest::test_runner::TestRunner;
    use ravel_logseg::block::{ColumnPlan, write_block};
    use ravel_logseg::field_dir::FieldDir;
    use ravel_logseg::footer::{
        COMP_NONE, LogFooter, SectionDesc, kind, open as open_footer, write_footer_and_trailer,
    };
    use ravel_logseg::reader::read_section;
    use ravel_logseg::record::{ColumnValue, ResolvedRow, stream_attrs_bytes};
    use ravel_logseg::skip_index::SkipIndex;
    use ravel_logseg::writer::BlocksBuilder;
    use ravel_logseg::{ObjectIdentity, RlogConfig, RlogReader, RlogWriter};

    const KEY: &str = "k";
    const STREAMS: usize = 3;
    /// Every occurrence type a key can be stored under; a generated block's key
    /// has a FIELD_DIR column for a subset of them.
    const TYPES: [FieldType; 5] = [
        FieldType::Str,
        FieldType::I64,
        FieldType::F64,
        FieldType::Bool,
        FieldType::Bytes,
    ];
    const DECLARED: [DeclaredType; 4] = [
        DeclaredType::Str,
        DeclaredType::I64,
        DeclaredType::Bool,
        DeclaredType::Bytes,
    ];
    /// `Str` cells of a dictionary-heavy block: two valid values and two that
    /// are not UTF-8, so a page dictionary carries non-UTF-8 entries.
    const STR_POOL: [&[u8]; 4] = [b"a", b"bc", &[b'b', 0xff], &[0xc3]];

    fn cfg() -> RlogConfig {
        RlogConfig {
            block_target_records: 256,
            max_dynamic_columns: 16,
            ..RlogConfig::default()
        }
    }

    /// One block row: its stream and its record cell per entry of [`TYPES`]. A
    /// cell of a type the block's key has no column for is not written.
    #[derive(Clone, Debug)]
    struct Row {
        stream: usize,
        cells: Vec<Option<ColumnValue>>,
    }

    #[derive(Clone, Debug)]
    struct Block {
        /// The types the key has a FIELD_DIR column for.
        types: Vec<FieldType>,
        rows: Vec<Row>,
        /// Per stream: the key's resource value and its scope value.
        overlays: Vec<(Option<AttrValue>, Option<AttrValue>)>,
        /// Rows `lo..=hi` survive the scan's content predicate.
        lo: usize,
        hi: usize,
        /// Where the surviving rows split into two builds, in percent.
        split_pct: usize,
    }

    /// A valid seed value of each type, so the writer's FIELD_DIR carries a
    /// column for it.
    fn seed_value(ty: FieldType) -> AttrValue {
        match ty {
            FieldType::Str => AttrValue::Str("seed".into()),
            FieldType::I64 => AttrValue::I64(0),
            FieldType::F64 => AttrValue::F64(0.5),
            FieldType::Bool => AttrValue::Bool(false),
            FieldType::Bytes => AttrValue::Bytes(vec![9]),
        }
    }

    /// Write `block` as one RLOG object: the writer lays out STREAM_DIR,
    /// FIELD_DIR and the footer from seed records (row `r` on stream `r % 3`,
    /// setting the key at every type in `block.types`), then the block itself is
    /// replaced with `block.rows` verbatim, non-UTF-8 `Str` cells included.
    fn encode(block: &Block, cfg: &RlogConfig) -> Vec<u8> {
        assert!(
            block.rows.len() >= STREAMS,
            "every stream has a seed record"
        );
        let stream_attrs = |s: usize| {
            let (resource, scope) = &block.overlays[s];
            let mut res = vec![("svc".to_string(), AttrValue::Str("s".into()))];
            res.extend(resource.iter().map(|v| (KEY.to_string(), v.clone())));
            let scope: Vec<(String, AttrValue)> =
                scope.iter().map(|v| (KEY.to_string(), v.clone())).collect();
            stream_attrs_bytes(&res, "scope", "1", &scope)
        };
        let mut w = RlogWriter::new(
            *cfg,
            ObjectIdentity {
                tenant_hash: [0; 16],
                shard: 0,
                writer_id: [0; 16],
                writer_epoch: 0,
                writer_seq: 0,
            },
        );
        for r in 0..block.rows.len() {
            let s = r % STREAMS;
            w.push(LogRecord {
                stream_id: sid(s as u8),
                stream_attrs: stream_attrs(s),
                ts_ns: r as i64 * 10,
                observed_ts_ns: r as i64 * 10,
                severity_num: 9,
                severity_text: "INFO".into(),
                body: "b".into(),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs: block
                    .types
                    .iter()
                    .map(|&ty| (KEY.to_string(), seed_value(ty)))
                    .collect(),
            })
            .expect("push");
        }
        let obj = w.finish().expect("finish");

        let footer = open_footer(&obj).expect("open the writer's footer");
        let desc = *footer.section(kind::FIELD_DIR).expect("FIELD_DIR");
        let dir = FieldDir::decode(&read_section(&obj, &desc, cfg).expect("read"), u64::MAX)
            .expect("decode FIELD_DIR");
        let mut plans: Vec<(usize, ColumnPlan)> = block
            .types
            .iter()
            .map(|&ty| {
                let t = TYPES.iter().position(|&x| x == ty).expect("a known type");
                let column_id = dir.column(KEY, ty).expect("a column per type").column_id;
                (t, ColumnPlan { column_id, ty })
            })
            .collect();
        plans.sort_by_key(|(_, p)| p.column_id);
        let rows: Vec<ResolvedRow> = block
            .rows
            .iter()
            .enumerate()
            .map(|(r, row)| ResolvedRow {
                stream_ref: row.stream as u32,
                ts_ns: r as i64 * 10,
                observed_ts_ns: r as i64 * 10,
                severity_num: 9,
                severity_text: "INFO".into(),
                body: "b".into(),
                trace_id: None,
                span_id: None,
                flags: 0,
                attrs_raw: None,
                columns: plans
                    .iter()
                    .filter_map(|(t, p)| row.cells[*t].clone().map(|v| (p.column_id, v)))
                    .collect(),
                indexed_terms: Vec::new(),
                stat_winners: Vec::new(),
            })
            .collect();
        let plans: Vec<ColumnPlan> = plans.into_iter().map(|(_, p)| p).collect();
        let written = write_block(&rows, &plans, cfg.zstd_level).expect("write one block");
        let mut layout = BlocksBuilder::version_4(cfg.group_target_blocks);
        layout.push(written);
        let (blocks_bytes, l0, page_dir) = layout.finish();
        let skip = SkipIndex::build(l0).encode();
        let page_dir_bytes = page_dir.encode();

        let mut out: Vec<u8> = Vec::new();
        let mut sections: Vec<SectionDesc> = Vec::new();
        for d in &footer.sections {
            let (bytes, comp, uncomp_len) = match d.kind {
                kind::BLOCKS => (blocks_bytes.clone(), COMP_NONE, blocks_bytes.len() as u64),
                kind::SKIP_IDX => (skip.clone(), COMP_NONE, skip.len() as u64),
                kind::PAGE_DIR => (
                    page_dir_bytes.clone(),
                    COMP_NONE,
                    page_dir_bytes.len() as u64,
                ),
                _ => {
                    let start = usize::try_from(d.offset).expect("offset fits");
                    let end = start + usize::try_from(d.len).expect("len fits");
                    (obj[start..end].to_vec(), d.comp, d.uncomp_len)
                }
            };
            sections.push(SectionDesc {
                kind: d.kind,
                offset: out.len() as u64,
                len: bytes.len() as u64,
                crc32c: crc32c::crc32c(&bytes),
                comp,
                uncomp_len,
                prefix_len: d.prefix_len,
            });
            out.extend_from_slice(&bytes);
        }
        let patched = LogFooter { sections, ..footer };
        write_footer_and_trailer(&mut out, &patched);
        out
    }

    /// The content predicate that keeps rows `lo..=hi`.
    fn window(block: &Block) -> Predicate {
        Predicate::TsRange {
            min_ns: block.lo as i64 * 10,
            max_ns: block.hi as i64 * 10,
        }
    }

    // --- the pre-change per-cell path, rebuilt from the view's own accessors --

    /// Whether the record sets the key in `col` at row `i`: a `Str` cell counts
    /// only when it is UTF-8.
    fn ref_present(view: &ColumnarBlockView<'_>, col: AttrColumn, i: usize) -> bool {
        match col.ty {
            FieldType::Str => view.bytes_cursor(col.column_id).str_at(i).is_some(),
            FieldType::Bytes => view.bytes_cursor(col.column_id).at(i).is_some(),
            FieldType::I64 => view.i64_cursor(col.column_id).at(i).is_some(),
            FieldType::F64 => view.f64_bits_cursor(col.column_id).at(i).is_some(),
            FieldType::Bool => view.bool_cursor(col.column_id).at(i).is_some(),
        }
    }

    /// The record's winning occurrence at row `i`: the highest type byte, the
    /// last one on a tie.
    fn ref_winner(view: &ColumnarBlockView<'_>, cols: &[AttrColumn], i: usize) -> Option<usize> {
        cols.iter()
            .enumerate()
            .filter(|(_, c)| ref_present(view, **c, i))
            .max_by_key(|(_, c)| c.ty.to_u8())
            .map(|(k, _)| k)
    }

    /// The row's stream's resource/scope value of the key.
    fn ref_fallback(view: &ColumnarBlockView<'_>, i: usize) -> Option<AttrValue> {
        let blob = view.stream_attrs(i)?;
        let attrs = decode_stream_attrs(blob).expect("stream attrs");
        find_attr(&attrs, KEY).cloned()
    }

    /// The merged value at row `i` with its variant not yet checked: the record's
    /// winning cell when it is of the declared type, NULL when the winner is of
    /// another type, and the fallback only when the record does not set the key.
    fn ref_merged(
        view: &ColumnarBlockView<'_>,
        dc: &DeclaredColumn,
        i: usize,
    ) -> Option<AttrValue> {
        let cols: Vec<AttrColumn> = view.attr_columns_for(&dc.key).collect();
        let Some(win) = ref_winner(view, &cols, i) else {
            return ref_fallback(view, i);
        };
        let col = cols[win];
        if col.ty != declared_field_type(dc.ty) {
            return None;
        }
        match col.ty {
            FieldType::Str => view
                .bytes_cursor(col.column_id)
                .str_at(i)
                .map(|s| AttrValue::Str(s.to_string())),
            FieldType::I64 => view.i64_cursor(col.column_id).at(i).map(AttrValue::I64),
            FieldType::Bool => view.bool_cursor(col.column_id).at(i).map(AttrValue::Bool),
            FieldType::Bytes => view
                .bytes_cursor(col.column_id)
                .at(i)
                .map(|b| AttrValue::Bytes(b.to_vec())),
            FieldType::F64 => None,
        }
    }

    /// The array the pre-change build produced for rows `start..end`, down to
    /// the `Str` dictionary's layout: a dictionary page's entries in page order
    /// (NULL for a non-UTF-8 entry) with fallback values appended past them, or
    /// an identity dictionary over a plain page.
    fn per_cell_reference(
        view: &ColumnarBlockView<'_>,
        dc: &DeclaredColumn,
        start: usize,
        end: usize,
    ) -> ArrayRef {
        match dc.ty {
            DeclaredType::I64 => Arc::new(Int64Array::from(
                (start..end)
                    .map(|i| match ref_merged(view, dc, i) {
                        Some(AttrValue::I64(v)) => Some(v),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
            DeclaredType::Bool => Arc::new(BooleanArray::from(
                (start..end)
                    .map(|i| match ref_merged(view, dc, i) {
                        Some(AttrValue::Bool(v)) => Some(v),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
            )),
            DeclaredType::Bytes => {
                let cells: Vec<Option<Vec<u8>>> = (start..end)
                    .map(|i| match ref_merged(view, dc, i) {
                        Some(AttrValue::Bytes(b)) => Some(b),
                        _ => None,
                    })
                    .collect();
                Arc::new(BinaryArray::from(
                    cells.iter().map(|c| c.as_deref()).collect::<Vec<_>>(),
                ))
            }
            DeclaredType::Str => {
                let cols: Vec<AttrColumn> = view.attr_columns_for(&dc.key).collect();
                let str_col = cols.iter().find(|c| c.ty == FieldType::Str);
                let mut values: Vec<Option<String>> = Vec::new();
                let mut keys: Vec<Option<i32>> = Vec::new();
                match str_col.and_then(|c| view.str_dict(c.column_id)) {
                    Some(page) => {
                        values.extend(
                            page.dict()
                                .iter()
                                .map(|v| std::str::from_utf8(v).ok().map(str::to_string)),
                        );
                        for i in start..end {
                            let key = match ref_winner(view, &cols, i) {
                                Some(win) if cols[win].ty == FieldType::Str => {
                                    page.id_at(i).map(|id| id as i32)
                                }
                                Some(_) => None,
                                None => match ref_fallback(view, i) {
                                    Some(AttrValue::Str(s)) => {
                                        values.push(Some(s));
                                        Some(values.len() as i32 - 1)
                                    }
                                    _ => None,
                                },
                            };
                            keys.push(key);
                        }
                    }
                    None => {
                        for i in start..end {
                            keys.push(match ref_merged(view, dc, i) {
                                Some(AttrValue::Str(s)) => {
                                    values.push(Some(s));
                                    Some(values.len() as i32 - 1)
                                }
                                _ => None,
                            });
                        }
                    }
                }
                Arc::new(
                    DictionaryArray::<Int32Type>::try_new(
                        Int32Array::from(keys),
                        Arc::new(StringArray::from(values)),
                    )
                    .expect("reference dictionary"),
                )
            }
        }
    }

    // --- generated blocks ---------------------------------------------------

    fn str_cell(pooled: bool) -> BoxedStrategy<Option<ColumnValue>> {
        if pooled {
            prop_oneof![
                Just(None),
                prop::sample::select(STR_POOL.to_vec())
                    .prop_map(|b| Some(ColumnValue::Str(b.to_vec()))),
            ]
            .boxed()
        } else {
            prop_oneof![
                Just(None),
                "[a-d]{0,3}".prop_map(|s| Some(ColumnValue::Str(s.into_bytes()))),
                prop::collection::vec(any::<u8>(), 1..4).prop_map(|b| Some(ColumnValue::Str(b))),
            ]
            .boxed()
        }
    }

    fn row(pooled: bool) -> impl Strategy<Value = Row> {
        (
            0..STREAMS,
            str_cell(pooled),
            prop::option::of((-3i64..3).prop_map(ColumnValue::I64)),
            prop::option::of(any::<u64>().prop_map(ColumnValue::F64)),
            prop::option::of(any::<bool>().prop_map(ColumnValue::Bool)),
            prop::option::of(prop::collection::vec(any::<u8>(), 0..3).prop_map(ColumnValue::Bytes)),
        )
            .prop_map(|(stream, s, i, f, b, y)| Row {
                stream,
                cells: vec![s, i, f, b, y],
            })
    }

    fn overlay_value(ty: FieldType) -> BoxedStrategy<AttrValue> {
        match ty {
            FieldType::Str => prop::sample::select(vec!["r", "a"])
                .prop_map(|s| AttrValue::Str(s.into()))
                .boxed(),
            FieldType::I64 => (-2i64..2).prop_map(AttrValue::I64).boxed(),
            FieldType::F64 => Just(AttrValue::F64(1.5)).boxed(),
            FieldType::Bool => any::<bool>().prop_map(AttrValue::Bool).boxed(),
            FieldType::Bytes => Just(AttrValue::Bytes(vec![7, 7])).boxed(),
        }
    }

    /// A resource or scope value of one of `types`, or none. The writer gives
    /// the key a FIELD_DIR column for each type a stream carries it at, so a
    /// block whose overlays stay inside the record's types keeps a
    /// single-column key single.
    fn overlay(types: Vec<FieldType>) -> BoxedStrategy<Option<AttrValue>> {
        if types.is_empty() {
            return Just(None).boxed();
        }
        prop_oneof![
            Just(None),
            prop::sample::select(types).prop_flat_map(|ty| overlay_value(ty).prop_map(Some)),
        ]
        .boxed()
    }

    fn block() -> impl Strategy<Value = Block> {
        (
            prop::collection::vec(any::<bool>(), TYPES.len()),
            any::<bool>(),
            any::<bool>(),
            STREAMS..40usize,
        )
            .prop_flat_map(|(mask, pooled, narrow, n)| {
                let types: Vec<FieldType> = TYPES
                    .iter()
                    .zip(&mask)
                    .filter(|(_, m)| **m)
                    .map(|(t, _)| *t)
                    .collect();
                let overlay_types = if narrow {
                    types.clone()
                } else {
                    TYPES.to_vec()
                };
                (
                    Just(types),
                    prop::collection::vec(row(pooled), n),
                    prop::collection::vec(
                        (overlay(overlay_types.clone()), overlay(overlay_types)),
                        STREAMS,
                    ),
                    0..n,
                    0..n,
                    0usize..=100,
                )
            })
            .prop_map(|(types, rows, overlays, a, b, split_pct)| Block {
                types,
                rows,
                overlays,
                lo: a.min(b),
                hi: a.max(b),
                split_pct,
            })
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// ADR-2121 D2 and D3's contract: for every declarable type, the
        /// declared-column build over a generated block equals the pre-change
        /// per-cell build ([`per_cell_reference`]) array for array, chunk by
        /// chunk with one resolver across the chunks as the scan uses it, and
        /// equals the row path's `declared_column_array` cell for cell.
        ///
        /// The generated blocks carry resource and scope values of every type,
        /// NULL cells, one key under up to five types, a surviving-row window,
        /// non-UTF-8 `Str` cells on plain pages and non-UTF-8 entries in page
        /// dictionaries ([`the_block_generator_reaches_every_case_the_property_names`]).
        ///
        /// Flipped assertions: each mutation below fails the chunk comparison
        /// against the reference, first on an `I64` column:
        /// - the single-cursor gather taken without `single_matching`
        ///   (`single_cursor` returning `matching_cursor` alone): a row whose
        ///   winning occurrence is of another type reads the declared cell or
        ///   the fallback instead of NULL;
        /// - an absent cell appended as NULL instead of reading the
        ///   resource/scope value in the gather arms;
        /// - `Str` presence reduced to "bytes present" (a `Str` cell with bytes
        ///   is `Presence::Present` even when they are not UTF-8): a non-UTF-8
        ///   cell stops falling through to the resource/scope value.
        #[test]
        fn the_page_fast_path_builds_the_same_array_as_the_per_cell_path(block in block()) {
            let cfg = cfg();
            let obj = encode(&block, &cfg);
            let reader = RlogReader::new(&obj, &cfg).expect("open");
            let pred = window(&block);

            let mut rows = reader
                .scan_blocks(&pred, &[], &ColumnSelection::all())
                .expect("scan");
            let records = rows.next_block(&obj).expect("row exit").expect("one block");
            let merged: Vec<Vec<(String, AttrValue)>> = records
                .iter()
                .map(|r| merged_attrs(r).expect("merge"))
                .collect();

            let mut scan = reader
                .scan_blocks(&pred, &[], &ColumnSelection::all())
                .expect("scan");
            let view = scan
                .next_block_columnar(&obj)
                .expect("columnar exit")
                .expect("one block");
            let n = view.surviving_count();
            prop_assert_eq!(n, block.hi - block.lo + 1);
            prop_assert_eq!(records.len(), n);
            let split = n * block.split_pct / 100;

            for ty in DECLARED {
                let dc = DeclaredColumn::new(KEY, ty);
                let plan = DeclaredPlan::build(&view, &dc);
                let mut resolver = DeclaredResolver::new(&plan);
                let mut cache = HashMap::new();
                let mut cells = Vec::with_capacity(n);
                for (start, end) in [(0, split), (split, n)] {
                    let built =
                        build_declared_columnar_array(&view, &mut resolver, start, end, &mut cache)
                            .expect("declared array");
                    let reference = per_cell_reference(&view, &dc, start, end);
                    prop_assert_eq!(
                        built.to_data(),
                        reference.to_data(),
                        "{:?} rows {}..{}",
                        ty,
                        start,
                        end
                    );
                    cells.extend(declared_cells(&built, ty));
                }
                let row_path = declared_cells(&declared_column_array(&dc, &merged), ty);
                prop_assert_eq!(cells, row_path, "{:?} against the row path", ty);
            }
        }
    }

    // --- ADR-2773 D3: the shared-values Str build ---------------------------

    /// Records a writer lays out itself, in blocks of `block_records` and row
    /// groups of two, so the key reads a row group dictionary in some blocks
    /// and a plain page in others.
    #[derive(Clone, Debug)]
    struct Written {
        block_records: usize,
        /// Per record: its stream and its value of the key, if it sets one.
        records: Vec<(usize, Option<AttrValue>)>,
        /// Per stream: the key's resource value and its scope value.
        overlays: Vec<(Option<AttrValue>, Option<AttrValue>)>,
        /// Records `lo..=hi` survive the scan's content predicate.
        lo: usize,
        hi: usize,
    }

    #[derive(Clone, Debug)]
    enum StrSource {
        /// One block rewritten with arbitrary cells ([`encode`]): non-UTF-8
        /// cells, per-block dictionary or plain pages, several types per row.
        Rewritten(Block),
        Written(Written),
    }

    fn written_value() -> BoxedStrategy<Option<AttrValue>> {
        prop_oneof![
            3 => prop::sample::select(vec![
                "pooled value x, long enough to share",
                "pooled value y, long enough to share",
                "pooled value z, long enough to share",
            ])
            .prop_map(|s| Some(AttrValue::Str(s.into()))),
            1 => (0u32..1000).prop_map(|n| Some(AttrValue::Str(format!("u{n}")))),
            1 => (-1i64..2).prop_map(|v| Some(AttrValue::I64(v))),
            1 => Just(None),
        ]
        .boxed()
    }

    fn written() -> impl Strategy<Value = Written> {
        (4usize..24, 8usize..120)
            .prop_flat_map(|(block_records, n)| {
                let types = vec![FieldType::Str, FieldType::I64];
                (
                    Just(block_records),
                    prop::collection::vec((0..STREAMS, written_value()), n),
                    prop::collection::vec((overlay(types.clone()), overlay(types)), STREAMS),
                    0..n,
                    0..n,
                )
            })
            .prop_map(|(block_records, records, overlays, a, b)| Written {
                block_records,
                records,
                overlays,
                lo: a.min(b),
                hi: a.max(b),
            })
    }

    /// A source and the chunk size its blocks are built in.
    fn str_source() -> impl Strategy<Value = (StrSource, usize)> {
        (
            prop_oneof![
                block().prop_map(StrSource::Rewritten),
                written().prop_map(StrSource::Written),
            ],
            1usize..16,
        )
    }

    /// The object, its reader config, its content predicate and its record
    /// count.
    fn str_object(src: &StrSource) -> (Vec<u8>, RlogConfig, Predicate, usize) {
        match src {
            StrSource::Rewritten(block) => {
                let cfg = cfg();
                (encode(block, &cfg), cfg, window(block), block.rows.len())
            }
            StrSource::Written(w) => {
                let cfg = RlogConfig {
                    block_target_records: w.block_records,
                    max_dynamic_columns: 16,
                    group_target_blocks: 2,
                    ..RlogConfig::default()
                };
                let mut writer = RlogWriter::new(
                    cfg,
                    ObjectIdentity {
                        tenant_hash: [0; 16],
                        shard: 0,
                        writer_id: [0; 16],
                        writer_epoch: 0,
                        writer_seq: 0,
                    },
                );
                for (r, (s, value)) in w.records.iter().enumerate() {
                    let (resource, scope) = &w.overlays[*s];
                    let mut res = vec![("svc".to_string(), AttrValue::Str("s".into()))];
                    res.extend(resource.iter().map(|v| (KEY.to_string(), v.clone())));
                    let scope: Vec<(String, AttrValue)> =
                        scope.iter().map(|v| (KEY.to_string(), v.clone())).collect();
                    writer
                        .push(LogRecord {
                            stream_id: sid(*s as u8),
                            stream_attrs: stream_attrs_bytes(&res, "scope", "1", &scope),
                            ts_ns: r as i64 * 10,
                            observed_ts_ns: r as i64 * 10,
                            severity_num: 9,
                            severity_text: "INFO".into(),
                            body: "b".into(),
                            trace_id: None,
                            span_id: None,
                            flags: 0,
                            attrs: value.iter().map(|v| (KEY.to_string(), v.clone())).collect(),
                        })
                        .expect("push");
                }
                let pred = Predicate::TsRange {
                    min_ns: w.lo as i64 * 10,
                    max_ns: w.hi as i64 * 10,
                };
                (writer.finish().expect("finish"), cfg, pred, w.records.len())
            }
        }
    }

    /// Reads every block of `obj` under `pred`, builds the declared `Str`
    /// column in `chunk`-row chunks through one scan-wide values cache, and
    /// checks each chunk against the pre-change build ([`per_cell_reference`])
    /// and each block against the row path. Returns the cases the object
    /// exercised and its surviving row count.
    fn check_str_build(
        obj: &[u8],
        cfg: &RlogConfig,
        pred: &Predicate,
        chunk: usize,
    ) -> Result<(BTreeSet<&'static str>, usize), TestCaseError> {
        let mut seen = BTreeSet::new();
        let mut surviving = 0;
        let reader = RlogReader::new(obj, cfg).expect("open");
        let mut rows = reader
            .scan_blocks(pred, &[], &ColumnSelection::all())
            .expect("scan");
        let mut scan = reader
            .scan_blocks(pred, &[], &ColumnSelection::all())
            .expect("scan");
        let shared = Arc::new(std::sync::Mutex::new(StrValuesCache::new(Count::new())));
        let dc = DeclaredColumn::new(KEY, DeclaredType::Str);
        let mut last_dict: Option<Arc<DictValues>> = None;
        while let Some(view) = scan.next_block_columnar(obj).expect("columnar exit") {
            let records = rows.next_block(obj).expect("row exit").expect("row block");
            let n = view.surviving_count();
            surviving += n;
            prop_assert_eq!(records.len(), n);
            if chunk < n {
                seen.insert("chunks smaller than the block");
            }
            let cols: Vec<AttrColumn> = view.attr_columns_for(KEY).collect();
            let str_col = cols.iter().find(|c| c.ty == FieldType::Str);
            let page = str_col.and_then(|c| view.str_dict(c.column_id));
            match (&page, str_col) {
                (Some(p), _) => {
                    seen.insert("a dictionary page");
                    if !p.values().all_valid() {
                        seen.insert("a non-UTF-8 dictionary entry");
                    }
                    if last_dict
                        .as_ref()
                        .is_some_and(|d| Arc::ptr_eq(d, p.values()))
                    {
                        seen.insert("a dictionary shared by two blocks");
                    }
                    last_dict = Some(Arc::clone(p.values()));
                }
                (None, Some(c)) => {
                    seen.insert("a plain page");
                    let cells = view.bytes_cursor(c.column_id);
                    if (0..n).any(|i| cells.at(i).is_some() && cells.str_at(i).is_none()) {
                        seen.insert("a non-UTF-8 plain-page cell");
                    }
                }
                (None, None) => {}
            }

            let plan = DeclaredPlan::build(&view, &dc);
            let mut resolver = DeclaredResolver::with_str_values(&plan, Arc::clone(&shared));
            let mut cache = HashMap::new();
            let mut cells = Vec::with_capacity(n);
            let mut values: Option<ArrayRef> = None;
            let mut next_plain_key = 0;
            let mut fallback_keys: HashMap<u32, Option<i32>> = HashMap::new();
            for start in (0..n).step_by(chunk) {
                let end = (start + chunk).min(n);
                let built =
                    build_declared_columnar_array(&view, &mut resolver, start, end, &mut cache)
                        .expect("declared Str chunk");
                let reference = per_cell_reference(&view, &dc, start, end);
                let got = built
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .expect("declared Str dictionary array");
                let want = reference
                    .as_any()
                    .downcast_ref::<DictionaryArray<Int32Type>>()
                    .expect("reference dictionary array");
                let vals = got
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Utf8 dictionary values");
                let keys: Vec<Option<i32>> = (0..got.len())
                    .map(|j| got.keys().is_valid(j).then(|| got.keys().value(j)))
                    .collect();
                for (j, key) in keys.iter().enumerate() {
                    if let Some(k) = key {
                        let k = usize::try_from(*k).expect("non-negative key");
                        prop_assert!(
                            k < vals.len() && vals.is_valid(k),
                            "row {} keys a NULL or missing value",
                            start + j
                        );
                    }
                }
                let want_nulls: Vec<bool> =
                    (0..want.len()).map(|j| want.keys().is_null(j)).collect();
                let got_nulls: Vec<bool> = keys.iter().map(Option::is_none).collect();
                prop_assert_eq!(got_nulls, want_nulls, "key nulls, rows {}..{}", start, end);
                let chunk_cells = declared_cells(&built, DeclaredType::Str);
                prop_assert_eq!(
                    &chunk_cells,
                    &declared_cells(&reference, DeclaredType::Str),
                    "cells, rows {}..{}",
                    start,
                    end
                );
                match &values {
                    Some(v) => prop_assert!(
                        Arc::ptr_eq(v, got.values()),
                        "the chunks of a block share one values array"
                    ),
                    None => values = Some(Arc::clone(got.values())),
                }
                for (j, i) in (start..end).enumerate() {
                    let present = cols.iter().filter(|c| ref_present(&view, **c, i)).count();
                    if present > 1 {
                        seen.insert("one key under several types in one row");
                    }
                    match ref_winner(&view, &cols, i) {
                        Some(win) if cols[win].ty == FieldType::Str => match &page {
                            Some(p) => prop_assert_eq!(
                                keys[j],
                                p.key_at(i),
                                "row {} keeps its page key",
                                i
                            ),
                            None => {
                                prop_assert_eq!(
                                    keys[j],
                                    Some(next_plain_key),
                                    "row {}: plain-page keys are the identity",
                                    i
                                );
                                next_plain_key += 1;
                            }
                        },
                        Some(_) => {
                            prop_assert_eq!(keys[j], None, "row {} is won by another type", i)
                        }
                        None => {
                            if chunk_cells[j].is_some() {
                                seen.insert("a resource or scope value shown through");
                                let attrs = view.stream_attrs(i).map(|blob| {
                                    ravel_logseg::record::decode_stream_attrs(blob)
                                        .expect("stream attrs")
                                });
                                if attrs.is_some_and(|a| {
                                    a.resource.iter().all(|(k, _)| k != KEY)
                                        && a.scope_attrs.iter().any(|(k, _)| k == KEY)
                                }) {
                                    seen.insert("a scope-only fallback value");
                                }
                            } else {
                                seen.insert("a NULL row");
                            }
                            let s = view.stream_ref(i).expect("stream ref");
                            match fallback_keys.get(&s) {
                                Some(first) => {
                                    if first.is_some() {
                                        seen.insert("one stream's fallback on several rows");
                                    }
                                    prop_assert_eq!(
                                        keys[j],
                                        *first,
                                        "row {}: one fallback key per stream",
                                        i
                                    );
                                }
                                None => {
                                    fallback_keys.insert(s, keys[j]);
                                }
                            }
                        }
                    }
                }
                cells.extend(chunk_cells);
            }
            if let (Some(p), Some(v)) = (&page, &values) {
                let v = v
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .expect("Utf8 dictionary values");
                let texts: Vec<Option<&str>> = p.values().texts().collect();
                prop_assert!(v.len() >= texts.len());
                if v.len() > texts.len() {
                    seen.insert("a dictionary page plus fallback in one block (extended cache)");
                }
                let prefix: Vec<Option<&str>> = (0..texts.len())
                    .map(|k| v.is_valid(k).then(|| v.value(k)))
                    .collect();
                prop_assert_eq!(prefix, texts, "the values open with the page's dictionary");
            }
            let merged: Vec<Vec<(String, AttrValue)>> = records
                .iter()
                .map(|r| merged_attrs(r).expect("merge"))
                .collect();
            let row_path = declared_cells(&declared_column_array(&dc, &merged), DeclaredType::Str);
            prop_assert_eq!(cells, row_path, "against the row path");
        }
        if seen.contains("a dictionary page") && seen.contains("a plain page") {
            seen.insert("an object mixing dictionary and plain pages");
        }
        Ok((seen, surviving))
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]

        /// ADR-2773 D3's contract: the declared `Str` column built as chunks of
        /// one block-wide key array over shared values equals the pre-change
        /// per-cell build ([`per_cell_reference`]) chunk by chunk: the same NULL
        /// keys and the same resolved values. On top of that, a record row on a
        /// dictionary page keeps its page key, a record row on a plain page
        /// keys the identity, one stream's fallback rows share one key, no key
        /// points at a NULL value, the values open with the page's dictionary,
        /// and every chunk of a block shares one values array. The resolved
        /// cells also equal the row path's.
        ///
        /// [`the_str_source_generator_reaches_every_case_the_property_names`]
        /// pins what the sources cover.
        ///
        /// Flipped: `DictValues::from_entries` marking a non-UTF-8 entry valid
        /// fails "keys a NULL or missing value"; the chunk slice in
        /// `chunk_keys` starting one row early fails the key NULL comparison.
        #[test]
        fn declared_str_column_equals_the_per_cell_build((src, chunk) in str_source()) {
            let (obj, cfg, pred, _) = str_object(&src);
            check_str_build(&obj, &cfg, &pred, chunk)?;
        }
    }

    /// The `Str` sources are not vacuous: over the deterministic runner's first
    /// 256 cases they produce every case
    /// [`declared_str_column_equals_the_per_cell_build`] names.
    #[test]
    fn the_str_source_generator_reaches_every_case_the_property_names() {
        let mut runner = TestRunner::deterministic();
        let strategy = str_source();
        let mut seen: BTreeSet<&'static str> = BTreeSet::new();
        for _ in 0..256 {
            let (src, chunk) = strategy.new_tree(&mut runner).expect("a source").current();
            seen.insert(match src {
                StrSource::Rewritten(_) => "a rewritten block",
                StrSource::Written(_) => "a writer-built object",
            });
            let (obj, cfg, pred, records) = str_object(&src);
            let (cases, surviving) = check_str_build(&obj, &cfg, &pred, chunk).expect("property");
            seen.extend(cases);
            if surviving < records {
                seen.insert("a surviving-row subset");
            }
        }
        let want: BTreeSet<&'static str> = [
            "a rewritten block",
            "a writer-built object",
            "chunks smaller than the block",
            "a dictionary page",
            "a dictionary shared by two blocks",
            "a non-UTF-8 dictionary entry",
            "a plain page",
            "a non-UTF-8 plain-page cell",
            "an object mixing dictionary and plain pages",
            "one key under several types in one row",
            "a resource or scope value shown through",
            "a scope-only fallback value",
            "a dictionary page plus fallback in one block (extended cache)",
            "a NULL row",
            "one stream's fallback on several rows",
            "a surviving-row subset",
        ]
        .into_iter()
        .collect();
        let missing: Vec<_> = want.difference(&seen).collect();
        assert!(missing.is_empty(), "never generated: {missing:?}");
    }

    /// The generator is not vacuous: over the deterministic runner's first 256
    /// blocks it produces every case the property names.
    #[test]
    fn the_block_generator_reaches_every_case_the_property_names() {
        let cfg = cfg();
        let mut runner = TestRunner::deterministic();
        let strategy = block();
        let mut seen: BTreeSet<&'static str> = BTreeSet::new();
        for _ in 0..256 {
            let block = strategy.new_tree(&mut runner).expect("a block").current();
            let obj = encode(&block, &cfg);
            let reader = RlogReader::new(&obj, &cfg).expect("open");
            let mut scan = reader
                .scan_blocks(&window(&block), &[], &ColumnSelection::all())
                .expect("scan");
            let view = scan
                .next_block_columnar(&obj)
                .expect("columnar exit")
                .expect("one block");
            if view.surviving_count() < block.rows.len() {
                seen.insert("a surviving-row subset");
            }
            let cols: Vec<AttrColumn> = view.attr_columns_for(KEY).collect();
            if cols.len() == 1 {
                seen.insert("a single-column key");
            }
            if let Some(c) = cols.iter().find(|c| c.ty == FieldType::Str) {
                match view.str_dict(c.column_id) {
                    Some(page) if page.dict().iter().any(|v| std::str::from_utf8(v).is_err()) => {
                        seen.insert("a non-UTF-8 dictionary entry");
                    }
                    Some(_) => {}
                    None => {
                        let cells = view.bytes_cursor(c.column_id);
                        if (0..view.surviving_count())
                            .any(|i| cells.at(i).is_some() && cells.str_at(i).is_none())
                        {
                            seen.insert("a non-UTF-8 plain-page cell");
                        }
                    }
                }
            }
            for i in 0..view.surviving_count() {
                let present = cols.iter().filter(|c| ref_present(&view, **c, i)).count();
                if present > 1 {
                    seen.insert("one key under several types in one row");
                }
                if present == 0 && ref_fallback(&view, i).is_some() {
                    seen.insert("a resource or scope value shown through");
                }
                if present == 0 && ref_fallback(&view, i).is_none() {
                    seen.insert("a NULL row");
                }
            }
            if block
                .overlays
                .iter()
                .any(|(res, scope)| res.is_none() && scope.is_some())
            {
                seen.insert("a scope-only value");
            }
        }
        let want: BTreeSet<&'static str> = [
            "a surviving-row subset",
            "a single-column key",
            "a non-UTF-8 dictionary entry",
            "a non-UTF-8 plain-page cell",
            "one key under several types in one row",
            "a resource or scope value shown through",
            "a NULL row",
            "a scope-only value",
        ]
        .into_iter()
        .collect();
        assert_eq!(seen, want);
    }

    // --- UTF-8 validation count -------------------------------------------

    fn validations() -> u64 {
        CELL_TEXT_VALIDATIONS.with(|n| n.get())
    }

    /// A block with the key under `types`, one stream with no overlay, `cells`
    /// per row parallel to `types`, and rows `lo..=hi` surviving.
    fn fixed_block(
        types: &[FieldType],
        cells: Vec<Vec<Option<ColumnValue>>>,
        lo: usize,
        hi: usize,
    ) -> Block {
        Block {
            types: types.to_vec(),
            rows: cells
                .into_iter()
                .map(|by_type| {
                    let mut all = vec![None; TYPES.len()];
                    for (ty, cell) in types.iter().zip(by_type) {
                        let t = TYPES.iter().position(|x| x == ty).expect("a known type");
                        all[t] = cell;
                    }
                    Row {
                        stream: 0,
                        cells: all,
                    }
                })
                .collect(),
            overlays: vec![(None, None); STREAMS],
            lo,
            hi,
            split_pct: 50,
        }
    }

    fn str_value(s: &[u8]) -> Option<ColumnValue> {
        Some(ColumnValue::Str(s.to_vec()))
    }

    /// Build the declared `Str` column over `block` in two chunks and return
    /// the UTF-8 validations the build ran, whether the page was a dictionary
    /// page, and the cells.
    fn count_str_build(block: &Block) -> (u64, bool, Vec<Option<String>>) {
        let cfg = cfg();
        let obj = encode(block, &cfg);
        let reader = RlogReader::new(&obj, &cfg).expect("open");
        let mut scan = reader
            .scan_blocks(&window(block), &[], &ColumnSelection::all())
            .expect("scan");
        let view = scan
            .next_block_columnar(&obj)
            .expect("columnar exit")
            .expect("one block");
        let dc = DeclaredColumn::new(KEY, DeclaredType::Str);
        let n = view.surviving_count();
        let before = validations();
        let plan = DeclaredPlan::build(&view, &dc);
        let dict_page = matches!(
            plan.matching_cursor(),
            Some(DeclaredCursor::Str(c)) if c.dict.is_some()
        );
        let mut resolver = DeclaredResolver::new(&plan);
        let mut cache = HashMap::new();
        let mut cells = Vec::new();
        for (start, end) in [(0, n / 2), (n / 2, n)] {
            let built = build_declared_columnar_array(&view, &mut resolver, start, end, &mut cache)
                .expect("declared array");
            cells.extend(declared_cells(&built, DeclaredType::Str));
        }
        (validations() - before, dict_page, cells)
    }

    /// The synthetic per-key column (issue #1768) over `block` in two chunks:
    /// the UTF-8 validations the build ran and the cells.
    fn count_attr_key_build(block: &Block) -> (u64, Vec<Option<String>>) {
        use datafusion::arrow::array::Array;
        let cfg = cfg();
        let obj = encode(block, &cfg);
        let reader = RlogReader::new(&obj, &cfg).expect("open");
        let mut scan = reader
            .scan_blocks(&window(block), &[], &ColumnSelection::all())
            .expect("scan");
        let view = scan
            .next_block_columnar(&obj)
            .expect("columnar exit")
            .expect("one block");
        let n = view.surviving_count();
        let before = validations();
        let plan = AttrKeyPlan::build(&view, KEY);
        let mut resolver = AttrKeyResolver::new(&plan);
        let mut cache = HashMap::new();
        let mut cells = Vec::new();
        for (start, end) in [(0, n / 2), (n / 2, n)] {
            let built = build_attr_key_columnar_array(&view, &mut resolver, start, end, &mut cache)
                .expect("per-key array");
            let strings = built
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("Utf8 per-key column");
            cells.extend(
                (0..strings.len()).map(|i| strings.is_valid(i).then(|| strings.value(i).into())),
            );
        }
        (validations() - before, cells)
    }

    /// ADR-2121 D3: a declared `Str` cell's UTF-8 is validated at most once per
    /// block. On a plain page each surviving cell is validated once, whether
    /// the key has one occurrence (the fused read) or also an `I64` one (the
    /// presence search's validation is the text appended). On a dictionary page
    /// each entry was validated once when the reader decoded the dictionary
    /// (counted in ravel-logseg's `a_mixed_dictionary_page_validates_each_entry_once_per_block`),
    /// so neither the declared column nor the per-key column validates
    /// anything, however many rows and chunks read the page, a non-UTF-8 entry
    /// among them.
    ///
    /// Flipped assertions, each seen failing:
    /// - the plain-page multi-occurrence build re-reading the winner's text
    ///   (`matching_str.and_then(|c| c.text_at(i))` in place of the presence
    ///   search's `text`) counts 9 where 6 is expected;
    /// - `dict_values_array` reading each entry through `cell_text` (its
    ///   former non-UTF-8 arm) counts 3 where 0 is expected;
    /// - `StrCursor::text_at`'s dictionary arm validating the entry bytes
    ///   (`d.values().entry(k).and_then(cell_text)`) counts 8 per-key reads
    ///   where 0 is expected.
    #[test]
    fn a_declared_str_cell_is_validated_at_most_once_per_block() {
        // Plain page, one occurrence: rows 1..=6 survive, and 1, 3, 4 and 6 of
        // them carry a cell (4 of them), one of which is not UTF-8.
        let plain = fixed_block(
            &[FieldType::Str],
            vec![
                vec![str_value(b"r0")],
                vec![str_value(b"r1")],
                vec![None],
                vec![str_value(&[b'r', 0xff])],
                vec![str_value(b"r4")],
                vec![None],
                vec![str_value(b"r6")],
                vec![str_value(b"r7")],
            ],
            1,
            6,
        );
        let (count, dict_page, cells) = count_str_build(&plain);
        assert!(!dict_page, "unique values encode as a plain page");
        assert_eq!(count, 4, "one validation per surviving present cell");
        assert_eq!(
            cells,
            vec![
                Some("r1".into()),
                None,
                None,
                Some("r4".into()),
                None,
                Some("r6".into())
            ]
        );

        // Plain page, `Str` and `I64` occurrences: every row has a `Str` cell,
        // rows 0..=3 an `I64` one that wins over it. Rows 1..=6 survive.
        let multi = fixed_block(
            &[FieldType::Str, FieldType::I64],
            (0..8)
                .map(|r| {
                    vec![
                        str_value(format!("m{r}").as_bytes()),
                        (r < 4).then_some(ColumnValue::I64(r)),
                    ]
                })
                .collect(),
            1,
            6,
        );
        let (count, dict_page, cells) = count_str_build(&multi);
        assert!(!dict_page, "unique values encode as a plain page");
        assert_eq!(count, 6, "one validation per surviving present cell");
        assert_eq!(
            cells,
            vec![
                None,
                None,
                None,
                Some("m4".into()),
                Some("m5".into()),
                Some("m6".into())
            ]
        );

        // Dictionary page: 12 rows over three entries, one not UTF-8, all rows
        // surviving, built in two chunks.
        let entries: [&[u8]; 3] = [b"a", b"bc", &[b'b', 0xff]];
        let dict = fixed_block(
            &[FieldType::Str],
            (0..12).map(|r| vec![str_value(entries[r % 3])]).collect(),
            0,
            11,
        );
        let (count, dict_page, cells) = count_str_build(&dict);
        assert!(
            dict_page,
            "three distinct of twelve encode as a dictionary page"
        );
        assert_eq!(count, 0, "entries validated at decode, none per row");
        let want: Vec<Option<String>> = (0..12)
            .map(|r| ["a", "bc"].get(r % 3).map(|s| s.to_string()))
            .collect();
        assert_eq!(cells, want);

        let (count, cells) = count_attr_key_build(&dict);
        assert_eq!(
            count, 0,
            "a per-key read of a dictionary page validates nothing"
        );
        assert_eq!(cells, want);
    }
}
