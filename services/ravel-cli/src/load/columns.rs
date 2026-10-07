//! Reading Arrow columns into log records: the column index, the per-type cell
//! readers and dictionary-column resolution.

use super::*;

/// Resolved column indices for the mapped fields of one batch.
pub(super) struct ColumnIndex {
    pub(super) ts: usize,
    pub(super) body: Option<usize>,
    pub(super) severity_number: Option<usize>,
    pub(super) severity_text: Option<usize>,
    pub(super) trace_id: Option<usize>,
    pub(super) span_id: Option<usize>,
    /// `(index, &AttrMap)` for each resource attribute column.
    pub(super) resource: Vec<(usize, usize)>,
    /// `(index, &AttrMap)` for each record attribute column.
    pub(super) record: Vec<(usize, usize)>,
    /// The batch's columns with every mapped dictionary column resolved once.
    /// Read by the row path ([`build_record`]). Empty for an index built by
    /// [`ColumnIndex::locate`], whose columnar caller reads a dictionary
    /// column's cells in place out of the batch's own columns.
    columns: ResolvedColumns,
}

impl ColumnIndex {
    /// The index for the row path: [`ColumnIndex::locate`] plus every mapped
    /// dictionary column resolved once for [`build_record`].
    pub(super) fn resolve(batch: &RecordBatch, mapping: &Mapping) -> Result<ColumnIndex, String> {
        let mut cols = Self::locate(batch, mapping)?;
        // Every column the row path reads a string, a byte string or an id out
        // of. The ts and severity-number columns are numeric.
        let dictionary_candidates = [cols.body, cols.severity_text, cols.trace_id, cols.span_id]
            .into_iter()
            .flatten()
            .chain(cols.resource.iter().map(|(i, _)| *i))
            .chain(cols.record.iter().map(|(i, _)| *i));
        cols.columns = ResolvedColumns::resolve(batch, dictionary_candidates)?;
        Ok(cols)
    }

    /// The column indices alone, with no dictionary column resolved: the
    /// columnar path ([`build_columnar_batch`]) reads dictionaries in place,
    /// except the two id columns, which it resolves itself.
    pub(super) fn locate(batch: &RecordBatch, mapping: &Mapping) -> Result<ColumnIndex, String> {
        let schema = batch.schema();
        let idx = |name: &str| -> Result<usize, String> {
            schema
                .index_of(name)
                .map_err(|_| format!("mapped column {name:?} is not present in the Parquet file"))
        };
        let opt = |name: &Option<String>| -> Result<Option<usize>, String> {
            match name {
                Some(n) => Ok(Some(idx(n)?)),
                None => Ok(None),
            }
        };
        let resource = mapping
            .resource_attributes
            .iter()
            .enumerate()
            .map(|(i, a)| Ok((idx(&a.column)?, i)))
            .collect::<Result<Vec<_>, String>>()?;
        let record = mapping
            .attributes
            .iter()
            .enumerate()
            .map(|(i, a)| Ok((idx(&a.column)?, i)))
            .collect::<Result<Vec<_>, String>>()?;
        let ts = idx(&mapping.ts_column)?;
        let body = opt(&mapping.body_column)?;
        let severity_number = opt(&mapping.severity_number_column)?;
        let severity_text = opt(&mapping.severity_text_column)?;
        let trace_id = opt(&mapping.trace_id_column)?;
        let span_id = opt(&mapping.span_id_column)?;
        Ok(ColumnIndex {
            ts,
            body,
            severity_number,
            severity_text,
            trace_id,
            span_id,
            resource,
            record,
            columns: ResolvedColumns::none(),
        })
    }

    /// The array and index the row path reads the cell at (`row`, column `i`)
    /// from: a mapped dictionary column's own values when it was resolved, the
    /// batch's column otherwise.
    fn cell<'a>(
        &'a self,
        batch: &'a RecordBatch,
        i: usize,
        row: usize,
    ) -> Result<(&'a ArrayRef, usize), String> {
        self.columns.cell(batch, i, row)
    }

    /// Read the cell at (`row`, column `i`) with `read`, as
    /// [`ResolvedColumns::read`].
    fn read<T>(
        &self,
        batch: &RecordBatch,
        i: usize,
        row: usize,
        read: impl FnOnce(&ArrayRef, usize) -> Result<T, String>,
    ) -> Result<T, String> {
        self.columns.read(batch, i, row, read)
    }
}

/// Build one [`NormalizedLogRecord`] from row `row` of `batch`, applying the
/// kept ADR-0089 admission checks. `Err` carries a per-row rejection reason.
pub(super) fn build_record(
    batch: &RecordBatch,
    cols: &ColumnIndex,
    mapping: &Mapping,
    limits: &LogIngestLimits,
    now_ns: i64,
    row: usize,
) -> Result<NormalizedLogRecord, String> {
    // Timestamp is required; a null or unreadable ts is a row rejection.
    let (ts_col, ts_at) = cols.cell(batch, cols.ts, row)?;
    let raw_ts = read_ts(ts_col, ts_at, mapping.ts_unit)?
        .ok_or_else(|| format!("ts column {:?} is null", mapping.ts_column))?;
    if raw_ts < 0 {
        return Err(negative_ts_rejection(
            raw_ts,
            ts_col.data_type(),
            mapping.ts_unit,
        ));
    }

    // Kept: future-skew bound, same `max_future_skew_ns` as ravel-otlp. The
    // past-event-time lag check is deliberately omitted (ADR-0089 relaxation).
    let skew_ns = raw_ts.saturating_sub(now_ns);
    if skew_ns > limits.max_future_skew_ns {
        return Err(format!(
            "timestamp is {skew_ns} ns ahead of load time, more than the max future skew of {} ns",
            limits.max_future_skew_ns
        ));
    }

    // Body (optional). Kept: max_body_len.
    let body = match cols.body {
        Some(i) => cols.read(batch, i, row, read_string)?.unwrap_or_default(),
        None => String::new(),
    };
    if body.len() > limits.max_body_len {
        return Err(format!(
            "body is {} bytes, more than the limit of {}",
            body.len(),
            limits.max_body_len
        ));
    }

    let severity_num = match cols.severity_number {
        // OTLP severity_number is 0..=24; an out-of-u8 value normalizes to 0
        // (UNSPECIFIED), matching ravel-otlp rather than truncating.
        Some(i) => cols
            .read(batch, i, row, read_i64)?
            .and_then(|v| u8::try_from(v).ok())
            .unwrap_or(0),
        None => 0,
    };
    let severity_text = match cols.severity_text {
        Some(i) => cols.read(batch, i, row, read_string)?.unwrap_or_default(),
        None => String::new(),
    };

    // Trace/span ids: exact byte length or absent (never padded or truncated),
    // matching ravel-otlp.
    let trace_id = match cols.trace_id {
        Some(i) => cols.read(batch, i, row, read_id::<16>)?,
        None => None,
    };
    let span_id = match cols.span_id {
        Some(i) => cols.read(batch, i, row, read_id::<8>)?,
        None => None,
    };

    // Resource attributes: part of stream identity. A null column is omitted.
    let mut resource_attrs: Vec<(String, AttrValue)> = Vec::with_capacity(cols.resource.len());
    for (col_idx, map_idx) in &cols.resource {
        let spec = &mapping.resource_attributes[*map_idx];
        if let Some(value) = cols.read(batch, *col_idx, row, |arr, at| {
            read_attr(arr, at, spec.value_type)
        })? {
            check_attr(&spec.key, &value, limits)?;
            resource_attrs.push((spec.key.clone(), value));
        }
    }

    // Record attributes: typed values in `attrs`, never part of identity.
    let mut attrs: Vec<(String, AttrValue)> = Vec::with_capacity(cols.record.len());
    for (col_idx, map_idx) in &cols.record {
        let spec = &mapping.attributes[*map_idx];
        if let Some(value) = cols.read(batch, *col_idx, row, |arr, at| {
            read_attr(arr, at, spec.value_type)
        })? {
            check_attr(&spec.key, &value, limits)?;
            attrs.push((spec.key.clone(), value));
        }
    }

    // Loader per-record attribute cap (ADR-0089 relaxation): rejected, not
    // silently truncated. Counts record attributes only, matching OTLP's
    // `max_attributes_per_record` axis.
    if attrs.len() > LOADER_MAX_ATTRIBUTES_PER_RECORD {
        return Err(format!(
            "record has {} attributes, more than the loader per-record cap of {}",
            attrs.len(),
            LOADER_MAX_ATTRIBUTES_PER_RECORD
        ));
    }

    // Stream identity: resource attributes plus an empty scope (a Parquet file
    // carries no OTLP instrumentation scope). Computed the same way ravel-otlp
    // computes it, so the shard buffer and RLOG writer verify it identically.
    let stream_id = log_stream_id(&resource_attrs, "", "", &[]);
    let stream_attrs = ravel_logseg::stream_attrs_bytes(&resource_attrs, "", "", &[]);

    Ok(NormalizedLogRecord {
        stream_id,
        stream_attrs,
        ts_ns: raw_ts,
        observed_ts_ns: raw_ts,
        severity_num,
        severity_text,
        body,
        trace_id,
        span_id,
        flags: 0,
        attrs,
    })
}

/// Kept length caps for one attribute, re-implemented identically to
/// `ravel-otlp` (attribute key length and value payload length).
pub(super) fn check_attr(
    key: &str,
    value: &AttrValue,
    limits: &LogIngestLimits,
) -> Result<(), String> {
    check_attr_len(key, attr_value_len(value), limits)
}

/// [`check_attr`] for a value whose payload is `len` bytes, as
/// [`attr_value_len`] counts it.
pub(super) fn check_attr_len(
    key: &str,
    len: usize,
    limits: &LogIngestLimits,
) -> Result<(), String> {
    if key.len() > limits.max_attribute_key_len {
        return Err(format!(
            "attribute key {key:?} is {} bytes, more than the limit of {}",
            key.len(),
            limits.max_attribute_key_len
        ));
    }
    if len > limits.max_attribute_value_len {
        return Err(format!(
            "attribute {key:?} value is {len} bytes, more than the limit of {}",
            limits.max_attribute_value_len
        ));
    }
    Ok(())
}

/// Payload bytes in a scalar attribute value, matching `ravel-otlp`'s
/// `attr_value_len` for the scalar kinds this path can produce.
fn attr_value_len(value: &AttrValue) -> usize {
    match value {
        AttrValue::Str(s) => s.len(),
        AttrValue::Bytes(b) => b.len(),
        AttrValue::I64(_) | AttrValue::F64(_) => 8,
        AttrValue::Bool(_) => 1,
        // Unreachable from a Parquet scalar column, but sized consistently.
        AttrValue::List(items) => items.iter().map(attr_value_len).sum(),
        AttrValue::Map(entries) => entries
            .iter()
            .map(|(k, v)| k.len() + attr_value_len(v))
            .sum(),
    }
}

/// Read one cell as the declared [`ColType`], returning `None` for a null cell
/// and `Err` for a type the column cannot supply.
pub(super) fn read_attr(
    arr: &ArrayRef,
    row: usize,
    ty: ColType,
) -> Result<Option<AttrValue>, String> {
    if arr.is_null(row) {
        return Ok(None);
    }
    let value = match ty {
        ColType::Str => AttrValue::Str(
            read_string(arr, row)?.ok_or_else(|| "unexpected null reading str".to_string())?,
        ),
        ColType::I64 => AttrValue::I64(
            read_i64(arr, row)?.ok_or_else(|| "unexpected null reading i64".to_string())?,
        ),
        ColType::F64 => AttrValue::F64(
            read_f64(arr, row)?.ok_or_else(|| "unexpected null reading f64".to_string())?,
        ),
        ColType::Bool => AttrValue::Bool(
            read_bool(arr, row)?.ok_or_else(|| "unexpected null reading bool".to_string())?,
        ),
        ColType::Bytes => AttrValue::Bytes(
            read_bytes(arr, row)?.ok_or_else(|| "unexpected null reading bytes".to_string())?,
        ),
    };
    Ok(Some(value))
}

/// Read an integer cell as `i64`, accepting any Arrow integer width and the two
/// Arrow date types.
///
/// `Date32` (days since the Unix epoch) and `Date64` (milliseconds since the
/// Unix epoch) land as their native-unit `i64`: a `Date32` day count and a
/// `Date64` millisecond count, NOT converted to nanoseconds (ADR-0100). A wide
/// analytical export routinely carries a date column mapped as an `i64`
/// attribute; the stored number's unit is documented in `docs/guides/ingest.md`
/// so a mapping author knows what a comparison against it means. Dates are
/// deliberately not accepted by the `ts` path (see [`read_ts`]).
pub(super) fn read_i64(arr: &ArrayRef, row: usize) -> Result<Option<i64>, String> {
    if arr.is_null(row) {
        return Ok(None);
    }
    let v = match arr.data_type() {
        DataType::Int8 => downcast::<Int8Array>(arr)?.value(row) as i64,
        DataType::Int16 => downcast::<Int16Array>(arr)?.value(row) as i64,
        DataType::Int32 => downcast::<Int32Array>(arr)?.value(row) as i64,
        DataType::Int64 => downcast::<Int64Array>(arr)?.value(row),
        DataType::UInt8 => downcast::<UInt8Array>(arr)?.value(row) as i64,
        DataType::UInt16 => downcast::<UInt16Array>(arr)?.value(row) as i64,
        DataType::UInt32 => downcast::<UInt32Array>(arr)?.value(row) as i64,
        DataType::UInt64 => i64::try_from(downcast::<UInt64Array>(arr)?.value(row))
            .map_err(|_| "u64 value does not fit in i64".to_string())?,
        // Date32 is i32 days; Date64 is i64 milliseconds. Stored in their
        // native unit, never rescaled to nanoseconds.
        DataType::Date32 => downcast::<Date32Array>(arr)?.value(row) as i64,
        DataType::Date64 => downcast::<Date64Array>(arr)?.value(row),
        other => {
            return Err(format!(
                "expected an integer or date column, found {other:?}"
            ));
        }
    };
    Ok(Some(v))
}

/// Read a floating cell as `f64`, accepting f32 or f64.
pub(super) fn read_f64(arr: &ArrayRef, row: usize) -> Result<Option<f64>, String> {
    if arr.is_null(row) {
        return Ok(None);
    }
    let v = match arr.data_type() {
        DataType::Float32 => downcast::<Float32Array>(arr)?.value(row) as f64,
        DataType::Float64 => downcast::<Float64Array>(arr)?.value(row),
        other => return Err(format!("expected a float column, found {other:?}")),
    };
    Ok(Some(v))
}

pub(super) fn read_bool(arr: &ArrayRef, row: usize) -> Result<Option<bool>, String> {
    if arr.is_null(row) {
        return Ok(None);
    }
    match arr.data_type() {
        DataType::Boolean => Ok(Some(downcast::<BooleanArray>(arr)?.value(row))),
        other => Err(format!("expected a boolean column, found {other:?}")),
    }
}

/// The refusal for a dictionary chunk whose dictionary is empty while a key
/// names a value in it. Arrow's `normalized_keys` asserts the values array is
/// non-empty and aborts the process on this shape, so it is refused before that
/// assertion is reached, in the resolution path and in the per-cell path alike
/// (#708 guards the same shape in [`str_src`] and [`bytes_src`]).
pub(super) const EMPTY_DICTIONARY: &str = "dictionary-encoded column has an empty dictionary under a non-null \
                                key, so no value can be resolved; the Parquet file's dictionary \
                                page is corrupt";

/// The string and binary value types a dictionary column is resolved for. These
/// are exactly the types the per-cell readers below resolve a dictionary key
/// into, so resolving the column ahead of them changes no answer.
fn is_resolvable_dictionary_value(ty: &DataType) -> bool {
    matches!(
        ty,
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::FixedSizeBinary(_)
    )
}

/// A dictionary-encoded string or binary column, its keys normalized once for
/// the batch. Each cell is read out of the dictionary's own `values` in place:
/// copying the values the cells reference would overflow a `Utf8` or `Binary`
/// column's i32 offsets once the referenced bytes pass 2 GiB, failing the
/// whole batch.
pub(super) struct DictionaryColumn {
    column: ArrayRef,
    values: ArrayRef,
    keys: Vec<usize>,
}

impl DictionaryColumn {
    /// The array and index cell `row` is read from: the dictionary value its
    /// key names, or the column itself at `row` for a null key, which every
    /// reader answers as null before it reads a value.
    fn cell(&self, row: usize) -> Result<(&ArrayRef, usize), String> {
        if self.column.is_null(row) {
            return Ok((&self.column, row));
        }
        let key = self
            .keys
            .get(row)
            .copied()
            .ok_or_else(|| format!("dictionary column has no key at row {row}"))?;
        Ok((&self.values, key))
    }
}

/// A mapped column as [`resolve_dictionary_column`] resolved it for the row
/// readers.
pub(super) enum RowColumn {
    /// A column read where it is: the all-null column of an empty
    /// dictionary's value type, or (from [`id_column`]) the batch's own.
    Plain(ArrayRef),
    /// A dictionary column read in place.
    Dictionary(DictionaryColumn),
}

impl RowColumn {
    /// The array and index cell `row` is read from.
    fn cell(&self, row: usize) -> Result<(&ArrayRef, usize), String> {
        match self {
            RowColumn::Plain(arr) => Ok((arr, row)),
            RowColumn::Dictionary(dict) => dict.cell(row),
        }
    }

    /// The array every non-null cell is read out of.
    #[cfg(test)]
    fn values(&self) -> &ArrayRef {
        match self {
            RowColumn::Plain(arr) => arr,
            RowColumn::Dictionary(dict) => &dict.values,
        }
    }
}

/// Resolve a dictionary-encoded string or binary column for the row readers,
/// or `None` for a column the per-cell readers already index in place.
///
/// `DictionaryArray::normalized_keys` builds a key vector the size of the whole
/// batch on every call, so a reader that resolves a dictionary cell per row
/// costs O(rows^2) per dictionary column. Every row path resolves its mapped
/// dictionary columns once, here, and reads each cell through the result.
pub(super) fn resolve_dictionary_column(arr: &ArrayRef) -> Result<Option<RowColumn>, String> {
    let DataType::Dictionary(_, value_ty) = arr.data_type() else {
        return Ok(None);
    };
    if !is_resolvable_dictionary_value(value_ty) {
        return Ok(None);
    }
    #[cfg(test)]
    DICT_COLUMNS_RESOLVED.with(|n| n.set(n.get() + 1));
    let dict = arr.as_any_dictionary();
    if dict.values().is_empty() {
        // An all-null chunk with an empty dictionary is a shape a Parquet
        // writer emits, and resolves to an all-null column of the value type,
        // the same answer `str_src` gives it. Anything else over an empty
        // dictionary is corrupt. Either way `normalized_keys`, which asserts
        // the values array is non-empty, is never reached.
        if arr.null_count() != arr.len() {
            return Err(EMPTY_DICTIONARY.to_string());
        }
        return Ok(Some(RowColumn::Plain(new_null_array(value_ty, arr.len()))));
    }
    Ok(Some(RowColumn::Dictionary(DictionaryColumn {
        column: Arc::clone(arr),
        values: Arc::clone(dict.values()),
        keys: dict.normalized_keys(),
    })))
}

/// One batch's columns, with every mapped dictionary column resolved once by
/// [`resolve_dictionary_column`]. Every other column is the batch's own.
pub(super) struct ResolvedColumns {
    /// Indexed by column; `None` for a column read as the batch's own.
    columns: Vec<Option<RowColumn>>,
}

impl ResolvedColumns {
    /// Resolve the columns `mapped` names. A column named twice (two attributes
    /// reading one column) resolves on the first pass only.
    pub(super) fn resolve(
        batch: &RecordBatch,
        mapped: impl IntoIterator<Item = usize>,
    ) -> Result<ResolvedColumns, String> {
        let mut columns: Vec<Option<RowColumn>> = Vec::new();
        columns.resize_with(batch.num_columns(), || None);
        for i in mapped {
            let Some(slot) = columns.get_mut(i) else {
                continue;
            };
            if slot.is_none() {
                *slot = resolve_dictionary_column(batch.column(i))?;
            }
        }
        Ok(ResolvedColumns { columns })
    }

    /// No column resolved: [`ResolvedColumns::cell`] answers every index with
    /// the batch's own column.
    fn none() -> ResolvedColumns {
        ResolvedColumns {
            columns: Vec::new(),
        }
    }

    /// The array and index the cell at (`row`, column `i`) of `batch` is read
    /// from.
    pub(super) fn cell<'a>(
        &'a self,
        batch: &'a RecordBatch,
        i: usize,
        row: usize,
    ) -> Result<(&'a ArrayRef, usize), String> {
        match self.columns.get(i).and_then(Option::as_ref) {
            Some(resolved) => resolved.cell(row),
            None => Ok((batch.column(i), row)),
        }
    }

    /// Read the cell at (`row`, column `i`) of `batch` with `read`, from where
    /// [`ResolvedColumns::cell`] says it is.
    pub(super) fn read<T>(
        &self,
        batch: &RecordBatch,
        i: usize,
        row: usize,
        read: impl FnOnce(&ArrayRef, usize) -> Result<T, String>,
    ) -> Result<T, String> {
        let (arr, at) = self.cell(batch, i, row)?;
        read(arr, at)
    }

    /// The array column `i`'s non-null cells are read out of: a resolved
    /// dictionary column's own values, the batch's column otherwise.
    #[cfg(test)]
    pub(super) fn col<'a>(&'a self, batch: &'a RecordBatch, i: usize) -> &'a ArrayRef {
        match self.columns.get(i).and_then(Option::as_ref) {
            Some(resolved) => resolved.values(),
            None => batch.column(i),
        }
    }
}

/// The dictionary key at `row`, refusing an empty dictionary rather than
/// aborting inside arrow's `normalized_keys`.
///
/// The row paths index columns [`ResolvedColumns`] has already resolved, so
/// this is reached only by a caller handed a dictionary column directly.
pub(super) fn dictionary_key(arr: &ArrayRef, row: usize) -> Result<usize, String> {
    #[cfg(test)]
    DICT_CELL_KEYS_RESOLVED.with(|n| n.set(n.get() + 1));
    let dict = arr.as_any_dictionary();
    if dict.values().is_empty() {
        return Err(EMPTY_DICTIONARY.to_string());
    }
    dict.normalized_keys()
        .get(row)
        .copied()
        .ok_or_else(|| format!("dictionary column has no key at row {row}"))
}

#[cfg(test)]
thread_local! {
    /// Columns [`resolve_dictionary_column`] has resolved, and attrs map
    /// children [`MapChild::new`](super::spans::MapChild::new) has normalized
    /// the keys of, on this thread,
    /// for the tests that pin one resolution per dictionary column per batch. A
    /// thread local rather than a global: tests share a process.
    pub(super) static DICT_COLUMNS_RESOLVED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// The same for [`dictionary_key`], which is the per-cell cost.
    pub(super) static DICT_CELL_KEYS_RESOLVED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Zero both dictionary counters and return a handle that reads them.
#[cfg(test)]
pub(super) fn dict_counters() -> DictCounters {
    DICT_COLUMNS_RESOLVED.with(|n| n.set(0));
    DICT_CELL_KEYS_RESOLVED.with(|n| n.set(0));
    DictCounters
}

#[cfg(test)]
pub(super) struct DictCounters;

#[cfg(test)]
impl DictCounters {
    /// Dictionary columns resolved once each, ahead of the row loop.
    pub(super) fn columns(&self) -> u64 {
        DICT_COLUMNS_RESOLVED.with(std::cell::Cell::get)
    }

    /// Dictionary keys resolved per cell, which is the quadratic cost.
    pub(super) fn cell_keys(&self) -> u64 {
        DICT_CELL_KEYS_RESOLVED.with(std::cell::Cell::get)
    }
}

/// Read a UTF-8 string cell, accepting `Utf8` and `LargeUtf8`.
pub(super) fn read_string(arr: &ArrayRef, row: usize) -> Result<Option<String>, String> {
    Ok(read_str_ref(arr, row)?.map(str::to_string))
}

/// [`read_string`] borrowing the cell out of the array.
fn read_str_ref(arr: &ArrayRef, row: usize) -> Result<Option<&str>, String> {
    if arr.is_null(row) {
        return Ok(None);
    }
    match arr.data_type() {
        DataType::Utf8 => Ok(Some(downcast::<StringArray>(arr)?.value(row))),
        DataType::LargeUtf8 => Ok(Some(downcast::<LargeStringArray>(arr)?.value(row))),
        // A dictionary-encoded string column (Arrow reconstructs one from a
        // Parquet file that carries Arrow dictionary schema metadata): resolve
        // the row's key to its value and read that. The row paths reach this
        // arm only for a column [`ResolvedColumns`] did not resolve.
        DataType::Dictionary(_, _) => {
            let dict = arr.as_any_dictionary();
            read_str_ref(dict.values(), dictionary_key(arr, row)?)
        }
        other => Err(format!("expected a string column, found {other:?}")),
    }
}

/// Read a binary cell, accepting `Binary`, `LargeBinary`, and `FixedSizeBinary`.
pub(super) fn read_bytes(arr: &ArrayRef, row: usize) -> Result<Option<Vec<u8>>, String> {
    Ok(read_bytes_ref(arr, row)?.map(<[u8]>::to_vec))
}

/// [`read_bytes`] borrowing the cell out of the array.
fn read_bytes_ref(arr: &ArrayRef, row: usize) -> Result<Option<&[u8]>, String> {
    if arr.is_null(row) {
        return Ok(None);
    }
    match arr.data_type() {
        DataType::Binary => Ok(Some(downcast::<BinaryArray>(arr)?.value(row))),
        DataType::LargeBinary => Ok(Some(downcast::<LargeBinaryArray>(arr)?.value(row))),
        DataType::FixedSizeBinary(_) => Ok(Some(downcast::<FixedSizeBinaryArray>(arr)?.value(row))),
        // Dictionary-encoded binary column: resolve the key to its value, as in
        // [`read_str_ref`].
        DataType::Dictionary(_, _) => {
            let dict = arr.as_any_dictionary();
            read_bytes_ref(dict.values(), dictionary_key(arr, row)?)
        }
        other => Err(format!("expected a binary column, found {other:?}")),
    }
}

/// The row rejection for a negative resolved `ts`, the logs and metrics
/// counterpart of the spans load's refusal: OTLP's timestamps are `u64`.
/// Every unit conversion multiplies by a positive factor and refuses overflow,
/// so a negative result always comes from a negative cell, never from the
/// unit.
///
/// The rejection names the unit [`read_ts`] and [`ts_src`] actually applied to
/// `ts_type`, as [`ts_read_unit`] phrases it.
pub(super) fn negative_ts_rejection(ts_ns: i64, ts_type: &DataType, declared: TsUnit) -> String {
    let unit = ts_read_unit(ts_type, declared, "ts_unit");
    format!(
        "timestamp is before the Unix epoch ({ts_ns} ns, {unit}); the column holds a negative value"
    )
}

/// The unit [`read_ts`] applied to a timestamp column of type `ts_type`, as a
/// refusal names it: a native Arrow `Timestamp` column's own unit, the
/// declared unit (under its mapping key `unit_key`) for any other column.
pub(super) fn ts_read_unit(ts_type: &DataType, declared: TsUnit, unit_key: &str) -> String {
    match ts_type {
        DataType::Timestamp(unit, _) => {
            let unit = match unit {
                TimeUnit::Second => TsUnit::Seconds,
                TimeUnit::Millisecond => TsUnit::Millis,
                TimeUnit::Microsecond => TsUnit::Micros,
                TimeUnit::Nanosecond => TsUnit::Nanos,
            };
            format!("read in the column's own Timestamp unit, {}", unit.as_str())
        }
        _ => format!("read as {unit_key} = {}", declared.as_str()),
    }
}

/// Read the `ts` column to nanoseconds. An integer column uses the mapping's
/// declared unit; a native Arrow `Timestamp` column uses its own unit (its
/// values are already scaled), and the declared unit is not applied again.
pub(super) fn read_ts(arr: &ArrayRef, row: usize, declared: TsUnit) -> Result<Option<i64>, String> {
    if arr.is_null(row) {
        return Ok(None);
    }
    let ns = match arr.data_type() {
        DataType::Timestamp(unit, _) => {
            let raw = match unit {
                TimeUnit::Second => downcast::<TimestampSecondArray>(arr)?.value(row),
                TimeUnit::Millisecond => downcast::<TimestampMillisecondArray>(arr)?.value(row),
                TimeUnit::Microsecond => downcast::<TimestampMicrosecondArray>(arr)?.value(row),
                TimeUnit::Nanosecond => downcast::<TimestampNanosecondArray>(arr)?.value(row),
            };
            let factor = match unit {
                TimeUnit::Second => 1_000_000_000,
                TimeUnit::Millisecond => 1_000_000,
                TimeUnit::Microsecond => 1_000,
                TimeUnit::Nanosecond => 1,
            };
            raw.checked_mul(factor)
                .ok_or_else(|| "timestamp overflows i64 nanoseconds".to_string())?
        }
        // A date column is not a valid event-time source: it lands as a native-
        // unit i64 attribute (read_i64), never rescaled to nanoseconds through
        // the ts path (ADR-0100). Reject it here rather than let the read_i64
        // fallback below silently multiply a day/millisecond count by the
        // declared ts unit.
        DataType::Date32 | DataType::Date64 => {
            return Err(format!(
                "ts column has date type {:?}; a date is not a valid ts source. Map it as an i64 \
                 attribute instead (its value is days since the epoch for Date32, milliseconds \
                 for Date64).",
                arr.data_type()
            ));
        }
        _ => {
            let raw =
                read_i64(arr, row)?.ok_or_else(|| "unexpected null reading ts".to_string())?;
            raw.checked_mul(declared.factor())
                .ok_or_else(|| "timestamp overflows i64 nanoseconds".to_string())?
        }
    };
    Ok(Some(ns))
}

/// Read an id column as an exact-length byte array, accepting either a binary
/// column of exactly `N` bytes or a hex string of exactly `2*N` characters. A
/// wrong length yields `None` (dropped, never padded or truncated), matching
/// ravel-otlp.
pub(super) fn read_id<const N: usize>(
    arr: &ArrayRef,
    row: usize,
) -> Result<Option<[u8; N]>, String> {
    if arr.is_null(row) {
        return Ok(None);
    }
    let bytes = match arr.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 => match read_string(arr, row)? {
            Some(s) => match hex::decode(s) {
                Ok(b) => b,
                Err(_) => return Ok(None),
            },
            None => return Ok(None),
        },
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            read_bytes(arr, row)?.unwrap_or_default()
        }
        // The fallback for a dictionary id column [`ResolvedColumns`] did not
        // resolve: resolve the row's key and read the value it names, by this
        // same rule. Every production caller reads a resolved column, so a
        // load (`dictionary_encoded_hex_id_columns_load`) never reaches this
        // arm; `unresolved_dictionary_id_cells_read_by_value` covers it.
        DataType::Dictionary(_, _) => {
            let dict = arr.as_any_dictionary();
            return read_id::<N>(dict.values(), dictionary_key(arr, row)?);
        }
        other => {
            return Err(format!(
                "expected a binary or string id column, found {other:?}"
            ));
        }
    };
    Ok(<[u8; N]>::try_from(bytes.as_slice()).ok())
}

/// Downcast an [`ArrayRef`] to a concrete Arrow array type, mapping a failure
/// to a readable error rather than panicking.
pub(super) fn downcast<A: 'static>(arr: &ArrayRef) -> Result<&A, String> {
    arr.as_any()
        .downcast_ref::<A>()
        .ok_or_else(|| "internal error: Arrow array downcast failed".to_string())
}

/// [`downcast`] as an `Option`, for the prepared column readers below: the
/// datatype has already been matched, so a `None` here is an internal
/// inconsistency the reader turns into a deferred error rather than a panic.
fn downcast_opt<A: 'static>(arr: &ArrayRef) -> Option<&A> {
    arr.as_any().downcast_ref::<A>()
}

/// The [`FieldType`] a mapped scalar column resolves to. Fixed by the mapping's
/// declared [`ColType`], so it is resolved once per column, not per cell
/// (ADR-0109 decision 6).
pub(super) fn field_type_of(ty: ColType) -> FieldType {
    match ty {
        ColType::Str => FieldType::Str,
        ColType::I64 => FieldType::I64,
        ColType::F64 => FieldType::F64,
        ColType::Bool => FieldType::Bool,
        ColType::Bytes => FieldType::Bytes,
    }
}

// ---------------------------------------------------------------------------
// Prepared per-column readers (ADR-0109 decision 6): the Arrow downcast and the
// `ts` unit scaling are resolved ONCE when the reader is built, not per cell. A
// column whose datatype the mapping cannot supply is captured as `Bad`, whose
// reader returns `Ok(None)` for a null cell and the SAME typed error the per-cell
// `read_*` helpers raise for a non-null one, so admission parity with the row
// path (`build_record`) holds byte for byte, including which row a coercion error
// is reported at.
// ---------------------------------------------------------------------------

/// An integer/date source resolved to a concrete Arrow array.
pub(super) enum IntSrc<'a> {
    I8(&'a Int8Array),
    I16(&'a Int16Array),
    I32(&'a Int32Array),
    I64(&'a Int64Array),
    U8(&'a UInt8Array),
    U16(&'a UInt16Array),
    U32(&'a UInt32Array),
    U64(&'a UInt64Array),
    D32(&'a Date32Array),
    D64(&'a Date64Array),
    Bad(&'a ArrayRef),
}

pub(super) fn int_src(arr: &ArrayRef) -> IntSrc<'_> {
    match arr.data_type() {
        DataType::Int8 => downcast_opt(arr).map_or(IntSrc::Bad(arr), IntSrc::I8),
        DataType::Int16 => downcast_opt(arr).map_or(IntSrc::Bad(arr), IntSrc::I16),
        DataType::Int32 => downcast_opt(arr).map_or(IntSrc::Bad(arr), IntSrc::I32),
        DataType::Int64 => downcast_opt(arr).map_or(IntSrc::Bad(arr), IntSrc::I64),
        DataType::UInt8 => downcast_opt(arr).map_or(IntSrc::Bad(arr), IntSrc::U8),
        DataType::UInt16 => downcast_opt(arr).map_or(IntSrc::Bad(arr), IntSrc::U16),
        DataType::UInt32 => downcast_opt(arr).map_or(IntSrc::Bad(arr), IntSrc::U32),
        DataType::UInt64 => downcast_opt(arr).map_or(IntSrc::Bad(arr), IntSrc::U64),
        DataType::Date32 => downcast_opt(arr).map_or(IntSrc::Bad(arr), IntSrc::D32),
        DataType::Date64 => downcast_opt(arr).map_or(IntSrc::Bad(arr), IntSrc::D64),
        _ => IntSrc::Bad(arr),
    }
}

impl IntSrc<'_> {
    pub(super) fn get(&self, row: usize) -> Result<Option<i64>, String> {
        match self {
            IntSrc::I8(a) => Ok((!a.is_null(row)).then(|| a.value(row) as i64)),
            IntSrc::I16(a) => Ok((!a.is_null(row)).then(|| a.value(row) as i64)),
            IntSrc::I32(a) => Ok((!a.is_null(row)).then(|| a.value(row) as i64)),
            IntSrc::I64(a) => Ok((!a.is_null(row)).then(|| a.value(row))),
            IntSrc::U8(a) => Ok((!a.is_null(row)).then(|| a.value(row) as i64)),
            IntSrc::U16(a) => Ok((!a.is_null(row)).then(|| a.value(row) as i64)),
            IntSrc::U32(a) => Ok((!a.is_null(row)).then(|| a.value(row) as i64)),
            IntSrc::U64(a) => {
                if a.is_null(row) {
                    return Ok(None);
                }
                i64::try_from(a.value(row))
                    .map(Some)
                    .map_err(|_| "u64 value does not fit in i64".to_string())
            }
            IntSrc::D32(a) => Ok((!a.is_null(row)).then(|| a.value(row) as i64)),
            IntSrc::D64(a) => Ok((!a.is_null(row)).then(|| a.value(row))),
            IntSrc::Bad(arr) => {
                if arr.is_null(row) {
                    Ok(None)
                } else {
                    Err(format!(
                        "expected an integer or date column, found {:?}",
                        arr.data_type()
                    ))
                }
            }
        }
    }
}

/// A float source resolved to a concrete Arrow array.
pub(super) enum FloatSrc<'a> {
    F32(&'a Float32Array),
    F64(&'a Float64Array),
    Bad(&'a ArrayRef),
}

fn float_src(arr: &ArrayRef) -> FloatSrc<'_> {
    match arr.data_type() {
        DataType::Float32 => downcast_opt(arr).map_or(FloatSrc::Bad(arr), FloatSrc::F32),
        DataType::Float64 => downcast_opt(arr).map_or(FloatSrc::Bad(arr), FloatSrc::F64),
        _ => FloatSrc::Bad(arr),
    }
}

impl FloatSrc<'_> {
    fn get(&self, row: usize) -> Result<Option<f64>, String> {
        match self {
            FloatSrc::F32(a) => Ok((!a.is_null(row)).then(|| a.value(row) as f64)),
            FloatSrc::F64(a) => Ok((!a.is_null(row)).then(|| a.value(row))),
            FloatSrc::Bad(arr) => {
                if arr.is_null(row) {
                    Ok(None)
                } else {
                    Err(format!(
                        "expected a float column, found {:?}",
                        arr.data_type()
                    ))
                }
            }
        }
    }
}

/// A boolean source resolved to a concrete Arrow array.
pub(super) enum BoolSrc<'a> {
    B(&'a BooleanArray),
    Bad(&'a ArrayRef),
}

fn bool_src(arr: &ArrayRef) -> BoolSrc<'_> {
    match arr.data_type() {
        DataType::Boolean => downcast_opt(arr).map_or(BoolSrc::Bad(arr), BoolSrc::B),
        _ => BoolSrc::Bad(arr),
    }
}

impl BoolSrc<'_> {
    fn get(&self, row: usize) -> Result<Option<bool>, String> {
        match self {
            BoolSrc::B(a) => Ok((!a.is_null(row)).then(|| a.value(row))),
            BoolSrc::Bad(arr) => {
                if arr.is_null(row) {
                    Ok(None)
                } else {
                    Err(format!(
                        "expected a boolean column, found {:?}",
                        arr.data_type()
                    ))
                }
            }
        }
    }
}

/// A UTF-8 source resolved to a concrete Arrow array. `Dict` carries a
/// dictionary-encoded column (Arrow reconstructs one from a Parquet file that
/// embeds Arrow dictionary schema metadata), read in place through its
/// normalized keys.
pub(super) enum StrSrc<'a> {
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Dict {
        arr: &'a ArrayRef,
        values: &'a ArrayRef,
        keys: Vec<usize>,
    },
    /// An all-null column: every row yields `None`. Used for a dictionary whose
    /// values array is empty (or whose every key is null), where there is no
    /// value any row could resolve to.
    AllNull,
    Bad(&'a ArrayRef),
}

// See #708 (empty-dictionary panic) and #680 (decode/encode overlap).
pub(super) fn str_src(arr: &ArrayRef) -> StrSrc<'_> {
    match arr.data_type() {
        DataType::Utf8 => downcast_opt(arr).map_or(StrSrc::Bad(arr), StrSrc::Utf8),
        DataType::LargeUtf8 => downcast_opt(arr).map_or(StrSrc::Bad(arr), StrSrc::LargeUtf8),
        DataType::Dictionary(_, value_ty)
            if matches!(**value_ty, DataType::Utf8 | DataType::LargeUtf8) =>
        {
            let dict = arr.as_any_dictionary();
            // arrow 59.1's `normalized_keys` asserts the values array is
            // non-empty, so an empty (or wholly-null) dictionary chunk that a
            // Parquet writer may emit must take the all-null path first (#708).
            if dict.values().is_empty() || arr.null_count() == arr.len() {
                return StrSrc::AllNull;
            }
            StrSrc::Dict {
                arr,
                values: dict.values(),
                keys: dict.normalized_keys(),
            }
        }
        _ => StrSrc::Bad(arr),
    }
}

impl<'a> StrSrc<'a> {
    #[cfg(test)]
    pub(super) fn get(&self, row: usize) -> Result<Option<String>, String> {
        Ok(self.get_ref(row)?.map(str::to_string))
    }

    /// The cell at `row`, borrowed out of the Arrow array.
    pub(super) fn get_ref(&self, row: usize) -> Result<Option<&'a str>, String> {
        match *self {
            StrSrc::Utf8(a) => Ok((!a.is_null(row)).then(|| a.value(row))),
            StrSrc::LargeUtf8(a) => Ok((!a.is_null(row)).then(|| a.value(row))),
            StrSrc::Dict {
                arr,
                values,
                ref keys,
            } => {
                if arr.is_null(row) {
                    return Ok(None);
                }
                read_str_ref(values, keys[row])
            }
            StrSrc::AllNull => Ok(None),
            StrSrc::Bad(arr) => {
                if arr.is_null(row) {
                    Ok(None)
                } else {
                    Err(format!(
                        "expected a string column, found {:?}",
                        arr.data_type()
                    ))
                }
            }
        }
    }
}

/// A reservation hint for the cells one mapped attribute column adds to its
/// dynamic column: its non-null cell count and, for a string or byte column,
/// the value bytes those cells hold. Exact for a plain array. A dictionary
/// array's bytes are its cell count times the mean length of its DISTINCT
/// values, not of its cells, so it over- or under-reserves when cells
/// reference the distinct values unevenly, without bound: the caller caps the
/// sum. Read from array metadata, never per cell.
pub(super) fn cell_capacity_hint(arr: &ArrayRef, ty: ColType) -> (usize, usize) {
    let cells = arr.len().saturating_sub(arr.null_count());
    let bytes = match ty {
        ColType::Str | ColType::Bytes => value_bytes_hint(arr, cells),
        ColType::I64 | ColType::F64 | ColType::Bool => 0,
    };
    (cells, bytes)
}

fn value_bytes_hint(arr: &ArrayRef, cells: usize) -> usize {
    match arr.data_type() {
        DataType::Utf8 => {
            downcast_opt::<StringArray>(arr).map_or(0, |a| offsets_span(a.value_offsets()))
        }
        DataType::LargeUtf8 => {
            downcast_opt::<LargeStringArray>(arr).map_or(0, |a| offsets_span(a.value_offsets()))
        }
        DataType::Binary => {
            downcast_opt::<BinaryArray>(arr).map_or(0, |a| offsets_span(a.value_offsets()))
        }
        DataType::LargeBinary => {
            downcast_opt::<LargeBinaryArray>(arr).map_or(0, |a| offsets_span(a.value_offsets()))
        }
        DataType::FixedSizeBinary(width) => {
            usize::try_from(*width).unwrap_or(0).saturating_mul(cells)
        }
        DataType::Dictionary(_, _) => {
            let values = arr.as_any_dictionary().values();
            match values.len() {
                0 => 0,
                distinct => value_bytes_hint(values, distinct).saturating_mul(cells) / distinct,
            }
        }
        _ => 0,
    }
}

/// The value bytes between the first and the last of `offsets`.
fn offsets_span<O: Copy + Into<i64>>(offsets: &[O]) -> usize {
    match (offsets.first(), offsets.last()) {
        (Some(first), Some(last)) => usize::try_from((*last).into() - (*first).into()).unwrap_or(0),
        _ => 0,
    }
}

/// A binary source resolved to a concrete Arrow array. `Dict` is the binary
/// analogue of [`StrSrc::Dict`].
pub(super) enum BytesSrc<'a> {
    Bin(&'a BinaryArray),
    LargeBin(&'a LargeBinaryArray),
    FixedBin(&'a FixedSizeBinaryArray),
    Dict {
        arr: &'a ArrayRef,
        values: &'a ArrayRef,
        keys: Vec<usize>,
    },
    /// The binary analogue of [`StrSrc::AllNull`]: every row yields `None`.
    AllNull,
    Bad(&'a ArrayRef),
}

fn bytes_src(arr: &ArrayRef) -> BytesSrc<'_> {
    match arr.data_type() {
        DataType::Binary => downcast_opt(arr).map_or(BytesSrc::Bad(arr), BytesSrc::Bin),
        DataType::LargeBinary => downcast_opt(arr).map_or(BytesSrc::Bad(arr), BytesSrc::LargeBin),
        DataType::FixedSizeBinary(_) => {
            downcast_opt(arr).map_or(BytesSrc::Bad(arr), BytesSrc::FixedBin)
        }
        DataType::Dictionary(_, value_ty)
            if matches!(
                **value_ty,
                DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_)
            ) =>
        {
            let dict = arr.as_any_dictionary();
            // See the #708 guard in `str_src`.
            if dict.values().is_empty() || arr.null_count() == arr.len() {
                return BytesSrc::AllNull;
            }
            BytesSrc::Dict {
                arr,
                values: dict.values(),
                keys: dict.normalized_keys(),
            }
        }
        _ => BytesSrc::Bad(arr),
    }
}

impl<'a> BytesSrc<'a> {
    /// The cell at `row`, borrowed out of the Arrow array.
    fn get_ref(&self, row: usize) -> Result<Option<&'a [u8]>, String> {
        match *self {
            BytesSrc::Bin(a) => Ok((!a.is_null(row)).then(|| a.value(row))),
            BytesSrc::LargeBin(a) => Ok((!a.is_null(row)).then(|| a.value(row))),
            BytesSrc::FixedBin(a) => Ok((!a.is_null(row)).then(|| a.value(row))),
            BytesSrc::Dict {
                arr,
                values,
                ref keys,
            } => {
                if arr.is_null(row) {
                    return Ok(None);
                }
                read_bytes_ref(values, keys[row])
            }
            BytesSrc::AllNull => Ok(None),
            BytesSrc::Bad(arr) => {
                if arr.is_null(row) {
                    Ok(None)
                } else {
                    Err(format!(
                        "expected a binary column, found {:?}",
                        arr.data_type()
                    ))
                }
            }
        }
    }
}

/// A `ts` source with its unit scaling resolved once (ADR-0109 decision 6). A
/// native Arrow `Timestamp` scales by its own unit; an integer column scales by
/// the mapping's declared unit; a date column is rejected as an invalid ts
/// source, exactly as [`read_ts`].
pub(super) enum TsSrc<'a> {
    Sec(&'a TimestampSecondArray),
    Milli(&'a TimestampMillisecondArray),
    Micro(&'a TimestampMicrosecondArray),
    Nano(&'a TimestampNanosecondArray),
    Int { src: IntSrc<'a>, factor: i64 },
    DateErr(&'a ArrayRef),
}

pub(super) fn ts_src(arr: &ArrayRef, declared: TsUnit) -> TsSrc<'_> {
    match arr.data_type() {
        DataType::Timestamp(unit, _) => match unit {
            TimeUnit::Second => downcast_opt(arr).map_or_else(
                || TsSrc::Int {
                    src: int_src(arr),
                    factor: declared.factor(),
                },
                TsSrc::Sec,
            ),
            TimeUnit::Millisecond => downcast_opt(arr).map_or_else(
                || TsSrc::Int {
                    src: int_src(arr),
                    factor: declared.factor(),
                },
                TsSrc::Milli,
            ),
            TimeUnit::Microsecond => downcast_opt(arr).map_or_else(
                || TsSrc::Int {
                    src: int_src(arr),
                    factor: declared.factor(),
                },
                TsSrc::Micro,
            ),
            TimeUnit::Nanosecond => downcast_opt(arr).map_or_else(
                || TsSrc::Int {
                    src: int_src(arr),
                    factor: declared.factor(),
                },
                TsSrc::Nano,
            ),
        },
        DataType::Date32 | DataType::Date64 => TsSrc::DateErr(arr),
        _ => TsSrc::Int {
            src: int_src(arr),
            factor: declared.factor(),
        },
    }
}

impl TsSrc<'_> {
    pub(super) fn get(&self, row: usize) -> Result<Option<i64>, String> {
        let overflow = || "timestamp overflows i64 nanoseconds".to_string();
        let scale = |raw: i64, factor: i64| raw.checked_mul(factor).map(Some).ok_or_else(overflow);
        match self {
            TsSrc::Sec(a) if a.is_null(row) => Ok(None),
            TsSrc::Sec(a) => scale(a.value(row), 1_000_000_000),
            TsSrc::Milli(a) if a.is_null(row) => Ok(None),
            TsSrc::Milli(a) => scale(a.value(row), 1_000_000),
            TsSrc::Micro(a) if a.is_null(row) => Ok(None),
            TsSrc::Micro(a) => scale(a.value(row), 1_000),
            TsSrc::Nano(a) if a.is_null(row) => Ok(None),
            TsSrc::Nano(a) => scale(a.value(row), 1),
            TsSrc::Int { src, factor } => match src.get(row)? {
                Some(raw) => scale(raw, *factor),
                None => Ok(None),
            },
            TsSrc::DateErr(arr) => {
                if arr.is_null(row) {
                    Ok(None)
                } else {
                    Err(format!(
                        "ts column has date type {:?}; a date is not a valid ts source. Map it as \
                         an i64 attribute instead (its value is days since the epoch for Date32, \
                         milliseconds for Date64).",
                        arr.data_type()
                    ))
                }
            }
        }
    }
}

/// A trace/span id source: a hex string or a raw binary column, resolved once.
/// The reader yields the candidate bytes (or `None` for a null cell or an
/// undecodable hex string); the caller length-checks into `[u8; N]`, dropping a
/// wrong length exactly as [`read_id`].
pub(super) enum IdSrc<'a> {
    Hex(&'a ArrayRef),
    Bin(&'a ArrayRef),
    /// A dictionary id column read in place, by the [`IdSrc::Hex`] rule when
    /// `hex` is set and the [`IdSrc::Bin`] rule otherwise.
    Dict {
        column: &'a DictionaryColumn,
        hex: bool,
    },
    Bad(&'a ArrayRef),
}

pub(super) fn id_src(arr: &ArrayRef) -> IdSrc<'_> {
    match arr.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 => IdSrc::Hex(arr),
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => IdSrc::Bin(arr),
        _ => IdSrc::Bad(arr),
    }
}

/// An id column as [`id_column_src`] reads it: a dictionary-encoded string or
/// binary column resolved by [`resolve_dictionary_column`] (a null key is a
/// null cell), any other column the batch's own.
pub(super) fn id_column(arr: &ArrayRef) -> Result<RowColumn, String> {
    Ok(resolve_dictionary_column(arr)?.unwrap_or_else(|| RowColumn::Plain(Arc::clone(arr))))
}

/// The [`IdSrc`] over an [`id_column`]: a dictionary column's cells are read
/// out of its own values, by the rule [`id_src`] picks for the value type.
pub(super) fn id_column_src(col: &RowColumn) -> IdSrc<'_> {
    match col {
        RowColumn::Plain(arr) => id_src(arr),
        RowColumn::Dictionary(column) => IdSrc::Dict {
            column,
            hex: matches!(
                column.values.data_type(),
                DataType::Utf8 | DataType::LargeUtf8
            ),
        },
    }
}

impl IdSrc<'_> {
    pub(super) fn get(&self, row: usize) -> Result<Option<Vec<u8>>, String> {
        match self {
            IdSrc::Hex(arr) => Ok(read_string(arr, row)?.and_then(|s| hex::decode(s).ok())),
            IdSrc::Bin(arr) => read_bytes(arr, row),
            IdSrc::Dict { column, hex } => {
                let (arr, at) = column.cell(row)?;
                if *hex {
                    Ok(read_string(arr, at)?.and_then(|s| hex::decode(s).ok()))
                } else {
                    read_bytes(arr, at)
                }
            }
            IdSrc::Bad(arr) => {
                if arr.is_null(row) {
                    Ok(None)
                } else {
                    Err(format!(
                        "expected a binary or string id column, found {:?}",
                        arr.data_type()
                    ))
                }
            }
        }
    }
}

/// One present attribute cell read in place: a string or byte cell borrows the
/// Arrow array's buffer, so its bytes are copied once, into the column the cell
/// lands in.
#[derive(Clone, Copy)]
pub(super) enum CellRef<'a> {
    I64(i64),
    F64(f64),
    Bool(bool),
    Str(&'a str),
    Bytes(&'a [u8]),
}

impl CellRef<'_> {
    /// Payload bytes, as [`attr_value_len`] counts the owned value.
    pub(super) fn value_len(self) -> usize {
        match self {
            CellRef::Str(s) => s.len(),
            CellRef::Bytes(b) => b.len(),
            CellRef::I64(_) | CellRef::F64(_) => 8,
            CellRef::Bool(_) => 1,
        }
    }

    pub(super) fn to_value(self) -> AttrValue {
        match self {
            CellRef::I64(v) => AttrValue::I64(v),
            CellRef::F64(v) => AttrValue::F64(v),
            CellRef::Bool(v) => AttrValue::Bool(v),
            CellRef::Str(s) => AttrValue::Str(s.to_string()),
            CellRef::Bytes(b) => AttrValue::Bytes(b.to_vec()),
        }
    }
}

/// One mapped scalar attribute column's source, resolved once to its declared
/// [`ColType`].
pub(super) enum AttrSrc<'a> {
    Int(IntSrc<'a>),
    Float(FloatSrc<'a>),
    Bool(BoolSrc<'a>),
    Str(StrSrc<'a>),
    Bytes(BytesSrc<'a>),
}

pub(super) fn attr_src(arr: &ArrayRef, ty: ColType) -> AttrSrc<'_> {
    match ty {
        ColType::Str => AttrSrc::Str(str_src(arr)),
        ColType::I64 => AttrSrc::Int(int_src(arr)),
        ColType::F64 => AttrSrc::Float(float_src(arr)),
        ColType::Bool => AttrSrc::Bool(bool_src(arr)),
        ColType::Bytes => AttrSrc::Bytes(bytes_src(arr)),
    }
}

impl<'a> AttrSrc<'a> {
    pub(super) fn get(&self, row: usize) -> Result<Option<AttrValue>, String> {
        Ok(self.get_ref(row)?.map(CellRef::to_value))
    }

    /// The cell at `row`, a string or byte cell borrowed out of the array.
    pub(super) fn get_ref(&self, row: usize) -> Result<Option<CellRef<'a>>, String> {
        Ok(match self {
            AttrSrc::Int(s) => s.get(row)?.map(CellRef::I64),
            AttrSrc::Float(s) => s.get(row)?.map(CellRef::F64),
            AttrSrc::Bool(s) => s.get(row)?.map(CellRef::Bool),
            AttrSrc::Str(s) => s.get_ref(row)?.map(CellRef::Str),
            AttrSrc::Bytes(s) => s.get_ref(row)?.map(CellRef::Bytes),
        })
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests;
