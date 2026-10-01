use super::*;

// ---------------------------------------------------------------------------
// Spans load (ADR-1751 decision 1 and 2, follow-up task 2)
//
// Everything structural is the metrics path's: provision or validate the
// signal, build the router from the same `build_ingest_config`, read one
// sequential cursor, write `WriteMode::Strict` batches through the same
// in-flight window (`Inflight`, `resolve_sequential_write` and friends). What
// differs is normalisation: one source row is exactly one
// `ravel_otlp::NormalizedSpan`, and the attribute coercion, the reserved
// attrs keys and the status mapping are OTLP's own, so a span loaded here and
// the same span sent over OTLP are stored as the same record.
// ---------------------------------------------------------------------------

/// Result of a successful (or partially-durable) spans load.
#[derive(Debug, Clone, Default)]
pub struct SpansLoadReport {
    /// Source rows whose spans acked durable, in submission order. One row is
    /// one span on this path, so this is also the span count.
    pub rows_processed: u64,
    /// `--skip-rows`, clamped to the file's total row count.
    pub rows_skipped: u64,
    /// `--skip-rows` as the operator gave it, before the clamp.
    pub skip_rows_requested: u64,
    /// The file's total row count, read from the Parquet footer.
    pub file_total_rows: u64,
    /// One token per shard acked, across every batch, in submission order.
    pub tokens: Vec<CommitToken>,
    /// Attributes dropped for a value over the OTLP value-length cap, or for an
    /// `attrs_map_column` key over the OTLP key-length cap. The span itself is
    /// kept, so a nonzero count means the stored record is an approximation of
    /// the source row and nothing else in the load says so.
    ///
    /// Counted where the row is BUILT, not where it acks: an attribute dropped
    /// from a span whose batch later failed to flush is counted here and its
    /// span is in no object. That is the direction to be wrong in, since the
    /// figure exists to make an approximation visible rather than to be
    /// reconciled against the stored records.
    pub attributes_dropped: u64,
    pub elapsed: Duration,
}

impl SpansLoadReport {
    /// Distinct commit tokens, which is the number of RSPAN objects the load
    /// wrote. Same derivation as [`LoadReport::objects_written`].
    pub fn objects_written(&self) -> usize {
        let mut seen = std::collections::HashSet::new();
        self.tokens
            .iter()
            .filter(|t| seen.insert(t.encode()))
            .count()
    }
}

/// Resolved column indices for the mapped fields of one spans batch.
struct SpansColumnIndex<'a> {
    trace_id: usize,
    span_id: usize,
    parent_span_id: Option<usize>,
    name: usize,
    start_ts: usize,
    end_ts: usize,
    status_code: Option<usize>,
    status_message: Option<usize>,
    /// One column index per `[[spans.resource_attribute]]`, in mapping order.
    resource_attributes: Vec<usize>,
    /// One column index per `[[spans.attribute]]`, in mapping order.
    attributes: Vec<usize>,
    /// The `attrs_map_column`, already checked to be a map of strings.
    attrs_map: Option<SpansAttrsMap<'a>>,
    /// The batch's columns with every mapped dictionary column resolved once.
    columns: ResolvedColumns,
}

impl<'a> SpansColumnIndex<'a> {
    /// Resolve every mapped column against this batch's schema, and check the
    /// id columns' types and declared widths here rather than per row: a
    /// `FixedSizeBinary(n)` states its width in the schema, so a mapping that
    /// points `trace_id_column` at an 8-byte column is a mapping error that
    /// can be reported before the first row is decoded.
    ///
    /// Each mapped dictionary column has its keys normalized here too, once
    /// for the whole batch rather than once per cell, and is read in place
    /// ([`resolve_dictionary_column`]), as the `attrs_map_column`'s key and
    /// value dictionaries are ([`SpansAttrsMap::resolve`]). `mapped_keys` is
    /// the mapping's [`SpansMapping::mapped_keys`], built once per load.
    fn resolve(
        batch: &RecordBatch,
        mapping: &SpansMapping,
        mapped_keys: &'a MappedKeys,
    ) -> Result<SpansColumnIndex<'a>, String> {
        let schema = batch.schema();
        let idx = |name: &str| -> Result<usize, String> {
            schema
                .index_of(name)
                .map_err(|_| format!("mapped column {name:?} is not present in the Parquet file"))
        };
        let id_idx = |name: &str, width: usize, empty_is_root: bool| -> Result<usize, String> {
            let i = idx(name)?;
            check_id_column(schema.field(i).data_type(), name, width, empty_is_root)?;
            Ok(i)
        };
        let trace_id = id_idx(&mapping.trace_id_column, 16, false)?;
        let span_id = id_idx(&mapping.span_id_column, 8, false)?;
        let parent_span_id = match &mapping.parent_span_id_column {
            Some(c) => Some(id_idx(c, 8, true)?),
            None => None,
        };
        let name = idx(&mapping.name_column)?;
        let start_ts = idx(&mapping.start_ts_column)?;
        let end_ts = idx(&mapping.end_ts_column)?;
        let status_code = match &mapping.status_code_column {
            Some(c) => Some(idx(c)?),
            None => None,
        };
        let status_message = match &mapping.status_message_column {
            Some(c) => Some(idx(c)?),
            None => None,
        };
        let resource_attributes = mapping
            .resource_attributes
            .iter()
            .map(|a| idx(&a.column))
            .collect::<Result<Vec<_>, String>>()?;
        let attributes = mapping
            .attributes
            .iter()
            .map(|a| idx(&a.column))
            .collect::<Result<Vec<_>, String>>()?;
        let attrs_map = match &mapping.attrs_map_column {
            Some(c) => {
                let i = idx(c)?;
                check_attrs_map_column(schema.field(i).data_type(), c)?;
                Some(SpansAttrsMap::resolve(batch.column(i), c, mapped_keys)?)
            }
            None => None,
        };
        // Every column a row reader may read a string, a byte string or an id
        // out of. The timestamp and status columns are numeric and carry no
        // dictionary a reader resolves.
        let dictionary_candidates = [Some(trace_id), Some(span_id), parent_span_id, Some(name)]
            .into_iter()
            .flatten()
            .chain(status_message)
            .chain(resource_attributes.iter().copied())
            .chain(attributes.iter().copied());
        let columns = ResolvedColumns::resolve(batch, dictionary_candidates)?;
        Ok(SpansColumnIndex {
            trace_id,
            span_id,
            parent_span_id,
            name,
            start_ts,
            end_ts,
            status_code,
            status_message,
            resource_attributes,
            attributes,
            attrs_map,
            columns,
        })
    }

    /// The array column `i`'s non-null cells are read out of, as
    /// [`ResolvedColumns::col`].
    #[cfg(test)]
    fn col<'b>(&'b self, batch: &'b RecordBatch, i: usize) -> &'b ArrayRef {
        self.columns.col(batch, i)
    }

    /// The array and index the row readers read the cell at (`row`, column
    /// `i`) from, as [`ResolvedColumns::cell`].
    fn cell<'b>(
        &'b self,
        batch: &'b RecordBatch,
        i: usize,
        row: usize,
    ) -> Result<(&'b ArrayRef, usize), String> {
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

/// Check that an id column can supply an exact-width id, by the same rule
/// [`read_id`] reads one: a binary or hex-string column. A
/// `FixedSizeBinary(n)` whose `n` is not the id's width can never produce one,
/// and says so in the schema, so it is refused when the batch's columns are
/// resolved, before any row of it is built or written.
///
/// `empty_is_root` is set for the parent column, where an empty value names no
/// parent rather than a malformed id: a `FixedSizeBinary(0)` column then says
/// every row is a root span, which is a file this loader can read.
///
/// A dictionary column is judged by its VALUE type, which is what a resolved
/// dictionary column carries and what [`read_id`] resolves each key to. A hex
/// id column reaches here in either form: as `Dictionary(_, Utf8)` when
/// [`dictionary_preserving_schema`] retyped it, which needs every chunk of it
/// dictionary encoded on every data page, and as plain `Utf8` otherwise, as a
/// column whose dictionary outgrew the writer's page limit is.
fn check_id_column(
    data_type: &DataType,
    column: &str,
    width: usize,
    empty_is_root: bool,
) -> Result<(), String> {
    match data_type {
        DataType::Dictionary(_, values) => check_id_column(values, column, width, empty_is_root),
        DataType::FixedSizeBinary(0) if empty_is_root => Ok(()),
        DataType::FixedSizeBinary(n) if *n as usize != width => Err(format!(
            "id column {column:?} is FixedSizeBinary({n}), but this id is {width} bytes. Ravel \
             never pads or truncates an id, so no row of this column can produce one."
        )),
        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_) => Ok(()),
        other => Err(format!(
            "id column {column:?} has type {other:?}; expected a binary column of {width} bytes \
             or a hex string column of {} characters",
            width * 2
        )),
    }
}

/// Whether an id cell carries no value at all: a null cell, an empty binary
/// value, an empty string, or a zero-width fixed-size value. On the parent
/// column this is OTLP's own root-span test, which reads the `parent_span_id`
/// field's emptiness and nothing else.
fn id_cell_is_empty(arr: &ArrayRef, row: usize) -> Result<bool, String> {
    if arr.is_null(row) {
        return Ok(true);
    }
    match arr.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 => {
            Ok(read_string(arr, row)?.is_none_or(|s| s.is_empty()))
        }
        // The fallback for a dictionary parent column [`ResolvedColumns`] did
        // not resolve, as in [`read_id`]: the emptiness that decides a root
        // span is the VALUE's, so the key is resolved first rather than the
        // column falling to the binary branch below. A load never reaches this
        // arm; `unresolved_dictionary_id_cells_read_by_value` covers it.
        DataType::Dictionary(_, _) => {
            let dict = arr.as_any_dictionary();
            id_cell_is_empty(dict.values(), dictionary_key(arr, row)?)
        }
        _ => Ok(read_bytes(arr, row)?.is_none_or(|b| b.is_empty())),
    }
}

/// Read the status column through `ravel-otlp`'s own enum mapping.
///
/// Every value outside `0..=2` is `Unset` there, so a value too wide for `i64`
/// (a `UInt64` cell above `i64::MAX`) is `Unset` here too rather than a
/// refusal: it is outside the enum by more, not by a different kind.
fn read_status_code(arr: &ArrayRef, row: usize) -> Result<StatusCode, String> {
    if arr.is_null(row) {
        return Ok(StatusCode::Unset);
    }
    let code = match arr.data_type() {
        DataType::UInt64 => i64::try_from(downcast::<UInt64Array>(arr)?.value(row)).ok(),
        _ => read_i64(arr, row)?,
    };
    Ok(match code {
        None => StatusCode::Unset,
        Some(code) => status_code_from_i32(i32::try_from(code).unwrap_or(i32::MAX)),
    })
}

/// Decode one source row against the mapping into a [`NormalizedSpan`].
///
/// The check order is [`ravel_otlp::traces_normalize`]'s `normalize_span`, and
/// the order is what decides which rejection a row with several problems
/// reports. The one admission rule deliberately absent is the past-event-time
/// lag bound (ADR-0089's relaxation, widened to every signal by ADR-1751
/// decision 1); the future-skew bound is kept, and anchors on the span's end
/// exactly as `checked_span_interval` does.
/// `dropped` accumulates this row's over-cap attribute drops; see
/// [`read_span_attrs`].
fn build_span(
    batch: &RecordBatch,
    cols: &SpansColumnIndex,
    mapping: &SpansMapping,
    limits: &SpanIngestLimits,
    now_ns: i64,
    row: usize,
    dropped: &mut u64,
) -> Result<NormalizedSpan, String> {
    let name = cols
        .read(batch, cols.name, row, read_string)?
        .ok_or_else(|| format!("name column {:?} is null", mapping.name_column))?;
    if name.len() > limits.max_name_len {
        return Err(format!(
            "span name is {} bytes, more than the limit of {}",
            name.len(),
            limits.max_name_len
        ));
    }

    // trace_id and span_id are the record's identity and RSPAN's sort key, so
    // a wrong width is a rejection and never a pad or a truncation. `read_id`
    // reports a wrong width as `None`, which is indistinguishable here from a
    // null cell; both are the same refusal, since neither can name a span.
    let trace_id = cols
        .read(batch, cols.trace_id, row, read_id::<16>)?
        .ok_or_else(|| {
            format!(
                "trace_id column {:?} is null, or is not a 16-byte value (or a 32-character hex \
             string). Ravel never pads or truncates an id.",
                mapping.trace_id_column
            )
        })?;
    let span_id = cols
        .read(batch, cols.span_id, row, read_id::<8>)?
        .ok_or_else(|| {
            format!(
                "span_id column {:?} is null, or is not an 8-byte value (or a 16-character hex \
             string). Ravel never pads or truncates an id.",
                mapping.span_id_column
            )
        })?;
    // A null parent cell and an EMPTY one are both a root span: OTLP reads an
    // empty `parent_span_id` field as a root, and empty bytes or "" is how
    // common trace exports write one. A present, non-empty, wrong-width value
    // is a rejection here, where the OTLP path drops it and admits the span:
    // an OTLP sender's malformed field is one record of a live stream, while a
    // mapped column producing unusable ids is a mapping mistake the whole file
    // shares, and a silently-rerooted span tree is not visible in the data.
    let parent_span_id = match cols.parent_span_id {
        None => None,
        Some(i) => {
            let (column, at) = cols.cell(batch, i, row)?;
            if id_cell_is_empty(column, at)? {
                None
            } else {
                Some(read_id::<8>(column, at)?.ok_or_else(|| {
                    format!(
                        "parent_span_id column {:?} is not an 8-byte value (or a 16-character hex \
                         string). Ravel never pads or truncates an id; leave the cell null or \
                         empty for a root span.",
                        mapping.parent_span_id_column.as_deref().unwrap_or_default()
                    )
                })?)
            }
        }
    };

    // A zero start takes load time and a zero end takes the start, exactly as
    // `normalize_span` does for the zeros an under-instrumented OTLP sender
    // emits. A NULL cell has no OTLP counterpart and is refused instead: in a
    // file the operator controls it is a mapping or export mistake, and
    // placing a span at load time because a column was empty would hide it.
    let (start_col, start_at) = cols.cell(batch, cols.start_ts, row)?;
    let start_cell_ns = read_ts(start_col, start_at, mapping.start_ts_unit)?
        .ok_or_else(|| format!("start_ts column {:?} is null", mapping.start_ts_column))?;
    let start_ts_ns = match start_cell_ns {
        0 => now_ns,
        v => v,
    };
    let (end_col, end_at) = cols.cell(batch, cols.end_ts, row)?;
    let end_cell_ns = read_ts(end_col, end_at, mapping.end_ts_unit)?
        .ok_or_else(|| format!("end_ts column {:?} is null", mapping.end_ts_column))?;
    let end_ts_ns = match end_cell_ns {
        0 => start_ts_ns,
        v => v,
    };
    // A negative timestamp has no OTLP counterpart (its two are `u64`), and a
    // negative start beside a positive end stores a span whose interval
    // overlaps nearly every query window. Unit conversion cannot flip a sign,
    // so a negative value always comes from a negative cell.
    if start_ts_ns < 0 || end_ts_ns < 0 {
        // A substituted value was never read from its own column, so naming
        // that column's unit would send the operator looking for a value it
        // does not hold.
        let start_unit = if start_cell_ns == 0 {
            "taken from load time because start_ts is 0".to_string()
        } else {
            ts_read_unit(
                start_col.data_type(),
                mapping.start_ts_unit,
                "start_ts_unit",
            )
        };
        let end_unit = if end_cell_ns == 0 {
            "taken from start_ts because end_ts is 0".to_string()
        } else {
            ts_read_unit(end_col.data_type(), mapping.end_ts_unit, "end_ts_unit")
        };
        return Err(format!(
            "span timestamps are before the Unix epoch (start {start_ts_ns} ns, {start_unit}; \
             end {end_ts_ns} ns, {end_unit}); a timestamp column holds a negative value"
        ));
    }
    if end_ts_ns < start_ts_ns {
        return Err(format!(
            "span ends at {end_ts_ns} ns, before it starts at {start_ts_ns} ns"
        ));
    }
    // Kept: the future-skew bound, at the spans OTLP limit, anchored on the
    // end. The past-lag check is deliberately omitted.
    let skew_ns = end_ts_ns.saturating_sub(now_ns);
    if skew_ns > limits.max_future_skew_ns {
        return Err(format!(
            "span end is {skew_ns} ns ahead of load time, more than the max future skew of {} ns",
            limits.max_future_skew_ns
        ));
    }

    let status_code = match cols.status_code {
        None => StatusCode::Unset,
        Some(i) => cols.read(batch, i, row, read_status_code)?,
    };
    let status_message = match cols.status_message {
        None => None,
        Some(i) => match cols.read(batch, i, row, read_string)? {
            None => None,
            Some(message) => {
                if message.len() > limits.max_status_message_len {
                    return Err(format!(
                        "status message is {} bytes, more than the limit of {}",
                        message.len(),
                        limits.max_status_message_len
                    ));
                }
                // An empty message is no message, as it is on the OTLP path.
                if message.is_empty() {
                    None
                } else {
                    Some(message)
                }
            }
        },
    };

    let resource_attrs = read_span_attrs(
        batch,
        cols,
        &cols.resource_attributes,
        &mapping.resource_attributes,
        limits,
        row,
        dropped,
    )?;
    let mut span_attrs = read_span_attrs(
        batch,
        cols,
        &cols.attributes,
        &mapping.attributes,
        limits,
        row,
        dropped,
    )?;
    if let Some(map) = &cols.attrs_map {
        let entries = read_span_attrs_map(map, row, span_attrs.len(), limits, dropped)?;
        span_attrs.extend(entries);
    }
    // The same merge the OTLP path runs, with an empty scope set: this loader
    // maps no instrumentation scope, so there is nothing between resource and
    // span precedence. The reserved-key strip `normalize_span` applies is not
    // repeated here because `SpansMapping::validate` refuses a mapping naming
    // any reserved key outright and `read_span_attrs_map` refuses a row whose
    // map holds one, so no merged map can hold one.
    let attrs = merge_attrs(&resource_attrs, &[], &span_attrs);

    Ok(NormalizedSpan {
        trace_id,
        span_id,
        parent_span_id,
        name,
        start_ts_ns,
        end_ts_ns,
        status_code,
        status_message,
        attrs,
    })
}

/// Read one row's mapped attributes for one precedence set, coerced to the
/// `Map<Utf8, Utf8>` strings RSPAN stores.
///
/// `dropped` counts the values this row lost to the value-length cap, so the
/// summary can say the stored record is an approximation. OTLP reports the
/// same drop as `AttributeValueTooLong` in its partial-success message.
#[allow(clippy::too_many_arguments)]
fn read_span_attrs(
    batch: &RecordBatch,
    cols: &SpansColumnIndex,
    indices: &[usize],
    maps: &[AttrMap],
    limits: &SpanIngestLimits,
    row: usize,
    dropped: &mut u64,
) -> Result<Vec<(String, String)>, String> {
    let mut out = Vec::with_capacity(indices.len());
    for (col, map) in indices.iter().zip(maps) {
        // A null cell is an attribute this row does not carry, the same as an
        // OTLP span that simply omits the key. An EMPTY string is a value and
        // is stored: unlike a metric label, an empty attribute value is
        // meaningful on the span path and OTLP keeps it.
        let Some(value) = cols.read(batch, *col, row, |arr, at| {
            read_attr(arr, at, map.value_type)
        })?
        else {
            continue;
        };
        let value = span_attr_string(&map.key, &value)?;
        // An over-cap value drops THAT attribute and keeps the span, which is
        // `convert_attrs_lossy`'s rule on the OTLP path: no span attribute
        // feeds an identity here, so one unstorable value never has to reject
        // its neighbours or the record they sit on.
        if value.len() > limits.max_attribute_value_len {
            *dropped += 1;
            continue;
        }
        out.push((map.key.clone(), value));
    }
    Ok(out)
}

/// Coerce one typed cell to the string RSPAN stores, by
/// `ravel_otlp::traces_normalize`'s own `convert_value` mapping: a bool and an
/// integer take their canonical string form, a float goes through the shared
/// [`format_float`], and bytes become lowercase hex. A list or map has no
/// Parquet scalar column source and no RSPAN representation, and is refused
/// rather than given a stringification this code would be inventing.
pub(crate) fn span_attr_string(key: &str, value: &AttrValue) -> Result<String, String> {
    Ok(match value {
        AttrValue::Str(s) => s.clone(),
        AttrValue::Bool(b) => b.to_string(),
        AttrValue::I64(i) => i.to_string(),
        AttrValue::F64(f) => format_float(*f),
        AttrValue::Bytes(b) => hex::encode(b),
        AttrValue::List(_) | AttrValue::Map(_) => {
            return Err(format!(
                "attribute {key:?} is a list or map, which RSPAN's Map<Utf8, Utf8> attrs cannot \
                 represent"
            ));
        }
    })
}

/// Check that the `attrs_map_column` is a map from strings to strings, the
/// shape `ravel-cli export` writes, when the batch's columns are resolved. A
/// dictionary-encoded key or value is a string too: [`read_string`] resolves
/// it.
fn check_attrs_map_column(data_type: &DataType, column: &str) -> Result<(), String> {
    fn is_string(ty: &DataType) -> bool {
        match ty {
            DataType::Utf8 | DataType::LargeUtf8 => true,
            DataType::Dictionary(_, values) => is_string(values),
            _ => false,
        }
    }
    if let DataType::Map(entries, _) = data_type
        && let DataType::Struct(fields) = entries.data_type()
        && fields.len() == 2
        && is_string(fields[0].data_type())
        && is_string(fields[1].data_type())
    {
        return Ok(());
    }
    Err(format!(
        "attrs_map_column {column:?} has type {data_type:?}; expected a map of string keys to \
         string values"
    ))
}

/// One batch's `attrs_map_column`, resolved once by [`SpansAttrsMap::resolve`]
/// for every row [`read_span_attrs_map`] reads out of it.
struct SpansAttrsMap<'a> {
    /// The mapping's name for the column, as a refusal quotes it.
    column: String,
    /// The batch's map column; its offsets and nulls index `keys` and `values`.
    map: MapArray,
    /// The map's key and value children.
    keys: MapChild,
    values: MapChild,
    /// Every key a mapped attribute names, with the column it reads.
    mapped_keys: &'a MappedKeys,
}

/// Every key a spans mapping's attributes name, with the column each reads:
/// [`SpansMapping::mapped_keys`].
pub(super) type MappedKeys = std::collections::HashMap<String, String>;

/// One string child of a spans attrs map, read per entry by [`MapChild::get`].
enum MapChild {
    /// A plain string child, or a dictionary child whose dictionary is empty,
    /// read by [`read_str`].
    Plain(ArrayRef),
    /// A dictionary child, its keys normalized once for the batch. Entries are
    /// read out of `values` in place: copying the values an entry references
    /// would overflow a `Utf8` child's i32 offsets once the referenced bytes
    /// pass 2 GiB, even when every one of them is over the value cap.
    Dictionary {
        child: ArrayRef,
        values: ArrayRef,
        keys: Vec<usize>,
    },
}

impl MapChild {
    /// Reading a dictionary child per entry calls `normalized_keys`, which
    /// builds a key vector the size of the whole child, so a row's entries
    /// would cost O(entries^2); it is called once here instead. A child whose
    /// dictionary is empty is left plain, so arrow's assertion in
    /// `normalized_keys` is never reached: every key of such a child is null,
    /// and its row is refused as holding a null key.
    fn new(child: &ArrayRef) -> Self {
        if matches!(child.data_type(), DataType::Dictionary(_, _)) {
            let dict = child.as_any_dictionary();
            if !dict.values().is_empty() {
                #[cfg(test)]
                DICT_COLUMNS_RESOLVED.with(|n| n.set(n.get() + 1));
                return MapChild::Dictionary {
                    child: Arc::clone(child),
                    values: Arc::clone(dict.values()),
                    keys: dict.normalized_keys(),
                };
            }
        }
        MapChild::Plain(Arc::clone(child))
    }

    /// Entry `i`'s string, borrowed out of the child, by the rule
    /// [`read_str`] reads a cell by.
    fn get(&self, i: usize) -> Result<Option<&str>, String> {
        match self {
            MapChild::Plain(arr) => read_str(arr, i),
            MapChild::Dictionary {
                child,
                values,
                keys,
            } => {
                if child.is_null(i) {
                    return Ok(None);
                }
                let key = keys
                    .get(i)
                    .copied()
                    .ok_or_else(|| format!("dictionary column has no key at row {i}"))?;
                read_str(values, key)
            }
        }
    }
}

impl<'a> SpansAttrsMap<'a> {
    /// Resolve the map column `arr` of one batch, already checked by
    /// [`check_attrs_map_column`]: each child's dictionary keys once, as
    /// [`MapChild::new`] describes. `mapped_keys` holds the mapped keys a map
    /// entry may not repeat.
    fn resolve(arr: &ArrayRef, column: &str, mapped_keys: &'a MappedKeys) -> Result<Self, String> {
        let map = arr
            .as_map_opt()
            .ok_or_else(|| format!("attrs_map_column {column:?} is not a map column"))?
            .clone();
        let keys = MapChild::new(map.keys());
        let values = MapChild::new(map.values());
        Ok(SpansAttrsMap {
            column: column.to_string(),
            map,
            keys,
            values,
            mapped_keys,
        })
    }
}

/// A string cell borrowed out of `arr`, read by the rule [`read_string`]
/// reads an owned one by.
fn read_str(arr: &ArrayRef, row: usize) -> Result<Option<&str>, String> {
    if arr.is_null(row) {
        return Ok(None);
    }
    match arr.data_type() {
        DataType::Utf8 => Ok(Some(downcast::<StringArray>(arr)?.value(row))),
        DataType::LargeUtf8 => Ok(Some(downcast::<LargeStringArray>(arr)?.value(row))),
        DataType::Dictionary(_, _) => {
            let dict = arr.as_any_dictionary();
            read_str(dict.values(), dictionary_key(arr, row)?)
        }
        other => Err(format!("expected a string column, found {other:?}")),
    }
}

/// Read one row's `attrs_map_column` entries, the attributes the mapping does
/// not name, as the `(key, value)` strings RSPAN stores.
///
/// A null cell and a null value are attributes the row does not carry, so an
/// entry with a null value is skipped before any check below. Each other
/// entry goes through the OTLP path's own attribute rule: a key or value over
/// its length cap drops that attribute, counted in `dropped`, and keeps the
/// span. A row is refused when its map holds a key a mapped attribute also
/// names, a key twice, or a reserved key: one span carries one attrs map with
/// unique keys, so which value reached the record would otherwise be decided
/// silently, and a reserved key would fabricate a span field this version does
/// not map.
///
/// A row is also refused when its `span_attributes` `[[spans.attribute]]`
/// values and its kept entries together pass
/// [`LOADER_MAX_ATTRIBUTES_PER_RECORD`]. That refusal comes after every entry
/// is checked, since any refusal above takes precedence over it and the
/// message states the full count, but no entry past the cap is copied out.
fn read_span_attrs_map(
    map: &SpansAttrsMap,
    row: usize,
    span_attributes: usize,
    limits: &SpanIngestLimits,
    dropped: &mut u64,
) -> Result<Vec<(String, String)>, String> {
    if map.map.is_null(row) {
        return Ok(Vec::new());
    }
    let column = map.column.as_str();
    let offsets = map.map.value_offsets();
    let (start, end) = (offsets[row] as usize, offsets[row + 1] as usize);
    let room = LOADER_MAX_ATTRIBUTES_PER_RECORD.saturating_sub(span_attributes);
    let mut out: Vec<(String, String)> = Vec::with_capacity((end - start).min(room));
    let mut kept = 0usize;
    let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
    for i in start..end {
        // Before the null-value skip, so an entry with both null is refused:
        // Arrow declares a map's key field non-nullable.
        let key = map
            .keys
            .get(i)?
            .ok_or_else(|| format!("attrs_map_column {column:?} holds a null key"))?;
        let Some(value) = map.values.get(i)? else {
            continue;
        };
        if let Some(mapped_column) = map.mapped_keys.get(key) {
            return Err(format!(
                "attrs_map_column {column:?} holds the key {key:?}, which the mapping also reads \
                 from the column {mapped_column:?}. A span carries one merged attrs map with \
                 unique keys, so one of the two would never reach the record; drop the key from \
                 the map or the entry from the mapping."
            ));
        }
        if is_reserved_key(key) {
            return Err(format!(
                "attrs_map_column {column:?} holds the reserved attribute key {key:?}, which \
                 holds a span field this version does not map"
            ));
        }
        if !seen.insert(key) {
            return Err(format!(
                "attrs_map_column {column:?} holds the key {key:?} twice. A span carries one \
                 merged attrs map with unique keys, so one of the two would never reach the \
                 record."
            ));
        }
        if key.len() > limits.max_attribute_key_len || value.len() > limits.max_attribute_value_len
        {
            *dropped += 1;
            continue;
        }
        kept += 1;
        if kept <= room {
            out.push((key.to_string(), value.to_string()));
        }
    }
    if kept > room {
        return Err(format!(
            "span carries {} attributes with its attrs_map_column entries, more than the loader \
             per-record cap of {LOADER_MAX_ATTRIBUTES_PER_RECORD}",
            span_attributes + kept
        ));
    }
    Ok(out)
}

/// The decode state the spans loader shuttles into and back out of each
/// batch's `spawn_blocking` task: the single sequential cursor plus the
/// `--skip-rows` offset, applied against each span's own file-absolute base
/// as [`collect_spans`] applies it on the logs path.
struct SpansDecodeState {
    cursor: CursorState,
    skip_rows: u64,
}

/// One batch's decode outcome.
enum SpansDecoded {
    /// Spans built from this batch, the source rows they came from (one per
    /// span here), the attribute values this batch lost to the value-length
    /// cap, and whether the input is exhausted.
    Batch {
        spans: Vec<NormalizedSpan>,
        rows: u64,
        attrs_dropped: u64,
        done: bool,
    },
    /// The batch failed to read from Parquet or to resolve against the
    /// mapping.
    Failed(String),
    /// A row failed a kept admission check, at its FILE-absolute index.
    /// `attrs_dropped` is what the rows built before it lost to the cap.
    Rejected {
        row: u64,
        reason: String,
        attrs_dropped: u64,
    },
}

/// Decode and build one spans batch from the sequential cursor.
fn decode_spans_batch(
    state: &mut SpansDecodeState,
    mapping: &SpansMapping,
    mapped_keys: &MappedKeys,
    limits: &SpanIngestLimits,
    now_ns: i64,
    batch_rows: usize,
) -> SpansDecoded {
    let taken = match cursor_take(&mut state.cursor, batch_rows) {
        Ok(taken) => taken,
        Err(reason) => return SpansDecoded::Failed(reason),
    };
    let Some((batch, file_base)) = taken else {
        return SpansDecoded::Batch {
            spans: Vec::new(),
            rows: 0,
            attrs_dropped: 0,
            done: true,
        };
    };

    // `--skip-rows` by file-absolute position, before mapping sees a row.
    let (batch, file_base) = {
        let end = file_base + batch.num_rows() as u64;
        if end <= state.skip_rows {
            return SpansDecoded::Batch {
                spans: Vec::new(),
                rows: 0,
                attrs_dropped: 0,
                done: false,
            };
        }
        if file_base < state.skip_rows {
            let cut = (state.skip_rows - file_base) as usize;
            (
                batch.slice(cut, batch.num_rows() - cut),
                file_base + cut as u64,
            )
        } else {
            (batch, file_base)
        }
    };

    let cols = match SpansColumnIndex::resolve(&batch, mapping, mapped_keys) {
        Ok(cols) => cols,
        Err(reason) => return SpansDecoded::Failed(reason),
    };

    let mut spans = Vec::with_capacity(batch.num_rows());
    let mut attrs_dropped = 0u64;
    for row in 0..batch.num_rows() {
        // Counted per row so a rejected row's own drops are not folded in: the
        // count covers the spans that were built.
        let mut row_dropped = 0u64;
        match build_span(
            &batch,
            &cols,
            mapping,
            limits,
            now_ns,
            row,
            &mut row_dropped,
        ) {
            Ok(span) => {
                attrs_dropped += row_dropped;
                spans.push(span);
            }
            Err(reason) => {
                return SpansDecoded::Rejected {
                    row: file_base + row as u64,
                    reason,
                    attrs_dropped,
                };
            }
        }
    }
    let rows = spans.len() as u64;
    SpansDecoded::Batch {
        spans,
        rows,
        attrs_dropped,
        done: false,
    }
}

/// Bulk-import `parquet_path` into `tenant`'s spans signal (ADR-1751
/// decision 1).
///
/// The same contract as [`load_metrics`]: the shard count is validated
/// against (or, for a fresh signal, written to) the durable provisioning
/// record through [`validate_or_adopt`]; the router is built from the same
/// [`build_ingest_config`]; every batch is a [`WriteMode::Strict`] write whose
/// ack means durable; and a failure mid-file is a genuine partial load whose
/// durable commit tokens are reported rather than swallowed.
///
/// `now_ns` anchors the future-skew check and the zero-start fallback only.
/// Bucketing is by the router's own clock (load-time wall clock), so an old
/// span lands in today's ingest hour and is reached by every later query's
/// listing window, whose upper bound is `now`.
#[allow(clippy::too_many_arguments)]
pub async fn load_spans(
    store: Arc<dyn ObjectStoreBackend>,
    parquet_path: &Path,
    tenant: &str,
    mapping: &SpansMapping,
    shards: u32,
    batch_rows: usize,
    skip_rows: u64,
    pipeline_depth: usize,
    max_inflight_flushes: u32,
    target_bytes: usize,
    max_flush_delay: Option<Duration>,
    now_ns: i64,
    clock: Arc<dyn Clock>,
) -> Result<SpansLoadReport, LoadError> {
    let mut report = SpansLoadReport::default();
    load_spans_into(
        &mut report,
        store,
        parquet_path,
        tenant,
        mapping,
        shards,
        batch_rows,
        skip_rows,
        pipeline_depth,
        max_inflight_flushes,
        target_bytes,
        max_flush_delay,
        now_ns,
        clock,
    )
    .await?;
    Ok(report)
}

/// [`load_spans`] writing into a caller-owned report, so a FAILED load still
/// leaves the figures [`LoadError`] does not carry where the caller can read
/// them. Today that is [`SpansLoadReport::attributes_dropped`]: an
/// approximation the operator has to be told about, and one a failure does not
/// undo for the batches that did land.
#[allow(clippy::too_many_arguments)]
pub(super) async fn load_spans_into(
    report: &mut SpansLoadReport,
    store: Arc<dyn ObjectStoreBackend>,
    parquet_path: &Path,
    tenant: &str,
    mapping: &SpansMapping,
    shards: u32,
    batch_rows: usize,
    skip_rows: u64,
    pipeline_depth: usize,
    max_inflight_flushes: u32,
    target_bytes: usize,
    max_flush_delay: Option<Duration>,
    now_ns: i64,
    clock: Arc<dyn Clock>,
) -> Result<(), LoadError> {
    // Same operator-facing lever guards as the other paths: a value that
    // cannot express what the flag means is rejected, never silently clamped.
    if batch_rows == 0 {
        return Err(LoadError::Setup(
            "--batch-rows must be at least 1 (each batch is one Strict flush per shard); 0 was \
             given"
                .to_string(),
        ));
    }
    if pipeline_depth == 0 {
        return Err(LoadError::Setup(
            "--pipeline-depth must be at least 1 (the number of concurrent in-flight writes); 0 \
             was given"
                .to_string(),
        ));
    }
    if max_inflight_flushes == 0 {
        return Err(LoadError::Setup(
            "--max-inflight-flushes must be at least 1 (the number of flushes one shard may have \
             in flight at once); 0 would deadlock every flush, since a shard could never acquire \
             a permit to run one"
                .to_string(),
        ));
    }
    if target_bytes == 0 {
        return Err(LoadError::Setup(
            "--target-bytes must be at least 1 (1 flushes every batch as its own object); 0 was \
             given"
                .to_string(),
        ));
    }
    // Both attribute-count caps live in `validate`, which also runs at mapping
    // parse: they are properties of the mapping alone.
    mapping.validate()?;

    let limits = SpanIngestLimits::default();
    let tenant_id = TenantId::new(tenant);

    // Provision or validate the SPANS signal, the same first-touch path
    // `services/ravel-server` runs.
    validate_or_adopt(
        store.as_ref(),
        &tenant_id.hash(),
        Signal::Spans,
        shards,
        now_ns,
        AbsentPolicy::CreateFromConfig,
    )
    .await
    .map_err(|e| {
        LoadError::Setup(format!(
            "shard-count provisioning check failed for tenant {tenant:?} \
             (configured --shards {shards}): {e}"
        ))
    })?;

    let router = Arc::new(SpanIngestRouter::new(
        build_ingest_config(shards, target_bytes, max_inflight_flushes, max_flush_delay),
        Arc::clone(&store),
        clock,
    ));
    let ack_deadline = write_ack_deadline(max_flush_delay);

    let input = FileInput { path: parquet_path };
    let metadata = read_input_metadata(&input)?;
    let row_group_lens = row_group_row_counts(&metadata);
    // One sequential cursor, as on the metrics path: a failed load's landed
    // rows stay a file prefix, which is what makes the printed resume offset
    // mean anything.
    let mut cursors = open_stride_cursors(&input, &metadata, &row_group_lens, 1, batch_rows)?;
    let Some(cursor) = cursors.pop() else {
        return Err(LoadError::Setup(
            "internal error: no read cursor was opened for the input file".to_string(),
        ));
    };

    let started = Instant::now();
    let total_rows: u64 = row_group_lens.iter().sum();
    report.rows_skipped = skip_rows.min(total_rows);
    report.skip_rows_requested = skip_rows;
    report.file_total_rows = total_rows;

    let mut state = SpansDecodeState { cursor, skip_rows };
    let mapping = Arc::new(mapping.clone());
    let mapped_keys = Arc::new(mapping.mapped_keys());

    let mut inflight: std::collections::VecDeque<Inflight<SpanWriteReceipt, SpanWriteError>> =
        std::collections::VecDeque::with_capacity(pipeline_depth);

    loop {
        let mapping_for_decode = Arc::clone(&mapping);
        let mapped_keys_for_decode = Arc::clone(&mapped_keys);
        let limits_for_decode = limits.clone();
        let (returned, decoded) = tokio::task::spawn_blocking(move || {
            let mut state = state;
            let outcome = decode_spans_batch(
                &mut state,
                &mapping_for_decode,
                &mapped_keys_for_decode,
                &limits_for_decode,
                now_ns,
                batch_rows,
            );
            (state, outcome)
        })
        .await
        .map_err(|join_err| LoadError::BatchFailed {
            reason: format!("Parquet decode/build task failed: {join_err}"),
            durable: report.tokens.clone(),
            resume: report.resume(),
        })?;
        state = returned;

        let (spans, rows, done) = match decoded {
            SpansDecoded::Failed(reason) => {
                let (durable, reason) =
                    drain_sequential_before_refusal(&mut inflight, report, reason).await;
                return Err(LoadError::BatchFailed {
                    reason,
                    durable,
                    resume: report.resume(),
                });
            }
            SpansDecoded::Rejected {
                row,
                reason,
                attrs_dropped,
            } => {
                report.attributes_dropped += attrs_dropped;
                let (durable, reason) =
                    drain_sequential_before_refusal(&mut inflight, report, reason).await;
                return Err(LoadError::RowRejected {
                    row,
                    reason,
                    durable,
                    resume: report.resume(),
                });
            }
            SpansDecoded::Batch {
                spans,
                rows,
                attrs_dropped,
                done,
            } => {
                report.attributes_dropped += attrs_dropped;
                (spans, rows, done)
            }
        };

        if spans.is_empty() {
            if done {
                break;
            }
            continue;
        }

        let handle = {
            let router = Arc::clone(&router);
            let tenant = tenant_id.clone();
            tokio::spawn(async move {
                router
                    .write(tenant, spans, WriteMode::Strict, ack_deadline)
                    .await
            })
        };
        inflight.push_back((rows, rows, handle));

        // Bound true concurrency to `pipeline_depth`, resolving strictly
        // oldest-first so the token list and the first error stay in
        // submission order however the underlying PUTs complete.
        while inflight.len() >= pipeline_depth {
            let Some(entry) = inflight.pop_front() else {
                break;
            };
            if let Err(mut e) = resolve_sequential_write(entry, report).await {
                harvest_sequential_after_failure(&mut inflight, &mut e).await;
                return Err(e);
            }
        }

        if done {
            break;
        }
    }

    // Publish the tail buffers before draining, for the same reason the other
    // paths do: no later batch is coming to push a buffer past
    // `--target-bytes`, so its writes' acks would otherwise wait out the age
    // trigger. The ticker sweeps a straggler whose send landed after this
    // flush.
    router.flush_all().await;
    let drain_result = {
        let ticker_router = Arc::clone(&router);
        let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
        let ticker = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut stop_rx => break,
                    () = tokio::time::sleep(Duration::from_secs(2)) => {
                        ticker_router.flush_all().await;
                    }
                }
            }
        });
        let result = drain_sequential_inflight(&mut inflight, report).await;
        let _ = stop_tx.send(());
        let _ = ticker.await;
        result
    };
    drain_result?;

    report.elapsed = started.elapsed();
    Ok(())
}

/// `ravel-cli load --signal spans` (ADR-1751 follow-up task 2). The
/// end-to-end round trip and the OTLP differential live in
/// `tests/load_spans.rs`; these cover what needs the crate-internal entry
/// point or a mapping that never reaches a router.
#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests;
