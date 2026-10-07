//! The columnar logs fast path: builds a whole record batch's columns at once
//! instead of one record at a time.

use super::*;
use ravel_logseg::LogSegError;

/// A columnar-build failure: a batch-level decode/resolve error, or a per-row
/// admission rejection carrying its FILE-absolute index (#541).
pub(super) enum ColBuildError {
    Batch(String),
    Row { row: u64, reason: String },
}

/// The load error for a value that would take `column` past the `u32::MAX`
/// bytes (one byte short of 4 GiB) one batch's `u32` offsets address: the
/// column's bytes grow with the batch's
/// rows, so a smaller `--batch-rows` is the lever.
fn column_too_large(column: &str, e: &LogSegError) -> String {
    format!(
        "column {column:?} holds too many bytes for one batch ({e}); rerun with a smaller \
         --batch-rows"
    )
}

/// Appends `cell` to `column`'s `cells`. A slot's cells and every source
/// feeding it take their type from the same declared [`ColType`], so a
/// mismatch is a bug. A `Str` or `Bytes` cell that would take the column past
/// its byte limit is refused with [`column_too_large`].
fn push_cell(column: &str, cells: &mut DynCells, cell: CellRef<'_>) -> Result<(), String> {
    let too_large = |e: LogSegError| column_too_large(column, &e);
    match (cells, cell) {
        (DynCells::Str(v), CellRef::Str(s)) => v.try_push(s.as_bytes()).map_err(too_large)?,
        (DynCells::Bytes(b), CellRef::Bytes(x)) => b.values.try_push(x).map_err(too_large)?,
        (DynCells::I64(v), CellRef::I64(x)) => v.push(x),
        (DynCells::F64(v), CellRef::F64(x)) => v.push(x),
        (DynCells::Bool(v), CellRef::Bool(x)) => v.push(x),
        (cells, _) => {
            return Err(format!(
                "a cell of another type reached a {:?} dynamic column",
                cells.field_type()
            ));
        }
    }
    Ok(())
}

/// Build a [`ColumnarLogBatch`] directly from a batch's Arrow spans and the
/// mapping (ADR-0109 decisions 1, 6). Every downcast and the `ts` unit scaling
/// are resolved once per column per span; stream identity is hashed once per
/// distinct resource tuple. Each dynamic column is filled typed and dense in
/// row order: a string or byte cell is copied once, out of its Arrow buffer
/// (dictionary-encoded or not) into the column, with no owned value per cell,
/// and no dictionary is attached (`dyn_col_dicts` stays empty).
///
/// The result is byte-identical, once written, to
/// [`ColumnarLogBatch::from_records`] over the [`NormalizedLogRecord`]s
/// [`build_record`] would produce for the same spans (decision 7): the dynamic
/// columns keep the same `(name, type)`-sorted order and first-occurrence
/// winner/residual split, and the stream directory is the same id-ascending
/// dense form. Admission rejections match `build_record`'s per-row check order
/// and report the first failing row's FILE-absolute index.
///
/// Dynamic column slots are resolved once per batch, not once per cell (#689).
pub(super) fn build_columnar_batch(
    spans: &[(RecordBatch, u64)],
    mapping: &Mapping,
    limits: &LogIngestLimits,
    now_ns: i64,
) -> Result<ColumnarLogBatch, ColBuildError> {
    use std::collections::{BTreeMap, HashMap};

    let total_rows: usize = spans.iter().map(|(b, _)| b.num_rows()).sum();
    let mut batch = ColumnarLogBatch::new();
    batch.num_rows = total_rows;
    if total_rows == 0 {
        return Ok(batch);
    }

    batch.ts_ns.reserve(total_rows);
    batch.observed_ts_ns.reserve(total_rows);
    batch.severity_num.reserve(total_rows);
    batch.flags.reserve(total_rows);
    batch.residual_attrs = vec![Vec::new(); total_rows];

    // Dynamic column slots, resolved once per batch rather than once per cell
    // (#689). `slot_keys` holds the distinct (name, type byte) pairs of the
    // mapped record attributes in ascending key order, which is the order the
    // `BTreeMap<(String, u8), _>` this replaced materialized its columns in, so
    // the column order is unchanged. `slot_of_attr[mi]` is the slot of
    // `mapping.attributes[mi]`, so the per-cell path is an indexed push with no
    // key comparison and no map lookup.
    let attr_key = |mi: usize| -> (&str, u8) {
        let spec = &mapping.attributes[mi];
        (spec.key.as_str(), field_type_of(spec.value_type).to_u8())
    };
    let mut attr_order: Vec<usize> = (0..mapping.attributes.len()).collect();
    attr_order.sort_unstable_by(|a, b| attr_key(*a).cmp(&attr_key(*b)));
    let mut slot_keys: Vec<(String, u8)> = Vec::new();
    let mut slot_of_attr: Vec<usize> = vec![0; mapping.attributes.len()];
    for mi in attr_order {
        let (name, ty) = attr_key(mi);
        if slot_keys.last().map(|(n, t)| (n.as_str(), *t)) != Some((name, ty)) {
            slot_keys.push((name.to_string(), ty));
        }
        slot_of_attr[mi] = slot_keys.len().saturating_sub(1);
    }

    let slot_types: Vec<FieldType> = slot_keys
        .iter()
        .map(|(_, ty)| FieldType::from_u8(*ty).unwrap_or(FieldType::Bytes))
        .collect();

    // Each slot's reservation: the [`cell_capacity_hint`] of every column
    // mapped to it, summed over the spans, then capped. The sum over-counts a
    // slot fed by several columns (one cell per row wins it) and a dictionary
    // column's hint is unbounded, so the cap is what the slot can hold: one
    // cell per row, each at most `max_attribute_value_len` bytes, which the row
    // loop's admission check enforces before any push, and never more than the
    // `VAR_BYTES_MAX` a column can hold before `try_push` refuses. A span whose columns
    // cannot be located adds nothing; the row loop raises its error in order.
    let mut slot_hint: Vec<(usize, usize)> = vec![(0, 0); slot_keys.len()];
    for (span, _) in spans {
        let Ok(cols) = ColumnIndex::locate(span, mapping) else {
            continue;
        };
        for (ci, mi) in &cols.record {
            let (cells, bytes) =
                cell_capacity_hint(span.column(*ci), mapping.attributes[*mi].value_type);
            let hint = &mut slot_hint[slot_of_attr[*mi]];
            hint.0 = hint.0.saturating_add(cells);
            hint.1 = hint.1.saturating_add(bytes);
        }
    }
    for hint in &mut slot_hint {
        hint.0 = hint.0.min(total_rows);
        hint.1 = hint
            .1
            .min(hint.0.saturating_mul(limits.max_attribute_value_len))
            .min(ravel_logseg::VAR_BYTES_MAX);
    }

    // A slot's cells and validity are allocated on its first present value: a
    // mapped attribute that is null across the whole batch materializes no
    // column. Validity is padded with absent rows up to each present one.
    let mut slot_cols: Vec<Option<(DynCells, Bitmap)>> = Vec::new();
    slot_cols.resize_with(slot_keys.len(), || None);
    // The row that most recently won each slot (1-based, 0 meaning never). The
    // stamp replaces the per-row `HashSet<(String, u8)>` that decided the
    // first-occurrence winner: same relation, no allocation and no key clone
    // per cell.
    let mut slot_taken_at: Vec<u64> = vec![0; slot_keys.len()];

    // Stream identity: hashed once per distinct resource tuple, keyed by the
    // STREAM_DIR blob (the canonical resource bytes) so the blake3 in
    // `log_stream_id` runs once per distinct tuple rather than once per row
    // (ADR-0109 decision 6). `stream_dir` is the id-ascending directory.
    let mut row_stream_id: Vec<LogStreamId> = Vec::with_capacity(total_rows);
    let mut stream_dir: BTreeMap<LogStreamId, Vec<u8>> = BTreeMap::new();
    let mut stream_cache: HashMap<Vec<u8>, LogStreamId> = HashMap::new();

    // Reused across every row of every span: the resource tuple is rebuilt per
    // row but its buffer is not reallocated per row.
    let mut resource_attrs: Vec<(String, AttrValue)> =
        Vec::with_capacity(mapping.resource_attributes.len());

    let mut grow = 0usize;
    for (span, file_base) in spans {
        let cols = ColumnIndex::locate(span, mapping).map_err(ColBuildError::Batch)?;

        // Prepare every reader once per span (downcast resolved here, not per
        // cell).
        let ts = ts_src(span.column(cols.ts), mapping.ts_unit);
        let body = cols.body.map(|i| str_src(span.column(i)));
        let sev_num = cols.severity_number.map(|i| int_src(span.column(i)));
        let sev_text = cols.severity_text.map(|i| str_src(span.column(i)));
        // The id columns are the only ones resolved from a dictionary here, and
        // are read in place: a default Parquet writer dictionary-encodes a hex
        // id column.
        let trace_col = cols
            .trace_id
            .map(|i| id_column(span.column(i)))
            .transpose()
            .map_err(ColBuildError::Batch)?;
        let span_id_col = cols
            .span_id
            .map(|i| id_column(span.column(i)))
            .transpose()
            .map_err(ColBuildError::Batch)?;
        let trace = trace_col.as_ref().map(id_column_src);
        let span_id_src = span_id_col.as_ref().map(id_column_src);
        let resource: Vec<(usize, AttrSrc)> = cols
            .resource
            .iter()
            .map(|(ci, mi)| {
                (
                    *mi,
                    attr_src(
                        span.column(*ci),
                        mapping.resource_attributes[*mi].value_type,
                    ),
                )
            })
            .collect();
        // The third element is the destination slot, resolved here from the
        // record batch's column index once per span, never per cell.
        let record: Vec<(usize, usize, AttrSrc)> = cols
            .record
            .iter()
            .map(|(ci, mi)| {
                (
                    *mi,
                    slot_of_attr[*mi],
                    attr_src(span.column(*ci), mapping.attributes[*mi].value_type),
                )
            })
            .collect();

        for local in 0..span.num_rows() {
            let file_row = file_base + local as u64;
            let row_err = |reason: String| ColBuildError::Row {
                row: file_row,
                reason,
            };

            // 1. ts (required), not negative, and 2. future-skew bound, in
            // build_record order.
            let raw_ts = match ts.get(local).map_err(row_err)? {
                Some(t) => t,
                None => {
                    return Err(row_err(format!(
                        "ts column {:?} is null",
                        mapping.ts_column
                    )));
                }
            };
            if raw_ts < 0 {
                return Err(row_err(negative_ts_rejection(
                    raw_ts,
                    span.column(cols.ts).data_type(),
                    mapping.ts_unit,
                )));
            }
            let skew_ns = raw_ts.saturating_sub(now_ns);
            if skew_ns > limits.max_future_skew_ns {
                return Err(row_err(format!(
                    "timestamp is {skew_ns} ns ahead of load time, more than the max future skew \
                     of {} ns",
                    limits.max_future_skew_ns
                )));
            }

            // 3. body (optional) and its length cap.
            let body_val = match &body {
                Some(s) => s.get_ref(local).map_err(row_err)?.unwrap_or_default(),
                None => "",
            };
            if body_val.len() > limits.max_body_len {
                return Err(row_err(format!(
                    "body is {} bytes, more than the limit of {}",
                    body_val.len(),
                    limits.max_body_len
                )));
            }

            // 4. severity number (out-of-u8 normalizes to 0) and severity text.
            let severity_num = match &sev_num {
                Some(s) => s
                    .get(local)
                    .map_err(row_err)?
                    .and_then(|v| u8::try_from(v).ok())
                    .unwrap_or(0),
                None => 0,
            };
            let severity_text = match &sev_text {
                Some(s) => s.get_ref(local).map_err(row_err)?.unwrap_or_default(),
                None => "",
            };

            // 5. trace/span ids: exact length or absent.
            let trace_id = match &trace {
                Some(s) => s
                    .get(local)
                    .map_err(row_err)?
                    .and_then(|b| <[u8; 16]>::try_from(b.as_slice()).ok()),
                None => None,
            };
            let span_id = match &span_id_src {
                Some(s) => s
                    .get(local)
                    .map_err(row_err)?
                    .and_then(|b| <[u8; 8]>::try_from(b.as_slice()).ok()),
                None => None,
            };

            // 6. resource attributes (stream identity), checked in mapping order.
            resource_attrs.clear();
            for (mi, src) in &resource {
                let spec = &mapping.resource_attributes[*mi];
                if let Some(v) = src.get(local).map_err(row_err)? {
                    check_attr(&spec.key, &v, limits).map_err(row_err)?;
                    resource_attrs.push((spec.key.clone(), v));
                }
            }

            // 7. record attributes: check, count for the per-record cap, and
            // split first-occurrence winner vs within-record residual exactly as
            // `from_records`.
            let mut present_record = 0usize;
            let row_stamp = grow as u64 + 1;
            for (mi, slot, src) in &record {
                let spec = &mapping.attributes[*mi];
                if let Some(cell) = src.get_ref(local).map_err(row_err)? {
                    check_attr_len(&spec.key, cell.value_len(), limits).map_err(row_err)?;
                    present_record += 1;
                    if slot_taken_at[*slot] == row_stamp {
                        batch.residual_attrs[grow].push((spec.key.clone(), cell.to_value()));
                    } else {
                        slot_taken_at[*slot] = row_stamp;
                        let (cells, validity) = slot_cols[*slot].get_or_insert_with(|| {
                            let (n, bytes) = slot_hint[*slot];
                            (
                                DynCells::with_capacity(slot_types[*slot], n, bytes),
                                Bitmap::with_capacity(total_rows),
                            )
                        });
                        validity.pad_to(grow);
                        validity.push(true);
                        push_cell(&slot_keys[*slot].0, cells, cell)
                            .map_err(ColBuildError::Batch)?;
                    }
                }
            }
            if present_record > LOADER_MAX_ATTRIBUTES_PER_RECORD {
                return Err(row_err(format!(
                    "record has {present_record} attributes, more than the loader per-record cap \
                     of {LOADER_MAX_ATTRIBUTES_PER_RECORD}"
                )));
            }

            // 8. stream identity: hash once per distinct resource tuple.
            let blob = stream_attrs_bytes(&resource_attrs, "", "", &[]);
            let stream_id = match stream_cache.get(&blob) {
                Some(id) => *id,
                None => {
                    let id = log_stream_id(&resource_attrs, "", "", &[]);
                    stream_cache.insert(blob.clone(), id);
                    id
                }
            };
            stream_dir.entry(stream_id).or_insert_with(|| blob.clone());
            row_stream_id.push(stream_id);

            // Fixed columns, appended in row order.
            batch.ts_ns.push(raw_ts);
            batch.observed_ts_ns.push(raw_ts);
            batch.severity_num.push(severity_num);
            batch.flags.push(0);
            batch
                .severity_text
                .try_push(severity_text.as_bytes())
                .map_err(|e| ColBuildError::Batch(column_too_large("severity_text", &e)))?;
            batch
                .body
                .try_push(body_val.as_bytes())
                .map_err(|e| ColBuildError::Batch(column_too_large("body", &e)))?;
            match trace_id {
                Some(t) => {
                    batch.trace_id.extend_from_slice(&t);
                    batch.trace_id_validity.push(true);
                }
                None => batch.trace_id_validity.push(false),
            }
            match span_id {
                Some(s) => {
                    batch.span_id.extend_from_slice(&s);
                    batch.span_id_validity.push(true);
                }
                None => batch.span_id_validity.push(false),
            }

            grow += 1;
        }
    }

    // Stream directory: id-ascending dense refs, matching `from_records`.
    // `stream_dir` is a `BTreeMap`, so `batch.stream_ids` fills in ascending,
    // duplicate-free order; a row's ref is its id's rank in that order,
    // found by binary search instead of a HashMap (ADR-2425 decision 2).
    for (id, blob) in stream_dir {
        batch.stream_ids.push(id);
        batch.stream_attrs.push(blob);
    }
    batch.stream_refs = row_stream_id
        .iter()
        .map(|id| {
            batch
                .stream_ids
                .binary_search(id)
                .map(|idx| idx as u32)
                .map_err(|_| {
                    ColBuildError::Batch(format!(
                        "row stream id {id:?} missing from its own stream directory"
                    ))
                })
        })
        .collect::<Result<Vec<u32>, ColBuildError>>()?;

    // Dynamic columns in (name, type) order. A slot that never took a present
    // value has no column.
    for ((name, _), slot) in slot_keys.into_iter().zip(slot_cols) {
        let Some((cells, mut validity)) = slot else {
            continue;
        };
        validity.pad_to(total_rows);
        batch.dyn_columns.push(DynColumn {
            name,
            field_type: cells.field_type(),
            cells,
            validity,
        });
    }

    Ok(batch)
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests;
