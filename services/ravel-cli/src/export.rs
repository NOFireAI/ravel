//! `ravel-cli export`: bulk read-out of a tenant's stored records into a
//! Parquet file, the inverse of `ravel-cli load` (ADR-1751 decision 4).
//!
//! Only `--signal logs` is implemented. ADR-1751's follow-up order puts bulk
//! import for metrics and for spans ahead of export for those signals, and
//! neither import path exists yet, so there is nothing for an export of them
//! to round-trip against; [`unsupported_signal_message`] refuses them by name
//! rather than producing a file nothing can read back.
//!
//! # What makes this a store read rather than a query
//!
//! The export resolves the catalog once, at a single snapshot, and reads the
//! RLOG objects that snapshot names. It runs no SQL and issues no remote read:
//! the decoders and the segment fetcher are the same ones the query path uses,
//! so a record excluded from a query is excluded here too. Concretely, the two
//! exclusion mechanisms both apply:
//!
//! - Retention tombstones and superseded (compacted-away) objects never enter
//!   the snapshot, because `Catalog::resolve` applies them.
//! - Pending selective-erasure requests (ADR-0064) are attached to the
//!   snapshot and handed to the fetcher as
//!   [`ravel_query::erasure::ErasurePredicate`]s, which excludes matching rows
//!   after the fetch and after any cache tier, exactly as the SQL log scan
//!   does.
//!
//! # Window semantics
//!
//! `--start`/`--end` are a half-open event-time window `[start, end)`: a
//! record at exactly `--end` is not exported. `LogQuery`'s own range is
//! inclusive on both ends, so the fetch asks for `[start, end - 1]` and the
//! half-open bound is re-applied here over the decoded rows.
//!
//! # Round-tripping through `ravel-cli load`
//!
//! The output columns are exactly the ones the `--mapping` TOML names, in the
//! Arrow types that mapping's reader accepts, so `ravel-cli load --parquet
//! <exported> --mapping <same file>` reads the file back. The one lossy axis
//! is `ts_unit`: the stored event time is nanoseconds and the `ts` column is
//! written in the mapping's unit, so a mapping declaring `millis` truncates
//! sub-millisecond precision. A file exported under the same mapping it was
//! loaded with never has sub-unit precision to lose.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use arrow::array::{
    ArrayRef, BinaryBuilder, BooleanBuilder, FixedSizeBinaryBuilder, Float64Builder, Int64Builder,
    MapBuilder, StringBuilder,
};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use ravel_logseg::record::{attr_value_to_string, decode_stream_attrs};
use ravel_logseg::{AttrValue, LogRecord, LogStreamId};
use ravel_object_store::ObjectStoreBackend;
use ravel_query::erasure::snapshot_pending_erasure_predicates;
use ravel_query::{LogQuery, LogSegmentFetcher};
use ravel_types::accounting::QueryAccounting;
use ravel_types::{Signal, TenantId, TimeRange};

use crate::load::{ColType, Mapping};
use crate::maintain::SignalArg;
use crate::store::{StoreSelection, require_tenant_data_present};

/// Rows per output Arrow batch. The decoded records are already resident, so
/// this bounds only the Arrow/Parquet write buffer, not the peak of the read.
const EXPORT_BATCH_ROWS: usize = 10_000;

/// What one `export --signal logs` run did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExportReport {
    /// Rows written to the output file: exactly the records in
    /// `[start_ns, end_ns)` that survived erasure exclusion.
    pub rows_written: u64,
    /// Segments the resolved snapshot named and the fetcher actually read
    /// (a segment whose blocks were all pruned reads as zero rows, not as a
    /// skipped segment).
    pub segments_read: u64,
    /// Segments `Catalog::resolve` pruned by event-time range before any
    /// object was fetched.
    pub segments_pruned: u64,
    /// Pending selective-erasure predicates applied to this read. Nonzero
    /// means rows may have been excluded that the raw objects still hold.
    pub erasure_predicates: usize,
}

/// Why `signal` cannot be exported yet, or `None` when it can.
///
/// `logs` is the only supported signal. ADR-1751 sequences bulk import for
/// metrics (follow-up 1) and for spans (follow-up 2) ahead of export for those
/// signals (follow-up 3), so refusing by name is the honest answer: an export
/// written now could not be loaded back by anything.
pub fn unsupported_signal_message(signal: SignalArg) -> Option<String> {
    let (name, waits_on) = match signal {
        SignalArg::Logs => return None,
        SignalArg::Metrics => ("metrics", "bulk import for metrics (ADR-1751 follow-up 1)"),
        SignalArg::Spans => ("spans", "bulk import for spans (ADR-1751 follow-up 2)"),
    };
    Some(format!(
        "export --signal {name} is not available: ADR-1751 follow-up 3 sequences export for \
         {name} behind {waits_on}, which does not exist yet, so an exported {name} file could \
         not be loaded back. Only --signal logs is supported."
    ))
}

/// Parse the `--mapping` file and run the export, or refuse the signal.
///
/// The signal check runs before the mapping file is opened, so refusing an
/// unsupported signal does not first fail on an unrelated path error.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    store: Arc<dyn ObjectStoreBackend>,
    selection: StoreSelection,
    tenant: &str,
    signal: SignalArg,
    start_ns: i64,
    end_ns: i64,
    mapping_path: &Path,
    out: &Path,
    shards: u32,
    now_ns: i64,
) -> anyhow::Result<()> {
    if let Some(message) = unsupported_signal_message(signal) {
        return Err(anyhow::anyhow!(message));
    }
    let text = std::fs::read_to_string(mapping_path)
        .with_context(|| format!("failed to read --mapping {}", mapping_path.display()))?;
    let mapping = crate::load::parse_mapping(&text)?;
    selection.print_header();
    let report = export_logs(
        store, selection, tenant, start_ns, end_ns, &mapping, out, shards, now_ns,
    )
    .await?;
    println!("output: {}", out.display());
    println!("rows_written: {}", report.rows_written);
    println!("segments_read: {}", report.segments_read);
    println!("segments_pruned: {}", report.segments_pruned);
    println!("erasure_predicates: {}", report.erasure_predicates);
    Ok(())
}

/// Export the logs a tenant holds in `[start_ns, end_ns)` to a Parquet file
/// laid out by `mapping`.
///
/// `shards` is the configured shard count, resolved the same way the other
/// read commands resolve it; the catalog reads the tenant's real shard
/// generations from its provisioning record on top of it.
#[allow(clippy::too_many_arguments)]
pub async fn export_logs(
    store: Arc<dyn ObjectStoreBackend>,
    selection: StoreSelection,
    tenant: &str,
    start_ns: i64,
    end_ns: i64,
    mapping: &Mapping,
    out: &Path,
    shards: u32,
    now_ns: i64,
) -> anyhow::Result<ExportReport> {
    if end_ns <= start_ns {
        anyhow::bail!(
            "--end must be after --start: the export window is half-open [start, end), and \
             [{start_ns}, {end_ns}) is empty"
        );
    }
    let tenant_hash = TenantId::new(tenant).hash();
    require_tenant_data_present(selection, store.as_ref(), "export", tenant, &tenant_hash).await?;

    // Enforcing, matching the server's query path and `catalog list`: the
    // tenant's real shard-generation history decides which shards each hour is
    // scanned across, instead of short-circuiting to generation 0 and
    // under-scanning `0..--shards` after a reshard-increase.
    let catalog_config = ravel_catalog::CatalogConfig {
        shard_count: shards,
        ..ravel_catalog::CatalogConfig::default()
    };
    let catalog = ravel_catalog::Catalog::new(Arc::clone(&store), catalog_config)
        .map_err(|err| anyhow::anyhow!("failed to build catalog: {err}"))?
        .with_provisioning_enforcement();
    let range = TimeRange { start_ns, end_ns };
    let snapshot = catalog
        .resolve(&tenant_hash, Signal::Logs, range, &[], now_ns)
        .await
        .map_err(|err| anyhow::anyhow!("failed to resolve catalog: {err}"))?;

    let predicates = snapshot_pending_erasure_predicates(&snapshot);
    let erasure_predicates = predicates.len();
    // `LogQuery`'s range is inclusive on both ends; the export window is
    // half-open, and the exact bound is re-applied over the decoded rows below.
    let query = LogQuery::new(start_ns, end_ns.saturating_sub(1)).with_erasure(predicates);
    let fetcher = LogSegmentFetcher::new(Arc::clone(&store));
    let accounting = QueryAccounting::new();

    let mut records: Vec<LogRecord> = Vec::new();
    let mut segments_read = 0u64;
    for seg_ref in &snapshot.segments {
        let fetched = fetcher
            .fetch_accounted_with_tenant(seg_ref, tenant_hash, &query, &accounting)
            .await
            .map_err(|err| {
                anyhow::anyhow!(
                    "failed to read log segment {}: {err}",
                    seg_ref.data_object_key
                )
            })?;
        if let Some(output) = fetched {
            segments_read += 1;
            records.extend(output.records);
        }
    }
    records.retain(|record| record.ts_ns >= start_ns && record.ts_ns < end_ns);
    records.sort_by_key(|record| record.ts_ns);

    let resource_by_stream = decode_resources(&records)?;
    let rows_written = write_parquet(mapping, &records, &resource_by_stream, out)?;

    Ok(ExportReport {
        rows_written,
        segments_read,
        segments_pruned: snapshot.segments_pruned,
        erasure_predicates,
    })
}

/// One decoded resource attribute set per distinct stream, so a stream's
/// `stream_attrs` blob is decoded once rather than once per record.
fn decode_resources(
    records: &[LogRecord],
) -> anyhow::Result<HashMap<LogStreamId, Vec<(String, AttrValue)>>> {
    let mut by_stream: HashMap<LogStreamId, Vec<(String, AttrValue)>> = HashMap::new();
    for record in records {
        if by_stream.contains_key(&record.stream_id) {
            continue;
        }
        let decoded = decode_stream_attrs(&record.stream_attrs).map_err(|err| {
            anyhow::anyhow!(
                "failed to decode the stream attributes of stream {}: {err}",
                hex::encode(record.stream_id.0)
            )
        })?;
        by_stream.insert(record.stream_id, decoded.resource);
    }
    Ok(by_stream)
}

/// Writes `records` to `out` in `EXPORT_BATCH_ROWS`-row batches and returns
/// the row count written. An empty export still writes a schema-only file, so
/// a loader pointed at it reads zero rows rather than failing to open it.
fn write_parquet(
    mapping: &Mapping,
    records: &[LogRecord],
    resource_by_stream: &HashMap<LogStreamId, Vec<(String, AttrValue)>>,
    out: &Path,
) -> anyhow::Result<u64> {
    let empty = build_batch(mapping, &[])?;
    let file = std::fs::File::create(out)
        .with_context(|| format!("failed to create {}", out.display()))?;
    let mut writer = ArrowWriter::try_new(file, empty.schema(), None)
        .with_context(|| format!("failed to open a Parquet writer on {}", out.display()))?;
    let mut rows_written = 0u64;
    for chunk in records.chunks(EXPORT_BATCH_ROWS) {
        let rows: Vec<ExportRow<'_>> = chunk
            .iter()
            .map(|record| ExportRow {
                record,
                resource: resource_by_stream
                    .get(&record.stream_id)
                    .map_or(&[][..], Vec::as_slice),
            })
            .collect();
        let batch = build_batch(mapping, &rows)?;
        writer
            .write(&batch)
            .with_context(|| format!("failed to write a batch to {}", out.display()))?;
        rows_written += batch.num_rows() as u64;
    }
    writer
        .close()
        .with_context(|| format!("failed to finish writing {}", out.display()))?;
    Ok(rows_written)
}

/// One output row: a decoded record plus its stream's resource attributes,
/// which live in the stream blob rather than on the record.
struct ExportRow<'a> {
    record: &'a LogRecord,
    resource: &'a [(String, AttrValue)],
}

/// Builds the output batch for `rows`, one column per field the mapping names.
///
/// The Arrow types are chosen to be exactly what the `--mapping` reader in
/// `load` accepts: `Int64` for the timestamp (already divided by the mapping's
/// unit) and for `i64` attributes, `Utf8` for the body, severity text and
/// `str` attributes, `FixedSizeBinary` of the id width for trace and span ids,
/// `Float64`/`Boolean`/`Binary` for the remaining attribute types.
fn build_batch(mapping: &Mapping, rows: &[ExportRow<'_>]) -> anyhow::Result<RecordBatch> {
    let mut columns: Vec<(String, ArrayRef)> = Vec::new();
    let mut seen: HashSet<&str> = HashSet::new();

    let factor = mapping.ts_unit.factor();
    let mut ts = Int64Builder::with_capacity(rows.len());
    for row in rows {
        ts.append_value(row.record.ts_ns / factor);
    }
    columns.push((mapping.ts_column.clone(), Arc::new(ts.finish())));

    if let Some(name) = &mapping.body_column {
        let mut body = StringBuilder::new();
        for row in rows {
            body.append_value(&row.record.body);
        }
        columns.push((name.clone(), Arc::new(body.finish())));
    }

    if let Some(name) = &mapping.severity_number_column {
        let mut severity = Int64Builder::with_capacity(rows.len());
        for row in rows {
            severity.append_value(i64::from(row.record.severity_num));
        }
        columns.push((name.clone(), Arc::new(severity.finish())));
    }

    if let Some(name) = &mapping.severity_text_column {
        let mut severity = StringBuilder::new();
        for row in rows {
            severity.append_value(&row.record.severity_text);
        }
        columns.push((name.clone(), Arc::new(severity.finish())));
    }

    if let Some(name) = &mapping.trace_id_column {
        let mut ids = FixedSizeBinaryBuilder::new(16);
        for row in rows {
            append_id(&mut ids, row.record.trace_id.as_ref().map(|id| &id[..]))?;
        }
        columns.push((name.clone(), Arc::new(ids.finish())));
    }

    if let Some(name) = &mapping.span_id_column {
        let mut ids = FixedSizeBinaryBuilder::new(8);
        for row in rows {
            append_id(&mut ids, row.record.span_id.as_ref().map(|id| &id[..]))?;
        }
        columns.push((name.clone(), Arc::new(ids.finish())));
    }

    for spec in &mapping.resource_attributes {
        let mut column = AttrColumn::new(spec.value_type);
        for row in rows {
            column.push(&spec.key, find_attr(row.resource, &spec.key))?;
        }
        columns.push((spec.column.clone(), column.finish()));
    }

    for spec in &mapping.attributes {
        let mut column = AttrColumn::new(spec.value_type);
        for row in rows {
            column.push(&spec.key, find_attr(&row.record.attrs, &spec.key))?;
        }
        columns.push((spec.column.clone(), column.finish()));
    }

    if let Some(name) = &mapping.attrs_map_column {
        columns.push((name.clone(), build_attrs_map(mapping, rows)?));
    }

    for (name, _) in &columns {
        if !seen.insert(name.as_str()) {
            anyhow::bail!(
                "the mapping writes two different fields to the output column {name:?}; give \
                 each one its own column name"
            );
        }
    }

    // Every field is declared nullable explicitly. `RecordBatch::try_from_iter`
    // would infer nullability from each array's own null count, which differs
    // between batches of the same export (and is always "not nullable" for the
    // empty batch the writer takes its schema from), so a later batch's nulls
    // would be written into a column the file declares required and read back
    // as values.
    RecordBatch::try_from_iter_with_nullable(
        columns.into_iter().map(|(name, array)| (name, array, true)),
    )
    .context("failed to build the export record batch")
}

/// The `attrs_map_column` overflow column: every record attribute the mapping
/// does not give a typed column of its own, stringified the same way
/// `attrs['<key>']` stringifies a value for SQL.
fn build_attrs_map(mapping: &Mapping, rows: &[ExportRow<'_>]) -> anyhow::Result<ArrayRef> {
    let typed: HashSet<&str> = mapping
        .attributes
        .iter()
        .map(|spec| spec.key.as_str())
        .collect();
    let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
    for row in rows {
        for (key, value) in &row.record.attrs {
            if typed.contains(key.as_str()) {
                continue;
            }
            builder.keys().append_value(key);
            builder.values().append_value(attr_value_to_string(value));
        }
        builder
            .append(true)
            .context("failed to build the attrs map column")?;
    }
    Ok(Arc::new(builder.finish()))
}

fn append_id(builder: &mut FixedSizeBinaryBuilder, id: Option<&[u8]>) -> anyhow::Result<()> {
    match id {
        Some(bytes) => builder
            .append_value(bytes)
            .context("failed to append a trace or span id"),
        None => {
            builder.append_null();
            Ok(())
        }
    }
}

fn find_attr<'a>(attrs: &'a [(String, AttrValue)], key: &str) -> Option<&'a AttrValue> {
    attrs
        .iter()
        .find_map(|(k, v)| (k.as_str() == key).then_some(v))
}

/// The stored kind of an attribute value, for the mismatch message below.
fn attr_type_name(value: &AttrValue) -> &'static str {
    match value {
        AttrValue::Str(_) => "str",
        AttrValue::I64(_) => "i64",
        AttrValue::F64(_) => "f64",
        AttrValue::Bool(_) => "bool",
        AttrValue::Bytes(_) => "bytes",
        AttrValue::List(_) => "list",
        AttrValue::Map(_) => "map",
    }
}

/// One typed attribute output column, in the Arrow type the mapping's
/// declared [`ColType`] corresponds to.
///
/// A record that carries no value for the key gets a null (the loader omits a
/// null source cell rather than storing an attribute, so null is what a round
/// trip must produce). A record whose stored value has a different type than
/// the mapping declares is an error rather than a null: silently writing null
/// would drop a value the store holds, with nothing in the output to say so.
enum AttrColumn {
    Str(StringBuilder),
    I64(Int64Builder),
    F64(Float64Builder),
    Bool(BooleanBuilder),
    Bytes(BinaryBuilder),
}

impl AttrColumn {
    fn new(value_type: ColType) -> Self {
        match value_type {
            ColType::Str => AttrColumn::Str(StringBuilder::new()),
            ColType::I64 => AttrColumn::I64(Int64Builder::new()),
            ColType::F64 => AttrColumn::F64(Float64Builder::new()),
            ColType::Bool => AttrColumn::Bool(BooleanBuilder::new()),
            ColType::Bytes => AttrColumn::Bytes(BinaryBuilder::new()),
        }
    }

    fn declared(&self) -> &'static str {
        match self {
            AttrColumn::Str(_) => "str",
            AttrColumn::I64(_) => "i64",
            AttrColumn::F64(_) => "f64",
            AttrColumn::Bool(_) => "bool",
            AttrColumn::Bytes(_) => "bytes",
        }
    }

    fn append_null(&mut self) {
        match self {
            AttrColumn::Str(b) => b.append_null(),
            AttrColumn::I64(b) => b.append_null(),
            AttrColumn::F64(b) => b.append_null(),
            AttrColumn::Bool(b) => b.append_null(),
            AttrColumn::Bytes(b) => b.append_null(),
        }
    }

    fn push(&mut self, key: &str, value: Option<&AttrValue>) -> anyhow::Result<()> {
        let Some(value) = value else {
            self.append_null();
            return Ok(());
        };
        match (&mut *self, value) {
            (AttrColumn::Str(b), AttrValue::Str(v)) => b.append_value(v),
            (AttrColumn::I64(b), AttrValue::I64(v)) => b.append_value(*v),
            (AttrColumn::F64(b), AttrValue::F64(v)) => b.append_value(*v),
            (AttrColumn::Bool(b), AttrValue::Bool(v)) => b.append_value(*v),
            (AttrColumn::Bytes(b), AttrValue::Bytes(v)) => b.append_value(v),
            (column, found) => anyhow::bail!(
                "attribute {key:?} is declared {} in the mapping but the stored value is {}; \
                 fix the mapping's type for this key, or drop the column and let \
                 attrs_map_column carry it",
                column.declared(),
                attr_type_name(found),
            ),
        }
        Ok(())
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            AttrColumn::Str(b) => Arc::new(b.finish()),
            AttrColumn::I64(b) => Arc::new(b.finish()),
            AttrColumn::F64(b) => Arc::new(b.finish()),
            AttrColumn::Bool(b) => Arc::new(b.finish()),
            AttrColumn::Bytes(b) => Arc::new(b.finish()),
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use arrow::array::{
        Array, BinaryArray, BooleanArray, FixedSizeBinaryArray, Float64Array, Int64Array, MapArray,
        StringArray,
    };
    use ravel_logseg::stream_attrs_bytes;

    use super::*;

    fn record(ts_ns: i64, attrs: Vec<(String, AttrValue)>) -> LogRecord {
        LogRecord {
            stream_id: LogStreamId([0u8; 16]),
            stream_attrs: stream_attrs_bytes(&[], "", "", &[]),
            ts_ns,
            observed_ts_ns: ts_ns,
            severity_num: 9,
            severity_text: "INFO".to_string(),
            body: "body".to_string(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs,
        }
    }

    fn mapping(text: &str) -> Mapping {
        crate::load::parse_mapping(text).expect("valid mapping")
    }

    fn batch_of(mapping: &Mapping, records: &[LogRecord]) -> RecordBatch {
        let resource: Vec<(String, AttrValue)> = Vec::new();
        let rows: Vec<ExportRow<'_>> = records
            .iter()
            .map(|record| ExportRow {
                record,
                resource: &resource,
            })
            .collect();
        build_batch(mapping, &rows).expect("batch builds")
    }

    #[test]
    fn unsupported_signal_message_names_the_follow_up_each_signal_waits_on() {
        assert_eq!(unsupported_signal_message(SignalArg::Logs), None);
        let metrics =
            unsupported_signal_message(SignalArg::Metrics).expect("metrics is unsupported");
        assert_eq!(
            metrics,
            "export --signal metrics is not available: ADR-1751 follow-up 3 sequences export for \
             metrics behind bulk import for metrics (ADR-1751 follow-up 1), which does not exist \
             yet, so an exported metrics file could not be loaded back. Only --signal logs is \
             supported."
        );
        let spans = unsupported_signal_message(SignalArg::Spans).expect("spans is unsupported");
        assert_eq!(
            spans,
            "export --signal spans is not available: ADR-1751 follow-up 3 sequences export for \
             spans behind bulk import for spans (ADR-1751 follow-up 2), which does not exist yet, \
             so an exported spans file could not be loaded back. Only --signal logs is supported."
        );
    }

    #[test]
    fn ts_column_is_written_in_the_mappings_declared_unit() {
        let m = mapping("ts_column = \"ts\"\nts_unit = \"millis\"\n");
        let batch = batch_of(&m, &[record(1_700_000_000_123_000_000, Vec::new())]);
        let ts = batch
            .column_by_name("ts")
            .expect("ts")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("ts is Int64");
        assert_eq!(ts.values(), &[1_700_000_000_123]);
    }

    #[test]
    fn a_typed_attribute_column_is_null_exactly_when_the_record_lacks_the_key() {
        let m = mapping(
            "ts_column = \"ts\"\nts_unit = \"nanos\"\n\n\
             [[attribute]]\nkey = \"count\"\ncolumn = \"count_col\"\ntype = \"i64\"\n",
        );
        let batch = batch_of(
            &m,
            &[
                record(1, vec![("count".to_string(), AttrValue::I64(7))]),
                record(2, Vec::new()),
            ],
        );
        let count = batch
            .column_by_name("count_col")
            .expect("count_col")
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("count_col is Int64");
        assert_eq!(count.len(), 2);
        assert!(!count.is_null(0));
        assert_eq!(count.value(0), 7);
        assert!(count.is_null(1));
    }

    #[test]
    fn every_declared_attribute_type_gets_its_matching_arrow_type() {
        let m = mapping(
            "ts_column = \"ts\"\nts_unit = \"nanos\"\n\n\
             [[attribute]]\nkey = \"s\"\ncolumn = \"s_col\"\ntype = \"str\"\n\n\
             [[attribute]]\nkey = \"f\"\ncolumn = \"f_col\"\ntype = \"f64\"\n\n\
             [[attribute]]\nkey = \"b\"\ncolumn = \"b_col\"\ntype = \"bool\"\n\n\
             [[attribute]]\nkey = \"y\"\ncolumn = \"y_col\"\ntype = \"bytes\"\n",
        );
        let batch = batch_of(
            &m,
            &[record(
                1,
                vec![
                    ("s".to_string(), AttrValue::Str("v".to_string())),
                    ("f".to_string(), AttrValue::F64(-0.5)),
                    ("b".to_string(), AttrValue::Bool(true)),
                    ("y".to_string(), AttrValue::Bytes(vec![1, 2])),
                ],
            )],
        );
        assert_eq!(
            batch
                .column_by_name("s_col")
                .expect("s_col")
                .as_any()
                .downcast_ref::<StringArray>()
                .expect("s_col is Utf8")
                .value(0),
            "v"
        );
        assert_eq!(
            batch
                .column_by_name("f_col")
                .expect("f_col")
                .as_any()
                .downcast_ref::<Float64Array>()
                .expect("f_col is Float64")
                .values(),
            &[-0.5]
        );
        assert!(
            batch
                .column_by_name("b_col")
                .expect("b_col")
                .as_any()
                .downcast_ref::<BooleanArray>()
                .expect("b_col is Boolean")
                .value(0)
        );
        assert_eq!(
            batch
                .column_by_name("y_col")
                .expect("y_col")
                .as_any()
                .downcast_ref::<BinaryArray>()
                .expect("y_col is Binary")
                .value(0),
            &[1u8, 2][..]
        );
    }

    #[test]
    fn a_stored_type_the_mapping_does_not_declare_is_refused_by_name() {
        let m = mapping(
            "ts_column = \"ts\"\nts_unit = \"nanos\"\n\n\
             [[attribute]]\nkey = \"count\"\ncolumn = \"count_col\"\ntype = \"i64\"\n",
        );
        let records = [record(
            1,
            vec![("count".to_string(), AttrValue::Str("seven".to_string()))],
        )];
        let resource: Vec<(String, AttrValue)> = Vec::new();
        let rows: Vec<ExportRow<'_>> = records
            .iter()
            .map(|record| ExportRow {
                record,
                resource: &resource,
            })
            .collect();
        let err = build_batch(&m, &rows).expect_err("a type mismatch is refused");
        assert_eq!(
            err.to_string(),
            "attribute \"count\" is declared i64 in the mapping but the stored value is str; fix \
             the mapping's type for this key, or drop the column and let attrs_map_column carry it"
        );
    }

    #[test]
    fn attrs_map_column_carries_exactly_the_attributes_no_typed_column_names() {
        let m = mapping(
            "ts_column = \"ts\"\nts_unit = \"nanos\"\nattrs_map_column = \"rest\"\n\n\
             [[attribute]]\nkey = \"count\"\ncolumn = \"count_col\"\ntype = \"i64\"\n",
        );
        let batch = batch_of(
            &m,
            &[record(
                1,
                vec![
                    ("count".to_string(), AttrValue::I64(7)),
                    ("note".to_string(), AttrValue::Str("hi".to_string())),
                    ("ratio".to_string(), AttrValue::F64(1.5)),
                ],
            )],
        );
        let rest = batch
            .column_by_name("rest")
            .expect("rest")
            .as_any()
            .downcast_ref::<MapArray>()
            .expect("rest is a Map")
            .value(0);
        let keys = rest
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("map keys are Utf8");
        let values = rest
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("map values are Utf8");
        let pairs: Vec<(&str, &str)> = (0..keys.len())
            .map(|i| (keys.value(i), values.value(i)))
            .collect();
        assert_eq!(pairs, vec![("note", "hi"), ("ratio", "1.5")]);
    }

    #[test]
    fn two_mapping_fields_sharing_one_output_column_are_refused() {
        let m = mapping(
            "ts_column = \"ts\"\nts_unit = \"nanos\"\nbody_column = \"same\"\n\n\
             [[attribute]]\nkey = \"count\"\ncolumn = \"same\"\ntype = \"i64\"\n",
        );
        let err = build_batch(&m, &[]).expect_err("a duplicate output column is refused");
        assert_eq!(
            err.to_string(),
            "the mapping writes two different fields to the output column \"same\"; give each \
             one its own column name"
        );
    }

    #[test]
    fn trace_and_span_ids_round_trip_as_fixed_width_or_null() {
        let m = mapping(
            "ts_column = \"ts\"\nts_unit = \"nanos\"\n\
             trace_id_column = \"trace_id\"\nspan_id_column = \"span_id\"\n",
        );
        let mut with_ids = record(1, Vec::new());
        with_ids.trace_id = Some([0xAB; 16]);
        with_ids.span_id = Some([0xCD; 8]);
        let batch = batch_of(&m, &[with_ids, record(2, Vec::new())]);
        let trace = batch
            .column_by_name("trace_id")
            .expect("trace_id")
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("trace_id is FixedSizeBinary");
        assert_eq!(trace.value(0), [0xAB; 16]);
        assert!(trace.is_null(1));
        let span = batch
            .column_by_name("span_id")
            .expect("span_id")
            .as_any()
            .downcast_ref::<FixedSizeBinaryArray>()
            .expect("span_id is FixedSizeBinary");
        assert_eq!(span.value(0), [0xCD; 8]);
        assert!(span.is_null(1));
    }
}
