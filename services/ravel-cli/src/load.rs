//! `ravel-cli load --parquet` (ADR-0089): bulk-import a Parquet file into the
//! logs signal through the existing [`ravel_ingest::LogIngestRouter`].
//!
//! This is a new *caller* of existing public APIs, not a new ingest path. The
//! loader constructs its own router in-process against the target tenant's
//! object store and provisioned shard count, reuses
//! [`ravel_otlp::NormalizedLogRecord`] as the record shape, re-implements the
//! `ravel-otlp` admission checks the ADR says to keep (future skew, length
//! caps), relaxes the ones it says to relax (past-event-time lag, per-record
//! attribute cap), and writes with [`WriteMode::Strict`] so every returned
//! success has no buffered-but-unflushed data.
//!
//! Which `ravel-otlp` rules this path keeps, relaxes, or bypasses, and why, is
//! documented in `docs/guides/ingest.md` ("Bulk import") per ADR-0089.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{
    Array, ArrayRef, BooleanArray, Float32Array, Float64Array, Int8Array, Int16Array, Int32Array,
    Int64Array, LargeBinaryArray, LargeStringArray, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    UInt16Array, UInt32Array, UInt64Array,
};
use arrow::array::{BinaryArray, FixedSizeBinaryArray};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use ravel_catalog::{AbsentPolicy, validate_or_adopt};
use ravel_ingest::{Clock, IngestConfig, LogIngestRouter, SystemClock, WriteMode};
use ravel_object_store::ObjectStoreBackend;
use ravel_otlp::NormalizedLogRecord;
use ravel_otlp::logs_limits::LogIngestLimits;
use ravel_types::logstream::{AttrValue, log_stream_id};
use ravel_types::{CommitToken, Signal, TenantId};
use serde::Deserialize;

/// Per-record attribute cap for the loader (ADR-0089 relaxation of the
/// `ravel-otlp` 128-per-record network cap).
///
/// 1024, deliberately far above OTLP's 128: bulk import is an offline,
/// operator-initiated action reading a file the operator already controls, a
/// different threat model than a networked OTLP sender, so a wide structured
/// export is admitted rather than rejected. This is a *per-record* axis and is
/// intentionally unrelated to the RLOG object's 1000-distinct-`(name, type)`
/// dynamic-column budget (`RlogConfig::max_dynamic_columns`): past that
/// per-object budget, extra columns fold into the `attrs_raw` overflow column
/// rather than being rejected, and the loader inherits that object-level
/// behavior from the writer unchanged (it writes no overflow logic of its own).
pub const LOADER_MAX_ATTRIBUTES_PER_RECORD: usize = 1024;

/// Default rows per Strict write. Each write is one flush per involved shard,
/// so this bounds the RLOG object size and the memory held while building a
/// batch. Every successful write is fully durable before the next batch starts.
pub const DEFAULT_BATCH_ROWS: usize = 10_000;

/// Ack deadline for each Strict write. Generous: a bulk load values completing
/// over racing a slow store.
const WRITE_ACK_DEADLINE: Duration = Duration::from_secs(60);

/// Source time unit for the mapped `ts` column, converted to nanoseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TsUnit {
    Seconds,
    Millis,
    Micros,
    Nanos,
}

impl TsUnit {
    fn factor(self) -> i64 {
        match self {
            TsUnit::Seconds => 1_000_000_000,
            TsUnit::Millis => 1_000_000,
            TsUnit::Micros => 1_000,
            TsUnit::Nanos => 1,
        }
    }
}

/// Declared type for a mapped attribute column, one of the scalar
/// [`AttrValue`] kinds. (Lists and maps have no Parquet-column source and are
/// not producible by this path.)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColType {
    Str,
    I64,
    F64,
    Bool,
    Bytes,
}

/// One mapped attribute: a source Parquet column, the record/resource key it
/// becomes, and its declared type.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AttrMap {
    /// The attribute key stored in the record (e.g. `service.name`).
    pub key: String,
    /// The source Parquet column name.
    pub column: String,
    /// Declared value type, used to build the typed [`AttrValue`].
    #[serde(rename = "type")]
    pub value_type: ColType,
}

/// The `--mapping` TOML: source Parquet columns to record fields.
///
/// ```toml
/// ts_column = "timestamp"
/// ts_unit   = "millis"        # seconds | millis | micros | nanos
///
/// body_column            = "message"   # optional
/// severity_number_column = "sev_num"   # optional (integer column)
/// severity_text_column   = "sev_text"  # optional (string column)
/// trace_id_column        = "trace_id"  # optional (16-byte binary or 32-hex str)
/// span_id_column         = "span_id"   # optional (8-byte binary or 16-hex str)
///
/// # Resource attributes: part of stream identity.
/// [[resource_attribute]]
/// key = "service.name"
/// column = "svc"
/// type = "str"
///
/// # Record attributes: typed values in `attrs`, NOT part of stream identity.
/// [[attribute]]
/// key = "http.status_code"
/// column = "status"
/// type = "i64"
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Mapping {
    pub ts_column: String,
    pub ts_unit: TsUnit,
    #[serde(default)]
    pub body_column: Option<String>,
    #[serde(default)]
    pub severity_number_column: Option<String>,
    #[serde(default)]
    pub severity_text_column: Option<String>,
    #[serde(default)]
    pub trace_id_column: Option<String>,
    #[serde(default)]
    pub span_id_column: Option<String>,
    /// Columns that determine stream identity (ADR-0029): distinct from record
    /// attributes.
    #[serde(default, rename = "resource_attribute")]
    pub resource_attributes: Vec<AttrMap>,
    /// Columns that become typed values in the record's `attrs`; never part of
    /// stream identity.
    #[serde(default, rename = "attribute")]
    pub attributes: Vec<AttrMap>,
}

/// Parse a `--mapping` TOML document.
pub fn parse_mapping(text: &str) -> Result<Mapping, LoadError> {
    toml::from_str(text).map_err(|e| LoadError::Setup(format!("invalid --mapping TOML: {e}")))
}

/// Printed to stderr before a load runs: the per-tenant `AdmissionController`
/// (active-stream cap, stream-creation rate, byte rate) lives in the server's
/// HTTP layer and is bypassed by construction on this path (ADR-0089).
pub const ADMISSION_BYPASS_WARNING: &str = "warning: bulk load writes directly to the log ingest router. The per-tenant \
     AdmissionController (active-stream cap, stream-creation rate, byte rate) that guards the \
     HTTP ingest path is NOT applied to loaded data. There is no resumability or deduplication: \
     re-running after a failure re-ingests the whole file from the start. Retention is measured \
     from load time, not from the records' event times.";

/// CLI entry point for `ravel-cli load --parquet`: read the mapping and Parquet
/// file paths, run the load, and print a summary on success or the error plus
/// the already-durable commit tokens on failure.
///
/// Returns `Err` (nonzero exit) for any failure. On a flush failure or a
/// rejected row, the durable commit tokens are printed rather than swallowed
/// (ADR-0089): a failure mid-file is a genuine partial load.
pub async fn run(
    store: Arc<dyn ObjectStoreBackend>,
    parquet_path: &Path,
    tenant: &str,
    mapping_path: &Path,
    shards: u32,
    now_ns: i64,
) -> anyhow::Result<()> {
    eprintln!("{ADMISSION_BYPASS_WARNING}");

    let mapping_text = std::fs::read_to_string(mapping_path)
        .map_err(|e| anyhow::anyhow!("failed to read --mapping {}: {e}", mapping_path.display()))?;
    let mapping = parse_mapping(&mapping_text)?;

    match load(
        store,
        parquet_path,
        tenant,
        &mapping,
        shards,
        DEFAULT_BATCH_ROWS,
        now_ns,
        Arc::new(SystemClock),
    )
    .await
    {
        Ok(report) => {
            print_summary(&report);
            Ok(())
        }
        Err(err) => {
            let durable = err.durable_tokens();
            print_durable_tokens(durable);
            Err(anyhow::Error::new(err))
        }
    }
}

/// Print the completion summary (ADR-0089 deliverable 6) to stdout.
fn print_summary(report: &LoadReport) {
    let secs = report.elapsed.as_secs_f64();
    let rows_per_sec = if secs > 0.0 {
        report.rows_processed as f64 / secs
    } else {
        report.rows_processed as f64
    };
    println!("bulk load complete");
    println!("  rows processed   : {}", report.rows_processed);
    println!("  rows/sec         : {rows_per_sec:.0}");
    println!("  objects written  : {}", report.objects_written());
    println!("  elapsed          : {secs:.3}s");
}

/// Print the commit tokens durable before a failure, one per line, so an
/// operator can see exactly what landed (ADR-0089 deliverable 7).
fn print_durable_tokens(tokens: &[CommitToken]) {
    if tokens.is_empty() {
        println!("no commit tokens were durable before the failure (nothing landed)");
        return;
    }
    println!(
        "{} commit token(s)/segment(s) were durable before the failure (a partial load; \
         re-running re-ingests the whole file, there is no dedup):",
        tokens.len()
    );
    for token in tokens {
        println!("  {}", token.encode());
    }
}

/// Result of a successful (or partially-durable) load, for the summary output.
#[derive(Debug, Clone, Default)]
pub struct LoadReport {
    pub rows_processed: u64,
    /// One token per shard flushed, across every batch. Its length is the
    /// number of objects/segments written.
    pub tokens: Vec<CommitToken>,
    pub elapsed: Duration,
}

impl LoadReport {
    pub fn objects_written(&self) -> usize {
        self.tokens.len()
    }
}

/// A load failure. Every variant that can occur after some data is already
/// durable carries the durable commit tokens, so the caller reports the
/// genuine partial load rather than swallowing it into a generic error
/// (ADR-0089: a failed flush is a partial load, not a rollback).
#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    /// Setup failed before any record was written (mapping, provisioning, file
    /// open, Parquet schema, missing column). Nothing is durable.
    #[error("{0}")]
    Setup(String),
    /// A row failed a kept admission check (future skew, length cap, or the
    /// loader per-record attribute cap). Fail-fast: the run stops at the first
    /// bad row. Any tokens listed are batches durable before this row.
    #[error("row {row}: {reason}")]
    RowRejected {
        row: u64,
        reason: String,
        durable: Vec<CommitToken>,
    },
    /// A flush (object-store PUT) failed. The tokens are what was durable
    /// before the failure; the failed batch and everything after it are not.
    #[error("flush failed: {cause}")]
    Flush {
        durable: Vec<CommitToken>,
        cause: String,
    },
}

impl LoadError {
    /// The commit tokens already durable when this error occurred (empty for a
    /// setup error).
    pub fn durable_tokens(&self) -> &[CommitToken] {
        match self {
            LoadError::Setup(_) => &[],
            LoadError::RowRejected { durable, .. } | LoadError::Flush { durable, .. } => durable,
        }
    }
}

/// Bulk-import `parquet_path` into `tenant`'s logs signal.
///
/// - `shards` is the configured shard count. It is validated against (or, for a
///   fresh signal, written to) the durable provisioning record via
///   [`validate_or_adopt`] with [`AbsentPolicy::CreateFromConfig`], the same
///   first-touch path `services/ravel-server` runs; the router then resolves
///   the active generation from that record itself.
/// - `batch_rows` rows are written per Strict flush.
/// - `now_ns` is the ingest-time anchor for the future-skew check (the past-lag
///   check is deliberately omitted per ADR-0089). Bucketing is by the router's
///   own clock (load-time wall clock), independent of the records' event times.
///
/// Fail-fast on the first row that fails a kept admission check. A run that
/// returns `Ok` has every row durable; a run that returns `Err` reports the
/// tokens already durable (a partial load — re-running re-ingests the whole
/// file, there is no resumability or dedup in this version).
#[allow(clippy::too_many_arguments)]
pub async fn load(
    store: Arc<dyn ObjectStoreBackend>,
    parquet_path: &Path,
    tenant: &str,
    mapping: &Mapping,
    shards: u32,
    batch_rows: usize,
    now_ns: i64,
    clock: Arc<dyn Clock>,
) -> Result<LoadReport, LoadError> {
    let limits = LogIngestLimits::default();
    let tenant_id = TenantId::new(tenant);

    // Reuse the server's provisioning validation/adoption. Fresh signal: pins
    // the record at `shards`. Existing record: a differing count is refused
    // here, before any write, exactly as the server refuses it at first touch.
    validate_or_adopt(
        store.as_ref(),
        &tenant_id.hash(),
        Signal::Logs,
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

    // `target_bytes: 1` makes each Strict batch flush immediately as one RLOG
    // object, inside `handle_write`'s size trigger, rather than waiting on the
    // age trigger's `max_flush_delay` clock. That is what a bulk loader wants:
    // one object per batch, `batch_rows` controls its size, and every write's
    // ack is durable with no lingering buffer. It also keeps flush timing off
    // the wall clock, so the object buckets by the flush-open reading directly.
    let router = LogIngestRouter::new(
        IngestConfig {
            shard_count: shards,
            target_bytes: 1,
            ..IngestConfig::default()
        },
        Arc::clone(&store),
        clock,
    );

    let file = std::fs::File::open(parquet_path)
        .map_err(|e| LoadError::Setup(format!("failed to open {}: {e}", parquet_path.display())))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| LoadError::Setup(format!("failed to open Parquet reader: {e}")))?;
    let reader = builder
        .with_batch_size(batch_rows.max(1))
        .build()
        .map_err(|e| LoadError::Setup(format!("failed to build Parquet reader: {e}")))?;

    let started = Instant::now();
    let mut report = LoadReport::default();
    let mut row_base: u64 = 0;

    for batch in reader {
        let batch =
            batch.map_err(|e| LoadError::Setup(format!("failed to read Parquet batch: {e}")))?;
        // Resolve column indices once per batch (schema is stable across
        // batches, but re-resolving keeps this self-contained and cheap).
        let cols = ColumnIndex::resolve(&batch, mapping).map_err(LoadError::Setup)?;

        let mut records = Vec::with_capacity(batch.num_rows());
        for row in 0..batch.num_rows() {
            let record =
                build_record(&batch, &cols, mapping, &limits, now_ns, row).map_err(|reason| {
                    LoadError::RowRejected {
                        row: row_base + row as u64,
                        reason,
                        durable: report.tokens.clone(),
                    }
                })?;
            records.push(record);
        }
        row_base += batch.num_rows() as u64;

        if records.is_empty() {
            continue;
        }
        let n = records.len() as u64;
        let receipt = router
            .write(
                tenant_id.clone(),
                records,
                WriteMode::Strict,
                WRITE_ACK_DEADLINE,
            )
            .await
            .map_err(|e| LoadError::Flush {
                durable: report.tokens.clone(),
                cause: e.to_string(),
            })?;
        report.rows_processed += n;
        report.tokens.extend(receipt.tokens);
    }

    // Strict acks already guarantee durability; flush_all is a defensive
    // no-op here (nothing is buffered after a Strict write returns).
    router.flush_all().await;

    report.elapsed = started.elapsed();
    Ok(report)
}

/// Resolved column indices for the mapped fields of one batch.
struct ColumnIndex {
    ts: usize,
    body: Option<usize>,
    severity_number: Option<usize>,
    severity_text: Option<usize>,
    trace_id: Option<usize>,
    span_id: Option<usize>,
    /// `(index, &AttrMap)` for each resource attribute column.
    resource: Vec<(usize, usize)>,
    /// `(index, &AttrMap)` for each record attribute column.
    record: Vec<(usize, usize)>,
}

impl ColumnIndex {
    fn resolve(batch: &RecordBatch, mapping: &Mapping) -> Result<ColumnIndex, String> {
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
        Ok(ColumnIndex {
            ts: idx(&mapping.ts_column)?,
            body: opt(&mapping.body_column)?,
            severity_number: opt(&mapping.severity_number_column)?,
            severity_text: opt(&mapping.severity_text_column)?,
            trace_id: opt(&mapping.trace_id_column)?,
            span_id: opt(&mapping.span_id_column)?,
            resource,
            record,
        })
    }
}

/// Build one [`NormalizedLogRecord`] from row `row` of `batch`, applying the
/// kept ADR-0089 admission checks. `Err` carries a per-row rejection reason.
fn build_record(
    batch: &RecordBatch,
    cols: &ColumnIndex,
    mapping: &Mapping,
    limits: &LogIngestLimits,
    now_ns: i64,
    row: usize,
) -> Result<NormalizedLogRecord, String> {
    // Timestamp is required; a null or unreadable ts is a row rejection.
    let ts_col = batch.column(cols.ts);
    let raw_ts = read_ts(ts_col, row, mapping.ts_unit)?
        .ok_or_else(|| format!("ts column {:?} is null", mapping.ts_column))?;

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
        Some(i) => read_string(batch.column(i), row)?.unwrap_or_default(),
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
        Some(i) => read_i64(batch.column(i), row)?
            .and_then(|v| u8::try_from(v).ok())
            .unwrap_or(0),
        None => 0,
    };
    let severity_text = match cols.severity_text {
        Some(i) => read_string(batch.column(i), row)?.unwrap_or_default(),
        None => String::new(),
    };

    // Trace/span ids: exact byte length or absent (never padded or truncated),
    // matching ravel-otlp.
    let trace_id = match cols.trace_id {
        Some(i) => read_id::<16>(batch.column(i), row)?,
        None => None,
    };
    let span_id = match cols.span_id {
        Some(i) => read_id::<8>(batch.column(i), row)?,
        None => None,
    };

    // Resource attributes: part of stream identity. A null column is omitted.
    let mut resource_attrs: Vec<(String, AttrValue)> = Vec::with_capacity(cols.resource.len());
    for (col_idx, map_idx) in &cols.resource {
        let spec = &mapping.resource_attributes[*map_idx];
        if let Some(value) = read_attr(batch.column(*col_idx), row, spec.value_type)? {
            check_attr(&spec.key, &value, limits)?;
            resource_attrs.push((spec.key.clone(), value));
        }
    }

    // Record attributes: typed values in `attrs`, never part of identity.
    let mut attrs: Vec<(String, AttrValue)> = Vec::with_capacity(cols.record.len());
    for (col_idx, map_idx) in &cols.record {
        let spec = &mapping.attributes[*map_idx];
        if let Some(value) = read_attr(batch.column(*col_idx), row, spec.value_type)? {
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
fn check_attr(key: &str, value: &AttrValue, limits: &LogIngestLimits) -> Result<(), String> {
    if key.len() > limits.max_attribute_key_len {
        return Err(format!(
            "attribute key {key:?} is {} bytes, more than the limit of {}",
            key.len(),
            limits.max_attribute_key_len
        ));
    }
    let len = attr_value_len(value);
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
fn read_attr(arr: &ArrayRef, row: usize, ty: ColType) -> Result<Option<AttrValue>, String> {
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

/// Read an integer cell as `i64`, accepting any Arrow integer width.
fn read_i64(arr: &ArrayRef, row: usize) -> Result<Option<i64>, String> {
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
        other => return Err(format!("expected an integer column, found {other:?}")),
    };
    Ok(Some(v))
}

/// Read a floating cell as `f64`, accepting f32 or f64.
fn read_f64(arr: &ArrayRef, row: usize) -> Result<Option<f64>, String> {
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

fn read_bool(arr: &ArrayRef, row: usize) -> Result<Option<bool>, String> {
    if arr.is_null(row) {
        return Ok(None);
    }
    match arr.data_type() {
        DataType::Boolean => Ok(Some(downcast::<BooleanArray>(arr)?.value(row))),
        other => Err(format!("expected a boolean column, found {other:?}")),
    }
}

/// Read a UTF-8 string cell, accepting `Utf8` and `LargeUtf8`.
fn read_string(arr: &ArrayRef, row: usize) -> Result<Option<String>, String> {
    if arr.is_null(row) {
        return Ok(None);
    }
    match arr.data_type() {
        DataType::Utf8 => Ok(Some(downcast::<StringArray>(arr)?.value(row).to_string())),
        DataType::LargeUtf8 => Ok(Some(
            downcast::<LargeStringArray>(arr)?.value(row).to_string(),
        )),
        other => Err(format!("expected a string column, found {other:?}")),
    }
}

/// Read a binary cell, accepting `Binary`, `LargeBinary`, and `FixedSizeBinary`.
fn read_bytes(arr: &ArrayRef, row: usize) -> Result<Option<Vec<u8>>, String> {
    if arr.is_null(row) {
        return Ok(None);
    }
    match arr.data_type() {
        DataType::Binary => Ok(Some(downcast::<BinaryArray>(arr)?.value(row).to_vec())),
        DataType::LargeBinary => Ok(Some(downcast::<LargeBinaryArray>(arr)?.value(row).to_vec())),
        DataType::FixedSizeBinary(_) => Ok(Some(
            downcast::<FixedSizeBinaryArray>(arr)?.value(row).to_vec(),
        )),
        other => Err(format!("expected a binary column, found {other:?}")),
    }
}

/// Read the `ts` column to nanoseconds. An integer column uses the mapping's
/// declared unit; a native Arrow `Timestamp` column uses its own unit (its
/// values are already scaled), and the declared unit is not applied again.
fn read_ts(arr: &ArrayRef, row: usize, declared: TsUnit) -> Result<Option<i64>, String> {
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
fn read_id<const N: usize>(arr: &ArrayRef, row: usize) -> Result<Option<[u8; N]>, String> {
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
fn downcast<A: 'static>(arr: &ArrayRef) -> Result<&A, String> {
    arr.as_any()
        .downcast_ref::<A>()
        .ok_or_else(|| "internal error: Arrow array downcast failed".to_string())
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    /// A fixed, plausible (post-2020) load-time anchor for the admission
    /// checks; the exact value only matters relative to the event timestamps.
    const NOW_NS: i64 = 1_700_000_000_000_000_000; // 2023-11-14

    fn batch(cols: Vec<(&str, ArrayRef)>) -> RecordBatch {
        RecordBatch::try_from_iter(cols.into_iter().map(|(n, a)| (n.to_string(), a)))
            .expect("record batch")
    }

    fn i64_col(vals: Vec<i64>) -> ArrayRef {
        Arc::new(Int64Array::from(vals))
    }

    fn str_col(vals: Vec<&str>) -> ArrayRef {
        Arc::new(StringArray::from(vals))
    }

    /// A minimal mapping over a single `ts` column (nanoseconds).
    fn base_mapping() -> Mapping {
        Mapping {
            ts_column: "ts".to_string(),
            ts_unit: TsUnit::Nanos,
            body_column: None,
            severity_number_column: None,
            severity_text_column: None,
            trace_id_column: None,
            span_id_column: None,
            resource_attributes: Vec::new(),
            attributes: Vec::new(),
        }
    }

    fn attr(key: &str, column: &str, ty: ColType) -> AttrMap {
        AttrMap {
            key: key.to_string(),
            column: column.to_string(),
            value_type: ty,
        }
    }

    fn build_row(
        batch: &RecordBatch,
        mapping: &Mapping,
        row: usize,
    ) -> Result<NormalizedLogRecord, String> {
        let cols = ColumnIndex::resolve(batch, mapping).expect("resolve columns");
        build_record(
            batch,
            &cols,
            mapping,
            &LogIngestLimits::default(),
            NOW_NS,
            row,
        )
    }

    #[test]
    fn future_skew_beyond_the_bound_is_rejected() {
        let limits = LogIngestLimits::default();
        let m = base_mapping();
        let b = batch(vec![(
            "ts",
            i64_col(vec![NOW_NS + limits.max_future_skew_ns + 1]),
        )]);
        let err = build_row(&b, &m, 0).expect_err("a far-future row must be rejected");
        assert!(err.contains("future skew"), "{err}");
    }

    #[test]
    fn event_exactly_at_the_future_skew_bound_is_accepted() {
        let limits = LogIngestLimits::default();
        let m = base_mapping();
        let b = batch(vec![(
            "ts",
            i64_col(vec![NOW_NS + limits.max_future_skew_ns]),
        )]);
        let rec = build_row(&b, &m, 0).expect("the bound itself passes");
        assert_eq!(rec.ts_ns, NOW_NS + limits.max_future_skew_ns);
    }

    /// The deliberate ADR-0089 relaxation: a 2013-era timestamp lagging load
    /// time by a decade is accepted, where OTLP would reject it as `TooOld`.
    #[test]
    fn past_event_time_lag_is_not_rejected() {
        let m = base_mapping();
        let ts_2013 = 1_356_998_400_000_000_000; // 2013-01-01
        let b = batch(vec![("ts", i64_col(vec![ts_2013]))]);
        let rec = build_row(&b, &m, 0).expect("a decade-old event is admitted, not rejected");
        assert_eq!(rec.ts_ns, ts_2013);
    }

    #[test]
    fn oversized_body_is_rejected_at_the_otlp_bound() {
        let limits = LogIngestLimits::default();
        let mut m = base_mapping();
        m.body_column = Some("body".to_string());
        let big = "x".repeat(limits.max_body_len + 1);
        let b = batch(vec![
            ("ts", i64_col(vec![NOW_NS])),
            ("body", str_col(vec![big.as_str()])),
        ]);
        let err = build_row(&b, &m, 0).expect_err("an oversized body is rejected");
        assert!(err.contains("body is"), "{err}");
    }

    #[test]
    fn oversized_attribute_key_is_rejected_at_the_otlp_bound() {
        let limits = LogIngestLimits::default();
        let mut m = base_mapping();
        let long_key = "k".repeat(limits.max_attribute_key_len + 1);
        m.attributes = vec![attr(&long_key, "v", ColType::Str)];
        let b = batch(vec![
            ("ts", i64_col(vec![NOW_NS])),
            ("v", str_col(vec!["value"])),
        ]);
        let err = build_row(&b, &m, 0).expect_err("an oversized attribute key is rejected");
        assert!(err.contains("attribute key"), "{err}");
    }

    #[test]
    fn oversized_attribute_value_is_rejected_at_the_otlp_bound() {
        let limits = LogIngestLimits::default();
        let mut m = base_mapping();
        m.attributes = vec![attr("k", "v", ColType::Str)];
        let big = "x".repeat(limits.max_attribute_value_len + 1);
        let b = batch(vec![
            ("ts", i64_col(vec![NOW_NS])),
            ("v", str_col(vec![big.as_str()])),
        ]);
        let err = build_row(&b, &m, 0).expect_err("an oversized attribute value is rejected");
        assert!(err.contains("value is"), "{err}");
    }

    #[test]
    fn record_attributes_at_the_loader_cap_pass_and_over_it_are_rejected() {
        // Build `cap + 1` record-attribute columns; one row over the cap is
        // rejected (not silently truncated), and exactly-at-cap passes.
        let cap = LOADER_MAX_ATTRIBUTES_PER_RECORD;
        let over = cap + 1;
        let mut cols: Vec<(String, ArrayRef)> = vec![("ts".to_string(), i64_col(vec![NOW_NS]))];
        let mut attrs = Vec::new();
        for i in 0..over {
            let name = format!("a{i}");
            cols.push((name.clone(), i64_col(vec![i as i64])));
            attrs.push(attr(&name, &name, ColType::I64));
        }
        let b = RecordBatch::try_from_iter(cols).expect("wide batch");

        let mut m_over = base_mapping();
        m_over.attributes = attrs.clone();
        let err = build_row(&b, &m_over, 0).expect_err("over the loader cap must be rejected");
        assert!(err.contains("loader per-record cap"), "{err}");

        let mut m_at = base_mapping();
        m_at.attributes = attrs[..cap].to_vec();
        let rec = build_row(&b, &m_at, 0).expect("exactly at the cap passes");
        assert_eq!(rec.attrs.len(), cap);
    }

    /// Resource-attribute columns determine stream identity; record-attribute
    /// columns never do.
    #[test]
    fn stream_identity_follows_resource_attributes_not_record_attributes() {
        let mut m = base_mapping();
        m.resource_attributes = vec![attr("service.name", "svc", ColType::Str)];
        m.attributes = vec![attr("http.status_code", "status", ColType::I64)];
        // Row 0: svc=api status=1; Row 1: svc=web status=1; Row 2: svc=api status=2.
        let b = batch(vec![
            ("ts", i64_col(vec![NOW_NS, NOW_NS, NOW_NS])),
            ("svc", str_col(vec!["api", "web", "api"])),
            ("status", i64_col(vec![1, 1, 2])),
        ]);
        let r0 = build_row(&b, &m, 0).expect("row 0");
        let r1 = build_row(&b, &m, 1).expect("row 1");
        let r2 = build_row(&b, &m, 2).expect("row 2");

        assert_ne!(
            r0.stream_id, r1.stream_id,
            "different resource attribute values must produce different streams"
        );
        assert_eq!(
            r0.stream_id, r2.stream_id,
            "a differing record attribute must not change stream identity"
        );
        // The record attribute is carried, typed, in attrs.
        assert_eq!(
            r0.attrs,
            vec![("http.status_code".to_string(), AttrValue::I64(1))]
        );
    }

    #[test]
    fn typed_columns_become_typed_attr_values() {
        let mut m = base_mapping();
        m.attributes = vec![
            attr("s", "s", ColType::Str),
            attr("i", "i", ColType::I64),
            attr("f", "f", ColType::F64),
            attr("b", "b", ColType::Bool),
        ];
        let b = batch(vec![
            ("ts", i64_col(vec![NOW_NS])),
            ("s", str_col(vec!["hi"])),
            ("i", i64_col(vec![7])),
            ("f", Arc::new(Float64Array::from(vec![1.5f64])) as ArrayRef),
            ("b", Arc::new(BooleanArray::from(vec![true])) as ArrayRef),
        ]);
        let rec = build_row(&b, &m, 0).expect("typed row");
        assert_eq!(
            rec.attrs,
            vec![
                ("s".to_string(), AttrValue::Str("hi".to_string())),
                ("i".to_string(), AttrValue::I64(7)),
                ("f".to_string(), AttrValue::F64(1.5)),
                ("b".to_string(), AttrValue::Bool(true)),
            ]
        );
    }

    #[test]
    fn ts_unit_scales_to_nanoseconds() {
        let mut m = base_mapping();
        m.ts_unit = TsUnit::Millis;
        let b = batch(vec![("ts", i64_col(vec![1_700_000_000_000]))]);
        let rec = build_row(&b, &m, 0).expect("millis ts");
        assert_eq!(rec.ts_ns, 1_700_000_000_000 * 1_000_000);
    }

    #[test]
    fn mapping_round_trips_through_toml() {
        let m = parse_mapping(
            r#"
ts_column = "timestamp"
ts_unit = "micros"
body_column = "msg"

[[resource_attribute]]
key = "service.name"
column = "svc"
type = "str"

[[attribute]]
key = "code"
column = "status"
type = "i64"
"#,
        )
        .expect("valid mapping");
        assert_eq!(m.ts_column, "timestamp");
        assert_eq!(m.ts_unit, TsUnit::Micros);
        assert_eq!(m.body_column.as_deref(), Some("msg"));
        assert_eq!(m.resource_attributes.len(), 1);
        assert_eq!(m.resource_attributes[0].value_type, ColType::Str);
        assert_eq!(m.attributes.len(), 1);
        assert_eq!(m.attributes[0].value_type, ColType::I64);
    }

    #[test]
    fn unknown_mapping_field_is_rejected() {
        let err = parse_mapping("ts_column = \"t\"\nts_unit = \"nanos\"\nbogus = 1\n")
            .expect_err("deny_unknown_fields rejects a typo");
        assert!(matches!(err, LoadError::Setup(_)));
    }
}
