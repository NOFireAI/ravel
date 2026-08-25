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

use arrow::array::AsArray;
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Float32Array, Float64Array, Int8Array,
    Int16Array, Int32Array, Int64Array, LargeBinaryArray, LargeStringArray, StringArray,
    TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt8Array, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::array::{BinaryArray, FixedSizeBinaryArray};
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use ravel_catalog::{AbsentPolicy, validate_or_adopt};
#[cfg(feature = "stage-timing")]
use ravel_ingest::LogStageSnapshot;
use ravel_ingest::{
    Clock, IngestConfig, LogIngestMetricsSnapshot, LogIngestRouter, LogWriteError, LogWriteReceipt,
    SystemClock, WriteMode,
};
use ravel_logseg::{
    Bitmap, ColumnarLogBatch, DynColumn, FieldType, StrColumnDict, stream_attrs_bytes,
};
use ravel_object_store::ObjectStoreBackend;
use ravel_otlp::NormalizedLogRecord;
use ravel_otlp::logs_limits::LogIngestLimits;
use ravel_types::logstream::{AttrValue, LogStreamId, log_stream_id};
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

/// Near-cap warning threshold: the loader warns when the widest single object's
/// `dynamic_columns_used` reaches this fraction of `max_dynamic_columns`,
/// expressed as a percentage so the comparison is exact integer arithmetic
/// (`used * 100 >= max * NEAR_CAP_PERCENT`) with no float rounding at the
/// boundary. 90% is deliberate headroom: it fires before an object overflows,
/// not only after.
const NEAR_CAP_PERCENT: u64 = 90;

/// Warnings to print to stderr after a load, derived from the router's
/// cumulative dynamic-column counters (ADR-0100 decision 1). Returns at most one
/// message:
///
/// - an overflow warning when any object crossed its dynamic-column budget
///   (`dynamic_columns_overflowed_total > 0`), naming the count; or, when
///   nothing overflowed,
/// - a distinct near-cap warning when the widest object's `dynamic_columns_used`
///   reached [`NEAR_CAP_PERCENT`]% of `max_dynamic_columns`.
///
/// Both state the same consequence an operator needs to act on: an overflowed
/// attribute folds into the object's `attrs_raw` column, so it stays queryable
/// through `attrs['<key>']` but gets no typed column (a typed predicate or
/// aggregate over it is unavailable, and a SQL filter pays a per-row string
/// cast). An empty vector means the load stayed comfortably under the budget and
/// nothing is printed.
pub fn dynamic_column_warnings(
    metrics: &LogIngestMetricsSnapshot,
    max_dynamic_columns: usize,
) -> Vec<String> {
    let max = max_dynamic_columns as u64;
    if metrics.dynamic_columns_overflowed_total > 0 {
        return vec![format!(
            "warning: {overflowed} distinct (attribute name, type) pair(s) overflowed the \
             per-object dynamic-column budget of {max} during this load. Each overflowed \
             attribute was folded into the object's attrs_raw overflow column: it stays \
             queryable through attrs['<key>'], but it gets NO typed column, so a typed predicate \
             or aggregate over it is unavailable and a SQL filter pays a per-row string cast. To \
             give an overflowed key a typed column, reduce the number of distinct attribute \
             columns per stream (map fewer columns, or split the load so each object stays under \
             {max} distinct (name, type) pairs).",
            overflowed = metrics.dynamic_columns_overflowed_total,
        )];
    }
    if max > 0 && metrics.dynamic_columns_used_max * 100 >= max * NEAR_CAP_PERCENT {
        return vec![format!(
            "warning: this load reached {used} distinct dynamic columns in a single object, at or \
             above {pct}% of the per-object budget of {max}. No object overflowed, but a wider \
             stream or one more attribute would push columns past the budget into the attrs_raw \
             overflow column, where they stay queryable through attrs['<key>'] but get no typed \
             column. Reduce the number of distinct attribute columns per stream, or split the \
             load, to keep headroom under {max}.",
            used = metrics.dynamic_columns_used_max,
            pct = NEAR_CAP_PERCENT,
        )];
    }
    Vec::new()
}

/// CLI entry point for `ravel-cli load --parquet`: read the mapping and Parquet
/// file paths, run the load, and print a summary on success or the error plus
/// the known-durable commit tokens on failure.
///
/// After a successful load, any dynamic-column overflow or near-cap pressure is
/// reported to stderr from [`dynamic_column_warnings`] over the router's
/// cumulative counters (ADR-0100 decision 1).
///
/// Returns `Err` (nonzero exit) for any failure. On a flush failure or a
/// rejected row, the durable commit tokens are printed rather than swallowed
/// (ADR-0089): a failure mid-file is a genuine partial load. See
/// [`print_durable_tokens`] for the residual cases (a flush that timed out or
/// lost a shard at send time) where the printed list can still be a lower
/// bound rather than exact.
#[allow(clippy::too_many_arguments)]
pub async fn run(
    store: Arc<dyn ObjectStoreBackend>,
    parquet_path: &Path,
    tenant: &str,
    mapping_path: &Path,
    shards: u32,
    batch_rows: usize,
    read_cursors: Option<usize>,
    pipeline_depth: usize,
    now_ns: i64,
) -> anyhow::Result<()> {
    run_warning_to(
        store,
        parquet_path,
        tenant,
        mapping_path,
        shards,
        batch_rows,
        read_cursors,
        pipeline_depth,
        now_ns,
        &mut std::io::stderr(),
    )
    .await
}

/// [`run`] with its warning stream injected, so a test can prove the warnings
/// actually reach a caller of the real entry point.
///
/// The seam exists because the alternative was untestable: the dynamic-column
/// warnings are the whole operator-facing deliverable of ADR-0100 decision 1,
/// and with `eprintln!` inlined here, deleting the emit loop left every test
/// green. Only [`run`]'s one-line delegation above is now unproven by a test.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_warning_to(
    store: Arc<dyn ObjectStoreBackend>,
    parquet_path: &Path,
    tenant: &str,
    mapping_path: &Path,
    shards: u32,
    batch_rows: usize,
    read_cursors: Option<usize>,
    pipeline_depth: usize,
    now_ns: i64,
    warnings: &mut dyn std::io::Write,
) -> anyhow::Result<()> {
    // A diagnostic that cannot be written is not worth failing a durable load
    // over, here or below.
    let _ = writeln!(warnings, "{ADMISSION_BYPASS_WARNING}");

    let mapping_text = std::fs::read_to_string(mapping_path)
        .map_err(|e| anyhow::anyhow!("failed to read --mapping {}: {e}", mapping_path.display()))?;
    let mapping = parse_mapping(&mapping_text)?;

    match load(
        store,
        parquet_path,
        tenant,
        &mapping,
        shards,
        batch_rows,
        read_cursors,
        pipeline_depth,
        now_ns,
        Arc::new(SystemClock),
    )
    .await
    {
        Ok(report) => {
            print_summary(&report);
            // Early, so an operator watching a long load sees it as soon as it is
            // known, ahead of the end-of-load dynamic-column pressure warnings.
            if let Some(skew) = &report.skew_warning {
                let _ = writeln!(warnings, "{skew}");
            }
            // The loader's writer uses `RlogConfig::default()` (log_shard.rs), so
            // its per-object dynamic-column budget is that default.
            let max_dynamic_columns = ravel_logseg::RlogConfig::default().max_dynamic_columns;
            for warning in dynamic_column_warnings(&report.metrics, max_dynamic_columns) {
                let _ = writeln!(warnings, "{warning}");
            }
            Ok(())
        }
        Err(err) => {
            print_durable_tokens(&err);
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
    #[cfg(feature = "stage-timing")]
    print_stage_timings(report);
}

/// Print the logs pipeline's per-stage timing breakdown (ADR-0104 decision 1)
/// after [`print_summary`]'s totals. A stage with zero samples (never wired,
/// or never reached) is omitted rather than printed as zero, matching
/// [`LogStageSnapshot::stages`]'s own present-only-if-recorded contract.
#[cfg(feature = "stage-timing")]
fn print_stage_timings(report: &LoadReport) {
    if report.stage_timings.is_empty() {
        return;
    }
    println!("  stage timings:");
    for stage in report.stage_timings.stages() {
        let Some(totals) = report.stage_timings.get(stage) else {
            continue;
        };
        let avg_us = (totals.total_ns as f64 / totals.samples.max(1) as f64) / 1e3;
        println!(
            "    {name:<8} samples={samples:<10} total_ms={total_ms:<12.3} avg_us={avg_us:.3}",
            name = stage.name(),
            samples = totals.samples,
            total_ms = totals.total_ns as f64 / 1e6,
        );
    }
}

/// Print the commit tokens known durable before a failure, one per line, so
/// an operator can see what landed (ADR-0089 deliverable 7). On
/// [`LoadError::Flush`] the list now includes the failing batch's own shards
/// that acked durable before a sibling shard failed, recovered from the
/// router error via `LogWriteError::durable_tokens` (issue #296), so it is
/// exact for the common partial-flush case. It remains a lower bound only when
/// the failing batch's ack round did not resolve at all -- an ack-deadline
/// timeout, or a shard channel dying at send time -- because no per-shard ack
/// is observed then. Every non-flush variant's tokens are exact: the failing
/// row or batch never reached `LogIngestRouter::write`.
fn print_durable_tokens(err: &LoadError) {
    let tokens = err.durable_tokens();
    let is_flush = matches!(err, LoadError::Flush { .. });
    if tokens.is_empty() {
        if is_flush {
            println!(
                "no commit tokens were durable before the failure (any earlier batches, and any \
                 shard of the failing batch that acked durable, are listed here; none did -- if \
                 the failing batch timed out or a shard died at send time, a shard may still have \
                 committed without an observable ack)"
            );
        } else {
            println!("no commit tokens were durable before the failure (nothing landed)");
        }
        return;
    }
    let suffix = if is_flush {
        " (exact for a partial flush where a sibling shard committed; still a lower bound if the \
          failing batch timed out or a shard died at send time, where a commit can land with no \
          observable ack)"
    } else {
        ""
    };
    println!(
        "{} commit token(s)/segment(s) were durable before the failure (a partial load; \
         re-running re-ingests the whole file, there is no dedup){suffix}:",
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
    /// The router's cumulative write metrics, snapshotted once the load
    /// finished. Carries the dynamic-column counters (ADR-0100 decision 1) the
    /// caller reads to emit an overflow or near-cap warning; there is no other
    /// return path for a per-load signal (`LogIngestRouter::metrics()` is only
    /// reachable by whoever constructed the router, which is `load` itself).
    pub metrics: LogIngestMetricsSnapshot,
    /// The early shard-skew warning (issue #560), set at most once, the first
    /// time the check at [`SKEW_CHECK_AFTER_BATCHES`] data batches finds the
    /// spread at or below the [`SKEW_WARN_DENOMINATOR`] threshold. `None` when
    /// the load never reached the check point or stayed above it.
    pub skew_warning: Option<String>,
    /// Number of columnar batches this load built and drove through
    /// `LogIngestRouter::write_columnar` (ADR-0109). Nonzero only on the columnar
    /// fast path [`load`] uses; the row differential path leaves it 0. This is
    /// the reachability signal a caller of the real entry point can observe to
    /// prove the columnar path ran, not merely that its builder compiles.
    pub columnar_batches_built: u64,
    /// The logs pipeline's per-stage timing breakdown (ADR-0104 decision 1),
    /// snapshotted once the load finished. Present only under the
    /// `stage-timing` feature; with it off this field does not exist, so a
    /// default build carries no timing seam.
    #[cfg(feature = "stage-timing")]
    pub stage_timings: LogStageSnapshot,
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
    /// open, Parquet reader construction). Nothing is durable. Not used once
    /// the batch loop starts — a failure there uses `BatchFailed` instead,
    /// since earlier batches in the same run may already be durable.
    #[error("{0}")]
    Setup(String),
    /// A batch failed to decode, or its columns failed to resolve against the
    /// mapping, once the loop has already started (a later Parquet batch may
    /// have a schema/type Parquet itself allows but the mapping cannot
    /// handle). Distinct from `Setup`: earlier batches in the same run may
    /// already be durable, and this variant reports them rather than
    /// silently losing them.
    #[error("{reason}")]
    BatchFailed {
        reason: String,
        durable: Vec<CommitToken>,
    },
    /// A row failed a kept admission check (future skew, length cap, or the
    /// loader per-record attribute cap). Fail-fast: the run stops at the first
    /// bad row. Any tokens listed are batches durable before this row.
    #[error("row {row}: {reason}")]
    RowRejected {
        row: u64,
        reason: String,
        durable: Vec<CommitToken>,
    },
    /// A flush (object-store PUT) failed. The tokens are what was durable from
    /// *earlier* batches, plus any shard of the failing batch itself that acked
    /// its commit durably before a sibling shard failed:
    /// `LogIngestRouter::write` returns those recovered tokens on the error via
    /// `LogWriteError::durable_tokens` (issue #296), and this variant appends
    /// them. This list is therefore exact for the common partial-flush case (a
    /// shard's flush abandoned or rejected while a sibling committed, all
    /// within a completed ack round). It remains a lower bound only when the
    /// ack round itself did not resolve: an ack-deadline timeout, or a shard's
    /// channel dying at send time, returns before any per-shard ack is
    /// observed, so no sibling token can be attributed even though one may have
    /// landed. See [`print_durable_tokens`].
    #[error("flush failed: {cause}")]
    Flush {
        durable: Vec<CommitToken>,
        cause: String,
    },
}

impl LoadError {
    /// The commit tokens already durable when this error occurred (empty for a
    /// setup error, since `Setup` never occurs once any batch could have
    /// flushed).
    pub fn durable_tokens(&self) -> &[CommitToken] {
        match self {
            LoadError::Setup(_) => &[],
            LoadError::BatchFailed { durable, .. }
            | LoadError::RowRejected { durable, .. }
            | LoadError::Flush { durable, .. } => durable,
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
/// tokens durable from batches that completed before the failure, plus any
/// shard of the failing batch that acked durable before a sibling shard failed
/// (recovered from the router error, issue #296) — a partial load, re-running
/// re-ingests the whole file, there is no resumability or dedup in this
/// version. On a [`LoadError::Flush`], the reported tokens are exact for a
/// partial flush where a sibling committed; they can still undercount only when
/// the failing batch's ack round did not resolve (a timeout, or a shard dying
/// at send time), where a commit can land with no observable ack (see the
/// variant's doc).
#[allow(clippy::too_many_arguments)]
pub async fn load(
    store: Arc<dyn ObjectStoreBackend>,
    parquet_path: &Path,
    tenant: &str,
    mapping: &Mapping,
    shards: u32,
    batch_rows: usize,
    read_cursors: Option<usize>,
    pipeline_depth: usize,
    now_ns: i64,
    clock: Arc<dyn Clock>,
) -> Result<LoadReport, LoadError> {
    load_instrumented(
        store,
        parquet_path,
        tenant,
        mapping,
        shards,
        batch_rows,
        read_cursors,
        pipeline_depth,
        now_ns,
        clock,
        LoadPath::Columnar,
        None,
    )
    .await
}

/// A test-only hook invoked at the start of each batch's decode/build. It lets a
/// test observe that batch N+1's decode/build begins while batch N's
/// `router.write` is still in flight (issue #541), which a purely
/// result-correct assertion cannot prove. `None` in production; the public
/// [`load`] always passes `None`.
type BuildStartHook = Arc<dyn Fn() + Send + Sync>;

/// [`load`] with the decode/build start hook injected. See [`load`] for the
/// contract; `on_build_start` fires once per batch that has data to build, on
/// the blocking decode/build task, before the per-row loop.
#[allow(clippy::too_many_arguments)]
async fn load_instrumented(
    store: Arc<dyn ObjectStoreBackend>,
    parquet_path: &Path,
    tenant: &str,
    mapping: &Mapping,
    shards: u32,
    batch_rows: usize,
    read_cursors: Option<usize>,
    pipeline_depth: usize,
    now_ns: i64,
    clock: Arc<dyn Clock>,
    path: LoadPath,
    on_build_start: Option<BuildStartHook>,
) -> Result<LoadReport, LoadError> {
    // Reject a zero batch size with a typed error rather than silently clamping
    // it to 1: `batch_rows` is the operator-facing `--batch-rows` lever, and a
    // silent clamp would hide a misconfigured value that changes object layout.
    if batch_rows == 0 {
        return Err(LoadError::Setup(
            "--batch-rows must be at least 1 (each batch is one Strict flush per shard); 0 was \
             given"
                .to_string(),
        ));
    }
    // Same shape as the `batch_rows == 0` guard above: `--pipeline-depth` is the
    // operator-facing lever bounding how many writes are in flight at once, so 0
    // (a pipeline that can hold no write) is a rejected value, not a silent
    // clamp to 1.
    if pipeline_depth == 0 {
        return Err(LoadError::Setup(
            "--pipeline-depth must be at least 1 (the number of concurrent in-flight writes); 0 \
             was given"
                .to_string(),
        ));
    }
    // Same shape as the `batch_rows == 0` guard above: `--read-cursors` is
    // operator-facing (issue #560), so 0 is a rejected value, not a silent
    // clamp to 1.
    if read_cursors == Some(0) {
        return Err(LoadError::Setup(
            "--read-cursors must be at least 1, or omitted for automatic sizing \
             (min(shard count, row-group count)); 0 was given"
                .to_string(),
        ));
    }

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
    // `Arc` so each batch's write can be `tokio::spawn`ed onto its own task and
    // run genuinely concurrently up to `pipeline_depth` (a constructed-but-
    // unawaited future does no I/O until polled; spawning is what makes the S3
    // PUT round trips overlap). `write`/`write_columnar` take `&self`, so this
    // is the only change the router's own type needs.
    let router = Arc::new(LogIngestRouter::new(
        IngestConfig {
            shard_count: shards,
            target_bytes: 1,
            ..IngestConfig::default()
        },
        Arc::clone(&store),
        clock,
    ));

    let row_group_lens = row_group_row_counts(parquet_path)?;
    let cursor_count = resolve_read_cursors(read_cursors, shards, row_group_lens.len());
    let cursors = open_stride_cursors(parquet_path, &row_group_lens, cursor_count, batch_rows)?;

    let started = Instant::now();
    let mut report = LoadReport::default();
    let mut shards_seen: std::collections::HashSet<u32> = std::collections::HashSet::new();
    let mut data_batches_flushed: u64 = 0;

    // The window of writes genuinely in flight, oldest (earliest-submitted)
    // first. Bounded to `pipeline_depth`: after a new write is spawned, if the
    // window is full the front (oldest) is popped and awaited before the next
    // batch's write starts. Popping strictly oldest-first is what preserves the
    // former loop's exact result ordering (same tokens, same first error) no
    // matter which underlying PUT actually completes first.
    let mut inflight: std::collections::VecDeque<(
        u64,
        tokio::task::JoinHandle<Result<LogWriteReceipt, LogWriteError>>,
    )> = std::collections::VecDeque::with_capacity(pipeline_depth);

    // Single-batch decode/build lookahead (issue #541), independent of the write
    // window above. Batch N+1's decode/build (sync Parquet decode via
    // `Iterator::next`, then the per-row `build_record` loop, both CPU-bound)
    // runs on a `spawn_blocking` task started *before* batch N's write I/O is
    // awaited, so N+1's CPU work overlaps the writes' I/O wait. The non-`Clone`
    // `ParquetRecordBatchReader` is shuttled into and back out of the closure
    // each iteration.
    //
    // Result ordering stays strict-FIFO regardless of `pipeline_depth`: writes
    // are recorded (and a write failure surfaced) only by consuming `inflight`
    // oldest-first, so `report.tokens` grows in submission order, and a build
    // error for a later batch is only reported after every earlier batch's write
    // has been drained from the window. At `pipeline_depth == 1` the window
    // holds at most one write, popped and awaited before the next batch's write
    // starts, so the observable results and ordering match the pre-widening loop
    // exactly (only the execution shape differs: the write now runs on a spawned
    // task rather than inline).
    let mapping = Arc::new(mapping.clone());
    let state = StrideCursors {
        cursors,
        deal_offset: 0,
    };

    // Spawn the decode/build of the next batch, moving the stride cursors in;
    // the task hands them back with the outcome.
    let spawn_build = |state: StrideCursors| {
        let mapping = Arc::clone(&mapping);
        let limits = limits.clone();
        let hook = on_build_start.clone();
        tokio::task::spawn_blocking(move || match path {
            LoadPath::Row => {
                decode_and_build_stride(state, mapping, limits, now_ns, batch_rows, hook.as_ref())
            }
            LoadPath::Columnar => decode_and_build_stride_columnar(
                state,
                mapping,
                limits,
                now_ns,
                batch_rows,
                hook.as_ref(),
            ),
        })
    };

    // Prime the pipeline with batch 0.
    let mut pending = spawn_build(state);

    loop {
        let (state, built) = match pending.await {
            Ok(pair) => pair,
            // A panic in the decode/build task is a batch decode failure;
            // earlier batches' writes may still be in flight, so drain them
            // first (oldest-first) so any already-durable earlier batch is
            // reported and any earlier write failure surfaces ahead of this one,
            // exactly as the former serial loop ordered them.
            Err(join_err) => {
                drain_inflight(
                    &mut inflight,
                    &mut report,
                    &mut shards_seen,
                    &mut data_batches_flushed,
                    shards,
                )
                .await?;
                return Err(LoadError::BatchFailed {
                    reason: format!("Parquet decode/build task failed: {join_err}"),
                    durable: report.tokens.clone(),
                });
            }
        };

        let built = match built {
            Prefetched::Done => break,
            Prefetched::BatchFailed { reason } => {
                drain_inflight(
                    &mut inflight,
                    &mut report,
                    &mut shards_seen,
                    &mut data_batches_flushed,
                    shards,
                )
                .await?;
                return Err(LoadError::BatchFailed {
                    reason,
                    durable: report.tokens.clone(),
                });
            }
            Prefetched::RowRejected { row, reason } => {
                drain_inflight(
                    &mut inflight,
                    &mut report,
                    &mut shards_seen,
                    &mut data_batches_flushed,
                    shards,
                )
                .await?;
                return Err(LoadError::RowRejected {
                    row,
                    reason,
                    durable: report.tokens.clone(),
                });
            }
            Prefetched::Batch(built) => built,
        };

        let n = built.num_rows() as u64;
        if n == 0 {
            // A zero-row batch writes nothing; advance and prefetch the next.
            pending = spawn_build(state);
            continue;
        }

        // Spawn this batch's write onto its own task so it runs concurrently
        // with the writes already in flight and with the next batch's
        // decode/build, then prefetch that next batch. A `tokio::spawn`ed write
        // begins executing immediately; a merely-constructed future would do no
        // I/O until polled, which is why the window is built from join handles,
        // not from unpolled futures.
        let handle = match built {
            Built::Row(records) => {
                let router = Arc::clone(&router);
                let tenant = tenant_id.clone();
                tokio::spawn(async move {
                    router
                        .write(tenant, records, WriteMode::Strict, WRITE_ACK_DEADLINE)
                        .await
                })
            }
            Built::Columnar(batch) => {
                // Reachability signal (ADR-0109): count each batch actually
                // driven through `write_columnar`, so a caller of the real entry
                // point can prove the columnar path ran.
                report.columnar_batches_built += 1;
                let router = Arc::clone(&router);
                let tenant = tenant_id.clone();
                tokio::spawn(async move {
                    router
                        .write_columnar(tenant, *batch, WriteMode::Strict, WRITE_ACK_DEADLINE)
                        .await
                })
            }
        };
        inflight.push_back((n, handle));
        pending = spawn_build(state);

        // Bound true concurrency to `pipeline_depth`: once the window is full,
        // resolve the oldest write before starting the next batch's write. This
        // is the only place `report.tokens` grows during the loop, and it grows
        // strictly oldest-first, so a later batch's write finishing early can
        // never record its token ahead of an earlier one (or ahead of an earlier
        // failure). On a write error, abort every still-queued write's
        // `JoinHandle`: this cancels the loader's own ack wait so it does not
        // block returning on a write it has already decided to fail, but it
        // does NOT stop the batch's underlying shard-actor flush (see
        // `LogIngestRouter::write` in crates/ravel-ingest/src/log_router.rs --
        // the actor holds only a channel `tx`, no join handle of its own). A
        // later batch can therefore still commit its data object and publish
        // its commit record in the background after the loader has returned an
        // error; the durable-token accounting excludes it regardless (see
        // `resolve_write_entry`/`drain_inflight` below), so the reported list
        // stays correct, but the object itself may still land. Tracked as a
        // known gap pending a `ravel-ingest` cancellation mechanism.
        // Only one write is spawned per iteration, so the window exceeds its
        // bound by at most one and this resolves exactly one oldest entry; the
        // `while` + `let`-else form avoids unwrapping a `pop_front` that is
        // always `Some` here (the bound is >= 1).
        while inflight.len() >= pipeline_depth {
            let Some(entry) = inflight.pop_front() else {
                break;
            };
            if let Err(e) = resolve_write_entry(
                entry,
                &mut report,
                &mut shards_seen,
                &mut data_batches_flushed,
                shards,
            )
            .await
            {
                for (_, handle) in inflight.drain(..) {
                    handle.abort();
                }
                return Err(e);
            }
        }
    }

    // The stream is exhausted; drain every write still in the window in the same
    // oldest-first order before reporting success.
    drain_inflight(
        &mut inflight,
        &mut report,
        &mut shards_seen,
        &mut data_batches_flushed,
        shards,
    )
    .await?;

    // Strict acks already guarantee durability; flush_all is a defensive
    // no-op here (nothing is buffered after a Strict write returns).
    router.flush_all().await;

    // Snapshot the router's cumulative counters before it drops: the caller
    // reads the dynamic-column figures to warn on overflow or near-cap pressure
    // (ADR-0100 decision 1).
    report.metrics = router.metrics().snapshot();
    #[cfg(feature = "stage-timing")]
    {
        report.stage_timings = router.stage_timings().snapshot();
    }
    report.elapsed = started.elapsed();
    Ok(report)
}

/// Await one popped in-flight write and fold its outcome into the report, or
/// turn its failure into a [`LoadError::Flush`]. Entries are always resolved
/// oldest-first, so `report.tokens` (and the skew-warning checkpoint) advance in
/// strict submission order: this is the only place the loop records a write's
/// tokens.
///
/// On the write's own error, `durable` is `report.tokens` as it stands at this
/// point (every batch strictly before this one, already recorded oldest-first)
/// plus this batch's own durably-acked shards recovered from
/// `LogWriteError::durable_tokens` (issue #296, a multi-shard write can
/// partially succeed). A `JoinError` (the spawned write task panicked or was
/// aborted) is itself a flush failure: it maps to [`LoadError::Flush`] with the
/// tokens durable up to this point, never to a batch-build error.
async fn resolve_write_entry(
    entry: (
        u64,
        tokio::task::JoinHandle<Result<LogWriteReceipt, LogWriteError>>,
    ),
    report: &mut LoadReport,
    shards_seen: &mut std::collections::HashSet<u32>,
    data_batches_flushed: &mut u64,
    shards: u32,
) -> Result<(), LoadError> {
    let (n, handle) = entry;
    let receipt = match handle.await {
        Ok(Ok(receipt)) => receipt,
        Ok(Err(e)) => {
            let mut durable = report.tokens.clone();
            durable.extend_from_slice(e.durable_tokens());
            return Err(LoadError::Flush {
                durable,
                cause: e.to_string(),
            });
        }
        Err(join_err) => {
            return Err(LoadError::Flush {
                durable: report.tokens.clone(),
                cause: format!("write task failed: {join_err}"),
            });
        }
    };
    report.rows_processed += n;
    shards_seen.extend(receipt.tokens.iter().map(|t| t.shard));
    report.tokens.extend(receipt.tokens);

    *data_batches_flushed += 1;
    if *data_batches_flushed == SKEW_CHECK_AFTER_BATCHES && report.skew_warning.is_none() {
        report.skew_warning = shard_skew_warning(shards_seen.len(), shards);
    }
    Ok(())
}

/// Resolve every write still in the window, oldest-first. On the first write
/// error it aborts every remaining (later) write's `JoinHandle` before
/// returning. This only cancels the loader's own ack wait on those writes --
/// it does not stop their underlying shard-actor flush, which has no join
/// handle of its own to cancel (see the loop comment above and
/// `crates/ravel-ingest/src/log_router.rs`) -- so a later batch can still
/// commit independently in the background even after the loader has reported
/// failure. The durable-token list is unaffected either way: it only ever
/// grows from a popped, successfully-resolved entry strictly oldest-first, so
/// an aborted entry's outcome (commit or not) is never consulted.
async fn drain_inflight(
    inflight: &mut std::collections::VecDeque<(
        u64,
        tokio::task::JoinHandle<Result<LogWriteReceipt, LogWriteError>>,
    )>,
    report: &mut LoadReport,
    shards_seen: &mut std::collections::HashSet<u32>,
    data_batches_flushed: &mut u64,
    shards: u32,
) -> Result<(), LoadError> {
    while let Some(entry) = inflight.pop_front() {
        if let Err(e) =
            resolve_write_entry(entry, report, shards_seen, data_batches_flushed, shards).await
        {
            for (_, handle) in inflight.drain(..) {
                handle.abort();
            }
            return Err(e);
        }
    }
    Ok(())
}

/// The Parquet reader shuttled through each stride cursor's decode/build task.
/// It owns file-reading state and is not `Clone`.
type BatchReader = parquet::arrow::arrow_reader::ParquetRecordBatchReader;

/// One prefetched batch's decode/build outcome. Errors carry only the reason
/// (and, for a rejected row, its absolute index): the `durable` token list is
/// attached by the loop when it *consumes* the outcome, after every earlier
/// batch's write has resolved, so the reported tokens are exactly those durable
/// from batches strictly before the failure regardless of when (wall-clock) the
/// decode ran.
enum Prefetched {
    /// Every stride cursor is exhausted; no batch was produced.
    Done,
    /// A batch decoded and built, assembled from up to K contiguous spans (one
    /// per live stride cursor, issue #560). Carries either the row-major records
    /// (the differential-reference path) or the columnar batch (the fast path
    /// `load` drives, ADR-0109). Every non-rejected row yields one record/one
    /// batch row (a rejection returns `RowRejected` instead of a partial batch),
    /// so the payload's own length is the source row count.
    Batch(Built),
    /// The batch failed to read from Parquet or to resolve against the mapping.
    BatchFailed { reason: String },
    /// A row failed a kept admission check. `row` is the FILE-absolute row
    /// index, translated from whichever cursor's span produced it.
    RowRejected { row: u64, reason: String },
}

/// The built form of one prefetched batch, selected by [`LoadPath`]. The row
/// form is kept as the differential reference (ADR-0109 decision 7); `load`
/// drives the columnar form through `write_columnar`.
enum Built {
    Row(Vec<NormalizedLogRecord>),
    Columnar(Box<ColumnarLogBatch>),
}

impl Built {
    /// Row count of the built batch, the source row count for reporting.
    fn num_rows(&self) -> usize {
        match self {
            Built::Row(records) => records.len(),
            Built::Columnar(batch) => batch.num_rows,
        }
    }
}

/// Which build/write path a load drives. `load` uses [`LoadPath::Columnar`]
/// (ADR-0109 decision 4); [`LoadPath::Row`] stays reachable so the byte-identity
/// differential test can run the same file through the pre-ADR row path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LoadPath {
    /// The differential-reference path, constructed only by the byte-identity
    /// test; `load` never selects it.
    #[cfg_attr(not(test), allow(dead_code))]
    Row,
    Columnar,
}

/// One of the K stride cursors' state (issue #560): its own file-backed
/// Parquet reader, restricted to a contiguous partition of row groups, plus
/// whatever Arrow batch it has pulled from `reader.next()` but not yet fully
/// handed out via [`cursor_take`]. Not `Clone`; shuttled into and back out of
/// the decode/build `spawn_blocking` task each iteration, same as the single
/// reader before it.
struct CursorState {
    /// `None` once the underlying reader is exhausted.
    reader: Option<BatchReader>,
    /// Rows pulled from `reader.next()` not yet fully consumed.
    buffered: Option<RecordBatch>,
    /// File-absolute row index of this cursor's partition's first row.
    partition_base: u64,
    /// Rows already handed out from this partition, so
    /// `partition_base + consumed` is the file-absolute index of the next row
    /// this cursor will yield.
    consumed: u64,
}

impl CursorState {
    /// This cursor has no more rows to give, now or in a future round.
    fn is_exhausted(&self) -> bool {
        self.reader.is_none() && self.buffered.as_ref().is_none_or(|b| b.num_rows() == 0)
    }
}

/// The K stride cursors' shared state, threaded through the decode/build
/// `spawn_blocking` task each iteration (issue #560): the K-cursor
/// generalization of the single [`BatchReader`] the pre-#560 loop shuttled.
struct StrideCursors {
    cursors: Vec<CursorState>,
    /// Rotates which live cursors receive the remainder share each round, so
    /// a live-cursor count that outnumbers `batch_rows` (an unusual but valid
    /// configuration) does not starve any cursor forever: see
    /// [`decode_and_build_stride`].
    deal_offset: usize,
}

/// Drain up to `want` contiguous rows from `cur`, pulling fresh Arrow batches
/// from its reader as needed. Returns `Ok(None)` once the cursor has no more
/// rows at all. The returned batch's row 0 is at the returned file-absolute
/// index. [`RecordBatch::slice`] is a zero-copy view, so a cursor whose
/// buffered batch is consumed in one call (the common case, including every
/// K=1 call once `with_batch_size` bounds a batch at `want`) is handed back
/// unsliced.
fn cursor_take(cur: &mut CursorState, want: usize) -> Result<Option<(RecordBatch, u64)>, String> {
    let buf = loop {
        match cur.buffered.take() {
            Some(buf) if buf.num_rows() > 0 => break buf,
            Some(_) | None => {}
        }
        let Some(reader) = cur.reader.as_mut() else {
            return Ok(None);
        };
        match reader.next() {
            None => {
                cur.reader = None;
                return Ok(None);
            }
            Some(Ok(batch)) => cur.buffered = Some(batch),
            Some(Err(e)) => return Err(format!("failed to read Parquet batch: {e}")),
        }
    };
    let total = buf.num_rows();
    let take_n = want.min(total);
    let file_base = cur.partition_base + cur.consumed;
    cur.consumed += take_n as u64;
    if take_n == total {
        Ok(Some((buf, file_base)))
    } else {
        let out = buf.slice(0, take_n);
        cur.buffered = Some(buf.slice(take_n, total - take_n));
        Ok(Some((out, file_base)))
    }
}

/// The spans making up one batch, or a terminal signal. Shared by the row and
/// columnar decode paths so the K-cursor share dealing (issue #560) lives in
/// exactly one place.
enum SpanOutcome {
    /// Every stride cursor is exhausted; no batch this round.
    Done,
    /// A cursor's Parquet read failed.
    Failed(String),
    /// Up to K contiguous spans, each with its own file-absolute base row. May
    /// be empty (every dealt share came back empty), which the caller turns
    /// into a zero-row batch.
    Spans(Vec<(RecordBatch, u64)>),
}

/// Deal one batch's worth of rows across the live stride cursors (issue #560).
/// Each live cursor contributes up to its share as one contiguous run via
/// [`cursor_take`]; the resulting spans keep their own `file_base` so a rejected
/// row's reported index translates to its FILE-absolute position regardless of
/// which cursor produced it.
///
/// Share sizing: `batch_rows` split evenly across the currently live cursor
/// count, with the remainder distributed via a rotating window (`deal_offset`)
/// rather than always to the same cursors. The denominator (live cursor count)
/// shrinks as cursors exhaust, so each remaining cursor's share grows on its own
/// with no separate redistribution step, and the rotation guarantees that even a
/// pathological live-cursor count greater than `batch_rows` (some cursors get a
/// zero share this round) eventually asks every live cursor for rows.
///
/// `on_build_start` fires once, before any row is built, whenever there is at
/// least one live cursor (matching the pre-#560 hook timing of firing whenever
/// `reader.next()` would yield `Some(_)`, regardless of whether that attempt
/// goes on to decode or resolve cleanly).
fn collect_spans(
    state: &mut StrideCursors,
    batch_rows: usize,
    on_build_start: Option<&BuildStartHook>,
) -> SpanOutcome {
    let live: Vec<usize> = state
        .cursors
        .iter()
        .enumerate()
        .filter(|(_, c)| !c.is_exhausted())
        .map(|(i, _)| i)
        .collect();
    if live.is_empty() {
        return SpanOutcome::Done;
    }
    if let Some(hook) = on_build_start {
        hook();
    }

    let l = live.len();
    let base = batch_rows / l;
    let extra = batch_rows % l;

    let mut spans: Vec<(RecordBatch, u64)> = Vec::with_capacity(l);
    for (j, &idx) in live.iter().enumerate() {
        let bonus = usize::from((j + state.deal_offset) % l < extra);
        let share = base + bonus;
        if share == 0 {
            continue;
        }
        match cursor_take(&mut state.cursors[idx], share) {
            Ok(Some((batch, file_base))) if batch.num_rows() > 0 => spans.push((batch, file_base)),
            Ok(_) => {}
            Err(reason) => return SpanOutcome::Failed(reason),
        }
    }
    state.deal_offset = (state.deal_offset + extra) % l;
    SpanOutcome::Spans(spans)
}

/// Assemble one batch's spans and build its records row by row (the
/// differential-reference path, [`LoadPath::Row`]).
fn decode_and_build_stride(
    mut state: StrideCursors,
    mapping: Arc<Mapping>,
    limits: LogIngestLimits,
    now_ns: i64,
    batch_rows: usize,
    on_build_start: Option<&BuildStartHook>,
) -> (StrideCursors, Prefetched) {
    let spans = match collect_spans(&mut state, batch_rows, on_build_start) {
        SpanOutcome::Done => return (state, Prefetched::Done),
        SpanOutcome::Failed(reason) => return (state, Prefetched::BatchFailed { reason }),
        SpanOutcome::Spans(spans) => spans,
    };

    let total_rows: usize = spans.iter().map(|(b, _)| b.num_rows()).sum();
    let mut records = Vec::with_capacity(total_rows);
    for (batch, file_base) in &spans {
        // Resolve column indices once per span (schema is stable across
        // spans and batches, but re-resolving keeps this self-contained and
        // cheap).
        let cols = match ColumnIndex::resolve(batch, &mapping) {
            Ok(cols) => cols,
            Err(reason) => return (state, Prefetched::BatchFailed { reason }),
        };
        for row in 0..batch.num_rows() {
            match build_record(batch, &cols, &mapping, &limits, now_ns, row) {
                Ok(record) => records.push(record),
                Err(reason) => {
                    return (
                        state,
                        Prefetched::RowRejected {
                            row: file_base + row as u64,
                            reason,
                        },
                    );
                }
            }
        }
    }
    (state, Prefetched::Batch(Built::Row(records)))
}

/// Assemble one batch's spans and build a [`ColumnarLogBatch`] directly, column
/// by column, without materializing a per-row struct (ADR-0109 decision 1, the
/// path [`load`] drives). Byte-identical output to [`decode_and_build_stride`]
/// on the same spans is the acceptance anchor (decision 7).
fn decode_and_build_stride_columnar(
    mut state: StrideCursors,
    mapping: Arc<Mapping>,
    limits: LogIngestLimits,
    now_ns: i64,
    batch_rows: usize,
    on_build_start: Option<&BuildStartHook>,
) -> (StrideCursors, Prefetched) {
    let spans = match collect_spans(&mut state, batch_rows, on_build_start) {
        SpanOutcome::Done => return (state, Prefetched::Done),
        SpanOutcome::Failed(reason) => return (state, Prefetched::BatchFailed { reason }),
        SpanOutcome::Spans(spans) => spans,
    };

    match build_columnar_batch(&spans, &mapping, &limits, now_ns) {
        Ok(batch) => (state, Prefetched::Batch(Built::Columnar(Box::new(batch)))),
        Err(ColBuildError::Batch(reason)) => (state, Prefetched::BatchFailed { reason }),
        Err(ColBuildError::Row { row, reason }) => (state, Prefetched::RowRejected { row, reason }),
    }
}

/// The early shard-skew warning checkpoint (issue #560): the number of data
/// batches flushed at which [`shard_skew_warning`] is evaluated, once.
const SKEW_CHECK_AFTER_BATCHES: u64 = 8;

/// The shard-skew warning threshold denominator (issue #560): the warning
/// fires when the distinct shard count seen so far is at or below
/// `shards / SKEW_WARN_DENOMINATOR`.
const SKEW_WARN_DENOMINATOR: u32 = 4;

/// Build the early shard-skew warning message, or `None` if the observed
/// spread does not cross the threshold (or `shards < 2`, where "skew" is not
/// a meaningful idea). Called once, at the [`SKEW_CHECK_AFTER_BATCHES`]
/// checkpoint.
fn shard_skew_warning(distinct_shards: usize, shards: u32) -> Option<String> {
    if shards < 2 {
        return None;
    }
    let threshold = shards / SKEW_WARN_DENOMINATOR;
    if distinct_shards as u32 > threshold {
        return None;
    }
    Some(format!(
        "warning: after the first {SKEW_CHECK_AFTER_BATCHES} data batches, only \
         {distinct_shards} of {shards} shards have received data (shard spread is at or below \
         shards / {SKEW_WARN_DENOMINATOR} = {threshold}). This usually means input rows are \
         arriving grouped by resource-attribute value, e.g. an entity-sorted bulk export \
         (ClickBench's hits.parquet, sorted by CounterID, is exactly this shape). \
         --read-cursors stride-reads the file so each batch draws rows from multiple far-apart \
         file regions instead of one contiguous run; the mapping's [[resource_attribute]] \
         choice is the other lever, since it determines what shard_for_log hashes on."
    ))
}

/// Read each row group's row count from `parquet_path`'s footer, in row-group
/// order, without decoding any data. Used to size and partition the stride
/// cursors (issue #560) before any reader is opened.
fn row_group_row_counts(parquet_path: &Path) -> Result<Vec<u64>, LoadError> {
    let file = std::fs::File::open(parquet_path)
        .map_err(|e| LoadError::Setup(format!("failed to open {}: {e}", parquet_path.display())))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| LoadError::Setup(format!("failed to open Parquet reader: {e}")))?;
    Ok(builder
        .metadata()
        .row_groups()
        .iter()
        .map(|rg| rg.num_rows() as u64)
        .collect())
}

/// Resolve `--read-cursors` (issue #560): absent means auto-sized to
/// `min(shard count, row-group count)`, floored at 1; an explicit value is
/// clamped to `[1, row_group_count.max(1)]` (more cursors than row groups
/// cannot each get a distinct contiguous partition). Zero is rejected by the
/// caller before this is reached, never clamped up silently.
fn resolve_read_cursors(read_cursors: Option<usize>, shards: u32, row_group_count: usize) -> usize {
    let max_cursors = row_group_count.max(1);
    match read_cursors {
        Some(k) => k.clamp(1, max_cursors),
        None => (shards as usize).min(row_group_count).max(1),
    }
}

/// Split `n` row groups into `k` contiguous, near-even ranges (the first
/// `n % k` ranges get one extra row group). Used to give each stride cursor
/// its own disjoint partition of row groups.
fn partition_row_group_ranges(n: usize, k: usize) -> Vec<std::ops::Range<usize>> {
    let base = n / k;
    let extra = n % k;
    let mut ranges = Vec::with_capacity(k);
    let mut start = 0;
    for i in 0..k {
        let len = base + usize::from(i < extra);
        ranges.push(start..start + len);
        start += len;
    }
    ranges
}

/// Open one [`BatchReader`] per stride cursor (issue #560), each restricted to
/// its own contiguous partition of `parquet_path`'s row groups, with
/// `partition_base` set to that partition's first row's file-absolute index.
/// An empty partition (only possible when `row_group_lens` is empty, the
/// degenerate zero-row-group case, which forces `k == 1`) yields an
/// already-exhausted cursor with no reader opened, rather than asking Parquet
/// to build a reader over zero row groups.
fn open_stride_cursors(
    parquet_path: &Path,
    row_group_lens: &[u64],
    k: usize,
    batch_rows: usize,
) -> Result<Vec<CursorState>, LoadError> {
    let mut group_file_base = Vec::with_capacity(row_group_lens.len());
    let mut running = 0u64;
    for &len in row_group_lens {
        group_file_base.push(running);
        running += len;
    }

    let mut cursors = Vec::with_capacity(k);
    for range in partition_row_group_ranges(row_group_lens.len(), k) {
        if range.is_empty() {
            cursors.push(CursorState {
                reader: None,
                buffered: None,
                partition_base: running,
                consumed: 0,
            });
            continue;
        }
        let partition_base = group_file_base[range.start];
        let file = std::fs::File::open(parquet_path).map_err(|e| {
            LoadError::Setup(format!("failed to open {}: {e}", parquet_path.display()))
        })?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| LoadError::Setup(format!("failed to open Parquet reader: {e}")))?;
        let reader = builder
            .with_row_groups(range.collect())
            .with_batch_size(batch_rows)
            .build()
            .map_err(|e| LoadError::Setup(format!("failed to build Parquet reader: {e}")))?;
        cursors.push(CursorState {
            reader: Some(reader),
            buffered: None,
            partition_base,
            consumed: 0,
        });
    }
    Ok(cursors)
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
        // A dictionary-encoded string column (Arrow reconstructs one from a
        // Parquet file that carries Arrow dictionary schema metadata): resolve
        // the row's key to its value and read that. The columnar fast path
        // passes such a column through as a `StrColumnDict`; the row path here,
        // its differential reference, must read the same values.
        DataType::Dictionary(_, _) => {
            let dict = arr.as_any_dictionary();
            let key = dict.normalized_keys()[row];
            read_string(dict.values(), key)
        }
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
        // Dictionary-encoded binary column: resolve the key to its value, as in
        // [`read_string`].
        DataType::Dictionary(_, _) => {
            let dict = arr.as_any_dictionary();
            let key = dict.normalized_keys()[row];
            read_bytes(dict.values(), key)
        }
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

/// [`downcast`] as an `Option`, for the prepared column readers below: the
/// datatype has already been matched, so a `None` here is an internal
/// inconsistency the reader turns into a deferred error rather than a panic.
fn downcast_opt<A: 'static>(arr: &ArrayRef) -> Option<&A> {
    arr.as_any().downcast_ref::<A>()
}

/// The [`FieldType`] a mapped scalar column resolves to. Fixed by the mapping's
/// declared [`ColType`], so it is resolved once per column, not per cell
/// (ADR-0109 decision 6).
fn field_type_of(ty: ColType) -> FieldType {
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
enum IntSrc<'a> {
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

fn int_src(arr: &ArrayRef) -> IntSrc<'_> {
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
    fn get(&self, row: usize) -> Result<Option<i64>, String> {
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
enum FloatSrc<'a> {
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
enum BoolSrc<'a> {
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
/// embeds Arrow dictionary schema metadata); its presence is what the columnar
/// builder keys the `StrColumnDict` fast path on (ADR-0109 decision 3).
enum StrSrc<'a> {
    Utf8(&'a StringArray),
    LargeUtf8(&'a LargeStringArray),
    Dict {
        arr: &'a ArrayRef,
        values: &'a ArrayRef,
        keys: Vec<usize>,
    },
    Bad(&'a ArrayRef),
}

fn str_src(arr: &ArrayRef) -> StrSrc<'_> {
    match arr.data_type() {
        DataType::Utf8 => downcast_opt(arr).map_or(StrSrc::Bad(arr), StrSrc::Utf8),
        DataType::LargeUtf8 => downcast_opt(arr).map_or(StrSrc::Bad(arr), StrSrc::LargeUtf8),
        DataType::Dictionary(_, value_ty)
            if matches!(**value_ty, DataType::Utf8 | DataType::LargeUtf8) =>
        {
            let dict = arr.as_any_dictionary();
            StrSrc::Dict {
                arr,
                values: dict.values(),
                keys: dict.normalized_keys(),
            }
        }
        _ => StrSrc::Bad(arr),
    }
}

impl StrSrc<'_> {
    fn get(&self, row: usize) -> Result<Option<String>, String> {
        match self {
            StrSrc::Utf8(a) => Ok((!a.is_null(row)).then(|| a.value(row).to_string())),
            StrSrc::LargeUtf8(a) => Ok((!a.is_null(row)).then(|| a.value(row).to_string())),
            StrSrc::Dict { arr, values, keys } => {
                if arr.is_null(row) {
                    return Ok(None);
                }
                read_string(values, keys[row])
            }
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

    fn is_dict(&self) -> bool {
        matches!(self, StrSrc::Dict { .. })
    }
}

/// A binary source resolved to a concrete Arrow array. `Dict` is the binary
/// analogue of [`StrSrc::Dict`].
enum BytesSrc<'a> {
    Bin(&'a BinaryArray),
    LargeBin(&'a LargeBinaryArray),
    FixedBin(&'a FixedSizeBinaryArray),
    Dict {
        arr: &'a ArrayRef,
        values: &'a ArrayRef,
        keys: Vec<usize>,
    },
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
            BytesSrc::Dict {
                arr,
                values: dict.values(),
                keys: dict.normalized_keys(),
            }
        }
        _ => BytesSrc::Bad(arr),
    }
}

impl BytesSrc<'_> {
    fn get(&self, row: usize) -> Result<Option<Vec<u8>>, String> {
        match self {
            BytesSrc::Bin(a) => Ok((!a.is_null(row)).then(|| a.value(row).to_vec())),
            BytesSrc::LargeBin(a) => Ok((!a.is_null(row)).then(|| a.value(row).to_vec())),
            BytesSrc::FixedBin(a) => Ok((!a.is_null(row)).then(|| a.value(row).to_vec())),
            BytesSrc::Dict { arr, values, keys } => {
                if arr.is_null(row) {
                    return Ok(None);
                }
                read_bytes(values, keys[row])
            }
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

    fn is_dict(&self) -> bool {
        matches!(self, BytesSrc::Dict { .. })
    }
}

/// A `ts` source with its unit scaling resolved once (ADR-0109 decision 6). A
/// native Arrow `Timestamp` scales by its own unit; an integer column scales by
/// the mapping's declared unit; a date column is rejected as an invalid ts
/// source, exactly as [`read_ts`].
enum TsSrc<'a> {
    Sec(&'a TimestampSecondArray),
    Milli(&'a TimestampMillisecondArray),
    Micro(&'a TimestampMicrosecondArray),
    Nano(&'a TimestampNanosecondArray),
    Int { src: IntSrc<'a>, factor: i64 },
    DateErr(&'a ArrayRef),
}

fn ts_src(arr: &ArrayRef, declared: TsUnit) -> TsSrc<'_> {
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
    fn get(&self, row: usize) -> Result<Option<i64>, String> {
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
enum IdSrc<'a> {
    Hex(&'a ArrayRef),
    Bin(&'a ArrayRef),
    Bad(&'a ArrayRef),
}

fn id_src(arr: &ArrayRef) -> IdSrc<'_> {
    match arr.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 => IdSrc::Hex(arr),
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => IdSrc::Bin(arr),
        _ => IdSrc::Bad(arr),
    }
}

impl IdSrc<'_> {
    fn get(&self, row: usize) -> Result<Option<Vec<u8>>, String> {
        match self {
            IdSrc::Hex(arr) => Ok(read_string(arr, row)?.and_then(|s| hex::decode(s).ok())),
            IdSrc::Bin(arr) => read_bytes(arr, row),
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

/// One mapped scalar attribute column's source, resolved once to its declared
/// [`ColType`]. Yields the typed [`AttrValue`] per present cell and exposes
/// whether the Arrow column arrived dictionary-encoded (for the `StrColumnDict`
/// fast path).
enum AttrSrc<'a> {
    Int(IntSrc<'a>),
    Float(FloatSrc<'a>),
    Bool(BoolSrc<'a>),
    Str(StrSrc<'a>),
    Bytes(BytesSrc<'a>),
}

fn attr_src(arr: &ArrayRef, ty: ColType) -> AttrSrc<'_> {
    match ty {
        ColType::Str => AttrSrc::Str(str_src(arr)),
        ColType::I64 => AttrSrc::Int(int_src(arr)),
        ColType::F64 => AttrSrc::Float(float_src(arr)),
        ColType::Bool => AttrSrc::Bool(bool_src(arr)),
        ColType::Bytes => AttrSrc::Bytes(bytes_src(arr)),
    }
}

impl AttrSrc<'_> {
    fn get(&self, row: usize) -> Result<Option<AttrValue>, String> {
        Ok(match self {
            AttrSrc::Int(s) => s.get(row)?.map(AttrValue::I64),
            AttrSrc::Float(s) => s.get(row)?.map(AttrValue::F64),
            AttrSrc::Bool(s) => s.get(row)?.map(AttrValue::Bool),
            AttrSrc::Str(s) => s.get(row)?.map(AttrValue::Str),
            AttrSrc::Bytes(s) => s.get(row)?.map(AttrValue::Bytes),
        })
    }

    fn is_dict(&self) -> bool {
        match self {
            AttrSrc::Str(s) => s.is_dict(),
            AttrSrc::Bytes(s) => s.is_dict(),
            _ => false,
        }
    }
}

/// A columnar-build failure: a batch-level decode/resolve error, or a per-row
/// admission rejection carrying its FILE-absolute index (#541).
enum ColBuildError {
    Batch(String),
    Row { row: u64, reason: String },
}

/// The `StrColumnDict` for one Str/Bytes dynamic column, interned from its final
/// dense cells (ADR-0109 decision 3). Distinct values are first-seen order; the
/// writer sorts them to match `encode_strings`, so ordering here is free. The
/// bytes are `resolve_value(cell).1` for a Str/Bytes value: the string/byte
/// payload verbatim, so the writer's dict path re-interns to exactly the same
/// per-object dictionary the plain path derives, and the object bytes match.
fn str_column_dict_from_cells(cells: &[AttrValue]) -> StrColumnDict {
    let mut interner: std::collections::HashMap<Vec<u8>, u32> = std::collections::HashMap::new();
    let mut distinct: Vec<Vec<u8>> = Vec::new();
    let mut ids: Vec<u32> = Vec::with_capacity(cells.len());
    for cell in cells {
        let bytes = match cell {
            AttrValue::Str(s) => s.as_bytes().to_vec(),
            AttrValue::Bytes(b) => b.clone(),
            // A Str/Bytes column holds only Str/Bytes values; anything else is a
            // mis-typed column that never reaches here.
            _ => Vec::new(),
        };
        let next = distinct.len() as u32;
        let id = *interner.entry(bytes.clone()).or_insert_with(|| {
            distinct.push(bytes);
            next
        });
        ids.push(id);
    }
    StrColumnDict { distinct, ids }
}

/// Build a [`ColumnarLogBatch`] directly from a batch's Arrow spans and the
/// mapping (ADR-0109 decisions 1, 3, 6). Every downcast and the `ts` unit
/// scaling are resolved once per column per span; stream identity is hashed once
/// per distinct resource tuple; a mapped Str/Bytes column that arrived
/// dictionary-encoded is carried as a `StrColumnDict` so the writer pays string
/// encoding and token bloom per distinct value, not per row.
///
/// The result is byte-identical, once written, to
/// [`ColumnarLogBatch::from_records`] over the [`NormalizedLogRecord`]s
/// [`build_record`] would produce for the same spans (decision 7): the dynamic
/// columns keep the same `(name, type)`-sorted order and first-occurrence
/// winner/residual split, and the stream directory is the same id-ascending
/// dense form. Admission rejections match `build_record`'s per-row check order
/// and report the first failing row's FILE-absolute index.
fn build_columnar_batch(
    spans: &[(RecordBatch, u64)],
    mapping: &Mapping,
    limits: &LogIngestLimits,
    now_ns: i64,
) -> Result<ColumnarLogBatch, ColBuildError> {
    use std::collections::{BTreeMap, HashMap, HashSet};

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

    // Dynamic columns, keyed by (name, type byte) as `from_records` keys them, so
    // their materialized order matches. `col_dict` tracks whether every winning
    // cell of a column came from a dictionary-encoded Arrow source.
    let mut col_cells: BTreeMap<(String, u8), Vec<Option<AttrValue>>> = BTreeMap::new();
    let mut col_dict: BTreeMap<(String, u8), bool> = BTreeMap::new();

    // Stream identity: hashed once per distinct resource tuple, keyed by the
    // STREAM_DIR blob (the canonical resource bytes) so the blake3 in
    // `log_stream_id` runs once per distinct tuple rather than once per row
    // (ADR-0109 decision 6). `stream_dir` is the id-ascending directory.
    let mut row_stream_id: Vec<LogStreamId> = Vec::with_capacity(total_rows);
    let mut stream_dir: BTreeMap<LogStreamId, Vec<u8>> = BTreeMap::new();
    let mut stream_cache: HashMap<Vec<u8>, LogStreamId> = HashMap::new();

    let mut grow = 0usize;
    for (span, file_base) in spans {
        let cols = ColumnIndex::resolve(span, mapping).map_err(ColBuildError::Batch)?;

        // Prepare every reader once per span (downcast resolved here, not per
        // cell).
        let ts = ts_src(span.column(cols.ts), mapping.ts_unit);
        let body = cols.body.map(|i| str_src(span.column(i)));
        let sev_num = cols.severity_number.map(|i| int_src(span.column(i)));
        let sev_text = cols.severity_text.map(|i| str_src(span.column(i)));
        let trace = cols.trace_id.map(|i| id_src(span.column(i)));
        let span_id_src = cols.span_id.map(|i| id_src(span.column(i)));
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
        let record: Vec<(usize, AttrSrc)> = cols
            .record
            .iter()
            .map(|(ci, mi)| {
                (
                    *mi,
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

            // 1. ts (required) and 2. future-skew bound, in build_record order.
            let raw_ts = match ts.get(local).map_err(row_err)? {
                Some(t) => t,
                None => {
                    return Err(row_err(format!(
                        "ts column {:?} is null",
                        mapping.ts_column
                    )));
                }
            };
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
                Some(s) => s.get(local).map_err(row_err)?.unwrap_or_default(),
                None => String::new(),
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
                Some(s) => s.get(local).map_err(row_err)?.unwrap_or_default(),
                None => String::new(),
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
            let mut resource_attrs: Vec<(String, AttrValue)> = Vec::with_capacity(resource.len());
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
            let mut taken: HashSet<(String, u8)> = HashSet::new();
            for (mi, src) in &record {
                let spec = &mapping.attributes[*mi];
                if let Some(v) = src.get(local).map_err(row_err)? {
                    check_attr(&spec.key, &v, limits).map_err(row_err)?;
                    present_record += 1;
                    let key = (spec.key.clone(), field_type_of(spec.value_type).to_u8());
                    if taken.insert(key.clone()) {
                        col_cells
                            .entry(key.clone())
                            .or_insert_with(|| vec![None; total_rows])[grow] = Some(v);
                        let flag = col_dict.entry(key).or_insert(true);
                        *flag &= src.is_dict();
                    } else {
                        batch.residual_attrs[grow].push((spec.key.clone(), v));
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
            batch.severity_text.push(severity_text.as_bytes());
            batch.body.push(body_val.as_bytes());
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
    let mut ref_of: HashMap<LogStreamId, u32> = HashMap::with_capacity(stream_dir.len());
    for (i, (id, blob)) in stream_dir.into_iter().enumerate() {
        ref_of.insert(id, i as u32);
        batch.stream_ids.push(id);
        batch.stream_attrs.push(blob);
    }
    batch.stream_refs = row_stream_id.iter().map(|id| ref_of[id]).collect();

    // Materialize dynamic columns in (name, type) order; attach a StrColumnDict
    // to a Str/Bytes column whose every winning cell came from a dictionary
    // source. If no column carries a dictionary, leave `dyn_col_dicts` empty (its
    // default), so a plain load is byte-identical to `from_records` without
    // `with_dictionaries`.
    let mut dicts: Vec<Option<StrColumnDict>> = Vec::with_capacity(col_cells.len());
    let mut any_dict = false;
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
        let use_dict = matches!(field_type, FieldType::Str | FieldType::Bytes)
            && col_dict
                .get(&(name.clone(), ty_byte))
                .copied()
                .unwrap_or(false);
        if use_dict {
            any_dict = true;
            dicts.push(Some(str_column_dict_from_cells(&dense)));
        } else {
            dicts.push(None);
        }
        batch.dyn_columns.push(DynColumn {
            name,
            field_type,
            cells: dense,
            validity,
        });
    }
    if any_dict {
        batch.dyn_col_dicts = dicts;
    }

    Ok(batch)
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

    // A batch that fails mid-loop (a later Parquet batch fails to decode, or
    // its columns fail to resolve against the mapping) must report whatever
    // was durable before it, not the empty slice `Setup` reports. Before this
    // fix, both in-loop failure sites used `LoadError::Setup`, so a load that
    // durably flushed earlier batches and then hit this error told the
    // operator "nothing landed" while `report.tokens` already held commit
    // tokens for those earlier batches -- confirmed by temporarily reverting
    // this test's expectation to `&[]` and observing it match `Setup`'s
    // behavior, which is the exact silent-loss shape the fix removes.
    #[test]
    fn batch_failed_reports_durable_tokens_not_empty() {
        let durable = vec![CommitToken {
            shard: 0,
            writer_id: uuid::Uuid::nil(),
            epoch: 0,
            seq: 1,
            ingest_hour_bucket: 0,
        }];
        let err = LoadError::BatchFailed {
            reason: "failed to read Parquet batch: corrupt page".into(),
            durable: durable.clone(),
        };
        assert_eq!(err.durable_tokens(), durable.as_slice());
        assert_ne!(
            err.durable_tokens(),
            LoadError::Setup("x".into()).durable_tokens()
        );
    }

    /// A clock pinned to `NOW_NS`, so the router buckets and routes against the
    /// same instant the provisioning `now_ns` uses (as the loader integration
    /// tests do).
    struct FixedClock(i64);
    impl Clock for FixedClock {
        fn now_ns(&self) -> i64 {
            self.0
        }
    }

    /// Load a fixture of one record with `n_attrs` distinct i64 attribute
    /// columns (plus one resource attribute) through the real `load`, and return
    /// its report. `n_attrs` past the writer's 1000-column budget forces
    /// overflow; below it exercises the near-cap path.
    async fn run_wide_load(n_attrs: usize) -> LoadReport {
        use parquet::arrow::ArrowWriter;
        use ravel_object_store::memory::MemoryStore;

        let dir = tempfile::tempdir().expect("tempdir");
        let pq = dir.path().join("wide.parquet");
        let mut cols: Vec<(String, ArrayRef)> = vec![
            ("ts".to_string(), i64_col(vec![NOW_NS])),
            ("svc".to_string(), str_col(vec!["api"])),
        ];
        let mut attr_toml = String::new();
        for i in 0..n_attrs {
            let name = format!("a{i}");
            cols.push((name.clone(), i64_col(vec![i as i64])));
            attr_toml.push_str(&format!(
                "\n[[attribute]]\nkey = \"{name}\"\ncolumn = \"{name}\"\ntype = \"i64\"\n"
            ));
        }
        let batch = RecordBatch::try_from_iter(cols).expect("wide batch");
        let file = std::fs::File::create(&pq).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        let m = parse_mapping(&format!(
            "ts_column = \"ts\"\nts_unit = \"nanos\"\n\n\
             [[resource_attribute]]\nkey = \"service.name\"\ncolumn = \"svc\"\ntype = \"str\"\n{attr_toml}"
        ))
        .expect("valid mapping");

        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        load(
            Arc::clone(&store),
            &pq,
            "acme",
            &m,
            4,
            10_000,
            None,
            1,
            NOW_NS,
            Arc::new(FixedClock(NOW_NS)),
        )
        .await
        .expect("load succeeds")
    }

    /// A real `load --parquet` run wires and records every stage of the logs
    /// pipeline, not a subset: admit, route, merge, and encode all recorded at
    /// least one sample. Drop `#[cfg(feature = "stage-timing")]` from
    /// `LogIngestRouter::stage_timings` (or from any one stage boundary) and
    /// this fails, either at compile time or on an empty/partial stage set.
    #[cfg(feature = "stage-timing")]
    #[tokio::test]
    async fn load_populates_the_stage_timing_breakdown() {
        let report = run_wide_load(4).await;
        let stages: Vec<_> = report.stage_timings.stages().collect();
        assert_eq!(
            stages,
            vec![
                ravel_ingest::LogStage::Admit,
                ravel_ingest::LogStage::Route,
                ravel_ingest::LogStage::Merge,
                ravel_ingest::LogStage::Encode,
            ],
            "a real load must wire and record every stage, not a subset"
        );
        for stage in stages {
            let totals = report
                .stage_timings
                .get(stage)
                .expect("a stage in `stages()` has totals");
            assert!(totals.samples > 0, "{stage:?} recorded zero samples");
        }
    }

    /// A load whose object crosses the 1000-column dynamic budget produces the
    /// overflow warning, driven from real router metrics rather than a
    /// hand-built snapshot. Flip the `dynamic_columns_overflowed_total > 0`
    /// guard in `dynamic_column_warnings` to `false` and this fails: no warning
    /// is emitted for a load that genuinely overflowed.
    #[tokio::test]
    async fn warns_when_dynamic_columns_overflow() {
        let report = run_wide_load(1001).await;
        assert!(
            report.metrics.dynamic_columns_overflowed_total > 0,
            "1001 distinct attribute columns overflow the 1000-column per-object budget"
        );
        let warnings = dynamic_column_warnings(
            &report.metrics,
            ravel_logseg::RlogConfig::default().max_dynamic_columns,
        );
        assert_eq!(warnings.len(), 1, "exactly the overflow warning fires");
        assert!(
            warnings[0].contains("overflowed the per-object dynamic-column budget")
                && warnings[0].contains("attrs_raw"),
            "the overflow warning states the count and the attrs_raw consequence: {}",
            warnings[0]
        );
    }

    /// The overflow warning reaches a caller of the real entry point, not just
    /// [`dynamic_column_warnings`].
    ///
    /// The sibling tests call that helper directly, so all of them stayed green
    /// when the emit loop was deleted from the entry point: they prove the text,
    /// not the wiring. This one drives [`run_warning_to`] end to end -- mapping
    /// file on disk, Parquet fixture, real router, real write -- and asserts the
    /// warning came out of the stream the CLI hands it.
    #[tokio::test]
    async fn the_entry_point_emits_the_overflow_warning() {
        use parquet::arrow::ArrowWriter;
        use ravel_object_store::memory::MemoryStore;

        let n_attrs = 1001;
        let dir = tempfile::tempdir().expect("tempdir");
        let pq = dir.path().join("wide.parquet");
        let mapping_path = dir.path().join("mapping.toml");

        let mut cols: Vec<(String, ArrayRef)> = vec![
            ("ts".to_string(), i64_col(vec![NOW_NS])),
            ("svc".to_string(), str_col(vec!["api"])),
        ];
        let mut attr_toml = String::new();
        for i in 0..n_attrs {
            let name = format!("a{i}");
            cols.push((name.clone(), i64_col(vec![i as i64])));
            attr_toml.push_str(&format!(
                "\n[[attribute]]\nkey = \"{name}\"\ncolumn = \"{name}\"\ntype = \"i64\"\n"
            ));
        }
        let batch = RecordBatch::try_from_iter(cols).expect("wide batch");
        let file = std::fs::File::create(&pq).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        std::fs::write(
            &mapping_path,
            format!(
                "ts_column = \"ts\"\nts_unit = \"nanos\"\n\n\
                 [[resource_attribute]]\nkey = \"service.name\"\ncolumn = \"svc\"\ntype = \"str\"\n{attr_toml}"
            ),
        )
        .expect("write mapping");

        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let mut sink: Vec<u8> = Vec::new();
        run_warning_to(
            store,
            &pq,
            "acme",
            &mapping_path,
            4,
            10_000,
            None,
            1,
            NOW_NS,
            &mut sink,
        )
        .await
        .expect("the load itself succeeds; overflow is a warning, not a failure");

        let emitted = String::from_utf8(sink).expect("warnings are utf-8");
        assert!(
            emitted.contains("overflowed the per-object dynamic-column budget"),
            "the entry point must emit the overflow warning it computed: {emitted}"
        );
        assert!(
            emitted.contains(ADMISSION_BYPASS_WARNING),
            "and the pre-existing admission warning still goes to the same stream: {emitted}"
        );
    }

    /// A load that reaches >= 90% of the budget without overflowing produces the
    /// distinct near-cap warning, again from real metrics.
    #[tokio::test]
    async fn warns_near_cap_without_overflow() {
        let report = run_wide_load(950).await;
        assert_eq!(
            report.metrics.dynamic_columns_overflowed_total, 0,
            "950 distinct columns stay under the 1000 budget, so nothing overflows"
        );
        assert!(
            report.metrics.dynamic_columns_used_max >= 900,
            "the widest object should sit near the cap: used_max = {}",
            report.metrics.dynamic_columns_used_max
        );
        let warnings = dynamic_column_warnings(&report.metrics, 1000);
        assert_eq!(warnings.len(), 1, "exactly the near-cap warning fires");
        assert!(
            warnings[0].contains("at or above") && warnings[0].contains("attrs_raw"),
            "the near-cap warning states the pressure and the attrs_raw consequence: {}",
            warnings[0]
        );
    }

    /// The near-cap boundary is exact: 90% warns, 89% does not, and an overflow
    /// takes precedence over the near-cap message. Flip the `>=` in
    /// `dynamic_column_warnings` to `>` and the exactly-90% case fails.
    #[test]
    fn near_cap_threshold_is_at_ninety_percent() {
        let snap = |used: u64, overflowed: u64| LogIngestMetricsSnapshot {
            dynamic_columns_used_max: used,
            dynamic_columns_overflowed_total: overflowed,
            ..Default::default()
        };
        assert_eq!(
            dynamic_column_warnings(&snap(900, 0), 1000).len(),
            1,
            "900 / 1000 = exactly 90% warns"
        );
        assert!(
            dynamic_column_warnings(&snap(899, 0), 1000).is_empty(),
            "899 / 1000 is just under 90% and does not warn"
        );
        assert!(
            dynamic_column_warnings(&snap(890, 0), 1000).is_empty(),
            "890 / 1000 = 89% does not warn"
        );
        let overflow = dynamic_column_warnings(&snap(900, 3), 1000);
        assert_eq!(overflow.len(), 1, "overflow still yields one message");
        assert!(
            overflow[0].contains("overflowed the per-object dynamic-column budget"),
            "overflow takes precedence over the near-cap message: {}",
            overflow[0]
        );
    }

    /// `--batch-rows 0` is rejected with a typed [`LoadError::Setup`] before any
    /// work, rather than silently clamped to 1.
    #[tokio::test]
    async fn batch_rows_zero_is_rejected() {
        use ravel_object_store::memory::MemoryStore;
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let m = base_mapping();
        let err = load(
            store,
            Path::new("/nonexistent.parquet"),
            "acme",
            &m,
            4,
            0,
            None,
            1,
            NOW_NS,
            Arc::new(FixedClock(NOW_NS)),
        )
        .await
        .expect_err("batch_rows of 0 is rejected");
        assert!(
            matches!(err, LoadError::Setup(_)),
            "a typed setup error, got: {err}"
        );
        assert!(
            err.to_string().contains("--batch-rows must be at least 1"),
            "the error names the lever: {err}"
        );
    }

    /// `--read-cursors 0` is rejected with a typed [`LoadError::Setup`] before
    /// any work, rather than silently clamped to 1 (issue #560), mirroring
    /// `batch_rows_zero_is_rejected` above.
    #[tokio::test]
    async fn read_cursors_zero_is_rejected() {
        use ravel_object_store::memory::MemoryStore;
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let m = base_mapping();
        let err = load(
            store,
            Path::new("/nonexistent.parquet"),
            "acme",
            &m,
            4,
            10,
            Some(0),
            1,
            NOW_NS,
            Arc::new(FixedClock(NOW_NS)),
        )
        .await
        .expect_err("read_cursors of 0 is rejected");
        assert!(
            matches!(err, LoadError::Setup(_)),
            "a typed setup error, got: {err}"
        );
        assert!(
            err.to_string()
                .contains("--read-cursors must be at least 1"),
            "the error names the lever: {err}"
        );
    }

    /// `--pipeline-depth 0` is rejected with a typed [`LoadError::Setup`]
    /// before any work, rather than silently clamped to 1, mirroring
    /// `batch_rows_zero_is_rejected` above. A depth of 0 would also make the
    /// main loop's `while inflight.len() >= pipeline_depth` true before any
    /// write is ever spawned; the guard makes that unreachable rather than
    /// relying on the `let`-else in the pop to save it.
    #[tokio::test]
    async fn pipeline_depth_zero_is_rejected() {
        use ravel_object_store::memory::MemoryStore;
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let m = base_mapping();
        let err = load(
            store,
            Path::new("/nonexistent.parquet"),
            "acme",
            &m,
            4,
            10,
            None,
            0,
            NOW_NS,
            Arc::new(FixedClock(NOW_NS)),
        )
        .await
        .expect_err("pipeline_depth of 0 is rejected");
        assert!(
            matches!(err, LoadError::Setup(_)),
            "a typed setup error, got: {err}"
        );
        assert!(
            err.to_string()
                .contains("--pipeline-depth must be at least 1"),
            "the error names the lever: {err}"
        );
    }

    /// The first host value (by an incrementing suffix) whose loader stream
    /// identity routes to `target` under `shards`. Uses the loader's own
    /// identity inputs -- resource attributes in mapping order, empty scope --
    /// so it matches how `build_record` computes `stream_id`.
    fn host_for_shard(target: u32, shards: u32) -> String {
        use ravel_types::shard_for_log;
        for i in 0..1_000_000u32 {
            let host = format!("h{i}");
            let resource = vec![
                (
                    "service.name".to_string(),
                    AttrValue::Str("api".to_string()),
                ),
                ("host".to_string(), AttrValue::Str(host.clone())),
            ];
            let stream_id = log_stream_id(&resource, "", "", &[]);
            if shard_for_log(&stream_id, shards) == target {
                return host;
            }
        }
        panic!("no host routes to shard {target} of {shards}");
    }

    /// Issue #296 reachability, end to end through the loader: a multi-shard
    /// batch where one shard's data-object PUT fails permanently while a
    /// sibling shard commits durably. `load` must return `LoadError::Flush`
    /// whose durable-token list -- the exact list `print_durable_tokens` prints
    /// -- includes the surviving shard's token, where before the fix that token
    /// was structurally unreportable and the list undercounted.
    ///
    /// Non-vacuity (prove-the-test): the failing shard (shard 0) sorts first in
    /// the router's ack loop, so the pre-fix early return dropped shard 1's
    /// token; against that code this fails at `durable.len() == 1` (the list is
    /// empty). The `FaultStore` counter is asserted so the abandonment is
    /// proven to have fired.
    #[tokio::test]
    async fn flush_failure_reports_the_surviving_shards_durable_token() {
        use parquet::arrow::ArrowWriter;
        use ravel_object_store::fault::{
            FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
        };
        use ravel_object_store::memory::MemoryStore;

        let shards = 4;
        let h_victim = host_for_shard(0, shards);
        let h_survivor = host_for_shard(1, shards);

        let dir = tempfile::tempdir().expect("tempdir");
        let pq = dir.path().join("two_shards.parquet");
        let cols: Vec<(String, ArrayRef)> = vec![
            ("ts".to_string(), i64_col(vec![NOW_NS, NOW_NS])),
            ("svc".to_string(), str_col(vec!["api", "api"])),
            (
                "host".to_string(),
                str_col(vec![h_victim.as_str(), h_survivor.as_str()]),
            ),
        ];
        let batch = RecordBatch::try_from_iter(cols).expect("two-row batch");
        let file = std::fs::File::create(&pq).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
        writer.write(&batch).expect("write batch");
        writer.close().expect("close writer");

        let m = parse_mapping(
            "ts_column = \"ts\"\nts_unit = \"nanos\"\n\n\
             [[resource_attribute]]\nkey = \"service.name\"\ncolumn = \"svc\"\ntype = \"str\"\n\n\
             [[resource_attribute]]\nkey = \"host\"\ncolumn = \"host\"\ntype = \"str\"\n",
        )
        .expect("valid mapping");

        // Fail every data-object PUT for shard 0 (`/l0/0000/`) permanently: a
        // non-retryable error abandons that flush at once, deterministically,
        // while shard 1 commits normally in the same Strict write.
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Put,
                ScriptedFault::Permanent("simulated permanent data-object PUT failure".into()),
            )
            .with_key_contains("/l0/0000/")
            .with_occurrence(Occurrence::Always),
        );
        let store = Arc::new(FaultStore::new(MemoryStore::new(), plan));

        let err = load(
            store.clone() as Arc<dyn ObjectStoreBackend>,
            &pq,
            "acme",
            &m,
            shards,
            // Both rows in one batch, so one Strict write spans both shards.
            10,
            None,
            1,
            NOW_NS,
            Arc::new(FixedClock(NOW_NS)),
        )
        .await
        .expect_err("one shard's flush was abandoned, so the load fails");

        let durable = match &err {
            LoadError::Flush { durable, cause } => {
                assert!(
                    cause.contains("flush abandoned"),
                    "the flush failure classifies as the underlying abandonment: {cause}"
                );
                durable.clone()
            }
            other => panic!("expected LoadError::Flush, got {other:?}"),
        };

        // The exact list `print_durable_tokens` iterates: the surviving shard's
        // token, recovered from the write error (issue #296).
        assert_eq!(
            err.durable_tokens().len(),
            1,
            "the surviving shard's token reaches the printed durable list, got {durable:?}"
        );
        assert_eq!(
            durable[0].shard, 1,
            "the recovered token is the surviving shard's (shard 1), not the abandoned shard 0"
        );

        assert_eq!(
            store.fault_count(Op::Put, FaultKind::Permanent),
            1,
            "the permanent data-object PUT fault fired exactly once (shard 0, no retry)"
        );
    }

    /// A store wrapper whose data-object (`/l0/`) PUT sleeps a fixed duration
    /// before completing, and which snapshots a shared "builds started" counter
    /// at the moment each such PUT finishes. Non-data PUTs (provisioning record,
    /// commit records) pass straight through, so only a batch's real RSEG write
    /// is timed. Every other method delegates unchanged.
    struct SlowPutStore {
        inner: Arc<dyn ObjectStoreBackend>,
        put_delay: Duration,
        builds_started: Arc<std::sync::atomic::AtomicUsize>,
        /// `builds_started` observed at completion of each data-object PUT.
        snapshots: Arc<std::sync::Mutex<Vec<usize>>>,
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for SlowPutStore {
        async fn put(
            &self,
            key: &str,
            data: bytes::Bytes,
            opts: ravel_object_store::PutOptions,
        ) -> Result<ravel_object_store::PutOutcome, ravel_object_store::StoreError> {
            let is_data_object = key.contains("/l0/");
            if is_data_object {
                tokio::time::sleep(self.put_delay).await;
            }
            let result = self.inner.put(key, data, opts).await;
            if is_data_object {
                self.snapshots.lock().expect("snapshots lock").push(
                    self.builds_started
                        .load(std::sync::atomic::Ordering::SeqCst),
                );
            }
            result
        }

        async fn get(
            &self,
            key: &str,
            range: ravel_object_store::GetRange,
        ) -> Result<ravel_object_store::GetOutcome, ravel_object_store::StoreError> {
            self.inner.get(key, range).await
        }

        async fn put_multipart<'a>(
            &'a self,
            key: &str,
        ) -> Result<Box<dyn ravel_object_store::MultipartUpload + 'a>, ravel_object_store::StoreError>
        {
            self.inner.put_multipart(key).await
        }

        async fn head(
            &self,
            key: &str,
        ) -> Result<ravel_object_store::ObjectMeta, ravel_object_store::StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<ravel_object_store::PageToken>,
        ) -> Result<ravel_object_store::ListPage, ravel_object_store::StoreError> {
            self.inner.list(prefix, page).await
        }

        async fn list_delimited(
            &self,
            prefix: &str,
        ) -> Result<ravel_object_store::DelimitedList, ravel_object_store::StoreError> {
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), ravel_object_store::StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> ravel_object_store::Capabilities {
            self.inner.capabilities()
        }
    }

    /// The pipeline actually overlaps (issue #541): batch N+1's decode/build
    /// begins while batch N's slow object-store PUT is still in flight, not
    /// after it returns. A single stream over `batch_rows = 2` splits into three
    /// batches; each batch's RSEG PUT sleeps 50ms, and the decode/build start
    /// hook bumps a shared counter. At the completion of every data-object PUT
    /// the counter is snapshotted; a value >= 2 means the *next* batch's build
    /// had already started before this batch's PUT returned.
    ///
    /// Non-vacuity (prove-the-test): against the former fully-serial loop
    /// (revert the `spawn_build` lookahead so batch N is decoded, written, and
    /// awaited before batch N+1 is even read) the first data PUT completes with
    /// only batch 0 built, so the snapshot is 1 and the `min >= 2` assertion
    /// fails.
    #[tokio::test]
    async fn next_batch_decode_overlaps_current_batch_write() {
        use parquet::arrow::ArrowWriter;
        use ravel_object_store::memory::MemoryStore;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let n_rows = 6;
        let dir = tempfile::tempdir().expect("tempdir");
        let pq = dir.path().join("multi.parquet");
        let b = batch(vec![
            ("ts", i64_col(vec![NOW_NS; n_rows])),
            ("svc", str_col(vec!["api"; n_rows])),
        ]);
        let file = std::fs::File::create(&pq).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, b.schema(), None).expect("arrow writer");
        writer.write(&b).expect("write batch");
        writer.close().expect("close writer");

        let m = parse_mapping(
            "ts_column = \"ts\"\nts_unit = \"nanos\"\n\n\
             [[resource_attribute]]\nkey = \"service.name\"\ncolumn = \"svc\"\ntype = \"str\"\n",
        )
        .expect("valid mapping");

        let builds_started = Arc::new(AtomicUsize::new(0));
        let snapshots = Arc::new(std::sync::Mutex::new(Vec::<usize>::new()));
        let store = Arc::new(SlowPutStore {
            inner: Arc::new(MemoryStore::new()),
            put_delay: Duration::from_millis(50),
            builds_started: Arc::clone(&builds_started),
            snapshots: Arc::clone(&snapshots),
        });

        let hook_counter = Arc::clone(&builds_started);
        let hook: BuildStartHook = Arc::new(move || {
            hook_counter.fetch_add(1, Ordering::SeqCst);
        });

        // `batch_rows = 2` over 6 rows yields three batches through one shard,
        // so three RSEG PUTs happen in sequence.
        let report = load_instrumented(
            store as Arc<dyn ObjectStoreBackend>,
            &pq,
            "acme",
            &m,
            1,
            2,
            None,
            1,
            NOW_NS,
            Arc::new(FixedClock(NOW_NS)),
            LoadPath::Columnar,
            Some(hook),
        )
        .await
        .expect("the pipelined load succeeds");

        assert_eq!(report.rows_processed, n_rows as u64, "every row is written");

        let snaps = snapshots.lock().expect("snapshots lock").clone();
        assert!(
            snaps.len() >= 2,
            "at least two data-object PUTs happened (three batches, one shard): {snaps:?}"
        );
        let min = *snaps.iter().min().expect("non-empty snapshots");
        assert!(
            min >= 2,
            "batch N+1's decode/build must start before batch N's slow PUT returns \
             (a serial loop leaves the counter at 1 when the first PUT completes); \
             builds-started-at-PUT-completion snapshots = {snaps:?}"
        );
        assert!(
            builds_started.load(Ordering::SeqCst) >= 3,
            "all three batches were decoded/built, got {}",
            builds_started.load(Ordering::SeqCst)
        );
    }

    /// A rejected row in the *second* batch must report its absolute index
    /// into the whole file, not an index relative to that batch or to its own
    /// stride cursor's partition. `file_base` is threaded through
    /// `decode_and_build_stride`, a free function outside the loop the
    /// prefetch refactor introduced, and easy to drop by accident (no
    /// existing test drove a real multi-batch `load()` far enough to catch
    /// it: `future_skew_beyond_the_bound_is_rejected` calls `build_record`
    /// directly on row 0, and `batch_failed_reports_durable_tokens_not_empty`
    /// hand-constructs a `LoadError` rather than running a load). Change
    /// `file_base + row as u64` to `row as u64` in `decode_and_build_stride`'s
    /// `RowRejected` arm and the first half of this test (the `read-cursors`
    /// auto-resolved to `1` case below) fails at `assert_eq!(row, 2, ..)` with
    /// `left: 0, right: 2` -- confirmed by performing the flip. The test
    /// panics there, before ever reaching the second half, because both
    /// halves exercise the same shared line: `decode_and_build_stride` is now
    /// the only row-decode path, used for every `--read-cursors` value
    /// including `1`, so this one flip is non-vacuous for both.
    ///
    /// Extended for issue #560: under `--read-cursors 4`, the same guarantee
    /// must hold when the rejected row sits in a stride cursor whose own
    /// partition starts partway through the file (row 5, at local index 1
    /// within its own stride cursor's 2-row span starting at file row 4), so
    /// the translation is via that span's own `file_base`, not a single
    /// file-wide accumulator.
    #[tokio::test]
    async fn a_rejected_row_in_a_later_batch_reports_its_absolute_index() {
        use parquet::arrow::ArrowWriter;
        use ravel_object_store::memory::MemoryStore;

        let limits = LogIngestLimits::default();
        let dir = tempfile::tempdir().expect("tempdir");
        let pq = dir.path().join("two_batches.parquet");

        // Rows 0-1 are batch 1 (batch_rows: 2) and valid. Row 2, the first row
        // of batch 2, is far enough in the future to be rejected.
        let ts = vec![
            NOW_NS,
            NOW_NS,
            NOW_NS + limits.max_future_skew_ns + 1,
            NOW_NS,
        ];
        let batch_data = batch(vec![("ts", i64_col(ts))]);
        let file = std::fs::File::create(&pq).expect("create parquet");
        let mut writer =
            ArrowWriter::try_new(file, batch_data.schema(), None).expect("arrow writer");
        writer.write(&batch_data).expect("write batch");
        writer.close().expect("close writer");

        let m = base_mapping();
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let err = load(
            Arc::clone(&store),
            &pq,
            "acme",
            &m,
            4,
            2,
            None,
            1,
            NOW_NS,
            Arc::new(FixedClock(NOW_NS)),
        )
        .await
        .expect_err("the far-future row must be rejected");

        match err {
            LoadError::RowRejected { row, durable, .. } => {
                assert_eq!(
                    row, 2,
                    "row index must be absolute across batches, not relative to its own batch"
                );
                assert!(
                    !durable.is_empty(),
                    "batch 1's write must already be durable: it was fully awaited before \
                     batch 2 was even decoded"
                );
            }
            other => panic!("expected RowRejected, got {other:?}"),
        }

        // Same guarantee under `--read-cursors 4`: a 4-row-group file, one row
        // group per stride cursor, with the future-skew violator at
        // file-absolute row 5 (local index 1 within row group 2's 2-row
        // span). Its reported index must still be 5, translated via that
        // span's own `file_base` rather than a shared `row_base`.
        let pq4 = dir.path().join("four_row_groups.parquet");
        let row_groups: Vec<Vec<i64>> = vec![
            vec![NOW_NS, NOW_NS],
            vec![NOW_NS, NOW_NS],
            vec![NOW_NS, NOW_NS + limits.max_future_skew_ns + 1],
            vec![NOW_NS, NOW_NS],
        ];
        let file4 = std::fs::File::create(&pq4).expect("create parquet");
        let mut writer4 =
            ArrowWriter::try_new(file4, batch_data.schema(), None).expect("arrow writer");
        for rg in &row_groups {
            let rg_batch = batch(vec![("ts", i64_col(rg.clone()))]);
            writer4.write(&rg_batch).expect("write row group");
            writer4.flush().expect("flush row group");
        }
        writer4.close().expect("close writer");

        let err4 = load(
            Arc::clone(&store),
            &pq4,
            "acme",
            &m,
            4,
            8,
            Some(4),
            1,
            NOW_NS,
            Arc::new(FixedClock(NOW_NS)),
        )
        .await
        .expect_err("the far-future row must be rejected under read-cursors=4");

        match err4 {
            LoadError::RowRejected { row, .. } => {
                assert_eq!(
                    row, 5,
                    "row index must be FILE-absolute even when a stride cursor's own \
                     span starts partway through the file"
                );
            }
            other => panic!("expected RowRejected, got {other:?}"),
        }
    }

    /// Stride reading (issue #560) turns a sorted, one-shard-per-run input
    /// into per-batch shard spread: a 4-row-group file where each row group
    /// holds only one shard's host value (`hits.parquet`'s CounterID-sorted
    /// shape in miniature). With one stride cursor per row group
    /// (`--read-cursors 4`), every `batch_rows`-sized batch draws one row
    /// from each group, so every flush touches all 4 shards and
    /// `objects_written()` is exactly `batches * shards`. With
    /// `--read-cursors 1` (today's sequential read), each batch is one whole
    /// row group -- one shard -- so it is exactly `batches * 1`.
    ///
    /// Non-vacuity (prove-the-test): change the `Some(shards as usize)`
    /// argument in the first `load` call below to `Some(1)` and the `16`
    /// assertion fails (`left: 4, right: 16`), since a single sequential
    /// cursor never interleaves the row groups.
    #[tokio::test]
    async fn stride_reading_spreads_a_sorted_batch_across_all_shards() {
        use parquet::arrow::ArrowWriter;
        use ravel_object_store::memory::MemoryStore;

        let shards = 4u32;
        let rows_per_group = 4usize;
        let hosts: Vec<String> = (0..shards).map(|s| host_for_shard(s, shards)).collect();

        let dir = tempfile::tempdir().expect("tempdir");
        let pq = dir.path().join("sorted_by_shard.parquet");
        let first = batch(vec![
            ("ts", i64_col(vec![NOW_NS; rows_per_group])),
            ("svc", str_col(vec!["api"; rows_per_group])),
            ("host", str_col(vec![hosts[0].as_str(); rows_per_group])),
        ]);
        let file = std::fs::File::create(&pq).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, first.schema(), None).expect("arrow writer");
        writer.write(&first).expect("write row group");
        writer.flush().expect("flush row group");
        for host in &hosts[1..] {
            let rg = batch(vec![
                ("ts", i64_col(vec![NOW_NS; rows_per_group])),
                ("svc", str_col(vec!["api"; rows_per_group])),
                ("host", str_col(vec![host.as_str(); rows_per_group])),
            ]);
            writer.write(&rg).expect("write row group");
            writer.flush().expect("flush row group");
        }
        writer.close().expect("close writer");

        let m = parse_mapping(
            "ts_column = \"ts\"\nts_unit = \"nanos\"\n\n\
             [[resource_attribute]]\nkey = \"service.name\"\ncolumn = \"svc\"\ntype = \"str\"\n\n\
             [[resource_attribute]]\nkey = \"host\"\ncolumn = \"host\"\ntype = \"str\"\n",
        )
        .expect("valid mapping");

        let n_rows = rows_per_group * shards as usize;
        let batches = n_rows / rows_per_group;

        let store4: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let report4 = load(
            store4,
            &pq,
            "acme",
            &m,
            shards,
            rows_per_group,
            Some(shards as usize),
            1,
            NOW_NS,
            Arc::new(FixedClock(NOW_NS)),
        )
        .await
        .expect("stride-read load succeeds");
        assert_eq!(report4.rows_processed, n_rows as u64);
        assert_eq!(
            report4.objects_written(),
            batches * shards as usize,
            "read-cursors=4: every batch draws one row from each row group/shard"
        );

        let store1: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let report1 = load(
            store1,
            &pq,
            "acme",
            &m,
            shards,
            rows_per_group,
            Some(1),
            1,
            NOW_NS,
            Arc::new(FixedClock(NOW_NS)),
        )
        .await
        .expect("sequential-read load succeeds");
        assert_eq!(report1.rows_processed, n_rows as u64);
        assert_eq!(
            report1.objects_written(),
            batches,
            "read-cursors=1: each batch is one whole row group, one shard"
        );
    }

    /// Every row is delivered exactly once regardless of `--read-cursors`,
    /// including the exhaustion/redistribution path: a 14-row, 4-row-group
    /// file with deliberately unequal group sizes (4/3/5/2, no two equal),
    /// loaded with `batch_rows=5` under `read-cursors=1` (sequential),
    /// `read-cursors=4` (one cursor per row group, all exhaust together),
    /// and `read-cursors=3` (partitions of uneven length `[7, 5, 2]` rows,
    /// so partitions exhaust at different rounds -- `3` divides neither
    /// `batch_rows` (5) nor the row-group count (4)) -- reports exactly 14
    /// durable rows every time.
    #[tokio::test]
    async fn every_read_cursors_setting_delivers_the_exact_row_count() {
        use parquet::arrow::ArrowWriter;
        use ravel_object_store::memory::MemoryStore;

        let dir = tempfile::tempdir().expect("tempdir");
        let pq = dir.path().join("uneven_row_groups.parquet");
        let group_sizes = [4usize, 3, 5, 2];
        let total: usize = group_sizes.iter().sum();

        let first = batch(vec![("ts", i64_col(vec![NOW_NS; group_sizes[0]]))]);
        let file = std::fs::File::create(&pq).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, first.schema(), None).expect("arrow writer");
        writer.write(&first).expect("write row group");
        writer.flush().expect("flush row group");
        for &size in &group_sizes[1..] {
            let rg = batch(vec![("ts", i64_col(vec![NOW_NS; size]))]);
            writer.write(&rg).expect("write row group");
            writer.flush().expect("flush row group");
        }
        writer.close().expect("close writer");

        let m = base_mapping();
        for read_cursors in [Some(1), Some(4), Some(3)] {
            let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
            let report = load(
                store,
                &pq,
                "acme",
                &m,
                4,
                5,
                read_cursors,
                1,
                NOW_NS,
                Arc::new(FixedClock(NOW_NS)),
            )
            .await
            .expect("load succeeds");
            assert_eq!(
                report.rows_processed, total as u64,
                "read_cursors={read_cursors:?}: every row must be loaded exactly once"
            );
        }
    }

    /// The early shard-skew warning (issue #560) fires exactly once when the
    /// observed spread stays at or below `shards / SKEW_WARN_DENOMINATOR`
    /// through the `SKEW_CHECK_AFTER_BATCHES`-batch checkpoint, and stays
    /// silent whenever stride reading (or an already-interleaved input)
    /// keeps the spread above it.
    ///
    /// Non-vacuity (prove-the-test): change `distinct_shards as u32 >
    /// threshold` to `>=` in `shard_skew_warning` and the first case below
    /// (distinct=1, threshold=1, an exact-boundary case) stops warning:
    /// `1 >= 1` incorrectly early-returns `None`.
    #[tokio::test]
    async fn early_skew_warning_fires_once_and_only_when_the_spread_stays_narrow() {
        use parquet::arrow::ArrowWriter;
        use ravel_object_store::memory::MemoryStore;

        let shards = 4u32;
        let group_len = 20usize;
        let batch_rows = 2usize;
        let hosts: Vec<String> = (0..shards).map(|s| host_for_shard(s, shards)).collect();

        let mapping_dir = tempfile::tempdir().expect("tempdir");
        let mapping_path = mapping_dir.path().join("mapping.toml");
        std::fs::write(
            &mapping_path,
            "ts_column = \"ts\"\nts_unit = \"nanos\"\n\n\
             [[resource_attribute]]\nkey = \"service.name\"\ncolumn = \"svc\"\ntype = \"str\"\n\n\
             [[resource_attribute]]\nkey = \"host\"\ncolumn = \"host\"\ntype = \"str\"\n",
        )
        .expect("write mapping");

        // (a)/(b): a sorted file -- 4 row groups, one per shard's host value,
        // `group_len` rows each.
        let dir = tempfile::tempdir().expect("tempdir");
        let sorted_pq = dir.path().join("sorted.parquet");
        let first = batch(vec![
            ("ts", i64_col(vec![NOW_NS; group_len])),
            ("svc", str_col(vec!["api"; group_len])),
            ("host", str_col(vec![hosts[0].as_str(); group_len])),
        ]);
        let file = std::fs::File::create(&sorted_pq).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, first.schema(), None).expect("arrow writer");
        writer.write(&first).expect("write row group");
        writer.flush().expect("flush row group");
        for host in &hosts[1..] {
            let rg = batch(vec![
                ("ts", i64_col(vec![NOW_NS; group_len])),
                ("svc", str_col(vec!["api"; group_len])),
                ("host", str_col(vec![host.as_str(); group_len])),
            ]);
            writer.write(&rg).expect("write row group");
            writer.flush().expect("flush row group");
        }
        writer.close().expect("close writer");

        // (a) sorted + read-cursors=1: the first `SKEW_CHECK_AFTER_BATCHES`
        // batches (16 rows) stay inside row group 0's single host/shard, so
        // the warning fires -- exactly once.
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let mut sink = Vec::new();
        run_warning_to(
            store,
            &sorted_pq,
            "acme",
            &mapping_path,
            shards,
            batch_rows,
            Some(1),
            1,
            NOW_NS,
            &mut sink,
        )
        .await
        .expect("load succeeds");
        let emitted = String::from_utf8(sink).expect("utf8");
        assert_eq!(
            emitted.matches("shard spread is at or below").count(),
            1,
            "sorted input read sequentially must warn exactly once: {emitted}"
        );

        // (b) sorted + read-cursors=4: one stride cursor per row group mixes
        // all 4 shards into the first couple of batches, so the spread never
        // narrows and the warning stays silent.
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let mut sink = Vec::new();
        run_warning_to(
            store,
            &sorted_pq,
            "acme",
            &mapping_path,
            shards,
            batch_rows,
            Some(4),
            1,
            NOW_NS,
            &mut sink,
        )
        .await
        .expect("load succeeds");
        let emitted = String::from_utf8(sink).expect("utf8");
        assert!(
            !emitted.contains("shard spread is at or below"),
            "stride reading the same sorted input must not warn: {emitted}"
        );

        // (c) an already-interleaved input, read-cursors=1: even a
        // sequential reader sees all 4 shards from row 0, so the warning
        // stays silent.
        let interleaved_pq = dir.path().join("interleaved.parquet");
        let n_rows = 32usize;
        let host_seq: Vec<&str> = (0..n_rows)
            .map(|i| hosts[i % shards as usize].as_str())
            .collect();
        let b = batch(vec![
            ("ts", i64_col(vec![NOW_NS; n_rows])),
            ("svc", str_col(vec!["api"; n_rows])),
            ("host", str_col(host_seq)),
        ]);
        let file = std::fs::File::create(&interleaved_pq).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, b.schema(), None).expect("arrow writer");
        writer.write(&b).expect("write batch");
        writer.close().expect("close writer");

        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let mut sink = Vec::new();
        run_warning_to(
            store,
            &interleaved_pq,
            "acme",
            &mapping_path,
            shards,
            batch_rows,
            Some(1),
            1,
            NOW_NS,
            &mut sink,
        )
        .await
        .expect("load succeeds");
        let emitted = String::from_utf8(sink).expect("utf8");
        assert!(
            !emitted.contains("shard spread is at or below"),
            "an already-interleaved input must not warn: {emitted}"
        );
    }

    // ---------------------------------------------------------------------
    // ADR-0109 columnar fast path.
    // ---------------------------------------------------------------------

    fn to_logrecord(r: &NormalizedLogRecord) -> ravel_logseg::LogRecord {
        ravel_logseg::LogRecord {
            stream_id: r.stream_id,
            stream_attrs: r.stream_attrs.clone(),
            ts_ns: r.ts_ns,
            observed_ts_ns: r.observed_ts_ns,
            severity_num: r.severity_num,
            severity_text: r.severity_text.clone(),
            body: r.body.clone(),
            trace_id: r.trace_id,
            span_id: r.span_id,
            flags: r.flags,
            attrs: r.attrs.clone(),
        }
    }

    /// The row differential-reference records for `batch` under `mapping`.
    fn row_records(batch: &RecordBatch, mapping: &Mapping) -> Vec<NormalizedLogRecord> {
        let cols = ColumnIndex::resolve(batch, mapping).expect("resolve columns");
        (0..batch.num_rows())
            .map(|r| {
                build_record(
                    batch,
                    &cols,
                    mapping,
                    &LogIngestLimits::default(),
                    NOW_NS,
                    r,
                )
                .expect("build_record")
            })
            .collect()
    }

    /// Write `batch` to a Parquet file and read it back as one RecordBatch, so a
    /// dictionary-encoded column exercises the real Parquet decode path (the
    /// Arrow schema `ArrowWriter` embeds is what lets a `Dictionary` column
    /// survive the round trip; a plain `BYTE_ARRAY` column comes back plain).
    fn roundtrip_parquet(batch: &RecordBatch) -> RecordBatch {
        use parquet::arrow::ArrowWriter;
        let dir = tempfile::tempdir().expect("tempdir");
        let pq = dir.path().join("rt.parquet");
        let file = std::fs::File::create(&pq).expect("create parquet");
        let mut w = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
        w.write(batch).expect("write batch");
        w.close().expect("close writer");
        let f = std::fs::File::open(&pq).expect("open parquet");
        let reader = ParquetRecordBatchReaderBuilder::try_new(f)
            .expect("reader builder")
            .build()
            .expect("reader");
        let mut batches: Vec<RecordBatch> = reader.map(|b| b.expect("read batch")).collect();
        assert_eq!(batches.len(), 1, "fixture fits one read batch");
        batches.pop().expect("one batch")
    }

    /// A pinned object identity so two objects are comparable byte for byte: the
    /// footer stamps `writer_id`/`epoch`/`seq` verbatim, so only a real drift in
    /// the encoded records could move a byte.
    fn fixed_identity() -> ravel_logseg::ObjectIdentity {
        ravel_logseg::ObjectIdentity {
            tenant_hash: [7u8; 16],
            shard: 0,
            writer_id: [9u8; 16],
            writer_epoch: 1,
            writer_seq: 0,
        }
    }

    fn row_object(records: &[NormalizedLogRecord]) -> Vec<u8> {
        let mut w =
            ravel_logseg::RlogWriter::new(ravel_logseg::RlogConfig::default(), fixed_identity());
        for r in records {
            w.push(to_logrecord(r)).expect("push row record");
        }
        w.finish().expect("finish row object")
    }

    fn columnar_object(batch: ColumnarLogBatch) -> Vec<u8> {
        let mut w =
            ravel_logseg::RlogWriter::new(ravel_logseg::RlogConfig::default(), fixed_identity());
        w.push_columnar(batch).expect("push columnar batch");
        w.finish().expect("finish columnar object")
    }

    fn build_columnar_or_panic(batch: &RecordBatch, mapping: &Mapping) -> ColumnarLogBatch {
        let spans = vec![(batch.clone(), 0u64)];
        match build_columnar_batch(&spans, mapping, &LogIngestLimits::default(), NOW_NS) {
            Ok(b) => b,
            Err(ColBuildError::Batch(reason)) => panic!("columnar batch failed: {reason}"),
            Err(ColBuildError::Row { row, reason }) => {
                panic!("columnar row {row} rejected: {reason}")
            }
        }
    }

    /// Build `batch` through both the row path and the columnar builder and
    /// assert (a) the columnar batch equals `from_records` of the row records
    /// (ignoring the additive dictionary shapes), and (b) the encoded RLOG
    /// objects are byte-for-byte identical (ADR-0109 decision 7). Returns the
    /// columnar batch for further inspection (e.g. dictionary attachment).
    fn assert_paths_match(batch: &RecordBatch, mapping: &Mapping) -> ColumnarLogBatch {
        let records = row_records(batch, mapping);
        let col = build_columnar_or_panic(batch, mapping);

        let logrecords: Vec<ravel_logseg::LogRecord> = records.iter().map(to_logrecord).collect();
        let expected = ColumnarLogBatch::from_records(&logrecords);
        let mut col_no_dict = col.clone();
        col_no_dict.dyn_col_dicts = Vec::new();
        assert_eq!(
            col_no_dict, expected,
            "columnar builder must produce the same batch as from_records of the row records"
        );

        let row_bytes = row_object(&records);
        let col_bytes = columnar_object(col.clone());
        assert_eq!(
            row_bytes, col_bytes,
            "row and columnar RLOG objects must be byte-for-byte identical"
        );
        col
    }

    /// The end-to-end byte-identity anchor (ADR-0109 decision 7): the same
    /// records, built row-wise and column-wise, encode to identical RLOG bytes
    /// across a corpus of nulls in every mapped column, each `TsUnit`, an
    /// out-of-`u8` severity number, an all-null attribute column, duplicate
    /// mapped keys (winner plus residual), and both dictionary-encoded and plain
    /// string columns.
    ///
    /// Prove-the-test: change `observed_ts_ns` to `push(0)` (instead of
    /// `raw_ts`), or drop the `TsUnit` scaling in `TsSrc::get` (return the raw
    /// value), or set `use_dict` to `false` unconditionally -- each flips a byte
    /// and the `assert_eq!` on the objects fails. Confirmed by making the
    /// `observed_ts_ns` flip: the objects diverged and the assertion tripped.
    #[test]
    fn columnar_load_matches_row_load_byte_for_byte() {
        use arrow::array::DictionaryArray;
        use arrow::datatypes::Int32Type;

        // Each TsUnit: an integer ts column scaled by the declared unit must
        // land identically on both paths.
        for (unit, raw) in [
            (TsUnit::Seconds, 1_700_000_000_i64),
            (TsUnit::Millis, 1_700_000_000_000),
            (TsUnit::Micros, 1_700_000_000_000_000),
            (TsUnit::Nanos, 1_700_000_000_000_000_000),
        ] {
            let b = roundtrip_parquet(&batch(vec![
                ("ts", i64_col(vec![raw, raw])),
                ("a", str_col(vec!["v", "v"])),
            ]));
            let mut m = base_mapping();
            m.ts_unit = unit;
            m.attributes = vec![attr("a", "a", ColType::Str)];
            assert_paths_match(&b, &m);
        }

        // A rich batch: nulls in every optional/attribute column, an out-of-u8
        // severity, an all-null attribute column, duplicate mapped keys, and a
        // dictionary column beside a plain one.
        let ts = Arc::new(Int64Array::from(vec![
            NOW_NS,
            NOW_NS + 1,
            NOW_NS + 2,
            NOW_NS + 3,
        ])) as ArrayRef;
        let body = Arc::new(StringArray::from(vec![
            Some("hello"),
            None,
            Some(""),
            Some("world"),
        ])) as ArrayRef;
        // 300 is out of u8 range and must normalize to 0 on both paths; row 2 is
        // null (also 0).
        let sev = Arc::new(Int64Array::from(vec![
            Some(9_i64),
            Some(300),
            None,
            Some(0),
        ])) as ArrayRef;
        let svc = Arc::new(StringArray::from(vec![
            Some("api"),
            None,
            Some("web"),
            Some("api"),
        ])) as ArrayRef;
        let allnull = Arc::new(Int64Array::from(
            vec![None, None, None, None] as Vec<Option<i64>>
        )) as ArrayRef;
        let dup_a =
            Arc::new(Int64Array::from(vec![Some(1_i64), Some(2), None, Some(4)])) as ArrayRef;
        let dup_b = Arc::new(Int64Array::from(vec![
            Some(10_i64),
            None,
            Some(30),
            Some(40),
        ])) as ArrayRef;
        let dictcol = Arc::new(
            vec![Some("x"), Some("y"), None, Some("x")]
                .into_iter()
                .collect::<DictionaryArray<Int32Type>>(),
        ) as ArrayRef;
        let plaincol = Arc::new(StringArray::from(vec![
            Some("p"),
            None,
            Some("q"),
            Some("p"),
        ])) as ArrayRef;

        let rich = batch(vec![
            ("ts", ts),
            ("body", body),
            ("sev", sev),
            ("svc", svc),
            ("allnull", allnull),
            ("dupA", dup_a),
            ("dupB", dup_b),
            ("dictcol", dictcol),
            ("plaincol", plaincol),
        ]);
        let rich = roundtrip_parquet(&rich);

        // The Parquet round trip preserves the Arrow dictionary encoding (the
        // report question): a column arrow-written as a Dictionary comes back a
        // Dictionary, while the plain column stays plain.
        let dict_idx = rich.schema().index_of("dictcol").expect("dictcol present");
        assert!(
            matches!(
                rich.column(dict_idx).data_type(),
                DataType::Dictionary(_, _)
            ),
            "an arrow-written dictionary column survives the Parquet round trip as a Dictionary"
        );
        let plain_idx = rich
            .schema()
            .index_of("plaincol")
            .expect("plaincol present");
        assert!(
            matches!(rich.column(plain_idx).data_type(), DataType::Utf8),
            "the plain string column arrives plain"
        );

        let mut m = base_mapping();
        m.body_column = Some("body".to_string());
        m.severity_number_column = Some("sev".to_string());
        m.resource_attributes = vec![attr("service.name", "svc", ColType::Str)];
        m.attributes = vec![
            attr("allnull", "allnull", ColType::I64),
            attr("dup", "dupA", ColType::I64),
            attr("dup", "dupB", ColType::I64),
            attr("dictkey", "dictcol", ColType::Str),
            attr("plainkey", "plaincol", ColType::Str),
        ];

        let col = assert_paths_match(&rich, &m);

        // The dictionary column reached the StrColumnDict fast path; the plain
        // one did not.
        assert!(
            !col.dyn_col_dicts.is_empty(),
            "at least one column carries a dictionary, so dyn_col_dicts is populated"
        );
        let dict_pos = col
            .dyn_columns
            .iter()
            .position(|c| c.name == "dictkey")
            .expect("dictkey column");
        let plain_pos = col
            .dyn_columns
            .iter()
            .position(|c| c.name == "plainkey")
            .expect("plainkey column");
        assert!(
            col.dyn_col_dicts[dict_pos].is_some(),
            "the dictionary-encoded column passes through as a StrColumnDict"
        );
        assert!(
            col.dyn_col_dicts[plain_pos].is_none(),
            "the plain string column stays plain, no dictionary attached"
        );
    }

    #[allow(clippy::too_many_arguments)]
    async fn load_row(
        store: Arc<dyn ObjectStoreBackend>,
        parquet_path: &Path,
        tenant: &str,
        mapping: &Mapping,
        shards: u32,
        batch_rows: usize,
        read_cursors: Option<usize>,
        now_ns: i64,
        clock: Arc<dyn Clock>,
    ) -> Result<LoadReport, LoadError> {
        load_instrumented(
            store,
            parquet_path,
            tenant,
            mapping,
            shards,
            batch_rows,
            read_cursors,
            1,
            now_ns,
            clock,
            LoadPath::Row,
            None,
        )
        .await
    }

    /// Reachability (the point of ADR-0109): the real `load` entry point drives
    /// the columnar path, not merely a builder that compiles. A load of a
    /// multi-batch file reports `columnar_batches_built > 0`, and the row
    /// differential path over the same file reports 0 -- an observation the row
    /// path cannot satisfy, evaluated at the point of reliance (each batch that
    /// was handed to `write_columnar`).
    ///
    /// Prove-the-test: change `load`'s `LoadPath::Columnar` argument to
    /// `LoadPath::Row`, and this fails at `columnar_batches_built > 0` (it reads
    /// 0). Confirmed by performing the flip.
    #[tokio::test]
    async fn load_drives_the_columnar_path_end_to_end() {
        use parquet::arrow::ArrowWriter;
        use ravel_object_store::memory::MemoryStore;

        let n_rows = 6usize;
        let dir = tempfile::tempdir().expect("tempdir");
        let pq = dir.path().join("reach.parquet");
        let b = batch(vec![
            ("ts", i64_col(vec![NOW_NS; n_rows])),
            ("svc", str_col(vec!["api"; n_rows])),
        ]);
        let file = std::fs::File::create(&pq).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, b.schema(), None).expect("arrow writer");
        writer.write(&b).expect("write batch");
        writer.close().expect("close writer");

        let m = parse_mapping(
            "ts_column = \"ts\"\nts_unit = \"nanos\"\n\n\
             [[resource_attribute]]\nkey = \"service.name\"\ncolumn = \"svc\"\ntype = \"str\"\n",
        )
        .expect("valid mapping");

        // One shard, batch_rows=2 over 6 rows: three columnar batches, three
        // write_columnar calls.
        let store_col: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let report_col = load(
            Arc::clone(&store_col),
            &pq,
            "acme",
            &m,
            1,
            2,
            None,
            1,
            NOW_NS,
            Arc::new(FixedClock(NOW_NS)),
        )
        .await
        .expect("columnar load succeeds");
        assert_eq!(report_col.rows_processed, n_rows as u64);
        assert!(
            report_col.columnar_batches_built > 0,
            "the real load entry point must drive the columnar path"
        );
        assert_eq!(
            report_col.columnar_batches_built, 3,
            "each of the three batches was built and driven through write_columnar"
        );

        // The row differential path over the same file never touches the
        // columnar builder, so the counter stays 0 -- proof the signal is
        // specific to the columnar path and not incremented incidentally.
        let store_row: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let report_row = load_row(
            Arc::clone(&store_row),
            &pq,
            "acme",
            &m,
            1,
            2,
            None,
            NOW_NS,
            Arc::new(FixedClock(NOW_NS)),
        )
        .await
        .expect("row load succeeds");
        assert_eq!(report_row.rows_processed, n_rows as u64);
        assert_eq!(
            report_row.columnar_batches_built, 0,
            "the row path builds no columnar batch"
        );
    }

    /// A store wrapper that instruments data-object (`/l0/`) PUTs for the
    /// `--pipeline-depth` tests: it can sleep a per-key-substring delay before a
    /// PUT, track the maximum number of data-object PUTs concurrently in flight,
    /// and count how many PUTs matching a watch prefix started versus completed.
    /// Non-data PUTs (provisioning record, commit records) pass straight
    /// through. Every other method delegates unchanged.
    struct InstrumentedPutStore {
        inner: Arc<dyn ObjectStoreBackend>,
        /// `(key substring, delay)`; the first matching entry's delay is applied
        /// before the PUT reaches `inner`.
        delays: Vec<(&'static str, Duration)>,
        /// Data-object PUTs currently sleeping-or-in-`inner`, and the running max.
        in_flight: Arc<std::sync::atomic::AtomicUsize>,
        max_in_flight: Arc<std::sync::atomic::AtomicUsize>,
        /// PUTs whose key contains any of these are counted as started (before
        /// the delay) and, separately, completed (only on a successful `inner`
        /// PUT). A `.abort()`ed task never reaches the completion increment.
        watch_prefixes: Vec<&'static str>,
        watch_started: Arc<std::sync::atomic::AtomicUsize>,
        watch_completed: Arc<std::sync::atomic::AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for InstrumentedPutStore {
        async fn put(
            &self,
            key: &str,
            data: bytes::Bytes,
            opts: ravel_object_store::PutOptions,
        ) -> Result<ravel_object_store::PutOutcome, ravel_object_store::StoreError> {
            use std::sync::atomic::Ordering::SeqCst;
            let is_data_object = key.contains("/l0/");
            let watched = self.watch_prefixes.iter().any(|p| key.contains(p));
            let delay = self
                .delays
                .iter()
                .find(|(p, _)| key.contains(p))
                .map(|(_, d)| *d);
            if is_data_object {
                let now = self.in_flight.fetch_add(1, SeqCst) + 1;
                self.max_in_flight.fetch_max(now, SeqCst);
            }
            if watched {
                self.watch_started.fetch_add(1, SeqCst);
            }
            if let Some(d) = delay {
                tokio::time::sleep(d).await;
            }
            let result = self.inner.put(key, data, opts).await;
            if is_data_object {
                self.in_flight.fetch_sub(1, SeqCst);
            }
            if watched && result.is_ok() {
                self.watch_completed.fetch_add(1, SeqCst);
            }
            result
        }

        async fn get(
            &self,
            key: &str,
            range: ravel_object_store::GetRange,
        ) -> Result<ravel_object_store::GetOutcome, ravel_object_store::StoreError> {
            self.inner.get(key, range).await
        }

        async fn put_multipart<'a>(
            &'a self,
            key: &str,
        ) -> Result<Box<dyn ravel_object_store::MultipartUpload + 'a>, ravel_object_store::StoreError>
        {
            self.inner.put_multipart(key).await
        }

        async fn head(
            &self,
            key: &str,
        ) -> Result<ravel_object_store::ObjectMeta, ravel_object_store::StoreError> {
            self.inner.head(key).await
        }

        async fn list(
            &self,
            prefix: &str,
            page: Option<ravel_object_store::PageToken>,
        ) -> Result<ravel_object_store::ListPage, ravel_object_store::StoreError> {
            self.inner.list(prefix, page).await
        }

        async fn list_delimited(
            &self,
            prefix: &str,
        ) -> Result<ravel_object_store::DelimitedList, ravel_object_store::StoreError> {
            self.inner.list_delimited(prefix).await
        }

        async fn delete(&self, key: &str) -> Result<(), ravel_object_store::StoreError> {
            self.inner.delete(key).await
        }

        fn capabilities(&self) -> ravel_object_store::Capabilities {
            self.inner.capabilities()
        }
    }

    /// `--pipeline-depth` is load-bearing, not accepted-and-ignored: the same
    /// stream run at depth 1 keeps at most one write outstanding, while at depth
    /// 3 up to three writes are genuinely in flight at once. Each batch is a
    /// single row routed to its own shard (so its flush runs on its own shard
    /// actor, and distinct batches' data-object PUTs can overlap); every
    /// data-object PUT sleeps 50ms while the store tracks the running maximum of
    /// concurrently outstanding data-object PUTs. Four rows over four shards
    /// split into four single-shard batches, submitted in turn.
    ///
    /// Non-vacuity (prove-the-test): the depth-1 arm asserts the observed max is
    /// exactly 1 and the depth-3 arm asserts it reaches 3. An implementation
    /// that accepted `--pipeline-depth` but still awaited each write inline
    /// before starting the next (today's behavior) would leave the max at 1 for
    /// both depths, failing the depth-3 assertion; confirmed by running the same
    /// body at both depths — `max_d1` is 1 and `max_d3` reaches 3 only because
    /// the write window is real.
    #[tokio::test]
    async fn pipeline_depth_bounds_concurrent_writes() {
        use parquet::arrow::ArrowWriter;
        use ravel_object_store::memory::MemoryStore;
        use std::sync::atomic::{AtomicUsize, Ordering};

        async fn max_concurrent_at_depth(depth: usize) -> usize {
            let shards = 4u32;
            // Each batch is one row on its own shard, so its flush runs on a
            // distinct shard actor and can overlap the others.
            let hosts: Vec<String> = (0..shards).map(|s| host_for_shard(s, shards)).collect();

            let dir = tempfile::tempdir().expect("tempdir");
            let pq = dir.path().join("depth.parquet");
            let cols: Vec<(String, ArrayRef)> = vec![
                ("ts".to_string(), i64_col(vec![NOW_NS; shards as usize])),
                ("svc".to_string(), str_col(vec!["api"; shards as usize])),
                (
                    "host".to_string(),
                    str_col(hosts.iter().map(|h| h.as_str()).collect()),
                ),
            ];
            let b = RecordBatch::try_from_iter(cols).expect("record batch");
            let file = std::fs::File::create(&pq).expect("create parquet");
            let mut writer = ArrowWriter::try_new(file, b.schema(), None).expect("arrow writer");
            writer.write(&b).expect("write batch");
            writer.close().expect("close writer");

            let m = parse_mapping(
                "ts_column = \"ts\"\nts_unit = \"nanos\"\n\n\
                 [[resource_attribute]]\nkey = \"service.name\"\ncolumn = \"svc\"\ntype = \"str\"\n\n\
                 [[resource_attribute]]\nkey = \"host\"\ncolumn = \"host\"\ntype = \"str\"\n",
            )
            .expect("valid mapping");

            let max_in_flight = Arc::new(AtomicUsize::new(0));
            let store = Arc::new(InstrumentedPutStore {
                inner: Arc::new(MemoryStore::new()),
                delays: vec![("/l0/", Duration::from_millis(50))],
                in_flight: Arc::new(AtomicUsize::new(0)),
                max_in_flight: Arc::clone(&max_in_flight),
                watch_prefixes: Vec::new(),
                watch_started: Arc::new(AtomicUsize::new(0)),
                watch_completed: Arc::new(AtomicUsize::new(0)),
            });

            let report = load(
                store as Arc<dyn ObjectStoreBackend>,
                &pq,
                "acme",
                &m,
                shards,
                1,
                None,
                depth,
                NOW_NS,
                Arc::new(FixedClock(NOW_NS)),
            )
            .await
            .expect("the load succeeds");
            assert_eq!(report.rows_processed, shards as u64, "every row is written");
            max_in_flight.load(Ordering::SeqCst)
        }

        let max_d1 = max_concurrent_at_depth(1).await;
        assert_eq!(
            max_d1, 1,
            "at --pipeline-depth 1 exactly one write is ever outstanding (today's behavior), \
             got {max_d1}"
        );

        let max_d3 = max_concurrent_at_depth(3).await;
        assert!(
            max_d3 >= 3,
            "at --pipeline-depth 3 the write window reaches three concurrently outstanding PUTs, \
             got {max_d3}"
        );
    }

    /// Durable-token correctness under a partial-window failure (the correctness
    /// constraint this ticket turns on): with `--pipeline-depth 4` and a stream
    /// of five single-shard batches, the data PUT for the *middle* batch (index
    /// 2, shard 2) is held ~300ms before failing permanently. The batches
    /// strictly after it (indices 3 and 4, shards 3 and 4) have no artificial
    /// delay at all, so their shard actors race far ahead of shard 2's held PUT
    /// and commit their objects *independently*, well before shard 2's failure
    /// is even detected -- because the loader routes each batch to its own
    /// shard actor and a shard actor's flush is downstream of the write future
    /// the loader holds, aborting that future's `JoinHandle` does not stop the
    /// shard actor already mid-PUT (see `LogIngestRouter::write` in
    /// `crates/ravel-ingest/src/log_router.rs`: the actor has no join handle of
    /// its own, only a channel). That independent, unstoppable success is
    /// exactly the trap: the reported durable list must still exclude them, even
    /// though the objects genuinely landed.
    ///
    /// Asserted: the reported durable list is exactly the tokens of the batches
    /// strictly before the failing one (shards 0 and 1), in submission order; it
    /// carries no token for the failing batch (a full single-shard PUT failure
    /// has no partial survivor) and none for the later batches -- even though
    /// `watch_completed` reading 2 proves batches 3 and 4's writes did in fact
    /// succeed and commit (the direct, non-inferred proof the spec requires:
    /// their objects landed in the underlying store, not merely "absent from
    /// the returned list"). This is the "even if their own write happens to
    /// succeed independently" clause made observable: the durable list grows
    /// only by consuming the queue oldest-first, never by whichever write
    /// finishes, and it stays correct even though the loader's own abort of the
    /// post-failure handles has no effect on whether those batches commit.
    ///
    /// Non-vacuity (prove-the-test): the discriminating shape is the *failing*
    /// batch being slow and a *post-failure* batch being fast, not the other way
    /// around. A wrong resolver that consumed the window via
    /// `select_all`/whichever-finishes-first instead of strict FIFO pop-front
    /// would, on this exact fixture, resolve shard 3's (and shard 4's) zero-delay
    /// success long before shard 2's ~300ms-delayed failure is ever observed --
    /// recording a shard-3 (or shard-4) token as durable, which the
    /// `durable.iter().all(|t| t.shard == 0 || t.shard == 1)` assertion rejects.
    /// The ordering is deterministic because a zero-delay in-memory PUT
    /// completes in microseconds while shard 2 is held 300ms: a 6000x margin
    /// leaves no scheduling jitter that could invert it. (An earlier version of
    /// this fixture delayed the *post-failure* batches instead of the failing
    /// one; that shape is vacuous -- delaying batches 3/4 stops them finishing
    /// early under any resolver, correct or not, so it can't distinguish FIFO
    /// from `select_all`. The failing batch must be the slow one.)
    #[tokio::test]
    async fn partial_window_failure_reports_only_earlier_batches_never_later() {
        use parquet::arrow::ArrowWriter;
        use ravel_object_store::fault::{
            FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
        };
        use ravel_object_store::memory::MemoryStore;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let shards = 5;
        // One row per batch (batch_rows = 1), each routed to its own shard, so
        // batch k is the sole write to shard k's `/l0/000k/` object.
        let hosts: Vec<String> = (0..shards).map(|s| host_for_shard(s, shards)).collect();

        let dir = tempfile::tempdir().expect("tempdir");
        let pq = dir.path().join("five_batches.parquet");
        let cols: Vec<(String, ArrayRef)> = vec![
            ("ts".to_string(), i64_col(vec![NOW_NS; shards as usize])),
            ("svc".to_string(), str_col(vec!["api"; shards as usize])),
            (
                "host".to_string(),
                str_col(hosts.iter().map(|h| h.as_str()).collect()),
            ),
        ];
        let b = RecordBatch::try_from_iter(cols).expect("five-row batch");
        let file = std::fs::File::create(&pq).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, b.schema(), None).expect("arrow writer");
        writer.write(&b).expect("write batch");
        writer.close().expect("close writer");

        let m = parse_mapping(
            "ts_column = \"ts\"\nts_unit = \"nanos\"\n\n\
             [[resource_attribute]]\nkey = \"service.name\"\ncolumn = \"svc\"\ntype = \"str\"\n\n\
             [[resource_attribute]]\nkey = \"host\"\ncolumn = \"host\"\ntype = \"str\"\n",
        )
        .expect("valid mapping");

        // Fail the middle batch's (shard 2) data PUT permanently; no retry, no
        // sibling shard, so no partial survivor.
        let plan = FaultPlan::empty().with_rule(
            Rule::new(
                Op::Put,
                ScriptedFault::Permanent("simulated permanent data-object PUT failure".into()),
            )
            .with_key_contains("/l0/0002/")
            .with_occurrence(Occurrence::Always),
        );
        let fault = Arc::new(FaultStore::new(MemoryStore::new(), plan));

        let watch_started = Arc::new(AtomicUsize::new(0));
        let watch_completed = Arc::new(AtomicUsize::new(0));
        let store = Arc::new(InstrumentedPutStore {
            inner: fault.clone() as Arc<dyn ObjectStoreBackend>,
            // Shard 2 (the failing batch) is held ~300ms before its permanent
            // fault fires. Shards 3 and 4 (after the failure) get no artificial
            // delay at all, so their zero-delay PUTs commit within microseconds
            // of being spawned -- long before shard 2's held failure surfaces.
            // This is the shape that discriminates FIFO from whichever-finishes-
            // first: delaying the *post-failure* batches instead would stop them
            // finishing early under any resolver and prove nothing (see the test
            // doc comment).
            delays: vec![("/l0/0002/", Duration::from_millis(300))],
            in_flight: Arc::new(AtomicUsize::new(0)),
            max_in_flight: Arc::new(AtomicUsize::new(0)),
            watch_prefixes: vec!["/l0/0003/", "/l0/0004/"],
            watch_started: Arc::clone(&watch_started),
            watch_completed: Arc::clone(&watch_completed),
        });

        let err = load(
            store as Arc<dyn ObjectStoreBackend>,
            &pq,
            "acme",
            &m,
            shards,
            1,
            None,
            4,
            NOW_NS,
            Arc::new(FixedClock(NOW_NS)),
        )
        .await
        .expect_err("the middle batch's permanent PUT failure fails the load");

        // Shards 3 and 4 have zero artificial delay, so by the time shard 2's
        // ~300ms-held failure surfaces (and `load` returns it) both have already
        // started AND completed their PUT -- a 6000x margin over a zero-delay
        // in-memory write, leaving no scheduling-jitter window that could catch
        // them mid-flight instead. This is the direct, non-inferred proof the
        // spec requires: the objects genuinely landed in the underlying store,
        // not merely "absent from the returned list" (which a lucky race could
        // satisfy even under a wrong implementation).
        let durable = match &err {
            LoadError::Flush { durable, .. } => durable.clone(),
            other => panic!("expected LoadError::Flush, got {other:?}"),
        };
        assert_eq!(
            durable.len(),
            2,
            "only the two batches strictly before the failing one are durable, got {durable:?}"
        );
        assert_eq!(
            durable[0].shard, 0,
            "first durable token is batch 0 (shard 0), in submission order"
        );
        assert_eq!(
            durable[1].shard, 1,
            "second durable token is batch 1 (shard 1), in submission order"
        );
        assert!(
            durable.iter().all(|t| t.shard == 0 || t.shard == 1),
            "no token from the failing batch (shard 2) or any later batch (shards 3, 4) is \
             reported durable, got {durable:?}"
        );

        assert_eq!(
            fault.fault_count(Op::Put, FaultKind::Permanent),
            1,
            "the permanent data-object PUT fault fired exactly once (shard 2, no retry)"
        );

        assert_eq!(
            watch_started.load(Ordering::SeqCst),
            2,
            "both after-failure writes (batches 3 and 4) reached their PUT"
        );
        assert_eq!(
            watch_completed.load(Ordering::SeqCst),
            2,
            "batches 3 and 4's writes did in fact succeed and commit -- the loader's abort of \
             their write handles only cancels its own ack wait, it does not stop the shard actor \
             already mid-PUT -- yet both are still excluded from the durable list above. Operators \
             resuming from a partial-window failure at depth > 1 must treat this as a known gap: \
             see the --pipeline-depth documentation in clickbench.md and the ravel-ingest \
             follow-up tracked from this test's discovery."
        );
    }
}
