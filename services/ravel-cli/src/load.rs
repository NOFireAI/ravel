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
    Clock, IngestConfig, LogIngestMetricsSnapshot, LogIngestRouter, SystemClock, WriteMode,
};
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
pub async fn run(
    store: Arc<dyn ObjectStoreBackend>,
    parquet_path: &Path,
    tenant: &str,
    mapping_path: &Path,
    shards: u32,
    batch_rows: usize,
    now_ns: i64,
) -> anyhow::Result<()> {
    run_warning_to(
        store,
        parquet_path,
        tenant,
        mapping_path,
        shards,
        batch_rows,
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
        now_ns,
        Arc::new(SystemClock),
    )
    .await
    {
        Ok(report) => {
            print_summary(&report);
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
        now_ns,
        clock,
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
    now_ns: i64,
    clock: Arc<dyn Clock>,
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
        .with_batch_size(batch_rows)
        .build()
        .map_err(|e| LoadError::Setup(format!("failed to build Parquet reader: {e}")))?;

    let started = Instant::now();
    let mut report = LoadReport::default();

    // Single-batch lookahead (issue #541). Batch N+1's decode/build (sync
    // Parquet decode via `Iterator::next`, then the per-row `build_record`
    // loop, both CPU-bound) runs on a `spawn_blocking` task started *before*
    // batch N's `router.write` I/O is awaited, so N+1's CPU work overlaps N's
    // I/O wait. The non-`Clone` `ParquetRecordBatchReader` is shuttled into and
    // back out of the closure each iteration.
    //
    // The loop stays sequential in every observable way: exactly one
    // `router.write` is awaited at a time, N+1's decode is never started before
    // N's has been consumed, and results are consumed in the same order as the
    // former serial loop. In particular a build error for batch N+1 (discovered
    // while N's write is still in flight) is only inspected after N's write
    // result has been handled, so it surfaces in the same order and with the
    // same `durable` tokens (those from batches strictly before it) as before;
    // the prefetch changes only *when* (wall-clock) the decode/build happens.
    let mapping = Arc::new(mapping.clone());
    let mut row_base: u64 = 0;

    // Spawn the decode/build of the batch starting at `row_base`, moving the
    // reader in; the task hands it back with the outcome.
    let spawn_build = |reader: BatchReader, row_base: u64| {
        let mapping = Arc::clone(&mapping);
        let limits = limits.clone();
        let hook = on_build_start.clone();
        tokio::task::spawn_blocking(move || {
            decode_and_build(reader, mapping, limits, now_ns, row_base, hook.as_ref())
        })
    };

    // Prime the pipeline with batch 0.
    let mut pending = spawn_build(reader, row_base);

    loop {
        let (reader, built) = match pending.await {
            Ok(pair) => pair,
            // A panic in the decode/build task is a batch decode failure;
            // earlier batches may already be durable.
            Err(join_err) => {
                return Err(LoadError::BatchFailed {
                    reason: format!("Parquet decode/build task failed: {join_err}"),
                    durable: report.tokens.clone(),
                });
            }
        };

        let (records, num_rows) = match built {
            Prefetched::Done => break,
            Prefetched::BatchFailed { reason } => {
                return Err(LoadError::BatchFailed {
                    reason,
                    durable: report.tokens.clone(),
                });
            }
            Prefetched::RowRejected { row, reason } => {
                return Err(LoadError::RowRejected {
                    row,
                    reason,
                    durable: report.tokens.clone(),
                });
            }
            Prefetched::Batch { records, num_rows } => (records, num_rows),
        };

        let next_row_base = row_base + num_rows as u64;

        if records.is_empty() {
            // A zero-row batch writes nothing; advance and prefetch the next.
            row_base = next_row_base;
            pending = spawn_build(reader, next_row_base);
            continue;
        }

        let n = records.len() as u64;
        // Start (but do not yet await) this batch's write, then spawn the next
        // batch's decode/build so its CPU work overlaps this write's I/O wait.
        // Building the future does no work; the write begins when it is awaited
        // below, by which point the prefetch task is already running.
        let write_fut = router.write(
            tenant_id.clone(),
            records,
            WriteMode::Strict,
            WRITE_ACK_DEADLINE,
        );
        pending = spawn_build(reader, next_row_base);
        row_base = next_row_base;

        let receipt = write_fut.await.map_err(|e| {
            // Earlier batches are already durable; append this batch's own
            // durably-acked shards, which `LogWriteError::durable_tokens`
            // recovers from a multi-shard partial failure (issue #296).
            let mut durable = report.tokens.clone();
            durable.extend_from_slice(e.durable_tokens());
            LoadError::Flush {
                durable,
                cause: e.to_string(),
            }
        })?;
        report.rows_processed += n;
        report.tokens.extend(receipt.tokens);
    }

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

/// The Parquet reader shuttled through each decode/build task. It owns
/// file-reading state and is not `Clone`, so a single instance is moved into
/// the blocking closure and handed back with the outcome each iteration.
type BatchReader = parquet::arrow::arrow_reader::ParquetRecordBatchReader;

/// One prefetched batch's decode/build outcome. Errors carry only the reason
/// (and, for a rejected row, its absolute index): the `durable` token list is
/// attached by the loop when it *consumes* the outcome, after every earlier
/// batch's write has resolved, so the reported tokens are exactly those durable
/// from batches strictly before the failure regardless of when (wall-clock) the
/// decode ran.
enum Prefetched {
    /// The reader is exhausted; no batch was produced.
    Done,
    /// A batch decoded and built into `records`. `num_rows` is the source row
    /// count; it equals `records.len()` since every non-rejected row yields one
    /// record (a rejection returns `RowRejected` instead of a partial batch).
    Batch {
        records: Vec<NormalizedLogRecord>,
        num_rows: usize,
    },
    /// The batch failed to read from Parquet or to resolve against the mapping.
    BatchFailed { reason: String },
    /// A row failed a kept admission check. `row` is the absolute row index.
    RowRejected { row: u64, reason: String },
}

/// Pull the next batch from `reader` and build its records, handing the reader
/// back for the next iteration. This is the pipeline's CPU-bound half (sync
/// Parquet decode plus the per-row `build_record` loop), run on a
/// `spawn_blocking` task so it overlaps the previous batch's `router.write` I/O
/// wait (issue #541). `on_build_start` fires once, before the per-row loop, only
/// when there is a batch to build.
fn decode_and_build(
    mut reader: BatchReader,
    mapping: Arc<Mapping>,
    limits: LogIngestLimits,
    now_ns: i64,
    row_base: u64,
    on_build_start: Option<&BuildStartHook>,
) -> (BatchReader, Prefetched) {
    let Some(batch) = reader.next() else {
        return (reader, Prefetched::Done);
    };
    if let Some(hook) = on_build_start {
        hook();
    }
    let batch = match batch {
        Ok(batch) => batch,
        Err(e) => {
            return (
                reader,
                Prefetched::BatchFailed {
                    reason: format!("failed to read Parquet batch: {e}"),
                },
            );
        }
    };
    // Resolve column indices once per batch (schema is stable across batches,
    // but re-resolving keeps this self-contained and cheap).
    let cols = match ColumnIndex::resolve(&batch, &mapping) {
        Ok(cols) => cols,
        Err(reason) => return (reader, Prefetched::BatchFailed { reason }),
    };
    let mut records = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        match build_record(&batch, &cols, &mapping, &limits, now_ns, row) {
            Ok(record) => records.push(record),
            Err(reason) => {
                return (
                    reader,
                    Prefetched::RowRejected {
                        row: row_base + row as u64,
                        reason,
                    },
                );
            }
        }
    }
    let num_rows = batch.num_rows();
    (reader, Prefetched::Batch { records, num_rows })
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
            NOW_NS,
            Arc::new(FixedClock(NOW_NS)),
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
    /// into the whole file, not an index relative to that batch. `row_base` is
    /// now threaded through `decode_and_build`, a free function outside the
    /// loop the prefetch refactor introduced, and easy to drop by accident
    /// (no existing test drove a real multi-batch `load()` far enough to catch
    /// it: `future_skew_beyond_the_bound_is_rejected` calls `build_record`
    /// directly on row 0, and `batch_failed_reports_durable_tokens_not_empty`
    /// hand-constructs a `LoadError` rather than running a load). Change
    /// `row_base + row as u64` to `row as u64` in `decode_and_build`'s
    /// `RowRejected` arm and this fails with `left: 2, right: 0`.
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
    }
}
