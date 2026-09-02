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
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use ravel_catalog::{AbsentPolicy, validate_or_adopt};
#[cfg(feature = "stage-timing")]
use ravel_ingest::LogStageSnapshot;
use ravel_ingest::{
    Clock, FlushTriggerMix, IngestConfig, LogIngestMetricsSnapshot, LogIngestRouter, LogWriteError,
    LogWriteReceipt, STRICT_VISIBILITY_RESERVE_NS, SystemClock, WriteMode,
};
use ravel_logseg::{
    Bitmap, ColumnarLogBatch, DynColumn, FieldType, StrColumnDict, stream_attrs_bytes,
};
use ravel_object_store::ObjectStoreBackend;
use ravel_otlp::NormalizedLogRecord;
use ravel_otlp::logs_limits::LogIngestLimits;
use ravel_types::logstream::{AttrValue, LogStreamId, log_stream_id};
use ravel_types::{CommitToken, Signal, TenantId};
use serde::{Deserialize, Serialize};

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

/// Default flush target size, in bytes, for a shard's buffer. `1` makes every
/// Strict write flush inside its own `handle_write`, so one batch is one RLOG
/// object per involved shard and every ack is answered by that write's own
/// flush. A larger value lets a shard hold several batches' records in one
/// buffer before it flushes, which defers those earlier batches' acks (see
/// [`load_instrumented`]).
///
/// The target is compared against the shard buffer's *estimated in-memory
/// footprint* (`est_bytes`), not against encoded RLOG bytes, and the comparison
/// runs once per write after a whole batch's slice has merged, so a target at or
/// below one batch's per-shard slice produces exactly the layout `1` does. See
/// [`target_bytes_no_effect_warning`] for the arithmetic and for what the loader
/// reports when a target turns out to change nothing.
pub const DEFAULT_TARGET_BYTES: usize = 1;

/// Default number of decoded batches allowed to sit queued between the Parquet
/// decode/build stage and the shard-write stage (issue #680). The decoder runs
/// ahead by up to this many batches while the encoders drain earlier ones, so
/// decode and encode overlap instead of running in lockstep. Bounds the memory
/// the queue holds to roughly this count times one batch's built size; stacks
/// with `--pipeline-depth`'s own in-flight-write working set.
pub const DEFAULT_DECODE_QUEUE_BATCHES: usize = 2;

/// Default number of Strict batch writes the loader keeps outstanding (issues
/// #800, #807), the `--pipeline-depth` lever and the OUTER of the two write
/// concurrency windows.
///
/// At `1` the submit loop awaits a batch's every-shard ack before it submits the
/// next, so each batch's encode, data PUT, and commit-record PUT are serial with
/// every other batch's, and the machine has nothing to run in between. Measured
/// on the logs pipeline's own per-shard skew counters (issue #865), that idle is
/// where a bulk load's wall goes: the shards report their whole flush duration
/// as `off_actor_ns` and a `flush_permit_wait_ns` of exactly zero, so the flush
/// tier is never the constraint at depth 1 -- the loader simply stops asking it
/// for work.
///
/// `4` is chosen, not `--shards` and not higher:
///
/// - It is the largest depth with a *measured* result behind it. ADR-0807
///   measured `--pipeline-depth 4 --max-inflight-flushes 4` at 1,519.75 s
///   against 4,466.76 s at `1`/`1` on the 100M-row ClickBench corpus, with the
///   object count unchanged at 8,424. A depth of 16 with the inner window left
///   at 1 aborted outright with `timed out waiting for shard ack`, because a
///   depth the inner window cannot absorb just queues batches behind the
///   [`write_ack_deadline`].
/// - It bounds the memory cost at a stated multiple rather than at the shard
///   count, which is a provisioning decision an operator may set far above 4.
///   The loader holds at most `--pipeline-depth` built batches for their
///   in-flight writes, plus `--decode-queue-batches` queued ahead, so this
///   default takes the resident batch working set from `1 + 2` to `4 + 2`
///   batches of `--batch-rows` rows.
/// - It is at least the default `--shards` (4), so every shard of a
///   default-provisioned signal can hold a write at once.
pub const DEFAULT_PIPELINE_DEPTH: usize = 4;

/// Default per-shard bound on concurrently in-flight flushes (issues #800,
/// #807), the `--max-inflight-flushes` lever and the INNER of the two write
/// concurrency windows.
///
/// Pinned to [`DEFAULT_PIPELINE_DEPTH`] by
/// `default_max_inflight_flushes_matches_pipeline_depth`, because the two
/// windows compose as `shards * min(pipeline_depth, max_inflight_flushes)`
/// (ADR-0807): an inner window below the outer one re-serialises each shard's
/// PUT round trips and makes batches queue behind a semaphore they will still
/// have to clear before [`write_ack_deadline`] elapses, and an inner window
/// above the outer one is unreachable, since the loader never hands any shard
/// more concurrent work than `--pipeline-depth` batches.
///
/// Unlike the outer window this one costs no additional memory on the bulk path.
/// The resident flush working set is whatever the outstanding batches carry, and
/// `--pipeline-depth` already caps that; this knob only decides whether that
/// same bounded set of objects is encoded and PUT concurrently or one at a time.
/// (On `ravel-server` the same field does bound memory, because there is no
/// outer window upstream of it; ADR-0067 decision 2 governs that default, which
/// this does not change.)
///
/// This deliberately no longer tracks [`IngestConfig::max_inflight_flushes`]'s
/// own default of 1: that default governs the client-facing serving path, whose
/// Strict ack contract ADR-0067 froze, and the bulk loader is a different
/// workload with a different memory owner.
pub const DEFAULT_MAX_INFLIGHT_FLUSHES: u32 = DEFAULT_PIPELINE_DEPTH as u32;

/// Build the router [`IngestConfig`] a load drives, given the three
/// operator-facing flush levers.
///
/// `max_flush_delay` is `None` when `--max-flush-delay` is unset: the field is
/// then left at its [`IngestConfig::default`] value, so an unset flag produces
/// a byte-for-byte default config and changes nothing. `Some(d)` overrides only
/// the router's age trigger, the third binding constraint on object layout
/// beside `target_bytes` and a batch's per-shard slice footprint (issue #801):
/// a shard buffer flushes when it reaches `target_bytes`, when its oldest point
/// ages past `max_flush_delay`, or at the final drain. At the default 2s a
/// buffer that fills slower than one target's worth every 2s is released by age
/// before it ever reaches a large `target_bytes`, so a bulk load that wants
/// target-sized objects must raise this delay past the time one target takes to
/// fill.
pub(crate) fn build_ingest_config(
    shards: u32,
    target_bytes: usize,
    max_inflight_flushes: u32,
    max_flush_delay: Option<Duration>,
) -> IngestConfig {
    let delay = max_flush_delay.unwrap_or_else(|| IngestConfig::default().max_flush_delay);
    IngestConfig {
        shard_count: shards,
        target_bytes,
        max_inflight_flushes,
        max_flush_delay: delay,
        // ADR-0076 decision 4: follows the actually-configured
        // `max_flush_delay`, not just its default, so the adaptive corridor
        // never contradicts the configured cadence. Must EXCEED the delay by
        // the same reserve `IngestConfig::default()` uses; setting it equal
        // collapses the corridor to its floor. Same derivation as
        // `services/ravel-server/src/lib.rs`'s router construction.
        strict_visibility_budget_ns: i64::try_from(delay.as_nanos())
            .unwrap_or(i64::MAX)
            .saturating_add(STRICT_VISIBILITY_RESERVE_NS),
        ..IngestConfig::default()
    }
}

/// Floor for a Strict write's ack deadline. Generous: a bulk load values
/// completing over racing a slow store.
const WRITE_ACK_DEADLINE_FLOOR: Duration = Duration::from_secs(60);

/// Headroom added over `--max-flush-delay` by [`write_ack_deadline`]: the
/// window the released flush still needs to encode and PUT its object once the
/// age trigger has opened it.
const WRITE_ACK_DEADLINE_MARGIN: Duration = Duration::from_secs(60);

/// Ack deadline for each Strict write, scaled to the configured age trigger.
///
/// A write whose shard buffer never reaches `--target-bytes` is answered by the
/// age trigger, so its ack can legitimately take `--max-flush-delay` plus the
/// flush itself. A fixed 60s deadline therefore turns any raised delay into a
/// `LogWriteError::AckTimeout` on the very batches the raised delay was meant
/// to let accumulate, failing a whole load at its documented settings. Scaling
/// keeps the deadline what it always was for an unset flag
/// ([`WRITE_ACK_DEADLINE_FLOOR`] exactly) while leaving a raised delay one
/// [`WRITE_ACK_DEADLINE_MARGIN`] of room past the trigger it configured.
pub(crate) fn write_ack_deadline(max_flush_delay: Option<Duration>) -> Duration {
    match max_flush_delay {
        None => WRITE_ACK_DEADLINE_FLOOR,
        Some(delay) => {
            WRITE_ACK_DEADLINE_FLOOR.max(delay.saturating_add(WRITE_ACK_DEADLINE_MARGIN))
        }
    }
}

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

/// Warning for a load whose `--target-bytes` above [`DEFAULT_TARGET_BYTES`] laid
/// the objects out exactly as `1` would have (issue #971). `None` when the
/// target was the default, when some buffer did span several writes, or when no
/// shard ever received two writes to accumulate in the first place.
///
/// Why a target of a few MiB is a no-op on a wide corpus. The value reaches
/// `IngestConfig::target_bytes` unmodified and the shard actor does consult it,
/// but against `est_bytes`: the buffer's *estimated in-memory footprint*
/// (`est_record_bytes`/`est_columnar_bytes` in
/// crates/ravel-ingest/src/log_shard.rs), where every attribute occurrence
/// charges a `size_of::<(String, AttrValue)>()` pair header plus its key bytes
/// and its uncompressed value bytes. For the 104-column ClickBench mapping that
/// is roughly 8 KB per row, against objects the same load writes at a bit over
/// 100 bytes per row. A target read off an observed object size is therefore
/// tens of times below the footprint of the rows that object holds. On top of
/// that the comparison runs once per write, after a whole batch's per-shard
/// slice has merged, so any target at or below one slice's footprint
/// (`--batch-rows / --shards` rows' worth) is already exceeded by the first
/// write into an empty buffer and flushes it, exactly as `1` does.
///
/// The loader cannot compute that footprint before it runs, so this reports the
/// outcome from figures [`LoadReport`] already carries. `tokens` holds one entry
/// per (batch, shard) Strict ack, and a flush that answered several batches
/// repeats its own token once per batch, so `objects_written() == tokens.len()`
/// means no buffer ever spanned two writes. That is evidence about the target
/// only if some shard took at least two writes, which is the second condition.
pub fn target_bytes_no_effect_warning(
    target_bytes: usize,
    report: &LoadReport,
    batch_rows: usize,
    shards: u32,
) -> Option<String> {
    if target_bytes <= DEFAULT_TARGET_BYTES {
        return None;
    }
    let writes = report.tokens.len();
    let objects = report.objects_written();
    if objects < writes {
        // At least one flush answered more than one batch: the target held a
        // buffer open, which is what it is for.
        return None;
    }
    let mut writes_per_shard: std::collections::HashMap<u32, usize> =
        std::collections::HashMap::new();
    for token in &report.tokens {
        *writes_per_shard.entry(token.shard).or_default() += 1;
    }
    if writes_per_shard.values().copied().max().unwrap_or(0) < 2 {
        // No shard was written twice, so nothing could have accumulated at any
        // target. Saying the target did nothing would blame the wrong lever.
        return None;
    }
    Some(format!(
        "warning: --target-bytes {target_bytes} did not change this load's object layout. All \
         {writes} (batch, shard) writes flushed as their own object ({objects} objects), which is \
         what --target-bytes 1 produces, and at least one shard took two or more writes without \
         accumulating them. This is the OBSERVED layout, not proof the target was the lever: \
         with --pipeline-depth 1, or when the gap between a shard's writes exceeds the \
         max-flush-delay clock, the age trigger flushes a waiting buffer before the next \
         batch arrives and no target value can make it accumulate -- check the pipeline \
         depth and write cadence before changing the target. Separately, the target is compared against the shard buffer's ESTIMATED in-memory \
         footprint, not the encoded object size: every attribute occurrence charges a {pair}-byte \
         (name, value) pair header plus its key and uncompressed value bytes, plus the \
         stream-attribute blob and 32 bytes per row, and the check runs once per write after a \
         whole batch has merged. So a target at or below one batch's per-shard slice (about \
         {slice} rows here, at --batch-rows {batch_rows} over {shards} shards) is already exceeded \
         by the first write into an empty buffer and flushes it. For objects that span several \
         batches, raise --target-bytes above that slice's estimated footprint, or lower \
         --batch-rows.",
        pair = size_of::<(String, AttrValue)>(),
        slice = batch_rows / (shards.max(1) as usize),
    ))
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
    max_inflight_flushes: u32,
    decode_queue_batches: usize,
    target_bytes: usize,
    max_flush_delay: Option<Duration>,
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
        max_inflight_flushes,
        decode_queue_batches,
        target_bytes,
        max_flush_delay,
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
    max_inflight_flushes: u32,
    decode_queue_batches: usize,
    target_bytes: usize,
    max_flush_delay: Option<Duration>,
    now_ns: i64,
    warnings: &mut dyn std::io::Write,
) -> anyhow::Result<()> {
    // A diagnostic that cannot be written is not worth failing a durable load
    // over, here or below.
    let _ = writeln!(warnings, "{ADMISSION_BYPASS_WARNING}");

    let mapping_text = std::fs::read_to_string(mapping_path)
        .map_err(|e| anyhow::anyhow!("failed to read --mapping {}: {e}", mapping_path.display()))?;
    let mapping = parse_mapping(&mapping_text)?;

    // The production entry point drives the columnar fast path (ADR-0109) with
    // the operator-configured decode-queue depth; `load` keeps a stable
    // signature for tests and callers that want the default depth.
    match load_instrumented(
        store,
        parquet_path,
        tenant,
        &mapping,
        shards,
        batch_rows,
        read_cursors,
        pipeline_depth,
        max_inflight_flushes,
        decode_queue_batches,
        target_bytes,
        max_flush_delay,
        now_ns,
        Arc::new(SystemClock),
        LoadPath::Columnar,
        None,
        None,
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
            // A `--target-bytes` above the default that laid the objects out
            // exactly as the default would have is reported rather than left
            // silent (issue #971): the flag's threshold is a footprint estimate
            // the operator cannot see, so the only honest place to state that
            // the value did nothing is after the load that proves it.
            if let Some(warning) =
                target_bytes_no_effect_warning(target_bytes, &report, batch_rows, shards)
            {
                let _ = writeln!(warnings, "{warning}");
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
    print_flush_mix(report);
    #[cfg(feature = "stage-timing")]
    print_stage_timings(report);
}

/// Print the per-shard flush trigger mix (issue #983) under the summary totals.
/// Object count is not a function of the command line, so the mix that produced
/// it is what makes two loads of the same input comparable. A load whose
/// metrics carry no per-shard mix (a router that never flushed) prints nothing
/// rather than a zeroed line.
fn print_flush_mix(report: &LoadReport) {
    let mix = report.flush_mix_report();
    if mix.shards.is_empty() {
        return;
    }
    let t = &mix.totals;
    println!(
        "  flush triggers   : size {}, age {}, final {} (total {})",
        t.size,
        t.age,
        t.final_drain,
        t.total(),
    );
    for s in &mix.shards {
        println!(
            "    shard {:>3}      : size {}, age {}, final {} (total {})",
            s.shard,
            s.counts.size,
            s.counts.age,
            s.counts.final_drain,
            s.counts.total(),
        );
    }
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
    /// One token per shard acked, across every batch, in submission order. At
    /// the default `--target-bytes 1` that is one token per object written; at
    /// a larger target one flush answers several batches' acks with the same
    /// token, so the list repeats it once per batch that flush carried. Use
    /// [`LoadReport::objects_written`] for the object count.
    pub tokens: Vec<CommitToken>,
    pub elapsed: Duration,
    /// Wall time the submit loop spent blocked waiting for a decoded batch to
    /// arrive from the decode/build stage (issue #800). Together with
    /// [`LoadReport::write_wait`] this partitions the loop's own wall clock into
    /// the two things it can be waiting on, so "the load is slow" resolves to a
    /// side without guessing: a large `decode_wait` means the single decoder
    /// task is the constraint, a large `write_wait` means the write path is.
    ///
    /// Bracketed on [`Instant`], not the injected `Clock`: these are
    /// measurements of the loader process, and a test clock (which the loader
    /// itself never installs) would report them as zero.
    pub decode_wait: Duration,
    /// Wall time the submit loop spent blocked resolving an in-flight write
    /// (issue #800). At `--pipeline-depth 1` this is the cross-batch barrier:
    /// the loop resolves each batch's every-shard ack before submitting the
    /// next, so this figure approaches the whole load and nothing else runs
    /// while it accrues. Raising the depth is what turns it back into overlap.
    pub write_wait: Duration,
    /// The router's cumulative write metrics, snapshotted once the load
    /// finished. Carries the dynamic-column counters (ADR-0100 decision 1) the
    /// caller reads to emit an overflow or near-cap warning; there is no other
    /// return path for a per-load signal (`LogIngestRouter::metrics()` is only
    /// reachable by whoever constructed the router, which is `load` itself).
    pub metrics: LogIngestMetricsSnapshot,
    /// Per-shard flush counts split by trigger cause (size / age / final drain),
    /// snapshotted with `metrics` once the load finished (issue #983). This is
    /// the honest basis for comparing two loads of the same input: the raw
    /// object count is not a function of the command line (input order
    /// concentrates consecutive rows on one shard, and the 2-second age trigger
    /// makes host speed change the layout), but the trigger mix that produced it
    /// is. Sorted by shard index; a shard that never flushed is absent. Empty on
    /// a tenant loaded before this change, which is not an error.
    pub flush_trigger_mix: Vec<(u32, FlushTriggerMix)>,
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
    /// Distinct commit tokens in [`LoadReport::tokens`], which is the number of
    /// RLOG objects the load wrote. Counting the list's length instead would
    /// report batches-times-shards, which only equals the object count while
    /// every write gets its own flush (`--target-bytes 1`).
    pub fn objects_written(&self) -> usize {
        let mut seen = std::collections::HashSet::new();
        self.tokens
            .iter()
            .filter(|t| seen.insert(t.encode()))
            .count()
    }

    /// The machine-readable per-shard flush trigger mix plus its totals (issue
    /// #983), the serializable projection of [`LoadReport::flush_trigger_mix`].
    /// Empty `shards` on a tenant loaded before this change, which is not an
    /// error.
    pub fn flush_mix_report(&self) -> FlushMixReport {
        let shards: Vec<ShardFlushMix> = self
            .flush_trigger_mix
            .iter()
            .map(|(shard, mix)| ShardFlushMix {
                shard: *shard,
                counts: FlushMixCounts {
                    size: mix.size,
                    age: mix.age,
                    final_drain: mix.final_drain,
                },
            })
            .collect();
        let mut totals = FlushMixCounts::default();
        for s in &shards {
            totals.size += s.counts.size;
            totals.age += s.counts.age;
            totals.final_drain += s.counts.final_drain;
        }
        FlushMixReport { shards, totals }
    }
}

/// Flush counts split by trigger cause: how many flushes each of the three
/// disjoint triggers opened (issue #983). The serializable counterpart of
/// [`ravel_ingest::FlushTriggerMix`], carried per shard and as load totals. The
/// `final` drain is serialized under that name (a Rust keyword, so the field is
/// `final_drain`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlushMixCounts {
    /// Flushes opened because a shard buffer reached `--target-bytes`.
    pub size: u64,
    /// Flushes opened because a shard buffer aged past `max_flush_delay`.
    pub age: u64,
    /// Flushes opened by the final drain at load close.
    #[serde(rename = "final")]
    pub final_drain: u64,
}

impl FlushMixCounts {
    /// The flushes-opened count, the sum of the three disjoint causes. On a load
    /// that abandons nothing this equals the objects written.
    pub fn total(&self) -> u64 {
        self.size + self.age + self.final_drain
    }
}

/// One shard's flush trigger mix, keyed by shard index (issue #983).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardFlushMix {
    pub shard: u32,
    #[serde(flatten)]
    pub counts: FlushMixCounts,
}

/// The load's per-shard flush trigger mix and its totals (issue #983), the
/// machine-readable form of the same figures [`print_summary`] prints. Object
/// count is not a function of the command line, so this states the trigger mix
/// that produced it, which is comparable between two loads of the same input.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FlushMixReport {
    /// One row per shard that flushed, sorted by shard index. Empty on a tenant
    /// loaded before issue #983, which is not an error.
    pub shards: Vec<ShardFlushMix>,
    /// The size / age / final counts summed across every shard.
    pub totals: FlushMixCounts,
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

    /// The durable-token list, for a caller that still has outstanding writes to
    /// fold into it ([`harvest_after_failure`]). `None` for
    /// [`LoadError::Setup`], which carries no list because it never occurs once
    /// a batch could have flushed.
    fn durable_tokens_mut(&mut self) -> Option<&mut Vec<CommitToken>> {
        match self {
            LoadError::Setup(_) => None,
            LoadError::BatchFailed { durable, .. }
            | LoadError::RowRejected { durable, .. }
            | LoadError::Flush { durable, .. } => Some(durable),
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
/// - `batch_rows` rows are written per Strict write. At this entry point's
///   fixed [`DEFAULT_TARGET_BYTES`] flush target that is also one flush, and so
///   one RLOG object per involved shard; [`load_instrumented`] takes the target
///   as a parameter and documents what a larger one changes.
/// - `pipeline_depth` is the outer write window; [`DEFAULT_PIPELINE_DEPTH`] is
///   what the CLI passes. The per-shard flush window is
///   [`DEFAULT_MAX_INFLIGHT_FLUSHES`] and the decode-queue depth
///   [`DEFAULT_DECODE_QUEUE_BATCHES`]; [`load_instrumented`] is the seam that
///   takes either as a parameter.
/// - `now_ns` is the ingest-time anchor for the future-skew check (the past-lag
///   check is deliberately omitted per ADR-0089). Bucketing is by the router's
///   own clock (load-time wall clock), independent of the records' event times.
///
/// Fail-fast on the first row that fails a kept admission check. A run that
/// returns `Ok` has every row durable; a run that returns `Err` reports the
/// tokens durable from batches that completed before the failure, plus any
/// shard of the failing batch that acked durable before a sibling shard failed
/// (recovered from the router error, issue #296), plus whatever any batch
/// submitted after the failing one had already committed by the time its write
/// resolved ([`harvest_after_failure`], issue #800) — a partial load, re-running
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
        DEFAULT_MAX_INFLIGHT_FLUSHES,
        DEFAULT_DECODE_QUEUE_BATCHES,
        DEFAULT_TARGET_BYTES,
        None,
        now_ns,
        clock,
        LoadPath::Columnar,
        None,
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

/// [`load`] with the decode/build hooks and the decode-queue depth injected. See
/// [`load`] for the contract; `on_build_start` fires once per batch that has
/// data to build, on the blocking decode/build task, before the per-row loop.
/// `on_batch_queued` fires once per built batch after it is handed to the
/// decode->encode channel (issue #680), so a test can observe how far the
/// decoder has run ahead of the encoders while a write is held. Both are `None`
/// in production.
///
/// `target_bytes` is the shard buffer's flush target (`--target-bytes`). At `1`
/// every Strict write flushes inside its own `handle_write`, so a write's ack is
/// answered by its own flush. Above that, a shard holds several batches'
/// records in one buffer, so an earlier batch's ack is not answered until a
/// later batch pushes the buffer over the target or the router's age trigger
/// (`max_flush_delay`) fires. The ack still means durable when it arrives; it
/// just arrives later, and the loader's in-flight window must be wide enough to
/// submit the batch that releases it (see the flush-target comment inside).
///
/// The target is measured in the router's estimated buffered footprint and is
/// tested once per write, so a value at or below one batch's per-shard slice
/// changes nothing at all; [`target_bytes_no_effect_warning`] documents that
/// arithmetic and is what reports the case to the operator.
///
/// `max_flush_delay` is the `--max-flush-delay` lever (`None` = the
/// [`IngestConfig::default`] age trigger). It is the third constraint on when a
/// buffer flushes, beside `target_bytes` and the slice footprint: a buffer that
/// does not reach `target_bytes` within this delay is released by the age
/// trigger, so a large `target_bytes` is only reachable once this is raised past
/// the time one target's worth accumulates. See [`build_ingest_config`]. It also
/// scales every write's ack deadline ([`write_ack_deadline`]), so a buffer the
/// age trigger releases late is still awaited rather than timed out.
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
    max_inflight_flushes: u32,
    decode_queue_batches: usize,
    target_bytes: usize,
    max_flush_delay: Option<Duration>,
    now_ns: i64,
    clock: Arc<dyn Clock>,
    path: LoadPath,
    on_build_start: Option<BuildStartHook>,
    on_batch_queued: Option<BuildStartHook>,
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
    // Same shape as the guards above: `--max-inflight-flushes` is the
    // operator-facing lever bounding how many flushes one shard may run at once
    // (issue #807). A bound of 0 is a semaphore no flush can ever acquire, so
    // the shard actor would park on its first flush trigger forever; reject it
    // here rather than silently clamping to 1.
    if max_inflight_flushes == 0 {
        return Err(LoadError::Setup(
            "--max-inflight-flushes must be at least 1 (the number of flushes one shard may have \
             in flight at once); 0 would deadlock every flush, since a shard could never acquire \
             a permit to run one"
                .to_string(),
        ));
    }
    // Same shape as the guards above: `--decode-queue-batches` is the
    // operator-facing lever bounding how many decoded batches may sit queued
    // ahead of the encoders (issue #680). A depth of 0 is a channel that can
    // hold no batch, so it is rejected rather than silently clamped.
    if decode_queue_batches == 0 {
        return Err(LoadError::Setup(
            "--decode-queue-batches must be at least 1 (the number of decoded batches allowed to \
             queue ahead of the shard writers); 0 was given"
                .to_string(),
        ));
    }
    // Same shape as the guards above: `--target-bytes` is the operator-facing
    // flush-target lever (issue #801). A target of 0 is not a smaller target
    // than 1, it is the same one (`est_bytes >= 0` holds for an empty buffer),
    // so it is rejected rather than silently behaving as 1.
    if target_bytes == 0 {
        return Err(LoadError::Setup(
            "--target-bytes must be at least 1 (1 flushes every batch as its own object); 0 was \
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

    // `target_bytes: 1` (the `--target-bytes` default) makes each Strict batch
    // flush immediately as one RLOG object, inside `handle_write`'s size
    // trigger, rather than waiting on the age trigger's `max_flush_delay`
    // clock. That is what a bulk loader wants by default: one object per batch,
    // `batch_rows` controls its size, and every write's ack is durable with no
    // lingering buffer. It also keeps flush timing off the wall clock, so the
    // object buckets by the flush-open reading directly.
    //
    // A larger target keeps the durability meaning of an ack (it is still sent
    // from `ack_waiters` only after that flush's object and commit record are
    // published) but drops the other three: a shard's buffer now spans several
    // batches, so a batch's ack waits for whichever later batch pushes the
    // buffer over the target, and mid-load a buffer that never reaches the
    // target is released by the wall-clock age trigger instead (at the end of
    // the input the `flush_all` below releases it, since no later batch is
    // coming). The loader's in-flight window must therefore be wide enough to
    // hold the batches that accumulate into one flush, or every flush waits out
    // `max_flush_delay`.
    //
    // That age trigger (`max_flush_delay`, the `--max-flush-delay` lever) is the
    // THIRD binding constraint on object layout, beside `target_bytes` and one
    // batch's per-shard slice footprint. At its 2s default a shard buffer that
    // fills slower than one target's worth every 2s ages out before it reaches a
    // large `target_bytes`, so the size trigger never fires and the target is
    // unreachable as a lever no matter how the other two are set: the v4 load's
    // ~11,871-row objects are about 2s of one shard's ingest rate. A bulk load
    // that wants target-sized objects must therefore raise `--max-flush-delay`
    // past the time one target takes to fill, in addition to widening the
    // in-flight window. `None` here leaves the age trigger at its default, so an
    // unset flag changes nothing.
    //
    // "Larger" is measured against the shard's `est_bytes` footprint estimate,
    // not the encoded object, and tested once per write after a whole batch's
    // slice has merged: below one slice's footprint the target is unreachable
    // as a lever, whatever byte figure it names
    // (`target_bytes_no_effect_warning`).
    //
    // `Arc` so each batch's write can be `tokio::spawn`ed onto its own task and
    // run genuinely concurrently up to `pipeline_depth` (a constructed-but-
    // unawaited future does no I/O until polled; spawning is what makes the S3
    // PUT round trips overlap). `write`/`write_columnar` take `&self`, so this
    // is the only change the router's own type needs.
    //
    // `max_inflight_flushes` is the second, inner concurrency window (issue
    // #807): `pipeline_depth` bounds the writes the loader keeps outstanding,
    // this bounds the flushes each shard actor may run at once, and the two
    // multiply. It reaches the shard actors' flush semaphores unmodified
    // (`Semaphore::new(config.max_inflight_flushes as usize)` in
    // crates/ravel-ingest/src/log_shard.rs); nothing downstream clamps it.
    let router = Arc::new(LogIngestRouter::new(
        build_ingest_config(shards, target_bytes, max_inflight_flushes, max_flush_delay),
        Arc::clone(&store),
        clock,
    ));

    // Every Strict write below waits this long for its ack, and the wait is
    // whatever the age trigger the same flag configured takes to release the
    // buffer (see `write_ack_deadline`): a buffer that misses `target_bytes`
    // through slice variance mid-load is answered by the age trigger, so the
    // deadline has to outlast it.
    let ack_deadline = write_ack_deadline(max_flush_delay);

    // Parse the input's Parquet footer exactly once here (issue #773). The
    // shared metadata sizes the stride cursors, derives the reader schema, and
    // is handed to every cursor's builder, so a 105-column footer is decoded a
    // single time per load instead of once per setup site plus once per cursor.
    let input = FileInput { path: parquet_path };
    let metadata = read_input_metadata(&input)?;
    let row_group_lens = row_group_row_counts(&metadata);
    let cursor_count = resolve_read_cursors(read_cursors, shards, row_group_lens.len());
    let cursors =
        open_stride_cursors(&input, &metadata, &row_group_lens, cursor_count, batch_rows)?;

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

    // Decode/encode overlap (issue #680). A single blocking decoder task owns
    // the K stride cursors and drives the existing `collect_spans` +
    // `build_columnar_batch` stage, in row-group order, pushing each built batch
    // into a bounded channel; this loop pulls from that channel and drives the
    // shard writes. The channel is the decouple point that replaces the former
    // single-batch lookahead (issue #541): the decoder runs ahead by up to
    // `decode_queue_batches` batches (back-pressured by `blocking_send` when the
    // channel is full) while the encoders drain earlier ones, so decode and
    // encode overlap instead of alternating in lockstep. Batch composition,
    // order, and shard assignment are unchanged (the decoder produces exactly
    // the same batches, in the same FIFO order, that the former inline loop did),
    // so the RLOG bytes written are identical for the same input and flags.
    //
    // Result ordering stays strict-FIFO regardless of `pipeline_depth` or
    // `decode_queue_batches`: batches arrive from the channel in submission
    // order, and writes are recorded (and a write failure surfaced) only by
    // consuming `inflight` oldest-first, so `report.tokens` grows in submission
    // order, and a build error for a later batch is only reported after every
    // earlier batch's write has been drained from the window.
    let mapping = Arc::new(mapping.clone());
    let state = StrideCursors {
        cursors,
        deal_offset: 0,
    };

    let (mut rx, decode_handle) = spawn_decode_pipeline(
        state,
        Arc::clone(&mapping),
        limits.clone(),
        now_ns,
        batch_rows,
        path,
        on_build_start,
        on_batch_queued,
        decode_queue_batches,
    );

    // `true` once the decoder signals clean exhaustion (`Prefetched::Done`). If
    // the channel instead closes without a `Done` (the decoder task panicked),
    // this stays `false` and the panic is surfaced as a batch decode failure.
    let mut decoder_done = false;

    loop {
        // Wall attribution (issue #800): everything the loop blocks on is either
        // this receive or a write resolution below, so timing both partitions
        // the loop's own wall clock with no third bucket to hide in.
        let decode_wait_start = Instant::now();
        let received = rx.recv().await;
        report.decode_wait += decode_wait_start.elapsed();
        let built = match received {
            Some(Prefetched::Done) => {
                decoder_done = true;
                break;
            }
            Some(Prefetched::BatchFailed { reason }) => {
                // Earlier batches' writes may still be in flight, so drain them
                // first (oldest-first) so any already-durable earlier batch is
                // reported and any earlier write failure surfaces ahead of this
                // one, exactly as the former serial loop ordered them.
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
            Some(Prefetched::RowRejected { row, reason }) => {
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
            Some(Prefetched::Batch(built)) => built,
            // The channel closed without a `Done`: the decoder task ended early,
            // which on this path means it panicked. Drain earlier writes, then
            // surface the panic as a batch decode failure (below).
            None => break,
        };

        let n = built.num_rows() as u64;
        if n == 0 {
            // A zero-row batch writes nothing; wait for the next.
            continue;
        }

        // Spawn this batch's write onto its own task so it runs concurrently
        // with the writes already in flight and with the decoder's next batch. A
        // `tokio::spawn`ed write begins executing immediately; a merely-
        // constructed future would do no I/O until polled, which is why the
        // window is built from join handles, not from unpolled futures.
        let handle = match built {
            Built::Row(records) => {
                let router = Arc::clone(&router);
                let tenant = tenant_id.clone();
                tokio::spawn(async move {
                    router
                        .write(tenant, records, WriteMode::Strict, ack_deadline)
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
                        .write_columnar(tenant, *batch, WriteMode::Strict, ack_deadline)
                        .await
                })
            }
        };
        inflight.push_back((n, handle));

        // Bound true concurrency to `pipeline_depth`: once the window is full,
        // resolve the oldest write before starting the next batch's write. This
        // is the only place `report.tokens` grows during the loop, and it grows
        // strictly oldest-first, so a later batch's write finishing early can
        // never record its token ahead of an earlier one (or ahead of an earlier
        // failure). On a write error, every still-outstanding later write is
        // resolved too, and whatever it committed is folded into the reported
        // durable list (`harvest_after_failure`): the loader cannot stop a
        // shard-actor flush it has already handed off, so awaiting the outcome
        // is what keeps the report equal to what landed.
        //
        // This is also where the loop's wall time goes at `pipeline_depth 1`,
        // which is why it is timed (issue #800): the window is full after every
        // single spawn, so the loop resolves each batch's every-shard ack before
        // it can even receive the next batch.
        //
        // Only one write is spawned per iteration, so the window exceeds its
        // bound by at most one and this resolves exactly one oldest entry; the
        // `while` + `let`-else form avoids unwrapping a `pop_front` that is
        // always `Some` here (the bound is >= 1).
        let write_wait_start = Instant::now();
        while inflight.len() >= pipeline_depth {
            let Some(entry) = inflight.pop_front() else {
                break;
            };
            if let Err(mut e) = resolve_write_entry(
                entry,
                &mut report,
                &mut shards_seen,
                &mut data_batches_flushed,
                shards,
            )
            .await
            {
                harvest_after_failure(&mut inflight, &mut e).await;
                report.write_wait += write_wait_start.elapsed();
                return Err(e);
            }
        }
        report.write_wait += write_wait_start.elapsed();
    }

    // The channel is drained. If it closed without a `Done`, the decoder task
    // ended early (a panic in the decode/build): drain the earlier writes
    // oldest-first, then surface the panic as a batch decode failure, matching
    // the ordering the former inline loop produced on a decode-task panic.
    if !decoder_done {
        drain_inflight(
            &mut inflight,
            &mut report,
            &mut shards_seen,
            &mut data_batches_flushed,
            shards,
        )
        .await?;
        let reason = match decode_handle.await {
            Err(join_err) => format!("Parquet decode/build task failed: {join_err}"),
            Ok(()) => "Parquet decode/build task ended without completing".to_string(),
        };
        return Err(LoadError::BatchFailed {
            reason,
            durable: report.tokens.clone(),
        });
    }

    // Clean exhaustion: reap the finished decoder task first, so no further
    // batch can be built.
    let _ = decode_handle.await;

    // Publish the tail buffers BEFORE waiting on the writes that are still in
    // the window. The input is exhausted, so no later batch will arrive to push
    // a buffer that sits under `target_bytes` over it, and its writes' acks are
    // then answered only by the age trigger -- which at a raised
    // `--max-flush-delay` outlasts the ack deadline, failing the whole load at
    // the end on exactly the settings the flag exists for. `FlushNow` travels
    // each shard's own channel, so it merges behind the writes already queued
    // there, publishes whatever they buffered, and answers their waiters; the
    // drain below then resolves at PUT speed instead of on the age clock. A
    // write whose task has not yet reached its channel send is the case
    // `write_ack_deadline` covers: it is released by the age trigger and its
    // ack is awaited long enough to arrive.
    router.flush_all().await;

    // Drain every write still in the window in the same oldest-first order
    // before reporting success.
    drain_inflight(
        &mut inflight,
        &mut report,
        &mut shards_seen,
        &mut data_batches_flushed,
        shards,
    )
    .await?;

    // Snapshot the router's cumulative counters before it drops: the caller
    // reads the dynamic-column figures to warn on overflow or near-cap pressure
    // (ADR-0100 decision 1).
    report.metrics = router.metrics().snapshot();
    report.flush_trigger_mix = router.metrics().flush_trigger_mix_by_shard();
    #[cfg(feature = "stage-timing")]
    {
        report.stage_timings = router.stage_timings().snapshot();
    }
    report.elapsed = started.elapsed();
    Ok(report)
}

/// Spawn the decode/build stage (issue #680) as one blocking task that owns the
/// K stride cursors and feeds a bounded channel. Each iteration decodes and
/// builds exactly one batch through the same `collect_spans` +
/// `build_columnar_batch` (or row) path the former inline loop used, in
/// row-group order, then `blocking_send`s the outcome; the send blocks when the
/// channel already holds `queue_depth` batches, which is the back-pressure that
/// bounds the queue's memory to `queue_depth` built batches. The task stops
/// after sending a terminal outcome (`Done`/`BatchFailed`/`RowRejected`) or when
/// the receiver is dropped (the loader returned early). Returns the receiving
/// half plus the task's join handle, which the caller reaps to distinguish a
/// clean end from a decode-task panic.
#[allow(clippy::too_many_arguments)]
fn spawn_decode_pipeline(
    mut state: StrideCursors,
    mapping: Arc<Mapping>,
    limits: LogIngestLimits,
    now_ns: i64,
    batch_rows: usize,
    path: LoadPath,
    on_build_start: Option<BuildStartHook>,
    on_batch_queued: Option<BuildStartHook>,
    queue_depth: usize,
) -> (
    tokio::sync::mpsc::Receiver<Prefetched>,
    tokio::task::JoinHandle<()>,
) {
    let (tx, rx) = tokio::sync::mpsc::channel(queue_depth);
    let handle = tokio::task::spawn_blocking(move || {
        loop {
            let (next_state, built) = match path {
                LoadPath::Row => decode_and_build_stride(
                    state,
                    Arc::clone(&mapping),
                    limits.clone(),
                    now_ns,
                    batch_rows,
                    on_build_start.as_ref(),
                ),
                LoadPath::Columnar => decode_and_build_stride_columnar(
                    state,
                    Arc::clone(&mapping),
                    limits.clone(),
                    now_ns,
                    batch_rows,
                    on_build_start.as_ref(),
                ),
            };
            state = next_state;
            let is_batch = matches!(built, Prefetched::Batch(_));
            if tx.blocking_send(built).is_err() {
                // Receiver dropped: the loader returned early (a write failed,
                // or an earlier terminal outcome was consumed). Stop decoding.
                break;
            }
            if is_batch {
                if let Some(hook) = &on_batch_queued {
                    hook();
                }
            } else {
                // A terminal outcome was the last thing to send.
                break;
            }
        }
    });
    (rx, handle)
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
/// error every remaining (later) write is resolved too and whatever it
/// committed is folded into that error's durable-token list; see
/// [`harvest_after_failure`] for why the loader waits rather than aborts.
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
        if let Err(mut e) =
            resolve_write_entry(entry, report, shards_seen, data_batches_flushed, shards).await
        {
            harvest_after_failure(inflight, &mut e).await;
            return Err(e);
        }
    }
    Ok(())
}

/// After the first write failure, resolve every write still outstanding and
/// append whatever it committed to `err`'s durable-token list, in submission
/// order behind the tokens already there.
///
/// The loader cannot cancel a write it has handed off. `JoinHandle::abort`
/// cancels only the loader's own wait for the ack; the shard actor holds a
/// channel `tx` and no join handle of the spawned flush (see
/// `LogIngestRouter::write` in `crates/ravel-ingest/src/log_router.rs`), so an
/// aborted batch's data object and commit record can still land afterwards.
/// Aborting therefore does not prevent a later batch from committing, it only
/// prevents the loader from *knowing* that it did -- which is precisely the gap
/// that makes rows query-visible while the report calls them not durable, and
/// makes a resume from that report re-ingest them as duplicates
/// (docs/consistency-model.md: a logs re-ingest is user-visible duplication).
///
/// Waiting closes the gap without needing a cancellation mechanism in
/// `ravel-ingest`: once every outstanding write has reached a terminal outcome
/// there is nothing left that can commit, so the reported list is exactly what
/// landed, at any `--pipeline-depth`. The cost is on the failure path only, and
/// it is bounded: the remaining writes were submitted before the failing one and
/// run concurrently, so this waits at most one [`write_ack_deadline`], which at
/// a raised `--max-flush-delay` is that delay plus its margin rather than a flat
/// minute. Reached from the clean-exhaustion drain the wait is usually short,
/// because the tail buffers were already published by the `flush_all` that
/// precedes that drain -- with one exception either path shares: a write whose
/// task had not yet reached its channel send when the flush ran lands in a
/// fresh buffer afterward and waits on the age trigger, which the scaled
/// deadline outlasts by construction.
///
/// A later write's own error is deliberately discarded apart from its recovered
/// tokens: the returned error stays the first failure in submission order, which
/// is the one the operator needs to act on.
async fn harvest_after_failure(
    inflight: &mut std::collections::VecDeque<(
        u64,
        tokio::task::JoinHandle<Result<LogWriteReceipt, LogWriteError>>,
    )>,
    err: &mut LoadError,
) {
    let Some(durable) = err.durable_tokens_mut() else {
        // `Setup` carries no token list because it cannot occur once a batch
        // could have flushed; there is nothing outstanding to harvest into.
        inflight.clear();
        return;
    };
    while let Some((_, handle)) = inflight.pop_front() {
        match handle.await {
            Ok(Ok(receipt)) => durable.extend(receipt.tokens),
            // A partial failure still committed the shards it reports
            // (`LogWriteError::PartialWrite`, issue #296); those objects are
            // query-visible and belong in the list.
            Ok(Err(write_err)) => durable.extend_from_slice(write_err.durable_tokens()),
            // The write task itself panicked or was cancelled: no ack was
            // observed, so nothing can be attributed to it.
            Err(_) => {}
        }
    }
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

/// Opens a fresh reader over the load input for each independent read. The
/// stride cursors read disjoint row-group partitions concurrently, so each
/// needs its own reader (its own file offset), but they all share one parsed
/// footer: only the data pages are re-read, never the metadata (issue #773).
trait InputReaders {
    type Reader: parquet::file::reader::ChunkReader + 'static;
    fn open(&self) -> Result<Self::Reader, LoadError>;
}

/// The production input: a Parquet file on disk. Each `open` is a new file
/// handle over the same path.
struct FileInput<'a> {
    path: &'a Path,
}

impl InputReaders for FileInput<'_> {
    type Reader = std::fs::File;

    fn open(&self) -> Result<std::fs::File, LoadError> {
        std::fs::File::open(self.path)
            .map_err(|e| LoadError::Setup(format!("failed to open {}: {e}", self.path.display())))
    }
}

/// Parse the input's Parquet footer once and return the metadata every setup
/// site reuses (issue #773). This is the single footer decode per load input;
/// `row_group_row_counts`, `load_reader_schema`, and each stride cursor's
/// builder all take the result rather than re-reading it.
fn read_input_metadata<S: InputReaders>(source: &S) -> Result<ArrowReaderMetadata, LoadError> {
    let reader = source.open()?;
    ArrowReaderMetadata::load(&reader, ArrowReaderOptions::default())
        .map_err(|e| LoadError::Setup(format!("failed to read Parquet metadata: {e}")))
}

/// Read each row group's row count from the already-parsed footer, in
/// row-group order, without decoding any data. Used to size and partition the
/// stride cursors (issue #560) before any reader is opened.
fn row_group_row_counts(metadata: &ArrowReaderMetadata) -> Vec<u64> {
    metadata
        .metadata()
        .row_groups()
        .iter()
        .map(|rg| rg.num_rows() as u64)
        .collect()
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

/// The Arrow key type every preserved Parquet dictionary is read back with.
/// Parquet dictionary indices are `i32`, so `Int32` is the exact key width and
/// no narrowing or widening happens on the way in.
const DICT_KEY_TYPE: DataType = DataType::Int32;

/// A dictionary data-page encoding: `RLE_DICTIONARY`, or `PLAIN_DICTIONARY` in
/// the pre-2.4 spelling.
fn is_dictionary_encoding(e: parquet::basic::Encoding) -> bool {
    matches!(
        e,
        parquet::basic::Encoding::RLE_DICTIONARY | parquet::basic::Encoding::PLAIN_DICTIONARY
    )
}

/// Is every one of `chunk`'s data pages dictionary encoded?
///
/// The chunk-level `encodings` list cannot answer this. A writer whose
/// dictionary outgrows its page-size limit falls back to plain part way through
/// the chunk, and the result lists `RLE_DICTIONARY` (the pages written before
/// the fallback) alongside `PLAIN` (the ones after) -- which is also what a
/// fully dictionary-encoded chunk lists, because its dictionary page is itself
/// `PLAIN`. The footer's page encoding statistics separate the two: they are
/// per page type, so the data pages can be read on their own.
///
/// When a file records no page statistics at all, this falls back to the
/// chunk-level list. That over-reports a fallback chunk as dictionary encoded
/// rather than under-reporting the ordinary case; the values read back are the
/// same either way, only the per-block work differs.
fn chunk_is_dictionary_encoded(chunk: &parquet::file::metadata::ColumnChunkMetaData) -> bool {
    // The reader condenses the statistics to a data-page-only encoding mask by
    // default, and keeps the full per-page list only when asked to.
    if let Some(mask) = chunk.page_encoding_stats_mask() {
        return data_page_encodings_are_all_dictionary(mask.encodings());
    }
    if let Some(stats) = chunk.page_encoding_stats() {
        return data_page_encodings_are_all_dictionary(
            stats
                .iter()
                .filter(|s| {
                    matches!(
                        s.page_type,
                        parquet::basic::PageType::DATA_PAGE
                            | parquet::basic::PageType::DATA_PAGE_V2
                    )
                })
                .map(|s| s.encoding),
        );
    }
    chunk.encodings().any(is_dictionary_encoding)
}

/// True when `encodings` is non-empty and every encoding in it is a dictionary
/// encoding. Empty means the footer recorded no data page for the chunk, which
/// is not evidence of a dictionary.
fn data_page_encodings_are_all_dictionary(
    encodings: impl Iterator<Item = parquet::basic::Encoding>,
) -> bool {
    let mut seen = false;
    for e in encodings {
        if !is_dictionary_encoding(e) {
            return false;
        }
        seen = true;
    }
    seen
}

/// Derive the Arrow schema the loader drives its data reader with, so that a
/// Parquet file's own string dictionaries survive into the Arrow batches and
/// ADR-0109 decision 3 engages (issue #660).
///
/// The rule, applied per top-level column of `inferred` (the schema the reader
/// would infer on its own, embedded Arrow metadata included):
///
/// - the column is retyped `Dictionary(Int32, Utf8)` when all of: the inferred
///   type is `Utf8`; the column is a top-level Parquet leaf of physical type
///   `BYTE_ARRAY` with the `String`/`UTF8` logical type; and *every* column
///   chunk for it, in every row group, is dictionary encoded on every data page
///   ([`chunk_is_dictionary_encoded`]);
/// - every other column keeps the type the reader would infer, unchanged. That
///   includes a non-string column the writer happened to dictionary-encode
///   (only string columns feed decision 3's per-distinct-value work), a column
///   the embedded Arrow metadata already types as a dictionary, and above all
///   a string column whose chunks are *not* dictionary encoded because the
///   writer's dictionary outgrew its page limit and it fell back to plain, as a
///   unique-per-row column does.
///
/// The rule only preserves an encoding the file already carries; it never
/// forces a dictionary onto a column that has none, which would move per-row
/// work into the reader instead of removing it.
///
/// Returns `None` when no column qualifies, which is the caller's signal to
/// open the reader with default options and infer as before.
fn dictionary_preserving_schema(
    inferred: &SchemaRef,
    metadata: &parquet::file::metadata::ParquetMetaData,
) -> Option<SchemaRef> {
    let descr = metadata.file_metadata().schema_descr();
    let row_groups = metadata.row_groups();
    if row_groups.is_empty() {
        return None;
    }

    let mut changed = false;
    let fields: Vec<Field> = inferred
        .fields()
        .iter()
        .map(|field| {
            let f = field.as_ref().clone();
            if *f.data_type() != DataType::Utf8 {
                return f;
            }
            // Only a top-level Parquet leaf (path length 1) maps one-to-one to
            // a top-level Arrow field; anything nested keeps its inferred type.
            let Some(leaf) = descr
                .columns()
                .iter()
                .position(|c| c.path().parts().len() == 1 && c.path().parts()[0] == *f.name())
            else {
                return f;
            };
            let col = descr.column(leaf);
            let is_utf8_byte_array = col.physical_type() == parquet::basic::Type::BYTE_ARRAY
                && (matches!(
                    col.logical_type_ref(),
                    Some(parquet::basic::LogicalType::String)
                ) || col.converted_type() == parquet::basic::ConvertedType::UTF8);
            if !is_utf8_byte_array {
                return f;
            }
            if !row_groups
                .iter()
                .all(|rg| chunk_is_dictionary_encoded(rg.column(leaf)))
            {
                return f;
            }
            changed = true;
            f.with_data_type(DataType::Dictionary(
                Box::new(DICT_KEY_TYPE),
                Box::new(DataType::Utf8),
            ))
        })
        .collect();

    changed.then(|| {
        Arc::new(Schema::new_with_metadata(
            fields,
            inferred.metadata().clone(),
        )) as SchemaRef
    })
}

/// Derive the reader schema [`dictionary_preserving_schema`] describes from the
/// already-parsed footer, or `None` when no column qualifies. The metadata is
/// the shared one from [`read_input_metadata`]; nothing is re-read here.
fn load_reader_schema(metadata: &ArrowReaderMetadata) -> Option<SchemaRef> {
    dictionary_preserving_schema(metadata.schema(), metadata.metadata())
}

/// Open one [`BatchReader`] per stride cursor (issue #560), each restricted to
/// its own contiguous partition of `parquet_path`'s row groups, with
/// `partition_base` set to that partition's first row's file-absolute index.
/// An empty partition (only possible when `row_group_lens` is empty, the
/// degenerate zero-row-group case, which forces `k == 1`) yields an
/// already-exhausted cursor with no reader opened, rather than asking Parquet
/// to build a reader over zero row groups.
fn open_stride_cursors<S: InputReaders>(
    source: &S,
    metadata: &ArrowReaderMetadata,
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

    // Derived once from the shared footer, then applied to every cursor: each
    // cursor reads a disjoint partition of the same file, so they must all
    // agree on the column types (issue #660).
    let reader_schema = load_reader_schema(metadata);

    // The `ArrowReaderMetadata` every cursor's builder is constructed from: the
    // shared footer, with the dictionary-preserving schema applied when one was
    // derived. Building it from the already-parsed metadata (issue #773) means
    // no cursor re-parses the footer; it only opens a reader for the data pages.
    let cursor_metadata = match &reader_schema {
        Some(schema) => ArrowReaderMetadata::try_new(
            Arc::clone(metadata.metadata()),
            ArrowReaderOptions::new().with_schema(Arc::clone(schema)),
        )
        .map_err(|e| LoadError::Setup(format!("failed to apply reader schema: {e}")))?,
        None => metadata.clone(),
    };

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
        let file = source.open()?;
        let builder =
            ParquetRecordBatchReaderBuilder::new_with_metadata(file, cursor_metadata.clone());
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
    /// An all-null column: every row yields `None`. Used for a dictionary whose
    /// values array is empty (or whose every key is null), where there is no
    /// value any row could resolve to.
    AllNull,
    Bad(&'a ArrayRef),
}

// See #708 (empty-dictionary panic) and #680 (decode/encode overlap).
fn str_src(arr: &ArrayRef) -> StrSrc<'_> {
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
///
/// Dynamic column slots are resolved once per batch, not once per cell (#689).
fn build_columnar_batch(
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

    // A slot's cells vector is allocated on its first present value: a mapped
    // attribute that is null across the whole batch never created a map entry
    // before, so it must materialize no column now either.
    let mut slot_cells: Vec<Option<Vec<Option<AttrValue>>>> = Vec::new();
    slot_cells.resize_with(slot_keys.len(), || None);
    // Whether every winning cell of a slot came from a dictionary-encoded Arrow
    // source, and the row that most recently won each slot (1-based, 0 meaning
    // never). The stamp replaces the per-row `HashSet<(String, u8)>` that
    // decided the first-occurrence winner: same relation, no allocation and no
    // key clone per cell.
    let mut slot_dict: Vec<bool> = vec![true; slot_keys.len()];
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
                if let Some(v) = src.get(local).map_err(row_err)? {
                    check_attr(&spec.key, &v, limits).map_err(row_err)?;
                    present_record += 1;
                    if slot_taken_at[*slot] == row_stamp {
                        batch.residual_attrs[grow].push((spec.key.clone(), v));
                    } else {
                        slot_taken_at[*slot] = row_stamp;
                        slot_cells[*slot].get_or_insert_with(|| vec![None; total_rows])[grow] =
                            Some(v);
                        slot_dict[*slot] &= src.is_dict();
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
    let mut dicts: Vec<Option<StrColumnDict>> = Vec::with_capacity(slot_keys.len());
    let mut any_dict = false;
    for (slot, ((name, ty_byte), cells)) in slot_keys.into_iter().zip(slot_cells).enumerate() {
        // No cells vector means the slot never took a present value, which is
        // exactly the case where the map this replaced held no entry at all.
        let Some(cells) = cells else { continue };
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
        let use_dict = matches!(field_type, FieldType::Str | FieldType::Bytes) && slot_dict[slot];
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
    use arrow::array::DictionaryArray;
    use arrow::datatypes::Int32Type;
    use proptest::prelude::*;

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

    /// A deterministic clock the test advances by hand, whose `sleep` the shard
    /// actor's flush tick waits on (mirrors `ravel-ingest`'s own unit-test
    /// `TestClock`, restated here because that clock is private to that crate's
    /// test module). Advancing it past `max_flush_delay` is what drives an age
    /// flush with no real-time sleep, so the age trigger can be exercised
    /// through the loader deterministically.
    struct TestClock {
        now_ns: std::sync::atomic::AtomicI64,
        wake_tx: tokio::sync::watch::Sender<()>,
    }

    impl TestClock {
        fn new(start_ns: i64) -> std::sync::Arc<Self> {
            let (wake_tx, _rx) = tokio::sync::watch::channel(());
            std::sync::Arc::new(TestClock {
                now_ns: std::sync::atomic::AtomicI64::new(start_ns),
                wake_tx,
            })
        }

        fn advance_ns(&self, delta_ns: i64) {
            self.now_ns
                .fetch_add(delta_ns, std::sync::atomic::Ordering::SeqCst);
            let _ = self.wake_tx.send(());
        }
    }

    impl Clock for TestClock {
        fn now_ns(&self) -> i64 {
            self.now_ns.load(std::sync::atomic::Ordering::SeqCst)
        }

        fn sleep(
            &self,
            dur: Duration,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send + '_>> {
            let deadline = self
                .now_ns()
                .saturating_add(i64::try_from(dur.as_nanos()).unwrap_or(i64::MAX));
            let mut rx = self.wake_tx.subscribe();
            Box::pin(async move {
                loop {
                    if self.now_ns() >= deadline {
                        return;
                    }
                    if rx.changed().await.is_err() {
                        return;
                    }
                }
            })
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

    /// Reachability (issue #983): a real `load` run carries the per-shard flush
    /// trigger mix in the same report the objects-written figure lives in, and
    /// the mix sums to the object count. One row routes to one shard and flushes
    /// by size at the default target, so this asserts an exact (size 1, age 0,
    /// final 0) on a single shard rather than `> 0`.
    #[tokio::test]
    async fn load_report_carries_the_flush_trigger_mix() {
        let report = run_wide_load(4).await;
        assert!(
            !report.flush_trigger_mix.is_empty(),
            "a real load records at least one shard's flushes"
        );
        let mix = report.flush_mix_report();
        assert_eq!(mix.shards.len(), 1, "one row routes to exactly one shard");
        assert_eq!(
            mix.shards[0].counts,
            FlushMixCounts {
                size: 1,
                age: 0,
                final_drain: 0,
            },
            "the single row flushes by size, nothing ages or drains"
        );
        assert_eq!(
            mix.totals,
            FlushMixCounts {
                size: 1,
                age: 0,
                final_drain: 0,
            }
        );
        assert_eq!(
            mix.totals.total(),
            report.objects_written() as u64,
            "the mix sums to the objects the same report reports"
        );
    }

    /// Round-trip (issue #983): the machine-readable flush mix serializes and
    /// deserializes without changing the counts, and the final drain is keyed
    /// `final` in the serialized form (the Rust field is `final_drain`, since
    /// `final` is a keyword). Test 3's serialize/deserialize half.
    #[test]
    fn flush_mix_report_round_trips_through_json() {
        let report = LoadReport {
            flush_trigger_mix: vec![
                (
                    0,
                    FlushTriggerMix {
                        size: 5,
                        age: 2,
                        final_drain: 1,
                    },
                ),
                (
                    2,
                    FlushTriggerMix {
                        size: 0,
                        age: 0,
                        final_drain: 3,
                    },
                ),
            ],
            ..LoadReport::default()
        };
        let mix = report.flush_mix_report();
        assert_eq!(
            mix.totals,
            FlushMixCounts {
                size: 5,
                age: 2,
                final_drain: 4,
            },
            "totals sum each cause across shards"
        );

        let json = serde_json::to_string(&mix).expect("serialize");
        assert!(
            json.contains("\"final\":"),
            "the drain is keyed `final` in the serialized form: {json}"
        );
        let back: FlushMixReport = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(
            back, mix,
            "the round trip preserves the exact per-shard counts and totals"
        );
        // The per-shard rows survive keyed by shard, not reordered or merged.
        assert_eq!(back.shards[0].shard, 0);
        assert_eq!(back.shards[0].counts.size, 5);
        assert_eq!(back.shards[1].shard, 2);
        assert_eq!(back.shards[1].counts.final_drain, 3);
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
            DEFAULT_MAX_INFLIGHT_FLUSHES,
            DEFAULT_DECODE_QUEUE_BATCHES,
            DEFAULT_TARGET_BYTES,
            None,
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

    /// `--decode-queue-batches 0` is rejected with a typed [`LoadError::Setup`]
    /// before any work, mirroring `pipeline_depth_zero_is_rejected` (issue
    /// #680). A depth of 0 is a channel that can hold no batch, so the guard
    /// makes it unreachable rather than letting `mpsc::channel(0)` be hit.
    #[tokio::test]
    async fn decode_queue_batches_zero_is_rejected() {
        use ravel_object_store::memory::MemoryStore;
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let m = base_mapping();
        let err = load_instrumented(
            store,
            Path::new("/nonexistent.parquet"),
            "acme",
            &m,
            4,
            10,
            None,
            1,
            DEFAULT_MAX_INFLIGHT_FLUSHES,
            0,
            DEFAULT_TARGET_BYTES,
            None,
            NOW_NS,
            Arc::new(FixedClock(NOW_NS)),
            LoadPath::Columnar,
            None,
            None,
        )
        .await
        .expect_err("decode_queue_batches of 0 is rejected");
        assert!(
            matches!(err, LoadError::Setup(_)),
            "a typed setup error, got: {err}"
        );
        assert!(
            err.to_string()
                .contains("--decode-queue-batches must be at least 1"),
            "the error names the lever: {err}"
        );
    }

    /// `--max-inflight-flushes 0` is rejected with a typed [`LoadError::Setup`]
    /// before any work, mirroring `pipeline_depth_zero_is_rejected` (issue
    /// #807). A bound of 0 builds `Semaphore::new(0)` in every shard actor, a
    /// permit no flush can ever acquire, so the guard makes it unreachable
    /// rather than letting the first flush trigger park forever.
    ///
    /// Non-vacuity (prove-the-test): the message assertion is what carries the
    /// weight. With the guard deleted, this fixture still fails, but on the
    /// missing-file open further down, so the `LoadError::Setup(_)` shape alone
    /// would pass while the flag went unvalidated; only the
    /// `--max-inflight-flushes must be at least 1` text distinguishes the two.
    #[tokio::test]
    async fn max_inflight_flushes_zero_is_rejected() {
        use ravel_object_store::memory::MemoryStore;
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let m = base_mapping();
        let err = load_instrumented(
            store,
            Path::new("/nonexistent.parquet"),
            "acme",
            &m,
            4,
            10,
            None,
            1,
            0,
            DEFAULT_DECODE_QUEUE_BATCHES,
            DEFAULT_TARGET_BYTES,
            None,
            NOW_NS,
            Arc::new(FixedClock(NOW_NS)),
            LoadPath::Columnar,
            None,
            None,
        )
        .await
        .expect_err("max_inflight_flushes of 0 is rejected");
        assert!(
            matches!(err, LoadError::Setup(_)),
            "a typed setup error, got: {err}"
        );
        assert!(
            err.to_string()
                .contains("--max-inflight-flushes must be at least 1"),
            "the error names the lever: {err}"
        );
    }

    /// The loader's two write windows compose as
    /// `shards * min(pipeline_depth, max_inflight_flushes)` (ADR-0807), so the
    /// inner default must equal the outer one, not be an independent literal
    /// that can drift below it. Below it, each shard's excess batches re-queue
    /// on the flush semaphore and the outer window buys nothing; above it, the
    /// value is unreachable because the loader never hands a shard more
    /// concurrent work than `--pipeline-depth` batches.
    #[test]
    fn default_max_inflight_flushes_matches_pipeline_depth() {
        assert_eq!(
            DEFAULT_MAX_INFLIGHT_FLUSHES as usize, DEFAULT_PIPELINE_DEPTH,
            "the inner flush window's default must track the outer pipeline depth's"
        );
    }

    /// The loader's `--max-inflight-flushes` default deliberately does NOT track
    /// `IngestConfig::max_inflight_flushes` (issue #800). That field's default of
    /// 1 governs the client-facing serving path, whose Strict ack contract
    /// ADR-0067 decision 2 froze and whose memory has no outer window capping
    /// it; the bulk loader is a different workload. This pins the divergence so
    /// that raising the serving default later is a deliberate edit here too,
    /// rather than something that silently re-couples the two.
    #[test]
    fn loader_flush_window_default_diverges_from_the_serving_default() {
        assert_eq!(
            IngestConfig::default().max_inflight_flushes,
            1,
            "the serving default is unchanged at 1 (ADR-0067 decision 2)"
        );
        assert!(
            DEFAULT_MAX_INFLIGHT_FLUSHES > IngestConfig::default().max_inflight_flushes,
            "the bulk loader pipelines flushes where the serving path does not"
        );
    }

    /// A multi-batch Parquet fixture with a dictionary-encoded string column, a
    /// plain string column, a resource column, and a ts column, so the decode
    /// stage does real `build_columnar_batch` work (dictionary interning
    /// included) rather than trivial passthrough.
    fn multi_batch_dict_fixture() -> (tempfile::TempDir, std::path::PathBuf, Mapping) {
        use parquet::arrow::ArrowWriter;

        let n = 8usize;
        let dir = tempfile::tempdir().expect("tempdir");
        let pq = dir.path().join("overlap.parquet");
        let ts: Vec<i64> = (0..n).map(|k| NOW_NS - (k as i64) * 1_000).collect();
        let dictvals: Vec<Option<&str>> = (0..n)
            .map(|k| {
                if k % 3 == 0 {
                    None
                } else {
                    Some(["a", "b", "c"][k % 3])
                }
            })
            .collect();
        let dictcol =
            Arc::new(dictvals.into_iter().collect::<DictionaryArray<Int32Type>>()) as ArrayRef;
        let plain = Arc::new(StringArray::from(
            (0..n).map(|k| format!("p{}", k % 4)).collect::<Vec<_>>(),
        )) as ArrayRef;
        let svc = Arc::new(StringArray::from_iter_values(
            (0..n).map(|k| format!("svc{}", k % 2)),
        )) as ArrayRef;
        let b = batch(vec![
            ("ts", Arc::new(Int64Array::from(ts)) as ArrayRef),
            ("svc", svc),
            ("dictcol", dictcol),
            ("plaincol", plain),
        ]);
        let file = std::fs::File::create(&pq).expect("create parquet");
        let mut w = ArrowWriter::try_new(file, b.schema(), None).expect("arrow writer");
        w.write(&b).expect("write batch");
        w.close().expect("close writer");

        let mut m = base_mapping();
        m.resource_attributes = vec![attr("service.name", "svc", ColType::Str)];
        m.attributes = vec![
            attr("dictkey", "dictcol", ColType::Str),
            attr("plainkey", "plaincol", ColType::Str),
        ];
        (dir, pq, m)
    }

    /// Drive the decode stage at a given queue depth and return the BLAKE3 of
    /// each built columnar batch's RLOG encoding, in build order. The writer is
    /// pinned to a fixed identity so the encoding depends only on batch content,
    /// not on the router's random per-run `writer_id`.
    async fn decode_object_hashes(
        pq: &Path,
        mapping: &Mapping,
        shards: u32,
        batch_rows: usize,
        read_cursors: Option<usize>,
        queue_depth: usize,
    ) -> Vec<[u8; 32]> {
        let input = FileInput { path: pq };
        let metadata = read_input_metadata(&input).expect("read metadata");
        let row_group_lens = row_group_row_counts(&metadata);
        let cursor_count = resolve_read_cursors(read_cursors, shards, row_group_lens.len());
        let cursors =
            open_stride_cursors(&input, &metadata, &row_group_lens, cursor_count, batch_rows)
                .expect("cursors");
        let state = StrideCursors {
            cursors,
            deal_offset: 0,
        };
        let (mut rx, handle) = spawn_decode_pipeline(
            state,
            Arc::new(mapping.clone()),
            LogIngestLimits::default(),
            NOW_NS,
            batch_rows,
            LoadPath::Columnar,
            None,
            None,
            queue_depth,
        );
        let mut hashes = Vec::new();
        while let Some(p) = rx.recv().await {
            match p {
                Prefetched::Batch(Built::Columnar(b)) => {
                    if b.num_rows > 0 {
                        hashes.push(*blake3::hash(&columnar_object(*b)).as_bytes());
                    }
                }
                Prefetched::Batch(Built::Row(_)) => panic!("columnar path only"),
                Prefetched::Done => {}
                Prefetched::BatchFailed { reason } => panic!("batch failed: {reason}"),
                Prefetched::RowRejected { row, reason } => {
                    panic!("row {row} rejected: {reason}")
                }
            }
        }
        handle.await.expect("decoder task joins");
        hashes
    }

    /// Byte-identity across decode-queue depths (issue #680): moving scheduling
    /// (the depth of the decode->encode channel) must not change the bytes of
    /// any RLOG object. The same fixture decoded at depth 1 (today's near-
    /// lockstep) and depth 4 must produce the identical ordered sequence of
    /// per-batch RLOG encodings. The comparison is at the batch encoding rather
    /// than at the stored object because the production `LogIngestRouter` draws a
    /// random `writer_id` per construction (crates/ravel-ingest, `SystemRng`),
    /// so two full loads never share stored bytes regardless of this change; the
    /// batch content the writer consumes is exactly what scheduling could
    /// perturb, and it is what this pins.
    ///
    /// Prove-the-test: make `spawn_decode_pipeline` reorder or drop a batch (or
    /// have `build_columnar_batch` depend on queue depth) and the two hash lists
    /// diverge. Confirmed conceptually by the depth-1-vs-4 equality: the only
    /// difference between the runs is the channel capacity.
    #[tokio::test]
    async fn rlog_objects_are_byte_identical_across_decode_queue_depths() {
        let (_dir, pq, m) = multi_batch_dict_fixture();
        // batch_rows = 2 over 8 rows -> four batches.
        let lockstep = decode_object_hashes(&pq, &m, 4, 2, None, 1).await;
        let deep = decode_object_hashes(&pq, &m, 4, 2, None, 4).await;
        assert!(
            lockstep.len() >= 3,
            "the fixture splits into several batches: {}",
            lockstep.len()
        );
        assert_eq!(
            lockstep,
            deep,
            "RLOG object bytes must not depend on the decode-queue depth ({} objects)",
            lockstep.len()
        );
    }

    /// Collect every data object (`/l0/`) a store holds, deduped by key, as
    /// `(key, size)`. Used to compare two loads structurally end to end.
    async fn list_data_objects(store: &dyn ObjectStoreBackend) -> Vec<(String, u64)> {
        let mut out = Vec::new();
        let mut seen = std::collections::HashSet::new();
        let mut page: Option<ravel_object_store::PageToken> = None;
        loop {
            let p = store.list("", page).await.expect("list");
            for o in p.objects {
                if o.key.contains("/l0/") && seen.insert(o.key.clone()) {
                    out.push((o.key, o.size));
                }
            }
            match p.next {
                Some(t) => page = Some(t),
                None => break,
            }
        }
        out
    }

    /// End-to-end structural invariance across decode-queue depths (issue #680):
    /// a real `load_instrumented` at depth 1 and at depth 4, each into its own
    /// `MemoryStore`, writes the same number of data objects with the same
    /// multiset of sizes. (Object bytes themselves differ only by the router's
    /// random `writer_id`; size is invariant to it, so equal sorted sizes plus
    /// equal counts is the strongest depth-independent store-level check. The
    /// content-level guarantee is `rlog_objects_are_byte_identical_...` above.)
    #[tokio::test]
    async fn load_writes_the_same_objects_across_decode_queue_depths() {
        use ravel_object_store::memory::MemoryStore;

        let (_dir, pq, m) = multi_batch_dict_fixture();

        let run = |depth: usize| {
            let pq = pq.clone();
            let m = m.clone();
            async move {
                let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
                load_instrumented(
                    Arc::clone(&store),
                    &pq,
                    "acme",
                    &m,
                    4,
                    2,
                    None,
                    2,
                    DEFAULT_MAX_INFLIGHT_FLUSHES,
                    depth,
                    DEFAULT_TARGET_BYTES,
                    None,
                    NOW_NS,
                    Arc::new(FixedClock(NOW_NS)),
                    LoadPath::Columnar,
                    None,
                    None,
                )
                .await
                .expect("load succeeds");
                let mut objs = list_data_objects(store.as_ref()).await;
                objs.sort_by_key(|(_, size)| *size);
                objs.into_iter().map(|(_, size)| size).collect::<Vec<u64>>()
            }
        };

        let sizes_1 = run(1).await;
        let sizes_4 = run(4).await;
        assert!(!sizes_1.is_empty(), "the load wrote data objects");
        assert_eq!(
            sizes_1, sizes_4,
            "object count and sizes must not depend on the decode-queue depth"
        );
    }

    /// A store that holds only the FIRST data-object PUT for a fixed duration,
    /// snapshotting a shared counter at the moment that PUT completes. Every
    /// other PUT and every non-PUT call passes straight through.
    struct FirstPutHoldStore {
        inner: Arc<dyn ObjectStoreBackend>,
        /// Hold the first data PUT until the decoder has queued this many
        /// batches, then snapshot the count. The bounded decode channel is
        /// what stops the decoder, so this target is reached (and not
        /// exceeded) as a property of the code rather than of how many
        /// batches the host managed inside a fixed hold.
        hold_until_queued: usize,
        queued: Arc<std::sync::atomic::AtomicUsize>,
        first_seen: Arc<std::sync::atomic::AtomicBool>,
        snapshot: Arc<std::sync::Mutex<Option<usize>>>,
    }

    #[async_trait::async_trait]
    impl ObjectStoreBackend for FirstPutHoldStore {
        async fn put(
            &self,
            key: &str,
            data: bytes::Bytes,
            opts: ravel_object_store::PutOptions,
        ) -> Result<ravel_object_store::PutOutcome, ravel_object_store::StoreError> {
            use std::sync::atomic::Ordering;
            if key.contains("/l0/") && !self.first_seen.swap(true, Ordering::SeqCst) {
                // Bounded so a regression that stops the decoder short fails
                // the assertion on the snapshot rather than hanging the suite.
                for _ in 0..10_000 {
                    if self.queued.load(Ordering::SeqCst) >= self.hold_until_queued {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
                *self.snapshot.lock().expect("snapshot lock") =
                    Some(self.queued.load(Ordering::SeqCst));
            }
            self.inner.put(key, data, opts).await
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

    /// The decoder runs more than one batch ahead of the encoders, and no
    /// further than the queue depth (issue #680). With `--pipeline-depth 1` the
    /// loader consumes exactly one batch, spawns its write, and blocks awaiting
    /// that write's ack. Holding that first data PUT lets the decoder fill the
    /// decode->encode channel and block on back-pressure: it will have queued
    /// exactly `decode_queue_batches + 1` batches (one consumed by the loop plus
    /// `decode_queue_batches` buffered in the full channel), no matter how many
    /// batches remain in the file.
    ///
    /// Non-vacuity (prove-the-test): forcing lockstep by changing
    /// `QUEUE_DEPTH` to 1 leaves only 2 batches queued (1 consumed + 1
    /// buffered), which fails the `> 2` "more than one ahead" assertion; and if
    /// the channel were unbounded the decoder would race to queue all ~20
    /// batches, failing the `== QUEUE_DEPTH + 1` bound.
    #[tokio::test]
    async fn decoder_runs_ahead_bounded_by_the_queue_depth() {
        use ravel_object_store::memory::MemoryStore;
        use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

        const QUEUE_DEPTH: usize = 3;
        // Many small batches so the bound (not the file's end) is what stops the
        // decoder: batch_rows = 2 over 40 rows -> 20 batches.
        let n_rows = 40usize;
        let dir = tempfile::tempdir().expect("tempdir");
        let pq = dir.path().join("many.parquet");
        let b = batch(vec![
            ("ts", i64_col(vec![NOW_NS; n_rows])),
            ("svc", str_col(vec!["api"; n_rows])),
        ]);
        {
            use parquet::arrow::ArrowWriter;
            let file = std::fs::File::create(&pq).expect("create parquet");
            let mut w = ArrowWriter::try_new(file, b.schema(), None).expect("arrow writer");
            w.write(&b).expect("write batch");
            w.close().expect("close writer");
        }
        let m = parse_mapping(
            "ts_column = \"ts\"\nts_unit = \"nanos\"\n\n\
             [[resource_attribute]]\nkey = \"service.name\"\ncolumn = \"svc\"\ntype = \"str\"\n",
        )
        .expect("valid mapping");

        let queued = Arc::new(AtomicUsize::new(0));
        let snapshot = Arc::new(std::sync::Mutex::new(None));
        let store = Arc::new(FirstPutHoldStore {
            inner: Arc::new(MemoryStore::new()),
            // The channel bound stops the decoder at exactly this many
            // (1 consumed by the loop + QUEUE_DEPTH buffered), which is also
            // what the assertions below pin. Holding until the decoder gets
            // there replaces a fixed 300 ms hold whose outcome was however
            // many batches the host happened to decode in that window.
            hold_until_queued: QUEUE_DEPTH + 1,
            queued: Arc::clone(&queued),
            first_seen: Arc::new(AtomicBool::new(false)),
            snapshot: Arc::clone(&snapshot),
        });

        let queued_hook = Arc::clone(&queued);
        let on_queued: BuildStartHook = Arc::new(move || {
            queued_hook.fetch_add(1, Ordering::SeqCst);
        });

        let report = load_instrumented(
            store as Arc<dyn ObjectStoreBackend>,
            &pq,
            "acme",
            &m,
            1,
            2,
            None,
            1,
            DEFAULT_MAX_INFLIGHT_FLUSHES,
            QUEUE_DEPTH,
            DEFAULT_TARGET_BYTES,
            None,
            NOW_NS,
            Arc::new(FixedClock(NOW_NS)),
            LoadPath::Columnar,
            None,
            Some(on_queued),
        )
        .await
        .expect("the pipelined load succeeds");
        assert_eq!(report.rows_processed, n_rows as u64, "every row is written");

        let ahead = snapshot
            .lock()
            .expect("snapshot lock")
            .expect("the first data PUT was held and snapshotted");
        assert!(
            ahead > 2,
            "the decoder ran more than one batch ahead while the encoder was blocked \
             (a lockstep depth-1 loop leaves it at 2): queued = {ahead}"
        );
        assert_eq!(
            ahead,
            QUEUE_DEPTH + 1,
            "and no further: 1 consumed by the loop plus {QUEUE_DEPTH} buffered in the full channel"
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
            DEFAULT_MAX_INFLIGHT_FLUSHES,
            DEFAULT_DECODE_QUEUE_BATCHES,
            DEFAULT_TARGET_BYTES,
            None,
            NOW_NS,
            Arc::new(FixedClock(NOW_NS)),
            LoadPath::Columnar,
            Some(hook),
            None,
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
    /// A multi-row-group fixture whose groups each hold exactly one shard's
    /// `host` value (`hits.parquet`'s CounterID-sorted shape in miniature):
    /// `shards` row groups of `rows_per_group` rows. Read with
    /// `--read-cursors <shards>` and `--batch-rows <rows_per_group>` it yields
    /// exactly `rows_per_group` batches, each drawing one row from every group,
    /// so every batch touches all `shards` shards.
    fn sorted_by_shard_fixture(
        shards: u32,
        rows_per_group: usize,
    ) -> (tempfile::TempDir, std::path::PathBuf, Mapping) {
        use parquet::arrow::ArrowWriter;

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

        (dir, pq, m)
    }

    /// [`sorted_by_shard_fixture`] plus one fat record attribute, and the
    /// mapping written to disk so the real entry point can be driven over the
    /// same file.
    ///
    /// `payload_len` bytes of filler per row is what makes the shard buffer's
    /// footprint estimate large enough for the `--target-bytes` regimes to be
    /// distinguishable at unit-test row counts: the estimate charges every
    /// attribute occurrence's key and uncompressed value bytes once per row,
    /// dictionary-encoded or not (`est_columnar_bytes`,
    /// crates/ravel-ingest/src/log_shard.rs), so one row's footprint is about
    /// `payload_len` and one (batch, shard) slice's is that times its rows.
    /// `payload` is a record attribute, not a resource attribute, so stream
    /// identity and therefore the shard each row lands on are unchanged.
    fn fat_attr_sorted_by_shard_fixture(
        shards: u32,
        rows_per_group: usize,
        payload_len: usize,
    ) -> (
        tempfile::TempDir,
        std::path::PathBuf,
        std::path::PathBuf,
        Mapping,
    ) {
        use parquet::arrow::ArrowWriter;

        let hosts: Vec<String> = (0..shards).map(|s| host_for_shard(s, shards)).collect();
        let payload = "p".repeat(payload_len);

        let dir = tempfile::tempdir().expect("tempdir");
        let pq = dir.path().join("fat_attr_sorted_by_shard.parquet");
        let mapping_path = dir.path().join("fat_attr_mapping.toml");

        let group = |host: &str| {
            batch(vec![
                ("ts", i64_col(vec![NOW_NS; rows_per_group])),
                ("svc", str_col(vec!["api"; rows_per_group])),
                ("host", str_col(vec![host; rows_per_group])),
                ("payload", str_col(vec![payload.as_str(); rows_per_group])),
            ])
        };
        let first = group(hosts[0].as_str());
        let file = std::fs::File::create(&pq).expect("create parquet");
        let mut writer = ArrowWriter::try_new(file, first.schema(), None).expect("arrow writer");
        writer.write(&first).expect("write row group");
        writer.flush().expect("flush row group");
        for host in &hosts[1..] {
            writer
                .write(&group(host.as_str()))
                .expect("write row group");
            writer.flush().expect("flush row group");
        }
        writer.close().expect("close writer");

        let toml = "ts_column = \"ts\"\nts_unit = \"nanos\"\n\n\
             [[resource_attribute]]\nkey = \"service.name\"\ncolumn = \"svc\"\ntype = \"str\"\n\n\
             [[resource_attribute]]\nkey = \"host\"\ncolumn = \"host\"\ntype = \"str\"\n\n\
             [[attribute]]\nkey = \"payload\"\ncolumn = \"payload\"\ntype = \"str\"\n";
        std::fs::write(&mapping_path, toml).expect("write mapping");
        let m = parse_mapping(toml).expect("valid mapping");

        (dir, pq, mapping_path, m)
    }

    #[tokio::test]
    async fn stride_reading_spreads_a_sorted_batch_across_all_shards() {
        use ravel_object_store::memory::MemoryStore;

        let shards = 4u32;
        let rows_per_group = 4usize;
        let (_dir, pq, m) = sorted_by_shard_fixture(shards, rows_per_group);

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

    /// Every `/l0/` object in `store`, decoded to its records and sorted, so
    /// two loads that laid the same rows out over a different number of objects
    /// can be compared on content alone. `Predicate::And(vec![])` matches every
    /// record, so this is the whole object, not a filtered view.
    async fn decoded_records(store: &dyn ObjectStoreBackend) -> Vec<String> {
        use ravel_logseg::{Predicate, RlogConfig, RlogReader};
        use ravel_object_store::GetRange;

        let cfg = RlogConfig::default();
        let mut out = Vec::new();
        for (key, _) in list_data_objects(store).await {
            let got = store.get(&key, GetRange::Full).await.expect("get object");
            let reader = RlogReader::new(got.data.as_ref(), &cfg).expect("open rlog");
            let (rows, _stats) = reader.scan(&Predicate::And(Vec::new())).expect("scan rlog");
            out.extend(rows.into_iter().map(|r| format!("{r:?}")));
        }
        out.sort();
        out
    }

    /// A flush target above one batch's encoded size is the object-count lever
    /// `--batch-rows` cannot be without a linear memory cost (issue #801).
    ///
    /// Geometry, pinned on both sides: the 4-row-group fixture holds 4 rows per
    /// group, one shard's `host` value per group, so `--batch-rows 4` with
    /// `--read-cursors 4` yields exactly 4 batches, each drawing one row from
    /// every group and therefore touching all 4 shards -- 16 (batch, shard)
    /// writes over 16 rows.
    ///
    /// At the default `--target-bytes 1` each of those 16 writes flushes inside
    /// its own `handle_write`: 16 objects. At 8 MiB none of them can, because a
    /// shard's whole share of the file is 4 one-row writes, so each shard
    /// flushes exactly once (released by the router's age trigger once every
    /// batch has been submitted, which `--pipeline-depth 4` guarantees): 4
    /// objects, one per shard. Same 16 rows, same decoded records.
    ///
    /// Prove-the-test: hardcode `target_bytes: 1` back into the `IngestConfig`
    /// in `load_instrumented` and the large-target side writes 16 objects, not
    /// 4 (`left: 16, right: 4`). Reverting `objects_written` to `tokens.len()`
    /// fails the same side at `left: 16, right: 4`, because one flush answers
    /// four batches' acks with the same token.
    #[tokio::test]
    async fn a_larger_target_bytes_writes_strictly_fewer_objects_for_the_same_rows() {
        use ravel_object_store::memory::MemoryStore;

        let shards = 4u32;
        let rows_per_group = 4usize;
        let (_dir, pq, m) = sorted_by_shard_fixture(shards, rows_per_group);
        let n_rows = (rows_per_group * shards as usize) as u64;

        let run = |target_bytes: usize| {
            let pq = pq.clone();
            let m = m.clone();
            async move {
                let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
                let report = load_instrumented(
                    Arc::clone(&store),
                    &pq,
                    "acme",
                    &m,
                    shards,
                    rows_per_group,
                    Some(shards as usize),
                    // Wide enough to hold every batch that accumulates into one
                    // flush; below this the loader would block on an ack no
                    // later batch can release.
                    rows_per_group,
                    DEFAULT_MAX_INFLIGHT_FLUSHES,
                    DEFAULT_DECODE_QUEUE_BATCHES,
                    target_bytes,
                    None,
                    NOW_NS,
                    // A real clock: above target 1 the tail of a load is
                    // released by the router's wall-clock age trigger, which a
                    // `FixedClock` can never fire.
                    Arc::new(SystemClock),
                    LoadPath::Columnar,
                    None,
                    None,
                )
                .await
                .expect("load succeeds");
                let stored = list_data_objects(store.as_ref()).await.len();
                (report, stored, decoded_records(store.as_ref()).await)
            }
        };

        let (small, stored_small, records_small) = run(DEFAULT_TARGET_BYTES).await;
        let (large, stored_large, records_large) = run(8 * 1024 * 1024).await;

        assert_eq!(small.rows_processed, n_rows);
        assert_eq!(large.rows_processed, n_rows);
        assert_eq!(
            stored_small, 16,
            "target 1: each of the 4 batches flushes on all 4 shards at once"
        );
        assert_eq!(
            stored_large, 4,
            "target 8 MiB: each shard accumulates all 4 batches into one object"
        );
        assert_eq!(
            small.objects_written(),
            stored_small,
            "the reported object count must equal what the store actually holds"
        );
        assert_eq!(
            large.objects_written(),
            stored_large,
            "the reported object count must equal what the store actually holds; \
             an ack that was answered by an earlier batch's flush repeats that \
             flush's token rather than naming a new object"
        );
        assert_eq!(
            records_small, records_large,
            "the same rows, decoded, regardless of how many objects hold them"
        );
    }

    /// Issue #971: what `--target-bytes` regime a value falls into is decided by
    /// one batch's PER-SHARD SLICE footprint, not by how large the byte figure
    /// looks next to the objects the load writes. Four targets over one input,
    /// each with a derivable object count.
    ///
    /// Geometry: 4 row groups of 16 rows, one shard's `host` value per group,
    /// read with `--read-cursors 4 --batch-rows 16`, so there are 4 batches and
    /// each batch puts 4 rows on every one of the 4 shards: 16 (batch, shard)
    /// writes over 64 rows. Every row carries a 4000-byte record attribute, so
    /// one row's estimated footprint is about 4.1 KB (the 4000 value bytes, a
    /// 56-byte pair header, the 7-byte key, the stream-attribute blob and 32
    /// fixed bytes), one slice's about 16.6 KB, and a shard's whole share of the
    /// file about 66 KB.
    ///
    /// - `1`: every write flushes itself. 4 x 4 = 16 objects.
    /// - `4096`: a quarter of one slice, so every write still reaches the target
    ///   on its own and flushes. 16 objects, IDENTICAL to `1`. This is the
    ///   reported defect in miniature: a byte figure that looks generous beside
    ///   the objects it produces (4 rows each) but sits far below the footprint
    ///   estimate it is actually compared against.
    /// - `24576`: above one slice and below two, so each shard flushes on every
    ///   second batch. 4 shards x 2 = 8 objects, exactly half of 16.
    /// - `1 MiB`: above a shard's whole 66 KB share, so each shard flushes once,
    ///   released by the age trigger. 4 objects, one per shard.
    ///
    /// Prove-the-test: hardcode `target_bytes: 1` into the `IngestConfig` in
    /// `load_instrumented` and both effective regimes fail (`left: 16, right:
    /// 8`), while the two no-effect regimes stay green, which is exactly the
    /// asymmetry the issue reported. Under-counting the magnitudes fails too:
    /// asserting 4 objects for the `24576` regime (a floor a fraction of the
    /// truth would clear) fails at `left: 8, right: 4`.
    #[tokio::test]
    async fn target_bytes_regimes_are_set_by_one_batchs_per_shard_slice() {
        use ravel_object_store::memory::MemoryStore;

        let shards = 4u32;
        let rows_per_group = 16usize;
        let batch_rows = 16usize;
        let (_dir, pq, _mapping_path, m) =
            fat_attr_sorted_by_shard_fixture(shards, rows_per_group, 4000);
        let n_rows = (rows_per_group * shards as usize) as u64;
        let batches = rows_per_group / (batch_rows / shards as usize);
        assert_eq!(batches, 4, "the geometry must yield 4 batches");

        let run = |target_bytes: usize| {
            let pq = pq.clone();
            let m = m.clone();
            async move {
                let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
                let report = load_instrumented(
                    Arc::clone(&store),
                    &pq,
                    "acme",
                    &m,
                    shards,
                    batch_rows,
                    Some(shards as usize),
                    // Wide enough to hold every batch that accumulates into one
                    // flush; below this the loader would block on an ack no
                    // later batch can release.
                    batches,
                    DEFAULT_MAX_INFLIGHT_FLUSHES,
                    DEFAULT_DECODE_QUEUE_BATCHES,
                    target_bytes,
                    None,
                    NOW_NS,
                    // A real clock: above the size trigger the tail of a load is
                    // released by the router's wall-clock age trigger, which a
                    // `FixedClock` can never fire.
                    Arc::new(SystemClock),
                    LoadPath::Columnar,
                    None,
                    None,
                )
                .await
                .expect("load succeeds");
                let stored = list_data_objects(store.as_ref()).await.len();
                (report, stored, decoded_records(store.as_ref()).await)
            }
        };

        let (default, objects_default, records_default) = run(DEFAULT_TARGET_BYTES).await;
        let (below, objects_below, records_below) = run(4096).await;
        let (mid, objects_mid, _) = run(24_576).await;
        let (whole, objects_whole, records_whole) = run(1024 * 1024).await;

        for report in [&default, &below, &mid, &whole] {
            assert_eq!(report.rows_processed, n_rows, "every run loads every row");
        }
        assert_eq!(
            objects_default, 16,
            "target 1: each of the 4 batches flushes on all 4 shards at once"
        );
        assert_eq!(
            objects_below, objects_default,
            "target 4096 is a quarter of one (batch, shard) slice's estimated footprint, so every \
             write still flushes itself: the same layout target 1 produces"
        );
        assert_eq!(
            objects_below, 16,
            "and that layout is 4 batches x 4 shards, pinned rather than compared"
        );
        assert_eq!(
            objects_mid, 8,
            "target 24576 sits above one slice and below two, so each shard flushes every second \
             batch: 4 shards x 2 flushes"
        );
        assert_eq!(
            objects_whole, 4,
            "target 1 MiB is above a shard's whole share of the file, so each shard flushes once"
        );
        for report in [&default, &below, &mid, &whole] {
            assert_eq!(
                report.tokens.len(),
                16,
                "every run acks the same 16 (batch, shard) writes whatever the target; only how \
                 many distinct objects answer them changes"
            );
        }
        assert_eq!(
            below.objects_written(),
            objects_below,
            "the reported object count must equal what the store holds"
        );
        assert_eq!(
            mid.objects_written(),
            objects_mid,
            "the reported object count must equal what the store holds"
        );
        assert_eq!(
            whole.objects_written(),
            objects_whole,
            "the reported object count must equal what the store holds"
        );
        assert_eq!(
            records_below, records_default,
            "the same rows, decoded, regardless of how many objects hold them"
        );
        assert_eq!(
            records_whole, records_default,
            "the same rows, decoded, regardless of how many objects hold them"
        );
    }

    /// The no-effect case reaches the operator through the real entry point
    /// (issue #971): a target that reproduced the `1` layout is reported on the
    /// warning stream, and one that changed the layout is not.
    ///
    /// Same fixture and geometry as
    /// `target_bytes_regimes_are_set_by_one_batchs_per_shard_slice`, driven
    /// through [`run_warning_to`] so the mapping file, the router, and the
    /// warning stream are the CLI's own.
    ///
    /// Prove-the-test: delete the `target_bytes_no_effect_warning` emit block in
    /// `run_warning_to` and the first assertion fails; make the helper return
    /// its message unconditionally and the 1 MiB case fails instead.
    #[tokio::test]
    async fn the_entry_point_reports_a_target_bytes_that_changed_nothing() {
        use ravel_object_store::memory::MemoryStore;

        let shards = 4u32;
        let (_dir, pq, mapping_path, _m) = fat_attr_sorted_by_shard_fixture(shards, 16, 4000);

        let run = |target_bytes: usize| {
            let pq = pq.clone();
            let mapping_path = mapping_path.clone();
            async move {
                let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
                let mut sink: Vec<u8> = Vec::new();
                run_warning_to(
                    store,
                    &pq,
                    "acme",
                    &mapping_path,
                    shards,
                    16,
                    Some(shards as usize),
                    4,
                    DEFAULT_MAX_INFLIGHT_FLUSHES,
                    DEFAULT_DECODE_QUEUE_BATCHES,
                    target_bytes,
                    None,
                    NOW_NS,
                    &mut sink,
                )
                .await
                .expect("the load itself succeeds; an ineffective target is a warning");
                String::from_utf8(sink).expect("warnings are utf-8")
            }
        };

        let emitted = run(4096).await;
        assert!(
            emitted.contains("--target-bytes 4096 did not change this load's object layout"),
            "the ineffective target is named with its value: {emitted}"
        );
        assert!(
            emitted.contains("ESTIMATED in-memory footprint")
                && emitted.contains("about 4 rows here"),
            "and the message states the unit and the slice it had to clear: {emitted}"
        );

        let effective = run(1024 * 1024).await;
        assert!(
            !effective.contains("did not change this load's object layout"),
            "a target that collapsed 16 writes into 4 objects must not be reported as inert: \
             {effective}"
        );
    }

    /// [`target_bytes_no_effect_warning`]'s three preconditions, each pinned by
    /// the case that would misfire without it. `writes` is
    /// `LoadReport::tokens`, one entry per (batch, shard) ack, so a flush that
    /// answered several batches shows up as a repeated token.
    ///
    /// Prove-the-test: delete the `objects < writes` early return and the
    /// accumulating case starts warning (its `is_none` assertion fails at
    /// "a repeated token is a flush that answered two batches"); delete the
    /// `< 2` writes-per-shard guard and the one-write-per-shard case starts
    /// warning; change the `target_bytes <= DEFAULT_TARGET_BYTES` guard to `<`
    /// and the default-target case starts warning.
    #[test]
    fn the_no_effect_warning_fires_only_when_the_target_is_what_did_nothing() {
        let token = |shard: u32, seq: u64| CommitToken {
            shard,
            writer_id: uuid::Uuid::nil(),
            epoch: 1,
            seq,
            ingest_hour_bucket: 7,
        };
        let report = |tokens: Vec<CommitToken>| LoadReport {
            tokens,
            ..LoadReport::default()
        };

        // Two writes on one shard, two distinct objects: nothing accumulated.
        let inert = report(vec![token(0, 1), token(0, 2)]);
        let warning = target_bytes_no_effect_warning(4096, &inert, 16, 4)
            .expect("two writes, two objects, one shard: the target did nothing");
        assert!(
            warning.contains("All 2 (batch, shard) writes flushed as their own object (2 objects)"),
            "the message reports the observed counts: {warning}"
        );
        assert!(
            warning.contains("about 4 rows here, at --batch-rows 16 over 4 shards"),
            "and the slice threshold it derives from the geometry: {warning}"
        );

        assert!(
            target_bytes_no_effect_warning(DEFAULT_TARGET_BYTES, &inert, 16, 4).is_none(),
            "the default target is not a no-op, it is the documented per-batch layout"
        );

        // Two writes answered by one flush: the same token repeats, so the
        // target held a buffer open.
        let accumulating = report(vec![token(0, 1), token(0, 1)]);
        assert!(
            target_bytes_no_effect_warning(4096, &accumulating, 16, 4).is_none(),
            "a repeated token is a flush that answered two batches: the target worked"
        );

        // One write per shard: no buffer could have spanned two writes at any
        // target, so the target is not what to blame.
        let single = report(vec![token(0, 1), token(1, 1), token(2, 1), token(3, 1)]);
        assert!(
            target_bytes_no_effect_warning(4096, &single, 16, 4).is_none(),
            "no shard was written twice, so nothing could have accumulated"
        );
    }

    /// The ack semantics a larger `--target-bytes` changes (issue #801,
    /// deliverable 3). A Strict write's ack is answered from `ack_waiters` in
    /// `crates/ravel-ingest/src/log_shard.rs`, which only runs once that
    /// buffer's flush has published its object and commit record. So an ack
    /// still means durable -- but above target 1 the flush that answers it is
    /// triggered by a LATER batch (or by the age trigger), not by the write
    /// itself, so the ack now waits for one.
    ///
    /// Pinned without a timing band: under a `FixedClock` the age trigger can
    /// never fire, and at an 8 MiB target no write in this 16-row fixture can
    /// reach the size trigger either. With no trigger reachable, the load
    /// cannot finish and -- the part that would be false under the old
    /// semantics -- the store holds zero data objects while it is suspended.
    /// The store is read with the load future still alive and pinned, so no
    /// shutdown flush from dropping the router can race the observation.
    ///
    /// Prove-the-test: hardcode `target_bytes: 1` back into the `IngestConfig`
    /// in `load_instrumented` and the load completes inside the window, hitting
    /// the `panic!` arm ("the load must not complete...").
    #[tokio::test]
    async fn a_strict_ack_above_target_one_waits_for_a_later_batchs_flush() {
        use ravel_object_store::memory::MemoryStore;

        let shards = 4u32;
        let rows_per_group = 4usize;
        let (_dir, pq, m) = sorted_by_shard_fixture(shards, rows_per_group);
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());

        let load_fut = load_instrumented(
            Arc::clone(&store),
            &pq,
            "acme",
            &m,
            shards,
            rows_per_group,
            Some(shards as usize),
            rows_per_group,
            DEFAULT_MAX_INFLIGHT_FLUSHES,
            DEFAULT_DECODE_QUEUE_BATCHES,
            8 * 1024 * 1024,
            None,
            NOW_NS,
            Arc::new(FixedClock(NOW_NS)),
            LoadPath::Columnar,
            None,
            None,
        );
        tokio::pin!(load_fut);

        let stored = tokio::select! {
            _ = &mut load_fut => panic!(
                "the load must not complete: at an 8 MiB target no write reaches the size \
                 trigger, and a FixedClock never fires the age trigger, so no ack can be \
                 answered"
            ),
            () = tokio::time::sleep(Duration::from_millis(300)) => {
                list_data_objects(store.as_ref()).await
            }
        };
        assert_eq!(
            stored.len(),
            0,
            "no flush has been triggered, so nothing is durable yet: {stored:?}"
        );
    }

    /// `--target-bytes 0` is rejected rather than silently behaving as `1`
    /// (`est_bytes >= 0` holds for an empty buffer, so `0` is not a smaller
    /// target than `1`), matching the other operator-facing lever guards.
    ///
    /// Prove-the-test: delete the `target_bytes == 0` guard in
    /// `load_instrumented` and the load runs to completion instead, failing
    /// `expect_err`.
    #[tokio::test]
    async fn target_bytes_of_zero_is_rejected() {
        use ravel_object_store::memory::MemoryStore;

        let (_dir, pq, m) = sorted_by_shard_fixture(4, 4);
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let err = load_instrumented(
            store,
            &pq,
            "acme",
            &m,
            4,
            4,
            Some(4),
            1,
            DEFAULT_MAX_INFLIGHT_FLUSHES,
            DEFAULT_DECODE_QUEUE_BATCHES,
            0,
            None,
            NOW_NS,
            Arc::new(FixedClock(NOW_NS)),
            LoadPath::Columnar,
            None,
            None,
        )
        .await
        .expect_err("target_bytes of 0 is rejected");
        assert!(
            matches!(err, LoadError::Setup(_)),
            "a typed setup error, got: {err}"
        );
        assert!(
            err.to_string()
                .contains("--target-bytes must be at least 1"),
            "the error names the lever: {err}"
        );
    }

    /// `--max-flush-delay` unset (`None`) leaves the router's age trigger at
    /// its `IngestConfig::default()` value, so an omitted flag builds a
    /// byte-for-byte default config (issue #801, deliverable 1). The config
    /// field is the thing that flows to the router, so it is what this asserts.
    ///
    /// Prove-the-test: change `build_ingest_config` to substitute any other
    /// duration when `max_flush_delay` is `None` (e.g.
    /// `Duration::from_secs(1)`) and this fails at
    /// `left: 1s, right: 2s`.
    #[test]
    fn max_flush_delay_unset_keeps_the_default_age_trigger() {
        let cfg = build_ingest_config(4, DEFAULT_TARGET_BYTES, DEFAULT_MAX_INFLIGHT_FLUSHES, None);
        assert_eq!(
            cfg.max_flush_delay,
            IngestConfig::default().max_flush_delay,
            "an unset --max-flush-delay must not change the router's age trigger"
        );
    }

    /// `--max-flush-delay 10m` reaches `IngestConfig::max_flush_delay` as
    /// exactly 600s (issue #801, deliverable 2). The humantime parse lives in
    /// `parse_max_flush_delay`; this pins that a `Some(_)` overrides the field
    /// exactly, with no scaling or rounding.
    ///
    /// Prove-the-test: change the `Some` arm of `build_ingest_config` to ignore
    /// its argument (fall through to the default) and this fails at
    /// `left: 2s, right: 600s`.
    #[test]
    fn max_flush_delay_set_reaches_the_config_exactly() {
        let cfg = build_ingest_config(
            4,
            DEFAULT_TARGET_BYTES,
            DEFAULT_MAX_INFLIGHT_FLUSHES,
            Some(Duration::from_secs(600)),
        );
        assert_eq!(
            cfg.max_flush_delay,
            Duration::from_secs(600),
            "--max-flush-delay 10m must arrive as exactly 600s"
        );
    }

    /// One side of the [`max_flush_delay_decides_whether_two_writes_coalesce`]
    /// pair. Every argument of the load is fixed here, so the only thing the
    /// two calls differ in is `max_flush_delay`.
    ///
    /// The injected clock advances 5s of load time every 20ms of real time and
    /// stops once it has advanced `GATE_NS`; the decoder is gated on that same
    /// point, blocking after the first batch is queued until the clock reaches
    /// it. So batch 1's write sits alone in the shard buffer across the whole
    /// advance, and batch 2's write arrives only once the clock has stopped
    /// moving. Whether the first buffer survives that window is the delay's
    /// decision and nothing else's.
    async fn load_two_writes_across_one_clock_advance(
        store: Arc<dyn ObjectStoreBackend>,
        pq: &Path,
        m: &Mapping,
        max_flush_delay: Duration,
    ) -> LoadReport {
        // TARGET between one 4.1 KB slice and two, with margin on both sides,
        // so one write never reaches it and two always do.
        const TARGET: usize = 6_000;
        const ADVANCE_STEP_NS: i64 = 5 * 1_000_000_000;
        const GATE_NS: i64 = 100 * 1_000_000_000;

        let clock = TestClock::new(NOW_NS);
        let gate = Arc::clone(&clock);
        let on_batch_queued: BuildStartHook = Arc::new(move || {
            while gate.now_ns() < NOW_NS + GATE_NS {
                std::thread::sleep(Duration::from_millis(1));
            }
        });

        let load_fut = load_instrumented(
            store,
            pq,
            "acme",
            m,
            1,       // one shard: both writes land in the same buffer
            1,       // one row per batch
            Some(1), // one read cursor: batches are strictly sequential
            4,       // pipeline depth above the write count: no mid-loop ack wait
            DEFAULT_MAX_INFLIGHT_FLUSHES,
            1, // one queued batch: the loader parks on `recv` between writes
            TARGET,
            Some(max_flush_delay),
            NOW_NS,
            Arc::clone(&clock) as Arc<dyn Clock>,
            LoadPath::Columnar,
            None,
            Some(on_batch_queued),
        );
        tokio::pin!(load_fut);
        loop {
            tokio::select! {
                report = &mut load_fut => break report.expect("the load completes"),
                () = tokio::time::sleep(Duration::from_millis(20)) => {
                    if clock.now_ns() < NOW_NS + GATE_NS {
                        clock.advance_ns(ADVANCE_STEP_NS);
                    }
                }
            }
        }
    }

    /// The `--max-flush-delay` lever decides an object layout, not just a config
    /// field (issue #801, deliverable 3). Both sides run
    /// [`load_two_writes_across_one_clock_advance`]: same two rows, same
    /// `--target-bytes`, same `--pipeline-depth`, same injected clock advanced
    /// by the same pattern, only the delay flipped.
    ///
    /// - `LONG` (1h) outlasts the 100s the clock ever advances, so nothing can
    ///   age out. The second write merges into the first's buffer, pushes it
    ///   past the target and flushes both as ONE object by size.
    /// - `SHORT` (1s) is shorter than a single 5s advance step, so the first
    ///   write's buffer ages out while the decoder is gated: one object by age.
    ///   The second write then lands in a fresh buffer with the clock already
    ///   stopped, so nothing can age it either, and the loader's end-of-input
    ///   flush publishes it. TWO objects, exactly one of them aged.
    ///
    /// The counts come from the real router's issue #983 trigger mix, so each
    /// side pins which trigger opened each object, not just how many there were.
    ///
    /// Prove-the-test, flipping only the delay: passing `SHORT` to the coalesced
    /// side fails its object count at `left: 2, right: 1` (the first write ages
    /// out during the gate instead of waiting for the second), and passing
    /// `LONG` to the split side fails at `left: 1, right: 2`, with its mix at
    /// `size: 1, age: 0, final: 0` where `size: 0, age: 1, final: 1` is
    /// required.
    #[tokio::test]
    async fn max_flush_delay_decides_whether_two_writes_coalesce() {
        use ravel_object_store::memory::MemoryStore;

        const LONG: Duration = Duration::from_secs(3600);
        const SHORT: Duration = Duration::from_secs(1);

        let (_dir, pq, _mapping_path, m) = fat_attr_sorted_by_shard_fixture(1, 2, 4000);

        let coalesced_store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let coalesced =
            load_two_writes_across_one_clock_advance(Arc::clone(&coalesced_store), &pq, &m, LONG)
                .await;

        assert_eq!(
            coalesced.objects_written(),
            1,
            "a delay longer than the advance keeps the first buffer alive for the second write: \
             one object"
        );
        assert_eq!(
            coalesced.flush_mix_report().totals,
            FlushMixCounts {
                size: 1,
                age: 0,
                final_drain: 0,
            },
            "the single object is a size flush; nothing ages under a delay longer than the advance"
        );

        let split_store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let split =
            load_two_writes_across_one_clock_advance(Arc::clone(&split_store), &pq, &m, SHORT)
                .await;

        assert_eq!(
            split.objects_written(),
            2,
            "a delay shorter than the advance ages the first write out before the second arrives: \
             two objects"
        );
        assert_eq!(
            split.flush_mix_report().totals,
            FlushMixCounts {
                size: 0,
                age: 1,
                final_drain: 1,
            },
            "the first object is the aged-out buffer, the second is the tail the end-of-input \
             flush published"
        );

        // Same rows either way, only their object layout differs.
        assert_eq!(coalesced.rows_processed, 2);
        assert_eq!(split.rows_processed, 2);
        assert_eq!(
            decoded_records(coalesced_store.as_ref()).await,
            decoded_records(split_store.as_ref()).await,
            "the same two rows, decoded, regardless of how many objects hold them"
        );
    }

    /// A load whose last slice stays under `--target-bytes` completes under a
    /// raised `--max-flush-delay`, with that tail published by the loader's
    /// end-of-input flush (issue #801). This is the run-burner the flag shipped
    /// with: the tail's Strict ack has nothing left to release it by size, so
    /// when the force-flush ran only after the in-flight window was drained,
    /// the ack waited on the age trigger, blew the deadline, and returned an
    /// error from a load whose every object had already landed.
    ///
    /// Geometry: one shard, four 4 KB-payload rows read one per batch, so each
    /// write's estimated per-shard slice is about 4.1 KB. `TARGET = 10_000`
    /// sits above two slices and below three, so writes 1-3 flush as one object
    /// by size and write 4 is left alone under the target. `--pipeline-depth 5`
    /// is above the batch count, so the loader never waits on an ack mid-loop.
    /// The clock is fixed, which makes the assertion sharp: the age trigger
    /// cannot fire at all here, so the tail's object exists only because the
    /// manual flush published it.
    ///
    /// Prove-the-test: move `router.flush_all()` back after `drain_inflight`
    /// and the load never returns a report -- the tail ack is answered by
    /// nothing, and after `write_ack_deadline` (10m + 1m here) it fails with
    /// `LoadError::Flush` carrying `timed out waiting for shard ack`.
    #[tokio::test]
    async fn a_tail_below_target_is_published_by_the_end_of_input_flush() {
        use ravel_object_store::memory::MemoryStore;

        const TARGET: usize = 10_000;
        let (_dir, pq, _mapping_path, m) = fat_attr_sorted_by_shard_fixture(1, 4, 4000);

        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let report = load_instrumented(
            Arc::clone(&store),
            &pq,
            "acme",
            &m,
            1,
            1,
            Some(1),
            5, // pipeline depth above the batch count: no mid-loop ack wait
            DEFAULT_MAX_INFLIGHT_FLUSHES,
            1,
            TARGET,
            Some(Duration::from_secs(600)),
            NOW_NS,
            Arc::new(FixedClock(NOW_NS)),
            LoadPath::Columnar,
            None,
            None,
        )
        .await
        .expect("a raised delay must not strand the tail buffer's ack");

        assert_eq!(report.rows_processed, 4, "every row is durable");
        assert_eq!(
            report.objects_written(),
            2,
            "three slices reach the target as one object, the fourth is the tail"
        );
        assert_eq!(
            report.flush_mix_report().totals,
            FlushMixCounts {
                size: 1,
                age: 0,
                final_drain: 1,
            },
            "the tail is published by the manual end-of-input flush, not by the age trigger"
        );
        assert_eq!(
            decoded_records(store.as_ref()).await.len(),
            4,
            "the two objects hold all four rows"
        );
    }

    /// The Strict ack deadline scales with the configured age trigger (issue
    /// #801). A tail or under-target buffer is answered by the age trigger, so a
    /// deadline that does not outlast the configured delay times out on exactly
    /// the buffers the raised delay was set to let accumulate. An unset flag
    /// keeps the deadline it always had, to the second.
    ///
    /// Prove-the-test: return `WRITE_ACK_DEADLINE_FLOOR` unconditionally from
    /// `write_ack_deadline` and the 10m case fails at `left: 60s, right: 660s`.
    #[test]
    fn write_ack_deadline_scales_with_the_configured_flush_delay() {
        assert_eq!(
            write_ack_deadline(None),
            Duration::from_secs(60),
            "an unset --max-flush-delay leaves the deadline at exactly 60s"
        );
        assert_eq!(
            write_ack_deadline(Some(Duration::from_secs(600))),
            Duration::from_secs(660),
            "--max-flush-delay 10m gives an 11m deadline: the delay plus one minute of margin"
        );
        assert_eq!(
            write_ack_deadline(Some(Duration::ZERO)),
            Duration::from_secs(60),
            "a delay under the floor cannot shorten the deadline"
        );
        let long = Duration::from_secs(3600);
        assert!(
            write_ack_deadline(Some(long)) > long,
            "the deadline always outlasts the age trigger it has to wait for"
        );
    }

    /// `strict_visibility_budget_ns` follows the configured `max_flush_delay`
    /// (ADR-0076 decision 4), exactly as ravel-server's own router construction
    /// derives it. The field is metrics-only on this path, but the coupling is
    /// documented on `IngestConfig` and a config that raises the delay while
    /// leaving the budget at the 2s-based default contradicts it.
    ///
    /// Prove-the-test: drop the `strict_visibility_budget_ns` field from
    /// `build_ingest_config` (falling through to `IngestConfig::default()`) and
    /// the raised-delay case fails at `left: 2500000000, right: 600500000000`.
    #[test]
    fn strict_visibility_budget_follows_the_configured_flush_delay() {
        let raised = build_ingest_config(
            4,
            DEFAULT_TARGET_BYTES,
            DEFAULT_MAX_INFLIGHT_FLUSHES,
            Some(Duration::from_secs(600)),
        );
        assert_eq!(
            raised.strict_visibility_budget_ns,
            600_000_000_000 + STRICT_VISIBILITY_RESERVE_NS,
            "the budget is the configured delay plus the reserve, never the default's"
        );
        let delay_ns = i64::try_from(raised.max_flush_delay.as_nanos()).expect("delay fits i64");
        assert!(
            raised.strict_visibility_budget_ns > delay_ns,
            "the budget must exceed the delay, not equal it: equal collapses the corridor"
        );

        let unset =
            build_ingest_config(4, DEFAULT_TARGET_BYTES, DEFAULT_MAX_INFLIGHT_FLUSHES, None);
        assert_eq!(
            unset.strict_visibility_budget_ns,
            IngestConfig::default().strict_visibility_budget_ns,
            "an unset flag still builds a byte-for-byte default config"
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
            DEFAULT_MAX_INFLIGHT_FLUSHES,
            DEFAULT_DECODE_QUEUE_BATCHES,
            DEFAULT_TARGET_BYTES,
            None,
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
            DEFAULT_MAX_INFLIGHT_FLUSHES,
            DEFAULT_DECODE_QUEUE_BATCHES,
            DEFAULT_TARGET_BYTES,
            None,
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
            DEFAULT_MAX_INFLIGHT_FLUSHES,
            DEFAULT_DECODE_QUEUE_BATCHES,
            DEFAULT_TARGET_BYTES,
            None,
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

    /// Write `batch` to a Parquet file with the default writer properties (which
    /// dictionary-encode a `BYTE_ARRAY` column until its dictionary outgrows the
    /// page-size limit). The returned `TempDir` must stay alive while the path
    /// is read.
    fn write_parquet(batch: &RecordBatch) -> (tempfile::TempDir, std::path::PathBuf) {
        use parquet::arrow::ArrowWriter;
        let dir = tempfile::tempdir().expect("tempdir");
        let pq = dir.path().join("rt.parquet");
        let file = std::fs::File::create(&pq).expect("create parquet");
        let mut w = ArrowWriter::try_new(file, batch.schema(), None).expect("arrow writer");
        w.write(batch).expect("write batch");
        w.close().expect("close writer");
        (dir, pq)
    }

    /// The dictionary-preserving reader schema the loader derives for `pq`, or
    /// `None` when no column qualifies. Parses the footer once, exactly as the
    /// loader does, then hands the shared metadata to [`load_reader_schema`].
    fn reader_schema_for(pq: &Path) -> Option<SchemaRef> {
        let metadata = read_input_metadata(&FileInput { path: pq }).expect("read metadata");
        load_reader_schema(&metadata)
    }

    /// A `ChunkReader` over the input bytes that counts Parquet footer parses.
    ///
    /// Every metadata parse issues exactly one `get_read` at `len - 8`: that
    /// eight-byte footer tail carries the metadata length and the `PAR1` magic,
    /// and `parse_metadata` reads it before anything else (parquet
    /// `file::metadata::reader`). A data-page reader built from already-parsed
    /// metadata (`new_with_metadata`) never touches that tail. Counting reads at
    /// that offset therefore counts footer parses and nothing else.
    struct CountingReader {
        inner: bytes::Bytes,
        footer_reads: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl parquet::file::reader::Length for CountingReader {
        fn len(&self) -> u64 {
            self.inner.len() as u64
        }
    }

    impl parquet::file::reader::ChunkReader for CountingReader {
        type T = <bytes::Bytes as parquet::file::reader::ChunkReader>::T;

        fn get_read(&self, start: u64) -> parquet::errors::Result<Self::T> {
            if start == self.inner.len() as u64 - parquet::file::FOOTER_SIZE as u64 {
                self.footer_reads
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            }
            parquet::file::reader::ChunkReader::get_read(&self.inner, start)
        }

        fn get_bytes(&self, start: u64, length: usize) -> parquet::errors::Result<bytes::Bytes> {
            parquet::file::reader::ChunkReader::get_bytes(&self.inner, start, length)
        }
    }

    /// An [`InputReaders`] that hands out [`CountingReader`]s over the same file
    /// bytes, all sharing one footer-parse counter. Every `open` (the initial
    /// metadata read plus each stride cursor's data reader) increments the same
    /// counter, so `footer_reads` is the total footer parses for the whole load
    /// setup.
    struct CountingInput {
        bytes: bytes::Bytes,
        footer_reads: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl CountingInput {
        fn new(pq: &Path) -> Self {
            let bytes = bytes::Bytes::from(std::fs::read(pq).expect("read fixture bytes"));
            Self {
                bytes,
                footer_reads: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            }
        }

        fn footer_reads(&self) -> usize {
            self.footer_reads.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl InputReaders for CountingInput {
        type Reader = CountingReader;

        fn open(&self) -> Result<CountingReader, LoadError> {
            Ok(CountingReader {
                inner: self.bytes.clone(),
                footer_reads: Arc::clone(&self.footer_reads),
            })
        }
    }

    /// A Parquet fixture forced to `groups` row groups (one row each), so a load
    /// over it opens one stride cursor per row group. Returns the temp dir (kept
    /// alive), the path, and a mapping that reads the string column as a resource
    /// attribute.
    fn multi_row_group_fixture(groups: usize) -> (tempfile::TempDir, std::path::PathBuf, Mapping) {
        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;

        let dir = tempfile::tempdir().expect("tempdir");
        let pq = dir.path().join("groups.parquet");
        let ts: Vec<i64> = (0..groups as i64).map(|k| NOW_NS + k).collect();
        let svc: Vec<&str> = (0..groups).map(|k| ["api", "web"][k % 2]).collect();
        let b = batch(vec![("ts", i64_col(ts)), ("svc", str_col(svc))]);
        let file = std::fs::File::create(&pq).expect("create parquet");
        // One row per row group: `groups` rows written with a max group size of 1
        // flush a fresh row group each row.
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(1))
            .build();
        let mut w = ArrowWriter::try_new(file, b.schema(), Some(props)).expect("arrow writer");
        w.write(&b).expect("write batch");
        w.close().expect("close writer");

        let mut m = base_mapping();
        m.resource_attributes = vec![attr("service.name", "svc", ColType::Str)];
        (dir, pq, m)
    }

    /// The load setup parses the input's Parquet footer exactly once, no matter
    /// how many stride cursors it opens.
    ///
    /// The counter is the footer-tail read (`CountingReader`). The fixture has 8
    /// row groups and the load requests 8 read cursors, so
    /// `resolve_read_cursors` gives one cursor per row group; every one of them
    /// takes the shared `ArrowReaderMetadata` through the `new_with_metadata`
    /// builder line in `open_stride_cursors`, which is what holds the count to
    /// exactly one. Flip that builder to `try_new`/`try_new_with_options` and
    /// the count becomes `cursors + 1`; stop sharing the metadata with
    /// `row_group_row_counts` and `load_reader_schema` too and it becomes
    /// `cursors + 2`.
    #[test]
    fn load_setup_parses_the_footer_once() {
        const GROUPS: usize = 8;
        let (_dir, pq, _m) = multi_row_group_fixture(GROUPS);
        let source = CountingInput::new(&pq);

        // The exact setup sequence `run_load` runs, driven through the counting
        // input instead of a file on disk.
        let metadata = read_input_metadata(&source).expect("read metadata");
        let row_group_lens = row_group_row_counts(&metadata);
        assert_eq!(
            row_group_lens.len(),
            GROUPS,
            "the fixture is forced to one row group per row"
        );
        let cursor_count = resolve_read_cursors(Some(8), 4, row_group_lens.len());
        assert_eq!(
            cursor_count, GROUPS,
            "8 requested cursors over 8 row groups gives 8 cursors"
        );
        let cursors = open_stride_cursors(&source, &metadata, &row_group_lens, cursor_count, 1024)
            .expect("cursors");
        assert_eq!(cursors.len(), GROUPS, "one cursor per row group");

        assert_eq!(
            source.footer_reads(),
            1,
            "the whole load setup parses the footer exactly once; before #773 it \
             parsed cursors + 2 = {} times",
            cursor_count + 2
        );
    }

    /// #773: sharing the parsed footer changes nothing the load writes. The same
    /// fixture loaded through the (changed) stride-cursor path produces the exact
    /// same rows, objects, and columnar batches it did before.
    #[tokio::test]
    async fn shared_footer_load_output_is_unchanged() {
        use ravel_object_store::memory::MemoryStore;

        const GROUPS: usize = 8;
        let (_dir, pq, m) = multi_row_group_fixture(GROUPS);

        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let report = load(
            Arc::clone(&store),
            &pq,
            "acme",
            &m,
            4,
            1,
            Some(8),
            1,
            NOW_NS,
            Arc::new(FixedClock(NOW_NS)),
        )
        .await
        .expect("load succeeds");

        // Exact figures, pre-registered from the fixture shape: 8 rows, and with
        // batch_rows == 1 one columnar batch (hence one RLOG object) per row.
        assert_eq!(report.rows_processed, GROUPS as u64, "every row is written");
        assert_eq!(
            report.columnar_batches_built, GROUPS as u64,
            "batch_rows == 1 builds one columnar batch per row"
        );
        assert_eq!(
            report.objects_written(),
            GROUPS,
            "one shard flush per batch is one object per row"
        );
    }

    /// Read `pq` back as one RecordBatch. `loader_schema` selects the reader the
    /// loader actually opens: `true` applies [`load_reader_schema`], the
    /// dictionary-preserving derivation `open_stride_cursors` uses (#660);
    /// `false` lets the reader infer on its own, which is what the loader did
    /// before #660 and what the byte-identity anchor compares against.
    fn read_parquet(pq: &Path, loader_schema: bool) -> RecordBatch {
        let schema = if loader_schema {
            reader_schema_for(pq)
        } else {
            None
        };
        let f = std::fs::File::open(pq).expect("open parquet");
        let builder = match &schema {
            Some(s) => ParquetRecordBatchReaderBuilder::try_new_with_options(
                f,
                ArrowReaderOptions::new().with_schema(Arc::clone(s)),
            ),
            None => ParquetRecordBatchReaderBuilder::try_new(f),
        }
        .expect("reader builder");
        let reader = builder
            // Every fixture here fits one row group, so one oversized read batch
            // yields the whole file and the assertion below holds.
            .with_batch_size(1 << 20)
            .build()
            .expect("reader");
        let mut batches: Vec<RecordBatch> = reader.map(|b| b.expect("read batch")).collect();
        assert_eq!(batches.len(), 1, "fixture fits one read batch");
        batches.pop().expect("one batch")
    }

    /// The encoding of every DATA page recorded for `column`'s chunk in each row
    /// group of `pq`, read out of the footer's page encoding statistics. Used to
    /// prove whether the writer dictionary-encoded a column or fell back to
    /// plain, rather than assuming it: the chunk-level `encodings` list shows
    /// `RLE_DICTIONARY` in both cases, since a fallback keeps the pages it wrote
    /// before overflowing.
    fn data_page_encodings(pq: &Path, column: &str) -> Vec<Vec<parquet::basic::Encoding>> {
        let f = std::fs::File::open(pq).expect("open parquet");
        let builder = ParquetRecordBatchReaderBuilder::try_new(f).expect("reader builder");
        let md = builder.metadata();
        let leaf = md
            .file_metadata()
            .schema_descr()
            .columns()
            .iter()
            .position(|c| c.path().parts().len() == 1 && c.path().parts()[0] == *column)
            .expect("column is a top-level leaf");
        md.row_groups()
            .iter()
            .map(|rg| {
                rg.column(leaf)
                    .page_encoding_stats_mask()
                    .expect("the footer records page encoding statistics")
                    .encodings()
                    .collect()
            })
            .collect()
    }

    /// The `StrColumnDict` attached to dynamic column `pos`, if any.
    /// `dyn_col_dicts` is left empty (not a vec of `None`) when no column in the
    /// batch carries a dictionary, so indexing it directly is not safe.
    fn col_dict(b: &ColumnarLogBatch, pos: usize) -> Option<&StrColumnDict> {
        b.dyn_col_dicts.get(pos).and_then(Option::as_ref)
    }

    /// Write `batch` to Parquet and read it back through the LOADER's reader, so
    /// a column the file dictionary-encodes arrives as an Arrow `Dictionary`
    /// (#660) exactly as it does under `ravel-cli load --parquet`.
    fn roundtrip_parquet(batch: &RecordBatch) -> RecordBatch {
        let (_dir, pq) = write_parquet(batch);
        read_parquet(&pq, true)
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
    /// It is also the #660 anchor: the rich fixture is read twice from the same
    /// file, once through the loader's dictionary-preserving schema and once
    /// through plain inference, and the two RLOG objects must be equal and hash
    /// to the pinned BLAKE3.
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
        // One file, read two ways: `on` is the loader's reader, which applies
        // #660's dictionary-preserving schema; `off` lets the reader infer on
        // its own, which is what the loader opened before #660.
        let (_rich_dir, rich_pq) = write_parquet(&rich);
        let on = read_parquet(&rich_pq, true);
        let off = read_parquet(&rich_pq, false);

        let dict_idx = on.schema().index_of("dictcol").expect("dictcol present");
        let plain_idx = on.schema().index_of("plaincol").expect("plaincol present");

        // A column arrow-written as a `DictionaryArray` comes back a Dictionary
        // either way: `ArrowWriter` embeds the Arrow schema that says so.
        assert!(
            matches!(off.column(dict_idx).data_type(), DataType::Dictionary(_, _)),
            "an arrow-written dictionary column survives the Parquet round trip as a Dictionary"
        );
        assert!(
            matches!(on.column(dict_idx).data_type(), DataType::Dictionary(_, _)),
            "the loader's schema leaves an already-dictionary column as it is"
        );

        // #605's expectation, flipped on purpose by #660. `plaincol` was written
        // from a plain `StringArray`, so the embedded Arrow schema calls it Utf8
        // and the reader infers Utf8 (`off`) even though the file
        // dictionary-encodes the column. The loader's schema reads the chunk
        // encodings instead and types it a Dictionary (`on`).
        assert!(
            matches!(off.column(plain_idx).data_type(), DataType::Utf8),
            "without the loader's schema a plain-written string column arrives Utf8"
        );
        assert_eq!(
            on.column(plain_idx).data_type(),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            "the loader's reader keeps the file's dictionary on a plain-written string column"
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

        let col_off = assert_paths_match(&off, &m);
        let col_on = assert_paths_match(&on, &m);

        // The load-bearing invariant: the extra dictionary the loader's reader
        // now carries changes nothing the writer emits. Same 4 records, same
        // object, byte for byte, with the schema on and off.
        let bytes_off = columnar_object(col_off.clone());
        let bytes_on = columnar_object(col_on.clone());
        assert_eq!(
            bytes_on, bytes_off,
            "the RLOG object must not depend on whether a column arrived dictionary-encoded"
        );
        // Pinned so a drift in either direction is a test failure, not a silent
        // re-baseline of both sides at once. The value moves only with the
        // writer: this one is the RLOG version 4 layout (ADR-0699).
        const RICH_OBJECT_BLAKE3: &str =
            "363ec1b22915ea2b11cb04e887bb4afb6629503962ea9916ca7719c28638be48";
        assert_eq!(
            blake3::hash(&bytes_off).to_hex().as_str(),
            RICH_OBJECT_BLAKE3,
            "object bytes without the dictionary-preserving schema"
        );
        assert_eq!(
            blake3::hash(&bytes_on).to_hex().as_str(),
            RICH_OBJECT_BLAKE3,
            "object bytes with the dictionary-preserving schema"
        );

        let dict_pos = col_on
            .dyn_columns
            .iter()
            .position(|c| c.name == "dictkey")
            .expect("dictkey column");
        let plain_pos = col_on
            .dyn_columns
            .iter()
            .position(|c| c.name == "plainkey")
            .expect("plainkey column");

        // Without the loader's schema, only the arrow-written dictionary column
        // reaches the StrColumnDict fast path (#605's original expectation).
        assert!(
            col_dict(&col_off, dict_pos).is_some(),
            "the arrow-written dictionary column passes through as a StrColumnDict"
        );
        assert!(
            col_dict(&col_off, plain_pos).is_none(),
            "without the loader's schema the plain-written column stays plain"
        );
        // With it, so does the plain-written one, because the file
        // dictionary-encodes it (#660).
        assert!(
            col_dict(&col_on, dict_pos).is_some(),
            "the arrow-written dictionary column still passes through as a StrColumnDict"
        );
        assert!(
            col_dict(&col_on, plain_pos).is_some(),
            "the plain-written but dictionary-encoded column now passes through as a StrColumnDict"
        );
    }

    /// #660: a plain `StringArray` with repeated values, written by
    /// `ArrowWriter` (which dictionary-encodes `BYTE_ARRAY` by default), now
    /// comes back through the loader's reader as a `Dictionary` and reaches the
    /// `StrColumnDict` fast path with the file's exact distinct set.
    ///
    /// This deliberately flips #605's expectation. Before the loader supplied a
    /// reader schema, the embedded Arrow schema said Utf8, arrow-rs fused the
    /// Parquet dictionary away, and the column took the plain per-row path;
    /// that is exactly what the `loader_schema = false` half still shows, and it
    /// is what the whole test asserted before this change. Its red form is the
    /// `assert_eq!` on `on.column(cat_idx).data_type()`: against the pre-#660
    /// reader it reads `Utf8` where `Dictionary(Int32, Utf8)` is expected.
    ///
    /// Prove-the-test: confirmed by making `load_reader_schema` return `None`
    /// unconditionally, which is exactly the pre-#660 reader. That assertion
    /// tripped with `left: Utf8, right: Dictionary(Int32, Utf8)`.
    #[test]
    fn repeated_value_string_column_reaches_the_dictionary_path() {
        const ROWS: usize = 1_000;
        const DISTINCT: usize = 3;
        let values = ["alpha", "beta", "gamma"];

        let ts: Vec<i64> = (0..ROWS as i64).map(|i| NOW_NS + i).collect();
        let cat: Vec<&str> = (0..ROWS).map(|i| values[i % DISTINCT]).collect();
        let b = batch(vec![("ts", i64_col(ts)), ("cat", str_col(cat))]);
        let (_dir, pq) = write_parquet(&b);

        // The premise: the writer really did dictionary-encode every data page.
        let encodings = data_page_encodings(&pq, "cat");
        assert_eq!(encodings.len(), 1, "one row group");
        assert!(
            !encodings[0].is_empty() && encodings[0].iter().copied().all(is_dictionary_encoding),
            "the writer dictionary-encoded every data page of `cat`: {:?}",
            encodings[0]
        );

        let mut m = base_mapping();
        m.attributes = vec![attr("cat", "cat", ColType::Str)];

        let on = read_parquet(&pq, true);
        let cat_idx = on.schema().index_of("cat").expect("cat present");
        assert_eq!(
            on.column(cat_idx).data_type(),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            "the loader's reader yields the file's dictionary for a repeated-value string column"
        );

        let col_on = assert_paths_match(&on, &m);
        assert_eq!(col_on.num_rows, ROWS, "every row is built");
        let pos = col_on
            .dyn_columns
            .iter()
            .position(|c| c.name == "cat")
            .expect("cat column");
        let dict = col_dict(&col_on, pos).expect("the column carries a StrColumnDict");
        assert_eq!(
            dict.distinct.len(),
            DISTINCT,
            "the StrColumnDict holds exactly the 3 distinct values"
        );
        assert_eq!(dict.ids.len(), ROWS, "one dictionary id per present cell");

        // The pre-#660 reader on the same file, for contrast: Utf8, plain path.
        let off = read_parquet(&pq, false);
        assert!(
            matches!(off.column(cat_idx).data_type(), DataType::Utf8),
            "plain inference fuses the Parquet dictionary away"
        );
        let col_off = assert_paths_match(&off, &m);
        assert!(
            col_dict(&col_off, pos).is_none(),
            "the plain path attaches no StrColumnDict"
        );
    }

    /// #660: a unique-per-row string column is left Utf8 and takes the plain
    /// path. Its dictionary outgrows the writer's default 1 MiB dictionary page
    /// limit, so the writer falls back to plain encoding, and the loader's rule
    /// preserves only an encoding the file carries -- it never forces one.
    ///
    /// The fallback is read out of the footer here rather than assumed: if a
    /// future writer default kept the column dictionary-encoded, the first
    /// assertion fails instead of the test silently proving nothing.
    ///
    /// Prove-the-test: confirmed by making `chunk_is_dictionary_encoded` return
    /// `true` unconditionally, the shape of the mistake this guards against. The
    /// `load_reader_schema(&pq).is_none()` assertion tripped.
    #[test]
    fn unique_per_row_string_column_stays_plain() {
        const ROWS: usize = 8_000;

        let ts: Vec<i64> = (0..ROWS as i64).map(|i| NOW_NS + i).collect();
        // ~256 bytes per value, so the dictionary passes 1 MiB well before the
        // last row and the writer falls back.
        let owned: Vec<String> = (0..ROWS).map(|i| format!("{i:0>256}")).collect();
        let uniq: Vec<&str> = owned.iter().map(String::as_str).collect();
        let b = batch(vec![("ts", i64_col(ts)), ("uniq", str_col(uniq))]);
        let (_dir, pq) = write_parquet(&b);

        let encodings = data_page_encodings(&pq, "uniq");
        assert_eq!(encodings.len(), 1, "one row group");
        assert!(
            encodings[0].iter().any(|e| !is_dictionary_encoding(*e)),
            "the writer's dictionary overflowed and it fell back to plain data pages: {:?}",
            encodings[0]
        );

        // The derivation leaves the column alone, so no schema is supplied at
        // all for this file.
        assert!(
            reader_schema_for(&pq).is_none(),
            "no column qualifies, so the loader opens the reader with default options"
        );

        let on = read_parquet(&pq, true);
        let idx = on.schema().index_of("uniq").expect("uniq present");
        assert!(
            matches!(on.column(idx).data_type(), DataType::Utf8),
            "a column the file does not dictionary-encode stays Utf8"
        );

        let mut m = base_mapping();
        m.attributes = vec![attr("uniq", "uniq", ColType::Str)];
        let col = build_columnar_or_panic(&on, &m);
        assert_eq!(col.num_rows, ROWS, "every row is built");
        let pos = col
            .dyn_columns
            .iter()
            .position(|c| c.name == "uniq")
            .expect("uniq column");
        assert!(
            col_dict(&col, pos).is_none(),
            "no StrColumnDict is built for a plain column"
        );
    }

    /// #660: the rule is scoped to string columns. `ArrowWriter`
    /// dictionary-encodes a low-cardinality `Int64` column too, and that column
    /// must keep the type the reader infers.
    #[test]
    fn dictionary_encoded_non_string_column_keeps_its_type() {
        const ROWS: usize = 1_000;
        const DISTINCT: i64 = 4;

        let ts: Vec<i64> = (0..ROWS as i64).map(|i| NOW_NS + i).collect();
        let nums: Vec<i64> = (0..ROWS as i64).map(|i| i % DISTINCT).collect();
        let cat: Vec<&str> = (0..ROWS).map(|i| ["a", "b"][i % 2]).collect();
        let b = batch(vec![
            ("ts", i64_col(ts)),
            ("num", i64_col(nums)),
            ("cat", str_col(cat)),
        ]);
        let (_dir, pq) = write_parquet(&b);

        // The premise: the Int64 column really is dictionary encoded in the file.
        let encodings = data_page_encodings(&pq, "num");
        assert_eq!(encodings.len(), 1, "one row group");
        assert!(
            !encodings[0].is_empty() && encodings[0].iter().copied().all(is_dictionary_encoding),
            "the writer dictionary-encoded every data page of `num`: {:?}",
            encodings[0]
        );

        // A schema IS supplied (the string column qualifies), so this proves the
        // rule skipped `num` rather than that it never ran.
        let schema =
            reader_schema_for(&pq).expect("the string column qualifies, so a schema is supplied");
        assert_eq!(
            schema
                .field_with_name("num")
                .expect("num field")
                .data_type(),
            &DataType::Int64,
            "a dictionary-encoded non-string column keeps its inferred type"
        );
        assert_eq!(
            schema
                .field_with_name("cat")
                .expect("cat field")
                .data_type(),
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            "the string column beside it is retyped"
        );
        assert_eq!(
            schema.field_with_name("ts").expect("ts field").data_type(),
            &DataType::Int64,
            "the ts column keeps its inferred type"
        );

        let on = read_parquet(&pq, true);
        assert_eq!(
            on.column(on.schema().index_of("num").expect("num present"))
                .data_type(),
            &DataType::Int64,
            "the Int64 column is read back as Int64"
        );
        assert_eq!(on.num_rows(), ROWS, "every row is read back");
    }

    /// #708: a dictionary-encoded string column whose values array is empty (an
    /// all-null dictionary chunk a Parquet writer may emit) must resolve to an
    /// all-null column. Before the fix, `str_src` called
    /// `DictionaryArray::normalized_keys`, which in arrow-array 59.1 asserts the
    /// values array is non-empty and panicked here instead.
    #[test]
    fn empty_dictionary_str_column_is_all_null() {
        let keys = Int32Array::from(vec![None, None, None]);
        let values = Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef;
        let dict = DictionaryArray::<Int32Type>::new(keys, values);
        let arr: ArrayRef = Arc::new(dict);

        let src = str_src(&arr);
        assert!(
            matches!(src, StrSrc::AllNull),
            "empty-dictionary string column takes the all-null path"
        );
        assert!(!src.is_dict(), "an all-null column is not a dictionary");
        for row in 0..arr.len() {
            assert_eq!(
                src.get(row).expect("no error"),
                None,
                "every row of an empty-dictionary column is null"
            );
        }
    }

    /// #708, binary analogue: an empty-dictionary binary column resolves to an
    /// all-null column rather than panicking in `normalized_keys`.
    #[test]
    fn empty_dictionary_bytes_column_is_all_null() {
        let keys = Int32Array::from(vec![None, None]);
        let values = Arc::new(BinaryArray::from(Vec::<&[u8]>::new())) as ArrayRef;
        let dict = DictionaryArray::<Int32Type>::new(keys, values);
        let arr: ArrayRef = Arc::new(dict);

        let src = bytes_src(&arr);
        assert!(
            matches!(src, BytesSrc::AllNull),
            "empty-dictionary binary column takes the all-null path"
        );
        assert!(!src.is_dict(), "an all-null column is not a dictionary");
        for row in 0..arr.len() {
            assert_eq!(
                src.get(row).expect("no error"),
                None,
                "every row of an empty-dictionary binary column is null"
            );
        }
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
            DEFAULT_MAX_INFLIGHT_FLUSHES,
            DEFAULT_DECODE_QUEUE_BATCHES,
            DEFAULT_TARGET_BYTES,
            None,
            now_ns,
            clock,
            LoadPath::Row,
            None,
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

    /// `--max-inflight-flushes` is load-bearing, not accepted-and-ignored, and
    /// it is a genuinely different window from `--pipeline-depth`: it bounds the
    /// flushes ONE SHARD may run at once, where `--pipeline-depth` bounds the
    /// writes the loader keeps outstanding across all shards. Every batch here
    /// lands on the same shard (`--shards 1`), so the shard's flush semaphore is
    /// the only thing that can serialize them, and `--pipeline-depth 4` is held
    /// above every flush setting under test so the loader's own window is never
    /// the binding constraint.
    ///
    /// Four rows, one row per batch, each data-object PUT held 50ms while the
    /// store tracks the running maximum of concurrently outstanding data-object
    /// PUTs. Both arms assert an exact figure, not a floor: the semaphore is a
    /// hard ceiling (a shard may not exceed its permit count) and, with four
    /// batches queued behind a four-deep loader window, it is also reached.
    ///
    /// Non-vacuity (prove-the-test): confirmed failing against the pre-change
    /// code by deleting the `max_inflight_flushes,` field from the
    /// `IngestConfig` literal in `load_instrumented`, which is exactly the state
    /// before this ticket -- the flag parsed and threaded but never reaching the
    /// router. The window then falls back to `IngestConfig::default()`'s 1 and
    /// the `flushes = 3` arm observes a high-water mark of 1 instead of 3. The
    /// `flushes = 1` arm is the control: it reads 1 either way, so on its own it
    /// proves nothing, which is why the higher setting is asserted exactly.
    #[tokio::test]
    async fn max_inflight_flushes_bounds_concurrent_flushes_per_shard() {
        use parquet::arrow::ArrowWriter;
        use ravel_object_store::memory::MemoryStore;
        use std::sync::atomic::{AtomicUsize, Ordering};

        /// Rows, and therefore single-row batches, in the fixture. One more
        /// than the highest flush window under test, so the window (not the
        /// supply of batches) is what the high-water mark measures.
        const BATCHES: usize = 4;
        /// Held above every flush setting under test: the loader's own window
        /// must never be the binding constraint on this fixture.
        const PIPELINE_DEPTH: usize = 4;

        async fn max_concurrent_at_flush_window(flushes: u32) -> usize {
            // One shard, so every batch's flush contends for the same shard
            // actor's semaphore. Distinct hosts keep the objects distinct
            // without changing routing.
            let shards = 1u32;
            let hosts: Vec<String> = (0..BATCHES).map(|i| format!("h{i}")).collect();

            let dir = tempfile::tempdir().expect("tempdir");
            let pq = dir.path().join("flush_window.parquet");
            let cols: Vec<(String, ArrayRef)> = vec![
                ("ts".to_string(), i64_col(vec![NOW_NS; BATCHES])),
                ("svc".to_string(), str_col(vec!["api"; BATCHES])),
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

            let report = load_instrumented(
                store as Arc<dyn ObjectStoreBackend>,
                &pq,
                "acme",
                &m,
                shards,
                1,
                None,
                PIPELINE_DEPTH,
                flushes,
                DEFAULT_DECODE_QUEUE_BATCHES,
                DEFAULT_TARGET_BYTES,
                None,
                NOW_NS,
                Arc::new(FixedClock(NOW_NS)),
                LoadPath::Columnar,
                None,
                None,
            )
            .await
            .expect("the load succeeds");
            assert_eq!(
                report.rows_processed, BATCHES as u64,
                "every row is written"
            );
            max_in_flight.load(Ordering::SeqCst)
        }

        let max_w1 = max_concurrent_at_flush_window(1).await;
        assert_eq!(
            max_w1, 1,
            "at --max-inflight-flushes 1 the shard runs exactly one flush at a time even with \
             --pipeline-depth {PIPELINE_DEPTH} handing it {BATCHES} writes, got {max_w1}"
        );

        let max_w3 = max_concurrent_at_flush_window(3).await;
        assert_eq!(
            max_w3, 3,
            "at --max-inflight-flushes 3 the shard runs exactly three flushes at once: the \
             semaphore is the ceiling and {BATCHES} queued batches reach it, got {max_w3}"
        );
    }

    /// The loader-side counterpart of `ravel-ingest`'s
    /// `neither_write_window_alone_moves_the_wall_and_the_counters_say_which`,
    /// measured end to end through `load_instrumented` with a real per-PUT
    /// latency injected (issue #800). ADR-0807 measured `4`/`4` against `1`/`1`
    /// on the ClickBench corpus but never isolated the two windows from each
    /// other, so the 2.94x it reports is not apportioned. This runs the full
    /// 2x2 on one fixture.
    ///
    /// Sixteen single-row batches, one shard, and a 40ms delay on every
    /// data-object PUT, so the whole load is round-trip bound by construction
    /// and the arms differ only in the two windows.
    ///
    /// Pre-registered before running (the arithmetic, not a post-hoc fit):
    /// `16 * 40ms = 640ms` for every arm that leaves either window at 1, and
    /// `4 * 40ms = 160ms` for the arm that raises both, a 4x ratio. Peak
    /// concurrent data-object PUTs: exactly 1, 1, 1, and 4.
    ///
    /// Asserted: the peak concurrency exactly, which is a count and cannot
    /// drift with machine load; and, on the wall, only the two conclusions the
    /// counts alone cannot give -- that raising both windows is at least 2x
    /// (against 4x predicted, so a loaded box has 2x of headroom before this
    /// misfires) and that neither window alone gets within 30% of that. Both
    /// wall bounds are ratios against this run's own `1`/`1` arm, so a uniformly
    /// slow machine moves numerator and denominator together.
    ///
    /// Non-vacuity (prove-the-test): the shipped-defaults arm's peak of 4 fails
    /// against the pre-change defaults. Confirmed by setting
    /// `DEFAULT_PIPELINE_DEPTH` back to 1: that arm reads `left: 1, right: 4`
    /// and its wall goes to 665.99ms, indistinguishable from the fully serial
    /// arm's 673.05ms in the same run. The `4`/`1` and `1`/`4` arms are what make
    /// the claim "both windows, not either" falsifiable: a change that only
    /// raised one would still pass a bare `1`/`1`-against-`4`/`4` comparison.
    #[tokio::test]
    async fn both_write_windows_are_needed_to_overlap_put_round_trips() {
        use parquet::arrow::ArrowWriter;
        use ravel_object_store::memory::MemoryStore;
        use std::sync::atomic::{AtomicUsize, Ordering};

        /// Single-row batches, so each is one flush on the single shard.
        const BATCHES: usize = 16;
        /// Injected cost of one data-object PUT.
        const PUT_DELAY: Duration = Duration::from_millis(40);

        /// One arm's outcome: the wall, the peak concurrent object PUTs, and the
        /// submit loop's own wall split into the only two things it can block on.
        struct Arm {
            wall: Duration,
            peak: usize,
            write_wait: Duration,
            decode_wait: Duration,
        }

        async fn arm(depth: usize, flushes: u32) -> Arm {
            let shards = 1u32;
            let hosts: Vec<String> = (0..BATCHES).map(|i| format!("h{i}")).collect();

            let dir = tempfile::tempdir().expect("tempdir");
            let pq = dir.path().join("windows.parquet");
            let cols: Vec<(String, ArrayRef)> = vec![
                ("ts".to_string(), i64_col(vec![NOW_NS; BATCHES])),
                ("svc".to_string(), str_col(vec!["api"; BATCHES])),
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
                delays: vec![("/l0/", PUT_DELAY)],
                in_flight: Arc::new(AtomicUsize::new(0)),
                max_in_flight: Arc::clone(&max_in_flight),
                watch_prefixes: Vec::new(),
                watch_started: Arc::new(AtomicUsize::new(0)),
                watch_completed: Arc::new(AtomicUsize::new(0)),
            });

            let started = Instant::now();
            let report = load_instrumented(
                store as Arc<dyn ObjectStoreBackend>,
                &pq,
                "acme",
                &m,
                shards,
                1,
                None,
                depth,
                flushes,
                DEFAULT_DECODE_QUEUE_BATCHES,
                DEFAULT_TARGET_BYTES,
                None,
                NOW_NS,
                Arc::new(FixedClock(NOW_NS)),
                LoadPath::Columnar,
                None,
                None,
            )
            .await
            .expect("the load succeeds");
            let wall = started.elapsed();
            assert_eq!(
                report.rows_processed, BATCHES as u64,
                "every row is written at depth {depth} / flushes {flushes}"
            );
            assert_eq!(
                report.objects_written(),
                BATCHES,
                "object layout is identical across arms: {BATCHES} objects however \
                 the windows are set, so the peak-concurrency comparison is not \
                 confounded by a different number of PUTs"
            );
            // The loop can only block on receiving a decoded batch or on
            // resolving a write, so these two partition its wall.
            //
            // hygiene-allow: wall-clock -- the gap here is manufactured by the
            // fixture's injected per-PUT delay, not by how fast the machine is:
            // 671.3 ms against 0.85 ms, about 790x. A slow or loaded runner
            // moves both sides together and cannot invert it. There is no
            // deterministic restatement of this claim, and the claim is the
            // whole point of the fixture: the submit loop blocks on writes, not
            // on the decoder.
            assert!(
                report.write_wait > report.decode_wait,
                "this fixture is round-trip bound by construction, so the submit \
                 loop must spend more time resolving writes ({:?}) than waiting \
                 for decoded batches ({:?}) at depth {depth} / flushes {flushes}",
                report.write_wait,
                report.decode_wait
            );
            Arm {
                wall,
                peak: max_in_flight.load(Ordering::SeqCst),
                write_wait: report.write_wait,
                decode_wait: report.decode_wait,
            }
        }

        let a_1_1 = arm(1, 1).await;
        let a_4_1 = arm(4, 1).await;
        let a_1_4 = arm(1, 4).await;
        let a_4_4 = arm(4, 4).await;
        let peak_1_1 = a_1_1.peak;
        let peak_4_1 = a_4_1.peak;
        let peak_1_4 = a_1_4.peak;
        let peak_4_4 = a_4_4.peak;

        println!("write-window 2x2 ({BATCHES} batches, {PUT_DELAY:?} per data PUT, 1 shard):");
        for (label, a) in [
            ("depth 1 / flushes 1", &a_1_1),
            ("depth 4 / flushes 1", &a_4_1),
            ("depth 1 / flushes 4", &a_1_4),
            ("depth 4 / flushes 4", &a_4_4),
        ] {
            println!(
                "  {label}: wall {:?} peak {} (submit loop: write_wait {:?}, decode_wait {:?})",
                a.wall, a.peak, a.write_wait, a.decode_wait
            );
        }

        assert_eq!(
            peak_1_1, 1,
            "at depth 1 / flushes 1 exactly one object PUT is ever outstanding"
        );
        assert_eq!(
            peak_4_1, 1,
            "raising only the loader's window still leaves the shard's flush \
             semaphore at one permit, so exactly one PUT is outstanding"
        );
        assert_eq!(
            peak_1_4, 1,
            "raising only the shard's flush window leaves the loader awaiting each \
             batch before submitting the next, so the extra permits are never asked \
             for and exactly one PUT is outstanding"
        );
        assert_eq!(
            peak_4_4, 4,
            "both windows raised: exactly four object PUTs overlap, the loader's \
             four outstanding batches each holding one of the shard's four permits"
        );

        // The walls are printed above, not asserted on. The four peak
        // assertions already carry this test's whole claim: 1, 1, 1 and 4
        // outstanding PUTs is the concurrency the windows are supposed to
        // produce, stated exactly and observed directly. A wall-clock band on
        // top of that adds no proof and does add a failure mode, since the
        // ratio between two elapsed times on a shared runner is not a property
        // of the code. The end-to-end wall figure that justifies the default
        // lives in ADR-0807, measured on a real corpus.

        // The shipped defaults, run through the same fixture: what an operator
        // gets with neither flag given must be the overlapped arm, not the
        // serial one. This is the assertion the default change is accountable
        // to; against the pre-change defaults of 1 and 1 it reads a peak of 1.
        let a_default = arm(DEFAULT_PIPELINE_DEPTH, DEFAULT_MAX_INFLIGHT_FLUSHES).await;
        let (wall_default, peak_default) = (a_default.wall, a_default.peak);
        println!(
            "  shipped defaults:    wall {wall_default:?} peak {peak_default} \
             (submit loop: write_wait {:?}, decode_wait {:?})",
            a_default.write_wait, a_default.decode_wait
        );
        assert_eq!(
            peak_default, 4,
            "the shipped defaults must overlap four object PUTs; a peak of 1 means \
             the loader ships serial"
        );
    }

    /// Durable-token correctness under a partial-window failure: the reported
    /// list equals what actually committed, at `--pipeline-depth` above 1
    /// (issue #800). With depth 4 and a stream of five single-shard batches, the
    /// data PUT for the *middle* batch (index 2, shard 2) is held ~300ms before
    /// failing permanently. The batches strictly after it (indices 3 and 4,
    /// shards 3 and 4) have no artificial delay at all, so their shard actors
    /// race far ahead of shard 2's held PUT and commit their objects
    /// *independently*, well before shard 2's failure is even detected. The
    /// loader cannot prevent that: it routes each batch to its own shard actor,
    /// and a shard actor's flush is downstream of the write future the loader
    /// holds, so aborting that future's `JoinHandle` does not stop an actor
    /// already mid-PUT (see `LogIngestRouter::write` in
    /// `crates/ravel-ingest/src/log_router.rs`: the actor has no join handle of
    /// its own, only a channel).
    ///
    /// Since it cannot stop them, it waits for them ([`harvest_after_failure`]).
    /// Asserted: the reported durable list is exactly shards `[0, 1, 3, 4]`, in
    /// submission order -- the two batches before the failure, then the two
    /// after it that committed anyway -- and carries no token for the failing
    /// batch itself (a full single-shard PUT failure has no partial survivor).
    /// `watch_completed` reading 2 is the direct, non-inferred proof that
    /// batches 3 and 4's objects genuinely landed in the underlying store, so
    /// the list equality is a claim about what happened, not about what the
    /// loader chose to look at.
    ///
    /// Non-vacuity (prove-the-test): this exact assertion fails against the
    /// pre-change code, which aborted the post-failure handles
    /// (`for (_, handle) in inflight.drain(..) { handle.abort(); }` at the two
    /// error sites). Confirmed by running it against that code: it panicked with
    /// `durable.len() == 2`, the two pre-failure shards only, while
    /// `watch_completed` still read 2 -- the gap this closes, stated as its own
    /// failure. The ordering is deterministic because a zero-delay in-memory PUT
    /// completes in microseconds while shard 2 is held 300ms, a 6000x margin
    /// that no scheduling jitter inverts; and the harvest pops the window
    /// front-to-back, so 3 precedes 4. A resolver that recorded whichever write
    /// finished first would report the post-failure shards ahead of the
    /// pre-failure ones and fail the exact-sequence assertion.
    #[tokio::test]
    async fn partial_window_failure_reports_every_batch_that_committed() {
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
        // not merely "present in the returned list" (which a wrong
        // implementation could also produce, by reporting a token for a batch
        // that never committed).
        let durable = match &err {
            LoadError::Flush { durable, .. } => durable.clone(),
            other => panic!("expected LoadError::Flush, got {other:?}"),
        };
        let shard_sequence: Vec<u32> = durable.iter().map(|t| t.shard).collect();
        assert_eq!(
            shard_sequence,
            vec![0, 1, 3, 4],
            "the durable list is exactly the batches that committed, in submission order: the two \
             before the failure, then the two after it whose independent writes landed anyway. It \
             carries no token for the failing batch (shard 2), which had no partial survivor. Got \
             {durable:?}"
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
            "batches 3 and 4's writes did in fact succeed and commit. The loader cannot stop a \
             shard actor already mid-PUT, so it waits for the outcome instead of abandoning it, \
             and both appear in the durable list above. That is what makes the report equal to \
             what landed at any --pipeline-depth: a resume from it re-ingests neither rows that \
             committed nor rows that did not."
        );
    }

    // ---- #689: the dynamic-column slot table, against the map build ----

    /// [`build_columnar_batch`] as it stood before #689, copied verbatim: every
    /// cell resolves its destination column through a
    /// `BTreeMap<(String, u8), _>` entry lookup keyed by a freshly cloned
    /// attribute name, and a per-row `HashSet` decides the first-occurrence
    /// winner. This is the differential oracle for the slot-table build. The two
    /// must agree on every field of the batch: the RLOG object the columnar
    /// writer produces is byte-identical only for identical batches, and the
    /// RSEG layout is a frozen contract.
    fn build_columnar_batch_reference(
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

        // Dynamic columns, keyed by (name, type byte) as `from_records` keys
        // them, so their materialized order matches. `col_dict` tracks whether
        // every winning cell of a column came from a dictionary-encoded Arrow
        // source.
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

            // Prepare every reader once per span (downcast resolved here, not
            // per cell).
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

                // 1. ts (required) and 2. future-skew bound, in build_record
                // order.
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
                        "timestamp is {skew_ns} ns ahead of load time, more than the max future \
                         skew of {} ns",
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

                // 4. severity number (out-of-u8 normalizes to 0) and severity
                // text.
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

                // 6. resource attributes (stream identity), checked in mapping
                // order.
                let mut resource_attrs: Vec<(String, AttrValue)> =
                    Vec::with_capacity(resource.len());
                for (mi, src) in &resource {
                    let spec = &mapping.resource_attributes[*mi];
                    if let Some(v) = src.get(local).map_err(row_err)? {
                        check_attr(&spec.key, &v, limits).map_err(row_err)?;
                        resource_attrs.push((spec.key.clone(), v));
                    }
                }

                // 7. record attributes: check, count for the per-record cap, and
                // split first-occurrence winner vs within-record residual
                // exactly as `from_records`.
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
                        "record has {present_record} attributes, more than the loader per-record \
                         cap of {LOADER_MAX_ATTRIBUTES_PER_RECORD}"
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

        // Materialize dynamic columns in (name, type) order; attach a
        // StrColumnDict to a Str/Bytes column whose every winning cell came from
        // a dictionary source. If no column carries a dictionary, leave
        // `dyn_col_dicts` empty (its default), so a plain load is byte-identical
        // to `from_records` without `with_dictionaries`.
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

    /// A 64-bit mix, so a generated case carries a seed instead of megabytes of
    /// literal cell data: every cell is derived from (seed, column, row).
    fn mix(seed: u64, col: usize, row: usize) -> u64 {
        let mut h = seed ^ 0x9e37_79b9_7f4a_7c15;
        h = h
            .wrapping_add((col as u64).wrapping_mul(0xff51_afd7_ed55_8ccd))
            .rotate_left(31);
        h = h
            .wrapping_add((row as u64).wrapping_mul(0xc4ce_b9fe_1a85_ec53))
            .rotate_left(27);
        h ^= h >> 33;
        h = h.wrapping_mul(0xbf58_476d_1ce4_e5b9);
        h ^ (h >> 29)
    }

    /// One generated attribute column: its source Parquet column, the record key
    /// it maps to, its declared type, and whether the Arrow array arrives
    /// dictionary-encoded (which drives the `StrColumnDict` decision).
    #[derive(Debug, Clone)]
    struct GenCol {
        column: String,
        key: String,
        ty: ColType,
        dict: bool,
    }

    /// Derive `n` attribute columns from a seed. Keys are drawn from a pool of
    /// `key_span` names, so distinct source columns collide on one
    /// `(name, type)` slot (exercising the within-row residual path) and one
    /// name splits across types (two slots). "k10" sorting before "k2" keeps the
    /// slot order non-numeric, the same order the map produced.
    fn gen_cols(n: usize, seed: u64, key_span: usize) -> Vec<GenCol> {
        (0..n)
            .map(|i| {
                let h = mix(seed, i, 0);
                let ty = match h % 5 {
                    0 => ColType::Str,
                    1 => ColType::I64,
                    2 => ColType::F64,
                    3 => ColType::Bool,
                    _ => ColType::Bytes,
                };
                GenCol {
                    column: format!("c{i}"),
                    key: format!("k{}", (h >> 8) as usize % key_span.max(1)),
                    ty,
                    dict: matches!(ty, ColType::Str) && (h >> 20).is_multiple_of(3),
                }
            })
            .collect()
    }

    /// Build one span's Arrow array for `col`, covering rows `start..start+len`
    /// of the logical batch. A cell is null when its mix falls under
    /// `null_pct`.
    fn gen_array(
        col: &GenCol,
        ci: usize,
        seed: u64,
        start: usize,
        len: usize,
        null_pct: u8,
    ) -> ArrayRef {
        let present = |row: usize| mix(seed, ci, row) % 100 >= u64::from(null_pct);
        let cell = |row: usize| mix(seed, ci.wrapping_add(7), row.wrapping_add(1));
        let text = |row: usize| format!("v{}", cell(row) % 997);
        match col.ty {
            ColType::I64 => Arc::new(Int64Array::from(
                (0..len)
                    .map(|k| present(start + k).then(|| cell(start + k) as i64))
                    .collect::<Vec<Option<i64>>>(),
            )),
            // No NaN and no -0.0: the batch comparison is a value comparison, and
            // those two are exactly the payloads it could not decide.
            ColType::F64 => Arc::new(Float64Array::from(
                (0..len)
                    .map(|k| present(start + k).then(|| (cell(start + k) % 1_000_000) as f64 / 8.0))
                    .collect::<Vec<Option<f64>>>(),
            )),
            ColType::Bool => Arc::new(BooleanArray::from(
                (0..len)
                    .map(|k| present(start + k).then(|| cell(start + k).is_multiple_of(2)))
                    .collect::<Vec<Option<bool>>>(),
            )),
            ColType::Str => {
                let vals: Vec<Option<String>> = (0..len)
                    .map(|k| present(start + k).then(|| text(start + k)))
                    .collect();
                // A span with no present value gets the plain encoding: a
                // dictionary array with zero distinct values makes arrow's
                // `normalized_keys` panic, so it is not a shape `str_src` can be
                // handed here (see the report on #689).
                if col.dict && vals.iter().any(Option::is_some) {
                    let arr: DictionaryArray<Int32Type> =
                        vals.iter().map(|v| v.as_deref()).collect();
                    Arc::new(arr)
                } else {
                    Arc::new(StringArray::from(vals))
                }
            }
            ColType::Bytes => Arc::new(
                (0..len)
                    .map(|k| present(start + k).then(|| text(start + k).into_bytes()))
                    .collect::<BinaryArray>(),
            ),
        }
    }

    /// Assemble `n_spans` record batches over `rows` logical rows, plus the
    /// mapping that reads them: a non-null `ts`, a low-cardinality resource
    /// column so the stream directory holds several streams, and one column per
    /// [`GenCol`].
    fn gen_spans_and_mapping(
        rows: usize,
        n_spans: usize,
        cols: &[GenCol],
        seed: u64,
        null_pct: u8,
    ) -> (Vec<(RecordBatch, u64)>, Mapping) {
        let mut spans = Vec::with_capacity(n_spans);
        let base = rows / n_spans.max(1);
        let extra = rows % n_spans.max(1);
        let mut start = 0usize;
        for s in 0..n_spans.max(1) {
            let len = base + usize::from(s < extra);
            if len == 0 {
                continue;
            }
            let mut arrays: Vec<(String, ArrayRef)> = Vec::with_capacity(cols.len() + 2);
            arrays.push((
                "ts".to_string(),
                Arc::new(Int64Array::from(
                    (0..len)
                        .map(|k| NOW_NS - ((start + k) as i64 % 1_000_000) * 1_000)
                        .collect::<Vec<i64>>(),
                )) as ArrayRef,
            ));
            arrays.push((
                "res".to_string(),
                Arc::new(StringArray::from_iter_values(
                    (0..len).map(|k| format!("svc{}", mix(seed, 4_242, start + k) % 4)),
                )) as ArrayRef,
            ));
            for (ci, c) in cols.iter().enumerate() {
                arrays.push((
                    c.column.clone(),
                    gen_array(c, ci, seed, start, len, null_pct),
                ));
            }
            spans.push((
                RecordBatch::try_from_iter(arrays).expect("record batch"),
                start as u64,
            ));
            start += len;
        }
        let mut mapping = base_mapping();
        mapping.resource_attributes = vec![AttrMap {
            key: "service.name".to_string(),
            column: "res".to_string(),
            value_type: ColType::Str,
        }];
        mapping.attributes = cols
            .iter()
            .map(|c| AttrMap {
                key: c.key.clone(),
                column: c.column.clone(),
                value_type: c.ty,
            })
            .collect();
        (spans, mapping)
    }

    /// Assert the slot-table build and the pre-#689 map build produce the same
    /// batch for one generated case.
    fn assert_same_batch(
        rows: usize,
        n_spans: usize,
        n_cols: usize,
        key_span: usize,
        null_pct: u8,
        seed: u64,
    ) {
        let cols = gen_cols(n_cols, seed, key_span);
        let (spans, mapping) = gen_spans_and_mapping(rows, n_spans, &cols, seed, null_pct);
        let limits = LogIngestLimits::default();
        let got = match build_columnar_batch(&spans, &mapping, &limits, NOW_NS) {
            Ok(b) => b,
            Err(ColBuildError::Batch(r)) => panic!("slot-table build failed the batch: {r}"),
            Err(ColBuildError::Row { row, reason }) => {
                panic!("slot-table build rejected row {row}: {reason}")
            }
        };
        let want = match build_columnar_batch_reference(&spans, &mapping, &limits, NOW_NS) {
            Ok(b) => b,
            Err(ColBuildError::Batch(r)) => panic!("reference build failed the batch: {r}"),
            Err(ColBuildError::Row { row, reason }) => {
                panic!("reference build rejected row {row}: {reason}")
            }
        };

        assert_eq!(
            got.dyn_columns.len(),
            want.dyn_columns.len(),
            "dynamic column count"
        );
        let got_keys: Vec<(&str, FieldType)> = got
            .dyn_columns
            .iter()
            .map(|c| (c.name.as_str(), c.field_type))
            .collect();
        let want_keys: Vec<(&str, FieldType)> = want
            .dyn_columns
            .iter()
            .map(|c| (c.name.as_str(), c.field_type))
            .collect();
        assert_eq!(
            got_keys, want_keys,
            "dynamic column (name, field_type) sequence, in order"
        );
        for (g, w) in got.dyn_columns.iter().zip(&want.dyn_columns) {
            assert_eq!(g.cells, w.cells, "cells of column {:?}", g.name);
            assert_eq!(
                g.validity.len(),
                w.validity.len(),
                "validity length of column {:?}",
                g.name
            );
            assert_eq!(
                g.validity.bytes(),
                w.validity.bytes(),
                "validity of column {:?}",
                g.name
            );
        }
        assert_eq!(got.dyn_col_dicts, want.dyn_col_dicts, "dictionary columns");
        assert_eq!(
            got.residual_attrs, want.residual_attrs,
            "within-row residual attributes"
        );
        assert_eq!(got, want, "the whole batch");
    }

    /// A fixed 48-column case (mixed types, dictionary and plain strings, key
    /// collisions, 20% nulls) that runs on every test run, independent of the
    /// proptest budget below.
    #[test]
    fn slot_table_build_matches_map_build_48_columns() {
        assert_same_batch(1_000, 3, 48, 20, 20, 0x5EED_0000_0000_0001);
    }

    /// Every attribute column null across the whole batch: the map held no entry
    /// for such a column, so the slot table must materialize none either.
    #[test]
    fn slot_table_build_drops_all_null_columns() {
        assert_same_batch(64, 1, 48, 20, 100, 0x5EED_0000_0000_0002);
        let cols = gen_cols(48, 0x5EED_0000_0000_0002, 20);
        let (spans, mapping) = gen_spans_and_mapping(64, 1, &cols, 0x5EED_0000_0000_0002, 100);
        let batch =
            match build_columnar_batch(&spans, &mapping, &LogIngestLimits::default(), NOW_NS) {
                Ok(b) => b,
                Err(_) => panic!("all-null attribute columns are not a rejection"),
            };
        assert!(
            batch.dyn_columns.is_empty(),
            "an all-null mapped column materializes no dynamic column"
        );
    }

    proptest! {
        // 24 cases, not the default 256: a case at the top of the range
        // materializes 4096 x 120 cells twice, once per implementation, so the
        // default turns this into a multi-minute test without covering anything
        // the slot table can get wrong that 24 cases do not reach.
        #![proptest_config(ProptestConfig::with_cases(24))]

        /// The slot-table build and the pre-#689 map build agree on every field
        /// of the produced batch, across row counts, column counts, key
        /// collisions, type mixes, null densities and span splits.
        #[test]
        fn slot_table_build_matches_map_build(
            rows in 1usize..=4096,
            n_cols in 1usize..=120,
            key_span in 1usize..=120,
            null_pct in 0u8..=100,
            n_spans in 1usize..=3,
            seed in any::<u64>(),
        ) {
            assert_same_batch(rows, n_spans, n_cols, key_span, null_pct, seed);
        }
    }

    /// A timing report, never an assertion: with `RAVEL_LOAD_BATCH_TIMING=1`,
    /// time both builds on a 65,536-row x 105-column batch (ClickBench `hits`
    /// width) and print the two wall times. Skipped otherwise, so a normal test
    /// run pays nothing for it.
    #[test]
    fn build_columnar_batch_timing_report() {
        if std::env::var("RAVEL_LOAD_BATCH_TIMING").ok().as_deref() != Some("1") {
            return;
        }
        const ROWS: usize = 65_536;
        const COLS: usize = 105;
        const SEED: u64 = 0xC0FF_EE00_1234_5678;

        let cols = gen_cols(COLS, SEED, COLS);
        let (spans, mapping) = gen_spans_and_mapping(ROWS, 1, &cols, SEED, 10);
        let limits = LogIngestLimits::default();

        let t0 = Instant::now();
        let want = build_columnar_batch_reference(&spans, &mapping, &limits, NOW_NS);
        let map_elapsed = t0.elapsed();
        let t1 = Instant::now();
        let got = build_columnar_batch(&spans, &mapping, &limits, NOW_NS);
        let slot_elapsed = t1.elapsed();

        assert!(want.is_ok(), "the reference build succeeds");
        assert!(got.is_ok(), "the slot-table build succeeds");
        println!(
            "build_columnar_batch over {ROWS} rows x {COLS} columns: map build {map_elapsed:?}, \
             slot-table build {slot_elapsed:?}"
        );
    }
}
