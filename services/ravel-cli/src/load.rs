//! `ravel-cli load --parquet` (ADR-0089, widened by ADR-1751): bulk-import a
//! Parquet file into the signal named by `--signal`, through the existing
//! [`ravel_ingest::LogIngestRouter`] (logs), [`ravel_ingest::IngestRouter`]
//! (metrics) or [`ravel_ingest::SpanIngestRouter`] (spans).
//!
//! This is a new *caller* of existing public APIs, not a new ingest path. The
//! loader constructs its own router in-process against the target tenant's
//! object store and provisioned shard count, reuses
//! [`ravel_otlp::NormalizedLogRecord`] / [`ravel_otlp::NormalizedPoint`] /
//! [`ravel_otlp::NormalizedSpan`] as the record shape, re-implements the
//! `ravel-otlp` admission checks the ADR says to keep (future skew, length
//! caps), relaxes the ones it says to relax (past-event-time lag, per-record
//! attribute cap), and writes with [`WriteMode::Strict`] so every returned
//! success has no buffered-but-unflushed data.
//!
//! Which `ravel-otlp` rules this path keeps, relaxes, or bypasses, and why, is
//! documented in `docs/guides/ingest.md` ("Bulk import") per ADR-0089;
//! ADR-1751 decision 1 states that the same table applies per signal, with
//! that signal's own OTLP limits.

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
use arrow::array::{BinaryArray, FixedSizeBinaryArray, new_null_array};
use arrow::compute::take;
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use ravel_catalog::{AbsentPolicy, validate_or_adopt};
#[cfg(feature = "stage-timing")]
use ravel_ingest::LogStageSnapshot;
use ravel_ingest::{
    Clock, FlushTriggerMix, IngestConfig, IngestRouter, LogIngestMetricsSnapshot, LogIngestRouter,
    LogWriteError, LogWriteReceipt, STRICT_VISIBILITY_RESERVE_NS, SpanIngestRouter, SpanWriteError,
    SpanWriteReceipt, SystemClock, WriteError, WriteMode, WriteReceipt,
};
use ravel_logseg::{
    Bitmap, ColumnarLogBatch, DynColumn, FieldType, StrColumnDict, stream_attrs_bytes,
};
use ravel_object_store::ObjectStoreBackend;
use ravel_otlp::logs_limits::LogIngestLimits;
use ravel_otlp::normalize::{prometheus_family_name, sanitize_label_name, sanitize_metric_name};
use ravel_otlp::promcompat::format_float;
use ravel_otlp::traces_limits::SpanIngestLimits;
use ravel_otlp::traces_normalize::{is_reserved_key, status_code_from_i32};
use ravel_otlp::{IngestLimits, MetricKind, NormalizedLogRecord, NormalizedPoint, NormalizedSpan};
use ravel_rspan::{StatusCode, merge_attrs};
use ravel_types::logstream::{AttrValue, LogStreamId, log_stream_id};
use ravel_types::{
    CommitToken, Label, LabelSet, METRIC_NAME_LABEL, Sample, SeriesId, Signal, TenantId,
};
use serde::{Deserialize, Serialize};

use crate::maintain::SignalArg;

/// The label the classic-histogram explosion synthesizes per bucket, carrying
/// that bucket's upper bound (`ravel_otlp::normalize`'s `explode_histogram`).
const LE_LABEL: &str = "le";

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
/// windows compose as
/// `shards * min(pipeline_depth, max_inflight_flushes, max_queued_flushes)`
/// (ADR-0807, third term added by the ADR-1642 amendment and issue #1740: a
/// shard refuses an ordinary trigger once it holds `max_queued_flushes`
/// spawned and unreaped flush tasks, so it never spawns enough to use more
/// permits than that; a drain and a buffer over its memory backstop are exempt
/// and spawn past the cap, so the third term binds the steady state, not every
/// instant). The loader leaves that cap at the `IngestConfig` default of 8 and
/// does not carry `ravel-server`'s `Cli::resolve_flush_concurrency` raise, so
/// a `--max-inflight-flushes` or `--pipeline-depth` above 8 is silently
/// capped at 8 here rather than warned about; at their own defaults of 4 the
/// cap is above both and never binds. An inner window below the
/// outer one re-serialises each shard's
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
    pub(crate) fn factor(self) -> i64 {
        match self {
            TsUnit::Seconds => 1_000_000_000,
            TsUnit::Millis => 1_000_000,
            TsUnit::Micros => 1_000,
            TsUnit::Nanos => 1,
        }
    }

    /// The spelling a mapping writes this unit as, for a rejection that has to
    /// point the operator back at the line that declared it.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            TsUnit::Seconds => "seconds",
            TsUnit::Millis => "millis",
            TsUnit::Micros => "micros",
            TsUnit::Nanos => "nanos",
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
    /// Opt-in output column for `ravel-cli export` (ADR-1751 decision 4): every
    /// record attribute not named by `attributes` above is written into this
    /// one `Map<Utf8, Utf8>` column, stringified the same way `attrs['<key>']`
    /// stringifies a value for SQL. Write side only: `ravel-cli load` does not
    /// read this column back. A mapping that never sets it is unaffected.
    #[serde(default)]
    pub attrs_map_column: Option<String>,
}

/// Parse the logs section of a `--mapping` TOML document.
///
/// Accepts both spellings ADR-1751 decision 2 leaves valid for logs: a
/// document whose one signal section is `[logs]`, and the pre-ADR-1751
/// top-level form whose logs keys sit at the document root. See
/// [`parse_mapping_document`] for the section rules.
pub fn parse_mapping(text: &str) -> Result<Mapping, LoadError> {
    match parse_mapping_document(text, SignalArg::Logs)? {
        MappingSection::Logs(mapping) => Ok(mapping),
        // `parse_mapping_document` returns the section matching the signal it
        // was asked for, so these arms are not reachable through this call.
        other => Err(wrong_section_resolved("logs", other)),
    }
}

/// Parse the metrics section of a `--mapping` TOML document (ADR-1751
/// decision 2).
pub fn parse_metrics_mapping(text: &str) -> Result<MetricsMapping, LoadError> {
    match parse_mapping_document(text, SignalArg::Metrics)? {
        MappingSection::Metrics(mapping) => Ok(mapping),
        other => Err(wrong_section_resolved("metrics", other)),
    }
}

/// Parse the spans section of a `--mapping` TOML document (ADR-1751
/// decision 2).
pub fn parse_spans_mapping(text: &str) -> Result<SpansMapping, LoadError> {
    match parse_mapping_document(text, SignalArg::Spans)? {
        MappingSection::Spans(mapping) => Ok(mapping),
        other => Err(wrong_section_resolved("spans", other)),
    }
}

/// The internal error a per-signal parse helper returns if
/// [`parse_mapping_document`] ever handed it another signal's section. Not
/// reachable: that function returns the section matching the signal it was
/// asked for, or an error.
fn wrong_section_resolved(wanted: &str, got: MappingSection) -> LoadError {
    let got = match got {
        MappingSection::Logs(_) => "logs",
        MappingSection::Metrics(_) => "metrics",
        MappingSection::Spans(_) => "spans",
    };
    LoadError::Setup(format!(
        "internal error: the {wanted} mapping resolved to a {got} section"
    ))
}

/// The signal section names a `--mapping` document may carry (ADR-1751
/// decision 2). Any other top-level key is read as the pre-ADR-1751
/// top-level logs form.
const MAPPING_SECTION_NAMES: [&str; 3] = ["logs", "metrics", "spans"];

/// The section name a `--signal` value selects.
fn mapping_section_name(signal: SignalArg) -> &'static str {
    match signal {
        SignalArg::Metrics => "metrics",
        SignalArg::Logs => "logs",
        SignalArg::Spans => "spans",
    }
}

/// The resolved mapping section for one load's `--signal`.
#[derive(Debug, Clone)]
pub enum MappingSection {
    Logs(Mapping),
    Metrics(MetricsMapping),
    Spans(SpansMapping),
}

/// Resolve the one signal section of a `--mapping` document and deserialize
/// it (ADR-1751 decision 2).
///
/// Exactly one of `[logs]`, `[metrics]` and `[spans]` may be present, and it
/// must match `--signal`. Two exceptions carry the pre-ADR-1751 form forward:
///
/// - a document with no signal section at all, whose top-level keys are the
///   ADR-0089 logs keys, is read as the `[logs]` section (so every mapping
///   written before this change keeps loading, unchanged, under the default
///   `--signal logs`);
/// - that same document under any other `--signal` is refused by name rather
///   than by a serde "unknown field" error, since the operator's real mistake
///   is a missing section, not a typo.
///
/// Mixing the two spellings (a signal section *and* top-level logs keys) is
/// refused: which one wins would otherwise be an invisible precedence rule.
pub fn parse_mapping_document(text: &str, signal: SignalArg) -> Result<MappingSection, LoadError> {
    let mut doc: toml::Table = toml::from_str(text)
        .map_err(|e| LoadError::Setup(format!("invalid --mapping TOML: {e}")))?;

    let present: Vec<&'static str> = MAPPING_SECTION_NAMES
        .iter()
        .copied()
        .filter(|name| doc.contains_key(*name))
        .collect();
    let top_level: Vec<String> = doc
        .keys()
        .filter(|k| !MAPPING_SECTION_NAMES.contains(&k.as_str()))
        .cloned()
        .collect();
    let wanted = mapping_section_name(signal);

    if present.len() > 1 {
        return Err(LoadError::Setup(format!(
            "--mapping file declares {} signal sections ({}). Exactly one must be present, and it \
             must match --signal {wanted}.",
            present.len(),
            present.join(", ")
        )));
    }
    if !present.is_empty() && !top_level.is_empty() {
        return Err(LoadError::Setup(format!(
            "--mapping file mixes a [{}] signal section with top-level keys ({}). Move every \
             mapped field inside the section: with both spellings present there is no rule \
             saying which one a load would use.",
            present.join(""),
            top_level.join(", ")
        )));
    }

    if let Some(section) = present.first().copied() {
        if section != wanted {
            return Err(LoadError::Setup(format!(
                "--mapping file declares a [{section}] section but --signal is {wanted}. Exactly \
                 one section must be present and it must match --signal (ADR-1751 decision 2)."
            )));
        }
        let value = doc
            .remove(section)
            .unwrap_or(toml::Value::Table(toml::Table::new()));
        return deserialize_section(text, &value, Some(section), signal);
    }

    if signal != SignalArg::Logs {
        return Err(LoadError::Setup(format!(
            "--mapping file has no [{wanted}] section, which --signal {wanted} requires \
             (ADR-1751 decision 2). Its top-level keys are {}.",
            if top_level.is_empty() {
                "none (the file is empty)".to_string()
            } else {
                format!(
                    "{} (the pre-ADR-1751 logs-only form, read as the [logs] section)",
                    top_level.join(", ")
                )
            }
        )));
    }
    deserialize_section(text, &toml::Value::Table(doc), None, signal)
}

/// Deserialize one already-selected section table into its typed mapping.
///
/// `section` is the section name the keys were read from, or `None` for the
/// pre-ADR-1751 top-level logs form. The distinction is only in the error
/// prefix, and it is there so a mapping written before ADR-1751 still fails
/// with the message it has always failed with (`invalid --mapping TOML: ...`)
/// rather than with one naming a section its author never wrote.
///
/// The typed pass re-reads `text` rather than converting `value`: only a
/// deserializer over the source text carries spans, so this is what keeps the
/// line and column in a schema error. The caller has already checked that the
/// document holds this one section and nothing beside it.
fn deserialize_section(
    text: &str,
    value: &toml::Value,
    section: Option<&str>,
    signal: SignalArg,
) -> Result<MappingSection, LoadError> {
    #[derive(Deserialize)]
    struct LogsSection {
        logs: Mapping,
    }
    #[derive(Deserialize)]
    struct MetricsSection {
        metrics: MetricsMapping,
    }
    #[derive(Deserialize)]
    struct SpansSection {
        spans: SpansMapping,
    }

    let bad = |e: toml::de::Error| match section {
        Some(section) => LoadError::Setup(format!("invalid --mapping [{section}] section: {e}")),
        None => LoadError::Setup(format!("invalid --mapping TOML: {e}")),
    };
    match (signal, section) {
        (SignalArg::Logs, None) => toml::from_str::<Mapping>(text)
            .map(MappingSection::Logs)
            .map_err(bad),
        (SignalArg::Logs, Some(_)) => toml::from_str::<LogsSection>(text)
            .map(|doc| MappingSection::Logs(doc.logs))
            .map_err(bad),
        (SignalArg::Metrics, _) => {
            reject_native_histogram_keys(value)?;
            let mapping = toml::from_str::<MetricsSection>(text).map_err(bad)?.metrics;
            mapping.validate()?;
            Ok(MappingSection::Metrics(mapping))
        }
        (SignalArg::Spans, _) => {
            reject_unmappable_span_keys(value)?;
            let mapping = toml::from_str::<SpansSection>(text).map_err(bad)?.spans;
            mapping.validate()?;
            Ok(MappingSection::Spans(mapping))
        }
    }
}

/// Keys that name a native (exponential) histogram, refused by name before
/// `deny_unknown_fields` can report them as a generic typo (ADR-1751
/// decision 2: native histograms are not mappable in this version).
const NATIVE_HISTOGRAM_KEYS: [&str; 2] = ["native_histogram", "exponential_histogram"];

/// Refuse a metrics section that names a native/exponential histogram at the
/// section's top level.
fn reject_native_histogram_keys(value: &toml::Value) -> Result<(), LoadError> {
    let Some(table) = value.as_table() else {
        return Ok(());
    };
    for key in NATIVE_HISTOGRAM_KEYS {
        if table.contains_key(key) {
            return Err(native_histogram_rejected(key));
        }
    }
    Ok(())
}

/// The refusal a native/exponential histogram mapping gets (ADR-1751
/// decision 2).
fn native_histogram_rejected(what: &str) -> LoadError {
    LoadError::Setup(format!(
        "--mapping names a native (exponential) histogram ({what}), which this version does not \
         map (ADR-1751 decision 2: native histograms, span events and span links are not \
         mappable, and a mapping that names them is rejected). Only the classic-histogram shape \
         (le plus sum and count columns) is supported."
    ))
}

/// Whether a metric's exploded/scalar series behaves as a monotonic counter
/// (`kind = "counter"`) or a gauge. Absent means gauge, which is what
/// `ravel-otlp` reports for every non-`Sum` point.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricKindArg {
    Gauge,
    Counter,
}

/// One mapped label: the Prometheus label name it becomes and the source
/// Parquet column. Values are read as strings (an integer, float or boolean
/// column is stringified), since a Prometheus label value is a string.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LabelMap {
    /// The label name stored on the series (e.g. `job`).
    pub name: String,
    /// The source Parquet column name.
    pub column: String,
}

/// Which histogram encoding a `[metrics.histogram]` section describes. Only
/// `classic` is mappable in this version (ADR-1751 decision 2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HistogramTypeArg {
    Classic,
    Native,
    Exponential,
}

/// The classic-histogram shape of a metrics mapping (ADR-1751 decision 2):
/// the `le` column plus the data point's `sum` and `count` columns.
///
/// One input ROW is one bucket of one data point. The rows of one data point
/// are those sharing a metric name, a label set and a `ts`, and they must be
/// CONTIGUOUS in the file; the loader groups a contiguous run and refuses a
/// run it has already closed rather than exploding a data point twice (see
/// [`MetricsLoadError`]'s non-contiguity message). Within a group:
///
/// - the row's `value` column is that bucket's OWN count, the OTLP
///   `bucket_counts[i]` convention, not a running total. The loader
///   accumulates, exactly as `ravel_otlp::normalize`'s `explode_histogram`
///   does, so a Prometheus-style already-cumulative `_bucket` export must be
///   de-accumulated before it is loaded;
/// - the row's `le` column is that bucket's explicit upper bound, and must be
///   finite. The `+Inf` bucket is NOT a row: it is synthesized from the
///   `count` column, matching OTLP, where `explicit_bounds` carries only the
///   finite bounds;
/// - `sum` and `count` are the whole data point's, so every row of one group
///   must carry the same values (compared by bit pattern for `sum`). A null
///   `sum` cell emits no `_sum` series, matching an OTLP data point with no
///   `sum` field.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HistogramMap {
    /// `classic` (the default) or an explicit rejection of `native` /
    /// `exponential`.
    #[serde(default, rename = "type")]
    pub histogram_type: Option<HistogramTypeArg>,
    /// Source column carrying each row's explicit bucket upper bound.
    pub le_column: String,
    /// Source column carrying the data point's `sum`. A null cell emits no
    /// `_sum` series.
    pub sum_column: String,
    /// Source column carrying the data point's total count, which is both the
    /// `+Inf` bucket's value and the `_count` series' value.
    pub count_column: String,
}

/// The `[metrics]` section of a `--mapping` TOML (ADR-1751 decision 2).
///
/// ```toml
/// [metrics]
/// name_column  = "metric"      # a column, OR name = "http_requests_total"
/// value_column = "value"
/// ts_column    = "ts"
/// ts_unit      = "millis"      # seconds | millis | micros | nanos
/// unit         = "s"           # optional UCUM unit, suffixed into the name
/// kind         = "counter"     # optional: gauge (default) | counter
///
/// [[metrics.label]]
/// name   = "job"
/// column = "svc"
///
/// # Optional classic-histogram shape. With it, one row is one bucket.
/// [metrics.histogram]
/// le_column    = "le"
/// sum_column   = "sum"
/// count_column = "count"
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MetricsMapping {
    /// Literal metric name, used for every row. Mutually exclusive with
    /// [`MetricsMapping::name_column`]; exactly one is required.
    #[serde(default)]
    pub name: Option<String>,
    /// Source column carrying each row's metric name. Mutually exclusive with
    /// [`MetricsMapping::name`]; exactly one is required.
    #[serde(default)]
    pub name_column: Option<String>,
    /// Source column carrying the sample value. For a classic histogram this
    /// is the row's own bucket count (see [`HistogramMap`]).
    pub value_column: String,
    /// Source column carrying the event timestamp.
    pub ts_column: String,
    /// Unit of [`MetricsMapping::ts_column`] when it is an integer column. A
    /// native Arrow `Timestamp` column carries its own unit and this is not
    /// applied again (see [`read_ts`]).
    pub ts_unit: TsUnit,
    /// The metric's UCUM unit, played by this mapping exactly as an OTLP
    /// `Metric`'s `unit` field is: it selects the Prometheus unit suffix
    /// appended to the family name (ADR-0085 decision 2, applied through
    /// `ravel_otlp::normalize::prometheus_family_name`). Absent is the empty
    /// unit, which suffixes nothing.
    #[serde(default)]
    pub unit: Option<String>,
    /// `counter` sets `is_monotonic_sum` on every point this mapping
    /// produces, as a monotonic OTLP `Sum` does, and adds the `_total` suffix
    /// its family name gets; absent or `gauge` leaves both off. Refused
    /// together with `[metrics.histogram]`: OTLP has no monotonic histogram,
    /// and every series a classic histogram explodes into is non-monotonic.
    #[serde(default)]
    pub kind: Option<MetricKindArg>,
    /// Columns that become Prometheus labels on the series.
    #[serde(default, rename = "label")]
    pub labels: Vec<LabelMap>,
    /// Optional classic-histogram shape. Absent means one row is one scalar
    /// sample.
    #[serde(default)]
    pub histogram: Option<HistogramMap>,
}

impl MetricsMapping {
    /// `true` when this mapping describes a classic histogram.
    pub fn is_histogram(&self) -> bool {
        self.histogram.is_some()
    }

    /// The OTLP metric kind this mapping stands for and whether its points
    /// are a monotonic sum, the two inputs
    /// `ravel_otlp::normalize::prometheus_family_name` takes beside the unit.
    ///
    /// A classic histogram is `Histogram`, never monotonic, exactly as
    /// `ravel_otlp::normalize` classifies an OTLP `Histogram`; `kind` cannot
    /// be set alongside one (see [`MetricsMapping::validate`]).
    pub(crate) fn metric_kind(&self) -> (MetricKind, bool) {
        if self.is_histogram() {
            return (MetricKind::Histogram, false);
        }
        match self.kind {
            Some(MetricKindArg::Counter) => (MetricKind::Counter, true),
            Some(MetricKindArg::Gauge) | None => (MetricKind::Gauge, false),
        }
    }

    /// The mapping's unit, empty when it declares none. Fed to
    /// `prometheus_family_name` where OTLP feeds `Metric::unit`.
    pub(crate) fn unit(&self) -> &str {
        self.unit.as_deref().unwrap_or("")
    }

    /// Each mapped label's Prometheus name, put through the same
    /// `sanitize_label_name` OTLP applies to an attribute key, paired with
    /// the source column. Computed once per batch rather than once per row.
    pub(crate) fn sanitized_label_names(&self) -> Vec<String> {
        self.labels
            .iter()
            .map(|l| sanitize_label_name(l.name.clone()))
            .collect()
    }

    /// The checks a metrics mapping fails before any Parquet byte is read.
    ///
    /// Everything here is a property of the mapping alone, so it is worth
    /// refusing at setup: a name that is neither a column nor a literal, a
    /// label name that would collide with a synthesized one, or a native
    /// histogram, each of which would otherwise be discovered per row (or,
    /// for the collision, only as a `DuplicateLabelName`-shaped rejection on
    /// the first row).
    pub fn validate(&self) -> Result<(), LoadError> {
        match (&self.name, &self.name_column) {
            (Some(_), Some(_)) => {
                return Err(LoadError::Setup(
                    "--mapping [metrics] sets both name and name_column. The metric name is \
                     either a literal (name) or a column (name_column), never both."
                        .to_string(),
                ));
            }
            (None, None) => {
                return Err(LoadError::Setup(
                    "--mapping [metrics] sets neither name nor name_column. The metric name is \
                     either a literal (name) or a column (name_column)."
                        .to_string(),
                ));
            }
            _ => {}
        }
        if let Some(literal) = &self.name {
            let limits = IngestLimits::default();
            if literal.is_empty() {
                return Err(LoadError::Setup(
                    "--mapping [metrics] name is empty; a metric name is required".to_string(),
                ));
            }
            if literal.len() > limits.max_metric_name_len {
                return Err(LoadError::Setup(format!(
                    "--mapping [metrics] name is {} bytes, more than the metric-name limit of {}",
                    literal.len(),
                    limits.max_metric_name_len
                )));
            }
        }

        let limits = IngestLimits::default();
        // Every check below is against the SANITIZED label name, because that
        // is the name the series carries: OTLP sanitizes an attribute key
        // before it becomes a label, so two mapped names that differ only in
        // characters the sanitizer rewrites are one label, not two, and a name
        // that sanitizes to `__name__` or `le` collides with a synthesized one
        // however it was spelled.
        let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
        for label in &self.labels {
            if label.name.is_empty() {
                return Err(LoadError::Setup(
                    "--mapping [[metrics.label]] has an empty name".to_string(),
                ));
            }
            let name = sanitize_label_name(label.name.clone());
            if name.len() > limits.max_label_name_len {
                return Err(LoadError::Setup(format!(
                    "--mapping [[metrics.label]] name {:?} is {} bytes, more than the label-name \
                     limit of {}",
                    label.name,
                    name.len(),
                    limits.max_label_name_len
                )));
            }
            if name == METRIC_NAME_LABEL {
                return Err(LoadError::Setup(format!(
                    "--mapping [[metrics.label]] maps {:?}, which becomes {METRIC_NAME_LABEL:?} \
                     and carries the metric name. Use name or name_column instead.",
                    label.name
                )));
            }
            if self.is_histogram() && name == LE_LABEL {
                return Err(LoadError::Setup(format!(
                    "--mapping [[metrics.label]] maps {:?}, which becomes {LE_LABEL:?}, the label \
                     the classic-histogram explosion synthesizes per bucket. Two labels of the \
                     same name cannot exist on one series.",
                    label.name
                )));
            }
            if !seen.insert(name.clone()) {
                return Err(LoadError::Setup(format!(
                    "--mapping [[metrics.label]] declares {:?} twice (label names are compared \
                     after the OTLP sanitizer rewrites them to {name:?}); label names are unique \
                     on a series",
                    label.name
                )));
            }
        }

        if self.is_histogram() && self.kind.is_some() {
            return Err(LoadError::Setup(
                "--mapping [metrics] sets kind together with [metrics.histogram]. A classic \
                 histogram has no monotonic form in OTLP: every series it explodes into \
                 (_bucket, _sum, _count) is non-monotonic and its family name takes no _total \
                 suffix, so kind here would name a behaviour the load cannot produce. Remove it."
                    .to_string(),
            ));
        }

        if let Some(histogram) = &self.histogram {
            match histogram.histogram_type {
                None | Some(HistogramTypeArg::Classic) => {}
                Some(HistogramTypeArg::Native) => {
                    return Err(native_histogram_rejected("histogram.type = \"native\""));
                }
                Some(HistogramTypeArg::Exponential) => {
                    return Err(native_histogram_rejected(
                        "histogram.type = \"exponential\"",
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Keys that name a span shape ADR-1751 decision 2 does not map (span events
/// and span links), refused by name before `deny_unknown_fields` can report
/// them as a generic typo.
const UNMAPPABLE_SPAN_KEYS: [&str; 10] = [
    "event",
    "events",
    "span_event",
    "span_events",
    "events_column",
    "link",
    "links",
    "span_link",
    "span_links",
    "links_column",
];

/// Refuse a spans section that names events or links at the section's top
/// level.
fn reject_unmappable_span_keys(value: &toml::Value) -> Result<(), LoadError> {
    let Some(table) = value.as_table() else {
        return Ok(());
    };
    for key in UNMAPPABLE_SPAN_KEYS {
        if table.contains_key(key) {
            return Err(span_shape_rejected(key));
        }
    }
    Ok(())
}

/// The refusal a mapping naming span events or links gets (ADR-1751
/// decision 2).
fn span_shape_rejected(what: &str) -> LoadError {
    LoadError::Setup(format!(
        "--mapping [spans] names {what}, which this version does not map (ADR-1751 decision 2: \
         native histograms, span events and span links are not mappable, and a mapping that names \
         them is rejected). The mappable span fields are trace_id, span_id, parent_span_id, name, \
         start_ts, end_ts, status_code, status_message, and the resource_attribute and attribute \
         column lists."
    ))
}

/// The `[spans]` section of a `--mapping` TOML (ADR-1751 decision 2).
///
/// ```toml
/// [spans]
/// trace_id_column       = "trace_id"   # 16-byte binary or 32-char hex string
/// span_id_column        = "span_id"    # 8-byte binary or 16-char hex string
/// parent_span_id_column = "parent"     # optional, same shape as span_id
/// name_column           = "name"
/// start_ts_column       = "start"
/// start_ts_unit         = "nanos"      # seconds | millis | micros | nanos
/// end_ts_column         = "end"
/// end_ts_unit           = "nanos"
/// status_code_column    = "status"     # optional, OTLP's 0/1/2 integer enum
/// status_message_column = "status_msg" # optional
///
/// # Resource attributes: merged into every span's one attrs map. A key may
/// # not appear in both attribute lists (such a mapping is refused), so this
/// # merge never has a collision to resolve.
/// [[spans.resource_attribute]]
/// key = "service.name"
/// column = "svc"
/// type = "str"
///
/// # Span attributes.
/// [[spans.attribute]]
/// key = "http.method"
/// column = "method"
/// type = "str"
/// ```
///
/// A span has no stream identity (ADR-0041 routes by `trace_id`), so unlike
/// the logs section the two attribute lists differ only in merge precedence,
/// not in what they identify.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SpansMapping {
    /// Source column carrying the 16-byte trace id.
    pub trace_id_column: String,
    /// Source column carrying the 8-byte span id.
    pub span_id_column: String,
    /// Source column carrying the parent's 8-byte span id. A null cell and an
    /// EMPTY cell (empty binary, empty string, or a zero-width fixed-size
    /// value) are both a root span, as OTLP's own empty `parent_span_id` is.
    #[serde(default)]
    pub parent_span_id_column: Option<String>,
    /// Source column carrying the span name.
    pub name_column: String,
    /// Source column carrying the span's start timestamp.
    pub start_ts_column: String,
    /// Unit of [`SpansMapping::start_ts_column`] when it is an integer
    /// column. A native Arrow `Timestamp` column carries its own unit and this
    /// is not applied again (see [`read_ts`]).
    pub start_ts_unit: TsUnit,
    /// Source column carrying the span's end timestamp.
    pub end_ts_column: String,
    /// Unit of [`SpansMapping::end_ts_column`], read like
    /// [`SpansMapping::start_ts_unit`]. Declared separately because a source
    /// file may well carry a second-granularity start beside a nanosecond
    /// duration-derived end.
    pub end_ts_unit: TsUnit,
    /// Source column carrying OTLP's status code as its integer enum (0
    /// unset, 1 ok, 2 error). Absent, or a null cell, is `Unset`. Read
    /// through `ravel_otlp`'s own mapping, so a value outside `0..=2`
    /// normalizes to `Unset` here exactly as it does on the OTLP path.
    #[serde(default)]
    pub status_code_column: Option<String>,
    /// Source column carrying the status message. An absent column, a null
    /// cell and an empty string all store no message, as an OTLP status with
    /// an empty `message` does.
    #[serde(default)]
    pub status_message_column: Option<String>,
    /// Columns merged into the span's `attrs` map with resource precedence.
    /// A key may not appear in both attribute lists, so that precedence never
    /// decides anything here; see [`SpansMapping::validate`].
    #[serde(default, rename = "resource_attribute")]
    pub resource_attributes: Vec<AttrMap>,
    /// Columns merged into the span's `attrs` map at span precedence.
    #[serde(default, rename = "attribute")]
    pub attributes: Vec<AttrMap>,
}

impl SpansMapping {
    /// Every mapped attribute column, resource ones first, paired with the
    /// precedence set it belongs to.
    fn mapped_attributes(&self) -> impl Iterator<Item = (&AttrMap, AttrScope)> {
        self.resource_attributes
            .iter()
            .map(|a| (a, AttrScope::Resource))
            .chain(self.attributes.iter().map(|a| (a, AttrScope::Span)))
    }

    /// The checks a spans mapping fails before any Parquet byte is read.
    ///
    /// Everything here is a property of the mapping alone: an attribute key
    /// that is empty, over the OTLP key-length cap, reserved for a span field
    /// this version does not map, or declared twice. The duplicate check spans
    /// both lists, not each list on its own: `attrs` is one map per span and
    /// `ravel_rspan::merge_attrs` resolves a collision by resource precedence,
    /// so a key named in both lists would silently make the span column dead.
    /// Which column reached the record is exactly the kind of thing a mapping
    /// must not decide invisibly.
    ///
    /// Both attribute-count caps are here for the same reason: the mapping
    /// bounds every row, since a row carries at most one attribute per list
    /// entry, so a mapping within a cap can never produce a span over it. The
    /// span cap is the loader per-record cap standing in for OTLP's
    /// `max_attributes_per_span`; the resource cap is OTLP's own
    /// `max_resource_attributes`, which bounds how much gets merged into every
    /// span under the resource and which the OTLP path enforces by rejecting
    /// those spans.
    ///
    /// Attribute VALUE lengths, the span name length and the status message
    /// length are per-row and are checked as rows are decoded.
    pub fn validate(&self) -> Result<(), LoadError> {
        let limits = SpanIngestLimits::default();
        if self.attributes.len() > LOADER_MAX_ATTRIBUTES_PER_RECORD {
            return Err(LoadError::Setup(format!(
                "--mapping [spans] declares {} attribute columns, more than the loader per-record \
                 cap of {}",
                self.attributes.len(),
                LOADER_MAX_ATTRIBUTES_PER_RECORD
            )));
        }
        if self.resource_attributes.len() > limits.max_resource_attributes {
            return Err(LoadError::Setup(format!(
                "--mapping [spans] declares {} resource_attribute columns, more than the OTLP \
                 per-resource cap of {}",
                self.resource_attributes.len(),
                limits.max_resource_attributes
            )));
        }
        let mut seen: std::collections::HashSet<&str> = std::collections::HashSet::new();
        for (attr, scope) in self.mapped_attributes() {
            let list = scope.list_name();
            if attr.key.is_empty() {
                return Err(LoadError::Setup(format!(
                    "--mapping [[spans.{list}]] has an empty key"
                )));
            }
            if attr.key.len() > limits.max_attribute_key_len {
                return Err(LoadError::Setup(format!(
                    "--mapping [[spans.{list}]] key {:?} is {} bytes, more than the \
                     attribute-key limit of {}",
                    attr.key,
                    attr.key.len(),
                    limits.max_attribute_key_len
                )));
            }
            // The OTLP path strips a sender's own attribute under a reserved
            // key and writes the span's real field there, so a mapped column
            // could only fabricate a field this version does not map.
            if is_reserved_key(&attr.key) {
                return Err(span_shape_rejected(&format!(
                    "the reserved attribute key {:?}",
                    attr.key
                )));
            }
            if !seen.insert(attr.key.as_str()) {
                return Err(LoadError::Setup(format!(
                    "--mapping [spans] declares the attribute key {:?} twice. A span carries one \
                     merged attrs map with unique keys, so one of the two columns would never \
                     reach the record.",
                    attr.key
                )));
            }
        }
        Ok(())
    }
}

/// Which precedence set a mapped span attribute belongs to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AttrScope {
    Resource,
    Span,
}

impl AttrScope {
    /// The mapping list this scope is spelled as.
    fn list_name(self) -> &'static str {
        match self {
            AttrScope::Resource => "resource_attribute",
            AttrScope::Span => "attribute",
        }
    }
}

/// Printed to stderr before a load runs: the per-tenant `AdmissionController`
/// (active-stream cap, stream-creation rate, byte rate) lives in the server's
/// HTTP layer and is bypassed by construction on this path (ADR-0089).
pub const ADMISSION_BYPASS_WARNING: &str = "warning: bulk load writes directly to the log ingest router. The per-tenant \
     AdmissionController (active-stream cap, stream-creation rate, byte rate) that guards the \
     HTTP ingest path is NOT applied to loaded data. There is no deduplication: re-running after \
     a failure re-ingests every row it is given. --skip-rows can resume a failed load \
     positionally, but only one started with --read-cursors 1 --pipeline-depth 1 (see \
     docs/guides/ingest.md). Retention is measured from load time, not from the records' event \
     times.";

/// [`ADMISSION_BYPASS_WARNING`] for a metrics load, which goes through the
/// metrics ingest router and reads one sequential cursor.
pub const METRICS_ADMISSION_BYPASS_WARNING: &str = "warning: bulk load writes directly to the \
     metrics ingest router. The per-tenant admission control that guards the HTTP ingest path \
     is NOT applied to loaded data. There is no deduplication: re-running after a failure \
     re-ingests every row it is given. --skip-rows can resume a failed load positionally, but \
     only one started with --pipeline-depth 1 (see docs/guides/ingest.md). Retention is \
     measured from load time, not from the samples' event times.";

/// [`ADMISSION_BYPASS_WARNING`] for a spans load, which goes through the span
/// ingest router and reads one sequential cursor.
pub const SPANS_ADMISSION_BYPASS_WARNING: &str = "warning: bulk load writes directly to the span \
     ingest router. The per-tenant admission control that guards the HTTP ingest path is NOT \
     applied to loaded data. There is no deduplication: re-running after a failure re-ingests \
     every row it is given, and a span id that already exists is stored again rather than \
     replaced. --skip-rows can resume a failed load positionally, but only one started with \
     --pipeline-depth 1 (see docs/guides/ingest.md). Retention is measured from load time, not \
     from the spans' event times.";

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
    signal: SignalArg,
    shards: u32,
    batch_rows: usize,
    skip_rows: u64,
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
        signal,
        shards,
        batch_rows,
        skip_rows,
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
    signal: SignalArg,
    shards: u32,
    batch_rows: usize,
    skip_rows: u64,
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
    let admission_warning = match signal {
        SignalArg::Metrics => METRICS_ADMISSION_BYPASS_WARNING,
        SignalArg::Spans => SPANS_ADMISSION_BYPASS_WARNING,
        SignalArg::Logs => ADMISSION_BYPASS_WARNING,
    };
    let _ = writeln!(warnings, "{admission_warning}");

    let mapping_text = std::fs::read_to_string(mapping_path)
        .map_err(|e| anyhow::anyhow!("failed to read --mapping {}: {e}", mapping_path.display()))?;

    let mapping = match parse_mapping_document(&mapping_text, signal)? {
        MappingSection::Metrics(metrics) => {
            return run_metrics(
                store,
                parquet_path,
                tenant,
                &metrics,
                shards,
                batch_rows,
                skip_rows,
                read_cursors,
                pipeline_depth,
                max_inflight_flushes,
                decode_queue_batches,
                target_bytes,
                max_flush_delay,
                now_ns,
                warnings,
            )
            .await;
        }
        MappingSection::Spans(spans) => {
            return run_spans(
                store,
                parquet_path,
                tenant,
                &spans,
                shards,
                batch_rows,
                skip_rows,
                read_cursors,
                pipeline_depth,
                max_inflight_flushes,
                decode_queue_batches,
                target_bytes,
                max_flush_delay,
                now_ns,
                warnings,
            )
            .await;
        }
        MappingSection::Logs(logs) => logs,
    };

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
        skip_rows,
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
            // A requested offset past the end of the file is always an operator
            // error: resuming an already-complete file needs
            // `--skip-rows == total rows` exactly, and anything larger is a
            // typo or an offset carried over from a different file. The run
            // still succeeds (it has nothing left to do), but the clamped
            // `rows_skipped` in the summary above cannot show that the request
            // exceeded the file, so a resume script reading only the exit code
            // would record the load as done.
            if let Some(warning) =
                skip_rows_past_end_warning(report.skip_rows_requested, report.file_total_rows)
            {
                let _ = writeln!(warnings, "{warning}");
            }
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
            print_durable_tokens(&err, LOGS_RESUMABLE_SETTINGS);
            // The report that held these figures is dropped with the error, and
            // they are the only thing an operator can act on to resume: print
            // them beside the error, with the settings precondition that says
            // whether they are an offset at all.
            if let Some(hint) = resume_hint(&err, read_cursors, pipeline_depth) {
                let _ = writeln!(warnings, "{hint}");
            }
            Err(anyhow::Error::new(err))
        }
    }
}

/// The CLI-facing metrics load: run it, print the summary on success, or the
/// durable commit tokens and the resume figures on failure.
///
/// Two of the logs path's levers do not reach the metrics decoder, so they are
/// reported rather than silently dropped: `--read-cursors` (the metrics path
/// reads one sequential cursor, because a classic histogram's data point is a
/// contiguous run of rows) and `--decode-queue-batches` (there is no
/// decode/encode queue here; each batch's decode is one `spawn_blocking` the
/// submit loop awaits). Everything that shapes the objects -- `--shards`,
/// `--batch-rows`, `--target-bytes`, `--max-inflight-flushes`,
/// `--max-flush-delay`, `--pipeline-depth` -- applies unchanged.
#[allow(clippy::too_many_arguments)]
async fn run_metrics(
    store: Arc<dyn ObjectStoreBackend>,
    parquet_path: &Path,
    tenant: &str,
    mapping: &MetricsMapping,
    shards: u32,
    batch_rows: usize,
    skip_rows: u64,
    read_cursors: Option<usize>,
    pipeline_depth: usize,
    max_inflight_flushes: u32,
    decode_queue_batches: usize,
    target_bytes: usize,
    max_flush_delay: Option<Duration>,
    now_ns: i64,
    warnings: &mut dyn std::io::Write,
) -> anyhow::Result<()> {
    check_sequential_levers(
        read_cursors,
        decode_queue_batches,
        SignalArg::Metrics,
        warnings,
    )?;

    match load_metrics(
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
        Arc::new(SystemClock),
    )
    .await
    {
        Ok(report) => {
            print_metrics_summary(&report);
            if let Some(warning) =
                skip_rows_past_end_warning(report.skip_rows_requested, report.file_total_rows)
            {
                let _ = writeln!(warnings, "{warning}");
            }
            Ok(())
        }
        Err(err) => {
            print_durable_tokens(&err, SEQUENTIAL_RESUMABLE_SETTINGS);
            if let Some(hint) = sequential_resume_hint(&err, pipeline_depth) {
                let _ = writeln!(warnings, "{hint}");
                if mapping.is_histogram() {
                    let _ = writeln!(
                        warnings,
                        "rows_written counts whole classic-histogram data points here: a data \
                         point's rows are credited to the write that carries its exploded \
                         points, so this offset is a data-point boundary and a resume at it \
                         loads the next data point whole rather than part-way through its \
                         buckets."
                    );
                }
            }
            Err(anyhow::Error::new(err))
        }
    }
}

/// The CLI-facing spans load: the same shape as [`run_metrics`], over the
/// span ingest router.
///
/// It shares the metrics path's two unused levers for the same reason the
/// metrics path has them: there is no decode/encode queue here (each batch's
/// decode is one `spawn_blocking` the submit loop awaits), and the reader is
/// one sequential cursor, which keeps a failed run's landed rows a file prefix
/// under `--pipeline-depth 1`. Everything that shapes the objects --
/// `--shards`, `--batch-rows`, `--target-bytes`, `--max-inflight-flushes`,
/// `--max-flush-delay`, `--pipeline-depth` -- applies unchanged.
#[allow(clippy::too_many_arguments)]
async fn run_spans(
    store: Arc<dyn ObjectStoreBackend>,
    parquet_path: &Path,
    tenant: &str,
    mapping: &SpansMapping,
    shards: u32,
    batch_rows: usize,
    skip_rows: u64,
    read_cursors: Option<usize>,
    pipeline_depth: usize,
    max_inflight_flushes: u32,
    decode_queue_batches: usize,
    target_bytes: usize,
    max_flush_delay: Option<Duration>,
    now_ns: i64,
    warnings: &mut dyn std::io::Write,
) -> anyhow::Result<()> {
    check_sequential_levers(
        read_cursors,
        decode_queue_batches,
        SignalArg::Spans,
        warnings,
    )?;

    // The report is owned here rather than returned, so a FAILED load can still
    // print the attributes it dropped: that figure is not one `LoadError`
    // carries, and a failure does not undo the approximation in the batches
    // that did land.
    let mut report = SpansLoadReport::default();
    match load_spans_into(
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
        Arc::new(SystemClock),
    )
    .await
    {
        Ok(()) => {
            print_spans_summary(&report);
            if let Some(warning) =
                skip_rows_past_end_warning(report.skip_rows_requested, report.file_total_rows)
            {
                let _ = writeln!(warnings, "{warning}");
            }
            Ok(())
        }
        Err(err) => {
            print_durable_tokens(&err, SEQUENTIAL_RESUMABLE_SETTINGS);
            print_spans_attrs_dropped(&report, AttrsDroppedScope::Failed);
            if let Some(hint) = sequential_resume_hint(&err, pipeline_depth) {
                let _ = writeln!(warnings, "{hint}");
            }
            Err(anyhow::Error::new(err))
        }
    }
}

/// The flags under which a failed logs load's landed rows are a file prefix.
const LOGS_RESUMABLE_SETTINGS: &str = "--read-cursors 1 --pipeline-depth 1";

/// The same for the metrics and spans loads, which each read one sequential
/// cursor and take `--read-cursors` out of the question.
const SEQUENTIAL_RESUMABLE_SETTINGS: &str = "--pipeline-depth 1";

/// `--read-cursors 0`, rejected on both signals.
const READ_CURSORS_ZERO: &str = "--read-cursors must be at least 1, or omitted for automatic \
                                 sizing (min(shard count, row-group count)); 0 was given";

/// `--decode-queue-batches 0`, rejected on both signals.
const DECODE_QUEUE_BATCHES_ZERO: &str = "--decode-queue-batches must be at least 1 (the number \
                                         of decoded batches allowed to queue ahead of the shard \
                                         writers); 0 was given";

/// Reject the two lever values both sequential paths call invalid and warn
/// about a value either path would ignore, shared by the metrics and spans
/// loads because the rule and the reasoning are identical on both.
///
/// The logs path rejects `0` for either lever; a lever a sequential path
/// ignores is still not one that may take a value its own documentation calls
/// invalid, so the rejection is unconditional and the *ignoring* is a warning.
fn check_sequential_levers(
    read_cursors: Option<usize>,
    decode_queue_batches: usize,
    signal: SignalArg,
    warnings: &mut dyn std::io::Write,
) -> Result<(), LoadError> {
    if read_cursors == Some(0) {
        return Err(LoadError::Setup(READ_CURSORS_ZERO.to_string()));
    }
    if decode_queue_batches == 0 {
        return Err(LoadError::Setup(DECODE_QUEUE_BATCHES_ZERO.to_string()));
    }
    if let Some(warning) = unused_lever_warning(read_cursors, decode_queue_batches, signal) {
        let _ = writeln!(warnings, "{warning}");
    }
    Ok(())
}

/// The warning naming the levers a sequential (metrics or spans) load does not
/// use, or `None` when the operator left both at a value that changes nothing.
fn unused_lever_warning(
    read_cursors: Option<usize>,
    decode_queue_batches: usize,
    signal: SignalArg,
) -> Option<String> {
    let mut unused: Vec<String> = Vec::new();
    if let Some(k) = read_cursors
        && k != 1
    {
        unused.push(format!("--read-cursors {k}"));
    }
    if decode_queue_batches != DEFAULT_DECODE_QUEUE_BATCHES {
        unused.push(format!("--decode-queue-batches {decode_queue_batches}"));
    }
    if unused.is_empty() {
        return None;
    }
    // Each signal's own reason for reading one cursor, since they differ: a
    // metrics histogram's data point is a contiguous run of rows, while a
    // spans load simply has no shape that stride reads would help and keeps
    // the failure prefix a resume depends on.
    let (noun, why) = match signal {
        SignalArg::Spans => (
            "spans",
            "It reads one sequential cursor (nothing in a span's mapping spans several rows, so \
             stride reads would buy a spread the trace_id routing already gives while costing the \
             file-prefix property a resume depends on)",
        ),
        SignalArg::Metrics => (
            "metrics",
            "It reads one sequential cursor (a classic histogram's data point is a contiguous run \
             of rows, which stride reads would split)",
        ),
        // The logs load reads stride cursors and runs a decode queue, so it
        // uses both levers and there is nothing to warn about.
        SignalArg::Logs => return None,
    };
    Some(format!(
        "warning: a {noun} load ignores {}. {why} and has no decode/encode queue. --shards, \
         --batch-rows, --target-bytes, --max-inflight-flushes, --max-flush-delay and \
         --pipeline-depth all apply.",
        unused.join(" and ")
    ))
}

/// Print the metrics load's completion summary to stdout.
fn print_metrics_summary(report: &MetricsLoadReport) {
    let secs = report.elapsed.as_secs_f64();
    let rows_per_sec = if secs > 0.0 {
        report.rows_processed as f64 / secs
    } else {
        report.rows_processed as f64
    };
    println!("bulk load complete");
    println!("  signal           : metrics");
    println!("  rows_skipped     : {}", report.rows_skipped);
    println!("  rows_written     : {}", report.rows_processed);
    println!("  points_written   : {}", report.points_written);
    if report.histogram_points_exploded > 0 {
        println!(
            "  histogram points : {} (exploded into _bucket/_sum/_count series)",
            report.histogram_points_exploded
        );
    }
    println!("  rows/sec         : {rows_per_sec:.0}");
    println!("  objects written  : {}", report.objects_written());
    println!("  elapsed          : {secs:.3}s");
}

/// Print the spans load's completion summary to stdout.
fn print_spans_summary(report: &SpansLoadReport) {
    let secs = report.elapsed.as_secs_f64();
    let rows_per_sec = if secs > 0.0 {
        report.rows_processed as f64 / secs
    } else {
        report.rows_processed as f64
    };
    println!("bulk load complete");
    println!("  signal           : spans");
    println!("  rows_skipped     : {}", report.rows_skipped);
    // One source row is exactly one span on this path, so there is no second
    // record count to print beside it.
    println!("  rows_written     : {}", report.rows_processed);
    println!("  rows/sec         : {rows_per_sec:.0}");
    println!("  objects written  : {}", report.objects_written());
    print_spans_attrs_dropped(report, AttrsDroppedScope::Complete);
    println!("  elapsed          : {secs:.3}s");
}

/// Print the attribute values a spans load dropped for being over the OTLP
/// value-length cap.
///
/// Printed on the success path and beside the durable-token banner on the
/// failure path, because a nonzero count means the records that landed are an
/// approximation of the source rows and nothing else in either output says so.
/// The OTLP path reports the same drop as `AttributeValueTooLong` in its
/// partial-success message; a load has no partial-success channel, so the
/// summary is where it goes. Zero prints too: an operator reading the line as
/// evidence that nothing was dropped needs it to be there when nothing was.
///
/// `scope` decides what the line claims about the spans behind the count; see
/// [`AttrsDroppedScope`].
fn print_spans_attrs_dropped(report: &SpansLoadReport, scope: AttrsDroppedScope) {
    println!("{}", spans_attrs_dropped_line(report, scope));
}

/// Whether the load this count belongs to ran to completion.
///
/// The count is taken where a span is BUILT, so on a failed load it also covers
/// the batches the failure abandoned, whose spans are in no object. The line
/// says which of the two it is rather than claiming stored records either way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AttrsDroppedScope {
    /// Every decoded batch was written and acknowledged.
    Complete,
    /// The load failed, so some decoded batches never landed.
    Failed,
}

/// The `attrs_dropped` summary line, as [`print_spans_attrs_dropped`] prints it.
fn spans_attrs_dropped_line(report: &SpansLoadReport, scope: AttrsDroppedScope) -> String {
    let tail = match scope {
        AttrsDroppedScope::Complete => "each span was stored without them",
        AttrsDroppedScope::Failed => {
            "counted where each span was built, so this includes batches the failure abandoned, \
             whose spans are in no object"
        }
    };
    format!(
        "  attrs_dropped    : {} (attribute values over the OTLP value-length cap; {tail})",
        report.attributes_dropped
    )
}

/// The warning for a `--skip-rows` past the end of the file, or `None` when the
/// requested offset was within it (including the `== total_rows` case, which is
/// the legitimate resume of an already-complete file and stays silent).
///
/// Named both numbers on purpose: the summary prints the CLAMPED
/// `rows_skipped`, so without the requested value beside the file's total an
/// operator cannot tell a completed resume from an offset that missed the file
/// entirely. Takes the two figures rather than a report so the logs and the
/// metrics report share it.
fn skip_rows_past_end_warning(skip_rows_requested: u64, file_total_rows: u64) -> Option<String> {
    if skip_rows_requested > file_total_rows {
        Some(format!(
            "warning: --skip-rows {skip_rows_requested} is past the end of this file, which holds \
             {file_total_rows} rows. Nothing was loaded. Resuming an already-complete file takes \
             --skip-rows equal to the row count, so a larger value is a typo or an offset from a \
             different file."
        ))
    } else {
        None
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
    println!("  rows_skipped     : {}", report.rows_skipped);
    println!("  rows_written     : {}", report.rows_processed);
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

/// Display name for the stage-timings table (ADR-0104 decision 1). `Bloom` is
/// nested inside `Encode`'s window rather than a disjoint fifth slice, so its
/// row is marked as contained: a reader summing the printed column would
/// otherwise double-count it against `Encode`. Every other stage renders its
/// bare [`ravel_ingest::LogStage::name`].
#[cfg(feature = "stage-timing")]
fn stage_display_name(stage: ravel_ingest::LogStage) -> &'static str {
    match stage {
        ravel_ingest::LogStage::Bloom => "bloom (in encode)",
        other => other.name(),
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
            "    {name:<18} samples={samples:<10} total_ms={total_ms:<12.3} avg_us={avg_us:.3}",
            name = stage_display_name(stage),
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
/// row or batch never reached the router's `write`. The one exception is a
/// metrics refusal whose drain of earlier writes found a write failure; its
/// reason names that failure, and the Flush caveat applies to it.
///
/// `resumable_with` is the flag set under which this signal's failed load is a
/// resumable prefix ([`LOGS_RESUMABLE_SETTINGS`] or
/// [`SEQUENTIAL_RESUMABLE_SETTINGS`]).
fn print_durable_tokens(err: &LoadError, resumable_with: &str) {
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
        "{} commit token(s)/segment(s) were durable before the failure (a partial load, not a \
         rollback; --skip-rows can resume it instead of re-ingesting the whole file, but only \
         when this run used {resumable_with} -- see the resume figures printed with the error. \
         There is still no deduplication: nothing checks the offset a re-run is given){suffix}:",
        tokens.len()
    );
    for token in tokens {
        println!("  {}", token.encode());
    }
}

/// The failure-path resume block: the two figures [`ResumeFigures`] carries out
/// of the dropped [`LoadReport`], the offset they add up to, and whether this
/// run's settings make that offset mean anything.
///
/// The precondition is the whole content of the message. `--skip-rows` drops a
/// file-absolute prefix exactly, at any cursor count, but the rows a FAILED run
/// landed are a contiguous prefix of the file only under `--read-cursors 1
/// --pipeline-depth 1`: K cursors read K far-apart partitions concurrently, and
/// above depth 1 a batch submitted after the failing one can still commit, so
/// the landed set has holes and `rows_skipped + rows_written` names a position
/// no boundary sits at. Even under those settings the offset is a floor rather
/// than an exact boundary: a batch spans every shard its rows hash to, and the
/// failing batch can have committed on some of them, which puts those rows in
/// the durable token list and not in `rows_written`. A resume then re-ingests
/// them. The error is one-sided, and that is the direction to be wrong in:
/// duplicated rows are visible in the data, dropped rows are not. Printing the figures without that sentence is what
/// turns them into an offset an operator would paste into a resume that both
/// duplicates and loses rows.
///
/// `None` for [`LoadError::Setup`]: it fails before the offset is applied or
/// anything is written, so the previous run's own figures still stand.
fn resume_hint(
    err: &LoadError,
    read_cursors: Option<usize>,
    pipeline_depth: usize,
) -> Option<String> {
    let resume = err.resume_figures()?;
    let sequential = read_cursors == Some(1) && pipeline_depth == 1;
    let verdict = if sequential {
        "this run used --read-cursors 1 --pipeline-depth 1, so the rows that landed are a \
         contiguous prefix of the file and this offset loses nothing. One batch straddles \
         every shard it touches, though, and the failing batch can have committed on some \
         shards and not others; those rows are in the durable token list above and are not \
         counted in rows_written, so resuming here re-ingests them"
            .to_string()
    } else {
        let cursors = match read_cursors {
            Some(k) => format!("--read-cursors {k}"),
            None => {
                "--read-cursors unset (sized automatically to min(shards, row groups))".to_string()
            }
        };
        format!(
            "this run used {cursors} and --pipeline-depth {pipeline_depth}, so the rows that \
             landed are NOT a contiguous prefix of the file: the cursors read far-apart \
             partitions concurrently, and a batch submitted after the failing one can still have \
             committed. Resuming at this offset would both re-ingest committed rows and skip rows \
             that never landed. Only a load started with --read-cursors 1 --pipeline-depth 1 is \
             resumable this way"
        )
    };
    Some(resume_block(resume, &verdict))
}

/// [`resume_hint`] for a metrics or spans load. Both paths always read one
/// sequential cursor and ignore `--read-cursors`, so the pipeline depth is the
/// only setting the verdict names.
fn sequential_resume_hint(err: &LoadError, pipeline_depth: usize) -> Option<String> {
    let resume = err.resume_figures()?;
    let verdict = if pipeline_depth == 1 {
        "this run used --pipeline-depth 1, so the rows that landed are a contiguous prefix of \
         the file and this offset loses nothing. One batch straddles every shard it touches, \
         though, and the failing batch can have committed on some shards and not others; those \
         rows are in the durable token list above and are not counted in rows_written, so \
         resuming here re-ingests them"
            .to_string()
    } else {
        format!(
            "this run used --pipeline-depth {pipeline_depth}, so the rows that landed are NOT a \
             contiguous prefix of the file: a batch submitted after the failing one can still \
             have committed. Resuming at this offset would both re-ingest committed rows and skip \
             rows that never landed. Only a load started with --pipeline-depth 1 is resumable \
             this way"
        )
    };
    Some(resume_block(resume, &verdict))
}

/// The figures and verdict both resume hints print.
fn resume_block(resume: ResumeFigures, verdict: &str) -> String {
    format!(
        "resume figures for this failed load:\n  \
         rows_skipped     : {skipped}\n  \
         rows_written     : {written}\n  \
         next --skip-rows : {next} (rows_skipped + rows_written)\n\
         {verdict}. There is no deduplication and no per-file idempotency marker, so nothing \
         checks the offset a re-run is given; see docs/guides/ingest.md for the procedure.",
        skipped = resume.rows_skipped,
        written = resume.rows_written,
        next = resume.next_skip_rows(),
    )
}

/// Result of a successful (or partially-durable) load, for the summary output.
#[derive(Debug, Clone, Default)]
pub struct LoadReport {
    pub rows_processed: u64,
    /// `--skip-rows` (issue #1713): count of leading file rows dropped before
    /// mapping, by FILE-absolute position. `min(skip_rows, total file rows)`,
    /// computed once from Parquet footer metadata before decode starts. This
    /// is a positional offset with no idempotency marker: nothing here detects
    /// a wrong value. `rows_skipped + rows_written` is the next run's offset
    /// only when this run used one read cursor and a pipeline depth of 1; see
    /// [`ResumeFigures`].
    pub rows_skipped: u64,
    /// The `--skip-rows` value as the operator gave it, before the clamp to the
    /// file's row count, and the file's own total. They differ only when the
    /// requested offset is past the end of the file, which is always an
    /// operator error: a resume of an already-complete file needs
    /// `skip_rows == total_rows` exactly. `run_warning_to` reports that case,
    /// because the clamped `rows_skipped` alone cannot show it.
    pub skip_rows_requested: u64,
    /// The file's total row count, read from the Parquet footer.
    pub file_total_rows: u64,
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

/// The two figures a failed run has to hand back for `--skip-rows` to be
/// usable: the offset that run started from and the rows it acked durable
/// before it failed. Their sum is the next run's `--skip-rows` value, but only
/// under `--read-cursors 1 --pipeline-depth 1`, where the acked rows are a
/// contiguous prefix of the file. With K cursors the loader reads K far-apart
/// partitions concurrently, and at a depth above 1 a batch submitted after the
/// failing one can still commit, so what landed has holes and no single offset
/// describes it. See [`resume_hint`], which is what states that to the
/// operator.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ResumeFigures {
    /// `--skip-rows` as the failed run applied it (capped at the file's total
    /// row count), i.e. [`LoadReport::rows_skipped`].
    pub rows_skipped: u64,
    /// Rows from writes that acked durable, in submission order, before the
    /// failure, i.e. [`LoadReport::rows_processed`] at that moment. It does
    /// not count rows a later in-flight write committed after the failure
    /// ([`harvest_after_failure`] recovers those as tokens, not as rows),
    /// which is one of the reasons the sum below is not an offset at a depth
    /// above 1.
    pub rows_written: u64,
}

impl ResumeFigures {
    fn from_report(report: &LoadReport) -> Self {
        ResumeFigures {
            rows_skipped: report.rows_skipped,
            rows_written: report.rows_processed,
        }
    }

    /// The `--skip-rows` value a resume would use. Valid only under the two
    /// settings named on this type.
    pub fn next_skip_rows(&self) -> u64 {
        self.rows_skipped + self.rows_written
    }
}

/// A load failure. Every variant that can occur after some data is already
/// durable carries the durable commit tokens, so the caller reports the
/// genuine partial load rather than swallowing it into a generic error
/// (ADR-0089: a failed flush is a partial load, not a rollback). Those same
/// variants carry [`ResumeFigures`], because the report holding them is
/// dropped on this path and the operator needs them to decide the next run's
/// `--skip-rows`.
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
        resume: ResumeFigures,
    },
    /// A row failed a kept admission check (future skew, length cap, or the
    /// loader per-record attribute cap). Fail-fast: the run stops at the first
    /// bad row. Any tokens listed are batches durable before this row.
    #[error("row {row}: {reason}")]
    RowRejected {
        row: u64,
        reason: String,
        durable: Vec<CommitToken>,
        resume: ResumeFigures,
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
        resume: ResumeFigures,
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

    /// The rows skipped and the rows acked durable when this error occurred.
    /// `None` for [`LoadError::Setup`], which occurs before the load applies an
    /// offset or writes anything, so it has no figures to resume from.
    pub fn resume_figures(&self) -> Option<ResumeFigures> {
        match self {
            LoadError::Setup(_) => None,
            LoadError::BatchFailed { resume, .. }
            | LoadError::RowRejected { resume, .. }
            | LoadError::Flush { resume, .. } => Some(*resume),
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
/// resolved ([`harvest_after_failure`], issue #800) — a partial load, not a
/// rollback. Nothing deduplicates a re-ingest; the CLI's `--skip-rows` can
/// resume such a load positionally, but only when it ran with one read cursor
/// and a pipeline depth of 1, which is what makes the landed rows a contiguous
/// prefix of the file ([`ResumeFigures`]). On a [`LoadError::Flush`], the reported tokens are exact for a
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
        0,
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
    skip_rows: u64,
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
        return Err(LoadError::Setup(READ_CURSORS_ZERO.to_string()));
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
        return Err(LoadError::Setup(DECODE_QUEUE_BATCHES_ZERO.to_string()));
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
    // `--skip-rows` is a positional offset against the file's total row count,
    // known entirely from the footer metadata already parsed above -- no need
    // to wait for the decode pipeline to find out how many rows it dropped.
    // Beyond the file's row count, every row is skipped and the load succeeds
    // having written nothing (issue #1713).
    let total_rows: u64 = row_group_lens.iter().sum();
    report.rows_skipped = skip_rows.min(total_rows);
    report.skip_rows_requested = skip_rows;
    report.file_total_rows = total_rows;
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
        skip_rows,
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
                    resume: ResumeFigures::from_report(&report),
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
                    resume: ResumeFigures::from_report(&report),
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
            resume: ResumeFigures::from_report(&report),
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
    // write whose task had not yet reached its channel send when this flush
    // ran lands in a fresh buffer afterward; without help it would wait out
    // the age trigger (up to the whole raised delay of silent tail stall,
    // inside the scaled ack deadline but ugly), so a re-flush ticker below
    // sweeps such stragglers every few seconds while the drain runs. Sends
    // are FIFO per shard, so re-flushing never splits a dispatched write's
    // records; at most a straggler gets its own object, exactly as
    // `--target-bytes 1` would have laid it out.
    router.flush_all().await;

    // Drain every write still in the window in the same oldest-first order
    // before reporting success, with the straggler re-flush ticker running
    // alongside and stopped (and its task reaped) as soon as the drain ends.
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
        let result = drain_inflight(
            &mut inflight,
            &mut report,
            &mut shards_seen,
            &mut data_batches_flushed,
            shards,
        )
        .await;
        let _ = stop_tx.send(());
        let _ = ticker.await;
        result
    };
    drain_result?;

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
                resume: ResumeFigures::from_report(report),
            });
        }
        Err(join_err) => {
            return Err(LoadError::Flush {
                durable: report.tokens.clone(),
                cause: format!("write task failed: {join_err}"),
                resume: ResumeFigures::from_report(report),
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
    /// `--skip-rows`: the count of leading rows, by FILE-absolute position, to
    /// drop before mapping (issue #1713). Applied in [`collect_spans`] against
    /// each span's own `file_base`, so the dropped rows are exactly the file's
    /// first `skip_rows` rows regardless of how many stride cursors are dealing
    /// rows or in what order they hand them out. That the DROP is cursor-count
    /// independent does not make a resume so: what a failed run LANDED is a
    /// prefix only at one cursor and depth 1 ([`ResumeFigures`]).
    skip_rows: u64,
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

    // `--skip-rows` (issue #1713): drop the leading rows of every span whose
    // FILE-absolute position is under the skip threshold, before mapping ever
    // sees them. Comparing against `file_base` rather than a running counter
    // is what makes this correct independent of cursor count or dealing
    // order: a span's own absolute position decides whether it is skipped,
    // not the order this function happened to hand it out in.
    if state.skip_rows > 0 {
        spans.retain_mut(|(batch, file_base)| {
            let end = *file_base + batch.num_rows() as u64;
            if end <= state.skip_rows {
                return false;
            }
            if *file_base < state.skip_rows {
                let cut = (state.skip_rows - *file_base) as usize;
                *batch = batch.slice(cut, batch.num_rows() - cut);
                *file_base += cut as u64;
            }
            true
        });
    }
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

/// The schema every load of `path` opens its readers with: the
/// dictionary-preserving one [`load_reader_schema`] derives, or `None` when no
/// column qualifies and the reader infers as usual.
///
/// Public because the types a load actually SEES are not the types the file's
/// own schema declares, and a test asserting behaviour on a dictionary-encoded
/// column has to be able to say that the column really reached the loader as a
/// `Dictionary`. Parses the footer the same way the loader does.
pub fn reader_schema_for_path(path: &Path) -> Result<Option<SchemaRef>, LoadError> {
    let metadata = read_input_metadata(&FileInput { path })?;
    Ok(load_reader_schema(&metadata))
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
    /// The batch's columns with every mapped dictionary column resolved once.
    /// Read by the row path ([`build_record`]). Empty for an index built by
    /// [`ColumnIndex::locate`], whose columnar caller keys its `StrColumnDict`
    /// fast path on the dictionary itself and reads the batch's own columns.
    columns: ResolvedColumns,
}

impl ColumnIndex {
    /// The index for the row path: [`ColumnIndex::locate`] plus every mapped
    /// dictionary column resolved once for [`build_record`].
    fn resolve(batch: &RecordBatch, mapping: &Mapping) -> Result<ColumnIndex, String> {
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
    fn locate(batch: &RecordBatch, mapping: &Mapping) -> Result<ColumnIndex, String> {
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

    /// Column `i` as the row path must read it: resolved when it was a mapped
    /// dictionary column, the batch's own otherwise.
    fn col<'a>(&'a self, batch: &'a RecordBatch, i: usize) -> &'a ArrayRef {
        self.columns.col(batch, i)
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
    let ts_col = cols.col(batch, cols.ts);
    let raw_ts = read_ts(ts_col, row, mapping.ts_unit)?
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
        Some(i) => read_string(cols.col(batch, i), row)?.unwrap_or_default(),
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
        Some(i) => read_i64(cols.col(batch, i), row)?
            .and_then(|v| u8::try_from(v).ok())
            .unwrap_or(0),
        None => 0,
    };
    let severity_text = match cols.severity_text {
        Some(i) => read_string(cols.col(batch, i), row)?.unwrap_or_default(),
        None => String::new(),
    };

    // Trace/span ids: exact byte length or absent (never padded or truncated),
    // matching ravel-otlp.
    let trace_id = match cols.trace_id {
        Some(i) => read_id::<16>(cols.col(batch, i), row)?,
        None => None,
    };
    let span_id = match cols.span_id {
        Some(i) => read_id::<8>(cols.col(batch, i), row)?,
        None => None,
    };

    // Resource attributes: part of stream identity. A null column is omitted.
    let mut resource_attrs: Vec<(String, AttrValue)> = Vec::with_capacity(cols.resource.len());
    for (col_idx, map_idx) in &cols.resource {
        let spec = &mapping.resource_attributes[*map_idx];
        if let Some(value) = read_attr(cols.col(batch, *col_idx), row, spec.value_type)? {
            check_attr(&spec.key, &value, limits)?;
            resource_attrs.push((spec.key.clone(), value));
        }
    }

    // Record attributes: typed values in `attrs`, never part of identity.
    let mut attrs: Vec<(String, AttrValue)> = Vec::with_capacity(cols.record.len());
    for (col_idx, map_idx) in &cols.record {
        let spec = &mapping.attributes[*map_idx];
        if let Some(value) = read_attr(cols.col(batch, *col_idx), row, spec.value_type)? {
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

/// The refusal for a dictionary chunk whose dictionary is empty while a key
/// names a value in it. Arrow's `normalized_keys` asserts the values array is
/// non-empty and aborts the process on this shape, so it is refused before that
/// assertion is reached, in the resolution path and in the per-cell path alike
/// (#708 guards the same shape in [`str_src`] and [`bytes_src`]).
const EMPTY_DICTIONARY: &str = "dictionary-encoded column has an empty dictionary under a non-null \
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

/// Resolve a dictionary-encoded string or binary column to a flat column of its
/// value type, or `None` for a column the per-cell readers already index in
/// place.
///
/// `DictionaryArray::normalized_keys` builds a key vector the size of the whole
/// batch on every call, so a reader that resolves a dictionary cell per row
/// costs O(rows^2) per dictionary column. Every row path resolves its mapped
/// dictionary columns once, here, and indexes the result.
fn resolve_dictionary_column(arr: &ArrayRef) -> Result<Option<ArrayRef>, String> {
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
        // dictionary is corrupt.
        if arr.null_count() != arr.len() {
            return Err(EMPTY_DICTIONARY.to_string());
        }
        return Ok(Some(new_null_array(value_ty, arr.len())));
    }
    take(dict.values().as_ref(), dict.keys(), None)
        .map(Some)
        .map_err(|e| format!("could not resolve a dictionary-encoded column: {e}"))
}

/// One batch's columns, with every mapped dictionary column resolved once by
/// [`resolve_dictionary_column`]. Every other column is the batch's own.
struct ResolvedColumns {
    columns: Vec<ArrayRef>,
}

impl ResolvedColumns {
    /// Resolve the columns `mapped` names. A column named twice (two attributes
    /// reading one column) resolves on the first pass and is already flat on
    /// the second.
    fn resolve(
        batch: &RecordBatch,
        mapped: impl IntoIterator<Item = usize>,
    ) -> Result<ResolvedColumns, String> {
        let mut columns = batch.columns().to_vec();
        for i in mapped {
            let Some(column) = columns.get(i) else {
                continue;
            };
            if let Some(resolved) = resolve_dictionary_column(column)? {
                columns[i] = resolved;
            }
        }
        Ok(ResolvedColumns { columns })
    }

    /// No column resolved: [`ResolvedColumns::col`] answers every index with
    /// the batch's own column.
    fn none() -> ResolvedColumns {
        ResolvedColumns {
            columns: Vec::new(),
        }
    }

    /// Column `i` of the batch these columns were resolved from.
    fn col<'a>(&'a self, batch: &'a RecordBatch, i: usize) -> &'a ArrayRef {
        self.columns.get(i).unwrap_or_else(|| batch.column(i))
    }
}

/// The dictionary key at `row`, refusing an empty dictionary rather than
/// aborting inside arrow's `normalized_keys`.
///
/// The row paths index columns [`ResolvedColumns`] has already resolved, so
/// this is reached only by a caller handed a dictionary column directly.
fn dictionary_key(arr: &ArrayRef, row: usize) -> Result<usize, String> {
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
    /// Columns [`resolve_dictionary_column`] has resolved on this thread, for
    /// the test that pins one resolution per dictionary column per batch. A
    /// thread local rather than a global: tests share a process.
    static DICT_COLUMNS_RESOLVED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
    /// The same for [`dictionary_key`], which is the per-cell cost.
    static DICT_CELL_KEYS_RESOLVED: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

/// Zero both dictionary counters and return a handle that reads them.
#[cfg(test)]
fn dict_counters() -> DictCounters {
    DICT_COLUMNS_RESOLVED.with(|n| n.set(0));
    DICT_CELL_KEYS_RESOLVED.with(|n| n.set(0));
    DictCounters
}

#[cfg(test)]
struct DictCounters;

#[cfg(test)]
impl DictCounters {
    /// Dictionary columns resolved once each, ahead of the row loop.
    fn columns(&self) -> u64 {
        DICT_COLUMNS_RESOLVED.with(std::cell::Cell::get)
    }

    /// Dictionary keys resolved per cell, which is the quadratic cost.
    fn cell_keys(&self) -> u64 {
        DICT_CELL_KEYS_RESOLVED.with(std::cell::Cell::get)
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
        // its differential reference, must read the same values. The row paths
        // reach this arm only for a column [`ResolvedColumns`] did not resolve.
        DataType::Dictionary(_, _) => {
            let dict = arr.as_any_dictionary();
            read_string(dict.values(), dictionary_key(arr, row)?)
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
            read_bytes(dict.values(), dictionary_key(arr, row)?)
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
fn negative_ts_rejection(ts_ns: i64, ts_type: &DataType, declared: TsUnit) -> String {
    let unit = ts_read_unit(ts_type, declared, "ts_unit");
    format!(
        "timestamp is before the Unix epoch ({ts_ns} ns, {unit}); the column holds a negative value"
    )
}

/// The unit [`read_ts`] applied to a timestamp column of type `ts_type`, as a
/// refusal names it: a native Arrow `Timestamp` column's own unit, the
/// declared unit (under its mapping key `unit_key`) for any other column.
fn ts_read_unit(ts_type: &DataType, declared: TsUnit, unit_key: &str) -> String {
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

/// An id column as [`IdSrc`] reads it: a dictionary-encoded string or binary
/// column resolved to its flat value type (a null key is a null cell), any
/// other column unchanged.
fn flat_id_column(arr: &ArrayRef) -> Result<ArrayRef, String> {
    Ok(resolve_dictionary_column(arr)?.unwrap_or_else(|| Arc::clone(arr)))
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
        let cols = ColumnIndex::locate(span, mapping).map_err(ColBuildError::Batch)?;

        // Prepare every reader once per span (downcast resolved here, not per
        // cell).
        let ts = ts_src(span.column(cols.ts), mapping.ts_unit);
        let body = cols.body.map(|i| str_src(span.column(i)));
        let sev_num = cols.severity_number.map(|i| int_src(span.column(i)));
        let sev_text = cols.severity_text.map(|i| str_src(span.column(i)));
        // The id columns are the only ones resolved from a dictionary here:
        // `IdSrc` reads flat string or binary cells, and a default Parquet
        // writer dictionary-encodes a hex id column.
        let trace_col = cols
            .trace_id
            .map(|i| flat_id_column(span.column(i)))
            .transpose()
            .map_err(ColBuildError::Batch)?;
        let span_id_col = cols
            .span_id
            .map(|i| flat_id_column(span.column(i)))
            .transpose()
            .map_err(ColBuildError::Batch)?;
        let trace = trace_col.as_ref().map(id_src);
        let span_id_src = span_id_col.as_ref().map(id_src);
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

// ---------------------------------------------------------------------------
// Metrics load (ADR-1751 decisions 1 and 2)
//
// The same shape as the logs load above -- provision or validate the signal,
// build the router from the same `build_ingest_config`, write `WriteMode::
// Strict` batches through an in-flight window -- against
// `ravel_ingest::IngestRouter` and `ravel_otlp::NormalizedPoint`. What differs
// is normalisation: a metric point is a series id, a label set and one `f64`
// sample, and a classic histogram row set explodes into the
// Prometheus-convention `_bucket`/`_sum`/`_count` series.
//
// It deliberately does not reuse the logs path's columnar fast path or its K
// stride cursors. The columnar builder targets `ColumnarLogBatch`, which has
// no metrics counterpart, and the stride cursors interleave far-apart file
// regions inside one batch, which would break the contiguity a classic
// histogram's row group depends on.
// ---------------------------------------------------------------------------

/// Result of a successful (or partially-durable) metrics load.
#[derive(Debug, Clone, Default)]
pub struct MetricsLoadReport {
    /// Source rows whose points acked durable, in submission order.
    pub rows_processed: u64,
    /// `--skip-rows`, clamped to the file's total row count.
    pub rows_skipped: u64,
    /// `--skip-rows` as the operator gave it, before the clamp.
    pub skip_rows_requested: u64,
    /// The file's total row count, read from the Parquet footer.
    pub file_total_rows: u64,
    /// Points acked durable. This is NOT the row count: a classic-histogram
    /// data point of `n` finite bounds explodes into `n + 2` or `n + 3`
    /// points out of `n` rows, and it is the reachability signal a caller of
    /// the real entry point reads to prove the explosion ran rather than that
    /// its builder compiles.
    pub points_written: u64,
    /// Classic-histogram data points exploded, zero on a scalar mapping.
    pub histogram_points_exploded: u64,
    /// One token per shard acked, across every batch, in submission order.
    pub tokens: Vec<CommitToken>,
    pub elapsed: Duration,
}

impl MetricsLoadReport {
    /// Distinct commit tokens, which is the number of RSEG objects the load
    /// wrote. Same derivation as [`LoadReport::objects_written`].
    pub fn objects_written(&self) -> usize {
        let mut seen = std::collections::HashSet::new();
        self.tokens
            .iter()
            .filter(|t| seen.insert(t.encode()))
            .count()
    }
}

/// Resolved column indices for the mapped fields of one metrics batch, plus
/// the parts of the name/label normalization that depend on the mapping alone
/// and are therefore resolved once per batch rather than once per row.
struct MetricsColumnIndex {
    /// `None` when the mapping carries a literal `name`.
    name: Option<usize>,
    /// The normalized family name of a literal `name`, `None` when the name
    /// comes from a column (it is then normalized per row).
    literal_family_name: Option<String>,
    value: usize,
    ts: usize,
    /// One `(sanitized label name, column index)` per `[[metrics.label]]`, in
    /// mapping order.
    labels: Vec<(String, usize)>,
    histogram: Option<HistogramColumnIndex>,
    /// The metric kind and monotonicity the family name is suffixed under.
    kind: MetricKind,
    is_monotonic_sum: bool,
    /// The batch's columns with every mapped dictionary column resolved once.
    columns: ResolvedColumns,
}

struct HistogramColumnIndex {
    le: usize,
    sum: usize,
    count: usize,
}

impl MetricsColumnIndex {
    fn resolve(
        batch: &RecordBatch,
        mapping: &MetricsMapping,
    ) -> Result<MetricsColumnIndex, String> {
        let schema = batch.schema();
        let idx = |name: &str| -> Result<usize, String> {
            schema
                .index_of(name)
                .map_err(|_| format!("mapped column {name:?} is not present in the Parquet file"))
        };
        let histogram = match &mapping.histogram {
            Some(h) => Some(HistogramColumnIndex {
                le: idx(&h.le_column)?,
                sum: idx(&h.sum_column)?,
                count: idx(&h.count_column)?,
            }),
            None => None,
        };
        let (kind, is_monotonic_sum) = mapping.metric_kind();
        let limits = IngestLimits::default();
        let literal_family_name = match &mapping.name {
            Some(literal) => Some(
                normalized_family_name(literal, mapping, kind, is_monotonic_sum, &limits)
                    .map_err(|e| format!("--mapping [metrics] name is unusable: {e}"))?,
            ),
            None => None,
        };
        let name = match &mapping.name_column {
            Some(c) => Some(idx(c)?),
            None => None,
        };
        let value = idx(&mapping.value_column)?;
        let ts = idx(&mapping.ts_column)?;
        let labels = mapping
            .sanitized_label_names()
            .into_iter()
            .zip(&mapping.labels)
            .map(|(name, l)| Ok((name, idx(&l.column)?)))
            .collect::<Result<Vec<_>, String>>()?;
        // The name and the label columns are the two a row reader reads a
        // string out of; the value, ts and histogram columns are numeric.
        let columns = ResolvedColumns::resolve(
            batch,
            name.into_iter().chain(labels.iter().map(|(_, i)| *i)),
        )?;
        Ok(MetricsColumnIndex {
            name,
            literal_family_name,
            value,
            ts,
            labels,
            histogram,
            kind,
            is_monotonic_sum,
            columns,
        })
    }

    /// Column `i` as the row readers must read it: resolved when it was a
    /// mapped dictionary column, the batch's own otherwise.
    fn col<'a>(&'a self, batch: &'a RecordBatch, i: usize) -> &'a ArrayRef {
        self.columns.col(batch, i)
    }
}

/// Put one raw metric name through the OTLP name pipeline: the raw-length cap,
/// `sanitize_metric_name`, the empty check, then the ADR-0085 decision 2
/// suffix pass in `prometheus_family_name`.
///
/// The order is `ravel_otlp::normalize`'s, and it is the order that matters:
/// the length cap is applied to the name as it arrives (as OTLP applies it to
/// `Metric::name`), and the suffixes are appended after sanitization, so a
/// mapped name and the OTLP metric it mirrors reach `SeriesId::compute` as the
/// same string.
pub(crate) fn normalized_family_name(
    raw: &str,
    mapping: &MetricsMapping,
    kind: MetricKind,
    is_monotonic_sum: bool,
    limits: &IngestLimits,
) -> Result<String, String> {
    if raw.len() > limits.max_metric_name_len {
        return Err(format!(
            "metric name {raw:?} is {} bytes, more than the limit of {}",
            raw.len(),
            limits.max_metric_name_len
        ));
    }
    let sanitized = sanitize_metric_name(raw);
    if sanitized.is_empty() {
        return Err("metric name is empty".to_string());
    }
    Ok(prometheus_family_name(
        &sanitized,
        mapping.unit(),
        kind,
        is_monotonic_sum,
    ))
}

/// One decoded source row, before it becomes a point or joins a histogram
/// group. Labels are the mapped ones only: `__name__` and the synthesized
/// `le` are added when the point is built.
struct MetricRow {
    name: String,
    labels: Vec<Label>,
    ts_ns: i64,
    payload: RowPayload,
}

/// What one source row carries beside its identity: a scalar sample, or one
/// bucket of a classic-histogram data point.
enum RowPayload {
    Scalar(f64),
    Bucket(BucketRow),
}

/// The classic-histogram fields of one source row.
struct BucketRow {
    /// This bucket's explicit (finite) upper bound.
    le: f64,
    /// This bucket's OWN count, from the `value` column (OTLP
    /// `bucket_counts[i]`). Read as an integer, never through `f64`: a count
    /// above 2^53 must survive, and OTLP carries these as `u64`.
    own_count: u64,
    /// The data point's `sum`, `None` for a null cell (no `_sum` series).
    sum: Option<f64>,
    /// The data point's total count: the `+Inf` bucket's value and `_count`.
    count: u64,
}

/// Read one numeric cell as `f64`, accepting float and integer columns.
///
/// An integer wider than 2^53 rounds to the nearest representable `f64`, the
/// same loss `ravel_otlp::normalize` documents for an OTLP `as_int` value:
/// the segment format stores `f64` only, so there is nowhere else for the
/// value to go.
fn read_metric_number(arr: &ArrayRef, row: usize) -> Result<Option<f64>, String> {
    if arr.is_null(row) {
        return Ok(None);
    }
    match arr.data_type() {
        DataType::Float32 | DataType::Float64 => read_f64(arr, row),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Ok(read_i64(arr, row)?.map(|v| v as f64)),
        other => Err(format!("expected a numeric column, found {other:?}")),
    }
}

/// Read one cell as a Prometheus label value. A label value is a string, so a
/// numeric or boolean column is stringified: floats through
/// [`format_float`], the same Go-compatible spelling the `le` label uses, so
/// two columns carrying the same number never produce two series.
fn read_label_value(arr: &ArrayRef, row: usize) -> Result<Option<String>, String> {
    if arr.is_null(row) {
        return Ok(None);
    }
    match arr.data_type() {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Dictionary(_, _) => read_string(arr, row),
        DataType::Boolean => Ok(read_bool(arr, row)?.map(|b| b.to_string())),
        DataType::Float32 | DataType::Float64 => Ok(read_f64(arr, row)?.map(format_float)),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Ok(read_i64(arr, row)?.map(|v| v.to_string())),
        other => Err(format!(
            "expected a string, numeric or boolean label column, found {other:?}"
        )),
    }
}

/// A bucket or data-point count read from a numeric cell: a non-negative
/// integer.
///
/// An integer column is read as an integer, never through `f64`: OTLP carries
/// `bucket_counts` and `count` as `u64`, and a round trip through `f64` would
/// silently move a count above 2^53 to the nearest representable value. A
/// float column is accepted only when its value is exactly integral, so a
/// fractional count is refused rather than truncated.
fn read_count(arr: &ArrayRef, row: usize) -> Result<Option<u64>, String> {
    if arr.is_null(row) {
        return Ok(None);
    }
    match arr.data_type() {
        DataType::UInt64 => Ok(Some(downcast::<UInt64Array>(arr)?.value(row))),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32 => {
            let v = read_i64(arr, row)?.ok_or_else(|| "count column is null".to_string())?;
            u64::try_from(v)
                .map(Some)
                .map_err(|_| format!("expected a non-negative whole-number count, found {v}"))
        }
        DataType::Float32 | DataType::Float64 => {
            let v = read_f64(arr, row)?.ok_or_else(|| "count column is null".to_string())?;
            exact_count(v).map(Some)
        }
        other => Err(format!("expected a numeric column, found {other:?}")),
    }
}

/// Decode one source row against the mapping, applying the kept admission
/// checks (future skew, the metric-name and label length caps, the loader
/// label cap). `Err` carries a per-row rejection reason.
fn build_metric_row(
    batch: &RecordBatch,
    cols: &MetricsColumnIndex,
    mapping: &MetricsMapping,
    limits: &IngestLimits,
    now_ns: i64,
    row: usize,
) -> Result<MetricRow, String> {
    let ts_col = cols.col(batch, cols.ts);
    let raw_ts = read_ts(ts_col, row, mapping.ts_unit)?
        .ok_or_else(|| format!("ts column {:?} is null", mapping.ts_column))?;
    if raw_ts < 0 {
        return Err(negative_ts_rejection(
            raw_ts,
            ts_col.data_type(),
            mapping.ts_unit,
        ));
    }

    // Kept: the future-skew bound, at the metrics OTLP limit. The past-lag
    // check is deliberately omitted (ADR-0089 relaxation, widened to every
    // signal by ADR-1751 decision 1).
    let skew_ns = raw_ts.saturating_sub(now_ns);
    if skew_ns > limits.max_future_skew_ns {
        return Err(format!(
            "timestamp is {skew_ns} ns ahead of load time, more than the max future skew of {} ns",
            limits.max_future_skew_ns
        ));
    }

    // The name goes through OTLP's own pipeline (length cap, sanitize, empty
    // check, unit and `_total` suffixes), so a metric loaded here lands on the
    // same family name, and therefore the same SeriesId, as the same metric
    // admitted over OTLP.
    let name = match (cols.name, &cols.literal_family_name) {
        (Some(i), _) => {
            let raw = read_string(cols.col(batch, i), row)?.ok_or_else(|| {
                format!(
                    "metric name column {:?} is null",
                    mapping.name_column.as_deref().unwrap_or_default()
                )
            })?;
            normalized_family_name(&raw, mapping, cols.kind, cols.is_monotonic_sum, limits)?
        }
        (None, Some(literal)) => literal.clone(),
        // `MetricsMapping::validate` refuses a mapping with neither, before
        // any row is read.
        (None, None) => return Err("mapping declares no metric name".to_string()),
    };

    // A null label cell omits that label, and so does an EMPTY one: OTLP drops
    // an empty attribute value before the label set is built (ADR-0038,
    // `push_checked`), so `{job=""}` and `{}` are one series there and must be
    // one series here too.
    let mut labels: Vec<Label> = Vec::with_capacity(cols.labels.len());
    for (name, col_idx) in &cols.labels {
        if let Some(value) = read_label_value(cols.col(batch, *col_idx), row)? {
            if value.is_empty() {
                continue;
            }
            check_label(name, &value, limits)?;
            labels.push(Label {
                name: name.clone(),
                value,
            });
        }
    }
    // The loader label cap stands in for OTLP's `max_attributes_per_point`
    // (ADR-1751 decision 1), rejected rather than silently truncated.
    if labels.len() > LOADER_MAX_ATTRIBUTES_PER_RECORD {
        return Err(format!(
            "point has {} labels, more than the loader per-record cap of {}",
            labels.len(),
            LOADER_MAX_ATTRIBUTES_PER_RECORD
        ));
    }

    let payload = match &cols.histogram {
        None => RowPayload::Scalar(
            read_metric_number(cols.col(batch, cols.value), row)?
                .ok_or_else(|| format!("value column {:?} is null", mapping.value_column))?,
        ),
        Some(h) => {
            let le = read_metric_number(cols.col(batch, h.le), row)?
                .ok_or_else(|| "histogram le column is null".to_string())?;
            if !le.is_finite() {
                // Matches `Rejection::NonFiniteHistogramBound`: OTLP's
                // `explicit_bounds` carries finite bounds only, and the `+Inf`
                // bucket is synthesized from `count`, never mapped as a row.
                return Err(format!(
                    "histogram le is {le}, which is not a finite bucket bound. The +Inf bucket is \
                     synthesized from the count column and must not be a row of its own."
                ));
            }
            // With a histogram mapping the value column is this bucket's own
            // count, so it is read as a count, not as a sample value.
            let own_count = read_count(cols.col(batch, cols.value), row)
                .map_err(|e| {
                    format!(
                        "value column {:?} is this bucket's own count on a classic-histogram \
                         mapping: {e}",
                        mapping.value_column
                    )
                })?
                .ok_or_else(|| format!("value column {:?} is null", mapping.value_column))?;
            let sum = read_metric_number(cols.col(batch, h.sum), row)?;
            let count = read_count(cols.col(batch, h.count), row)?
                .ok_or_else(|| "histogram count column is null".to_string())?;
            RowPayload::Bucket(BucketRow {
                le,
                own_count,
                sum,
                count,
            })
        }
    };

    Ok(MetricRow {
        name,
        labels,
        ts_ns: raw_ts,
        payload,
    })
}

/// Kept length caps for one label, at the metrics OTLP limits.
fn check_label(name: &str, value: &str, limits: &IngestLimits) -> Result<(), String> {
    if name.len() > limits.max_label_name_len {
        return Err(format!(
            "label name {name:?} is {} bytes, more than the limit of {}",
            name.len(),
            limits.max_label_name_len
        ));
    }
    if value.len() > limits.max_label_value_len {
        return Err(format!(
            "label {name:?} value is {} bytes, more than the limit of {}",
            value.len(),
            limits.max_label_value_len
        ));
    }
    Ok(())
}

/// Build one [`NormalizedPoint`], the metrics loader's counterpart of
/// `ravel_otlp::normalize`'s private `finish_point`: base labels plus
/// `__name__`, plus at most one synthesized label (`le`), through
/// [`LabelSet::new`] and [`SeriesId::compute`].
#[allow(clippy::too_many_arguments)]
fn metrics_point(
    tenant: &TenantId,
    base_labels: &[Label],
    metric_name: &str,
    extra_label: Option<(&str, String)>,
    ts_ns: i64,
    value: f64,
    is_monotonic_sum: bool,
    limits: &IngestLimits,
) -> Result<NormalizedPoint, String> {
    // No metric-name check here: the caller has already put the family name
    // through `normalized_family_name`, and the explosion's `_bucket`/`_sum`/
    // `_count` suffixes are appended after OTLP's own length cap the same way,
    // so re-checking would reject a name OTLP admits.
    let mut labels = Vec::with_capacity(base_labels.len() + 2);
    labels.extend_from_slice(base_labels);
    labels.push(Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: metric_name.to_string(),
    });
    if let Some((name, value)) = extra_label {
        check_label(name, &value, limits)?;
        labels.push(Label {
            name: name.to_string(),
            value,
        });
    }
    let label_set = LabelSet::new(labels)
        .map_err(|e| format!("labels are not a valid series label set: {e}"))?;
    let series_id = SeriesId::compute(tenant, metric_name, &label_set)
        .map_err(|e| format!("series identity could not be computed: {e}"))?;
    Ok(NormalizedPoint {
        series_id,
        labels: Arc::new(label_set),
        sample: Sample { ts_ns, value },
        is_monotonic_sum,
    })
}

/// One classic-histogram data point accumulated from a contiguous run of
/// source rows.
struct PendingHistogram {
    name: String,
    labels: Vec<Label>,
    ts_ns: i64,
    /// The finite explicit bounds, in row order.
    bounds: Vec<f64>,
    /// Each bound's OWN bucket count, in row order (OTLP `bucket_counts`).
    counts: Vec<u64>,
    /// The data point's `sum`, from its rows' `sum` column.
    sum: Option<f64>,
    /// The data point's total count, from its rows' `count` column.
    count: u64,
    /// File-absolute index of the group's first row, so a rejection raised
    /// when the group closes points at a row the operator can find.
    first_row: u64,
    /// Source rows folded into this group so far, possibly spanning a batch
    /// boundary. These rows are not durable until the write carrying this
    /// group's points acks, so they are credited to that write and to no
    /// earlier one.
    rows: u64,
}

impl PendingHistogram {
    /// Is this row another bucket of this same data point? A data point is one
    /// metric name, one label set and one `ts`.
    fn accepts(&self, name: &str, ts_ns: i64, labels: &[Label]) -> bool {
        self.name == name && self.ts_ns == ts_ns && self.labels == labels
    }
}

/// What closing zero or more histogram groups produced: their points, and the
/// source rows those points were built from.
#[derive(Default)]
struct Closed {
    points: Vec<NormalizedPoint>,
    rows: u64,
}

/// Groups contiguous classic-histogram rows into data points and explodes
/// each into its Prometheus-convention series.
struct HistogramGrouper {
    tenant: TenantId,
    pending: Option<PendingHistogram>,
    /// Every group already closed, keyed by its base series id and `ts`. A
    /// key that comes back means the file interleaves two data points'
    /// buckets, which no contiguous-run grouping can read correctly, so it is
    /// refused rather than exploded twice. This costs one 24-byte key per
    /// data point for the length of the load.
    closed: std::collections::HashSet<(SeriesId, i64)>,
    exploded: u64,
}

impl HistogramGrouper {
    fn new(tenant: TenantId) -> Self {
        HistogramGrouper {
            tenant,
            pending: None,
            closed: std::collections::HashSet::new(),
            exploded: 0,
        }
    }

    /// Fold one row into the open group, returning the points of the group it
    /// closed and the number of SOURCE ROWS those points came from (both empty
    /// and zero while the group is still accumulating). `file_row` is the
    /// row's file-absolute index, used for the rejection the caller reports.
    ///
    /// The row count is the group's, not the batch's: a group opened in an
    /// earlier batch carries its earlier rows here, and the rows of a group
    /// this row has just opened stay uncounted until that group closes. That
    /// is what keeps `rows_written`, and the resume offset derived from it,
    /// equal to a whole number of exploded data points.
    fn push(
        &mut self,
        row: MetricRow,
        file_row: u64,
        limits: &IngestLimits,
    ) -> Result<Closed, (u64, String)> {
        let MetricRow {
            name,
            labels,
            ts_ns,
            payload,
        } = row;
        let RowPayload::Bucket(bucket) = payload else {
            return Err((
                file_row,
                "internal error: a histogram mapping produced a row with no bucket".to_string(),
            ));
        };
        let mut closed = Closed::default();
        match &mut self.pending {
            Some(open) if open.accepts(&name, ts_ns, &labels) => {
                // Every row of one data point carries the SAME sum and count:
                // they describe the point, not the bucket. Compared by bit
                // pattern, so a NaN sum is compared like any other payload.
                if open.count != bucket.count {
                    return Err((
                        file_row,
                        format!(
                            "histogram count is {} here but {} on the first row of the same data \
                             point (row {}); the count column carries the DATA POINT's total, so \
                             it must repeat on every one of its bucket rows",
                            bucket.count, open.count, open.first_row
                        ),
                    ));
                }
                if open.sum.map(f64::to_bits) != bucket.sum.map(f64::to_bits) {
                    return Err((
                        file_row,
                        format!(
                            "histogram sum differs from the first row of the same data point (row \
                             {}); the sum column carries the DATA POINT's sum, so it must repeat \
                             on every one of its bucket rows",
                            open.first_row
                        ),
                    ));
                }
                // Refused here rather than at close so a mis-declared mapping
                // that makes the whole file one data point is not buffered
                // whole before the limit is checked.
                if open.bounds.len() >= limits.max_histogram_buckets {
                    return Err((
                        open.first_row,
                        too_many_bounds(
                            &open.name,
                            open.ts_ns,
                            open.bounds.len() + 1,
                            limits.max_histogram_buckets,
                        ),
                    ));
                }
                open.bounds.push(bucket.le);
                open.counts.push(bucket.own_count);
                open.rows += 1;
            }
            _ => {
                if let Some(open) = self.pending.take() {
                    closed = self.close(open, limits)?;
                }
                self.pending = Some(PendingHistogram {
                    name,
                    labels,
                    ts_ns,
                    bounds: vec![bucket.le],
                    counts: vec![bucket.own_count],
                    sum: bucket.sum,
                    count: bucket.count,
                    first_row: file_row,
                    rows: 1,
                });
            }
        }
        Ok(closed)
    }

    /// Close whatever group is open at end of input.
    fn finish(&mut self, limits: &IngestLimits) -> Result<Closed, (u64, String)> {
        match self.pending.take() {
            Some(open) => self.close(open, limits),
            None => Ok(Closed::default()),
        }
    }

    fn close(
        &mut self,
        group: PendingHistogram,
        limits: &IngestLimits,
    ) -> Result<Closed, (u64, String)> {
        let first_row = group.first_row;
        let key = histogram_group_key(&self.tenant, &group).map_err(|e| (first_row, e))?;
        if !self.closed.insert(key) {
            return Err((
                first_row,
                format!(
                    "the rows of the {:?} data point at ts {} are not contiguous: a group with \
                     this identity was already closed earlier in the file. Sort the input by \
                     metric name, labels and ts so every data point's bucket rows sit together.",
                    group.name, group.ts_ns
                ),
            ));
        }
        let points =
            explode_classic_histogram(&self.tenant, &group, limits).map_err(|e| (first_row, e))?;
        self.exploded += 1;
        Ok(Closed {
            points,
            rows: group.rows,
        })
    }
}

/// The base series identity of a histogram data point (its labels without the
/// synthesized `le`, under its unsuffixed name), paired with its `ts`. This
/// is the grouping key, computed once per group rather than once per row.
fn histogram_group_key(
    tenant: &TenantId,
    group: &PendingHistogram,
) -> Result<(SeriesId, i64), String> {
    let mut labels = group.labels.clone();
    labels.push(Label {
        name: METRIC_NAME_LABEL.to_string(),
        value: group.name.clone(),
    });
    let label_set = LabelSet::new(labels)
        .map_err(|e| format!("labels are not a valid series label set: {e}"))?;
    let series_id = SeriesId::compute(tenant, &group.name, &label_set)
        .map_err(|e| format!("series identity could not be computed: {e}"))?;
    Ok((series_id, group.ts_ns))
}

/// A count read from a float cell: a non-negative whole number representable
/// as `u64`.
///
/// The upper bound is `>=`, not `>`: `u64::MAX as f64` rounds UP to 2^64,
/// which is one past the largest `u64`, so a cell holding exactly 2^64 passes
/// a `>` test and then saturates to `u64::MAX` on the cast. Refused instead.
fn exact_count(value: f64) -> Result<u64, String> {
    if !value.is_finite() || value < 0.0 || value.fract() != 0.0 || value >= u64::MAX as f64 {
        return Err(format!(
            "expected a non-negative whole-number count that fits in u64, found {value}"
        ));
    }
    Ok(value as u64)
}

/// The refusal for a data point with more explicit bounds than
/// `IngestLimits::max_histogram_buckets`.
fn too_many_bounds(name: &str, ts_ns: i64, bounds: usize, limit: usize) -> String {
    format!(
        "the {name:?} data point at ts {ts_ns} has {bounds} explicit bounds, more than the limit \
         of {limit}"
    )
}

/// Explode one classic-histogram data point into its Prometheus-convention
/// series: one `{name}_bucket{le=<bound>}` per explicit bound plus
/// `{name}_bucket{le="+Inf"}` (= the point's count), `{name}_sum` when the
/// `sum` column was not null, and `{name}_count`.
///
/// Mirrors `ravel_otlp::normalize`'s private `explode_histogram` (which takes
/// an OTLP `HistogramDataPoint` and applies the past-lag check this path
/// relaxes, so it cannot be called directly), including its check order,
/// its accumulation of per-bucket counts into cumulative values, its use of
/// the point's raw count for BOTH the `+Inf` bucket and `_count`, and its
/// atomic-rejection contract: an `Err` means none of this point's series were
/// admitted, never a partial set (ADR-0016). `crates/ravel-otap/src/
/// normalize.rs`'s `explode_histogram_point` is the same mirror for the OTAP
/// surface. The `le` value goes through
/// [`ravel_otlp::promcompat::format_float`], the shared formatter that makes
/// the same histogram admitted through any of these surfaces land on the same
/// `SeriesId`.
///
/// Two OTLP behaviours have no source here and are therefore absent rather
/// than reimplemented: the stale marker (`DataPointFlags`, which a Parquet
/// row carries no equivalent of) and exemplars (ADR-1751 decision 2 maps
/// none).
fn explode_classic_histogram(
    tenant: &TenantId,
    group: &PendingHistogram,
    limits: &IngestLimits,
) -> Result<Vec<NormalizedPoint>, String> {
    if !group.bounds.windows(2).all(|w| w[0] < w[1]) {
        return Err(format!(
            "the {:?} data point at ts {} has bucket bounds that do not strictly increase in row \
             order ({:?}); sort each data point's rows by le",
            group.name, group.ts_ns, group.bounds
        ));
    }
    if group.bounds.len() > limits.max_histogram_buckets {
        return Err(too_many_bounds(
            &group.name,
            group.ts_ns,
            group.bounds.len(),
            limits.max_histogram_buckets,
        ));
    }

    let bucket_name = format!("{}_bucket", group.name);
    let mut series = Vec::with_capacity(group.bounds.len() + 3);
    let mut cumulative: u64 = 0;
    for (bound, count) in group.bounds.iter().zip(&group.counts) {
        cumulative = cumulative
            .checked_add(*count)
            .ok_or_else(|| "histogram bucket counts overflow u64".to_string())?;
        series.push(metrics_point(
            tenant,
            &group.labels,
            &bucket_name,
            Some((LE_LABEL, format_float(*bound))),
            group.ts_ns,
            cumulative as f64,
            false,
            limits,
        )?);
    }

    // The +Inf bucket and _count both use the point's raw count directly, not
    // the accumulated bucket sum (matches the collector's mapping and
    // ravel_otlp::explode_histogram).
    let count_value = group.count as f64;
    series.push(metrics_point(
        tenant,
        &group.labels,
        &bucket_name,
        Some((LE_LABEL, "+Inf".to_string())),
        group.ts_ns,
        count_value,
        false,
        limits,
    )?);

    if let Some(sum) = group.sum {
        series.push(metrics_point(
            tenant,
            &group.labels,
            &format!("{}_sum", group.name),
            None,
            group.ts_ns,
            sum,
            false,
            limits,
        )?);
    }

    series.push(metrics_point(
        tenant,
        &group.labels,
        &format!("{}_count", group.name),
        None,
        group.ts_ns,
        count_value,
        false,
        limits,
    )?);

    Ok(series)
}

/// The decode/build state the metrics loader shuttles into and back out of
/// each batch's `spawn_blocking` task: the single sequential cursor, plus the
/// histogram grouper whose open group can span a batch boundary.
struct MetricsDecodeState {
    cursor: CursorState,
    grouper: Option<HistogramGrouper>,
    /// `--skip-rows`, applied against each span's own file-absolute base, as
    /// [`collect_spans`] applies it on the logs path.
    skip_rows: u64,
}

/// One batch's decode outcome.
enum MetricsDecoded {
    /// Points built from this batch, the source rows THOSE POINTS came from,
    /// and whether the input is exhausted.
    ///
    /// `rows` is not the batch's row count on a histogram mapping: it is the
    /// row count of the groups this batch closed, which can include rows read
    /// in an earlier batch and excludes the rows of a group this batch only
    /// opened. Those uncounted rows are credited to the later write that
    /// carries their group's points, so every ack advances the resume offset
    /// to a group boundary and never past one.
    Batch {
        points: Vec<NormalizedPoint>,
        rows: u64,
        done: bool,
    },
    /// The batch failed to read from Parquet or to resolve against the
    /// mapping.
    Failed(String),
    /// A row failed a kept admission check, at its FILE-absolute index.
    Rejected { row: u64, reason: String },
}

/// Decode and build one metrics batch from the sequential cursor.
#[allow(clippy::too_many_arguments)]
fn decode_metrics_batch(
    state: &mut MetricsDecodeState,
    tenant: &TenantId,
    mapping: &MetricsMapping,
    limits: &IngestLimits,
    now_ns: i64,
    batch_rows: usize,
    is_monotonic_sum: bool,
) -> MetricsDecoded {
    let taken = match cursor_take(&mut state.cursor, batch_rows) {
        Ok(taken) => taken,
        Err(reason) => return MetricsDecoded::Failed(reason),
    };
    let Some((batch, file_base)) = taken else {
        // Input exhausted: close whatever histogram group is still open, so
        // the last data point in the file is exploded rather than dropped.
        let closed = match state.grouper.as_mut() {
            Some(grouper) => match grouper.finish(limits) {
                Ok(closed) => closed,
                Err((row, reason)) => return MetricsDecoded::Rejected { row, reason },
            },
            None => Closed::default(),
        };
        return MetricsDecoded::Batch {
            points: closed.points,
            rows: closed.rows,
            done: true,
        };
    };

    // `--skip-rows` by file-absolute position, before mapping sees a row.
    let (batch, file_base) = {
        let end = file_base + batch.num_rows() as u64;
        if end <= state.skip_rows {
            return MetricsDecoded::Batch {
                points: Vec::new(),
                rows: 0,
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

    let cols = match MetricsColumnIndex::resolve(&batch, mapping) {
        Ok(cols) => cols,
        Err(reason) => return MetricsDecoded::Failed(reason),
    };

    match build_batch_points(
        &batch,
        &cols,
        file_base,
        tenant,
        mapping,
        limits,
        now_ns,
        is_monotonic_sum,
        state.grouper.as_mut(),
    ) {
        Ok((points, rows)) => MetricsDecoded::Batch {
            points,
            rows,
            done: false,
        },
        Err((row, reason)) => MetricsDecoded::Rejected { row, reason },
    }
}

/// Turn one resolved Arrow batch into points, and report the SOURCE ROWS those
/// points came from.
///
/// Split out of [`decode_metrics_batch`] so the mapping-to-points half can be
/// driven directly over a hand-built `RecordBatch`, without a Parquet file and
/// a read cursor in the way. `Err` is `(file-absolute row, reason)`.
#[allow(clippy::too_many_arguments)]
fn build_batch_points(
    batch: &RecordBatch,
    cols: &MetricsColumnIndex,
    file_base: u64,
    tenant: &TenantId,
    mapping: &MetricsMapping,
    limits: &IngestLimits,
    now_ns: i64,
    is_monotonic_sum: bool,
    mut grouper: Option<&mut HistogramGrouper>,
) -> Result<(Vec<NormalizedPoint>, u64), (u64, String)> {
    let mut points = Vec::with_capacity(batch.num_rows());
    // Source rows the points above were built from. On a scalar mapping that
    // is one per row; on a histogram mapping it is the row count of the groups
    // this batch closed (see [`MetricsDecoded::Batch`]).
    let mut rows: u64 = 0;
    for row in 0..batch.num_rows() {
        let file_row = file_base + row as u64;
        let decoded = build_metric_row(batch, cols, mapping, limits, now_ns, row)
            .map_err(|reason| (file_row, reason))?;
        match grouper.as_deref_mut() {
            Some(grouper) => {
                let closed = grouper.push(decoded, file_row, limits)?;
                points.extend(closed.points);
                rows += closed.rows;
            }
            None => {
                let RowPayload::Scalar(value) = decoded.payload else {
                    return Err((
                        file_row,
                        "internal error: a scalar mapping produced a bucket row".to_string(),
                    ));
                };
                points.push(
                    metrics_point(
                        tenant,
                        &decoded.labels,
                        &decoded.name,
                        None,
                        decoded.ts_ns,
                        value,
                        is_monotonic_sum,
                        limits,
                    )
                    .map_err(|reason| (file_row, reason))?,
                );
                rows += 1;
            }
        }
    }
    Ok((points, rows))
}

/// One in-flight Strict write on a sequential load path: the source rows it
/// carries, the records it carries, and the task running it.
///
/// Shared by the metrics and spans loads, whose write windows differ only in
/// the router's receipt and error types ([`WriteAck`], [`WriteFailure`]) and
/// in which report the acks fold into ([`SequentialReport`]).
type Inflight<A, F> = (u64, u64, tokio::task::JoinHandle<Result<A, F>>);

/// A Strict write's success value, reduced to the commit tokens a load report
/// keeps.
trait WriteAck {
    fn into_tokens(self) -> Vec<CommitToken>;
}

impl WriteAck for WriteReceipt {
    fn into_tokens(self) -> Vec<CommitToken> {
        self.tokens
    }
}

impl WriteAck for SpanWriteReceipt {
    fn into_tokens(self) -> Vec<CommitToken> {
        self.tokens
    }
}

/// A Strict write's failure, reduced to its message and to whatever sibling
/// shards the router recovered from a partial write. Deliberately not named
/// `durable_tokens`: both concrete error types already have an inherent method
/// of that name, and an inherent method shadows a trait one at every call
/// site, which would make this trait's impls silently recursive.
trait WriteFailure: std::fmt::Display {
    fn recovered_tokens(&self) -> &[CommitToken];
}

impl WriteFailure for WriteError {
    fn recovered_tokens(&self) -> &[CommitToken] {
        self.durable_tokens()
    }
}

impl WriteFailure for SpanWriteError {
    fn recovered_tokens(&self) -> &[CommitToken] {
        self.durable_tokens()
    }
}

/// The load-report surface the shared in-flight window writes through.
trait SequentialReport {
    /// Fold one acked write's tokens and counts into the report.
    fn record_ack(&mut self, rows: u64, records: u64, tokens: Vec<CommitToken>);
    /// Tokens known durable so far, in submission order.
    fn tokens(&self) -> &[CommitToken];
    /// The two figures a failed load hands back.
    fn resume(&self) -> ResumeFigures;
}

impl SequentialReport for MetricsLoadReport {
    fn record_ack(&mut self, rows: u64, records: u64, tokens: Vec<CommitToken>) {
        self.tokens.extend(tokens);
        self.rows_processed += rows;
        self.points_written += records;
    }

    fn tokens(&self) -> &[CommitToken] {
        &self.tokens
    }

    fn resume(&self) -> ResumeFigures {
        ResumeFigures {
            rows_skipped: self.rows_skipped,
            rows_written: self.rows_processed,
        }
    }
}

impl SequentialReport for SpansLoadReport {
    fn record_ack(&mut self, rows: u64, _records: u64, tokens: Vec<CommitToken>) {
        self.tokens.extend(tokens);
        self.rows_processed += rows;
    }

    fn tokens(&self) -> &[CommitToken] {
        &self.tokens
    }

    fn resume(&self) -> ResumeFigures {
        ResumeFigures {
            rows_skipped: self.rows_skipped,
            rows_written: self.rows_processed,
        }
    }
}

/// Bulk-import `parquet_path` into `tenant`'s metrics signal (ADR-1751
/// decision 1).
///
/// The same contract as [`load`] on the logs side: the shard count is
/// validated against (or, for a fresh signal, written to) the durable
/// provisioning record through [`validate_or_adopt`]; the router is built
/// from the same [`build_ingest_config`]; every batch is a
/// [`WriteMode::Strict`] write whose ack means durable; and a failure
/// mid-file is a genuine partial load whose durable commit tokens are
/// reported rather than swallowed.
///
/// `now_ns` anchors the future-skew check only. Bucketing is by the router's
/// own clock (load-time wall clock), so a thirty-day-old sample lands in
/// today's ingest hour and is reached by every later query's listing window,
/// whose upper bound is `now` (ADR-1751 Context, ADR-0089's discoverability
/// argument).
#[allow(clippy::too_many_arguments)]
pub async fn load_metrics(
    store: Arc<dyn ObjectStoreBackend>,
    parquet_path: &Path,
    tenant: &str,
    mapping: &MetricsMapping,
    shards: u32,
    batch_rows: usize,
    skip_rows: u64,
    pipeline_depth: usize,
    max_inflight_flushes: u32,
    target_bytes: usize,
    max_flush_delay: Option<Duration>,
    now_ns: i64,
    clock: Arc<dyn Clock>,
) -> Result<MetricsLoadReport, LoadError> {
    // Same operator-facing lever guards as the logs path: a value that cannot
    // express what the flag means is rejected, never silently clamped.
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
    mapping.validate()?;

    let limits = IngestLimits::default();
    let tenant_id = TenantId::new(tenant);

    // Provision or validate the METRICS signal, the same first-touch path
    // `services/ravel-server` runs and the same one the logs load takes for
    // `Signal::Logs`.
    validate_or_adopt(
        store.as_ref(),
        &tenant_id.hash(),
        Signal::Metrics,
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

    let router = Arc::new(IngestRouter::new(
        build_ingest_config(shards, target_bytes, max_inflight_flushes, max_flush_delay),
        Arc::clone(&store),
        Signal::Metrics,
        clock,
    ));
    let ack_deadline = write_ack_deadline(max_flush_delay);

    let input = FileInput { path: parquet_path };
    let metadata = read_input_metadata(&input)?;
    let row_group_lens = row_group_row_counts(&metadata);
    // One sequential cursor, not the logs path's K stride cursors: a classic
    // histogram's data point is a CONTIGUOUS run of rows, and interleaving
    // far-apart file regions inside one batch would split every such run.
    let mut cursors = open_stride_cursors(&input, &metadata, &row_group_lens, 1, batch_rows)?;
    let Some(cursor) = cursors.pop() else {
        return Err(LoadError::Setup(
            "internal error: no read cursor was opened for the input file".to_string(),
        ));
    };

    let started = Instant::now();
    let mut report = MetricsLoadReport::default();
    let total_rows: u64 = row_group_lens.iter().sum();
    report.rows_skipped = skip_rows.min(total_rows);
    report.skip_rows_requested = skip_rows;
    report.file_total_rows = total_rows;

    let is_monotonic_sum = mapping.kind == Some(MetricKindArg::Counter);
    let mut state = MetricsDecodeState {
        cursor,
        grouper: mapping
            .is_histogram()
            .then(|| HistogramGrouper::new(tenant_id.clone())),
        skip_rows,
    };
    let mapping = Arc::new(mapping.clone());

    let mut inflight: std::collections::VecDeque<Inflight<WriteReceipt, WriteError>> =
        std::collections::VecDeque::with_capacity(pipeline_depth);

    loop {
        let tenant_for_decode = tenant_id.clone();
        let mapping_for_decode = Arc::clone(&mapping);
        let limits_for_decode = limits.clone();
        let (returned, decoded) = tokio::task::spawn_blocking(move || {
            let mut state = state;
            let outcome = decode_metrics_batch(
                &mut state,
                &tenant_for_decode,
                &mapping_for_decode,
                &limits_for_decode,
                now_ns,
                batch_rows,
                is_monotonic_sum,
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

        let (points, rows, done) = match decoded {
            MetricsDecoded::Failed(reason) => {
                let (durable, reason) =
                    drain_sequential_before_refusal(&mut inflight, &mut report, reason).await;
                return Err(LoadError::BatchFailed {
                    reason,
                    durable,
                    resume: report.resume(),
                });
            }
            MetricsDecoded::Rejected { row, reason } => {
                let (durable, reason) =
                    drain_sequential_before_refusal(&mut inflight, &mut report, reason).await;
                return Err(LoadError::RowRejected {
                    row,
                    reason,
                    durable,
                    resume: report.resume(),
                });
            }
            MetricsDecoded::Batch { points, rows, done } => (points, rows, done),
        };

        // No points means no group closed (or every row was skipped), so this
        // batch's rows belong to a group still open and are credited to the
        // later write that carries it, not to any write here.
        if points.is_empty() {
            if done {
                break;
            }
            continue;
        }

        let batch_points = points.len() as u64;
        let batch_rows_written = rows;
        let handle = {
            let router = Arc::clone(&router);
            let tenant = tenant_id.clone();
            tokio::spawn(async move {
                router
                    .write(tenant, points, WriteMode::Strict, ack_deadline)
                    .await
            })
        };
        inflight.push_back((batch_rows_written, batch_points, handle));

        // Bound true concurrency to `pipeline_depth`, resolving strictly
        // oldest-first so the token list and the first error stay in
        // submission order however the underlying PUTs complete.
        while inflight.len() >= pipeline_depth {
            let Some(entry) = inflight.pop_front() else {
                break;
            };
            if let Err(mut e) = resolve_sequential_write(entry, &mut report).await {
                harvest_sequential_after_failure(&mut inflight, &mut e).await;
                return Err(e);
            }
        }

        if done {
            break;
        }
    }

    // Publish the tail buffers before draining, for the same reason the logs
    // path does: no later batch is coming to push a buffer past
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
        let result = drain_sequential_inflight(&mut inflight, &mut report).await;
        let _ = stop_tx.send(());
        let _ = ticker.await;
        result
    };
    drain_result?;

    report.histogram_points_exploded = state.grouper.as_ref().map_or(0, |g| g.exploded);
    report.elapsed = started.elapsed();
    Ok(report)
}

/// Resolve one in-flight write, folding its tokens and counts into the report
/// or turning its failure into a [`LoadError::Flush`] that carries the tokens
/// already durable, including any sibling shard the router recovered from a
/// partial write.
async fn resolve_sequential_write<A: WriteAck, F: WriteFailure, P: SequentialReport>(
    entry: Inflight<A, F>,
    report: &mut P,
) -> Result<(), LoadError> {
    let (rows, records, handle) = entry;
    match handle.await {
        Ok(Ok(receipt)) => {
            report.record_ack(rows, records, receipt.into_tokens());
            Ok(())
        }
        Ok(Err(err)) => {
            let mut durable = report.tokens().to_vec();
            durable.extend_from_slice(err.recovered_tokens());
            Err(LoadError::Flush {
                durable,
                cause: err.to_string(),
                resume: report.resume(),
            })
        }
        Err(join_err) => Err(LoadError::Flush {
            durable: report.tokens().to_vec(),
            cause: format!("write task failed: {join_err}"),
            resume: report.resume(),
        }),
    }
}

/// Resolve every remaining in-flight write, oldest-first. On the first write
/// error every later write is still resolved and whatever it committed is
/// folded into that error's durable-token list
/// ([`harvest_sequential_after_failure`]), as the steady-state loop does.
async fn drain_sequential_inflight<A: WriteAck, F: WriteFailure, P: SequentialReport>(
    inflight: &mut std::collections::VecDeque<Inflight<A, F>>,
    report: &mut P,
) -> Result<(), LoadError> {
    while let Some(entry) = inflight.pop_front() {
        if let Err(mut e) = resolve_sequential_write(entry, report).await {
            harvest_sequential_after_failure(inflight, &mut e).await;
            return Err(e);
        }
    }
    Ok(())
}

/// Drain the window ahead of a decode refusal, returning the durable-token
/// list the refusal reports and its reason. A write failure found by the drain
/// does not replace the refusal: its tokens (including any it harvested from
/// later writes) become the refusal's durable list and its cause is appended
/// to the reason, since the resume figures then stop at that write rather than
/// at the refused row.
async fn drain_sequential_before_refusal<A: WriteAck, F: WriteFailure, P: SequentialReport>(
    inflight: &mut std::collections::VecDeque<Inflight<A, F>>,
    report: &mut P,
    reason: String,
) -> (Vec<CommitToken>, String) {
    match drain_sequential_inflight(inflight, report).await {
        Ok(()) => (report.tokens().to_vec(), reason),
        Err(e) => (
            e.durable_tokens().to_vec(),
            format!("{reason} (an earlier write had also failed: {e})"),
        ),
    }
}

/// Fold whatever the still-outstanding writes committed into an error's
/// durable-token list. The loader cannot stop a shard-actor flush it already
/// handed off, so awaiting the outcome is what keeps the report equal to what
/// landed (issue #800's reasoning on the logs path).
async fn harvest_sequential_after_failure<A: WriteAck, F: WriteFailure>(
    inflight: &mut std::collections::VecDeque<Inflight<A, F>>,
    err: &mut LoadError,
) {
    let mut recovered: Vec<CommitToken> = Vec::new();
    while let Some((_, _, handle)) = inflight.pop_front() {
        match handle.await {
            Ok(Ok(receipt)) => recovered.extend(receipt.into_tokens()),
            Ok(Err(write_err)) => recovered.extend_from_slice(write_err.recovered_tokens()),
            Err(_) => {}
        }
    }
    if let Some(durable) = err.durable_tokens_mut() {
        durable.extend(recovered);
    }
}

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
    /// Attribute values dropped for being over the OTLP value-length cap. The
    /// span itself is kept, so a nonzero count means the stored record is an
    /// approximation of the source row and nothing else in the load says so.
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
struct SpansColumnIndex {
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
    /// The batch's columns with every mapped dictionary column resolved once.
    columns: ResolvedColumns,
}

impl SpansColumnIndex {
    /// Resolve every mapped column against this batch's schema, and check the
    /// id columns' types and declared widths here rather than per row: a
    /// `FixedSizeBinary(n)` states its width in the schema, so a mapping that
    /// points `trace_id_column` at an 8-byte column is a mapping error that
    /// can be reported before the first row is decoded.
    ///
    /// Each mapped dictionary column is resolved to its value type here too,
    /// once for the whole batch rather than once per cell
    /// ([`resolve_dictionary_column`]).
    fn resolve(batch: &RecordBatch, mapping: &SpansMapping) -> Result<SpansColumnIndex, String> {
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
            columns,
        })
    }

    /// Column `i` as the row readers must read it: resolved when it was a
    /// mapped dictionary column, the batch's own otherwise.
    fn col<'a>(&'a self, batch: &'a RecordBatch, i: usize) -> &'a ArrayRef {
        self.columns.col(batch, i)
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
    let name = read_string(cols.col(batch, cols.name), row)?
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
    let trace_id = read_id::<16>(cols.col(batch, cols.trace_id), row)?.ok_or_else(|| {
        format!(
            "trace_id column {:?} is null, or is not a 16-byte value (or a 32-character hex \
             string). Ravel never pads or truncates an id.",
            mapping.trace_id_column
        )
    })?;
    let span_id = read_id::<8>(cols.col(batch, cols.span_id), row)?.ok_or_else(|| {
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
            let column = cols.col(batch, i);
            if id_cell_is_empty(column, row)? {
                None
            } else {
                Some(read_id::<8>(column, row)?.ok_or_else(|| {
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
    let start_ts_ns = match read_ts(cols.col(batch, cols.start_ts), row, mapping.start_ts_unit)?
        .ok_or_else(|| format!("start_ts column {:?} is null", mapping.start_ts_column))?
    {
        0 => now_ns,
        v => v,
    };
    let end_cell_ns = read_ts(cols.col(batch, cols.end_ts), row, mapping.end_ts_unit)?
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
        let start_unit = ts_read_unit(
            cols.col(batch, cols.start_ts).data_type(),
            mapping.start_ts_unit,
            "start_ts_unit",
        );
        // A substituted end was never read from the end column, so naming
        // that column's unit would send the operator looking for a value it
        // does not hold.
        let end_unit = if end_cell_ns == 0 {
            "taken from start_ts because end_ts is 0".to_string()
        } else {
            ts_read_unit(
                cols.col(batch, cols.end_ts).data_type(),
                mapping.end_ts_unit,
                "end_ts_unit",
            )
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
        Some(i) => read_status_code(cols.col(batch, i), row)?,
    };
    let status_message = match cols.status_message {
        None => None,
        Some(i) => match read_string(cols.col(batch, i), row)? {
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
    let span_attrs = read_span_attrs(
        batch,
        cols,
        &cols.attributes,
        &mapping.attributes,
        limits,
        row,
        dropped,
    )?;
    // The same merge the OTLP path runs, with an empty scope set: this loader
    // maps no instrumentation scope, so there is nothing between resource and
    // span precedence. The reserved-key strip `normalize_span` applies is not
    // repeated here because `SpansMapping::validate` refuses a mapping naming
    // any reserved key outright, so no merged map can hold one.
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
        let Some(value) = read_attr(cols.col(batch, *col), row, map.value_type)? else {
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
fn span_attr_string(key: &str, value: &AttrValue) -> Result<String, String> {
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

    let cols = match SpansColumnIndex::resolve(&batch, mapping) {
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
async fn load_spans_into(
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

    let mut inflight: std::collections::VecDeque<Inflight<SpanWriteReceipt, SpanWriteError>> =
        std::collections::VecDeque::with_capacity(pipeline_depth);

    loop {
        let mapping_for_decode = Arc::clone(&mapping);
        let limits_for_decode = limits.clone();
        let (returned, decoded) = tokio::task::spawn_blocking(move || {
            let mut state = state;
            let outcome = decode_spans_batch(
                &mut state,
                &mapping_for_decode,
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
            attrs_map_column: None,
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
            resume: ResumeFigures::default(),
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
        /// Clock reads and sleep registrations since construction. Every shard
        /// actor loop iteration, every write the actor handles, and every flush
        /// task reads this clock, so the counter standing still means no task
        /// in the router is runnable: the progress signal
        /// [`yield_until_router_is_quiet`] samples.
        reads: std::sync::atomic::AtomicU64,
        wake_tx: tokio::sync::watch::Sender<()>,
    }

    impl TestClock {
        fn new(start_ns: i64) -> std::sync::Arc<Self> {
            let (wake_tx, _rx) = tokio::sync::watch::channel(());
            std::sync::Arc::new(TestClock {
                now_ns: std::sync::atomic::AtomicI64::new(start_ns),
                reads: std::sync::atomic::AtomicU64::new(0),
                wake_tx,
            })
        }

        fn advance_ns(&self, delta_ns: i64) {
            self.now_ns
                .fetch_add(delta_ns, std::sync::atomic::Ordering::SeqCst);
            let _ = self.wake_tx.send(());
        }

        fn reads(&self) -> u64 {
            self.reads.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    impl Clock for TestClock {
        fn now_ns(&self) -> i64 {
            self.reads.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
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

    /// Consecutive scheduler rounds [`yield_until_router_is_quiet`] needs to see
    /// the clock unread before it calls the router quiet. A routed write reaches
    /// its shard buffer within a handful of rounds, so this is generous rather
    /// than tuned; it costs nothing, because a round is one `yield_now` and no
    /// wall-clock wait.
    const QUIET_ROUNDS: usize = 64;

    /// Nanoseconds [`load_with_released_tail`] advances the injected clock by to
    /// release a load's tail. Above the router's 2s default `max_flush_delay`,
    /// so a buffer no size trigger can reach ages out; far below the 60s Strict
    /// ack deadline ([`WRITE_ACK_DEADLINE_FLOOR`]), so no in-flight write's
    /// deadline can fire on the same advance.
    const TAIL_RELEASE_ADVANCE_NS: i64 = 5 * 1_000_000_000;

    /// Yields to the runtime until the router has stopped making progress, so
    /// the caller can advance the clock knowing every dispatched write is
    /// already in its shard buffer.
    ///
    /// Quiescence is read off `clock.reads()`: with the clock frozen no timer
    /// can fire, so once nothing reads it for [`QUIET_ROUNDS`] consecutive
    /// rounds, no task in the router is runnable. Nothing here waits on wall
    /// time.
    async fn yield_until_router_is_quiet(clock: &TestClock) {
        let mut last = clock.reads();
        let mut still = 0usize;
        while still < QUIET_ROUNDS {
            tokio::task::yield_now().await;
            let reads = clock.reads();
            if reads == last {
                still += 1;
            } else {
                last = reads;
                still = 0;
            }
        }
    }

    /// Run one load whose object count is a function of `target_bytes` and the
    /// input geometry alone, with no wall-clock input at all.
    ///
    /// The router gets a frozen [`TestClock`], so neither the age trigger nor
    /// the drain-time re-flush can fire on their own. Two things then have to be
    /// arranged by hand, and both are what makes the count exact:
    ///
    /// - The end-of-input `flush_all` must not run while a write is still on its
    ///   way to a shard, or that write lands in a fresh buffer afterwards and
    ///   gets an object of its own. The last batch's `on_batch_queued` hook
    ///   blocks the decoder thread, so no `Done` can reach the loader until this
    ///   driver releases it.
    /// - The tail still has to be published. Once the router is quiet -- every
    ///   batch routed and every size-triggered flush finished -- the clock is
    ///   advanced past `max_flush_delay` exactly once, which ages out whatever
    ///   no size trigger could reach, one flush per shard.
    ///
    /// `pipeline_depth` is one wider than the batch count so the loader never
    /// parks mid-input on an ack: an ack the size trigger cannot answer would
    /// otherwise need the age trigger to fire before the input is exhausted,
    /// which is the layout decision the driver is here to make.
    #[allow(clippy::too_many_arguments)]
    async fn load_with_released_tail(
        store: Arc<dyn ObjectStoreBackend>,
        pq: &Path,
        m: &Mapping,
        shards: u32,
        batch_rows: usize,
        read_cursors: usize,
        batches: usize,
        target_bytes: usize,
    ) -> LoadReport {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let clock = TestClock::new(NOW_NS);
        let queued = Arc::new(AtomicUsize::new(0));
        let (gate_tx, mut gate_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let release_rx = std::sync::Mutex::new(release_rx);
        let on_batch_queued: BuildStartHook = Arc::new(move || {
            if queued.fetch_add(1, Ordering::SeqCst) + 1 == batches {
                let _ = gate_tx.send(());
                let guard = release_rx
                    .lock()
                    .expect("the release channel is not poisoned");
                let _ = guard.recv();
            }
        });

        let load_fut = load_instrumented(
            store,
            pq,
            "acme",
            m,
            shards,
            batch_rows,
            0,
            Some(read_cursors),
            batches + 1,
            DEFAULT_MAX_INFLIGHT_FLUSHES,
            DEFAULT_DECODE_QUEUE_BATCHES,
            target_bytes,
            None,
            NOW_NS,
            Arc::clone(&clock) as Arc<dyn Clock>,
            LoadPath::Columnar,
            None,
            Some(on_batch_queued),
        );

        // The driver never resolves, so the load future is what ends the
        // `select!`; both are polled on every round, which is what lets the
        // driver's yields hand the runtime to the writes still in flight.
        let driver = async {
            let () = gate_rx.recv().await.expect("the last batch is queued");
            yield_until_router_is_quiet(&clock).await;
            clock.advance_ns(TAIL_RELEASE_ADVANCE_NS);
            yield_until_router_is_quiet(&clock).await;
            // Releasing the decoder lets `Done` through, and with it the
            // end-of-input flush and the drain; both find empty buffers.
            drop(release_tx);
            std::future::pending::<()>().await
        };

        tokio::select! {
            report = load_fut => report.expect("load succeeds"),
            () = driver => unreachable!("the driver parks once the tail is released"),
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
    /// pipeline, not a subset: admit, route, merge, encode, and bloom all
    /// recorded at least one sample. Drop `#[cfg(feature = "stage-timing")]`
    /// from `LogIngestRouter::stage_timings` (or from any one stage boundary,
    /// including the `LogStage::Bloom` recording added for issue #1516) and
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
                ravel_ingest::LogStage::Bloom,
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

    /// `bloom` is nested inside `encode`'s timing window, not a disjoint fifth
    /// stage (ADR-0104 decision 1): its printed row must say so, or an
    /// operator summing the `stage timings` table's `total_ms` column
    /// double-counts it against `encode`. This pins the exact rendered name
    /// for every stage, not a substring, so a later rename of the marker text
    /// cannot quietly drop it.
    #[cfg(feature = "stage-timing")]
    #[test]
    fn stage_display_name_marks_bloom_as_nested_in_encode() {
        assert_eq!(stage_display_name(ravel_ingest::LogStage::Admit), "admit");
        assert_eq!(stage_display_name(ravel_ingest::LogStage::Route), "route");
        assert_eq!(stage_display_name(ravel_ingest::LogStage::Merge), "merge");
        assert_eq!(stage_display_name(ravel_ingest::LogStage::Encode), "encode");
        assert_eq!(
            stage_display_name(ravel_ingest::LogStage::Bloom),
            "bloom (in encode)"
        );
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
            SignalArg::Logs,
            4,
            10_000,
            0,
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
            0,
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
            0,
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
            skip_rows: 0,
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
                    0,
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
            0,
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
            LoadError::Flush { durable, cause, .. } => {
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
            0,
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
    /// flushes exactly once (released by [`load_with_released_tail`], which
    /// advances the injected clock past the age trigger once every batch has
    /// been routed): 4 objects, one per shard. Same 16 rows, same decoded
    /// records.
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
                let report = load_with_released_tail(
                    Arc::clone(&store),
                    &pq,
                    &m,
                    shards,
                    rows_per_group,
                    shards as usize,
                    rows_per_group,
                    target_bytes,
                )
                .await;
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
    ///   released by the tail advance. 4 objects, one per shard.
    ///
    /// Every regime runs through [`load_with_released_tail`], so the four counts
    /// are a function of `target_bytes` and the geometry alone: the router's
    /// clock is frozen, and the test advances it past `max_flush_delay` once,
    /// after every batch has been routed, to release the tail (issue #1111).
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
                let report = load_with_released_tail(
                    Arc::clone(&store),
                    &pq,
                    &m,
                    shards,
                    batch_rows,
                    shards as usize,
                    batches,
                    target_bytes,
                )
                .await;
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
        // Which trigger opened each object, not only how many there were: the
        // flake this test used to carry (issue #1111) showed up here first, as
        // size 4 / final 8 on the mid regime, before it showed up as 12 objects.
        for (target, report, expected) in [
            (
                DEFAULT_TARGET_BYTES,
                &default,
                FlushMixCounts {
                    size: 16,
                    age: 0,
                    final_drain: 0,
                },
            ),
            (
                4096,
                &below,
                FlushMixCounts {
                    size: 16,
                    age: 0,
                    final_drain: 0,
                },
            ),
            (
                24_576,
                &mid,
                FlushMixCounts {
                    size: 8,
                    age: 0,
                    final_drain: 0,
                },
            ),
            (
                1024 * 1024,
                &whole,
                FlushMixCounts {
                    size: 0,
                    age: 4,
                    final_drain: 0,
                },
            ),
        ] {
            assert_eq!(
                report.flush_mix_report().totals,
                expected,
                "target {target}: every object below the target's reach is opened by the tail \
                 advance and every one at or above it by the size trigger; nothing is left for \
                 the end-of-input drain to sweep"
            );
        }
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
                    SignalArg::Logs,
                    shards,
                    16,
                    0,
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
    /// Pinned without a timing band: under a [`TestClock`] this test never
    /// advances, the age trigger can never fire (the shard actor's flush tick
    /// waits on that clock's own `sleep`, which only returns on an advance),
    /// and at an 8 MiB target no write in this 16-row fixture can reach the
    /// size trigger either. With no trigger reachable, the load cannot finish
    /// and -- the part that would be false under the old semantics -- the
    /// store holds zero data objects while it is suspended. The store is read
    /// with the load future still alive and pinned, so no shutdown flush from
    /// dropping the router can race the observation.
    ///
    /// The suspension is observed off the pipeline's own progress, not off a
    /// wall-clock window and not off a yield count: the decoder's
    /// `on_batch_queued` hook gates on every batch having been queued (that
    /// hook runs on the `spawn_blocking` decode thread, so no filesystem read
    /// of the fixture can still be outstanding), and
    /// [`yield_until_router_is_quiet`] then waits for `clock.reads()` to
    /// settle (so no routed write can still be moving toward its shard
    /// buffer). A yield count would bound neither, since yields on this task
    /// do not wait on the blocking decode thread.
    ///
    /// Prove-the-test: pass `1` as `target_bytes` to the
    /// `build_ingest_config` call in `load_instrumented` (that function has
    /// other callers, so change the call, not the function) and every write
    /// triggers its own flush, so the
    /// acks are answered, the load runs to completion while the driver is
    /// still waiting for the router to go quiet, and the `biased` select hits
    /// the `panic!` arm ("the load must not complete...").
    #[tokio::test]
    async fn a_strict_ack_above_target_one_waits_for_a_later_batchs_flush() {
        use ravel_object_store::memory::MemoryStore;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let shards = 4u32;
        let rows_per_group = 4usize;
        // The fixture holds one row group of `rows_per_group` rows per shard and
        // `--batch-rows` is `rows_per_group`, so the decoder queues exactly
        // `shards` batches of `rows_per_group` rows each.
        let batches = shards as usize;
        let (_dir, pq, m) = sorted_by_shard_fixture(shards, rows_per_group);
        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let clock = TestClock::new(NOW_NS);

        let queued = Arc::new(AtomicUsize::new(0));
        let (gate_tx, mut gate_rx) = tokio::sync::mpsc::unbounded_channel::<()>();
        let on_batch_queued: BuildStartHook = Arc::new(move || {
            if queued.fetch_add(1, Ordering::SeqCst) + 1 == batches {
                let _ = gate_tx.send(());
            }
        });

        let load_fut = load_instrumented(
            Arc::clone(&store),
            &pq,
            "acme",
            &m,
            shards,
            rows_per_group,
            0,
            Some(shards as usize),
            rows_per_group,
            DEFAULT_MAX_INFLIGHT_FLUSHES,
            DEFAULT_DECODE_QUEUE_BATCHES,
            8 * 1024 * 1024,
            None,
            NOW_NS,
            Arc::clone(&clock) as Arc<dyn Clock>,
            LoadPath::Columnar,
            None,
            Some(on_batch_queued),
        );
        tokio::pin!(load_fut);

        // Wait for the pipeline to run out of work, then confirm the load is
        // still parked and nothing was made durable. `biased` polls the load
        // first on every round, so a load that does complete trips the panic
        // rather than losing the race to the driver.
        let stored = tokio::select! {
            biased;
            _ = &mut load_fut => panic!(
                "the load must not complete: at an 8 MiB target no write reaches the size \
                 trigger, and a TestClock this test never advances cannot fire the age \
                 trigger, so no ack can be answered"
            ),
            () = async {
                let () = gate_rx.recv().await.expect("every batch is queued");
                yield_until_router_is_quiet(&clock).await;
            } => {
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
            0,
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
    /// Pacing comes only from the injected [`TestClock`], modelled on
    /// [`load_with_released_tail`]: the decoder is held on a two-phase gate
    /// (`on_batch_queued` reports each batch's ordinal on `gate_tx`, then
    /// blocks on `release_rx.recv()`) so batch 1's write reaches the shard
    /// buffer alone, the driver advances the clock by a single `ADVANCE_NS`
    /// (5s) once the router has gone quiet, then releases the decoder for
    /// batch 2. Quiescence is read off [`TestClock::reads`] via
    /// [`yield_until_router_is_quiet`]; there is no wall-clock sleep, poll, or
    /// timeout anywhere in this path.
    ///
    /// 5s clears `SHORT`'s 1s delay (so the first buffer always ages out on
    /// that side) while staying far under `LONG`'s 3600s delay, under
    /// `max_flush_lifetime`'s 3600s default, and unreachable by the real-time
    /// 60s Strict ack deadline, so exactly one deadline can come due on the
    /// jump.
    ///
    /// The bounded wait for a published object before the second quiesce
    /// proves different things on each side, which is why both sides share
    /// this one helper instead of diverging: on `LONG` the only object that
    /// can exist there is write 2's size flush, so the wait pins that write 2
    /// reached the shard buffer before `Done` and the end-of-input
    /// `flush_all` could split it. On `SHORT` the aged object from the
    /// advance already satisfies it; a `SHORT`-side write 2 published as a
    /// straggler and one published as an ordered tail both count as the same
    /// `final_drain` flush, so that side's expected layout does not depend on
    /// the ordering.
    async fn load_two_writes_across_one_clock_advance(
        store: Arc<dyn ObjectStoreBackend>,
        pq: &Path,
        m: &Mapping,
        max_flush_delay: Duration,
    ) -> LoadReport {
        use std::sync::atomic::{AtomicUsize, Ordering};

        // TARGET between one 4.1 KB slice and two, with margin on both sides,
        // so one write never reaches it and two always do.
        const TARGET: usize = 6_000;
        const ADVANCE_NS: i64 = 5 * 1_000_000_000;

        let clock = TestClock::new(NOW_NS);
        let probe: Arc<dyn ObjectStoreBackend> = Arc::clone(&store);

        let ordinal = Arc::new(AtomicUsize::new(0));
        let (gate_tx, mut gate_rx) = tokio::sync::mpsc::unbounded_channel::<usize>();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let release_rx = std::sync::Mutex::new(release_rx);
        let on_batch_queued: BuildStartHook = Arc::new(move || {
            let n = ordinal.fetch_add(1, Ordering::SeqCst) + 1;
            let _ = gate_tx.send(n);
            let guard = release_rx
                .lock()
                .expect("the release channel is not poisoned");
            let _ = guard.recv();
        });

        let load_fut = load_instrumented(
            store,
            pq,
            "acme",
            m,
            1, // one shard: both writes land in the same buffer
            1, // one row per batch
            0,
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

        let driver = async {
            assert_eq!(
                gate_rx.recv().await,
                Some(1),
                "the first batch reaches the decoder before anything else runs"
            );
            yield_until_router_is_quiet(&clock).await;
            assert_eq!(
                list_data_objects(probe.as_ref()).await.len(),
                0,
                "one write alone is under TARGET and the clock has not moved: nothing can \
                 have flushed yet"
            );
            clock.advance_ns(ADVANCE_NS);
            yield_until_router_is_quiet(&clock).await;
            release_tx
                .send(())
                .expect("the hook is still waiting on its first release");
            assert_eq!(
                gate_rx.recv().await,
                Some(2),
                "the second batch reaches the decoder once released"
            );

            const MAX_SETTLE_ROUNDS: usize = 10_000;
            let mut rounds = 0;
            while list_data_objects(probe.as_ref()).await.is_empty() {
                rounds += 1;
                assert!(
                    rounds < MAX_SETTLE_ROUNDS,
                    "issue #1235: settle loop exceeded {MAX_SETTLE_ROUNDS} rounds \
                     without a published object"
                );
                tokio::task::yield_now().await;
            }
            yield_until_router_is_quiet(&clock).await;
            drop(release_tx);
            std::future::pending::<()>().await
        };

        let report = tokio::select! {
            report = load_fut => report.expect("the load completes"),
            () = driver => unreachable!("the driver parks once the decoder is released"),
        };

        assert_eq!(
            clock.now_ns(),
            NOW_NS + ADVANCE_NS,
            "the driver advances the injected clock exactly once, by exactly ADVANCE_NS"
        );

        report
    }

    /// The `--max-flush-delay` lever decides an object layout, not just a config
    /// field (issue #801, deliverable 3). Both sides run
    /// [`load_two_writes_across_one_clock_advance`]: same two rows, same
    /// `--target-bytes`, same `--pipeline-depth`, same injected clock advanced
    /// by the same pattern, only the delay flipped.
    ///
    /// - `LONG` (1h) outlasts the single 5s advance the driver ever makes, so
    ///   nothing can age out. The second write merges into the first's buffer,
    ///   pushes it past the target and flushes both as ONE object by size.
    /// - `SHORT` (1s) is shorter than the single 5s advance, so the first
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
    ///
    /// Inside the helper itself: removing its single `clock.advance_ns` call
    /// fails the helper's own post-select clock assertion on the first
    /// (coalesced) call, at `left: 1700000000000000000, right:
    /// 1700000005000000000`, before the split side ever runs; calling it
    /// twice fails the same assertion at
    /// `left: 1700000010000000000, right: 1700000005000000000`; and lowering
    /// the helper's `TARGET` to `3_000` (so write 1 flushes by size alone)
    /// fails the pre-advance zero-object assertion at `left: 1, right: 0`.
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
            0,
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
            SignalArg::Logs,
            shards,
            batch_rows,
            0,
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
            SignalArg::Logs,
            shards,
            batch_rows,
            0,
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
            SignalArg::Logs,
            shards,
            batch_rows,
            0,
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
        // writer: this one is the RLOG version 5 layout (ADR-2135).
        const RICH_OBJECT_BLAKE3: &str =
            "845645933a0c853183dfd9a6744eadbdf8d009e627e010b8d53d439bb97bbc95";
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

    /// With no id column mapped, the columnar path reads its mapped dictionary
    /// columns in place: none is flattened ahead of it (only a mapped id column
    /// is, which `the_columnar_path_resolves_each_mapped_id_column_once` pins),
    /// and a dictionary attribute still reaches the `StrColumnDict` fast path.
    /// The row path resolves the same batch's dictionary columns once each, and
    /// the two build the same batch.
    ///
    /// An all-null chunk over an empty dictionary is part of the batch, so the
    /// columnar path's per-cell answer for it (`str_src`'s all-null path) is
    /// what this load exercises too.
    #[test]
    fn the_columnar_path_resolves_no_dictionary_column_when_no_id_column_is_mapped() {
        const ROWS: usize = 64;
        let dict = |vals: Vec<&str>| -> ArrayRef {
            Arc::new(
                vals.into_iter()
                    .map(Some)
                    .collect::<DictionaryArray<Int32Type>>(),
            )
        };
        let empty_dict: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::new(
            Int32Array::from(vec![None::<i32>; ROWS]),
            Arc::new(StringArray::from(Vec::<&str>::new())),
        ));
        let ts: Vec<i64> = (0..ROWS as i64).map(|i| NOW_NS + i).collect();
        let b = batch(vec![
            ("ts", i64_col(ts)),
            ("body", dict(vec!["hello"; ROWS])),
            ("svc", dict(vec!["api"; ROWS])),
            ("cat", dict(vec!["alpha"; ROWS])),
            ("gone", empty_dict),
        ]);
        let mut m = base_mapping();
        m.body_column = Some("body".to_string());
        m.resource_attributes = vec![attr("service.name", "svc", ColType::Str)];
        m.attributes = vec![
            attr("cat", "cat", ColType::Str),
            attr("gone", "gone", ColType::Str),
        ];

        let counters = dict_counters();
        let col = build_columnar_or_panic(&b, &m);
        assert_eq!(
            counters.columns(),
            0,
            "with no id column mapped, the columnar path flattens none of the four mapped \
             dictionary columns"
        );
        assert_eq!(
            counters.cell_keys(),
            0,
            "nor resolves a dictionary key per cell"
        );
        assert_eq!(col.num_rows, ROWS, "every row is built");
        let pos = col
            .dyn_columns
            .iter()
            .position(|c| c.name == "cat")
            .expect("cat column");
        assert!(
            col_dict(&col, pos).is_some(),
            "the dictionary attribute keeps its StrColumnDict"
        );

        let counters = dict_counters();
        let matched = assert_paths_match(&b, &m);
        assert_eq!(
            matched, col,
            "the columnar build is the same on a second run"
        );
        assert_eq!(
            counters.columns(),
            4,
            "the row reference resolves each mapped dictionary column once"
        );
    }

    /// The columnar path flattens a mapped dictionary id column once per
    /// batch, ahead of the row loop, and no other mapped dictionary column:
    /// two id columns beside a dictionary body resolve exactly two columns,
    /// whatever the row count is.
    #[test]
    fn the_columnar_path_resolves_each_mapped_id_column_once() {
        const ROWS: usize = 64;
        let trace_hex = hex::encode([1u8; 16]);
        let span_hex = hex::encode([2u8; 8]);
        let dict = |vals: Vec<&str>| -> ArrayRef {
            Arc::new(
                vals.into_iter()
                    .map(Some)
                    .collect::<DictionaryArray<Int32Type>>(),
            )
        };
        let ts: Vec<i64> = (0..ROWS as i64).map(|i| NOW_NS + i).collect();
        let b = batch(vec![
            ("ts", i64_col(ts)),
            ("body", dict(vec!["hello"; ROWS])),
            ("trace_id", dict(vec![trace_hex.as_str(); ROWS])),
            ("span_id", dict(vec![span_hex.as_str(); ROWS])),
        ]);
        let mut m = base_mapping();
        m.body_column = Some("body".to_string());
        m.trace_id_column = Some("trace_id".to_string());
        m.span_id_column = Some("span_id".to_string());

        let counters = dict_counters();
        let col = build_columnar_or_panic(&b, &m);
        assert_eq!(
            counters.columns(),
            2,
            "the two id columns are resolved once each, over {ROWS} rows, and the body is not"
        );
        assert_eq!(
            counters.cell_keys(),
            0,
            "no dictionary key is resolved per cell"
        );
        assert_eq!(col.num_rows, ROWS, "every row is built");
        assert_eq!(
            assert_paths_match(&b, &m),
            col,
            "the row path builds the same batch"
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
            0,
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
                0,
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

            let started = Instant::now(); // allow-wall-clock: measures real wall for the diagnostic printed below, never asserted on; the test's claim is the exact peak-concurrency count, not any elapsed time
            let report = load_instrumented(
                store as Arc<dyn ObjectStoreBackend>,
                &pq,
                "acme",
                &m,
                shards,
                1,
                0,
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
            let wall = started.elapsed(); // allow-wall-clock: real elapsed for the printed diagnostic only; the peak-concurrency assertions below carry the whole claim
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
            let cols = ColumnIndex::locate(span, mapping).map_err(ColBuildError::Batch)?;

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

    /// The reference build refuses a negative timestamp at the same row, with
    /// the same reason, as the production build.
    #[test]
    fn the_reference_build_refuses_a_negative_timestamp_like_production() {
        let spans = vec![(batch(vec![("ts", i64_col(vec![NOW_NS, -5, NOW_NS]))]), 10)];
        let mapping = base_mapping();
        let limits = LogIngestLimits::default();
        let refusal = |result: Result<ColumnarLogBatch, ColBuildError>| match result {
            Err(ColBuildError::Row { row, reason }) => (row, reason),
            Err(ColBuildError::Batch(r)) => panic!("expected a row rejection, got batch: {r}"),
            Ok(_) => panic!("expected a row rejection, got a batch"),
        };
        let got = refusal(build_columnar_batch(&spans, &mapping, &limits, NOW_NS));
        let want = refusal(build_columnar_batch_reference(
            &spans, &mapping, &limits, NOW_NS,
        ));
        assert_eq!(
            got,
            (
                11,
                "timestamp is before the Unix epoch (-5 ns, read as ts_unit = nanos); the column \
                 holds a negative value"
                    .to_string()
            )
        );
        assert_eq!(want, got, "the reference refuses the same row the same way");
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

    /// `--skip-rows` (issue #1713): a positional resume with no idempotency
    /// marker. These tests exercise `load_instrumented` directly (rather than
    /// the public [`load`] wrapper, which hardcodes `skip_rows: 0` to avoid
    /// changing its own signature) since that is the only entry point that
    /// carries the parameter.
    mod load_skip_rows {
        use super::*;
        use ravel_object_store::memory::MemoryStore;

        /// A 10-row single-row-group fixture with a distinguishing `idx`
        /// attribute (0..n), so a landed record's identity -- not just its
        /// count -- can be checked against the source file.
        fn skip_rows_fixture(
            n: i64,
        ) -> (tempfile::TempDir, std::path::PathBuf, Mapping, RecordBatch) {
            let mut m = base_mapping();
            m.attributes = vec![attr("idx", "idx", ColType::I64)];
            let b = batch(vec![
                ("ts", i64_col(vec![NOW_NS; n as usize])),
                ("idx", i64_col((0..n).collect())),
            ]);
            let (dir, pq) = write_parquet(&b);
            (dir, pq, m, b)
        }

        /// The same fixture written as `rows / group_rows` row groups, so a load
        /// over it can open one stride cursor per row group. Row identity is the
        /// same `idx` attribute, so the landed set is still checkable against
        /// file-absolute positions.
        fn skip_rows_row_group_fixture(
            rows: i64,
            group_rows: usize,
        ) -> (tempfile::TempDir, std::path::PathBuf, Mapping, RecordBatch) {
            use parquet::arrow::ArrowWriter;
            use parquet::file::properties::WriterProperties;

            let mut m = base_mapping();
            m.attributes = vec![attr("idx", "idx", ColType::I64)];
            let b = batch(vec![
                ("ts", i64_col(vec![NOW_NS; rows as usize])),
                ("idx", i64_col((0..rows).collect())),
            ]);
            let dir = tempfile::tempdir().expect("tempdir");
            let pq = dir.path().join("skip_groups.parquet");
            let file = std::fs::File::create(&pq).expect("create parquet");
            let props = WriterProperties::builder()
                .set_max_row_group_row_count(Some(group_rows))
                .build();
            let mut w = ArrowWriter::try_new(file, b.schema(), Some(props)).expect("arrow writer");
            w.write(&b).expect("write batch");
            w.close().expect("close writer");
            (dir, pq, m, b)
        }

        /// The `idx`-attributed records for `rows` of `full`, in the form
        /// [`decoded_records`] returns, so a landed set can be compared by
        /// identity rather than by count.
        fn expected_records(
            full: &RecordBatch,
            m: &Mapping,
            rows: std::ops::Range<usize>,
        ) -> Vec<String> {
            let mut expected: Vec<String> = row_records(&full.slice(rows.start, rows.len()), m)
                .iter()
                .map(|r| format!("{:?}", to_logrecord(r)))
                .collect();
            expected.sort();
            expected
        }

        async fn run_skip_rows(
            store: Arc<dyn ObjectStoreBackend>,
            pq: &Path,
            m: &Mapping,
            skip_rows: u64,
        ) -> LoadReport {
            run_skip_rows_with(store, pq, m, skip_rows, 10, Some(1)).await
        }

        #[allow(clippy::too_many_arguments)]
        async fn run_skip_rows_with(
            store: Arc<dyn ObjectStoreBackend>,
            pq: &Path,
            m: &Mapping,
            skip_rows: u64,
            batch_rows: usize,
            read_cursors: Option<usize>,
        ) -> LoadReport {
            load_instrumented(
                store,
                pq,
                "acme",
                m,
                1,
                batch_rows,
                skip_rows,
                read_cursors,
                1,
                DEFAULT_MAX_INFLIGHT_FLUSHES,
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
            .expect("load succeeds")
        }

        /// A 10-row file loaded with `skip_rows=7` lands exactly the 3 rows at
        /// file-absolute positions 7, 8, 9 -- verified by reading the RLOG
        /// objects the router actually wrote back (via [`decoded_records`]),
        /// not merely by a row count, against an independently computed
        /// differential reference ([`row_records`] over the same rows sliced
        /// straight out of the source batch).
        ///
        /// Non-vacuity (prove-the-test): change the `cut` computation in
        /// `collect_spans` from `state.skip_rows - *file_base` to
        /// `state.skip_rows - *file_base + 1` (an off-by-one that slices one
        /// row too few off the straddling batch) and this test's exact
        /// `rows_processed == 3` assertion fails: `left: 2, right: 3`, since
        /// row 7 (file-absolute) is wrongly dropped along with 0..6.
        #[tokio::test]
        async fn skip_rows_lands_exactly_the_rows_after_the_offset() {
            let (_dir, pq, m, full) = skip_rows_fixture(10);
            let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());

            let report = run_skip_rows(Arc::clone(&store), &pq, &m, 7).await;

            assert_eq!(
                report.rows_skipped, 7,
                "skip_rows=7 against a 10-row file must report exactly 7 skipped"
            );
            assert_eq!(
                report.rows_processed, 3,
                "skip_rows=7 against a 10-row file must land exactly 3 rows"
            );

            assert_eq!(
                decoded_records(store.as_ref()).await,
                expected_records(&full, &m, 7..10),
                "the landed records must be exactly file rows 7..10, not merely 3 of them"
            );
        }

        /// A `skip_rows` at or beyond the file's total row count lands 0
        /// records and the load still exits `Ok` -- there is no error case
        /// for "skip past the end", since a positional resume of an
        /// already-fully-loaded file is exactly this shape.
        #[tokio::test]
        async fn skip_rows_beyond_the_file_lands_nothing_and_succeeds() {
            let (_dir, pq, m, _full) = skip_rows_fixture(10);
            let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());

            let report = run_skip_rows(Arc::clone(&store), &pq, &m, 1_000).await;

            assert_eq!(
                report.rows_skipped, 10,
                "rows_skipped caps at the file's total row count (10), not the requested 1000"
            );
            assert_eq!(
                report.rows_processed, 0,
                "no row survives a skip past the file's end"
            );
            assert!(
                decoded_records(store.as_ref()).await.is_empty(),
                "no RLOG object holds any record when every row is skipped"
            );

            // The clamped figure above cannot show that the REQUEST missed the
            // file, so the report carries the requested value beside the
            // file's own total and the warning is built from those two.
            assert_eq!(
                (report.skip_rows_requested, report.file_total_rows),
                (1_000, 10),
                "the unclamped request and the file's row count are both carried"
            );
            let warning =
                skip_rows_past_end_warning(report.skip_rows_requested, report.file_total_rows)
                    .expect("a request past the end of the file must warn");
            assert!(
                warning.contains("--skip-rows 1000") && warning.contains("10 rows"),
                "the warning names the requested offset and the file's row count: {warning}"
            );
        }

        /// `--skip-rows` EQUAL to the file's row count is the legitimate resume
        /// of an already-complete file, so it stays silent while a strictly
        /// larger value warns. Without this case the warning could be written
        /// as `>=` and nothing would fail, which would make every completed
        /// resume print an error-shaped line.
        ///
        /// Non-vacuity (prove-the-test): changing the predicate in
        /// `skip_rows_past_end_warning` from `>` to `>=` fails this test on the
        /// `is_none` assertion while leaving the case above passing.
        #[tokio::test]
        async fn skip_rows_exactly_at_the_end_lands_nothing_and_stays_silent() {
            let (_dir, pq, m, _full) = skip_rows_fixture(10);
            let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());

            let report = run_skip_rows(Arc::clone(&store), &pq, &m, 10).await;

            assert_eq!(
                (report.rows_skipped, report.rows_processed),
                (10, 0),
                "a skip of exactly the row count drops every row and writes none"
            );
            assert!(
                skip_rows_past_end_warning(report.skip_rows_requested, report.file_total_rows)
                    .is_none(),
                "a completed resume is not an operator error and must not warn"
            );
        }

        /// The drop `--skip-rows` performs is FILE-absolute, not per-cursor:
        /// over a 12-row file in 4 row groups read by 4 stride cursors,
        /// `skip_rows=5` lands exactly file rows 5..12 -- the same set a single
        /// sequential cursor would land -- even though the cursor whose
        /// partition straddles the offset (rows 3..6) is not the one the offset
        /// counted through. The row-group count is asserted first: with one row
        /// group `resolve_read_cursors` clamps the requested 4 cursors to 1 and
        /// the test would prove nothing about cursor count.
        ///
        /// This is the flag's own positional guarantee. It is NOT the resume
        /// guarantee: what a FAILED multi-cursor run left behind is a different
        /// question, pinned by
        /// `a_failed_load_prints_the_resume_figures_and_the_settings_precondition`.
        ///
        /// Non-vacuity (prove-the-test), both demonstrated failing:
        /// drop the `*file_base +` term from `collect_spans`'s `end` (the skip
        /// read per-span instead of file-absolute, which is what a per-cursor
        /// offset would be) and every one-row span is dropped: `and must land
        /// exactly the remaining 7 rows ... left: 0, right: 7`. Asserting
        /// `4..11` instead of `5..12` keeps the count at 7 and still fails, on
        /// `idx` 4 against 11, so the landed-set assertion is pinned by row
        /// identity and not by how many rows arrived.
        #[tokio::test]
        async fn skip_rows_is_file_absolute_across_multiple_cursors_and_row_groups() {
            let (_dir, pq, m, full) = skip_rows_row_group_fixture(12, 3);
            let metadata = read_input_metadata(&FileInput { path: &pq }).expect("read metadata");
            let groups = row_group_row_counts(&metadata).len();
            assert_eq!(
                groups, 4,
                "the fixture must hold 4 row groups, or the 4 requested cursors clamp to the row \
                 group count and the load is single-cursor after all"
            );
            assert_eq!(
                resolve_read_cursors(Some(4), 1, groups),
                4,
                "and the load must therefore open 4 stride cursors, one per row group"
            );

            let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
            // 4 rows per batch over 4 live cursors: each batch takes one row
            // from each partition, so every batch spans the whole file and the
            // offset is crossed by a cursor that is not reading from row 0.
            let report = run_skip_rows_with(Arc::clone(&store), &pq, &m, 5, 4, Some(4)).await;

            assert_eq!(
                report.rows_skipped, 5,
                "skip_rows=5 against a 12-row file must report exactly 5 skipped, whatever the \
                 cursor count"
            );
            assert_eq!(
                report.rows_processed, 7,
                "and must land exactly the remaining 7 rows"
            );
            assert_eq!(
                decoded_records(store.as_ref()).await,
                expected_records(&full, &m, 5..12),
                "the landed records must be exactly file rows 5..12: the cursor holding rows 3..6 \
                 keeps only row 5, and the cursors at rows 6..12 keep everything"
            );
        }

        /// The past-end warning reaches the operator, not just the function
        /// that builds it. The two cases above call
        /// `skip_rows_past_end_warning` directly, so deleting the `if let`
        /// that writes it in `run_warning_to` leaves them green while the
        /// clamped summary silently returns to reporting success. This file
        /// already states that rule at the admission-bypass warning: with the
        /// write inlined there, deleting the emit left every test green.
        ///
        /// Non-vacuity (prove-the-test), demonstrated failing: deleting the
        /// `skip_rows_past_end_warning` emit block in `run_warning_to` fails
        /// this test on the first assertion, against a sink carrying only the
        /// admission-bypass warning.
        #[tokio::test]
        async fn a_skip_past_the_end_warns_through_the_cli_entry_point() {
            let (dir, pq, _m, _full) = skip_rows_fixture(6);
            let mapping_path = dir.path().join("mapping.toml");
            std::fs::write(
                &mapping_path,
                "ts_column = \"ts\"\nts_unit = \"nanos\"\n\n\
                 [[attribute]]\nkey = \"idx\"\ncolumn = \"idx\"\ntype = \"i64\"\n",
            )
            .expect("write mapping");

            let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
            let mut sink: Vec<u8> = Vec::new();
            run_warning_to(
                Arc::clone(&store),
                &pq,
                "acme",
                &mapping_path,
                SignalArg::Logs,
                1,
                2,
                99,
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
            .expect("a skip past the end writes nothing and still succeeds");

            let emitted = String::from_utf8(sink).expect("warnings are utf-8");
            assert!(
                emitted.contains("--skip-rows 99") && emitted.contains("6 rows"),
                "the operator is told the requested offset and the file's row count: {emitted}"
            );
            assert!(
                decoded_records(store.as_ref()).await.is_empty(),
                "and nothing was loaded"
            );
        }

        /// A load that fails mid-file prints the two figures a resume needs
        /// (`rows_skipped` and `rows_written`), their sum as the next
        /// `--skip-rows`, and whether this run's settings make that sum mean
        /// anything -- end to end through [`run_warning_to`], the CLI's own
        /// entry point, so the mapping file, the error path and the output
        /// stream are the operator's.
        ///
        /// The run is `--read-cursors 1 --pipeline-depth 1` so the failure point
        /// is deterministic: batches are submitted and resolved one at a time,
        /// the first batch (file rows 2, 3) commits, and the scripted fault
        /// fails the second batch's data-object PUT. The same error is then
        /// asked for the multi-cursor verdict, which is the case the figures
        /// must NOT be pasted into a resume.
        ///
        /// Non-vacuity (prove-the-test), both demonstrated failing: delete the
        /// `resume_hint` emit block in `run_warning_to` and the first
        /// assertion fails against a stream carrying only the admission-bypass
        /// warning; force `sequential` in `resume_hint` to `true` (one verdict
        /// for every geometry) and the multi-cursor assertion fails against the
        /// prefix verdict.
        #[tokio::test]
        async fn a_failed_load_prints_the_resume_figures_and_the_settings_precondition() {
            use ravel_object_store::fault::{
                FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
            };

            let (dir, pq, _m, _full) = skip_rows_fixture(6);
            let mapping_path = dir.path().join("mapping.toml");
            std::fs::write(
                &mapping_path,
                "ts_column = \"ts\"\nts_unit = \"nanos\"\n\n\
                 [[attribute]]\nkey = \"idx\"\ncolumn = \"idx\"\ntype = \"i64\"\n",
            )
            .expect("write mapping");

            // The second data-object PUT into shard 0 fails permanently, so the
            // first batch is durable and the second is not.
            let plan = FaultPlan::empty().with_rule(
                Rule::new(
                    Op::Put,
                    ScriptedFault::Permanent("simulated permanent data-object PUT failure".into()),
                )
                .with_key_contains("/l0/0000/")
                .with_occurrence(Occurrence::Nth(2)),
            );
            let store = Arc::new(FaultStore::new(MemoryStore::new(), plan));

            let mut sink: Vec<u8> = Vec::new();
            let err = run_warning_to(
                store.clone() as Arc<dyn ObjectStoreBackend>,
                &pq,
                "acme",
                &mapping_path,
                SignalArg::Logs,
                1,
                2,
                2,
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
            .expect_err("the second batch's PUT fails permanently, so the load fails");
            assert_eq!(
                store.fault_count(Op::Put, FaultKind::Permanent),
                1,
                "the scripted fault must have fired, or the failure under test never happened"
            );

            let emitted = String::from_utf8(sink).expect("warnings are utf-8");
            assert!(
                emitted.contains("rows_skipped     : 2"),
                "the offset this run started from reaches the operator: {emitted}"
            );
            assert!(
                emitted.contains("rows_written     : 2"),
                "so do the rows it acked durable before failing: {emitted}"
            );
            assert!(
                emitted.contains("next --skip-rows : 4 (rows_skipped + rows_written)"),
                "and the sum, named as the flag it goes into: {emitted}"
            );
            assert!(
                emitted.contains(
                    "this run used --read-cursors 1 --pipeline-depth 1, so the rows that landed \
                     are a contiguous prefix of the file"
                ),
                "with the settings precondition stated as met for this run: {emitted}"
            );

            let load_err = err
                .downcast::<LoadError>()
                .expect("the CLI error wraps the typed load error");
            assert_eq!(
                load_err.resume_figures(),
                Some(ResumeFigures {
                    rows_skipped: 2,
                    rows_written: 2,
                }),
                "the figures are carried on the error itself, not only printed"
            );

            // The same failure under the default geometry: the figures are the
            // same numbers and are NOT an offset.
            let multi = resume_hint(&load_err, Some(4), 4).expect("a non-setup error has figures");
            assert!(
                multi.contains("next --skip-rows : 4 (rows_skipped + rows_written)")
                    && multi.contains("NOT a contiguous prefix of the file")
                    && multi.contains("--read-cursors 4 and --pipeline-depth 4"),
                "a multi-cursor, pipelined run must be told the sum is not a resume offset: \
                 {multi}"
            );
        }
    }

    /// The metrics loader's series identity against `ravel_otlp::normalize`'s,
    /// for a gauge, a monotonic counter and a classic histogram.
    ///
    /// This is the property the changelog promises and the reason the loader
    /// calls ravel-otlp's own `sanitize_metric_name`, `sanitize_label_name`
    /// and `prometheus_family_name` rather than storing what the mapping says:
    /// a metric bulk-loaded from Parquet and the same metric admitted over
    /// OTLP must be ONE series, not two. Both sides use a dotted metric name,
    /// a dotted attribute key and a unit, which is exactly what the loader
    /// used to store raw.
    ///
    /// The OTLP side is the real `normalize_metrics` entry point over real
    /// OTLP messages, not a hand-built expectation: an expectation written
    /// from the loader's own helpers would agree with any bug they share.
    mod otlp_series_identity_parity {
        use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
        use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValueVariant;
        use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue};
        use opentelemetry_proto::tonic::metrics::v1::number_data_point::Value as NumberValue;
        use opentelemetry_proto::tonic::metrics::v1::{
            AggregationTemporality, Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint,
            ResourceMetrics, ScopeMetrics, Sum, metric::Data as MetricData,
        };
        use opentelemetry_proto::tonic::resource::v1::Resource;
        use ravel_otlp::normalize_metrics;

        use super::*;

        const METRIC: &str = "http.server.duration";
        const ATTR_KEY: &str = "http.method";
        const ATTR_VALUE: &str = "GET";
        const UNIT: &str = "s";
        const EVENT_NS: i64 = NOW_NS;

        fn tenant() -> TenantId {
            TenantId::new("acme")
        }

        fn attributes() -> Vec<KeyValue> {
            vec![KeyValue {
                key: ATTR_KEY.to_string(),
                value: Some(AnyValue {
                    value: Some(AnyValueVariant::StringValue(ATTR_VALUE.to_string())),
                }),
                ..Default::default()
            }]
        }

        /// One `ExportMetricsServiceRequest` carrying `data` under the shared
        /// name and unit, through a resource with no attributes (so no `job`
        /// or `instance` label enters on either side).
        fn request(data: MetricData) -> ExportMetricsServiceRequest {
            ExportMetricsServiceRequest {
                resource_metrics: vec![ResourceMetrics {
                    resource: Some(Resource::default()),
                    scope_metrics: vec![ScopeMetrics {
                        scope: Some(InstrumentationScope::default()),
                        metrics: vec![Metric {
                            name: METRIC.to_string(),
                            unit: UNIT.to_string(),
                            data: Some(data),
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
            }
        }

        /// Every series id the OTLP path produced, sorted, with the rejections
        /// asserted empty: a normalize that rejected everything would
        /// otherwise "match" a loader that produced nothing.
        fn otlp_series_ids(data: MetricData) -> Vec<SeriesId> {
            let out = normalize_metrics(&tenant(), request(data), &IngestLimits::default(), NOW_NS);
            assert!(
                out.rejected.is_empty(),
                "the OTLP fixture must be admitted whole, got {:?}",
                out.rejected
            );
            assert!(
                !out.points.is_empty(),
                "the OTLP fixture must produce at least one point"
            );
            let mut ids: Vec<SeriesId> = out.points.iter().map(|p| p.series_id).collect();
            ids.sort_unstable();
            ids
        }

        /// The same figures through the loader: its series ids, sorted, plus
        /// the points themselves for the caller to inspect.
        fn loader_points(mapping: &MetricsMapping, batch: &RecordBatch) -> Vec<NormalizedPoint> {
            mapping.validate().expect("the mapping is valid");
            let limits = IngestLimits::default();
            let cols = MetricsColumnIndex::resolve(batch, mapping).expect("columns resolve");
            let (_, is_monotonic_sum) = mapping.metric_kind();
            let mut grouper = mapping
                .is_histogram()
                .then(|| HistogramGrouper::new(tenant()));
            let (mut points, rows) = build_batch_points(
                batch,
                &cols,
                0,
                &tenant(),
                mapping,
                &limits,
                NOW_NS,
                is_monotonic_sum,
                grouper.as_mut(),
            )
            .expect("every row is admitted");
            if let Some(grouper) = grouper.as_mut() {
                let closed = grouper.finish(&limits).expect("the last group closes");
                points.extend(closed.points);
                assert_eq!(
                    rows + closed.rows,
                    batch.num_rows() as u64,
                    "every source row is accounted to some closed group"
                );
            }
            points
        }

        fn sorted_ids(points: &[NormalizedPoint]) -> Vec<SeriesId> {
            let mut ids: Vec<SeriesId> = points.iter().map(|p| p.series_id).collect();
            ids.sort_unstable();
            ids
        }

        fn scalar_mapping(kind: Option<MetricKindArg>) -> MetricsMapping {
            MetricsMapping {
                name: Some(METRIC.to_string()),
                name_column: None,
                value_column: "value".to_string(),
                ts_column: "ts".to_string(),
                ts_unit: TsUnit::Nanos,
                unit: Some(UNIT.to_string()),
                kind,
                labels: vec![LabelMap {
                    name: ATTR_KEY.to_string(),
                    column: "method".to_string(),
                }],
                histogram: None,
            }
        }

        fn scalar_batch(value: f64) -> RecordBatch {
            batch(vec![
                ("ts", i64_col(vec![EVENT_NS])),
                (
                    "value",
                    Arc::new(Float64Array::from(vec![value])) as ArrayRef,
                ),
                ("method", str_col(vec![ATTR_VALUE])),
            ])
        }

        fn number_point() -> NumberDataPoint {
            NumberDataPoint {
                attributes: attributes(),
                time_unix_nano: EVENT_NS as u64,
                value: Some(NumberValue::AsDouble(1.5)),
                ..Default::default()
            }
        }

        /// The `__name__` label of one point, which is also the family name
        /// `SeriesId::compute` was keyed on.
        fn metric_name_of(point: &NormalizedPoint) -> String {
            point
                .labels
                .iter()
                .find(|l| l.name == METRIC_NAME_LABEL)
                .map(|l| l.value.clone())
                .expect("every point carries __name__")
        }

        fn label_names(point: &NormalizedPoint) -> Vec<String> {
            point.labels.iter().map(|l| l.name.clone()).collect()
        }

        #[test]
        fn a_gauge_lands_on_the_otlp_series_id() {
            let points = loader_points(&scalar_mapping(None), &scalar_batch(1.5));
            assert_eq!(points.len(), 1);
            assert_eq!(
                metric_name_of(&points[0]),
                "http_server_duration_seconds",
                "the dotted name is sanitized and the unit suffix applied"
            );
            assert!(
                label_names(&points[0]).contains(&"http_method".to_string()),
                "the dotted attribute key is sanitized: {:?}",
                label_names(&points[0])
            );
            assert!(
                !points[0].is_monotonic_sum,
                "a gauge is not a monotonic sum"
            );
            assert_eq!(
                sorted_ids(&points),
                otlp_series_ids(MetricData::Gauge(Gauge {
                    data_points: vec![number_point()],
                })),
                "a gauge loaded from Parquet is the same series as the same OTLP gauge"
            );
        }

        #[test]
        fn a_counter_gains_total_and_lands_on_the_otlp_series_id() {
            let points = loader_points(
                &scalar_mapping(Some(MetricKindArg::Counter)),
                &scalar_batch(1.5),
            );
            assert_eq!(points.len(), 1);
            assert_eq!(
                metric_name_of(&points[0]),
                "http_server_duration_seconds_total",
                "kind = \"counter\" adds _total exactly as a monotonic OTLP Sum does"
            );
            assert!(
                points[0].is_monotonic_sum,
                "kind = \"counter\" sets is_monotonic_sum, which a gauge leaves false"
            );
            assert_eq!(
                sorted_ids(&points),
                otlp_series_ids(MetricData::Sum(Sum {
                    data_points: vec![number_point()],
                    aggregation_temporality: AggregationTemporality::Cumulative as i32,
                    is_monotonic: true,
                })),
                "a counter loaded from Parquet is the same series as a monotonic OTLP Sum"
            );
        }

        #[test]
        fn a_classic_histogram_lands_on_the_otlp_series_ids() {
            let mapping = MetricsMapping {
                name: Some(METRIC.to_string()),
                name_column: None,
                value_column: "bucket_count".to_string(),
                ts_column: "ts".to_string(),
                ts_unit: TsUnit::Nanos,
                unit: Some(UNIT.to_string()),
                kind: None,
                labels: vec![LabelMap {
                    name: ATTR_KEY.to_string(),
                    column: "method".to_string(),
                }],
                histogram: Some(HistogramMap {
                    histogram_type: None,
                    le_column: "le".to_string(),
                    sum_column: "sum".to_string(),
                    count_column: "count".to_string(),
                }),
            };
            // Two explicit bounds, each row carrying that bucket's own count,
            // and the data point's sum and count repeated on both rows.
            let rows = batch(vec![
                ("ts", i64_col(vec![EVENT_NS, EVENT_NS])),
                (
                    "le",
                    Arc::new(Float64Array::from(vec![0.1, 1.0])) as ArrayRef,
                ),
                (
                    "bucket_count",
                    Arc::new(Float64Array::from(vec![2.0, 3.0])) as ArrayRef,
                ),
                (
                    "sum",
                    Arc::new(Float64Array::from(vec![12.5, 12.5])) as ArrayRef,
                ),
                ("count", i64_col(vec![7, 7])),
                ("method", str_col(vec![ATTR_VALUE, ATTR_VALUE])),
            ]);
            let points = loader_points(&mapping, &rows);
            assert_eq!(
                points.len(),
                5,
                "two bounds explode into 2 buckets + the +Inf bucket + _sum + _count"
            );
            for point in &points {
                assert!(
                    metric_name_of(point).starts_with("http_server_duration_seconds_"),
                    "every exploded name carries the sanitized, unit-suffixed family name: {}",
                    metric_name_of(point)
                );
                assert!(
                    !metric_name_of(point).contains("_total"),
                    "no exploded histogram series is a monotonic sum: {}",
                    metric_name_of(point)
                );
                assert!(!point.is_monotonic_sum);
            }
            // OTLP's bucket_counts is one longer than explicit_bounds: the last
            // element is the +Inf bucket's own count.
            assert_eq!(
                sorted_ids(&points),
                otlp_series_ids(MetricData::Histogram(Histogram {
                    data_points: vec![HistogramDataPoint {
                        attributes: attributes(),
                        time_unix_nano: EVENT_NS as u64,
                        count: 7,
                        sum: Some(12.5),
                        bucket_counts: vec![2, 3, 2],
                        explicit_bounds: vec![0.1, 1.0],
                        ..Default::default()
                    }],
                    aggregation_temporality: AggregationTemporality::Cumulative as i32,
                })),
                "every exploded series of a loaded histogram matches the OTLP explosion"
            );
        }

        /// The empty-label-value rule, at the identity level: a row whose
        /// label cell is empty is the same series as one whose cell is null,
        /// and both are the series OTLP produces for a data point with no
        /// such attribute at all.
        #[test]
        fn an_empty_label_value_is_dropped_like_a_missing_attribute() {
            let mapping = scalar_mapping(None);
            let empty = batch(vec![
                ("ts", i64_col(vec![EVENT_NS])),
                ("value", Arc::new(Float64Array::from(vec![1.5])) as ArrayRef),
                ("method", str_col(vec![""])),
            ]);
            let null = batch(vec![
                ("ts", i64_col(vec![EVENT_NS])),
                ("value", Arc::new(Float64Array::from(vec![1.5])) as ArrayRef),
                (
                    "method",
                    Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
                ),
            ]);
            let from_empty = loader_points(&mapping, &empty);
            let from_null = loader_points(&mapping, &null);
            assert_eq!(
                sorted_ids(&from_empty),
                sorted_ids(&from_null),
                "an empty label cell and a null one are one series, not two"
            );
            assert!(
                !label_names(&from_empty[0]).contains(&"http_method".to_string()),
                "the empty label is absent from the series: {:?}",
                label_names(&from_empty[0])
            );
            assert_eq!(
                sorted_ids(&from_empty),
                otlp_series_ids(MetricData::Gauge(Gauge {
                    data_points: vec![NumberDataPoint {
                        attributes: vec![KeyValue {
                            key: ATTR_KEY.to_string(),
                            value: Some(AnyValue {
                                value: Some(AnyValueVariant::StringValue(String::new())),
                            }),
                            ..Default::default()
                        }],
                        time_unix_nano: EVENT_NS as u64,
                        value: Some(NumberValue::AsDouble(1.5)),
                        ..Default::default()
                    }],
                })),
                "OTLP drops an empty attribute value before the label set is built"
            );
        }
    }

    /// A count column wider than `f64`'s exact integer range keeps its value.
    /// Reading it through `f64` moved it to the nearest representable value,
    /// which is a silently wrong count, not a rejection.
    #[test]
    fn a_count_above_two_to_the_53_survives_an_integer_column() {
        let big: u64 = (1u64 << 53) + 1;
        let arr: ArrayRef = Arc::new(UInt64Array::from(vec![big]));
        assert_eq!(
            read_count(&arr, 0).expect("a u64 column is a valid count column"),
            Some(big),
            "the count must not round through f64"
        );
    }

    /// A float count of exactly 2^64 is one past `u64::MAX` and used to pass
    /// the `> u64::MAX as f64` test (that cast rounds UP to 2^64), then
    /// saturate to `u64::MAX` on the way in.
    #[test]
    fn a_float_count_of_exactly_two_to_the_64_is_refused() {
        let two_to_64 = 18_446_744_073_709_551_616.0f64;
        let err = exact_count(two_to_64).expect_err("2^64 does not fit in u64");
        assert!(
            err.contains("fits in u64"),
            "the refusal says what is wrong: {err}"
        );
        // One representable step below still passes, so the bound is at 2^64
        // and not merely "large floats are refused".
        let below = 18_446_744_073_709_549_568.0f64;
        assert!(below < two_to_64);
        assert_eq!(exact_count(below).expect("below 2^64 fits"), below as u64);
    }

    /// `ravel-cli load --signal spans` (ADR-1751 follow-up task 2). The
    /// end-to-end round trip and the OTLP differential live in
    /// `tests/load_spans.rs`; these cover what needs the crate-internal entry
    /// point or a mapping that never reaches a router.
    mod spans {
        use super::*;

        /// The smallest legal spans mapping plus one attribute of each kind.
        const MAPPING_TOML: &str = r#"
[spans]
trace_id_column = "trace_id"
span_id_column  = "span_id"
name_column     = "name"
start_ts_column = "start_ns"
start_ts_unit   = "nanos"
end_ts_column   = "end_ns"
end_ts_unit     = "nanos"

[[spans.attribute]]
key = "http.method"
column = "method"
type = "str"
"#;

        fn bin_col(vals: Vec<Vec<u8>>) -> ArrayRef {
            let refs: Vec<&[u8]> = vals.iter().map(|v| v.as_slice()).collect();
            Arc::new(BinaryArray::from(refs))
        }

        /// One span's Parquet file and mapping file on disk, for a test that
        /// drives the CLI entry point rather than [`load_spans`].
        fn fixture() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
            let dir = tempfile::tempdir().expect("tempdir");
            let pq = dir.path().join("spans.parquet");
            let mapping_path = dir.path().join("mapping.toml");
            let batch = batch(vec![
                ("trace_id", bin_col(vec![vec![1u8; 16]])),
                ("span_id", bin_col(vec![vec![2u8; 8]])),
                ("name", str_col(vec!["op"])),
                ("start_ns", i64_col(vec![NOW_NS])),
                ("end_ns", i64_col(vec![NOW_NS])),
                ("method", str_col(vec!["GET"])),
            ]);
            let file = std::fs::File::create(&pq).expect("create parquet");
            let mut writer = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None)
                .expect("arrow writer");
            writer.write(&batch).expect("write batch");
            writer.close().expect("close writer");
            std::fs::write(&mapping_path, MAPPING_TOML).expect("write mapping");
            (dir, pq, mapping_path)
        }

        /// `--read-cursors 0` and `--decode-queue-batches 0` are rejected on
        /// the spans path with the same messages the other paths give: a lever
        /// this path ignores is still not one that may take a value its own
        /// documentation calls invalid.
        #[tokio::test]
        async fn zero_levers_are_rejected() {
            let (_dir, pq, mapping_path) = fixture();
            for (read_cursors, decode_queue_batches, want) in [
                (Some(0), DEFAULT_DECODE_QUEUE_BATCHES, READ_CURSORS_ZERO),
                (None, 0, DECODE_QUEUE_BATCHES_ZERO),
            ] {
                let store: Arc<dyn ObjectStoreBackend> =
                    Arc::new(ravel_object_store::memory::MemoryStore::new());
                let mut sink: Vec<u8> = Vec::new();
                let err = run_warning_to(
                    store,
                    &pq,
                    "acme",
                    &mapping_path,
                    SignalArg::Spans,
                    1,
                    10_000,
                    0,
                    read_cursors,
                    1,
                    DEFAULT_MAX_INFLIGHT_FLUSHES,
                    decode_queue_batches,
                    DEFAULT_TARGET_BYTES,
                    None,
                    NOW_NS,
                    &mut sink,
                )
                .await
                .expect_err("a zero lever is rejected before anything is written");
                assert_eq!(err.to_string(), want);
            }
        }

        /// The entry point prints the spans admission-bypass warning and names
        /// both levers a spans load ignores.
        #[tokio::test]
        async fn the_entry_point_warns_about_the_levers_it_ignores() {
            let (_dir, pq, mapping_path) = fixture();
            let store: Arc<dyn ObjectStoreBackend> =
                Arc::new(ravel_object_store::memory::MemoryStore::new());
            let mut sink: Vec<u8> = Vec::new();
            run_warning_to(
                store,
                &pq,
                "acme",
                &mapping_path,
                SignalArg::Spans,
                1,
                10_000,
                0,
                Some(4),
                1,
                DEFAULT_MAX_INFLIGHT_FLUSHES,
                8,
                DEFAULT_TARGET_BYTES,
                None,
                NOW_NS,
                &mut sink,
            )
            .await
            .expect("an ignored lever is a warning, not a failure");
            let emitted = String::from_utf8(sink).expect("warnings are utf-8");
            assert!(
                emitted.contains(SPANS_ADMISSION_BYPASS_WARNING),
                "the spans admission-bypass warning reaches the CLI's stream: {emitted}"
            );
            assert!(
                emitted
                    .contains("a spans load ignores --read-cursors 4 and --decode-queue-batches 8"),
                "both ignored levers are named: {emitted}"
            );
        }

        /// Two mapped attributes cannot share a key, in either list: the
        /// stored `attrs` is one map and the merge would silently pick one.
        #[test]
        fn a_duplicate_attribute_key_is_refused() {
            let text = format!(
                "{MAPPING_TOML}\n[[spans.resource_attribute]]\nkey = \"http.method\"\ncolumn = \
                 \"m2\"\ntype = \"str\"\n"
            );
            let err = parse_spans_mapping(&text).expect_err("one key, two columns");
            let LoadError::Setup(message) = err else {
                panic!("expected a setup error");
            };
            assert!(
                message.contains("declares the attribute key \"http.method\" twice"),
                "the refusal names the key: {message}"
            );
        }

        /// A mapped attribute key over the OTLP key-length cap is refused
        /// before any row is read, since the mapping alone decides it.
        #[test]
        fn an_oversized_attribute_key_is_refused_at_the_otlp_bound() {
            let limit = SpanIngestLimits::default().max_attribute_key_len;
            let key = "k".repeat(limit + 1);
            let text = format!(
                "{MAPPING_TOML}\n[[spans.attribute]]\nkey = \"{key}\"\ncolumn = \"x\"\ntype = \
                 \"str\"\n"
            );
            let err = parse_spans_mapping(&text).expect_err("over the key-length cap");
            let LoadError::Setup(message) = err else {
                panic!("expected a setup error");
            };
            assert!(
                message.contains(&format!(
                    "is {} bytes, more than the attribute-key limit of {limit}",
                    limit + 1
                )),
                "the refusal names both lengths: {message}"
            );
        }

        /// Every key ravel-otlp reserves for a span field RSPAN has no column
        /// for is refused in either attribute list. The list is ravel-otlp's
        /// own, so a key added there is covered here without an edit.
        #[test]
        fn every_reserved_attribute_key_is_refused_in_either_list() {
            use ravel_otlp::traces_normalize::RESERVED_ATTR_KEYS;

            for key in RESERVED_ATTR_KEYS {
                for list in ["attribute", "resource_attribute"] {
                    let text = format!(
                        "{MAPPING_TOML}\n[[spans.{list}]]\nkey = \"{key}\"\ncolumn = \"x\"\ntype \
                         = \"str\"\n"
                    );
                    let err = parse_spans_mapping(&text).expect_err("a reserved key is refused");
                    let LoadError::Setup(message) = err else {
                        panic!("expected a setup error for {key:?} in {list}");
                    };
                    assert!(
                        message.starts_with(&format!(
                            "--mapping [spans] names the reserved attribute key {key:?}, which \
                             this version does not map"
                        )),
                        "the refusal names {key:?} in {list}: {message}"
                    );
                }
            }
        }

        /// A logs load uses both levers a sequential load ignores, so it has
        /// no warning to give whatever they are set to.
        #[test]
        fn a_logs_load_has_no_unused_lever_warning() {
            assert_eq!(unused_lever_warning(Some(4), 8, SignalArg::Logs), None);
            assert!(unused_lever_warning(Some(4), 8, SignalArg::Metrics).is_some());
        }

        /// The future-skew bound is kept and the past-lag bound is relaxed,
        /// both anchored on the span's END exactly as `checked_span_interval`
        /// anchors them.
        #[test]
        fn future_skew_is_kept_and_past_lag_is_relaxed() {
            let limits = SpanIngestLimits::default();
            let mapping = parse_spans_mapping(MAPPING_TOML).expect("valid mapping");
            let build = |start_ns: i64, end_ns: i64| {
                let batch = batch(vec![
                    ("trace_id", bin_col(vec![vec![1u8; 16]])),
                    ("span_id", bin_col(vec![vec![2u8; 8]])),
                    ("name", str_col(vec!["op"])),
                    ("start_ns", i64_col(vec![start_ns])),
                    ("end_ns", i64_col(vec![end_ns])),
                    ("method", str_col(vec!["GET"])),
                ]);
                let cols = SpansColumnIndex::resolve(&batch, &mapping).expect("columns resolve");
                build_span(&batch, &cols, &mapping, &limits, NOW_NS, 0, &mut 0)
            };

            // The start stays well inside the bound in both cases, so only the
            // END decides: a start-anchored check would admit the second span
            // and this assertion would fail.
            let at_bound = NOW_NS + limits.max_future_skew_ns;
            let span = build(NOW_NS, at_bound).expect("an end exactly at the bound is admitted");
            assert_eq!(span.end_ts_ns, at_bound);
            let over = build(NOW_NS, at_bound + 1)
                .expect_err("an end one ns past the bound is rejected, with its start in window");
            assert!(
                over.contains("more than the max future skew"),
                "the rejection names the bound: {over}"
            );

            // Thirty days old: far past `max_ingest_lag_ns`, and admitted.
            let old = NOW_NS - 30 * 86_400 * 1_000_000_000;
            let span = build(old, old).expect("the past-lag bound is relaxed on this path");
            assert_eq!(span.start_ts_ns, old);
            assert!(
                limits.max_ingest_lag_ns < NOW_NS - old,
                "the fixture really is past the OTLP lag bound"
            );
        }

        /// One dictionary-encoded `Utf8` column over `vals`, the shape a
        /// Parquet trace export's name, id and string attribute columns reach
        /// the loader as.
        fn dict_str_col(vals: Vec<&str>) -> ArrayRef {
            let arr: DictionaryArray<Int32Type> = vals.into_iter().map(Some).collect();
            Arc::new(arr)
        }

        /// Each mapped dictionary column is resolved ONCE per batch, and the
        /// row loop resolves no dictionary key of its own.
        ///
        /// `normalized_keys` builds a key vector the size of the whole batch on
        /// every call, so a per-cell resolution costs O(rows^2) per dictionary
        /// column. Counting both resolutions pins the shape rather than a
        /// duration: one per dictionary column, none per cell, whatever the row
        /// count is.
        #[test]
        fn dictionary_columns_are_resolved_once_per_batch() {
            const ROWS: usize = 256;
            let limits = SpanIngestLimits::default();
            let mapping = parse_spans_mapping(MAPPING_TOML).expect("valid mapping");
            let trace_hex = hex::encode([1u8; 16]);
            let span_hex = hex::encode([2u8; 8]);
            // Four dictionary columns (both ids, the name, the one mapped
            // attribute) beside two plain integer columns.
            let batch = batch(vec![
                ("trace_id", dict_str_col(vec![trace_hex.as_str(); ROWS])),
                ("span_id", dict_str_col(vec![span_hex.as_str(); ROWS])),
                ("name", dict_str_col(vec!["op"; ROWS])),
                ("start_ns", i64_col(vec![NOW_NS; ROWS])),
                ("end_ns", i64_col(vec![NOW_NS; ROWS])),
                ("method", dict_str_col(vec!["GET"; ROWS])),
            ]);

            let counters = dict_counters();
            let cols = SpansColumnIndex::resolve(&batch, &mapping).expect("columns resolve");
            assert_eq!(
                counters.columns(),
                4,
                "each of the four dictionary columns is resolved exactly once"
            );
            assert!(
                !matches!(
                    cols.col(&batch, cols.name).data_type(),
                    DataType::Dictionary(_, _)
                ),
                "the row readers index a resolved column, not the dictionary: {:?}",
                cols.col(&batch, cols.name).data_type()
            );

            for row in 0..ROWS {
                let span = build_span(&batch, &cols, &mapping, &limits, NOW_NS, row, &mut 0)
                    .expect("every row builds");
                assert_eq!(span.name, "op", "the resolved column reads the same values");
                assert_eq!(span.trace_id, [1u8; 16]);
                assert_eq!(span.span_id, [2u8; 8]);
                assert_eq!(
                    span.attrs,
                    vec![("http.method".to_string(), "GET".to_string())]
                );
            }
            assert_eq!(
                counters.cell_keys(),
                0,
                "no row reader resolves a dictionary key of its own, over {ROWS} rows"
            );
            assert_eq!(
                counters.columns(),
                4,
                "the row loop resolves no further columns"
            );
        }

        /// A dictionary chunk whose dictionary is empty is answered rather than
        /// aborting inside arrow's `normalized_keys`, which asserts the values
        /// array is non-empty: an all-null chunk resolves to an all-null column
        /// of the value type (#708's shape, which a Parquet writer emits), and
        /// a key that names a value in an empty dictionary is corrupt input and
        /// a typed error.
        #[test]
        fn an_empty_dictionary_chunk_is_a_typed_error_not_a_panic() {
            let keys = Int32Array::from(vec![None, None]);
            let values = Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef;
            let arr: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::new(keys, values));

            let resolved = resolve_dictionary_column(&arr)
                .expect("an all-null chunk is answered, not refused")
                .expect("a dictionary column resolves");
            assert_eq!(
                resolved.data_type(),
                &DataType::Utf8,
                "the column resolves to its value type"
            );
            assert_eq!(resolved.len(), arr.len());
            for row in 0..resolved.len() {
                assert_eq!(
                    read_string(&resolved, row).expect("no error"),
                    None,
                    "every row of an empty-dictionary column is null"
                );
            }

            // The per-cell path reaches the same guard. Arrow asserts on the
            // empty values array whatever the key's nullness is, so this is
            // where the abort was.
            let err = dictionary_key(&arr, 0).expect_err("an empty dictionary names no value");
            assert_eq!(err, EMPTY_DICTIONARY);
        }

        /// The `Dictionary` fallback arms of [`read_id`] and
        /// [`id_cell_is_empty`], which no load reaches because every row path
        /// reads resolved columns: a column handed to them unresolved reads by
        /// the value its key names.
        #[test]
        fn unresolved_dictionary_id_cells_read_by_value() {
            let span_hex = hex::encode([2u8; 8]);
            let ids = dict_str_col(vec![span_hex.as_str(), ""]);

            assert_eq!(read_id::<8>(&ids, 0).expect("reads"), Some([2u8; 8]));
            assert_eq!(read_id::<8>(&ids, 1).expect("reads"), None);
            assert!(!id_cell_is_empty(&ids, 0).expect("reads"));
            assert!(
                id_cell_is_empty(&ids, 1).expect("reads"),
                "an empty VALUE is an empty parent, though its key is not"
            );
        }

        /// A span that ends before it starts is rejected rather than stored
        /// with an interval no query window can mean anything against.
        #[test]
        fn an_end_before_its_start_is_rejected() {
            let limits = SpanIngestLimits::default();
            let mapping = parse_spans_mapping(MAPPING_TOML).expect("valid mapping");
            let batch = batch(vec![
                ("trace_id", bin_col(vec![vec![1u8; 16]])),
                ("span_id", bin_col(vec![vec![2u8; 8]])),
                ("name", str_col(vec!["op"])),
                ("start_ns", i64_col(vec![NOW_NS])),
                ("end_ns", i64_col(vec![NOW_NS - 1])),
                ("method", str_col(vec!["GET"])),
            ]);
            let cols = SpansColumnIndex::resolve(&batch, &mapping).expect("columns resolve");
            let err = build_span(&batch, &cols, &mapping, &limits, NOW_NS, 0, &mut 0)
                .expect_err("end before start");
            assert_eq!(
                err,
                format!(
                    "span ends at {} ns, before it starts at {NOW_NS} ns",
                    NOW_NS - 1
                )
            );
        }

        /// [`MAPPING_TOML`] plus the parent, status code and status message
        /// columns, so one fixture shape can drive every optional field.
        const FULL_MAPPING_TOML: &str = r#"
[spans]
trace_id_column       = "trace_id"
span_id_column        = "span_id"
parent_span_id_column = "parent"
name_column           = "name"
start_ts_column       = "start_ns"
start_ts_unit         = "nanos"
end_ts_column         = "end_ns"
end_ts_unit           = "nanos"
status_code_column    = "status"
status_message_column = "status_msg"

[[spans.attribute]]
key = "http.method"
column = "method"
type = "str"
"#;

        fn opt_bin_col(vals: Vec<Option<Vec<u8>>>) -> ArrayRef {
            let refs: Vec<Option<&[u8]>> = vals.iter().map(|v| v.as_deref()).collect();
            Arc::new(BinaryArray::from(refs))
        }

        fn opt_str_col(vals: Vec<Option<&str>>) -> ArrayRef {
            Arc::new(StringArray::from(vals))
        }

        fn opt_i64_col(vals: Vec<Option<i64>>) -> ArrayRef {
            Arc::new(Int64Array::from(vals))
        }

        /// One row over [`FULL_MAPPING_TOML`], every column overridable.
        struct Row {
            parent: ArrayRef,
            name: ArrayRef,
            start: ArrayRef,
            end: ArrayRef,
            status: ArrayRef,
            status_msg: ArrayRef,
            method: ArrayRef,
        }

        impl Default for Row {
            fn default() -> Self {
                Row {
                    parent: opt_bin_col(vec![None]),
                    name: str_col(vec!["op"]),
                    start: i64_col(vec![NOW_NS]),
                    end: i64_col(vec![NOW_NS]),
                    status: opt_i64_col(vec![None]),
                    status_msg: opt_str_col(vec![None]),
                    method: str_col(vec!["GET"]),
                }
            }
        }

        /// Build the single row `row` describes through the real
        /// [`build_span`], under [`FULL_MAPPING_TOML`], with the count of
        /// attribute values it dropped for being over the cap.
        fn build_one_counting(row: Row) -> (Result<NormalizedSpan, String>, u64) {
            let mut dropped = 0u64;
            let span = build_one_into(row, &mut dropped);
            (span, dropped)
        }

        fn build_one_into(row: Row, dropped: &mut u64) -> Result<NormalizedSpan, String> {
            let limits = SpanIngestLimits::default();
            let mapping = parse_spans_mapping(FULL_MAPPING_TOML).expect("valid mapping");
            let batch = batch(vec![
                ("trace_id", bin_col(vec![vec![1u8; 16]])),
                ("span_id", bin_col(vec![vec![2u8; 8]])),
                ("parent", row.parent),
                ("name", row.name),
                ("start_ns", row.start),
                ("end_ns", row.end),
                ("status", row.status),
                ("status_msg", row.status_msg),
                ("method", row.method),
            ]);
            let cols = SpansColumnIndex::resolve(&batch, &mapping).expect("columns resolve");
            build_span(&batch, &cols, &mapping, &limits, NOW_NS, 0, dropped)
        }

        /// [`build_one_counting`] for the tests that do not care how many
        /// attribute values the row lost to the cap.
        fn build_one(row: Row) -> Result<NormalizedSpan, String> {
            build_one_counting(row).0
        }

        /// A zero start takes the load time and a zero end takes the resolved
        /// start, the two fallbacks `normalize_span` applies to the zeros an
        /// under-instrumented OTLP sender emits.
        #[test]
        fn a_zero_start_takes_load_time_and_a_zero_end_takes_the_start() {
            let span = build_one(Row {
                start: i64_col(vec![0]),
                end: i64_col(vec![0]),
                ..Row::default()
            })
            .expect("two zeros are admitted");
            assert_eq!(span.start_ts_ns, NOW_NS, "a zero start takes the load time");
            assert_eq!(span.end_ts_ns, NOW_NS, "a zero end takes the start");

            // A zero end beside a REAL start takes that start, not the load
            // time: the two fallbacks are distinguishable only here.
            let earlier = NOW_NS - 5 * 60 * 1_000_000_000;
            let span = build_one(Row {
                start: i64_col(vec![earlier]),
                end: i64_col(vec![0]),
                ..Row::default()
            })
            .expect("a zero end beside a real start is admitted");
            assert_eq!(span.start_ts_ns, earlier);
            assert_eq!(
                span.end_ts_ns, earlier,
                "a zero end takes the span's own start, not the load time"
            );
        }

        /// A status outside OTLP's `0..=2` enum is `Unset`, including a value
        /// too wide for `i64`: `status_code_from_i32` maps everything outside
        /// the enum to `Unset`, and a `UInt64` cell above `i64::MAX` is
        /// outside it by more, not by a different kind.
        #[test]
        fn a_status_outside_the_otlp_enum_is_unset() {
            for (code, want) in [
                (0i64, StatusCode::Unset),
                (1, StatusCode::Ok),
                (2, StatusCode::Error),
                (3, StatusCode::Unset),
                (-1, StatusCode::Unset),
                (i64::MAX, StatusCode::Unset),
            ] {
                let span = build_one(Row {
                    status: opt_i64_col(vec![Some(code)]),
                    ..Row::default()
                })
                .unwrap_or_else(|e| panic!("status {code} is admitted: {e}"));
                assert_eq!(span.status_code, want, "status {code}");
            }

            // A UInt64 column carrying a value above i64::MAX.
            let wide: ArrayRef = Arc::new(UInt64Array::from(vec![u64::MAX]));
            let span = build_one(Row {
                status: wide,
                ..Row::default()
            })
            .expect("a status above i64::MAX is admitted, not refused");
            assert_eq!(span.status_code, StatusCode::Unset);
        }

        /// An empty parent value is a root span, in each spelling a Parquet
        /// column can carry one; a present, non-empty value of the wrong width
        /// is still refused.
        #[test]
        fn an_empty_parent_is_a_root_and_a_wrong_width_one_is_refused() {
            for (label, cell) in [
                ("a null cell", opt_bin_col(vec![None])),
                ("an empty binary value", opt_bin_col(vec![Some(Vec::new())])),
                ("an empty string", opt_str_col(vec![Some("")])),
            ] {
                let span = build_one(Row {
                    parent: cell,
                    ..Row::default()
                })
                .unwrap_or_else(|e| panic!("{label} is a root span: {e}"));
                assert_eq!(span.parent_span_id, None, "{label} is a root span");
            }

            // A `FixedSizeBinary(0)` parent column: the schema itself says
            // every row is a root, and `check_id_column` accepts it only
            // BECAUSE it is the parent column (`empty_is_root`). Any other id
            // column of that width is refused for having no width to give.
            let zero_width: ArrayRef = Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    vec![Some::<&[u8]>(&[])].into_iter(),
                    0,
                )
                .expect("a zero-width fixed-size column"),
            );
            assert_eq!(
                zero_width.data_type(),
                &DataType::FixedSizeBinary(0),
                "the fixture really is the zero-width arm"
            );
            let span = build_one(Row {
                parent: zero_width,
                ..Row::default()
            })
            .expect("a zero-width fixed-size parent column is a file of root spans");
            assert_eq!(span.parent_span_id, None, "every row is a root span");
            let err = check_id_column(&DataType::FixedSizeBinary(0), "span_id", 8, false)
                .expect_err("a zero-width span_id column can produce no id");
            assert_eq!(
                err,
                "id column \"span_id\" is FixedSizeBinary(0), but this id is 8 bytes. Ravel never \
                 pads or truncates an id, so no row of this column can produce one."
            );

            // A hex string of the right width is a parent, so the empty-string
            // case above is emptiness and not "strings are never parents".
            let span = build_one(Row {
                parent: opt_str_col(vec![Some("0202020202020202")]),
                ..Row::default()
            })
            .expect("a 16-character hex parent is read");
            assert_eq!(span.parent_span_id, Some([2u8; 8]));

            let err = build_one(Row {
                parent: opt_bin_col(vec![Some(vec![3u8; 4])]),
                ..Row::default()
            })
            .expect_err("a non-empty 4-byte parent is refused");
            assert!(
                err.contains("is not an 8-byte value"),
                "the refusal names the width: {err}"
            );
        }

        /// A null timestamp cell and a null name cell are both refused. OTLP
        /// has no null for either, so neither has a reading to match; giving
        /// one a load-time default would hide a mapping mistake.
        #[test]
        fn a_null_timestamp_or_name_cell_is_refused() {
            for (want, row) in [
                (
                    "start_ts column \"start_ns\" is null",
                    Row {
                        start: opt_i64_col(vec![None]),
                        ..Row::default()
                    },
                ),
                (
                    "end_ts column \"end_ns\" is null",
                    Row {
                        end: opt_i64_col(vec![None]),
                        ..Row::default()
                    },
                ),
                (
                    "name column \"name\" is null",
                    Row {
                        name: opt_str_col(vec![None]),
                        ..Row::default()
                    },
                ),
            ] {
                let err = build_one(row).expect_err("a null cell is refused");
                assert_eq!(err, want, "the refusal names the mapped column");
            }
        }

        /// An empty status message is no message, as an OTLP status with an
        /// empty `message` field is.
        #[test]
        fn an_empty_status_message_is_stored_as_no_message() {
            let span = build_one(Row {
                status_msg: opt_str_col(vec![Some("")]),
                ..Row::default()
            })
            .expect("an empty status message is admitted");
            assert_eq!(span.status_message, None);

            let span = build_one(Row {
                status_msg: opt_str_col(vec![Some("deadlock")]),
                ..Row::default()
            })
            .expect("a real status message is admitted");
            assert_eq!(span.status_message.as_deref(), Some("deadlock"));
        }

        /// An attribute value over the OTLP cap drops THAT attribute and keeps
        /// the span, which is `convert_attrs_lossy`'s rule on the OTLP path,
        /// and the drop is COUNTED so the load summary can say the stored
        /// record is an approximation.
        #[test]
        fn an_over_cap_attribute_value_is_dropped_and_the_span_kept() {
            let limits = SpanIngestLimits::default();
            let big = "x".repeat(limits.max_attribute_value_len + 1);
            let (span, dropped) = build_one_counting(Row {
                method: str_col(vec![big.as_str()]),
                ..Row::default()
            });
            let span = span.expect("an over-cap attribute value does not reject the span");
            assert_eq!(span.attrs, Vec::new(), "the attribute itself is dropped");
            assert_eq!(dropped, 1, "the drop is counted, exactly once");

            // Exactly at the cap is kept, so the case above is the cap and not
            // the column going missing, and it counts nothing.
            let at_cap = "x".repeat(limits.max_attribute_value_len);
            let (span, dropped) = build_one_counting(Row {
                method: str_col(vec![at_cap.as_str()]),
                ..Row::default()
            });
            let span = span.expect("exactly at the cap is admitted");
            assert_eq!(span.attrs, vec![("http.method".to_string(), at_cap)]);
            assert_eq!(dropped, 0, "nothing was dropped");
        }

        /// On a FAILED load the `attrs_dropped` line says what the count
        /// covers, because the count is taken where each span is BUILT: two
        /// values were dropped from a batch whose write never landed, so no
        /// stored span is missing them and the success path's wording would be
        /// false. The success path keeps that wording.
        #[tokio::test]
        async fn a_failed_spans_load_says_attrs_dropped_covers_abandoned_batches() {
            use ravel_object_store::fault::{FaultPlan, FaultStore, Op, ScriptedFault, Sequence};
            use ravel_object_store::memory::MemoryStore;

            let limits = SpanIngestLimits::default();
            let big = "x".repeat(limits.max_attribute_value_len + 1);
            // Two rows in one batch, each carrying one over-cap attribute
            // value: two drops counted before any write is attempted.
            let batch = batch(vec![
                ("trace_id", bin_col(vec![vec![1u8; 16], vec![1u8; 16]])),
                ("span_id", bin_col(vec![vec![2u8; 8], vec![3u8; 8]])),
                ("name", str_col(vec!["op", "op"])),
                ("start_ns", i64_col(vec![NOW_NS, NOW_NS])),
                ("end_ns", i64_col(vec![NOW_NS, NOW_NS])),
                ("method", str_col(vec![big.as_str(), big.as_str()])),
            ]);
            let dir = tempfile::tempdir().expect("tempdir");
            let pq = dir.path().join("spans.parquet");
            let file = std::fs::File::create(&pq).expect("create parquet");
            let mut writer = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None)
                .expect("arrow writer");
            writer.write(&batch).expect("write batch");
            writer.close().expect("close writer");

            // Fail every span data-object PUT, so the one batch that was
            // decoded is the one the failure abandons and nothing lands.
            let fault = ScriptedFault::Transient("injected PUT failure".into());
            let mut seq = Sequence::new(Op::Put).with_key_contains("/s/l0/");
            for _ in 0..8 {
                seq = seq.then_fault(fault.clone());
            }
            let store: Arc<dyn ObjectStoreBackend> = Arc::new(FaultStore::new(
                MemoryStore::new(),
                FaultPlan::empty().with_sequence(seq),
            ));

            let mapping = parse_spans_mapping(MAPPING_TOML).expect("valid mapping");
            let mut report = SpansLoadReport::default();
            let err = load_spans_into(
                &mut report,
                store,
                &pq,
                "acme",
                &mapping,
                1,
                10_000,
                0,
                1,
                1,
                1,
                None,
                NOW_NS,
                Arc::new(SystemClock),
            )
            .await
            .expect_err("the scripted PUT fault fails the load");

            assert!(
                matches!(err, LoadError::Flush { .. }),
                "expected a flush failure, got: {err}"
            );
            assert_eq!(
                report.attributes_dropped, 2,
                "both drops are counted, though neither span landed"
            );
            assert_eq!(
                report.rows_processed, 0,
                "no row acked durable, so the count covers spans in no object"
            );
            assert_eq!(
                spans_attrs_dropped_line(&report, AttrsDroppedScope::Failed),
                "  attrs_dropped    : 2 (attribute values over the OTLP value-length cap; counted \
                 where each span was built, so this includes batches the failure abandoned, whose \
                 spans are in no object)"
            );
            assert_eq!(
                spans_attrs_dropped_line(&report, AttrsDroppedScope::Complete),
                "  attrs_dropped    : 2 (attribute values over the OTLP value-length cap; each \
                 span was stored without them)",
                "a load that completed still says the spans were stored without them"
            );
        }

        /// A row rejection mid-batch still counts the drops of the rows built
        /// before it, which the failure-path line claims to cover, and not the
        /// rejected row's own.
        #[tokio::test]
        async fn a_row_rejected_spans_load_counts_the_drops_built_before_it() {
            use ravel_object_store::memory::MemoryStore;

            let limits = SpanIngestLimits::default();
            let big = "x".repeat(limits.max_attribute_value_len + 1);
            // Every row drops its over-cap `method` value. Row 2 is then
            // rejected by the attribute after it, an integer attribute whose
            // cell there is a string, so its own drop is counted before the
            // rejection and must not reach the report.
            let batch = batch(vec![
                ("trace_id", bin_col(vec![vec![1u8; 16]; 3])),
                (
                    "span_id",
                    bin_col(vec![vec![2u8; 8], vec![3u8; 8], vec![4u8; 8]]),
                ),
                ("name", str_col(vec!["op"; 3])),
                ("start_ns", i64_col(vec![NOW_NS; 3])),
                ("end_ns", i64_col(vec![NOW_NS; 3])),
                ("method", str_col(vec![big.as_str(); 3])),
                (
                    "code",
                    Arc::new(StringArray::from(vec![None, None, Some("x")])) as ArrayRef,
                ),
            ]);
            let mapping_toml = format!(
                "{MAPPING_TOML}\n[[spans.attribute]]\nkey = \"code\"\ncolumn = \"code\"\ntype = \
                 \"i64\"\n"
            );
            let dir = tempfile::tempdir().expect("tempdir");
            let pq = dir.path().join("spans.parquet");
            let file = std::fs::File::create(&pq).expect("create parquet");
            let mut writer = parquet::arrow::ArrowWriter::try_new(file, batch.schema(), None)
                .expect("arrow writer");
            writer.write(&batch).expect("write batch");
            writer.close().expect("close writer");

            let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
            let mapping = parse_spans_mapping(&mapping_toml).expect("valid mapping");
            let mut report = SpansLoadReport::default();
            let err = load_spans_into(
                &mut report,
                store,
                &pq,
                "acme",
                &mapping,
                1,
                10_000,
                0,
                1,
                1,
                1,
                None,
                NOW_NS,
                Arc::new(SystemClock),
            )
            .await
            .expect_err("row 2 is rejected");

            assert!(
                matches!(err, LoadError::RowRejected { row: 2, .. }),
                "expected row 2 rejected, got: {err}"
            );
            assert_eq!(
                report.attributes_dropped, 2,
                "the two rows built before the rejection are counted, the rejected row is not"
            );
        }

        /// The cap applies to the STORED string, so a bytes attribute is
        /// measured as its lowercase hex: a value of `cap / 2 + 1` raw bytes
        /// is under the cap as bytes and over it as hex, and is dropped.
        #[test]
        fn a_bytes_attribute_is_measured_as_its_hex_form() {
            let limits = SpanIngestLimits::default();
            let cap = limits.max_attribute_value_len;
            let mapping_toml = format!(
                "{FULL_MAPPING_TOML}\n[[spans.attribute]]\nkey = \"request.digest\"\ncolumn = \
                 \"digest\"\ntype = \"bytes\"\n"
            );
            let mapping = parse_spans_mapping(&mapping_toml).expect("valid mapping");
            let build = |raw: Vec<u8>| {
                let batch = batch(vec![
                    ("trace_id", bin_col(vec![vec![1u8; 16]])),
                    ("span_id", bin_col(vec![vec![2u8; 8]])),
                    ("parent", opt_bin_col(vec![None])),
                    ("name", str_col(vec!["op"])),
                    ("start_ns", i64_col(vec![NOW_NS])),
                    ("end_ns", i64_col(vec![NOW_NS])),
                    ("status", opt_i64_col(vec![None])),
                    ("status_msg", opt_str_col(vec![None])),
                    ("method", str_col(vec!["GET"])),
                    ("digest", bin_col(vec![raw])),
                ]);
                let cols = SpansColumnIndex::resolve(&batch, &mapping).expect("columns resolve");
                let mut dropped = 0u64;
                let span = build_span(&batch, &cols, &mapping, &limits, NOW_NS, 0, &mut dropped)
                    .expect("an over-cap attribute value does not reject the span");
                (span, dropped)
            };

            // cap/2 + 1 raw bytes: under the cap as bytes, two characters over
            // it as hex. Measuring the raw bytes would keep this attribute and
            // this assertion would fail.
            let raw_len = cap / 2 + 1;
            assert!(raw_len <= cap, "the raw value is itself under the cap");
            assert_eq!(raw_len * 2, cap + 2, "its hex form is over the cap");
            let (span, dropped) = build(vec![0xABu8; raw_len]);
            assert_eq!(
                span.attrs,
                vec![("http.method".to_string(), "GET".to_string())],
                "the digest is dropped and its neighbour is kept"
            );
            assert_eq!(dropped, 1, "the drop is counted");

            // cap/2 raw bytes hexes to exactly the cap and is kept, so the case
            // above is the hex length and not the column going missing.
            let (span, dropped) = build(vec![0xABu8; cap / 2]);
            assert_eq!(
                span.attrs,
                vec![
                    ("http.method".to_string(), "GET".to_string()),
                    ("request.digest".to_string(), "ab".repeat(cap / 2)),
                ]
            );
            assert_eq!(dropped, 0, "nothing was dropped");
        }

        /// A negative start or end is refused, naming the unit each was read
        /// in: OTLP's two `u64` timestamps have no negative to match against.
        #[test]
        fn a_negative_timestamp_is_refused() {
            let err = build_one(Row {
                start: i64_col(vec![-1]),
                ..Row::default()
            })
            .expect_err("a negative start is refused");
            assert_eq!(
                err,
                format!(
                    "span timestamps are before the Unix epoch (start -1 ns, read as \
                     start_ts_unit = nanos; end {NOW_NS} ns, read as end_ts_unit = nanos); a \
                     timestamp column holds a negative value"
                )
            );

            let err = build_one(Row {
                start: i64_col(vec![-2]),
                end: i64_col(vec![-1]),
                ..Row::default()
            })
            .expect_err("a wholly negative interval is refused too");
            assert!(err.contains("before the Unix epoch"), "{err}");

            // Zero is the epoch, not a negative, and it takes the zero
            // fallbacks rather than this refusal.
            build_one(Row {
                start: i64_col(vec![0]),
                end: i64_col(vec![0]),
                ..Row::default()
            })
            .expect("zero is the fallback case, not a negative one");
        }

        /// A native `Timestamp(Second)` start scales by its own unit, not by
        /// the declared `start_ts_unit`, so the refusal names seconds for the
        /// start, while the integer end still names `end_ts_unit`.
        #[test]
        fn a_negative_native_start_names_its_own_unit() {
            let err = build_one(Row {
                start: Arc::new(TimestampSecondArray::from(vec![-5])) as ArrayRef,
                ..Row::default()
            })
            .expect_err("a negative native start is refused");
            assert_eq!(
                err,
                format!(
                    "span timestamps are before the Unix epoch (start -5000000000 ns, read in \
                     the column's own Timestamp unit, seconds; end {NOW_NS} ns, read as \
                     end_ts_unit = nanos); a timestamp column holds a negative value"
                )
            );
        }

        /// A zero end cell takes the start, so a negative start makes the end
        /// negative too. The end was never read from the end column, and the
        /// refusal says it came from the start rather than naming
        /// `end_ts_unit`, under which the end column holds a 0.
        #[test]
        fn a_substituted_end_names_the_start_as_its_source() {
            let err = build_one(Row {
                start: Arc::new(TimestampSecondArray::from(vec![-5])) as ArrayRef,
                end: i64_col(vec![0]),
                ..Row::default()
            })
            .expect_err("a negative start with a zero end is refused");
            assert_eq!(
                err,
                "span timestamps are before the Unix epoch (start -5000000000 ns, read in the \
                 column's own Timestamp unit, seconds; end -5000000000 ns, taken from start_ts \
                 because end_ts is 0); a timestamp column holds a negative value"
            );
        }

        /// Both attribute-count caps are properties of the mapping, so both
        /// are refused at mapping parse rather than per row.
        #[test]
        fn the_attribute_count_caps_are_checked_against_the_mapping() {
            let attr_list = |list: &str, key_prefix: &str, n: usize| {
                let mut text = MAPPING_TOML.to_string();
                for i in 0..n {
                    text.push_str(&format!(
                        "\n[[spans.{list}]]\nkey = \"{key_prefix}{i}\"\ncolumn = \
                         \"c{key_prefix}{i}\"\ntype = \"str\"\n"
                    ));
                }
                text
            };

            // MAPPING_TOML already declares one [[spans.attribute]], so the
            // cap is reached at `cap - 1` more.
            let cap = LOADER_MAX_ATTRIBUTES_PER_RECORD;
            parse_spans_mapping(&attr_list("attribute", "a", cap - 1))
                .expect("exactly at the loader per-record cap is accepted");
            let err = parse_spans_mapping(&attr_list("attribute", "a", cap))
                .expect_err("one column past the loader per-record cap");
            let LoadError::Setup(message) = err else {
                panic!("expected a setup error");
            };
            assert_eq!(
                message,
                format!(
                    "--mapping [spans] declares {} attribute columns, more than the loader \
                     per-record cap of {cap}",
                    cap + 1
                )
            );

            let resource_cap = SpanIngestLimits::default().max_resource_attributes;
            parse_spans_mapping(&attr_list("resource_attribute", "r", resource_cap))
                .expect("exactly at the OTLP per-resource cap is accepted");
            let err = parse_spans_mapping(&attr_list("resource_attribute", "r", resource_cap + 1))
                .expect_err("one column past OTLP's max_resource_attributes");
            let LoadError::Setup(message) = err else {
                panic!("expected a setup error");
            };
            assert_eq!(
                message,
                format!(
                    "--mapping [spans] declares {} resource_attribute columns, more than the OTLP \
                     per-resource cap of {resource_cap}",
                    resource_cap + 1
                )
            );
        }
    }

    /// Fix-round regressions for the metrics submit loop and the histogram
    /// grouper (PR #2096 review).
    mod metrics_pipeline_review {
        use ravel_object_store::fault::{
            FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault,
        };
        use ravel_object_store::memory::MemoryStore;

        use super::*;

        const DRAIN_MAPPING: &str = "[metrics]\nname = \"drain_probe\"\nvalue_column = \
                                     \"value\"\nts_column = \"ts\"\nts_unit = \"nanos\"\nkind = \
                                     \"gauge\"\n\n[[metrics.label]]\nname = \"host\"\ncolumn = \
                                     \"host\"\n";

        fn drain_mapping() -> MetricsMapping {
            match parse_mapping_document(DRAIN_MAPPING, SignalArg::Metrics).expect("valid mapping")
            {
                MappingSection::Metrics(m) => m,
                _ => panic!("a [metrics] section parses as metrics"),
            }
        }

        fn drain_batch(hosts: &[&str]) -> RecordBatch {
            let n = hosts.len();
            batch(vec![
                ("ts", i64_col(vec![NOW_NS - 60_000_000_000; n])),
                (
                    "value",
                    Arc::new(Float64Array::from(vec![1.0; n])) as ArrayRef,
                ),
                ("host", str_col(hosts.to_vec())),
            ])
        }

        /// A `host` label value whose series routes to each of `shards` shards,
        /// found through the loader's own point builder so the routing is the
        /// one `IngestRouter::write` applies.
        fn host_per_shard(mapping: &MetricsMapping, shards: u32) -> Vec<String> {
            let candidates: Vec<String> = (0..256).map(|i| format!("h{i}")).collect();
            let refs: Vec<&str> = candidates.iter().map(String::as_str).collect();
            let b = drain_batch(&refs);
            let cols = MetricsColumnIndex::resolve(&b, mapping).expect("columns resolve");
            let (points, _) = build_batch_points(
                &b,
                &cols,
                0,
                &TenantId::new("acme"),
                mapping,
                &IngestLimits::default(),
                NOW_NS,
                false,
                None,
            )
            .expect("every candidate row is admitted");
            (0..shards)
                .map(|shard| {
                    let idx = points
                        .iter()
                        .position(|p| ravel_types::shard_for(&p.series_id, shards) == shard)
                        .expect("some candidate routes to every shard");
                    candidates[idx].clone()
                })
                .collect()
        }

        /// Every data-object key under one metrics shard.
        async fn shard_data_keys(store: &dyn ObjectStoreBackend, shard: u32) -> Vec<String> {
            let needle = format!("/m/l0/{shard:04}/");
            let mut out = Vec::new();
            let mut page: Option<ravel_object_store::PageToken> = None;
            loop {
                let p = store.list("", page).await.expect("list");
                out.extend(
                    p.objects
                        .into_iter()
                        .map(|o| o.key)
                        .filter(|k| k.contains(&needle)),
                );
                match p.next {
                    Some(t) => page = Some(t),
                    None => break,
                }
            }
            out
        }

        /// A write that fails in the FINAL drain, with a later outstanding
        /// write that succeeds, reports that later write's token in the
        /// returned error's durable list.
        ///
        /// Two one-row batches at `--pipeline-depth 3` never fill the window,
        /// so both writes are still outstanding when the loop ends and both
        /// resolve in the end-of-load drain, oldest first. Batch 0 routes to
        /// shard 0, whose data PUT fails permanently; batch 1 routes to shard
        /// 1 and commits. The durable list must be exactly batch 1's one token,
        /// and that token must name the object that actually landed on shard 1.
        ///
        /// Non-vacuity: against the drain that kept the first error and
        /// resolved the rest into the report only, this fails on the
        /// `durable.len()` assertion with 0 tokens, because the error's list
        /// was cloned before batch 1 resolved.
        #[tokio::test]
        async fn a_final_drain_failure_keeps_a_later_writes_tokens() {
            use parquet::arrow::ArrowWriter;

            let shards = 2;
            let mapping = drain_mapping();
            let hosts = host_per_shard(&mapping, shards);

            let dir = tempfile::tempdir().expect("tempdir");
            let pq = dir.path().join("drain.parquet");
            let b = drain_batch(&[hosts[0].as_str(), hosts[1].as_str()]);
            let file = std::fs::File::create(&pq).expect("create parquet");
            let mut writer = ArrowWriter::try_new(file, b.schema(), None).expect("arrow writer");
            writer.write(&b).expect("write batch");
            writer.close().expect("close writer");

            let plan = FaultPlan::empty().with_rule(
                Rule::new(
                    Op::Put,
                    ScriptedFault::Permanent("simulated permanent data-object PUT failure".into()),
                )
                .with_key_contains("/m/l0/0000/")
                .with_occurrence(Occurrence::Always),
            );
            let fault = Arc::new(FaultStore::new(MemoryStore::new(), plan));

            let err = load_metrics(
                fault.clone() as Arc<dyn ObjectStoreBackend>,
                &pq,
                "acme",
                &mapping,
                shards,
                1,
                0,
                3,
                DEFAULT_MAX_INFLIGHT_FLUSHES,
                1,
                None,
                NOW_NS,
                Arc::new(FixedClock(NOW_NS)),
            )
            .await
            .expect_err("batch 0's permanent PUT failure fails the load");
            assert!(
                fault.fault_count(Op::Put, FaultKind::Permanent) >= 1,
                "the scripted fault must have fired"
            );
            let LoadError::Flush { durable, .. } = &err else {
                panic!("expected LoadError::Flush, got {err:?}");
            };
            assert_eq!(
                durable.len(),
                1,
                "exactly batch 1's one token is durable, and it resolved after the failure: \
                 {durable:?}"
            );
            let token = &durable[0];
            assert_eq!(token.shard, 1, "the durable token is batch 1's shard");
            let landed = shard_data_keys(fault.inner(), 1).await;
            let prefix = format!(
                "/m/l0/0001/{}.{}.{:020}.",
                token.writer_id, token.epoch, token.seq
            );
            assert_eq!(landed.len(), 1, "one object landed on shard 1: {landed:?}");
            assert!(
                landed[0].contains(&prefix),
                "the reported token names the object that landed ({prefix} in {landed:?})"
            );
            assert!(
                shard_data_keys(fault.inner(), 0).await.is_empty(),
                "nothing landed on the failing shard"
            );
        }

        /// A row rejection found while an earlier write is failing keeps the
        /// rejection (its row and reason) and carries the drain's durable
        /// list, including a later write that committed, rather than being
        /// replaced by the write's `Flush` error.
        ///
        /// Three one-row batches at `--pipeline-depth 3`: batch 0 routes to
        /// shard 0 and its PUT fails, batch 1 routes to shard 1 and commits,
        /// and row 2 carries a far-future timestamp the decoder rejects, so
        /// the rejection drains both writes first.
        ///
        /// Non-vacuity: against the `drain_sequential_inflight(..).await?` call
        /// in the `Rejected` arm, the load returns the `Flush` error and this
        /// fails on the `RowRejected` match.
        #[tokio::test]
        async fn a_row_rejection_after_a_failed_write_keeps_its_reason_and_the_drained_tokens() {
            use parquet::arrow::ArrowWriter;

            let shards = 2;
            let mapping = drain_mapping();
            let hosts = host_per_shard(&mapping, shards);

            let dir = tempfile::tempdir().expect("tempdir");
            let pq = dir.path().join("reject.parquet");
            let b = batch(vec![
                (
                    "ts",
                    i64_col(vec![
                        NOW_NS - 60_000_000_000,
                        NOW_NS - 60_000_000_000,
                        NOW_NS + 86_400_000_000_000,
                    ]),
                ),
                (
                    "value",
                    Arc::new(Float64Array::from(vec![1.0, 1.0, 1.0])) as ArrayRef,
                ),
                (
                    "host",
                    str_col(vec![
                        hosts[0].as_str(),
                        hosts[1].as_str(),
                        hosts[1].as_str(),
                    ]),
                ),
            ]);
            let file = std::fs::File::create(&pq).expect("create parquet");
            let mut writer = ArrowWriter::try_new(file, b.schema(), None).expect("arrow writer");
            writer.write(&b).expect("write batch");
            writer.close().expect("close writer");

            let plan = FaultPlan::empty().with_rule(
                Rule::new(
                    Op::Put,
                    ScriptedFault::Permanent("simulated permanent data-object PUT failure".into()),
                )
                .with_key_contains("/m/l0/0000/")
                .with_occurrence(Occurrence::Always),
            );
            let fault = Arc::new(FaultStore::new(MemoryStore::new(), plan));

            let err = load_metrics(
                fault.clone() as Arc<dyn ObjectStoreBackend>,
                &pq,
                "acme",
                &mapping,
                shards,
                1,
                0,
                3,
                DEFAULT_MAX_INFLIGHT_FLUSHES,
                1,
                None,
                NOW_NS,
                Arc::new(FixedClock(NOW_NS)),
            )
            .await
            .expect_err("row 2 is rejected");
            assert!(
                fault.fault_count(Op::Put, FaultKind::Permanent) >= 1,
                "the scripted fault must have fired"
            );
            let LoadError::RowRejected {
                row,
                reason,
                durable,
                ..
            } = &err
            else {
                panic!("expected the row rejection to survive the drain, got {err:?}");
            };
            assert_eq!(*row, 2, "the rejected row is the far-future one");
            assert!(
                reason.contains("an earlier write had also failed: flush failed:"),
                "the write failure is named beside the rejection: {reason}"
            );
            assert_eq!(durable.len(), 1, "batch 1's one token: {durable:?}");
            let token = &durable[0];
            let prefix = format!(
                "/m/l0/0001/{}.{}.{:020}.",
                token.writer_id, token.epoch, token.seq
            );
            let landed = shard_data_keys(fault.inner(), 1).await;
            assert_eq!(landed.len(), 1, "one object landed on shard 1: {landed:?}");
            assert!(
                landed[0].contains(&prefix),
                "the reported token names the object that landed ({prefix} in {landed:?})"
            );
        }

        /// A two-row metrics file whose second row is far in the future, and
        /// the mapping file beside it, for the CLI-level tests below.
        fn rejecting_fixture() -> (tempfile::TempDir, std::path::PathBuf, std::path::PathBuf) {
            use parquet::arrow::ArrowWriter;

            let dir = tempfile::tempdir().expect("tempdir");
            let pq = dir.path().join("metrics.parquet");
            let b = batch(vec![
                (
                    "ts",
                    i64_col(vec![NOW_NS - 60_000_000_000, NOW_NS + 86_400_000_000_000]),
                ),
                (
                    "value",
                    Arc::new(Float64Array::from(vec![1.0, 1.0])) as ArrayRef,
                ),
                ("host", str_col(vec!["a", "a"])),
            ]);
            let file = std::fs::File::create(&pq).expect("create parquet");
            let mut writer = ArrowWriter::try_new(file, b.schema(), None).expect("arrow writer");
            writer.write(&b).expect("write batch");
            writer.close().expect("close writer");
            let mapping_path = dir.path().join("mapping.toml");
            std::fs::write(&mapping_path, DRAIN_MAPPING).expect("write mapping");
            (dir, pq, mapping_path)
        }

        async fn run_metrics_cli(
            pq: &Path,
            mapping_path: &Path,
            read_cursors: Option<usize>,
            pipeline_depth: usize,
            decode_queue_batches: usize,
        ) -> (anyhow::Result<()>, String) {
            let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
            let mut sink: Vec<u8> = Vec::new();
            let outcome = run_warning_to(
                store,
                pq,
                "acme",
                mapping_path,
                SignalArg::Metrics,
                1,
                1,
                0,
                read_cursors,
                pipeline_depth,
                DEFAULT_MAX_INFLIGHT_FLUSHES,
                decode_queue_batches,
                DEFAULT_TARGET_BYTES,
                None,
                NOW_NS,
                &mut sink,
            )
            .await;
            (
                outcome,
                String::from_utf8(sink).expect("warnings are utf-8"),
            )
        }

        /// A failed metrics load above depth 1 blames the pipeline depth only
        /// and names `--pipeline-depth 1` as the remedy, never the
        /// `--read-cursors` flag the metrics path ignores.
        ///
        /// Non-vacuity: against `resume_hint(&err, Some(1), pipeline_depth)` in
        /// `run_metrics`, the emitted-verdict assertion fails, the stream
        /// carrying "this run used --read-cursors 1 and --pipeline-depth 2".
        #[tokio::test]
        async fn a_failed_metrics_load_names_only_the_pipeline_depth() {
            let (_dir, pq, mapping_path) = rejecting_fixture();
            let (outcome, emitted) =
                run_metrics_cli(&pq, &mapping_path, None, 2, DEFAULT_DECODE_QUEUE_BATCHES).await;
            let err = outcome.expect_err("row 1 is far in the future and is rejected");
            let load_err = err
                .downcast::<LoadError>()
                .expect("the CLI error wraps the typed load error");
            assert!(
                matches!(load_err, LoadError::RowRejected { row: 1, .. }),
                "{load_err:?}"
            );

            let verdict = "this run used --pipeline-depth 2, so the rows that landed are NOT a \
                           contiguous prefix of the file: a batch submitted after the failing \
                           one can still have committed. Resuming at this offset would both \
                           re-ingest committed rows and skip rows that never landed. Only a load \
                           started with --pipeline-depth 1 is resumable this way.";
            assert!(
                emitted.contains(verdict),
                "the metrics verdict is emitted: {emitted}"
            );
            assert!(
                emitted.contains(METRICS_ADMISSION_BYPASS_WARNING),
                "the metrics admission warning is the one printed: {emitted}"
            );
            assert!(
                !emitted.contains("--read-cursors"),
                "a metrics failure never names --read-cursors: {emitted}"
            );

            let hint = sequential_resume_hint(&load_err, 2).expect("a row rejection has figures");
            assert_eq!(
                hint,
                format!(
                    "resume figures for this failed load:\n  \
                     rows_skipped     : 0\n  \
                     rows_written     : 1\n  \
                     next --skip-rows : 1 (rows_skipped + rows_written)\n\
                     {verdict} There is no deduplication and no per-file idempotency marker, so \
                     nothing checks the offset a re-run is given; see docs/guides/ingest.md for \
                     the procedure."
                )
            );
        }

        /// `--read-cursors 0` and `--decode-queue-batches 0` are rejected on a
        /// metrics load with the logs path's messages, before the warning that
        /// the metrics path ignores them.
        ///
        /// Non-vacuity: without the two guards in `run_metrics` the load runs
        /// and fails on the fixture's far-future row instead, so the message
        /// assertion fails with "row 1: timestamp is ... ahead of load time".
        #[tokio::test]
        async fn zero_read_cursors_or_decode_queue_is_rejected_on_a_metrics_load() {
            let (_dir, pq, mapping_path) = rejecting_fixture();
            for (read_cursors, decode_queue, message) in [
                (Some(0), DEFAULT_DECODE_QUEUE_BATCHES, READ_CURSORS_ZERO),
                (None, 0, DECODE_QUEUE_BATCHES_ZERO),
            ] {
                let (outcome, emitted) =
                    run_metrics_cli(&pq, &mapping_path, read_cursors, 1, decode_queue).await;
                let err = outcome.expect_err("a zero lever is rejected");
                assert_eq!(err.to_string(), message);
                assert!(
                    !emitted.contains("a metrics load ignores"),
                    "the rejection comes before the unused-lever warning: {emitted}"
                );
            }
            assert!(READ_CURSORS_ZERO.starts_with("--read-cursors must be at least 1"));
            assert!(
                DECODE_QUEUE_BATCHES_ZERO.starts_with("--decode-queue-batches must be at least 1")
            );
        }

        fn bucket_row(le: f64) -> MetricRow {
            MetricRow {
                name: "latency".to_string(),
                labels: Vec::new(),
                ts_ns: NOW_NS,
                payload: RowPayload::Bucket(BucketRow {
                    le,
                    own_count: 1,
                    sum: Some(10.0),
                    count: 10,
                }),
            }
        }

        /// The bucket limit is enforced while a group accumulates, not only
        /// when it closes: a ten-row data point under a limit of 4 is refused
        /// on its fifth bucket row, with the close-time message and the
        /// group's first row, and the open group never holds more than 4
        /// bounds.
        ///
        /// Non-vacuity: with the check only at close time, the fifth push
        /// returns `Ok` and the test fails on the `held <= 4` assertion with
        /// "the open group holds 5 bounds after row 4".
        #[test]
        fn the_bucket_limit_refuses_an_open_group_at_the_first_row_past_it() {
            let limits = IngestLimits {
                max_histogram_buckets: 4,
                ..IngestLimits::default()
            };
            let first_row = 100;
            let mut grouper = HistogramGrouper::new(TenantId::new("acme"));
            let mut refused = None;
            for i in 0..10u64 {
                let outcome = grouper.push(bucket_row(i as f64 + 1.0), first_row + i, &limits);
                let held = grouper.pending.as_ref().map_or(0, |g| g.bounds.len());
                assert!(
                    held <= 4,
                    "the open group holds {held} bounds after row {i}"
                );
                if let Err(e) = outcome {
                    refused = Some((i, e));
                    break;
                }
            }
            let Some((index, (row, message))) = refused else {
                panic!("a ten-row group over a limit of 4 must be refused");
            };
            assert_eq!(index, 4, "the fifth bucket row is refused");
            assert_eq!(
                row, first_row,
                "the refusal points at the group's first row"
            );
            assert_eq!(
                message,
                format!(
                    "the \"latency\" data point at ts {NOW_NS} has 5 explicit bounds, more than \
                     the limit of 4"
                ),
            );
        }
    }

    mod logs_ids_and_negative_timestamps {
        use std::path::PathBuf;

        use parquet::arrow::ArrowWriter;
        use parquet::file::properties::WriterProperties;
        use ravel_object_store::memory::MemoryStore;

        use super::*;

        const TRACE_A: &str = "0102030405060708090a0b0c0d0e0f10";
        const TRACE_B: &str = "a1a2a3a4a5a6a7a8a9aaabacadaeafb0";
        const SPAN_A: &str = "1112131415161718";
        const SPAN_B: &str = "b1b2b3b4b5b6b7b8";

        /// Write `batch` to a Parquet file with dictionary encoding on or off
        /// for every column.
        fn write_with(batch: &RecordBatch, dictionary: bool) -> (tempfile::TempDir, PathBuf) {
            let dir = tempfile::tempdir().expect("tempdir");
            let pq = dir.path().join("in.parquet");
            let props = WriterProperties::builder()
                .set_dictionary_enabled(dictionary)
                .build();
            let file = std::fs::File::create(&pq).expect("create parquet");
            let mut w =
                ArrowWriter::try_new(file, batch.schema(), Some(props)).expect("arrow writer");
            w.write(batch).expect("write batch");
            w.close().expect("close writer");
            (dir, pq)
        }

        /// A stored log record's `(ts, trace_id, span_id)`.
        type StoredIds = (i64, Option<[u8; 16]>, Option<[u8; 8]>);

        /// Every stored log record's [`StoredIds`], sorted by ts.
        async fn stored_ids(store: &dyn ObjectStoreBackend) -> Vec<StoredIds> {
            use ravel_logseg::{Predicate, RlogConfig, RlogReader};
            use ravel_object_store::GetRange;

            let cfg = RlogConfig::default();
            let mut out = Vec::new();
            for (key, _) in list_data_objects(store).await {
                let got = store.get(&key, GetRange::Full).await.expect("get object");
                let reader = RlogReader::new(got.data.as_ref(), &cfg).expect("open rlog");
                let (rows, _) = reader.scan(&Predicate::And(Vec::new())).expect("scan");
                out.extend(rows.into_iter().map(|r| (r.ts_ns, r.trace_id, r.span_id)));
            }
            out.sort();
            out
        }

        fn id<const N: usize>(hex_id: &str) -> Option<[u8; N]> {
            hex::decode(hex_id).ok().and_then(|b| b.try_into().ok())
        }

        async fn load_columnar(
            pq: &Path,
            mapping: &Mapping,
        ) -> (Result<LoadReport, LoadError>, Arc<dyn ObjectStoreBackend>) {
            let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
            let result = load(
                Arc::clone(&store),
                pq,
                "acme",
                mapping,
                1,
                1_000,
                None,
                1,
                NOW_NS,
                Arc::new(FixedClock(NOW_NS)),
            )
            .await;
            (result, store)
        }

        /// A hex id column loads the same whether its Parquet pages are plain
        /// or dictionary-encoded: the columnar path resolves a dictionary id
        /// column instead of refusing it, and a null cell (a null key, once
        /// encoded) stores no id either way.
        #[tokio::test]
        async fn dictionary_encoded_hex_id_columns_load_like_plain_ones() {
            let ts: Vec<i64> = (0..6).map(|i| NOW_NS - 1_000 + i).collect();
            let trace = vec![
                Some(TRACE_A),
                Some(TRACE_A),
                None,
                Some(TRACE_B),
                Some(TRACE_A),
                Some(TRACE_B),
            ];
            let span = vec![
                Some(SPAN_A),
                Some(SPAN_B),
                Some(SPAN_A),
                None,
                Some(SPAN_B),
                Some(SPAN_A),
            ];
            let b = batch(vec![
                ("ts", i64_col(ts.clone())),
                ("svc", str_col(vec!["api"; 6])),
                (
                    "trace_id",
                    Arc::new(StringArray::from(trace.clone())) as ArrayRef,
                ),
                (
                    "span_id",
                    Arc::new(StringArray::from(span.clone())) as ArrayRef,
                ),
            ]);
            let mut m = base_mapping();
            m.trace_id_column = Some("trace_id".to_string());
            m.span_id_column = Some("span_id".to_string());
            m.resource_attributes = vec![attr("service.name", "svc", ColType::Str)];

            let (_plain_dir, plain) = write_with(&b, false);
            let (_dict_dir, dict) = write_with(&b, true);
            let id_types = |pq: &Path| -> Vec<DataType> {
                let schema = reader_schema_for(pq);
                ["trace_id", "span_id"]
                    .iter()
                    .map(|name| {
                        schema.as_ref().map_or(DataType::Utf8, |s| {
                            s.field_with_name(name)
                                .expect("id field")
                                .data_type()
                                .clone()
                        })
                    })
                    .collect()
            };
            let dict_ty = DataType::Dictionary(Box::new(DICT_KEY_TYPE), Box::new(DataType::Utf8));
            assert_eq!(id_types(&plain), vec![DataType::Utf8, DataType::Utf8]);
            assert_eq!(
                id_types(&dict),
                vec![dict_ty.clone(), dict_ty],
                "the loader reads both id columns of the encoded file as dictionaries"
            );

            let (plain_result, plain_store) = load_columnar(&plain, &m).await;
            let plain_report = plain_result.expect("the plain file loads");
            let (dict_result, dict_store) = load_columnar(&dict, &m).await;
            let dict_report = dict_result.expect("the dictionary-encoded file loads");
            assert_eq!(plain_report.rows_processed, 6);
            assert_eq!(dict_report.rows_processed, 6);
            assert!(
                dict_report.columnar_batches_built > 0,
                "the columnar path ran"
            );

            let want: Vec<StoredIds> = (0..6)
                .map(|i| (ts[i], trace[i].and_then(id), span[i].and_then(id)))
                .collect();
            assert_eq!(stored_ids(plain_store.as_ref()).await, want);
            assert_eq!(
                stored_ids(dict_store.as_ref()).await,
                want,
                "the encoded file stores the same ids, and none for a null key"
            );
            assert_eq!(
                decoded_records(dict_store.as_ref()).await,
                decoded_records(plain_store.as_ref()).await,
                "every stored field matches the plain load"
            );
        }

        fn logs_mapping_millis() -> Mapping {
            let mut m = base_mapping();
            m.ts_unit = TsUnit::Millis;
            m
        }

        fn negative_ts_file() -> (tempfile::TempDir, PathBuf) {
            let now_ms = NOW_NS / 1_000_000;
            write_with(
                &batch(vec![("ts", i64_col(vec![now_ms, -5, now_ms]))]),
                false,
            )
        }

        const NEGATIVE_MILLIS: &str = "timestamp is before the Unix epoch (-5000000 ns, read as \
                                       ts_unit = millis); the column holds a negative value";

        /// The logs row path refuses a negative resolved timestamp as a row
        /// rejection naming the declared unit, and stores nothing.
        #[tokio::test]
        async fn the_logs_row_path_refuses_a_negative_timestamp() {
            let (_dir, pq) = negative_ts_file();
            let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
            let err = load_row(
                Arc::clone(&store),
                &pq,
                "acme",
                &logs_mapping_millis(),
                1,
                1_000,
                None,
                NOW_NS,
                Arc::new(FixedClock(NOW_NS)),
            )
            .await
            .expect_err("a negative timestamp is refused");
            let LoadError::RowRejected { row, reason, .. } = &err else {
                panic!("expected RowRejected, got {err:?}");
            };
            assert_eq!(*row, 1);
            assert_eq!(reason, NEGATIVE_MILLIS);
            assert!(list_data_objects(store.as_ref()).await.is_empty());
        }

        /// The same refusal on the columnar path.
        #[tokio::test]
        async fn the_logs_columnar_path_refuses_a_negative_timestamp() {
            let (_dir, pq) = negative_ts_file();
            let (result, store) = load_columnar(&pq, &logs_mapping_millis()).await;
            let err = result.expect_err("a negative timestamp is refused");
            let LoadError::RowRejected { row, reason, .. } = &err else {
                panic!("expected RowRejected, got {err:?}");
            };
            assert_eq!(*row, 1);
            assert_eq!(reason, NEGATIVE_MILLIS);
            assert!(list_data_objects(store.as_ref()).await.is_empty());
        }

        /// The same refusal on the metrics path.
        #[tokio::test]
        async fn the_metrics_path_refuses_a_negative_timestamp() {
            let mapping = parse_metrics_mapping(
                "[metrics]\nname = \"probe\"\nvalue_column = \"value\"\nts_column = \
                 \"ts\"\nts_unit = \"millis\"\nkind = \"gauge\"\n",
            )
            .expect("valid mapping");
            let now_ms = NOW_NS / 1_000_000;
            let (_dir, pq) = write_with(
                &batch(vec![
                    ("ts", i64_col(vec![now_ms, -5, now_ms])),
                    (
                        "value",
                        Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])) as ArrayRef,
                    ),
                ]),
                false,
            );
            let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
            let err = load_metrics(
                Arc::clone(&store),
                &pq,
                "acme",
                &mapping,
                1,
                1_000,
                0,
                1,
                DEFAULT_MAX_INFLIGHT_FLUSHES,
                1,
                None,
                NOW_NS,
                Arc::new(FixedClock(NOW_NS)),
            )
            .await
            .expect_err("a negative timestamp is refused");
            let LoadError::RowRejected { row, reason, .. } = &err else {
                panic!("expected RowRejected, got {err:?}");
            };
            assert_eq!(*row, 1);
            assert_eq!(reason, NEGATIVE_MILLIS);
            assert!(list_data_objects(store.as_ref()).await.is_empty());
        }

        async fn load_logs_row(
            pq: &Path,
            mapping: &Mapping,
        ) -> (Result<LoadReport, LoadError>, Arc<dyn ObjectStoreBackend>) {
            let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
            let result = load_row(
                Arc::clone(&store),
                pq,
                "acme",
                mapping,
                1,
                1_000,
                None,
                NOW_NS,
                Arc::new(FixedClock(NOW_NS)),
            )
            .await;
            (result, store)
        }

        /// A gauge mapping over `ts` and `value`, declaring `ts_unit`.
        fn metrics_mapping(ts_unit: &str) -> MetricsMapping {
            parse_metrics_mapping(&format!(
                "[metrics]\nname = \"probe\"\nvalue_column = \"value\"\nts_column = \
                 \"ts\"\nts_unit = \"{ts_unit}\"\nkind = \"gauge\"\n"
            ))
            .expect("valid mapping")
        }

        async fn load_metrics_on(
            pq: &Path,
            mapping: &MetricsMapping,
        ) -> (
            Result<MetricsLoadReport, LoadError>,
            Arc<dyn ObjectStoreBackend>,
        ) {
            let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
            let result = load_metrics(
                Arc::clone(&store),
                pq,
                "acme",
                mapping,
                1,
                1_000,
                0,
                1,
                DEFAULT_MAX_INFLIGHT_FLUSHES,
                1,
                None,
                NOW_NS,
                Arc::new(FixedClock(NOW_NS)),
            )
            .await;
            (result, store)
        }

        /// A file whose `ts` is `ts` beside a `value` column, one row per cell.
        fn ts_file(ts: ArrayRef) -> (tempfile::TempDir, PathBuf) {
            let values: Vec<f64> = (0..ts.len()).map(|i| i as f64).collect();
            write_with(
                &batch(vec![
                    ("ts", ts),
                    ("value", Arc::new(Float64Array::from(values)) as ArrayRef),
                ]),
                false,
            )
        }

        /// A native `Timestamp(Second)` column scales by its own unit, not by
        /// the declared `ts_unit`, so the refusal names the unit that was
        /// applied: `-5` seconds is `-5000000000` ns, and `nanos` is not what
        /// produced it.
        const NEGATIVE_NATIVE_SECONDS: &str = "timestamp is before the Unix epoch (-5000000000 \
                                               ns, read in the column's own Timestamp unit, \
                                               seconds); the column holds a negative value";

        fn native_seconds_negative_file() -> (tempfile::TempDir, PathBuf) {
            let now_s = NOW_NS / 1_000_000_000;
            ts_file(Arc::new(TimestampSecondArray::from(vec![now_s, -5, now_s])) as ArrayRef)
        }

        fn assert_native_seconds_refusal(err: &LoadError) {
            let LoadError::RowRejected { row, reason, .. } = err else {
                panic!("expected RowRejected, got {err:?}");
            };
            assert_eq!(*row, 1);
            assert_eq!(reason, NEGATIVE_NATIVE_SECONDS);
        }

        /// The logs and metrics refusal spells every native Arrow unit, and an
        /// integer column's declared unit, exactly as the mapping writes it.
        #[test]
        fn the_logs_and_metrics_refusal_spells_every_unit() {
            let native = |unit: TimeUnit| {
                negative_ts_rejection(-5, &DataType::Timestamp(unit, None), TsUnit::Nanos)
            };
            for (unit, name) in [
                (TimeUnit::Second, "seconds"),
                (TimeUnit::Millisecond, "millis"),
                (TimeUnit::Microsecond, "micros"),
                (TimeUnit::Nanosecond, "nanos"),
            ] {
                assert_eq!(
                    native(unit),
                    format!(
                        "timestamp is before the Unix epoch (-5 ns, read in the column's own \
                         Timestamp unit, {name}); the column holds a negative value"
                    )
                );
            }
            assert_eq!(
                negative_ts_rejection(-5_000_000, &DataType::Int64, TsUnit::Millis),
                NEGATIVE_MILLIS
            );
        }

        #[tokio::test]
        async fn a_native_timestamp_refusal_names_the_column_unit_on_every_path() {
            let (_dir, pq) = native_seconds_negative_file();
            let schema = reader_schema_for(&pq);
            let ts_type = schema.as_ref().map_or_else(
                || {
                    let file = std::fs::File::open(&pq).expect("open parquet");
                    parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
                        .expect("reader")
                        .schema()
                        .field_with_name("ts")
                        .expect("ts field")
                        .data_type()
                        .clone()
                },
                |s| {
                    s.field_with_name("ts")
                        .expect("ts field")
                        .data_type()
                        .clone()
                },
            );
            assert_eq!(
                ts_type,
                DataType::Timestamp(TimeUnit::Second, None),
                "the loader reads a native seconds column"
            );
            let mut logs = base_mapping();
            logs.ts_unit = TsUnit::Nanos;

            let (result, store) = load_logs_row(&pq, &logs).await;
            assert_native_seconds_refusal(&result.expect_err("row path refuses"));
            assert!(list_data_objects(store.as_ref()).await.is_empty());

            let (result, store) = load_columnar(&pq, &logs).await;
            assert_native_seconds_refusal(&result.expect_err("columnar path refuses"));
            assert!(list_data_objects(store.as_ref()).await.is_empty());

            let (result, store) = load_metrics_on(&pq, &metrics_mapping("nanos")).await;
            assert_native_seconds_refusal(&result.expect_err("metrics path refuses"));
            assert!(list_data_objects(store.as_ref()).await.is_empty());
        }

        /// A timestamp of exactly 0 is the epoch, not before it: every path
        /// loads it.
        fn zero_ts_file() -> (tempfile::TempDir, PathBuf) {
            ts_file(i64_col(vec![NOW_NS, 0, NOW_NS]))
        }

        #[tokio::test]
        async fn the_logs_row_path_accepts_a_zero_timestamp() {
            let (_dir, pq) = zero_ts_file();
            let (result, store) = load_logs_row(&pq, &base_mapping()).await;
            let report = result.expect("a zero timestamp loads");
            assert_eq!(report.rows_processed, 3);
            let ts: Vec<i64> = stored_ids(store.as_ref())
                .await
                .into_iter()
                .map(|(ts, _, _)| ts)
                .collect();
            assert_eq!(ts, vec![0, NOW_NS, NOW_NS], "the zero row is stored at 0");
        }

        #[tokio::test]
        async fn the_logs_columnar_path_accepts_a_zero_timestamp() {
            let (_dir, pq) = zero_ts_file();
            let (result, store) = load_columnar(&pq, &base_mapping()).await;
            let report = result.expect("a zero timestamp loads");
            assert_eq!(report.rows_processed, 3);
            assert!(report.columnar_batches_built > 0, "the columnar path ran");
            let ts: Vec<i64> = stored_ids(store.as_ref())
                .await
                .into_iter()
                .map(|(ts, _, _)| ts)
                .collect();
            assert_eq!(ts, vec![0, NOW_NS, NOW_NS], "the zero row is stored at 0");
        }

        #[tokio::test]
        async fn the_metrics_path_accepts_a_zero_timestamp() {
            let (_dir, pq) = zero_ts_file();
            let (result, _store) = load_metrics_on(&pq, &metrics_mapping("nanos")).await;
            let report = result.expect("a zero timestamp loads");
            assert_eq!(report.rows_processed, 3, "every row, the zero one included");
        }
    }

    mod empty_dictionary_chunk_file {
        use std::path::PathBuf;

        use parquet::column::page::{CompressedPage, Page, PageWriteSpec, PageWriter};
        use parquet::column::writer::{get_column_writer, get_typed_column_writer};
        use parquet::data_type::{ByteArray, ByteArrayType, Int64Type};
        use parquet::file::properties::WriterProperties;
        use parquet::file::writer::{SerializedFileWriter, SerializedPageWriter, TrackedWrite};
        use parquet::schema::parser::parse_message_type;
        use ravel_object_store::memory::MemoryStore;

        use super::*;

        /// Passes every page through except the dictionary page, which it
        /// replaces with one holding no values. The data pages still carry the
        /// keys the real dictionary answered, so a non-null key names a value
        /// the written dictionary does not have.
        struct EmptyDictionaryPage<P> {
            inner: P,
            empty: bool,
        }

        impl<P: PageWriter> PageWriter for EmptyDictionaryPage<P> {
            fn write_page(
                &mut self,
                page: CompressedPage,
            ) -> parquet::errors::Result<PageWriteSpec> {
                let Page::DictionaryPage {
                    encoding,
                    is_sorted,
                    ..
                } = page.compressed_page()
                else {
                    return self.inner.write_page(page);
                };
                if !self.empty {
                    return self.inner.write_page(page);
                }
                let empty = Page::DictionaryPage {
                    buf: bytes::Bytes::new(),
                    num_values: 0,
                    encoding: *encoding,
                    is_sorted: *is_sorted,
                };
                self.inner.write_page(CompressedPage::new(empty, 0))
            }

            fn close(&mut self) -> parquet::errors::Result<()> {
                self.inner.close()
            }
        }

        /// A three-row file: `ts` plain, and `svc` dictionary-encoded with keys
        /// `[0, null, 0]`, under an empty dictionary page when `empty_dictionary`
        /// is set and under its real one-value dictionary otherwise. The `svc`
        /// chunk is written by a column writer over [`EmptyDictionaryPage`] and
        /// spliced into the row group whole.
        fn write_file(empty_dictionary: bool) -> (tempfile::TempDir, PathBuf) {
            let dir = tempfile::tempdir().expect("tempdir");
            let pq = dir.path().join("in.parquet");
            let schema = Arc::new(
                parse_message_type(
                    "message log { required int64 ts; optional binary svc (UTF8); }",
                )
                .expect("schema"),
            );
            let props = Arc::new(
                WriterProperties::builder()
                    .set_dictionary_enabled(true)
                    .build(),
            );
            let file = std::fs::File::create(&pq).expect("create parquet");
            let mut writer =
                SerializedFileWriter::new(file, schema, Arc::clone(&props)).expect("file writer");
            let svc_descr = writer.schema_descr().column(1);
            let mut rg = writer.next_row_group().expect("row group");

            let mut ts = rg.next_column().expect("ts column").expect("ts writer");
            ts.typed::<Int64Type>()
                .write_batch(&[NOW_NS, NOW_NS + 1, NOW_NS + 2], None, None)
                .expect("write ts");
            ts.close().expect("close ts");

            let mut chunk = TrackedWrite::new(Vec::new());
            let close = {
                let pages = EmptyDictionaryPage {
                    inner: SerializedPageWriter::new(&mut chunk),
                    empty: empty_dictionary,
                };
                let mut svc = get_typed_column_writer::<ByteArrayType>(get_column_writer(
                    svc_descr,
                    props,
                    Box::new(pages),
                ));
                svc.write_batch(
                    &[ByteArray::from("api"), ByteArray::from("api")],
                    Some(&[1, 0, 1]),
                    None,
                )
                .expect("write svc");
                svc.close().expect("close svc")
            };
            let chunk = bytes::Bytes::from(chunk.into_inner().expect("chunk bytes"));
            rg.append_column(&chunk, close).expect("splice svc");
            rg.close().expect("close row group");
            writer.close().expect("close file");
            (dir, pq)
        }

        fn svc_mapping() -> Mapping {
            let mut m = base_mapping();
            m.resource_attributes = vec![attr("service.name", "svc", ColType::Str)];
            m
        }

        async fn load_on(
            path: LoadPath,
            pq: &Path,
        ) -> (Result<LoadReport, LoadError>, Arc<dyn ObjectStoreBackend>) {
            let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
            let result = load_instrumented(
                Arc::clone(&store),
                pq,
                "acme",
                &svc_mapping(),
                1,
                1_000,
                0,
                None,
                1,
                DEFAULT_MAX_INFLIGHT_FLUSHES,
                DEFAULT_DECODE_QUEUE_BATCHES,
                DEFAULT_TARGET_BYTES,
                None,
                NOW_NS,
                Arc::new(FixedClock(NOW_NS)),
                path,
                None,
                None,
            )
            .await;
            (result, store)
        }

        /// The loader reads `svc` as a dictionary column, so the batch this
        /// file would decode to is the shape `str_src` and
        /// `resolve_dictionary_column` answer differently.
        fn assert_read_as_dictionary(pq: &Path) {
            let schema = reader_schema_for(pq).expect("a dictionary-preserving schema");
            assert_eq!(
                schema
                    .field_with_name("svc")
                    .expect("svc field")
                    .data_type(),
                &DataType::Dictionary(Box::new(DICT_KEY_TYPE), Box::new(DataType::Utf8)),
            );
        }

        /// An empty dictionary page under a non-null key never reaches either
        /// load path as a batch: the Parquet reader itself fails the decode,
        /// with the same batch refusal on the row path and the columnar path,
        /// and nothing is stored by either.
        #[tokio::test]
        async fn both_paths_refuse_an_empty_dictionary_page_under_a_key() {
            let (_dir, pq) = write_file(true);
            assert_read_as_dictionary(&pq);

            let mut reasons = Vec::new();
            for path in [LoadPath::Row, LoadPath::Columnar] {
                let (result, store) = load_on(path, &pq).await;
                let err = result.expect_err("the corrupt file is refused");
                let LoadError::BatchFailed { reason, .. } = &err else {
                    panic!("expected BatchFailed on {path:?}, got {err:?}");
                };
                assert!(
                    reason.starts_with("failed to read Parquet batch: "),
                    "the reader refuses the chunk on {path:?}: {reason}"
                );
                assert!(list_data_objects(store.as_ref()).await.is_empty());
                reasons.push(reason.clone());
            }
            assert_eq!(reasons[0], reasons[1], "both paths refuse identically");
        }

        /// The control: the same writer with the dictionary page left intact
        /// loads all three rows on both paths, so the refusal above comes from
        /// the emptied dictionary and not from the spliced chunk.
        #[tokio::test]
        async fn the_same_file_with_its_dictionary_loads_on_both_paths() {
            let (_dir, pq) = write_file(false);
            assert_read_as_dictionary(&pq);

            let mut stored = Vec::new();
            for path in [LoadPath::Row, LoadPath::Columnar] {
                let (result, store) = load_on(path, &pq).await;
                let report = result.expect("the intact file loads");
                assert_eq!(report.rows_processed, 3, "every row loads on {path:?}");
                stored.push(decoded_records(store.as_ref()).await);
            }
            assert_eq!(stored[0], stored[1], "both paths store the same records");
        }
    }
}
