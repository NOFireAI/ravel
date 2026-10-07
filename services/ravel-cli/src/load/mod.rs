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
use arrow::array::{BinaryArray, FixedSizeBinaryArray, MapArray, new_null_array};
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
    LogWriteError, LogWriteReceipt, RlogZstdLevel, STRICT_VISIBILITY_RESERVE_NS, SpanIngestRouter,
    SpanWriteError, SpanWriteReceipt, SystemClock, WriteError, WriteMode, WriteReceipt,
};
use ravel_logseg::{Bitmap, ColumnarLogBatch, DynCells, DynColumn, FieldType, stream_attrs_bytes};
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

mod mapping;
pub use mapping::*;
mod cli;
pub use cli::*;
mod logs;
pub use logs::*;
mod input;
pub use input::*;
mod columns;
use columns::*;
mod columnar;
use columnar::*;
mod metrics;
pub use metrics::*;
mod sequential;
use sequential::*;
mod spans;
pub use spans::*;

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
    /// time the check at [`SKEW_CHECK_AFTER_BATCHES`](logs::SKEW_CHECK_AFTER_BATCHES) data batches finds the
    /// spread at or below the [`SKEW_WARN_DENOMINATOR`](logs::SKEW_WARN_DENOMINATOR) threshold. `None` when
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
/// machine-readable form of the same figures [`print_summary`](cli::print_summary) prints. Object
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
    /// ([`harvest_after_failure`](logs::harvest_after_failure) recovers those as tokens, not as rows),
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
    /// landed. See [`print_durable_tokens`](cli::print_durable_tokens).
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
    /// fold into it ([`harvest_after_failure`](logs::harvest_after_failure)). `None` for
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

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod test_support;

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests;
