//! The `load` command driver: per-signal entry points, operator warnings,
//! the end-of-load summaries and the resume hints.

use super::*;

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
/// but against the buffer's estimated *uncompressed object content*
/// (`est_record_object_bytes`/`est_columnar_object_bytes` in
/// crates/ravel-ingest/src/log_shard.rs): body, severity text, stream
/// attributes, attribute names and values, and fixed per-row fields. Stored
/// objects are compressed, so a target read off an observed object size is
/// several times below the content of the rows that object holds. On top of
/// that the comparison runs once per write, after a whole batch's per-shard
/// slice has merged, so any target at or below one slice's content
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
         depth and write cadence before changing the target. Separately, the target is compared \
         against the buffer's estimated UNCOMPRESSED object content (body, severity text, stream \
         attributes, attribute names and values, fixed per-row fields), not the stored object \
         size, and the check runs once per write after a whole batch has merged. So a target at \
         or below one batch's per-shard slice (about {slice} rows here, at --batch-rows \
         {batch_rows} over {shards} shards) is already exceeded by the first write into an empty \
         buffer and flushes it. For objects that span several batches, raise --target-bytes \
         above that slice's estimated content, or lower --batch-rows.",
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
    zstd_level: RlogZstdLevel,
    load_memory_bytes: Option<u64>,
    fold_after_load: bool,
    now_ns: i64,
) -> anyhow::Result<()> {
    run_fold_warning_to(
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
        zstd_level,
        LoadMemoryRequest::from_flag_on_host(load_memory_bytes),
        fold_after_load,
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
#[cfg(test)]
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
    zstd_level: RlogZstdLevel,
    load_memory_bytes: Option<u64>,
    now_ns: i64,
    warnings: &mut dyn std::io::Write,
) -> anyhow::Result<()> {
    run_memory_warning_to(
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
        zstd_level,
        LoadMemoryRequest::from_flag_on_host(load_memory_bytes),
        now_ns,
        warnings,
    )
    .await
}

/// [`run_warning_to`] with the memory request injected in place of the host
/// read, so a test can give a logs load a derived budget below one batch.
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_memory_warning_to(
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
    zstd_level: RlogZstdLevel,
    load_memory: LoadMemoryRequest,
    now_ns: i64,
    warnings: &mut dyn std::io::Write,
) -> anyhow::Result<()> {
    run_fold_warning_to(
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
        zstd_level,
        load_memory,
        false,
        now_ns,
        warnings,
    )
    .await
}

/// [`run_memory_warning_to`] with `--fold-after-load` (ADR-2677 decision 1).
/// Only a logs load folds; a metrics or spans load given the flag is refused
/// before anything is written.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_fold_warning_to(
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
    zstd_level: RlogZstdLevel,
    load_memory: LoadMemoryRequest,
    fold_after_load: bool,
    now_ns: i64,
    warnings: &mut dyn std::io::Write,
) -> anyhow::Result<()> {
    let refused = match signal {
        SignalArg::Metrics => Some("metrics"),
        SignalArg::Spans => Some("spans"),
        SignalArg::Logs => None,
    };
    if let (true, Some(name)) = (fold_after_load, refused) {
        anyhow::bail!(
            "--fold-after-load supports only --signal logs; this {name} load was refused before \
             any row was read or written. Load without it, then seal the loaded hours with \
             `ravel-cli catalog fold --signal {name} --writers-stopped` once the load has exited"
        );
    }
    let load_memory_bytes = match load_memory {
        LoadMemoryRequest::Flag(bytes) => Some(bytes),
        LoadMemoryRequest::Derived { .. } | LoadMemoryRequest::Unbudgeted => None,
    };
    // A diagnostic that cannot be written is not worth failing a durable load
    // over, here or below.
    let admission_warning = match signal {
        SignalArg::Metrics => METRICS_ADMISSION_BYPASS_WARNING,
        SignalArg::Spans => SPANS_ADMISSION_BYPASS_WARNING,
        SignalArg::Logs => ADMISSION_BYPASS_WARNING,
    };
    let _ = writeln!(warnings, "{admission_warning}");
    if let Some(warning) = unused_zstd_level_warning(zstd_level, signal) {
        let _ = writeln!(warnings, "{warning}");
    }
    if let Some(warning) = unused_load_memory_warning(load_memory_bytes, signal) {
        let _ = writeln!(warnings, "{warning}");
    }

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

    // The loader resolves the budget once it knows the read-cursor count the
    // floor depends on.
    if let Some(warning) = load_memory.fallback_warning() {
        let _ = writeln!(warnings, "{warning}");
    }
    let memory_options = LoadMemoryOptions::new(load_memory);
    let one_batch_warning = Arc::clone(&memory_options.one_batch_warning);

    // The production entry point drives the columnar fast path (ADR-0109) with
    // the operator-configured decode-queue depth; `load` keeps a stable
    // signature for tests and callers that want the default depth.
    match load_instrumented_at(
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
        zstd_level,
        Some(memory_options),
        fold_after_load,
    )
    .await
    {
        Ok(report) => {
            print_summary(&report);
            if let Some(warning) = &report.load_memory_warning {
                let _ = writeln!(warnings, "{warning}");
            }
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
            // The loader's writer uses `RlogConfig::default()` apart from its zstd
            // level (log_shard.rs), so its per-object dynamic-column budget is
            // that default.
            let max_dynamic_columns = ravel_logseg::RlogConfig::default().max_dynamic_columns;
            for warning in dynamic_column_warnings(&report.metrics, max_dynamic_columns) {
                let _ = writeln!(warnings, "{warning}");
            }
            Ok(())
        }
        Err(err) => {
            print_durable_tokens(&err, LOGS_RESUMABLE_SETTINGS);
            // The failed load dropped the report that carried this warning, and
            // it still explains the run: the load went one batch at a time.
            if let Some(warning) = one_batch_warning.get() {
                let _ = writeln!(warnings, "{warning}");
            }
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
pub(super) const READ_CURSORS_ZERO: &str = "--read-cursors must be at least 1, or omitted for automatic \
                                 sizing (min(shard count, row-group count)); 0 was given";

/// `--decode-queue-batches 0`, rejected on both signals.
pub(super) const DECODE_QUEUE_BATCHES_ZERO: &str = "--decode-queue-batches must be at least 1 (the number \
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
pub(super) fn unused_lever_warning(
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

/// The warning for a `--zstd-level` a metrics or spans load cannot use, or
/// `None` when the level is the default or the load is a logs load. The level
/// reaches only the RLOG writer a logs flush runs.
pub(super) fn unused_zstd_level_warning(
    zstd_level: RlogZstdLevel,
    signal: SignalArg,
) -> Option<String> {
    let noun = match signal {
        SignalArg::Logs => return None,
        SignalArg::Metrics => "metrics",
        SignalArg::Spans => "spans",
    };
    (zstd_level != RlogZstdLevel::DEFAULT).then(|| {
        format!(
            "warning: a {noun} load ignores --zstd-level {}. The level applies only to the \
             RLOG objects a logs load writes.",
            zstd_level.get()
        )
    })
}

/// The warning for a `--load-memory-bytes` a metrics or spans load cannot
/// use, or `None` when the flag is unset or the load is a logs load. Only the
/// logs loader's columnar path charges its batches to the budget.
pub(super) fn unused_load_memory_warning(
    load_memory_bytes: Option<u64>,
    signal: SignalArg,
) -> Option<String> {
    let noun = match signal {
        SignalArg::Logs => return None,
        SignalArg::Metrics => "metrics",
        SignalArg::Spans => "spans",
    };
    load_memory_bytes.map(|bytes| {
        format!(
            "warning: a {noun} load ignores --load-memory-bytes {bytes}. The budget applies \
             only to the batches a logs load holds."
        )
    })
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

/// Print the attributes a spans load dropped for a value over the OTLP
/// value-length cap or an `attrs_map_column` key over the key-length cap.
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
pub(super) enum AttrsDroppedScope {
    /// Every decoded batch was written and acknowledged.
    Complete,
    /// The load failed, so some decoded batches never landed.
    Failed,
}

/// The `attrs_dropped` summary line, as [`print_spans_attrs_dropped`] prints it.
pub(super) fn spans_attrs_dropped_line(
    report: &SpansLoadReport,
    scope: AttrsDroppedScope,
) -> String {
    let tail = match scope {
        AttrsDroppedScope::Complete => "each span was stored without them",
        AttrsDroppedScope::Failed => {
            "counted where each span was built, so this includes batches the failure abandoned, \
             whose spans are in no object"
        }
    };
    format!(
        "  attrs_dropped    : {} (attribute values over the OTLP value-length cap and \
         attrs_map_column keys over the key-length cap; {tail})",
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
pub(super) fn skip_rows_past_end_warning(
    skip_rows_requested: u64,
    file_total_rows: u64,
) -> Option<String> {
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
    if let Some(fold) = &report.fold {
        print!("{}", fold_summary(fold));
    }
    let memory = &report.load_memory;
    // A resolved budget always has a nonzero floor; a derived budget can be 0.
    if memory.floor_bytes > 0 {
        println!(
            "  load memory      : budget {} bytes ({}), floor {} bytes, peak {} bytes, \
             largest batch {} bytes, decoder waits {}",
            memory.budget_bytes,
            memory.source_label(),
            memory.floor_bytes,
            report.load_memory_peak_bytes,
            report.load_memory_max_batch_bytes,
            report.load_memory_waits,
        );
    }
    print_flush_mix(report);
    #[cfg(feature = "stage-timing")]
    print_stage_timings(report);
}

/// The `--fold-after-load` lines of the summary (ADR-2677 decision 1). The
/// fold's elapsed is part of the `elapsed` line above it, not added to it.
pub(super) fn fold_summary(fold: &LoadFold) -> String {
    if fold.seal_through_hour.is_none() {
        return "  fold after load  : no fold: nothing was written\n".to_string();
    }
    let hour = |h: Option<u32>| h.map_or_else(|| "none".to_string(), |h| h.to_string());
    let outcome = if fold.no_op {
        "no-op, HEAD already sealed"
    } else {
        "sealed"
    };
    format!(
        "  fold after load  : {outcome}, seal_through_hour {}, watermark_hour {}, entries {}, \
         elapsed {:.3}s (included in elapsed)\n",
        hour(fold.seal_through_hour),
        hour(fold.watermark_hour),
        fold.entry_count,
        fold.elapsed.as_secs_f64(),
    )
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
    // A `--fold-after-load` failure happens after every row is durable: the
    // load itself finished, so its summary is printed and nothing is partial.
    if let Some(report) = err.finished_report() {
        print_summary(report);
        println!(
            "{} commit token(s)/segment(s) are durable (the whole file loaded; only the fold \
             after it failed):",
            tokens.len()
        );
        for token in tokens {
            println!("  {}", token.encode());
        }
        return;
    }
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
pub(super) fn resume_hint(
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
pub(super) fn sequential_resume_hint(err: &LoadError, pipeline_depth: usize) -> Option<String> {
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

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests;
