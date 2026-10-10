//! The logs load pipeline: decode stages, stride cursors, write submission
//! and draining the in-flight writes.

use super::*;

const NS_PER_HOUR: i64 = 3_600_000_000_000;

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

/// [`load`] followed by `--fold-after-load` (ADR-2677 decision 1): once every
/// write is acked the router is shut down, so no flush can follow, and the
/// logs snapshot is folded through the highest ingest hour the load wrote,
/// with a fresh reading of `clock` as the fold's time. The fold's figures are
/// in [`LoadReport::fold`] and its time is inside [`LoadReport::elapsed`].
/// A fold that fails fails the load with [`LoadError::Fold`]; the loaded
/// objects are durable either way.
#[allow(clippy::too_many_arguments)]
pub async fn load_with_fold_after_load(
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
    load_instrumented_at(
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
        RlogZstdLevel::DEFAULT,
        None,
        true,
    )
    .await
}

/// [`load`] with the object-size levers and the memory budget given: the
/// flush target (`--target-bytes`), the age trigger (`--max-flush-delay`,
/// `None` = the default) and the memory budget's inputs (see
/// [`LoadMemory::resolve`]).
#[allow(clippy::too_many_arguments)]
pub async fn load_with_memory(
    store: Arc<dyn ObjectStoreBackend>,
    parquet_path: &Path,
    tenant: &str,
    mapping: &Mapping,
    shards: u32,
    batch_rows: usize,
    read_cursors: Option<usize>,
    pipeline_depth: usize,
    target_bytes: usize,
    max_flush_delay: Option<Duration>,
    load_memory: LoadMemoryOptions,
    now_ns: i64,
    clock: Arc<dyn Clock>,
) -> Result<LoadReport, LoadError> {
    load_instrumented_at(
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
        target_bytes,
        max_flush_delay,
        now_ns,
        clock,
        LoadPath::Columnar,
        None,
        None,
        RlogZstdLevel::DEFAULT,
        Some(load_memory),
        false,
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
pub(super) async fn load_instrumented(
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
    load_instrumented_at(
        store,
        parquet_path,
        tenant,
        mapping,
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
        clock,
        path,
        on_build_start,
        on_batch_queued,
        RlogZstdLevel::DEFAULT,
        None,
        false,
    )
    .await
}

/// [`load_instrumented`] with the `--zstd-level` lever: `zstd_level` becomes
/// the router's [`IngestConfig::rlog_zstd_level`], the level every page and
/// section of each object the load writes compresses at. `load_memory` is the
/// memory budget's inputs, resolved once the read-cursor count is known;
/// `None` derives the budget from this host's memory. `fold_after_load` is
/// the `--fold-after-load` flag ([`load_with_fold_after_load`]).
#[allow(clippy::too_many_arguments)]
pub(super) async fn load_instrumented_at(
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
    zstd_level: RlogZstdLevel,
    load_memory: Option<LoadMemoryOptions>,
    fold_after_load: bool,
) -> Result<LoadReport, LoadError> {
    load_with_drain_reflush_period(
        store,
        parquet_path,
        tenant,
        mapping,
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
        clock,
        path,
        on_build_start,
        on_batch_queued,
        zstd_level,
        DRAIN_REFLUSH_PERIOD,
        load_memory,
        fold_after_load,
    )
    .await
}

/// Period of the straggler re-flush ticker that runs while the final drain
/// waits on the in-flight window.
const DRAIN_REFLUSH_PERIOD: Duration = Duration::from_secs(2);

/// [`load_instrumented_at`] with the drain-time re-flush ticker's period
/// injected, so a test can push the ticker out of reach and attribute the
/// tail's publication to the end-of-input `flush_all` alone.
#[allow(clippy::too_many_arguments)]
pub(super) async fn load_with_drain_reflush_period(
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
    zstd_level: RlogZstdLevel,
    drain_reflush_period: Duration,
    load_memory: Option<LoadMemoryOptions>,
    fold_after_load: bool,
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
    // than 1, it is the same one (an estimate `>= 0` holds for an empty buffer),
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

    // Before the provisioning check below, which can write a record.
    if fold_after_load {
        refuse_a_sealed_current_hour(
            Arc::clone(&store),
            tenant,
            &tenant_id,
            shards,
            clock.now_ns(),
        )
        .await?;
    }

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
    // "Larger" is measured against the shard's uncompressed object-content
    // estimate, not the encoded object, and tested once per write after a whole
    // batch's slice has merged: below one slice's content the target is unreachable
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
        IngestConfig {
            rlog_zstd_level: zstd_level,
            ..build_ingest_config(shards, target_bytes, max_inflight_flushes, max_flush_delay)
        },
        Arc::clone(&store),
        Arc::clone(&clock),
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
    let cursors = open_stride_cursors(
        &input,
        &metadata,
        &row_group_lens,
        cursor_count,
        batch_rows,
        reader_batch_rows(batch_rows, cursor_count),
    )?;

    // The memory budget (issue #2626) charges every built columnar batch from
    // before its build until the last flush carrying its rows finishes. The
    // row path is the differential reference and charges nothing. The floor
    // is sized with the resolved cursor count, not the flag.
    let memory_options = load_memory
        .unwrap_or_else(|| LoadMemoryOptions::new(LoadMemoryRequest::from_flag_on_host(None)));
    let load_memory = LoadMemory::resolve(
        memory_options.request,
        cursor_count,
        u64::from(shards) * u64::from(max_inflight_flushes),
    )
    .map_err(LoadError::Setup)?;
    let charger = match load_memory {
        Some(load_memory) if path == LoadPath::Columnar => {
            Some(BatchCharger::new(load_memory.budget()))
        }
        _ => None,
    };
    let budget = charger.as_ref().map(|c| Arc::clone(&c.budget));
    if let (Some(budget), Some(on_budget)) = (&budget, &memory_options.on_budget) {
        on_budget(budget);
    }
    let load_memory = load_memory.unwrap_or_default();

    let started = Instant::now();
    let mut report = LoadReport::default();
    if budget.is_some() {
        report.load_memory = load_memory;
    }
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
        charger,
    );

    // Above the default target a shard buffer holds charged batches until it
    // reaches the target or ages out. If the decoder is waiting for memory and
    // the held bytes do not drop for a whole period, nothing new can reach
    // those buffers, so only a flush can make room: flush them rather than leave the
    // load parked on the age trigger, or forever on a raised one. At the
    // default target every write flushes itself and this never runs, so the
    // default layout is unchanged.
    let stall_flusher = match &budget {
        Some(budget) if target_bytes > DEFAULT_TARGET_BYTES => Some(spawn_stall_flusher(
            Arc::clone(&router),
            Arc::clone(budget),
            memory_options.stall_flush_period,
        )),
        _ => None,
    };

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
            Built::Columnar(batch, charge) => {
                // A batch larger than the whole budget is admitted only alone,
                // so it holds the budget past its limit. An explicit budget
                // refuses it (the first batch before anything is written); a
                // derived one runs one batch at a time and says so once.
                if let Some(charge) = &charge
                    && charge.bytes() > load_memory.budget_bytes
                    && !load_memory.is_explicit()
                {
                    if report.load_memory_warning.is_none() {
                        let warning = load_memory.one_batch_warning(charge.bytes(), batch.num_rows);
                        let _ = memory_options.one_batch_warning.set(warning.clone());
                        report.load_memory_warning = Some(warning);
                    }
                } else if let Some(charge) = &charge
                    && charge.bytes() > load_memory.budget_bytes
                {
                    let reason = load_memory.batch_too_large(charge.bytes(), batch.num_rows);
                    if report.columnar_batches_built == 0 {
                        return Err(LoadError::Setup(reason));
                    }
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
                if let Some(charge) = &charge {
                    report.load_memory_max_batch_bytes =
                        report.load_memory_max_batch_bytes.max(charge.bytes());
                }
                // Reachability signal (ADR-0109): count each batch actually
                // driven through `write_columnar`, so a caller of the real entry
                // point can prove the columnar path ran.
                report.columnar_batches_built += 1;
                let router = Arc::clone(&router);
                let tenant = tenant_id.clone();
                tokio::spawn(async move {
                    match charge {
                        Some(charge) => {
                            router
                                .write_columnar_charged(
                                    tenant,
                                    *batch,
                                    WriteMode::Strict,
                                    ack_deadline,
                                    charge,
                                )
                                .await
                        }
                        None => {
                            router
                                .write_columnar(tenant, *batch, WriteMode::Strict, ack_deadline)
                                .await
                        }
                    }
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
                    () = tokio::time::sleep(drain_reflush_period) => {
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
    report.load_memory_peak_bytes = budget.as_ref().map_or(0, |b| b.peak_bytes());
    report.load_memory_waits = budget.as_ref().map_or(0, |b| b.waits_total());
    if fold_after_load {
        let fold_started = Instant::now();
        if let Some(stall_flusher) = stall_flusher {
            stall_flusher.stop().await;
        }
        let outcome = fold_after_load_through(
            store,
            router,
            clock.as_ref(),
            &tenant_id,
            shards,
            &report.tokens,
            fold_started,
        )
        .await;
        report.elapsed = started.elapsed();
        return match outcome {
            Ok(fold) => {
                report.fold = Some(fold);
                Ok(report)
            }
            Err(FoldFailure::Failed(cause)) => Err(LoadError::Fold {
                durable: report.tokens.clone(),
                cause,
                rerun: format!(
                    "ravel-cli catalog fold --tenant {} --shards {shards} --signal logs \
                     --writers-stopped",
                    shell_word(tenant)
                ),
                report: Box::new(report),
            }),
            Err(FoldFailure::Uncovered {
                fold,
                hours,
                finding,
            }) => {
                report.fold = Some(fold);
                Err(LoadError::FoldLeftCommitsUncovered {
                    durable: report.tokens.clone(),
                    hours: hours
                        .iter()
                        .map(u32::to_string)
                        .collect::<Vec<_>>()
                        .join(", "),
                    finding,
                    verify: format!(
                        "ravel-cli catalog verify --tenant {} --signal logs",
                        shell_word(tenant)
                    ),
                    report: Box::new(report),
                })
            }
        };
    }
    report.elapsed = started.elapsed();
    Ok(report)
}

/// The `--fold-after-load` preflight (ADR-2677 decision 1): refuse the load
/// when the logs catalog HEAD has already sealed the hour bucket of `now_ns`.
/// Only an earlier operator-asserted seal puts the watermark that high, and
/// every object this load would write falls in that hour or a later one, so
/// the part written before the next hour begins would be invisible to
/// queries that carry no commit token. An absent HEAD, or a watermark below
/// the current hour, passes.
pub(super) async fn refuse_a_sealed_current_hour(
    store: Arc<dyn ObjectStoreBackend>,
    tenant: &str,
    tenant_id: &TenantId,
    shards: u32,
    now_ns: i64,
) -> Result<(), LoadError> {
    let hour = u32::try_from(now_ns.div_euclid(NS_PER_HOUR)).map_err(|_| {
        LoadError::Setup(format!(
            "--fold-after-load: clock reading {now_ns} ns has no ingest-hour bucket"
        ))
    })?;
    let catalog = crate::catalog::enforcing_catalog(
        store,
        ravel_catalog::CatalogConfig {
            shard_count: shards.max(1),
            ..ravel_catalog::CatalogConfig::default()
        },
    )
    .map_err(|err| {
        LoadError::Setup(format!(
            "--fold-after-load: failed to build catalog for the preflight: {err}"
        ))
    })?;
    let watermark_hour = catalog
        .head_watermark_hour(&tenant_id.hash(), Signal::Logs)
        .await
        .map_err(|err| {
            LoadError::Setup(format!(
                "--fold-after-load: could not read the logs catalog HEAD for tenant {tenant:?}: \
                 {err}"
            ))
        })?;
    match watermark_hour {
        Some(watermark_hour) if watermark_hour >= hour => Err(LoadError::HourAlreadySealed {
            tenant: tenant.to_string(),
            hour,
            watermark_hour,
            first_open_hour: u64::from(watermark_hour) + 1,
        }),
        _ => Ok(()),
    }
}

/// `word` as one shell word: unchanged when it holds only characters no
/// shell treats specially, single-quoted otherwise. A leading `=` is quoted
/// because zsh expands `=name` to the path of the command `name`.
fn shell_word(word: &str) -> String {
    let plain = !word.is_empty()
        && !word.starts_with('=')
        && word
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "-_.:/@+=,".contains(c));
    if plain {
        word.to_string()
    } else {
        format!("'{}'", word.replace('\'', "'\\''"))
    }
}

/// Why the `--fold-after-load` step did not succeed. The caller attaches the
/// finished report and turns it into a [`LoadError`].
#[derive(Debug)]
enum FoldFailure {
    /// The fold could not run or returned an error.
    Failed(String),
    /// The fold ran, and the snapshot of the HEAD it left does not cover every
    /// commit this load wrote, or that could not be checked.
    Uncovered {
        fold: LoadFold,
        hours: Vec<u32>,
        finding: String,
    },
}

/// The most commits an uncovered-commits finding names one by one.
const UNCOVERED_COMMITS_NAMED: usize = 10;

/// The snapshot entry identity of the L0 commit `token` names.
fn token_identity(token: &CommitToken) -> ravel_catalog::EntryIdentity {
    (
        token.shard,
        token.ingest_hour_bucket,
        *token.writer_id.as_bytes(),
        token.epoch,
        token.seq,
    )
}

/// What a `--fold-after-load` coverage check over `tokens` found, as the
/// ingest hours to name and the sentence naming them; `None` when the
/// snapshot covers every token. A `coverage` error names every token hour,
/// since none of them could be confirmed.
fn uncovered_commits<E: std::fmt::Display>(
    tokens: &[CommitToken],
    coverage: Result<ravel_catalog::SnapshotCoverage, E>,
) -> Option<(Vec<u32>, String)> {
    let joined = |hours: &[u32]| {
        hours
            .iter()
            .map(u32::to_string)
            .collect::<Vec<_>>()
            .join(", ")
    };
    match coverage {
        Ok(coverage) if coverage.missing.is_empty() => None,
        Ok(coverage) => {
            let missing = &coverage.missing;
            let mut hours: Vec<u32> = missing.iter().map(|id| id.1).collect();
            hours.sort_unstable();
            hours.dedup();
            let mut named = missing
                .iter()
                .take(UNCOVERED_COMMITS_NAMED)
                .map(|(shard, hour, writer_id, epoch, seq)| {
                    format!(
                        "shard {shard} hour {hour} writer {} epoch {epoch} seq {seq}",
                        uuid::Uuid::from_bytes(*writer_id)
                    )
                })
                .collect::<Vec<_>>()
                .join("; ");
            if missing.len() > UNCOVERED_COMMITS_NAMED {
                named.push_str(&format!(
                    "; and {} more",
                    missing.len() - UNCOVERED_COMMITS_NAMED
                ));
            }
            let finding = if coverage.watermark_hour.is_none() {
                format!(
                    "{} of the {} commits it published are in no snapshot: the fold left no \
                     catalog HEAD, in ingest hour(s) {}: {named}",
                    missing.len(),
                    tokens.len(),
                    joined(&hours)
                )
            } else {
                format!(
                    "{} of the {} commits it published are not in the snapshot of the catalog \
                     HEAD its fold left, in ingest hour(s) {}: {named}",
                    missing.len(),
                    tokens.len(),
                    joined(&hours)
                )
            };
            Some((hours, finding))
        }
        Err(err) => {
            let mut hours: Vec<u32> = tokens.iter().map(|t| t.ingest_hour_bucket).collect();
            hours.sort_unstable();
            hours.dedup();
            let finding = format!(
                "whether the snapshot of the catalog HEAD its fold left covers the {} commits it \
                 published into ingest hour(s) {} could not be checked: {err}",
                tokens.len(),
                joined(&hours)
            );
            Some((hours, finding))
        }
    }
}

/// The `--fold-after-load` step (ADR-2677 decision 1), run once every write
/// has acked. Shutting the router down waits for every shard actor to finish
/// its last flush and closes its mailbox, which is what makes the loader's
/// assertion true: nothing it started can publish into an hour after the
/// fold seals it. The fold then seals the logs snapshot through the highest
/// ingest hour among `tokens`, at a fresh reading of `clock`. Last, the
/// snapshot of the HEAD the fold left is read back, and the step fails with
/// [`FoldFailure::Uncovered`] unless it covers every one of `tokens` (see
/// [`ravel_catalog::snapshot_coverage`]) or when it cannot be read. With no
/// tokens there is nothing to seal, no fold runs and nothing is read.
async fn fold_after_load_through(
    store: Arc<dyn ObjectStoreBackend>,
    router: Arc<LogIngestRouter>,
    clock: &dyn Clock,
    tenant_id: &TenantId,
    shards: u32,
    tokens: &[CommitToken],
    started: Instant,
) -> Result<LoadFold, FoldFailure> {
    // Every write task, the drain ticker and the stall flusher have been
    // joined by now, so this is the last handle.
    let router = Arc::try_unwrap(router).map_err(|_| {
        FoldFailure::Failed(
            "the ingest router is still shared, so it cannot be shut down".to_string(),
        )
    })?;
    router.shutdown().await;

    let token_hours: Vec<u32> = tokens.iter().map(|t| t.ingest_hour_bucket).collect();
    let Some(seal_through_hour) = token_hours.iter().copied().max() else {
        return Ok(LoadFold {
            elapsed: started.elapsed(),
            ..LoadFold::default()
        });
    };
    let now_ns = clock.now_ns();
    let coverage_store = Arc::clone(&store);
    let catalog = crate::catalog::enforcing_catalog(
        store,
        ravel_catalog::CatalogConfig {
            shard_count: shards,
            ..ravel_catalog::CatalogConfig::default()
        },
    )
    .map_err(|err| FoldFailure::Failed(format!("failed to build catalog: {err}")))?;
    let fold = catalog
        .fold_with_seal_through(
            &tenant_id.hash(),
            Signal::Logs,
            uuid::Uuid::new_v4(),
            now_ns,
            &[],
            None,
            &ravel_catalog::RefoldRequest::new(),
            Some(seal_through_hour),
        )
        .await
        .map_err(|err| FoldFailure::Failed(err.to_string()))?;
    let identities: Vec<ravel_catalog::EntryIdentity> = tokens.iter().map(token_identity).collect();
    let coverage = ravel_catalog::snapshot_coverage(
        coverage_store.as_ref(),
        &tenant_id.hash(),
        Signal::Logs,
        &identities,
    )
    .await;
    let (parts_read, buckets_listed, records_read) = coverage.as_ref().map_or((0, 0, 0), |c| {
        (c.parts_read, c.buckets_listed, c.records_read)
    });
    let load_fold = LoadFold {
        elapsed: started.elapsed(),
        entry_count: fold.entry_count,
        watermark_hour: fold.watermark_hour,
        seal_through_hour: Some(seal_through_hour),
        no_op: fold.no_op,
        parts_read,
        buckets_listed,
        records_read,
    };
    match uncovered_commits(tokens, coverage) {
        None => Ok(load_fold),
        Some((hours, finding)) => Err(FoldFailure::Uncovered {
            fold: load_fold,
            hours,
            finding,
        }),
    }
}

/// Aborts its task when dropped, so every return path of the loader stops it.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl AbortOnDrop {
    /// Aborts the task and waits for it to end, so whatever it held is
    /// released before this returns.
    async fn stop(mut self) {
        self.0.abort();
        let _ = (&mut self.0).await;
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Flushes every shard buffer when two consecutive samples `period` apart
/// both find the decoder waiting on `budget` and the gauge no lower than the
/// first sample read. The gauge is net: a refund and a new admission between
/// the samples can cancel out, and a refund can come from a flush already
/// running rather than a buffer, so this can flush when room was on its way.
/// That costs an early flush and a smaller object, never a hang.
fn spawn_stall_flusher(
    router: Arc<LogIngestRouter>,
    budget: Arc<IngestByteBudget>,
    period: Duration,
) -> AbortOnDrop {
    AbortOnDrop(tokio::spawn(async move {
        let mut stalled_at: Option<u64> = None;
        loop {
            tokio::time::sleep(period).await;
            if budget.waiting() == 0 {
                stalled_at = None;
                continue;
            }
            let held = budget.in_flight_bytes();
            if stalled_at.is_some_and(|before| held >= before) {
                router.flush_all().await;
                stalled_at = None;
            } else {
                stalled_at = Some(held);
            }
        }
    }))
}

/// Charges each built columnar batch to the load's memory budget (issue
/// #2626): an estimate before the build, from the previous batch's measured
/// bytes per row (zero for the first batch, which nothing else is held
/// beside, so its correction is admitted however large it is), corrected to
/// the built batch's measured
/// [`ColumnarLogBatch::heap_bytes`] once it exists. Both steps wait for room
/// and never fail.
pub(super) struct BatchCharger {
    budget: Arc<IngestByteBudget>,
    bytes_per_row: Option<u64>,
}

impl BatchCharger {
    pub(super) fn new(budget: Arc<IngestByteBudget>) -> Self {
        Self {
            budget,
            bytes_per_row: None,
        }
    }

    fn charge_before_build(&self, rows: usize) -> IngestByteCharge {
        let estimate = self
            .bytes_per_row
            .map_or(0, |per_row| per_row.saturating_mul(rows as u64));
        self.budget.charge_waiting(estimate)
    }

    fn settle(
        &mut self,
        mut charge: IngestByteCharge,
        batch: &ColumnarLogBatch,
    ) -> IngestByteCharge {
        let bytes = batch.heap_bytes() as u64;
        charge.resize_waiting(bytes);
        if batch.num_rows > 0 {
            self.bytes_per_row = Some(bytes.div_ceil(batch.num_rows as u64));
        }
        charge
    }
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
    mut charger: Option<BatchCharger>,
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
                    charger.as_mut(),
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
/// drives the columnar form through `write_columnar`. A columnar batch carries
/// its memory-budget charge (issue #2626) when the load has a budget.
enum Built {
    Row(Vec<NormalizedLogRecord>),
    Columnar(Box<ColumnarLogBatch>, Option<IngestByteCharge>),
}

impl Built {
    /// Row count of the built batch, the source row count for reporting.
    fn num_rows(&self) -> usize {
        match self {
            Built::Row(records) => records.len(),
            Built::Columnar(batch, _) => batch.num_rows,
        }
    }
}

/// Which build/write path a load drives. `load` uses [`LoadPath::Columnar`]
/// (ADR-0109 decision 4); [`LoadPath::Row`] stays reachable so the byte-identity
/// differential test can run the same file through the pre-ADR row path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LoadPath {
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
pub(super) struct CursorState {
    /// `None` once the underlying reader is exhausted.
    pub(super) reader: Option<BatchReader>,
    /// Rows pulled from `reader.next()` not yet fully consumed.
    pub(super) buffered: Option<RecordBatch>,
    /// File-absolute row index of this cursor's partition's first row.
    pub(super) partition_base: u64,
    /// Rows already handed out from this partition, so
    /// `partition_base + consumed` is the file-absolute index of the next row
    /// this cursor will yield.
    pub(super) consumed: u64,
    /// The dealing block: [`cursor_take_spans`] never hands out rows across a
    /// multiple of this many rows from the partition start. It is the
    /// `batch_rows` the reader used to decode at, so the spans a round deals
    /// are the same whatever Arrow batch size the reader decodes at now
    /// (issue #2613).
    pub(super) block_rows: usize,
    /// Rows in this cursor's partition, from the footer's row-group counts:
    /// where the pre-#2613 reader's last, short batch ended.
    pub(super) partition_rows: u64,
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
pub(super) fn cursor_take(
    cur: &mut CursorState,
    want: usize,
) -> Result<Option<(RecordBatch, u64)>, String> {
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

/// Deal one round's share of up to `want` rows from `cur`, as one or more
/// contiguous spans: exactly the rows the pre-#2613 dealer's single
/// [`cursor_take`] call on a `block_rows`-row reader would have returned
/// (issue #2613). Returns an empty vector when that call would have returned
/// nothing.
///
/// That call returned at most the rest of the reader's current batch, which
/// ended at the next multiple of `block_rows` from the partition start or at
/// the partition's end, whichever came first, so the share is capped at both.
/// At the partition's end it asked the reader once, found it exhausted, and
/// dropped it, so the cursor counted as live for that one round and dealt
/// nothing. Refilling until the share is met instead would find the reader
/// exhausted in the round that dealt the last rows, retire the cursor a round
/// early and grow every other cursor's share from then on.
///
/// A reader decoding `block_rows`-row batches yields one span per call here.
/// One decoding smaller batches yields the same rows as several spans, each
/// with its own file-absolute base, so the rows a round deals do not depend on
/// the reader's batch size, while the decoded Arrow a cursor holds between
/// rounds does.
pub(super) fn cursor_take_spans(
    cur: &mut CursorState,
    want: usize,
) -> Result<Vec<(RecordBatch, u64)>, String> {
    let block = cur.block_rows.max(1) as u64;
    let to_boundary = block - cur.consumed % block;
    let left_in_partition = cur.partition_rows.saturating_sub(cur.consumed);
    if left_in_partition == 0 {
        return Ok(cursor_take(cur, want.min(to_boundary as usize))?
            .into_iter()
            .collect());
    }
    let mut spans = Vec::new();
    let mut remaining = (want as u64).min(to_boundary).min(left_in_partition) as usize;
    while remaining > 0 {
        let Some((span, file_base)) = cursor_take(cur, remaining)? else {
            break;
        };
        remaining -= span.num_rows();
        spans.push((span, file_base));
    }
    Ok(spans)
}

/// The spans making up one batch, or a terminal signal. Shared by the row and
/// columnar decode paths so the K-cursor share dealing (issue #560) lives in
/// exactly one place.
enum SpanOutcome {
    /// Every stride cursor is exhausted; no batch this round.
    Done,
    /// A cursor's Parquet read failed.
    Failed(String),
    /// The round's non-empty spans, each with its own file-absolute base row.
    /// A cursor's share can arrive as several spans, so a round can hold more
    /// than K; a cursor dealt nothing, or whose rows were all skipped,
    /// contributes none. May be empty, which the caller turns into a zero-row
    /// batch.
    Spans(Vec<(RecordBatch, u64)>),
}

/// Deal one batch's worth of rows across the live stride cursors (issue #560).
/// Each live cursor contributes up to its share as one contiguous run of rows
/// via [`cursor_take_spans`], which may hand that run over as several spans;
/// the spans keep their own `file_base` so a rejected
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
        match cursor_take_spans(&mut state.cursors[idx], share) {
            Ok(taken) => spans.extend(taken.into_iter().filter(|(b, _)| b.num_rows() > 0)),
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
/// on the same spans is the acceptance anchor (decision 7). With a `charger`
/// the batch is charged after its spans are collected and before it is built,
/// so the decoder waits there, holding only the spans, while the budget is
/// full.
fn decode_and_build_stride_columnar(
    mut state: StrideCursors,
    mapping: Arc<Mapping>,
    limits: LogIngestLimits,
    now_ns: i64,
    batch_rows: usize,
    on_build_start: Option<&BuildStartHook>,
    charger: Option<&mut BatchCharger>,
) -> (StrideCursors, Prefetched) {
    let spans = match collect_spans(&mut state, batch_rows, on_build_start) {
        SpanOutcome::Done => return (state, Prefetched::Done),
        SpanOutcome::Failed(reason) => return (state, Prefetched::BatchFailed { reason }),
        SpanOutcome::Spans(spans) => spans,
    };

    let rows: usize = spans.iter().map(|(b, _)| b.num_rows()).sum();
    let charge = charger.as_deref().map(|c| c.charge_before_build(rows));
    match build_columnar_batch(&spans, &mapping, &limits, now_ns) {
        Ok(batch) => {
            let charge = match (charger, charge) {
                (Some(charger), Some(charge)) => Some(charger.settle(charge, &batch)),
                _ => None,
            };
            (
                state,
                Prefetched::Batch(Built::Columnar(Box::new(batch), charge)),
            )
        }
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

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests;
