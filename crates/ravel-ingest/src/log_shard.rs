//! One actor per log shard: actor-local buffering, pipelined flush, and the
//! pinned-identity commit sequence, the log-pipeline counterpart of
//! [`crate::shard`] (docs/ingest.md "Shard actor", docs/catalog-and-mvcc.md
//! "Pinned flush identity" and "Commit sequence").
//!
//! Buffer ownership and flush execution are split (ADR-0067 decision 1): the
//! actor is the single-threaded owner of buffered state and, at flush trigger,
//! pins the flush's identity and moves its `LogTenantBuf` (including its
//! waiters and its ADR-0069 byte charges) into a task spawned onto
//! [`LogFlushCtx::run_flush`], then keeps draining its channel.
//! `max_inflight_flushes` (ADR-0067 decision 2) bounds how many such tasks may
//! run at once per shard via a semaphore acquired before spawning; at the
//! bound, the acquire blocks the flush trigger (and therefore the actor's
//! ability to pull its next message), which is exactly where backpressure is
//! meant to propagate. This ports ADR-0067 decisions 1 and 2 from the metrics
//! [`crate::shard`]; the adaptive flush delay (decision 3) is metrics-only and
//! deliberately absent here (the age trigger stays the fixed
//! `max_flush_delay`/`max_flush_delay_idle` in [`LogShardActor::age_threshold_ns`]).
//!
//! The divergences from the metrics shard actor are otherwise deliberate and
//! narrow: the buffer holds [`NormalizedLogRecord`]s instead of points, the
//! flush builds an RLOG object with [`RlogWriter`] instead of an RSEG segment,
//! and identity is a `stream_id` rather than a `series_id`. One difference is
//! worth restating: this buffer performs no stream-identity collision check
//! of its own. Unlike [`crate::shard::TenantBuf::merge`]'s ADR-0005 series-id
//! check, the equivalent fail-loud check for logs already lives in
//! [`RlogWriter::finish`] (`LogSegError::InconsistentStreamAttrs`), which compares every buffered record's `stream_attrs` for a shared
//! `stream_id`. Duplicating it here would only be dead code with a second
//! chance to drift, so the flush step maps that one `finish()` error variant
//! to [`LogWriteError::StreamIdCollision`] instead.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use ravel_commit::keys;
use ravel_commit::publish::{self, PublishError, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_commit::rng::RngSource;
use ravel_logseg::{LogRecord, LogSegError, ObjectIdentity, RlogConfig, RlogWriter};
use ravel_object_store::ObjectStoreBackend;
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_proto::commit::v1::CommitRecord;
use ravel_types::logstream::{AttrValue, LogStreamId};
use ravel_types::{CommitToken, Signal, TenantId};

use crate::indexed_fields::IndexedFieldsOverlay;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::task::JoinSet;
use tokio::time::Duration;
use uuid::Uuid;

use crate::budget::IngestByteCharge;
use crate::clock::Clock;
use crate::config::{IngestConfig, LOG_SEGMENT_FORMAT_VERSION, checked_ingest_hour_bucket};
use crate::log_error::LogWriteError;
use crate::log_metrics::LogIngestMetrics;
use crate::metrics::FlushTrigger;

pub(crate) type LogAck = oneshot::Sender<Result<CommitToken, LogWriteError>>;

pub(crate) enum LogShardMsg {
    Write {
        tenant: TenantId,
        records: Vec<NormalizedLogRecord>,
        ack: Option<LogAck>,
        /// This request's global ingest-byte-budget charge (ADR-0069), held in
        /// the tenant buffer until it flushes, then dropped -- refunding the
        /// bytes -- when the flush's outcome is reached. `None` only for a test
        /// write that bypasses the budget; production charges via
        /// [`crate::LogIngestRouter`].
        charge: Option<Arc<IngestByteCharge>>,
    },
    /// Flush every buffered tenant now, regardless of size/age thresholds.
    FlushNow { done: oneshot::Sender<()> },
    /// Flush every buffered tenant, then stop the actor loop.
    Shutdown { done: oneshot::Sender<()> },
}

/// Estimated encoded length of one attribute value, for the `est_bytes`
/// flush-trigger heuristic. Nested `List`/`Map` values recurse. This is a
/// sizing estimate only, not the RLOG encoder's exact output.
fn attr_value_len(value: &AttrValue) -> usize {
    match value {
        AttrValue::Str(s) => s.len(),
        AttrValue::Bytes(b) => b.len(),
        AttrValue::I64(_) | AttrValue::F64(_) => 8,
        AttrValue::Bool(_) => 1,
        AttrValue::List(items) => items.iter().map(attr_value_len).sum(),
        AttrValue::Map(entries) => entries
            .iter()
            .map(|(k, v)| k.len() + attr_value_len(v))
            .sum(),
    }
}

/// Estimated buffered byte cost of one record, per the `est_bytes` rule
/// (docs/ingest.md, mirroring [`crate::shard::TenantBuf::merge`]'s register):
/// the two string fields, the stream_attrs blob, every attribute key/value
/// encoded length, plus a fixed 32 covering the two i64 timestamps,
/// severity_num, flags, and the optional trace/span ids. A `target_bytes`
/// flush-trigger estimate, not a byte-exact accounting of the RLOG output.
pub(crate) fn est_record_bytes(rec: &NormalizedLogRecord) -> usize {
    let attr_bytes: usize = rec
        .attrs
        .iter()
        .map(|(k, v)| k.len() + attr_value_len(v))
        .sum();
    rec.body.len() + rec.severity_text.len() + rec.stream_attrs.len() + attr_bytes + 32
}

/// Type-level bridge from the OTLP-independent [`NormalizedLogRecord`] to the
/// writer's [`LogRecord`]. Every field maps one to one; there is no data
/// transformation, only a struct rename.
fn to_logseg_record(rec: NormalizedLogRecord) -> LogRecord {
    LogRecord {
        stream_id: rec.stream_id,
        stream_attrs: rec.stream_attrs,
        ts_ns: rec.ts_ns,
        observed_ts_ns: rec.observed_ts_ns,
        severity_num: rec.severity_num,
        severity_text: rec.severity_text,
        body: rec.body,
        trace_id: rec.trace_id,
        span_id: rec.span_id,
        flags: rec.flags,
        attrs: rec.attrs,
    }
}

/// One tenant's accumulated log records in a single shard buffer, the
/// log-pipeline counterpart of [`crate::shard::TenantBuf`].
///
/// No stream-identity bookkeeping lives here on purpose (see the module
/// docs): the fail-loud collision check is [`RlogWriter::finish`]'s job.
#[derive(Default)]
struct LogTenantBuf {
    records: Vec<NormalizedLogRecord>,
    est_bytes: usize,
    oldest_arrival_ns: Option<i64>,
    min_ingest_ts_ns: Option<i64>,
    max_ingest_ts_ns: Option<i64>,
    waiters: Vec<LogAck>,
    /// Global ingest-byte-budget charges (ADR-0069) for every request whose
    /// records this buffer holds. Dropped -- refunding the bytes -- when the
    /// buffer flushes (or its flush fails), never before.
    charges: Vec<Arc<IngestByteCharge>>,
}

impl LogTenantBuf {
    fn note_arrival(&mut self, arrival_ns: i64) {
        self.oldest_arrival_ns.get_or_insert(arrival_ns);
        self.min_ingest_ts_ns = Some(match self.min_ingest_ts_ns {
            Some(m) => m.min(arrival_ns),
            None => arrival_ns,
        });
        self.max_ingest_ts_ns = Some(match self.max_ingest_ts_ns {
            Some(m) => m.max(arrival_ns),
            None => arrival_ns,
        });
    }

    /// Appends `records` to this buffer and returns the estimated byte cost
    /// added (per [`est_record_bytes`]). Unlike the metrics buffer this never
    /// fails: the stream-id collision check is deferred to
    /// [`RlogWriter::finish`] at flush time, so a merge cannot reject.
    fn merge(&mut self, records: Vec<NormalizedLogRecord>, arrival_ns: i64) -> usize {
        self.note_arrival(arrival_ns);
        let bytes_added: usize = records.iter().map(est_record_bytes).sum();
        self.records.extend(records);
        self.est_bytes += bytes_added;
        bytes_added
    }
}

/// Everything one log flush task needs to encode, PUT twice, and ack, bundled
/// so it can be handed to a spawned task by move (ADR-0067 decision 1: "no
/// shared mutable state is introduced"). Built once per shard actor and shared
/// by every flush's task through an `Arc`; nothing here is mutated after
/// construction (the atomics inside `metrics` are already safe for concurrent
/// access from many in-flight flush tasks at once).
struct LogFlushCtx {
    shard: u32,
    writer_id: Uuid,
    epoch: u64,
    store: Arc<dyn ObjectStoreBackend>,
    clock: Arc<dyn Clock>,
    rng: Arc<dyn RngSource>,
    config: IngestConfig,
    metrics: Arc<LogIngestMetrics>,
    /// Resolves each tenant's POSTINGS indexed-field list at flush time
    /// (ADR-0049 decision 3), now via the durable-override cache-aside overlay
    /// (ADR-0079). Shared across shards by `Arc`; its per-tenant cache is the
    /// one place a tenant's `TenantConfig.indexed_fields` override is read.
    indexed_fields: Arc<IndexedFieldsOverlay>,
}

/// One log flush's identity and payload, pinned by the actor before the flush
/// task takes over (docs/catalog-and-mvcc.md "Pinned flush identity"): `seq`,
/// `ingest_hour_bucket`, and every field derived from the clock are fixed here
/// and carried verbatim into the task. Nothing in [`LogFlushCtx::run_flush`]
/// may re-read the clock or re-derive any of these.
struct LogPinnedFlush {
    tenant_hash: ravel_types::TenantHash,
    seq: u64,
    identity: ObjectIdentity,
    ingest_hour_bucket: u32,
    flush_open_ns: i64,
    deadline_ns: i64,
    min_ingest_ts_ns: i64,
    max_ingest_ts_ns: i64,
    records: Vec<NormalizedLogRecord>,
    waiters: Vec<LogAck>,
    /// The global ingest-byte-budget charges this flush's buffer held (ADR-0069).
    /// Carried into the flush task purely so they are dropped -- and the bytes
    /// refunded -- when the flush's terminal outcome is reached, no earlier.
    charges: Vec<Arc<IngestByteCharge>>,
}

impl LogFlushCtx {
    /// Runs the full pinned-identity commit sequence for one flush, mirroring
    /// [`crate::shard::FlushCtx::run_flush`] step for step: the serialized RLOG
    /// object and its blake3 hash are each computed exactly once here and reused
    /// verbatim by every retry (docs/catalog-and-mvcc.md "Pinned flush
    /// identity"). Nothing below may re-serialize, accrete new records, or
    /// re-read the clock for identity purposes.
    ///
    /// The one log-specific step is `finish()` error mapping: an
    /// `InconsistentStreamAttrs` becomes [`LogWriteError::StreamIdCollision`]
    /// and increments `stream_id_collisions`; every other `LogSegError` becomes
    /// [`LogWriteError::SegmentBuild`]. This is the only site that constructs
    /// `StreamIdCollision`, because the collision check itself lives in
    /// `finish()`, not in this module.
    async fn run_flush(&self, pinned: LogPinnedFlush) {
        let LogPinnedFlush {
            tenant_hash,
            seq,
            identity,
            ingest_hour_bucket,
            flush_open_ns,
            deadline_ns,
            min_ingest_ts_ns,
            max_ingest_ts_ns,
            records,
            waiters,
            charges,
        } = pinned;
        // Held to this flush's terminal outcome (every early `return` below is
        // still inside this scope), then dropped here: that drop is the
        // ADR-0069 budget refund for exactly the bytes this buffer held.
        let _charges = charges;

        // One pass over the batch computes the commit-record fields RlogWriter
        // does not surface after `finish()`: the distinct stream count (the log
        // analogue of series_count) and the event-time bounds. `records` is
        // never empty here: the actor's `flush_tenant` returns before spawning
        // for an empty buffer, so `min_event_ts_ns`/`max_event_ts_ns` always
        // see at least one record.
        let mut stream_ids: HashSet<LogStreamId> = HashSet::new();
        let mut min_event_ts_ns = i64::MAX;
        let mut max_event_ts_ns = i64::MIN;
        for rec in &records {
            stream_ids.insert(rec.stream_id);
            min_event_ts_ns = min_event_ts_ns.min(rec.ts_ns);
            max_event_ts_ns = max_event_ts_ns.max(rec.ts_ns);
        }
        let series_count = stream_ids.len() as u64;
        let sample_count = records.len() as u64;

        // Resolve this tenant's POSTINGS indexed-field list (ADR-0049 decision
        // 3) once per object and hand it to the writer. An empty list leaves the
        // object without a POSTINGS section, which is always legal (decision 5).
        //
        // Cache-aside over the durable per-tenant override (ADR-0079), the same
        // two-step dance `log_router.rs::active_set` uses: a sync fast path on a
        // fresh cached entry, else an async single-GET refresh installed and
        // proceeded on. `now_ns` is the flush's own pinned `flush_open_ns`
        // (log_shard.rs's pinned-identity contract forbids re-reading the clock
        // in `run_flush`), which also keeps the cache deterministic under test.
        // A stale-cache/failed-read/validation fallback is degraded, never fatal:
        // it counts the visibility metric and proceeds on the last-known-good or
        // CLI-only list, never failing the flush closed (ADR-0079 Safety).
        let indexed_fields = match self
            .indexed_fields
            .fields_for_cached(&tenant_hash, flush_open_ns)
        {
            Some(fields) => fields,
            None => {
                let outcome = self
                    .indexed_fields
                    .refresh(self.store.as_ref(), &tenant_hash, flush_open_ns)
                    .await;
                if outcome.is_fallback() {
                    self.metrics.record_indexed_fields_stale_fallback();
                }
                outcome.into_fields()
            }
        };
        let mut writer =
            RlogWriter::new(RlogConfig::default(), identity).with_indexed_fields(indexed_fields);
        for rec in records {
            if let Err(e) = writer.push(to_logseg_record(rec)) {
                self.metrics.record_abandoned_input_rejected();
                self.ack_waiters(waiters, Err(LogWriteError::SegmentBuild(e.to_string())));
                return;
            }
        }
        let bytes = match writer.finish_with_stats() {
            Ok((bytes, stats)) => {
                // Write-side POSTINGS metrics: section bytes, per-field distinct
                // counts, and the cap-exceeded counter.
                self.metrics.record_postings(stats);
                bytes
            }
            Err(LogSegError::InconsistentStreamAttrs(msg)) => {
                self.metrics.record_stream_id_collision();
                self.ack_waiters(waiters, Err(LogWriteError::StreamIdCollision(msg)));
                return;
            }
            Err(e) => {
                self.metrics.record_abandoned_input_rejected();
                self.ack_waiters(waiters, Err(LogWriteError::SegmentBuild(e.to_string())));
                return;
            }
        };

        let content_hash: [u8; 32] = *blake3::hash(&bytes).as_bytes();
        let data = Bytes::from(bytes);

        let data_key = match keys::data_key(
            &tenant_hash,
            Signal::Logs,
            self.shard,
            self.writer_id,
            self.epoch,
            seq,
            &content_hash,
        ) {
            Ok(k) => k,
            Err(e) => {
                self.metrics.record_abandoned_input_rejected();
                self.ack_waiters(waiters, Err(LogWriteError::SegmentBuild(e.to_string())));
                return;
            }
        };

        if !self
            .put_data_object_with_retry(&data_key, data.clone(), deadline_ns)
            .await
        {
            self.metrics.record_abandoned_retry_exhausted();
            self.ack_waiters(
                waiters,
                Err(LogWriteError::Abandoned(
                    "data object put exhausted retry budget or exceeded max_flush_lifetime".into(),
                )),
            );
            return;
        }

        let record = match record::build(NewCommitRecord {
            tenant_hash,
            signal: Signal::Logs,
            shard: self.shard,
            writer_id: self.writer_id,
            writer_epoch: self.epoch,
            writer_seq: seq,
            object_size: data.len() as u64,
            content_hash,
            sample_count,
            series_count,
            min_event_ts_ns,
            max_event_ts_ns,
            min_ingest_ts_ns,
            max_ingest_ts_ns,
            segment_format_version: u32::from(LOG_SEGMENT_FORMAT_VERSION),
            created_unix_ns: flush_open_ns,
            ingest_hour_bucket,
        }) {
            Ok(r) => r,
            Err(e) => {
                self.metrics.record_abandoned_input_rejected();
                self.ack_waiters(waiters, Err(LogWriteError::SegmentBuild(e.to_string())));
                return;
            }
        };

        match self.publish_with_retry(&record, deadline_ns).await {
            Some(token) => {
                // Both PUTs landed: attribute this flush's PUT cost to the
                // tenant (ADR-0076 decision 2, success-time). `tenant_hash` is
                // `Copy` and untouched by the commit-record build above.
                self.metrics.record_flush_puts(tenant_hash);
                self.ack_waiters(waiters, Ok(token));
            }
            None => {
                self.metrics.record_abandoned_retry_exhausted();
                self.ack_waiters(
                    waiters,
                    Err(LogWriteError::Abandoned(
                        "commit publish exhausted retry budget or exceeded max_flush_lifetime"
                            .into(),
                    )),
                );
            }
        }
    }

    /// Acks exactly this flush's own waiters with exactly this flush's own
    /// result: `waiters` was moved out of this flush's `LogTenantBuf` at pin
    /// time and never merged with another flush's, so there is no other waiter
    /// list this call could reach.
    fn ack_waiters(&self, waiters: Vec<LogAck>, result: Result<CommitToken, LogWriteError>) {
        let ok = result.is_ok();
        self.metrics.record_acks(waiters.len(), ok);
        for waiter in waiters {
            let _ = waiter.send(result.clone());
        }
    }

    /// Races `fut` against the remaining budget to `deadline_ns` on the injected
    /// `Clock`, returning `None` if the deadline is already past or elapses
    /// while `fut` is still in flight. Built on `tokio::select!` racing
    /// `self.clock.sleep(..)` rather than `tokio::time::timeout`, so the
    /// deadline stays on the injected clock a test can pin and advance.
    async fn bound_to_deadline<F, T>(&self, deadline_ns: i64, fut: F) -> Option<T>
    where
        F: Future<Output = T>,
    {
        let remaining_ns = deadline_ns.saturating_sub(self.clock.now_ns());
        if remaining_ns <= 0 {
            return None;
        }
        let remaining = Duration::from_nanos(u64::try_from(remaining_ns).unwrap_or(u64::MAX));
        tokio::select! {
            result = fut => Some(result),
            () = self.clock.sleep(remaining) => None,
        }
    }

    /// Retries the data-object PUT with the caller's own budget, reusing the
    /// pinned `key`/`bytes` on every attempt. Each attempt is bounded to
    /// `deadline_ns` via [`Self::bound_to_deadline`], so a timeout never retries
    /// past the deadline and is treated exactly like the abandonment path.
    async fn put_data_object_with_retry(&self, key: &str, bytes: Bytes, deadline_ns: i64) -> bool {
        let mut attempt: u32 = 0;
        loop {
            let call = publish::put_data_object(self.store.as_ref(), key, bytes.clone());
            match self.bound_to_deadline(deadline_ns, call).await {
                Some(Ok(())) => return true,
                Some(Err(PublishError::Store { source, .. })) if source.is_retryable() => {
                    // `put_retry_max_attempts` is the number of retries after
                    // the first attempt (total attempts = this + 1), matching
                    // `ravel_commit::publish::RetryPolicy`. Check the budget
                    // before consuming a retry so the first attempt is not
                    // itself counted against it.
                    if attempt >= self.config.put_retry_max_attempts
                        || self.clock.now_ns() >= deadline_ns
                    {
                        return false;
                    }
                    self.metrics.record_put_retry();
                    self.backoff_sleep(attempt).await;
                    attempt += 1;
                }
                Some(Err(_)) | None => return false,
            }
        }
    }

    /// Retries the commit-record PUT with the caller's own budget, passing
    /// `publish` a zero-retry policy so it attempts once per call and this loop
    /// checks `deadline_ns` between attempts. Includes the pinned-identity
    /// split-brain panic: identity is fixed at flush open, so a split-brain
    /// cannot fire on a benign retry and means the pinning invariant was broken
    /// upstream.
    async fn publish_with_retry(
        &self,
        record: &CommitRecord,
        deadline_ns: i64,
    ) -> Option<CommitToken> {
        let single_attempt = RetryPolicy {
            max_attempts: 0,
            base_delay: self.config.put_retry_base_delay,
            max_delay: self.config.put_retry_max_delay,
        };
        let mut attempt: u32 = 0;
        loop {
            let call = publish::publish(self.store.as_ref(), record, &single_attempt);
            match self.bound_to_deadline(deadline_ns, call).await {
                Some(Ok(token)) => return Some(token),
                Some(Err(PublishError::SplitBrain { this, stored })) => {
                    panic!(
                        "ravel-ingest: fatal split-brain on pinned log flush identity: this={this} stored={stored}"
                    );
                }
                Some(Err(PublishError::Store { source, .. })) if source.is_retryable() => {
                    // See `put_data_object_with_retry`: `put_retry_max_attempts`
                    // is retries after the first attempt (total = this + 1).
                    if attempt >= self.config.put_retry_max_attempts
                        || self.clock.now_ns() >= deadline_ns
                    {
                        return None;
                    }
                    self.metrics.record_put_retry();
                    self.backoff_sleep(attempt).await;
                    attempt += 1;
                }
                Some(Err(_)) | None => return None,
            }
        }
    }

    async fn backoff_sleep(&self, attempt: u32) {
        let shift = attempt.min(20);
        let exp = self
            .config
            .put_retry_base_delay
            .saturating_mul(1u32 << shift);
        let capped = exp.min(self.config.put_retry_max_delay);
        let capped_ms = u64::try_from(capped.as_millis()).unwrap_or(u64::MAX);
        let jittered_ms = self.rng.jitter_ms(capped_ms);
        // Route the backoff wait through the injected `Clock`, not the tokio
        // timer, so retry timing shares the one clock the rest of the flush path
        // already uses (`bound_to_deadline`) and a test can drive it
        // deterministically by advancing that clock, with no real sleep.
        self.clock.sleep(Duration::from_millis(jittered_ms)).await;
    }
}

/// Handles one reaped flush task's outcome. A panic inside
/// [`LogFlushCtx::run_flush`] (the `SplitBrain` panic on a broken pinning
/// invariant, or any other) must still take this shard actor down with it,
/// exactly as it did before flush execution moved into its own spawned task:
/// resuming the unwind here propagates it out of `run()`'s own task, which
/// drops this actor (and `rx` with it), so the router observes the closed
/// mailbox and reports `ShardUnavailable`. A task ending by cancellation (never
/// triggered in today's code; `flushes` is never explicitly aborted) is merely
/// logged, since it carries no panic payload to propagate.
fn handle_flush_join_result(shard: u32, result: Result<(), tokio::task::JoinError>) {
    if let Err(join_err) = result {
        if join_err.is_panic() {
            std::panic::resume_unwind(join_err.into_panic());
        }
        tracing::error!(
            shard,
            error = %join_err,
            "ravel-ingest: log flush task ended abnormally (cancelled)"
        );
    }
}

/// RAII in-flight-flush accounting: incremented when a flush task is spawned,
/// decremented on `Drop` when it ends, including on panic. Moved into the
/// spawned task itself (not held by the actor) so the decrement fires exactly
/// once, whenever that task's future is finally dropped, with no separate
/// bookkeeping the actor could get out of sync with.
struct InFlightFlushGuard {
    metrics: Arc<LogIngestMetrics>,
    shard: u32,
}

impl Drop for InFlightFlushGuard {
    fn drop(&mut self) {
        self.metrics.record_inflight_flush_delta(self.shard, -1);
    }
}

pub(crate) struct LogShardActor {
    shard: u32,
    writer_id: Uuid,
    epoch: u64,
    next_seq: u64,
    clock: Arc<dyn Clock>,
    config: IngestConfig,
    metrics: Arc<LogIngestMetrics>,
    /// Immutable bundle handed by `Arc::clone` to every spawned flush task
    /// (ADR-0067 decision 1).
    ctx: Arc<LogFlushCtx>,
    /// Bounds concurrently in-flight flush tasks (ADR-0067 decision 2).
    semaphore: Arc<Semaphore>,
    /// Tracks spawned flush tasks so `join_all_flushes` can await durability
    /// before `FlushNow`/`Shutdown`/the channel-close drain return, and so the
    /// actor loop can opportunistically reap finished ones (the `select!` branch
    /// in `run`) rather than growing this set for the shard's whole lifetime.
    flushes: JoinSet<()>,
    rx: mpsc::Receiver<LogShardMsg>,
    tenants: HashMap<TenantId, LogTenantBuf>,
}

impl LogShardActor {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        shard: u32,
        writer_id: Uuid,
        epoch: u64,
        store: Arc<dyn ObjectStoreBackend>,
        clock: Arc<dyn Clock>,
        rng: Arc<dyn RngSource>,
        config: IngestConfig,
        metrics: Arc<LogIngestMetrics>,
        rx: mpsc::Receiver<LogShardMsg>,
        indexed_fields: Arc<IndexedFieldsOverlay>,
    ) -> Self {
        let ctx = Arc::new(LogFlushCtx {
            shard,
            writer_id,
            epoch,
            store,
            clock: Arc::clone(&clock),
            rng,
            config,
            metrics: Arc::clone(&metrics),
            indexed_fields,
        });
        LogShardActor {
            shard,
            writer_id,
            epoch,
            next_seq: 0,
            clock,
            config,
            metrics,
            ctx,
            semaphore: Arc::new(Semaphore::new(config.max_inflight_flushes as usize)),
            flushes: JoinSet::new(),
            rx,
            tenants: HashMap::new(),
        }
    }

    pub(crate) async fn run(mut self) {
        // The flush-tick cadence runs on the injected `Clock`, not the tokio
        // timer, exactly as [`crate::shard::ShardActor::run`] does it: age-based
        // flush timing shares the one clock the age check itself reads, so a
        // test that advances the injected clock past `max_flush_delay` drives a
        // flush tick deterministically with no real sleep.
        let clock = Arc::clone(&self.clock);
        let flush_tick_ns = i64::try_from(self.config.flush_tick.as_nanos()).unwrap_or(i64::MAX);
        let mut next_tick_ns = clock.now_ns().saturating_add(flush_tick_ns);
        loop {
            let until_ns = next_tick_ns.saturating_sub(clock.now_ns()).max(0);
            let until = Duration::from_nanos(u64::try_from(until_ns).unwrap_or(u64::MAX));
            tokio::select! {
                msg = self.rx.recv() => {
                    match msg {
                        Some(LogShardMsg::Write { tenant, records, ack, charge }) => {
                            self.handle_write(tenant, records, ack, charge).await;
                        }
                        Some(LogShardMsg::FlushNow { done }) => {
                            self.flush_all(FlushTrigger::Manual).await;
                            let _ = done.send(());
                        }
                        Some(LogShardMsg::Shutdown { done }) => {
                            self.flush_all(FlushTrigger::Manual).await;
                            let _ = done.send(());
                            break;
                        }
                        None => {
                            // Every sender was dropped without an explicit
                            // shutdown. A graceful teardown is not a crash, and
                            // buffered records are only permitted to be lost to
                            // a crash (docs/consistency-model.md), so flush
                            // before breaking; log first so the close is
                            // observable even if the flush is abandoned.
                            if !self.tenants.is_empty() {
                                let (tenant_count, buffered_records) = self.buffered_summary();
                                tracing::warn!(
                                    shard = self.shard,
                                    tenant_count,
                                    buffered_records,
                                    "log shard actor channel closed without shutdown; \
                                     flushing buffered tenants before stopping"
                                );
                            }
                            self.flush_all(FlushTrigger::Manual).await;
                            break;
                        }
                    }
                }
                _ = clock.sleep(until) => {
                    self.flush_aged().await;
                    next_tick_ns = clock.now_ns().saturating_add(flush_tick_ns);
                }
                Some(result) = self.flushes.join_next(), if !self.flushes.is_empty() => {
                    handle_flush_join_result(self.shard, result);
                }
            }
        }
    }

    async fn handle_write(
        &mut self,
        tenant: TenantId,
        records: Vec<NormalizedLogRecord>,
        ack: Option<LogAck>,
        charge: Option<Arc<IngestByteCharge>>,
    ) {
        if records.is_empty() && ack.is_none() {
            // Nothing buffered: dropping `charge` here refunds its bytes.
            return;
        }
        let arrival_ns = self.clock.now_ns();
        let records_len = records.len() as u64;
        let target_bytes = self.config.target_bytes;

        let buf = self.tenants.entry(tenant.clone()).or_default();
        let bytes_added = buf.merge(records, arrival_ns);
        // The records are now buffered: hold their budget charge with the buffer
        // until it flushes (ADR-0069).
        if let Some(charge) = charge {
            buf.charges.push(charge);
        }
        if let Some(ack) = ack {
            buf.waiters.push(ack);
        }
        self.metrics
            .record_buffered(bytes_added as u64, records_len);

        let should_flush = self
            .tenants
            .get(&tenant)
            .map(|b| b.est_bytes >= target_bytes)
            .unwrap_or(false);
        if should_flush && let Some(buf) = self.tenants.remove(&tenant) {
            self.flush_tenant(tenant, buf, FlushTrigger::Size).await;
        }
    }

    /// A buffer with a strict-mode waiter or at least `min_flush_bytes`
    /// already justifies a PUT on the fast `max_flush_delay` clock; anything
    /// else is idle and waits for the slower `max_flush_delay_idle` instead
    /// (ADR-0051 section 7). Strict-mode ack latency is unaffected:
    /// a strict write always leaves `waiters` non-empty for its whole flush
    /// window.
    fn age_threshold_ns(&self, buf: &LogTenantBuf) -> i64 {
        let has_priority = !buf.waiters.is_empty() || buf.est_bytes >= self.config.min_flush_bytes;
        if has_priority {
            self.config.max_flush_delay.as_nanos() as i64
        } else {
            self.config.max_flush_delay_idle.as_nanos() as i64
        }
    }

    async fn flush_aged(&mut self) {
        let now = self.clock.now_ns();
        let due: Vec<TenantId> = self
            .tenants
            .iter()
            .filter(|(_, buf)| {
                buf.oldest_arrival_ns
                    .map(|t| now.saturating_sub(t) >= self.age_threshold_ns(buf))
                    .unwrap_or(false)
            })
            .map(|(t, _)| t.clone())
            .collect();
        for tenant in due {
            if let Some(buf) = self.tenants.remove(&tenant) {
                self.flush_tenant(tenant, buf, FlushTrigger::Age).await;
            }
        }
    }

    /// Returns `(tenant_count, buffered_record_count)` across every buffered
    /// tenant, for the channel-close log line.
    fn buffered_summary(&self) -> (usize, u64) {
        let records: u64 = self
            .tenants
            .values()
            .map(|buf| buf.records.len() as u64)
            .sum();
        (self.tenants.len(), records)
    }

    async fn flush_all(&mut self, trigger: FlushTrigger) {
        let tenants: Vec<TenantId> = self.tenants.keys().cloned().collect();
        for tenant in tenants {
            if let Some(buf) = self.tenants.remove(&tenant) {
                self.flush_tenant(tenant, buf, trigger).await;
            }
        }
        self.join_all_flushes().await;
    }

    /// Awaits every spawned flush task, not only ones triggered by this call:
    /// any still in flight from an earlier size/age trigger too. So a caller of
    /// `flush_all` (`FlushNow`, `Shutdown`, or the channel-close drain) only
    /// observes completion once every flush this shard has ever opened is
    /// durable or abandoned. Without this, pipelining would let `Shutdown`
    /// return (and the process exit) while an earlier flush's PUT was still in
    /// flight, silently discarding an acknowledged record -- and
    /// docs/consistency-model.md's buffered-mode contract tolerates only crash
    /// loss, not a graceful shutdown racing its own flushes.
    async fn join_all_flushes(&mut self) {
        while let Some(result) = self.flushes.join_next().await {
            handle_flush_join_result(self.shard, result);
        }
    }

    /// Pins `buf`'s flush identity, then moves `buf`'s payload, waiters, and
    /// ADR-0069 charges into a task spawned onto [`LogFlushCtx::run_flush`]
    /// (ADR-0067 decision 1), mirroring [`crate::shard::ShardActor::flush_tenant`].
    /// Everything up to and including the semaphore acquire runs here, on the
    /// actor; nothing after it does, so a slow encode or a slow PUT never blocks
    /// the actor from processing its next message once a permit is free (true
    /// even at `max_inflight_flushes == 1`: the actor still returns from this
    /// call, and therefore drains its channel, the moment the task is spawned).
    ///
    /// An empty buffer never reaches the semaphore or a spawned task: there is
    /// nothing to encode, and a flush identity pinned for nothing would burn a
    /// `seq` for no object.
    async fn flush_tenant(&mut self, tenant: TenantId, buf: LogTenantBuf, trigger: FlushTrigger) {
        let LogTenantBuf {
            records,
            min_ingest_ts_ns,
            max_ingest_ts_ns,
            waiters,
            charges,
            ..
        } = buf;
        if records.is_empty() {
            // Nothing to write, so no flush task runs: dropping `charges` here
            // is the ADR-0069 refund for this (record-less) buffer.
            drop(charges);
            // `waiters` is empty here by construction: the log router mints a
            // strict-mode ack only for a shard that actually received records
            // (`by_shard` only holds shards with at least one record, and the
            // ack rides that same shard message), so a record-less buffer has
            // nobody to answer. If that ever changes, this returns without
            // acking and the router reads the dropped oneshot as a dead shard;
            // the assert makes the invariant loud rather than silently dropping.
            debug_assert!(waiters.is_empty());
            return;
        }
        self.metrics.record_flush(trigger);

        let tenant_hash = tenant.hash();
        let seq = self.next_seq;
        self.next_seq += 1;
        let flush_open_ns = self.clock.now_ns();
        let ingest_hour_bucket = match checked_ingest_hour_bucket(flush_open_ns) {
            Ok(bucket) => bucket,
            Err(msg) => {
                self.metrics.record_abandoned_input_rejected();
                self.ctx
                    .ack_waiters(waiters, Err(LogWriteError::SegmentBuild(msg)));
                return;
            }
        };
        let deadline_ns =
            flush_open_ns.saturating_add(self.config.max_flush_lifetime.as_nanos() as i64);

        let identity = ObjectIdentity {
            tenant_hash: tenant_hash.0,
            shard: self.shard,
            writer_id: self.writer_id.into_bytes(),
            writer_epoch: self.epoch,
            writer_seq: seq,
        };
        let min_ingest_ts_ns = min_ingest_ts_ns.unwrap_or(flush_open_ns);
        let max_ingest_ts_ns = max_ingest_ts_ns.unwrap_or(flush_open_ns);

        let pinned = LogPinnedFlush {
            tenant_hash,
            seq,
            identity,
            ingest_hour_bucket,
            flush_open_ns,
            deadline_ns,
            min_ingest_ts_ns,
            max_ingest_ts_ns,
            records,
            waiters,
            charges,
        };

        // ADR-0067 decision 2: the only place a flush trigger blocks. At
        // `max_inflight_flushes` already-spawned tasks, this await parks until
        // one ends and releases its permit; because `flush_tenant` is itself
        // awaited from `handle_write`/`flush_aged`/`flush_all`, that park keeps
        // the actor from pulling its next channel message, exactly the
        // backpressure path the bounded mpsc already relies on.
        let permit = match Arc::clone(&self.semaphore).acquire_owned().await {
            Ok(permit) => permit,
            Err(_) => panic!(
                "ravel-ingest: log flush semaphore closed unexpectedly on shard {}",
                self.shard
            ),
        };
        self.metrics.record_inflight_flush_delta(self.shard, 1);
        let guard = InFlightFlushGuard {
            metrics: Arc::clone(&self.metrics),
            shard: self.shard,
        };
        let ctx = Arc::clone(&self.ctx);
        self.flushes.spawn(async move {
            let _permit = permit;
            let _guard = guard;
            ctx.run_flush(pinned).await;
        });
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::pin::Pin;
    use std::sync::atomic::{AtomicI64, Ordering};

    use ravel_commit::rng::RngSource;
    use ravel_commit::{keys, record};
    use ravel_logseg::{Predicate, RlogReader, stream_attrs_bytes};
    use ravel_object_store::fault::{
        FaultKind, FaultPlan, FaultStore, Occurrence, Op, Rule, ScriptedFault, Sequence,
    };
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{GetRange, list_all};
    use ravel_types::TenantHash;
    use ravel_types::logstream::log_stream_id;
    use tokio::sync::watch;
    use tokio::task::JoinHandle;

    use super::*;
    use crate::budget::{IngestByteBudget, IngestByteBudgetLimit};

    const BASE_NS: i64 = 1_700_000_000_000_000_000;

    /// Deterministic injected clock, the shard actor's flush tick sleeps on it
    /// (mirrors `tests/common`'s `TestClock`, restated here because unit tests
    /// cannot import the integration-test harness).
    struct TestClock {
        now_ns: AtomicI64,
        wake_tx: watch::Sender<()>,
    }

    impl TestClock {
        fn new(start_ns: i64) -> Arc<Self> {
            let (wake_tx, _rx) = watch::channel(());
            Arc::new(TestClock {
                now_ns: AtomicI64::new(start_ns),
                wake_tx,
            })
        }

        fn advance_ns(&self, delta_ns: i64) {
            self.now_ns.fetch_add(delta_ns, Ordering::SeqCst);
            let _ = self.wake_tx.send(());
        }
    }

    impl Clock for TestClock {
        fn now_ns(&self) -> i64 {
            self.now_ns.load(Ordering::SeqCst)
        }

        fn sleep(&self, dur: Duration) -> Pin<Box<dyn Future<Output = ()> + Send + '_>> {
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

    /// Flushes on the first write (`target_bytes: 1`) and never on age, so a
    /// strict write drives one complete flush inline.
    fn flush_on_first() -> IngestConfig {
        IngestConfig {
            shard_count: 1,
            target_bytes: 1,
            max_flush_delay: Duration::from_secs(3600),
            flush_tick: Duration::from_millis(10),
            put_retry_base_delay: Duration::from_millis(1),
            put_retry_max_delay: Duration::from_millis(5),
            ..IngestConfig::default()
        }
    }

    /// Never flushes on size (`target_bytes` huge); only age or manual can.
    fn no_size_flush(max_flush_delay: Duration) -> IngestConfig {
        IngestConfig {
            shard_count: 1,
            target_bytes: 8 * 1024 * 1024,
            max_flush_delay,
            flush_tick: Duration::from_millis(10),
            put_retry_base_delay: Duration::from_millis(1),
            put_retry_max_delay: Duration::from_millis(5),
            ..IngestConfig::default()
        }
    }

    struct Harness {
        tx: mpsc::Sender<LogShardMsg>,
        task: JoinHandle<()>,
        store: Arc<dyn ObjectStoreBackend>,
        metrics: Arc<LogIngestMetrics>,
        clock: Arc<TestClock>,
    }

    impl Harness {
        fn spawn(config: IngestConfig) -> Self {
            Self::spawn_with_store(config, Arc::new(MemoryStore::new()))
        }

        fn spawn_with_store(config: IngestConfig, store: Arc<dyn ObjectStoreBackend>) -> Self {
            let clock = TestClock::new(BASE_NS);
            let metrics = Arc::new(LogIngestMetrics::default());
            let (tx, rx) = mpsc::channel(64);
            let actor = LogShardActor::new(
                0,
                Uuid::new_v4(),
                7,
                Arc::clone(&store),
                clock.clone(),
                Arc::new(ravel_commit::rng::SystemRng),
                config,
                Arc::clone(&metrics),
                rx,
                Arc::new(IndexedFieldsOverlay::new(Arc::new(
                    crate::log_router::NoIndexedFields,
                ))),
            );
            let task = tokio::spawn(actor.run());
            Harness {
                tx,
                task,
                store,
                metrics,
                clock,
            }
        }

        async fn shutdown(self) {
            let (done_tx, done_rx) = oneshot::channel();
            let _ = self.tx.send(LogShardMsg::Shutdown { done: done_tx }).await;
            let _ = done_rx.await;
            let _ = self.task.await;
        }
    }

    /// A consistently-built record: `stream_id` and `stream_attrs` derived from
    /// the same resource+scope inputs, so `finish()`'s collision check passes.
    fn norm_record(
        resource: &[(&str, &str)],
        scope_name: &str,
        ts_ns: i64,
        body: &str,
    ) -> NormalizedLogRecord {
        let res: Vec<(String, AttrValue)> = resource
            .iter()
            .map(|(k, v)| ((*k).to_string(), AttrValue::Str((*v).to_string())))
            .collect();
        let scope_attrs: Vec<(String, AttrValue)> = Vec::new();
        let stream_id = log_stream_id(&res, scope_name, "", &scope_attrs);
        let stream_attrs = stream_attrs_bytes(&res, scope_name, "", &scope_attrs);
        NormalizedLogRecord {
            stream_id,
            stream_attrs,
            ts_ns,
            observed_ts_ns: ts_ns,
            severity_num: 9,
            severity_text: "INFO".to_string(),
            body: body.to_string(),
            trace_id: None,
            span_id: None,
            flags: 0,
            attrs: Vec::new(),
        }
    }

    /// Follows the commit token to its RLOG object and returns the decoded
    /// commit record plus every record an unfiltered scan yields.
    async fn read_back(
        store: &dyn ObjectStoreBackend,
        tenant_hash: &TenantHash,
        token: &CommitToken,
    ) -> (CommitRecord, Vec<LogRecord>) {
        let commit_key =
            keys::commit_key_for_token(tenant_hash, Signal::Logs, token).expect("commit key");
        let commit_bytes = store
            .get(&commit_key, GetRange::Full)
            .await
            .expect("get commit record")
            .data;
        let rec = record::decode(&commit_bytes).expect("decode commit record");
        let data_bytes = store
            .get(&rec.object_key, GetRange::Full)
            .await
            .expect("get data object")
            .data;
        let reader = RlogReader::new(&data_bytes, &RlogConfig::default()).expect("open rlog");
        let (records, _stats) = reader
            .scan(&Predicate::And(Vec::new()))
            .expect("unfiltered scan");
        (rec, records)
    }

    #[tokio::test]
    async fn size_flush_round_trips_to_a_readable_rlog_object() {
        let h = Harness::spawn(flush_on_first());
        let tenant = TenantId::new("acme");
        let records = vec![
            norm_record(&[("service.name", "api")], "scope", 1_000, "first"),
            norm_record(&[("service.name", "api")], "scope", 2_000, "second"),
        ];

        let (ack_tx, ack_rx) = oneshot::channel();
        h.tx.send(LogShardMsg::Write {
            tenant: tenant.clone(),
            records,
            ack: Some(ack_tx),
            charge: None,
        })
        .await
        .expect("send write");
        // The strict ack only fires after the commit publishes.
        let token = ack_rx
            .await
            .expect("ack sender not dropped")
            .expect("strict write commits");

        let (rec, scanned) = read_back(h.store.as_ref(), &tenant.hash(), &token).await;
        assert_eq!(
            rec.segment_format_version,
            u32::from(LOG_SEGMENT_FORMAT_VERSION)
        );
        assert_eq!(rec.sample_count, 2, "both records are one RLOG object");
        assert_eq!(rec.series_count, 1, "both share one stream");
        assert_eq!(scanned.len(), 2, "every pushed record reads back");

        let snap = h.metrics.snapshot();
        assert_eq!(snap.flushes_by_size, 1);
        assert_eq!(snap.acks_ok, 1);
        assert_eq!(snap.stream_id_collisions, 0);
        h.shutdown().await;
    }

    #[tokio::test]
    async fn age_flush_fires_when_the_injected_clock_advances() {
        let h = Harness::spawn(no_size_flush(Duration::from_millis(50)));
        let tenant = TenantId::new("acme");
        let records = vec![norm_record(&[("service.name", "api")], "scope", 1_000, "x")];

        // Buffer first (so oldest_arrival is the pre-advance time), then push
        // the clock past max_flush_delay from the joined arm.
        let (ack_tx, ack_rx) = oneshot::channel();
        h.tx.send(LogShardMsg::Write {
            tenant: tenant.clone(),
            records,
            ack: Some(ack_tx),
            charge: None,
        })
        .await
        .expect("send write");

        let (ack, ()) = tokio::join!(ack_rx, async {
            while h.metrics.snapshot().buffered_records_total < 1 {
                tokio::task::yield_now().await;
            }
            h.clock.advance_ns(100_000_000);
        });
        let token = ack
            .expect("ack sender not dropped")
            .expect("age flush commits");

        let (_rec, scanned) = read_back(h.store.as_ref(), &tenant.hash(), &token).await;
        assert_eq!(scanned.len(), 1);
        let snap = h.metrics.snapshot();
        assert_eq!(snap.flushes_by_age, 1);
        assert_eq!(snap.flushes_by_size, 0);
        h.shutdown().await;
    }

    #[tokio::test]
    async fn shutdown_drains_buffered_records() {
        let h = Harness::spawn(no_size_flush(Duration::from_secs(3600)));
        let tenant = TenantId::new("acme");
        // Buffered (no ack): the record only reaches the store via the drain.
        h.tx.send(LogShardMsg::Write {
            tenant: tenant.clone(),
            records: vec![norm_record(&[("service.name", "api")], "scope", 1_000, "x")],
            ack: None,
            charge: None,
        })
        .await
        .expect("send write");
        while h.metrics.snapshot().buffered_records_total < 1 {
            tokio::task::yield_now().await;
        }

        let store = Arc::clone(&h.store);
        let metrics = Arc::clone(&h.metrics);
        h.shutdown().await;

        let objects = list_all(store.as_ref(), "t/").await.expect("list");
        assert!(
            objects.iter().any(|o| o.key.contains("/l0/")),
            "the shutdown drain stored a data object"
        );
        assert!(
            objects.iter().any(|o| o.key.contains("/c/")),
            "the shutdown drain stored a commit record"
        );
        assert_eq!(metrics.snapshot().flushes_manual, 1);
    }

    #[tokio::test]
    async fn two_tenants_in_one_shard_flush_independently() {
        let h = Harness::spawn(flush_on_first());
        let mut tokens = Vec::new();
        for name in ["acme", "globex"] {
            let tenant = TenantId::new(name);
            let (ack_tx, ack_rx) = oneshot::channel();
            h.tx.send(LogShardMsg::Write {
                tenant: tenant.clone(),
                records: vec![norm_record(&[("service.name", name)], "scope", 1_000, "x")],
                ack: Some(ack_tx),
                charge: None,
            })
            .await
            .expect("send write");
            let token = ack_rx.await.expect("ack").expect("commit");
            let (_rec, scanned) = read_back(h.store.as_ref(), &tenant.hash(), &token).await;
            assert_eq!(scanned.len(), 1);
            tokens.push(token);
        }
        assert_ne!(tokens[0], tokens[1], "each tenant flushes its own object");
        h.shutdown().await;
    }

    #[tokio::test]
    async fn shared_stream_id_with_diverging_attrs_flushes_to_stream_id_collision() {
        let h = Harness::spawn(flush_on_first());
        let tenant = TenantId::new("acme");

        // Two records sharing a stream_id but carrying different stream_attrs:
        // the exact state RlogWriter::finish()'s real collision check rejects.
        // Not something normalize_logs would ever produce; hand-built here.
        let good = norm_record(&[("service.name", "api")], "scope", 1_000, "x");
        let mut collider = good.clone();
        collider.stream_attrs = b"deliberately-different-attrs".to_vec();
        assert_eq!(
            good.stream_id, collider.stream_id,
            "the fixture must share a stream_id for finish() to compare attrs"
        );

        let (ack_tx, ack_rx) = oneshot::channel();
        h.tx.send(LogShardMsg::Write {
            tenant: tenant.clone(),
            records: vec![good, collider],
            ack: Some(ack_tx),
            charge: None,
        })
        .await
        .expect("send write");
        let err = ack_rx
            .await
            .expect("ack sender not dropped")
            .expect_err("a stream-id collision fails the flush");
        assert!(
            matches!(err, LogWriteError::StreamIdCollision(_)),
            "the finish() InconsistentStreamAttrs must map to StreamIdCollision, got {err:?}"
        );

        let snap = h.metrics.snapshot();
        assert_eq!(
            snap.stream_id_collisions, 1,
            "the collision increments its own counter exactly once"
        );
        assert_eq!(
            snap.abandoned_input_rejected, 0,
            "a collision is not a generic segment-build failure"
        );
        h.shutdown().await;
    }

    #[tokio::test]
    async fn shared_stream_id_with_identical_attrs_flushes_ok() {
        let h = Harness::spawn(flush_on_first());
        let tenant = TenantId::new("acme");
        let first = norm_record(&[("service.name", "api")], "scope", 1_000, "a");
        let second = norm_record(&[("service.name", "api")], "scope", 2_000, "b");
        assert_eq!(first.stream_id, second.stream_id);
        assert_eq!(first.stream_attrs, second.stream_attrs);

        let (ack_tx, ack_rx) = oneshot::channel();
        h.tx.send(LogShardMsg::Write {
            tenant: tenant.clone(),
            records: vec![first, second],
            ack: Some(ack_tx),
            charge: None,
        })
        .await
        .expect("send write");
        let token = ack_rx
            .await
            .expect("ack")
            .expect("identical attrs flush successfully");

        let (rec, scanned) = read_back(h.store.as_ref(), &tenant.hash(), &token).await;
        assert_eq!(rec.series_count, 1);
        assert_eq!(scanned.len(), 2);
        assert_eq!(h.metrics.snapshot().stream_id_collisions, 0);
        h.shutdown().await;
    }

    /// Flushes on the first write with zero backoff, so a retry-exhaustion
    /// count is deterministic with no clock advance.
    fn exhaustion_config(max_attempts: u32) -> IngestConfig {
        IngestConfig {
            shard_count: 1,
            target_bytes: 1,
            max_flush_delay: Duration::from_secs(3600),
            flush_tick: Duration::from_millis(10),
            put_retry_max_attempts: max_attempts,
            put_retry_base_delay: Duration::from_millis(0),
            put_retry_max_delay: Duration::from_millis(0),
            ..IngestConfig::default()
        }
    }

    /// A permanently-retryable fault on every RLOG data-object PUT drives
    /// exactly `put_retry_max_attempts + 1` inner PUT calls: one
    /// first attempt plus `max_attempts` retries, counted off the FaultStore.
    #[tokio::test]
    async fn log_data_put_makes_exactly_max_attempts_plus_one_calls() {
        let max_attempts = 3;
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::Transient("data down".into()))
                .with_key_contains("/l0/"),
        );
        let fault = Arc::new(FaultStore::new(MemoryStore::new(), plan));
        let store: Arc<dyn ObjectStoreBackend> = fault.clone();
        let h = Harness::spawn_with_store(exhaustion_config(max_attempts), Arc::clone(&store));

        let (ack_tx, ack_rx) = oneshot::channel();
        h.tx.send(LogShardMsg::Write {
            tenant: TenantId::new("acme"),
            records: vec![norm_record(&[("service.name", "api")], "scope", 1_000, "x")],
            ack: Some(ack_tx),
            charge: None,
        })
        .await
        .expect("send write");
        let err = ack_rx
            .await
            .expect("ack sender not dropped")
            .expect_err("a data PUT that always fails must abandon the flush");
        assert!(matches!(err, LogWriteError::Abandoned(_)));

        assert_eq!(
            fault.fault_count(Op::Put, FaultKind::Transient),
            u64::from(max_attempts) + 1,
            "total data PUT calls must be max_attempts + 1"
        );
        let snap = h.metrics.snapshot();
        assert_eq!(snap.put_retries, u64::from(max_attempts));
        assert_eq!(snap.abandoned_retry_exhausted, 1);
        h.shutdown().await;
    }

    /// Same budget on the commit-record PUT: the data object lands, then every
    /// commit PUT fails, giving `max_attempts + 1` commit PUT calls.
    #[tokio::test]
    async fn log_commit_put_makes_exactly_max_attempts_plus_one_calls() {
        let max_attempts = 3;
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::Transient("commit down".into()))
                .with_key_contains("/c/"),
        );
        let fault = Arc::new(FaultStore::new(MemoryStore::new(), plan));
        let store: Arc<dyn ObjectStoreBackend> = fault.clone();
        let h = Harness::spawn_with_store(exhaustion_config(max_attempts), Arc::clone(&store));

        let (ack_tx, ack_rx) = oneshot::channel();
        h.tx.send(LogShardMsg::Write {
            tenant: TenantId::new("acme"),
            records: vec![norm_record(&[("service.name", "api")], "scope", 1_000, "x")],
            ack: Some(ack_tx),
            charge: None,
        })
        .await
        .expect("send write");
        let err = ack_rx
            .await
            .expect("ack sender not dropped")
            .expect_err("a commit PUT that always fails must abandon the flush");
        assert!(matches!(err, LogWriteError::Abandoned(_)));

        assert_eq!(
            fault.fault_count(Op::Put, FaultKind::Transient),
            u64::from(max_attempts) + 1,
            "total commit PUT calls must be max_attempts + 1"
        );
        let snap = h.metrics.snapshot();
        assert_eq!(snap.put_retries, u64::from(max_attempts));
        assert_eq!(snap.abandoned_retry_exhausted, 1);

        let objects = list_all(store.as_ref(), "t/").await.expect("list");
        assert!(
            objects.iter().any(|o| o.key.contains("/l0/")),
            "the data object landed (orphan) before the commit PUT failed"
        );
        assert!(
            !objects.iter().any(|o| o.key.contains("/c/")),
            "no commit record ever lands"
        );
        h.shutdown().await;
    }

    /// The retry backoff waits on the injected `Clock`: with a huge backoff
    /// delay the flush parks after one retryable data-PUT fault and does not
    /// ack until the test advances the injected clock past that delay. A real
    /// timer would ignore the advance and could only finish by truly sleeping.
    #[tokio::test]
    async fn log_retry_backoff_waits_on_the_injected_clock() {
        let plan = FaultPlan::empty().with_sequence(
            Sequence::new(Op::Put)
                .with_key_contains("/l0/")
                .then_fault(ScriptedFault::Transient("blip".into()))
                .then_passthrough(),
        );
        let store: Arc<dyn ObjectStoreBackend> =
            Arc::new(FaultStore::new(MemoryStore::new(), plan));
        let backoff = Duration::from_secs(1_000);
        let config = IngestConfig {
            shard_count: 1,
            target_bytes: 1,
            max_flush_delay: Duration::from_secs(3600),
            flush_tick: Duration::from_millis(10),
            put_retry_max_attempts: 4,
            put_retry_base_delay: backoff,
            put_retry_max_delay: backoff,
            ..IngestConfig::default()
        };
        let h = Harness::spawn_with_store(config, Arc::clone(&store));
        let tenant = TenantId::new("acme");

        let (ack_tx, mut ack_rx) = oneshot::channel();
        h.tx.send(LogShardMsg::Write {
            tenant: tenant.clone(),
            records: vec![norm_record(&[("service.name", "api")], "scope", 1_000, "x")],
            ack: Some(ack_tx),
            charge: None,
        })
        .await
        .expect("send write");

        // Wait until the one retry is taken and the flush parks in backoff.
        while h.metrics.snapshot().put_retries < 1 {
            tokio::task::yield_now().await;
        }
        for _ in 0..50 {
            tokio::task::yield_now().await;
        }
        assert!(
            matches!(
                ack_rx.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            ),
            "the flush must stay parked in backoff while the injected clock is still"
        );

        h.clock.advance_ns(1_100 * 1_000_000_000);
        let token = ack_rx
            .await
            .expect("ack sender not dropped")
            .expect("the retried flush commits once the clock advances");
        let (_rec, scanned) = read_back(h.store.as_ref(), &tenant.hash(), &token).await;
        assert_eq!(scanned.len(), 1);
        assert_eq!(h.metrics.snapshot().put_retries, 1);
        h.shutdown().await;
    }

    /// An `RngSource` whose jitter draw panics, used only to inject a panic
    /// inside a spawned flush task's retry backoff (`backoff_sleep` is the one
    /// call site that touches the rng). `new_uuid` is never reached on that
    /// path, so it returns a fixed value rather than panicking.
    struct PanicOnJitterRng;

    impl RngSource for PanicOnJitterRng {
        fn jitter_ms(&self, _max_ms: u64) -> u64 {
            panic!("injected panic inside the flush task");
        }

        fn new_uuid(&self) -> Uuid {
            Uuid::nil()
        }
    }

    /// ADR-0067 decision 1 + requirement 5: `Shutdown` must not return until
    /// every in-flight flush is durable. The flush's data PUT is held so the
    /// flush is provably in flight when `Shutdown` is requested; the actor must
    /// stay inside `join_all_flushes` (its `done` unfired) until the PUT is
    /// released and the object and commit record are durable.
    ///
    /// Flip proof: deleting `self.join_all_flushes().await;` from
    /// `LogShardActor::flush_all` makes this test fail -- `done` then fires while
    /// the data PUT is still held, so the `try_recv() == Empty` assertion below
    /// trips and no `/l0/` object is durable. Verified by removing that one line
    /// during development, observing the failure, and restoring it.
    #[tokio::test]
    async fn shutdown_joins_inflight_flush_before_returning() {
        let fault = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
        let store: Arc<dyn ObjectStoreBackend> = fault.clone();
        let h = Harness::spawn_with_store(flush_on_first(), Arc::clone(&store));
        // Hold the data-object PUT: the spawned flush task parks on it.
        let gate = fault.hold(Op::Put, Some("/l0/".to_string()), Occurrence::Nth(1));

        // target_bytes == 1, so this write triggers a size flush at once.
        h.tx.send(LogShardMsg::Write {
            tenant: TenantId::new("acme"),
            records: vec![norm_record(&[("service.name", "api")], "scope", 1_000, "x")],
            ack: None,
            charge: None,
        })
        .await
        .expect("send write");
        gate.wait_until_held(1).await;

        // Request shutdown while the flush is parked mid-PUT.
        let (done_tx, mut done_rx) = oneshot::channel();
        h.tx.send(LogShardMsg::Shutdown { done: done_tx })
            .await
            .expect("send shutdown");
        for _ in 0..200 {
            tokio::task::yield_now().await;
        }
        assert!(
            matches!(
                done_rx.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            ),
            "shutdown returned while a flush PUT was still in flight"
        );
        let before = list_all(store.as_ref(), "t/").await.expect("list");
        assert!(
            !before.iter().any(|o| o.key.contains("/l0/")),
            "the held PUT has written no object yet"
        );

        // Release the PUT: the flush drains, and only now may shutdown return.
        let ids = gate.held();
        assert_eq!(ids.len(), 1, "exactly one PUT is held");
        gate.release(ids[0]);
        done_rx
            .await
            .expect("shutdown completes once the flush is durable");

        let objects = list_all(store.as_ref(), "t/").await.expect("list");
        assert!(
            objects.iter().any(|o| o.key.contains("/l0/")),
            "the joined flush stored its data object before shutdown returned"
        );
        assert!(
            objects.iter().any(|o| o.key.contains("/c/")),
            "the joined flush stored its commit record before shutdown returned"
        );
        let _ = h.task.await;
    }

    /// ADR-0069 refund on the happy path: the charge the buffer held is dropped
    /// inside the flush task at its terminal outcome, so the process-wide gauge
    /// returns to exactly the sentinel it started at (never leaking the flush's
    /// bytes, never double-refunding below the sentinel). Asserted against the
    /// budget's own `in_flight_bytes` counter.
    #[tokio::test]
    async fn charge_refunded_once_on_successful_flush() {
        const SENTINEL: u64 = 4_242;
        const FLUSH: u64 = 777;
        let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(1_000_000));
        let _sentinel = budget.try_charge(SENTINEL).expect("sentinel charge");
        let charge = Arc::new(budget.try_charge(FLUSH).expect("flush charge"));
        assert_eq!(budget.in_flight_bytes(), SENTINEL + FLUSH);

        let h = Harness::spawn(flush_on_first());
        let (ack_tx, ack_rx) = oneshot::channel();
        h.tx.send(LogShardMsg::Write {
            tenant: TenantId::new("acme"),
            records: vec![norm_record(&[("service.name", "api")], "scope", 1_000, "x")],
            ack: Some(ack_tx),
            charge: Some(charge),
        })
        .await
        .expect("send write");
        ack_rx.await.expect("ack").expect("strict write commits");
        // shutdown joins the flush task, so `_charges` is provably dropped.
        h.shutdown().await;
        assert_eq!(
            budget.in_flight_bytes(),
            SENTINEL,
            "the flush charge refunded exactly once on a successful flush"
        );
    }

    /// ADR-0069 refund when the flush is abandoned at the commit PUT: the charge
    /// must still refund exactly once. Same sentinel assertion.
    #[tokio::test]
    async fn charge_refunded_once_on_publish_failure() {
        const SENTINEL: u64 = 4_242;
        const FLUSH: u64 = 777;
        let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(1_000_000));
        let _sentinel = budget.try_charge(SENTINEL).expect("sentinel charge");
        let charge = Arc::new(budget.try_charge(FLUSH).expect("flush charge"));

        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::Transient("commit down".into()))
                .with_key_contains("/c/"),
        );
        let store: Arc<dyn ObjectStoreBackend> =
            Arc::new(FaultStore::new(MemoryStore::new(), plan));
        let h = Harness::spawn_with_store(exhaustion_config(2), Arc::clone(&store));
        let (ack_tx, ack_rx) = oneshot::channel();
        h.tx.send(LogShardMsg::Write {
            tenant: TenantId::new("acme"),
            records: vec![norm_record(&[("service.name", "api")], "scope", 1_000, "x")],
            ack: Some(ack_tx),
            charge: Some(charge),
        })
        .await
        .expect("send write");
        let err = ack_rx
            .await
            .expect("ack")
            .expect_err("commit PUT that always fails abandons the flush");
        assert!(matches!(err, LogWriteError::Abandoned(_)));
        h.shutdown().await;
        assert_eq!(
            budget.in_flight_bytes(),
            SENTINEL,
            "the flush charge refunded exactly once on publish failure"
        );
    }

    /// ADR-0069 refund when the flush task panics: `_charges` is on the task's
    /// stack, so its `Drop` runs during the unwind and the bytes still refund
    /// exactly once. A panicking rng in the retry backoff supplies the panic;
    /// the actor is constructed directly here to inject it.
    #[tokio::test]
    async fn charge_refunded_once_on_flush_task_panic() {
        const SENTINEL: u64 = 4_242;
        const FLUSH: u64 = 777;
        let budget = IngestByteBudget::shared(IngestByteBudgetLimit::Bounded(1_000_000));
        let _sentinel = budget.try_charge(SENTINEL).expect("sentinel charge");
        let charge = Arc::new(budget.try_charge(FLUSH).expect("flush charge"));

        // A permanently-retryable data PUT drives the flush into its retry
        // backoff, where the panicking rng fires.
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::Transient("data down".into()))
                .with_key_contains("/l0/"),
        );
        let store: Arc<dyn ObjectStoreBackend> =
            Arc::new(FaultStore::new(MemoryStore::new(), plan));
        let clock = TestClock::new(BASE_NS);
        let metrics = Arc::new(LogIngestMetrics::default());
        let (tx, rx) = mpsc::channel(64);
        let actor = LogShardActor::new(
            0,
            Uuid::new_v4(),
            7,
            Arc::clone(&store),
            clock.clone(),
            Arc::new(PanicOnJitterRng),
            exhaustion_config(4),
            Arc::clone(&metrics),
            rx,
            Arc::new(IndexedFieldsOverlay::new(Arc::new(
                crate::log_router::NoIndexedFields,
            ))),
        );
        let task = tokio::spawn(actor.run());

        tx.send(LogShardMsg::Write {
            tenant: TenantId::new("acme"),
            records: vec![norm_record(&[("service.name", "api")], "scope", 1_000, "x")],
            ack: None,
            charge: Some(charge),
        })
        .await
        .expect("send write");

        // The flush task panics during backoff; its `_charges` drop refunds the
        // bytes. Poll the budget until it returns to the sentinel.
        let mut refunded = false;
        for _ in 0..100_000 {
            if budget.in_flight_bytes() == SENTINEL {
                refunded = true;
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(
            refunded,
            "the panicking flush task must still refund the charge exactly once (gauge {} != {SENTINEL})",
            budget.in_flight_bytes()
        );
        // The actor reaps the panicked task and resumes the unwind, so its own
        // task ends by panic; awaiting it here just drains that outcome.
        let _ = task.await;
    }

    /// Logs do not dedup at query time (docs/consistency-model.md), so
    /// resolution over pipelined flushes must include BOTH landed commits even
    /// when a higher seq lands first. Two concurrent flushes are forced
    /// out-of-order: seq0's commit PUT is held so seq1 commits first, then seq0
    /// is released. Both objects resolve independently and every record survives
    /// with no loss and no duplication.
    #[tokio::test]
    async fn catalog_resolve_correct_over_out_of_order_commit_landings() {
        let config = IngestConfig {
            max_inflight_flushes: 2,
            ..flush_on_first()
        };
        let fault = Arc::new(FaultStore::new(MemoryStore::new(), FaultPlan::empty()));
        let store: Arc<dyn ObjectStoreBackend> = fault.clone();
        let h = Harness::spawn_with_store(config, Arc::clone(&store));
        let tenant = TenantId::new("acme");
        // Hold the first commit-record PUT: that is seq0's.
        let gate = fault.hold(Op::Put, Some("/c/".to_string()), Occurrence::Nth(1));

        // Write A -> flush seq0. Its data PUT lands; its commit PUT is held.
        let (ack_a_tx, mut ack_a_rx) = oneshot::channel();
        h.tx.send(LogShardMsg::Write {
            tenant: tenant.clone(),
            records: vec![norm_record(&[("service.name", "api")], "scope", 1_000, "a")],
            ack: Some(ack_a_tx),
            charge: None,
        })
        .await
        .expect("send A");
        gate.wait_until_held(1).await;

        // Write B -> flush seq1. Nothing is held for it, so it commits while A
        // is still parked: seq1 lands before seq0.
        let (ack_b_tx, ack_b_rx) = oneshot::channel();
        h.tx.send(LogShardMsg::Write {
            tenant: tenant.clone(),
            records: vec![norm_record(&[("service.name", "api")], "scope", 2_000, "b")],
            ack: Some(ack_b_tx),
            charge: None,
        })
        .await
        .expect("send B");
        let token_b = ack_b_rx.await.expect("ack B").expect("B commits first");
        assert!(
            matches!(
                ack_a_rx.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            ),
            "seq0 must still be in flight while seq1 has already committed"
        );

        // Release seq0's commit; A now commits too.
        let ids = gate.held();
        assert_eq!(ids.len(), 1);
        gate.release(ids[0]);
        let token_a = ack_a_rx
            .await
            .expect("ack A")
            .expect("A commits after release");

        let (rec_a, recs_a) = read_back(store.as_ref(), &tenant.hash(), &token_a).await;
        let (rec_b, recs_b) = read_back(store.as_ref(), &tenant.hash(), &token_b).await;
        assert_eq!(rec_a.writer_seq, 0, "A pinned first -> seq 0");
        assert_eq!(
            rec_b.writer_seq, 1,
            "B pinned second -> seq 1, though it landed first"
        );
        assert_ne!(token_a, token_b, "each flush resolves to its own object");
        assert_eq!(recs_a.len(), 1);
        assert_eq!(recs_b.len(), 1);
        assert_eq!(
            recs_a[0].body, "a",
            "seq0's record resolves to its own body"
        );
        assert_eq!(
            recs_b[0].body, "b",
            "seq1's record resolves to its own body"
        );
        h.shutdown().await;
    }

    /// A log record on a fixed stream carrying one per-record attribute, so a
    /// durable indexed-field override on that attribute has something to index.
    fn norm_record_with_status(status: &str) -> NormalizedLogRecord {
        let mut rec = norm_record(&[("service.name", "api")], "scope", 1_000, "x");
        rec.attrs = vec![(
            "http.status_code".to_string(),
            AttrValue::Str(status.to_string()),
        )];
        rec
    }

    /// ADR-0079 acceptance test: a durable `TenantConfig.indexed_fields` override
    /// genuinely changes what a REAL log flush indexes, driven end to end through
    /// `LogFlushCtx::run_flush` (not a bare overlay unit test). The overlay's base
    /// is `NoIndexedFields` (indexes nothing), so any indexing observed comes
    /// solely from the durable override the overlay read off the store the flush
    /// path passes it. Then the override is cleared and, once the cache goes stale
    /// past the horizon, a second real flush reverts to indexing nothing -- a
    /// no-restart change in both directions.
    ///
    /// Observed through the write-side POSTINGS counters, the same "observable
    /// behaviour changed" signal `postings_config.rs`'s acceptance tests use, and
    /// templated on `lifecycle.rs`'s
    /// `refresh_reinvokes_set_tenant_limits_and_admission_behaviour_changes`
    /// (write override, drive the real refresh, assert live behaviour changed,
    /// clear it, assert it reverts).
    ///
    /// Prove-the-test: stubbing the overlay's durable branch to always resolve the
    /// base (i.e. ignoring `TenantConfig.indexed_fields`) makes the first
    /// assertion below fail -- `postings_indexed_fields_total` stays 0 because the
    /// `NoIndexedFields` base indexes nothing, so the override never took effect.
    #[tokio::test]
    async fn durable_indexed_fields_override_changes_a_real_flush_then_reverts() {
        use ravel_catalog::{TenantConfig, TenantLifecycleState, set_tenant_config};

        let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
        let tenant = TenantId::new("acme");

        // Write the durable override: index `http.status_code`. The overlay's base
        // is NoIndexedFields, so this is the only thing that can index anything.
        set_tenant_config(
            store.as_ref(),
            &tenant.hash(),
            &TenantConfig {
                indexed_fields: Some(vec!["http.status_code".to_string()]),
                ..TenantConfig::new(TenantLifecycleState::Active)
            },
            1,
        )
        .await
        .expect("write durable override");

        let h = Harness::spawn_with_store(flush_on_first(), Arc::clone(&store));

        // First real flush: the override applies, so the object carries a POSTINGS
        // section indexing exactly the one overridden field.
        let (ack_tx, ack_rx) = oneshot::channel();
        h.tx.send(LogShardMsg::Write {
            tenant: tenant.clone(),
            records: vec![
                norm_record_with_status("200"),
                norm_record_with_status("500"),
            ],
            ack: Some(ack_tx),
            charge: None,
        })
        .await
        .expect("send first write");
        ack_rx.await.expect("ack").expect("first flush commits");

        let snap = h.metrics.snapshot();
        assert_eq!(
            snap.postings_objects, 1,
            "the durable override made the first flush index a field, so the object \
             carries a POSTINGS section"
        );
        assert_eq!(
            snap.postings_indexed_fields_total, 1,
            "exactly the one overridden field (http.status_code) is indexed"
        );
        assert_eq!(
            snap.indexed_fields_stale_fallbacks, 0,
            "a healthy store resolves the override from a fresh durable read, not a fallback"
        );

        // Clear the override (no per-tenant indexed-field record => the base
        // NoIndexedFields governs again).
        set_tenant_config(
            store.as_ref(),
            &tenant.hash(),
            &TenantConfig::new(TenantLifecycleState::Active),
            2,
        )
        .await
        .expect("clear durable override");

        // Age the overlay's cached entry past the staleness horizon so the next
        // flush re-reads the (now cleared) durable config rather than serving the
        // still-fresh cached override.
        h.clock
            .advance_ns(ravel_ingest_default_lifecycle_horizon_ns().saturating_add(1_000_000_000));

        // Second real flush: the override is gone, so the object indexes nothing
        // and the POSTINGS counters do not move.
        let (ack_tx, ack_rx) = oneshot::channel();
        h.tx.send(LogShardMsg::Write {
            tenant: tenant.clone(),
            records: vec![norm_record_with_status("200")],
            ack: Some(ack_tx),
            charge: None,
        })
        .await
        .expect("send second write");
        ack_rx.await.expect("ack").expect("second flush commits");

        let snap = h.metrics.snapshot();
        assert_eq!(
            snap.postings_objects, 1,
            "clearing the override reverts to indexing nothing: the second object \
             carries no POSTINGS section, so the object count does not move"
        );
        assert_eq!(
            snap.postings_indexed_fields_total, 1,
            "the second flush indexed no field, so the cumulative indexed-fields \
             count stays at the first flush's 1"
        );
        assert_eq!(
            snap.indexed_fields_stale_fallbacks, 0,
            "both re-reads succeeded against a healthy store; nothing fell back"
        );
        h.shutdown().await;
    }

    /// The staleness horizon the overlay uses by default, restated here so the
    /// acceptance test can age the cache past it without depending on the private
    /// constant. Kept in sync with `IndexedFieldsOverlay::new`'s horizon.
    fn ravel_ingest_default_lifecycle_horizon_ns() -> i64 {
        crate::DEFAULT_LIFECYCLE_REFRESH_INTERVAL_NS
    }
}
