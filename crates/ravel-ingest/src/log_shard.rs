//! One actor per log shard: actor-local buffering, adaptive flush, and the
//! pinned-identity commit sequence, the log-pipeline counterpart of
//! [`crate::shard`] (docs/ingest.md "Shard actor", docs/catalog-and-mvcc.md
//! "Pinned flush identity" and "Commit sequence").
//!
//! The divergences from the metrics shard actor are deliberate and narrow:
//! the buffer holds [`NormalizedLogRecord`]s instead of points, the flush
//! builds an RLOG object with [`RlogWriter`] instead of an RSEG segment, and
//! identity is a `stream_id` rather than a `series_id`. One difference is
//! worth restating: this buffer performs no stream-identity collision check
//! of its own. Unlike [`crate::shard::TenantBuf::merge`]'s ADR-0005 series-id
//! check, the equivalent fail-loud check for logs already lives in
//! [`RlogWriter::finish`] (`LogSegError::InconsistentStreamAttrs`, issue
//! #225), which compares every buffered record's `stream_attrs` for a shared
//! `stream_id`. Duplicating it here would only be dead code with a second
//! chance to drift, so the flush step maps that one `finish()` error variant
//! to [`LogWriteError::StreamIdCollision`] instead.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use rand::RngExt as _;
use ravel_commit::keys;
use ravel_commit::publish::{self, PublishError, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_logseg::{LogRecord, LogSegError, ObjectIdentity, RlogConfig, RlogWriter};
use ravel_object_store::ObjectStoreBackend;
use ravel_otlp::logs_normalize::NormalizedLogRecord;
use ravel_proto::commit::v1::CommitRecord;
use ravel_types::logstream::{AttrValue, LogStreamId};
use ravel_types::{CommitToken, Signal, TenantId};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Duration;
use uuid::Uuid;

use crate::clock::Clock;
use crate::config::{IngestConfig, LOG_SEGMENT_FORMAT_VERSION};
use crate::log_error::LogWriteError;
use crate::log_metrics::LogIngestMetrics;
use crate::metrics::FlushTrigger;

const NS_PER_HOUR: i64 = 3_600_000_000_000;

pub(crate) type LogAck = oneshot::Sender<Result<CommitToken, LogWriteError>>;

pub(crate) enum LogShardMsg {
    Write {
        tenant: TenantId,
        records: Vec<NormalizedLogRecord>,
        ack: Option<LogAck>,
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
fn est_record_bytes(rec: &NormalizedLogRecord) -> usize {
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

pub(crate) struct LogShardActor {
    shard: u32,
    writer_id: Uuid,
    epoch: u64,
    next_seq: u64,
    store: Arc<dyn ObjectStoreBackend>,
    clock: Arc<dyn Clock>,
    config: IngestConfig,
    metrics: Arc<LogIngestMetrics>,
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
        config: IngestConfig,
        metrics: Arc<LogIngestMetrics>,
        rx: mpsc::Receiver<LogShardMsg>,
    ) -> Self {
        LogShardActor {
            shard,
            writer_id,
            epoch,
            next_seq: 0,
            store,
            clock,
            config,
            metrics,
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
                        Some(LogShardMsg::Write { tenant, records, ack }) => {
                            self.handle_write(tenant, records, ack).await;
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
            }
        }
    }

    async fn handle_write(
        &mut self,
        tenant: TenantId,
        records: Vec<NormalizedLogRecord>,
        ack: Option<LogAck>,
    ) {
        if records.is_empty() && ack.is_none() {
            return;
        }
        let arrival_ns = self.clock.now_ns();
        let records_len = records.len() as u64;
        let target_bytes = self.config.target_bytes;

        let buf = self.tenants.entry(tenant.clone()).or_default();
        let bytes_added = buf.merge(records, arrival_ns);
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

    async fn flush_aged(&mut self) {
        let now = self.clock.now_ns();
        let max_delay_ns = self.config.max_flush_delay.as_nanos() as i64;
        let due: Vec<TenantId> = self
            .tenants
            .iter()
            .filter(|(_, buf)| {
                buf.oldest_arrival_ns
                    .map(|t| now.saturating_sub(t) >= max_delay_ns)
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
    }

    /// Runs the full pinned-identity commit sequence for one tenant's buffer,
    /// mirroring [`crate::shard::ShardActor::flush_tenant`] step for step:
    /// `seq`, `ingest_hour_bucket`, the serialized RLOG object, and its blake3
    /// hash are each computed exactly once here and reused verbatim by every
    /// retry (docs/catalog-and-mvcc.md "Pinned flush identity"). Nothing below
    /// may re-serialize, accrete new records, or re-read the clock.
    ///
    /// The one log-specific step is `finish()` error mapping: an
    /// `InconsistentStreamAttrs` becomes [`LogWriteError::StreamIdCollision`]
    /// and increments `stream_id_collisions`; every other `LogSegError`
    /// becomes [`LogWriteError::SegmentBuild`]. This is the only site that
    /// constructs `StreamIdCollision`, because the collision check itself now
    /// lives in `finish()` (issue #225), not in this module.
    async fn flush_tenant(&mut self, tenant: TenantId, buf: LogTenantBuf, trigger: FlushTrigger) {
        let LogTenantBuf {
            records,
            min_ingest_ts_ns,
            max_ingest_ts_ns,
            waiters,
            ..
        } = buf;
        if records.is_empty() {
            return;
        }
        self.metrics.record_flush(trigger);

        let tenant_hash = tenant.hash();
        let seq = self.next_seq;
        self.next_seq += 1;
        let flush_open_ns = self.clock.now_ns();
        let ingest_hour_bucket = u32::try_from(flush_open_ns.div_euclid(NS_PER_HOUR)).unwrap_or(0);
        let deadline_ns =
            flush_open_ns.saturating_add(self.config.max_flush_lifetime.as_nanos() as i64);

        // One pass over the batch computes the commit-record fields RlogWriter
        // does not surface after `finish()`: the distinct stream count (the
        // log analogue of series_count) and the event-time bounds. Tracked
        // locally, the same way min/max ingest ts are, rather than re-derived
        // from the written bytes.
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

        let identity = ObjectIdentity {
            tenant_hash: tenant_hash.0,
            shard: self.shard,
            writer_id: self.writer_id.into_bytes(),
            writer_epoch: self.epoch,
            writer_seq: seq,
        };
        let mut writer = RlogWriter::new(RlogConfig::default(), identity);
        for rec in records {
            if let Err(e) = writer.push(to_logseg_record(rec)) {
                self.metrics.record_abandoned_input_rejected();
                self.ack_waiters(waiters, Err(LogWriteError::SegmentBuild(e.to_string())));
                return;
            }
        }
        let bytes = match writer.finish() {
            Ok(bytes) => bytes,
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

        let min_ingest_ts_ns = min_ingest_ts_ns.unwrap_or(flush_open_ns);
        let max_ingest_ts_ns = max_ingest_ts_ns.unwrap_or(flush_open_ns);
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

    fn ack_waiters(&self, waiters: Vec<LogAck>, result: Result<CommitToken, LogWriteError>) {
        let ok = result.is_ok();
        self.metrics.record_acks(waiters.len(), ok);
        for waiter in waiters {
            let _ = waiter.send(result.clone());
        }
    }

    /// Races `fut` against the remaining budget to `deadline_ns` on the
    /// injected `Clock`, returning `None` if the deadline is already past or
    /// elapses while `fut` is still in flight. Identical in construction to
    /// [`crate::shard::ShardActor`]'s own `bound_to_deadline`: built on
    /// `tokio::select!` racing `self.clock.sleep(..)` rather than
    /// `tokio::time::timeout`, so the deadline stays on the injected clock a
    /// test can pin and advance.
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
    /// `deadline_ns` via [`Self::bound_to_deadline`], so a timeout never
    /// retries past the deadline and is treated exactly like the abandonment
    /// path. Mirrors [`crate::shard::ShardActor`]'s equivalent.
    async fn put_data_object_with_retry(&self, key: &str, bytes: Bytes, deadline_ns: i64) -> bool {
        let mut attempt: u32 = 0;
        loop {
            let call = publish::put_data_object(self.store.as_ref(), key, bytes.clone());
            match self.bound_to_deadline(deadline_ns, call).await {
                Some(Ok(())) => return true,
                Some(Err(PublishError::Store { source, .. })) if source.is_retryable() => {
                    attempt += 1;
                    if attempt >= self.config.put_retry_max_attempts
                        || self.clock.now_ns() >= deadline_ns
                    {
                        return false;
                    }
                    self.metrics.record_put_retry();
                    self.backoff_sleep(attempt).await;
                }
                Some(Err(_)) | None => return false,
            }
        }
    }

    /// Retries the commit-record PUT with the caller's own budget, passing
    /// `publish` a zero-retry policy so it attempts once per call and this loop
    /// checks `deadline_ns` between attempts. Mirrors
    /// [`crate::shard::ShardActor`]'s equivalent, including the pinned-identity
    /// split-brain panic: identity is fixed at flush open, so a split-brain
    /// cannot fire on a benign retry and means the pinning invariant was
    /// broken upstream.
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
                    attempt += 1;
                    if attempt >= self.config.put_retry_max_attempts
                        || self.clock.now_ns() >= deadline_ns
                    {
                        return None;
                    }
                    self.metrics.record_put_retry();
                    self.backoff_sleep(attempt).await;
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
        let jittered_ms = rand::rng().random_range(0..=capped_ms);
        tokio::time::sleep(Duration::from_millis(jittered_ms)).await;
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::pin::Pin;
    use std::sync::atomic::{AtomicI64, Ordering};

    use ravel_commit::{keys, record};
    use ravel_logseg::{Predicate, RlogReader, stream_attrs_bytes};
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{GetRange, list_all};
    use ravel_types::TenantHash;
    use ravel_types::logstream::log_stream_id;
    use tokio::sync::watch;
    use tokio::task::JoinHandle;

    use super::*;

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
            let store: Arc<dyn ObjectStoreBackend> = Arc::new(MemoryStore::new());
            let clock = TestClock::new(BASE_NS);
            let metrics = Arc::new(LogIngestMetrics::default());
            let (tx, rx) = mpsc::channel(64);
            let actor = LogShardActor::new(
                0,
                Uuid::new_v4(),
                7,
                Arc::clone(&store),
                clock.clone(),
                config,
                Arc::clone(&metrics),
                rx,
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
}
