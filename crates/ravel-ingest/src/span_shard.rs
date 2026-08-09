//! One actor per span shard: actor-local buffering, adaptive flush, and the
//! pinned-identity commit sequence, the span-pipeline counterpart of
//! [`crate::log_shard`] (docs/ingest.md "Shard actor", docs/catalog-and-mvcc.md
//! "Pinned flush identity" and "Commit sequence", ADR-0041).
//!
//! The divergences from the log shard actor are deliberate and narrow: the
//! buffer holds [`NormalizedSpan`]s, the flush builds an RSPAN object with
//! [`RspanWriter`] instead of an RLOG one, the commit record is published under
//! [`Signal::Spans`], and the commit record's `series_count` counts distinct
//! `trace_id`s rather than distinct stream ids. There is also no
//! identity-collision check anywhere in this path: a span's `trace_id` and
//! `span_id` come from the sender verbatim, so unlike a log's derived
//! `stream_id` there is nothing that two different inputs could collide on
//! (see [`SpanWriteError`]).
//!
//! Event-time bounds on the commit record come from the span *interval*: the
//! minimum `start_ts_ns` and the maximum `end_ts_ns` across the batch, so the
//! range a commit record advertises is the same interval RSPAN's skip index
//! prunes with (ADR-0041 decision 3), not a point-in-time min/max.

use std::collections::{HashMap, HashSet};
use std::future::Future;
use std::sync::Arc;

use bytes::Bytes;
use ravel_commit::keys;
use ravel_commit::publish::{self, PublishError, RetryPolicy};
use ravel_commit::record::{self, NewCommitRecord};
use ravel_commit::rng::RngSource;
use ravel_object_store::ObjectStoreBackend;
use ravel_otlp::traces_normalize::NormalizedSpan;
use ravel_proto::commit::v1::CommitRecord;
use ravel_rspan::{ObjectIdentity, RspanConfig, RspanWriter, SpanRecord};
use ravel_types::{CommitToken, Signal, TenantId};
use tokio::sync::{mpsc, oneshot};
use tokio::time::Duration;
use uuid::Uuid;

use crate::clock::Clock;
use crate::config::{IngestConfig, SPAN_SEGMENT_FORMAT_VERSION, checked_ingest_hour_bucket};
use crate::metrics::FlushTrigger;
use crate::span_error::SpanWriteError;
use crate::span_metrics::SpanIngestMetrics;

pub(crate) type SpanAck = oneshot::Sender<Result<CommitToken, SpanWriteError>>;

pub(crate) enum SpanShardMsg {
    Write {
        tenant: TenantId,
        spans: Vec<NormalizedSpan>,
        ack: Option<SpanAck>,
    },
    /// Flush every buffered tenant now, regardless of size/age thresholds.
    FlushNow { done: oneshot::Sender<()> },
    /// Flush every buffered tenant, then stop the actor loop.
    Shutdown { done: oneshot::Sender<()> },
}

/// Estimated buffered byte cost of one span, per the `est_bytes` rule
/// (docs/ingest.md, mirroring [`crate::log_shard`]'s `est_record_bytes`): the
/// name, the status message, every attribute key and value, plus a fixed 64
/// covering the two i64 timestamps, the 16-byte trace id, the two 8-byte span
/// ids, and the status code. A `target_bytes` flush-trigger estimate, not a
/// byte-exact accounting of the RSPAN output.
fn est_span_bytes(span: &NormalizedSpan) -> usize {
    let attr_bytes: usize = span.attrs.iter().map(|(k, v)| k.len() + v.len()).sum();
    span.name.len() + span.status_message.as_ref().map(String::len).unwrap_or(0) + attr_bytes + 64
}

/// Type-level bridge from the OTLP-independent [`NormalizedSpan`] to the
/// writer's [`SpanRecord`]. Every field maps one to one; there is no data
/// transformation, only a struct rename. In particular `attrs` is already the
/// merged resource+scope+span map [`ravel_rspan::merge_attrs`] produced during
/// normalization, so no merge happens at flush time.
fn to_rspan_record(span: NormalizedSpan) -> SpanRecord {
    SpanRecord {
        trace_id: span.trace_id,
        span_id: span.span_id,
        parent_span_id: span.parent_span_id,
        name: span.name,
        start_ts_ns: span.start_ts_ns,
        end_ts_ns: span.end_ts_ns,
        status_code: span.status_code,
        status_message: span.status_message,
        attrs: span.attrs,
    }
}

/// One tenant's accumulated spans in a single shard buffer, the span-pipeline
/// counterpart of [`crate::log_shard`]'s `LogTenantBuf`.
#[derive(Default)]
struct SpanTenantBuf {
    spans: Vec<NormalizedSpan>,
    est_bytes: usize,
    oldest_arrival_ns: Option<i64>,
    min_ingest_ts_ns: Option<i64>,
    max_ingest_ts_ns: Option<i64>,
    waiters: Vec<SpanAck>,
}

impl SpanTenantBuf {
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

    /// Appends `spans` to this buffer and returns the estimated byte cost
    /// added (per [`est_span_bytes`]). Like the log buffer and unlike the
    /// metrics one this never fails: there is no identity check to reject on.
    fn merge(&mut self, spans: Vec<NormalizedSpan>, arrival_ns: i64) -> usize {
        self.note_arrival(arrival_ns);
        let bytes_added: usize = spans.iter().map(est_span_bytes).sum();
        self.spans.extend(spans);
        self.est_bytes += bytes_added;
        bytes_added
    }
}

pub(crate) struct SpanShardActor {
    shard: u32,
    writer_id: Uuid,
    epoch: u64,
    next_seq: u64,
    store: Arc<dyn ObjectStoreBackend>,
    clock: Arc<dyn Clock>,
    rng: Arc<dyn RngSource>,
    config: IngestConfig,
    metrics: Arc<SpanIngestMetrics>,
    rx: mpsc::Receiver<SpanShardMsg>,
    tenants: HashMap<TenantId, SpanTenantBuf>,
}

impl SpanShardActor {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        shard: u32,
        writer_id: Uuid,
        epoch: u64,
        store: Arc<dyn ObjectStoreBackend>,
        clock: Arc<dyn Clock>,
        rng: Arc<dyn RngSource>,
        config: IngestConfig,
        metrics: Arc<SpanIngestMetrics>,
        rx: mpsc::Receiver<SpanShardMsg>,
    ) -> Self {
        SpanShardActor {
            shard,
            writer_id,
            epoch,
            next_seq: 0,
            store,
            clock,
            rng,
            config,
            metrics,
            rx,
            tenants: HashMap::new(),
        }
    }

    pub(crate) async fn run(mut self) {
        // The flush-tick cadence runs on the injected `Clock`, not the tokio
        // timer, exactly as [`crate::log_shard`] does it: age-based flush
        // timing shares the one clock the age check itself reads, so a test
        // that advances the injected clock past `max_flush_delay` drives a
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
                        Some(SpanShardMsg::Write { tenant, spans, ack }) => {
                            self.handle_write(tenant, spans, ack).await;
                        }
                        Some(SpanShardMsg::FlushNow { done }) => {
                            self.flush_all(FlushTrigger::Manual).await;
                            let _ = done.send(());
                        }
                        Some(SpanShardMsg::Shutdown { done }) => {
                            self.flush_all(FlushTrigger::Manual).await;
                            let _ = done.send(());
                            break;
                        }
                        None => {
                            // Every sender was dropped without an explicit
                            // shutdown. A graceful teardown is not a crash, and
                            // buffered spans are only permitted to be lost to a
                            // crash (docs/consistency-model.md), so flush before
                            // breaking; log first so the close is observable
                            // even if the flush is abandoned.
                            if !self.tenants.is_empty() {
                                let (tenant_count, buffered_spans) = self.buffered_summary();
                                tracing::warn!(
                                    shard = self.shard,
                                    tenant_count,
                                    buffered_spans,
                                    "span shard actor channel closed without shutdown; \
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
        spans: Vec<NormalizedSpan>,
        ack: Option<SpanAck>,
    ) {
        if spans.is_empty() && ack.is_none() {
            return;
        }
        let arrival_ns = self.clock.now_ns();
        let spans_len = spans.len() as u64;
        let target_bytes = self.config.target_bytes;

        let buf = self.tenants.entry(tenant.clone()).or_default();
        let bytes_added = buf.merge(spans, arrival_ns);
        if let Some(ack) = ack {
            buf.waiters.push(ack);
        }
        self.metrics.record_buffered(bytes_added as u64, spans_len);

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
    /// (ADR-0051 section 7, S3-05). Strict-mode ack latency is unaffected:
    /// a strict write always leaves `waiters` non-empty for its whole flush
    /// window.
    fn age_threshold_ns(&self, buf: &SpanTenantBuf) -> i64 {
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

    /// Returns `(tenant_count, buffered_span_count)` across every buffered
    /// tenant, for the channel-close log line.
    fn buffered_summary(&self) -> (usize, u64) {
        let spans: u64 = self
            .tenants
            .values()
            .map(|buf| buf.spans.len() as u64)
            .sum();
        (self.tenants.len(), spans)
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
    /// mirroring [`crate::log_shard`]'s `flush_tenant` step for step: `seq`,
    /// `ingest_hour_bucket`, the serialized RSPAN object, and its blake3 hash
    /// are each computed exactly once here and reused verbatim by every retry
    /// (docs/catalog-and-mvcc.md "Pinned flush identity"). Nothing below may
    /// re-serialize, accrete new spans, or re-read the clock.
    async fn flush_tenant(&mut self, tenant: TenantId, buf: SpanTenantBuf, trigger: FlushTrigger) {
        let SpanTenantBuf {
            spans,
            min_ingest_ts_ns,
            max_ingest_ts_ns,
            waiters,
            ..
        } = buf;
        if spans.is_empty() {
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
                self.ack_waiters(waiters, Err(SpanWriteError::SegmentBuild(msg)));
                return;
            }
        };
        let deadline_ns =
            flush_open_ns.saturating_add(self.config.max_flush_lifetime.as_nanos() as i64);

        // One pass computes the commit-record fields RspanWriter does not
        // surface after `finish()`: the distinct trace count (the span
        // analogue of series_count, ADR-0041's routing key) and the event-time
        // interval bounds. Tracked locally, the same way min/max ingest ts are,
        // rather than re-derived from the written bytes.
        let mut trace_ids: HashSet<[u8; 16]> = HashSet::new();
        let mut min_event_ts_ns = i64::MAX;
        let mut max_event_ts_ns = i64::MIN;
        for span in &spans {
            trace_ids.insert(span.trace_id);
            min_event_ts_ns = min_event_ts_ns.min(span.start_ts_ns);
            max_event_ts_ns = max_event_ts_ns.max(span.end_ts_ns);
        }
        let series_count = trace_ids.len() as u64;
        let sample_count = spans.len() as u64;

        let identity = ObjectIdentity {
            tenant_hash: tenant_hash.0,
            shard: self.shard,
            writer_id: self.writer_id.into_bytes(),
            writer_epoch: self.epoch,
            writer_seq: seq,
        };
        let mut writer = RspanWriter::new(RspanConfig::default(), identity);
        for span in spans {
            writer.push(to_rspan_record(span));
        }
        let bytes = match writer.finish() {
            Ok(bytes) => bytes,
            Err(e) => {
                self.metrics.record_abandoned_input_rejected();
                self.ack_waiters(waiters, Err(SpanWriteError::SegmentBuild(e.to_string())));
                return;
            }
        };

        let content_hash: [u8; 32] = *blake3::hash(&bytes).as_bytes();
        let data = Bytes::from(bytes);

        let data_key = match keys::data_key(
            &tenant_hash,
            Signal::Spans,
            self.shard,
            self.writer_id,
            self.epoch,
            seq,
            &content_hash,
        ) {
            Ok(k) => k,
            Err(e) => {
                self.metrics.record_abandoned_input_rejected();
                self.ack_waiters(waiters, Err(SpanWriteError::SegmentBuild(e.to_string())));
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
                Err(SpanWriteError::Abandoned(
                    "data object put exhausted retry budget or exceeded max_flush_lifetime".into(),
                )),
            );
            return;
        }

        let min_ingest_ts_ns = min_ingest_ts_ns.unwrap_or(flush_open_ns);
        let max_ingest_ts_ns = max_ingest_ts_ns.unwrap_or(flush_open_ns);
        let record = match record::build(NewCommitRecord {
            tenant_hash,
            signal: Signal::Spans,
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
            segment_format_version: u32::from(SPAN_SEGMENT_FORMAT_VERSION),
            created_unix_ns: flush_open_ns,
            ingest_hour_bucket,
        }) {
            Ok(r) => r,
            Err(e) => {
                self.metrics.record_abandoned_input_rejected();
                self.ack_waiters(waiters, Err(SpanWriteError::SegmentBuild(e.to_string())));
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
                    Err(SpanWriteError::Abandoned(
                        "commit publish exhausted retry budget or exceeded max_flush_lifetime"
                            .into(),
                    )),
                );
            }
        }
    }

    fn ack_waiters(&self, waiters: Vec<SpanAck>, result: Result<CommitToken, SpanWriteError>) {
        let ok = result.is_ok();
        self.metrics.record_acks(waiters.len(), ok);
        for waiter in waiters {
            let _ = waiter.send(result.clone());
        }
    }

    /// Races `fut` against the remaining budget to `deadline_ns` on the
    /// injected `Clock`, returning `None` if the deadline is already past or
    /// elapses while `fut` is still in flight. Identical in construction to
    /// [`crate::log_shard`]'s own `bound_to_deadline`: built on
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
    /// path. Mirrors [`crate::log_shard`]'s equivalent.
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
    /// checks `deadline_ns` between attempts. Mirrors [`crate::log_shard`]'s
    /// equivalent, including the pinned-identity split-brain panic: identity is
    /// fixed at flush open, so a split-brain cannot fire on a benign retry and
    /// means the pinning invariant was broken upstream.
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
                        "ravel-ingest: fatal split-brain on pinned span flush identity: this={this} stored={stored}"
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
        // timer, so retry timing shares the one clock the rest of the flush
        // path already uses (`bound_to_deadline`) and a test can drive it
        // deterministically by advancing that clock, with no real sleep.
        self.clock.sleep(Duration::from_millis(jittered_ms)).await;
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use std::pin::Pin;
    use std::sync::atomic::{AtomicI64, Ordering};

    use ravel_commit::{keys, record};
    use ravel_object_store::fault::{
        FaultKind, FaultPlan, FaultStore, Op, Rule, ScriptedFault, Sequence,
    };
    use ravel_object_store::memory::MemoryStore;
    use ravel_object_store::{GetRange, list_all};
    use ravel_rspan::{RspanReader, SpanQuery, StatusCode};
    use ravel_types::TenantHash;
    use tokio::sync::watch;
    use tokio::task::JoinHandle;

    use super::*;

    const BASE_NS: i64 = 1_700_000_000_000_000_000;

    /// Deterministic injected clock, the shard actor's flush tick sleeps on it
    /// (mirrors `log_shard`'s own `TestClock`, restated here because unit tests
    /// cannot import another module's private test harness).
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
        tx: mpsc::Sender<SpanShardMsg>,
        task: JoinHandle<()>,
        store: Arc<dyn ObjectStoreBackend>,
        metrics: Arc<SpanIngestMetrics>,
        clock: Arc<TestClock>,
    }

    impl Harness {
        fn spawn(config: IngestConfig) -> Self {
            Self::spawn_with_store(config, Arc::new(MemoryStore::new()))
        }

        fn spawn_with_store(config: IngestConfig, store: Arc<dyn ObjectStoreBackend>) -> Self {
            let clock = TestClock::new(BASE_NS);
            let metrics = Arc::new(SpanIngestMetrics::default());
            let (tx, rx) = mpsc::channel(64);
            let actor = SpanShardActor::new(
                0,
                Uuid::new_v4(),
                7,
                Arc::clone(&store),
                clock.clone(),
                Arc::new(ravel_commit::rng::SystemRng),
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
            let _ = self.tx.send(SpanShardMsg::Shutdown { done: done_tx }).await;
            let _ = done_rx.await;
            let _ = self.task.await;
        }
    }

    fn norm_span(trace: u8, span: u8, start_ns: i64, name: &str) -> NormalizedSpan {
        NormalizedSpan {
            trace_id: [trace; 16],
            span_id: [span; 8],
            parent_span_id: None,
            name: name.to_string(),
            start_ts_ns: start_ns,
            end_ts_ns: start_ns + 100,
            status_code: StatusCode::Unset,
            status_message: None,
            attrs: vec![("service.name".to_string(), "checkout".to_string())],
        }
    }

    /// Follows the commit token to its RSPAN object and returns the decoded
    /// commit record plus every span an unfiltered scan yields.
    async fn read_back(
        store: &dyn ObjectStoreBackend,
        tenant_hash: &TenantHash,
        token: &CommitToken,
    ) -> (CommitRecord, Vec<SpanRecord>) {
        let commit_key =
            keys::commit_key_for_token(tenant_hash, Signal::Spans, token).expect("commit key");
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
        let reader = RspanReader::new(&data_bytes, &RspanConfig::default()).expect("open rspan");
        let (spans, _stats) = reader
            .scan(&SpanQuery::ts_range(i64::MIN, i64::MAX))
            .expect("unfiltered scan");
        (rec, spans)
    }

    #[tokio::test]
    async fn size_flush_round_trips_to_a_readable_rspan_object() {
        let h = Harness::spawn(flush_on_first());
        let tenant = TenantId::new("acme");
        let spans = vec![
            norm_span(1, 1, 1_000, "root"),
            norm_span(1, 2, 2_000, "child"),
            norm_span(2, 1, 3_000, "other trace"),
        ];

        let (ack_tx, ack_rx) = oneshot::channel();
        h.tx.send(SpanShardMsg::Write {
            tenant: tenant.clone(),
            spans,
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
            u32::from(SPAN_SEGMENT_FORMAT_VERSION)
        );
        assert_eq!(rec.sample_count, 3, "every span is in one RSPAN object");
        assert_eq!(rec.series_count, 2, "two distinct trace ids");
        // The commit record's event-time range is the batch's span interval.
        assert_eq!(rec.min_event_ts_ns, 1_000);
        assert_eq!(rec.max_event_ts_ns, 3_100);
        assert_eq!(scanned.len(), 3, "every pushed span reads back");

        let snap = h.metrics.snapshot();
        assert_eq!(snap.flushes_by_size, 1);
        assert_eq!(snap.acks_ok, 1);
        h.shutdown().await;
    }

    #[tokio::test]
    async fn every_span_field_survives_the_round_trip() {
        let h = Harness::spawn(flush_on_first());
        let tenant = TenantId::new("acme");
        let mut span = norm_span(9, 4, 5_000, "GET /checkout");
        span.parent_span_id = Some([8u8; 8]);
        span.end_ts_ns = 9_000;
        span.status_code = StatusCode::Error;
        span.status_message = Some("boom".to_string());
        span.attrs = vec![
            ("http.method".to_string(), "GET".to_string()),
            ("service.name".to_string(), "checkout".to_string()),
        ];
        let expected = span.clone();

        let (ack_tx, ack_rx) = oneshot::channel();
        h.tx.send(SpanShardMsg::Write {
            tenant: tenant.clone(),
            spans: vec![span],
            ack: Some(ack_tx),
        })
        .await
        .expect("send write");
        let token = ack_rx.await.expect("ack").expect("commit");

        let (_rec, scanned) = read_back(h.store.as_ref(), &tenant.hash(), &token).await;
        assert_eq!(scanned, vec![to_rspan_record(expected)]);
        h.shutdown().await;
    }

    #[tokio::test]
    async fn age_flush_fires_when_the_injected_clock_advances() {
        let h = Harness::spawn(no_size_flush(Duration::from_millis(50)));
        let tenant = TenantId::new("acme");

        // Buffer first (so oldest_arrival is the pre-advance time), then push
        // the clock past max_flush_delay from the joined arm.
        let (ack_tx, ack_rx) = oneshot::channel();
        h.tx.send(SpanShardMsg::Write {
            tenant: tenant.clone(),
            spans: vec![norm_span(1, 1, 1_000, "op")],
            ack: Some(ack_tx),
        })
        .await
        .expect("send write");

        let (ack, ()) = tokio::join!(ack_rx, async {
            while h.metrics.snapshot().buffered_spans_total < 1 {
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
    async fn shutdown_drains_buffered_spans_under_the_spans_keyspace() {
        let h = Harness::spawn(no_size_flush(Duration::from_secs(3600)));
        let tenant = TenantId::new("acme");
        // Buffered (no ack): the span only reaches the store via the drain.
        h.tx.send(SpanShardMsg::Write {
            tenant: tenant.clone(),
            spans: vec![norm_span(1, 1, 1_000, "op")],
            ack: None,
        })
        .await
        .expect("send write");
        while h.metrics.snapshot().buffered_spans_total < 1 {
            tokio::task::yield_now().await;
        }

        let store = Arc::clone(&h.store);
        let metrics = Arc::clone(&h.metrics);
        h.shutdown().await;

        let prefix = format!("t/{}/s/", tenant.hash().to_hex());
        let objects = list_all(store.as_ref(), "t/").await.expect("list");
        assert!(
            objects
                .iter()
                .any(|o| o.key.starts_with(&prefix) && o.key.contains("/l0/")),
            "the shutdown drain stored a data object under the spans keyspace"
        );
        assert!(
            objects
                .iter()
                .any(|o| o.key.starts_with(&prefix) && o.key.contains("/c/")),
            "the shutdown drain stored a commit record"
        );
        assert_eq!(metrics.snapshot().flushes_manual, 1);
    }

    #[tokio::test]
    async fn two_tenants_in_one_shard_flush_independently() {
        let h = Harness::spawn(flush_on_first());
        let mut tokens = Vec::new();
        for (i, name) in ["acme", "globex"].into_iter().enumerate() {
            let tenant = TenantId::new(name);
            let (ack_tx, ack_rx) = oneshot::channel();
            h.tx.send(SpanShardMsg::Write {
                tenant: tenant.clone(),
                spans: vec![norm_span(i as u8 + 1, 1, 1_000, "op")],
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

    /// A permanently-retryable fault on every RSPAN data-object PUT drives
    /// exactly `put_retry_max_attempts + 1` inner PUT calls: one first attempt
    /// plus `max_attempts` retries, counted off the FaultStore.
    #[tokio::test]
    async fn span_data_put_makes_exactly_max_attempts_plus_one_calls() {
        let max_attempts = 3;
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::Transient("data down".into()))
                .with_key_contains("/l0/"),
        );
        let fault = Arc::new(FaultStore::new(MemoryStore::new(), plan));
        let store: Arc<dyn ObjectStoreBackend> = fault.clone();
        let h = Harness::spawn_with_store(exhaustion_config(max_attempts), Arc::clone(&store));

        let (ack_tx, ack_rx) = oneshot::channel();
        h.tx.send(SpanShardMsg::Write {
            tenant: TenantId::new("acme"),
            spans: vec![norm_span(1, 1, 1_000, "op")],
            ack: Some(ack_tx),
        })
        .await
        .expect("send write");
        let err = ack_rx
            .await
            .expect("ack sender not dropped")
            .expect_err("a data PUT that always fails must abandon the flush");
        assert!(matches!(err, SpanWriteError::Abandoned(_)));

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
    async fn span_commit_put_makes_exactly_max_attempts_plus_one_calls() {
        let max_attempts = 3;
        let plan = FaultPlan::empty().with_rule(
            Rule::new(Op::Put, ScriptedFault::Transient("commit down".into()))
                .with_key_contains("/c/"),
        );
        let fault = Arc::new(FaultStore::new(MemoryStore::new(), plan));
        let store: Arc<dyn ObjectStoreBackend> = fault.clone();
        let h = Harness::spawn_with_store(exhaustion_config(max_attempts), Arc::clone(&store));

        let (ack_tx, ack_rx) = oneshot::channel();
        h.tx.send(SpanShardMsg::Write {
            tenant: TenantId::new("acme"),
            spans: vec![norm_span(1, 1, 1_000, "op")],
            ack: Some(ack_tx),
        })
        .await
        .expect("send write");
        let err = ack_rx
            .await
            .expect("ack sender not dropped")
            .expect_err("a commit PUT that always fails must abandon the flush");
        assert!(matches!(err, SpanWriteError::Abandoned(_)));

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
    async fn span_retry_backoff_waits_on_the_injected_clock() {
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
        h.tx.send(SpanShardMsg::Write {
            tenant: tenant.clone(),
            spans: vec![norm_span(1, 1, 1_000, "op")],
            ack: Some(ack_tx),
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
}
